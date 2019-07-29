/*
 * Copyright (C) 2018 NetEase Inc. All rights reserved.
 * Project: Curve
 *
 * History:
 *          2018/12/20  Wenyu Zhou   Initial version
 */

#include <sys/statvfs.h>
#include <sys/time.h>
#include <brpc/channel.h>
#include <brpc/controller.h>
#include <braft/closure_helper.h>

#include <vector>
#include <memory>

#include "src/fs/fs_common.h"
#include "src/common/timeutility.h"
#include "src/chunkserver/heartbeat.h"
#include "src/chunkserver/chunkserverStorage/chunkserver_adaptor_util.h"

using curve::fs::FileSystemInfo;

namespace curve {
namespace chunkserver {
TaskStatus Heartbeat::PurgeCopyset(LogicPoolID poolId, CopysetID copysetId) {
    if (!copysetMan_->PurgeCopysetNodeData(poolId, copysetId)) {
        LOG(ERROR) << "Failed to clean copyset "
                   << ToGroupIdStr(poolId, copysetId) << " and its data.";

        return TaskStatus(-1, "Failed to clean copyset");
    }

    LOG(INFO) << "Successfully cleaned copyset "
              << ToGroupIdStr(poolId, copysetId) << " and its data.";

    return TaskStatus::OK();
}

int Heartbeat::Init(const HeartbeatOptions &options) {
    toStop_.store(false, std::memory_order_release);
    options_ = options;

    butil::ip_t csIp;
    storePath_ = FsAdaptorUtil::GetPathFromUri(options_.storeUri);
    if (butil::str2ip(options_.ip.c_str(), &csIp) < 0) {
        LOG(ERROR) << "Invalid Chunkserver IP provided: " << options_.ip;
        return -1;
    }
    csEp_ = butil::EndPoint(csIp, options_.port);
    LOG(INFO) << "Chunkserver address: " << options_.ip << ":" << options_.port;

    // mdsEps不能为空
    ::curve::common::SplitString(options_.mdsListenAddr, ",", &mdsEps_);
    if (mdsEps_.empty()) {
        LOG(ERROR) << "Invalid mds ip provided: " << options_.mdsListenAddr;
        return -1;
    }
    // 检查每个地址的合法性
    for (auto addr : mdsEps_) {
        butil::EndPoint endpt;
        if (butil::str2endpoint(addr.c_str(), &endpt) < 0) {
            LOG(ERROR) << "Invalid sub mds ip:port provided: " << addr;
            return -1;
        }
    }

    inServiceIndex_ = 0;
    LOG(INFO) << "MDS address: " << options_.mdsListenAddr;

    copysetMan_ = options.copysetNodeManager;

    // 获取当前unix时间戳
    startUpTime_ = ::curve::common::TimeUtility::GetTimeofDaySec();
    return 0;
}

int Heartbeat::Run() {
    hbThread_ = std::unique_ptr<std::thread>(
            new std::thread(std::bind(Heartbeat::HeartbeatWorker, this)));

    return 0;
}

int Heartbeat::Stop() {
    LOG(INFO) << "Stopping Heartbeat manager.";
    toStop_.store(true, std::memory_order_release);

    hbThread_->join();
    LOG(INFO) << "Stopped Heartbeat manager.";

    return 0;
}

int Heartbeat::Fini() {
    Stop();

    LOG(INFO) << "Heartbeat manager cleaned up.";
    return 0;
}

int Heartbeat::GetFileSystemSpaces(size_t* capacity, size_t* avail) {
    int ret;
    struct FileSystemInfo info;

    ret = options_.fs->Statfs(storePath_, &info);
    if (ret != 0) {
        LOG(ERROR) << "Failed to get file system space information, "
                   << " error message: " << strerror(errno);
        return -1;
    }

    *capacity = info.total;
    *avail = info.available;

    return 0;
}

int Heartbeat::BuildCopysetInfo(curve::mds::heartbeat::CopySetInfo* info,
                                CopysetNodePtr copyset) {
    int ret;
    LogicPoolID poolId = copyset->GetLogicPoolId();
    CopysetID copysetId = copyset->GetCopysetId();

    info->set_logicalpoolid(poolId);
    info->set_copysetid(copysetId);
    info->set_epoch(copyset->GetConfEpoch());

    std::vector<Peer> peers;
    copyset->ListPeers(&peers);
    for (Peer peer : peers) {
        auto replica = info->add_peers();
        replica->set_address(peer.address().c_str());
    }

    PeerId leader = copyset->GetLeaderId();
    auto replica = new ::curve::common::Peer();
    replica->set_address(leader.to_string());
    info->set_allocated_leaderpeer(replica);

    curve::mds::heartbeat::CopysetStatistics* stats =
        new curve::mds::heartbeat::CopysetStatistics();
    CopysetMetricPtr copysetMetric =
        ChunkServerMetric::GetInstance()->GetCopysetMetric(poolId, copysetId);
    if (copysetMetric != nullptr) {
        IOMetricPtr readMetric = copysetMetric->GetReadMetric();
        IOMetricPtr writeMetric = copysetMetric->GetWriteMetric();
        if (readMetric != nullptr && writeMetric != nullptr) {
            stats->set_readrate(readMetric->bps_.get_value(1));
            stats->set_writerate(writeMetric->bps_.get_value(1));
            stats->set_readiops(readMetric->iops_.get_value(1));
            stats->set_writeiops(writeMetric->iops_.get_value(1));
            info->set_allocated_stats(stats);
        } else {
            LOG(ERROR) << "Failed to get copyset io metric."
                       << "logic pool id: " << poolId
                       << ", copyset id: " << copysetId;
        }
    }

    ConfigChangeType type;
    Configuration conf;
    Peer peer;

    if ((ret = copyset->GetConfChange(&type, &conf, &peer)) != 0) {
        LOG(ERROR) << "Failed to get config change state of copyset "
                   << ToGroupIdStr(poolId, copysetId);
        return ret;
    } else if (type == curve::mds::heartbeat::NONE) {
        return 0;
    }

    ConfigChangeInfo* confChxInfo = new ConfigChangeInfo();
    replica = new(std::nothrow) ::curve::common::Peer();
    if (replica == nullptr) {
        LOG(ERROR) << "apply memory error";
        return -1;
    }
    replica->set_address(peer.address());
    confChxInfo->set_allocated_peer(replica);
    confChxInfo->set_type(type);
    confChxInfo->set_finished(false);
    info->set_allocated_configchangeinfo(confChxInfo);

    return 0;
}

int Heartbeat::BuildRequest(HeartbeatRequest* req) {
    int ret;

    req->set_chunkserverid(options_.chunkserverId);
    req->set_token(options_.chunkserverToken);
    req->set_starttime(startUpTime_);
    req->set_ip(options_.ip);
    req->set_port(options_.port);

    /*
     * TODO(wenyu): DiskState field is not valid yet until disk health feature
     * is ready
     */
    curve::mds::heartbeat::DiskState* diskState =
                    new curve::mds::heartbeat::DiskState();
    diskState->set_errtype(0);
    diskState->set_errmsg("");
    req->set_allocated_diskstate(diskState);

    curve::mds::heartbeat::ChunkServerStatisticInfo* stats =
        new curve::mds::heartbeat::ChunkServerStatisticInfo();
    IOMetricPtr readMetric =
        ChunkServerMetric::GetInstance()->GetReadMetric();
    IOMetricPtr writeMetric =
        ChunkServerMetric::GetInstance()->GetWriteMetric();
    if (readMetric != nullptr && writeMetric != nullptr) {
        stats->set_readrate(readMetric->bps_.get_value(1));
        stats->set_writerate(writeMetric->bps_.get_value(1));
        stats->set_readiops(readMetric->iops_.get_value(1));
        stats->set_writeiops(writeMetric->iops_.get_value(1));
        req->set_allocated_stats(stats);
    }

    size_t cap, avail;
    ret = GetFileSystemSpaces(&cap, &avail);
    if (ret != 0) {
        LOG(ERROR) << "Failed to get file system space information for path "
                   << storePath_;
        return -1;
    }
    req->set_diskcapacity(cap);
    req->set_diskused(cap - avail);

    std::vector<CopysetNodePtr> copysets;
    copysetMan_->GetAllCopysetNodes(&copysets);

    req->set_copysetcount(copysets.size());
    int leaders = 0;

    for (CopysetNodePtr copyset : copysets) {
        curve::mds::heartbeat::CopySetInfo* info = req->add_copysetinfos();

        ret = BuildCopysetInfo(info, copyset);
        if (ret != 0) {
            LOG(ERROR) << "Failed to build heartbeat information of copyset "
                       << ToGroupIdStr(copyset->GetLogicPoolId(),
                                     copyset->GetCopysetId());
            continue;
        }
        if (copyset->IsLeaderTerm()) {
            ++leaders;
        }
    }
    req->set_leadercount(leaders);

    return 0;
}

void Heartbeat::DumpHeartbeatRequest(const HeartbeatRequest& request) {
    DVLOG(6) << "Heartbeat request: Chunkserver ID: "
             << request.chunkserverid()
             << ", IP: " << request.ip() << ", port: " << request.port()
             << ", copyset count: " << request.copysetcount()
             << ", leader count: " << request.leadercount();
    for (int i = 0; i < request.copysetinfos_size(); i ++) {
        const curve::mds::heartbeat::CopySetInfo& info =
            request.copysetinfos(i);

        std::string peersStr = "";
        for (int j = 0; j < info.peers_size(); j ++) {
            peersStr += info.peers(j).address() + ",";
        }

        DVLOG(6) << "Copyset " << i << " "
                 << ToGroupIdStr(info.logicalpoolid(), info.copysetid())
                 << ", epoch: " << info.epoch()
                 << ", leader: " << info.leaderpeer().address()
                 << ", peers: " << peersStr;

        if (info.has_configchangeinfo()) {
            const ConfigChangeInfo& cxInfo = info.configchangeinfo();
            DVLOG(6) << "Config change info: peer: " << cxInfo.peer().address()
                     << ", finished: " << cxInfo.finished()
                     << ", errno: " << cxInfo.err().errtype()
                     << ", errmsg: " << cxInfo.err().errmsg();
        }
    }
}

void Heartbeat::DumpHeartbeatResponse(const HeartbeatResponse& response) {
    int count = response.needupdatecopysets_size();
    if (count > 0) {
        LOG(INFO) << "Received " << count << " config change commands:";
        for (int i = 0; i < count; i ++) {
            CopySetConf conf = response.needupdatecopysets(i);

            int type = (conf.has_type()) ? conf.type() : 0;
            std::string item = (conf.has_configchangeitem()) ?
                conf.configchangeitem().address() : "";

            std::string peersStr = "";
            for (int j = 0; j < conf.peers_size(); j ++) {
                peersStr += conf.peers(j).address();
            }

            LOG(INFO) << "Config change " << i << ": "
                     << "Copyset < " << conf.logicalpoolid()
                     << ", " << conf.copysetid() << ">, epoch: "
                     << conf.epoch() << ", Peers: " << peersStr
                     << ", type: " << type << ", item: " << item;
        }
    } else {
        LOG(INFO) << "Received no config change command.";
    }
}

int Heartbeat::SendHeartbeat(const HeartbeatRequest& request,
                             HeartbeatResponse* response) {
    brpc::Channel channel;
    if (channel.Init(mdsEps_[inServiceIndex_].c_str(), NULL) != 0) {
        LOG(ERROR) << csEp_.ip << ":" << csEp_.port
                   << " Fail to init channel to MDS "
                   << mdsEps_[inServiceIndex_];
        return -1;
    }

    curve::mds::heartbeat::HeartbeatService_Stub stub(&channel);
    brpc::Controller cntl;
    cntl.set_timeout_ms(options_.timeout);

    DumpHeartbeatRequest(request);

    stub.ChunkServerHeartbeat(&cntl, &request, response, nullptr);
    if (cntl.Failed()) {
        LOG(ERROR) << csEp_.ip << ":" << csEp_.port
                   << " Fail to send heartbeat to MDS "
                   << mdsEps_[inServiceIndex_] << ","
                   << " cntl errorCode: " << cntl.ErrorCode()
                   << " cntl error: " << cntl.ErrorText();
        if (cntl.ErrorCode() == EHOSTDOWN ||
            cntl.ErrorCode() == brpc::ELOGOFF) {
            LOG(WARNING) << "current mds: " << mdsEps_[inServiceIndex_]
                         << " is shutdown or going to quit";
            inServiceIndex_ = (inServiceIndex_ + 1) % mdsEps_.size();
            LOG(INFO) << "next heartbeat switch to "
                      << mdsEps_[inServiceIndex_];
        }
        return -1;
    } else {
        DumpHeartbeatResponse(*response);
    }

    return 0;
}

int Heartbeat::ExecTask(const HeartbeatResponse& response) {
    int count = response.needupdatecopysets_size();
    for (int i = 0; i < count; i ++) {
        CopySetConf conf = response.needupdatecopysets(i);
        GroupNid groupId = ToGroupNid(conf.logicalpoolid(), conf.copysetid());

        uint64_t epoch = conf.epoch();

        CopysetNodePtr copyset = copysetMan_->GetCopysetNode(
                conf.logicalpoolid(), conf.copysetid());
        // chunkserver中不存在需要变更的copyset
        if (copyset == nullptr) {
            TaskStatus status(ENOENT, "Failed to find copyset <%u, %u>",
                              conf.logicalpoolid(), conf.copysetid());
            LOG(ERROR) << status.error_str();
            continue;
        }

        // CLDCFS-1004 bug-fix: mds下发epoch为0, 配置为空的copyset
        if (0 == epoch && conf.peers().empty()) {
            LOG(INFO) << "Clean copyset "
                      << ToGroupIdStr(conf.logicalpoolid(), conf.copysetid())
                      << "in peer " << csEp_
                      << ", witch is not exist in mds record";
            PurgeCopyset(conf.logicalpoolid(), conf.copysetid());
            continue;
        }

        // 下发的变更epoch < copyset实际的epoch
        if (epoch < copyset->GetConfEpoch()) {
            TaskStatus status(EINVAL, "Invalid epoch aginast copyset <%u, %u>"
                                      " expected epoch: %lu, recevied: %lu",
                              conf.logicalpoolid(),
                              conf.copysetid(),
                              copyset->GetConfEpoch(),
                              epoch);
            LOG(WARNING) << status.error_str();
            continue;
        }

        // 该chunkserver不在copyset的配置中
        bool cleanPeer = true;
        for (int j = 0; j < conf.peers_size(); j ++) {
            std::string epStr = std::string(butil::endpoint2str(csEp_).c_str());
            if (conf.peers(j).address().find(epStr) != std::string::npos) {
                cleanPeer = false;
                break;
            }
        }

        if (cleanPeer) {
            LOG(INFO) << "Clean peer " << csEp_ << " of copyset "
                    << ToGroupIdStr(conf.logicalpoolid(), conf.copysetid());
            PurgeCopyset(conf.logicalpoolid(), conf.copysetid());
            continue;
        }

        bool confChx = conf.has_type();
        if (!confChx) {
            TaskStatus status(EINVAL, "Failed to parse task against copyset"
                                      "<%u, %u>",
                              conf.logicalpoolid(), conf.copysetid());
            LOG(ERROR) << status.error_str();
            continue;
        }
        PeerId peerId;
        std::string peerStr = conf.configchangeitem().address();
        if (peerId.parse(peerStr) != 0) {
            TaskStatus status(EINVAL, "Failed to parse peerId against copyset"
                                      "<%u, %u>, received peerId %s",
                              conf.logicalpoolid(), conf.copysetid(),
                              peerStr.c_str());
            LOG(ERROR) << status.error_str();
            continue;
        }
        Peer peer;
        peer.set_address(peerId.to_string());
        switch (conf.type()) {
        case curve::mds::heartbeat::TRANSFER_LEADER:
            LOG(INFO) << "Transfer leader to " << peerId << " on copyset "
                      << ToGroupIdStr(conf.logicalpoolid(), conf.copysetid());
            copyset->TransferLeader(peer);
            break;
        case curve::mds::heartbeat::ADD_PEER:
            LOG(INFO) << "Adding peer " << peerId << " to copyset "
                      << ToGroupIdStr(conf.logicalpoolid(), conf.copysetid());
            copyset->AddPeer(peer);
            break;
        case curve::mds::heartbeat::REMOVE_PEER:
            LOG(INFO) << "Removing peer " << peerId << " from copyset "
                      << ToGroupIdStr(conf.logicalpoolid(), conf.copysetid());
            copyset->RemovePeer(peer);
            break;
        default:
            TaskStatus status(EINVAL, "Invalid operation %d against copyset"
                                      "<%u, %u>",
                              conf.type(),
                              conf.logicalpoolid(), conf.copysetid());
            LOG(ERROR) << status.error_str();
            break;
        }
    }

    return 0;
}

void Heartbeat::WaitForNextHeartbeat() {
    static int64_t t0 = butil::monotonic_time_ms();
    int64_t t1 = butil::monotonic_time_ms();

    int rest = options_.interval - (t1 - t0) / 1000;
    rest = (rest < 0) ? 0 : rest;

    t0 = t1;
    DVLOG(6) << "Heartbeat waiting interval";

    sleep(rest);
    DVLOG(6) << "Heartbeat finished waiting";
}

void Heartbeat::HeartbeatWorker(Heartbeat *heartbeat) {
    int ret;

    LOG(INFO) << "Starting Heartbeat worker thread.";

    while (!heartbeat->toStop_.load(std::memory_order_acquire)) {
        HeartbeatRequest req;
        HeartbeatResponse resp;

        LOG(INFO) << "building heartbeat info";
        ret = heartbeat->BuildRequest(&req);
        if (ret != 0) {
            LOG(ERROR) << "Failed to build heartbeat request";
            sleep(1);
            continue;
        }

        LOG(INFO) << "sending heartbeat info";
        ret = heartbeat->SendHeartbeat(req, &resp);
        if (ret != 0) {
            LOG(WARNING) << "Failed to send heartbeat to MDS";
            sleep(1);
            continue;
        }

        LOG(INFO) << "executing heartbeat info";
        ret = heartbeat->ExecTask(resp);
        if (ret != 0) {
            LOG(ERROR) << "Failed to execute heartbeat tasks";
            sleep(1);
            continue;
        }

        heartbeat->WaitForNextHeartbeat();
    }

    LOG(INFO) << "Heartbeat worker thread stopped.";
}

}  // namespace chunkserver
}  // namespace curve
