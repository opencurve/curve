/*
 * Copyright (C) 2018 NetEase Inc. All rights reserved.
 * Project: Curve
 *
 * History:
 *          2018/12/20  Wenyu Zhou   Initial version
 */

#include <sys/statvfs.h>
#include <brpc/channel.h>
#include <brpc/controller.h>
#include <braft/closure_helper.h>

#include <vector>

#include "src/fs/fs_common.h"
#include "src/chunkserver/heartbeat.h"
#include "src/chunkserver/chunkserverStorage/chunkserver_adaptor_util.h"

namespace curve {
namespace chunkserver {

using curve::fs::FileSystemInfo;

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

    dataDirPath_ = FsAdaptorUtil::GetPathFromUri(options_.dataUri);

    butil::ip_t mdsIp;
    butil::ip_t csIp;
    if (butil::str2ip(options_.mdsIp.c_str(), &mdsIp) < 0) {
        LOG(ERROR) << "Invalid MDS IP provided: " << options_.mdsIp;
        return -1;
    }
    if (butil::str2ip(options_.ip.c_str(), &csIp) < 0) {
        LOG(ERROR) << "Invalid Chunkserver IP provided: " << options_.ip;
        return -1;
    }
    if (options_.mdsPort <= 0 || options_.mdsPort >= 65535) {
        LOG(ERROR) << "Invalid MDS port provided: " << options_.mdsPort;
        return -1;
    }
    if (options_.port <= 0 || options_.port >= 65535) {
        LOG(ERROR) << "Invalid Chunkserver port provided: " << options_.port;
        return -1;
    }
    mdsEp_ = butil::EndPoint(mdsIp, options_.mdsPort);
    csEp_ = butil::EndPoint(csIp, options_.port);
    LOG(INFO) << "MDS address: " << options_.mdsIp << ":" << options_.mdsPort;
    LOG(INFO) << "Chunkserver address: " << options_.ip << ":" << options_.port;

    copysetMan_ = options.copysetNodeManager;

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

    ret = options_.fs->Statfs(dataDirPath_, &info);
    if (ret != 0) {
        LOG(ERROR) << "Failed to get file system space information, "
                   << " error message: " << strerror(errno);
        return -1;
    }

    *capacity = info.total;
    *avail = info.available;

    return 0;
}

int Heartbeat::BuildCopysetInfo(curve::mds::heartbeat::CopysetInfo* info,
                                CopysetNodePtr copyset) {
    int ret;
    LogicPoolID poolId = copyset->GetLogicPoolId();
    CopysetID copysetId = copyset->GetCopysetId();

    info->set_logicalpoolid(poolId);
    info->set_copysetid(copysetId);
    info->set_epoch(copyset->GetConfEpoch());

    std::vector<PeerId> peers;

    copyset->ListPeers(&peers);
    for (PeerId peer : peers) {
        info->add_peers(peer.to_string().c_str());
    }

    PeerId leader = copyset->GetLeaderId();
    info->set_leaderpeer(leader.to_string());

    /*
     * TODO(wenyu): copyset stats field will not be valid until performance
     * monitoring feature is ready
     */

    ConfigChangeType    type;
    Configuration       conf;
    PeerId              peer;

    if ((ret = copyset->GetConfChange(&type, &conf, &peer)) != 0) {
        LOG(ERROR) << "Failed to get config change state of copyset "
                   << ToGroupIdStr(poolId, copysetId);
        return ret;
    } else if (type == curve::mds::heartbeat::NONE) {
        return 0;
    }

    ConfigChangeInfo* confChxInfo = new ConfigChangeInfo();
    confChxInfo->set_peer(peer.to_string());
    confChxInfo->set_finished(false);
    info->set_allocated_configchangeinfo(confChxInfo);

    return 0;
}

int Heartbeat::BuildRequest(HeartbeatRequest* req) {
    int ret;

    req->set_chunkserverid(options_.chunkserverId);
    req->set_token(options_.chunkserverToken);
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
    /*
     * TODO(wenyu): stats field will not be valid until performance monitoring
     * is ready
     */
    curve::mds::heartbeat::ChunkServerStatisticInfo* stats =
                    new curve::mds::heartbeat::ChunkServerStatisticInfo();
    stats->set_readrate(0);
    stats->set_writerate(0);
    stats->set_readiops(0);
    stats->set_writeiops(0);
    req->set_allocated_stats(stats);

    size_t cap, avail;
    ret = GetFileSystemSpaces(&cap, &avail);
    if (ret != 0) {
        LOG(ERROR) << "Failed to get file system space information for path "
                   << dataDirPath_;
        return -1;
    }
    req->set_diskcapacity(cap);
    req->set_diskused(cap - avail);

    std::vector<CopysetNodePtr> copysets;
    copysetMan_->GetAllCopysetNodes(&copysets);

    req->set_copysetcount(copysets.size());
    int leaders = 0;

    for (CopysetNodePtr copyset : copysets) {
        curve::mds::heartbeat::CopysetInfo* info = req->add_copysetinfos();

        ret = BuildCopysetInfo(info, copyset);
        if (ret != 0) {
            LOG(ERROR) << "Failed to build heartbeat information of copyset "
                       << ToGroupIdStr(copyset->GetLogicPoolId(),
                                     copyset->GetCopysetId());
            return -1;
        }
        if (copyset->IsLeader()) {
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
        const curve::mds::heartbeat::CopysetInfo& info =
            request.copysetinfos(i);

        std::string peersStr = "";
        for (int j = 0; j < info.peers_size(); j ++) {
            peersStr += info.peers(j) + ",";
        }

        DVLOG(6) << "Copyset " << i << " "
                 << ToGroupIdStr(info.logicalpoolid(), info.copysetid())
                 << ", epoch: " << info.epoch()
                 << ", leader: " << info.leaderpeer()
                 << ", peers: " << peersStr;

        if (info.has_configchangeinfo()) {
            const ConfigChangeInfo& cxInfo = info.configchangeinfo();
            DVLOG(6) << "Config change info: peer: " << cxInfo.peer()
                     << ", finished: " << cxInfo.finished()
                     << ", errno: " << cxInfo.err().errtype()
                     << ", errmsg: " << cxInfo.err().errmsg();
        }
    }
}

void Heartbeat::DumpHeartbeatResponse(const HeartbeatResponse& response) {
    int count = response.needupdatecopysets_size();
    if (count > 0) {
        DVLOG(6) << "Received " << count << " config change commands:";
        for (int i = 0; i < count; i ++) {
            CopysetConf conf = response.needupdatecopysets(i);

            int type = (conf.has_type()) ? conf.type() : 0;
            std::string item = (conf.has_configchangeitem()) ?
                conf.configchangeitem() : "";

            std::string peersStr = "";
            for (int j = 0; j < conf.peers_size(); j ++) {
                peersStr += conf.peers(j);
            }

            DVLOG(6) << "Config change " << i << ": "
                     << "Copyset < " << conf.logicalpoolid()
                     << ", " << conf.copysetid() << ">, epoch: "
                     << conf.epoch() << ", Peers: " << peersStr
                     << ", type: " << type << ", item: " << item;
        }
    } else {
        DVLOG(6) << "Received no config change command.";
    }
}

int Heartbeat::SendHeartbeat(const HeartbeatRequest& request,
                             HeartbeatResponse* response) {
    brpc::Channel channel;
    if (channel.Init(mdsEp_, NULL) != 0) {
        LOG(ERROR) << "Fail to init channel to MDS " << mdsEp_;
    }

    curve::mds::heartbeat::HeartbeatService_Stub stub(&channel);
    brpc::Controller cntl;
    cntl.set_timeout_ms(options_.timeout);

    DVLOG(6) << "Sending heartbeat to MDS " << mdsEp_;
    DumpHeartbeatRequest(request);

    stub.ChunkServerHeartbeat(&cntl, &request, response, nullptr);
    if (cntl.Failed()) {
        LOG(ERROR) << "Fail to send heartbeat to MDS " << mdsEp_ << ","
                   << " cntl error: " << cntl.ErrorText();
        return -1;
    } else {
        DumpHeartbeatResponse(*response);
    }

    return 0;
}

int Heartbeat::ExecTask(const HeartbeatResponse& response) {
    int count = response.needupdatecopysets_size();
    for (int i = 0; i < count; i ++) {
        CopysetConf conf = response.needupdatecopysets(i);
        GroupNid groupId = ToGroupNid(conf.logicalpoolid(), conf.copysetid());

        uint64_t epoch = conf.epoch();

        // Perform validations
        CopysetNodePtr copyset = copysetMan_->GetCopysetNode(
                conf.logicalpoolid(), conf.copysetid());
        if (copyset == nullptr) {
            TaskStatus status(ENOENT, "Failed to find copyset <%u, %u>",
                              conf.logicalpoolid(), conf.copysetid());
            LOG(ERROR) << status.error_str();
            continue;
        }
        if (epoch < copyset->GetConfEpoch()) {
            TaskStatus status(EINVAL, "Invalid epoch aginast copyset <%u, %u>"
                                      " expected epoch: %lu, recevied: %lu",
                              conf.logicalpoolid(),
                              conf.copysetid(),
                              copyset->GetConfEpoch(),
                              epoch);
            LOG(ERROR) << status.error_str();
            continue;
        }

        // Determine if to clean peer or not
        bool cleanPeer = true;
        for (int j = 0; j < conf.peers_size(); j ++) {
            std::string epStr = std::string(butil::endpoint2str(csEp_).c_str());
            if (conf.peers(j).find(epStr) != std::string::npos) {
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
        std::string peerStr = conf.configchangeitem();
        if (peerId.parse(peerStr) != 0) {
            TaskStatus status(EINVAL, "Failed to parse peerId against copyset"
                                      "<%u, %u>, received peerId %s",
                              conf.logicalpoolid(), conf.copysetid(),
                              peerStr.c_str());
            LOG(ERROR) << status.error_str();
            continue;
        }
        switch (conf.type()) {
        case curve::mds::heartbeat::TRANSFER_LEADER:
            LOG(INFO) << "Transfer leader to " << peerId << " on copyset "
                      << ToGroupIdStr(conf.logicalpoolid(), conf.copysetid());
            copyset->TransferLeader(peerId);
            break;
        case curve::mds::heartbeat::ADD_PEER:
            LOG(INFO) << "Adding peer " << peerId << " to copyset "
                      << ToGroupIdStr(conf.logicalpoolid(), conf.copysetid());
            copyset->AddPeer(peerId);
            break;
        case curve::mds::heartbeat::REMOVE_PEER:
            LOG(INFO) << "Removing peer " << peerId << " from copyset "
                      << ToGroupIdStr(conf.logicalpoolid(), conf.copysetid());
            copyset->RemovePeer(peerId);
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

        DVLOG(1) << "building heartbeat info";
        ret = heartbeat->BuildRequest(&req);
        if (ret != 0) {
            LOG(ERROR) << "Failed to build heartbeat request";
            sleep(1);
            continue;
        }

        DVLOG(1) << "sending heartbeat info";
        ret = heartbeat->SendHeartbeat(req, &resp);
        if (ret != 0) {
            LOG(ERROR) << "Failed to send heartbeat to MDS";
            sleep(1);
            continue;
        }

        DVLOG(1) << "executing heartbeat info";
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
