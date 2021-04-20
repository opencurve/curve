/*
 *  Copyright (c) 2020 NetEase Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

/*
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
#include "src/chunkserver/uri_paser.h"
#include "src/chunkserver/heartbeat_helper.h"

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
    storePath_ = UriParser::GetPathFromUri(options_.storeUri);
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

    // 初始化timer
    waitInterval_.Init(options_.intervalSec * 1000);

    // 获取当前unix时间戳
    startUpTime_ = ::curve::common::TimeUtility::GetTimeofDaySec();
    return 0;
}

int Heartbeat::Run() {
    hbThread_ = Thread(&Heartbeat::HeartbeatWorker, this);
    return 0;
}

int Heartbeat::Stop() {
    LOG(INFO) << "Stopping Heartbeat manager.";

    waitInterval_.StopWait();
    toStop_.store(true, std::memory_order_release);
    hbThread_.join();

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
        IOMetricPtr readMetric =
            copysetMetric->GetIOMetric(CSIOMetricType::READ_CHUNK);
        IOMetricPtr writeMetric =
            copysetMetric->GetIOMetric(CSIOMetricType::WRITE_CHUNK);
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

    ChunkServerMetric* metric = ChunkServerMetric::GetInstance();
    curve::mds::heartbeat::ChunkServerStatisticInfo* stats =
        new curve::mds::heartbeat::ChunkServerStatisticInfo();
    IOMetricPtr readMetric = metric->GetIOMetric(CSIOMetricType::READ_CHUNK);
    IOMetricPtr writeMetric = metric->GetIOMetric(CSIOMetricType::WRITE_CHUNK);
    if (readMetric != nullptr && writeMetric != nullptr) {
        stats->set_readrate(readMetric->bps_.get_value(1));
        stats->set_writerate(writeMetric->bps_.get_value(1));
        stats->set_readiops(readMetric->iops_.get_value(1));
        stats->set_writeiops(writeMetric->iops_.get_value(1));
    }
    CopysetNodeOptions opt = copysetMan_->GetCopysetNodeOptions();
    uint64_t chunkFileSize = opt.maxChunkSize;
    uint64_t walSegmentFileSize = opt.maxWalSegmentSize;
    uint64_t usedChunkSize = metric->GetTotalSnapshotCount() * chunkFileSize
                           + metric->GetTotalChunkCount() * chunkFileSize;
    uint64_t usedWalSegmentSize = metric->GetTotalWalSegmentCount()
                                * walSegmentFileSize;
    uint64_t trashedChunkSize = metric->GetChunkTrashedCount() * chunkFileSize;
    uint64_t leftChunkSize = metric->GetChunkLeftCount() * chunkFileSize;
    // leftWalSegmentSize will be 0 when CHUNK and WAL share file pool
    uint64_t leftWalSegmentSize = metric->GetWalSegmentLeftCount()
                                * walSegmentFileSize;

    stats->set_chunksizeusedbytes(usedChunkSize+usedWalSegmentSize);
    stats->set_chunksizeleftbytes(leftChunkSize+leftWalSegmentSize);
    stats->set_chunksizetrashedbytes(trashedChunkSize);
    req->set_allocated_stats(stats);

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
        if (cntl.ErrorCode() == EHOSTDOWN ||
            cntl.ErrorCode() == ETIMEDOUT ||
            cntl.ErrorCode() == brpc::ELOGOFF ||
            cntl.ErrorCode() == brpc::ERPCTIMEDOUT) {
            LOG(WARNING) << "current mds: " << mdsEps_[inServiceIndex_]
                         << " is shutdown or going to quit";
            inServiceIndex_ = (inServiceIndex_ + 1) % mdsEps_.size();
            LOG(INFO) << "next heartbeat switch to "
                      << mdsEps_[inServiceIndex_];
        } else {
            LOG(ERROR) << csEp_.ip << ":" << csEp_.port
                    << " Fail to send heartbeat to MDS "
                    << mdsEps_[inServiceIndex_] << ","
                    << " cntl errorCode: " << cntl.ErrorCode()
                    << " cntl error: " << cntl.ErrorText();
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
        CopysetNodePtr copyset = copysetMan_->GetCopysetNode(
                conf.logicalpoolid(), conf.copysetid());

        // 判断copyconf是否合法
        if (!HeartbeatHelper::CopySetConfValid(conf, copyset)) {
            continue;
        }

        // 解析该chunkserver上的copyset是否需要删除
        // 需要删除则清理copyset
        if (HeartbeatHelper::NeedPurge(csEp_, conf, copyset)) {
            LOG(INFO) << "Clean peer " << csEp_ << " of copyset("
                << conf.logicalpoolid() << "," << conf.copysetid()
                << "), groupId: "
                << ToGroupIdStr(conf.logicalpoolid(), conf.copysetid());
            PurgeCopyset(conf.logicalpoolid(), conf.copysetid());
            continue;
        }

        // 解析是否有配置变更需要执行
        if (!conf.has_type()) {
            LOG(INFO) << "Failed to parse task for copyset("
                << conf.logicalpoolid() << "," << conf.copysetid()
                << "), groupId: "
                << ToGroupIdStr(conf.logicalpoolid(), conf.copysetid());
            continue;
        }

        // 如果有配置变更需要执行，下发变更到copyset
        if (!HeartbeatHelper::PeerVaild(conf.configchangeitem().address())) {
            continue;
        }

        if (conf.epoch() != copyset->GetConfEpoch()) {
            LOG(WARNING) << "Config change epoch:" << conf.epoch()
                << " is not same as current:" << copyset->GetConfEpoch()
                << " on copyset("
                << conf.logicalpoolid() << "," <<  conf.copysetid()
                << "), groupId: "
                << ToGroupIdStr(conf.logicalpoolid(), conf.copysetid())
                << ", refuse change";
            continue;
        }

        // 根据不同的变更类型下发配置
        switch (conf.type()) {
        case curve::mds::heartbeat::TRANSFER_LEADER:
            {
                if (!HeartbeatHelper::ChunkServerLoadCopySetFin(
                        conf.configchangeitem().address())) {
                    LOG(INFO) << "Transfer leader to "
                        << conf.configchangeitem().address() << " on copyset"
                        << ToGroupIdStr(conf.logicalpoolid(), conf.copysetid())
                        << " reject. target chunkserver is loading copyset";
                    break;
                }

                LOG(INFO) << "Transfer leader to "
                    << conf.configchangeitem().address() << " on copyset"
                    << ToGroupIdStr(conf.logicalpoolid(), conf.copysetid());
                copyset->TransferLeader(conf.configchangeitem());
                break;
            }

        case curve::mds::heartbeat::ADD_PEER:
            LOG(INFO) << "Adding peer " << conf.configchangeitem().address()
                << " to copyset"
                << ToGroupIdStr(conf.logicalpoolid(), conf.copysetid());
            copyset->AddPeer(conf.configchangeitem());
            break;

        case curve::mds::heartbeat::REMOVE_PEER:
            LOG(INFO) << "Removing peer " << conf.configchangeitem().address()
                << " from copyset"
                << ToGroupIdStr(conf.logicalpoolid(), conf.copysetid());
            copyset->RemovePeer(conf.configchangeitem());
            break;

        case curve::mds::heartbeat::CHANGE_PEER:
            {
                std::vector<Peer> newPeers;
                if (HeartbeatHelper::BuildNewPeers(conf, &newPeers)) {
                    LOG(INFO) << "Change peer from "
                        << conf.oldpeer().address() << " to "
                        << conf.configchangeitem().address() << " on copyset"
                        << ToGroupIdStr(conf.logicalpoolid(), conf.copysetid());
                    copyset->ChangePeer(newPeers);
                } else {
                    LOG(ERROR) << "Build new peer for copyset"
                        << ToGroupIdStr(conf.logicalpoolid(), conf.copysetid())
                        << " failed";
                }
            }
            break;
        
        case curve::mds::heartbeat::START_SCAN_PEER:
            {
                ConfigChangeType type;
                Configuration confTemp;
                Peer peer;
                LogicPoolID poolId = conf.logicalpoolid();
                CopysetID copysetId = conf.copysetid();
                int ret = 0;

                if (ret = copyset->GetConfChange(&type, &confTemp, &peer) != 0) {
                    LOG(ERROR) << "Failed to get config change state of copyset "
                    << ToGroupIdStr(poolId, copysetId);
                    return ret;
                }  else if (type == curve::mds::heartbeat::NONE) {
                    LOG(INFO) << "Scan peer " << conf.configchangeitem().address()
                    << "to copyset " 
                    << ToGroupIdStr(poolId, copysetId);
                    if (!scanMan_->IsRepeatReq(poolId, copysetId)) {
                        scanMan_->Enqueue(poolId, copysetId);
                    } else {
                        LOG(INFO) << "Scan peer repeat request";
                    }
                }  else {
                    LOG(INFO) << "drop Scan peer, because exist config change, ConfigChangeType:" << type;
                }
                
            }
            break;
        
        case curve::mds::heartbeat::CANCEL_SCAN_PEER:
            {
                //todo Abnormal scenario
                int ret;
                LogicPoolID poolId = conf.logicalpoolid();
                CopysetID copysetId = conf.copysetid();
                ret = scanMan_->CancelScanJob(poolId, copysetId);
            }
            break;

        default:
            LOG(ERROR) << "Invalid configchange type: " << conf.type();
            break;
        }
    }

    return 0;
}

void Heartbeat::HeartbeatWorker() {
    int ret;
    int errorIntervalSec = 2;

    LOG(INFO) << "Starting Heartbeat worker thread.";

    // 处理配置等于0等异常情况
    if (options_.intervalSec <= 4) {
        errorIntervalSec = 2;
    } else {
        errorIntervalSec = options_.intervalSec / 2;
    }

    while (!toStop_.load(std::memory_order_acquire)) {
        HeartbeatRequest req;
        HeartbeatResponse resp;

        LOG(INFO) << "building heartbeat info";
        ret = BuildRequest(&req);
        if (ret != 0) {
            LOG(ERROR) << "Failed to build heartbeat request";
            ::sleep(errorIntervalSec);
            continue;
        }

        LOG(INFO) << "sending heartbeat info";
        ret = SendHeartbeat(req, &resp);
        if (ret != 0) {
            LOG(WARNING) << "Failed to send heartbeat to MDS";
            ::sleep(errorIntervalSec);
            continue;
        }

        LOG(INFO) << "executing heartbeat info";
        ret = ExecTask(resp);
        if (ret != 0) {
            LOG(ERROR) << "Failed to execute heartbeat tasks";
            ::sleep(errorIntervalSec);
            continue;
        }

        waitInterval_.WaitForNextExcution();
    }

    LOG(INFO) << "Heartbeat worker thread stopped.";
}

}  // namespace chunkserver
}  // namespace curve
