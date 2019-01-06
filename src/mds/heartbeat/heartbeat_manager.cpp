/*
 * Project: curve
 * Created Date: Thu Jan 03 2019
 * Author: lixiaocui
 * Copyright (c) 2018 netease
 */

#include <glog/logging.h>
#include <thread>  //NOLINT
#include <utility>
#include <set>
#include "src/mds/heartbeat/heartbeat_manager.h"
#include "src/common/string_util.h"

using ::curve::mds::topology::CopySetKey;
using ::curve::mds::topology::PoolIdType;
using ::curve::mds::topology::CopySetIdType;
using ::curve::mds::topology::ChunkServerState;

namespace curve {
namespace mds {
namespace heartbeat {
int HeartbeatManager::Init(const HeartbeatOption &option) {
    this->heartbeatIntervalSec_ = option.heartbeatInterval;
    this->heartbeatMissTimeOutSec_ = option.heartbeatMissTimeOut;
    this->offLineTimeOutSec_ = option.offLineTimeOut;
    for (auto value : topology_->GetChunkServerInCluster()) {
        UpdateLastReceivedHeartbeatTime(value, steady_clock::now());
    }
    LOG(INFO) << "init heartbeat ok!";
}

void HeartbeatManager::Run() {
    if (isStop_.exchange(false)) {
        backEndThread_ = std::thread(&HeartbeatManager::HeartbeatBackEnd, this);
    }
}

void HeartbeatManager::Stop() {
    if (!isStop_.exchange(true)) {
        LOG(INFO) << "stop heartbeat manager";
        backEndThread_.join();
    }
}

void HeartbeatManager::UpdateLastReceivedHeartbeatTime(
    ChunkServerIdType csId, const steady_clock::time_point &time) {
    DVLOG(6) << "Update chunkServer:" << csId << " receive heartbeat time";
    ::curve::common::WriteLockGuard lk(hbinfoLock_);
    if (heartbeatInfos_.find(csId) == heartbeatInfos_.end()) {
        DVLOG(6) << "chunkServer " << csId
                 << " first register in heartbeatManager";
        heartbeatInfos_.emplace(csId, HeartbeatInfo(csId, time, true));
        return;
    }
    heartbeatInfos_[csId].lastReceivedTime = time;
}

void HeartbeatManager::ChunkServerHeartbeat(
    const ChunkServerHeartbeatRequest &request,
    ChunkServerHeartbeatResponse *response) {
    if (!CheckRequest(request)) {
        return;
    }
    UpdateLastReceivedHeartbeatTime(request.chunkserverid(),
                                    steady_clock::now());
    // TODO(lixiaocui): update chunkServer info to topologyStat
    // handle copySetInfo
    DVLOG(6) << "handle copySetInfo on chunkServer:"
             << request.chunkserverid() << " begin";
    for (auto &value : request.copysetinfos()) {
        DVLOG(6) << "handle copySet(logicalPoolId:" << value.logicalpoolid()
                 << ", copySetId:" << value.copysetid() << " begin";
        ::curve::mds::heartbeat::CopysetConf conf;
        ChunkServerIdType leader = GetPeerIdByIPPort(value.leaderpeer());
        if (leader == ::curve::mds::topology::UNINTIALIZE_ID) {
            LOG(ERROR) << "heartbeat manager can not get chunkServerInfo"
                       " according to report leader ipPort: "
                       << value.leaderpeer();
            continue;
        }

        if (request.chunkserverid() != leader) {
            DVLOG(6) << "this chunkServer is not leader of "
                     "copySet(logicalPoolId: " << value.logicalpoolid()
                     << ", copySetId:" << value.copysetid() << ")";
            continue;
        }

        if (HandleCopysetInfo(value, &conf)) {
            DVLOG(6) << "handle copySet(logicalPoolId: "
                     << value.logicalpoolid()
                     << ", copySetId:" << value.copysetid() << "), ok."
                     << "this copySet have order to execute";
            CopysetConf *res = response->add_needupdatecopysets();
            *res = conf;
        }
        DVLOG(6) << "handle copySet(logicalPoolId:" << value.logicalpoolid()
                 << ", copySetId:" << value.copysetid() << " end";
    }
    DVLOG(6) << "handle copySetInfo on chunkServer:"
             << request.chunkserverid() << " end";
}

bool HeartbeatManager::CheckRequest(
    const ChunkServerHeartbeatRequest &request) {
    ::curve::mds::topology::ChunkServer chunkServer;
    DVLOG(6) << "Check request from chunkServer(ip:" << request.ip()
             << ", port:" << request.port() << ") begin";
    if (!request.IsInitialized()) {
        LOG(ERROR) << "heartbeatManager receive heartbeat from chunkServer: "
                   << request.chunkserverid()
                   << " but not all required field is initialized";
        return false;
    }

    if (!topology_->GetChunkServer(request.chunkserverid(), &chunkServer)) {
        LOG(ERROR) << "heartbeatManager receive heartbeat from chunkServer: "
                   << request.chunkserverid()
                   << "but topology do not contain this one";
        return false;
    }

    // TODO(lixiaocui): handle the condition
    if (request.ip() != chunkServer.GetHostIp()
            || request.port() != chunkServer.GetPort()) {
        LOG(ERROR) << "heartbeatManager receive heartbeat from chunkServer: "
                   << request.chunkserverid() << ", but find report ip:"
                   << request.ip() << ", report port:" << request.port()
                   << " do not consistent with topo record ip:"
                   << chunkServer.GetHostIp() << ", record port:"
                   << chunkServer.GetPort();
        return false;
    }

    if (request.token() != chunkServer.GetToken()) {
        LOG(ERROR) << "heartbeatManager receive heartbeat from chunkServer"
                   << request.chunkserverid() << ", but fine report token:"
                   << request.token()
                   << " do not consistent with topo record token:"
                   << chunkServer.GetToken();
        return false;
    }
    DVLOG(6) << "Check request from chunkServer(ip:" << request.ip()
             << ", port:" << request.port() << "), OK";
    return true;
}

bool HeartbeatManager::HandleCopysetInfo(const CopysetInfo &copysetInfo,
        CopysetConf *copysetConf) {
    ::curve::mds::topology::CopySetInfo recordCopySetInfo;
    CopySetKey key =
        std::pair<PoolIdType, CopySetIdType>(
            static_cast<const PoolIdType &>(copysetInfo.logicalpoolid()),
            copysetInfo.copysetid());
    ::curve::mds::topology::CopySetInfo reportCopySetInfo;
    // convert ::curve::mds::heartbeat::CopysetInfo to
    //         ::curve::mds::topology::CopySetInfo
    if (!FromHeartbeatCopySetInfoToTopologyOne(copysetInfo,
            &reportCopySetInfo)) {
        LOG(ERROR) << "heartbeatManager receive copySet(logicalPoolId: "
                   << key.first << ", copySetId: " << key.second
                   << ") information, but can not transfer to topology one";
        return false;
    }
    // topology do not have copySet reported by heartbeat
    // return empty copysetConf to instruct chunkServer delete copySet
    if (!topology_->GetCopySet(key, &recordCopySetInfo)) {
        LOG(ERROR) << "heartbeatManager receive copySet(logicalPoolId: "
                   << key.first << ", copySetId: " << key.second
                   << ") information, but can not get info from topology";
        copysetConf->set_logicalpoolid(key.first);
        copysetConf->set_copysetid(key.second);
        copysetConf->set_epoch(0);
        return true;
    }
    // stale epoch, need to update copySet info
    bool needUpdate = false;
    if (recordCopySetInfo.GetEpoch() < copysetInfo.epoch()) {
        DVLOG(6) << "heartbeatManager find report CopySet(logicalPoolId"
                 << key.first << ", copySetId: " << key.second
                 << ") epoch:" << copysetInfo.epoch() << " > recordEpoch:"
                 << recordCopySetInfo.GetEpoch() << " need to update";
        needUpdate = true;
    } else if (recordCopySetInfo.GetEpoch() == copysetInfo.epoch()) {
        // report no candidate
        if (!reportCopySetInfo.HasCandidate()) {
            // report no candidate but record have
            if (recordCopySetInfo.HasCandidate()) {
                LOG(WARNING) << "heartbeatManager find report"
                             " CopySet(logicalPoolId"
                             << key.first << ", copySetId: " << key.second
                             << ") no candidate but record has candidate: "
                             << recordCopySetInfo.GetCandidate();
                needUpdate = true;
            }
        } else if (!recordCopySetInfo.HasCandidate()) {
            // report has candidate but record do not have
            needUpdate = true;
        } else if (reportCopySetInfo.GetCandidate() !=
                   recordCopySetInfo.GetCandidate()) {
            // report candidate is not same as record one
            LOG(WARNING) << "heartbeatManager find report candidate "
                         << reportCopySetInfo.GetCandidate()
                         << ", record candidate: "
                         << recordCopySetInfo.GetCandidate()
                         << " on copySet(logicalPoolId: "
                         << key.first << ", copySetId: " << key.second
                         << "not same";
            needUpdate = true;
        }
    } else if (recordCopySetInfo.GetEpoch() > copysetInfo.epoch()) {
        LOG(ERROR) << "heartbeatManager find copySet(logicalPoolId: "
                   << key.first << "copySetId: " << key.second
                   << "), record epoch:" << recordCopySetInfo.GetEpoch()
                   << " bigger than report epoch:" << copysetInfo.epoch();
        return false;
    }
    // update the repo and memory
    if (needUpdate) {
        LOG(INFO) << "heartbeatManager find copySet(" << key.first << ","
                  << key.second << ") need to update";

        int updateCode =
            topology_->UpdateCopySet(reportCopySetInfo);
        if (::curve::mds::topology::kTopoErrCodeSuccess != updateCode) {
            LOG(ERROR) << "heartbeatManager update copySet(logicalPoolId:"
                       << key.first << ", copySetId:" << key.second
                       << ") got error code: " << updateCode;
            return false;
        }
    }
    // tranfer to scheduler
    return PatrolCopySetInfo(recordCopySetInfo, copysetConf);
}

void HeartbeatManager::HeartbeatBackEnd() {
    while (!isStop_) {
        std::this_thread::
        sleep_for(std::chrono::seconds(heartbeatMissTimeOutSec_));
        CheckHeartBeatInterval();
    }
}
void HeartbeatManager::CheckHeartBeatInterval() {
    // check if heartbeat miss
    DVLOG(6) << "time to check heartbeat at backend";
    ::curve::common::WriteLockGuard lk(hbinfoLock_);

    for (auto &value : heartbeatInfos_) {
        bool needUpdate = false;
        steady_clock::duration timePass =
            steady_clock::now() - value.second.lastReceivedTime;
        if (timePass < std::chrono::seconds(heartbeatMissTimeOutSec_)) {
            if (!value.second.OnlineFlag) {
                LOG(INFO) << "chunkServer " << value.first << "is online";
                needUpdate = true;
            }
        } else {
            if (timePass < std::chrono::seconds(offLineTimeOutSec_)) {
                LOG(WARNING) << "heartbeatManager find chunkServer: "
                             << value.first << " heartbeat miss, "
                             << timePass / std::chrono::seconds(1)
                             << " seconds from last heartbeat";
            } else {
                if (value.second.OnlineFlag) {
                    LOG(ERROR) << "heartbeatManager find chunkServer: "
                               << value.first << " offline, "
                               << timePass / std::chrono::seconds(1)
                               << " seconds from last heartbeat";
                    needUpdate = true;
                }
            }
        }
        if (needUpdate) {
            LOG(INFO) << "heartbeatManager update chunkServer " << value.first
                      << " online flag from "
                      << value.second.OnlineFlag << " to "
                      << !value.second.OnlineFlag;
            value.second.OnlineFlag = !value.second.OnlineFlag;
            ::curve::mds::topology::ChunkServer chunkServer;
            topology_->GetChunkServer(value.first, &chunkServer);
            ChunkServerState state = chunkServer.GetChunkServerState();

            if (value.second.OnlineFlag) {
                state.SetOnlineState(OnlineState::ONLINE);
            } else {
                state.SetOnlineState(OnlineState::OFFLINE);
            }

            int updateErrCode =
                topology_->UpdateChunkServerState(state, value.first);
            if (::curve::mds::topology::kTopoErrCodeSuccess != updateErrCode) {
                LOG(ERROR) << "heartbeatManager update chunkserver state get "
                           "error code: " << updateErrCode;
            }
        }
    }
}

bool HeartbeatManager::GetHeartBeatInfo(ChunkServerIdType id,
                                        HeartbeatInfo *info) {
    if (heartbeatInfos_.find(id) == heartbeatInfos_.end()) {
        return false;
    }

    *info = heartbeatInfos_[id];
    return true;
}

ChunkServerIdType HeartbeatManager::GetPeerIdByIPPort(
    const std::string &ipPort) {
    std::vector<std::string> splits;
    ::curve::common::SplitString(ipPort, ":", &splits);
    if (splits.size() < 3) {
        LOG(ERROR) << "heartbeatManager find invalid form, "
                   "required: IP:PORT:ID, but get " << ipPort;
        return ::curve::mds::topology::UNINTIALIZE_ID;
    }
    ::curve::mds::topology::ChunkServer chunkServer;
    if (topology_->GetChunkServer(
                splits[0],
                static_cast<uint32_t>(std::stoul(splits[1], nullptr, 0)),
                &chunkServer)) {
        return chunkServer.GetId();
    }

    LOG(ERROR) << "heartbeatManager can not get chunkServer ip: " << splits[0]
               << ", port: " << splits[1] << " from topology";
    return ::curve::mds::topology::UNINTIALIZE_ID;
}

bool HeartbeatManager::FromHeartbeatCopySetInfoToTopologyOne(
    const curve::mds::heartbeat::CopysetInfo &info,
    ::curve::mds::topology::CopySetInfo *out) {
    ::curve::mds::topology::CopySetInfo needUpdateInfo(
        static_cast<PoolIdType>(info.logicalpoolid()),
        info.copysetid());
    // set epoch
    needUpdateInfo.SetEpoch(info.epoch());

    // set peers
    std::set<ChunkServerIdType> peers;
    for (auto value : info.peers()) {
        ChunkServerIdType res = GetPeerIdByIPPort(value);
        if (res == ::curve::mds::topology::UNINTIALIZE_ID) {
            LOG(ERROR) << "heartbeat manager can not get chunkServerInfo"
                       " according to report ipPort: " << value;
            return false;
        }
        peers.emplace(res);
    }
    needUpdateInfo.SetCopySetMembers(peers);

    // set leader
    ChunkServerIdType res = GetPeerIdByIPPort(info.leaderpeer());
    if (res == ::curve::mds::topology::UNINTIALIZE_ID) {
        LOG(ERROR) << "heartbeat manager can not get chunkServerInfo"
                   "according to report leader ipPort: "
                   << info.leaderpeer();
        return false;
    }
    needUpdateInfo.SetLeader(res);
    // has candidate, set
    if (info.configchangeinfo().IsInitialized()) {
        ChunkServerIdType res =
            GetPeerIdByIPPort(info.configchangeinfo().peer());
        if (res == ::curve::mds::topology::UNINTIALIZE_ID) {
            LOG(ERROR) << "heartbeat manager can not get chunkServerInfo"
                       "according to report candidate ipPort: "
                       << info.configchangeinfo().peer();
            return false;
        }
        needUpdateInfo.SetCandidate(res);
    }
    *out = needUpdateInfo;
    return true;
}

bool HeartbeatManager::PatrolCopySetInfo(
    const ::curve::mds::topology::CopySetInfo &reportInfo,
    curve::mds::heartbeat::CopysetConf *conf) {
    DVLOG(6) << "patrol copySet(logicalPoolId:"
             << reportInfo.GetLogicalPoolId() << ", copySetId:"
             << reportInfo.GetId() << ") to schedule";

    // transfer copySet info to schedule one
    ::curve::mds::schedule::CopySetInfo scheduleCopySet;
    if (!topoAdapter_->CopySetFromTopoToSchedule(
                reportInfo, &scheduleCopySet)) {
        LOG(ERROR) << "heartbeatManager cannot convert copySet(logicalPoolId:"
                   << reportInfo.GetLogicalPoolId() << ", copySetId:"
                   << reportInfo.GetId()
                   << ") from heartbeat topo form to schedule form error";
        return false;
    }

    ::curve::mds::schedule::CopySetConf res;
    bool status = coordinator_->CopySetHeartbeat(scheduleCopySet, &res);
    if (status) {
        conf->set_logicalpoolid(res.id.first);
        conf->set_copysetid(res.id.second);
        conf->set_epoch(res.epoch);
        conf->set_type(res.type);
        std::string candidate = GetIpPortByChunkServerId(res.configChangeItem);
        if (candidate.empty()) {
            return false;
        }
        conf->set_configchangeitem(candidate);
        for (auto peer : res.peers) {
            conf->add_peers(peer.ip + ":" + std::to_string(peer.port) + ":0");
        }
    }
    return status;
}

std::string HeartbeatManager::GetIpPortByChunkServerId(ChunkServerIdType id) {
    ::curve::mds::topology::ChunkServer chunkServer;
    std::string res;
    if (!topology_->GetChunkServer(id, &chunkServer)) {
        LOG(ERROR) << "heartbeatManager can not get chunkServer "
                   << id << " from topology";
        return res;
    }
    return chunkServer.GetHostIp() + ":" +
           std::to_string(chunkServer.GetPort()) + ":0";
}
}  // namespace heartbeat
}  // namespace mds
}  // namespace curve




