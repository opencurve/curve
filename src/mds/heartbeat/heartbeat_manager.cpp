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
 * Project: curve
 * Created Date: Thu Jan 03 2019
 * Author: lixiaocui
 */

#include <glog/logging.h>
#include <utility>
#include <set>
#include "src/mds/heartbeat/heartbeat_manager.h"
#include "src/common/string_util.h"
#include "src/mds/topology/topology_stat.h"

using ::curve::mds::topology::ChunkServer;
using ::curve::mds::topology::CopySetKey;
using ::curve::mds::topology::PoolIdType;
using ::curve::mds::topology::CopySetIdType;
using ::curve::mds::topology::ChunkServerState;
using ::curve::mds::topology::UNINTIALIZE_ID;
using ::curve::mds::topology::ChunkServerStatus;
using ::curve::mds::topology::ChunkServerStat;
using ::curve::mds::topology::CopysetStat;
using ::curve::mds::topology::SplitPeerId;

namespace curve {
namespace mds {
namespace heartbeat {
HeartbeatManager::HeartbeatManager(HeartbeatOption option,
    std::shared_ptr<Topology> topology,
    std::shared_ptr<TopologyStat> topologyStat,
    std::shared_ptr<Coordinator> coordinator)
    : topology_(topology),
      topologyStat_(topologyStat) {
    healthyChecker_ =
        std::make_shared<ChunkserverHealthyChecker>(option, topology);

    topoUpdater_ = std::make_shared<TopoUpdater>(topology);

    copysetConfGenerator_ =
        std::make_shared<CopysetConfGenerator>(topology, coordinator,
            option.mdsStartTime, option.cleanFollowerAfterMs);

    isStop_ = true;
    chunkserverHealthyCheckerRunInter_ = option.heartbeatMissTimeOutMs;
}

void HeartbeatManager::Init() {
    for (auto value : topology_->GetChunkServerInCluster(
        [] (const ChunkServer &cs) {
           return cs.GetStatus() != ChunkServerStatus::RETIRED;
        })) {
        healthyChecker_->UpdateLastReceivedHeartbeatTime(
            value, steady_clock::now());
    }
    LOG(INFO) << "init heartbeatManager ok!";
}

void HeartbeatManager::Run() {
    if (isStop_.exchange(false)) {
        backEndThread_ =
            Thread(&HeartbeatManager::ChunkServerHealthyChecker, this);
    }
}

void HeartbeatManager::Stop() {
    if (!isStop_.exchange(true)) {
        LOG(INFO) << "stop heartbeatManager...";
        sleeper_.interrupt();
        backEndThread_.join();
        LOG(INFO) << "stop heartbeatManager ok.";
    } else {
        LOG(INFO) << "heartbeatManager not running.";
    }
}

void HeartbeatManager::ChunkServerHealthyChecker() {
    while (sleeper_.wait_for(
        std::chrono::milliseconds(chunkserverHealthyCheckerRunInter_))) {
        healthyChecker_->CheckHeartBeatInterval();
    }
}

void HeartbeatManager::UpdateChunkServerDiskStatus(
    const ChunkServerHeartbeatRequest &request) {
    // update ChunkServerState status (disk status)
    ChunkServerState state;
    if (request.diskstate().errtype() != 0) {
        state.SetDiskState(curve::mds::topology::DISKERROR);
        LOG(ERROR) << "heartbeat report disk error, "
                   << "errortype = " << request.diskstate().errtype()
                   << "errmsg = " << request.diskstate().errmsg();
    } else {
        state.SetDiskState(curve::mds::topology::DISKNORMAL);
    }

    state.SetDiskCapacity(request.diskcapacity());
    state.SetDiskUsed(request.diskused());
    int ret = topology_->UpdateChunkServerDiskStatus(state,
        request.chunkserverid());
    if (ret != curve::mds::topology::kTopoErrCodeSuccess) {
        LOG(ERROR) << "heartbeat UpdateDiskStatus get an error, ret ="
                   << ret;
    }
}

void HeartbeatManager::UpdateChunkServerStatistics(
    const ChunkServerHeartbeatRequest &request) {
    ChunkServerStat stat;
    stat.leaderCount = request.leadercount();
    stat.copysetCount = request.copysetcount();
    // update chunkserver status to topologyStat
    if (request.has_stats()) {
        stat.readRate = request.stats().readrate();
        stat.writeRate = request.stats().writerate();
        stat.readIOPS = request.stats().readiops();
        stat.writeIOPS = request.stats().writeiops();
        stat.chunkSizeUsedBytes = request.stats().chunksizeusedbytes();
        stat.chunkSizeLeftBytes = request.stats().chunksizeleftbytes();
        stat.chunkSizeTrashedBytes = request.stats().chunksizetrashedbytes();

        for (int i = 0; i < request.copysetinfos_size(); i++) {
            CopysetStat cstat;
            cstat.logicalPoolId = request.copysetinfos(i).logicalpoolid();
            cstat.copysetId = request.copysetinfos(i).copysetid();

            // TODO(xuchaojie): use id instead when new protocol supported
            std::string leaderPeer =
                request.copysetinfos(i).leaderpeer().address();
            std::string leaderIp;
            uint32_t leaderPort;
            if (SplitPeerId(leaderPeer, &leaderIp, &leaderPort)) {
                cstat.leader =
                    topology_->FindChunkServerNotRetired(
                    leaderIp, leaderPort);
                if (UNINTIALIZE_ID == cstat.leader) {
                    LOG(INFO) << "hearbeat receive from chunkserver(id:"
                        << request.chunkserverid()
                        << ",ip:"<< request.ip() << ",port:" << request.port()
                        << "), in which copyset(" << cstat.logicalPoolId
                        << "," << cstat.copysetId << ") dose not have leader.";
                }
            } else {
                LOG(ERROR) << "hearbeat failed on SplitPeerId, "
                           << "peerId string = " << leaderPeer;
            }
            if (request.copysetinfos(i).has_stats()) {
                cstat.readRate = request.copysetinfos(i).stats().readrate();
                cstat.writeRate = request.copysetinfos(i).stats().writerate();
                cstat.readIOPS = request.copysetinfos(i).stats().readiops();
                cstat.writeIOPS = request.copysetinfos(i).stats().writeiops();
            } else {
                LOG(WARNING) << "hearbeat manager receive request "
                             << "copyset {" << cstat.logicalPoolId
                             << ", " << cstat.copysetId << "} "
                             << "do not have CopysetStatistics";
            }
            stat.copysetStats.push_back(cstat);
        }

    } else {
        LOG(WARNING) << "hearbeat manager receive request "
                     << "do not have ChunkServerStatisticInfo";
    }
    topologyStat_->UpdateChunkServerStat(request.chunkserverid(), stat);
}

void HeartbeatManager::ChunkServerHeartbeat(
    const ChunkServerHeartbeatRequest &request,
    ChunkServerHeartbeatResponse *response) {
    response->set_statuscode(HeartbeatStatusCode::hbOK);
    // check validity of heartbeat request
    HeartbeatStatusCode ret = CheckRequest(request);
    if (ret != HeartbeatStatusCode::hbOK) {
        LOG(ERROR) << "heartbeatManager get error request";
        response->set_statuscode(ret);
        return;
    }

    // record startUpTime data from chunkserver to topology
    if (request.has_starttime()) {
        topology_->UpdateChunkServerStartUpTime(
            request.starttime(), request.chunkserverid());
    }

    // pass heartbeat timestamp to chunkserver health checker
    healthyChecker_->UpdateLastReceivedHeartbeatTime(request.chunkserverid(),
                                    steady_clock::now());

    UpdateChunkServerDiskStatus(request);

    UpdateChunkServerStatistics(request);
    // no copyset info in the request
    if (request.copysetinfos_size() == 0) {
        response->set_statuscode(HeartbeatStatusCode::hbRequestNoCopyset);
    }
    // dealing with copysets included in the heartbeat request
    for (auto &value : request.copysetinfos()) {
        // discard copysets of invalid logical pool
        ::curve::mds::topology::LogicalPool lPool;
        if (topology_->GetLogicalPool(value.logicalpoolid(), &lPool)) {
            if (lPool.GetLogicalPoolAvaliableFlag() != true) {
                continue;
            }
        }
        // convert copysetInfo from heartbeat format to topology format
        ::curve::mds::topology::CopySetInfo reportCopySetInfo;
        if (!FromHeartbeatCopySetInfoToTopologyOne(value,
                &reportCopySetInfo)) {
            LOG(ERROR) << "heartbeatManager receive copyset("
                       << value.logicalpoolid() << ","
                       << value.copysetid()
                       << ") information, but can not transfer to topology one";
            response->set_statuscode(
                            HeartbeatStatusCode::hbAnalyseCopysetError);
            continue;
        }

        // forward reported copyset info to CopysetConfGenerator
        CopySetConf conf;
        ConfigChangeInfo configChInfo;
        if (copysetConfGenerator_->GenCopysetConf(
                request.chunkserverid(), reportCopySetInfo,
                value.configchangeinfo(), &conf)) {
            CopySetConf *res = response->add_needupdatecopysets();
            *res = conf;
        }

        // if a copyset is the leader, update (e.g. epoch) topology according
        // to its info
        if (request.chunkserverid() == reportCopySetInfo.GetLeader()) {
            topoUpdater_->UpdateTopo(reportCopySetInfo);
        }
    }
}

HeartbeatStatusCode HeartbeatManager::CheckRequest(
    const ChunkServerHeartbeatRequest &request) {
    ChunkServer chunkServer;
    // check for any uninitialize field
    if (!request.IsInitialized()) {
        LOG(ERROR) << "heartbeatManager receive heartbeat from unknown"
                   " chunkServer not all required field is initialized: "
                   << request.InitializationErrorString();
        return HeartbeatStatusCode::hbParamUnInitialized;
    }
    // check for validity of chunkserver id
    if (!topology_->GetChunkServer(request.chunkserverid(), &chunkServer)) {
        LOG(ERROR) << "heartbeatManager receive heartbeat from chunkServer: "
                   << request.chunkserverid()
                   << ", ip:"<< request.ip() << ", port:" << request.port()
                   << ", but topology do not contain this one";
        return HeartbeatStatusCode::hbChunkserverUnknown;
    }

    if (chunkServer.GetStatus() == ChunkServerStatus::RETIRED) {
        LOG(ERROR) << "heartbeatManager receive heartbeat from"
                   << "retired chunkserver: " << chunkServer.GetId()
                   << ", reject.";
        return HeartbeatStatusCode::hbChunkserverRetired;
    }

    // TODO(lixiaocui): How to deal with this situation?
    // if there's any change on ip address or port, certain things will change:
    // 1. ip address and port of chunkserver itself will change, this should be
    //    reflected on topology
    // 2. copy relationship that recorded on mds and raft will be different.
    //    in normal cases the one that recorded on raft should be the standard
    //    and we should acknowledge raft (in this case) to update the location
    //    of eplicas, but we still have no idea how to implement.
    // mismatch ip address reported by chunkserver and mds record
    if (request.ip() != chunkServer.GetHostIp()
            || request.port() != chunkServer.GetPort()) {
        LOG(ERROR) << "heartbeatManager receive heartbeat from chunkServer: "
                   << request.chunkserverid() << ", but find report ip:"
                   << request.ip() << ", report port:" << request.port()
                   << " do not consistent with topo record ip:"
                   << chunkServer.GetHostIp() << ", record port:"
                   << chunkServer.GetPort();
        return HeartbeatStatusCode::hbChunkserverIpPortNotMatch;
    }

    // mismatch token reported by chunkserver and mds record
    if (request.token() != chunkServer.GetToken()) {
        LOG(ERROR) << "heartbeatManager receive heartbeat from chunkServer"
                   << request.chunkserverid() << ", but fine report token:"
                   << request.token()
                   << " do not consistent with topo record token:"
                   << chunkServer.GetToken();
        return HeartbeatStatusCode::hbChunkserverTokenNotMatch;
    }
    return HeartbeatStatusCode::hbOK;
}

bool HeartbeatManager::FromHeartbeatCopySetInfoToTopologyOne(
    const ::curve::mds::heartbeat::CopySetInfo &info,
    ::curve::mds::topology::CopySetInfo *out) {
    ::curve::mds::topology::CopySetInfo topoCopysetInfo(
        static_cast<PoolIdType>(info.logicalpoolid()), info.copysetid());
    // set epoch
    topoCopysetInfo.SetEpoch(info.epoch());

    // set peers
    std::set<ChunkServerIdType> peers;
    ChunkServerIdType leader;
    for (auto value : info.peers()) {
        ChunkServerIdType res = GetChunkserverIdByPeerStr(value.address());
        if (UNINTIALIZE_ID == res) {
            LOG(ERROR) << "heartbeat manager can not get chunkServerInfo"
                       " according to report ipPort: " << value.address();
            return false;
        }

        if (value.address() == info.leaderpeer().address()) {
            leader = res;
        }
        peers.emplace(res);
    }
    topoCopysetInfo.SetCopySetMembers(peers);

    // set leader
    topoCopysetInfo.SetLeader(leader);

    // set info of configuration changes
    if (info.configchangeinfo().IsInitialized()) {
        ChunkServerIdType res =
            GetChunkserverIdByPeerStr(info.configchangeinfo().peer().address());
        if (res == UNINTIALIZE_ID) {
            LOG(ERROR) << "heartbeat manager can not get chunkServerInfo"
                       "according to report candidate ipPort: "
                       << info.configchangeinfo().peer().address();
            return false;
        }
        topoCopysetInfo.SetCandidate(res);
    }

    // set scaning
    if (info.has_scaning()) {
        topoCopysetInfo.SetScaning(info.scaning());
    }

    // set last scan
    if (info.has_lastscansec()) {
        topoCopysetInfo.SetLastScanSec(info.lastscansec());
    }

    *out = topoCopysetInfo;
    return true;
}

ChunkServerIdType HeartbeatManager::GetChunkserverIdByPeerStr(
    std::string peer) {
    // resolute peer string for ip, port and chunkserverid
    std::string ip;
    uint32_t port, id;
    bool ok = curve::mds::topology::SplitPeerId(peer, &ip, &port, &id);
    if (!ok) {
        LOG(ERROR) << "report [" << peer << "] is not a valid ip:port:id form";
        return false;
    }

    // fetch chunkserverId according to ip:port pair
    ChunkServer chunkServer;
    if (topology_->GetChunkServerNotRetired(ip, port, &chunkServer)) {
        return chunkServer.GetId();
    }

    LOG(ERROR) << "heartbeatManager can not get chunkServer ip: " << ip
               << ", port: " << port << " from topology";
    return UNINTIALIZE_ID;
}
}  // namespace heartbeat
}  // namespace mds
}  // namespace curve




