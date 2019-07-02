/*
 * Project: curve
 * Created Date: Thu Jan 03 2019
 * Author: lixiaocui
 * Copyright (c) 2018 netease
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
        backEndThread_.join();
        LOG(INFO) << "stop heartbeatManager ok!";
    } else {
        LOG(INFO) << "heartbeatManager not running.";
    }
}

void HeartbeatManager::ChunkServerHealthyChecker() {
    while (!isStop_) {
        std::this_thread::
        sleep_for(
            std::chrono::milliseconds(chunkserverHealthyCheckerRunInter_));
        healthyChecker_->CheckHeartBeatInterval();
    }
}

void HeartbeatManager::UpdateChunkServerDiskStatus(
    const ChunkServerHeartbeatRequest &request) {
    // 更新ChunkServerState数据
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
    // 更新到topologyStat
    if (request.has_stats()) {
        stat.readRate = request.stats().readrate();
        stat.writeRate = request.stats().writerate();
        stat.readIOPS = request.stats().readiops();
        stat.writeIOPS = request.stats().writeiops();

        for (int i = 0; i < request.copysetinfos_size(); i++) {
            CopysetStat cstat;
            cstat.logicalPoolId = request.copysetinfos(i).logicalpoolid();
            cstat.copysetId = request.copysetinfos(i).copysetid();

            // TODO(xuchaojie) : 后续支持新的协议之后可直接使用id
            std::string leaderPeer =
                request.copysetinfos(i).leaderpeer().address();
            std::string leaderIp;
            uint32_t leaderPort;
            if (SplitPeerId(leaderPeer, &leaderIp, &leaderPort)) {
                cstat.leader =
                    topology_->FindChunkServerNotRetired(
                    leaderIp, leaderPort);
                if (UNINTIALIZE_ID == cstat.leader) {
                    LOG(ERROR) << "hearbeat failed on FindChunkServer,"
                               << "leaderIp = " << leaderIp
                               << "leaderPort = " << leaderPort;
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
                LOG(ERROR) << "hearbeat manager receive request "
                           << "copyset {" << cstat.logicalPoolId
                           << ", " << cstat.copysetId << "} "
                           << "do not have CopysetStatistics";
            }
            stat.copysetStats.push_back(cstat);
        }

    } else {
        LOG(ERROR) << "hearbeat manager receive request "
                   << "do not have ChunkServerStatisticInfo";
    }
    topologyStat_->UpdateChunkServerStat(request.chunkserverid(), stat);
}

void HeartbeatManager::ChunkServerHeartbeat(
    const ChunkServerHeartbeatRequest &request,
    ChunkServerHeartbeatResponse *response) {
    // 检查request的合法性
    if (!CheckRequest(request)) {
        LOG(ERROR) << "heartbeatManager get error request";
        return;
    }
    // 将心跳上报时间点pass到chunkserver健康检查模块
    healthyChecker_->UpdateLastReceivedHeartbeatTime(request.chunkserverid(),
                                    steady_clock::now());

    UpdateChunkServerDiskStatus(request);

    UpdateChunkServerStatistics(request);

    // 处理心跳中的copyset
    for (auto &value : request.copysetinfos()) {
        // 逻辑池不可用时，不处理该逻辑池的copyset信息
        ::curve::mds::topology::LogicalPool lPool;
        if (topology_->GetLogicalPool(value.logicalpoolid(), &lPool)) {
            if (lPool.GetLogicalPoolAvaliableFlag() != true) {
                continue;
            }
        }
        // heartbeat中copysetInfo格式转化为topology的格式
        ::curve::mds::topology::CopySetInfo reportCopySetInfo;
        if (!FromHeartbeatCopySetInfoToTopologyOne(value,
                &reportCopySetInfo)) {
            LOG(ERROR) << "heartbeatManager receive copySet(logicalPoolId: "
                       << value.logicalpoolid() << ", copySetId: "
                       << value.copysetid()
                       << ") information, but can not transfer to topology one";
            continue;
        }

        // 把上报的copyset的信息转发到CopysetConfGenerator模块处理
        CopySetConf conf;
        ConfigChangeInfo configChInfo;
        if (copysetConfGenerator_->GenCopysetConf(
                request.chunkserverid(), reportCopySetInfo,
                value.configchangeinfo(), &conf)) {
            CopySetConf *res = response->add_needupdatecopysets();
            *res = conf;
        }

        // 如果是leader, 根据leader上报的信息
        if (request.chunkserverid() == reportCopySetInfo.GetLeader()) {
            topoUpdater_->UpdateTopo(reportCopySetInfo);
        }
    }
}

bool HeartbeatManager::CheckRequest(
    const ChunkServerHeartbeatRequest &request) {
    ChunkServer chunkServer;
    // 所有的字段是否都初始化
    if (!request.IsInitialized()) {
        LOG(ERROR) << "heartbeatManager receive heartbeat from unknown"
                   " chunkServer not all required field is initialized: "
                   << request.InitializationErrorString();
        return false;
    }

    if (!topology_->GetChunkServer(request.chunkserverid(), &chunkServer)) {
        LOG(ERROR) << "heartbeatManager receive heartbeat from chunkServer: "
                   << request.chunkserverid()
                   << "but topology do not contain this one";
        return false;
    }

    if (chunkServer.GetStatus() == ChunkServerStatus::RETIRED) {
        LOG(ERROR) << "heartbeatManager receive heartbeat from"
                   << "retired chunkserver: " << chunkServer.GetId()
                   << ", reject.";
        return false;
    }

    // TODO(lixiaocui): 这种情况具体如何处理
    // ip和port如果变化，涉及到的变更信息如下：
    // 1. chunkserver本身ip和port要变，topology中需要体现这个
    // 2. mds记录的副本关系和raft中记录的副本关系就不一致了，
    //    正常情况副本关系都应该以raft中的为准，这种情况应该要通知raft去
    //    更正正确的副本位置，这里具体的实现方式还不知道
    // chunkserver上报的ip和mds记录的不匹配
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

    // chunkserver上报的token和mds记录的不匹配
    if (request.token() != chunkServer.GetToken()) {
        LOG(ERROR) << "heartbeatManager receive heartbeat from chunkServer"
                   << request.chunkserverid() << ", but fine report token:"
                   << request.token()
                   << " do not consistent with topo record token:"
                   << chunkServer.GetToken();
        return false;
    }
    return true;
}

bool HeartbeatManager::FromHeartbeatCopySetInfoToTopologyOne(
    const ::curve::mds::heartbeat::CopySetInfo &info,
    ::curve::mds::topology::CopySetInfo *out) {
    ::curve::mds::topology::CopySetInfo topoCopysetInfo(
        static_cast<PoolIdType>(info.logicalpoolid()), info.copysetid());
    // 设置 epoch
    topoCopysetInfo.SetEpoch(info.epoch());

    // 设置 peers
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

    // 设置 leader
    topoCopysetInfo.SetLeader(leader);

    // 设置配置变更信息
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
    *out = topoCopysetInfo;
    return true;
}

ChunkServerIdType HeartbeatManager::GetChunkserverIdByPeerStr(
    std::string peer) {
    // 解析string, 获取ip, port, id
    std::string ip;
    uint32_t port, id;
    bool ok = curve::mds::topology::SplitPeerId(peer, &ip, &port, &id);
    if (!ok) {
        LOG(ERROR) << "report [" << peer << "] is not a valid ip:port:id form";
        return false;
    }

    // 根据ip:port获取chunkserverId
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




