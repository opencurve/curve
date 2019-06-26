/*
 * Project: curve
 * Created Date: Mon Nov 17 2018
 * Author: lixiaocui
 * Copyright (c) 2018 netease
 */

#include <glog/logging.h>
#include <string>
#include <memory>
#include <thread>  //NOLINT
#include "src/mds/schedule/coordinator.h"
#include "src/mds/topology/topology_item.h"

namespace curve {
namespace mds {
namespace schedule {
Coordinator::Coordinator(const std::shared_ptr<TopoAdapter> &topo) {
    this->topo_ = topo;
    schedulerRunning_ = false;
}

Coordinator::~Coordinator() {
    Stop();
}

void Coordinator::InitScheduler(const ScheduleOption &conf) {
    opController_ =
        std::make_shared<OperatorController>(conf.operatorConcurrent);

    if (conf.enableLeaderScheduler) {
        schedulerController_[SchedulerType::LeaderSchedulerType] =
            std::make_shared<LeaderScheduler>(opController_,
                                              conf.leaderSchedulerIntervalSec,
                                              conf.transferLeaderTimeLimitSec,
                                              conf.removePeerTimeLimitSec,
                                              conf.addPeerTimeLimitSec,
                                              conf.scatterWithRangePerent,
                                              conf.minScatterWidth, topo_);
        LOG(INFO) << "init leader scheduler ok!";
    }

    if (conf.enableCopysetScheduler) {
        schedulerController_[SchedulerType::CopySetSchedulerType] =
            std::make_shared<CopySetScheduler>(opController_,
                                               conf.copysetSchedulerIntervalSec,
                                               conf.transferLeaderTimeLimitSec,
                                               conf.removePeerTimeLimitSec,
                                               conf.addPeerTimeLimitSec,
                                               conf.copysetNumRangePercent,
                                               conf.scatterWithRangePerent,
                                               conf.minScatterWidth,
                                               topo_);
        LOG(INFO) << "init copySet scheduler ok!";
    }

    if (conf.enableRecoverScheduler) {
        schedulerController_[SchedulerType::RecoverSchedulerType] =
            std::make_shared<RecoverScheduler>(opController_,
                                               conf.recoverSchedulerIntervalSec,
                                               conf.transferLeaderTimeLimitSec,
                                               conf.removePeerTimeLimitSec,
                                               conf.addPeerTimeLimitSec,
                                               conf.scatterWithRangePerent,
                                               conf.minScatterWidth,
                                               conf.chunkserverFailureTolerance,
                                               topo_);
        LOG(INFO) << "init recover scheduler ok!";
    }

    if (conf.enableReplicaScheduler) {
        schedulerController_[SchedulerType::ReplicaSchedulerType] =
            std::make_shared<ReplicaScheduler>(opController_,
                                               conf.replicaSchedulerIntervalSec,
                                               conf.transferLeaderTimeLimitSec,
                                               conf.removePeerTimeLimitSec,
                                               conf.addPeerTimeLimitSec,
                                               conf.scatterWithRangePerent,
                                               conf.minScatterWidth, topo_);
        LOG(INFO) << "init replica scheduler ok!";
    }
}

void Coordinator::Run() {
    SetSchedulerRunning(true);
    // run different scheduler at interval in different threads
    for (auto &v : schedulerController_) {
        runSchedulerThreads_[v.first] = std::thread(
            &Coordinator::RunScheduler, this, v.second);
    }
}

void Coordinator::Stop() {
    if (schedulerRunning_) {
        SetSchedulerRunning(false);
        for (auto &v : schedulerController_) {
            runSchedulerThreads_[v.first].join();
        }
    }
}

ChunkServerIdType Coordinator::CopySetHeartbeat(
    const ::curve::mds::topology::CopySetInfo &originInfo,
    const ::curve::mds::heartbeat::ConfigChangeInfo &configChInfo,
    ::curve::mds::heartbeat::CopySetConf *out) {
    CopySetInfo info;
    if (!topo_->CopySetFromTopoToSchedule(originInfo, &info)) {
        LOG(ERROR) << "coordinator cannot convert copySet(logicalPoolId:"
                   << originInfo.GetLogicalPoolId() << ", copySetId:"
                   << originInfo.GetId()
                   << ") from heartbeat topo form to schedule form error";
        return ::curve::mds::topology::UNINTIALIZE_ID;
    }
    info.configChangeInfo = configChInfo;

    Operator op;
    if (!opController_->GetOperatorById(info.id, &op)) {
        return ::curve::mds::topology::UNINTIALIZE_ID;
    }

    LOG(INFO) << "find operator on copySet(logicalPoolId:"
              << info.id.first
              << ", copySetId:" << info.id.second << "), operator: "
              << op.OpToString();

    // 根据leader上报的copyset信息更新operator的状态
    // 如果有新的配置要下发，返回为true
    CopySetConf res;
    bool hasOrder = opController_->ApplyOperator(info, &res);
    if (hasOrder) {
        // build心跳中需要返回的copysetConf
        if (!BuildCopySetConf(res, out)) {
            LOG(ERROR) << "build copyset conf for copySet(" << info.id.first
                       << "," << info.id.second << ") fail";
            return ::curve::mds::topology::UNINTIALIZE_ID;
        }

        LOG(INFO) << "order operator " << op.OpToString();
        return res.configChangeItem;
    }

    return ::curve::mds::topology::UNINTIALIZE_ID;
}

void Coordinator::RunScheduler(const std::shared_ptr<Scheduler> &s) {
    while (schedulerRunning_) {
        std::this_thread::
        sleep_for(std::chrono::seconds(s->GetRunningInterval()));

        s->Schedule();
    }
    LOG(INFO) << "scheduler exist.";
}

bool Coordinator::BuildCopySetConf(
    const CopySetConf &res, ::curve::mds::heartbeat::CopySetConf *out) {
    // build心跳中需要返回的copysetConf
    out->set_logicalpoolid(res.id.first);
    out->set_copysetid(res.id.second);
    out->set_epoch(res.epoch);
    out->set_type(res.type);

    // set candidate
    ChunkServerInfo chunkServer;
    if (!topo_->GetChunkServerInfo(res.configChangeItem, &chunkServer)) {
        LOG(ERROR) << "coordinator can not get chunkServer "
                << res.configChangeItem << " from topology";
        return false;
    }
    auto replica = new ::curve::common::Peer();
    replica->set_id(res.configChangeItem);
    replica->set_address(::curve::mds::topology::BuildPeerId(
        chunkServer.info.ip, chunkServer.info.port, 0));
    out->set_allocated_configchangeitem(replica);

    // set 副本
    for (auto peer : res.peers) {
        auto replica = out->add_peers();
        replica->set_id(peer.id);
        replica->set_address(::curve::mds::topology::BuildPeerId(
            peer.ip, peer.port, 0));
    }

    return true;
}

bool Coordinator::ChunkserverGoingToAdd(
    ChunkServerIDType csId, CopySetKey key) {
    Operator op;
    // copyset上没有需要变更的operator,
    if (!opController_->GetOperatorById(key, &op)) {
        return false;
    }

    // 该operator是add类型, 并且add=csId
    AddPeer *res = dynamic_cast<AddPeer *>(op.step.get());
    LOG(INFO) << "find operator " << op.OpToString();
    if (res != nullptr && csId == res->GetTargetPeer()) {
        LOG(INFO) << "chunkserver " << csId
                  << " is target of pending operator " << op.OpToString();
        return true;
    }

    return false;
}


void Coordinator::SetSchedulerRunning(bool flag) {
    std::lock_guard<std::mutex> guard(mutex_);
    schedulerRunning_ = flag;
}

std::shared_ptr<OperatorController> Coordinator::GetOpController() {
    return opController_;
}
}  // namespace schedule
}  // namespace mds
}  // namespace curve
