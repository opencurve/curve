/*
 * Project: curve
 * Created Date: Mon Nov 17 2018
 * Author: lixiaocui
 * Copyright (c) 2018 netease
 */

#include <glog/logging.h>
#include <string>
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
                                              conf.leaderSchedulerInterval,
                                              conf.transferLeaderTimeLimitSec,
                                              conf.removePeerTimeLimitSec,
                                              conf.addPeerTimeLimitSec);
        LOG(INFO) << "run leader scheduler ok!";
    }

    if (conf.enableCopysetScheduler) {
        schedulerController_[SchedulerType::CopySetSchedulerType] =
            std::make_shared<CopySetScheduler>(opController_,
                                               conf.copysetSchedulerInterval,
                                               conf.transferLeaderTimeLimitSec,
                                               conf.removePeerTimeLimitSec,
                                               conf.addPeerTimeLimitSec);
        LOG(INFO) << "run copySet scheduler ok!";
    }

    if (conf.enableRecoverScheduler) {
        schedulerController_[SchedulerType::RecoverSchedulerType] =
            std::make_shared<RecoverScheduler>(opController_,
                                               conf.recoverSchedulerInterval,
                                               conf.transferLeaderTimeLimitSec,
                                               conf.removePeerTimeLimitSec,
                                               conf.addPeerTimeLimitSec);
        LOG(INFO) << "run recover scheduler ok!";
    }

    if (conf.enableReplicaScheduler) {
        schedulerController_[SchedulerType::ReplicaSchedulerType] =
            std::make_shared<ReplicaScheduler>(opController_,
                                               conf.replicaSchedulerInterval,
                                               conf.transferLeaderTimeLimitSec,
                                               conf.removePeerTimeLimitSec,
                                               conf.addPeerTimeLimitSec);
        LOG(INFO) << "run replica scheduler ok!";
    }
}

void Coordinator::Run() {
    // run different scheduler at interval in different threads
    for (auto &v : schedulerController_) {
        runSchedulerThreads_[v.first] = std::thread(
            &Coordinator::RunScheduler, this, v.second);
    }
    SetSchedulerRunning(true);
}

void Coordinator::Stop() {
    if (schedulerRunning_) {
        SetSchedulerRunning(false);
        for (auto &v : schedulerController_) {
            runSchedulerThreads_[v.first].join();
        }
    }
}

bool Coordinator::CopySetHeartbeat(
    const ::curve::mds::topology::CopySetInfo &originInfo,
    ::curve::mds::heartbeat::CopysetConf *out) {
    CopySetInfo info;
    if (!topo_->CopySetFromTopoToSchedule(originInfo, &info)) {
        LOG(ERROR) << "coordinator cannot convert copySet(logicalPoolId:"
                   << originInfo.GetLogicalPoolId() << ", copySetId:"
                   << originInfo.GetId()
                   << ") from heartbeat topo form to schedule form error";
        return false;
    }

    Operator op;
    if (!opController_->GetOperatorById(info.id, &op)) {
        return false;
    }

    LOG(INFO) << "find operator on copySet(logicalPoolId:"
              << info.id.first
              << ", copySetId:" << info.id.second << ")";

    // 根据leader上报的copyset信息更新operator的状态
    // 如果有新的配置要下发，返回为true
    CopySetConf res;
    bool hasOrder = opController_->ApplyOperator(info, &res);
    if (hasOrder) {
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
        std::string candidate = ::curve::mds::topology::BuildPeerId(
            chunkServer.info.ip, chunkServer.info.port, 0);
        out->set_configchangeitem(candidate);

        // set 副本
        for (auto peer : res.peers) {
            out->add_peers(::curve::mds::topology::BuildPeerId(
                peer.ip, peer.port, 0));
        }
    }
    return hasOrder;
}

void Coordinator::RunScheduler(const std::shared_ptr<Scheduler> &s) {
    while (schedulerRunning_) {
        std::this_thread::
        sleep_for(std::chrono::seconds(s->GetRunningInterval()));

        s->Schedule(topo_);
    }
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
