/*
 * Project: curve
 * Created Date: Mon Nov 17 2018
 * Author: lixiaocui
 * Copyright (c) 2018 netease
 */

#include <glog/logging.h>
#include <thread>  //NOLINT
#include "src/mds/schedule/coordinator.h"

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

void Coordinator::InitScheduler(const ScheduleConfig &conf) {
    opController_ =
        std::make_shared<OperatorController>(conf.OperatorConcurrent);

    if (conf.EnableLeaderScheduler) {
        schedulerController_[SchedulerType::LeaderSchedulerType] =
            std::make_shared<LeaderScheduler>(opController_,
                                              conf.LeaderSchedulerInterval,
                                              conf.TransferLeaderTimeLimitSec,
                                              conf.RemovePeerTimeLimitSec,
                                              conf.AddPeerTimeLimitSec);
        LOG(INFO) << "run leader scheduler ok!";
    }

    if (conf.EnableCopysetScheduler) {
        schedulerController_[SchedulerType::CopySetSchedulerType] =
            std::make_shared<CopySetScheduler>(opController_,
                                               conf.CopysetSchedulerInterval,
                                               conf.TransferLeaderTimeLimitSec,
                                               conf.RemovePeerTimeLimitSec,
                                               conf.AddPeerTimeLimitSec);
        LOG(INFO) << "run copySet scheduler ok!";
    }

    if (conf.EnableRecoverScheduler) {
        schedulerController_[SchedulerType::RecoverSchedulerType] =
            std::make_shared<RecoverScheduler>(opController_,
                                               conf.RecoverSchedulerInterval,
                                               conf.TransferLeaderTimeLimitSec,
                                               conf.RemovePeerTimeLimitSec,
                                               conf.AddPeerTimeLimitSec);
        LOG(INFO) << "run recover scheduler ok!";
    }

    if (conf.EnableReplicaScheduler) {
        schedulerController_[SchedulerType::ReplicaSchedulerType] =
            std::make_shared<ReplicaScheduler>(opController_,
                                               conf.ReplicaSchedulerInterval,
                                               conf.TransferLeaderTimeLimitSec,
                                               conf.RemovePeerTimeLimitSec,
                                               conf.AddPeerTimeLimitSec);
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

bool Coordinator::CopySetHeartbeat(const CopySetInfo &originInfo,
                                   CopySetConf *newConf) {
    Operator op;
    if (!opController_->GetOperatorById(originInfo.id, &op)) {
        return false;
    }

    LOG(INFO) << "find operator on copySet(logicalPoolId:"
              << originInfo.id.first
              << ", copySetId:" << originInfo.id.second << ")";
    return opController_->ApplyOperator(originInfo, newConf);
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
