/*
 * Project: curve
 * Created Date: Thu Jan 03 2019
 * Author: lixiaocui
 * Copyright (c) 2018 netease
 */

#include "src/mds/schedule/scheduler.h"
namespace curve {
namespace mds {
namespace schedule {
Scheduler::Scheduler(int trans, int remove, int add) {
    this->transTimeSec_ = trans;
    this->removeTimeSec_ = remove;
    this->addTimeSec_ = add;
}

int Scheduler::Schedule(const std::shared_ptr<TopoAdapter> &topo) {
    return 0;
}

int64_t Scheduler::GetRunningInterval() {
    return 0;
}

int Scheduler::GetAddPeerTimeLimitSec() {
    return addTimeSec_;
}

int Scheduler::GetRemovePeerTimeLimitSec() {
    return removeTimeSec_;
}

int Scheduler::GetTransferLeaderTimeLimitSec() {
    return transTimeSec_;
}
}  // namespace schedule
}  // namespace mds
}  // namespace curve


