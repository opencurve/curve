/*
 *  Copyright (c) 2023 NetEase Inc.
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

#include "curvefs/src/client/fsused_updater.h"

namespace curvefs {
namespace client {

void FsUsedUpdater::UpdateDeltaBytes(int64_t deltaBytes) {
    deltaBytes_.fetch_add(deltaBytes);
}

void FsUsedUpdater::UpdateFsUsed() {
    using curvefs::metaserver::FsUsedDelta;
    FsUsedDelta delta;
    delta.set_fsid(fsId_);
    uint64_t deltaBytes = deltaBytes_.exchange(0);
    if (deltaBytes == 0) return;
    delta.set_bytes(deltaBytes);
    metaserverClient_->UpdateFsUsed(fsId_, delta, true);
}

int64_t FsUsedUpdater::GetDeltaBytes() { return deltaBytes_.load(); }

bool UpdateFsUsedTask::OnTriggeringTask(timespec* next_abstime) {
    fsUsedUpdater_->UpdateFsUsed();
    *next_abstime = butil::seconds_from_now(interval_s_);
    return true;
}

void UpdateFsUsedTask::OnDestroyingTask() {}

void StartUpdateFsUsedTask(FsUsedUpdater* updater, int64_t interval_s) {
    brpc::PeriodicTaskManager::StartTaskAt(
        new UpdateFsUsedTask(updater, interval_s),
        butil::seconds_from_now(interval_s));
}
}  // namespace client
}  // namespace curvefs
