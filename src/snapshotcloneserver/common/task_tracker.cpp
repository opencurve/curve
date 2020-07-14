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
 * Created Date: Mon Sep 16 2019
 * Author: xuchaojie
 */

#include "src/snapshotcloneserver/common/task_tracker.h"

#include <memory>

#include "src/snapshotcloneserver/common/task.h"
#include "src/snapshotcloneserver/clone/clone_task.h"

namespace curve {
namespace snapshotcloneserver {

void TaskTracker::AddOneTrace() {
    concurrent_.fetch_add(1, std::memory_order_acq_rel);
}

void TaskTracker::HandleResponse(int retCode) {
    if (retCode < 0) {
        lastErr_ = retCode;
    }
    concurrent_.fetch_sub(1, std::memory_order_acq_rel);
    std::unique_lock<Mutex> lk(cv_m);
    cv_.notify_all();
}

void TaskTracker::Wait() {
    std::unique_lock<Mutex> lk(cv_m);
    cv_.wait(lk, [this](){
        return concurrent_.load(std::memory_order_acquire) == 0;});
}

void TaskTracker::WaitSome(uint32_t num) {
    // 记录当前数量
    uint32_t max = concurrent_.load(std::memory_order_acquire);
    std::unique_lock<Mutex> lk(cv_m);
    cv_.wait(lk, [this, &max, &num](){
        return (concurrent_.load(std::memory_order_acquire) == 0) ||
            (max - concurrent_.load(std::memory_order_acquire) >= num);});
}

}  // namespace snapshotcloneserver
}  // namespace curve
