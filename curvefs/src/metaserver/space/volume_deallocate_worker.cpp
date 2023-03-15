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

/*
 * Project: curve
 * Date: Tue Apr 25 16:52:55 CST 2023
 * Author: lixiaocui
 */

#include "absl/cleanup/cleanup.h"

#include "curvefs/src/common/threading.h"
#include "curvefs/src/metaserver/space/volume_deallocate_worker.h"

namespace curvefs {
namespace metaserver {
void VolumeDeallocateWorker::Run() {
    calWorkingThread_ =
        std::thread(&VolumeDeallocateWorker::DeallocatetWorker, this);
}

void VolumeDeallocateWorker::Stop() {
    sleeper_.interrupt();

    if (calWorkingThread_.joinable()) {
        calWorkingThread_.join();
    }

    LOG(INFO) << "VolumeDeallocateWorker stopped";
}

void VolumeDeallocateWorker::Cancel(uint32_t partitionId) {
    assert(current_->GetPartitionID() == partitionId);
    current_->SetCanceled();
    sleeper_.interrupt();
}

void VolumeDeallocateWorker::SetDeallocate(uint64_t blockGroupOffset) {
    current_->SetDeallocateTask(blockGroupOffset);
}

bool VolumeDeallocateWorker::HasDeallocate() {
    return current_->HasDeallocateTask();
}

bool VolumeDeallocateWorker::WaitDeallocate() {
    // wait for task arrive
    std::unique_lock<std::mutex> lock(ctx_->mtx);
    if (ctx_->allTasks.empty()) {
        ctx_->cond.wait(
            lock, [this] { return !ctx_->allTasks.empty() || !ctx_->running; });
    }

    // cancel by manager
    if (!ctx_->running) {
        return false;
    }

    // wait ok
    current_ = std::move(ctx_->allTasks.front());
    ctx_->allTasks.pop_front();
    ctx_->tasking.emplace(current_->GetPartitionID(), this);

    VLOG(1) << "VolumeDeallocateWorker get task, partitionId = "
            << current_->GetPartitionID();

    sleeper_.init();
    return true;
}

void VolumeDeallocateWorker::Cleanup() {
    std::lock_guard<std::mutex> lock(ctx_->mtx);

    ctx_->tasking.erase(current_->GetPartitionID());

    if (current_->IsCanceled()) {
        LOG(INFO) << "VolumeDeallocateWorker task canceled, partitionId="
                  << current_->GetPartitionID();
        return;
    }

    ctx_->allTasks.emplace_back(std::move(current_.value()));
    ctx_->cond.notify_one();

    current_.reset();
}

void VolumeDeallocateWorker::DeallocatetWorker() {
    common::SetThreadName("CalVolumeDeallocateWorker");
    while (ctx_->running) {
        if (!WaitDeallocate()) {
            break;
        }

        CHECK(current_.has_value());
        VLOG(1) << "VolumeDeallocateWorker Going to statistic deallocatable "
                   "space for block group";

        auto cleanup = absl::MakeCleanup([&, this] { Cleanup(); });

        if (!current_->CanStart()) {
            if (current_->HasDeallocateTask()) {
                current_->ResetDeallocateTask();
            }
            break;
        }

        if (current_->HasDeallocateTask()) {
            current_->DeallocateOneBlockGroup(current_->GetDeallocateTask());
            VLOG(1) << "VolumeDeallocateWorker deallocate for "
                       "partitionId="
                    << current_->GetPartitionID() << " ok";
            current_->ResetDeallocateTask();
        } else {
            current_->CalDeallocatableSpace();
            VLOG(1) << "VolumeDeallocateWorker cal deallocatable space for "
                       "partitionId="
                    << current_->GetPartitionID() << " ok";
        }
    }
}

}  // namespace metaserver
}  // namespace curvefs
