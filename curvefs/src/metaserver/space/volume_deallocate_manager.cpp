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
 * Date: Tue Apr 25 15:46:58 CST 2023
 * Author: lixiaocui
 */

#include <utility>
#include "curvefs/src/metaserver/space/volume_deallocate_manager.h"

namespace curvefs {
namespace metaserver {
void VolumeDeallocateManager::Register(InodeVolumeSpaceDeallocate task) {
    {
        std::lock_guard<std::mutex> lk(workerCtx_.mtx);
        task.Init(executeOpt_);
        workerCtx_.allTasks.emplace_back(std::move(task));
    }

    workerCtx_.cond.notify_one();
}

void VolumeDeallocateManager::Cancel(uint32_t partitionId) {
    std::lock_guard<std::mutex> lk(workerCtx_.mtx);

    // cancle from tasking
    auto it = workerCtx_.tasking.find(partitionId);
    if (it != workerCtx_.tasking.end()) {
        it->second->Cancel(partitionId);
        LOG(INFO) << "VolumeDeallocateManager cancel task, partitionId="
                  << partitionId;
        return;
    }

    // cancel from waiting
    workerCtx_.allTasks.remove_if([partitionId](
                                      const InodeVolumeSpaceDeallocate &task) {
        bool ret = (task.GetPartitionID() == partitionId);
        if (ret) {
            LOG(INFO)
                << "VolumeDeallocateManager cancel waiting task, partitionId="
                << partitionId;
        }
        return ret;
    });
}

void VolumeDeallocateManager::Deallocate(uint32_t partitioId,
                                         uint64_t blockGroupOffset) {
    std::lock_guard<std::mutex> lk(workerCtx_.mtx);

    // set from waiting
    {
        auto it =
            std::find_if(workerCtx_.allTasks.begin(), workerCtx_.allTasks.end(),
                         [partitioId](const InodeVolumeSpaceDeallocate &task) {
                             return task.GetPartitionID() == partitioId;
                         });
        if (it != workerCtx_.allTasks.end()) {
            it->SetDeallocateTask(blockGroupOffset);
            return;
        }
    }

    // set from tasking
    {
        auto it = workerCtx_.tasking.find(partitioId);
        if (it != workerCtx_.tasking.end()) {
            it->second->SetDeallocate(blockGroupOffset);
            return;
        }
    }

    LOG(ERROR) << "VolumeDeallocateManager do not find deallocate handler for "
                  "partitionId="
               << partitioId;
}

bool VolumeDeallocateManager::HasDeallocate() {
    std::lock_guard<std::mutex> lk(workerCtx_.mtx);

    // view from waiting
    for (auto &task : workerCtx_.allTasks) {
        if (task.HasDeallocateTask()) {
            return true;
        }
    }

    // view from tasking
    for (auto &task : workerCtx_.tasking) {
        if (task.second->HasDeallocate()) {
            return true;
        }
    }

    return false;
}

void VolumeDeallocateManager::Run() {
    if (!workerOpt_.enable) {
        LOG(INFO) << "VolumeDeallocateManager not enable";
        return;
    }

    if (!workerCtx_.running.exchange(true)) {
        for (uint16_t i = 0; i < workerOpt_.workerNum; i++) {
            workers_.emplace_back(
                absl::make_unique<VolumeDeallocateWorker>(&workerCtx_));
            workers_.back()->Run();
        }
        LOG(INFO) << "VolumeDeallocateManager start all workers ok";
    } else {
        LOG(INFO) << "VolumeDeallocateManager already run";
    }
}

void VolumeDeallocateManager::Stop() {
    if (!workerCtx_.running.exchange(false)) {
        LOG(INFO) << "VolumeDeallocateManager has no worker running, skip";
        return;
    }

    workerCtx_.cond.notify_all();
    for (auto &worker : workers_) {
        worker->Stop();
    }
    LOG(INFO) << "VolumeDeallocateManager stop all workers ok";
}

}  // namespace metaserver
}  // namespace curvefs
