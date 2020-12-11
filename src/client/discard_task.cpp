
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
 * File Created: Thu Dec 17 11:05:38 CST 2020
 * Author: wuhanqing
 */

#include "src/client/discard_task.h"

#include <bthread/unstable.h>

#include <memory>
#include <unordered_set>
#include <utility>

namespace curve {
namespace client {

void DiscardTask::Run() {
    const FInfo* fileInfo = metaCache_->GetFileInfo();
    uint64_t offset =
        static_cast<uint64_t>(segmentIndex_) * fileInfo->segmentsize;
    FileSegment* fileSegment = metaCache_->GetFileSegment(segmentIndex_);

    fileSegment->AcquireWriteLock();
    metric_->pending << -1;

    if (!fileSegment->IsAllBitSet()) {
        LOG(WARNING) << "DiscardTask find bitmap was cleared, cancel task, "
                        "filename = "
                     << fileInfo->fullPathName << ", offset = " << offset
                     << ", taskid = " << timerId_;
        metric_->totalCanceled << 1;
        fileSegment->ReleaseLock();
        taskManager_->OnTaskFinish(timerId_);
        return;
    }

    LIBCURVE_ERROR errCode = mdsClient_->DeAllocateSegment(fileInfo, offset);
    if (errCode == LIBCURVE_ERROR::OK) {
        metric_->totalSuccess << 1;
        fileSegment->ClearBitmap();
        metaCache_->CleanChunksInSegment(segmentIndex_);
        LOG(INFO) << "DiscardTask success, filename = "
                  << fileInfo->fullPathName << ", offset = " << offset
                  << ", taskid = " << timerId_;
    } else if (errCode == LIBCURVE_ERROR::UNDER_SNAPSHOT) {
        metric_->totalError << 1;
        LOG(WARNING) << "DiscardTask failed, " << fileInfo->fullPathName
                     << " has snapshot, offset = " << offset
                     << ", taskid = " << timerId_;
    } else {
        metric_->totalError << 1;
        LOG(ERROR) << "DiscardTask failed, mds return error = " << errCode
                   << ", filename = " << fileInfo->fullPathName
                   << ", offset = " << offset << ", taskid = " << timerId_;
    }

    fileSegment->ReleaseLock();
    taskManager_->OnTaskFinish(timerId_);
}

DiscardTaskManager::DiscardTaskManager(DiscardMetric* metric)
    : mtx_(), cond_(), unfinishedTasks_(), metric_(metric) {}

static void RunDiscardTask(void* arg) {
    DiscardTask* task = static_cast<DiscardTask*>(arg);
    task->Run();
}

void DiscardTaskManager::OnTaskFinish(bthread_timer_t timerId) {
    std::lock_guard<bthread::Mutex> lk(mtx_);
    unfinishedTasks_.erase(timerId);
    cond_.notify_one();
}

bool DiscardTaskManager::ScheduleTask(SegmentIndex segmentIndex,
                                      MetaCache* metaCache,
                                      MDSClient* mdsclient, timespec abstime) {
    bthread_timer_t timerId;
    std::unique_ptr<DiscardTask> task(
        new DiscardTask(this, segmentIndex, metaCache, mdsclient, metric_));

    int ret = bthread_timer_add(&timerId, abstime, RunDiscardTask, task.get());
    if (ret == 0) {
        task->SetId(timerId);
        LOG(INFO) << "Schedule discard task success, taskid = " << task->Id();
        std::lock_guard<bthread::Mutex> lk(mtx_);
        unfinishedTasks_.emplace(timerId, std::move(task));
        metric_->pending << 1;
        return true;
    }

    LOG(ERROR) << "bthread_timer_add failed, ret = " << ret
               << ", errno = " << errno << ", run task directly";
    return false;
}

void DiscardTaskManager::Stop() {
    std::unordered_set<bthread_timer_t> currentTasks;

    {
        std::lock_guard<bthread::Mutex> lk(mtx_);
        for (auto& kv : unfinishedTasks_) {
            currentTasks.emplace(kv.first);
        }
    }

    for (const auto& timerId : currentTasks) {
        int ret = bthread_timer_del(timerId);

        if (ret == 0) {
            LOG(INFO) << "Cancenl discard task success, taskid = " << timerId;
        } else if (ret == 1) {
            LOG(WARNING) << "Task is running, taskid = " << timerId;
            continue;
        } else if (ret == EINVAL) {
            LOG(WARNING)
                << "Task has been completed or taskid is invalid, taskid = "
                << timerId;
        }

        OnTaskFinish(timerId);
    }

    std::unique_lock<bthread::Mutex> lk(mtx_);
    while (!unfinishedTasks_.empty()) {
        cond_.wait(lk);
    }
}

}  // namespace client
}  // namespace curve
