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

#ifndef SRC_CLIENT_DISCARD_TASK_H_
#define SRC_CLIENT_DISCARD_TASK_H_

#include <bthread/condition_variable.h>
#include <bthread/mutex.h>
#include <bthread/types.h>
#include <time.h>

#include <atomic>

#include "src/client/client_metric.h"
#include "src/client/metacache.h"
#include "src/client/metacache_struct.h"

namespace curve {
namespace client {

class DiscardTaskManager;


/**
 * DiscardTask corresponding to one segment discard task.
 * It's main function is to send DeAllocateSegment request to MDS
 * and clear cached segment info on success.
 */
class DiscardTask {
 public:
    DiscardTask(DiscardTaskManager* taskManager, SegmentIndex segmentIndex,
                MetaCache* metaCache, MDSClient* mdsClient,
                DiscardMetric* metric)
        : taskManager_(taskManager),
          segmentIndex_(segmentIndex),
          metaCache_(metaCache),
          mdsClient_(mdsClient),
          timerId_(0),
          metric_(metric) {}

    void Run();

    bthread_timer_t Id() const {
        return timerId_;
    }

    void SetId(bthread_timer_t id) {
        timerId_ = id;
    }

 private:
    DiscardTaskManager* taskManager_;
    SegmentIndex segmentIndex_;
    MetaCache* metaCache_;
    MDSClient* mdsClient_;
    bthread_timer_t timerId_;
    DiscardMetric* metric_;

    static std::atomic<uint64_t> taskId_;
};

/**
 * DiscardTaskManager stores all pending DiscardTasks
 */
class DiscardTaskManager {
 public:
    explicit DiscardTaskManager(DiscardMetric* metric);

    void OnTaskFinish(bthread_timer_t timerId);

    bool ScheduleTask(SegmentIndex segmentIndex, MetaCache* metaCache,
                      MDSClient* mdsclient, timespec abstime);

    /**
     * @brief Cancel all unfinished discard tasks
     */
    void Stop();

 private:
    bthread::Mutex mtx_;
    bthread::ConditionVariable cond_;
    std::unordered_map<bthread_timer_t, std::unique_ptr<DiscardTask>> unfinishedTasks_;  // NOLINT

    DiscardMetric* metric_;
};

}  // namespace client
}  // namespace curve

#endif  // SRC_CLIENT_DISCARD_TASK_H_
