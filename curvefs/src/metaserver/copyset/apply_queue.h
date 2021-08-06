/*
 *  Copyright (c) 2021 NetEase Inc.
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
 * Date: Thu Sep  2 14:49:04 CST 2021
 * Author: wuhanqing
 */

#ifndef CURVEFS_SRC_METASERVER_COPYSET_APPLY_QUEUE_H_
#define CURVEFS_SRC_METASERVER_COPYSET_APPLY_QUEUE_H_

#include <atomic>
#include <cstdint>
#include <memory>
#include <thread>
#include <utility>
#include <vector>

#include "include/curve_compiler_specific.h"
#include "src/common/concurrent/count_down_event.h"
#include "src/common/concurrent/task_queue.h"

namespace curvefs {
namespace metaserver {
namespace copyset {

struct ApplyQueueOption {
    uint32_t workerCount = 1;
    uint32_t queueDepth = 1;
};

class CURVE_CACHELINE_ALIGNMENT ApplyQueue {
 public:
    ApplyQueue() : option_(), running_(false), workers_() {}

    bool Start(const ApplyQueueOption& option);

    template <typename Func, typename... Args>
    void Push(uint64_t hash, Func&& f, Args&&... args) {
        workers_[hash % option_.workerCount]->tasks.Push(
            std::forward<Func>(f), std::forward<Args>(args)...);
    }

    void Flush();

    void Stop();

 private:
    void StartWorkers();

    struct TaskWorker {
        explicit TaskWorker(size_t cap)
            : running(false), worker(), tasks(cap) {}

        void Start();

        void Stop();

        void Work();

        std::atomic<bool> running;
        std::thread worker;
        curve::common::TaskQueue tasks;
    };

 private:
    ApplyQueueOption option_;
    std::atomic<bool> running_;
    std::vector<std::unique_ptr<TaskWorker>> workers_;
};

}  // namespace copyset
}  // namespace metaserver
}  // namespace curvefs

#endif  // CURVEFS_SRC_METASERVER_COPYSET_APPLY_QUEUE_H_
