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

#include "curvefs/src/metaserver/copyset/apply_queue.h"

#include <glog/logging.h>

#include "absl/memory/memory.h"
#include "absl/strings/str_cat.h"
#include "curvefs/src/common/threading.h"
#include "curvefs/src/metaserver/copyset/copyset_node.h"

namespace curvefs {
namespace metaserver {
namespace copyset {

using ::curvefs::common::SetThreadName;

void ApplyQueue::StartWorkers() {
    for (uint32_t i = 0; i < option_.workerCount; ++i) {
        std::string name = [this, i]() -> std::string {
            if (option_.copysetNode == nullptr) {
                return "apply";
            }

            return absl::StrCat("apply", ":", option_.copysetNode->GetPoolId(),
                                "_", option_.copysetNode->GetCopysetId(), ":",
                                i);
        }();
        auto taskThread =
            absl::make_unique<TaskWorker>(option_.queueDepth, std::move(name));
        taskThread->Start();
        workers_.emplace_back(std::move(taskThread));
    }
}

bool ApplyQueue::Start(const ApplyQueueOption& option) {
    if (running_) {
        return true;
    }

    if (option.queueDepth < 1 || option.workerCount < 1) {
        LOG(ERROR) << "ApplyQueue start failed, invalid argument, queue depth: "
                   << option.queueDepth
                   << ", worker count: " << option.workerCount;
        return false;
    }

    option_ = option;

    StartWorkers();
    running_.store(true);
    return true;
}

void ApplyQueue::Flush() {
    if (!running_.load(std::memory_order_relaxed)) {
        return;
    }

    curve::common::CountDownEvent event(option_.workerCount);

    auto flush = [&event]() { event.Signal(); };

    for (auto& worker : workers_) {
        worker->tasks.Push(flush);
    }

    event.Wait();
}

void ApplyQueue::Stop() {
    if (!running_.exchange(false)) {
        return;
    }

    LOG(INFO) << "Going to stop apply queue";

    for (auto& worker : workers_) {
        worker->Stop();
    }

    workers_.clear();
    LOG(INFO) << "Apply queue stopped";
}

void ApplyQueue::TaskWorker::Start() {
    if (running.exchange(true)) {
        return;
    }

    worker = std::thread(&TaskWorker::Work, this);
}

void ApplyQueue::TaskWorker::Stop() {
    if (!running.exchange(false)) {
        return;
    }

    auto wakeup = []() {};
    tasks.Push(wakeup);

    worker.join();
}

void ApplyQueue::TaskWorker::Work() {
    SetThreadName(workerName_.c_str());

    while (running.load(std::memory_order_relaxed)) {
        tasks.Pop()();
    }
}

}  // namespace copyset
}  // namespace metaserver
}  // namespace curvefs
