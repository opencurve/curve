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
 * Created Date: Wednesday March 20th 2019
 * Author: yangyaokai
 */

#include "src/chunkserver/clone_manager.h"

namespace curve {
namespace chunkserver {

CloneManager::CloneManager() : isRunning_(false) {}

CloneManager::~CloneManager() {
    if (isRunning_.load(std::memory_order_acquire))
        Fini();
}

int CloneManager::Init(const CloneOptions& options) {
    options_ = options;
    return 0;
}

int CloneManager::Run() {
    if (isRunning_.load(std::memory_order_acquire))
        return 0;
    // Start Thread Pool
    LOG(INFO) << "Begin to run clone manager.";
    tp_ = std::make_shared<TaskThreadPool<>>();
    int ret = tp_->Start(options_.threadNum, options_.queueCapacity);
    if (ret < 0) {
        LOG(ERROR) << "clone manager start error."
                   << "threadNum: " << options_.threadNum
                   << ", queueCapacity: " << options_.queueCapacity;
        return -1;
    }
    isRunning_.store(true, std::memory_order_release);
    LOG(INFO) << "Start clone manager success.";
    return 0;
}

int CloneManager::Fini() {
    if (!isRunning_.load(std::memory_order_acquire))
        return 0;

    LOG(INFO) << "Begin to stop clone manager.";
    isRunning_.store(false, std::memory_order_release);
    tp_->Stop();
    LOG(INFO) << "Stop clone manager success.";

    return 0;
}

std::shared_ptr<CloneTask> CloneManager::GenerateCloneTask(
    std::shared_ptr<ReadChunkRequest> request,
    ::google::protobuf::Closure *done) {
    // If the core is empty, the task cannot be processed, so it returns empty
    if (options_.core == nullptr)
        return nullptr;

    std::shared_ptr<CloneTask> cloneTask =
        std::make_shared<CloneTask>(request, options_.core, done);
    return cloneTask;
}

bool CloneManager::IssueCloneTask(std::shared_ptr<CloneTask> cloneTask) {
    if (!isRunning_.load(std::memory_order_acquire))
        return false;

    if (cloneTask == nullptr)
        return false;

    tp_->Enqueue(cloneTask->Closure());

    return true;
}

}  // namespace chunkserver
}  // namespace curve
