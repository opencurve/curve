/*
 * Project: curve
 * Created Date: Wednesday March 20th 2019
 * Author: yangyaokai
 * Copyright (c) 2018 netease
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
    // 启动线程池
    tp_ = std::make_shared<TaskThreadPool>();
    int ret = tp_->Start(options_.threadNum, options_.queueCapacity);
    if (ret < 0) {
        LOG(ERROR) << "clone manager start error."
                   << "threadNum: " << options_.threadNum
                   << ", queueCapacity: " << options_.queueCapacity;
        return -1;
    }
    isRunning_.store(true, std::memory_order_release);
    return 0;
}

int CloneManager::Fini() {
    if (!isRunning_.load(std::memory_order_acquire))
        return 0;

    isRunning_.store(false, std::memory_order_release);
    tp_->Stop();

    return 0;
}

std::shared_ptr<CloneTask> CloneManager::GenerateCloneTask(
    std::shared_ptr<ReadChunkRequest> request,
    ::google::protobuf::Closure *done) {
    // 如果core是空的,任务无法被处理,所以返回空
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
