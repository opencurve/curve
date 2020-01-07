/*
 * Project: curve
 * Created Date: Mon Sep 16 2019
 * Author: xuchaojie
 * Copyright (c) 2019 netease
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

void RecoverChunkTaskTracker::PushResultContext(RecoverChunkContextPtr ctx) {
    std::unique_lock<Mutex> lk(ctxMutex_);
    contexts_.push_back(ctx);
}

std::list<RecoverChunkContextPtr>
    RecoverChunkTaskTracker::PopResultContexts() {
    std::unique_lock<Mutex> lk(ctxMutex_);
    std::list<RecoverChunkContextPtr> ret;
    ret.swap(contexts_);
    return ret;
}



}  // namespace snapshotcloneserver
}  // namespace curve
