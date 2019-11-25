/*
 * Project: curve
 * Created Date: Mon Sep 16 2019
 * Author: xuchaojie
 * Copyright (c) 2019 netease
 */

#include "src/snapshotcloneserver/common/task_tracker.h"

#include <memory>

#include "src/snapshotcloneserver/common/task.h"

namespace curve {
namespace snapshotcloneserver {

void TaskTracker::AddTask(TrackerTask* task) {
    std::unique_lock<Mutex> lk(cv_m);
    task->SetTracker(shared_from_this());
    concurrent_++;
}

void TaskTracker::HandleResponse(int retCode) {
    {
        std::unique_lock<Mutex> lk(cv_m);
        if (retCode < 0) {
            lastErr_ = retCode;
        }
        concurrent_--;
    }
    cv_.notify_all();
}

void TaskTracker::Wait() {
    std::unique_lock<Mutex> lk(cv_m);
    cv_.wait(lk, [this](){
        return concurrent_ == 0;});
}

void TaskTracker::WaitSome(uint32_t num) {
    // 记录当前数量
    uint32_t max = concurrent_;
    std::unique_lock<Mutex> lk(cv_m);
    cv_.wait(lk, [this, &max, &num](){
        return (concurrent_ == 0) ||
            (max - concurrent_ >= num);});
}

}  // namespace snapshotcloneserver
}  // namespace curve
