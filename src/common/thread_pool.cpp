/*
 * Project: curve
 * Created Date: 18-9-26
 * Author: wudemiao
 * Copyright (c) 2018 netease
 */

#include "src/common/thread_pool.h"

namespace curve {
namespace common {

ThreadPool::ThreadPool()
    : numThreads_(-1),
      starting_(false) {
}

ThreadPool::~ThreadPool() {
    if (starting_.load()) {
        Stop();
    }
}

int ThreadPool::Init(int numThreads, std::function<void()> threadFunc) {
    if (0 >= numThreads) {
        return -1;
    }
    numThreads_ = numThreads;
    threadFunc_ = threadFunc;
    return 0;
}

void ThreadPool::Start() {
    if (!starting_.exchange(true, std::memory_order_acq_rel)) {
        threads_.reserve(numThreads_);
        for (int i = 0; i < numThreads_; ++i) {
            threads_.emplace_back(new std::thread(threadFunc_));
        }
    }
}

void ThreadPool::Stop() {
    if (starting_.exchange(false, std::memory_order_acq_rel)) {
        for (auto &thr : threads_) {
            thr->join();
        }
    }
}
int ThreadPool::NumOfThreads() {
    return numThreads_;
}

}  // namespace common
}  // namespace curve
