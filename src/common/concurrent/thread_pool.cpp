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
 * Created Date: 18-9-26
 * Author: wudemiao
 */

#include "src/common/concurrent/thread_pool.h"

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
            threads_.emplace_back(std::make_unique<std::thread>(threadFunc_));
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
