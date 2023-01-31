/*
 *  Copyright (c) 2023 NetEase Inc.
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
 * Created Date: 2023-02-15
 * Author: chengyi01
 */

#include "curvefs/src/common/task_thread_pool.h"

#include <gtest/gtest.h>
#include <bthread/condition_variable.h>
#include <glog/logging.h>

#include <cstdint>
#include <functional>
#include <memory>
#include <utility>

#include "src/common/concurrent/concurrent.h"
#include "src/common/concurrent/rw_lock.h"
#include "absl/memory/memory.h"

namespace curvefs {
namespace common {

using curve::common::BthreadRWLock;
using curve::common::ReadLockGuard;
using curve::common::Thread;
using curve::common::WriteLockGuard;
using ThreadPool = TaskThreadPool2<bthread::Mutex, bthread::ConditionVariable>;

const uint64_t TIME = 1000 * 1000;
const uint64_t THREAD = 10;

class SumTask {
 public:
    SumTask(uint64_t start, uint64_t end, uint64_t sum = 0)
        : start_(start), end_(end), sum_(sum) {}
    uint64_t start_ = 0;
    uint64_t end_ = 0;
    uint64_t sum_ = 0;
};

class SumManager {
 public:
    void AddSumTask(uint64_t key, std::function<void()> task) {
        std::unique_ptr<ThreadPool> tp = absl::make_unique<ThreadPool>();
        tp->Start(THREAD);
        sumThreadPoolMutex_.WRLock();
        auto iter = sumThreadPool_.emplace(key, std::move(tp));
        sumThreadPoolMutex_.Unlock();
        if (!iter.first->second->Enqueue(task)) {
            LOG(ERROR) << "add task " << key << " fail!";
        }
    }

    void AddSum(uint64_t key, uint64_t start, uint64_t end) {
        WriteLockGuard lock(sumMutex_);
        auto iter = sum_.emplace(key, SumTask(start, end));
        iter.first->second.sum_ += start;

        auto leftTask = [key, start, end, this]() {
            uint64_t left = Left(start);
            if (left <= end) {
                WriteLockGuard lock(sumMutex_);
                sum_.emplace(key, SumTask(start, end));
                auto task = [this, key, left, end]() {
                    AddSum(key, left, end);
                };
                AddSumTask(key, task);
            }
        };
        AddSumTask(key, leftTask);

        auto rightTask = [key, start, end, this]() {
            uint64_t right = Right(start);
            if (right <= end) {
                WriteLockGuard lock(sumMutex_);
                sum_.emplace(key, SumTask(start, end));
                auto task = [this, key, right, end]() {
                    AddSum(key, right, end);
                };
                AddSumTask(key, task);
            }
        };
        AddSumTask(key, rightTask);

        if (iter.second) {
            LOG(INFO) << "add task:" << key << " start:" << start
                      << " success!";
        }
    }

    void Init() {
        bgFetchStop_.store(false, std::memory_order_release);
        bgFetchThread_ = Thread(&SumManager::backGroundFetch, this);
        initbgFetchThread_ = true;
    }
    void Unit() {
        bgFetchStop_.store(true, std::memory_order_release);
        if (initbgFetchThread_) {
            bgFetchThread_.join();
        }

        {
            for (auto &iter : sumThreadPool_) {
                LOG(INFO) << "Stop task:" << iter.first;
                iter.second->Stop();
            }
            WriteLockGuard lock(sumThreadPoolMutex_);
            sumThreadPool_.clear();
        }

        {
            WriteLockGuard lock(sumMutex_);
            for (auto iter : sum_) {
                uint64_t result = Sum(iter.second.start_, iter.second.end_);
                LOG(INFO) << "Sum Done:" << iter.first
                          << " sum:" << iter.second.sum_
                          << " result:" << result;
                ASSERT_EQ(iter.second.sum_, result);
            }
        }
    }

    void WaitAllDone() {
        bool working = true;
        while (working) {
            working = false;
            usleep(TIME);
            ReadLockGuard lock(sumMutex_);
            for (auto const &iter : sum_) {
                if (!SumDone(iter.first)) {
                    working = true;
                }
            }
        }
        LOG(INFO) << "all done";
    }

 private:
    bool SumDone(uint64_t key) {
        ReadLockGuard lock(sumThreadPoolMutex_);
        return sumThreadPool_.find(key) == sumThreadPool_.end();
    }

    uint64_t Sum(uint64_t start, uint64_t end) {
        return (start + end) * (end - start + 1) / 2;
    }
    void backGroundFetch() {
        while (!bgFetchStop_.load(std::memory_order_acquire)) {
            usleep(TIME);
            {
                sumThreadPoolMutex_.RDLock();
                for (auto iter = sumThreadPool_.begin();
                     iter != sumThreadPool_.end();) {
                    if (iter->second->QueueSize() == 0) {
                        LOG(INFO) << "Stop task:" << iter->first;
                        sumThreadPoolMutex_.Unlock();
                        iter->second->Stop();
                        sumThreadPoolMutex_.WRLock();
                        iter = sumThreadPool_.erase(iter);
                        sumThreadPoolMutex_.Unlock();
                        sumThreadPoolMutex_.RDLock();
                    } else {
                        ++iter;
                    }
                }
                sumThreadPoolMutex_.Unlock();
            }
            {
                WriteLockGuard lock(sumMutex_);
                for (auto iter = sum_.begin(); iter != sum_.end();) {
                    if (SumDone(iter->first)) {
                        uint64_t result =
                            Sum(iter->second.start_, iter->second.end_);
                        LOG(INFO) << "rm Sum Done:" << iter->first
                                  << " sum:" << iter->second.sum_
                                  << " result:" << result;
                        ASSERT_EQ(iter->second.sum_, result);
                        iter = sum_.erase(iter);
                    } else {
                        ++iter;
                    }
                }
            }
        }
    }


    static uint64_t Left(uint64_t n) { return 2 * n; }

    static uint64_t Right(uint64_t n) { return 2 * n + 1; }

 protected:
    std::map<uint64_t, std::unique_ptr<ThreadPool>> sumThreadPool_;
    BthreadRWLock sumThreadPoolMutex_;

    std::map<uint64_t, SumTask> sum_;
    BthreadRWLock sumMutex_;

    Thread bgFetchThread_;
    std::atomic<bool> bgFetchStop_;
    bool initbgFetchThread_;
};

class TaskThreadPool2Test : public testing::Test {
 protected:
    void SetUp() override {
        sumManager_ = std::make_shared<SumManager>();
        sumManager_->Init();
    }
    void TearDown() override { sumManager_->Unit(); }

    std::shared_ptr<SumManager> sumManager_;
};

TEST_F(TaskThreadPool2Test, test) {
    sumManager_->AddSum(1, 1, 10);
    sumManager_->AddSum(2, 1, 100);
    sumManager_->AddSum(3, 1, 1000);
    sumManager_->AddSum(4, 1, 10000);
    sumManager_->AddSum(5, 1, 100000);
    sumManager_->WaitAllDone();
}

TEST_F(TaskThreadPool2Test, test2) {
    sumManager_->AddSum(4, 1, 10000);
    sumManager_->WaitAllDone();
}

}  // namespace common
}  // namespace curvefs
