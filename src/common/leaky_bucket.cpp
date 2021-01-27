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
 * Created Date: 20210127
 * Author: wuhanqing
 */

#include "src/common/leaky_bucket.h"

#include <glog/logging.h>

#include <algorithm>
#include <mutex>
#include <utility>

#include "src/common/timeutility.h"

namespace curve {
namespace common {

DEFINE_uint32(bucketLeakIntervalMs, 20,
              "leaky bucket leak interval in milliseconds");

std::once_flag LeakyBucket::initTimerThreadOnce_;
bthread::TimerThread LeakyBucket::timer_;

double LeakyBucket::Bucket::Add(double tokens) {
    double available = 0;

    if (burst > 0) {
        // if burst is enabled, available tokens is limit by two conditions
        // 1. bucket-level is limited by bucket capacity which is calculate by [burst * burstLength]  // NOLINT
        // 2. bucket-burst-level is limited by burst limit which is equal to [burst]  // NOLINT
        available = std::max(
            std::min(burst * burstLength - level, burst - burstLevel), 0.0);

        if (available >= tokens) {
            level += tokens;
            burstLevel += tokens;

            return 0;
        } else {
            level += available;
            burstLevel += available;

            return tokens - available;
        }
    } else {
        // if burst is not enable, available token is limit only by bucket
        // capacity which is equal to [avg]
        available = std::max(avg - level, 0.0);

        if (available >= tokens) {
            level += tokens;
            return 0;
        } else {
            level += available;
            return tokens - available;
        }
    }
}

void LeakyBucket::Bucket::Leak(uint64_t intervalUs) {
    double leak = static_cast<double>(avg) * intervalUs /
                  TimeUtility::MicroSecondsPerSecond;
    level = std::max(level - leak, 0.0);

    if (burst > 0) {
        leak = static_cast<double>(burst) * intervalUs /
               TimeUtility::MicroSecondsPerSecond;
        burstLevel = std::max(burstLevel - leak, 0.0);
    }
}

void LeakyBucket::Bucket::Reset(uint64_t avg, uint64_t burst,
                                uint64_t burstLength) {
    this->avg = avg;
    this->burst = burst;
    this->burstLength = burstLength;
}

LeakyBucket::LeakyBucket(const std::string& name)
    : mtx_(),
      name_(name),
      pendings_(),
      timerId_(bthread::TimerThread::INVALID_TASK_ID),
      bucket_(),
      lastLeakUs_(TimeUtility::GetTimeofDayUs()),
      stopMtx_(),
      stopCond_(),
      stopped_(false) {
    std::call_once(initTimerThreadOnce_, &LeakyBucket::InitTimerThread);
    RegisterLeakTask();
}

LeakyBucket::~LeakyBucket() {
    Stop();

    std::deque<PendingRequest> tmp;
    {
        std::lock_guard<bthread::Mutex> lock(mtx_);
        tmp.swap(pendings_);
    }

    for (auto& b : tmp) {
        b.done->Run();
    }
}

void LeakyBucket::Add(uint64_t tokens) {
    ThrottleClosure done;
    if (Add(tokens, &done)) {
        done.Wait();
    }
}

bool LeakyBucket::Add(uint64_t tokens, google::protobuf::Closure* done) {
    std::lock_guard<bthread::Mutex> lock(mtx_);
    if (bucket_.avg == 0) {
        return false;
    }

    bool wait = false;
    if (!pendings_.empty()) {
        wait = true;
        pendings_.emplace_back(tokens, done);
    } else {
        auto left = bucket_.Add(tokens);
        if (left > 0.0) {
            wait = true;
            pendings_.emplace_back(left, done);
        }
    }

    return wait;
}

bool LeakyBucket::SetLimit(uint64_t average, uint64_t burst,
                           uint64_t burstLength) {
    // check param valid
    if (burst > 0) {
        if (average >= burst) {
            LOG(WARNING) << "when burst is enabled, burst should greater than "
                            "average, average = "
                         << average << ", burst = " << burst
                         << ", burst length = " << burstLength;
            return false;
        }

        if (burstLength < 1) {
            LOG(WARNING) << "when burst is enabled, burst length should "
                            "greater than or equal to 1, average = "
                         << average << ", burst = " << burst
                         << ", burst length = " << burstLength;
            return false;
        }
    } else if (burstLength != 0) {
        LOG(WARNING) << "when burst is disabled, burst length show equal "
                        "to 0, average = "
                     << average << ", burst = " << burst
                     << ", burst length = " << burstLength;
        return false;
    }

    std::lock_guard<bthread::Mutex> lock(mtx_);
    bucket_.Reset(average, burst, burstLength);
    lastLeakUs_ = TimeUtility::GetTimeofDayUs();

    return true;
}

void LeakyBucket::Stop() {
    std::unique_lock<bthread::Mutex> lock(stopMtx_);
    if (stopped_ == true) {
        return;
    }

    stopped_ = true;

    // when timer unschedule return 1, it means scheduler timer task is running,
    // cause we have held stopMtx_, so wait for scheduler timer task exit with
    // condtiaon variable
    if (timerId_ != bthread::TimerThread::INVALID_TASK_ID &&
        timer_.unschedule(timerId_) == 1) {
        stopCond_.wait(lock);
    }
}

void LeakyBucket::Leak() {
    std::deque<PendingRequest> tmpPendings;

    {
        std::lock_guard<bthread::Mutex> lock(mtx_);
        uint64_t now = TimeUtility::GetTimeofDayUs();
        uint64_t intervalUs = now - lastLeakUs_;
        lastLeakUs_ = now;

        bucket_.Leak(intervalUs);

        while (!pendings_.empty()) {
            auto& request = pendings_.front();
            // LOG(INFO) << "In leak, add " << request.left;
            double left = bucket_.Add(request.left);
            if (left > 0.0) {
                request.left = left;
                break;
            }

            tmpPendings.push_back(std::move(pendings_.front()));
            pendings_.pop_front();
        }
    }

    for (auto& b : tmpPendings) {
        b.done->Run();
    }
}

void LeakyBucket::RegisterLeakTask() {
    timespec abstime = butil::milliseconds_from_now(FLAGS_bucketLeakIntervalMs);
    timerId_ = timer_.schedule(&LeakyBucket::LeakTask, this, abstime);
}

void LeakyBucket::LeakTask(void* arg) {
    LeakyBucket* throttle = static_cast<LeakyBucket*>(arg);

    std::lock_guard<bthread::Mutex> lock(throttle->stopMtx_);
    if (throttle->stopped_) {
        throttle->stopCond_.notify_one();
        return;
    }

    throttle->Leak();
    throttle->RegisterLeakTask();
}

void LeakyBucket::InitTimerThread() {
    bthread::TimerThreadOptions options;
    options.bvar_prefix = "leaky_bucket_throttle";
    int rc = timer_.start(&options);
    if (rc == 0) {
        LOG(INFO) << "init throttle timer thread success";
    } else {
        LOG(FATAL) << "init throttle timer thread failed, " << berror(rc);
    }
}

}  // namespace common
}  // namespace curve
