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

#ifndef SRC_COMMON_LEAKY_BUCKET_H_
#define SRC_COMMON_LEAKY_BUCKET_H_

#include <bthread/condition_variable.h>
#include <bthread/mutex.h>
#include <bthread/timer_thread.h>
#include <gflags/gflags.h>
#include <google/protobuf/stubs/callback.h>

#include <cstddef>
#include <cstdint>
#include <deque>
#include <string>

namespace curve {
namespace common {

DECLARE_uint32(bucketLeakIntervalMs);

struct ThrottleParams {
    // maximum number of iops or bps, 0 means no limit
    uint64_t limit;

    // burst is the number of iops/bps that can exceed over limit,
    // 0 means no burst
    uint64_t burst;

    // maximum length in seconds of burst
    // note: the real burst duration will be longer than burstSeconds
    uint64_t burstSeconds;

    ThrottleParams() : ThrottleParams(0, 0, 0) {}

    ThrottleParams(uint64_t limit, uint64_t burst, uint64_t burstSeconds)
        : limit(limit), burst(burst), burstSeconds(burstSeconds) {}
};

class LeakyBucket {
    struct Bucket {
        // average leak rate
        uint64_t avg = 0;

        // burst leak rate
        uint64_t burst = 0;

        // burst leak duration
        uint64_t burstSeconds = 0;

        // the capacity of avgBucket if burst is set
        double capacity = 0;

        // currnet level
        double level = 0;

        // burst level
        double burstLevel = 0;
        // whether level has been initial
        bool levelInitial = true;
        // whether burst level has been initial
        bool burstLevelInitial = true;
        // the times that burst leaky bucket is empty
        uint32_t burstBucketEmptyTimes = 0;
        // the times that leaky bucket is empty
        uint32_t avgBucketEmptyTimes = 0;

        /**
         * @brief initial bucket, prevent the rate exceed
         */
        void BucketInitial(bool permitBurst = false);

        /**
         * @brief Add tokens to current bucket
         * @param tokens number tokens to add
         * @return return 0 if bucket can store so much tokens,
         *         otherwise return the number of tokens that could not be put
         *         into the bucket
         */
        double Add(double tokens);

        /**
         * @brief Leak tokens in bucket
         */
        void Leak();

        /**
         * @brief Reset bucket limit
         */
        void Reset(uint64_t avg, uint64_t burst, uint64_t burstSeconds);
    };

    struct PendingRequest {
        double left;
        google::protobuf::Closure* done;

        PendingRequest(double left, google::protobuf::Closure* done)
            : left(left), done(done) {}
    };

    class ThrottleClosure : public google::protobuf::Closure {
     public:
        ThrottleClosure() : mtx_(), cond_(), pass_(false) {}

        void Run() override {
            std::lock_guard<bthread::Mutex> lock(mtx_);
            pass_ = true;
            cond_.notify_one();
        }

        void Wait() {
            std::unique_lock<bthread::Mutex> lock(mtx_);
            while (!this->pass_) {
                cond_.wait(lock);
            }
        }

     private:
        bthread::Mutex mtx_;
        bthread::ConditionVariable cond_;
        bool pass_;
    };

 public:
    explicit LeakyBucket(const std::string& name = "");

    ~LeakyBucket();

    LeakyBucket(const LeakyBucket&) = delete;
    LeakyBucket& operator=(const LeakyBucket&) = delete;

    /**
     * @brief Add number of tokens to bucket. If the bucket doesn't have enough
     *        space to place so many tokens, this call will block until the
     *        requirement is met
     */
    void Add(uint64_t tokens);

    /**
     * @brief Add number of tokens to bucket. Return false if the bucket doesn't
     *        have enough space to place so many tokens and when the requirement
     *        is met, it will call done->Run()
     */
    bool Add(uint64_t tokens, google::protobuf::Closure* done);

    /**
     * @brief Set the throttle params
     * @return return true if parameters are valid, otherwise return false
     */
    bool SetLimit(uint64_t average, uint64_t burst, uint64_t burstSeconds);

    /**
     * @brief Stop this throttle
     */
    void Stop();

 private:
    /**
     * @brief Do leak task
     */
    void Leak();

    /**
     * @brief Register timed task
     */
    void RegisterLeakTask();

    /**
     * @brief Callback of timed task
     */
    static void LeakTask(void* arg);

    /**
     * @brief Init backend timer thread
     */
    static void InitTimerThread();

 private:
    // mutex to protect this throttle
    bthread::Mutex mtx_;

    // throttle's name
    std::string name_;

    // store all unsatisfied requests
    std::deque<PendingRequest> pendings_;

    // id of timer task
    bthread::TimerThread::TaskId timerId_;

    // leaky bucket
    Bucket bucket_;

    // last leay timestamp in microsecond
    uint64_t lastLeakUs_;

    // the following three fields are used to stop timer task
    bthread::Mutex stopMtx_;
    bthread::ConditionVariable stopCond_;
    bool stopped_;

    static std::once_flag initTimerThreadOnce_;

    // backend timer thread for all throttle
    static bthread::TimerThread timer_;
};

inline bool operator==(const ThrottleParams& lhs, const ThrottleParams& rhs) {
    return lhs.limit == rhs.limit && lhs.burst == rhs.burst &&
           lhs.burstSeconds== rhs.burstSeconds;
}

inline std::ostream& operator<<(std::ostream& os,
                                const ThrottleParams& params) {
    os << "limit = " << params.limit << ", burst = " << params.burst
       << ", burst length = " << params.burstSeconds;

    return os;
}

}  // namespace common
}  // namespace curve

#endif  // SRC_COMMON_LEAKY_BUCKET_H_
