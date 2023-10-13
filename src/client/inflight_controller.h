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
 * File Created: Wednesday, 17th July 2019 12:53:17 pm
 * Author: tongguangxun
 */

#ifndef SRC_CLIENT_INFLIGHT_CONTROLLER_H_
#define SRC_CLIENT_INFLIGHT_CONTROLLER_H_

#include "src/common/concurrent/concurrent.h"

namespace curve {
namespace client {

using curve::common::ConditionVariable;
using curve::common::Mutex;

class InflightControl {
 public:
    InflightControl() = default;

    void SetMaxInflightNum(uint64_t maxInflightNum) {
        maxInflightNum_ = maxInflightNum;
    }

    /**
     * @brief calls the interface to wait for all inflight returns, which is a
     * period of hang, Called when closing a file
     */
    void WaitInflightAllComeBack() {
        LOG(INFO) << "wait inflight to complete, count = " << curInflightIONum_;
        std::unique_lock<std::mutex> lk(inflightAllComeBackmtx_);
        inflightAllComeBackcv_.wait(lk, [this]() {
            return curInflightIONum_.load(std::memory_order_acquire) == 0;
        });
        LOG(INFO) << "inflight ALL come back.";
    }

    /**
     * @brief calls the interface to wait for inflight to return, which is
     * during the hang period
     */
    void WaitInflightComeBack() {
        if (curInflightIONum_.load(std::memory_order_acquire) >=
            maxInflightNum_) {
            std::unique_lock<Mutex> lk(inflightComeBackmtx_);
            inflightComeBackcv_.wait(lk, [this]() {
                return curInflightIONum_.load(std::memory_order_acquire) <
                       maxInflightNum_;
            });
        }
    }

    /**
     * @brief increment inflight num
     */
    void IncremInflightNum() {
        curInflightIONum_.fetch_add(1, std::memory_order_release);
    }

    /**
     * @brief decreasing inflight num
     */
    void DecremInflightNum() {
        std::lock_guard<Mutex> lk(inflightComeBackmtx_);
        {
            std::lock_guard<Mutex> lk(inflightAllComeBackmtx_);
            const auto cnt =
                curInflightIONum_.fetch_sub(1, std::memory_order_acq_rel);
            if (cnt == 1) {
                inflightAllComeBackcv_.notify_all();
            }
        }
        inflightComeBackcv_.notify_one();
    }

    /**
     * WaitInflightComeBack checks if the current number of pending IOs exceeds
     * our maximum allowed inflight limit. However, the actual inflight count is
     * influenced by concurrent calls from upper-layer threads. Suppose we set
     * maxinflight to 100, and there are three upper-layer threads
     * simultaneously calling GetInflightToken. If, at this moment, the inflight
     * count is 99, then in a concurrent scenario, all three threads in
     * WaitInflightComeBack will pass and proceed to concurrently execute
     * IncremInflightNum. Consequently, the actual inflight count becomes 102.
     * The next dispatch operation will need to wait until the inflight count is
     * less than 100 to proceed, which means it needs at least 3 IOs to return
     * before proceeding. This margin of error is acceptable and is related to
     * the concurrency level on the scheduler side, with a defined upper limit.
     * If precise control over the inflight count is required, it would
     * necessitate adding locks at the interface level, converting originally
     * concurrent logic into serial, which would not be a cost-effective
     * solution. Therefore, we choose to tolerate a certain margin of error in
     * this scenario.
     */
    void GetInflightToken() {
        WaitInflightComeBack();
        IncremInflightNum();
    }

    void ReleaseInflightToken() { DecremInflightNum(); }

    /**
     * @brief Get current inflight io num, only use in test code
     */
    uint64_t GetCurrentInflightNum() const {
        return curInflightIONum_.load(std::memory_order_acquire);
    }

 private:
    uint64_t maxInflightNum_ = 0;
    std::atomic<uint64_t> curInflightIONum_{0};

    Mutex inflightComeBackmtx_;
    ConditionVariable inflightComeBackcv_;
    Mutex inflightAllComeBackmtx_;
    ConditionVariable inflightAllComeBackcv_;
};

}  //  namespace client
}  //  namespace curve

#endif  // SRC_CLIENT_INFLIGHT_CONTROLLER_H_
