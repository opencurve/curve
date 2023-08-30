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

using curve::common::Mutex;
using curve::common::ConditionVariable;

class InflightControl {
 public:
    InflightControl() = default;

    void SetMaxInflightNum(uint64_t maxInflightNum) {
        maxInflightNum_ = maxInflightNum;
    }

    /**
     * @brief calls the interface to wait for all inflight returns, which is a period of hang,
     *Called when closing a file
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
     * @brief calls the interface to wait for inflight to return, which is during the hang period
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
     *WaitInflightComeBack will check if the current number of unreturned io exceeds our maximum number of unreturned inflights
     *But the true number of inflight is related to the number of threads that are concurrently called in the upper layer.
     *Assuming we set maxinflight=100, there are three threads in the upper layer calling GetInflightToken simultaneously,
     *If the number of inflights at this time is 99, then under concurrent conditions, these three threads are waiting for WaitInflightComeBack
     *Will execute IncremInflightNum in parallel downwards, and the actual inflight at this time is 102,
     *When the next distribution is made, it is necessary to wait until the number of inflights is less than 100 before continuing, which means waiting for at least 3 IOs to return before continuing
     *Distribute. This error is acceptable as it is related to the concurrency on the scheduler side, and there is an upper limit to the error.
     *If you want to accurately control the number of inflights, you need to add locks at the interface, so that the logic that could have been concurrent becomes
     *Serialization is not worth the loss. Therefore, we choose to tolerate a certain range of errors here.
     */
    void GetInflightToken() {
        WaitInflightComeBack();
        IncremInflightNum();
    }

    void ReleaseInflightToken() {
        DecremInflightNum();
    }

    /**
     * @brief Get current inflight io num, only use in test code
     */
    uint64_t GetCurrentInflightNum() const {
        return curInflightIONum_.load(std::memory_order_acquire);
    }

 private:
    uint64_t              maxInflightNum_ = 0;
    std::atomic<uint64_t> curInflightIONum_{0};

    Mutex                 inflightComeBackmtx_;
    ConditionVariable     inflightComeBackcv_;
    Mutex                 inflightAllComeBackmtx_;
    ConditionVariable     inflightAllComeBackcv_;
};

}   //  namespace client
}   //  namespace curve

#endif  // SRC_CLIENT_INFLIGHT_CONTROLLER_H_
