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

using curve::common::Mutex;
using curve::common::ConditionVariable;

namespace curve {
namespace client {
class InflightControl {
 public:
    InflightControl() {
        curInflightIONum_.store(0);
    }

    void SetMaxInflightNum(uint64_t maxInflightNum) {
        maxInflightNum_ = maxInflightNum;
    }

    /**
     * 调用该接口等待inflight全部回来，这段期间是hang的
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
     * 调用该接口等待inflight回来，这段期间是hang的
     */
    void WaitInflightComeBack() {
        if (curInflightIONum_.load() >= maxInflightNum_) {
            std::unique_lock<Mutex> lk(inflightComeBackmtx_);
            inflightComeBackcv_.wait(lk, [this]() {
                return curInflightIONum_.load() < maxInflightNum_;
            });
        }
    }

    /**
     * 递增inflight num
     */
    void IncremInflightNum() {
        curInflightIONum_.fetch_add(1, std::memory_order_release);
    }

    /**
     * 递减inflight num
     */
    void DecremInflightNum() {
        std::lock_guard<Mutex> lk(inflightComeBackmtx_);
        {
            std::lock_guard<Mutex> lk(inflightAllComeBackmtx_);
            curInflightIONum_.fetch_sub(1, std::memory_order_release);
            if (curInflightIONum_.load() == 0) {
                inflightAllComeBackcv_.notify_all();
            }
        }
        inflightComeBackcv_.notify_one();
    }

    /**
     * WaitInflightComeBack会检查当前未返回的io数量是否超过我们限制的最大未返回inflight数量
     * 但是真正的inflight数量与上层并发调用的线程数有关。
     * 假设我们设置的maxinflight=100，上层有三个线程在同时调用GetInflightToken，
     * 如果这个时候inflight数量为99，那么并发状况下这3个线程在WaitInflightComeBack
     * 都会通过然后向下并发执行IncremInflightNum，这个时候真正的inflight为102，
     * 下一个下发的时候需要等到inflight数量小于100才能继续，也就是等至少3个IO回来才能继续
     * 下发。这个误差是可以接受的，他与scheduler一侧并发度有关，误差有上限。
     * 如果想要精确控制inflight数量，就需要在接口处加锁，让原本可以并发的逻辑变成了
     * 串行，这样得不偿失。因此我们这里选择容忍一定误差范围。
     */
    void GetInflightToken() {
        WaitInflightComeBack();
        IncremInflightNum();
    }

    void ReleaseInflightToken() {
        DecremInflightNum();
    }

    uint64_t GetCurrentInflightNum() const {
        return curInflightIONum_;
    }

 private:
    uint64_t              maxInflightNum_;
    std::atomic<uint64_t> curInflightIONum_;
    Mutex                 inflightComeBackmtx_;
    ConditionVariable     inflightComeBackcv_;
    Mutex                 inflightAllComeBackmtx_;
    ConditionVariable     inflightAllComeBackcv_;
};

}   //  namespace client
}   //  namespace curve

#endif  // SRC_CLIENT_INFLIGHT_CONTROLLER_H_
