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
 * Created Date: 19-06-18
 * Author: wudemiao
 */

#include <atomic>
#include <cstdint>

#ifndef SRC_CHUNKSERVER_INFLIGHT_THROTTLE_H_
#define SRC_CHUNKSERVER_INFLIGHT_THROTTLE_H_

namespace curve {
namespace chunkserver {

/**
 * Responsible for controlling the maximum number of inflight requests
 */
class InflightThrottle {
 public:
    explicit InflightThrottle(uint64_t maxInflight)
        : inflightRequestCount_(0), kMaxInflightRequest_(maxInflight) {}
    virtual ~InflightThrottle() = default;

    /**
     * @brief: Determine if there is an overload
     * @return true, overload, false No overload
     */
    inline bool IsOverLoad() {
        if (kMaxInflightRequest_ >=
            inflightRequestCount_.load(std::memory_order_relaxed)) {
            return false;
        } else {
            return true;
        }
    }

    /**
     * @brief: inflight request count plus 1
     */
    inline void Increment() {
        inflightRequestCount_.fetch_add(1, std::memory_order_relaxed);
    }

    /**
     * @brief: inflight request count minus 1
     */
    inline void Decrement() {
        inflightRequestCount_.fetch_sub(1, std::memory_order_relaxed);
    }

 private:
    // Current number of inflight requests
    std::atomic<uint64_t> inflightRequestCount_;
    // Maximum number of inflight requests
    const uint64_t kMaxInflightRequest_;
};

}  // namespace chunkserver
}  // namespace curve

#endif  // SRC_CHUNKSERVER_INFLIGHT_THROTTLE_H_
