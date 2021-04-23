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

#ifndef SRC_COMMON_THROTTLE_H_
#define SRC_COMMON_THROTTLE_H_

#include <cstdint>
#include <memory>
#include <utility>
#include <vector>

#include "src/common/leaky_bucket.h"

namespace curve {
namespace common {

struct ReadWriteThrottleParams {
    ThrottleParams iopsTotal;
    ThrottleParams iopsRead;
    ThrottleParams iopsWrite;
    ThrottleParams bpsTotal;
    ThrottleParams bpsRead;
    ThrottleParams bpsWrite;
};

class Throttle {
 public:
    enum class Type {
        IOPS_TOTAL,
        IOPS_READ,
        IOPS_WRITE,
        BPS_TOTAL,
        BPS_READ,
        BPS_WRITE,
    };

    struct InternalThrottle {
        Type type;
        bool enabled;
        std::unique_ptr<common::LeakyBucket> leakyBucket;

        InternalThrottle(Type type, bool enabled,
                         common::LeakyBucket* leakyBucket)
            : type(type), enabled(enabled), leakyBucket(leakyBucket) {}
    };

 public:
    Throttle();

    ~Throttle() = default;

    /**
     * @brief Stop all throttles, and let all blocked requests go
     */
    void Stop();

    /**
     * @brief Add tokens, if it's iops throttle add 1 token,
     *        otherwise add the tokens corresponding to the length.
     *        And, If there are not enough tokens,
     *        the call will block until the requirement is met
     * @param isRead is read operations
     * @param length io request's length
     */
    void Add(bool isRead, uint64_t length);

    /**
     * @brief Update throttle params
     * @param params throttle params
     */
    void UpdateThrottleParams(const ReadWriteThrottleParams& params);

    /**
     * @brief Check if corresponding throttle is enabled
     * @param type throttle type
     * @return return true if enabled, else return false
     */
    bool IsThrottleEnabled(Type type) const;

 private:
    void UpdateIfNotEqual(Type type,
                          const curve::common::ThrottleParams& oldParams,
                          const curve::common::ThrottleParams& newParams);

    /**
     * @brief Reset new params to internal throttle according to flag.
     *        if limit is 0, it means disable the throttle.
     */
    void ResetThrottleParams(Type type, uint64_t limit, uint64_t burst,
                             uint64_t burstLength);

    /**
     * @brief calculate tokens required by current request
     * @param isRead is read operation
     * @param length request length
     * @param type throttle type
     * @return If no need throttle, return 0.
     *         If flag is iops throttle, return 1.
     *         If flag is bps throttle, return length.
     */
    uint64_t CalcTokens(bool isRead, uint64_t length, Type type) const;

    bool IsWriteThrottle(Type type) const;
    bool IsReadThrottle(Type type) const;
    bool IsIOPSThrottle(Type type) const;

    // current throttle params
    ReadWriteThrottleParams throttleParams_;

    // all throttles, currently it contains
    // iops-total/iops-read/iops-write/bps-total/bps-read/bps-write throttle
    // std::vector<std::pair<Type, common::LeakyBucketThrottle*>> throttles_;
    std::vector<InternalThrottle> throttles_;
};

}  // namespace common
}  // namespace curve

#endif  // SRC_COMMON_THROTTLE_H_
