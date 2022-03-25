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

#include "src/common/throttle.h"

#include <glog/logging.h>

#include <ostream>
#include <string>
#include <utility>

namespace curve {
namespace common {

const char* ThrottleTypeToName(Throttle::Type type) {
    switch (type) {
        case Throttle::Type::IOPS_TOTAL:
            return "IOPS_TOTAL";
        case Throttle::Type::IOPS_READ:
            return "IOPS_READ";
        case Throttle::Type::IOPS_WRITE:
            return "IOPS_WRITE";
        case Throttle::Type::BPS_TOTAL:
            return "BPS_TOTAL";
        case Throttle::Type::BPS_READ:
            return "BPS_READ";
        case Throttle::Type::BPS_WRITE:
            return "BPS_WRITE";
        default:
            return "Unknown";
    }
}

const std::vector<Throttle::Type> kDefaultEnabledThrottleTypes = {
    Throttle::Type::IOPS_TOTAL, Throttle::Type::IOPS_READ,
    Throttle::Type::IOPS_WRITE, Throttle::Type::BPS_TOTAL,
    Throttle::Type::BPS_READ,   Throttle::Type::BPS_WRITE};

Throttle::Throttle() : throttleParams_(), throttles_() {
    for (auto type : kDefaultEnabledThrottleTypes) {
        throttles_.emplace_back(
            type, false,
            new common::LeakyBucket(ThrottleTypeToName(type)));
    }
}

void Throttle::Stop() { throttles_.clear(); }

void Throttle::Add(bool isReadOp, uint64_t length) {
    for (auto& throttle : throttles_) {
        if (!throttle.enabled) {
            continue;
        }

        auto tokens = CalcTokens(isReadOp, length, throttle.type);
        if (tokens > 0) {
            throttle.leakyBucket->Add(tokens);
        }
    }
}

void Throttle::ResetThrottleParams(Type type, uint64_t limit, uint64_t burst,
                                   uint64_t burstLength) {
    for (auto& throttle : throttles_) {
        if (throttle.type != type) {
            continue;
        }

        throttle.enabled = (limit != 0);
        throttle.leakyBucket->SetLimit(limit, burst, burstLength);
    }
}

void Throttle::UpdateThrottleParams(const ReadWriteThrottleParams& params) {
    UpdateIfNotEqual(Type::IOPS_TOTAL, throttleParams_.iopsTotal,
                     params.iopsTotal);
    UpdateIfNotEqual(Type::IOPS_READ, throttleParams_.iopsRead,
                     params.iopsRead);
    UpdateIfNotEqual(Type::IOPS_WRITE, throttleParams_.iopsWrite,
                     params.iopsWrite);
    UpdateIfNotEqual(Type::BPS_TOTAL, throttleParams_.bpsTotal,
                     params.bpsTotal);
    UpdateIfNotEqual(Type::BPS_READ, throttleParams_.bpsRead, params.bpsRead);
    UpdateIfNotEqual(Type::BPS_WRITE, throttleParams_.bpsWrite,
                     params.bpsWrite);

    throttleParams_ = params;
}

bool Throttle::IsThrottleEnabled(Type type) const {
    for (auto& throttle : throttles_) {
        if (type != throttle.type) {
            continue;
        }

        return throttle.enabled;
    }

    return false;
}

void Throttle::UpdateIfNotEqual(
    Type type, const curve::common::ThrottleParams& oldParams,
    const curve::common::ThrottleParams& newParams) {
    if (oldParams == newParams) {
        return;
    }

    LOG(INFO) << "Update " << ThrottleTypeToName(type) << " to " << newParams;
    ResetThrottleParams(type, newParams.limit, newParams.burst,
                        newParams.burstSeconds);
}

inline bool Throttle::IsWriteThrottle(Type type) const {
    return type == Type::IOPS_WRITE || type == Type::BPS_WRITE;
}

inline bool Throttle::IsReadThrottle(Type type) const {
    return type == Type::IOPS_READ || type == Type::BPS_READ;
}

inline bool Throttle::IsIOPSThrottle(Type type) const {
    return type == Type::IOPS_TOTAL || type == Type::IOPS_READ ||
           type == Type::IOPS_WRITE;
}

uint64_t Throttle::CalcTokens(bool isRead, uint64_t length, Type type) const {
    if (isRead && IsWriteThrottle(type)) {
        return 0;
    } else if (!isRead && IsReadThrottle(type)) {
        return 0;
    }

    return IsIOPSThrottle(type) ? 1 : length;
}

}  // namespace common
}  // namespace curve
