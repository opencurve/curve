/*
 *  Copyright (c) 2021 NetEase Inc.
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
 * Date: Fri Sep  3 17:30:00 CST 2021
 * Author: wuhanqing
 */

#include "curvefs/src/metaserver/copyset/metric.h"

#include "absl/memory/memory.h"

namespace curvefs {
namespace metaserver {
namespace copyset {

OperatorMetric::OperatorMetric(PoolId poolId, CopysetId copysetId) {
    std::string prefix = "op_apply_pool_" + std::to_string(poolId) +
                         "_copyset_" + std::to_string(copysetId);
    std::string fromLogPrefix = "op_apply_from_log_pool_" +
        std::to_string(poolId) + "_copyset_" + std::to_string(copysetId);

    for (uint32_t i = 0; i < kTotalOperatorNum; ++i) {
        opMetrics_[i] = absl::make_unique<OpMetric>(
            prefix + OperatorTypeName(static_cast<OperatorType>(i)));
        opMetricsFromLog_[i] = absl::make_unique<OpMetric>(
            fromLogPrefix + OperatorTypeName(static_cast<OperatorType>(i)));
    }
}

void OperatorMetric::OnOperatorComplete(OperatorType type,
                                             uint64_t latencyUs, bool success) {
    auto index = static_cast<uint32_t>(type);
    if (index < kTotalOperatorNum) {
        if (success) {
            opMetrics_[index]->latRecorder << latencyUs;
        } else {
            opMetrics_[index]->errorCount << 1;
        }
    }
}

void OperatorMetric::OnOperatorCompleteFromLog(OperatorType type,
                                             uint64_t latencyUs, bool success) {
    auto index = static_cast<uint32_t>(type);
    if (index < kTotalOperatorNum) {
        if (success) {
            opMetricsFromLog_[index]->latRecorder << latencyUs;
        } else {
            opMetricsFromLog_[index]->errorCount << 1;
        }
    }
}

void OperatorMetric::WaitInQueueLatency(OperatorType type,
                                             uint64_t latencyUs) {
    auto index = static_cast<uint32_t>(type);
    if (index < kTotalOperatorNum) {
        opMetrics_[index]->waitInQueueLatency << latencyUs;
    }
}

void OperatorMetric::ExecuteLatency(OperatorType type,
                                             uint64_t latencyUs) {
    auto index = static_cast<uint32_t>(type);
    if (index < kTotalOperatorNum) {
        opMetrics_[index]->executeLatency << latencyUs;
    }
}

void OperatorMetric::NewArrival(OperatorType type) {
    auto index = static_cast<uint32_t>(type);
    if (index < kTotalOperatorNum) {
        opMetrics_[index]->rcount << 1;
    }
}

}  // namespace copyset
}  // namespace metaserver
}  // namespace curvefs
