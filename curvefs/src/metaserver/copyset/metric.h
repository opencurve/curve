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

#ifndef CURVEFS_SRC_METASERVER_COPYSET_METRIC_H_
#define CURVEFS_SRC_METASERVER_COPYSET_METRIC_H_

#include <bvar/bvar.h>

#include <array>
#include <memory>
#include <string>

#include "curvefs/src/metaserver/common/types.h"
#include "curvefs/src/metaserver/copyset/operator_type.h"
#include "curvefs/src/metaserver/copyset/utils.h"
#include "include/curve_compiler_specific.h"
#include "src/common/timeutility.h"

namespace curvefs {
namespace metaserver {
namespace copyset {

// Metric for each copyset to statictic operators apply latency/qps/eps/...
class OperatorApplyMetric {
 public:
    OperatorApplyMetric(PoolId poolId, CopysetId copysetId);

    void OnOperatorComplete(OperatorType type, uint64_t latencyUs,
                            bool success = true);

    void OnOperatorCompleteFromLog(OperatorType type, uint64_t latencyUs,
                            bool success = true);

    OperatorApplyMetric(const OperatorApplyMetric&) = delete;
    OperatorApplyMetric& operator=(const OperatorApplyMetric&) = delete;

 private:
    struct OpMetric {
        explicit OpMetric(const std::string& prefix)
            : latRecorder(prefix, "_latency"),
              errorCount(prefix, "_total_error"),
              eps(prefix, "_eps", &errorCount, 1) {}

        // latency recorder support latency/qps/count
        bvar::LatencyRecorder latRecorder;

        // total errored operators
        bvar::Adder<uint64_t> errorCount;

        // error per second
        bvar::PerSecond<bvar::Adder<uint64_t>> eps;
    };

 private:
    static constexpr uint32_t kTotalOperatorNum =
        static_cast<uint32_t>(OperatorType::OperatorTypeMax);

    std::array<std::unique_ptr<OpMetric>, kTotalOperatorNum> opMetrics_;
    std::array<std::unique_ptr<OpMetric>, kTotalOperatorNum> opMetricsFromLog_;
};

// Metric for statictic raft snapshot latency/error count/...
class RaftSnapshotMetric {
 public:
    static RaftSnapshotMetric& GetInstance() {
        static RaftSnapshotMetric instance;
        return instance;
    }

    RaftSnapshotMetric(const RaftSnapshotMetric&) = delete;
    RaftSnapshotMetric& operator=(const RaftSnapshotMetric&) = delete;

    struct MetricContext {
        bool success;

     private:
        uint64_t startUs;

        MetricContext()
            : success(false),
              startUs(curve::common::TimeUtility::GetTimeofDayUs()) {}

        ~MetricContext() = default;

        friend class RaftSnapshotMetric;
    };

    MetricContext* OnSnapshotSaveStart() {
        flying_ << 1;
        return new MetricContext();
    }

    void OnSnapshotSaveDone(MetricContext* ctx) {
        if (ctx->success) {
            latRecorder_ << (curve::common::TimeUtility::GetTimeofDayUs() -
                             ctx->startUs);
        } else {
            errorCount_ << 1;
        }

        flying_ << -1;
        delete ctx;
    }

 private:
    RaftSnapshotMetric()
        : latRecorder_("copyset_snapshot_latency"),
          errorCount_("copyset_snapshot_error_count"),
          flying_("copyset_snapshot_flying_count") {}

 private:
    bvar::LatencyRecorder latRecorder_;
    bvar::Adder<uint64_t> errorCount_;
    bvar::Adder<int64_t> flying_;
};

}  // namespace copyset
}  // namespace metaserver
}  // namespace curvefs

#endif  // CURVEFS_SRC_METASERVER_COPYSET_METRIC_H_
