/*
 *  Copyright (c) 2023 NetEase Inc.
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
 * Project: Curve
 * Created Date: 2023-07-25
 * Author: Jingli Chen (Wine93)
 */

#include <string>

#include "curvefs/src/common/metric_utils.h"

#ifndef CURVEFS_SRC_CLIENT_FUSE_METRIC_H_
#define CURVEFS_SRC_CLIENT_FUSE_METRIC_H_

namespace curvefs {
namespace client {
namespace fuse {

#define DEFINE_METRICS(seq) END(A seq)
#define BODY(x) OpMetric x = OpMetric(#x);
#define A(x) BODY(x) B
#define B(x) BODY(x) A
#define A_END
#define B_END
#define END(...) END_(__VA_ARGS__)
#define END_(...) __VA_ARGS__##_END

struct OpMetric {
    static const std::string prefix = "fuse_ll_"

    explicit OpMetric(const std::string& name)
          ninflight(prefix, name + "_ninflight"),
          nerror(prefix, name + "_nerror"),
          latency(prefix, name + "_latency") {}

    bvar::Adder<int64_t> ninflight;
    bvar::Adder<uint64_t> nerror;
    bvar::LatencyRecorder latency;
};

// OpMetric lookup = OpMetric("lookup");
// OpMetric getattr = OpMetric("getattr");
// ...
struct FuseLLOpMetric {
    DEFINE_METRICS(
        (lookup)
        (getattr)
        (setattr)
        (readlink)
        (mknod)
        (mkdir)
        (unlink)
        (rmdir)
        (symlink)
        (rename)
        (link)
        (open)
        (read)
        (write)
        (flush)
        (release)
        (fsync)
        (opendir)
        (readdir)
        (readdirplus)
        (releasedir)
        (statfs)
        (setxattr)
        (getxattr)
        (listxattr)
        (create)
        (bmap)
    )

    FuseLLOpMetric GetInstance() {
        static FuseLLOpMetric instance;
        return instance;
    }
};

struct CodeGuard {
    CodeGuard(bvar::Adder<uint64_t>* nerror, CURVEFS_ERROR* code)
        :  nerror(nerror), code(code) {}

    ~CodeGuard() {
        if (*code != CURVEFS_ERROR::OK) {
            (*nerror) << 1;
        }
    }

    bvar::Adder<uint64_t>* nerror;
    CURVEFS_ERROR* code;
};

struct MetricGuards {
    explicit MetricGuards(OpMetric* metric, CURVEFS_ERROR* code)
        : iGuard(&metric->ninflight),
          cGuard(&metric->nerror, code),
          lGuard(&metric->latency) {}

    ~MetricGuards() = default;

    InflightGuard iGuard;
    CodeGuard cGuard;
    LatencyUpdater lGuard;
};

// NOTE: param rc is implicit
#define MetricGuard(OP) MetricGuards(&FuseLLOpMetric::GetInstance().OP, &rc);

}  // namespace fuse
}  // namespace client
}  // namespace curvefs

#endif  // CURVEFS_SRC_CLIENT_FUSE_METRIC_H_
