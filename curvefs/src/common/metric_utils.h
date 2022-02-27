/*
 *  Copyright (c) 2022 NetEase Inc.
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
 * Date: Tuesday Apr 05 15:42:46 CST 2022
 * Author: wuhanqing
 */

#ifndef CURVEFS_SRC_COMMON_METRIC_UTILS_H_
#define CURVEFS_SRC_COMMON_METRIC_UTILS_H_

#include <butil/time.h>
#include <bvar/bvar.h>

namespace curvefs {
namespace common {

struct LatencyUpdater {
    explicit LatencyUpdater(bvar::LatencyRecorder* recorder)
        : recorder(recorder) {
        timer.start();
    }

    ~LatencyUpdater() {
        timer.stop();
        (*recorder) << timer.u_elapsed();
    }

    bvar::LatencyRecorder* recorder;
    butil::Timer timer;
};

}  // namespace common
}  // namespace curvefs

#endif  // CURVEFS_SRC_COMMON_METRIC_UTILS_H_
