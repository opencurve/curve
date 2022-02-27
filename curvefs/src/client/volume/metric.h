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
 * Date: Monday Mar 21 16:05:24 CST 2022
 * Author: wuhanqing
 */

#ifndef CURVEFS_SRC_CLIENT_VOLUME_METRIC_H_
#define CURVEFS_SRC_CLIENT_VOLUME_METRIC_H_

#include <bvar/bvar.h>

#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"

namespace curvefs {
namespace client {

struct VolumeStorageMetric {
    bvar::LatencyRecorder readLatency;
    bvar::LatencyRecorder writeLatency;
    bvar::LatencyRecorder flushLatency;

    explicit VolumeStorageMetric(absl::string_view prefix)
        : readLatency(absl::StrCat(prefix, "_read")),
          writeLatency(absl::StrCat(prefix, "_write")),
          flushLatency(absl::StrCat(prefix, "_flush")) {}
};

}  // namespace client
}  // namespace curvefs

#endif  // CURVEFS_SRC_CLIENT_VOLUME_METRIC_H_
