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
 * Created Date: 2021-10-22
 * Author: chengyi01
 */

#include "curvefs/src/tools/usage/curvefs_space_base_tool.h"

namespace curvefs {
namespace tools {
namespace usage {

constexpr uint64_t kKiB = 1024ULL;
constexpr uint64_t kMiB = 1024ULL * kKiB;
constexpr uint64_t kGiB = 1024ULL * kMiB;
constexpr uint64_t kTiB = 1024ULL * kGiB;

std::string ToReadableByte(uint64_t byte) {
    // Convert byte KB to a appropriate magnitude
    // like 1024KB to 1 MB
    std::stringstream ss;
    ss.setf(std::ios::fixed);
    ss.precision(2);  // 2 decimal places
    if (byte >= 1 * kTiB) {
        // TB
        ss << double(byte) / double(kTiB) << " TiB";
    } else if (byte >= 1 * kGiB) {
        // GB
        ss << double(byte) / double(kGiB) << " GiB";
    } else if (byte >= 1 * kMiB) {
        // MB
        ss << double(byte) / double(kMiB) << " MiB";
    } else if (byte >= 1 * kKiB) {
        // KB
        ss << double(byte) / double(kKiB) << " KiB";
    } else {
        ss << double(byte)  << " Byte";
    }
    return ss.str();
}
}  // namespace usage
}  // namespace tools
}  // namespace curvefs
