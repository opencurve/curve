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

#include "curvefs/src/tools/space/curvefs_space_base_tool.h"

namespace curvefs {
namespace tools {
namespace space {

std::string ByteToStringByMagnitude(uint64_t byte) {
    // Convert byte KB to a appropriate magnitude
    // like 1024KB to 1 MB
    std::stringstream ss;
    ss.setf(std::ios::fixed);
    ss.precision(2);  // 2 decimal places
    if (byte >= 1024 * 1024 * 1024) {
        // TB
        ss << double(byte) / double(1024 * 1024 * 1024) << " TB";
    } else if (byte >= 1024 * 1024) {
        // GB
        ss << double(byte) / double(1024 * 1024) << " GB";
    } else if (byte >= 1024) {
        // MB
        ss << double(byte) / double(1024) << " MB";
    } else {
        // KB
        ss << byte << " KB";
    }
    return ss.str();
}
}  // namespace space
}  // namespace tools
}  // namespace curvefs
