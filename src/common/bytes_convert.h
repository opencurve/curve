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
 * Project: curve
 * Created Date: 2023-10-06
 * Author: yyyyufeng
 */

#ifndef SRC_COMMON_BYTES_CONVERT_H_
#define SRC_COMMON_BYTES_CONVERT_H_

#include <algorithm>
#include <string>

namespace curve {
namespace common {

constexpr uint64_t kKiB = 1024ULL;
constexpr uint64_t kMiB = 1024ULL * kKiB;
constexpr uint64_t kGiB = 1024ULL * kMiB;
constexpr uint64_t kTiB = 1024ULL * kGiB;

/**
 *  @brief convert string to bytes
 *  @return true if success
 */
inline bool ToNumbericByte(const std::string& source, uint64_t* target) {
    int len = source.size();
    if (source[len - 1] >= '0' && source[len - 1] <= '9') {
        *target = std::stoul(source);
        return true;
    }
    if (len < 3 || (source[len - 1] != 'b' && source[len - 1] != 'B')) {
        return false;
    }
    *target = 0;
    for (int i = 0; i < len - 2; i++) {
        char ch = source[i];
        if (ch < '0' || ch > '9') {
            return false;
        }
        *target *= 10;
        *target += (ch - '0');
    }
    bool ret = true;
    switch (source[len - 2]) {
        case 'k':
        case 'K':
            *target *= kKiB;
            break;
        case 'M':
        case 'm':
            *target *= kMiB;
            break;
        case 'G':
        case 'g':
            *target *= kGiB;
            break;
        case 'T':
        case 't':
            *target *= kTiB;
            break;
        default:
            ret = false;
            break;
    }
    return ret;
}

}  // namespace common
}  // namespace curve
#endif  // SRC_COMMON_BYTES_CONVERT_H_
