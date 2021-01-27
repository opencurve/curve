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
 * Created Date: Sun Sep  6 17:13:58 CST 2020
 */

#ifndef SRC_COMMON_MATH_UTIL_H_
#define SRC_COMMON_MATH_UTIL_H_

#include <cmath>
#include <cstdint>
#include <algorithm>
#include <numeric>

namespace curve {
namespace common {

inline uint64_t MaxPowerTimesLessEqualValue(uint64_t value) {
    uint64_t pow = 0;
    while (value > 1) {
        value >>= 1;
        pow++;
    }
    return pow;
}

/**
 * @brief clamp value between low and high
 * @return if value compares less than low, return low;
 *         otherwise, if value compares greater then high, return high;
 *         otherwise, return value.
 */
template <typename T, typename Compare>
inline const T& Clamp(const T& value, const T& low, const T& high,
                      Compare comp) {
    return comp(value, low) ? low : (comp(high, value) ? high : value);
}

/**
 * @brief clamp value between low and high
 * @return if value compares less than low, return low;
 *         otherwise, if value compares greater then high, return high;
 *         otherwise, return value.
 */
template <typename T>
inline const T& Clamp(const T& value, const T& low, const T& high) {
    return Clamp(value, low, high,
                 [](const T& a, const T& b) { return a < b ? true : false; });
}

}  // namespace common
}  // namespace curve

#endif  // SRC_COMMON_MATH_UTIL_H_
