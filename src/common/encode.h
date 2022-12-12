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
 * Created Date: Sunday September 9th 2018
 * Author: hzsunjianliang
 */
#ifndef SRC_COMMON_ENCODE_H_
#define SRC_COMMON_ENCODE_H_

#include <stdint.h>
#include <cstdint>

namespace curve {
namespace common {

// NOTE: value passed to this function will convert to `uint64_t'
static inline void EncodeBigEndian(char* buf, uint64_t value) {
    buf[0] = (value >> 56) & 0xff;
    buf[1] = (value >> 48) & 0xff;
    buf[2] = (value >> 40) & 0xff;
    buf[3] = (value >> 32) & 0xff;
    buf[4] = (value >> 24) & 0xff;
    buf[5] = (value >> 16) & 0xff;
    buf[6] = (value >> 8) & 0xff;
    buf[7] = value & 0xff;
}

inline uint64_t DecodeBigEndian(const char* buf) {
    return (uint64_t(buf[0]) << 56) | (uint64_t(buf[1]) << 48) |
           (uint64_t(buf[2]) << 40) | (uint64_t(buf[3]) << 32) |
           (uint64_t(buf[4]) << 24) | (uint64_t(buf[5]) << 16) |
           (uint64_t(buf[6]) << 8) | uint64_t(buf[7]);
}

inline void EncodeBigEndian_uint32(char* buf, uint32_t value) {
    buf[0] = (value >> 24) & 0xff;
    buf[1] = (value >> 16) & 0xff;
    buf[2] = (value >> 8) & 0xff;
    buf[3] = value & 0xff;
}

}  // namespace common
}  // namespace curve

#endif  // SRC_COMMON_ENCODE_H_
