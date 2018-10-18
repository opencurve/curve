/*
 * Project: curve
 * Created Date: Sunday September 9th 2018
 * Author: hzsunjianliang
 * Copyright (c) 2018 netease
 */
#ifndef SRC_COMMON_ENCODE_H_
#define SRC_COMMON_ENCODE_H_

#include <stdint.h>
#include <glog/logging.h>

namespace curve {
namespace common {
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
}  // namespace common
}  // namespace curve

#endif  // SRC_COMMON_ENCODE_H_
