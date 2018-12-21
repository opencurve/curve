/*
 * Project: curve
 * Created Date: Saturday December 29th 2018
 * Author: yangyaokai
 * Copyright (c) 2018 netease
 */

#ifndef CURVE_COMMON_CRC32_H
#define CURVE_COMMON_CRC32_H

#include <stdint.h>
#include <sys/types.h>

namespace curve {
namespace common {

uint32_t CRC32(const char *pData, size_t iLen);

}  // namespace common
}  // namespace curve

#endif  // CURVE_COMMON_CRC32_H
