/*
 * Project: curve
 * Created Date: 18-8-31
 * Author: wudemiao
 * Copyright (c) 2018 netease
 */

#ifndef CURVE_COMMON_CRC32C_H
#define CURVE_COMMON_CRC32C_H

#include <cstdint>

namespace curve {
namespace common {

uint32_t CurveCrc32c(uint32_t crc_init, unsigned char const *buffer, unsigned int len);

}  // namespace common
}  // namespace curve

#endif  // CURVE_COMMON_CRC32C_H
