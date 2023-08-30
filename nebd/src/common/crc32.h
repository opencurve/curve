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
 * Created Date: Saturday December 29th 2018
 * Author: yangyaokai
 */

#ifndef NEBD_SRC_COMMON_CRC32_H_
#define NEBD_SRC_COMMON_CRC32_H_

#include <stdint.h>
#include <sys/types.h>

#include <butil/crc32c.h>

namespace nebd {
namespace common {

/**
 *Calculate the CRC32 checksum (CRC32C) of the data and encapsulate it based on the crc32 library of brpc
 * @param pData The data to be calculated
 * @param iLen The length of data to be calculated
 * @return 32-bit data CRC32 checksum
 */
inline uint32_t CRC32(const char *pData, size_t iLen) {
    return butil::crc32c::Value(pData, iLen);
}

/**
 *Calculate the CRC32 checksum (CRC32C) of the data and encapsulate it based on the crc32 library of brpc This function supports inheritance
 *Calculate to support the calculation of a single CRC checksum for SGL type data. Meet the following constraints:
 *CRC32("hello world", 11) == CRC32(CRC32("hello ", 6), "world", 5)
 * @param crc starting crc checksum
 * @param pData The data to be calculated
 * @param iLen The length of data to be calculated
 * @return 32-bit data CRC32 checksum
 */
inline uint32_t CRC32(uint32_t crc, const char *pData, size_t iLen) {
    return butil::crc32c::Extend(crc, pData, iLen);
}

}  // namespace common
}  // namespace nebd

#endif  // NEBD_SRC_COMMON_CRC32_H_
