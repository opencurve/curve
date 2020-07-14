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
 * 计算数据的CRC32校验码(CRC32C)，基于brpc的crc32库进行封装
 * @param pData 待计算的数据
 * @param iLen 待计算的数据长度
 * @return 32位的数据CRC32校验码
 */
inline uint32_t CRC32(const char *pData, size_t iLen) {
    return butil::crc32c::Value(pData, iLen);
}

/**
 * 计算数据的CRC32校验码(CRC32C)，基于brpc的crc32库进行封装. 此函数支持继承式
 * 计算，以支持对SGL类型的数据计算单个CRC校验码。满足如下约束:
 * CRC32("hello world", 11) == CRC32(CRC32("hello ", 6), "world", 5)
 * @param crc 起始的crc校验码
 * @param pData 待计算的数据
 * @param iLen 待计算的数据长度
 * @return 32位的数据CRC32校验码
 */
inline uint32_t CRC32(uint32_t crc, const char *pData, size_t iLen) {
    return butil::crc32c::Extend(crc, pData, iLen);
}

}  // namespace common
}  // namespace nebd

#endif  // NEBD_SRC_COMMON_CRC32_H_
