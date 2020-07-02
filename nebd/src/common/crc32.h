/*
 * Project: curve
 * Created Date: Saturday December 29th 2018
 * Author: yangyaokai
 * Copyright (c) 2018 netease
 */

#ifndef SRC_COMMON_CRC32_H_
#define SRC_COMMON_CRC32_H_

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

#endif  // SRC_COMMON_CRC32_H_
