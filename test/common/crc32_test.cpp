/*
 * Project: curve
 * Created Date: Saturday December 29th 2018
 * Author: yangyaokai
 * Copyright (c) 2018 netease
 */

#include <gtest/gtest.h>

#include "src/common/crc32.h"

namespace curve {
namespace common {

TEST(Crc32TEST, BasicTest) {
    char buf1[10];
    ::memset(buf1, 0, 10);
    char buf2[10];
    ::memset(buf2, 1, 10);
    char buf3[20];
    ::memset(buf3, 0, 20);
    char buf4[10];
    ::memset(buf4, 0, 10);
    uint32_t crc1 = CRC32(buf1, sizeof(buf1));
    uint32_t crc2 = CRC32(buf2, sizeof(buf2));
    uint32_t crc3 = CRC32(buf3, sizeof(buf3));
    uint32_t crc4 = CRC32(buf4, sizeof(buf4));
    ASSERT_EQ(crc1, crc4);
    ASSERT_NE(crc1, crc2);
    ASSERT_NE(crc1, crc3);
    ASSERT_NE(crc2, crc3);
}

}  // namespace common
}  // namespace curve
