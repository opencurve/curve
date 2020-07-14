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

#include <gtest/gtest.h>

#include "nebd/src/common/crc32.h"

namespace nebd {
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

TEST(Crc32TEST, StandardResults) {
  // From rfc3720 section B.4.
  char buf[32];

  memset(buf, 0, sizeof(buf));
  ASSERT_EQ(0x8a9136aaU, CRC32(buf, sizeof(buf)));

  memset(buf, 0xff, sizeof(buf));
  ASSERT_EQ(0x62a8ab43U, CRC32(buf, sizeof(buf)));

  for (int i = 0; i < 32; i++) {
    buf[i] = i;
  }
  ASSERT_EQ(0x46dd794eU, CRC32(buf, sizeof(buf)));

  for (int i = 0; i < 32; i++) {
    buf[i] = 31 - i;
  }
  ASSERT_EQ(0x113fdb5cU, CRC32(buf, sizeof(buf)));

  unsigned char data[48] = {
    0x01, 0xc0, 0x00, 0x00,
    0x00, 0x00, 0x00, 0x00,
    0x00, 0x00, 0x00, 0x00,
    0x00, 0x00, 0x00, 0x00,
    0x14, 0x00, 0x00, 0x00,
    0x00, 0x00, 0x04, 0x00,
    0x00, 0x00, 0x00, 0x14,
    0x00, 0x00, 0x00, 0x18,
    0x28, 0x00, 0x00, 0x00,
    0x00, 0x00, 0x00, 0x00,
    0x02, 0x00, 0x00, 0x00,
    0x00, 0x00, 0x00, 0x00,
  };
  ASSERT_EQ(0xd9963a56, CRC32(reinterpret_cast<char*>(data), sizeof(data)));
}

TEST(Crc32TEST, Values) {
  ASSERT_NE(CRC32("a", 1), CRC32("foo", 3));
}

TEST(Crc32TEST, Extend) {
  ASSERT_EQ(CRC32("hello world", 11),
            CRC32(CRC32("hello ", 6), "world", 5));
}

}  // namespace common
}  // namespace nebd
