/*
 *  Copyright (c) 2021 NetEase Inc.
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
 * @Project: curve
 * @Date: 2021-10-22
 * @Author: chengyi01
 */

#include "curvefs/src/tools/space/curvefs_space_base_tool.h"

#include <gmock/gmock.h>
#include <gtest/gtest.h>

namespace curvefs {
namespace tools {
namespace space {

class BaseToolTest : public testing::Test {
 protected:
    void SetUp() override {}
    void TearDown() override {}
};

TEST_F(BaseToolTest, KB_test) {
    ASSERT_EQ(ByteToStringByMagnitude(521), "521 KB");
}

TEST_F(BaseToolTest, MB_test) {
    ASSERT_EQ(ByteToStringByMagnitude(2 * 1024 + 1), "2.00 MB");
}

TEST_F(BaseToolTest, GB_test) {
    ASSERT_EQ(ByteToStringByMagnitude(3 * 1024 * 1024 + 1023 * 1024),
              "4.00 GB");
}

TEST_F(BaseToolTest, TB_test) {
    uint64_t byte =
        (uint64_t)100 * (uint64_t)1024 * (uint64_t)1024 * (uint64_t)1024;
    ASSERT_EQ(ByteToStringByMagnitude(byte), "100.00 TB");
}

}  // namespace space
}  // namespace tools
}  // namespace curvefs
