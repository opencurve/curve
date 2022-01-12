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

#include "curvefs/src/tools/usage/curvefs_space_base_tool.h"

#include <gmock/gmock.h>
#include <gtest/gtest.h>

namespace curvefs {
namespace tools {
namespace usage {

class BaseToolTest : public testing::Test {};

TEST(BaseToolTest, KB_test) {
    ASSERT_EQ(ToReadableByte(521), "521 KB");
}

TEST(BaseToolTest, MB_test) {
    ASSERT_EQ(ToReadableByte(2 * 1024 + 1), "2.00 MB");
}

TEST(BaseToolTest, GB_test) {
    ASSERT_EQ(ToReadableByte(3 * 1024 * 1024 + 1023 * 1024), "4.00 GB");
}

TEST(BaseToolTest, TB_test) {
    uint64_t byte = 100ull * 1024 * 1024 * 1024;
    ASSERT_EQ(ToReadableByte(byte), "100.00 TB");
}

}  // namespace usage
}  // namespace tools
}  // namespace curvefs
