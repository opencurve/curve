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
 * Created Date: Mon Jul  5 19:53:34 CST 2021
 * Author: wuhanqing
 */

#include "src/common/fast_align.h"

#include <gtest/gtest.h>

namespace curve {
namespace common {

TEST(FastAlignTest, TestAlignUp) {
    ASSERT_EQ(0, align_up(0, 512));
    ASSERT_EQ(512, align_up(1, 512));
    ASSERT_EQ(512, align_up(511, 512));
    ASSERT_EQ(1024, align_up(513, 512));
}

TEST(FastAlignTest, TestAlignDown) {
    ASSERT_EQ(0, align_down(0, 512));
    ASSERT_EQ(0, align_down(1, 512));
    ASSERT_EQ(0, align_down(511, 512));
    ASSERT_EQ(512, align_down(512, 512));
    ASSERT_EQ(512, align_down(513, 512));
}

TEST(FastAlignTest, TestIsAligned) {
    ASSERT_TRUE(is_aligned(512, 512));
    ASSERT_TRUE(is_aligned(4096, 512));

    ASSERT_FALSE(is_aligned(511, 512));
    ASSERT_FALSE(is_aligned(4095, 4096));
}

}  // namespace common
}  // namespace curve
