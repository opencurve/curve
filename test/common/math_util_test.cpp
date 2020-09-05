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
 * Created Date: Sun Sep  6 17:13:58 CST 2020
 */

#include "src/common/math_util.h"

#include <gtest/gtest.h>

namespace curve {
namespace common {

TEST(MathUtilTest, CommonTest) {
    ASSERT_EQ(0, MaxPowerTimesLessEqualValue(0));
    ASSERT_EQ(0, MaxPowerTimesLessEqualValue(1));
    ASSERT_EQ(2, MaxPowerTimesLessEqualValue(4));
    ASSERT_EQ(1, MaxPowerTimesLessEqualValue(2));
    ASSERT_EQ(1, MaxPowerTimesLessEqualValue(3));
    ASSERT_EQ(2, MaxPowerTimesLessEqualValue(7));
    ASSERT_EQ(3, MaxPowerTimesLessEqualValue(10));
    ASSERT_EQ(3, MaxPowerTimesLessEqualValue(15));
    ASSERT_EQ(5, MaxPowerTimesLessEqualValue(32));
    ASSERT_EQ(5, MaxPowerTimesLessEqualValue(63));
    ASSERT_EQ(6, MaxPowerTimesLessEqualValue(64));
    ASSERT_EQ(7, MaxPowerTimesLessEqualValue(255));
    ASSERT_EQ(8, MaxPowerTimesLessEqualValue(256));
    ASSERT_EQ(8, MaxPowerTimesLessEqualValue(257));
    ASSERT_EQ(10, MaxPowerTimesLessEqualValue(1024));
    ASSERT_EQ(10, MaxPowerTimesLessEqualValue(2047));
    ASSERT_EQ(11, MaxPowerTimesLessEqualValue(2048));
    ASSERT_EQ(11, MaxPowerTimesLessEqualValue(2049));
}

}  // namespace common
}  // namespace curve
