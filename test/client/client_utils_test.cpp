/*
 *  Copyright (c) 2023 NetEase Inc.
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

#include <gtest/gtest.h>
#include <sys/resource.h>

#include "src/client/utils.h"

namespace curve {
namespace client {

TEST(AdjustOpenFileSoftLimitTest, TestAdjustToZero) {
    ASSERT_TRUE(AdjustOpenFileSoftLimit(0));
}

TEST(AdjustOpenFileSoftLimitTest, TestExceedHardLimit) {
    struct rlimit rlim;
    ASSERT_EQ(0, getrlimit(RLIMIT_NOFILE, &rlim));
    ASSERT_FALSE(AdjustOpenFileSoftLimit(rlim.rlim_max + 1));
}

TEST(AdjustOpenFileSoftLimitTest, TestAdjustToHardLimit) {
    struct rlimit rlim;
    ASSERT_EQ(0, getrlimit(RLIMIT_NOFILE, &rlim));

    rlim.rlim_cur = rlim.rlim_max / 2;
    ASSERT_EQ(0, setrlimit(RLIMIT_NOFILE, &rlim));

    ASSERT_TRUE(AdjustOpenFileSoftLimit(rlim.rlim_max - 1));
    ASSERT_EQ(0, getrlimit(RLIMIT_NOFILE, &rlim));
    ASSERT_EQ(rlim.rlim_cur, rlim.rlim_max);
}

}  // namespace client
}  // namespace curve
