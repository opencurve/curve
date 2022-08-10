/*
 *  Copyright (c) 2022 NetEase Inc.
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
 * @Date: 2022-09-13 14:55:30
 * @Author: chenwei
 */

#include <gtest/gtest.h>

#include "curvefs/src/client/common/config.h"

namespace curvefs {
namespace client {
namespace common {
TEST(TestAddULLString, test1) {
    std::string first = "1";
    ASSERT_TRUE(AddUllStringToFirst(&first, 1, true));
    ASSERT_EQ(first, "2");

    ASSERT_TRUE(AddUllStringToFirst(&first, 1, true));
    ASSERT_EQ(first, "3");

    ASSERT_TRUE(AddUllStringToFirst(&first, 1, false));
    ASSERT_EQ(first, "2");

    ASSERT_TRUE(AddUllStringToFirst(&first, 1, false));
    ASSERT_EQ(first, "1");

    ASSERT_FALSE(AddUllStringToFirst(&first, 2, false));
}

TEST(TestAddULLString, test2) {
    uint64_t first = 1;
    ASSERT_TRUE(AddUllStringToFirst(&first, "1"));
    ASSERT_EQ(first, 2);

    ASSERT_TRUE(AddUllStringToFirst(&first, "1"));
    ASSERT_EQ(first, 3);
}
}  // namespace common
}  // namespace client
}  // namespace curvefs
