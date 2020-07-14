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
 * File Created: 2020-02-07
 * Author: charisu
 */

#include <gtest/gtest.h>
#include "src/tools/common.h"

TEST(ToolMDSClientHelperTest, Trim) {
    std::string str = "test";
    curve::tool::TrimMetricString(&str);
    ASSERT_EQ("test", str);
    str = " test";
    curve::tool::TrimMetricString(&str);
    ASSERT_EQ("test", str);
    str = " test\r\n";
    curve::tool::TrimMetricString(&str);
    ASSERT_EQ("test", str);
    str = " \"test\"\r\n";
    curve::tool::TrimMetricString(&str);
    ASSERT_EQ("test", str);
}
