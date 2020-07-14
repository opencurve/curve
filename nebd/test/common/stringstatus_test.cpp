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
 * Project: nebd
 * Created Date: 20190819
 * Author: lixiaocui
 */

#include <gtest/gtest.h>
#include "nebd/src/common/stringstatus.h"

namespace nebd {
namespace common {

TEST(Common, string_status_test) {
    StringStatus status;
    status.ExposeAs("test1_", "1");
    status.Update();
    ASSERT_TRUE(status.JsonBody().empty());

    status.Set("hello", "world");
    status.Update();
    ASSERT_EQ("{\"hello\":\"world\"}", status.JsonBody());
    ASSERT_EQ("world", status.GetValueByKey("hello"));

    status.Set("code", "smart");
    status.Update();
    ASSERT_EQ("{\"code\":\"smart\",\"hello\":\"world\"}", status.JsonBody());
    ASSERT_EQ("smart", status.GetValueByKey("code"));
}

}  // namespace common
}  // namespace nebd
