/*
 * Project: nebd
 * Created Date: 20190819
 * Author: lixiaocui
 * Copyright (c) 2019 netease
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
