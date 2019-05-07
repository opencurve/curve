/*
 * Project: curve
 * File Created: Tuesday, 7th May 2019 1:16:08 pm
 * Author: tongguangxun
 * Copyright (c)￼ 2018 netease
 */

#include <gtest/gtest.h>
#include <gmock/gmock.h>
#include <string>
#include "src/common/net_common.h"

namespace curve {
namespace common {
TEST(Common, NetCommon) {
    std::string addr = "123.0.0.1";
    ASSERT_FALSE(NetCommon::CheckAddressValid(addr));
    addr = "123.0.0.1:65537";
    ASSERT_FALSE(NetCommon::CheckAddressValid(addr));
    addr = "123.0.q.1:65537";
    ASSERT_FALSE(NetCommon::CheckAddressValid(addr));
    addr = "123.0.0.1:657";
    ASSERT_TRUE(NetCommon::CheckAddressValid(addr));
}
}   // namespace common
}   // namespace curve
