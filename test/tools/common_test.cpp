/*
 * Project: curve
 * File Created: 2020-02-07
 * Author: charisu
 * Copyright (c)￼ 2018 netease
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
