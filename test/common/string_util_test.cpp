/*
 * Project: curve
 * Created Date: Friday September 14th 2018
 * Author: hzsunjianliang
 * Copyright (c) 2018 netease
 */

#include <gtest/gtest.h>
#include <gmock/gmock.h>
#include <string>
#include "src/common/string_util.h"

namespace curve {
namespace common {

TEST(Common, SpliteString) {
    const struct {std::string path; std::string sep;
        const int size; std::vector<std::string> items;}
    testCases[] = {
        {"", "/", 0, {}},
        {"", "/", 0, {}},
        {"/path", "/", 1, {"path"} },
        {"//path", "/", 1, {"path"}},
        {"//path/", "/", 1, {"path"}},
        {"//path//", "/", 1, {"path"}},
        {"//path1//path2", "/", 2, {"path1", "path2"}},
    };

    for (int i = 0; i < sizeof(testCases)/ sizeof(testCases[0]); i++) {
        std::vector<std::string> items;
        SplitString(testCases[i].path, testCases[i].sep, &items);
        ASSERT_EQ(items.size(), testCases[i].size);
        ASSERT_EQ(items, testCases[i].items);
        items.clear();
    }
}

TEST(Common, StringToUll) {
    std::string str = "18446744073709551615";
    uint64_t out;
    ASSERT_TRUE(StringToUll(str, &out));
    ASSERT_EQ(ULLONG_MAX, out);

    str = "ffff";
    ASSERT_FALSE(StringToUll(str, &out));
}

TEST(Common, ToUnderScored) {
    // 只有字母和数字的情况
    std::string str = "test1String2";
    std::string out;
    ToUnderScored(str, &out);
    ASSERT_EQ("test1_string2", out);
    // 只有特殊字符串的情况
    str = "*&%";
    ToUnderScored(str, &out);
    ASSERT_EQ("_", out);
    // 空字符串
    str = "";
    ToUnderScored(str, &out);
    ASSERT_EQ("", out);
    // 大小写字母，数字，其他字符串组合
    str = "TestSTring1_forUnder*&%Score";
    ToUnderScored(str, &out);
    ASSERT_EQ("test_string1_for_under_score", out);
}
}  // namespace common
}  // namespace curve
