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
}  // namespace common
}  // namespace curve
