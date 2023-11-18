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
 * Created Date: Friday September 14th 2018
 * Author: hzsunjianliang
 */

#include <gtest/gtest.h>
#include <gmock/gmock.h>
#include <string>
#include "src/common/string_util.h"

namespace curve {
namespace common {

TEST(Common, SplitString) {
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

TEST(StringUtilTest, LTrim) {
  std::array<std::array<std::string, 2>, 4> cases = {
    std::array<std::string, 2>{"hello", "hello"},
    std::array<std::string, 2>{"", ""},
    std::array<std::string, 2>{"  ", ""},
    std::array<std::string, 2>{"  hello", "hello"},
  };

  for (auto &c : cases) {
    curve::common::LTrim(c[0]);
    EXPECT_EQ(c[0], c[1]);
  }
}

TEST(StringUtilTest, RTrim) {
  std::array<std::array<std::string, 2>, 4> cases = {
    std::array<std::string, 2>{"hello", "hello"},
    std::array<std::string, 2>{"", ""},
    std::array<std::string, 2>{"  ", ""},
    std::array<std::string, 2>{"hello  ", "hello"},
  };

  for (auto &c : cases) {
    curve::common::RTrim(c[0]);
    EXPECT_EQ(c[0], c[1]);
  }
}

TEST(StringUtilTest, Trim) {
  std::array<std::array<std::string, 2>, 10> cases = {
      std::array<std::string, 2>{"hello", "hello"},
      std::array<std::string, 2>{"   hello", "hello"},
      std::array<std::string, 2>{"hello    ", "hello"},
      std::array<std::string, 2>{"    hello  ", "hello"},
      std::array<std::string, 2>{"S3 Browser", "S3 Browser"},
      std::array<std::string, 2>{"S3 Browser  ", "S3 Browser"},
      std::array<std::string, 2>{"   S3 Browser", "S3 Browser"},
      std::array<std::string, 2>{" S3 Browser  ", "S3 Browser"},
      std::array<std::string, 2>{"    ", ""},
      std::array<std::string, 2>{"", ""},
  };

  for (auto &c : cases) {
    curve::common::Trim(c[0]);
    EXPECT_EQ(c[0], c[1]);
  }
}

}  // namespace common
}  // namespace curve
