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

/*
 * Project: Curve
 * Created Date: 2023-09-18
 * Author: Jingli Chen (Wine93)
 */

#include <gtest/gtest.h>

#include <vector>
#include <string>

#include "curvefs/src/client/vfs/utils.h"
#include "curvefs/test/client/vfs/helper/helper.h"

namespace curvefs {
namespace client {
namespace vfs {

class StringsTest : public ::testing::Test {};
class FilepathTest : public ::testing::Test {};

TEST_F(StringsTest, TrimSpace) {
    struct TestCase {
        std::string in;
        std::string out;
    };

    std::vector<TestCase> tests {
        { "  abc", "abc" },
        { "abc  ", "abc" },
        { "  abc  ", "abc" },
        { "  a bc  ", "a bc" },
        { "a b c", "a b c" },
        { "   ", "" },
    };
    for (const auto& t : tests) {
        ASSERT_EQ(strings::TrimSpace(t.in), t.out);
    }
}

TEST_F(StringsTest, HasPrefix) {
    struct TestCase {
        std::string str;
        std::string prefix;
        bool yes;
    };

    std::vector<TestCase> tests {
        { "abcde", "a", true },
        { "abcde", "abc", true },
        { "abcde", "abcde", true },
        { "abcde", "", true },
        { "abcde", "xyz", false },
        { "abcde", "abcdef", false },
        { "abcde", "bcd", false },
        { "abcde", "bcde", false },
    };
    for (const auto& t : tests) {
        bool yes = strings::HasPrefix(t.str, t.prefix);
        ASSERT_EQ(yes, t.yes);
    }
}

TEST_F(StringsTest, Split) {
    struct TestCase {
        std::string str;
        std::string sep;
        std::vector<std::string> out;
    };

    std::vector<TestCase> tests {
        { "/a/b/c", "/", { "", "a", "b", "c" } },
        { "a/b/c", "/", { "a", "b", "c" } },
        { "a//b/c", "/", { "a", "", "b", "c" } },
        { "a///b/c", "/", { "a", "", "", "b", "c" } },
        { "/", "/", { "", "" } },
        { "abc", "/", { "abc" } },
        { "aaaa", "aa", { "", "", "" } },
        { "aaaa", "aaa", { "", "a" } },
    };
    for (const auto& t : tests) {
        auto out = strings::Split(t.str, t.sep);
        ASSERT_EQ(out, t.out);
    }
}

TEST_F(StringsTest, Join) {
    struct TestCase {
        std::vector<std::string> range;
        std::string delim;
        std::string out;
    };

    std::vector<TestCase> tests {
        { { "a", "b", "c" }, "/", "a/b/c" },
        { { "", "a", "b", "c" }, "/", "/a/b/c" },
        { { "abc" }, "/", "abc" },
        { {}, "/", "" },
        { { "/a/b/c", "d/e/f" }, "/", "/a/b/c/d/e/f" },
        { { "a", "b", "c" }, "", "abc" },
    };
    for (const auto& t : tests) {
        auto out = strings::Join(t.range, t.delim);
        ASSERT_EQ(out, t.out);
    }
}

TEST_F(StringsTest, JoinRange) {
    struct TestCase {
        std::vector<std::string> strs;
        uint32_t start;
        uint32_t end;
        std::string delim;
        std::string out;
    };

    std::vector<TestCase> tests {
        { { "a", "b", "c" }, 0, 2, "/", "a/b" },
        { { "a", "b", "c" }, 0, 4, "/", "a/b/c" },
        { { "", "a", "b", "c" }, 0, 4, "/", "/a/b/c" },
    };
    for (const auto& t : tests) {
        auto out = strings::Join(t.strs, t.start, t.end, t.delim);
        ASSERT_EQ(out, t.out);
    }
}

TEST_F(FilepathTest, ParentDir) {
    struct TestCase {
        std::string path;
        std::string parent;
    };

    std::vector<TestCase> tests {
        { "/a/b/c", "/a/b" },
        { "/abc", "/" },
        { "/a/b/c/", "/a/b/c" },
        { "/", "/" },
        { "abc", "/" },
        { "", "/" },
    };
    for (const auto& t : tests) {
        auto parent = filepath::ParentDir(t.path);
        ASSERT_EQ(parent, t.parent);
    }
}

TEST_F(FilepathTest, Filename) {
    struct TestCase {
        std::string path;
        std::string filename;
    };

    std::vector<TestCase> tests {
        { "/abc/def", "def" },
        { "/abc", "abc" },
        { "/a/b/c", "c" },
        { "/a/b/c/", "" },
        { "/", "" },
    };
    for (const auto& t : tests) {
        auto filename = filepath::Filename(t.path);
        ASSERT_EQ(filename, t.filename);
    }
}

TEST_F(FilepathTest, Split) {
    struct TestCase {
        std::string path;
        std::vector<std::string> out;
    };

    std::vector<TestCase> tests {
        { "/a/b/c", { "a", "b", "c" } },
        { "/a/b/c/", { "a", "b", "c" } },
        { "/abc/def", { "abc", "def" } },
        { "abc", { "abc" } },
        { "/", { } },
        { "///", { } },
    };
    for (const auto& t : tests) {
        auto out = filepath::Split(t.path);
        ASSERT_EQ(out, t.out);
    }
}

}  // namespace vfs
}  // namespace client
}  // namespace curvefs
