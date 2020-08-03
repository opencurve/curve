/*
 *     Copyright (c) 2020 NetEase Inc.
 *
 *  This program is free software; you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License as published by
 *  the Free Software Foundation; either version 2 of the License, or
 *  (at your option) any later version.
 *
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License along
 *  with this program; if not, write to the Free Software Foundation, Inc.,
 *  51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.
 */

/*
 * Project: curve
 * Created Date: Wed Jul 22 11:19:30 CST 2020
 * Author: wuhanqing
 */

#include <glog/logging.h>
#include <gtest/gtest.h>
#include <limits>
#include <string>
#include "nbd/src/argparse.h"

namespace curve {
namespace nbd {

std::string Quoted(const std::string& str) {
    return '\'' + str + '\'';
}

float strict_strtof(const char* str, std::string* err);
int64_t strict_strtoll(const char* str, int base, std::string* err);
int strict_strtol(const char* str, int base, std::string* err);

TEST(TestArgParse, Test_strict_strtof) {
    std::string error;
    const std::vector<const char*> validNums = {
        "1.2345",
        "-1.2345",
        "   1"
    };

    for (const auto& num : validNums) {
        error.clear();
        LOG(INFO) << "testing: " << Quoted(num);
        EXPECT_FLOAT_EQ(std::stof(num), strict_strtof(num, &error));
        EXPECT_EQ(0, error.size());
    }

    const std::vector<const char*> invalidNums = {
        "-1.2 ",  // trailing whitespace is illegal
        " 1.2 ",
        "-1.2-1.2",
        "1  0",
        "------1.2",
    };

    for (const auto& num : invalidNums) {
        error.clear();
        LOG(INFO) << "testing: " << Quoted(num);
        EXPECT_FLOAT_EQ(0.0f, strict_strtof(num, &error));
        ASSERT_NE(0, error.size());
    }
}

TEST(TestArgParse, Test_strict_strtol) {
    std::string error;
    std::vector<const char*> validNums = {
        "12345",
        "-12345",
        "      12345",
        "  -12456",
        "+12345",
        "+2147483647",
        "-2147483648",
    };

    for (const auto& num : validNums) {
        error.clear();
        LOG(INFO) << "testing: " << Quoted(num);
        ASSERT_EQ(std::stol(num), strict_strtol(num, 10, &error));
        ASSERT_EQ(0, error.size());
    }

    std::vector<const char*> invalidNums = {
        "-----------1",
        "++++++++++++1",
        "1234567890123124",  // out of range
        "-1234567890123",    // out of range
        "12345    ",
        "12345   1",
    };

    for (const auto& num : invalidNums) {
        error.clear();
        LOG(INFO) << "testing: " << Quoted(num);
        ASSERT_EQ(0, strict_strtol(num, 10, &error));
        ASSERT_NE(0, error.size());
    }
}

TEST(TestArgParse, Test_strict_strtoll) {
    std::string error;
    std::vector<const char*> validNums = {
        "12345",
        "-12345",
        "      12345",
        "  -12456",
        "+12345",
        "+9223372036854775807",
        "-9223372036854775808",
    };

    for (const auto& num : validNums) {
        error.clear();
        LOG(INFO) << "testing: " << Quoted(num);
        ASSERT_EQ(std::stoll(num), strict_strtoll(num, 10, &error));
        ASSERT_EQ(0, error.size());
    }

    std::vector<const char*> invalidNums = {
        "-----------1",
        "++++++++++++1",
        "9223372036854775808",   // out of range
        "-9223372036854775809",  // out of range
        "12345    ",
        "12345   1",
    };

    for (const auto& num : invalidNums) {
        error.clear();
        LOG(INFO) << "testing: " << Quoted(num);
        ASSERT_EQ(0, strict_strtoll(num, 10, &error));
        ASSERT_NE(0, error.size());
    }
}

}  // namespace nbd
}  // namespace curve
