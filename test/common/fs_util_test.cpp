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
 * File Created: 2019-07-03
 * Author: charisu
 */

#include <gtest/gtest.h>
#include <gmock/gmock.h>
#include <string>
#include "src/common/fs_util.h"

namespace curve {
namespace common {

TEST(Common, CalcRelativePath) {
    const struct {std::string path1; std::string path2; std::string result;}
    testCases[] = {
        {"", "", ""},
        {"/test1", "/test1", "."},
        {"/test1/dir1", "/test1/dir1/file1", "./file1"},
        {"/test1/dir1", "/test1/dir2/file1", "../dir2/file1"},
        {"/test1/dir1/file2", "/test1/dir2/file1", "../../dir2/file1"},
        {"/test1/dir1/file2", "/test1/dir1", ".."},
        {"/test1/dir1/subdir1/file1", "/test1/dir1/", "../.."},
        {"./test1/dir1/dir2", "./test1/dir3/file1", "../../dir3/file1"}};
    for (int i = 0; i < sizeof(testCases)/ sizeof(testCases[0]); i++) {
        ASSERT_EQ(testCases[i].result,
                CalcRelativePath(testCases[i].path1, testCases[i].path2));
    }
}

}  // namespace common
}  // namespace curve
