/*
 * Project: curve
 * File Created: 2019-07-03
 * Author: charisu
 * Copyright (c)￼ 2018 netease
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
