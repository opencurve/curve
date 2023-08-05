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
 * Created Date: 2023-03-29
 * Author: Jingli Chen (Wine93)
 */

#include <gtest/gtest.h>

#include "curvefs/src/client/filesystem/utils.h"
#include "curvefs/test/client/filesystem/helper/helper.h"

namespace curvefs {
namespace client {
namespace filesystem {

class UtilsTest : public ::testing::Test {};

TEST_F(UtilsTest, IsDir) {
    InodeAttr attr;
    attr.set_type(FsFileType::TYPE_DIRECTORY);

    ASSERT_TRUE(IsDir(attr));
    ASSERT_FALSE(IsS3File(attr));
    ASSERT_FALSE(IsVolmeFile(attr));
    ASSERT_FALSE(IsSymLink(attr));
}

TEST_F(UtilsTest, IsS3File) {
    InodeAttr attr;
    attr.set_type(FsFileType::TYPE_S3);

    ASSERT_FALSE(IsDir(attr));
    ASSERT_TRUE(IsS3File(attr));
    ASSERT_FALSE(IsVolmeFile(attr));
    ASSERT_FALSE(IsSymLink(attr));
}

TEST_F(UtilsTest, IsVolmeFile) {
    InodeAttr attr;
    attr.set_type(FsFileType::TYPE_FILE);

    ASSERT_FALSE(IsDir(attr));
    ASSERT_FALSE(IsS3File(attr));
    ASSERT_TRUE(IsVolmeFile(attr));
    ASSERT_FALSE(IsSymLink(attr));
}

TEST_F(UtilsTest, IsSymLink) {
    InodeAttr attr;
    attr.set_type(FsFileType::TYPE_SYM_LINK);

    ASSERT_FALSE(IsDir(attr));
    ASSERT_FALSE(IsS3File(attr));
    ASSERT_FALSE(IsVolmeFile(attr));
    ASSERT_TRUE(IsSymLink(attr));
}

TEST_F(UtilsTest, AttrMtime) {
    InodeAttr attr;
    attr.set_mtime(12345);
    attr.set_mtime_ns(67890);

    auto time = AttrMtime(attr);
    ASSERT_EQ(time.seconds, 12345);
    ASSERT_EQ(time.nanoSeconds, 67890);
}

TEST_F(UtilsTest, AttrCtime) {
    InodeAttr attr;
    attr.set_ctime(12345);
    attr.set_ctime_ns(67890);

    auto time = AttrCtime(attr);
    ASSERT_EQ(time.seconds, 12345);
    ASSERT_EQ(time.nanoSeconds, 67890);
}

TEST_F(UtilsTest, InodeMtime) {
    auto inode = MkInode(1, InodeOption().mtime(123, 456));

    auto time = InodeMtime(inode);
    ASSERT_EQ(time.seconds, 123);
    ASSERT_EQ(time.nanoSeconds, 456);
}

}  // namespace filesystem
}  // namespace client
}  // namespace curvefs
