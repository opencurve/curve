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
 * Created Date: Mon Aug 30 2021
 * Author: hzwuhongsong
 */

#include <gtest/gtest.h>
#include <gmock/gmock.h>

#include "src/common/lru_cache.h"
#include "curvefs/test/client/mock_test_posix_wapper.h"
#include "curvefs/test/client/mock_disk_cache_base.h"
#include "curvefs/src/client/s3/disk_cache_read.h"

namespace curvefs {
namespace client {

using ::testing::_;
using ::testing::Contains;
using ::testing::DoAll;
using ::testing::ElementsAre;
using ::testing::Ge;
using ::testing::Gt;
using ::testing::Mock;
using ::testing::NotNull;
using ::testing::Return;
using ::testing::ReturnArg;
using ::testing::ReturnNull;
using ::testing::ReturnPointee;
using ::testing::ReturnRef;
using ::testing::SetArgPointee;
using ::testing::StrEq;

using ::curve::common::CacheMetrics;
using ::curve::common::SglLRUCache;

class TestDiskCacheRead : public ::testing::Test {
 protected:
    TestDiskCacheRead() {}
    ~TestDiskCacheRead() {}

    virtual void SetUp() {
        diskCacheRead_ = std::make_shared<DiskCacheRead>();
        wrapper_ = std::make_shared<MockPosixWrapper>();

        diskCacheRead_->Init(wrapper_, "test");
    }

    virtual void TearDown() {
        // allows the destructor of lfs_ to be invoked correctly
        Mock::VerifyAndClear(wrapper_.get());
        Mock::VerifyAndClear(diskCacheRead_.get());
    }
    std::shared_ptr<DiskCacheRead> diskCacheRead_;
    std::shared_ptr<MockPosixWrapper> wrapper_;
};

TEST_F(TestDiskCacheRead, ReadDiskFile) {
    EXPECT_CALL(*wrapper_, open(_, _, _)).WillOnce(Return(-1));
    std::string fileName = "test";
    uint64_t length = 10;
    int ret = diskCacheRead_->ReadDiskFile(
        fileName, const_cast<char *>(fileName.c_str()), length, length);
    ASSERT_EQ(-1, ret);

    EXPECT_CALL(*wrapper_, open(_, _, _)).WillOnce(Return(0));
    EXPECT_CALL(*wrapper_, lseek(_, _, _)).WillOnce(Return(-1));
    EXPECT_CALL(*wrapper_, close(_)).WillOnce(Return(0));
    ret = diskCacheRead_->ReadDiskFile(
        fileName, const_cast<char *>(fileName.c_str()), length, length);
    ASSERT_EQ(-1, ret);

    EXPECT_CALL(*wrapper_, open(_, _, _)).WillOnce(Return(0));
    EXPECT_CALL(*wrapper_, lseek(_, _, _)).WillOnce(Return(0));
    EXPECT_CALL(*wrapper_, read(_, _, _)).WillOnce(Return(-1));
    EXPECT_CALL(*wrapper_, close(_)).WillOnce(Return(0));
    ret = diskCacheRead_->ReadDiskFile(
        fileName, const_cast<char *>(fileName.c_str()), length, length);
    ASSERT_EQ(-1, ret);

    EXPECT_CALL(*wrapper_, open(_, _, _)).WillOnce(Return(0));
    EXPECT_CALL(*wrapper_, lseek(_, _, _)).WillOnce(Return(0));
    EXPECT_CALL(*wrapper_, read(_, _, _)).WillOnce(Return(length - 1));
    EXPECT_CALL(*wrapper_, close(_)).WillOnce(Return(0));
    ret = diskCacheRead_->ReadDiskFile(
        fileName, const_cast<char *>(fileName.c_str()), length, length);
    ASSERT_EQ(length - 1, ret);

    EXPECT_CALL(*wrapper_, open(_, _, _)).WillOnce(Return(0));
    EXPECT_CALL(*wrapper_, lseek(_, _, _)).WillOnce(Return(0));
    EXPECT_CALL(*wrapper_, read(_, _, _)).WillOnce(Return(length));
    EXPECT_CALL(*wrapper_, close(_)).WillOnce(Return(0));
    ret = diskCacheRead_->ReadDiskFile(
        fileName, const_cast<char *>(fileName.c_str()), length, length);
    ASSERT_EQ(length, ret);
}

TEST_F(TestDiskCacheRead, LinkWriteToRead) {
    EXPECT_CALL(*wrapper_, stat(NotNull(), NotNull())).WillOnce(Return(-1));
    std::string fileName = "test";
    int ret = diskCacheRead_->LinkWriteToRead(fileName, fileName, fileName);
    ASSERT_EQ(-1, ret);

    EXPECT_CALL(*wrapper_, stat(NotNull(), NotNull())).WillOnce(Return(0));
    EXPECT_CALL(*wrapper_, link(NotNull(), NotNull())).WillOnce(Return(-1));
    ret = diskCacheRead_->LinkWriteToRead(fileName, fileName, fileName);
    ASSERT_EQ(-1, ret);

    EXPECT_CALL(*wrapper_, stat(NotNull(), NotNull())).WillOnce(Return(0));
    EXPECT_CALL(*wrapper_, link(NotNull(), NotNull())).WillOnce(Return(0));
    ret = diskCacheRead_->LinkWriteToRead(fileName, fileName, fileName);
    ASSERT_EQ(0, ret);
}

TEST_F(TestDiskCacheRead, LoadAllCacheFile) {
    EXPECT_CALL(*wrapper_, stat(NotNull(), NotNull())).WillOnce(Return(-1));
    std::shared_ptr<LRUCache<std::string, bool>> cachedObj;
    cachedObj =  std::make_shared<LRUCache<std::string, bool>>
        (0, std::make_shared<CacheMetrics>("diskcache"));;
    int ret = diskCacheRead_->LoadAllCacheReadFile(cachedObj);
    ASSERT_EQ(-1, ret);

    EXPECT_CALL(*wrapper_, stat(NotNull(), NotNull())).WillOnce(Return(0));
    EXPECT_CALL(*wrapper_, opendir(NotNull())).WillOnce(ReturnNull());
    ret = diskCacheRead_->LoadAllCacheReadFile(cachedObj);
    ASSERT_EQ(-1, ret);

    DIR *dir = opendir(".");
    EXPECT_CALL(*wrapper_, stat(NotNull(), NotNull())).WillOnce(Return(0));
    EXPECT_CALL(*wrapper_, opendir(NotNull())).WillOnce(Return(dir));
    EXPECT_CALL(*wrapper_, closedir(NotNull())).WillOnce(Return(0));
    EXPECT_CALL(*wrapper_, readdir(NotNull())).WillOnce(ReturnNull());
    ret = diskCacheRead_->LoadAllCacheReadFile(cachedObj);
    ASSERT_EQ(0, ret);

    struct dirent *dirent;
    dir = opendir(".");
    dirent = readdir(dir);
    EXPECT_CALL(*wrapper_, stat(NotNull(), NotNull())).WillOnce(Return(0));
    EXPECT_CALL(*wrapper_, opendir(NotNull())).WillOnce(Return(dir));
    EXPECT_CALL(*wrapper_, readdir(NotNull()))
        .Times(2)
        .WillOnce(Return(dirent))
        .WillOnce(ReturnNull());
    EXPECT_CALL(*wrapper_, closedir(NotNull())).WillOnce(Return(0));
    ret = diskCacheRead_->LoadAllCacheReadFile(cachedObj);
    ASSERT_EQ(0, ret);
}

TEST_F(TestDiskCacheRead, WriteDiskFile) {
    EXPECT_CALL(*wrapper_, open(_, _, _)).WillOnce(Return(-1));
    std::string fileName = "test";
    uint64_t length = 10;
    int ret = diskCacheRead_->WriteDiskFile(
        fileName, const_cast<char *>(fileName.c_str()), length);
    ASSERT_EQ(-1, ret);

    EXPECT_CALL(*wrapper_, open(_, _, _)).WillOnce(Return(0));
    EXPECT_CALL(*wrapper_, write(_, _, length)).WillOnce(Return(-1));
    EXPECT_CALL(*wrapper_, close(_)).WillOnce(Return(0));
    ret = diskCacheRead_->WriteDiskFile(
        fileName, const_cast<char *>(fileName.c_str()), length);
    ASSERT_EQ(-1, ret);

    EXPECT_CALL(*wrapper_, open(_, _, _)).WillOnce(Return(0));
    EXPECT_CALL(*wrapper_, write(_, _, length)).WillOnce(Return(length + 1));
    EXPECT_CALL(*wrapper_, close(_)).WillOnce(Return(-1));
    ret = diskCacheRead_->WriteDiskFile(
        fileName, const_cast<char *>(fileName.c_str()), length);
    ASSERT_EQ(-1, ret);

    EXPECT_CALL(*wrapper_, open(_, _, _)).WillOnce(Return(0));
    EXPECT_CALL(*wrapper_, write(_, _, length)).WillOnce(Return(length));
    EXPECT_CALL(*wrapper_, close(_)).WillOnce(Return(0));
    ret = diskCacheRead_->WriteDiskFile(
        fileName, const_cast<char *>(fileName.c_str()), length);
    ASSERT_EQ(length, ret);
}

TEST_F(TestDiskCacheRead, ClearReadCache) {
    std::list<std::string> files{"16777216"};

    LOG(INFO) << "##############case1: load cache file fail.";
    EXPECT_CALL(*wrapper_, stat(NotNull(), NotNull())).WillOnce(Return(-1));
    std::set<std::string> cachedObj;
    ASSERT_EQ(-1, diskCacheRead_->ClearReadCache(files));

    LOG(INFO) << "##############case2: remove file fail, and file not exist";
    struct dirent fake;
    strcpy(fake.d_name, "1_16777216_2_0_0");  // NOLINT
    EXPECT_CALL(*wrapper_, stat(NotNull(), NotNull()))
        .Times(2)
        .WillRepeatedly(Return(0));
    EXPECT_CALL(*wrapper_, remove(NotNull())).WillOnce(Return(-1));
    ASSERT_EQ(-1, diskCacheRead_->ClearReadCache(files));

    LOG(INFO) << "##############case3: remove file ok";
    EXPECT_CALL(*wrapper_, stat(NotNull(), NotNull())).WillOnce(Return(0));
    EXPECT_CALL(*wrapper_, remove(NotNull())).WillOnce(Return(0));
    ASSERT_EQ(0, diskCacheRead_->ClearReadCache(files));

    LOG(INFO) << "##############case4: remove file fail, and file not exist";
    strcpy(fake.d_name, "1_16777216_2_0_0");  // NOLINT
    EXPECT_CALL(*wrapper_, stat(NotNull(), NotNull()))
        .WillOnce(Return(0))
        .WillOnce(Return(-1));
    EXPECT_CALL(*wrapper_, remove(NotNull())).WillOnce(Return(-1));
    ASSERT_EQ(0, diskCacheRead_->ClearReadCache(files));
}

}  // namespace client
}  // namespace curvefs

