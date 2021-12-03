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

#include "curvefs/test/client/mock_disk_cache_write.h"
#include "curvefs/test/client/mock_disk_cache_read.h"
#include "curvefs/test/client/mock_disk_cache_base.h"
#include "curvefs/test/client/mock_test_posix_wapper.h"
#include "curvefs/src/client/s3/disk_cache_manager.h"
#include "curvefs/src/client/s3/client_s3_adaptor.h"

namespace curvefs {
namespace client {

using ::testing::_;
using ::testing::Contains;
using ::curve::common::Configuration;
using ::testing::Ge;
using ::testing::Gt;
using ::testing::Mock;
using ::testing::DoAll;
using ::testing::Return;
using ::testing::ReturnRef;
using ::testing::ReturnNull;
using ::testing::ReturnPointee;
using ::testing::NotNull;
using ::testing::StrEq;
using ::testing::ElementsAre;
using ::testing::SetArgPointee;
using ::testing::ReturnArg;
using ::testing::SetArgReferee;

class TestDiskCacheManager : public ::testing::Test {
 protected:
    TestDiskCacheManager() {}
    ~TestDiskCacheManager() {}

    virtual void SetUp() {
        S3Client *client = nullptr;
        wrapper = std::make_shared<MockPosixWrapper>();
        diskCacheWrite_ =  std::make_shared<MockDiskCacheWrite>();
        diskCacheRead_ =  std::make_shared<MockDiskCacheRead>();
        diskCacheManager_ = std::make_shared<DiskCacheManager>(
                          wrapper, diskCacheWrite_, diskCacheRead_);
        diskCacheRead_->Init(wrapper, "/mnt/test");
        diskCacheWrite_->Init(client, wrapper, "/mnt/test", 1);
    }

    virtual void TearDown() {
        Mock::VerifyAndClear(wrapper.get());
        Mock::VerifyAndClear(diskCacheWrite_.get());
        Mock::VerifyAndClear(diskCacheRead_.get());
        Mock::VerifyAndClear(diskCacheManager_.get());
    }
    std::shared_ptr<MockDiskCacheRead> diskCacheRead_;
    std::shared_ptr<MockDiskCacheWrite> diskCacheWrite_;
    std::shared_ptr<DiskCacheManager> diskCacheManager_;
    std::shared_ptr<MockPosixWrapper> wrapper;
};

TEST_F(TestDiskCacheManager, Init) {
    S3ClientAdaptorOption s3AdaptorOption;
    S3Client *client = nullptr;
    EXPECT_CALL(*wrapper, stat(NotNull(), NotNull()))
        .WillOnce(Return(-1));
    EXPECT_CALL(*wrapper, mkdir(_, _))
        .WillOnce(Return(-1));
    int ret = diskCacheManager_->Init(client, s3AdaptorOption);
    ASSERT_EQ(-1, ret);

    EXPECT_CALL(*wrapper, stat(NotNull(), NotNull()))
        .WillOnce(Return(-1));
    EXPECT_CALL(*wrapper, mkdir(_, _))
        .WillOnce(Return(0));
    EXPECT_CALL(*diskCacheWrite_, CreateIoDir("/mnt/test"))
        .WillOnce(Return(-1));
    ret = diskCacheManager_->Init(client, s3AdaptorOption);
    ASSERT_EQ(-1, ret);

    EXPECT_CALL(*wrapper, stat(NotNull(), NotNull()))
        .WillOnce(Return(0));
    EXPECT_CALL(*diskCacheWrite_, CreateIoDir("/mnt/test"))
        .WillOnce(Return(-1));
    ret = diskCacheManager_->Init(client, s3AdaptorOption);
    ASSERT_EQ(-1, ret);

    EXPECT_CALL(*wrapper, stat(NotNull(), NotNull()))
          .WillOnce(Return(0));
    EXPECT_CALL(*diskCacheWrite_, CreateIoDir(_))
          .WillOnce(Return(0));
    EXPECT_CALL(*diskCacheRead_, CreateIoDir(_))
          .WillOnce(Return(-1));
    ret = diskCacheManager_->Init(client, s3AdaptorOption);
    ASSERT_EQ(-1, ret);
/*
    EXPECT_CALL(*wrapper, stat(NotNull(), NotNull()))
          .WillOnce(Return(0));
    EXPECT_CALL(*diskCacheWrite_, CreateIoDir(_))
          .WillOnce(Return(0));
    EXPECT_CALL(*diskCacheRead_, CreateIoDir(_))
          .WillOnce(Return(0));
    EXPECT_CALL(*diskCacheWrite_, AsyncUploadRun())
          .WillOnce(Return(0));
    EXPECT_CALL(*diskCacheWrite_, UploadAllCacheWriteFile())
          .WillOnce(Return(-1));
    EXPECT_CALL(*diskCacheRead_, LoadAllCacheReadFile(_))
          .WillOnce(Return(0));
    ret = diskCacheManager_->Init(client, s3AdaptorOption);
    ASSERT_EQ(0, ret);

    EXPECT_CALL(*wrapper, stat(NotNull(), NotNull()))
          .WillOnce(Return(0));
    EXPECT_CALL(*diskCacheWrite_, CreateIoDir(_))
          .WillOnce(Return(0));
    EXPECT_CALL(*diskCacheRead_, CreateIoDir(_))
          .WillOnce(Return(0));
    EXPECT_CALL(*diskCacheWrite_, AsyncUploadRun())
          .WillOnce(Return(0));
    EXPECT_CALL(*diskCacheWrite_, UploadAllCacheWriteFile())
          .WillOnce(Return(0));
    EXPECT_CALL(*diskCacheRead_, LoadAllCacheReadFile(_))
          .WillOnce(Return(-1));
    ret = diskCacheManager_->Init(client, s3AdaptorOption);
    ASSERT_EQ(-1, ret);

    EXPECT_CALL(*wrapper, stat(NotNull(), NotNull()))
          .WillOnce(Return(0));
    EXPECT_CALL(*diskCacheWrite_, CreateIoDir(_))
          .WillOnce(Return(0));
    EXPECT_CALL(*diskCacheRead_, CreateIoDir(_))
          .WillOnce(Return(0));
    EXPECT_CALL(*diskCacheWrite_, AsyncUploadRun())
          .WillOnce(Return(0));
    EXPECT_CALL(*diskCacheWrite_, UploadAllCacheWriteFile())
          .WillOnce(Return(0));
    EXPECT_CALL(*diskCacheRead_, LoadAllCacheReadFile(_))
          .WillOnce(Return(0));
    ret = diskCacheManager_->Init(client, s3AdaptorOption);
    ASSERT_EQ(0, ret);
*/
}

TEST_F(TestDiskCacheManager, CreateDir) {
    EXPECT_CALL(*wrapper, stat(NotNull(), NotNull()))
        .WillOnce(Return(-1));
    EXPECT_CALL(*wrapper, mkdir(_, _))
        .WillOnce(Return(-1));
    int ret = diskCacheManager_->CreateDir();
    ASSERT_EQ(-1, ret);

    EXPECT_CALL(*wrapper, stat(NotNull(), NotNull()))
        .WillOnce(Return(-1));
    EXPECT_CALL(*wrapper, mkdir(_, _))
        .WillOnce(Return(0));
    EXPECT_CALL(*diskCacheWrite_, CreateIoDir("/mnt/test"))
        .WillOnce(Return(-1));
    ret = diskCacheManager_->CreateDir();
    ASSERT_EQ(-1, ret);

    EXPECT_CALL(*wrapper, stat(NotNull(), NotNull()))
        .WillOnce(Return(0));
    EXPECT_CALL(*diskCacheWrite_, CreateIoDir("/mnt/test"))
        .WillOnce(Return(-1));
    ret = diskCacheManager_->CreateDir();
    ASSERT_EQ(-1, ret);

    EXPECT_CALL(*wrapper, stat(NotNull(), NotNull()))
          .WillOnce(Return(0));
    EXPECT_CALL(*diskCacheWrite_, CreateIoDir(_))
          .WillOnce(Return(0));
    EXPECT_CALL(*diskCacheRead_, CreateIoDir(_))
          .WillOnce(Return(-1));
    ret = diskCacheManager_->CreateDir();
    ASSERT_EQ(-1, ret);

    EXPECT_CALL(*wrapper, stat(NotNull(), NotNull()))
          .WillOnce(Return(0));
    EXPECT_CALL(*diskCacheWrite_, CreateIoDir(_))
          .WillOnce(Return(0));
    EXPECT_CALL(*diskCacheRead_, CreateIoDir(_))
          .WillOnce(Return(0));
    ret = diskCacheManager_->CreateDir();
    ASSERT_EQ(0, ret);
}

TEST_F(TestDiskCacheManager, AsyncUploadEnqueue) {
    std::string objName = "test";
    EXPECT_CALL(*diskCacheWrite_, AsyncUploadEnqueue(_))
          .WillOnce(Return());
    diskCacheManager_->AsyncUploadEnqueue(objName);
}

TEST_F(TestDiskCacheManager, ReadDiskFile) {
    std::string fileName = "test";
    uint64_t length = 10;
    EXPECT_CALL(*diskCacheRead_, ReadDiskFile(_, _, _, _))
          .WillOnce(Return(-1));
    int ret = diskCacheManager_->ReadDiskFile(fileName,
                                const_cast<char*>(fileName.c_str()),
                                length, length);
    ASSERT_EQ(-1, ret);
    EXPECT_CALL(*diskCacheRead_, ReadDiskFile(_, _, _, _))
          .WillOnce(Return(0));
    ret = diskCacheManager_->ReadDiskFile(fileName,
                            const_cast<char*>(fileName.c_str()),
                            length, length);
    ASSERT_EQ(0, ret);
}

TEST_F(TestDiskCacheManager, LinkWriteToRead) {
    std::string fileName = "test";
    EXPECT_CALL(*diskCacheRead_, LinkWriteToRead(_, _, _))
          .WillOnce(Return(-1));
    int ret = diskCacheManager_->LinkWriteToRead(
            fileName, fileName, fileName);
    ASSERT_EQ(-1, ret);

    EXPECT_CALL(*diskCacheRead_, LinkWriteToRead(_, _, _))
          .WillOnce(Return(0));
    ret = diskCacheManager_->LinkWriteToRead(
            fileName, fileName, fileName);
    ASSERT_EQ(0, ret);
}

TEST_F(TestDiskCacheManager, WriteDiskFile) {
    std::string fileName = "test";
    uint64_t length = 10;
    EXPECT_CALL(*diskCacheWrite_, WriteDiskFile(_, _, _, _))
          .WillOnce(Return(-1));
    int ret = diskCacheManager_->WriteDiskFile(fileName,
                                 const_cast<char*>(fileName.c_str()),
                                 length, true);
    ASSERT_EQ(-1, ret);

    EXPECT_CALL(*diskCacheWrite_, WriteDiskFile(_, _, _, _))
          .WillOnce(Return(0));
    ret = diskCacheManager_->WriteDiskFile(fileName,
                                 const_cast<char*>(fileName.c_str()),
                                 length, true);
    ASSERT_EQ(0, ret);
}

TEST_F(TestDiskCacheManager, SetDiskFsUsedRatio) {
    EXPECT_CALL(*wrapper, statfs(NotNull(), NotNull()))
        .WillOnce(Return(-1));
    int ret = diskCacheManager_->SetDiskFsUsedRatio();
    ASSERT_EQ(-1, ret);

    struct statfs stat;
    stat.f_frsize = 1;
    stat.f_blocks = 1;
    stat.f_bfree = 0;
    stat.f_bavail = 0;
    EXPECT_CALL(*wrapper, statfs(NotNull(), _))
       .WillOnce(DoAll(SetArgPointee<1>(stat), Return(0)));
    ret = diskCacheManager_->SetDiskFsUsedRatio();
    ASSERT_EQ(101, ret);
}

TEST_F(TestDiskCacheManager, IsDiskCacheFull) {
    int ret = diskCacheManager_->IsDiskCacheFull();
    ASSERT_EQ(true, ret);

    struct statfs stat;
    stat.f_frsize = 1;
    stat.f_blocks = 1;
    stat.f_bfree = 0;
    stat.f_bavail = 0;
    ret = diskCacheManager_->IsDiskCacheFull();
    ASSERT_EQ(true, ret);
}

TEST_F(TestDiskCacheManager, IsDiskCacheSafe) {
    int ret = diskCacheManager_->IsDiskCacheSafe();
    ASSERT_EQ(true, ret);
}

TEST_F(TestDiskCacheManager, TrimStop) {
    int ret = diskCacheManager_->TrimStop();
    ASSERT_EQ(0, ret);
}

TEST_F(TestDiskCacheManager, TrimRun_1) {
    EXPECT_CALL(*wrapper, statfs(NotNull(), NotNull()))
           .WillRepeatedly(Return(-1));
    int ret = diskCacheManager_->TrimRun();
    sleep(6);
    diskCacheManager_->TrimStop();
}

TEST_F(TestDiskCacheManager, TrimCache_2) {
     struct statfs stat;
     stat.f_frsize = 1;
     stat.f_blocks = 1;
     stat.f_bfree = 0;
     stat.f_bavail = 0;
     EXPECT_CALL(*wrapper, statfs(NotNull(), _))
         .WillRepeatedly(DoAll(SetArgPointee<1>(stat), Return(0)));
     stat.f_frsize = 1;
     stat.f_blocks = 1;
     stat.f_bfree = 2;
     stat.f_bavail = 101;
     EXPECT_CALL(*wrapper, statfs(NotNull(), _))
         .WillRepeatedly(DoAll(SetArgPointee<1>(stat), Return(0)));
     std::string buf = "test";
     EXPECT_CALL(*diskCacheWrite_, GetCacheIoFullDir())
           .WillRepeatedly(Return(buf));
     EXPECT_CALL(*diskCacheRead_, GetCacheIoFullDir())
           .WillRepeatedly(Return(buf));
     EXPECT_CALL(*wrapper, stat(NotNull(), NotNull()))
        .WillRepeatedly(Return(0));
     diskCacheManager_->AddCache("test");
     int ret = diskCacheManager_->TrimRun();
     sleep(6);
     diskCacheManager_->TrimStop();
}

TEST_F(TestDiskCacheManager, TrimCache_4) {
     struct statfs stat;
     stat.f_frsize = 1;
     stat.f_blocks = 1;
     stat.f_bfree = 0;
     stat.f_bavail = 0;
     EXPECT_CALL(*wrapper, statfs(NotNull(), _))
         .WillRepeatedly(DoAll(SetArgPointee<1>(stat), Return(0)));
     stat.f_frsize = 1;
     stat.f_blocks = 1;
     stat.f_bfree = 2;
     stat.f_bavail = 101;
     EXPECT_CALL(*wrapper, statfs(NotNull(), _))
         .WillRepeatedly(DoAll(SetArgPointee<1>(stat), Return(0)));
     std::string buf = "test";
     EXPECT_CALL(*diskCacheWrite_, GetCacheIoFullDir())
           .WillRepeatedly(Return(buf));
     EXPECT_CALL(*diskCacheRead_, GetCacheIoFullDir())
           .WillRepeatedly(Return(buf));
     EXPECT_CALL(*wrapper, stat(NotNull(), NotNull()))
        .WillRepeatedly(Return(-1));
     EXPECT_CALL(*wrapper, remove(_))
        .WillRepeatedly(Return(-1));
     diskCacheManager_->AddCache("test");
     int ret = diskCacheManager_->TrimRun();
     sleep(6);
     diskCacheManager_->TrimStop();
}

TEST_F(TestDiskCacheManager, TrimCache_5) {
     struct statfs stat;
     stat.f_frsize = 1;
     stat.f_blocks = 1;
     stat.f_bfree = 0;
     stat.f_bavail = 0;
     EXPECT_CALL(*wrapper, statfs(NotNull(), _))
         .WillRepeatedly(DoAll(SetArgPointee<1>(stat), Return(0)));
     stat.f_frsize = 1;
     stat.f_blocks = 1;
     stat.f_bfree = 2;
     stat.f_bavail = 101;
     EXPECT_CALL(*wrapper, statfs(NotNull(), _))
         .WillRepeatedly(DoAll(SetArgPointee<1>(stat), Return(0)));

     std::string buf = "test";
     EXPECT_CALL(*diskCacheWrite_, GetCacheIoFullDir())
           .WillRepeatedly(Return(buf));
     EXPECT_CALL(*diskCacheRead_, GetCacheIoFullDir())
           .WillRepeatedly(Return(buf));
     EXPECT_CALL(*wrapper, stat(NotNull(), NotNull()))
        .WillRepeatedly(Return(-1));
     EXPECT_CALL(*wrapper, remove(_))
        .WillRepeatedly(Return(0));
     diskCacheManager_->AddCache("test");
     int ret = diskCacheManager_->TrimRun();
     sleep(6);
     diskCacheManager_->TrimStop();
}

TEST_F(TestDiskCacheManager, WriteReadDirect) {
    std::string fileName = "test";
    std::string buf = "test";

    EXPECT_CALL(*diskCacheRead_, WriteDiskFile(_, _, _))
          .WillOnce(Return(0));
    int ret = diskCacheManager_->WriteReadDirect(fileName,
            const_cast<char*>(buf.c_str()), 10);
    ASSERT_EQ(0, ret);
}

}  // namespace client
}  // namespace curvefs
