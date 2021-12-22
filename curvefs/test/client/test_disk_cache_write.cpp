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

#include "curvefs/test/client/mock_test_posix_wapper.h"
#include "curvefs/test/client/mock_client_s3.h"
#include "curvefs/src/client/s3/disk_cache_write.h"
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
using ::testing::Invoke;
using ::testing::Return;
using ::testing::ReturnRef;
using ::testing::ReturnNull;
using ::testing::ReturnPointee;
using ::testing::NotNull;
using ::testing::StrEq;
using ::testing::ElementsAre;
using ::testing::SetArgPointee;
using ::testing::ReturnArg;

class TestDiskCacheWrite : public ::testing::Test {
 protected:
    TestDiskCacheWrite() {}
    ~TestDiskCacheWrite() {}

    virtual void SetUp() {
        Aws::InitAPI(awsOptions_);
        client_ = new MockS3Client();
        diskCacheWrite_ = std::make_shared<DiskCacheWrite>();
        wrapper_ = std::make_shared<MockPosixWrapper>();

        diskCacheWrite_->Init(client_, wrapper_, "test", 1);
    }

    virtual void TearDown() {
        // allows the destructor of lfs_ to be invoked correctly
        Mock::VerifyAndClear(wrapper_.get());
        delete client_;
        Mock::VerifyAndClear(diskCacheWrite_.get());
        Aws::ShutdownAPI(awsOptions_);
    }
    std::shared_ptr<DiskCacheWrite> diskCacheWrite_;
    std::shared_ptr<MockPosixWrapper> wrapper_;
    MockS3Client *client_;
    Aws::SDKOptions awsOptions_;
};

TEST_F(TestDiskCacheWrite, ReadFile) {
    uint64_t length;
    char* buf;
    EXPECT_CALL(*wrapper_, stat(NotNull(), NotNull()))
        .WillOnce(Return(-1));
    std::string fileName = "test";
    int ret = diskCacheWrite_->ReadFile(fileName, &buf, &length);
    ASSERT_EQ(-1, ret);

    EXPECT_CALL(*wrapper_, stat(NotNull(), NotNull()))
        .Times(2)
        .WillOnce(Return(0))
        .WillOnce(Return(-1));
    ret = diskCacheWrite_->ReadFile(fileName, &buf, &length);
    ASSERT_EQ(-1, ret);

    EXPECT_CALL(*wrapper_, stat(NotNull(), NotNull()))
        .Times(2)
        .WillRepeatedly(Return(0));
    EXPECT_CALL(*wrapper_, open(_, _, _))
        .WillOnce(Return(-1));
    ret = diskCacheWrite_->ReadFile(fileName, &buf, &length);
    ASSERT_EQ(-1, ret);

    std::string path = "test";
    EXPECT_CALL(*wrapper_, stat(NotNull(), NotNull()))
        .Times(2)
        .WillRepeatedly(Return(0));
    EXPECT_CALL(*wrapper_, open(_, _, _))
        .WillOnce(Return(0));
    EXPECT_CALL(*wrapper_, close(_))
        .WillOnce(Return(0));
    EXPECT_CALL(*wrapper_, malloc(_))
        .WillOnce(ReturnNull());
    ret = diskCacheWrite_->ReadFile(fileName, &buf, &length);
    ASSERT_EQ(-1, ret);

    EXPECT_CALL(*wrapper_, stat(NotNull(), NotNull()))
        .Times(2)
        .WillRepeatedly(Return(0));
    EXPECT_CALL(*wrapper_, open(_, _, _))
        .WillOnce(Return(0));
    EXPECT_CALL(*wrapper_, close(_))
        .WillOnce(Return(0));
    EXPECT_CALL(*wrapper_, malloc(_))
        .WillOnce(Return(&path));
    EXPECT_CALL(*wrapper_, memset(_, _, _))
        .WillOnce(ReturnNull());
    EXPECT_CALL(*wrapper_, free(_))
        .WillOnce(Return());
    ret = diskCacheWrite_->ReadFile(fileName, &buf, &length);
    ASSERT_EQ(-1, ret);

    EXPECT_CALL(*wrapper_, stat(NotNull(), NotNull()))
        .Times(2)
        .WillRepeatedly(Return(0));
    EXPECT_CALL(*wrapper_, open(_, _, _))
        .WillOnce(Return(0));
    EXPECT_CALL(*wrapper_, close(_))
        .WillOnce(Return(0));
    EXPECT_CALL(*wrapper_, malloc(_))
        .WillOnce(Return(&path));
    EXPECT_CALL(*wrapper_, memset(_, _, _))
        .WillOnce(Return(&path));
    EXPECT_CALL(*wrapper_, free(_))
        .WillOnce(Return());
    EXPECT_CALL(*wrapper_, read(_, _, _))
        .WillOnce(Return(-1));
    ret = diskCacheWrite_->ReadFile(fileName, &buf, &length);
    ASSERT_EQ(-1, ret);

    EXPECT_CALL(*wrapper_, stat(NotNull(), NotNull()))
        .Times(2)
        .WillRepeatedly(Return(0));
    EXPECT_CALL(*wrapper_, open(_, _, _))
        .WillOnce(Return(0));
    EXPECT_CALL(*wrapper_, close(_))
        .WillOnce(Return(0));
    EXPECT_CALL(*wrapper_, malloc(_))
        .WillOnce(Return(&path));
    EXPECT_CALL(*wrapper_, memset(_, _, _))
        .WillOnce(Return(&path));
    EXPECT_CALL(*wrapper_, free(_))
        .WillOnce(Return());
    EXPECT_CALL(*wrapper_, read(_, _, _))
        .WillOnce(Return(length - 1));
    ret = diskCacheWrite_->ReadFile(fileName, &buf, &length);
    ASSERT_EQ(-1, ret);

    EXPECT_CALL(*wrapper_, stat(NotNull(), NotNull()))
        .Times(2)
        .WillRepeatedly(Return(0));
    EXPECT_CALL(*wrapper_, open(_, _, _))
        .WillOnce(Return(0));
    EXPECT_CALL(*wrapper_, close(_))
        .WillOnce(Return(0));
    EXPECT_CALL(*wrapper_, malloc(_))
        .WillOnce(Return(&path));
    EXPECT_CALL(*wrapper_, memset(_, _, _))
        .WillOnce(Return(&path));
    EXPECT_CALL(*wrapper_, free(_))
        .WillRepeatedly(Return());
    EXPECT_CALL(*wrapper_, read(_, _, _))
        .WillOnce(Return(239772865546436));

    ret = diskCacheWrite_->ReadFile(fileName, &buf, &length);
    ASSERT_EQ(0, ret);
}

TEST_F(TestDiskCacheWrite, UploadFile) {
    uint64_t length = 10;
    EXPECT_CALL(*wrapper_, stat(NotNull(), NotNull()))
        .WillOnce(Return(-1));
    std::string fileName = "test";
    int ret = diskCacheWrite_->UploadFile(fileName);
    ASSERT_EQ(-1, ret);

    std::string path = "test";
    EXPECT_CALL(*wrapper_, stat(NotNull(), NotNull()))
        .Times(2)
        .WillRepeatedly(Return(0));
    EXPECT_CALL(*wrapper_, open(_, _, _))
        .WillOnce(Return(0));
    EXPECT_CALL(*wrapper_, close(_))
        .WillOnce(Return(0));
    EXPECT_CALL(*wrapper_, malloc(_))
        .WillOnce(Return(&path));
    EXPECT_CALL(*wrapper_, memset(_, _, _))
        .WillOnce(Return(&path));
    EXPECT_CALL(*wrapper_, free(_))
        .WillRepeatedly(Return());
    EXPECT_CALL(*wrapper_, read(_, _, _))
        .WillOnce(Return(239772865546436));
    EXPECT_CALL(*wrapper_, remove(_))
        .WillOnce(Return(0));
    EXPECT_CALL(*client_, UploadAsync(_))
        .WillRepeatedly(Invoke(
            [&] (const std::shared_ptr<PutObjectAsyncContext>& context) {
                context->key = "test";
                context->retCode = 0;
                context->cb(context);
    }));
    ret = diskCacheWrite_->UploadFile(fileName);
    ASSERT_EQ(0, ret);
}

TEST_F(TestDiskCacheWrite, WriteDiskFile) {
    EXPECT_CALL(*wrapper_, open(_, _, _))
        .WillOnce(Return(-1));
    std::string fileName = "test";
    uint64_t length = 10;
    int ret = diskCacheWrite_->WriteDiskFile(fileName,
                                 const_cast<char*>(fileName.c_str()),
                                 length, true);
    ASSERT_EQ(-1, ret);

    EXPECT_CALL(*wrapper_, open(_, _, _))
        .WillOnce(Return(0));
    EXPECT_CALL(*wrapper_, write(_, _, length))
        .WillOnce(Return(0));
    EXPECT_CALL(*wrapper_, close(_))
        .WillOnce(Return(0));
    ret = diskCacheWrite_->WriteDiskFile(fileName,
                             const_cast<char*>(fileName.c_str()),
                             length, true);
    ASSERT_EQ(-1, ret);

    EXPECT_CALL(*wrapper_, open(_, _, _))
        .WillOnce(Return(0));
    EXPECT_CALL(*wrapper_, write(_, _, length))
        .WillOnce(Return(length));
    EXPECT_CALL(*wrapper_, fdatasync(_))
        .WillOnce(Return(-1));
    EXPECT_CALL(*wrapper_, close(_))
        .WillOnce(Return(0));
    ret = diskCacheWrite_->WriteDiskFile(fileName,
                             const_cast<char*>(fileName.c_str()),
                             length, true);
    ASSERT_EQ(-1, ret);

    EXPECT_CALL(*wrapper_, open(_, _, _))
        .WillOnce(Return(0));
    EXPECT_CALL(*wrapper_, write(_, _, length))
        .WillOnce(Return(length));
    EXPECT_CALL(*wrapper_, fdatasync(_))
        .WillOnce(Return(0));
    EXPECT_CALL(*wrapper_, close(_))
        .WillOnce(Return(-1));
    ret = diskCacheWrite_->WriteDiskFile(fileName,
                             const_cast<char*>(fileName.c_str()),
                             length, true);
    ASSERT_EQ(-1, ret);

    EXPECT_CALL(*wrapper_, open(_, _, _))
        .WillOnce(Return(0));
    EXPECT_CALL(*wrapper_, write(_, _, length))
        .WillOnce(Return(length));
    EXPECT_CALL(*wrapper_, fdatasync(_))
        .WillOnce(Return(0));
    EXPECT_CALL(*wrapper_, close(_))
        .WillOnce(Return(0));
    ret = diskCacheWrite_->WriteDiskFile(fileName,
                             const_cast<char*>(fileName.c_str()),
                             length, true);
    ASSERT_EQ(length, ret);
}

TEST_F(TestDiskCacheWrite, UploadAllCacheWriteFile) {
    EXPECT_CALL(*wrapper_, stat(NotNull(), NotNull()))
        .WillOnce(Return(-1));
    int ret = diskCacheWrite_->UploadAllCacheWriteFile();
    ASSERT_EQ(-1, ret);

    EXPECT_CALL(*wrapper_, stat(NotNull(), NotNull()))
        .WillOnce(Return(0));
    EXPECT_CALL(*wrapper_, opendir(NotNull()))
        .WillOnce(ReturnNull());
    ret = diskCacheWrite_->UploadAllCacheWriteFile();
    ASSERT_EQ(-1, ret);

    DIR* dir = opendir(".");
    EXPECT_CALL(*wrapper_, stat(NotNull(), NotNull()))
        .WillOnce(Return(0));
    EXPECT_CALL(*wrapper_, opendir(NotNull()))
        .WillOnce(Return(dir));
    EXPECT_CALL(*wrapper_, readdir(NotNull()))
        .WillOnce(ReturnNull());
    EXPECT_CALL(*wrapper_, closedir(NotNull()))
        .WillOnce(Return(0));
    ret = diskCacheWrite_->UploadAllCacheWriteFile();
    ASSERT_EQ(0, ret);

    dir = opendir(".");
    EXPECT_NE(dir, nullptr);

    struct dirent fake;
    strcpy(fake.d_name, "fake");  // NOLINT

    EXPECT_CALL(*wrapper_, stat(NotNull(), NotNull()))
        .WillOnce(Return(0));
    EXPECT_CALL(*wrapper_, opendir(NotNull()))
        .WillOnce(Return(dir));
    EXPECT_CALL(*wrapper_, readdir(NotNull()))
        .Times(2)
        .WillOnce(Return(&fake))
        .WillOnce(ReturnNull());
    EXPECT_CALL(*wrapper_, closedir(NotNull()))
        .WillOnce(Return(-1));
    ret = diskCacheWrite_->UploadAllCacheWriteFile();
    ASSERT_EQ(-1, ret);

    dir = opendir(".");
    EXPECT_NE(dir, nullptr);

    EXPECT_CALL(*wrapper_, stat(NotNull(), NotNull()))
        .Times(2)
        .WillOnce(Return(0))
        .WillOnce(Return(-1));
    EXPECT_CALL(*wrapper_, opendir(NotNull()))
        .WillOnce(Return(dir));
    EXPECT_CALL(*wrapper_, readdir(NotNull()))
        .Times(2)
        .WillOnce(Return(&fake))
        .WillOnce(ReturnNull());
    EXPECT_CALL(*wrapper_, closedir(NotNull()))
        .WillOnce(Return(0));
    ret = diskCacheWrite_->UploadAllCacheWriteFile();
    ASSERT_EQ(0, ret);
}

TEST_F(TestDiskCacheWrite, UploadAllCacheWriteFile_2) {
    std::string path = "test";
    DIR* dir;

    dir = opendir(".");
    EXPECT_NE(dir, nullptr);

    struct dirent fake;
    strcpy(fake.d_name, "fake");  // NOLINT

    EXPECT_CALL(*wrapper_, stat(NotNull(), NotNull()))
        .Times(3)
        .WillOnce(Return(0))
        .WillOnce(Return(0))
        .WillOnce(Return(0));
    EXPECT_CALL(*wrapper_, opendir(NotNull()))
        .WillOnce(Return(dir));
    EXPECT_CALL(*wrapper_, readdir(NotNull()))
        .Times(2)
        .WillOnce(Return(&fake))
        .WillOnce(ReturnNull());
    EXPECT_CALL(*wrapper_, closedir(NotNull()))
        .WillOnce(Return(0));
    EXPECT_CALL(*wrapper_, open(_, _, _))
        .WillOnce(Return(0));
    EXPECT_CALL(*wrapper_, close(_))
        .WillOnce(Return(0));

    EXPECT_CALL(*wrapper_, malloc(_))
        .WillOnce(Return(&path));
    EXPECT_CALL(*wrapper_, memset(_, _, _))
        .WillOnce(Return(&path));
    EXPECT_CALL(*wrapper_, free(_))
        .WillRepeatedly(Return());
    EXPECT_CALL(*wrapper_, read(_, _, _))
        .WillOnce(Return(239772865546436));

     EXPECT_CALL(*client_, UploadAsync(_))
        .WillRepeatedly(Invoke(
            [&] (const std::shared_ptr<PutObjectAsyncContext>& context) {
                context->key = "test";
                context->retCode = 0;
                context->cb(context);
    }));
    EXPECT_CALL(*wrapper_, remove(_))
        .WillRepeatedly(Return(0));
    int ret = diskCacheWrite_->UploadAllCacheWriteFile();
    ASSERT_EQ(0, ret);
}

TEST_F(TestDiskCacheWrite, RemoveFile) {
    std::string file = "test";
    EXPECT_CALL(*wrapper_, remove(_))
        .WillOnce(Return(-1));
    int ret = diskCacheWrite_->RemoveFile(file);
    ASSERT_EQ(-1, ret);

    EXPECT_CALL(*wrapper_, remove(_))
        .WillOnce(Return(0));
    ret = diskCacheWrite_->RemoveFile(file);
    ASSERT_EQ(0, ret);
}

TEST_F(TestDiskCacheWrite, AsyncUploadRun) {
    EXPECT_CALL(*wrapper_, stat(NotNull(), NotNull()))
        .WillRepeatedly(Return(0));
    std::string path = "test";
    EXPECT_CALL(*wrapper_, stat(NotNull(), NotNull()))
        .WillRepeatedly(Return(0));
    EXPECT_CALL(*wrapper_, open(_, _, _))
        .WillRepeatedly(Return(0));
    EXPECT_CALL(*wrapper_, close(_))
        .WillRepeatedly(Return(0));
    EXPECT_CALL(*wrapper_, malloc(_))
        .WillRepeatedly(Return(&path));
    EXPECT_CALL(*wrapper_, memset(_, _, _))
        .WillRepeatedly(Return(&path));
    EXPECT_CALL(*wrapper_, free(_))
        .WillRepeatedly(Return());
    EXPECT_CALL(*wrapper_, read(_, _, _))
        .WillRepeatedly(Return(239772865546436));
    EXPECT_CALL(*wrapper_, remove(_))
        .WillRepeatedly(Return(0));
    EXPECT_CALL(*client_, UploadAsync(_))
        .WillRepeatedly(Invoke(
            [&] (const std::shared_ptr<PutObjectAsyncContext>& context) {
                context->key = "test";
                context->retCode = 0;
                context->cb(context);
    }));
    diskCacheWrite_->AsyncUploadEnqueue("test");
    diskCacheWrite_->AsyncUploadEnqueue("test");
    int ret = diskCacheWrite_->AsyncUploadRun();
    sleep(1);
    diskCacheWrite_->AsyncUploadEnqueue("test");
    std::string t1 = "test";
    curve::common::Thread backEndThread =
        std::thread(&DiskCacheWrite::AsyncUploadEnqueue, diskCacheWrite_, t1);
    diskCacheWrite_->AsyncUploadStop();
    backEndThread.join();
}

}  // namespace client
}  // namespace curvefs

