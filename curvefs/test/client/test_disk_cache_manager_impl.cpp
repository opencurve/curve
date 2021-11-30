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
#include "curvefs/test/client/mock_disk_cache_manager.h"
#include "curvefs/test/client/mock_disk_cache_base.h"
#include "curvefs/test/client/mock_client_s3.h"
#include "curvefs/test/client/mock_test_posix_wapper.h"
#include "curvefs/src/client/s3/disk_cache_manager_impl.h"
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

class TestDiskCacheManagerImpl : public ::testing::Test {
 protected:
    TestDiskCacheManagerImpl() {}
    ~TestDiskCacheManagerImpl() {}

    virtual void SetUp() {
        client_ = new MockS3Client();
        wrapper_ = std::make_shared<MockPosixWrapper>();
        diskCacheWrite_ =  std::make_shared<MockDiskCacheWrite>();
        diskCacheRead_ =  std::make_shared<MockDiskCacheRead>();
        diskCacheRead_->Init(wrapper_, "/mnt/test");
        diskCacheWrite_->Init(client_, wrapper_, "/mnt/test", 1);
        diskCacheManager_ = std::make_shared<MockDiskCacheManager>(
                          wrapper_, diskCacheWrite_, diskCacheRead_);
        diskCacheManagerImpl_ = std::make_shared<DiskCacheManagerImpl>
                              (diskCacheManager_, client_);
    }

    virtual void TearDown() {
        delete client_;
        Mock::VerifyAndClear(wrapper_.get());
        Mock::VerifyAndClear(diskCacheManagerImpl_.get());
        Mock::VerifyAndClear(diskCacheWrite_.get());
        Mock::VerifyAndClear(diskCacheRead_.get());
        Mock::VerifyAndClear(diskCacheManager_.get());
    }
    std::shared_ptr<MockDiskCacheRead> diskCacheRead_;
    std::shared_ptr<MockDiskCacheWrite> diskCacheWrite_;
    std::shared_ptr<MockDiskCacheManager> diskCacheManager_;
    std::shared_ptr<DiskCacheManagerImpl> diskCacheManagerImpl_;
    std::shared_ptr<MockPosixWrapper> wrapper_;
    MockS3Client* client_;
};


TEST_F(TestDiskCacheManagerImpl, Init) {
    S3ClientAdaptorOption s3AdaptorOption;
    EXPECT_CALL(*diskCacheManager_, Init(_, _)).WillOnce(Return(-1));
    int ret = diskCacheManagerImpl_->Init(s3AdaptorOption);
    ASSERT_EQ(-1, ret);

    EXPECT_CALL(*diskCacheManager_, Init(_, _)).WillOnce(Return(0));
    ret = diskCacheManagerImpl_->Init(s3AdaptorOption);
    ASSERT_EQ(0, ret);
}

TEST_F(TestDiskCacheManagerImpl, Write) {
    std::string fileName = "test";
    std::string buf = "test";
    int ret;
    EXPECT_CALL(*client_, Upload(_, _, _))
        .WillOnce(Return(-1));
    EXPECT_CALL(*diskCacheManager_, IsDiskCacheFull())
           .WillOnce(Return(true));
    ret = diskCacheManagerImpl_->Write(fileName,
            const_cast<char*>(buf.c_str()), 10);
    ASSERT_EQ(-1, ret);

    EXPECT_CALL(*diskCacheManager_, IsDiskCacheFull())
           .WillOnce(Return(false));
    EXPECT_CALL(*diskCacheWrite_, WriteDiskFile(_, _, _, _))
           .WillOnce(Return(-1));
    EXPECT_CALL(*client_, Upload(_, _, _))
        .WillOnce(Return(-1));
    ret = diskCacheManagerImpl_->Write(fileName,
            const_cast<char*>(buf.c_str()), 10);
    ASSERT_EQ(-1, ret);

    EXPECT_CALL(*diskCacheManager_, IsDiskCacheFull())
        .WillOnce(Return(false));
    EXPECT_CALL(*diskCacheWrite_, WriteDiskFile(_, _, _, _))
           .WillOnce(Return(0));
    EXPECT_CALL(*diskCacheWrite_, GetCacheIoFullDir())
          .WillOnce(Return(buf));
    EXPECT_CALL(*diskCacheRead_, GetCacheIoFullDir())
          .WillOnce(Return(buf));
    EXPECT_CALL(*diskCacheRead_, LinkWriteToRead(_, _, _))
          .WillOnce(Return(-1));
    EXPECT_CALL(*client_, Upload(_, _, _))
        .WillOnce(Return(-1));
    ret = diskCacheManagerImpl_->Write(fileName,
            const_cast<char*>(buf.c_str()), 10);
    ASSERT_EQ(-1, ret);

    EXPECT_CALL(*diskCacheManager_, IsDiskCacheFull())
        .WillOnce(Return(false));
    EXPECT_CALL(*diskCacheWrite_, WriteDiskFile(_, _, _, _))
        .WillOnce(Return(0));
    EXPECT_CALL(*diskCacheWrite_, GetCacheIoFullDir())
          .WillOnce(Return(buf));
    EXPECT_CALL(*diskCacheRead_, GetCacheIoFullDir())
          .WillOnce(Return(buf));
    EXPECT_CALL(*diskCacheRead_, LinkWriteToRead(_, _, _))
          .WillOnce(Return(0));
    EXPECT_CALL(*diskCacheWrite_, AsyncUploadEnqueue(_))
          .WillOnce(Return());
    ret = diskCacheManagerImpl_->Write(fileName,
            const_cast<char*>(buf.c_str()), 10);
    ASSERT_EQ(0, ret);

    EXPECT_CALL(*diskCacheManager_, IsDiskCacheFull())
        .Times(2)
        .WillOnce(Return(true))
        .WillOnce(Return(false));
    EXPECT_CALL(*client_, Upload(_, _, _))
        .WillOnce(Return(0));
    EXPECT_CALL(*diskCacheManager_, WriteReadDirect(_, _, _))
        .WillOnce(Return(-1));
    ret = diskCacheManagerImpl_->Write(fileName,
            const_cast<char*>(buf.c_str()), 10);
    ASSERT_EQ(-1, ret);

    EXPECT_CALL(*diskCacheManager_, IsDiskCacheFull())
        .Times(2)
        .WillOnce(Return(true))
        .WillOnce(Return(false));
    EXPECT_CALL(*client_, Upload(_, _, _))
        .WillOnce(Return(0));
    EXPECT_CALL(*diskCacheManager_, WriteReadDirect(_, _, _))
        .WillOnce(Return(0));
    ret = diskCacheManagerImpl_->Write(fileName,
            const_cast<char*>(buf.c_str()), 10);
    ASSERT_EQ(0, ret);
}

TEST_F(TestDiskCacheManagerImpl, Read) {
    std::string fileName = "test";
    char* buf = NULL;
    int ret;
    int length = 10;
    ret = diskCacheManagerImpl_->Read(fileName, buf, 10, length);
    ASSERT_EQ(-1, ret);

    std::string buf2 = "test";
    EXPECT_CALL(*diskCacheRead_, ReadDiskFile(_, _, _, _))
          .WillOnce(Return(1));
    EXPECT_CALL(*client_, Download(_, _, _, _))
        .WillOnce(Return(-1));
    ret = diskCacheManagerImpl_->Read(fileName,
            const_cast<char*>(buf2.c_str()), 10, length);
    ASSERT_EQ(-1, ret);

    EXPECT_CALL(*diskCacheRead_, ReadDiskFile(_, _, _, _))
          .WillOnce(Return(1));
    EXPECT_CALL(*client_, Download(_, _, _, _))
        .WillOnce(Return(0));
    ret = diskCacheManagerImpl_->Read(fileName,
            const_cast<char*>(buf2.c_str()), 10, length);
    ASSERT_EQ(0, ret);

    EXPECT_CALL(*diskCacheRead_, ReadDiskFile(_, _, _, _))
          .WillOnce(Return(length));
    ret = diskCacheManagerImpl_->Read(fileName,
            const_cast<char*>(buf2.c_str()), 10, length);
    ASSERT_EQ(length, ret);
}

TEST_F(TestDiskCacheManagerImpl, IsCached) {
    std::string fileName = "test";
    bool ret;
    ret = diskCacheManagerImpl_->IsCached(fileName);
    ASSERT_EQ(false, ret);
    diskCacheManager_->AddCache(fileName);
    ret = diskCacheManagerImpl_->IsCached(fileName);
    ASSERT_EQ(true, ret);
}

TEST_F(TestDiskCacheManagerImpl, UmountDiskCache) {
    EXPECT_CALL(*diskCacheWrite_, UploadAllCacheWriteFile())
          .WillOnce(Return(-1));
    int ret = diskCacheManagerImpl_->UmountDiskCache();
    ASSERT_EQ(0, ret);

    EXPECT_CALL(*diskCacheWrite_, UploadAllCacheWriteFile())
          .WillOnce(Return(0));
    ret = diskCacheManagerImpl_->UmountDiskCache();
    ASSERT_EQ(0, ret);
}

TEST_F(TestDiskCacheManagerImpl, WriteReadDirect) {
    std::string fileName = "test";
    std::string buf = "test";
    EXPECT_CALL(*diskCacheManager_, IsDiskCacheFull())
        .WillOnce(Return(true));
    int ret = diskCacheManagerImpl_->WriteReadDirect(fileName,
            const_cast<char*>(buf.c_str()), 10);
    ASSERT_EQ(-1, ret);

    EXPECT_CALL(*diskCacheManager_, IsDiskCacheFull())
        .WillOnce(Return(false));
    EXPECT_CALL(*diskCacheManager_, WriteReadDirect(_, _, _))
        .WillOnce(Return(-1));
    ret = diskCacheManagerImpl_->WriteReadDirect(fileName,
            const_cast<char*>(buf.c_str()), 10);
    ASSERT_EQ(-1, ret);

    EXPECT_CALL(*diskCacheManager_, IsDiskCacheFull())
        .WillOnce(Return(false));
    EXPECT_CALL(*diskCacheManager_, WriteReadDirect(_, _, _))
        .WillOnce(Return(0));
    ret = diskCacheManagerImpl_->WriteReadDirect(fileName,
            const_cast<char*>(buf.c_str()), 10);
    ASSERT_EQ(0, ret);
}

}  // namespace client
}  // namespace curvefs
