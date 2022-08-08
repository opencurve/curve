/*
 *  Copyright (c) 2021 NetEase Inc.
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
 * Created Date: Thur Jun 22 2021
 * Author: huyao
 */
#include "curvefs/src/client/s3/client_s3.h"

#include <glog/logging.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "curvefs/test/client/mock_s3_adapter.h"

namespace curvefs {
namespace client {
using ::testing::_;
using ::testing::DoAll;
using ::testing::Invoke;
using ::testing::Return;
using ::testing::SetArgPointee;

class ClientS3Test : public testing::Test {
 protected:
    ClientS3Test() {}
    ~ClientS3Test() {}
    virtual void SetUp() {
        Aws::InitAPI(awsOptions_);
        client_ = new S3ClientImpl();
        s3Client_ = std::make_shared<MockS3Adapter>();
        client_->SetAdapter(s3Client_);
    }

    virtual void TearDown() {
        delete client_;
        Aws::ShutdownAPI(awsOptions_);
    }

 protected:
    S3ClientImpl* client_;
    std::shared_ptr<MockS3Adapter> s3Client_;
    Aws::SDKOptions awsOptions_;
};

TEST_F(ClientS3Test, upload) {
    const std::string obj("test");
    uint64_t len = 1024;

    char* buf = new char[len];

    EXPECT_CALL(*s3Client_, PutObject(_, _, _))
        .Times(2)
        .WillOnce(Return(0))
        .WillOnce(Return(-1));
    ASSERT_EQ(0, client_->Upload(obj, buf, len));
    ASSERT_EQ(-1, client_->Upload(obj, buf, len));
    delete buf;
}

TEST_F(ClientS3Test, download) {
    const std::string obj("test");
    uint64_t offset = 0;
    uint64_t len = 1024;
    char* buf = new char[len];

    EXPECT_CALL(*s3Client_, GetObject(_, _, _, _))
        .Times(3)
        .WillOnce(Return(0))
        .WillOnce(Return(-1))
        .WillOnce(Return(-1));
    EXPECT_CALL(*s3Client_, ObjectExist(_))
        .Times(2)
        .WillOnce(Return(true))
        .WillOnce(Return(false));
    ASSERT_EQ(0, client_->Download(obj, buf, offset, len));
    ASSERT_EQ(-1, client_->Download(obj, buf, offset, len));
    ASSERT_EQ(-2, client_->Download(obj, buf, offset, len));
    delete buf;
}

TEST_F(ClientS3Test, uploadync) {
    const std::string obj("test");
    uint64_t len = 1024;
    char* buf = new char[len];
    std::shared_ptr<PutObjectAsyncContext> context =
      std::make_shared<PutObjectAsyncContext>();
    context->key = "name";
    context->buffer = buf;
    context->bufferSize = 1;
    context->cb = nullptr;
    EXPECT_CALL(*s3Client_, PutObjectAsync(_))
        .WillOnce(Return());
    client_->UploadAsync(context);
    delete buf;
}

TEST_F(ClientS3Test, downloadAsync) {
    const std::string obj("test");
    uint64_t offset = 0;
    uint64_t len = 1024;
    char* buf = new char[len];

    auto context = std::make_shared<GetObjectAsyncContext>();
    context->key = "name";
    context->buf = buf;
    context->offset = 1;
    context->len = 10;
    context->cb = nullptr;

    EXPECT_CALL(*s3Client_, GetObjectAsync(_))
        .WillOnce(Return());
    client_->DownloadAsync(context);
    delete buf;
}


}  // namespace client
}  // namespace curvefs
