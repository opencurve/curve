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
        client_ = new S3ClientImpl();
        s3Client_ = std::make_shared<MockS3Adapter>();
        client_->SetAdapter(s3Client_);
    }

    virtual void TearDown() {
        delete client_;
    }

 protected:
    S3ClientImpl* client_;
    std::shared_ptr<MockS3Adapter> s3Client_;
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
}

TEST_F(ClientS3Test, download) {
    const std::string obj("test");
    uint64_t offset = 0;
    uint64_t len = 1024;
    char* buf = new char[len];

    EXPECT_CALL(*s3Client_, GetObject(_, _, _, _))
        .Times(2)
        .WillOnce(Return(0))
        .WillOnce(Return(-1));
    ASSERT_EQ(0, client_->Download(obj, buf, offset, len));
    ASSERT_EQ(-1, client_->Download(obj, buf, offset, len));
}

}  // namespace client
}  // namespace curvefs
