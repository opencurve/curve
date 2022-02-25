/*
 *  Copyright (c) 2022 NetEase Inc.
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
 * Created Date: 2022-03-02
 * Author: chengyi
 */

#include "curvefs/src/mds/s3/mds_s3.h"

#include <glog/logging.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <memory>

#include "test/common/mock_s3_adapter.h"

namespace curvefs {
namespace mds {
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
        s3Adapter_ = std::make_shared<curve::common::MockS3Adapter>();
        s3Client_ = std::make_shared<S3ClientImpl>();
        s3Client_->SetAdaptor(s3Adapter_);
    }

    virtual void TearDown() {
    }

 protected:
    std::shared_ptr<curve::common::MockS3Adapter> s3Adapter_;
    std::shared_ptr<S3ClientImpl> s3Client_;
};

TEST_F(ClientS3Test, Init) {
    curve::common::S3AdapterOption option;
    s3Client_->Init(option);
}

TEST_F(ClientS3Test, Reinit) {
    curve::common::S3AdapterOption option;
    s3Client_->Reinit("", "", "", "");
}

TEST_F(ClientS3Test, BucketExist) {
    EXPECT_CALL(*s3Adapter_, BucketExist()).WillOnce(Return(true));
    auto ret = s3Client_->BucketExist();
    ASSERT_EQ(ret, true);
}

TEST_F(ClientS3Test, BucketNotExist) {
    EXPECT_CALL(*s3Adapter_, BucketExist()).WillOnce(Return(false));
    auto ret = s3Client_->BucketExist();
    ASSERT_EQ(ret, false);
}

}  // namespace mds
}  // namespace curvefs
