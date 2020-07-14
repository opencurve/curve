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

/*************************************************************************
> File Name: test_snapshot_s3_adapter.cpp
> Author:
> Created Time: Thu 27 Dec 2018 04:34:29 PM CST
 ************************************************************************/

#include<iostream>
#include <gtest/gtest.h>  //NOLINT
#include <gmock/gmock.h>  //NOLINT
#include "src/common/s3_adapter.h"
#include "src/common/uuid.h"
#include "src/common/concurrent/count_down_event.h"

namespace curve {
namespace common {

class TestS3Adapter : public ::testing::Test {
 public:
    TestS3Adapter() {}
    virtual ~TestS3Adapter() {}

    void SetUp() {
        adapter_ = new S3Adapter();
        adapter_->Init("./conf/s3.conf");
        adapter_->SetBucketName("curve-unit-test-bkt");
        if (!adapter_->BucketExist()) {
            ASSERT_EQ(0, adapter_->CreateBucket());
        }
    }
    void TearDown() {
        adapter_->Deinit();
        delete adapter_;
    }
    S3Adapter *adapter_;
};

TEST_F(TestS3Adapter, testS3ObjectRequest) {
    Aws::String tmpBucket1 = "unexist";
    Aws::String tmpBucket2;
    ASSERT_EQ(false, adapter_->ObjectExist("test"));
    ASSERT_EQ(0, adapter_->PutObject("test", "test"));
    tmpBucket2 = adapter_->GetBucketName();
    adapter_->SetBucketName(tmpBucket1);
    ASSERT_EQ(-1, adapter_->PutObject("test", "test"));
    adapter_->SetBucketName(tmpBucket2);
    std::string data;
    ASSERT_EQ(0, adapter_->GetObject("test", &data));
    ASSERT_EQ(-1, adapter_->GetObject("test-null", &data));
    ASSERT_EQ("test", data);
    ASSERT_EQ(true, adapter_->ObjectExist("test"));
    ASSERT_EQ(0, adapter_->DeleteObject("test"));
    ASSERT_EQ(-1, adapter_->DeleteObject("test-null"));
    char *buf = new char[10];
    std::string teststr = "0123456789";
    ASSERT_EQ(0, adapter_->PutObject("teststr", teststr));
    ASSERT_EQ(0, adapter_->GetObject("teststr", buf, 0, 4));
    std::string tmp(buf, 4);
    ASSERT_EQ("0123", tmp);

    CountDownEvent cond(1);

    // test GetObjectAsync
    GetObjectAsyncCallBack cb = [&cond] (const S3Adapter* adapter,
    const std::shared_ptr<GetObjectAsyncContext>& ctx) {
        ASSERT_EQ(0, ctx->retCode);
        cond.Signal();
    };
    auto context = std::make_shared<GetObjectAsyncContext>();
    context->key = "teststr";
    context->buf = new char[10];
    context->offset = 1;
    context->len = 4;
    context->cb = cb;
    context->retCode = -1;
    adapter_->GetObjectAsync(context);

    cond.Wait();
    std::string tmp2(context->buf, 4);
    ASSERT_EQ("1234", tmp2);

    ASSERT_EQ(0, adapter_->DeleteObject("teststr"));
}

TEST_F(TestS3Adapter, testS3MulitPartRequest) {
    Aws::String key = "test-multipart";
    Aws::String key_err = "";
    ASSERT_EQ("", adapter_->MultiUploadInit(key_err));
    Aws::String id = adapter_->MultiUploadInit(key);
    ASSERT_NE("", id);
    const char *buf = new char[1024*1024];
    ASSERT_EQ("errorTag",
              adapter_->UploadOnePart("err", "", 1, 1024*1024, buf).GetETag());
    Aws::S3::Model::CompletedPart cp1 =
        adapter_->UploadOnePart(key, id, 1, 1024*1024, buf);
    Aws::S3::Model::CompletedPart cp2 =
        adapter_->UploadOnePart(key, id, 2, 1024*1024, buf);
    Aws::S3::Model::CompletedPart cp3 =
        adapter_->UploadOnePart(key, id, 3, 1024*1024, buf);
    Aws::S3::Model::CompletedPart cp4 =
        adapter_->UploadOnePart(key, id, 4, 1024*1024, buf);
    Aws::Vector<Aws::S3::Model::CompletedPart> cp_v;
    cp_v.push_back(cp1);
    cp_v.push_back(cp2);
    cp_v.push_back(cp3);
    cp_v.push_back(cp4);
    ASSERT_EQ(0, adapter_->CompleteMultiUpload(key, id, cp_v));
    ASSERT_EQ(-1, adapter_->CompleteMultiUpload(key_err, "", cp_v));
    ASSERT_EQ(-1, adapter_->AbortMultiUpload(key, id));
    ASSERT_EQ(0, adapter_->DeleteObject("test-multipart"));
    delete [] buf;
}

}  // namespace common
}  // namespace curve

