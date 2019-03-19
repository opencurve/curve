/*************************************************************************
> File Name: test_snapshot_s3_adapter.cpp
> Author:
> Created Time: Thu 27 Dec 2018 04:34:29 PM CST
> Copyright (c) 2018 netease
 ************************************************************************/

#include<iostream>
#include <gtest/gtest.h>  //NOLINT
#include <gmock/gmock.h>  //NOLINT
#include "src/snapshotcloneserver/common/s3_adapter.h"

namespace curve {
namespace snapshotcloneserver {

class TestS3Adapter : public ::testing::Test {
 public:
     TestS3Adapter() {}
     virtual ~TestS3Adapter() {}

    void SetUp() {
        adapter_ = new S3Adapter();
        adapter_->Init();
        adapter_->SetBucketName("curve-snapshot-test");
    }
    void TearDown() {
        adapter_->Deinit();
        delete adapter_;
    }
    S3Adapter *adapter_;
};

TEST_F(TestS3Adapter, testS3BucketRequest) {
    ASSERT_EQ(false, adapter_->BucketExist());
    ASSERT_EQ(0, adapter_->CreateBucket());
    ASSERT_EQ(true, adapter_->BucketExist());
    ASSERT_EQ(0, adapter_->DeleteBucket());
    ASSERT_EQ(-1, adapter_->DeleteBucket());
    ASSERT_EQ(0, adapter_->CreateBucket());
    ASSERT_EQ(-1, adapter_->CreateBucket());
}

TEST_F(TestS3Adapter, testS3ObjectRequest) {
    Aws::String tmpBucket1 = "tmp";
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
    ASSERT_EQ(0, adapter_->DeleteBucket());
    delete [] buf;
}

}  // namespace snapshotcloneserver
}  // namespace curve

