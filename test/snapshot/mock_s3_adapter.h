/*************************************************************************
> File Name: mock_s3_adapter.h
> Author:
> Created Time: Thu 27 Dec 2018 09:56:04 PM CST
> Copyright (c) 2018 netease
 ************************************************************************/
#include <gtest/gtest.h>
#include <gmock/gmock.h>
#include <string>
#include "src/snapshot/s3_adapter.h"
#ifndef _MOCK_S3_ADAPTER_H
#define _MOCK_S3_ADAPTER_H

using ::testing::Return;
namespace curve {
namespace snapshotserver {

class MockS3Adapter : public S3Adapter {
 public:
    MockS3Adapter() {}
    ~MockS3Adapter() {}

    MOCK_METHOD0(Init, void());
    MOCK_METHOD0(Deinit, void());
    MOCK_METHOD0(CreateBucket, int());
    MOCK_METHOD0(DeleteBucket, int());
    MOCK_METHOD0(BucketExist, bool());

    MOCK_METHOD2(PutObject, int(const Aws::String &,
                                const std::string &));
    MOCK_METHOD2(GetObject, int(const Aws::String &,
                                std::string *));
    MOCK_METHOD1(DeleteObject, int(const Aws::String &));
    MOCK_METHOD1(ObjectExist, bool(const Aws::String &));
/*
    MOCK_METHOD2(UpdateObjectMeta, int(const Aws::String &,
                         const Aws::Map<Aws::String, Aws::String> &));
    MOCK_METHOD2(GetObjectMeta, int(const Aws::String &key,
                         Aws::Map<Aws::String, Aws::String> *));
*/
    MOCK_METHOD1(MultiUploadInit, Aws::String(const Aws::String &));
    MOCK_METHOD5(UploadOnePart,
            Aws::S3::Model::CompletedPart(const Aws::String &,
            const Aws::String,
            int,
            int,
            const char*));
    MOCK_METHOD3(CompleteMultiUpload,
                int(const Aws::String &,
                const Aws::String &,
            const Aws::Vector<Aws::S3::Model::CompletedPart> &));
    MOCK_METHOD2(AbortMultiUpload, int(const Aws::String &,
                                           const Aws::String &));
};
}  // namespace snapshotserver
}  // namespace curve
#endif
