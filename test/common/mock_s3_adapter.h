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
> File Name: mock_s3_adapter.h
> Author:
> Created Time: Thu 27 Dec 2018 09:56:04 PM CST
 ************************************************************************/

#ifndef TEST_COMMON_MOCK_S3_ADAPTER_H_
#define TEST_COMMON_MOCK_S3_ADAPTER_H_

#include <gtest/gtest.h>
#include <gmock/gmock.h>
#include <string>
#include <memory>
#include <list>
#include "src/common/s3_adapter.h"

using ::testing::Return;
namespace curve {
namespace common {

class MockS3Adapter : public S3Adapter {
 public:
    MockS3Adapter() {}
    ~MockS3Adapter() {}

    MOCK_METHOD1(Init, void(const std::string &));
    MOCK_METHOD1(Init, void(const S3AdapterOption &));
    MOCK_METHOD0(Deinit, void());
    MOCK_METHOD0(CreateBucket, int());
    MOCK_METHOD0(DeleteBucket, int());
    MOCK_METHOD0(BucketExist, bool());

    MOCK_METHOD2(PutObject, int(const Aws::String &,
                                const std::string &));
    MOCK_METHOD2(GetObject, int(const Aws::String &,
                                std::string *));
    MOCK_METHOD4(GetObject, int(const std::string &,
                                char *,
                                off_t,
                                size_t));
    MOCK_METHOD1(GetObjectAsync, void(std::shared_ptr<GetObjectAsyncContext>));
    MOCK_METHOD1(DeleteObject, int(const Aws::String &));
    MOCK_METHOD1(DeleteObjects, int(const std::list<Aws::String>& keyList));
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
}  // namespace common
}  // namespace curve
#endif  // TEST_COMMON_MOCK_S3_ADAPTER_H_

