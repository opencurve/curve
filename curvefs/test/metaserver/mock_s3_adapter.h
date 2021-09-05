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
> Created Time: Tue 7 Sept 2021
 ************************************************************************/

#ifndef CURVEFS_TEST_METASERVER_MOCK_S3_ADAPTER_H_
#define CURVEFS_TEST_METASERVER_MOCK_S3_ADAPTER_H_

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <memory>
#include <string>

#include "src/common/s3_adapter.h"

using ::curve::common::S3Adapter;
using ::testing::Return;

namespace curvefs {
namespace metaserver {

class MockS3Adapter : public S3Adapter {
 public:
    MockS3Adapter() {}
    ~MockS3Adapter() {}

    MOCK_METHOD1(Init, void(const std::string&));
    MOCK_METHOD0(Deinit, void());
    MOCK_METHOD2(PutObject, int(const Aws::String&, const std::string&));
    MOCK_METHOD2(GetObject, int(const Aws::String&, std::string*));
    MOCK_METHOD1(DeleteObject, int(const Aws::String&));
};
}  // namespace metaserver
}  // namespace curvefs
#endif  // CURVEFS_TEST_METASERVER_MOCK_S3_ADAPTER_H_
