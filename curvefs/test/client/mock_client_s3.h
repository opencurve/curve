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
 * Created Date: Thur Jun 22 2021
 * Author: huyao
 */

#ifndef CURVEFS_TEST_CLIENT_MOCK_CLIENT_S3_H_
#define CURVEFS_TEST_CLIENT_MOCK_CLIENT_S3_H_

#include <gtest/gtest.h>
#include <gmock/gmock.h>
#include <string>

#include "curvefs/src/client/s3/client_s3.h"

using ::testing::Return;
using ::testing::_;

namespace curvefs {
namespace client {

class MockS3Client : public S3Client {
 public:
    MockS3Client() {}
    ~MockS3Client() {}

    MOCK_METHOD1(Init, void(const curve::common::S3AdapterOption& options));
    MOCK_METHOD3(Upload, int(const std::string& name,
                             const char* buf, uint64_t length));
    MOCK_METHOD3(Append, int(const std::string& name,
                             const char* buf, uint64_t length));
    MOCK_METHOD4(Download, int(const std::string& name,
                               char* buf, uint64_t offset, uint64_t length));
};


}  // namespace client
}  // namespace curvefs

#endif  // CURVEFS_TEST_CLIENT_MOCK_CLIENT_S3_H_
