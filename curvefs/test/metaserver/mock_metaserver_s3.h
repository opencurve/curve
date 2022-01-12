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
 * Created Date: 2021-8-13
 * Author: chengyi
 */

#ifndef CURVEFS_TEST_METASERVER_MOCK_METASERVER_S3_H_
#define CURVEFS_TEST_METASERVER_MOCK_METASERVER_S3_H_

#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <string>
#include <list>

#include "curvefs/src/metaserver/s3/metaserver_s3.h"

using ::testing::_;
using ::testing::Return;

namespace curvefs {
namespace metaserver {
class MockS3Client : public S3Client {
 public:
    MockS3Client() {}
    ~MockS3Client() {}

    MOCK_METHOD1(Init, void(const curve::common::S3AdapterOption &options));
    MOCK_METHOD1(Delete, int(const std::string &name));
    MOCK_METHOD1(DeleteBatch, int(const std::list<std::string>& nameList));
};
}  // namespace metaserver
}  // namespace curvefs

#endif  // CURVEFS_TEST_METASERVER_MOCK_METASERVER_S3_H_
