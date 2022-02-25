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
 * Created Date: 2022-02-28
 * Author: chengyi
 */

#ifndef CURVEFS_TEST_MDS_MOCK_MDS_S3_H_
#define CURVEFS_TEST_MDS_MOCK_MDS_S3_H_

#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <string>
#include <list>

#include "curvefs/src/mds/s3/mds_s3.h"

using ::testing::_;
using ::testing::Return;

namespace curvefs {
namespace mds {
class MockS3Client : public S3Client {
 public:
    MockS3Client() {}
    ~MockS3Client() {}

    MOCK_METHOD1(Init, void(const curve::common::S3AdapterOption &options));
    MOCK_METHOD4(Reinit, void(const std::string& ak, const std::string& sk,
                        const std::string& endpoint,
                        const std::string& bucketName));
    MOCK_METHOD0(BucketExist, bool());
};
}  // namespace mds
}  // namespace curvefs

#endif  // CURVEFS_TEST_MDS_MOCK_MDS_S3_H_
