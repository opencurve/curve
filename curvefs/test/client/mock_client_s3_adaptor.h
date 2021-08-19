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
 * Created Date: Thur May 27 2021
 * Author: xuchaojie
 */

#ifndef CURVEFS_TEST_CLIENT_MOCK_CLIENT_S3_ADAPTOR_H_
#define CURVEFS_TEST_CLIENT_MOCK_CLIENT_S3_ADAPTOR_H_

#include "curvefs/src/client/s3/client_s3_adaptor.h"

namespace curvefs {
namespace client {

class MockS3ClientAdaptor : public S3ClientAdaptor {
 public:
    MockS3ClientAdaptor() {}
    ~MockS3ClientAdaptor() {}

    MOCK_METHOD2(Init, void(const S3ClientAdaptorOption& option,
                            S3Client *client));

    MOCK_METHOD4(Write, int(Inode *inode, uint64_t offset,
                            uint64_t length, const char* buf));

    MOCK_METHOD4(Read, int(Inode *inode, uint64_t offset,
                           uint64_t length, char* buf));
    MOCK_METHOD2(Truncate, int(Inode *inode, uint64_t length));
};

}  // namespace client
}  // namespace curvefs

#endif  // CURVEFS_TEST_CLIENT_MOCK_CLIENT_S3_ADAPTOR_H_
