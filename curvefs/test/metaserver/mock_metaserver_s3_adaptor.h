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
 * Created Date: 2021-8-16
 * Author: chengyi
 */
#ifndef CURVEFS_TEST_METASERVER_MOCK_METASERVER_S3_ADAPTOR_H_
#define CURVEFS_TEST_METASERVER_MOCK_METASERVER_S3_ADAPTOR_H_

#include <gmock/gmock.h>
#include "curvefs/src/metaserver/s3/metaserver_s3_adaptor.h"

namespace curvefs {
namespace metaserver {

class MockS3ClientAdaptor : public S3ClientAdaptor {
 public:
    MockS3ClientAdaptor() {}
    ~MockS3ClientAdaptor() {}

    MOCK_METHOD2(Init,
                 void(const S3ClientAdaptorOption& option, S3Client* client));
    MOCK_METHOD1(Delete, int(const Inode& inode));
    MOCK_METHOD1(DeleteBatch, int(const Inode& inode));
};

}  // namespace metaserver
}  // namespace curvefs

#endif  // CURVEFS_TEST_METASERVER_MOCK_METASERVER_S3_ADAPTOR_H_
