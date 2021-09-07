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

#ifndef CURVEFS_TEST_CLIENT_MOCK_MDS_CLIENT_H_
#define CURVEFS_TEST_CLIENT_MOCK_MDS_CLIENT_H_

#include <gtest/gtest.h>
#include <gmock/gmock.h>

#include <string>
#include <vector>

#include "curvefs/src/client/rpcclient/mds_client.h"

using ::testing::Return;
using ::testing::_;

namespace curvefs {
namespace client {
namespace rpcclient {

class MockMdsClient : public MdsClient {
 public:
    MockMdsClient() {}
    ~MockMdsClient() {}

    MOCK_METHOD2(Init, FSStatusCode(
        const ::curve::client::MetaServerOption &mdsOpt,
        MDSBaseClient *baseclient));

    MOCK_METHOD3(CreateFs, FSStatusCode(const std::string &fsName,
        uint64_t blockSize,
        const Volume &volume));

    MOCK_METHOD3(CreateFsS3, FSStatusCode(const std::string &fsName,
        uint64_t blockSize,
        const S3Info &s3Info));

    MOCK_METHOD1(DeleteFs, FSStatusCode(const std::string &fsName));

    MOCK_METHOD3(MountFs, FSStatusCode(const std::string &fsName,
        const std::string &mountPt,
        FsInfo *fsInfo));

    MOCK_METHOD2(UmountFs, FSStatusCode(const std::string &fsName,
        const std::string &mountPt));

    MOCK_METHOD2(GetFsInfo, FSStatusCode(const std::string& fsName,
        FsInfo* fsInfo));

    MOCK_METHOD2(GetFsInfo, FSStatusCode(uint32_t fsId, FsInfo* fsInfo));

    MOCK_METHOD1(CommitTx,
                 FSStatusCode(const std::vector<PartitionTxId>& txIds));
};

}  // namespace rpcclient
}  // namespace client
}  // namespace curvefs

#endif  // CURVEFS_TEST_CLIENT_MOCK_MDS_CLIENT_H_
