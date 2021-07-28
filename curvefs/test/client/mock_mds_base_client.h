/*
 *  Copyright (c) 2021 NetEase Inc.
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
 * Created Date: Thur Jun 16 2021
 * Author: lixiaocui
 */


#ifndef CURVEFS_TEST_CLIENT_MOCK_MDS_BASE_CLIENT_H_
#define CURVEFS_TEST_CLIENT_MOCK_MDS_BASE_CLIENT_H_

#include <gmock/gmock.h>
#include <string>
#include "curvefs/src/client/base_client.h"

namespace curvefs {
namespace client {

class MockMDSBaseClient : public MDSBaseClient {
 public:
    MockMDSBaseClient() : MDSBaseClient() {}
    ~MockMDSBaseClient() = default;

    MOCK_METHOD6(CreateFs,
                 void(const std::string &fsName, uint64_t blockSize,
                      const Volume &volume, CreateFsResponse *response,
                      brpc::Controller *cntl, brpc::Channel *channel));

    MOCK_METHOD4(DeleteFs,
                 void(const std::string &fsName, DeleteFsResponse *response,
                      brpc::Controller *cntl, brpc::Channel *channel));

    MOCK_METHOD5(MountFs,
                 void(const std::string &fsName, const MountPoint &mountPt,
                      MountFsResponse *response, brpc::Controller *cntl,
                      brpc::Channel *channel));

    MOCK_METHOD5(UmountFs,
                 void(const std::string &fsName, const MountPoint &mountPt,
                      UmountFsResponse *response, brpc::Controller *cntl,
                      brpc::Channel *channel));

    MOCK_METHOD4(GetFsInfo,
                 void(const std::string &fsName, GetFsInfoResponse *response,
                      brpc::Controller *cntl, brpc::Channel *channel));

    MOCK_METHOD4(GetFsInfo,
                 void(uint32_t fsId, GetFsInfoResponse *response,
                      brpc::Controller *cntl, brpc::Channel *channel));
};
}  // namespace client
}  // namespace curvefs

#endif  // CURVEFS_TEST_CLIENT_MOCK_MDS_BASE_CLIENT_H_
