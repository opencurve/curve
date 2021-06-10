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


#ifndef CURVEFS_TEST_CLIENT_MOCK_METASERVER_BASE_CLIENT_H_
#define CURVEFS_TEST_CLIENT_MOCK_METASERVER_BASE_CLIENT_H_

#include <gmock/gmock.h>
#include <string>
#include "curvefs/src/client/base_client.h"

namespace curvefs {
namespace client {

class MockMetaServerBaseClient : public MetaServerBaseClient {
 public:
    MockMetaServerBaseClient() : MetaServerBaseClient() {}
    ~MockMetaServerBaseClient() = default;

    MOCK_METHOD6(GetDentry,
                 void(uint32_t fsId, uint64_t inodeid, const std::string &name,
                      GetDentryResponse *response, brpc::Controller *cntl,
                      brpc::Channel *channel));

    MOCK_METHOD7(ListDentry,
                 void(uint32_t fsId, uint64_t inodeid, const std::string &last,
                      uint32_t count, ListDentryResponse *response,
                      brpc::Controller *cntl, brpc::Channel *channel));

    MOCK_METHOD4(CreateDentry,
                 void(const Dentry &dentry, CreateDentryResponse *response,
                      brpc::Controller *cntl, brpc::Channel *channel));

    MOCK_METHOD6(DeleteDentry,
                 void(uint32_t fsId, uint64_t inodeid, const std::string &name,
                      DeleteDentryResponse *response, brpc::Controller *cntl,
                      brpc::Channel *channel));

    MOCK_METHOD5(GetInode,
                 void(uint32_t fsId, uint64_t inodeid,
                      GetInodeResponse *response, brpc::Controller *cntl,
                      brpc::Channel *channel));

    MOCK_METHOD4(UpdateInode,
                 void(const Inode &inode, UpdateInodeResponse *response,
                      brpc::Controller *cntl, brpc::Channel *channel));

    MOCK_METHOD4(CreateInode,
                 void(const InodeParam &param, CreateInodeResponse *response,
                      brpc::Controller *cntl, brpc::Channel *channel));

    MOCK_METHOD5(DeleteInode,
                 void(uint32_t fsId, uint64_t inodeid,
                      DeleteInodeResponse *response, brpc::Controller *cntl,
                      brpc::Channel *channel));
};
}  // namespace client
}  // namespace curvefs

#endif  // CURVEFS_TEST_CLIENT_MOCK_METASERVER_BASE_CLIENT_H_
