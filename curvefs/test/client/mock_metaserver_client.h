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

#ifndef CURVEFS_TEST_CLIENT_MOCK_METASERVER_CLIENT_H_
#define CURVEFS_TEST_CLIENT_MOCK_METASERVER_CLIENT_H_

#include <gtest/gtest.h>
#include <gmock/gmock.h>

#include <list>
#include <string>

#include "curvefs/src/client/metaserver_client.h"

using ::testing::Return;
using ::testing::_;

namespace curvefs {
namespace client {

class MockMetaServerClient : public MetaServerClient {
 public:
    MockMetaServerClient() {}
    ~MockMetaServerClient() {}

    MOCK_METHOD2(Init, CURVEFS_ERROR(const MetaServerOption &metaopt,
                       MetaServerBaseClient *baseclient));

    MOCK_METHOD0(Uinit, CURVEFS_ERROR());

    MOCK_METHOD4(GetDentry, CURVEFS_ERROR(uint32_t fsId, uint64_t inodeid,
                  const std::string &name, Dentry *out));

    MOCK_METHOD5(ListDentry, CURVEFS_ERROR(uint32_t fsId, uint64_t inodeid,
            const std::string &last, uint32_t count,
            std::list<Dentry> *dentryList));

    MOCK_METHOD1(CreateDentry, CURVEFS_ERROR(const Dentry &dentry));

    MOCK_METHOD3(DeleteDentry, CURVEFS_ERROR(
            uint32_t fsId, uint64_t inodeid, const std::string &name));

    MOCK_METHOD3(GetInode, CURVEFS_ERROR(
            uint32_t fsId, uint64_t inodeid, Inode *out));

    MOCK_METHOD1(UpdateInode, CURVEFS_ERROR(const Inode &inode));

    MOCK_METHOD2(CreateInode, CURVEFS_ERROR(
            const InodeParam &param, Inode *out));

    MOCK_METHOD2(DeleteInode, CURVEFS_ERROR(uint32_t fsId, uint64_t inodeid));
};

}  // namespace client
}  // namespace curvefs

#endif  // CURVEFS_TEST_CLIENT_MOCK_METASERVER_CLIENT_H_
