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
 * Created Date: Thur Jun 17 2021
 * Author: lixiaocui
 */

#include <brpc/server.h>
#include <google/protobuf/util/message_differencer.h>
#include <gtest/gtest.h>

#include <list>
#include <string>

#include "curvefs/src/client/space_client.h"
#include "curvefs/test/client/mock_spacealloc_base_client.h"
#include "curvefs/test/client/mock_spacealloc_service.h"

namespace curvefs {
namespace client {
using ::testing::_;
using ::testing::DoAll;
using ::testing::Invoke;
using ::testing::Return;
using ::testing::SetArgPointee;

class SpaceAllocServerClientImplTest : public testing::Test {
 protected:
    void SetUp() override {
        SpaceAllocServerOption opt;
        opt.spaceaddr = addr_;
        opt.rpcTimeoutMs = 1000;

        ASSERT_EQ(CURVEFS_ERROR::OK,
                  spaceclient_.Init(opt, &mockspacebasecli_));

        ASSERT_EQ(0, server_.AddService(&mocspaceallocService_,
                                        brpc::SERVER_DOESNT_OWN_SERVICE));
        ASSERT_EQ(0, server_.Start(addr_.c_str(), nullptr));
    }

    void TearDown() override {
        server_.Stop(0);
        server_.Join();
    }

 protected:
    SpaceAllocServerClientImpl spaceclient_;
    MockSpaceBaseClient mockspacebasecli_;

    MockSpaceAllocService mocspaceallocService_;
    std::string addr_ = "127.0.0.1:5703";
    brpc::Server server_;
};

TEST_F(SpaceAllocServerClientImplTest, test_AllocExtents) {
    uint32_t fsid = 1;
    std::list<ExtentAllocInfo> toallocatedExtents;
    ExtentAllocInfo info;
    info.lOffset = 0;
    info.len = 1024;
    info.leftHintAvailable = true;
    info.pOffsetLeft = 0;
    info.rightHintAvailable = true;
    info.pOffsetRight = 0;
    toallocatedExtents.push_back(info);
    info.lOffset = 1024;
    toallocatedExtents.push_back(info);
    std::list<Extent> out;

    curvefs::space::AllocateSpaceResponse response1;
    response1.set_status(curvefs::space::SpaceStatusCode::SPACE_OK);
    auto extent1 = response1.add_extents();
    extent1->set_offset(0);
    extent1->set_length(1024);
    curvefs::space::AllocateSpaceResponse response2;
    response2.set_status(curvefs::space::SpaceStatusCode::SPACE_OK);
    auto extent2 = response2.add_extents();
    extent2->set_offset(1024);
    extent2->set_length(1024);
    EXPECT_CALL(mockspacebasecli_, AllocExtents(_, _, _, _, _, _))
        .Times(2)
        .WillOnce(SetArgPointee<3>(response1))
        .WillOnce(SetArgPointee<3>(response2));
    ASSERT_EQ(CURVEFS_ERROR::OK, spaceclient_.AllocExtents(
                                     fsid, toallocatedExtents,
                                     curvefs::space::AllocateType::NONE, &out));
    ASSERT_EQ(out.size(), 2);
}

TEST_F(SpaceAllocServerClientImplTest, test_DeAllocExtents) {
    uint32_t fsid = 1;
    std::list<Extent> allocatedExtents;
    Extent extent;
    extent.set_offset(0);
    extent.set_length(1024);
    allocatedExtents.push_back(extent);
    extent.set_offset(1024);
    allocatedExtents.push_back(extent);

    curvefs::space::DeallocateSpaceResponse response;
    response.set_status(curvefs::space::SpaceStatusCode::SPACE_OK);
    EXPECT_CALL(mockspacebasecli_, DeAllocExtents(_, _, _, _, _))
        .WillOnce(SetArgPointee<2>(response));
    ASSERT_EQ(CURVEFS_ERROR::OK,
              spaceclient_.DeAllocExtents(fsid, allocatedExtents));
}


}  // namespace client
}  // namespace curvefs
