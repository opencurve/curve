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
 * Created Date: Thur Jun 15 2021
 * Author: lixiaocui
 */

#include <brpc/server.h>
#include <gtest/gtest.h>
#include <google/protobuf/util/message_differencer.h>

#include "curvefs/src/client/base_client.h"
#include "curvefs/test/client/mock_mds_service.h"
#include "curvefs/test/client/mock_metaserver_service.h"
#include "curvefs/test/client/mock_spacealloc_service.h"

namespace curvefs {
namespace client {
using ::testing::_;
using ::testing::DoAll;
using ::testing::Invoke;
using ::testing::Return;
using ::testing::SetArgPointee;


template <typename RpcRequestType, typename RpcResponseType,
          bool RpcFailed = false>
void RpcService(google::protobuf::RpcController *cntl_base,
                const RpcRequestType *request, RpcResponseType *response,
                google::protobuf::Closure *done) {
    if (RpcFailed) {
        brpc::Controller *cntl = static_cast<brpc::Controller *>(cntl_base);
        cntl->SetFailed(112, "Not connected to");
    }
    done->Run();
}

class BaseClientTest : public testing::Test {
 protected:
    void SetUp() override {
        ASSERT_EQ(0, server_.AddService(&mockSpaceAllocService_,
                                        brpc::SERVER_DOESNT_OWN_SERVICE));
        ASSERT_EQ(0, server_.Start(addr_.c_str(), nullptr));
    }

    void TearDown() override {
        server_.Stop(0);
        server_.Join();
    }

 protected:
    MockSpaceAllocService mockSpaceAllocService_;
    SpaceBaseClient spacebasecli_;

    std::string addr_ = "127.0.0.1:5700";
    brpc::Server server_;
};

TEST_F(BaseClientTest, test_AllocExtents) {
    uint32_t fsId = 1;
    ExtentAllocInfo info;
    info.lOffset = 0;
    info.len = 1024;
    info.leftHintAvailable = true;
    info.pOffsetLeft = 0;
    info.rightHintAvailable = true;
    info.pOffsetRight = 0;
    curvefs::space::AllocateSpaceResponse resp;
    brpc::Controller cntl;
    cntl.set_timeout_ms(1000);
    brpc::Channel ch;
    ASSERT_EQ(0, ch.Init(addr_.c_str(), nullptr));

    curvefs::space::AllocateSpaceResponse response;
    auto extent = response.add_extents();
    extent->set_offset(0);
    extent->set_length(1024);
    response.set_status(curvefs::space::SpaceStatusCode::SPACE_OK);
    EXPECT_CALL(mockSpaceAllocService_, AllocateSpace(_, _, _, _))
        .WillOnce(DoAll(
            SetArgPointee<2>(response),
            Invoke(RpcService<AllocateSpaceRequest, AllocateSpaceResponse>)));

    spacebasecli_.AllocExtents(fsId, info, curvefs::space::AllocateType::NONE,
                               &resp, &cntl, &ch);
    ASSERT_FALSE(cntl.Failed()) << cntl.ErrorText();
    ASSERT_TRUE(
        google::protobuf::util::MessageDifferencer::Equals(resp, response))
        << "resp:\n"
        << resp.ShortDebugString() << "response:\n"
        << response.ShortDebugString();
}

TEST_F(BaseClientTest, test_AllocExtents1) {
    uint32_t fsId = 1;
    ExtentAllocInfo info;
    info.lOffset = 0;
    info.len = 1024;
    info.leftHintAvailable = false;
    info.pOffsetLeft = 0;
    info.rightHintAvailable = false;
    info.pOffsetRight = 0;
    curvefs::space::AllocateSpaceResponse resp;
    brpc::Controller cntl;
    cntl.set_timeout_ms(1000);
    brpc::Channel ch;
    ASSERT_EQ(0, ch.Init(addr_.c_str(), nullptr));

    curvefs::space::AllocateSpaceResponse response;
    auto extent = response.add_extents();
    extent->set_offset(0);
    extent->set_length(1024);
    response.set_status(curvefs::space::SpaceStatusCode::SPACE_OK);
    EXPECT_CALL(mockSpaceAllocService_, AllocateSpace(_, _, _, _))
        .WillOnce(DoAll(
            SetArgPointee<2>(response),
            Invoke(RpcService<AllocateSpaceRequest, AllocateSpaceResponse>)));

    spacebasecli_.AllocExtents(fsId, info, curvefs::space::AllocateType::NONE,
                               &resp, &cntl, &ch);
    ASSERT_FALSE(cntl.Failed()) << cntl.ErrorText();
    ASSERT_TRUE(
        google::protobuf::util::MessageDifferencer::Equals(resp, response))
        << "resp:\n"
        << resp.ShortDebugString() << "response:\n"
        << response.ShortDebugString();
}

TEST_F(BaseClientTest, test_DeAllocExtents) {
    uint32_t fsId = 1;
    Extent extent;
    extent.set_offset(0);
    extent.set_length(1024);
    std::list<Extent> allocatedExtents;
    allocatedExtents.push_back(extent);
    curvefs::space::DeallocateSpaceResponse resp;
    brpc::Controller cntl;
    cntl.set_timeout_ms(1000);
    brpc::Channel ch;
    ASSERT_EQ(0, ch.Init(addr_.c_str(), nullptr));

    curvefs::space::DeallocateSpaceResponse response;
    response.set_status(curvefs::space::SpaceStatusCode::SPACE_OK);
    EXPECT_CALL(mockSpaceAllocService_, DeallocateSpace(_, _, _, _))
        .WillOnce(DoAll(
            SetArgPointee<2>(response),
            Invoke(
                RpcService<DeallocateSpaceRequest, DeallocateSpaceResponse>)));

    spacebasecli_.DeAllocExtents(fsId, allocatedExtents, &resp, &cntl, &ch);
    ASSERT_FALSE(cntl.Failed()) << cntl.ErrorText();
    ASSERT_TRUE(
        google::protobuf::util::MessageDifferencer::Equals(resp, response))
        << "resp:\n"
        << resp.ShortDebugString() << "response:\n"
        << response.ShortDebugString();
}

}  // namespace client
}  // namespace curvefs
