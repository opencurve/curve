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
 * @Project: curve
 * @Date: 2021-07-07 14:46:28
 * @Author: chenwei
 */

#include "curvefs/src/mds/space_client.h"
#include <brpc/channel.h>
#include <brpc/server.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include "curvefs/test/mds/mock_space.h"

using ::testing::AtLeast;
using ::testing::StrEq;
using ::testing::_;
using ::testing::Return;
using ::testing::ReturnArg;
using ::testing::DoAll;
using ::testing::SetArgPointee;
using ::testing::SaveArg;
using ::testing::Mock;
using ::testing::Invoke;
using ::curvefs::space::MockSpaceService;
using curvefs::space::InitSpaceRequest;
using curvefs::space::InitSpaceResponse;
using curvefs::space::UnInitSpaceRequest;
using curvefs::space::UnInitSpaceResponse;
using curvefs::space::SpaceStatusCode;

namespace curvefs {
namespace mds {
class SpaceClientTest : public ::testing::Test {
 protected:
    void SetUp() override {
        addr_ = "127.0.0.1:6704";
        ASSERT_EQ(0, server_.AddService(&mockSpaceService_,
                                        brpc::SERVER_DOESNT_OWN_SERVICE));
        ASSERT_EQ(0, server_.Start(addr_.c_str(), nullptr));

        return;
    }

    void TearDown() override {
        server_.Stop(0);
        server_.Join();
        return;
    }

 protected:
    MockSpaceService mockSpaceService_;
    brpc::Server server_;
    std::string addr_;
};

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

TEST_F(SpaceClientTest, InitFailTest) {
    SpaceOptions options;
    SpaceClient client(options);
    ASSERT_FALSE(client.Init());
}

TEST_F(SpaceClientTest, InitSuccess) {
    SpaceOptions options;
    options.spaceAddr = addr_;
    SpaceClient client(options);
    ASSERT_TRUE(client.Init());
}

TEST_F(SpaceClientTest, InitSpaceNotInitFail) {
    SpaceOptions options;
    options.spaceAddr = addr_;
    SpaceClient client(options);
    FsInfo fsinfo;
    ASSERT_EQ(client.InitSpace(fsinfo), FSStatusCode::SPACE_CLIENT_NOT_INITED);
}

TEST_F(SpaceClientTest, UnInitSpaceNotInitFail) {
    SpaceOptions options;
    options.spaceAddr = addr_;
    SpaceClient client(options);
    uint32_t fsId = 0;
    ASSERT_EQ(client.UnInitSpace(fsId), FSStatusCode::SPACE_CLIENT_NOT_INITED);
}

TEST_F(SpaceClientTest, InitSpaceSuccess) {
    SpaceOptions options;
    options.spaceAddr = addr_;
    SpaceClient client(options);
    ASSERT_TRUE(client.Init());
    FsInfo fsinfo;
    fsinfo.set_fsid(1);
    fsinfo.set_fsname("fs1");
    fsinfo.set_status(FsStatus::INITED);
    fsinfo.set_rootinodeid(1);
    fsinfo.set_capacity(0);
    fsinfo.set_blocksize(0);
    fsinfo.set_mountnum(0);
    fsinfo.set_fstype(FSType::TYPE_VOLUME);
    fsinfo.mutable_detail();

    InitSpaceResponse response;
    response.set_status(SpaceStatusCode::SPACE_OK);
    EXPECT_CALL(mockSpaceService_, InitSpace(_, _, _, _))
        .WillOnce(
            DoAll(SetArgPointee<2>(response),
                  Invoke(RpcService<InitSpaceRequest, InitSpaceResponse>)));
    ASSERT_EQ(client.InitSpace(fsinfo), FSStatusCode::OK);
}

TEST_F(SpaceClientTest, InitSpaceFail) {
    SpaceOptions options;
    options.spaceAddr = addr_;
    SpaceClient client(options);
    ASSERT_TRUE(client.Init());
    FsInfo fsinfo;
    fsinfo.set_fsid(1);
    fsinfo.set_fsname("fs1");
    fsinfo.set_status(FsStatus::INITED);
    fsinfo.set_rootinodeid(1);
    fsinfo.set_capacity(0);
    fsinfo.set_blocksize(0);
    fsinfo.set_mountnum(0);
    fsinfo.set_fstype(FSType::TYPE_VOLUME);
    fsinfo.mutable_detail();

    InitSpaceResponse response;
    response.set_status(SpaceStatusCode::SPACE_UNKNOWN_ERROR);
    EXPECT_CALL(mockSpaceService_, InitSpace(_, _, _, _))
        .WillOnce(
            DoAll(SetArgPointee<2>(response),
                  Invoke(RpcService<InitSpaceRequest, InitSpaceResponse>)));
    ASSERT_EQ(client.InitSpace(fsinfo), FSStatusCode::INIT_SPACE_ERROR);
}

TEST_F(SpaceClientTest, UnInitSpaceSuccess) {
    SpaceOptions options;
    options.spaceAddr = addr_;
    SpaceClient client(options);
    ASSERT_TRUE(client.Init());
    uint32_t fsId = 0;

    UnInitSpaceResponse response;
    response.set_status(SpaceStatusCode::SPACE_OK);
    EXPECT_CALL(mockSpaceService_, UnInitSpace(_, _, _, _))
        .WillOnce(
            DoAll(SetArgPointee<2>(response),
                  Invoke(RpcService<UnInitSpaceRequest, UnInitSpaceResponse>)));
    ASSERT_EQ(client.UnInitSpace(fsId), FSStatusCode::OK);
}

TEST_F(SpaceClientTest, UnInitSpaceFail) {
    SpaceOptions options;
    options.spaceAddr = addr_;
    SpaceClient client(options);
    ASSERT_TRUE(client.Init());
    uint32_t fsId = 0;

    UnInitSpaceResponse response;
    response.set_status(SpaceStatusCode::SPACE_UNKNOWN_ERROR);
    EXPECT_CALL(mockSpaceService_, UnInitSpace(_, _, _, _))
        .WillOnce(
            DoAll(SetArgPointee<2>(response),
                  Invoke(RpcService<UnInitSpaceRequest, UnInitSpaceResponse>)));
    ASSERT_EQ(client.UnInitSpace(fsId), FSStatusCode::UNINIT_SPACE_ERROR);
}
}  // namespace mds
}  // namespace curvefs
