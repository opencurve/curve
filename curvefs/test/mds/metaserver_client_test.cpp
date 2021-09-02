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
 * @Date: 2021-07-02 14:57:55
 * @Author: chenwei
 */

#include "curvefs/src/mds/metaserverclient/metaserver_client.h"
#include <brpc/channel.h>
#include <brpc/server.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include "curvefs/test/mds/mock/mock_metaserver.h"
#include "curvefs/test/mds/mock/mock_cli2.h"

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
using ::curvefs::metaserver::MockMetaserverService;
using curvefs::metaserver::CreateRootInodeRequest;
using curvefs::metaserver::CreateRootInodeResponse;
using curvefs::metaserver::DeleteInodeRequest;
using curvefs::metaserver::DeleteInodeResponse;
using curvefs::metaserver::MetaStatusCode;
using curvefs::metaserver::copyset::MockCliService2;
using curvefs::metaserver::copyset::GetLeaderRequest2;
using curvefs::metaserver::copyset::GetLeaderResponse2;
using curvefs::metaserver::copyset::MockCopysetService;
using curvefs::metaserver::copyset::CreateCopysetRequest;
using curvefs::metaserver::copyset::CreateCopysetResponse;
using curvefs::metaserver::copyset::COPYSET_OP_STATUS;

namespace brpc {
    DECLARE_int32(health_check_interval);
}  // namespace brpc

auto gg = []() {
    brpc::FLAGS_health_check_interval = -1;
    return 0;
}();

namespace curvefs {
namespace mds {
class MetaserverClientTest : public ::testing::Test {
 protected:
    void SetUp() override {
        addr_ = "127.0.0.1:6704";
        ASSERT_EQ(0, server_.AddService(&mockMetaserverService_,
                                        brpc::SERVER_DOESNT_OWN_SERVICE));
        ASSERT_EQ(0, server_.AddService(&mockCliService2_,
                                        brpc::SERVER_DOESNT_OWN_SERVICE));
        ASSERT_EQ(0, server_.AddService(&mockCopysetService_,
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
    MockMetaserverService mockMetaserverService_;
    MockCliService2 mockCliService2_;
    MockCopysetService mockCopysetService_;
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

TEST_F(MetaserverClientTest, InitFailTest) {
    MetaserverOptions options;
    MetaserverClient client(options);
    ASSERT_FALSE(client.Init());
}

TEST_F(MetaserverClientTest, InitSuccess) {
    MetaserverOptions options;
    options.metaserverAddr = addr_;
    MetaserverClient client(options);
    ASSERT_TRUE(client.Init());
}

TEST_F(MetaserverClientTest, CreateRootInodeNotInitFail) {
    MetaserverOptions options;
    options.metaserverAddr = addr_;
    MetaserverClient client(options);
    uint32_t fsId = 0;
    uint32_t uid = 0;
    uint32_t gid = 0;
    uint32_t mode = 0;
    ASSERT_EQ(client.CreateRootInode(fsId, uid, gid, mode),
              FSStatusCode::METASERVER_CLIENT_NOT_INITED);
}

TEST_F(MetaserverClientTest, DeleteInodeNotInitFail) {
    MetaserverOptions options;
    options.metaserverAddr = addr_;
    MetaserverClient client(options);
    uint32_t fsId = 0;
    uint32_t inodeId = 0;
    ASSERT_EQ(client.DeleteInode(fsId, inodeId),
              FSStatusCode::METASERVER_CLIENT_NOT_INITED);
}

TEST_F(MetaserverClientTest, CreateRootInodeSuccess) {
    MetaserverOptions options;
    options.metaserverAddr = addr_;
    MetaserverClient client(options);
    ASSERT_TRUE(client.Init());
    uint32_t fsId = 0;
    uint32_t uid = 0;
    uint32_t gid = 0;
    uint32_t mode = 0;

    CreateRootInodeResponse response;
    response.set_statuscode(MetaStatusCode::OK);
    EXPECT_CALL(mockMetaserverService_, CreateRootInode(_, _, _, _))
        .WillOnce(DoAll(
            SetArgPointee<2>(response),
            Invoke(
                RpcService<CreateRootInodeRequest, CreateRootInodeResponse>)));
    ASSERT_EQ(client.CreateRootInode(fsId, uid, gid, mode), FSStatusCode::OK);
}

TEST_F(MetaserverClientTest, CreateRootInodeFail) {
    MetaserverOptions options;
    options.metaserverAddr = addr_;
    MetaserverClient client(options);
    ASSERT_TRUE(client.Init());
    uint32_t fsId = 0;
    uint32_t uid = 0;
    uint32_t gid = 0;
    uint32_t mode = 0;

    CreateRootInodeResponse response;
    response.set_statuscode(MetaStatusCode::UNKNOWN_ERROR);
    EXPECT_CALL(mockMetaserverService_, CreateRootInode(_, _, _, _))
        .WillOnce(DoAll(
            SetArgPointee<2>(response),
            Invoke(
                RpcService<CreateRootInodeRequest, CreateRootInodeResponse>)));
    ASSERT_EQ(client.CreateRootInode(fsId, uid, gid, mode),
              FSStatusCode::INSERT_ROOT_INODE_ERROR);
}

TEST_F(MetaserverClientTest, DeleteInodeSuccess) {
    MetaserverOptions options;
    options.metaserverAddr = addr_;
    MetaserverClient client(options);
    ASSERT_TRUE(client.Init());
    uint32_t fsId = 0;
    uint32_t inodeId = 0;

    DeleteInodeResponse response;
    response.set_statuscode(MetaStatusCode::OK);
    EXPECT_CALL(mockMetaserverService_, DeleteInode(_, _, _, _))
        .WillOnce(
            DoAll(SetArgPointee<2>(response),
                  Invoke(RpcService<DeleteInodeRequest, DeleteInodeResponse>)));
    ASSERT_EQ(client.DeleteInode(fsId, inodeId), FSStatusCode::OK);
}

TEST_F(MetaserverClientTest, DeleteInodeFail) {
    MetaserverOptions options;
    options.metaserverAddr = addr_;
    MetaserverClient client(options);
    ASSERT_TRUE(client.Init());
    uint32_t fsId = 0;
    uint32_t inodeId = 0;

    DeleteInodeResponse response;
    response.set_statuscode(MetaStatusCode::UNKNOWN_ERROR);
    EXPECT_CALL(mockMetaserverService_, DeleteInode(_, _, _, _))
        .WillOnce(
            DoAll(SetArgPointee<2>(response),
                  Invoke(RpcService<DeleteInodeRequest, DeleteInodeResponse>)));
    ASSERT_EQ(client.DeleteInode(fsId, inodeId),
              FSStatusCode::DELETE_INODE_ERROR);
}

TEST_F(MetaserverClientTest, GetLeaderSuccess) {
    MetaserverOptions options;
    options.metaserverAddr = addr_;
    MetaserverClient client(options);

    uint32_t poolId = 1;
    uint32_t csId = 2;
    std::set<std::string> addr;
    addr.emplace("127.0.0.1:6704");
    addr.emplace("127.0.0.1:6705");
    addr.emplace("127.0.0.1:6706");
    std::string leader;

    GetLeaderResponse2 response;
    curvefs::common::Peer *peer = response.mutable_leader();
    peer->set_address("127.0.0.1:6704");
    peer->set_id(1);

    EXPECT_CALL(mockCliService2_, GetLeader(_, _, _, _))
        .WillOnce(
            DoAll(SetArgPointee<2>(response),
                  Invoke(RpcService<GetLeaderRequest2, GetLeaderResponse2>)));
    ASSERT_EQ(client.GetLeader(poolId, csId, addr, &leader), FSStatusCode::OK);
}

TEST_F(MetaserverClientTest, GetLeaderRpcFail) {
    MetaserverOptions options;
    options.metaserverAddr = addr_;
    MetaserverClient client(options);

    uint32_t poolId = 1;
    uint32_t csId = 2;
    std::set<std::string> addr;
    addr.emplace("127.0.0.1:6700");
    addr.emplace("127.0.0.1:6705");
    addr.emplace("127.0.0.1:6706");
    std::string leader;

    GetLeaderResponse2 response;
    curvefs::common::Peer *peer = response.mutable_leader();
    peer->set_address("127.0.0.1:6700");
    peer->set_id(1);
    ASSERT_EQ(client.GetLeader(poolId, csId, addr, &leader),
        FSStatusCode::NOT_FOUND);
}

TEST_F(MetaserverClientTest, GetLeaderNoResponseFail) {
    MetaserverOptions options;
    options.metaserverAddr = addr_;
    MetaserverClient client(options);

    uint32_t poolId = 1;
    uint32_t csId = 2;
    std::set<std::string> addr;
    addr.emplace("127.0.0.1:6704");
    addr.emplace("127.0.0.1:6705");
    addr.emplace("127.0.0.1:6706");
    std::string leader;

    GetLeaderResponse2 response;

    EXPECT_CALL(mockCliService2_, GetLeader(_, _, _, _))
        .WillOnce(
            DoAll(SetArgPointee<2>(response),
                  Invoke(RpcService<GetLeaderRequest2, GetLeaderResponse2>)));
    ASSERT_EQ(client.GetLeader(poolId, csId, addr, &leader),
        FSStatusCode::NOT_FOUND);
}

TEST_F(MetaserverClientTest, CreatePartitionSuccess) {
    MetaserverOptions options;
    options.metaserverAddr = addr_;
    options.rpcTimeoutMs = 500;
    MetaserverClient client(options);

    curvefs::metaserver::CreatePartitionResponse response;
    response.set_statuscode(MetaStatusCode::OK);
    EXPECT_CALL(mockMetaserverService_, CreatePartition(_, _, _, _))
        .WillOnce(
            DoAll(SetArgPointee<2>(response),
                  Invoke(RpcService<curvefs::metaserver::CreatePartitionRequest,
                   curvefs::metaserver::CreatePartitionResponse>)));
    ASSERT_EQ(client.CreatePartition(0, 1, 2, 3, 1, 100, addr_),
        FSStatusCode::OK);
}

TEST_F(MetaserverClientTest, CreatePartitionFailed) {
    MetaserverOptions options;
    options.metaserverAddr = addr_;
    options.rpcTimeoutMs = 500;
    MetaserverClient client(options);

    curvefs::metaserver::CreatePartitionResponse response;
    response.set_statuscode(MetaStatusCode::PARTITION_EXIST);
    EXPECT_CALL(mockMetaserverService_, CreatePartition(_, _, _, _))
        .WillOnce(
            DoAll(SetArgPointee<2>(response),
                  Invoke(RpcService<curvefs::metaserver::CreatePartitionRequest,
                   curvefs::metaserver::CreatePartitionResponse>)));
    ASSERT_EQ(client.CreatePartition(0, 1, 2, 3, 1, 100, addr_),
        FSStatusCode::CREATE_PARTITION_ERROR);
}

TEST_F(MetaserverClientTest, CreateCopySetSuccess) {
    MetaserverOptions options;
    options.metaserverAddr = addr_;
    options.rpcTimeoutMs = 500;
    MetaserverClient client(options);
    std::set<std::string> addrs;
    addrs.emplace("127.0.0.1:6704");

    CreateCopysetResponse response;
    response.set_status(COPYSET_OP_STATUS::COPYSET_OP_STATUS_SUCCESS);
    EXPECT_CALL(mockCopysetService_, CreateCopysetNode(_, _, _, _))
        .WillOnce(
            DoAll(SetArgPointee<2>(response),
            Invoke(RpcService<CreateCopysetRequest, CreateCopysetResponse>)));
    ASSERT_EQ(client.CreateCopySet(1, {2}, addrs), FSStatusCode::OK);
}

TEST_F(MetaserverClientTest, CreateCopySetRpcFailed) {
    MetaserverOptions options;
    options.metaserverAddr = addr_;
    options.rpcTimeoutMs = 500;
    MetaserverClient client(options);
    std::set<std::string> addrs;
    addrs.emplace("127.0.0.1:6705");

    ASSERT_EQ(client.CreateCopySet(1, {2}, addrs), FSStatusCode::RPC_ERROR);
}

TEST_F(MetaserverClientTest, CreateCopySetFailed) {
    MetaserverOptions options;
    options.metaserverAddr = addr_;
    options.rpcTimeoutMs = 500;
    MetaserverClient client(options);
    std::set<std::string> addrs;
    addrs.emplace("127.0.0.1:6704");

    CreateCopysetResponse response;
    response.set_status(COPYSET_OP_STATUS::COPYSET_OP_STATUS_FAILURE_UNKNOWN);
    EXPECT_CALL(mockCopysetService_, CreateCopysetNode(_, _, _, _))
        .WillOnce(
            DoAll(SetArgPointee<2>(response),
            Invoke(RpcService<CreateCopysetRequest, CreateCopysetResponse>)));
    ASSERT_EQ(client.CreateCopySet(1, {2}, addrs),
        FSStatusCode::CREATE_COPYSET_ERROR);
}


}  // namespace mds
}  // namespace curvefs
