/*
 * Project: curve
 * Created Date: Wed Mar 13 2019
 * Author: xuchaojie
 * Copyright (c) 2018 netease
 */


#include <gtest/gtest.h>
#include <gmock/gmock.h>
#include <brpc/controller.h>
#include <brpc/channel.h>
#include <brpc/server.h>

#include <chrono>  //NOLINT
#include <thread>  //NOLINT

#include "proto/cli.pb.h"
#include "proto/chunk.pb.h"
#include "src/mds/chunkserverclient/chunkserver_client.h"
#include "test/mds/chunkserverclient/mock_topology.h"
#include "test/mds/chunkserverclient/mock_chunkserver.h"


using ::curve::mds::topology::READWRITE;
using ::curve::mds::topology::MockTopology;
using ::curve::mds::topology::UNINTIALIZE_ID;
using ::curve::mds::topology::DISKNORMAL;
using ::curve::mds::topology::ONLINE;
using ::curve::mds::topology::OFFLINE;
using ::curve::mds::topology::ChunkServerState;

using ::curve::chunkserver::MockChunkService;
using ::curve::chunkserver::MockCliService;
using ::curve::chunkserver::ChunkRequest;
using ::curve::chunkserver::ChunkResponse;
using ::curve::chunkserver::CHUNK_OP_TYPE;
using ::curve::chunkserver::CHUNK_OP_STATUS;
using ::curve::chunkserver::CHUNK_OP_STATUS_FAILURE_UNKNOWN;
using ::curve::chunkserver::CHUNK_OP_STATUS_REDIRECTED;

using ::curve::chunkserver::GetLeaderRequest;
using ::curve::chunkserver::GetLeaderResponse;

using ::google::protobuf::RpcController;
using ::google::protobuf::Closure;

using ::testing::Return;
using ::testing::_;
using ::testing::SetArgPointee;
using ::testing::Invoke;

namespace curve {
namespace mds {
namespace chunkserverclient {

class TestChunkServerClient : public ::testing::Test {
 protected:
    TestChunkServerClient() {}
    void SetUp() {
        listenAddr_ = "127.0.0.1:8888";
        server_ = new brpc::Server();
        topo_ = std::make_shared<MockTopology>();
        client_ = std::make_shared<ChunkServerClient>(topo_);
    }
    void TearDown() {
        topo_ = nullptr;
        client_ = nullptr;

        server_->Stop(0);
        server_->Join();
        delete server_;
        server_ = nullptr;
    }

 protected:
    std::shared_ptr<MockTopology> topo_;
    std::shared_ptr<ChunkServerClient> client_;
    std::string listenAddr_;
    brpc::Server *server_;
};

TEST_F(TestChunkServerClient, TestDeleteChunkSnapshotSuccess) {
    MockChunkService chunkService;
    ASSERT_EQ(server_->AddService(&chunkService,
                                  brpc::SERVER_DOESNT_OWN_SERVICE), 0);
    ASSERT_EQ(server_->Start(listenAddr_.c_str(), nullptr), 0);

    ChunkServerIdType csId = 0x01;
    LogicalPoolID logicalPoolId = 0x11;
    CopysetID copysetId = 0x21;
    ChunkID chunkId = 0x31;
    uint64_t sn = 100;

    ChunkServer chunkserver(
        csId, "", "", 0x101, "127.0.0.1", 8888, "", READWRITE);
    ChunkServerState csState;
    csState.SetDiskState(DISKNORMAL);
    csState.SetOnlineState(ONLINE);
    chunkserver.SetChunkServerState(csState);

    EXPECT_CALL(*topo_, GetChunkServer(csId, _))
        .WillOnce(DoAll(SetArgPointee<1>(chunkserver),
            Return(true)));
    ChunkResponse response;
    response.set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS);
    EXPECT_CALL(chunkService, DeleteChunkSnapshot(_, _, _, _))
        .WillOnce(DoAll(SetArgPointee<2>(response),
                Invoke([](RpcController *controller,
                          const ChunkRequest *request,
                          ChunkResponse *response,
                          Closure *done){
                          brpc::ClosureGuard doneGuard(done);
                    })));

    int ret = client_->DeleteChunkSnapshot(
        csId, logicalPoolId, copysetId, chunkId, sn);
    ASSERT_EQ(kMdsSuccess, ret);
}

TEST_F(TestChunkServerClient, TestDeleteChunkSnapshotGetChunkServerFail) {
    MockChunkService chunkService;
    ASSERT_EQ(server_->AddService(&chunkService,
                                  brpc::SERVER_DOESNT_OWN_SERVICE), 0);
    ASSERT_EQ(server_->Start(listenAddr_.c_str(), nullptr), 0);

    ChunkServerIdType csId = 0x01;
    LogicalPoolID logicalPoolId = 0x11;
    CopysetID copysetId = 0x21;
    ChunkID chunkId = 0x31;
    uint64_t sn = 100;

    ChunkServer chunkserver(
        csId, "", "", 0x101, "127.0.0.1", 8888, "", READWRITE);
    ChunkServerState csState;
    csState.SetDiskState(DISKNORMAL);
    csState.SetOnlineState(ONLINE);
    chunkserver.SetChunkServerState(csState);

    EXPECT_CALL(*topo_, GetChunkServer(csId, _))
        .WillOnce(DoAll(SetArgPointee<1>(chunkserver),
            Return(false)));


    int ret = client_->DeleteChunkSnapshot(
        csId, logicalPoolId, copysetId, chunkId, sn);
    ASSERT_EQ(kMdsFail, ret);
}

TEST_F(TestChunkServerClient, TestDeleteChunkSnapshotChunkServerOFFLINE) {
    MockChunkService chunkService;
    ASSERT_EQ(server_->AddService(&chunkService,
                                  brpc::SERVER_DOESNT_OWN_SERVICE), 0);
    ASSERT_EQ(server_->Start(listenAddr_.c_str(), nullptr), 0);

    ChunkServerIdType csId = 0x01;
    LogicalPoolID logicalPoolId = 0x11;
    CopysetID copysetId = 0x21;
    ChunkID chunkId = 0x31;
    uint64_t sn = 100;

    ChunkServer chunkserver(
        csId, "", "", 0x101, "127.0.0.1", 8888, "", READWRITE);
    ChunkServerState csState;
    csState.SetDiskState(DISKNORMAL);
    csState.SetOnlineState(OFFLINE);
    chunkserver.SetChunkServerState(csState);

    EXPECT_CALL(*topo_, GetChunkServer(csId, _))
        .WillOnce(DoAll(SetArgPointee<1>(chunkserver),
            Return(true)));

    int ret = client_->DeleteChunkSnapshot(
        csId, logicalPoolId, copysetId, chunkId, sn);
    ASSERT_EQ(kCsClientCSOffline, ret);
}

TEST_F(TestChunkServerClient, TestDeleteChunkSnapshotRpcChannelInitFail) {
    MockChunkService chunkService;
    ASSERT_EQ(server_->AddService(&chunkService,
                                  brpc::SERVER_DOESNT_OWN_SERVICE), 0);
    ASSERT_EQ(server_->Start(listenAddr_.c_str(), nullptr), 0);

    ChunkServerIdType csId = 0x01;
    LogicalPoolID logicalPoolId = 0x11;
    CopysetID copysetId = 0x21;
    ChunkID chunkId = 0x31;
    uint64_t sn = 100;

    ChunkServer chunkserver(
        csId, "", "", 0x101, "", 8888, "", READWRITE);
    ChunkServerState csState;
    csState.SetDiskState(DISKNORMAL);
    csState.SetOnlineState(ONLINE);
    chunkserver.SetChunkServerState(csState);

    EXPECT_CALL(*topo_, GetChunkServer(csId, _))
        .WillOnce(DoAll(SetArgPointee<1>(chunkserver),
            Return(true)));

    int ret = client_->DeleteChunkSnapshot(
        csId, logicalPoolId, copysetId, chunkId, sn);
    ASSERT_EQ(kRpcChannelInitFail, ret);
}

TEST_F(TestChunkServerClient, TestDeleteChunkSnapshotRpcCntlFail) {
    MockChunkService chunkService;
    ASSERT_EQ(server_->AddService(&chunkService,
                                  brpc::SERVER_DOESNT_OWN_SERVICE), 0);
    ASSERT_EQ(server_->Start(listenAddr_.c_str(), nullptr), 0);

    ChunkServerIdType csId = 0x01;
    LogicalPoolID logicalPoolId = 0x11;
    CopysetID copysetId = 0x21;
    ChunkID chunkId = 0x31;
    uint64_t sn = 100;

    ChunkServer chunkserver(
        csId, "", "", 0x101, "127.0.0.1", 8888, "", READWRITE);
    ChunkServerState csState;
    csState.SetDiskState(DISKNORMAL);
    csState.SetOnlineState(ONLINE);
    chunkserver.SetChunkServerState(csState);

    EXPECT_CALL(*topo_, GetChunkServer(csId, _))
        .WillOnce(DoAll(SetArgPointee<1>(chunkserver),
            Return(true)));
    ChunkResponse response;
    response.set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS);
    EXPECT_CALL(chunkService, DeleteChunkSnapshot(_, _, _, _))
        .Times(kRpcRetryTime)
        .WillRepeatedly(DoAll(SetArgPointee<2>(response),
                Invoke([](RpcController *controller,
                          const ChunkRequest *request,
                          ChunkResponse *response,
                          Closure *done){
                          brpc::ClosureGuard doneGuard(done);
                          std::this_thread::sleep_for(
                                std::chrono::milliseconds(kRpcTimeoutMs + 1));
                    })));

    int ret = client_->DeleteChunkSnapshot(
        csId, logicalPoolId, copysetId, chunkId, sn);
    ASSERT_EQ(kRpcFail, ret);
}

TEST_F(TestChunkServerClient, TestDeleteChunkSnapshotRpcReturnFail) {
    MockChunkService chunkService;
    ASSERT_EQ(server_->AddService(&chunkService,
                                  brpc::SERVER_DOESNT_OWN_SERVICE), 0);
    ASSERT_EQ(server_->Start(listenAddr_.c_str(), nullptr), 0);

    ChunkServerIdType csId = 0x01;
    LogicalPoolID logicalPoolId = 0x11;
    CopysetID copysetId = 0x21;
    ChunkID chunkId = 0x31;
    uint64_t sn = 100;

    ChunkServer chunkserver(
        csId, "", "", 0x101, "127.0.0.1", 8888, "", READWRITE);
    ChunkServerState csState;
    csState.SetDiskState(DISKNORMAL);
    csState.SetOnlineState(ONLINE);
    chunkserver.SetChunkServerState(csState);

    EXPECT_CALL(*topo_, GetChunkServer(csId, _))
        .WillOnce(DoAll(SetArgPointee<1>(chunkserver),
            Return(true)));
    ChunkResponse response;
    response.set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_FAILURE_UNKNOWN);
    EXPECT_CALL(chunkService, DeleteChunkSnapshot(_, _, _, _))
        .WillOnce(DoAll(SetArgPointee<2>(response),
                Invoke([](RpcController *controller,
                          const ChunkRequest *request,
                          ChunkResponse *response,
                          Closure *done){
                          brpc::ClosureGuard doneGuard(done);
                    })));

    int ret = client_->DeleteChunkSnapshot(
        csId, logicalPoolId, copysetId, chunkId, sn);
    ASSERT_EQ(kCsClientReturnFail, ret);
}

TEST_F(TestChunkServerClient, TestDeleteChunkSnapshotReturnNotLeader) {
    MockChunkService chunkService;
    ASSERT_EQ(server_->AddService(&chunkService,
                                  brpc::SERVER_DOESNT_OWN_SERVICE), 0);
    ASSERT_EQ(server_->Start(listenAddr_.c_str(), nullptr), 0);

    ChunkServerIdType csId = 0x01;
    LogicalPoolID logicalPoolId = 0x11;
    CopysetID copysetId = 0x21;
    ChunkID chunkId = 0x31;
    uint64_t sn = 100;

    ChunkServer chunkserver(
        csId, "", "", 0x101, "127.0.0.1", 8888, "", READWRITE);
    ChunkServerState csState;
    csState.SetDiskState(DISKNORMAL);
    csState.SetOnlineState(ONLINE);
    chunkserver.SetChunkServerState(csState);

    EXPECT_CALL(*topo_, GetChunkServer(csId, _))
        .WillOnce(DoAll(SetArgPointee<1>(chunkserver),
            Return(true)));
    ChunkResponse response;
    response.set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_REDIRECTED);
    EXPECT_CALL(chunkService, DeleteChunkSnapshot(_, _, _, _))
        .WillOnce(DoAll(SetArgPointee<2>(response),
                Invoke([](RpcController *controller,
                          const ChunkRequest *request,
                          ChunkResponse *response,
                          Closure *done){
                          brpc::ClosureGuard doneGuard(done);
                    })));

    int ret = client_->DeleteChunkSnapshot(
        csId, logicalPoolId, copysetId, chunkId, sn);
    ASSERT_EQ(kCsClientNotLeader, ret);
}

TEST_F(TestChunkServerClient, TestGetLeaderSuccess) {
    MockCliService mockCliService;
    ASSERT_EQ(server_->AddService(&mockCliService,
                                  brpc::SERVER_DOESNT_OWN_SERVICE), 0);
    ASSERT_EQ(server_->Start(listenAddr_.c_str(), nullptr), 0);

    ChunkServerIdType csId = 0x01;
    LogicalPoolID logicalPoolId = 0x11;
    CopysetID copysetId = 0x21;
    ChunkServerIdType leader;

    ChunkServer chunkserver(
        csId, "", "", 0x101, "127.0.0.1", 8888, "", READWRITE);
    ChunkServerState csState;
    csState.SetDiskState(DISKNORMAL);
    csState.SetOnlineState(ONLINE);
    chunkserver.SetChunkServerState(csState);

    EXPECT_CALL(*topo_, GetChunkServer(csId, _))
        .WillOnce(DoAll(SetArgPointee<1>(chunkserver),
            Return(true)));

    std::string leaderIp = "127.0.0.2";
    uint32_t leaderPort = 8888;
    std::string leaderPeer = leaderIp + ":" + std::to_string(leaderPort) + ":0";
    ChunkServerIdType leaderReturn = 0x02;
    GetLeaderResponse response;
    response.set_leader_id(leaderPeer);
    EXPECT_CALL(mockCliService, get_leader(_, _, _, _))
        .WillOnce(DoAll(SetArgPointee<2>(response),
                Invoke([](RpcController *controller,
                          const GetLeaderRequest *request,
                          GetLeaderResponse *response,
                          Closure *done){
                          brpc::ClosureGuard doneGuard(done);
                    })));
    EXPECT_CALL(*topo_, FindChunkServer(leaderIp, leaderPort))
        .WillOnce(Return(leaderReturn));

    int ret = client_->GetLeader(
        csId, logicalPoolId, copysetId, &leader);
    ASSERT_EQ(kMdsSuccess, ret);
    ASSERT_EQ(leaderReturn, leader);
}

TEST_F(TestChunkServerClient, TestGetLeaderGetChunkServerFail) {
    MockCliService mockCliService;
    ASSERT_EQ(server_->AddService(&mockCliService,
                                  brpc::SERVER_DOESNT_OWN_SERVICE), 0);
    ASSERT_EQ(server_->Start(listenAddr_.c_str(), nullptr), 0);

    ChunkServerIdType csId = 0x01;
    LogicalPoolID logicalPoolId = 0x11;
    CopysetID copysetId = 0x21;
    ChunkServerIdType leader;

    ChunkServer chunkserver(
        csId, "", "", 0x101, "127.0.0.1", 8888, "", READWRITE);
    ChunkServerState csState;
    csState.SetDiskState(DISKNORMAL);
    csState.SetOnlineState(ONLINE);
    chunkserver.SetChunkServerState(csState);

    EXPECT_CALL(*topo_, GetChunkServer(csId, _))
        .WillOnce(DoAll(SetArgPointee<1>(chunkserver),
            Return(false)));

    int ret = client_->GetLeader(
        csId, logicalPoolId, copysetId, &leader);
    ASSERT_EQ(kMdsFail, ret);
}

TEST_F(TestChunkServerClient, TestGetLeaderChunkServerOFFLINE) {
    MockCliService mockCliService;
    ASSERT_EQ(server_->AddService(&mockCliService,
                                  brpc::SERVER_DOESNT_OWN_SERVICE), 0);
    ASSERT_EQ(server_->Start(listenAddr_.c_str(), nullptr), 0);

    ChunkServerIdType csId = 0x01;
    LogicalPoolID logicalPoolId = 0x11;
    CopysetID copysetId = 0x21;
    ChunkServerIdType leader;

    ChunkServer chunkserver(
        csId, "", "", 0x101, "127.0.0.1", 8888, "", READWRITE);
    ChunkServerState csState;
    csState.SetDiskState(DISKNORMAL);
    csState.SetOnlineState(OFFLINE);
    chunkserver.SetChunkServerState(csState);

    EXPECT_CALL(*topo_, GetChunkServer(csId, _))
        .WillOnce(DoAll(SetArgPointee<1>(chunkserver),
            Return(true)));

    int ret = client_->GetLeader(
        csId, logicalPoolId, copysetId, &leader);
    ASSERT_EQ(kCsClientCSOffline, ret);
}

TEST_F(TestChunkServerClient, TestGetLeaderRpcChannelInitFail) {
    MockCliService mockCliService;
    ASSERT_EQ(server_->AddService(&mockCliService,
                                  brpc::SERVER_DOESNT_OWN_SERVICE), 0);
    ASSERT_EQ(server_->Start(listenAddr_.c_str(), nullptr), 0);

    ChunkServerIdType csId = 0x01;
    LogicalPoolID logicalPoolId = 0x11;
    CopysetID copysetId = 0x21;
    ChunkServerIdType leader;

    ChunkServer chunkserver(
        csId, "", "", 0x101, "", 8888, "", READWRITE);
    ChunkServerState csState;
    csState.SetDiskState(DISKNORMAL);
    csState.SetOnlineState(ONLINE);
    chunkserver.SetChunkServerState(csState);

    EXPECT_CALL(*topo_, GetChunkServer(csId, _))
        .WillOnce(DoAll(SetArgPointee<1>(chunkserver),
            Return(true)));

    int ret = client_->GetLeader(
        csId, logicalPoolId, copysetId, &leader);
    ASSERT_EQ(kRpcChannelInitFail, ret);
}

TEST_F(TestChunkServerClient, TestGetLeaderRpcCntlFail) {
    MockCliService mockCliService;
    ASSERT_EQ(server_->AddService(&mockCliService,
                                  brpc::SERVER_DOESNT_OWN_SERVICE), 0);
    ASSERT_EQ(server_->Start(listenAddr_.c_str(), nullptr), 0);

    ChunkServerIdType csId = 0x01;
    LogicalPoolID logicalPoolId = 0x11;
    CopysetID copysetId = 0x21;
    ChunkServerIdType leader;

    ChunkServer chunkserver(
        csId, "", "", 0x101, "127.0.0.1", 8888, "", READWRITE);
    ChunkServerState csState;
    csState.SetDiskState(DISKNORMAL);
    csState.SetOnlineState(ONLINE);
    chunkserver.SetChunkServerState(csState);

    EXPECT_CALL(*topo_, GetChunkServer(csId, _))
        .WillOnce(DoAll(SetArgPointee<1>(chunkserver),
            Return(true)));

    std::string leaderIp = "127.0.0.2";
    uint32_t leaderPort = 8888;
    std::string leaderPeer = leaderIp + ":" + std::to_string(leaderPort) + ":0";
    ChunkServerIdType leaderReturn = 0x02;
    GetLeaderResponse response;
    response.set_leader_id(leaderPeer);
    EXPECT_CALL(mockCliService, get_leader(_, _, _, _))
        .Times(kRpcRetryTime)
        .WillRepeatedly(DoAll(SetArgPointee<2>(response),
                Invoke([](RpcController *controller,
                          const GetLeaderRequest *request,
                          GetLeaderResponse *response,
                          Closure *done){
                          brpc::ClosureGuard doneGuard(done);
                          std::this_thread::sleep_for(
                                  std::chrono::milliseconds(kRpcTimeoutMs + 1));
                    })));

    int ret = client_->GetLeader(
        csId, logicalPoolId, copysetId, &leader);
    ASSERT_EQ(kRpcFail, ret);
}

TEST_F(TestChunkServerClient, TestGetLeaderRpcReturnLeaderPeerInvalid) {
    MockCliService mockCliService;
    ASSERT_EQ(server_->AddService(&mockCliService,
                                  brpc::SERVER_DOESNT_OWN_SERVICE), 0);
    ASSERT_EQ(server_->Start(listenAddr_.c_str(), nullptr), 0);

    ChunkServerIdType csId = 0x01;
    LogicalPoolID logicalPoolId = 0x11;
    CopysetID copysetId = 0x21;
    ChunkServerIdType leader;

    ChunkServer chunkserver(
        csId, "", "", 0x101, "127.0.0.1", 8888, "", READWRITE);
    ChunkServerState csState;
    csState.SetDiskState(DISKNORMAL);
    csState.SetOnlineState(ONLINE);
    chunkserver.SetChunkServerState(csState);

    EXPECT_CALL(*topo_, GetChunkServer(csId, _))
        .WillOnce(DoAll(SetArgPointee<1>(chunkserver),
            Return(true)));

    std::string leaderPeer = "abcde";
    GetLeaderResponse response;
    response.set_leader_id(leaderPeer);
    EXPECT_CALL(mockCliService, get_leader(_, _, _, _))
        .WillOnce(DoAll(SetArgPointee<2>(response),
                Invoke([](RpcController *controller,
                          const GetLeaderRequest *request,
                          GetLeaderResponse *response,
                          Closure *done){
                          brpc::ClosureGuard doneGuard(done);
                    })));

    int ret = client_->GetLeader(
        csId, logicalPoolId, copysetId, &leader);
    ASSERT_EQ(kMdsFail, ret);
}

TEST_F(TestChunkServerClient, TestGetLeaderRpcReturnLeaderPeerNotExist) {
    MockCliService mockCliService;
    ASSERT_EQ(server_->AddService(&mockCliService,
                                  brpc::SERVER_DOESNT_OWN_SERVICE), 0);
    ASSERT_EQ(server_->Start(listenAddr_.c_str(), nullptr), 0);

    ChunkServerIdType csId = 0x01;
    LogicalPoolID logicalPoolId = 0x11;
    CopysetID copysetId = 0x21;
    ChunkServerIdType leader;

    ChunkServer chunkserver(
        csId, "", "", 0x101, "127.0.0.1", 8888, "", READWRITE);
    ChunkServerState csState;
    csState.SetDiskState(DISKNORMAL);
    csState.SetOnlineState(ONLINE);
    chunkserver.SetChunkServerState(csState);

    EXPECT_CALL(*topo_, GetChunkServer(csId, _))
        .WillOnce(DoAll(SetArgPointee<1>(chunkserver),
            Return(true)));

    std::string leaderIp = "127.0.0.2";
    uint32_t leaderPort = 8888;
    std::string leaderPeer = leaderIp + ":" + std::to_string(leaderPort) + ":0";
    ChunkServerIdType leaderReturn = 0x02;
    GetLeaderResponse response;
    response.set_leader_id(leaderPeer);
    EXPECT_CALL(mockCliService, get_leader(_, _, _, _))
        .WillOnce(DoAll(SetArgPointee<2>(response),
                Invoke([](RpcController *controller,
                          const GetLeaderRequest *request,
                          GetLeaderResponse *response,
                          Closure *done){
                          brpc::ClosureGuard doneGuard(done);
                    })));
    EXPECT_CALL(*topo_, FindChunkServer(leaderIp, leaderPort))
        .WillOnce(Return(UNINTIALIZE_ID));

    int ret = client_->GetLeader(
        csId, logicalPoolId, copysetId, &leader);
    ASSERT_EQ(kMdsFail, ret);
}

}  // namespace chunkserverclient
}  // namespace mds
}  // namespace curve

