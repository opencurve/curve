/*
 * Project: curve
 * Created Date: Fri Mar 15 2019
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
#include "src/mds/chunkserverclient/copyset_client.h"
#include "test/mds/chunkserverclient/mock_topology.h"
#include "test/mds/chunkserverclient/mock_chunkserver.h"
#include "test/mds/chunkserverclient/mock_chunkserverclient.h"


using ::curve::mds::topology::READWRITE;
using ::curve::mds::topology::MockTopology;
using ::curve::mds::topology::UNINTIALIZE_ID;

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

class TestCopysetClient : public ::testing::Test {
 protected:
    TestCopysetClient() {}
    void SetUp() {
        listenAddr_ = "127.0.0.1:8888";
        server_ = new brpc::Server();
        topo_ = std::make_shared<MockTopology>();
        client_ = std::make_shared<CopysetClient>(topo_);
        mockCsClient_ = std::make_shared<MockChunkServerClient>(topo_);
        client_->SetChunkServerClient(mockCsClient_);
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
    std::shared_ptr<MockChunkServerClient> mockCsClient_;
    std::shared_ptr<CopysetClient> client_;
    std::string listenAddr_;
    brpc::Server *server_;
};

TEST_F(TestCopysetClient, TestDeleteSnapShotChunkSuccess) {
    ChunkServerIdType leader = 0x01;
    LogicalPoolID logicalPoolId = 0x11;
    CopysetID copysetId = 0x21;
    ChunkID chunkId = 0x31;
    uint64_t sn = 100;

    CopySetInfo copyset(logicalPoolId, copysetId);
    copyset.SetLeader(leader);
    copyset.SetCopySetMembers({0x01, 0x02, 0x03});
    EXPECT_CALL(*topo_, GetCopySet(_, _))
        .WillOnce(DoAll(SetArgPointee<1>(copyset),
            Return(true)));


    EXPECT_CALL(*mockCsClient_, DeleteChunkSnapshot(
            leader, logicalPoolId, copysetId, chunkId, sn))
        .WillOnce(Return(kMdsSuccess));

    int ret = client_->DeleteSnapShotChunk(
        logicalPoolId, copysetId, chunkId, sn);
    ASSERT_EQ(kMdsSuccess, ret);
}

TEST_F(TestCopysetClient, TestDeleteSnapShotChunkRedirectSuccess) {
    ChunkServerIdType leader = 0x01;
    LogicalPoolID logicalPoolId = 0x11;
    CopysetID copysetId = 0x21;
    ChunkID chunkId = 0x31;
    uint64_t sn = 100;

    CopySetInfo copyset(logicalPoolId, copysetId);
    copyset.SetLeader(leader);
    copyset.SetCopySetMembers({0x01, 0x02, 0x03});
    EXPECT_CALL(*topo_, GetCopySet(_, _))
        .WillOnce(DoAll(SetArgPointee<1>(copyset),
            Return(true)));

    EXPECT_CALL(*mockCsClient_, DeleteChunkSnapshot(
            _, logicalPoolId, copysetId, chunkId, sn))
        .WillOnce(Return(kCsClientNotLeader))
        .WillOnce(Return(kMdsSuccess));

    ChunkServerIdType newLeader = 0x02;
    EXPECT_CALL(*mockCsClient_, GetLeader(
        _, logicalPoolId, copysetId, _))
        .WillOnce(DoAll(SetArgPointee<3>(newLeader),
                Return(kMdsSuccess)));

    int ret = client_->DeleteSnapShotChunk(
        logicalPoolId, copysetId, chunkId, sn);
    ASSERT_EQ(kMdsSuccess, ret);
}

TEST_F(TestCopysetClient, TestDeleteSnapShotChunkRetryTimeOut) {
    ChunkServerIdType leader = 0x01;
    LogicalPoolID logicalPoolId = 0x11;
    CopysetID copysetId = 0x21;
    ChunkID chunkId = 0x31;
    uint64_t sn = 100;

    CopySetInfo copyset(logicalPoolId, copysetId);
    copyset.SetLeader(leader);
    copyset.SetCopySetMembers({0x01, 0x02, 0x03});
    EXPECT_CALL(*topo_, GetCopySet(_, _))
        .WillOnce(DoAll(SetArgPointee<1>(copyset),
            Return(true)));

    EXPECT_CALL(*mockCsClient_, DeleteChunkSnapshot(
            _, logicalPoolId, copysetId, chunkId, sn))
        .WillRepeatedly(Return(kCsClientNotLeader));

    EXPECT_CALL(*mockCsClient_, GetLeader(
        _, logicalPoolId, copysetId, _))
        .WillRepeatedly(Return(kMdsSuccess));

    int ret = client_->DeleteSnapShotChunk(
        logicalPoolId, copysetId, chunkId, sn);
    ASSERT_EQ(kCsClientNotLeader, ret);
}

TEST_F(TestCopysetClient, TestDeleteSnapShotChunkFail) {
    ChunkServerIdType leader = 0x01;
    LogicalPoolID logicalPoolId = 0x11;
    CopysetID copysetId = 0x21;
    ChunkID chunkId = 0x31;
    uint64_t sn = 100;

    CopySetInfo copyset(logicalPoolId, copysetId);
    copyset.SetLeader(leader);
    copyset.SetCopySetMembers({0x01, 0x02, 0x03});
    EXPECT_CALL(*topo_, GetCopySet(_, _))
        .WillOnce(DoAll(SetArgPointee<1>(copyset),
            Return(true)));

    EXPECT_CALL(*mockCsClient_, DeleteChunkSnapshot(
            _, logicalPoolId, copysetId, chunkId, sn))
        .WillRepeatedly(Return(kMdsFail));

    int ret = client_->DeleteSnapShotChunk(
        logicalPoolId, copysetId, chunkId, sn);
    ASSERT_EQ(kMdsFail, ret);
}

TEST_F(TestCopysetClient, TestDeleteSnapShotChunkGetCopysetFail) {
    ChunkServerIdType leader = 0x01;
    LogicalPoolID logicalPoolId = 0x11;
    CopysetID copysetId = 0x21;
    ChunkID chunkId = 0x31;
    uint64_t sn = 100;

    CopySetInfo copyset(logicalPoolId, copysetId);
    copyset.SetLeader(leader);
    copyset.SetCopySetMembers({0x01, 0x02, 0x03});
    EXPECT_CALL(*topo_, GetCopySet(_, _))
        .WillOnce(DoAll(SetArgPointee<1>(copyset),
            Return(false)));

    int ret = client_->DeleteSnapShotChunk(
        logicalPoolId, copysetId, chunkId, sn);
    ASSERT_EQ(kMdsFail, ret);
}

TEST_F(TestCopysetClient, TestDeleteSnapShotChunkRedirectFail) {
    ChunkServerIdType leader = 0x01;
    LogicalPoolID logicalPoolId = 0x11;
    CopysetID copysetId = 0x21;
    ChunkID chunkId = 0x31;
    uint64_t sn = 100;

    CopySetInfo copyset(logicalPoolId, copysetId);
    copyset.SetLeader(leader);
    copyset.SetCopySetMembers({0x01, 0x02, 0x03});
    EXPECT_CALL(*topo_, GetCopySet(_, _))
        .WillOnce(DoAll(SetArgPointee<1>(copyset),
            Return(true)));

    EXPECT_CALL(*mockCsClient_, DeleteChunkSnapshot(
            _, logicalPoolId, copysetId, chunkId, sn))
        .WillOnce(Return(kCsClientNotLeader));

    ChunkServerIdType newLeader = 0x02;
    EXPECT_CALL(*mockCsClient_, GetLeader(
        _, logicalPoolId, copysetId, _))
        .WillRepeatedly(DoAll(SetArgPointee<3>(newLeader),
                Return(kMdsFail)));

    int ret = client_->DeleteSnapShotChunk(
        logicalPoolId, copysetId, chunkId, sn);
    ASSERT_EQ(kMdsFail, ret);
}


}  // namespace chunkserverclient
}  // namespace mds
}  // namespace curve




