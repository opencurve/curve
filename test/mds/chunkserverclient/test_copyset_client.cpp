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
 * Created Date: Fri Mar 15 2019
 * Author: xuchaojie
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
#include "test/mds/mock/mock_topology.h"
#include "test/mds/mock/mock_chunkserver.h"
#include "test/mds/chunkserverclient/mock_chunkserverclient.h"
#include "src/mds/chunkserverclient/chunkserverclient_config.h"


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
        ChunkServerClientOption option;
        auto channelPool = std::make_shared<ChannelPool>();
        client_ = std::make_shared<CopysetClient>(topo_, option, channelPool);
        mockCsClient_ = std::make_shared<MockChunkServerClient>(topo_,
                                                        option, channelPool);
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

TEST_F(TestCopysetClient, TestDeleteChunkSnapshotOrCorrectSnSuccess) {
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


    EXPECT_CALL(*mockCsClient_, DeleteChunkSnapshotOrCorrectSn(
            leader, logicalPoolId, copysetId, chunkId, sn))
        .WillOnce(Return(kMdsSuccess));

    int ret = client_->DeleteChunkSnapshotOrCorrectSn(
        logicalPoolId, copysetId, chunkId, sn);
    ASSERT_EQ(kMdsSuccess, ret);
}

TEST_F(TestCopysetClient, TestDeleteChunkSnapshotOrCorrectSnRedirectSuccess) {
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

    EXPECT_CALL(*mockCsClient_, DeleteChunkSnapshotOrCorrectSn(
            _, logicalPoolId, copysetId, chunkId, sn))
        .WillOnce(Return(kCsClientNotLeader))
        .WillOnce(Return(kMdsSuccess));

    ChunkServerIdType newLeader = 0x02;
    EXPECT_CALL(*mockCsClient_, GetLeader(
        _, logicalPoolId, copysetId, _))
        .WillOnce(DoAll(SetArgPointee<3>(newLeader),
                Return(kMdsSuccess)));

    int ret = client_->DeleteChunkSnapshotOrCorrectSn(
        logicalPoolId, copysetId, chunkId, sn);
    ASSERT_EQ(kMdsSuccess, ret);
}

TEST_F(TestCopysetClient, TestDeleteChunkSnapshotOrCorrectSnRetryTimeOut) {
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

    EXPECT_CALL(*mockCsClient_, DeleteChunkSnapshotOrCorrectSn(
            _, logicalPoolId, copysetId, chunkId, sn))
        .WillRepeatedly(Return(kCsClientNotLeader));

    EXPECT_CALL(*mockCsClient_, GetLeader(
        _, logicalPoolId, copysetId, _))
        .WillRepeatedly(DoAll(SetArgPointee<3>(0x02), Return(kMdsSuccess)));

    int ret = client_->DeleteChunkSnapshotOrCorrectSn(
        logicalPoolId, copysetId, chunkId, sn);
    ASSERT_EQ(kCsClientNotLeader, ret);
}

TEST_F(TestCopysetClient, TestDeleteChunkSnapshotOrCorrectSnFail) {
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

    EXPECT_CALL(*mockCsClient_, DeleteChunkSnapshotOrCorrectSn(
            _, logicalPoolId, copysetId, chunkId, sn))
        .WillRepeatedly(Return(kMdsFail));

    int ret = client_->DeleteChunkSnapshotOrCorrectSn(
        logicalPoolId, copysetId, chunkId, sn);
    ASSERT_EQ(kMdsFail, ret);
}

TEST_F(TestCopysetClient, TestDeleteChunkSnapshotOrCorrectSnGetCopysetFail) {
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

    int ret = client_->DeleteChunkSnapshotOrCorrectSn(
        logicalPoolId, copysetId, chunkId, sn);
    ASSERT_EQ(kMdsFail, ret);
}

TEST_F(TestCopysetClient, TestDeleteChunkSnapshotOrCorrectSnRedirectFail) {
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

    EXPECT_CALL(*mockCsClient_, DeleteChunkSnapshotOrCorrectSn(
            _, logicalPoolId, copysetId, chunkId, sn))
        .WillOnce(Return(kCsClientNotLeader));

    ChunkServerIdType newLeader = 0x02;
    EXPECT_CALL(*mockCsClient_, GetLeader(
        _, logicalPoolId, copysetId, _))
        .WillRepeatedly(DoAll(SetArgPointee<3>(newLeader),
                Return(kMdsFail)));

    int ret = client_->DeleteChunkSnapshotOrCorrectSn(
        logicalPoolId, copysetId, chunkId, sn);
    ASSERT_EQ(kMdsFail, ret);
}

TEST_F(TestCopysetClient, TestDeleteChunkSuccess) {
    uint64_t fileId = 1;
    uint64_t originFileId = 0;
    uint64_t chunkIndex = 1;
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


    EXPECT_CALL(*mockCsClient_, DeleteChunk(
            leader, fileId, originFileId, chunkIndex, 1,
            logicalPoolId, copysetId, chunkId, sn))
        .WillOnce(Return(kMdsSuccess));

    int ret = client_->DeleteChunk(
        fileId, originFileId, chunkIndex, 1,
        logicalPoolId, copysetId, chunkId, sn);
    ASSERT_EQ(kMdsSuccess, ret);
}

TEST_F(TestCopysetClient, TestDeleteChunkRedirectSuccess) {
    uint64_t fileId = 1;
    uint64_t originFileId = 0;
    uint64_t chunkIndex = 1;
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

    EXPECT_CALL(*mockCsClient_, DeleteChunk(
            _, fileId, originFileId, chunkIndex, 1,
            logicalPoolId, copysetId, chunkId, sn))
        .WillOnce(Return(kCsClientNotLeader))
        .WillOnce(Return(kMdsSuccess));

    ChunkServerIdType newLeader = 0x02;
    EXPECT_CALL(*mockCsClient_, GetLeader(
        _, logicalPoolId, copysetId, _))
        .WillOnce(DoAll(SetArgPointee<3>(newLeader),
                Return(kMdsSuccess)));

    int ret = client_->DeleteChunk(
        fileId, originFileId, chunkIndex, 1,
        logicalPoolId, copysetId, chunkId, sn);
    ASSERT_EQ(kMdsSuccess, ret);
}

TEST_F(TestCopysetClient, TestDeleteChunkRetryTimeOut) {
    uint64_t fileId = 1;
    uint64_t originFileId = 0;
    uint64_t chunkIndex = 1;
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

    EXPECT_CALL(*mockCsClient_, DeleteChunk(
            _, fileId, originFileId, chunkIndex, 1,
            logicalPoolId, copysetId, chunkId, sn))
        .WillRepeatedly(Return(kCsClientNotLeader));

    EXPECT_CALL(*mockCsClient_, GetLeader(
        _, logicalPoolId, copysetId, _))
        .WillRepeatedly(DoAll(SetArgPointee<3>(0x02), Return(kMdsSuccess)));

    int ret = client_->DeleteChunk(
        fileId, originFileId, chunkIndex, 1,
        logicalPoolId, copysetId, chunkId, sn);
    ASSERT_EQ(kCsClientNotLeader, ret);
}

TEST_F(TestCopysetClient, TestDeleteChunkFail) {
    uint64_t fileId = 1;
    uint64_t originFileId = 0;
    uint64_t chunkIndex = 1;
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

    EXPECT_CALL(*mockCsClient_, DeleteChunk(
            _, fileId, originFileId, chunkIndex, 1,
            logicalPoolId, copysetId, chunkId, sn))
        .WillRepeatedly(Return(kMdsFail));

    int ret = client_->DeleteChunk(
        fileId, originFileId, chunkIndex, 1,
        logicalPoolId, copysetId, chunkId, sn);
    ASSERT_EQ(kMdsFail, ret);
}

TEST_F(TestCopysetClient, TestDeleteChunkGetCopysetFail) {
    uint64_t fileId = 1;
    uint64_t originFileId = 0;
    uint64_t chunkIndex = 1;
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

    int ret = client_->DeleteChunk(
        fileId, originFileId, chunkIndex, 1,
        logicalPoolId, copysetId, chunkId, sn);
    ASSERT_EQ(kMdsFail, ret);
}

TEST_F(TestCopysetClient, TestDeleteChunkRedirectFail) {
    uint64_t fileId = 1;
    uint64_t originFileId = 0;
    uint64_t chunkIndex = 1;
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

    EXPECT_CALL(*mockCsClient_, DeleteChunk(
            _, fileId, originFileId, chunkIndex, 1,
            logicalPoolId, copysetId, chunkId, sn))
        .WillOnce(Return(kCsClientNotLeader));

    ChunkServerIdType newLeader = 0x02;
    EXPECT_CALL(*mockCsClient_, GetLeader(
        _, logicalPoolId, copysetId, _))
        .WillRepeatedly(DoAll(SetArgPointee<3>(newLeader),
                Return(kMdsFail)));

    int ret = client_->DeleteChunk(
        fileId, originFileId, chunkIndex, 1,
        logicalPoolId, copysetId, chunkId, sn);
    ASSERT_EQ(kMdsFail, ret);
}
}  // namespace chunkserverclient
}  // namespace mds
}  // namespace curve




