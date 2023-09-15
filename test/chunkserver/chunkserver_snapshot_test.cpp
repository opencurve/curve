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
 * Created Date: 18-12-24
 * Author: wudemiao
 */

#include <brpc/channel.h>
#include <gtest/gtest.h>
#include <butil/at_exit.h>

#include <vector>

#include "test/chunkserver/chunkserver_test_util.h"
#include "src/chunkserver/copyset_node_manager.h"
#include "src/chunkserver/cli.h"
#include "src/fs/fs_common.h"
#include "src/fs/local_filesystem.h"
#include "proto/common.pb.h"
#include "proto/copyset.pb.h"

namespace curve {
namespace chunkserver {

using curve::fs::LocalFileSystem;
using curve::fs::LocalFsFactory;
using curve::fs::FileSystemType;

static constexpr uint32_t kOpRequestAlignSize = 4096;

class ChunkServerSnapshotTest : public testing::Test {
 protected:
    virtual void SetUp() {
        ASSERT_EQ(0, peer1.parse("127.0.0.1:9051:0"));
        ASSERT_EQ(0, peer2.parse("127.0.0.1:9052:0"));
        ASSERT_EQ(0, peer3.parse("127.0.0.1:9053:0"));
        ASSERT_EQ(0, peer4.parse("127.0.0.1:9054:0"));
        Exec(TestCluster::RemoveCopysetDirCmd(peer1).c_str());
        Exec(TestCluster::RemoveCopysetDirCmd(peer2).c_str());
        Exec(TestCluster::RemoveCopysetDirCmd(peer3).c_str());
        Exec(TestCluster::RemoveCopysetDirCmd(peer4).c_str());

        electionTimeoutMs = 3000;
        snapshotIntervalS = 60;
    }
    virtual void TearDown() {
        Exec(TestCluster::RemoveCopysetDirCmd(peer1).c_str());
        Exec(TestCluster::RemoveCopysetDirCmd(peer2).c_str());
        Exec(TestCluster::RemoveCopysetDirCmd(peer3).c_str());
        Exec(TestCluster::RemoveCopysetDirCmd(peer4).c_str());
        /* wait for process exit */
        ::usleep(100*1000);
    }

 public:
    PeerId peer1;
    PeerId peer2;
    PeerId peer3;
    PeerId peer4;
    int electionTimeoutMs;
    int snapshotIntervalS;
};

/**
 * TODO(wudemiao) will further abstract I/O and verification in the later stage
 */

/**
 * Normal I/O verification, write it in first, then read it out for verification
 * @param leaderId Primary ID
 * @param logicPoolId Logical Pool ID
 * @param copysetId Copy Group ID
 * @param chunkId chunk id
 * @param length The length of each IO
 * @param fillCh Characters filled in each IO
 * @param loop The number of times repeatedly initiates IO
 */
static void WriteThenReadVerify(PeerId leaderId,
                                LogicPoolID logicPoolId,
                                CopysetID copysetId,
                                ChunkID chunkId,
                                int length,
                                char fillCh,
                                int loop) {
    brpc::Channel channel;
    uint64_t sn = 1;
    ASSERT_EQ(0, channel.Init(leaderId.addr, NULL));
    ChunkService_Stub stub(&channel);
    for (int i = 0; i < loop; ++i) {
        // write
        {
            brpc::Controller cntl;
            cntl.set_timeout_ms(5000);
            ChunkRequest request;
            ChunkResponse response;
            request.set_optype(CHUNK_OP_TYPE::CHUNK_OP_WRITE);
            request.set_logicpoolid(logicPoolId);
            request.set_copysetid(copysetId);
            request.set_chunkid(chunkId);
            request.set_offset(length*i);
            request.set_size(length);
            request.set_sn(sn);
            cntl.request_attachment().resize(length, fillCh);
            stub.WriteChunk(&cntl, &request, &response, nullptr);
            LOG_IF(INFO, cntl.Failed()) << "error msg: "
                                        << cntl.ErrorCode() << " : "
                                        << cntl.ErrorText();
            ASSERT_FALSE(cntl.Failed());
            ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS,
                      response.status());
        }
        // read
        {
            brpc::Controller cntl;
            cntl.set_timeout_ms(5000);
            ChunkRequest request;
            ChunkResponse response;
            request.set_optype(CHUNK_OP_TYPE::CHUNK_OP_READ);
            request.set_logicpoolid(logicPoolId);
            request.set_copysetid(copysetId);
            request.set_chunkid(chunkId);
            request.set_offset(length*i);
            request.set_size(length);
            request.set_sn(sn);
            stub.ReadChunk(&cntl, &request, &response, nullptr);
            LOG_IF(INFO, cntl.Failed()) << "error msg: "
                                        << cntl.ErrorCode() << " : "
                                        << cntl.ErrorText();
            ASSERT_FALSE(cntl.Failed());
            ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS,
                      response.status());
            std::string expectRead(length, fillCh);
            ASSERT_STREQ(expectRead.c_str(),
                         cntl.response_attachment().to_string().c_str());
        }
    }
}

/**
 * Normal I/O verification, read data verification
 * @param leaderId Primary ID
 * @param logicPoolId Logical Pool ID
 * @param copysetId Copy Group ID
 * @param chunkId chunk id
 * @param length The length of each IO
 * @param fillCh Characters filled in each IO
 * @param loop The number of times repeatedly initiates IO
 */
static void ReadVerify(PeerId leaderId,
                       LogicPoolID logicPoolId,
                       CopysetID copysetId,
                       ChunkID chunkId,
                       int length,
                       char fillCh,
                       int loop) {
    brpc::Channel channel;
    uint64_t sn = 1;
    ASSERT_EQ(0, channel.Init(leaderId.addr, NULL));
    ChunkService_Stub stub(&channel);
    for (int i = 0; i < loop; ++i) {
        brpc::Controller cntl;
        cntl.set_timeout_ms(5000);
        ChunkRequest request;
        ChunkResponse response;
        request.set_optype(CHUNK_OP_TYPE::CHUNK_OP_READ);
        request.set_logicpoolid(logicPoolId);
        request.set_copysetid(copysetId);
        request.set_chunkid(chunkId);
        request.set_offset(length*i);
        request.set_size(length);
        request.set_sn(sn);
        stub.ReadChunk(&cntl, &request, &response, nullptr);
        LOG_IF(INFO, cntl.Failed()) << "error msg: "
                                    << cntl.ErrorCode() << " : "
                                    << cntl.ErrorText();
        ASSERT_FALSE(cntl.Failed());
        ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS,
                  response.status());
        std::string expectRead(length, fillCh);
        ASSERT_STREQ(expectRead.c_str(),
                     cntl.response_attachment().to_string().c_str());
    }
}

/**
 * Abnormal I/O verification to verify if the cluster is in an unavailable state
 * @param leaderId Primary ID
 * @param logicPoolId Logical Pool ID
 * @param copysetId Copy Group ID
 * @param chunkId chunk id
 * @param length The length of each IO
 * @param fillCh Characters filled in each IO
 * @param loop The number of times repeatedly initiates IO
 */
static void ReadVerifyNotAvailable(PeerId leaderId,
                                   LogicPoolID logicPoolId,
                                   CopysetID copysetId,
                                   ChunkID chunkId,
                                   int length,
                                   char fillCh,
                                   int loop) {
    brpc::Channel channel;
    uint64_t sn = 1;
    ASSERT_EQ(0, channel.Init(leaderId.addr, NULL));
    ChunkService_Stub stub(&channel);
    for (int i = 0; i < loop; ++i) {
        // write
        brpc::Controller cntl;
        cntl.set_timeout_ms(5000);
        ChunkRequest request;
        ChunkResponse response;
        request.set_optype(CHUNK_OP_TYPE::CHUNK_OP_READ);
        request.set_logicpoolid(logicPoolId);
        request.set_copysetid(copysetId);
        request.set_chunkid(chunkId);
        request.set_offset(length*i);
        request.set_size(length);
        request.set_sn(sn);
        stub.ReadChunk(&cntl, &request, &response, nullptr);
        LOG_IF(INFO, cntl.Failed()) << "error msg: "
                                    << cntl.ErrorCode() << " : "
                                    << cntl.ErrorText();
        LOG(INFO) << "read: " << CHUNK_OP_STATUS_Name(response.status());
        ASSERT_TRUE(cntl.Failed() ||
            response.status() != CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS);
    }
}

/**
 * Verify if the copyset status meets expectations
 * @param peerId: peer id
 * @param logicPoolID: Logical Pool ID
 * @param copysetId: Copy group ID
 * @param expectResp: Expected copyset status
 */
static void CopysetStatusVerify(PeerId peerId,
                                LogicPoolID logicPoolID,
                                CopysetID copysetId,
                                CopysetStatusResponse *expectResp) {
    brpc::Channel channel;
    ASSERT_EQ(0, channel.Init(peerId.addr, NULL));
    CopysetService_Stub stub(&channel);
    CopysetStatusRequest request;
    CopysetStatusResponse response;
    brpc::Controller cntl;
    cntl.set_timeout_ms(5000);
    request.set_logicpoolid(logicPoolID);
    request.set_copysetid(copysetId);
    Peer *peer = new Peer();
    request.set_allocated_peer(peer);
    peer->set_address(peerId.to_string());
    request.set_queryhash(true);
    stub.GetCopysetStatus(&cntl, &request, &response, nullptr);
    LOG_IF(INFO, cntl.Failed()) << cntl.ErrorText();
    ASSERT_FALSE(cntl.Failed());
    response.clear_state();
    response.clear_peer();
    response.clear_firstindex();
    expectResp->clear_state();
    expectResp->clear_peer();
    expectResp->clear_firstindex();
    ASSERT_STREQ(expectResp->DebugString().c_str(),
                 response.DebugString().c_str());
}

/**
 * Verify if the copyset status of several replicas is consistent
 * @param peerIds: Peers to be verified
 * @param logicPoolID: Logical Pool ID
 * @param copysetId: Copy group ID
 */
static void CopysetStatusVerify(const std::vector<PeerId> &peerIds,
                                LogicPoolID logicPoolID,
                                CopysetID copysetId,
                                uint64_t expectEpoch = 0) {
    std::vector<CopysetStatusResponse> resps;
    for (PeerId peerId : peerIds) {
        LOG(INFO) << "Get " << peerId.to_string() << " copyset status";
        brpc::Channel channel;
        ASSERT_EQ(0, channel.Init(peerId.addr, NULL));
        CopysetService_Stub stub(&channel);
        CopysetStatusRequest request;
        CopysetStatusResponse response;
        brpc::Controller cntl;
        cntl.set_timeout_ms(5000);
        request.set_logicpoolid(logicPoolID);
        request.set_copysetid(copysetId);
        Peer *peer = new Peer();
        request.set_allocated_peer(peer);
        peer->set_address(peerId.to_string());
        request.set_queryhash(true);
        stub.GetCopysetStatus(&cntl, &request, &response, nullptr);
        LOG_IF(INFO, cntl.Failed()) << cntl.ErrorText();
        ASSERT_FALSE(cntl.Failed());
        LOG(INFO) << peerId.to_string() << "'s status is: \n"
                  << response.DebugString();
        // The states of multiple replicas are different because there are leaders and followers
        response.clear_state();
        response.clear_peer();
        response.clear_firstindex();
        resps.push_back(response);

        if (0 != expectEpoch) {
            ASSERT_GE(response.epoch(), expectEpoch);
        }
    }

    auto len = resps.size();
    if (len >= 2) {
        for (int i = 1; i < len; ++i) {
            LOG(INFO) << "CopysetStatus " << i + 1 << "th compare.";
            ASSERT_STREQ(resps[0].DebugString().c_str(),
                         resps[i].DebugString().c_str());
        }
    }
}

butil::AtExitManager atExitManager;

/**
 * Verify whether the replication group of one node can provide services normally
 * 1. Create a replication group for a replica
 * 2. Wait for the leader to generate, write the data, and then read it out for verification
 */
TEST_F(ChunkServerSnapshotTest, OneNode) {
    LogicPoolID logicPoolId = 2;
    CopysetID copysetId = 100001;
    uint64_t chunkId = 1;
    int length = kOpRequestAlignSize;
    char ch = 'a';
    int loop = 25;

    PeerId leaderId;
    std::vector<PeerId> peers;
    peers.push_back(peer1);
    TestCluster cluster("InitShutdown-cluster", logicPoolId, copysetId, peers);
    cluster.SetElectionTimeoutMs(electionTimeoutMs);
    cluster.SetsnapshotIntervalS(snapshotIntervalS);
    ASSERT_EQ(0, cluster.StartPeer(peer1));
    ASSERT_EQ(0, cluster.WaitLeader(&leaderId));
    ASSERT_STREQ(peer1.to_string().c_str(), leaderId.to_string().c_str());

    WriteThenReadVerify(leaderId,
                        logicPoolId,
                        copysetId,
                        chunkId,
                        length,
                        ch + 1,
                        loop);

    CopysetStatusResponse expectResp;
    // read, write, 1 configuration change
    int64_t commitedIndex = loop + 1;
    expectResp.set_status(COPYSET_OP_STATUS::COPYSET_OP_STATUS_SUCCESS);
    expectResp.set_state(braft::STATE_LEADER);
    Peer *peer = new Peer();
    expectResp.set_allocated_peer(peer);
    peer->set_address(peer1.to_string());
    Peer *leader = new Peer();
    expectResp.set_allocated_leader(leader);
    leader->set_address(peer1.to_string());
    expectResp.set_readonly(false);
    expectResp.set_term(2);
    expectResp.set_committedindex(commitedIndex);
    expectResp.set_knownappliedindex(commitedIndex);
    expectResp.set_pendingindex(0);
    expectResp.set_pendingqueuesize(0);
    expectResp.set_applyingindex(0);
    expectResp.set_firstindex(1);
    expectResp.set_lastindex(commitedIndex);
    expectResp.set_diskindex(commitedIndex);
    expectResp.set_epoch(1);
    expectResp.set_hash("3049021227");
    CopysetStatusVerify(leaderId, logicPoolId, copysetId, &expectResp);
}

/**
 * Verify whether the shutdown of the leader and restart of one node can provide normal service
 * 1. Create a replication group for 1 replica
 * 2. Wait for the leader to generate, write the data, and then read it out for verification
 * 3. Shutdown leader, then pull it up again
 * 4. Wait for the leader to be generated, and then verify the data written before the read
 * 5. Write the data again and read it out for verification
 */
TEST_F(ChunkServerSnapshotTest, OneNodeShutdown) {
    LogicPoolID logicPoolId = 2;
    CopysetID copysetId = 100001;
    uint64_t chunkId = 1;
    int length = kOpRequestAlignSize;
    char ch = 'a';
    int loop = 25;

    PeerId leaderId;
    std::vector<PeerId> peers;
    peers.push_back(peer1);
    TestCluster cluster("InitShutdown-cluster", logicPoolId, copysetId, peers);
    cluster.SetElectionTimeoutMs(electionTimeoutMs);
    cluster.SetsnapshotIntervalS(snapshotIntervalS);
    ASSERT_EQ(0, cluster.StartPeer(peer1));
    ASSERT_EQ(0, cluster.WaitLeader(&leaderId));
    ASSERT_STREQ(peer1.to_string().c_str(), leaderId.to_string().c_str());

    WriteThenReadVerify(leaderId,
                        logicPoolId,
                        copysetId,
                        chunkId,
                        length,
                        ch,
                        loop);

    ASSERT_EQ(0, cluster.ShutdownPeer(peer1));
    // Testing found that the cluster is not available
    ReadVerifyNotAvailable(leaderId,
                           logicPoolId,
                           copysetId,
                           chunkId,
                           length,
                           ch,
                           1);

    ASSERT_EQ(0, cluster.StartPeer(peer1));
    ASSERT_EQ(0, cluster.WaitLeader(&leaderId));
    ASSERT_STREQ(peer1.to_string().c_str(), leaderId.to_string().c_str());

    ReadVerify(leaderId, logicPoolId, copysetId, chunkId, length, ch, loop);
    WriteThenReadVerify(leaderId,
                        logicPoolId,
                        copysetId,
                        chunkId,
                        length,
                        ch + 1,
                        loop);

    CopysetStatusResponse expectResp;
    int64_t commitedIndex = 2 * loop + 2;
    expectResp.set_status(COPYSET_OP_STATUS::COPYSET_OP_STATUS_SUCCESS);
    expectResp.set_state(braft::STATE_LEADER);
    Peer *peer = new Peer();
    expectResp.set_allocated_peer(peer);
    peer->set_address(peer1.to_string());
    Peer *leader = new Peer();
    expectResp.set_allocated_leader(leader);
    leader->set_address(peer1.to_string());
    expectResp.set_readonly(false);
    expectResp.set_term(3);
    expectResp.set_committedindex(commitedIndex);
    expectResp.set_knownappliedindex(commitedIndex);
    expectResp.set_pendingindex(0);
    expectResp.set_pendingqueuesize(0);
    expectResp.set_applyingindex(0);
    expectResp.set_firstindex(1);
    expectResp.set_lastindex(commitedIndex);
    expectResp.set_diskindex(commitedIndex);
    expectResp.set_epoch(2);
    expectResp.set_hash("3049021227");

    CopysetStatusVerify(leaderId, logicPoolId, copysetId, &expectResp);
}

/**
 * Verify whether two nodes can provide services normally
 * 1. Create a replication group of 2 replicas
 * 2. Wait for the leader to generate, write the data, and then read it out for verification
 */
TEST_F(ChunkServerSnapshotTest, TwoNodes) {
    LogicPoolID logicPoolId = 2;
    CopysetID copysetId = 100001;
    uint64_t chunkId = 1;
    int length = kOpRequestAlignSize;
    char ch = 'a';
    int loop = 25;

    std::vector<PeerId> peers;
    peers.push_back(peer1);
    peers.push_back(peer2);

    TestCluster cluster("ThreeNode-cluster", logicPoolId, copysetId, peers);
    cluster.SetElectionTimeoutMs(electionTimeoutMs);
    cluster.SetsnapshotIntervalS(snapshotIntervalS);
    ASSERT_EQ(0, cluster.StartPeer(peer1));
    ASSERT_EQ(0, cluster.StartPeer(peer2));

    PeerId leaderId;
    ASSERT_EQ(0, cluster.WaitLeader(&leaderId));

    WriteThenReadVerify(leaderId,
                        logicPoolId,
                        copysetId,
                        chunkId,
                        length,
                        ch,
                        loop);

    ::usleep(2000 * 1000);
    CopysetStatusVerify(peers, logicPoolId, copysetId, 1);
}

/**
 * Verify whether restarting two nodes after closing non leader nodes can provide normal service
 * 1. Create a replication group of 2 replicas
 * 2. Wait for the leader to generate, write the data, and then read it out for verification
 * 3. Shutdown is not a leader, then pull it up again
 * 4. Wait for the leader to be generated, and then verify the data written before the read
 * 5. Write the data again and read it out for verification
 */
TEST_F(ChunkServerSnapshotTest, TwoNodesShutdownOnePeer) {
    LogicPoolID logicPoolId = 2;
    CopysetID copysetId = 100001;
    uint64_t chunkId = 1;
    int length = kOpRequestAlignSize;
    char ch = 'a';
    int loop = 25;

    std::vector<PeerId> peers;
    peers.push_back(peer1);
    peers.push_back(peer2);

    TestCluster cluster("ThreeNode-cluster", logicPoolId, copysetId, peers);
    cluster.SetElectionTimeoutMs(electionTimeoutMs);
    cluster.SetsnapshotIntervalS(snapshotIntervalS);
    ASSERT_EQ(0, cluster.StartPeer(peer1));
    ASSERT_EQ(0, cluster.StartPeer(peer2));

    PeerId leaderId;
    ASSERT_EQ(0, cluster.WaitLeader(&leaderId));

    // Initiate read/write
    WriteThenReadVerify(leaderId,
                        logicPoolId,
                        copysetId,
                        chunkId,
                        length,
                        ch,
                        loop);

    // Shutdown a non leader peer
    PeerId shutdownPeerid;
    if (0 == ::strcmp(leaderId.to_string().c_str(),
                      peer1.to_string().c_str())) {
        shutdownPeerid = peer2;
    } else {
        shutdownPeerid = peer1;
    }
    ASSERT_EQ(0, cluster.ShutdownPeer(shutdownPeerid));

    // leader lease, can read from local
    ReadVerify(leaderId, logicPoolId, copysetId, chunkId, length, ch, 1);

    ::usleep(2000 * electionTimeoutMs);

    // Testing found that the cluster is not available
    ReadVerifyNotAvailable(leaderId,
                           logicPoolId,
                           copysetId,
                           chunkId,
                           length,
                           ch,
                           1);

    ASSERT_EQ(0, cluster.StartPeer(shutdownPeerid));
    ASSERT_EQ(0, cluster.WaitLeader(&leaderId));

    // Read it out and verify it again
    ReadVerify(leaderId, logicPoolId, copysetId, chunkId, length, ch, loop);
    // Initiate read/write again
    WriteThenReadVerify(leaderId,
                        logicPoolId,
                        copysetId,
                        chunkId,
                        length,
                        ch + 1,
                        loop);

    ::usleep(2000 * 1000);
    CopysetStatusVerify(peers, logicPoolId, copysetId, 1);
}

/**
 * Verify whether the shutdown of the leader and restart of two nodes can provide normal service
 * 1. Create a replication group of 2 replicas
 * 2. Wait for the leader to generate, write the data, and then read it out for verification
 * 3. Shutdown leader, then pull it up again
 * 4. Wait for the leader to be generated, and then verify the data written before the read
 * 5. Write the data again and read it out for verification
 */
TEST_F(ChunkServerSnapshotTest, TwoNodesShutdownLeader) {
    LogicPoolID logicPoolId = 2;
    CopysetID copysetId = 100001;
    uint64_t chunkId = 1;
    int length = kOpRequestAlignSize;
    char ch = 'a';
    int loop = 25;

    std::vector<PeerId> peers;
    peers.push_back(peer1);
    peers.push_back(peer2);

    TestCluster cluster("ThreeNode-cluster", logicPoolId, copysetId, peers);
    cluster.SetElectionTimeoutMs(electionTimeoutMs);
    cluster.SetsnapshotIntervalS(snapshotIntervalS);
    ASSERT_EQ(0, cluster.StartPeer(peer1));
    ASSERT_EQ(0, cluster.StartPeer(peer2));

    PeerId leaderId;
    ASSERT_EQ(0, cluster.WaitLeader(&leaderId));

    // Initiate read/write
    WriteThenReadVerify(leaderId,
                        logicPoolId,
                        copysetId,
                        chunkId,
                        length,
                        ch,
                        loop);

    // shutdown leader
    ASSERT_EQ(0, cluster.ShutdownPeer(leaderId));
    // Testing found that the cluster is not available
    ReadVerifyNotAvailable(leaderId,
                           logicPoolId,
                           copysetId,
                           chunkId,
                           length,
                           ch,
                           1);

    ASSERT_EQ(0, cluster.StartPeer(leaderId));
    ASSERT_EQ(0, cluster.WaitLeader(&leaderId));

    // Read it out and verify it again
    ReadVerify(leaderId, logicPoolId, copysetId, chunkId, length, ch, loop);
    // Initiate read/write again
    WriteThenReadVerify(leaderId,
                        logicPoolId,
                        copysetId,
                        chunkId,
                        length,
                        ch + 1,
                        loop);

    ::usleep(2000 * 1000);
    CopysetStatusVerify(peers, logicPoolId, copysetId, 2);
}

/**
 * Verify whether the three nodes can provide services normally
 * 1. Create a replication group of 3 replicas
 * 2. Wait for the leader to generate, write the data, and then read it out for verification
 */
TEST_F(ChunkServerSnapshotTest, ThreeNodes) {
    LogicPoolID logicPoolId = 2;
    CopysetID copysetId = 100001;
    uint64_t chunkId = 1;
    int length = kOpRequestAlignSize;
    char ch = 'a';
    int loop = 25;

    std::vector<PeerId> peers;
    peers.push_back(peer1);
    peers.push_back(peer2);
    peers.push_back(peer3);

    TestCluster cluster("ThreeNode-cluster", logicPoolId, copysetId, peers);
    cluster.SetElectionTimeoutMs(electionTimeoutMs);
    cluster.SetsnapshotIntervalS(snapshotIntervalS);
    ASSERT_EQ(0, cluster.StartPeer(peer1));
    ASSERT_EQ(0, cluster.StartPeer(peer2));
    ASSERT_EQ(0, cluster.StartPeer(peer3));

    PeerId leaderId;
    ASSERT_EQ(0, cluster.WaitLeader(&leaderId));

    // Initiate read/write again
    WriteThenReadVerify(leaderId,
                        logicPoolId,
                        copysetId,
                        chunkId,
                        length,
                        ch + 1,
                        loop);

    ::usleep(2000 * 1000);
    CopysetStatusVerify(peers, logicPoolId, copysetId, 1);
}

/**
 * Verify whether restarting after closing non leader nodes on three nodes can provide normal service
 * 1. Create a replication group of 3 replicas
 * 2. Wait for the leader to generate, write the data, and then read it out for verification
 * 3. Shutdown is not a leader, then pull it up again
 * 4. Wait for the leader to be generated, and then verify the data written before the read
 * 5. Write the data again and read it out for verification
 */
TEST_F(ChunkServerSnapshotTest, ThreeNodesShutdownOnePeer) {
    LogicPoolID logicPoolId = 2;
    CopysetID copysetId = 100001;
    uint64_t chunkId = 1;
    int length = kOpRequestAlignSize;
    char ch = 'a';
    int loop = 25;

    std::vector<PeerId> peers;
    peers.push_back(peer1);
    peers.push_back(peer2);
    peers.push_back(peer3);

    TestCluster cluster("ThreeNode-cluster", logicPoolId, copysetId, peers);
    cluster.SetElectionTimeoutMs(electionTimeoutMs);
    cluster.SetsnapshotIntervalS(snapshotIntervalS);
    ASSERT_EQ(0, cluster.StartPeer(peer1));
    ASSERT_EQ(0, cluster.StartPeer(peer2));
    ASSERT_EQ(0, cluster.StartPeer(peer3));

    PeerId leaderId;
    ASSERT_EQ(0, cluster.WaitLeader(&leaderId));

    // Initiate read/write
    WriteThenReadVerify(leaderId,
                        logicPoolId,
                        copysetId,
                        chunkId,
                        length,
                        ch,
                        loop);

    // Shutdown a non leader peer
    PeerId shutdownPeerid;
    if (0 == ::strcmp(leaderId.to_string().c_str(),
                      peer1.to_string().c_str())) {
        shutdownPeerid = peer2;
    } else {
        shutdownPeerid = peer1;
    }
    ASSERT_EQ(0, cluster.ShutdownPeer(shutdownPeerid));
    ASSERT_EQ(0, cluster.StartPeer(shutdownPeerid));
    ASSERT_EQ(0, cluster.WaitLeader(&leaderId));

    // Read it out and verify it again
    ReadVerify(leaderId, logicPoolId, copysetId, chunkId, length, ch, loop);
    // Initiate read/write again
    WriteThenReadVerify(leaderId,
                        logicPoolId,
                        copysetId,
                        chunkId,
                        length,
                        ch + 1,
                        loop);

    ::usleep(2000 * 1000);
    CopysetStatusVerify(peers, logicPoolId, copysetId, 1);
}

/**
 * Verify whether the shutdown of the leader node and restart of three nodes can provide normal service
 * 1. Create a replication group of 3 replicas
 * 2. Wait for the leader to generate, write the data, and then read it out for verification
 * 3. Shutdown leader, then pull it up again
 * 4. Wait for the leader to be generated, and then verify the data written before the read
 * 5. Write the data again and read it out for verification
 */
TEST_F(ChunkServerSnapshotTest, ThreeNodesShutdownLeader) {
    LogicPoolID logicPoolId = 2;
    CopysetID copysetId = 100001;
    uint64_t chunkId = 1;
    int length = kOpRequestAlignSize;
    char ch = 'a';
    int loop = 25;

    std::vector<PeerId> peers;
    peers.push_back(peer1);
    peers.push_back(peer2);
    peers.push_back(peer3);

    TestCluster cluster("ThreeNode-cluster", logicPoolId, copysetId, peers);
    cluster.SetElectionTimeoutMs(electionTimeoutMs);
    cluster.SetsnapshotIntervalS(snapshotIntervalS);
    ASSERT_EQ(0, cluster.StartPeer(peer1));
    ASSERT_EQ(0, cluster.StartPeer(peer2));
    ASSERT_EQ(0, cluster.StartPeer(peer3));

    PeerId leaderId;
    ASSERT_EQ(0, cluster.WaitLeader(&leaderId));

    // Initiate read/write
    WriteThenReadVerify(leaderId,
                        logicPoolId,
                        copysetId,
                        chunkId,
                        length,
                        ch,
                        loop);


    // shutdown leader
    ASSERT_EQ(0, cluster.ShutdownPeer(leaderId));
    // Testing found that the cluster is temporarily unavailable
    ReadVerifyNotAvailable(leaderId,
                           logicPoolId,
                           copysetId,
                           chunkId,
                           length,
                           ch,
                           1);

    ASSERT_EQ(0, cluster.StartPeer(leaderId));
    ASSERT_EQ(0, cluster.WaitLeader(&leaderId));

    // Read it out and verify it again
    ReadVerify(leaderId, logicPoolId, copysetId, chunkId, length, ch, loop);


    // Read it out and verify it again
    ReadVerify(leaderId, logicPoolId, copysetId, chunkId, length, ch, loop);
    // Initiate read/write again
    WriteThenReadVerify(leaderId,
                        logicPoolId,
                        copysetId,
                        chunkId,
                        length,
                        ch + 1,
                        loop);

    ::usleep(2000 * 1000);
    CopysetStatusVerify(peers, logicPoolId, copysetId, 2);
}

/**
 * Verify the shutdown of non leader nodes on three nodes, restart, and control the recovery from install snapshot
 * 1. Create a replication group of 3 replicas
 * 2. Wait for the leader to generate, write the data, and then read it out for verification
 * 3. Shutdown non leader
 * 4. Then sleep exceeds one snapshot interval and write read data
 * 5. Then sleep for more than one snapshot interval and write read data; 4,5 two-step
 *    It is to ensure that at least two snapshots are taken, so that when the node restarts again, it must pass the install snapshot,
 *    Because the log has been deleted
 * 6. Wait for the leader to be generated, and then verify the data written before the read
 * 7. transfer leader to shut down peer
 * 8. Verification of data written before read
 * 9. Write the data again and read it out for verification
 */
TEST_F(ChunkServerSnapshotTest, ShutdownOnePeerRestartFromInstallSnapshot) {
    LogicPoolID logicPoolId = 2;
    CopysetID copysetId = 100001;
    uint64_t chunkId = 1;
    int length = kOpRequestAlignSize;
    char ch = 'a';
    int loop = 25;
    int snapshotTimeoutS = 2;

    std::vector<PeerId> peers;
    peers.push_back(peer1);
    peers.push_back(peer2);
    peers.push_back(peer3);

    TestCluster cluster("ThreeNode-cluster", logicPoolId, copysetId, peers);
    cluster.SetElectionTimeoutMs(electionTimeoutMs);
    cluster.SetsnapshotIntervalS(snapshotTimeoutS);
    ASSERT_EQ(0, cluster.StartPeer(peer1));
    ASSERT_EQ(0, cluster.StartPeer(peer2));
    ASSERT_EQ(0, cluster.StartPeer(peer3));

    PeerId leaderId;
    ASSERT_EQ(0, cluster.WaitLeader(&leaderId));

    // Initiate read/write
    WriteThenReadVerify(leaderId,
                        logicPoolId,
                        copysetId,
                        chunkId,
                        length,
                        ch,
                        loop);

    // Shutdown a non leader peer
    PeerId shutdownPeerid;
    if (0 == ::strcmp(leaderId.to_string().c_str(),
                      peer1.to_string().c_str())) {
        shutdownPeerid = peer2;
    } else {
        shutdownPeerid = peer1;
    }
    LOG(INFO) << "shutdown peer: " << shutdownPeerid.to_string();
    LOG(INFO) << "leader peer: " << leaderId.to_string();
    ASSERT_NE(0, ::strcmp(shutdownPeerid.to_string().c_str(),
                          leaderId.to_string().c_str()));
    ASSERT_EQ(0, cluster.ShutdownPeer(shutdownPeerid));

    // Wait snapshot to ensure that the installation snapshot can be triggered
    ::sleep(1.5*snapshotTimeoutS);
    // Initiate read/write again
    WriteThenReadVerify(leaderId,
                        logicPoolId,
                        copysetId,
                        chunkId,
                        length,
                        ch + 1,
                        loop);
    ::sleep(1.5*snapshotTimeoutS);
    // Initiate read/write again
    WriteThenReadVerify(leaderId,
                        logicPoolId,
                        copysetId,
                        chunkId,
                        length,
                        ch + 2,
                        loop);

    // Restart, needs to be restored from install snapshot
    ASSERT_EQ(0, cluster.StartPeer(shutdownPeerid));
    ASSERT_EQ(0, cluster.WaitLeader(&leaderId));

    // Read it out and verify it again
    ReadVerify(leaderId, logicPoolId, copysetId, chunkId, length, ch + 2, loop);
    // Initiate read/write again
    WriteThenReadVerify(leaderId,
                        logicPoolId,
                        copysetId,
                        chunkId,
                        length,
                        ch + 3,
                        loop);

    // Wait shutdown peer recovery, and then transfer leader to it
    ::sleep(3);
    Configuration conf = cluster.CopysetConf();
    braft::cli::CliOptions options;
    options.max_retry = 3;
    options.timeout_ms = 3000;
    const int kMaxLoop = 10;
    butil::Status status;
    for (int i = 0; i < kMaxLoop; ++i) {
        status = TransferLeader(logicPoolId,
                                copysetId,
                                conf,
                                shutdownPeerid,
                                options);
        if (0 == status.error_code()) {
            cluster.WaitLeader(&leaderId);
            if (leaderId == shutdownPeerid) {
                break;
            }
        }
        ::sleep(1);
    }

    ASSERT_EQ(0, ::strcmp(leaderId.to_string().c_str(),
                          shutdownPeerid.to_string().c_str()));

    // Read it out and verify it again
    ReadVerify(leaderId, logicPoolId, copysetId, chunkId, length, ch + 3, loop);
    // Initiate read/write again
    WriteThenReadVerify(leaderId,
                        logicPoolId,
                        copysetId,
                        chunkId,
                        length,
                        ch + 4,
                        loop);

    ::usleep(2000 * 1000);
    CopysetStatusVerify(peers, logicPoolId, copysetId, 1);
}

/**
 * Verify the shutdown of non leader nodes on three nodes, restart, and control the recovery from install snapshot
 * 1. Create a replication group of 3 replicas
 * 2. Wait for the leader to generate, write the data, and then read it out for verification
 * 3. Shutdown non leader
 * 4. Verify the data written before read
 * 5. Write the data again, and then read it out for verification
 * 6. Then sleep exceeds one snapshot interval and write read data
 * 7. Then sleep for more than one snapshot interval and write read data; 4,5 two-step
 *    It is to ensure that at least two snapshots are taken, so that when the node restarts again, it must pass the install snapshot,
 *    Because the log has been deleted
 * 9. Delete the data directory of the shutdown peer and then pull it up again
 * 10. Then verify the data written before read
 * 11. Transfer leader to shut down peer
 * 12. Verification of data written before read
 * 13. Write the data again and read it out for verification
 */
TEST_F(ChunkServerSnapshotTest, ShutdownOnePeerAndRemoveData) {
    LogicPoolID logicPoolId = 2;
    CopysetID copysetId = 100001;
    uint64_t chunkId = 1;
    int length = kOpRequestAlignSize;
    char ch = 'a';
    int loop = 25;
    int snapshotTimeoutS = 2;

    std::vector<PeerId> peers;
    peers.push_back(peer1);
    peers.push_back(peer2);
    peers.push_back(peer3);

    TestCluster cluster("ThreeNode-cluster", logicPoolId, copysetId, peers);
    cluster.SetElectionTimeoutMs(electionTimeoutMs);
    cluster.SetsnapshotIntervalS(snapshotTimeoutS);
    ASSERT_EQ(0, cluster.StartPeer(peer1));
    ASSERT_EQ(0, cluster.StartPeer(peer2));
    ASSERT_EQ(0, cluster.StartPeer(peer3));

    PeerId leaderId;
    ASSERT_EQ(0, cluster.WaitLeader(&leaderId));

    // Initiate read/write
    WriteThenReadVerify(leaderId,
                        logicPoolId,
                        copysetId,
                        chunkId,
                        length,
                        ch,
                        loop);

    // Shutdown a non leader peer
    PeerId shutdownPeerid;
    if (0 == ::strcmp(leaderId.to_string().c_str(),
                      peer1.to_string().c_str())) {
        shutdownPeerid = peer2;
    } else {
        shutdownPeerid = peer1;
    }
    LOG(INFO) << "shutdown peer: " << shutdownPeerid.to_string();
    LOG(INFO) << "leader peer: " << leaderId.to_string();
    ASSERT_NE(0, ::strcmp(shutdownPeerid.to_string().c_str(),
                          leaderId.to_string().c_str()));
    ASSERT_EQ(0, cluster.ShutdownPeer(shutdownPeerid));

    // Read it out and verify it again
    ReadVerify(leaderId, logicPoolId, copysetId, chunkId, length, ch, loop);
    // Initiate read/write again
    WriteThenReadVerify(leaderId,
                        logicPoolId,
                        copysetId,
                        chunkId,
                        length,
                        ch + 1,
                        loop);

    // Wait snapshot to ensure that the installation snapshot can be triggered
    ::sleep(1.5*snapshotTimeoutS);
    // Initiate read/write again
    WriteThenReadVerify(leaderId,
                        logicPoolId,
                        copysetId,
                        chunkId,
                        length,
                        ch + 2,
                        loop);
    ::sleep(1.5*snapshotTimeoutS);
    // Initiate read/write again
    WriteThenReadVerify(leaderId,
                        logicPoolId,
                        copysetId,
                        chunkId,
                        length,
                        ch + 3,
                        loop);

    // Delete the data for this peer and restart it
    ASSERT_EQ(0,
              ::system(TestCluster::RemoveCopysetDirCmd(shutdownPeerid)
                           .c_str()));   //NOLINT
    LOG(INFO) << "remove data cmd: "
              << TestCluster::RemoveCopysetDirCmd(shutdownPeerid);
    std::shared_ptr<LocalFileSystem>
        fs(LocalFsFactory::CreateFs(FileSystemType::EXT4, ""));
    Exec(TestCluster::CopysetDirWithoutProtocol(shutdownPeerid).c_str());
    LOG(INFO) << "remove data dir: "
              << TestCluster::CopysetDirWithoutProtocol(shutdownPeerid);
    ASSERT_FALSE(fs->DirExists(TestCluster::CopysetDirWithoutProtocol(
        shutdownPeerid).c_str()));    //NOLINT
    ASSERT_EQ(0, cluster.StartPeer(shutdownPeerid));
    ASSERT_EQ(0, cluster.WaitLeader(&leaderId));

    // Read it out and verify it again
    ReadVerify(leaderId, logicPoolId, copysetId, chunkId, length, ch + 3, loop);

    // Wait shutdown peer recovery, and then transfer leader to it
    ::sleep(3);
    Configuration conf = cluster.CopysetConf();
    braft::cli::CliOptions options;
    options.max_retry = 3;
    options.timeout_ms = 3000;
    const int kMaxLoop = 10;
    butil::Status status;
    for (int i = 0; i < kMaxLoop; ++i) {
        status = TransferLeader(logicPoolId,
                                copysetId,
                                conf,
                                shutdownPeerid,
                                options);
        if (0 == status.error_code()) {
            cluster.WaitLeader(&leaderId);
            if (leaderId == shutdownPeerid) {
                break;
            }
        }
        ::sleep(1);
    }

    ASSERT_EQ(0, ::strcmp(leaderId.to_string().c_str(),
                          shutdownPeerid.to_string().c_str()));

    // Read it out and verify it again
    ReadVerify(leaderId, logicPoolId, copysetId, chunkId, length, ch + 3, loop);
    // Initiate read/write again
    WriteThenReadVerify(leaderId,
                        logicPoolId,
                        copysetId,
                        chunkId,
                        length,
                        ch + 4,
                        loop);

    ::usleep(2000 * 1000);
    CopysetStatusVerify(peers, logicPoolId, copysetId, 2);
}

/**
 * Verify the shutdown of non leader nodes on three nodes, restart, and control the recovery from install snapshot
 * 1. Create a replication group of 3 replicas
 * 2. Wait for the leader to generate, write the data, and then read it out for verification
 * 3. Shutdown non leader
 * 4. Verify the data written before read
 * 5. Write the data again, and then read it out for verification
 * 6. Then sleep exceeds one snapshot interval and write read data
 * 7. Then sleep for more than one snapshot interval and write read data; 4,5 two-step
 *    It is to ensure that at least two snapshots are taken, so that when the node restarts again, it must pass the install snapshot,
 *    Because the log has been deleted
 * 9. Add peer through configuration changes
 * 10. Then verify the data written before read
 * 11. Initiate write and read again to verify
 * 12. Transfer leader to add's peer
 * 13. Verification of data written before read
 * 14. Write the data again and read it out for verification
 */
TEST_F(ChunkServerSnapshotTest, AddPeerAndRecoverFromInstallSnapshot) {
    LogicPoolID logicPoolId = 2;
    CopysetID copysetId = 100001;
    ChunkID kMaxChunkId = 10;
    int length = kOpRequestAlignSize;
    char ch = 'a';
    int loop = 25;
    int snapshotTimeoutS = 2;

    std::vector<PeerId> peers;
    peers.push_back(peer1);
    peers.push_back(peer2);
    peers.push_back(peer3);

    TestCluster cluster("ThreeNode-cluster", logicPoolId, copysetId, peers);
    cluster.SetElectionTimeoutMs(electionTimeoutMs);
    cluster.SetsnapshotIntervalS(snapshotTimeoutS);
    ASSERT_EQ(0, cluster.StartPeer(peer1));
    ASSERT_EQ(0, cluster.StartPeer(peer2));
    ASSERT_EQ(0, cluster.StartPeer(peer3));

    PeerId leaderId;
    ASSERT_EQ(0, cluster.WaitLeader(&leaderId));

    // Initiate read/write, multiple chunk files
    for (int i = 0; i < kMaxChunkId; ++i) {
        WriteThenReadVerify(leaderId,
                            logicPoolId,
                            copysetId,
                            i,
                            length,
                            ch,
                            loop);
    }

    // Shutdown a non leader peer
    PeerId shutdownPeerid;
    if (0 == ::strcmp(leaderId.to_string().c_str(),
                      peer1.to_string().c_str())) {
        shutdownPeerid = peer2;
    } else {
        shutdownPeerid = peer1;
    }
    LOG(INFO) << "shutdown peer: " << shutdownPeerid.to_string();
    LOG(INFO) << "leader peer: " << leaderId.to_string();
    ASSERT_NE(0, ::strcmp(shutdownPeerid.to_string().c_str(),
                          leaderId.to_string().c_str()));
    ASSERT_EQ(0, cluster.ShutdownPeer(shutdownPeerid));

    // Read it out and verify it again
    for (int i = 0; i < kMaxChunkId; ++i) {
        ReadVerify(leaderId, logicPoolId, copysetId, i, length, ch, loop);
    }
    // Initiate read/write again
    for (int i = 0; i < kMaxChunkId; ++i) {
        WriteThenReadVerify(leaderId,
                            logicPoolId,
                            copysetId,
                            i,
                            length,
                            ch + 1,
                            loop);
    }

    // Wait snapshot to ensure that the installation snapshot can be triggered
    ::sleep(1.5*snapshotTimeoutS);
    // Initiate read/write again
    for (int i = 0; i < kMaxChunkId; ++i) {
        WriteThenReadVerify(leaderId,
                            logicPoolId,
                            copysetId,
                            i,
                            length,
                            ch + 2,
                            loop);
    }
    ::sleep(1.5*snapshotTimeoutS);
    // Initiate read/write again
    for (int i = 0; i < kMaxChunkId; ++i) {
        WriteThenReadVerify(leaderId,
                            logicPoolId,
                            copysetId,
                            i,
                            length,
                            ch + 3,
                            loop);
    }

    // Add a peer
    {
        ASSERT_EQ(0, cluster.StartPeer(peer4, true));
        ASSERT_EQ(0, cluster.WaitLeader(&leaderId));
        Configuration conf = cluster.CopysetConf();
        braft::cli::CliOptions options;
        options.max_retry = 3;
        options.timeout_ms = 80000;
        butil::Status status = AddPeer(logicPoolId,
                                       copysetId,
                                       cluster.CopysetConf(),
                                       peer4,
                                       options);
        ASSERT_EQ(0, status.error_code());
    }

    // Read it out and verify it again
    for (int i = 0; i < kMaxChunkId; ++i) {
        ReadVerify(leaderId, logicPoolId, copysetId, i, length, ch + 3, loop);
    }
    // Initiate read/write again
    for (int i = 0; i < kMaxChunkId; ++i) {
        WriteThenReadVerify(leaderId,
                            logicPoolId,
                            copysetId,
                            i,
                            length,
                            ch + 4,
                            loop);
    }

    // Wait add peer recovery, and then transfer leader to it
    ::sleep(3);
    Configuration conf = cluster.CopysetConf();
    braft::cli::CliOptions options;
    options.max_retry = 3;
    options.timeout_ms = 3000;
    const int kMaxLoop = 10;
    butil::Status status;
    for (int i = 0; i < kMaxLoop; ++i) {
        status = TransferLeader(logicPoolId,
                                copysetId,
                                conf,
                                peer4,
                                options);
        if (0 == status.error_code()) {
            cluster.WaitLeader(&leaderId);
            if (leaderId == peer4) {
                break;
            }
        }
        ::sleep(1);
    }

    ASSERT_EQ(0, ::strcmp(leaderId.to_string().c_str(),
                          peer4.to_string().c_str()));

    // Read it out and verify it again
    for (int i = 0; i < kMaxChunkId; ++i) {
        ReadVerify(leaderId, logicPoolId, copysetId, i, length, ch + 4, loop);
    }
    // Initiate read/write again
    for (int i = 0; i < kMaxChunkId; ++i) {
        WriteThenReadVerify(leaderId,
                            logicPoolId,
                            copysetId,
                            i,
                            length,
                            ch + 5,
                            loop);
    }

    ::usleep(2000 * 1000);
    peers.push_back(peer4);
    std::vector<PeerId> newPeers;
    for (PeerId peerId : peers) {
        if (peerId.to_string() != shutdownPeerid.to_string()) {
            newPeers.push_back(peerId);
        }
    }
    CopysetStatusVerify(newPeers, logicPoolId, copysetId, 3);
}

/**
 * Verify the removal of one node from three nodes, then add it back and control it to recover from the install snapshot
 * 1. Create a replication group of 3 replicas
 * 2. Wait for the leader to generate, write the data, and then read it out for verification
 * 3. Remove a non leader through configuration changes
 * 4. Verify the data written before read
 * 5. Write the data again, and then read it out for verification
 * 6. Then sleep exceeds one snapshot interval and write read data
 * 7. Then sleep for more than one snapshot interval and write read data; 4,5 two-step
 *    It is to ensure that at least two snapshots are taken, so that when the node restarts again, it must pass the install snapshot,
 *    Because the log has been deleted
 * 9. Add the previously removed peer back through configuration changes
 * 10. Transfer leader to this peer
 * 11. Verification of data written before read
 * 12. Write the data again and read it out for verification
 */
TEST_F(ChunkServerSnapshotTest, RemovePeerAndRecoverFromInstallSnapshot) {
    LogicPoolID logicPoolId = 2;
    CopysetID copysetId = 100001;
    ChunkID kMaxChunkId = 10;
    int length = kOpRequestAlignSize;
    char ch = 'a';
    int loop = 25;
    int snapshotTimeoutS = 2;

    std::vector<PeerId> peers;
    peers.push_back(peer1);
    peers.push_back(peer2);
    peers.push_back(peer3);

    TestCluster cluster("ThreeNode-cluster", logicPoolId, copysetId, peers);
    cluster.SetElectionTimeoutMs(electionTimeoutMs);
    cluster.SetsnapshotIntervalS(snapshotTimeoutS);
    ASSERT_EQ(0, cluster.StartPeer(peer1));
    ASSERT_EQ(0, cluster.StartPeer(peer2));
    ASSERT_EQ(0, cluster.StartPeer(peer3));

    PeerId leaderId;
    ASSERT_EQ(0, cluster.WaitLeader(&leaderId));

    // Initiate read/write, multiple chunk files
    for (int i = 0; i < kMaxChunkId; ++i) {
        WriteThenReadVerify(leaderId,
                            logicPoolId,
                            copysetId,
                            i,
                            length,
                            ch,
                            loop);
    }

    // Shutdown a non leader peer
    PeerId removePeerid;
    if (0 == ::strcmp(leaderId.to_string().c_str(),
                      peer1.to_string().c_str())) {
        removePeerid = peer2;
    } else {
        removePeerid = peer1;
    }
    LOG(INFO) << "remove peer: " << removePeerid.to_string();
    LOG(INFO) << "leader peer: " << leaderId.to_string();
    ASSERT_NE(0, ::strcmp(removePeerid.to_string().c_str(),
                          leaderId.to_string().c_str()));
    // Remove a peer
    {
        Configuration conf = cluster.CopysetConf();
        braft::cli::CliOptions options;
        options.max_retry = 3;
        options.timeout_ms = 8000;
        butil::Status status = RemovePeer(logicPoolId,
                                          copysetId,
                                          cluster.CopysetConf(),
                                          removePeerid,
                                          options);
        ASSERT_EQ(0, status.error_code());
    }

    // Read it out and verify it again
    for (int i = 0; i < kMaxChunkId; ++i) {
        ReadVerify(leaderId, logicPoolId, copysetId, i, length, ch, loop);
    }
    // Initiate read/write again
    for (int i = 0; i < kMaxChunkId; ++i) {
        WriteThenReadVerify(leaderId,
                            logicPoolId,
                            copysetId,
                            i,
                            length,
                            ch + 1,
                            loop);
    }

    // Wait snapshot to ensure that the installation snapshot can be triggered
    ::sleep(1.5*snapshotTimeoutS);
    // Initiate read/write again
    for (int i = 0; i < kMaxChunkId; ++i) {
        WriteThenReadVerify(leaderId,
                            logicPoolId,
                            copysetId,
                            i,
                            length,
                            ch + 2,
                            loop);
    }
    ::sleep(1.5*snapshotTimeoutS);
    // Initiate read/write again
    for (int i = 0; i < kMaxChunkId; ++i) {
        WriteThenReadVerify(leaderId,
                            logicPoolId,
                            copysetId,
                            i,
                            length,
                            ch + 3,
                            loop);
    }

    // Add, come back
    {
        Configuration conf = cluster.CopysetConf();
        braft::cli::CliOptions options;
        options.max_retry = 3;
        options.timeout_ms = 80000;
        butil::Status status = AddPeer(logicPoolId,
                                       copysetId,
                                       cluster.CopysetConf(),
                                       removePeerid,
                                       options);
        ASSERT_EQ(0, status.error_code());
    }

    // Wait add peer recovery, and then transfer leader to it
    ::sleep(3);
    Configuration conf = cluster.CopysetConf();
    braft::cli::CliOptions options;
    options.max_retry = 3;
    options.timeout_ms = 3000;
    const int kMaxLoop = 10;
    butil::Status status;
    for (int i = 0; i < kMaxLoop; ++i) {
        status = TransferLeader(logicPoolId,
                                copysetId,
                                conf,
                                removePeerid,
                                options);
        if (0 == status.error_code()) {
            cluster.WaitLeader(&leaderId);
            if (leaderId == removePeerid) {
                break;
            }
        }
        ::sleep(1);
    }

    ASSERT_EQ(0, ::strcmp(leaderId.to_string().c_str(),
                          removePeerid.to_string().c_str()));

    // Read it out and verify it again
    for (int i = 0; i < kMaxChunkId; ++i) {
        ReadVerify(leaderId, logicPoolId, copysetId, i, length, ch + 3, loop);
    }
    // Initiate read/write again
    for (int i = 0; i < kMaxChunkId; ++i) {
        WriteThenReadVerify(leaderId,
                            logicPoolId,
                            copysetId,
                            i,
                            length,
                            ch + 4,
                            loop);
    }

    ::usleep(2000 * 1000);
    CopysetStatusVerify(peers, logicPoolId, copysetId, 2);
}

}  // namespace chunkserver
}  // namespace curve
