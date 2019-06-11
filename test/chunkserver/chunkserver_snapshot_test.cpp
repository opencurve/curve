/*
 * Project: curve
 * Created Date: 18-12-24
 * Author: wudemiao
 * Copyright (c) 2018 netease
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

namespace curve {
namespace chunkserver {

using curve::fs::LocalFileSystem;
using curve::fs::LocalFsFactory;
using curve::fs::FileSystemType;

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
 * TODO(wudemiao) 后期将发 I/O 和验证再抽象一下
 */

/**
 * 正常 I/O 验证，先写进去，再读出来验证
 * @param leaderId      主的 id
 * @param logicPoolId   逻辑池 id
 * @param copysetId 复制组 id
 * @param chunkId   chunk id
 * @param length    每次 IO 的 length
 * @param fillCh    每次 IO 填充的字符
 * @param loop      重复发起 IO 的次数
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
 * 正常 I/O 验证，read 数据验证
 * @param leaderId      主的 id
 * @param logicPoolId   逻辑池 id
 * @param copysetId 复制组 id
 * @param chunkId   chunk id
 * @param length    每次 IO 的 length
 * @param fillCh    每次 IO 填充的字符
 * @param loop      重复发起 IO 的次数
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
 * 异常 I/O 验证，验证集群是否处于不可用状态
 * @param leaderId      主的 id
 * @param logicPoolId   逻辑池 id
 * @param copysetId 复制组 id
 * @param chunkId   chunk id
 * @param length    每次 IO 的 length
 * @param fillCh    每次 IO 填充的字符
 * @param loop      重复发起 IO 的次数
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
        cntl.set_timeout_ms(1000);
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
        ASSERT_TRUE(cntl.Failed());
    }
}

butil::AtExitManager atExitManager;

/**
 * 验证1个节点的复制组是否能够正常提供服务
 * 1. 创建一个副本的复制组
 * 2. 等待 leader 产生，write 数据，然后 read 出来验证一遍
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
}

/**
 * 验证1个节点的关闭 leader 后重启是否能够正常服务
 * 1. 创建1个副本的复制组
 * 2. 等待 leader 产生，write 数据，然后 read 出来验证一遍
 * 3. shutdown leader，然后再拉起来
 * 4. 等待 leader 产生，然后 read 之前写入的数据验证一遍
 * 5. 再 write 数据，再 read 出来验证一遍
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
    // 测试发现集群不可用
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
}

/**
 * 验证2个节点是否能够正常提供服务
 * 1. 创建2个副本的复制组
 * 2. 等待 leader 产生，write 数据，然后 read 出来验证一遍
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
}

/**
 * 验证2个节点的关闭非 leader 节点 后重启是否能够正常服务
 * 1. 创建2个副本的复制组
 * 2. 等待 leader 产生，write 数据，然后 read 出来验证一遍
 * 3. shutdown 非 leader，然后再拉起来
 * 4. 等待 leader 产生，然后 read 之前写入的数据验证一遍
 * 5. 再 write 数据，再 read 出来验证一遍
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

    // 发起 read/write
    WriteThenReadVerify(leaderId,
                        logicPoolId,
                        copysetId,
                        chunkId,
                        length,
                        ch,
                        loop);

    // shutdown 某个非 leader 的 peer
    PeerId shutdownPeerid;
    if (0 == ::strcmp(leaderId.to_string().c_str(),
                      peer1.to_string().c_str())) {
        shutdownPeerid = peer2;
    } else {
        shutdownPeerid = peer1;
    }
    ASSERT_EQ(0, cluster.ShutdownPeer(shutdownPeerid));
    // 测试发现集群不可用
    ReadVerifyNotAvailable(leaderId,
                           logicPoolId,
                           copysetId,
                           chunkId,
                           length,
                           ch,
                           1);

    ASSERT_EQ(0, cluster.StartPeer(shutdownPeerid));
    ASSERT_EQ(0, cluster.WaitLeader(&leaderId));

    // 读出来验证一遍
    ReadVerify(leaderId, logicPoolId, copysetId, chunkId, length, ch, loop);
    // 再次发起 read/write
    WriteThenReadVerify(leaderId,
                        logicPoolId,
                        copysetId,
                        chunkId,
                        length,
                        ch + 1,
                        loop);
}

/**
 * 验证2个节点的关闭 leader 后重启是否能够正常服务
 * 1. 创建2个副本的复制组
 * 2. 等待 leader 产生，write 数据，然后 read 出来验证一遍
 * 3. shutdown leader，然后再拉起来
 * 4. 等待 leader 产生，然后 read 之前写入的数据验证一遍
 * 5. 再 write 数据，再 read 出来验证一遍
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

    // 发起 read/write
    WriteThenReadVerify(leaderId,
                        logicPoolId,
                        copysetId,
                        chunkId,
                        length,
                        ch,
                        loop);

    // shutdown leader
    ASSERT_EQ(0, cluster.ShutdownPeer(leaderId));
    // 测试发现集群不可用
    ReadVerifyNotAvailable(leaderId,
                           logicPoolId,
                           copysetId,
                           chunkId,
                           length,
                           ch,
                           1);

    ASSERT_EQ(0, cluster.StartPeer(leaderId));
    ASSERT_EQ(0, cluster.WaitLeader(&leaderId));

    // 读出来验证一遍
    ReadVerify(leaderId, logicPoolId, copysetId, chunkId, length, ch, loop);
    // 再次发起 read/write
    WriteThenReadVerify(leaderId,
                        logicPoolId,
                        copysetId,
                        chunkId,
                        length,
                        ch + 1,
                        loop);
}

/**
 * 验证3个节点是否能够正常提供服务
 * 1. 创建3个副本的复制组
 * 2. 等待 leader 产生，write 数据，然后 read 出来验证一遍
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

    // 再次发起 read/write
    WriteThenReadVerify(leaderId,
                        logicPoolId,
                        copysetId,
                        chunkId,
                        length,
                        ch + 1,
                        loop);
}

/**
 * 验证3个节点的关闭非 leader 节点 后重启是否能够正常服务
 * 1. 创建3个副本的复制组
 * 2. 等待 leader 产生，write 数据，然后 read 出来验证一遍
 * 3. shutdown 非 leader，然后再拉起来
 * 4. 等待 leader 产生，然后 read 之前写入的数据验证一遍
 * 5. 再 write 数据，再 read 出来验证一遍
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

    // 发起 read/write
    WriteThenReadVerify(leaderId,
                        logicPoolId,
                        copysetId,
                        chunkId,
                        length,
                        ch,
                        loop);

    // shutdown 某个非 leader 的 peer
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

    // 读出来验证一遍
    ReadVerify(leaderId, logicPoolId, copysetId, chunkId, length, ch, loop);
    // 再次发起 read/write
    WriteThenReadVerify(leaderId,
                        logicPoolId,
                        copysetId,
                        chunkId,
                        length,
                        ch + 1,
                        loop);
}

/**
 * 验证3个节点的关闭 leader 节点 后重启是否能够正常服务
 * 1. 创建3个副本的复制组
 * 2. 等待 leader 产生，write 数据，然后 read 出来验证一遍
 * 3. shutdown leader，然后再拉起来
 * 4. 等待 leader 产生，然后 read 之前写入的数据验证一遍
 * 5. 再 write 数据，再 read 出来验证一遍
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

    // 发起 read/write
    WriteThenReadVerify(leaderId,
                        logicPoolId,
                        copysetId,
                        chunkId,
                        length,
                        ch,
                        loop);


    // shutdown leader
    ASSERT_EQ(0, cluster.ShutdownPeer(leaderId));
    // 测试发现集群暂时不可用
    ReadVerifyNotAvailable(leaderId,
                           logicPoolId,
                           copysetId,
                           chunkId,
                           length,
                           ch,
                           1);

    ASSERT_EQ(0, cluster.StartPeer(leaderId));
    ASSERT_EQ(0, cluster.WaitLeader(&leaderId));

    // 读出来验证一遍
    ReadVerify(leaderId, logicPoolId, copysetId, chunkId, length, ch, loop);


    // 读出来验证一遍
    ReadVerify(leaderId, logicPoolId, copysetId, chunkId, length, ch, loop);
    // 再次发起 read/write
    WriteThenReadVerify(leaderId,
                        logicPoolId,
                        copysetId,
                        chunkId,
                        length,
                        ch + 1,
                        loop);
}

/**
 * 验证3个节点的关闭非 leader 节点，重启，控制让其从 install snapshot 恢复
 * 1. 创建3个副本的复制组
 * 2. 等待 leader 产生，write 数据，然后 read 出来验证一遍
 * 3. shutdown 非 leader
 * 4. 然后 sleep 超过一个 snapshot interval，write read 数据
 * 5. 然后再 sleep 超过一个 snapshot interval，write read 数据；4,5两步
 *    是为了保证打至少两次快照，这样，节点再重启的时候必须通过 install snapshot,
 *    因为 log 已经被删除了
 * 6. 等待 leader 产生，然后 read 之前写入的数据验证一遍
 * 7. transfer leader 到shut down 的peer 上
 * 8. 在 read 之前写入的数据验证
 * 9. 再 write 数据，再 read 出来验证一遍
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

    // 发起 read/write
    WriteThenReadVerify(leaderId,
                        logicPoolId,
                        copysetId,
                        chunkId,
                        length,
                        ch,
                        loop);

    // shutdown 某个非 leader 的 peer
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

    // wait snapshot, 保证能够触发安装快照
    ::sleep(1.5*snapshotTimeoutS);
    // 再次发起 read/write
    WriteThenReadVerify(leaderId,
                        logicPoolId,
                        copysetId,
                        chunkId,
                        length,
                        ch + 1,
                        loop);
    ::sleep(1.5*snapshotTimeoutS);
    // 再次发起 read/write
    WriteThenReadVerify(leaderId,
                        logicPoolId,
                        copysetId,
                        chunkId,
                        length,
                        ch + 2,
                        loop);

    // restart, 需要从 install snapshot 恢复
    ASSERT_EQ(0, cluster.StartPeer(shutdownPeerid));
    ASSERT_EQ(0, cluster.WaitLeader(&leaderId));

    // 读出来验证一遍
    ReadVerify(leaderId, logicPoolId, copysetId, chunkId, length, ch + 2, loop);
    // 再次发起 read/write
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

    // 读出来验证一遍
    ReadVerify(leaderId, logicPoolId, copysetId, chunkId, length, ch + 3, loop);
    // 再次发起 read/write
    WriteThenReadVerify(leaderId,
                        logicPoolId,
                        copysetId,
                        chunkId,
                        length,
                        ch + 4,
                        loop);
}

/**
 * 验证3个节点的关闭非 leader 节点，重启，控制让其从 install snapshot 恢复
 * 1. 创建3个副本的复制组
 * 2. 等待 leader 产生，write 数据，然后 read 出来验证一遍
 * 3. shutdown 非 leader
 * 4. read 之前 write 的数据验证一遍
 * 5. 再 write 数据，然后 read 出来验证一遍
 * 6. 然后 sleep 超过一个 snapshot interval，write read 数据
 * 7. 然后再 sleep 超过一个 snapshot interval，write read 数据；4,5两步
 *    是为了保证打至少两次快照，这样，节点再重启的时候必须通过 install snapshot,
 *    因为 log 已经被删除了
 * 9. 删除 shutdown peer 的数据目录，然后再拉起来
 * 10. 然后 read 之前写入的数据验证一遍
 * 11. transfer leader 到shut down 的 peer 上
 * 12. 在 read 之前写入的数据验证
 * 13. 再 write 数据，再 read 出来验证一遍
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

    // 发起 read/write
    WriteThenReadVerify(leaderId,
                        logicPoolId,
                        copysetId,
                        chunkId,
                        length,
                        ch,
                        loop);

    // shutdown 某个非 leader 的 peer
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

    // 读出来验证一遍
    ReadVerify(leaderId, logicPoolId, copysetId, chunkId, length, ch, loop);
    // 再次发起 read/write
    WriteThenReadVerify(leaderId,
                        logicPoolId,
                        copysetId,
                        chunkId,
                        length,
                        ch + 1,
                        loop);

    // wait snapshot, 保证能够触发安装快照
    ::sleep(1.5*snapshotTimeoutS);
    // 再次发起 read/write
    WriteThenReadVerify(leaderId,
                        logicPoolId,
                        copysetId,
                        chunkId,
                        length,
                        ch + 2,
                        loop);
    ::sleep(1.5*snapshotTimeoutS);
    // 再次发起 read/write
    WriteThenReadVerify(leaderId,
                        logicPoolId,
                        copysetId,
                        chunkId,
                        length,
                        ch + 3,
                        loop);

    // 删除此 peer 的数据，然后重启
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

    // 读出来验证一遍
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

    // 读出来验证一遍
    ReadVerify(leaderId, logicPoolId, copysetId, chunkId, length, ch + 3, loop);
    // 再次发起 read/write
    WriteThenReadVerify(leaderId,
                        logicPoolId,
                        copysetId,
                        chunkId,
                        length,
                        ch + 4,
                        loop);
}

/**
 * 验证3个节点的关闭非 leader 节点，重启，控制让其从 install snapshot 恢复
 * 1. 创建3个副本的复制组
 * 2. 等待 leader 产生，write 数据，然后 read 出来验证一遍
 * 3. shutdown 非 leader
 * 4. read 之前 write 的数据验证一遍
 * 5. 再 write 数据，然后 read 出来验证一遍
 * 6. 然后 sleep 超过一个 snapshot interval，write read 数据
 * 7. 然后再 sleep 超过一个 snapshot interval，write read 数据；4,5两步
 *    是为了保证打至少两次快照，这样，节点再重启的时候必须通过 install snapshot,
 *    因为 log 已经被删除了
 * 9. 通过配置变更 add peer
 * 10. 然后 read 之前写入的数据验证一遍
 * 11. 在发起 write，再 read 读出来验证一遍
 * 12. transfer leader 到 add 的 peer 上
 * 13. 在 read 之前写入的数据验证
 * 14. 再 write 数据，再 read 出来验证一遍
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

    // 发起 read/write，多个 chunk file
    for (int i = 0; i < kMaxChunkId; ++i) {
        WriteThenReadVerify(leaderId,
                            logicPoolId,
                            copysetId,
                            i,
                            length,
                            ch,
                            loop);
    }

    // shutdown 某个非 leader 的 peer
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

    // 读出来验证一遍
    for (int i = 0; i < kMaxChunkId; ++i) {
        ReadVerify(leaderId, logicPoolId, copysetId, i, length, ch, loop);
    }
    // 再次发起 read/write
    for (int i = 0; i < kMaxChunkId; ++i) {
        WriteThenReadVerify(leaderId,
                            logicPoolId,
                            copysetId,
                            i,
                            length,
                            ch + 1,
                            loop);
    }

    // wait snapshot, 保证能够触发安装快照
    ::sleep(1.5*snapshotTimeoutS);
    // 再次发起 read/write
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
    // 再次发起 read/write
    for (int i = 0; i < kMaxChunkId; ++i) {
        WriteThenReadVerify(leaderId,
                            logicPoolId,
                            copysetId,
                            i,
                            length,
                            ch + 3,
                            loop);
    }

    // add 一个 peer
    {
        ASSERT_EQ(0, cluster.StartPeer(peer4, true));
        Configuration conf = cluster.CopysetConf();
        braft::cli::CliOptions options;
        options.max_retry = 3;
        options.timeout_ms = 8000;
        butil::Status status = AddPeer(logicPoolId,
                                       copysetId,
                                       cluster.CopysetConf(),
                                       peer4,
                                       options);
        ASSERT_EQ(0, status.error_code());
    }

    // 读出来验证一遍
    for (int i = 0; i < kMaxChunkId; ++i) {
        ReadVerify(leaderId, logicPoolId, copysetId, i, length, ch + 3, loop);
    }
    // 再次发起 read/write
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

    // 读出来验证一遍
    for (int i = 0; i < kMaxChunkId; ++i) {
        ReadVerify(leaderId, logicPoolId, copysetId, i, length, ch + 4, loop);
    }
    // 再次发起 read/write
    for (int i = 0; i < kMaxChunkId; ++i) {
        WriteThenReadVerify(leaderId,
                            logicPoolId,
                            copysetId,
                            i,
                            length,
                            ch + 5,
                            loop);
    }
}

/**
 *  * 验证3个节点的 remove 一个节点，然后再 add 回来，并控制让其从 install snapshot 恢复
 * 1. 创建3个副本的复制组
 * 2. 等待 leader 产生，write 数据，然后 read 出来验证一遍
 * 3. 通过配置变更 remove 一个 非 leader
 * 4. read 之前 write 的数据验证一遍
 * 5. 再 write 数据，然后 read 出来验证一遍
 * 6. 然后 sleep 超过一个 snapshot interval，write read 数据
 * 7. 然后再 sleep 超过一个 snapshot interval，write read 数据；4,5两步
 *    是为了保证打至少两次快照，这样，节点再重启的时候必须通过 install snapshot,
 *    因为 log 已经被删除了
 * 9. 通过配置变更再将之前 remove 的 peer add 回来
 * 10. transfer leader 到此 peer
 * 11. 在 read 之前写入的数据验证
 * 12. 再 write 数据，再 read 出来验证一遍
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

    // 发起 read/write，多个 chunk file
    for (int i = 0; i < kMaxChunkId; ++i) {
        WriteThenReadVerify(leaderId,
                            logicPoolId,
                            copysetId,
                            i,
                            length,
                            ch,
                            loop);
    }

    // shutdown 某个非 leader 的 peer
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
    // remove 一个 peer
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

    // 读出来验证一遍
    for (int i = 0; i < kMaxChunkId; ++i) {
        ReadVerify(leaderId, logicPoolId, copysetId, i, length, ch, loop);
    }
    // 再次发起 read/write
    for (int i = 0; i < kMaxChunkId; ++i) {
        WriteThenReadVerify(leaderId,
                            logicPoolId,
                            copysetId,
                            i,
                            length,
                            ch + 1,
                            loop);
    }

    // wait snapshot, 保证能够触发安装快照
    ::sleep(1.5*snapshotTimeoutS);
    // 再次发起 read/write
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
    // 再次发起 read/write
    for (int i = 0; i < kMaxChunkId; ++i) {
        WriteThenReadVerify(leaderId,
                            logicPoolId,
                            copysetId,
                            i,
                            length,
                            ch + 3,
                            loop);
    }

    // add 回来
    {
        Configuration conf = cluster.CopysetConf();
        braft::cli::CliOptions options;
        options.max_retry = 3;
        options.timeout_ms = 8000;
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

    // 读出来验证一遍
    for (int i = 0; i < kMaxChunkId; ++i) {
        ReadVerify(leaderId, logicPoolId, copysetId, i, length, ch + 3, loop);
    }
    // 再次发起 read/write
    for (int i = 0; i < kMaxChunkId; ++i) {
        WriteThenReadVerify(leaderId,
                            logicPoolId,
                            copysetId,
                            i,
                            length,
                            ch + 4,
                            loop);
    }
}

}  // namespace chunkserver
}  // namespace curve
