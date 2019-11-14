/*
 * Project: curve
 * Created Date: 19-07-12
 * Author: wudemiao
 * Copyright (c) 2019 netease
 */

#include <brpc/channel.h>
#include <gtest/gtest.h>
#include <butil/at_exit.h>

#include <vector>

#include "src/chunkserver/copyset_node_manager.h"
#include "src/chunkserver/cli.h"
#include "src/fs/fs_common.h"
#include "src/fs/local_filesystem.h"
#include "test/integration/common/peer_cluster.h"
#include "test/integration/common/config_generator.h"

namespace curve {
namespace chunkserver {

using curve::fs::LocalFileSystem;
using curve::fs::LocalFsFactory;
using curve::fs::FileSystemType;

static char* raftVoteParam[3][13] = {
    {
        "chunkserver",
        "-chunkServerIp=127.0.0.1",
        "-chunkServerPort=9091",
        "-chunkServerStoreUri=local://./9091/",
        "-chunkServerMetaUri=local://./9091/chunkserver.dat",
        "-copySetUri=local://./9091/copysets",
        "-recycleUri=local://./9091/recycler",
        "-chunkFilePoolDir=./9091/chunkfilepool/",
        "-chunkFilePoolMetaPath=./9091/chunkfilepool.meta",
        "-conf=./9091/chunkserver.conf",
        "-raft_sync_segments=true",
        NULL
    },
    {
        "chunkserver",
        "-chunkServerIp=127.0.0.1",
        "-chunkServerPort=9092",
        "-chunkServerStoreUri=local://./9092/",
        "-chunkServerMetaUri=local://./9092/chunkserver.dat",
        "-copySetUri=local://./9092/copysets",
        "-recycleUri=local://./9092/recycler",
        "-chunkFilePoolDir=./9092/chunkfilepool/",
        "-chunkFilePoolMetaPath=./9092/chunkfilepool.meta",
        "-conf=./9092/chunkserver.conf",
        "-raft_sync_segments=true",
        NULL
    },
    {
        "chunkserver",
        "-chunkServerIp=127.0.0.1",
        "-chunkServerPort=9093",
        "-chunkServerStoreUri=local://./9093/",
        "-chunkServerMetaUri=local://./9093/chunkserver.dat",
        "-copySetUri=local://./9093/copysets",
        "-recycleUri=local://./9093/recycler",
        "-chunkFilePoolDir=./9093/chunkfilepool/",
        "-chunkFilePoolMetaPath=./9093/chunkfilepool.meta",
        "-conf=./9093/chunkserver.conf",
        "-raft_sync_segments=true",
        NULL
    },
};

class RaftVoteTest : public testing::Test {
 protected:
    virtual void SetUp() {
        peer1.set_address("127.0.0.1:9091:0");
        peer2.set_address("127.0.0.1:9092:0");
        peer3.set_address("127.0.0.1:9093:0");

        std::string mkdir1("mkdir ");
        mkdir1 += std::to_string(PeerCluster::PeerToId(peer1));
        std::string mkdir2("mkdir ");
        mkdir2 += std::to_string(PeerCluster::PeerToId(peer2));
        std::string mkdir3("mkdir ");
        mkdir3 += std::to_string(PeerCluster::PeerToId(peer3));

        ::system(mkdir1.c_str());
        ::system(mkdir2.c_str());
        ::system(mkdir3.c_str());

        electionTimeoutMs = 3000;
        snapshotIntervalS = 60;
        waitMultiReplicasBecomeConsistent = 3000;

        ASSERT_TRUE(cg1.Init("9091"));
        ASSERT_TRUE(cg2.Init("9092"));
        ASSERT_TRUE(cg3.Init("9093"));
        cg1.SetKV("copyset.election_timeout_ms", "3000");
        cg1.SetKV("copyset.snapshot_interval_s", "60");
        cg2.SetKV("copyset.election_timeout_ms", "3000");
        cg2.SetKV("copyset.snapshot_interval_s", "60");
        cg3.SetKV("copyset.election_timeout_ms", "3000");
        cg3.SetKV("copyset.snapshot_interval_s", "60");
        ASSERT_TRUE(cg1.Generate());
        ASSERT_TRUE(cg2.Generate());
        ASSERT_TRUE(cg3.Generate());

        paramsIndexs[PeerCluster::PeerToId(peer1)] = 0;
        paramsIndexs[PeerCluster::PeerToId(peer2)] = 1;
        paramsIndexs[PeerCluster::PeerToId(peer3)] = 2;

        params.push_back(raftVoteParam[0]);
        params.push_back(raftVoteParam[1]);
        params.push_back(raftVoteParam[2]);
    }
    virtual void TearDown() {
        std::string rmdir1("rm -fr ");
        rmdir1 += std::to_string(PeerCluster::PeerToId(peer1));
        std::string rmdir2("rm -fr ");
        rmdir2 += std::to_string(PeerCluster::PeerToId(peer2));
        std::string rmdir3("rm -fr ");
        rmdir3 += std::to_string(PeerCluster::PeerToId(peer3));

        ::system(rmdir1.c_str());
        ::system(rmdir2.c_str());
        ::system(rmdir3.c_str());

        // wait for process exit
        ::usleep(100 * 1000);
    }

 public:
    Peer peer1;
    Peer peer2;
    Peer peer3;
    CSTConfigGenerator cg1;
    CSTConfigGenerator cg2;
    CSTConfigGenerator cg3;
    int electionTimeoutMs;
    int snapshotIntervalS;

    std::map<int, int> paramsIndexs;
    std::vector<char **> params;
    // 等待多个副本数据一致的时间
    int waitMultiReplicasBecomeConsistent;
};



butil::AtExitManager atExitManager;

/**
 * 验证1个节点的复制组
 * 1. 创建1个成员的复制组，等待leader产生，write数据，然后read出来验证一遍
 * 2. 挂掉leader，验证可用性
 * 3. 拉起leader
 * 4. hang住leader
 * 5. 恢复leader
 */
TEST_F(RaftVoteTest, OneNode) {
    LogicPoolID logicPoolId = 2;
    CopysetID copysetId = 100001;
    uint64_t chunkId = 1;
    int length = kOpRequestAlignSize;
    char ch = 'a';
    int loop = 25;

    // 1. 启动一个成员的复制组
    PeerId leaderId;
    Peer leaderPeer;
    std::vector<Peer> peers;
    peers.push_back(peer1);
    PeerCluster cluster("InitShutdown-cluster",
                        logicPoolId,
                        copysetId,
                        peers,
                        params,
                        paramsIndexs);
    cluster.SetElectionTimeoutMs(electionTimeoutMs);
    cluster.SetsnapshotIntervalS(snapshotIntervalS);
    ASSERT_EQ(0, cluster.StartPeer(peer1, PeerCluster::PeerToId(peer1)));

    ASSERT_EQ(0, cluster.WaitLeader(&leaderPeer));
    ASSERT_EQ(0, leaderId.parse(leaderPeer.address()));
    ASSERT_STREQ(peer1.address().c_str(), leaderId.to_string().c_str());

    WriteThenReadVerify(leaderPeer,
                        logicPoolId,
                        copysetId,
                        chunkId,
                        length,
                        ch++,
                        loop);

    // 2. 挂掉这个节点
    ASSERT_EQ(0, cluster.ShutdownPeer(peer1));
    ReadVerifyNotAvailable(leaderPeer,
                           logicPoolId,
                           copysetId,
                           chunkId,
                           length,
                           ch - 1,
                           1);

    // 3. 将节点拉起来
    ASSERT_EQ(0, cluster.StartPeer(peer1, PeerCluster::PeerToId(peer1)));
    ASSERT_EQ(0, cluster.WaitLeader(&leaderPeer));
    ASSERT_EQ(0, leaderId.parse(leaderPeer.address()));
    ASSERT_STREQ(peer1.address().c_str(), leaderId.to_string().c_str());
    // read之前写入的数据验证
    ReadVerify(leaderPeer,
               logicPoolId,
               copysetId,
               chunkId,
               length,
               ch - 1,
               loop);

    WriteThenReadVerify(leaderPeer,
                        logicPoolId,
                        copysetId,
                        chunkId,
                        length,
                        ch++,
                        loop);

    // 4. hang住此节点
    ASSERT_EQ(0, cluster.HangPeer(peer1));
    ::usleep(200 * 1000);
    ReadVerifyNotAvailable(leaderPeer,
                           logicPoolId,
                           copysetId,
                           chunkId,
                           length,
                           ch - 1,
                           1);

    // 5. 恢复节点
    ASSERT_EQ(0, cluster.SignalPeer(peer1));
    ASSERT_EQ(0, cluster.WaitLeader(&leaderPeer));
    ASSERT_EQ(0, leaderId.parse(leaderPeer.address()));
    ASSERT_STREQ(peer1.address().c_str(), leaderId.to_string().c_str());
    // read之前写入的数据验证
    ReadVerify(leaderPeer,
               logicPoolId,
               copysetId,
               chunkId,
               length,
               ch - 1,
               loop);

    WriteThenReadVerify(leaderPeer,
                        logicPoolId,
                        copysetId,
                        chunkId,
                        length,
                        ch++,
                        loop);
}

/**
 * 验证2个节点的复制组，并挂掉leader
 * 1. 创建2个成员的复制组，等待leader产生，write数据，然后read出来验证一遍
 * 2. 挂掉leader
 * 3. 恢复leader
 */
TEST_F(RaftVoteTest, TwoNodeKillLeader) {
    LogicPoolID logicPoolId = 2;
    CopysetID copysetId = 100001;
    uint64_t chunkId = 1;
    int length = kOpRequestAlignSize;
    char ch = 'a';
    int loop = 25;

    // 1. 启动2个成员的复制组
    PeerId leaderId;
    Peer leaderPeer;
    std::vector<Peer> peers;
    peers.push_back(peer1);
    peers.push_back(peer2);
    PeerCluster cluster("InitShutdown-cluster",
                        logicPoolId,
                        copysetId,
                        peers,
                        params,
                        paramsIndexs);
    cluster.SetElectionTimeoutMs(electionTimeoutMs);
    cluster.SetsnapshotIntervalS(snapshotIntervalS);
    ASSERT_EQ(0, cluster.StartPeer(peer1, PeerCluster::PeerToId(peer1)));
    ASSERT_EQ(0, cluster.StartPeer(peer2, PeerCluster::PeerToId(peer2)));

    ASSERT_EQ(0, cluster.WaitLeader(&leaderPeer));
    ASSERT_EQ(0, leaderId.parse(leaderPeer.address()));

    WriteThenReadVerify(leaderPeer,
                        logicPoolId,
                        copysetId,
                        chunkId,
                        length,
                        ch++,
                        loop);

    // 2. 挂掉leader
    ASSERT_EQ(0, cluster.ShutdownPeer(leaderPeer));
    ReadVerifyNotAvailable(leaderPeer,
                           logicPoolId,
                           copysetId,
                           chunkId,
                           length,
                           ch - 1,
                           1);

    // 3. 拉起leader
    ASSERT_EQ(0,
              cluster.StartPeer(leaderPeer, PeerCluster::PeerToId(leaderPeer)));
    ASSERT_EQ(0, cluster.WaitLeader(&leaderPeer));
    ASSERT_EQ(0, leaderId.parse(leaderPeer.address()));
    // read之前写入的数据验证
    ReadVerify(leaderPeer,
               logicPoolId,
               copysetId,
               chunkId,
               length,
               ch - 1,
               loop);

    WriteThenReadVerify(leaderPeer,
                        logicPoolId,
                        copysetId,
                        chunkId,
                        length,
                        ch++,
                        loop);

    ::usleep(waitMultiReplicasBecomeConsistent * 1000);
    CopysetStatusVerify(peers, logicPoolId, copysetId, 2);
}

/**
 * 验证2个节点的复制组，并挂掉follower
 * 1. 创建2个成员的复制组，等待leader产生，write数据，然后read出来验证一遍
 * 2. 挂掉follower
 * 3. 恢复follower
 */
TEST_F(RaftVoteTest, TwoNodeKillFollower) {
    LogicPoolID logicPoolId = 2;
    CopysetID copysetId = 100001;
    uint64_t chunkId = 1;
    int length = kOpRequestAlignSize;
    char ch = 'a';
    int loop = 25;

    // 1. 启动2个成员的复制组
    LOG(INFO) << "init 2 members copyset";
    PeerId leaderId;
    Peer leaderPeer;
    std::vector<Peer> peers;
    peers.push_back(peer1);
    peers.push_back(peer2);
    PeerCluster cluster("InitShutdown-cluster",
                        logicPoolId,
                        copysetId,
                        peers,
                        params,
                        paramsIndexs);
    cluster.SetElectionTimeoutMs(electionTimeoutMs);
    cluster.SetsnapshotIntervalS(snapshotIntervalS);
    ASSERT_EQ(0, cluster.StartPeer(peer1, PeerCluster::PeerToId(peer1)));
    ASSERT_EQ(0, cluster.StartPeer(peer2, PeerCluster::PeerToId(peer2)));

    ASSERT_EQ(0, cluster.WaitLeader(&leaderPeer));
    ASSERT_EQ(0, leaderId.parse(leaderPeer.address()));

    WriteThenReadVerify(leaderPeer,
                        logicPoolId,
                        copysetId,
                        chunkId,
                        length,
                        ch++,
                        loop);

    // 2. 挂掉follower
    Peer followerPeer;
    if (leaderPeer.address() == peer1.address()) {
        followerPeer = peer2;
    } else {
        followerPeer = peer1;
    }
    LOG(INFO) << "kill follower " << followerPeer.address();
    ASSERT_EQ(0, cluster.ShutdownPeer(followerPeer));
    LOG(INFO) << "fill ch: " << std::to_string(ch - 1);
    // step down之前的request，最终会被提交
    WriteVerifyNotAvailable(leaderPeer,
                            logicPoolId,
                            copysetId,
                            chunkId,
                            length,
                            ch,
                            1);
    // 等待leader step down，之后，也不支持read了
    ::usleep(1000 * electionTimeoutMs * 1.2);
    ReadVerifyNotAvailable(leaderPeer,
                           logicPoolId,
                           copysetId,
                           chunkId,
                           length,
                           ch,
                           1);

    // 3. 拉起follower
    LOG(INFO) << "restart follower " << followerPeer.address();
    ASSERT_EQ(0,
              cluster.StartPeer(followerPeer,
                                PeerCluster::PeerToId(followerPeer)));
    ASSERT_EQ(0, cluster.WaitLeader(&leaderPeer));
    ASSERT_EQ(0, leaderId.parse(leaderPeer.address()));
    // read之前写入的数据验证，step down之前的write
    ReadVerify(leaderPeer,
               logicPoolId,
               copysetId,
               chunkId,
               length,
               ch,
               1);

    WriteThenReadVerify(leaderPeer,
                        logicPoolId,
                        copysetId,
                        chunkId,
                        length,
                        ch++,
                        loop);

    ::usleep(waitMultiReplicasBecomeConsistent * 1000);
    CopysetStatusVerify(peers, logicPoolId, copysetId, 2);
}

/**
 * 验证2个节点的复制组，并hang leader
 * 1. 创建2个成员的复制组，等待leader产生，write数据，然后read出来验证一遍
 * 2. hang leader
 * 3. 恢复leader
 */
TEST_F(RaftVoteTest, TwoNodeHangLeader) {
    LogicPoolID logicPoolId = 2;
    CopysetID copysetId = 100001;
    uint64_t chunkId = 1;
    int length = kOpRequestAlignSize;
    char ch = 'a';
    int loop = 25;

    // 1. 启动2个成员的复制组
    PeerId leaderId;
    Peer leaderPeer;
    std::vector<Peer> peers;
    peers.push_back(peer1);
    peers.push_back(peer2);
    PeerCluster cluster("InitShutdown-cluster",
                        logicPoolId,
                        copysetId,
                        peers,
                        params,
                        paramsIndexs);
    cluster.SetElectionTimeoutMs(electionTimeoutMs);
    cluster.SetsnapshotIntervalS(snapshotIntervalS);
    ASSERT_EQ(0, cluster.StartPeer(peer1, PeerCluster::PeerToId(peer1)));
    ASSERT_EQ(0, cluster.StartPeer(peer2, PeerCluster::PeerToId(peer2)));

    ASSERT_EQ(0, cluster.WaitLeader(&leaderPeer));
    ASSERT_EQ(0, leaderId.parse(leaderPeer.address()));

    WriteThenReadVerify(leaderPeer,
                        logicPoolId,
                        copysetId,
                        chunkId,
                        length,
                        ch++,
                        loop);

    // 2. Hang leader
    LOG(INFO) << "hang leader peer: " << leaderPeer.address();
    ASSERT_EQ(0, cluster.HangPeer(leaderPeer));
    ReadVerifyNotAvailable(leaderPeer,
                           logicPoolId,
                           copysetId,
                           chunkId,
                           length,
                           ch - 1,
                           1);

    // 3. 恢复leader
    LOG(INFO) << "recover leader peer: " << leaderPeer.address();
    ASSERT_EQ(0, cluster.SignalPeer(leaderPeer));
    ASSERT_EQ(0, cluster.WaitLeader(&leaderPeer));
    ASSERT_EQ(0, leaderId.parse(leaderPeer.address()));
    // read之前写入的数据验证
    ReadVerify(leaderPeer,
               logicPoolId,
               copysetId,
               chunkId,
               length,
               ch - 1,
               loop);

    WriteThenReadVerify(leaderPeer,
                        logicPoolId,
                        copysetId,
                        chunkId,
                        length,
                        ch++,
                        loop);

    ::usleep(waitMultiReplicasBecomeConsistent * 1000);
    CopysetStatusVerify(peers, logicPoolId, copysetId, 1);
}

/**
 * 验证2个节点的复制组，并发Hang一个follower
 * 1. 创建2个成员的复制组，等待leader产生，write数据，然后read出来验证一遍
 * 2. hang follower
 * 3. 恢复follower
 */
TEST_F(RaftVoteTest, TwoNodeHangFollower) {
    LogicPoolID logicPoolId = 2;
    CopysetID copysetId = 100001;
    uint64_t chunkId = 1;
    int length = kOpRequestAlignSize;
    char ch = 'a';
    int loop = 25;

    // 1. 启动2个成员的复制组
    LOG(INFO) << "init 2 members copyset";
    PeerId leaderId;
    Peer leaderPeer;
    std::vector<Peer> peers;
    peers.push_back(peer1);
    peers.push_back(peer2);
    PeerCluster cluster("InitShutdown-cluster",
                        logicPoolId,
                        copysetId,
                        peers,
                        params,
                        paramsIndexs);
    cluster.SetElectionTimeoutMs(electionTimeoutMs);
    cluster.SetsnapshotIntervalS(snapshotIntervalS);
    ASSERT_EQ(0, cluster.StartPeer(peer1, PeerCluster::PeerToId(peer1)));
    ASSERT_EQ(0, cluster.StartPeer(peer2, PeerCluster::PeerToId(peer2)));

    ASSERT_EQ(0, cluster.WaitLeader(&leaderPeer));
    ASSERT_EQ(0, leaderId.parse(leaderPeer.address()));

    WriteThenReadVerify(leaderPeer,
                        logicPoolId,
                        copysetId,
                        chunkId,
                        length,
                        ch++,
                        loop);

    // 2. hang follower
    Peer followerPeer;
    if (leaderPeer.address() == peer1.address()) {
        followerPeer = peer2;
    } else {
        followerPeer = peer1;
    }
    LOG(INFO) << "hang follower " << followerPeer.address();
    ASSERT_EQ(0, cluster.HangPeer(followerPeer));
    LOG(INFO) << "fill ch: " << std::to_string(ch - 1);
    // step down之前的request，最终会被提交
    WriteVerifyNotAvailable(leaderPeer,
                            logicPoolId,
                            copysetId,
                            chunkId,
                            length,
                            ch,
                            1);
    // 等待leader step down之后，也不支持read了
    ::usleep(1000 * electionTimeoutMs * 1.3);
    ReadVerifyNotAvailable(leaderPeer,
                           logicPoolId,
                           copysetId,
                           chunkId,
                           length,
                           ch,
                           1);

    // 3. 恢复follower
    LOG(INFO) << "recover follower " << followerPeer.address();
    ASSERT_EQ(0, cluster.SignalPeer(followerPeer));
    ASSERT_EQ(0, cluster.WaitLeader(&leaderPeer));
    ASSERT_EQ(0, leaderId.parse(leaderPeer.address()));
    // read之前写入的数据验证，step down之前的write
    ReadVerify(leaderPeer,
               logicPoolId,
               copysetId,
               chunkId,
               length,
               ch,
               1);

    WriteThenReadVerify(leaderPeer,
                        logicPoolId,
                        copysetId,
                        chunkId,
                        length,
                        ch++,
                        loop);

    ::usleep(1.3 * waitMultiReplicasBecomeConsistent * 1000);
    CopysetStatusVerify(peers, logicPoolId, copysetId, 2);
}

/**
 * 验证3个节点是否能够正常提供服务
 * 1. 创建3个副本的复制组，等待leader产生，write数据，然后read出来验证一遍
 */
TEST_F(RaftVoteTest, ThreeNodesNormal) {
    LogicPoolID logicPoolId = 2;
    CopysetID copysetId = 100001;
    uint64_t chunkId = 1;
    int length = kOpRequestAlignSize;
    char ch = 'a';
    int loop = 25;

    std::vector<Peer> peers;
    peers.push_back(peer1);
    peers.push_back(peer2);
    peers.push_back(peer3);

    PeerCluster cluster("InitShutdown-cluster",
                        logicPoolId,
                        copysetId,
                        peers,
                        params,
                        paramsIndexs);
    cluster.SetElectionTimeoutMs(electionTimeoutMs);
    cluster.SetsnapshotIntervalS(snapshotIntervalS);
    ASSERT_EQ(0, cluster.StartPeer(peer1, PeerCluster::PeerToId(peer1)));
    ASSERT_EQ(0, cluster.StartPeer(peer2, PeerCluster::PeerToId(peer2)));
    ASSERT_EQ(0, cluster.StartPeer(peer3, PeerCluster::PeerToId(peer3)));

    Peer leaderPeer;
    ASSERT_EQ(0, cluster.WaitLeader(&leaderPeer));
    PeerId leaderId;
    ASSERT_EQ(0, leaderId.parse(leaderPeer.address()));

    // 再次发起 read/write
    WriteThenReadVerify(leaderPeer,
                        logicPoolId,
                        copysetId,
                        chunkId,
                        length,
                        ch++,
                        loop);

    ::usleep(waitMultiReplicasBecomeConsistent * 1000);
    CopysetStatusVerify(peers, logicPoolId, copysetId, 1);
}

/**
 * 验证3个节点的复制组，并挂掉leader
 * 1. 创建3个成员的复制组，等待leader产生，write数据，然后read出来验证一遍
 * 2. 挂掉leader
 * 3. 恢复leader
 */
TEST_F(RaftVoteTest, ThreeNodeKillLeader) {
    LogicPoolID logicPoolId = 2;
    CopysetID copysetId = 100001;
    uint64_t chunkId = 1;
    int length = kOpRequestAlignSize;
    char ch = 'a';
    int loop = 25;

    // 1. 启动3个成员的复制组
    PeerId leaderId;
    Peer leaderPeer;
    std::vector<Peer> peers;
    peers.push_back(peer1);
    peers.push_back(peer2);
    peers.push_back(peer3);
    PeerCluster cluster("InitShutdown-cluster",
                        logicPoolId,
                        copysetId,
                        peers,
                        params,
                        paramsIndexs);
    cluster.SetElectionTimeoutMs(electionTimeoutMs);
    cluster.SetsnapshotIntervalS(snapshotIntervalS);
    ASSERT_EQ(0, cluster.StartPeer(peer1, PeerCluster::PeerToId(peer1)));
    ASSERT_EQ(0, cluster.StartPeer(peer2, PeerCluster::PeerToId(peer2)));
    ASSERT_EQ(0, cluster.StartPeer(peer3, PeerCluster::PeerToId(peer3)));

    ASSERT_EQ(0, cluster.WaitLeader(&leaderPeer));
    ASSERT_EQ(0, leaderId.parse(leaderPeer.address()));

    WriteThenReadVerify(leaderPeer,
                        logicPoolId,
                        copysetId,
                        chunkId,
                        length,
                        ch++,
                        loop);

    // 2. 挂掉leader
    ASSERT_EQ(0, cluster.ShutdownPeer(leaderPeer));
    ReadVerifyNotAvailable(leaderPeer,
                           logicPoolId,
                           copysetId,
                           chunkId,
                           length,
                           ch - 1,
                           1);

    // 3. 拉起leader
    ASSERT_EQ(0,
              cluster.StartPeer(leaderPeer, PeerCluster::PeerToId(leaderPeer)));
    ASSERT_EQ(0, cluster.WaitLeader(&leaderPeer));
    ASSERT_EQ(0, leaderId.parse(leaderPeer.address()));
    // read之前写入的数据验证
    ReadVerify(leaderPeer,
               logicPoolId,
               copysetId,
               chunkId,
               length,
               ch - 1,
               loop);

    WriteThenReadVerify(leaderPeer,
                        logicPoolId,
                        copysetId,
                        chunkId,
                        length,
                        ch++,
                        loop);

    ::usleep(waitMultiReplicasBecomeConsistent * 1000);
    CopysetStatusVerify(peers, logicPoolId, copysetId, 2);
}

/**
 * 验证3个节点的复制组，并挂掉follower
 * 1. 创建3个成员的复制组，等待leader产生，write数据，然后read出来验证一遍
 * 2. 挂掉follower
 * 3. 恢复follower
 */
TEST_F(RaftVoteTest, ThreeNodeKillOneFollower) {
    LogicPoolID logicPoolId = 2;
    CopysetID copysetId = 100001;
    uint64_t chunkId = 1;
    int length = kOpRequestAlignSize;
    char ch = 'a';
    int loop = 25;

    // 1. 启动3个成员的复制组
    PeerId leaderId;
    Peer leaderPeer;
    std::vector<Peer> peers;
    peers.push_back(peer1);
    peers.push_back(peer2);
    peers.push_back(peer3);
    PeerCluster cluster("InitShutdown-cluster",
                        logicPoolId,
                        copysetId,
                        peers,
                        params,
                        paramsIndexs);
    cluster.SetElectionTimeoutMs(electionTimeoutMs);
    cluster.SetsnapshotIntervalS(snapshotIntervalS);
    ASSERT_EQ(0, cluster.StartPeer(peer1, PeerCluster::PeerToId(peer1)));
    ASSERT_EQ(0, cluster.StartPeer(peer2, PeerCluster::PeerToId(peer2)));
    ASSERT_EQ(0, cluster.StartPeer(peer3, PeerCluster::PeerToId(peer3)));

    ASSERT_EQ(0, cluster.WaitLeader(&leaderPeer));
    ASSERT_EQ(0, leaderId.parse(leaderPeer.address()));

    WriteThenReadVerify(leaderPeer,
                        logicPoolId,
                        copysetId,
                        chunkId,
                        length,
                        ch++,
                        loop);

    // 2. 挂掉1个follower
    std::vector<Peer> followerPeers;
    PeerCluster::GetFollwerPeers(peers, leaderPeer, &followerPeers);
    ASSERT_GE(followerPeers.size(), 1);
    ASSERT_EQ(0, cluster.ShutdownPeer(followerPeers[0]));
    WriteThenReadVerify(leaderPeer,
                        logicPoolId,
                        copysetId,
                        chunkId,
                        length,
                        ch++,
                        loop);

    // 3. 拉起follower
    ASSERT_EQ(0,
              cluster.StartPeer(followerPeers[0],
                                PeerCluster::PeerToId(followerPeers[0])));
    // read之前写入的数据验证
    ReadVerify(leaderPeer,
               logicPoolId,
               copysetId,
               chunkId,
               length,
               ch - 1,
               loop);

    WriteThenReadVerify(leaderPeer,
                        logicPoolId,
                        copysetId,
                        chunkId,
                        length,
                        ch++,
                        loop);

    ::usleep(1.6 * waitMultiReplicasBecomeConsistent * 1000);
    CopysetStatusVerify(peers, logicPoolId, copysetId, 1);
}

/**
 * 验证3个节点的复制组，反复restart leader
 * 1. 创建3个成员的复制组，等待leader产生，write数据，然后read出来验证一遍
 * 2. 反复restart leader
 */
TEST_F(RaftVoteTest, ThreeNodeRestartLeader) {
    LogicPoolID logicPoolId = 2;
    CopysetID copysetId = 100001;
    uint64_t chunkId = 1;
    int length = kOpRequestAlignSize;
    char ch = 'a';
    int loop = 25;

    // 1. 启动3个成员的复制组
    PeerId leaderId;
    Peer leaderPeer;
    std::vector<Peer> peers;
    peers.push_back(peer1);
    peers.push_back(peer2);
    peers.push_back(peer3);
    PeerCluster cluster("InitShutdown-cluster",
                        logicPoolId,
                        copysetId,
                        peers,
                        params,
                        paramsIndexs);
    cluster.SetElectionTimeoutMs(electionTimeoutMs);
    cluster.SetsnapshotIntervalS(snapshotIntervalS);
    ASSERT_EQ(0, cluster.StartPeer(peer1, PeerCluster::PeerToId(peer1)));
    ASSERT_EQ(0, cluster.StartPeer(peer2, PeerCluster::PeerToId(peer2)));
    ASSERT_EQ(0, cluster.StartPeer(peer3, PeerCluster::PeerToId(peer3)));

    ASSERT_EQ(0, cluster.WaitLeader(&leaderPeer));
    ASSERT_EQ(0, leaderId.parse(leaderPeer.address()));

    WriteThenReadVerify(leaderPeer,
                        logicPoolId,
                        copysetId,
                        chunkId,
                        length,
                        ch++,
                        loop);

    // 2. restart leader
    for (int i = 0; i < 5; ++i) {
        ASSERT_EQ(0, cluster.ShutdownPeer(leaderPeer));
        ::sleep(3);
        ASSERT_EQ(0, cluster.StartPeer(leaderPeer,
                                       PeerCluster::PeerToId(leaderPeer)));
        ReadVerifyNotAvailable(leaderPeer,
                               logicPoolId,
                               copysetId,
                               chunkId,
                               length,
                               ch - 1,
                               1);

        ASSERT_EQ(0, cluster.WaitLeader(&leaderPeer));
        ASSERT_EQ(0, leaderId.parse(leaderPeer.address()));
        // read之前写入的数据验证
        ReadVerify(leaderPeer,
                   logicPoolId,
                   copysetId,
                   chunkId,
                   length,
                   ch - 1,
                   loop);

        WriteThenReadVerify(leaderPeer,
                            logicPoolId,
                            copysetId,
                            chunkId,
                            length,
                            ch++,
                            loop);
    }

    ::usleep(1.3 * waitMultiReplicasBecomeConsistent * 1000);
    CopysetStatusVerify(peers, logicPoolId, copysetId, 0);
}

/**
 * 验证3个节点的复制组，反复重启一个follower
 * 1. 创建3个成员的复制组，等待leader产生，write数据，然后read出来验证一遍
 * 2. 反复重启follower
 */
TEST_F(RaftVoteTest, ThreeNodeRestartFollower) {
    LogicPoolID logicPoolId = 2;
    CopysetID copysetId = 100001;
    uint64_t chunkId = 1;
    int length = kOpRequestAlignSize;
    char ch = 'a';
    int loop = 25;

    // 1. 启动3个成员的复制组
    PeerId leaderId;
    Peer leaderPeer;
    std::vector<Peer> peers;
    peers.push_back(peer1);
    peers.push_back(peer2);
    peers.push_back(peer3);
    PeerCluster cluster("InitShutdown-cluster",
                        logicPoolId,
                        copysetId,
                        peers,
                        params,
                        paramsIndexs);
    cluster.SetElectionTimeoutMs(electionTimeoutMs);
    cluster.SetsnapshotIntervalS(snapshotIntervalS);
    ASSERT_EQ(0, cluster.StartPeer(peer1, PeerCluster::PeerToId(peer1)));
    ASSERT_EQ(0, cluster.StartPeer(peer2, PeerCluster::PeerToId(peer2)));
    ASSERT_EQ(0, cluster.StartPeer(peer3, PeerCluster::PeerToId(peer3)));

    ASSERT_EQ(0, cluster.WaitLeader(&leaderPeer));
    ASSERT_EQ(0, leaderId.parse(leaderPeer.address()));

    WriteThenReadVerify(leaderPeer,
                        logicPoolId,
                        copysetId,
                        chunkId,
                        length,
                        ch++,
                        loop);

    // 2. 反复 restart follower
    for (int i = 0; i < 5; ++i) {
        std::vector<Peer> followerPeers;
        PeerCluster::GetFollwerPeers(peers, leaderPeer, &followerPeers);
        ASSERT_GE(followerPeers.size(), 1);
        ASSERT_EQ(0, cluster.ShutdownPeer(followerPeers[0]));
        WriteThenReadVerify(leaderPeer,
                            logicPoolId,
                            copysetId,
                            chunkId,
                            length,
                            ch++,
                            loop);
        ASSERT_EQ(0,
                  cluster.StartPeer(followerPeers[0],
                                    PeerCluster::PeerToId(followerPeers[0])));
        ::sleep(1);
    }

    ::usleep(1.5 * waitMultiReplicasBecomeConsistent * 1000);
    CopysetStatusVerify(peers, logicPoolId, copysetId, 0);
}

/**
 * 验证3个节点的复制组，并挂掉leader和1个follower
 * 1. 创建3个成员的复制组，等待leader产生，write数据，然后read出来验证一遍
 * 2. 挂掉leader和1个follwoer
 * 3. 拉起leader
 * 4. 拉起follower
 */
TEST_F(RaftVoteTest, ThreeNodeKillLeaderAndOneFollower) {
    LogicPoolID logicPoolId = 2;
    CopysetID copysetId = 100001;
    uint64_t chunkId = 1;
    int length = kOpRequestAlignSize;
    char ch = 'a';
    int loop = 25;

    // 1. 启动3个成员的复制组
    PeerId leaderId;
    Peer leaderPeer;
    std::vector<Peer> peers;
    peers.push_back(peer1);
    peers.push_back(peer2);
    peers.push_back(peer3);
    PeerCluster cluster("InitShutdown-cluster",
                        logicPoolId,
                        copysetId,
                        peers,
                        params,
                        paramsIndexs);
    cluster.SetElectionTimeoutMs(electionTimeoutMs);
    cluster.SetsnapshotIntervalS(snapshotIntervalS);
    ASSERT_EQ(0, cluster.StartPeer(peer1, PeerCluster::PeerToId(peer1)));
    ASSERT_EQ(0, cluster.StartPeer(peer2, PeerCluster::PeerToId(peer2)));
    ASSERT_EQ(0, cluster.StartPeer(peer3, PeerCluster::PeerToId(peer3)));

    ASSERT_EQ(0, cluster.WaitLeader(&leaderPeer));
    ASSERT_EQ(0, leaderId.parse(leaderPeer.address()));

    WriteThenReadVerify(leaderPeer,
                        logicPoolId,
                        copysetId,
                        chunkId,
                        length,
                        ch++,
                        loop);

    // 2. 挂掉leader和Follower
    ASSERT_EQ(0, cluster.ShutdownPeer(leaderPeer));
    std::vector<Peer> followerPeers;
    PeerCluster::GetFollwerPeers(peers, leaderPeer, &followerPeers);
    ASSERT_GE(followerPeers.size(), 1);
    ASSERT_EQ(0, cluster.ShutdownPeer(followerPeers[0]));
    ReadVerifyNotAvailable(leaderPeer,
                           logicPoolId,
                           copysetId,
                           chunkId,
                           length,
                           ch - 1,
                           1);

    // 3. 拉起leader
    ASSERT_EQ(0,
              cluster.StartPeer(leaderPeer, PeerCluster::PeerToId(leaderPeer)));
    ASSERT_EQ(0, cluster.WaitLeader(&leaderPeer));
    ASSERT_EQ(0, leaderId.parse(leaderPeer.address()));
    // read之前写入的数据验证
    ReadVerify(leaderPeer,
               logicPoolId,
               copysetId,
               chunkId,
               length,
               ch - 1,
               loop);

    WriteThenReadVerify(leaderPeer,
                        logicPoolId,
                        copysetId,
                        chunkId,
                        length,
                        ch++,
                        loop);

    // 4. 拉起follower
    ASSERT_EQ(0, cluster.StartPeer(followerPeers[0],
                                   PeerCluster::PeerToId(followerPeers[0])));
    WriteThenReadVerify(leaderPeer,
                        logicPoolId,
                        copysetId,
                        chunkId,
                        length,
                        ch++,
                        loop);

    ::usleep(2 * waitMultiReplicasBecomeConsistent * 1000);
    CopysetStatusVerify(peers, logicPoolId, copysetId, 2);
}

/**
 * 验证3个节点的复制组，并挂掉2个follower
 * 1. 创建3个成员的复制组，等待leader产生，write数据，然后read出来验证一遍
 * 2. 挂掉2个follower
 * 3. 拉起1个follower
 * 4. 拉起1个follower
 */
TEST_F(RaftVoteTest, ThreeNodeKillTwoFollower) {
    LogicPoolID logicPoolId = 2;
    CopysetID copysetId = 100001;
    uint64_t chunkId = 1;
    int length = kOpRequestAlignSize;
    char ch = 'a';
    int loop = 25;

    // 1. 启动3个成员的复制组
    PeerId leaderId;
    Peer leaderPeer;
    std::vector<Peer> peers;
    peers.push_back(peer1);
    peers.push_back(peer2);
    peers.push_back(peer3);
    PeerCluster cluster("InitShutdown-cluster",
                        logicPoolId,
                        copysetId,
                        peers,
                        params,
                        paramsIndexs);
    cluster.SetElectionTimeoutMs(electionTimeoutMs);
    cluster.SetsnapshotIntervalS(snapshotIntervalS);
    ASSERT_EQ(0, cluster.StartPeer(peer1, PeerCluster::PeerToId(peer1)));
    ASSERT_EQ(0, cluster.StartPeer(peer2, PeerCluster::PeerToId(peer2)));
    ASSERT_EQ(0, cluster.StartPeer(peer3, PeerCluster::PeerToId(peer3)));

    ASSERT_EQ(0, cluster.WaitLeader(&leaderPeer));
    ASSERT_EQ(0, leaderId.parse(leaderPeer.address()));

    WriteThenReadVerify(leaderPeer,
                        logicPoolId,
                        copysetId,
                        chunkId,
                        length,
                        ch++,
                        loop);

    // 2. 挂掉2个Follower
    std::vector<Peer> followerPeers;
    PeerCluster::GetFollwerPeers(peers, leaderPeer, &followerPeers);
    ASSERT_GE(followerPeers.size(), 2);
    ASSERT_EQ(0, cluster.ShutdownPeer(followerPeers[0]));
    ASSERT_EQ(0, cluster.ShutdownPeer(followerPeers[1]));
    WriteVerifyNotAvailable(leaderPeer,
                            logicPoolId,
                            copysetId,
                            chunkId,
                            length,
                            ch - 1,
                            1);

    // 3. 拉起1个follower
    ASSERT_EQ(0, cluster.StartPeer(followerPeers[0],
                                   PeerCluster::PeerToId(followerPeers[0])));
    ASSERT_EQ(0, cluster.WaitLeader(&leaderPeer));
    ASSERT_EQ(0, leaderId.parse(leaderPeer.address()));
    // read之前写入的数据验证
    ReadVerify(leaderPeer,
               logicPoolId,
               copysetId,
               chunkId,
               length,
               ch - 1,
               loop);

    WriteThenReadVerify(leaderPeer,
                        logicPoolId,
                        copysetId,
                        chunkId,
                        length,
                        ch++,
                        loop);

    // 4. 拉起follower
    ASSERT_EQ(0, cluster.StartPeer(followerPeers[1],
                                   PeerCluster::PeerToId(followerPeers[1])));
    WriteThenReadVerify(leaderPeer,
                        logicPoolId,
                        copysetId,
                        chunkId,
                        length,
                        ch ++,
                        loop);

    ::usleep(1.3 * waitMultiReplicasBecomeConsistent * 1000);
    CopysetStatusVerify(peers, logicPoolId, copysetId, 1);
}

/**
 * 验证3个节点的复制组，并挂掉3个成员
 * 1. 创建3个成员的复制组，等待leader产生，write数据，然后read出来验证一遍
 * 2. 挂掉3个成员
 * 3. 拉起1个成员
 * 4. 拉起1个成员
 * 5. 拉起1个成员
 */
TEST_F(RaftVoteTest, ThreeNodeKillThreeMember) {
    LogicPoolID logicPoolId = 2;
    CopysetID copysetId = 100001;
    uint64_t chunkId = 1;
    int length = kOpRequestAlignSize;
    char ch = 'a';
    int loop = 25;

    // 1. 启动3个成员的复制组
    PeerId leaderId;
    Peer leaderPeer;
    std::vector<Peer> peers;
    peers.push_back(peer1);
    peers.push_back(peer2);
    peers.push_back(peer3);
    PeerCluster cluster("InitShutdown-cluster",
                        logicPoolId,
                        copysetId,
                        peers,
                        params,
                        paramsIndexs);
    cluster.SetElectionTimeoutMs(electionTimeoutMs);
    cluster.SetsnapshotIntervalS(snapshotIntervalS);
    ASSERT_EQ(0, cluster.StartPeer(peer1, PeerCluster::PeerToId(peer1)));
    ASSERT_EQ(0, cluster.StartPeer(peer2, PeerCluster::PeerToId(peer2)));
    ASSERT_EQ(0, cluster.StartPeer(peer3, PeerCluster::PeerToId(peer3)));

    ASSERT_EQ(0, cluster.WaitLeader(&leaderPeer));
    ASSERT_EQ(0, leaderId.parse(leaderPeer.address()));

    WriteThenReadVerify(leaderPeer,
                        logicPoolId,
                        copysetId,
                        chunkId,
                        length,
                        ch++,
                        loop);

    // 2. 挂掉3个成员
    std::vector<Peer> followerPeers;
    ASSERT_EQ(0, cluster.ShutdownPeer(peer1));
    ASSERT_EQ(0, cluster.ShutdownPeer(peer2));
    ASSERT_EQ(0, cluster.ShutdownPeer(peer3));
    WriteVerifyNotAvailable(leaderPeer,
                            logicPoolId,
                            copysetId,
                            chunkId,
                            length,
                            ch - 1,
                            1);

    // 3. 拉起1个成员
    ASSERT_EQ(0, cluster.StartPeer(peer1,
                                   PeerCluster::PeerToId(peer1)));
    ::usleep(1000 * electionTimeoutMs * 1.2);
    ReadVerifyNotAvailable(peer1,
                           logicPoolId,
                           copysetId,
                           chunkId,
                           length,
                           ch - 1,
                           1);


    // 4. 拉起1个成员
    ASSERT_EQ(0, cluster.StartPeer(peer2,
                                   PeerCluster::PeerToId(peer2)));
    ASSERT_EQ(0, cluster.WaitLeader(&leaderPeer));
    ASSERT_EQ(0, leaderId.parse(leaderPeer.address()));

    WriteThenReadVerify(leaderPeer,
                        logicPoolId,
                        copysetId,
                        chunkId,
                        length,
                        ch++,
                        loop);

    // 5. 再拉起1个成员
    ASSERT_EQ(0, cluster.StartPeer(peer3,
                                   PeerCluster::PeerToId(peer3)));
    ASSERT_EQ(0, cluster.WaitLeader(&leaderPeer));
    ASSERT_EQ(0, leaderId.parse(leaderPeer.address()));

    WriteThenReadVerify(leaderPeer,
                        logicPoolId,
                        copysetId,
                        chunkId,
                        length,
                        ch++,
                        loop);

    ::usleep(1.3 * waitMultiReplicasBecomeConsistent * 1000);
    CopysetStatusVerify(peers, logicPoolId, copysetId, 2);
}



/**
 * 验证3个节点的复制组，并hang leader
 * 1. 创建3个成员的复制组，等待leader产生，write数据，然后read出来验证一遍
 * 2. hang leader
 * 3. 恢复leader
 */
TEST_F(RaftVoteTest, ThreeNodeHangLeader) {
    LogicPoolID logicPoolId = 2;
    CopysetID copysetId = 100001;
    uint64_t chunkId = 1;
    int length = kOpRequestAlignSize;
    char ch = 'a';
    int loop = 25;

    // 1. 启动3个成员的复制组
    PeerId leaderId;
    Peer leaderPeer;
    std::vector<Peer> peers;
    peers.push_back(peer1);
    peers.push_back(peer2);
    peers.push_back(peer3);
    PeerCluster cluster("InitShutdown-cluster",
                        logicPoolId,
                        copysetId,
                        peers,
                        params,
                        paramsIndexs);
    cluster.SetElectionTimeoutMs(electionTimeoutMs);
    cluster.SetsnapshotIntervalS(snapshotIntervalS);
    ASSERT_EQ(0, cluster.StartPeer(peer1, PeerCluster::PeerToId(peer1)));
    ASSERT_EQ(0, cluster.StartPeer(peer2, PeerCluster::PeerToId(peer2)));
    ASSERT_EQ(0, cluster.StartPeer(peer3, PeerCluster::PeerToId(peer3)));

    ASSERT_EQ(0, cluster.WaitLeader(&leaderPeer));
    ASSERT_EQ(0, leaderId.parse(leaderPeer.address()));

    WriteThenReadVerify(leaderPeer,
                        logicPoolId,
                        copysetId,
                        chunkId,
                        length,
                        ch++,
                        loop);

    // 2. hang leader
    Peer oldPeer = leaderPeer;
    ASSERT_EQ(0, cluster.HangPeer(leaderPeer));
    ReadVerifyNotAvailable(leaderPeer,
                           logicPoolId,
                           copysetId,
                           chunkId,
                           length,
                           ch - 1,
                           1);

    // 等待new leader产生
    ::usleep(1.3 * electionTimeoutMs * 1000);
    ASSERT_EQ(0, cluster.WaitLeader(&leaderPeer));
    ASSERT_EQ(0, leaderId.parse(leaderPeer.address()));
    WriteThenReadVerify(leaderPeer,
                        logicPoolId,
                        copysetId,
                        chunkId,
                        length,
                        ch++,
                        loop);

    // 3. 恢复 old leader
    ASSERT_EQ(0, cluster.SignalPeer(oldPeer));
    // read之前写入的数据验证
    ReadVerify(leaderPeer,
               logicPoolId,
               copysetId,
               chunkId,
               length,
               ch - 1,
               loop);

    WriteThenReadVerify(leaderPeer,
                        logicPoolId,
                        copysetId,
                        chunkId,
                        length,
                        ch++,
                        loop);

    ::usleep(1.6 * waitMultiReplicasBecomeConsistent * 1000);
    CopysetStatusVerify(peers, logicPoolId, copysetId, 2);
}


/**
 * 验证3个节点的复制组，并hang1个follower
 * 1. 创建3个成员的复制组，等待leader产生，write数据，然后read出来验证一遍
 * 2. 挂掉follower
 * 3. 恢复follower
 */
TEST_F(RaftVoteTest, ThreeNodeHangOneFollower) {
    LogicPoolID logicPoolId = 2;
    CopysetID copysetId = 100001;
    uint64_t chunkId = 1;
    int length = kOpRequestAlignSize;
    char ch = 'a';
    int loop = 25;

    // 1. 启动3个成员的复制组
    PeerId leaderId;
    Peer leaderPeer;
    std::vector<Peer> peers;
    peers.push_back(peer1);
    peers.push_back(peer2);
    peers.push_back(peer3);
    PeerCluster cluster("InitShutdown-cluster",
                        logicPoolId,
                        copysetId,
                        peers,
                        params,
                        paramsIndexs);
    cluster.SetElectionTimeoutMs(electionTimeoutMs);
    cluster.SetsnapshotIntervalS(snapshotIntervalS);
    ASSERT_EQ(0, cluster.StartPeer(peer1, PeerCluster::PeerToId(peer1)));
    ASSERT_EQ(0, cluster.StartPeer(peer2, PeerCluster::PeerToId(peer2)));
    ASSERT_EQ(0, cluster.StartPeer(peer3, PeerCluster::PeerToId(peer3)));

    ASSERT_EQ(0, cluster.WaitLeader(&leaderPeer));
    ASSERT_EQ(0, leaderId.parse(leaderPeer.address()));

    WriteThenReadVerify(leaderPeer,
                        logicPoolId,
                        copysetId,
                        chunkId,
                        length,
                        ch++,
                        loop);

    // 2. hang 1个follower
    std::vector<Peer> followerPeers;
    PeerCluster::GetFollwerPeers(peers, leaderPeer, &followerPeers);
    ASSERT_GE(followerPeers.size(), 1);
    ASSERT_EQ(0, cluster.HangPeer(followerPeers[0]));
    WriteThenReadVerify(leaderPeer,
                        logicPoolId,
                        copysetId,
                        chunkId,
                        length,
                        ch++,
                        loop);

    // 3. 恢复follower
    ASSERT_EQ(0, cluster.SignalPeer(followerPeers[0]));
    // read之前写入的数据验证
    ReadVerify(leaderPeer,
               logicPoolId,
               copysetId,
               chunkId,
               length,
               ch - 1,
               loop);

    WriteThenReadVerify(leaderPeer,
                        logicPoolId,
                        copysetId,
                        chunkId,
                        length,
                        ch++,
                        loop);

    ::usleep(1.3 * waitMultiReplicasBecomeConsistent * 1000);
    CopysetStatusVerify(peers, logicPoolId, copysetId, 1);
}

/**
 * 验证3个节点的复制组，并hang leader和1个follower
 * 1. 创建3个成员的复制组，等待leader产生，write数据，然后read出来验证一遍
 * 2. hang leader和1个follower
 * 3. 恢复old leader
 * 4. 恢复follower
 */
TEST_F(RaftVoteTest, ThreeNodeHangLeaderAndOneFollower) {
    LogicPoolID logicPoolId = 2;
    CopysetID copysetId = 100001;
    uint64_t chunkId = 1;
    int length = kOpRequestAlignSize;
    char ch = 'a';
    int loop = 25;

    // 1. 启动3个成员的复制组
    PeerId leaderId;
    Peer leaderPeer;
    std::vector<Peer> peers;
    peers.push_back(peer1);
    peers.push_back(peer2);
    peers.push_back(peer3);
    PeerCluster cluster("InitShutdown-cluster",
                        logicPoolId,
                        copysetId,
                        peers,
                        params,
                        paramsIndexs);
    cluster.SetElectionTimeoutMs(electionTimeoutMs);
    cluster.SetsnapshotIntervalS(snapshotIntervalS);
    ASSERT_EQ(0, cluster.StartPeer(peer1, PeerCluster::PeerToId(peer1)));
    ASSERT_EQ(0, cluster.StartPeer(peer2, PeerCluster::PeerToId(peer2)));
    ASSERT_EQ(0, cluster.StartPeer(peer3, PeerCluster::PeerToId(peer3)));

    ASSERT_EQ(0, cluster.WaitLeader(&leaderPeer));
    ASSERT_EQ(0, leaderId.parse(leaderPeer.address()));

    WriteThenReadVerify(leaderPeer,
                        logicPoolId,
                        copysetId,
                        chunkId,
                        length,
                        ch++,
                        loop);

    // 2. hang leader
    ASSERT_EQ(0, cluster.HangPeer(leaderPeer));
    std::vector<Peer> followerPeers;
    PeerCluster::GetFollwerPeers(peers, leaderPeer, &followerPeers);
    ASSERT_GE(followerPeers.size(), 1);
    ASSERT_EQ(0, cluster.HangPeer(followerPeers[0]));
    ReadVerifyNotAvailable(leaderPeer,
                           logicPoolId,
                           copysetId,
                           chunkId,
                           length,
                           ch - 1,
                           1);

    // 3. 恢复 old leader
    ASSERT_EQ(0, cluster.SignalPeer(leaderPeer));
    ASSERT_EQ(0, cluster.WaitLeader(&leaderPeer));
    ASSERT_EQ(0, leaderId.parse(leaderPeer.address()));
    // read之前写入的数据验证
    ReadVerify(leaderPeer,
               logicPoolId,
               copysetId,
               chunkId,
               length,
               ch - 1,
               loop);

    WriteThenReadVerify(leaderPeer,
                        logicPoolId,
                        copysetId,
                        chunkId,
                        length,
                        ch++,
                        loop);

    // 4. 恢复follower
    ASSERT_EQ(0, cluster.SignalPeer(followerPeers[0]));
    ReadVerify(leaderPeer,
               logicPoolId,
               copysetId,
               chunkId,
               length,
               ch - 1,
               loop);

    WriteThenReadVerify(leaderPeer,
                        logicPoolId,
                        copysetId,
                        chunkId,
                        length,
                        ch++,
                        loop);

    ::usleep(1.6 * waitMultiReplicasBecomeConsistent * 1000);
    CopysetStatusVerify(peers, logicPoolId, copysetId, 0);
}

/**
 * 验证3个节点的复制组，并hang 2个follower
 * 1. 创建3个成员的复制组，等待leader产生，write数据，然后read出来验证一遍
 * 2. hang两个follower
 * 3. 恢复old leader
 * 4. 恢复follower
 */
TEST_F(RaftVoteTest, ThreeNodeHangTwoFollower) {
    LogicPoolID logicPoolId = 2;
    CopysetID copysetId = 100001;
    uint64_t chunkId = 1;
    int length = kOpRequestAlignSize;
    char ch = 'a';
    int loop = 25;

    // 1. 启动3个成员的复制组
    PeerId leaderId;
    Peer leaderPeer;
    std::vector<Peer> peers;
    peers.push_back(peer1);
    peers.push_back(peer2);
    peers.push_back(peer3);
    PeerCluster cluster("InitShutdown-cluster",
                        logicPoolId,
                        copysetId,
                        peers,
                        params,
                        paramsIndexs);
    cluster.SetElectionTimeoutMs(electionTimeoutMs);
    cluster.SetsnapshotIntervalS(snapshotIntervalS);
    ASSERT_EQ(0, cluster.StartPeer(peer1, PeerCluster::PeerToId(peer1)));
    ASSERT_EQ(0, cluster.StartPeer(peer2, PeerCluster::PeerToId(peer2)));
    ASSERT_EQ(0, cluster.StartPeer(peer3, PeerCluster::PeerToId(peer3)));

    ASSERT_EQ(0, cluster.WaitLeader(&leaderPeer));
    ASSERT_EQ(0, leaderId.parse(leaderPeer.address()));

    WriteThenReadVerify(leaderPeer,
                        logicPoolId,
                        copysetId,
                        chunkId,
                        length,
                        ch++,
                        loop);

    // 2. hang 2个follower
    std::vector<Peer> followerPeers;
    PeerCluster::GetFollwerPeers(peers, leaderPeer, &followerPeers);
    ASSERT_GE(followerPeers.size(), 2);
    ASSERT_EQ(0, cluster.HangPeer(followerPeers[0]));
    ASSERT_EQ(0, cluster.HangPeer(followerPeers[1]));
    // step down之前提交request会超时
    WriteVerifyNotAvailable(leaderPeer,
                            logicPoolId,
                            copysetId,
                            chunkId,
                            length,
                            ch ++,
                            1);

    // 等待step down之后，读也不可提供服务
    ::usleep(1000 * electionTimeoutMs * 1.2);
    ReadVerifyNotAvailable(leaderPeer,
                           logicPoolId,
                           copysetId,
                           chunkId,
                           length,
                           ch - 1,
                           1);

    // 3. 恢复1个follower
    ASSERT_EQ(0, cluster.SignalPeer(followerPeers[0]));
    ASSERT_EQ(0, cluster.WaitLeader(&leaderPeer));
    ASSERT_EQ(0, leaderId.parse(leaderPeer.address()));

    ReadVerify(leaderPeer,
               logicPoolId,
               copysetId,
               chunkId,
               length,
               ch - 1,
               1);

    WriteThenReadVerify(leaderPeer,
                        logicPoolId,
                        copysetId,
                        chunkId,
                        length,
                        ch++,
                        loop);

    // 4. 恢复1个follower
    ASSERT_EQ(0, cluster.SignalPeer(followerPeers[1]));
    ReadVerify(leaderPeer,
               logicPoolId,
               copysetId,
               chunkId,
               length,
               ch - 1,
               loop);

    WriteThenReadVerify(leaderPeer,
                        logicPoolId,
                        copysetId,
                        chunkId,
                        length,
                        ch++,
                        loop);

    ::usleep(2 * waitMultiReplicasBecomeConsistent * 1000);
    CopysetStatusVerify(peers, logicPoolId, copysetId, 2);
}

/**
 * 验证3个节点的复制组，并hang 3个成员
 * 1. 创建3个成员的复制组，等待leader产生，write数据，然后read出来验证一遍
 * 2. hang 3个成员
 * 3. 恢复1个成员
 * 4. 恢复1个成员
 * 5. 恢复1个成员
 */
TEST_F(RaftVoteTest, ThreeNodeHangThreeMember) {
    LogicPoolID logicPoolId = 2;
    CopysetID copysetId = 100001;
    uint64_t chunkId = 1;
    int length = kOpRequestAlignSize;
    char ch = 'a';
    int loop = 25;

    // 1. 启动3个成员的复制组
    PeerId leaderId;
    Peer leaderPeer;
    std::vector<Peer> peers;
    peers.push_back(peer1);
    peers.push_back(peer2);
    peers.push_back(peer3);
    PeerCluster cluster("InitShutdown-cluster",
                        logicPoolId,
                        copysetId,
                        peers,
                        params,
                        paramsIndexs);
    cluster.SetElectionTimeoutMs(electionTimeoutMs);
    cluster.SetsnapshotIntervalS(snapshotIntervalS);
    ASSERT_EQ(0, cluster.StartPeer(peer1, PeerCluster::PeerToId(peer1)));
    ASSERT_EQ(0, cluster.StartPeer(peer2, PeerCluster::PeerToId(peer2)));
    ASSERT_EQ(0, cluster.StartPeer(peer3, PeerCluster::PeerToId(peer3)));

    ASSERT_EQ(0, cluster.WaitLeader(&leaderPeer));
    ASSERT_EQ(0, leaderId.parse(leaderPeer.address()));

    WriteThenReadVerify(leaderPeer,
                        logicPoolId,
                        copysetId,
                        chunkId,
                        length,
                        ch++,
                        loop);

    // 2. 挂掉3个成员
    std::vector<Peer> followerPeers;
    ASSERT_EQ(0, cluster.HangPeer(peer1));
    ASSERT_EQ(0, cluster.HangPeer(peer2));
    ASSERT_EQ(0, cluster.HangPeer(peer3));
    WriteVerifyNotAvailable(leaderPeer,
                            logicPoolId,
                            copysetId,
                            chunkId,
                            length,
                            ch - 1,
                            1);
    ReadVerifyNotAvailable(leaderPeer,
                           logicPoolId,
                           copysetId,
                           chunkId,
                           length,
                           ch - 1,
                           1);

    // 3. 恢复1个成员
    ASSERT_EQ(0, cluster.SignalPeer(peer1));
    ::usleep(1000 * electionTimeoutMs * 1.2);
    ReadVerifyNotAvailable(leaderPeer,
                           logicPoolId,
                           copysetId,
                           chunkId,
                           length,
                           ch - 1,
                           1);


    // 4. 恢复1个成员
    ASSERT_EQ(0, cluster.SignalPeer(peer2));
    ASSERT_EQ(0, cluster.WaitLeader(&leaderPeer));
    ASSERT_EQ(0, leaderId.parse(leaderPeer.address()));

    ReadVerify(leaderPeer,
               logicPoolId,
               copysetId,
               chunkId,
               length,
               ch - 1,
               loop);

    WriteThenReadVerify(leaderPeer,
                        logicPoolId,
                        copysetId,
                        chunkId,
                        length,
                        ch++,
                        loop);

    // 5. 再恢复1个成员
    ASSERT_EQ(0, cluster.SignalPeer(peer3));

    WriteThenReadVerify(leaderPeer,
                        logicPoolId,
                        copysetId,
                        chunkId,
                        length,
                        ch++,
                        loop);

    ::usleep(1.6 * waitMultiReplicasBecomeConsistent * 1000);
    CopysetStatusVerify(peers, logicPoolId, copysetId, 2);
}

}  // namespace chunkserver
}  // namespace curve
