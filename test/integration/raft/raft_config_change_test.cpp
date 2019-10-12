/*
 * Project: curve
 * Created Date: 19-07-09
 * Author: wudemiao
 * Copyright (c) 2019 netease
 */

#include <brpc/channel.h>
#include <gtest/gtest.h>
#include <butil/at_exit.h>

#include <vector>

#include "src/chunkserver/copyset_node_manager.h"
#include "src/chunkserver/cli2.h"
#include "src/fs/fs_common.h"
#include "src/fs/local_filesystem.h"
#include "test/integration/common/peer_cluster.h"
#include "test/integration/common/config_generator.h"

namespace curve {
namespace chunkserver {

using curve::fs::LocalFileSystem;
using curve::fs::LocalFsFactory;
using curve::fs::FileSystemType;

static char* raftConfigParam[5][13] = {
    {
        "chunkserver",
        "-chunkServerIp=127.0.0.1",
        "-chunkServerPort=9081",
        "-chunkServerStoreUri=local://./9081/",
        "-chunkServerMetaUri=local://./9081/chunkserver.dat",
        "-copySetUri=local://./9081/copysets",
        "-recycleUri=local://./9081/recycler",
        "-chunkFilePoolDir=./9081/chunkfilepool/",
        "-chunkFilePoolMetaPath=./9081/chunkfilepool.meta",
        "-conf=./9081/chunkserver.conf",
        "-raft_sync_segments=true",
        NULL
    },
    {
        "chunkserver",
        "-chunkServerIp=127.0.0.1",
        "-chunkServerPort=9082",
        "-chunkServerStoreUri=local://./9082/",
        "-chunkServerMetaUri=local://./9082/chunkserver.dat",
        "-copySetUri=local://./9082/copysets",
        "-recycleUri=local://./9082/recycler",
        "-chunkFilePoolDir=./9082/chunkfilepool/",
        "-chunkFilePoolMetaPath=./9082/chunkfilepool.meta",
        "-conf=./9082/chunkserver.conf",
        "-raft_sync_segments=true",
        NULL
    },
    {
        "chunkserver",
        "-chunkServerIp=127.0.0.1",
        "-chunkServerPort=9083",
        "-chunkServerStoreUri=local://./9083/",
        "-chunkServerMetaUri=local://./9083/chunkserver.dat",
        "-copySetUri=local://./9083/copysets",
        "-recycleUri=local://./9083/recycler",
        "-chunkFilePoolDir=./9083/chunkfilepool/",
        "-chunkFilePoolMetaPath=./9073/chunkfilepool.meta",
        "-conf=./9083/chunkserver.conf",
        "-raft_sync_segments=true",
        NULL
    },
    {
        "chunkserver",
        "-chunkServerIp=127.0.0.1",
        "-chunkServerPort=9084",
        "-chunkServerStoreUri=local://./9084/",
        "-chunkServerMetaUri=local://./9084/chunkserver.dat",
        "-copySetUri=local://./9084/copysets",
        "-recycleUri=local://./9084/recycler",
        "-chunkFilePoolDir=./9084/chunkfilepool/",
        "-chunkFilePoolMetaPath=./9084/chunkfilepool.meta",
        "-conf=./9084/chunkserver.conf",
        "-raft_sync_segments=true",
        NULL
    },
    {
        "chunkserver",
        "-chunkServerIp=127.0.0.1",
        "-chunkServerPort=9085",
        "-chunkServerStoreUri=local://./9085/",
        "-chunkServerMetaUri=local://./9085/chunkserver.dat",
        "-copySetUri=local://./9085/copysets",
        "-recycleUri=local://./9085/recycler",
        "-chunkFilePoolDir=./9085/chunkfilepool/",
        "-chunkFilePoolMetaPath=./9085/chunkfilepool.meta",
        "-conf=./9085/chunkserver.conf",
        "-raft_sync_segments=true",
        NULL
    },
};

class RaftConfigChangeTest : public testing::Test {
 protected:
    virtual void SetUp() {
        peer1.set_address("127.0.0.1:9081:0");
        peer2.set_address("127.0.0.1:9082:0");
        peer3.set_address("127.0.0.1:9083:0");
        peer4.set_address("127.0.0.1:9084:0");
        peer5.set_address("127.0.0.1:9085:0");

        std::string mkdir1("mkdir ");
        mkdir1 += std::to_string(PeerCluster::PeerToId(peer1));
        std::string mkdir2("mkdir ");
        mkdir2 += std::to_string(PeerCluster::PeerToId(peer2));
        std::string mkdir3("mkdir ");
        mkdir3 += std::to_string(PeerCluster::PeerToId(peer3));
        std::string mkdir4("mkdir ");
        mkdir4 += std::to_string(PeerCluster::PeerToId(peer4));
        std::string mkdir5("mkdir ");
        mkdir5 += std::to_string(PeerCluster::PeerToId(peer5));

        ::system(mkdir1.c_str());
        ::system(mkdir2.c_str());
        ::system(mkdir3.c_str());
        ::system(mkdir4.c_str());
        ::system(mkdir5.c_str());

        electionTimeoutMs = 3000;
        confChangeTimeoutMs = 6000;
        snapshotIntervalS = 1;
        maxWaitInstallSnapshotMs = 5000;
        waitMultiReplicasBecomeConsistent = 3000;

        ASSERT_TRUE(cg1.Init("9081"));
        ASSERT_TRUE(cg2.Init("9082"));
        ASSERT_TRUE(cg3.Init("9083"));
        ASSERT_TRUE(cg4.Init("9084"));
        ASSERT_TRUE(cg5.Init("9085"));
        cg1.SetKV("copyset.election_timeout_ms", "3000");
        cg1.SetKV("copyset.snapshot_interval_s", "1");
        cg2.SetKV("copyset.election_timeout_ms", "3000");
        cg2.SetKV("copyset.snapshot_interval_s", "1");
        cg3.SetKV("copyset.election_timeout_ms", "3000");
        cg3.SetKV("copyset.snapshot_interval_s", "1");
        cg4.SetKV("copyset.election_timeout_ms", "3000");
        cg4.SetKV("copyset.snapshot_interval_s", "1");
        cg5.SetKV("copyset.election_timeout_ms", "3000");
        cg5.SetKV("copyset.snapshot_interval_s", "1");
        ASSERT_TRUE(cg1.Generate());
        ASSERT_TRUE(cg2.Generate());
        ASSERT_TRUE(cg3.Generate());
        ASSERT_TRUE(cg4.Generate());
        ASSERT_TRUE(cg5.Generate());

        paramsIndexs[PeerCluster::PeerToId(peer1)] = 0;
        paramsIndexs[PeerCluster::PeerToId(peer2)] = 1;
        paramsIndexs[PeerCluster::PeerToId(peer3)] = 2;
        paramsIndexs[PeerCluster::PeerToId(peer4)] = 3;
        paramsIndexs[PeerCluster::PeerToId(peer5)] = 4;

        params.push_back(raftConfigParam[0]);
        params.push_back(raftConfigParam[1]);
        params.push_back(raftConfigParam[2]);
        params.push_back(raftConfigParam[3]);
        params.push_back(raftConfigParam[4]);
    }
    virtual void TearDown() {
        // wait for process exit
        ::usleep(2000 * 1000);

        std::string rmdir1("rm -fr ");
        rmdir1 += std::to_string(PeerCluster::PeerToId(peer1));
        std::string rmdir2("rm -fr ");
        rmdir2 += std::to_string(PeerCluster::PeerToId(peer2));
        std::string rmdir3("rm -fr ");
        rmdir3 += std::to_string(PeerCluster::PeerToId(peer3));
        std::string rmdir4("rm -fr ");
        rmdir4 += std::to_string(PeerCluster::PeerToId(peer4));
        std::string rmdir5("rm -fr ");
        rmdir5 += std::to_string(PeerCluster::PeerToId(peer5));

        ::system(rmdir1.c_str());
        ::system(rmdir2.c_str());
        ::system(rmdir3.c_str());
        ::system(rmdir4.c_str());
        ::system(rmdir5.c_str());
    }

 public:
    Peer peer1;
    Peer peer2;
    Peer peer3;
    Peer peer4;
    Peer peer5;
    CSTConfigGenerator cg1;
    CSTConfigGenerator cg2;
    CSTConfigGenerator cg3;
    CSTConfigGenerator cg4;
    CSTConfigGenerator cg5;
    int electionTimeoutMs;
    int confChangeTimeoutMs;
    int snapshotIntervalS;
    std::map<int, int> paramsIndexs;
    std::vector<char **> params;
    int maxWaitInstallSnapshotMs;
    // 等待多个副本数据一致的时间
    int waitMultiReplicasBecomeConsistent;
};



butil::AtExitManager atExitManager;

/**
 * 1. 3个节点正常启动
 * 2. 移除一个follower
 * 3. 重复移除上一个follower
 * 4. 再添加回来
 * 5. 重复添加回来
 */
TEST_F(RaftConfigChangeTest, ThreeNodeBasicAddAndRemovePeer) {
    LogicPoolID logicPoolId = 2;
    CopysetID copysetId = 100001;
    uint64_t chunkId = 1;
    int length = kOpRequestAlignSize;
    char ch = 'a';
    int loop = 25;

    // 1. 启动3个成员的复制组
    LOG(INFO) << "start 3 chunkservers";
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

    // 2. 移除1个follower
    LOG(INFO) << "remove 1 follower";
    std::vector<Peer> followerPeers;
    PeerCluster::GetFollwerPeers(peers, leaderPeer, &followerPeers);
    ASSERT_GE(followerPeers.size(), 1);
    Peer removePeer = followerPeers[0];
    braft::cli::CliOptions options;
    options.max_retry = 3;
    options.timeout_ms = confChangeTimeoutMs;
    Configuration conf = cluster.CopysetConf();
    butil::Status
        st1 = RemovePeer(logicPoolId, copysetId, conf, removePeer, options);
    ASSERT_TRUE(st1.ok());


    WriteThenReadVerify(leaderPeer,
                        logicPoolId,
                        copysetId,
                        chunkId,
                        length,
                        ch++,
                        loop);

    // 3. 重复移除，验证重复移除的逻辑是否正常
    butil::Status
        st2 = RemovePeer(logicPoolId, copysetId, conf, removePeer, options);
    ASSERT_TRUE(st2.ok());

    WriteThenReadVerify(leaderPeer,
                        logicPoolId,
                        copysetId,
                        chunkId,
                        length,
                        ch++,
                        loop);

    // 4. add回来
    conf.remove_peer(removePeer.address());
    butil::Status
        st3 = AddPeer(logicPoolId, copysetId, conf, removePeer, options);
    ASSERT_TRUE(st3.ok());

    WriteThenReadVerify(leaderPeer,
                        logicPoolId,
                        copysetId,
                        chunkId,
                        length,
                        ch++,
                        loop);

    // 5. 重复add回来，验证重复添加的逻辑是否正常
    conf.add_peer(removePeer.address());
    butil::Status
        st4 = AddPeer(logicPoolId, copysetId, conf, removePeer, options);
    ASSERT_TRUE(st4.ok());

    WriteThenReadVerify(leaderPeer,
                        logicPoolId,
                        copysetId,
                        chunkId,
                        length,
                        ch++,
                        loop);

    // 验证3个副本数据一致性
    ::usleep(waitMultiReplicasBecomeConsistent * 1000);
    CopysetStatusVerify(peers, logicPoolId, copysetId, 3);
}

TEST_F(RaftConfigChangeTest, ThreeNodeRemoveShutdownPeer) {
    LogicPoolID logicPoolId = 2;
    CopysetID copysetId = 100001;
    uint64_t chunkId = 1;
    int length = kOpRequestAlignSize;
    char ch = 'a';
    int loop = 25;

    // 1. 启动3个成员的复制组
    LOG(INFO) << "start 3 chunkservers";
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
    LOG(INFO) << "shutdown 1 follower";
    std::vector<Peer> followerPeers;
    PeerCluster::GetFollwerPeers(peers, leaderPeer, &followerPeers);
    ASSERT_GE(followerPeers.size(), 1);
    Peer shutdownPeer = followerPeers[0];
    ASSERT_EQ(0, cluster.ShutdownPeer(shutdownPeer));
    WriteThenReadVerify(leaderPeer,
                        logicPoolId,
                        copysetId,
                        chunkId,
                        length,
                        ch++,
                        loop);

    // 3. 移除此follower
    braft::cli::CliOptions options;
    options.max_retry = 3;
    options.timeout_ms = confChangeTimeoutMs;
    Configuration conf = cluster.CopysetConf();
    butil::Status
        st1 = RemovePeer(logicPoolId, copysetId, conf, shutdownPeer, options);
    ASSERT_TRUE(st1.ok());

    WriteThenReadVerify(leaderPeer,
                        logicPoolId,
                        copysetId,
                        chunkId,
                        length,
                        ch++,
                        loop);

    // 4. 拉起follower
    LOG(INFO) << "restart shutdown follower";
    ASSERT_EQ(0, cluster.StartPeer(shutdownPeer,
                                   PeerCluster::PeerToId(shutdownPeer)));

    // 5. add回来
    conf.remove_peer(shutdownPeer.address());
    butil::Status
        st2 = AddPeer(logicPoolId, copysetId, conf, shutdownPeer, options);
    ASSERT_TRUE(st2.ok());
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
                        ch ++,
                        loop);

    // 验证3个副本数据一致性
    ::usleep(1.3 * waitMultiReplicasBecomeConsistent * 1000);
    CopysetStatusVerify(peers, logicPoolId, copysetId, 2);
}

TEST_F(RaftConfigChangeTest, ThreeNodeRemoveHangPeer) {
    LogicPoolID logicPoolId = 2;
    CopysetID copysetId = 100001;
    uint64_t chunkId = 1;
    int length = kOpRequestAlignSize;
    char ch = 'a';
    int loop = 25;

    // 1. 启动3个成员的复制组
    LOG(INFO) << "start 3 chunkservers";
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
    LOG(INFO) << "shutdown 1 follower";
    std::vector<Peer> followerPeers;
    PeerCluster::GetFollwerPeers(peers, leaderPeer, &followerPeers);
    ASSERT_GE(followerPeers.size(), 1);
    Peer shutdownPeer = followerPeers[0];
    ASSERT_EQ(0, cluster.HangPeer(shutdownPeer));
    WriteThenReadVerify(leaderPeer,
                        logicPoolId,
                        copysetId,
                        chunkId,
                        length,
                        ch++,
                        loop);

    // 3. 移除此follower
    braft::cli::CliOptions options;
    options.max_retry = 3;
    options.timeout_ms = confChangeTimeoutMs;
    Configuration conf = cluster.CopysetConf();
    butil::Status
        st1 = RemovePeer(logicPoolId, copysetId, conf, shutdownPeer, options);
    ASSERT_TRUE(st1.ok());

    WriteThenReadVerify(leaderPeer,
                        logicPoolId,
                        copysetId,
                        chunkId,
                        length,
                        ch++,
                        loop);

    // 4. 恢复follower
    LOG(INFO) << "recover hang follower";
    ASSERT_EQ(0, cluster.SignalPeer(shutdownPeer));

    // 5. add回来
    conf.remove_peer(shutdownPeer.address());
    butil::Status
        st2 = AddPeer(logicPoolId, copysetId, conf, shutdownPeer, options);
    ASSERT_TRUE(st2.ok());
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
                        ch ++,
                        loop);

    // 验证3个副本数据一致性
    ::usleep(waitMultiReplicasBecomeConsistent * 1000);
    CopysetStatusVerify(peers, logicPoolId, copysetId, 2);
}

/**
 * 1. 3个节点正常启动
 * 2. 移除leader
 * 3. 再将old leader添加回来
 */
TEST_F(RaftConfigChangeTest, ThreeNodeRemoveLeader) {
    LogicPoolID logicPoolId = 2;
    CopysetID copysetId = 100001;
    uint64_t chunkId = 1;
    int length = kOpRequestAlignSize;
    char ch = 'a';
    int loop = 25;

    // 1. 启动3个成员的复制组
    LOG(INFO) << "start 3 chunkservers";
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

    // 2. 移除leader
    LOG(INFO) << "remove leader";
    braft::cli::CliOptions options;
    options.max_retry = 3;
    options.timeout_ms = confChangeTimeoutMs;
    Configuration conf = cluster.CopysetConf();
    butil::Status
        st1 = RemovePeer(logicPoolId, copysetId, conf, leaderPeer, options);
    ASSERT_TRUE(st1.ok());
    Peer oldLeader = leaderPeer;

    ASSERT_EQ(0, cluster.WaitLeader(&leaderPeer));
    ASSERT_EQ(0, leaderId.parse(leaderPeer.address()));
    ASSERT_STRNE(oldLeader.address().c_str(), leaderPeer.address().c_str());

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

    // 3. add回来
    conf.remove_peer(oldLeader.address());
    butil::Status
        st3 = AddPeer(logicPoolId, copysetId, conf, oldLeader, options);
    ASSERT_TRUE(st3.ok());

    ASSERT_EQ(0, cluster.WaitLeader(&leaderPeer));
    ASSERT_EQ(0, leaderId.parse(leaderPeer.address()));
    ASSERT_STRNE(oldLeader.address().c_str(), leaderPeer.address().c_str());

    WriteThenReadVerify(leaderPeer,
                        logicPoolId,
                        copysetId,
                        chunkId,
                        length,
                        ch++,
                        loop);

    // 验证3个副本数据一致性
    ::usleep(waitMultiReplicasBecomeConsistent * 1000);
    CopysetStatusVerify(peers, logicPoolId, copysetId, 3);
}

/**
 * 1. 3个节点正常启动
 * 2. 挂一个follower
 * 3. 再将leader移除掉
 * 4. follower拉起来
 */
TEST_F(RaftConfigChangeTest, ThreeNodeShutdownPeerThenRemoveLeader) {
    LogicPoolID logicPoolId = 2;
    CopysetID copysetId = 100001;
    uint64_t chunkId = 1;
    int length = kOpRequestAlignSize;
    char ch = 'a';
    int loop = 25;

    // 1. 启动3个成员的复制组
    LOG(INFO) << "start 3 chunkservers";
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
    LOG(INFO) << "shutdown 1 follower";
    std::vector<Peer> followerPeers;
    PeerCluster::GetFollwerPeers(peers, leaderPeer, &followerPeers);
    ASSERT_GE(followerPeers.size(), 1);
    Peer shutdownPeer = followerPeers[0];
    ASSERT_EQ(0, cluster.ShutdownPeer(shutdownPeer));
    WriteThenReadVerify(leaderPeer,
                        logicPoolId,
                        copysetId,
                        chunkId,
                        length,
                        ch++,
                        loop);

    // 3. 移除leader
    LOG(INFO) << "remove leader: " << leaderPeer.address();
    braft::cli::CliOptions options;
    options.max_retry = 3;
    options.timeout_ms = confChangeTimeoutMs;
    Configuration conf = cluster.CopysetConf();
    butil::Status
        st1 = RemovePeer(logicPoolId, copysetId, conf, leaderPeer, options);
    Peer oldLeader = leaderPeer;
    /**
     * 一般能够移除成功，但是因为一个follower已经down了，那么
     * leader会自动进行check term，会发现已经有大多数的follower
     * 已经失联，此时leader会主动step down，所以的request会提前
     * 返回失败，所以下面的断言会失败，但是移除本身会成功
     */
//    ASSERT_TRUE(st1.ok());
    ReadVerifyNotAvailable(leaderPeer,
                           logicPoolId,
                           copysetId,
                           chunkId,
                           length,
                           ch - 1,
                           1);

    // 4. 拉起follower
    LOG(INFO) << "restart shutdown follower";
    ASSERT_EQ(0, cluster.StartPeer(shutdownPeer,
                                   PeerCluster::PeerToId(shutdownPeer)));
    ::usleep(1.2 * electionTimeoutMs * 1000);
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
                        ch ++,
                        loop);

    // leader已经移除，所以只用验证2个副本数据一致性
    ::usleep(waitMultiReplicasBecomeConsistent * 1000);
    std::vector<Peer> newPeers;
    for (Peer peer : peers) {
        if (peer.address() != oldLeader.address()) {
            newPeers.push_back(peer);
        }
    }
    CopysetStatusVerify(newPeers, logicPoolId, copysetId, 2);
}

/**
 * 1. 3个节点正常启动
 * 2. hang一个follower
 * 3. 再将leader移除掉
 * 4. follower拉起来
 */
TEST_F(RaftConfigChangeTest, ThreeNodeHangPeerThenRemoveLeader) {
    LogicPoolID logicPoolId = 2;
    CopysetID copysetId = 100001;
    uint64_t chunkId = 1;
    int length = kOpRequestAlignSize;
    char ch = 'a';
    int loop = 25;

    // 1. 启动3个成员的复制组
    LOG(INFO) << "start 3 chunkservers";
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

    // 2. hang1个follower
    LOG(INFO) << "shutdown 1 follower";
    std::vector<Peer> followerPeers;
    PeerCluster::GetFollwerPeers(peers, leaderPeer, &followerPeers);
    ASSERT_GE(followerPeers.size(), 1);
    Peer hangPeer = followerPeers[0];
    ASSERT_EQ(0, cluster.HangPeer(hangPeer));
    WriteThenReadVerify(leaderPeer,
                        logicPoolId,
                        copysetId,
                        chunkId,
                        length,
                        ch++,
                        loop);

    // 3. 移除leader
    LOG(INFO) << "remove leader: " << leaderPeer.address();
    braft::cli::CliOptions options;
    options.max_retry = 3;
    options.timeout_ms = confChangeTimeoutMs;
    Configuration conf = cluster.CopysetConf();
    butil::Status
        st1 = RemovePeer(logicPoolId, copysetId, conf, leaderPeer, options);
    Peer oldLeader = leaderPeer;
    /**
     * 一般能够移除成功，但是因为一个follower已经down了，那么
     * leader会自动进行check term，会发现已经有大多数的follower
     * 已经失联，此时leader会主动step down，所以的request会提前
     * 返回失败，所以下面的断言会失败，但是移除本身会成功
     */
//    ASSERT_TRUE(st1.ok());
    ReadVerifyNotAvailable(leaderPeer,
                           logicPoolId,
                           copysetId,
                           chunkId,
                           length,
                           ch - 1,
                           1);

    // 4. 拉起follower
    LOG(INFO) << "restart shutdown follower";
    ASSERT_EQ(0, cluster.SignalPeer(hangPeer));
    ::usleep(1.2 * electionTimeoutMs * 1000);
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
                        ch ++,
                        loop);

    // leader已经移除，所以验证2个副本数据一致性
    ::usleep(waitMultiReplicasBecomeConsistent * 1000);
    std::vector<Peer> newPeers;
    for (Peer peer : peers) {
        if (peer.address() != oldLeader.address()) {
            newPeers.push_back(peer);
        }
    }
    CopysetStatusVerify(newPeers, logicPoolId, copysetId, 1);
}

/**
 * 1. {A、B、C} 3个节点正常启动，假设A是leader
 * 2. 挂掉B，transfer leader给B
 * 3. 拉起B，transfer leader给B
 */
TEST_F(RaftConfigChangeTest, ThreeNodeShutdownPeerThenTransferLeaderTo) {
    LogicPoolID logicPoolId = 2;
    CopysetID copysetId = 100001;
    uint64_t chunkId = 1;
    int length = kOpRequestAlignSize;
    char ch = 'a';
    int loop = 25;

    // 1. 启动3个成员的复制组
    LOG(INFO) << "start 3 chunkservers";
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
    LOG(INFO) << "shutdown 1 follower";
    std::vector<Peer> followerPeers;
    PeerCluster::GetFollwerPeers(peers, leaderPeer, &followerPeers);
    ASSERT_GE(followerPeers.size(), 1);
    Peer shutdownPeer = followerPeers[0];
    ASSERT_EQ(0, cluster.ShutdownPeer(shutdownPeer));
    WriteThenReadVerify(leaderPeer,
                        logicPoolId,
                        copysetId,
                        chunkId,
                        length,
                        ch++,
                        loop);

    // 3. transfer leader to shutdown peer
    braft::cli::CliOptions options;
    options.max_retry = 3;
    options.timeout_ms = confChangeTimeoutMs;
    Configuration conf = cluster.CopysetConf();
    butil::Status st1 =
        TransferLeader(logicPoolId, copysetId, conf, shutdownPeer, options);
    ASSERT_TRUE(st1.ok());
    ReadVerifyNotAvailable(leaderPeer,
                           logicPoolId,
                           copysetId,
                           chunkId,
                           length,
                           ch -1,
                           1);

    ::usleep(electionTimeoutMs * 1000);
    ASSERT_EQ(0, cluster.WaitLeader(&leaderPeer));
    ASSERT_EQ(0, leaderId.parse(leaderPeer.address()));
    ASSERT_STRNE(shutdownPeer.address().c_str(), leaderId.to_string().c_str());

    // 4. 拉起follower，然后再把leader transfer过去
    LOG(INFO) << "restart shutdown follower";
    ASSERT_EQ(0, cluster.StartPeer(shutdownPeer,
                                   PeerCluster::PeerToId(shutdownPeer)));

    LOG(INFO) << "transfer leader to " << shutdownPeer.address() << " again";

    const int kMaxLoop = 10;
    butil::Status status;
    LOG(INFO) << "start transfer leader to " << shutdownPeer.address();
    for (int i = 0; i < kMaxLoop; ++i) {
        status = TransferLeader(logicPoolId,
                                copysetId,
                                conf,
                                shutdownPeer,
                                options);
        if (0 == status.error_code()) {
            cluster.WaitLeader(&leaderPeer);
            if (leaderPeer.address() == shutdownPeer.address()) {
                break;
            }
        }
        LOG(INFO) << i + 1 << " th transfer leader to "
                  << shutdownPeer.address() << " failed";
        ::sleep(1);
    }

    ASSERT_STREQ(shutdownPeer.address().c_str(), leaderPeer.address().c_str());

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
                        ch ++,
                        loop);

    // 验证3个副本数据一致性
    ::usleep(waitMultiReplicasBecomeConsistent * 1000);
    CopysetStatusVerify(peers, logicPoolId, copysetId, 2);
}

/**
 * 1. {A、B、C} 3个节点正常启动，假设A是leader
 * 2. hang B，transfer leader给B
 * 3. 恢复 B，transfer leader给B
 */
TEST_F(RaftConfigChangeTest, ThreeNodeHangPeerThenTransferLeaderTo) {
    LogicPoolID logicPoolId = 2;
    CopysetID copysetId = 100001;
    uint64_t chunkId = 1;
    int length = kOpRequestAlignSize;
    char ch = 'a';
    int loop = 25;

    // 1. 启动3个成员的复制组
    LOG(INFO) << "start 3 chunkservers";
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

    // 2. hang1个follower
    LOG(INFO) << "shutdown 1 follower";
    std::vector<Peer> followerPeers;
    PeerCluster::GetFollwerPeers(peers, leaderPeer, &followerPeers);
    ASSERT_GE(followerPeers.size(), 1);
    Peer hangPeer = followerPeers[0];
    ASSERT_EQ(0, cluster.HangPeer(hangPeer));
    WriteThenReadVerify(leaderPeer,
                        logicPoolId,
                        copysetId,
                        chunkId,
                        length,
                        ch++,
                        loop);

    // 3. transfer leader to hang peer
    braft::cli::CliOptions options;
    options.max_retry = 3;
    options.timeout_ms = confChangeTimeoutMs;
    Configuration conf = cluster.CopysetConf();
    butil::Status st1 =
        TransferLeader(logicPoolId, copysetId, conf, hangPeer, options);
    ASSERT_TRUE(st1.ok());

    ::usleep(electionTimeoutMs * 1000);
    ASSERT_EQ(0, cluster.WaitLeader(&leaderPeer));
    ASSERT_EQ(0, leaderId.parse(leaderPeer.address()));
    ASSERT_STRNE(hangPeer.address().c_str(), leaderId.to_string().c_str());

    // 4. 恢复follower，然后再把leader transfer过去
    LOG(INFO) << "restart shutdown follower";
    ASSERT_EQ(0, cluster.SignalPeer(hangPeer));

    LOG(INFO) << "transfer leader to " << hangPeer.address() << " again";

    const int kMaxLoop = 10;
    butil::Status status;
    LOG(INFO) << "start transfer leader to " << hangPeer.address();
    for (int i = 0; i < kMaxLoop; ++i) {
        status = TransferLeader(logicPoolId,
                                copysetId,
                                conf,
                                hangPeer,
                                options);
        if (0 == status.error_code()) {
            cluster.WaitLeader(&leaderPeer);
            if (leaderPeer.address() == hangPeer.address()) {
                break;
            }
        }
        LOG(INFO) << i + 1 << " th transfer leader to "
                  << hangPeer.address() << " failed";
        ::sleep(1);
    }

    ASSERT_STREQ(hangPeer.address().c_str(), leaderPeer.address().c_str());

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
                        ch ++,
                        loop);

    // 验证3个副本数据一致性
    ::usleep(waitMultiReplicasBecomeConsistent * 1000);
    CopysetStatusVerify(peers, logicPoolId, copysetId, 2);
}

/**
 *
 * 1. {A、B、C} 3个节点正常启
 * 2. 挂掉一个follower
 * 3. 起一个节点D，Add D（需要额外确保通过snapshot恢复）
 * 4. remove挂掉的follower
 */
TEST_F(RaftConfigChangeTest, ThreeNodeShutdownPeerAndThenAddNewFollowerFromInstallSnapshot) {   // NOLINT
    LogicPoolID logicPoolId = 2;
    CopysetID copysetId = 100001;
    uint64_t chunkId = 1;
    int length = kOpRequestAlignSize;
    char ch = 'a';
    int loop = 25;

    // 1. 启动3个成员的复制组
    LOG(INFO) << "start 3 chunkservers";
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
    LOG(INFO) << "shutdown 1 follower";
    std::vector<Peer> followerPeers;
    PeerCluster::GetFollwerPeers(peers, leaderPeer, &followerPeers);
    ASSERT_GE(followerPeers.size(), 1);
    Peer shutdownPeer = followerPeers[0];
    ASSERT_EQ(0, cluster.ShutdownPeer(shutdownPeer));
    WriteThenReadVerify(leaderPeer,
                        logicPoolId,
                        copysetId,
                        chunkId,
                        length,
                        ch++,
                        loop);

    // wait snapshot, 通过打两次快照确保后面的恢复必须走安装快照
    LOG(INFO) << "wait snapshot 1";
    ::sleep(2 * snapshotIntervalS);
    // 再次发起 read/write
    WriteThenReadVerify(leaderPeer,
                        logicPoolId,
                        copysetId,
                        chunkId,
                        length,
                        ch ++,
                        loop);
    LOG(INFO) << "wait snapshot 2";
    ::sleep(2 * snapshotIntervalS);
    // 再次发起 read/write
    WriteThenReadVerify(leaderPeer,
                        logicPoolId,
                        copysetId,
                        chunkId,
                        length,
                        ch ++,
                        loop);

    // 3. 拉起peer4
    ASSERT_EQ(0, cluster.StartPeer(peer4,
                                   PeerCluster::PeerToId(peer4)));

    ::sleep(1);
    Configuration conf = cluster.CopysetConf();
    braft::cli::CliOptions options;
    options.max_retry = 3;
    options.timeout_ms = confChangeTimeoutMs;
    butil::Status st = AddPeer(logicPoolId, copysetId, conf, peer4, options);
    ASSERT_TRUE(st.ok()) << st.error_str();

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
                        ch ++,
                        loop);

    butil::Status
        st1 = RemovePeer(logicPoolId, copysetId, conf, shutdownPeer, options);

    ::usleep(waitMultiReplicasBecomeConsistent * 1000);
    peers.push_back(peer4);
    std::vector<Peer> newPeers;
    for (Peer peer : peers) {
        if (peer.address() != shutdownPeer.address()) {
            newPeers.push_back(peer);
        }
    }
    CopysetStatusVerify(newPeers, logicPoolId, copysetId, 3);
}

/**
 *
 * 1. {A、B、C} 3个节点正常启
 * 2. hang一个follower
 * 3. 起一个节点D，Add D（需要额外确保通过snapshot恢复）
 * 4. remove挂掉的follower
 */
TEST_F(RaftConfigChangeTest, ThreeNodeHangPeerAndThenAddNewFollowerFromInstallSnapshot) {   // NOLINT
    LogicPoolID logicPoolId = 2;
    CopysetID copysetId = 100001;
    uint64_t chunkId = 1;
    int length = kOpRequestAlignSize;
    char ch = 'a';
    int loop = 25;

    // 1. 启动3个成员的复制组
    LOG(INFO) << "start 3 chunkservers";
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
    LOG(INFO) << "shutdown 1 follower";
    std::vector<Peer> followerPeers;
    PeerCluster::GetFollwerPeers(peers, leaderPeer, &followerPeers);
    ASSERT_GE(followerPeers.size(), 1);
    Peer hangPeer = followerPeers[0];
    ASSERT_EQ(0, cluster.HangPeer(hangPeer));
    WriteThenReadVerify(leaderPeer,
                        logicPoolId,
                        copysetId,
                        chunkId,
                        length,
                        ch++,
                        loop);

    // wait snapshot, 保证能够触发安装快照
    LOG(INFO) << "wait snapshot 1";
    ::sleep(2 * snapshotIntervalS);
    // 再次发起 read/write
    WriteThenReadVerify(leaderPeer,
                        logicPoolId,
                        copysetId,
                        chunkId,
                        length,
                        ch ++,
                        loop);
    LOG(INFO) << "wait snapshot 2";
    ::sleep(2 * snapshotIntervalS);
    // 再次发起 read/write
    WriteThenReadVerify(leaderPeer,
                        logicPoolId,
                        copysetId,
                        chunkId,
                        length,
                        ch ++,
                        loop);

    // 3. 拉起peer4
    ASSERT_EQ(0, cluster.StartPeer(peer4,
                                   PeerCluster::PeerToId(peer4)));

    ::sleep(1);
    Configuration conf = cluster.CopysetConf();
    braft::cli::CliOptions options;
    options.max_retry = 3;
    options.timeout_ms = confChangeTimeoutMs;
    const int kMaxLoop = 10;
    butil::Status st = AddPeer(logicPoolId, copysetId, conf, peer4, options);
    ASSERT_TRUE(st.ok());

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
                        ch ++,
                        loop);

    butil::Status
        st1 = RemovePeer(logicPoolId, copysetId, conf, hangPeer, options);

    ::usleep(waitMultiReplicasBecomeConsistent * 1000);
    peers.push_back(peer4);
    std::vector<Peer> newPeers;
    for (Peer peer : peers) {
        if (peer.address() != hangPeer.address()) {
            newPeers.push_back(peer);
        }
    }
    CopysetStatusVerify(newPeers, logicPoolId, copysetId, 3);
}

/**
 * 1. {A、B、C} 3个节点正常启
 * 2. 挂了follower，并删除其所有raft log和数据
 * 3. 重启follower，follower能够通过数据恢复最终追上leader
 */
TEST_F(RaftConfigChangeTest, ThreeNodeRemoveDataAndThenRecoverFromInstallSnapshot) {    // NOLINT
    LogicPoolID logicPoolId = 2;
    CopysetID copysetId = 100001;
    uint64_t chunkId = 1;
    int length = kOpRequestAlignSize;
    char ch = 'a';
    int loop = 25;

    // 1. 启动3个成员的复制组
    LOG(INFO) << "start 3 chunkservers";
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
    LOG(INFO) << "shutdown 1 follower";
    std::vector<Peer> followerPeers;
    PeerCluster::GetFollwerPeers(peers, leaderPeer, &followerPeers);
    ASSERT_GE(followerPeers.size(), 1);
    Peer shutdownPeer = followerPeers[0];
    ASSERT_EQ(0, cluster.ShutdownPeer(shutdownPeer));
    WriteThenReadVerify(leaderPeer,
                        logicPoolId,
                        copysetId,
                        chunkId,
                        length,
                        ch++,
                        loop);

    // 删除此peer的数据，然后重启
    ASSERT_EQ(0,
              ::system(PeerCluster::RemoveCopysetDirCmd(shutdownPeer).c_str()));
    std::shared_ptr<LocalFileSystem>
        fs(LocalFsFactory::CreateFs(FileSystemType::EXT4, ""));
    ASSERT_FALSE(fs->DirExists(PeerCluster::CopysetDirWithoutProtocol(
        shutdownPeer)));

    // wait snapshot, 保证能够触发安装快照
    LOG(INFO) << "wait snapshot 1";
    ::sleep(2 * snapshotIntervalS);
    // 再次发起 read/write
    WriteThenReadVerify(leaderPeer,
                        logicPoolId,
                        copysetId,
                        chunkId,
                        length,
                        ch ++,
                        loop);
    LOG(INFO) << "wait snapshot 2";
    ::sleep(2 * snapshotIntervalS);
    // 再次发起 read/write
    WriteThenReadVerify(leaderPeer,
                        logicPoolId,
                        copysetId,
                        chunkId,
                        length,
                        ch ++,
                        loop);

    // 3. 拉起follower
    LOG(INFO) << "restart shutdown follower";
    ASSERT_EQ(0, cluster.StartPeer(shutdownPeer,
                                   PeerCluster::PeerToId(shutdownPeer)));

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
                        ch ++,
                        loop);

    ::usleep(3 * waitMultiReplicasBecomeConsistent * 1000);
    CopysetStatusVerify(peers, logicPoolId, copysetId, 1);
}

/**
 * 1. {A、B、C} 3个节点正常启
 * 2. 挂了follower，并删除其所有raft log
 * 3. 重启follower
 */
TEST_F(RaftConfigChangeTest, ThreeNodeRemoveRaftLogAndThenRecoverFromInstallSnapshot) {    // NOLINT
    LogicPoolID logicPoolId = 2;
    CopysetID copysetId = 100001;
    uint64_t chunkId = 1;
    int length = kOpRequestAlignSize;
    char ch = 'a';
    int loop = 25;

    // 1. 启动3个成员的复制组
    LOG(INFO) << "start 3 chunkservers";
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
    LOG(INFO) << "shutdown 1 follower";
    std::vector<Peer> followerPeers;
    PeerCluster::GetFollwerPeers(peers, leaderPeer, &followerPeers);
    ASSERT_GE(followerPeers.size(), 1);
    Peer shutdownPeer = followerPeers[0];
    ASSERT_EQ(0, cluster.ShutdownPeer(shutdownPeer));
    WriteThenReadVerify(leaderPeer,
                        logicPoolId,
                        copysetId,
                        chunkId,
                        length,
                        ch++,
                        loop);

    // 删除此peer的log，然后重启
    ::system(PeerCluster::RemoveCopysetLogDirCmd(shutdownPeer,
                                                 logicPoolId,
                                                 copysetId).c_str());
    std::shared_ptr<LocalFileSystem>
        fs(LocalFsFactory::CreateFs(FileSystemType::EXT4, ""));
    ASSERT_FALSE(fs->DirExists(PeerCluster::RemoveCopysetLogDirCmd(shutdownPeer,
                                                                   logicPoolId,
                                                                   copysetId)));

    // wait snapshot, 保证能够触发安装快照
    LOG(INFO) << "wait snapshot 1";
    ::sleep(2 * snapshotIntervalS);
    // 再次发起 read/write
    WriteThenReadVerify(leaderPeer,
                        logicPoolId,
                        copysetId,
                        chunkId,
                        length,
                        ch ++,
                        loop);
    LOG(INFO) << "wait snapshot 2";
    ::sleep(2 * snapshotIntervalS);
    // 再次发起 read/write
    WriteThenReadVerify(leaderPeer,
                        logicPoolId,
                        copysetId,
                        chunkId,
                        length,
                        ch ++,
                        loop);

    // 3. 拉起follower
    LOG(INFO) << "restart shutdown follower";
    ASSERT_EQ(0, cluster.StartPeer(shutdownPeer,
                                   PeerCluster::PeerToId(shutdownPeer)));

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
                        ch ++,
                        loop);

    ::usleep(1.6 * waitMultiReplicasBecomeConsistent * 1000);
    CopysetStatusVerify(peers, logicPoolId, copysetId, 1);
}

/**
 * 1. {A、B、C} 3个节点正常启动
 * 2. 挂了follower
 * 3. 重启恢复follower（需要额外确保通过snapshot恢复），恢复过程中挂掉leader
 *    本次install snapshot失败，但是new leader会被选出来，new leader继续给
 *    follower恢复数据，最终follower数据追上leader并一致
 */
TEST_F(RaftConfigChangeTest, ThreeNodeRecoverFollowerFromInstallSnapshotButLeaderShutdown) {    // NOLINT
    LogicPoolID logicPoolId = 2;
    CopysetID copysetId = 100001;
    uint64_t chunkId = 1;
    int length = kOpRequestAlignSize;
    char ch = 'a';
    int loop = 25;

    // 1. 启动3个成员的复制组
    LOG(INFO) << "start 3 chunkservers";
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
    LOG(INFO) << "shutdown 1 follower";
    std::vector<Peer> followerPeers;
    PeerCluster::GetFollwerPeers(peers, leaderPeer, &followerPeers);
    ASSERT_GE(followerPeers.size(), 1);
    Peer shutdownPeer = followerPeers[0];
    ASSERT_EQ(0, cluster.ShutdownPeer(shutdownPeer));
    WriteThenReadVerify(leaderPeer,
                        logicPoolId,
                        copysetId,
                        chunkId,
                        length,
                        ch++,
                        loop);

    // wait snapshot, 保证能够触发安装快照
    LOG(INFO) << "wait snapshot 1";
    ::sleep(2 * snapshotIntervalS);
    // 再次发起 read/write
    WriteThenReadVerify(leaderPeer,
                        logicPoolId,
                        copysetId,
                        chunkId,
                        length,
                        ch ++,
                        loop);
    LOG(INFO) << "wait snapshot 2";
    ::sleep(2 * snapshotIntervalS);
    // 再次发起 read/write
    WriteThenReadVerify(leaderPeer,
                        logicPoolId,
                        copysetId,
                        chunkId,
                        length,
                        ch ++,
                        loop);

    // 3. 拉起follower
    LOG(INFO) << "restart shutdown follower";
    ASSERT_EQ(0, cluster.StartPeer(shutdownPeer,
                                   PeerCluster::PeerToId(shutdownPeer)));

    // 4. 随机睡眠一段时间后，挂掉leader，模拟install snapshot的时候leader挂掉
    int sleepMs = butil::fast_rand_less_than(maxWaitInstallSnapshotMs) + 1;
    ::usleep(1000 * sleepMs);

    ASSERT_EQ(0, cluster.ShutdownPeer(leaderPeer));
    Peer oldLeader = leaderPeer;
    ReadVerifyNotAvailable(leaderPeer,
                           logicPoolId,
                           copysetId,
                           chunkId,
                           length,
                           ch - 1,
                           1);

    // 避免查到老leader
    ::usleep(1.3 * electionTimeoutMs * 1000);
    ASSERT_EQ(0, cluster.WaitLeader(&leaderPeer));
    ASSERT_EQ(0, leaderId.parse(leaderPeer.address()));
    LOG(INFO) << "new leader is: " << leaderPeer.address();

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
                        ch ++,
                        loop);

    ::usleep(waitMultiReplicasBecomeConsistent * 1000);
    std::vector<Peer> newPeers;
    for (Peer peer : peers) {
        if (peer.address() != oldLeader.address()) {
            newPeers.push_back(peer);
        }
    }
    CopysetStatusVerify(newPeers, logicPoolId, copysetId, 2);
}

/**
 * 1. {A、B、C} 3个节点正常启动
 * 2. 挂了follower
 * 3. 重启恢复follower（需要额外确保通过snapshot恢复），恢复过程中leader重启
 */
TEST_F(RaftConfigChangeTest, ThreeNodeRecoverFollowerFromInstallSnapshotButLeaderRestart) {    // NOLINT
    LogicPoolID logicPoolId = 2;
    CopysetID copysetId = 100001;
    uint64_t chunkId = 1;
    int length = kOpRequestAlignSize;
    char ch = 'a';
    int loop = 25;

    // 1. 启动3个成员的复制组
    LOG(INFO) << "start 3 chunkservers";
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
    LOG(INFO) << "shutdown 1 follower";
    std::vector<Peer> followerPeers;
    PeerCluster::GetFollwerPeers(peers, leaderPeer, &followerPeers);
    ASSERT_GE(followerPeers.size(), 1);
    Peer shutdownPeer = followerPeers[0];
    ASSERT_EQ(0, cluster.ShutdownPeer(shutdownPeer));
    WriteThenReadVerify(leaderPeer,
                        logicPoolId,
                        copysetId,
                        chunkId,
                        length,
                        ch++,
                        loop);

    // wait snapshot, 保证能够触发安装快照
    LOG(INFO) << "wait snapshot 1";
    ::sleep(2 * snapshotIntervalS);
    // 再次发起 read/write
    WriteThenReadVerify(leaderPeer,
                        logicPoolId,
                        copysetId,
                        chunkId,
                        length,
                        ch ++,
                        loop);
    LOG(INFO) << "wait snapshot 2";
    ::sleep(2 * snapshotIntervalS);
    // 再次发起 read/write
    WriteThenReadVerify(leaderPeer,
                        logicPoolId,
                        copysetId,
                        chunkId,
                        length,
                        ch ++,
                        loop);

    // 3. 拉起follower
    LOG(INFO) << "restart shutdown follower";
    ASSERT_EQ(0, cluster.StartPeer(shutdownPeer,
                                   PeerCluster::PeerToId(shutdownPeer)));

    // 4. 随机睡眠一段时间后，挂掉leader，模拟install snapshot的时候leader挂掉
    int sleepMs = butil::fast_rand_less_than(maxWaitInstallSnapshotMs) + 1;
    ::usleep(1000 * sleepMs);

    ASSERT_EQ(0, cluster.ShutdownPeer(leaderPeer));
    Peer oldLeader = leaderPeer;
    ReadVerifyNotAvailable(leaderPeer,
                           logicPoolId,
                           copysetId,
                           chunkId,
                           length,
                           ch - 1,
                           1);
    ASSERT_EQ(0, cluster.StartPeer(leaderPeer,
                                   PeerCluster::PeerToId(leaderPeer)));

    ASSERT_EQ(0, cluster.WaitLeader(&leaderPeer));
    ASSERT_EQ(0, leaderId.parse(leaderPeer.address()));
    LOG(INFO) << "new leader is: " << leaderPeer.address();

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
                        ch ++,
                        loop);

    ::usleep(waitMultiReplicasBecomeConsistent * 1000);
    std::vector<Peer> newPeers;
    for (Peer peer : peers) {
        if (peer.address() != oldLeader.address()) {
            newPeers.push_back(peer);
        }
    }
    CopysetStatusVerify(newPeers, logicPoolId, copysetId, 2);
}

/**
 * 1. {A、B、C} 3个节点正常启动
 * 2. 挂了follower
 * 3. 重启恢复follower（需要额外确保通过snapshot恢复），恢复过程中hang leader
 */
TEST_F(RaftConfigChangeTest, ThreeNodeRecoverFollowerFromInstallSnapshotButLeaderHang) {    // NOLINT
    LogicPoolID logicPoolId = 2;
    CopysetID copysetId = 100001;
    uint64_t chunkId = 1;
    int length = kOpRequestAlignSize;
    char ch = 'a';
    int loop = 25;

    // 1. 启动3个成员的复制组
    LOG(INFO) << "start 3 chunkservers";
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
    LOG(INFO) << "shutdown 1 follower";
    std::vector<Peer> followerPeers;
    PeerCluster::GetFollwerPeers(peers, leaderPeer, &followerPeers);
    ASSERT_GE(followerPeers.size(), 1);
    Peer shutdownPeer = followerPeers[0];
    ASSERT_EQ(0, cluster.ShutdownPeer(shutdownPeer));
    WriteThenReadVerify(leaderPeer,
                        logicPoolId,
                        copysetId,
                        chunkId,
                        length,
                        ch++,
                        loop);

    // wait snapshot, 保证能够触发安装快照
    LOG(INFO) << "wait snapshot 1";
    ::sleep(2 * snapshotIntervalS);
    // 再次发起 read/write
    WriteThenReadVerify(leaderPeer,
                        logicPoolId,
                        copysetId,
                        chunkId,
                        length,
                        ch ++,
                        loop);
    LOG(INFO) << "wait snapshot 2";
    ::sleep(2 * snapshotIntervalS);
    // 再次发起 read/write
    WriteThenReadVerify(leaderPeer,
                        logicPoolId,
                        copysetId,
                        chunkId,
                        length,
                        ch ++,
                        loop);

    // 3. 拉起follower
    LOG(INFO) << "restart shutdown follower";
    ASSERT_EQ(0, cluster.StartPeer(shutdownPeer,
                                   PeerCluster::PeerToId(shutdownPeer)));

    // 4. 随机睡眠一段时间后，挂掉leader，模拟install snapshot的时候leader hang
    int sleepMs = butil::fast_rand_less_than(maxWaitInstallSnapshotMs) + 1;
    ::usleep(1000 * sleepMs);

    ASSERT_EQ(0, cluster.HangPeer(leaderPeer));
    Peer oldLeader = leaderPeer;
    ReadVerifyNotAvailable(leaderPeer,
                           logicPoolId,
                           copysetId,
                           chunkId,
                           length,
                           ch - 1,
                           1);

    ASSERT_EQ(0, cluster.WaitLeader(&leaderPeer));
    ASSERT_EQ(0, leaderId.parse(leaderPeer.address()));
    LOG(INFO) << "new leader is: " << leaderPeer.address();

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
                        ch ++,
                        loop);

    ::usleep(waitMultiReplicasBecomeConsistent * 1000);
    std::vector<Peer> newPeers;
    for (Peer peer : peers) {
        if (peer.address() != oldLeader.address()) {
            newPeers.push_back(peer);
        }
    }
    CopysetStatusVerify(newPeers, logicPoolId, copysetId, 2);
}

/**
 * 1. {A、B、C} 3个节点正常启动
 * 2. 挂了follower
 * 3. 重启恢复follower（需要额外确保通过snapshot恢复），恢复过程中leader hang一会
 */
TEST_F(RaftConfigChangeTest, ThreeNodeRecoverFollowerFromInstallSnapshotButLeaderHangMoment) {    // NOLINT
    LogicPoolID logicPoolId = 2;
    CopysetID copysetId = 100001;
    uint64_t chunkId = 1;
    int length = kOpRequestAlignSize;
    char ch = 'a';
    int loop = 25;

    // 1. 启动3个成员的复制组
    LOG(INFO) << "start 3 chunkservers";
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
    LOG(INFO) << "shutdown 1 follower";
    std::vector<Peer> followerPeers;
    PeerCluster::GetFollwerPeers(peers, leaderPeer, &followerPeers);
    ASSERT_GE(followerPeers.size(), 1);
    Peer shutdownPeer = followerPeers[0];
    ASSERT_EQ(0, cluster.ShutdownPeer(shutdownPeer));
    WriteThenReadVerify(leaderPeer,
                        logicPoolId,
                        copysetId,
                        chunkId,
                        length,
                        ch++,
                        loop);

    // wait snapshot, 保证能够触发安装快照
    LOG(INFO) << "wait snapshot 1";
    ::sleep(2 * snapshotIntervalS);
    // 再次发起 read/write
    WriteThenReadVerify(leaderPeer,
                        logicPoolId,
                        copysetId,
                        chunkId,
                        length,
                        ch ++,
                        loop);
    LOG(INFO) << "wait snapshot 2";
    ::sleep(2 * snapshotIntervalS);
    // 再次发起 read/write
    WriteThenReadVerify(leaderPeer,
                        logicPoolId,
                        copysetId,
                        chunkId,
                        length,
                        ch ++,
                        loop);

    // 3. 拉起follower
    LOG(INFO) << "restart shutdown follower";
    ASSERT_EQ(0, cluster.StartPeer(shutdownPeer,
                                   PeerCluster::PeerToId(shutdownPeer)));

    // 4. 随机睡眠一段时间后，挂掉leader，模拟install snapshot的时候leader挂掉
    int sleepMs1 = butil::fast_rand_less_than(maxWaitInstallSnapshotMs) + 1;
    ::usleep(1000 * sleepMs1);

    ASSERT_EQ(0, cluster.HangPeer(leaderPeer));
    Peer oldLeader = leaderPeer;
    int sleepMs2 = butil::fast_rand_less_than(1000) + 1;
    ::usleep(1000 * sleepMs2);
    ASSERT_EQ(0, cluster.SignalPeer(leaderPeer));

    ASSERT_EQ(0, cluster.WaitLeader(&leaderPeer));
    ASSERT_EQ(0, leaderId.parse(leaderPeer.address()));
    LOG(INFO) << "new leader is: " << leaderPeer.address();

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
                        ch ++,
                        loop);

    ::usleep(1.6 * waitMultiReplicasBecomeConsistent * 1000);
    std::vector<Peer> newPeers;
    for (Peer peer : peers) {
        if (peer.address() != oldLeader.address()) {
            newPeers.push_back(peer);
        }
    }
    CopysetStatusVerify(newPeers, logicPoolId, copysetId, 1);
}

/**
 * 1. {A、B、C} 3个节点正常启动
 * 2. 挂了follower，
 * 3. 重启恢复follower（需要额外确保通过snapshot恢复），恢复过程中follower挂了
 * 4. 一段时间后拉起来
 */
TEST_F(RaftConfigChangeTest, ThreeNodeRecoverFollowerFromInstallSnapshotButFollowerShutdown) {  // NOLINT
    LogicPoolID logicPoolId = 2;
    CopysetID copysetId = 100001;
    uint64_t chunkId = 1;
    int length = kOpRequestAlignSize;
    char ch = 'a';
    int loop = 25;

    // 1. 启动3个成员的复制组
    LOG(INFO) << "start 3 chunkservers";
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
    LOG(INFO) << "shutdown 1 follower";
    std::vector<Peer> followerPeers;
    PeerCluster::GetFollwerPeers(peers, leaderPeer, &followerPeers);
    ASSERT_GE(followerPeers.size(), 1);
    Peer shutdownPeer = followerPeers[0];
    ASSERT_EQ(0, cluster.ShutdownPeer(shutdownPeer));
    WriteThenReadVerify(leaderPeer,
                        logicPoolId,
                        copysetId,
                        chunkId,
                        length,
                        ch++,
                        loop);

    // wait snapshot, 保证能够触发安装快照
    LOG(INFO) << "wait snapshot 1";
    ::sleep(2 * snapshotIntervalS);
    // 再次发起 read/write
    WriteThenReadVerify(leaderPeer,
                        logicPoolId,
                        copysetId,
                        chunkId,
                        length,
                        ch ++,
                        loop);
    LOG(INFO) << "wait snapshot 2";
    ::sleep(2 * snapshotIntervalS);
    // 再次发起 read/write
    WriteThenReadVerify(leaderPeer,
                        logicPoolId,
                        copysetId,
                        chunkId,
                        length,
                        ch ++,
                        loop);

    // 3. 拉起follower
    LOG(INFO) << "restart shutdown follower";
    ASSERT_EQ(0, cluster.StartPeer(shutdownPeer,
                                   PeerCluster::PeerToId(shutdownPeer)));

    // 4. 随机睡眠一段时间后，挂掉follower，模拟install snapshot的时候
    // follower出现问题
    int sleepMs1 = butil::fast_rand_less_than(maxWaitInstallSnapshotMs) + 1;
    ::usleep(1000 * sleepMs1);
    ASSERT_EQ(0, cluster.ShutdownPeer(shutdownPeer));

    // 5. 把follower拉来
    int sleepMs2 = butil::fast_rand_less_than(1000) + 1;
    ::usleep(1000 * sleepMs2);
    ASSERT_EQ(0, cluster.StartPeer(shutdownPeer,
                                   PeerCluster::PeerToId(shutdownPeer)));

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
                        ch ++,
                        loop);

    ::usleep(1.3 * waitMultiReplicasBecomeConsistent * 1000);
    CopysetStatusVerify(peers, logicPoolId, copysetId, 1);
}

/**
 * 1. {A、B、C} 3个节点正常启动
 * 2. 挂了follower，
 * 3. 重启恢复follower（需要额外确保通过snapshot恢复），恢复过程中follower重启了
 */
TEST_F(RaftConfigChangeTest, ThreeNodeRecoverFollowerFromInstallSnapshotButFollowerRestart) {  // NOLINT
    LogicPoolID logicPoolId = 2;
    CopysetID copysetId = 100001;
    uint64_t chunkId = 1;
    int length = kOpRequestAlignSize;
    char ch = 'a';
    int loop = 25;

    // 1. 启动3个成员的复制组
    LOG(INFO) << "start 3 chunkservers";
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
    LOG(INFO) << "shutdown 1 follower";
    std::vector<Peer> followerPeers;
    PeerCluster::GetFollwerPeers(peers, leaderPeer, &followerPeers);
    ASSERT_GE(followerPeers.size(), 1);
    Peer shutdownPeer = followerPeers[0];
    ASSERT_EQ(0, cluster.ShutdownPeer(shutdownPeer));
    WriteThenReadVerify(leaderPeer,
                        logicPoolId,
                        copysetId,
                        chunkId,
                        length,
                        ch++,
                        loop);

    // wait snapshot, 保证能够触发安装快照
    LOG(INFO) << "wait snapshot 1";
    ::sleep(2 * snapshotIntervalS);
    // 再次发起 read/write
    WriteThenReadVerify(leaderPeer,
                        logicPoolId,
                        copysetId,
                        chunkId,
                        length,
                        ch ++,
                        loop);
    LOG(INFO) << "wait snapshot 2";
    ::sleep(2 * snapshotIntervalS);
    // 再次发起 read/write
    WriteThenReadVerify(leaderPeer,
                        logicPoolId,
                        copysetId,
                        chunkId,
                        length,
                        ch ++,
                        loop);

    // 3. 拉起follower
    LOG(INFO) << "restart shutdown follower";
    ASSERT_EQ(0, cluster.StartPeer(shutdownPeer,
                                   PeerCluster::PeerToId(shutdownPeer)));

    // 4. 随机睡眠一段时间后，挂掉follower，模拟install snapshot的时候
    // follower出现问题
    int sleepMs1 = butil::fast_rand_less_than(maxWaitInstallSnapshotMs) + 1;
    ::usleep(1000 * sleepMs1);
    ASSERT_EQ(0, cluster.ShutdownPeer(shutdownPeer));

    // 5. 把follower拉来
    ASSERT_EQ(0, cluster.StartPeer(shutdownPeer,
                                   PeerCluster::PeerToId(shutdownPeer)));

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
                        ch ++,
                        loop);

    ::usleep(1.3 * waitMultiReplicasBecomeConsistent * 1000);
    CopysetStatusVerify(peers, logicPoolId, copysetId, 1);
}

/**
 * 1. {A、B、C} 3个节点正常启动
 * 2. 挂了follower，
 * 3. 重启恢复follower（需要额外确保通过snapshot恢复），恢复过程中follower hang了
 * 4. 一段时间后恢复
 */
TEST_F(RaftConfigChangeTest, ThreeNodeRecoverFollowerFromInstallSnapshotButFollowerHang) {  // NOLINT
    LogicPoolID logicPoolId = 2;
    CopysetID copysetId = 100001;
    uint64_t chunkId = 1;
    int length = kOpRequestAlignSize;
    char ch = 'a';
    int loop = 25;

    // 1. 启动3个成员的复制组
    LOG(INFO) << "start 3 chunkservers";
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
    LOG(INFO) << "shutdown 1 follower";
    std::vector<Peer> followerPeers;
    PeerCluster::GetFollwerPeers(peers, leaderPeer, &followerPeers);
    ASSERT_GE(followerPeers.size(), 1);
    Peer shutdownPeer = followerPeers[0];
    ASSERT_EQ(0, cluster.ShutdownPeer(shutdownPeer));
    WriteThenReadVerify(leaderPeer,
                        logicPoolId,
                        copysetId,
                        chunkId,
                        length,
                        ch++,
                        loop);

    // wait snapshot, 保证能够触发安装快照
    LOG(INFO) << "wait snapshot 1";
    ::sleep(2 * snapshotIntervalS);
    // 再次发起 read/write
    WriteThenReadVerify(leaderPeer,
                        logicPoolId,
                        copysetId,
                        chunkId,
                        length,
                        ch ++,
                        loop);
    LOG(INFO) << "wait snapshot 2";
    ::sleep(2 * snapshotIntervalS);
    // 再次发起 read/write
    WriteThenReadVerify(leaderPeer,
                        logicPoolId,
                        copysetId,
                        chunkId,
                        length,
                        ch ++,
                        loop);

    // 3. 拉起follower
    LOG(INFO) << "restart shutdown follower";
    ASSERT_EQ(0, cluster.StartPeer(shutdownPeer,
                                   PeerCluster::PeerToId(shutdownPeer)));

    // 4. 随机睡眠一段时间后，hang follower，模拟install snapshot的时候
    // follower出现问题
    int sleepMs1 = butil::fast_rand_less_than(maxWaitInstallSnapshotMs) + 1;
    ::usleep(1000 * sleepMs1);
    ASSERT_EQ(0, cluster.HangPeer(shutdownPeer));

    // 5. 把follower恢复
    int sleepMs2 = butil::fast_rand_less_than(1000) + 1;
    ::usleep(1000 * sleepMs2);
    ASSERT_EQ(0, cluster.SignalPeer(shutdownPeer));

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
                        ch ++,
                        loop);

    ::usleep(1.3 * waitMultiReplicasBecomeConsistent * 1000);
    CopysetStatusVerify(peers, logicPoolId, copysetId, 1);
}

/**
 * 验证3个节点的复制组，并挂掉follower
 * 1. 创建3个成员的复制组，等待leader产生，write数据，然后read出来验证一遍
 * 2. 挂掉follower
 * 3. 恢复follower
 */
TEST_F(RaftConfigChangeTest, ThreeNodeRecoverFollowerFromInstallSnapshot) {
    LogicPoolID logicPoolId = 2;
    CopysetID copysetId = 100001;
    uint64_t chunkId = 1;
    int length = kOpRequestAlignSize;
    char ch = 'a';
    int loop = 25;

    // 1. 启动3个成员的复制组
    LOG(INFO) << "start 3 chunkservers";
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
    LOG(INFO) << "shutdown 1 follower";
    std::vector<Peer> followerPeers;
    PeerCluster::GetFollwerPeers(peers, leaderPeer, &followerPeers);
    ASSERT_GE(followerPeers.size(), 1);
    Peer shutdownPeer = followerPeers[0];
    ASSERT_EQ(0, cluster.ShutdownPeer(shutdownPeer));
    WriteThenReadVerify(leaderPeer,
                        logicPoolId,
                        copysetId,
                        chunkId,
                        length,
                        ch++,
                        loop);

    // wait snapshot, 保证能够触发安装快照
    LOG(INFO) << "wait snapshot 1";
    ::sleep(2 * snapshotIntervalS);
    // 再次发起 read/write
    WriteThenReadVerify(leaderPeer,
                        logicPoolId,
                        copysetId,
                        chunkId,
                        length,
                        ch ++,
                        loop);
    LOG(INFO) << "wait snapshot 2";
    ::sleep(2 * snapshotIntervalS);
    // 再次发起 read/write
    WriteThenReadVerify(leaderPeer,
                        logicPoolId,
                        copysetId,
                        chunkId,
                        length,
                        ch ++,
                        loop);

    // 3. 拉起follower
    LOG(INFO) << "restart shutdown follower";
    ASSERT_EQ(0, cluster.StartPeer(shutdownPeer,
                                   PeerCluster::PeerToId(shutdownPeer)));

    // Wait add peer recovery, and then transfer leader to it
    ::sleep(3);
    Configuration conf = cluster.CopysetConf();
    braft::cli::CliOptions options;
    options.max_retry = 3;
    options.timeout_ms = confChangeTimeoutMs;
    const int kMaxLoop = 10;
    butil::Status status;
    LOG(INFO) << "start transfer leader to " << shutdownPeer.address();
    for (int i = 0; i < kMaxLoop; ++i) {
        status = TransferLeader(logicPoolId,
                                copysetId,
                                conf,
                                shutdownPeer,
                                options);
        if (0 == status.error_code()) {
            cluster.WaitLeader(&leaderPeer);
            if (leaderPeer.address() == shutdownPeer.address()) {
                break;
            }
        }
        LOG(INFO) << i + 1 << " th transfer leader to "
                  << shutdownPeer.address() << " failed";
        ::sleep(1);
    }

    ASSERT_EQ(0, ::strcmp(leaderPeer.address().c_str(),
                          shutdownPeer.address().c_str()));

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
                        ch ++,
                        loop);

    ::usleep(1.3 * waitMultiReplicasBecomeConsistent * 1000);
    CopysetStatusVerify(peers, logicPoolId, copysetId, 2);
}

/**
 * 1. 创建5个成员的复制组，等待leader产生，write数据，然后read出来验证一遍
 * 2. 挂掉两个follower
 * 3. 让两个follower从installsnapshot恢复
 */
TEST_F(RaftConfigChangeTest, FiveNodeRecoverTwoFollowerFromInstallSnapshot) {
    LogicPoolID logicPoolId = 2;
    CopysetID copysetId = 100001;
    uint64_t chunkId = 1;
    int length = kOpRequestAlignSize;
    char ch = 'a';
    int loop = 25;

    // 1. 启动5个成员的复制组
    LOG(INFO) << "start 5 chunkservers";
    PeerId leaderId;
    Peer leaderPeer;
    std::vector<Peer> peers;
    peers.push_back(peer1);
    peers.push_back(peer2);
    peers.push_back(peer3);
    peers.push_back(peer4);
    peers.push_back(peer5);
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
    ASSERT_EQ(0, cluster.StartPeer(peer4, PeerCluster::PeerToId(peer4)));
    ASSERT_EQ(0, cluster.StartPeer(peer5, PeerCluster::PeerToId(peer5)));

    ASSERT_EQ(0, cluster.WaitLeader(&leaderPeer));
    ASSERT_EQ(0, leaderId.parse(leaderPeer.address()));

    WriteThenReadVerify(leaderPeer,
                        logicPoolId,
                        copysetId,
                        chunkId,
                        length,
                        ch++,
                        loop);

    // 2. 挂掉2个follower
    LOG(INFO) << "shutdown 2 follower";
    std::vector<Peer> followerPeers;
    PeerCluster::GetFollwerPeers(peers, leaderPeer, &followerPeers);
    ASSERT_GE(followerPeers.size(), 4);
    Peer shutdownPeer1 = followerPeers[0];
    Peer shutdownPeer2 = followerPeers[1];
    ASSERT_EQ(0, cluster.ShutdownPeer(shutdownPeer1));
    ASSERT_EQ(0, cluster.ShutdownPeer(shutdownPeer2));
    WriteThenReadVerify(leaderPeer,
                        logicPoolId,
                        copysetId,
                        chunkId,
                        length,
                        ch++,
                        loop);

    // wait snapshot, 保证能够触发安装快照
    LOG(INFO) << "wait snapshot 1";
    ::sleep(2 * snapshotIntervalS);
    // 再次发起 read/write
    WriteThenReadVerify(leaderPeer,
                        logicPoolId,
                        copysetId,
                        chunkId,
                        length,
                        ch ++,
                        loop);
    LOG(INFO) << "wait snapshot 2";
    ::sleep(2 * snapshotIntervalS);
    // 再次发起 read/write
    WriteThenReadVerify(leaderPeer,
                        logicPoolId,
                        copysetId,
                        chunkId,
                        length,
                        ch ++,
                        loop);

    // 3. 拉起follower
    LOG(INFO) << "restart shutdown 2 follower";
    ASSERT_EQ(0, cluster.StartPeer(shutdownPeer1,
                                   PeerCluster::PeerToId(shutdownPeer1)));
    ASSERT_EQ(0, cluster.StartPeer(shutdownPeer2,
                                   PeerCluster::PeerToId(shutdownPeer2)));

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
                        ch ++,
                        loop);

    ::usleep(1.6 * waitMultiReplicasBecomeConsistent * 1000);
    CopysetStatusVerify(peers, logicPoolId, copysetId, 1);
}

}  // namespace chunkserver
}  // namespace curve
