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
 * Created Date: 19-07-09
 * Author: wudemiao
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

const char kRaftConfigChangeTestLogDir[] = "./runlog/RaftConfigChange";
const char* kFakeMdsAddr = "127.0.0.1:9080";

static constexpr uint32_t kOpRequestAlignSize = 4096;

static const char* raftConfigParam[5][16] = {
    {
        "chunkserver",
        "-chunkServerIp=127.0.0.1",
        "-chunkServerPort=9081",
        "-chunkServerStoreUri=local://./9081/",
        "-chunkServerMetaUri=local://./9081/chunkserver.dat",
        "-copySetUri=local://./9081/copysets",
        "-raftSnapshotUri=curve://./9081/copysets",
        "-raftLogUri=curve://./9081/copysets",
        "-recycleUri=local://./9081/recycler",
        "-chunkFilePoolDir=./9081/chunkfilepool/",
        "-chunkFilePoolMetaPath=./9081/chunkfilepool.meta",
        "-walFilePoolDir=./9081/walfilepool/",
        "-walFilePoolMetaPath=./9081/walfilepool.meta",
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
        "-raftSnapshotUri=curve://./9082/copysets",
        "-raftLogUri=curve://./9082/copysets",
        "-recycleUri=local://./9082/recycler",
        "-chunkFilePoolDir=./9082/chunkfilepool/",
        "-chunkFilePoolMetaPath=./9082/chunkfilepool.meta",
        "-walFilePoolDir=./9082/walfilepool/",
        "-walFilePoolMetaPath=./9082/walfilepool.meta",
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
        "-raftSnapshotUri=curve://./9083/copysets",
        "-raftLogUri=curve://./9083/copysets",
        "-recycleUri=local://./9083/recycler",
        "-chunkFilePoolDir=./9083/chunkfilepool/",
        "-chunkFilePoolMetaPath=./9083/chunkfilepool.meta",
        "-walFilePoolDir=./9083/walfilepool/",
        "-walFilePoolMetaPath=./9083/walfilepool.meta",
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
        "-raftSnapshotUri=curve://./9084/copysets",
        "-raftLogUri=curve://./9084/copysets",
        "-recycleUri=local://./9084/recycler",
        "-chunkFilePoolDir=./9084/chunkfilepool/",
        "-chunkFilePoolMetaPath=./9084/chunkfilepool.meta",
        "-walFilePoolDir=./9084/walfilepool/",
        "-walFilePoolMetaPath=./9084/walfilepool.meta",
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
        "-raftSnapshotUri=curve://./9085/copysets",
        "-raftLogUri=curve://./9085/copysets",
        "-recycleUri=local://./9085/recycler",
        "-chunkFilePoolDir=./9085/chunkfilepool/",
        "-chunkFilePoolMetaPath=./9085/chunkfilepool.meta",
        "-walFilePoolDir=./9085/walfilepool/",
        "-walFilePoolMetaPath=./9085/walfilepool.meta",
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
        std::string mkdir6("mkdir ");
        mkdir6 += kRaftConfigChangeTestLogDir;

        ::system(mkdir1.c_str());
        ::system(mkdir2.c_str());
        ::system(mkdir3.c_str());
        ::system(mkdir4.c_str());
        ::system(mkdir5.c_str());
        ::system(mkdir6.c_str());

        electionTimeoutMs = 1000;
        confChangeTimeoutMs = 6000;
        snapshotIntervalS = 5;
        maxWaitInstallSnapshotMs = 5000;
        waitMultiReplicasBecomeConsistent = 3000;

        ASSERT_TRUE(cg1.Init("9081"));
        ASSERT_TRUE(cg2.Init("9082"));
        ASSERT_TRUE(cg3.Init("9083"));
        ASSERT_TRUE(cg4.Init("9084"));
        ASSERT_TRUE(cg5.Init("9085"));
        cg1.SetKV("copyset.election_timeout_ms",
            std::to_string(electionTimeoutMs));
        cg1.SetKV("copyset.snapshot_interval_s",
            std::to_string(snapshotIntervalS));
        cg1.SetKV("chunkserver.common.logDir",
            kRaftConfigChangeTestLogDir);
        cg1.SetKV("mds.listen.addr", kFakeMdsAddr);
        cg2.SetKV("copyset.election_timeout_ms",
            std::to_string(electionTimeoutMs));
        cg2.SetKV("copyset.snapshot_interval_s",
            std::to_string(snapshotIntervalS));
        cg2.SetKV("chunkserver.common.logDir",
            kRaftConfigChangeTestLogDir);
        cg2.SetKV("mds.listen.addr", kFakeMdsAddr);
        cg3.SetKV("copyset.election_timeout_ms",
            std::to_string(electionTimeoutMs));
        cg3.SetKV("copyset.snapshot_interval_s",
            std::to_string(snapshotIntervalS));
        cg3.SetKV("chunkserver.common.logDir",
            kRaftConfigChangeTestLogDir);
        cg3.SetKV("mds.listen.addr", kFakeMdsAddr);
        cg4.SetKV("copyset.election_timeout_ms",
            std::to_string(electionTimeoutMs));
        cg4.SetKV("copyset.snapshot_interval_s",
            std::to_string(snapshotIntervalS));
        cg4.SetKV("chunkserver.common.logDir",
            kRaftConfigChangeTestLogDir);
        cg4.SetKV("mds.listen.addr", kFakeMdsAddr);
        cg5.SetKV("copyset.election_timeout_ms",
            std::to_string(electionTimeoutMs));
        cg5.SetKV("copyset.snapshot_interval_s",
            std::to_string(snapshotIntervalS));
        cg5.SetKV("chunkserver.common.logDir",
            kRaftConfigChangeTestLogDir);
        cg5.SetKV("mds.listen.addr", kFakeMdsAddr);
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

        params.push_back(const_cast<char**>(raftConfigParam[0]));
        params.push_back(const_cast<char**>(raftConfigParam[1]));
        params.push_back(const_cast<char**>(raftConfigParam[2]));
        params.push_back(const_cast<char**>(raftConfigParam[3]));
        params.push_back(const_cast<char**>(raftConfigParam[4]));
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
    //Waiting for multiple replica data to be consistent
    int waitMultiReplicasBecomeConsistent;
};



butil::AtExitManager atExitManager;

/**
 *1 3 nodes start normally
 *2 Remove a follower
 *3 Repeatedly remove the previous follower
 *4 Add it back again
 *5 Repeatedly add back
 */
TEST_F(RaftConfigChangeTest, ThreeNodeBasicAddAndRemovePeer) {
    LogicPoolID logicPoolId = 2;
    CopysetID copysetId = 100001;
    uint64_t chunkId = 1;
    int length = kOpRequestAlignSize;
    char ch = 'a';
    int loop = 25;

    //1 Start a replication group of 3 member
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
    ASSERT_EQ(0, cluster.StartFakeTopoloyService(kFakeMdsAddr));
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

    //2 Remove 1 followe
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

    //3 Duplicate removal, verify if the logic of duplicate removal is normal
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

    //4 Add, come back
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

    //5 Repeat the add and verify if the logic added repeatedly is normal
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

    //Verify data consistency across 3 replicas
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

    //1 Start a replication group of 3 members
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
    ASSERT_EQ(0, cluster.StartFakeTopoloyService(kFakeMdsAddr));
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

    //2 Hang up 1 follower
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

    //3 Remove this follower
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

    //4 Pull up the follower
    LOG(INFO) << "restart shutdown follower";
    ASSERT_EQ(0, cluster.StartPeer(shutdownPeer,
                                   PeerCluster::PeerToId(shutdownPeer)));
    ASSERT_EQ(0, cluster.WaitLeader(&leaderPeer));

    //5 Add, come back
    conf.remove_peer(shutdownPeer.address());
    butil::Status
        st2 = AddPeer(logicPoolId, copysetId, conf, shutdownPeer, options);
    ASSERT_TRUE(st2.ok()) << st2.error_str();
    //Verification of data written before read
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

    //Verify data consistency across 3 replicas
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

    //1 Start a replication group of 3 members
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
    ASSERT_EQ(0, cluster.StartFakeTopoloyService(kFakeMdsAddr));
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

    //2 Hang 1 follower
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

    //3 Remove this follower
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

    //4 Restore follower
    LOG(INFO) << "recover hang follower";
    ASSERT_EQ(0, cluster.SignalPeer(shutdownPeer));

    //5 Add, come back
    conf.remove_peer(shutdownPeer.address());
    butil::Status
        st2 = AddPeer(logicPoolId, copysetId, conf, shutdownPeer, options);
    ASSERT_TRUE(st2.ok());
    //Verification of data written before read
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

    //Verify data consistency across 3 replicas
    ::usleep(waitMultiReplicasBecomeConsistent * 1000);
    CopysetStatusVerify(peers, logicPoolId, copysetId, 2);
}

/**
 *1 3 nodes start normally
 *2 Remove leader
 *3 Add the old leader back again
 */
TEST_F(RaftConfigChangeTest, ThreeNodeRemoveLeader) {
    LogicPoolID logicPoolId = 2;
    CopysetID copysetId = 100001;
    uint64_t chunkId = 1;
    int length = kOpRequestAlignSize;
    char ch = 'a';
    int loop = 25;

    //1 Start a replication group of 3 members
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
    ASSERT_EQ(0, cluster.StartFakeTopoloyService(kFakeMdsAddr));
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

    //2 Remove leader
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

    //3 Add, come back
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

    //Verify data consistency across 3 replicas
    ::usleep(waitMultiReplicasBecomeConsistent * 1000);
    CopysetStatusVerify(peers, logicPoolId, copysetId, 3);
}

/**
 *1 3 nodes start normally
 *2 Hang a follower
 *3 Remove the leader again
 *4 Follower, pull it up
 */
TEST_F(RaftConfigChangeTest, ThreeNodeShutdownPeerThenRemoveLeader) {
    LogicPoolID logicPoolId = 2;
    CopysetID copysetId = 100001;
    uint64_t chunkId = 1;
    int length = kOpRequestAlignSize;
    char ch = 'a';
    int loop = 25;

    //1 Start a replication group of 3 members
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
    ASSERT_EQ(0, cluster.StartFakeTopoloyService(kFakeMdsAddr));
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

    //2 Hang up 1 follower
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

    //3 Remove leader
    LOG(INFO) << "remove leader: " << leaderPeer.address();
    braft::cli::CliOptions options;
    options.max_retry = 3;
    options.timeout_ms = confChangeTimeoutMs;
    Configuration conf = cluster.CopysetConf();
    butil::Status
        st1 = RemovePeer(logicPoolId, copysetId, conf, leaderPeer, options);
    Peer oldLeader = leaderPeer;
    /**
     *Usually, it can be successfully removed, but because a follower has already been down, then
     *The leader will automatically check the terms and find that there are already most followers
     *The connection has been lost, and at this point, the leader will take the initiative to step down, so the request will be advanced
     *Returns a failure, so the following assertion will fail, but removing itself will succeed
     */
//    ASSERT_TRUE(st1.ok());
    ReadVerifyNotAvailable(leaderPeer,
                           logicPoolId,
                           copysetId,
                           chunkId,
                           length,
                           ch - 1,
                           1);

    ::usleep(1000 * electionTimeoutMs * 2);
    //4 Pull up the follower
    LOG(INFO) << "restart shutdown follower";
    ASSERT_EQ(0, cluster.StartPeer(shutdownPeer,
                                   PeerCluster::PeerToId(shutdownPeer)));
    ASSERT_EQ(0, cluster.WaitLeader(&leaderPeer));
    ASSERT_EQ(0, leaderId.parse(leaderPeer.address()));

    //Verification of data written before read
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

    //The leader has been removed, so only the consistency of the data for two replicas is verified
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
 *1 3 nodes start normally
 *2 Hang a follower
 *3 Remove the leader again
 *4 Follower, pull it up
 */
TEST_F(RaftConfigChangeTest, ThreeNodeHangPeerThenRemoveLeader) {
    LogicPoolID logicPoolId = 2;
    CopysetID copysetId = 100001;
    uint64_t chunkId = 1;
    int length = kOpRequestAlignSize;
    char ch = 'a';
    int loop = 25;

    //1 Start a replication group of 3 members
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
    ASSERT_EQ(0, cluster.StartFakeTopoloyService(kFakeMdsAddr));
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

    //2 Hang1 follower
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

    //3 Remove leader
    LOG(INFO) << "remove leader: " << leaderPeer.address();
    braft::cli::CliOptions options;
    options.max_retry = 3;
    options.timeout_ms = confChangeTimeoutMs;
    Configuration conf = cluster.CopysetConf();
    butil::Status
        st1 = RemovePeer(logicPoolId, copysetId, conf, leaderPeer, options);
    Peer oldLeader = leaderPeer;
    /**
     *Usually, it can be successfully removed, but because a follower has already been down, then
     *The leader will automatically check the terms and find that there are already most followers
     *The connection has been lost, and at this point, the leader will take the initiative to step down, so the request will be advanced
     *Returns a failure, so the following assertion will fail, but removing itself will succeed
     */
//    ASSERT_TRUE(st1.ok());
    ReadVerifyNotAvailable(leaderPeer,
                           logicPoolId,
                           copysetId,
                           chunkId,
                           length,
                           ch - 1,
                           1);

    ::usleep(1000 * electionTimeoutMs * 2);
    //4 Pull up the follower
    LOG(INFO) << "restart shutdown follower";
    ASSERT_EQ(0, cluster.SignalPeer(hangPeer));
    ASSERT_EQ(0, cluster.WaitLeader(&leaderPeer));
    ASSERT_EQ(0, leaderId.parse(leaderPeer.address()));

    //Verification of data written before read
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

    //The leader has been removed, so verify the data consistency of the two replicas
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
 *1 {A, B, C} three nodes start normally, assuming A is the leader
 *2 Hang up B, transfer leader to B
 *3 Pull up B, transfer leader to B
 */
TEST_F(RaftConfigChangeTest, ThreeNodeShutdownPeerThenTransferLeaderTo) {
    LogicPoolID logicPoolId = 2;
    CopysetID copysetId = 100001;
    uint64_t chunkId = 1;
    int length = kOpRequestAlignSize;
    char ch = 'a';
    int loop = 25;

    //1 Start a replication group of 3 members
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
    ASSERT_EQ(0, cluster.StartFakeTopoloyService(kFakeMdsAddr));
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

    //2 Hang up 1 follower
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

    ASSERT_EQ(0, cluster.WaitLeader(&leaderPeer));
    ASSERT_EQ(0, leaderId.parse(leaderPeer.address()));
    ASSERT_STRNE(shutdownPeer.address().c_str(), leaderId.to_string().c_str());

    //4 Pull up the follower and then transfer the leader over
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

    //Verification of data written before read
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

    //Verify data consistency across 3 replicas
    ::usleep(waitMultiReplicasBecomeConsistent * 1000);
    CopysetStatusVerify(peers, logicPoolId, copysetId, 2);
}

/**
 *1 {A, B, C} three nodes start normally, assuming A is the leader
 *2 Hang B, transfer leader to B
 *3 Restore B, transfer leader to B
 */
TEST_F(RaftConfigChangeTest, ThreeNodeHangPeerThenTransferLeaderTo) {
    LogicPoolID logicPoolId = 2;
    CopysetID copysetId = 100001;
    uint64_t chunkId = 1;
    int length = kOpRequestAlignSize;
    char ch = 'a';
    int loop = 25;

    //1 Start a replication group of 3 members
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
    ASSERT_EQ(0, cluster.StartFakeTopoloyService(kFakeMdsAddr));
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

    //2 Hang1 follower
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

    ASSERT_EQ(0, cluster.WaitLeader(&leaderPeer));
    ASSERT_EQ(0, leaderId.parse(leaderPeer.address()));
    ASSERT_STRNE(hangPeer.address().c_str(), leaderId.to_string().c_str());

    //4 Restore the follower and then transfer the leader
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

    //Verification of data written before read
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

    //Verify data consistency across 3 replicas
    ::usleep(waitMultiReplicasBecomeConsistent * 1000);
    CopysetStatusVerify(peers, logicPoolId, copysetId, 2);
}

/**
 *
 *1 {A, B, C} three nodes start normally
 *2 Hang up a follower
 *3 Start a node D, Add D (additional ensure recovery through snapshot)
 *4 Remove the failed follower
 */
TEST_F(RaftConfigChangeTest, ThreeNodeShutdownPeerAndThenAddNewFollowerFromInstallSnapshot) {   // NOLINT
    LogicPoolID logicPoolId = 2;
    CopysetID copysetId = 100001;
    uint64_t chunkId = 1;
    int length = kOpRequestAlignSize;
    char ch = 'a';
    int loop = 25;

    //1 Start a replication group of 3 members
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
    ASSERT_EQ(0, cluster.StartFakeTopoloyService(kFakeMdsAddr));
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

    //2 Hang up 1 follower
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

    //Wait snapshot, ensuring that subsequent restores must follow the installation snapshot by taking two snapshots
    LOG(INFO) << "wait snapshot 1";
    ::sleep(2 * snapshotIntervalS);
    //Initiate read/write again
    WriteThenReadVerify(leaderPeer,
                        logicPoolId,
                        copysetId,
                        chunkId,
                        length,
                        ch ++,
                        loop);
    LOG(INFO) << "wait snapshot 2";
    ::sleep(2 * snapshotIntervalS);
    //Initiate read/write again
    WriteThenReadVerify(leaderPeer,
                        logicPoolId,
                        copysetId,
                        chunkId,
                        length,
                        ch ++,
                        loop);

    //3 Pull up peer4
    ASSERT_EQ(0, cluster.StartPeer(peer4,
                                   PeerCluster::PeerToId(peer4)));
    ASSERT_EQ(0, cluster.WaitLeader(&leaderPeer));
    ::sleep(1);
    Configuration conf = cluster.CopysetConf();
    braft::cli::CliOptions options;
    options.max_retry = 3;
    options.timeout_ms = confChangeTimeoutMs;
    butil::Status st = AddPeer(logicPoolId, copysetId, conf, peer4, options);
    ASSERT_TRUE(st.ok()) << st.error_str();

    //Verification of data written before read
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
 *1 {A, B, C} three nodes start normally
 *2 Hang a follower
 *3 Start a node D, Add D (additional ensure recovery through snapshot)
 *4 Remove the failed follower
 */
TEST_F(RaftConfigChangeTest, ThreeNodeHangPeerAndThenAddNewFollowerFromInstallSnapshot) {   // NOLINT
    LogicPoolID logicPoolId = 2;
    CopysetID copysetId = 100001;
    uint64_t chunkId = 1;
    int length = kOpRequestAlignSize;
    char ch = 'a';
    int loop = 25;

    //1 Start a replication group of 3 members
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
    ASSERT_EQ(0, cluster.StartFakeTopoloyService(kFakeMdsAddr));
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

    //2 Hang up 1 follower
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

    //Wait snapshot to ensure that the installation snapshot can be triggered
    LOG(INFO) << "wait snapshot 1";
    ::sleep(2 * snapshotIntervalS);
    //Initiate read/write again
    WriteThenReadVerify(leaderPeer,
                        logicPoolId,
                        copysetId,
                        chunkId,
                        length,
                        ch ++,
                        loop);
    LOG(INFO) << "wait snapshot 2";
    ::sleep(2 * snapshotIntervalS);
    //Initiate read/write again
    WriteThenReadVerify(leaderPeer,
                        logicPoolId,
                        copysetId,
                        chunkId,
                        length,
                        ch ++,
                        loop);

    //3 Pull up peer4
    ASSERT_EQ(0, cluster.StartPeer(peer4,
                                   PeerCluster::PeerToId(peer4)));
    ASSERT_EQ(0, cluster.WaitLeader(&leaderPeer));

    ::sleep(1);
    Configuration conf = cluster.CopysetConf();
    braft::cli::CliOptions options;
    options.max_retry = 3;
    options.timeout_ms = confChangeTimeoutMs;
    butil::Status st = AddPeer(logicPoolId, copysetId, conf, peer4, options);
    ASSERT_TRUE(st.ok());

    //Verification of data written before read
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
 *1 {A, B, C} three nodes start normally
 *2 Hang up the follower and delete all its raft logs and data
 *3 Restart the follower, and the follower can eventually catch up with the leader through data recovery
 */
TEST_F(RaftConfigChangeTest, ThreeNodeRemoveDataAndThenRecoverFromInstallSnapshot) {    // NOLINT
    LogicPoolID logicPoolId = 2;
    CopysetID copysetId = 100001;
    uint64_t chunkId = 1;
    int length = kOpRequestAlignSize;
    char ch = 'a';
    int loop = 25;

    //1 Start a replication group of 3 members
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
    ASSERT_EQ(0, cluster.StartFakeTopoloyService(kFakeMdsAddr));
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

    //2 Hang up 1 follower
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

    //Delete the data for this peer and restart it
    ASSERT_EQ(0,
              ::system(PeerCluster::RemoveCopysetDirCmd(shutdownPeer).c_str()));
    std::shared_ptr<LocalFileSystem>
        fs(LocalFsFactory::CreateFs(FileSystemType::EXT4, ""));
    ASSERT_FALSE(fs->DirExists(PeerCluster::CopysetDirWithoutProtocol(
        shutdownPeer)));

    //Wait snapshot to ensure that the installation snapshot can be triggered
    LOG(INFO) << "wait snapshot 1";
    ::sleep(2 * snapshotIntervalS);
    //Initiate read/write again
    WriteThenReadVerify(leaderPeer,
                        logicPoolId,
                        copysetId,
                        chunkId,
                        length,
                        ch ++,
                        loop);
    LOG(INFO) << "wait snapshot 2";
    ::sleep(2 * snapshotIntervalS);
    //Initiate read/write again
    WriteThenReadVerify(leaderPeer,
                        logicPoolId,
                        copysetId,
                        chunkId,
                        length,
                        ch ++,
                        loop);

    //3 Pull up the follower
    LOG(INFO) << "restart shutdown follower";
    ASSERT_EQ(0, cluster.StartPeer(shutdownPeer,
                                   PeerCluster::PeerToId(shutdownPeer)));
    ASSERT_EQ(0, cluster.WaitLeader(&leaderPeer));

    //Verification of data written before read
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
 *1 {A, B, C} three nodes start normally
 *2 Hang up the follower and delete all its raft logs
 *3 Restart follower
 */
TEST_F(RaftConfigChangeTest, ThreeNodeRemoveRaftLogAndThenRecoverFromInstallSnapshot) {    // NOLINT
    LogicPoolID logicPoolId = 2;
    CopysetID copysetId = 100001;
    uint64_t chunkId = 1;
    int length = kOpRequestAlignSize;
    char ch = 'a';
    int loop = 25;

    //1 Start a replication group of 3 members
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
    ASSERT_EQ(0, cluster.StartFakeTopoloyService(kFakeMdsAddr));
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

    //2 Hang up 1 follower
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

    //Delete the log of this peer and restart it
    ::system(PeerCluster::RemoveCopysetLogDirCmd(shutdownPeer,
                                                 logicPoolId,
                                                 copysetId).c_str());
    std::shared_ptr<LocalFileSystem>
        fs(LocalFsFactory::CreateFs(FileSystemType::EXT4, ""));
    ASSERT_FALSE(fs->DirExists(PeerCluster::RemoveCopysetLogDirCmd(shutdownPeer,
                                                                   logicPoolId,
                                                                   copysetId)));

    //Wait snapshot to ensure that the installation snapshot can be triggered
    LOG(INFO) << "wait snapshot 1";
    ::sleep(2 * snapshotIntervalS);
    //Initiate read/write again
    WriteThenReadVerify(leaderPeer,
                        logicPoolId,
                        copysetId,
                        chunkId,
                        length,
                        ch ++,
                        loop);
    LOG(INFO) << "wait snapshot 2";
    ::sleep(2 * snapshotIntervalS);
    //Initiate read/write again
    WriteThenReadVerify(leaderPeer,
                        logicPoolId,
                        copysetId,
                        chunkId,
                        length,
                        ch ++,
                        loop);

    //3 Pull up the follower
    LOG(INFO) << "restart shutdown follower";
    ASSERT_EQ(0, cluster.StartPeer(shutdownPeer,
                                   PeerCluster::PeerToId(shutdownPeer)));
    ASSERT_EQ(0, cluster.WaitLeader(&leaderPeer));

    //Verification of data written before read
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
 *1 {A, B, C} 3 nodes start normally
 *2 Hang up the follower
 *3 Restart to recover the follower (additional assurance is required to recover through snapshot), and hang the leader during the recovery process
 *    The install snapshot failed this time, but the new leader will be selected and will continue to provide
 *    The follower recovers data, and ultimately the follower data catches up with the leader and is consistent
 */
TEST_F(RaftConfigChangeTest, ThreeNodeRecoverFollowerFromInstallSnapshotButLeaderShutdown) {    // NOLINT
    LogicPoolID logicPoolId = 2;
    CopysetID copysetId = 100001;
    uint64_t chunkId = 1;
    int length = kOpRequestAlignSize;
    char ch = 'a';
    int loop = 25;

    //1 Start a replication group of 3 members
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
    ASSERT_EQ(0, cluster.StartFakeTopoloyService(kFakeMdsAddr));
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

    //2 Hang up 1 follower
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

    //Wait snapshot to ensure that the installation snapshot can be triggered
    LOG(INFO) << "wait snapshot 1";
    ::sleep(2 * snapshotIntervalS);
    //Initiate read/write again
    WriteThenReadVerify(leaderPeer,
                        logicPoolId,
                        copysetId,
                        chunkId,
                        length,
                        ch ++,
                        loop);
    LOG(INFO) << "wait snapshot 2";
    ::sleep(2 * snapshotIntervalS);
    //Initiate read/write again
    WriteThenReadVerify(leaderPeer,
                        logicPoolId,
                        copysetId,
                        chunkId,
                        length,
                        ch ++,
                        loop);

    //3 Pull up the follower
    LOG(INFO) << "restart shutdown follower";
    ASSERT_EQ(0, cluster.StartPeer(shutdownPeer,
                                   PeerCluster::PeerToId(shutdownPeer)));
    ASSERT_EQ(0, cluster.WaitLeader(&leaderPeer));

    //4 After a period of random sleep, hang up the leader and simulate the installation snapshot when the leader hangs up
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

    ASSERT_EQ(0, cluster.WaitLeader(&leaderPeer));
    ASSERT_EQ(0, leaderId.parse(leaderPeer.address()));
    LOG(INFO) << "new leader is: " << leaderPeer.address();

    //Verification of data written before read
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
 *1 {A, B, C} 3 nodes start normally
 *2 Hang up the follower
 *3 Restart to recover the follower (additional assurance is required to recover through snapshot), and restart the leader during the recovery process
 */
TEST_F(RaftConfigChangeTest, ThreeNodeRecoverFollowerFromInstallSnapshotButLeaderRestart) {    // NOLINT
    LogicPoolID logicPoolId = 2;
    CopysetID copysetId = 100001;
    uint64_t chunkId = 1;
    int length = kOpRequestAlignSize;
    char ch = 'a';
    int loop = 25;

    //1 Start a replication group of 3 members
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
    ASSERT_EQ(0, cluster.StartFakeTopoloyService(kFakeMdsAddr));
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

    //2 Hang up 1 follower
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

    //Wait snapshot to ensure that the installation snapshot can be triggered
    LOG(INFO) << "wait snapshot 1";
    ::sleep(2 * snapshotIntervalS);
    //Initiate read/write again
    WriteThenReadVerify(leaderPeer,
                        logicPoolId,
                        copysetId,
                        chunkId,
                        length,
                        ch ++,
                        loop);
    LOG(INFO) << "wait snapshot 2";
    ::sleep(2 * snapshotIntervalS);
    //Initiate read/write again
    WriteThenReadVerify(leaderPeer,
                        logicPoolId,
                        copysetId,
                        chunkId,
                        length,
                        ch ++,
                        loop);

    //3 Pull up the follower
    LOG(INFO) << "restart shutdown follower";
    ASSERT_EQ(0, cluster.StartPeer(shutdownPeer,
                                   PeerCluster::PeerToId(shutdownPeer)));
    ASSERT_EQ(0, cluster.WaitLeader(&leaderPeer));

    //4 After a period of random sleep, hang up the leader and simulate the installation snapshot when the leader hangs up
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

    //Verification of data written before read
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
 *1 {A, B, C} 3 nodes start normally
 *2 Hang up the follower
 *3 Restart to recover the follower (additional assurance is required to recover through snapshot), and hang the leader during the recovery process
 */
TEST_F(RaftConfigChangeTest, ThreeNodeRecoverFollowerFromInstallSnapshotButLeaderHang) {    // NOLINT
    LogicPoolID logicPoolId = 2;
    CopysetID copysetId = 100001;
    uint64_t chunkId = 1;
    int length = kOpRequestAlignSize;
    char ch = 'a';
    int loop = 25;

    //1 Start a replication group of 3 members
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
    ASSERT_EQ(0, cluster.StartFakeTopoloyService(kFakeMdsAddr));
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

    //2 Hang up 1 follower
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

    //Wait snapshot to ensure that the installation snapshot can be triggered
    LOG(INFO) << "wait snapshot 1";
    ::sleep(2 * snapshotIntervalS);
    //Initiate read/write again
    WriteThenReadVerify(leaderPeer,
                        logicPoolId,
                        copysetId,
                        chunkId,
                        length,
                        ch ++,
                        loop);
    LOG(INFO) << "wait snapshot 2";
    ::sleep(2 * snapshotIntervalS);
    //Initiate read/write again
    WriteThenReadVerify(leaderPeer,
                        logicPoolId,
                        copysetId,
                        chunkId,
                        length,
                        ch ++,
                        loop);

    //3 Pull up the follower
    LOG(INFO) << "restart shutdown follower";
    ASSERT_EQ(0, cluster.StartPeer(shutdownPeer,
                                   PeerCluster::PeerToId(shutdownPeer)));
    ASSERT_EQ(0, cluster.WaitLeader(&leaderPeer));

    //4 After a period of random sleep, hang up the leader and simulate the leader hang during installation snapshot
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

    //Verification of data written before read
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
 *1 {A, B, C} 3 nodes start normally
 *2 Hang up the follower
 *3 Restart to recover the follower (additional assurance is required to recover through snapshot), and during the recovery process, the leader will hang for a while
 */
TEST_F(RaftConfigChangeTest, ThreeNodeRecoverFollowerFromInstallSnapshotButLeaderHangMoment) {    // NOLINT
    LogicPoolID logicPoolId = 2;
    CopysetID copysetId = 100001;
    uint64_t chunkId = 1;
    int length = kOpRequestAlignSize;
    char ch = 'a';
    int loop = 25;

    //1 Start a replication group of 3 members
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
    ASSERT_EQ(0, cluster.StartFakeTopoloyService(kFakeMdsAddr));
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

    //2 Hang up 1 follower
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

    //Wait snapshot to ensure that the installation snapshot can be triggered
    LOG(INFO) << "wait snapshot 1";
    ::sleep(2 * snapshotIntervalS);
    //Initiate read/write again
    WriteThenReadVerify(leaderPeer,
                        logicPoolId,
                        copysetId,
                        chunkId,
                        length,
                        ch ++,
                        loop);
    LOG(INFO) << "wait snapshot 2";
    ::sleep(2 * snapshotIntervalS);
    //Initiate read/write again
    WriteThenReadVerify(leaderPeer,
                        logicPoolId,
                        copysetId,
                        chunkId,
                        length,
                        ch ++,
                        loop);

    //3 Pull up the follower
    LOG(INFO) << "restart shutdown follower";
    ASSERT_EQ(0, cluster.StartPeer(shutdownPeer,
                                   PeerCluster::PeerToId(shutdownPeer)));
    ASSERT_EQ(0, cluster.WaitLeader(&leaderPeer));

    //4 After a period of random sleep, hang up the leader and simulate the installation snapshot when the leader hangs up
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

    //Verification of data written before read
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
 *1 {A, B, C} 3 nodes start normally
 *2 Hang up the follower,
 *3 Restart to recover the follower (additional assurance is required to recover through snapshot), but the follower hung during the recovery process
 *4 After a period of time, pull it up
 */
TEST_F(RaftConfigChangeTest, ThreeNodeRecoverFollowerFromInstallSnapshotButFollowerShutdown) {  // NOLINT
    LogicPoolID logicPoolId = 2;
    CopysetID copysetId = 100001;
    uint64_t chunkId = 1;
    int length = kOpRequestAlignSize;
    char ch = 'a';
    int loop = 25;

    //1 Start a replication group of 3 members
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
    ASSERT_EQ(0, cluster.StartFakeTopoloyService(kFakeMdsAddr));
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

    //2 Hang up 1 follower
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

    //Wait snapshot to ensure that the installation snapshot can be triggered
    LOG(INFO) << "wait snapshot 1";
    ::sleep(2 * snapshotIntervalS);
    //Initiate read/write again
    WriteThenReadVerify(leaderPeer,
                        logicPoolId,
                        copysetId,
                        chunkId,
                        length,
                        ch ++,
                        loop);
    LOG(INFO) << "wait snapshot 2";
    ::sleep(2 * snapshotIntervalS);
    //Initiate read/write again
    WriteThenReadVerify(leaderPeer,
                        logicPoolId,
                        copysetId,
                        chunkId,
                        length,
                        ch ++,
                        loop);

    //3 Pull up the follower
    LOG(INFO) << "restart shutdown follower";
    ASSERT_EQ(0, cluster.StartPeer(shutdownPeer,
                                   PeerCluster::PeerToId(shutdownPeer)));

    //4 After a random period of sleep, hang up the follower and simulate the installation snapshot
    //Problem with follower
    int sleepMs1 = butil::fast_rand_less_than(maxWaitInstallSnapshotMs) + 1;
    ::usleep(1000 * sleepMs1);
    ASSERT_EQ(0, cluster.ShutdownPeer(shutdownPeer));

    //5 Bring the follower here
    int sleepMs2 = butil::fast_rand_less_than(1000) + 1;
    ::usleep(1000 * sleepMs2);
    ASSERT_EQ(0, cluster.StartPeer(shutdownPeer,
                                   PeerCluster::PeerToId(shutdownPeer)));

    ASSERT_EQ(0, cluster.WaitLeader(&leaderPeer));
    ASSERT_EQ(0, leaderId.parse(leaderPeer.address()));

    //Verification of data written before read
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
 *1 {A, B, C} 3 nodes start normally
 *2 Hang up the follower,
 *3 Restart to recover the follower (additional assurance is required to recover through snapshot). During the recovery process, the follower restarted
 */
TEST_F(RaftConfigChangeTest, ThreeNodeRecoverFollowerFromInstallSnapshotButFollowerRestart) {  // NOLINT
    LogicPoolID logicPoolId = 2;
    CopysetID copysetId = 100001;
    uint64_t chunkId = 1;
    int length = kOpRequestAlignSize;
    char ch = 'a';
    int loop = 25;

    //1 Start a replication group of 3 members
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
    ASSERT_EQ(0, cluster.StartFakeTopoloyService(kFakeMdsAddr));
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

    //2 Hang up 1 follower
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

    //Wait snapshot to ensure that the installation snapshot can be triggered
    LOG(INFO) << "wait snapshot 1";
    ::sleep(2 * snapshotIntervalS);
    //Initiate read/write again
    WriteThenReadVerify(leaderPeer,
                        logicPoolId,
                        copysetId,
                        chunkId,
                        length,
                        ch ++,
                        loop);
    LOG(INFO) << "wait snapshot 2";
    ::sleep(2 * snapshotIntervalS);
    //Initiate read/write again
    WriteThenReadVerify(leaderPeer,
                        logicPoolId,
                        copysetId,
                        chunkId,
                        length,
                        ch ++,
                        loop);

    //3 Pull up the follower
    LOG(INFO) << "restart shutdown follower";
    ASSERT_EQ(0, cluster.StartPeer(shutdownPeer,
                                   PeerCluster::PeerToId(shutdownPeer)));

    //4 After a random period of sleep, hang up the follower and simulate the installation snapshot
    //Problem with follower
    int sleepMs1 = butil::fast_rand_less_than(maxWaitInstallSnapshotMs) + 1;
    ::usleep(1000 * sleepMs1);
    ASSERT_EQ(0, cluster.ShutdownPeer(shutdownPeer));

    //5 Bring the follower here
    ASSERT_EQ(0, cluster.StartPeer(shutdownPeer,
                                   PeerCluster::PeerToId(shutdownPeer)));

    ASSERT_EQ(0, cluster.WaitLeader(&leaderPeer));
    ASSERT_EQ(0, leaderId.parse(leaderPeer.address()));

    //Verification of data written before read
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
 *1 {A, B, C} 3 nodes start normally
 *2 Hang up the follower,
 *3 Restart to recover the follower (additional assurance is required to recover through snapshot), and the follower has changed during the recovery process
 *4 Restore after a period of time
 */
TEST_F(RaftConfigChangeTest, ThreeNodeRecoverFollowerFromInstallSnapshotButFollowerHang) {  // NOLINT
    LogicPoolID logicPoolId = 2;
    CopysetID copysetId = 100001;
    uint64_t chunkId = 1;
    int length = kOpRequestAlignSize;
    char ch = 'a';
    int loop = 25;

    //1 Start a replication group of 3 members
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
    ASSERT_EQ(0, cluster.StartFakeTopoloyService(kFakeMdsAddr));
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

    //2 Hang up 1 follower
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

    //Wait snapshot to ensure that the installation snapshot can be triggered
    LOG(INFO) << "wait snapshot 1";
    ::sleep(2 * snapshotIntervalS);
    //Initiate read/write again
    WriteThenReadVerify(leaderPeer,
                        logicPoolId,
                        copysetId,
                        chunkId,
                        length,
                        ch ++,
                        loop);
    LOG(INFO) << "wait snapshot 2";
    ::sleep(2 * snapshotIntervalS);
    //Initiate read/write again
    WriteThenReadVerify(leaderPeer,
                        logicPoolId,
                        copysetId,
                        chunkId,
                        length,
                        ch ++,
                        loop);

    //3 Pull up the follower
    LOG(INFO) << "restart shutdown follower";
    ASSERT_EQ(0, cluster.StartPeer(shutdownPeer,
                                   PeerCluster::PeerToId(shutdownPeer)));

    //4 After a period of random sleep, hang the follower and simulate the installation snapshot
    //Problem with follower
    int sleepMs1 = butil::fast_rand_less_than(maxWaitInstallSnapshotMs) + 1;
    ::usleep(1000 * sleepMs1);
    ASSERT_EQ(0, cluster.HangPeer(shutdownPeer));

    //5 Restore the follower
    int sleepMs2 = butil::fast_rand_less_than(1000) + 1;
    ::usleep(1000 * sleepMs2);
    ASSERT_EQ(0, cluster.SignalPeer(shutdownPeer));

    ASSERT_EQ(0, cluster.WaitLeader(&leaderPeer));
    ASSERT_EQ(0, leaderId.parse(leaderPeer.address()));

    //Verification of data written before read
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
 *Verify the replication groups of three nodes and hang the follower
 *1 Create a replication group of 3 members, wait for the leader to generate, write the data, and then read it out for verification
 *2 Hang up the follower
 *3 Restore follower
 */
TEST_F(RaftConfigChangeTest, ThreeNodeRecoverFollowerFromInstallSnapshot) {
    LogicPoolID logicPoolId = 2;
    CopysetID copysetId = 100001;
    uint64_t chunkId = 1;
    int length = kOpRequestAlignSize;
    char ch = 'a';
    int loop = 25;

    //1 Start a replication group of 3 members
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
    ASSERT_EQ(0, cluster.StartFakeTopoloyService(kFakeMdsAddr));
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

    //2 Hang up 1 follower
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

    //Wait snapshot to ensure that the installation snapshot can be triggered
    LOG(INFO) << "wait snapshot 1";
    ::sleep(2 * snapshotIntervalS);
    //Initiate read/write again
    WriteThenReadVerify(leaderPeer,
                        logicPoolId,
                        copysetId,
                        chunkId,
                        length,
                        ch ++,
                        loop);
    LOG(INFO) << "wait snapshot 2";
    ::sleep(2 * snapshotIntervalS);
    //Initiate read/write again
    WriteThenReadVerify(leaderPeer,
                        logicPoolId,
                        copysetId,
                        chunkId,
                        length,
                        ch ++,
                        loop);

    //3 Pull up the follower
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

    //Verification of data written before read
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
 *1 Create a replication group of 5 members, wait for the leader to generate, write the data, and then read it out for verification
 *2 Hang up two followers
 *3 Restore two followers from installsnapshot
 */
TEST_F(RaftConfigChangeTest, FiveNodeRecoverTwoFollowerFromInstallSnapshot) {
    LogicPoolID logicPoolId = 2;
    CopysetID copysetId = 100001;
    uint64_t chunkId = 1;
    int length = kOpRequestAlignSize;
    char ch = 'a';
    int loop = 25;

    //1 Start a replication group of 5 member
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
    ASSERT_EQ(0, cluster.StartFakeTopoloyService(kFakeMdsAddr));
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

    //2 Hang up 2 followers
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

    //Wait snapshot to ensure that the installation snapshot can be triggered
    LOG(INFO) << "wait snapshot 1";
    ::sleep(2 * snapshotIntervalS);
    //Initiate read/write again
    WriteThenReadVerify(leaderPeer,
                        logicPoolId,
                        copysetId,
                        chunkId,
                        length,
                        ch ++,
                        loop);
    LOG(INFO) << "wait snapshot 2";
    ::sleep(2 * snapshotIntervalS);
    //Initiate read/write again
    WriteThenReadVerify(leaderPeer,
                        logicPoolId,
                        copysetId,
                        chunkId,
                        length,
                        ch ++,
                        loop);

    //3 Pull up the follower
    LOG(INFO) << "restart shutdown 2 follower";
    ASSERT_EQ(0, cluster.StartPeer(shutdownPeer1,
                                   PeerCluster::PeerToId(shutdownPeer1)));
    ASSERT_EQ(0, cluster.StartPeer(shutdownPeer2,
                                   PeerCluster::PeerToId(shutdownPeer2)));
    ASSERT_EQ(0, cluster.WaitLeader(&leaderPeer));

    //Verification of data written before read
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
 *Verify the replication groups {A, B, C} of three nodes and hang the follower
 *1 Create a replication group of 3 members, wait for the leader to generate, write the data, and then read it out for verification
 *2 Hang up the follower
 *3 Change the configuration to {A, B, D}
 *4 Transfer leader to D and read data validation
 */
TEST_F(RaftConfigChangeTest, ThreeNodeKillFollowerThenChangePeers) {
    LogicPoolID logicPoolId = 2;
    CopysetID copysetId = 100001;
    uint64_t chunkId = 1;
    int length = kOpRequestAlignSize;
    char ch = 'a';
    int loop = 25;

    //1 Start a replication group of 3 members
    LOG(INFO) << "start 3 chunkservers";
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
    ASSERT_EQ(0, cluster.StartFakeTopoloyService(kFakeMdsAddr));
    cluster.SetElectionTimeoutMs(electionTimeoutMs);
    cluster.SetsnapshotIntervalS(snapshotIntervalS);
    ASSERT_EQ(0, cluster.StartPeer(peer1, PeerCluster::PeerToId(peer1)));
    ASSERT_EQ(0, cluster.StartPeer(peer2, PeerCluster::PeerToId(peer2)));
    ASSERT_EQ(0, cluster.StartPeer(peer3, PeerCluster::PeerToId(peer3)));

    ASSERT_EQ(0, cluster.WaitLeader(&leaderPeer));

    WriteThenReadVerify(leaderPeer,
                        logicPoolId,
                        copysetId,
                        chunkId,
                        length,
                        ch++,   // a
                        loop);

    //2 Hang up 1 follower
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
                        ch++,   // b
                        loop);

    //3. Pull up Peer4 and change the configuration
    ASSERT_EQ(0, cluster.StartPeer(peer4, PeerCluster::PeerToId(peer4)));
    ::sleep(2);

    Configuration conf = cluster.CopysetConf();
    braft::cli::CliOptions options;
    options.max_retry = 3;
    options.timeout_ms = confChangeTimeoutMs;
    Configuration newConf = conf;
    newConf.remove_peer(PeerId(shutdownPeer.address()));
    newConf.add_peer(PeerId(peer4.address()));
    butil::Status st = ChangePeers(logicPoolId,
                                   copysetId,
                                   conf,
                                   newConf,
                                   options);
    ASSERT_TRUE(st.ok()) << st.error_str();

    ASSERT_EQ(0, cluster.WaitLeader(&leaderPeer));

    //Verification of data written before read
    ReadVerify(leaderPeer,
               logicPoolId,
               copysetId,
               chunkId,
               length,
               ch - 1,  // b
               loop);

    //Transfer leader to newly added node
    TransferLeaderAssertSuccess(&cluster, peer4, options);
    leaderPeer = peer4;
    //Verification of data written before read
    ReadVerify(leaderPeer,
               logicPoolId,
               copysetId,
               chunkId,
               length,
               ch - 1,  // b
               loop);

    ::usleep(1.3 * waitMultiReplicasBecomeConsistent * 1000);
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
 *Verify the replication groups {A, B, C} of three nodes and hang the follower
 *1 Create a replication group of 3 members, wait for the leader to generate, write the data, and then read it out for verification
 *2 Hang follower
 *3 Change the configuration to {A, B, D}
 *4 Transfer leader to D and read data validation
 */
TEST_F(RaftConfigChangeTest, ThreeNodeHangFollowerThenChangePeers) {
    LogicPoolID logicPoolId = 2;
    CopysetID copysetId = 100001;
    uint64_t chunkId = 1;
    int length = kOpRequestAlignSize;
    char ch = 'a';
    int loop = 25;

    //1 Start a replication group of 3 members
    LOG(INFO) << "start 3 chunkservers";
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
    ASSERT_EQ(0, cluster.StartFakeTopoloyService(kFakeMdsAddr));
    cluster.SetElectionTimeoutMs(electionTimeoutMs);
    cluster.SetsnapshotIntervalS(snapshotIntervalS);
    ASSERT_EQ(0, cluster.StartPeer(peer1, PeerCluster::PeerToId(peer1)));
    ASSERT_EQ(0, cluster.StartPeer(peer2, PeerCluster::PeerToId(peer2)));
    ASSERT_EQ(0, cluster.StartPeer(peer3, PeerCluster::PeerToId(peer3)));

    ASSERT_EQ(0, cluster.WaitLeader(&leaderPeer));

    WriteThenReadVerify(leaderPeer,
                        logicPoolId,
                        copysetId,
                        chunkId,
                        length,
                        ch++,   // a
                        loop);

    //2 Hang up 1 follower
    LOG(INFO) << "hang 1 follower";
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
                        ch++,   // b
                        loop);

    //3. Pull up Peer4 and change the configuration
    ASSERT_EQ(0, cluster.StartPeer(peer4, PeerCluster::PeerToId(peer4)));
    ::sleep(2);

    Configuration conf = cluster.CopysetConf();
    braft::cli::CliOptions options;
    options.max_retry = 3;
    options.timeout_ms = confChangeTimeoutMs;
    Configuration newConf = conf;
    newConf.remove_peer(PeerId(hangPeer.address()));
    newConf.add_peer(PeerId(peer4.address()));
    butil::Status st = ChangePeers(logicPoolId,
                                   copysetId,
                                   conf,
                                   newConf,
                                   options);
    ASSERT_TRUE(st.ok()) << st.error_str();

    ASSERT_EQ(0, cluster.WaitLeader(&leaderPeer));

    //Verification of data written before read
    ReadVerify(leaderPeer,
               logicPoolId,
               copysetId,
               chunkId,
               length,
               ch - 1,  // b
               loop);

    //Transfer leader to newly added node
    TransferLeaderAssertSuccess(&cluster, peer4, options);
    leaderPeer = peer4;
    //Verification of data written before read
    ReadVerify(leaderPeer,
               logicPoolId,
               copysetId,
               chunkId,
               length,
               ch - 1,  // b
               loop);

    ::usleep(1.3 * waitMultiReplicasBecomeConsistent * 1000);
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
 *Verify the replication groups {A, B, C} of three nodes and hang the leader
 *1 Create a replication group of 3 members, wait for the leader to generate, write the data, and then read it out for verification
 *2 Hang up the leader
 *3 Change the configuration to {A, B, D}
 *4 Transfer leader to D and read data validation
 */
TEST_F(RaftConfigChangeTest, ThreeNodeKillLeaderThenChangePeers) {
    LogicPoolID logicPoolId = 2;
    CopysetID copysetId = 100001;
    uint64_t chunkId = 1;
    int length = kOpRequestAlignSize;
    char ch = 'a';
    int loop = 25;

    //1 Start a replication group of 3 members
    LOG(INFO) << "start 3 chunkservers";
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
    ASSERT_EQ(0, cluster.StartFakeTopoloyService(kFakeMdsAddr));
    cluster.SetElectionTimeoutMs(electionTimeoutMs);
    cluster.SetsnapshotIntervalS(snapshotIntervalS);
    ASSERT_EQ(0, cluster.StartPeer(peer1, PeerCluster::PeerToId(peer1)));
    ASSERT_EQ(0, cluster.StartPeer(peer2, PeerCluster::PeerToId(peer2)));
    ASSERT_EQ(0, cluster.StartPeer(peer3, PeerCluster::PeerToId(peer3)));

    ASSERT_EQ(0, cluster.WaitLeader(&leaderPeer));

    WriteThenReadVerify(leaderPeer,
                        logicPoolId,
                        copysetId,
                        chunkId,
                        length,
                        ch++,   // a
                        loop);

    //2 Hang up the leader
    LOG(INFO) << "shutdown 1 leader";
    Peer shutdownPeer = leaderPeer;
    ASSERT_EQ(0, cluster.ShutdownPeer(shutdownPeer));
    //Waiting for a new leader to be generated
    ASSERT_EQ(0, cluster.WaitLeader(&leaderPeer));
    WriteThenReadVerify(leaderPeer,
                        logicPoolId,
                        copysetId,
                        chunkId,
                        length,
                        ch++,   // b
                        loop);

    //3. Pull up Peer4 and change the configuration
    ASSERT_EQ(0, cluster.StartPeer(peer4, PeerCluster::PeerToId(peer4)));
    ::sleep(2);

    Configuration conf = cluster.CopysetConf();
    braft::cli::CliOptions options;
    options.max_retry = 3;
    options.timeout_ms = confChangeTimeoutMs;
    Configuration newConf = conf;
    newConf.remove_peer(PeerId(shutdownPeer.address()));
    newConf.add_peer(PeerId(peer4.address()));
    butil::Status st = ChangePeers(logicPoolId,
                                   copysetId,
                                   conf,
                                   newConf,
                                   options);
    ASSERT_TRUE(st.ok()) << st.error_str();

    ASSERT_EQ(0, cluster.WaitLeader(&leaderPeer));

    //Verification of data written before read
    ReadVerify(leaderPeer,
               logicPoolId,
               copysetId,
               chunkId,
               length,
               ch - 1,  // b
               loop);

    //Transfer leader to newly added node
    TransferLeaderAssertSuccess(&cluster, peer4, options);
    leaderPeer = peer4;
    //Verification of data written before read
    ReadVerify(leaderPeer,
               logicPoolId,
               copysetId,
               chunkId,
               length,
               ch - 1,  // b
               loop);

    ::usleep(1.3 * waitMultiReplicasBecomeConsistent * 1000);
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
 *Verify the replication groups {A, B, C} of three nodes and hang the leader
 *1 Create a replication group of 3 members, wait for the leader to generate, write the data, and then read it out for verification
 *2 Hang leader
 *3 Change the configuration to {A, B, D}
 *4 Transfer leader to D and read data validation
 */
TEST_F(RaftConfigChangeTest, ThreeNodeHangLeaderThenChangePeers) {
    LogicPoolID logicPoolId = 2;
    CopysetID copysetId = 100001;
    uint64_t chunkId = 1;
    int length = kOpRequestAlignSize;
    char ch = 'a';
    int loop = 25;

    //1 Start a replication group of 3 members
    LOG(INFO) << "start 3 chunkservers";
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
    ASSERT_EQ(0, cluster.StartFakeTopoloyService(kFakeMdsAddr));
    cluster.SetElectionTimeoutMs(electionTimeoutMs);
    cluster.SetsnapshotIntervalS(snapshotIntervalS);
    ASSERT_EQ(0, cluster.StartPeer(peer1, PeerCluster::PeerToId(peer1)));
    ASSERT_EQ(0, cluster.StartPeer(peer2, PeerCluster::PeerToId(peer2)));
    ASSERT_EQ(0, cluster.StartPeer(peer3, PeerCluster::PeerToId(peer3)));

    ASSERT_EQ(0, cluster.WaitLeader(&leaderPeer));

    WriteThenReadVerify(leaderPeer,
                        logicPoolId,
                        copysetId,
                        chunkId,
                        length,
                        ch++,   // a
                        loop);

    //2 Hang up 1 follower
    LOG(INFO) << "hang 1 leader";
    Peer hangPeer = leaderPeer;
    ASSERT_EQ(0, cluster.HangPeer(hangPeer));
    //Waiting for a new leader to be generated
    ASSERT_EQ(0, cluster.WaitLeader(&leaderPeer));
    WriteThenReadVerify(leaderPeer,
                        logicPoolId,
                        copysetId,
                        chunkId,
                        length,
                        ch++,   // b
                        loop);

    //3. Pull up Peer4 and change the configuration
    ASSERT_EQ(0, cluster.StartPeer(peer4, PeerCluster::PeerToId(peer4)));
    ::sleep(2);

    Configuration conf = cluster.CopysetConf();
    braft::cli::CliOptions options;
    options.max_retry = 3;
    options.timeout_ms = confChangeTimeoutMs;
    Configuration newConf = conf;
    newConf.remove_peer(PeerId(hangPeer.address()));
    newConf.add_peer(PeerId(peer4.address()));
    butil::Status st = ChangePeers(logicPoolId,
                                   copysetId,
                                   conf,
                                   newConf,
                                   options);
    ASSERT_TRUE(st.ok()) << st.error_str();

    ASSERT_EQ(0, cluster.WaitLeader(&leaderPeer));

    //Verification of data written before read
    ReadVerify(leaderPeer,
               logicPoolId,
               copysetId,
               chunkId,
               length,
               ch - 1,  // b
               loop);

    //Transfer leader to newly added node
    TransferLeaderAssertSuccess(&cluster, peer4, options);
    leaderPeer = peer4;
    //Verification of data written before read
    ReadVerify(leaderPeer,
               logicPoolId,
               copysetId,
               chunkId,
               length,
               ch - 1,  // b
               loop);

    ::usleep(1.3 * waitMultiReplicasBecomeConsistent * 1000);
    peers.push_back(peer4);
    std::vector<Peer> newPeers;
    for (Peer peer : peers) {
        if (peer.address() != hangPeer.address()) {
            newPeers.push_back(peer);
        }
    }
    CopysetStatusVerify(newPeers, logicPoolId, copysetId, 3);
}

}  // namespace chunkserver
}  // namespace curve
