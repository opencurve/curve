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
 * Created Date: 19-06-13
 * Author: wudemiao
 */

#include <brpc/channel.h>
#include <butil/at_exit.h>
#include <gtest/gtest.h>

#include <map>
#include <vector>

#include "src/chunkserver/cli.h"
#include "src/chunkserver/copyset_node_manager.h"
#include "src/fs/fs_common.h"
#include "src/fs/local_filesystem.h"
#include "test/integration/common/config_generator.h"
#include "test/integration/common/peer_cluster.h"

namespace curve {
namespace chunkserver {

using curve::fs::FileSystemType;
using curve::fs::LocalFileSystem;
using curve::fs::LocalFsFactory;

const char kRaftLogRepTestLogDir[] = "./runlog/RaftLogRep";
const char* kFakeMdsAddr = "127.0.0.1:9070";
static constexpr uint32_t kOpRequestAlignSize = 4096;

static const char* raftLogParam[5][16] = {
    {"chunkserver", "-chunkServerIp=127.0.0.1", "-chunkServerPort=9071",
     "-chunkServerStoreUri=local://./9071/",
     "-chunkServerMetaUri=local://./9071/chunkserver.dat",
     "-copySetUri=local://./9071/copysets",
     "-raftSnapshotUri=curve://./9071/copysets",
     "-raftLogUri=curve://./9071/copysets",
     "-recycleUri=local://./9071/recycler",
     "-chunkFilePoolDir=./9071/chunkfilepool/",
     "-chunkFilePoolMetaPath=./9071/chunkfilepool.meta",
     "-walFilePoolDir=./9071/walfilepool/",
     "-walFilePoolMetaPath=./9071/walfilepool.meta",
     "-conf=./9071/chunkserver.conf", "-raft_sync_segments=true", NULL},
    {"chunkserver", "-chunkServerIp=127.0.0.1", "-chunkServerPort=9072",
     "-chunkServerStoreUri=local://./9072/",
     "-chunkServerMetaUri=local://./9072/chunkserver.dat",
     "-copySetUri=local://./9072/copysets",
     "-raftSnapshotUri=curve://./9072/copysets",
     "-raftLogUri=curve://./9072/copysets",
     "-recycleUri=local://./9072/recycler",
     "-chunkFilePoolDir=./9072/chunkfilepool/",
     "-chunkFilePoolMetaPath=./9072/chunkfilepool.meta",
     "-walFilePoolDir=./9072/walfilepool/",
     "-walFilePoolMetaPath=./9072/walfilepool.meta",
     "-conf=./9072/chunkserver.conf", "-raft_sync_segments=true", NULL},
    {"chunkserver", "-chunkServerIp=127.0.0.1", "-chunkServerPort=9073",
     "-chunkServerStoreUri=local://./9073/",
     "-chunkServerMetaUri=local://./9073/chunkserver.dat",
     "-copySetUri=local://./9073/copysets",
     "-raftSnapshotUri=curve://./9073/copysets",
     "-raftLogUri=curve://./9073/copysets",
     "-recycleUri=local://./9073/recycler",
     "-chunkFilePoolDir=./9073/chunkfilepool/",
     "-chunkFilePoolMetaPath=./9073/chunkfilepool.meta",
     "-walFilePoolDir=./9073/walfilepool/",
     "-walFilePoolMetaPath=./9073/walfilepool.meta",
     "-conf=./9073/chunkserver.conf", "-raft_sync_segments=true", NULL},
    {"chunkserver", "-chunkServerIp=127.0.0.1", "-chunkServerPort=9074",
     "-chunkServerStoreUri=local://./9074/",
     "-chunkServerMetaUri=local://./9074/chunkserver.dat",
     "-copySetUri=local://./9074/copysets",
     "-raftSnapshotUri=curve://./9074/copysets",
     "-raftLogUri=curve://./9074/copysets",
     "-recycleUri=local://./9074/recycler",
     "-chunkFilePoolDir=./9074/chunkfilepool/",
     "-chunkFilePoolMetaPath=./9074/chunkfilepool.meta",
     "-walFilePoolDir=./9074/walfilepool/",
     "-walFilePoolMetaPath=./9074/walfilepool.meta",
     "-conf=./9074/chunkserver.conf", "-raft_sync_segments=true", NULL},
    {"chunkserver", "-chunkServerIp=127.0.0.1", "-chunkServerPort=9075",
     "-chunkServerStoreUri=local://./9075/",
     "-chunkServerMetaUri=local://./9075/chunkserver.dat",
     "-copySetUri=local://./9075/copysets",
     "-raftSnapshotUri=curve://./9075/copysets",
     "-raftLogUri=curve://./9075/copysets",
     "-recycleUri=local://./9075/recycler",
     "-chunkFilePoolDir=./9075/chunkfilepool/",
     "-chunkFilePoolMetaPath=./9075/chunkfilepool.meta",
     "-walFilePoolDir=./9075/walfilepool/",
     "-walFilePoolMetaPath=./9075/walfilepool.meta",
     "-conf=./9075/chunkserver.conf", "-raft_sync_segments=true", NULL},
};

class RaftLogReplicationTest : public testing::Test {
 protected:
    virtual void SetUp() {
        peer1.set_address("127.0.0.1:9071:0");
        peer2.set_address("127.0.0.1:9072:0");
        peer3.set_address("127.0.0.1:9073:0");
        peer4.set_address("127.0.0.1:9074:0");
        peer5.set_address("127.0.0.1:9075:0");

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
        mkdir6 += kRaftLogRepTestLogDir;

        ::system(mkdir1.c_str());
        ::system(mkdir2.c_str());
        ::system(mkdir3.c_str());
        ::system(mkdir4.c_str());
        ::system(mkdir5.c_str());
        ::system(mkdir6.c_str());

        electionTimeoutMs = 1000;
        snapshotIntervalS = 20;
        waitMultiReplicasBecomeConsistent = 3000;

        ASSERT_TRUE(cg1.Init("9071"));
        ASSERT_TRUE(cg2.Init("9072"));
        ASSERT_TRUE(cg3.Init("9073"));
        ASSERT_TRUE(cg4.Init("9074"));
        ASSERT_TRUE(cg5.Init("9075"));
        cg1.SetKV("copyset.election_timeout_ms",
                  std::to_string(electionTimeoutMs));
        cg1.SetKV("copyset.snapshot_interval_s",
                  std::to_string(snapshotIntervalS));
        cg1.SetKV("chunkserver.common.logDir", kRaftLogRepTestLogDir);
        cg1.SetKV("mds.listen.addr", kFakeMdsAddr);
        cg2.SetKV("copyset.election_timeout_ms",
                  std::to_string(electionTimeoutMs));
        cg2.SetKV("copyset.snapshot_interval_s",
                  std::to_string(snapshotIntervalS));
        cg2.SetKV("chunkserver.common.logDir", kRaftLogRepTestLogDir);
        cg2.SetKV("mds.listen.addr", kFakeMdsAddr);
        cg3.SetKV("copyset.election_timeout_ms",
                  std::to_string(electionTimeoutMs));
        cg3.SetKV("copyset.snapshot_interval_s",
                  std::to_string(snapshotIntervalS));
        cg3.SetKV("chunkserver.common.logDir", kRaftLogRepTestLogDir);
        cg3.SetKV("mds.listen.addr", kFakeMdsAddr);
        cg4.SetKV("copyset.election_timeout_ms",
                  std::to_string(electionTimeoutMs));
        cg4.SetKV("copyset.snapshot_interval_s",
                  std::to_string(snapshotIntervalS));
        cg4.SetKV("chunkserver.common.logDir", kRaftLogRepTestLogDir);
        cg4.SetKV("mds.listen.addr", kFakeMdsAddr);
        cg5.SetKV("copyset.election_timeout_ms",
                  std::to_string(electionTimeoutMs));
        cg5.SetKV("copyset.snapshot_interval_s",
                  std::to_string(snapshotIntervalS));
        cg5.SetKV("chunkserver.common.logDir", kRaftLogRepTestLogDir);
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

        params.push_back(const_cast<char**>(raftLogParam[0]));
        params.push_back(const_cast<char**>(raftLogParam[1]));
        params.push_back(const_cast<char**>(raftLogParam[2]));
        params.push_back(const_cast<char**>(raftLogParam[3]));
        params.push_back(const_cast<char**>(raftLogParam[4]));
    }
    virtual void TearDown() {
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

        // wait for process exit
        ::usleep(1000 * 1000);
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
    int snapshotIntervalS;
    std::map<int, int> paramsIndexs;
    std::vector<char**> params;
    // Waiting for multiple replica data to be consistent
    int waitMultiReplicasBecomeConsistent;
};

butil::AtExitManager atExitManager;

/**
 * Validate replication groups for 3 nodes and test implicit commit
 * 1. Create a replication group of 3 members, wait for the leader to generate,
 * write the data, and then read it out for verification
 * 2. Hang up 2 followers
 * 3. Wait for step down
 * 3. Pull up 1 follower
 */
TEST_F(RaftLogReplicationTest, ThreeNodeImplicitCommit) {
    LogicPoolID logicPoolId = 2;
    CopysetID copysetId = 100001;
    uint64_t chunkId = 1;
    int length = kOpRequestAlignSize;
    char ch = 'a';
    int loop = 10;

    // 1. Start a replication group of 3 members
    PeerId leaderId;
    Peer leaderPeer;
    std::vector<Peer> peers;
    peers.push_back(peer1);
    peers.push_back(peer2);
    peers.push_back(peer3);
    PeerCluster cluster("InitShutdown-cluster", logicPoolId, copysetId, peers,
                        params, paramsIndexs);
    ASSERT_EQ(0, cluster.StartFakeTopoloyService(kFakeMdsAddr));
    cluster.SetElectionTimeoutMs(electionTimeoutMs);
    cluster.SetsnapshotIntervalS(snapshotIntervalS);
    ASSERT_EQ(0, cluster.StartPeer(peer1, PeerCluster::PeerToId(peer1)));
    ASSERT_EQ(0, cluster.StartPeer(peer2, PeerCluster::PeerToId(peer2)));
    ASSERT_EQ(0, cluster.StartPeer(peer3, PeerCluster::PeerToId(peer3)));

    ASSERT_EQ(0, cluster.WaitLeader(&leaderPeer));
    ASSERT_EQ(0, leaderId.parse(leaderPeer.address()));

    WriteThenReadVerify(leaderPeer, logicPoolId, copysetId, chunkId, length,
                        ch++, loop);

    // 2. Hang 2 Followers
    std::vector<Peer> followerPeers;
    PeerCluster::GetFollwerPeers(peers, leaderPeer, &followerPeers);
    ASSERT_GE(followerPeers.size(), 2);
    ASSERT_EQ(0, cluster.ShutdownPeer(followerPeers[0]));
    ASSERT_EQ(0, cluster.ShutdownPeer(followerPeers[1]));
    WriteVerifyNotAvailable(leaderPeer, logicPoolId, copysetId, chunkId, length,
                            ch++, 1);

    // 3. Wait for step down, wait for 2 elections to timeout, ensure a certain
    // step down
    ::usleep(1000 * electionTimeoutMs * 2);
    ReadVerifyNotAvailable(leaderPeer, logicPoolId, copysetId, chunkId, length,
                           ch - 1, 1);

    // 4. Pull up 1 follower
    ASSERT_EQ(0, cluster.StartPeer(followerPeers[0],
                                   PeerCluster::PeerToId(followerPeers[0])));
    Peer newLeader;
    ASSERT_EQ(0, cluster.WaitLeader(&newLeader));
    ASSERT_EQ(0, leaderId.parse(leaderPeer.address()));
    // new leader is an old leader
    ASSERT_STREQ(leaderPeer.address().c_str(), newLeader.address().c_str());
    // Read the log entries appended before the "read step down" to test
    // implicit commits.
    ReadVerify(leaderPeer, logicPoolId, copysetId, chunkId, length, ch - 1, 1);

    WriteThenReadVerify(leaderPeer, logicPoolId, copysetId, chunkId, length,
                        ch++, loop);

    ::usleep(waitMultiReplicasBecomeConsistent * 1000);
    std::vector<Peer> newPeers;
    for (Peer peer : peers) {
        if (peer.address() != followerPeers[1].address()) {
            newPeers.push_back(peer);
        }
    }
    CopysetStatusVerify(newPeers, logicPoolId, copysetId, 2);
}

/**
 * Verify the replication groups of three nodes and test log truncation
 * 1. Create a replication group of 3 members, wait for the leader to generate,
 * write the data, and then read it out for verification
 * 2. Hang up 2 followers
 * 3. Hang up the leader
 * 3. Pull up 2 followers
 */
TEST_F(RaftLogReplicationTest, ThreeNodeTruncateLog) {
    LogicPoolID logicPoolId = 2;
    CopysetID copysetId = 100001;
    uint64_t chunkId = 1;
    int length = kOpRequestAlignSize;
    char ch = 'a';
    int loop = 10;

    // 1. Start a replication group of 3 members
    PeerId leaderId;
    Peer leaderPeer;
    std::vector<Peer> peers;
    peers.push_back(peer1);
    peers.push_back(peer2);
    peers.push_back(peer3);
    PeerCluster cluster("InitShutdown-cluster", logicPoolId, copysetId, peers,
                        params, paramsIndexs);
    ASSERT_EQ(0, cluster.StartFakeTopoloyService(kFakeMdsAddr));
    cluster.SetElectionTimeoutMs(electionTimeoutMs);
    cluster.SetsnapshotIntervalS(snapshotIntervalS);
    ASSERT_EQ(0, cluster.StartPeer(peer1, PeerCluster::PeerToId(peer1)));
    ASSERT_EQ(0, cluster.StartPeer(peer2, PeerCluster::PeerToId(peer2)));
    ASSERT_EQ(0, cluster.StartPeer(peer3, PeerCluster::PeerToId(peer3)));

    ASSERT_EQ(0, cluster.WaitLeader(&leaderPeer));
    ASSERT_EQ(0, leaderId.parse(leaderPeer.address()));

    WriteThenReadVerify(leaderPeer, logicPoolId, copysetId, chunkId, length,
                        ch++, loop);

    // 2. Hang 2 Followers
    std::vector<Peer> followerPeers;
    PeerCluster::GetFollwerPeers(peers, leaderPeer, &followerPeers);
    ASSERT_GE(followerPeers.size(), 2);
    ASSERT_EQ(0, cluster.ShutdownPeer(followerPeers[0]));
    ASSERT_EQ(0, cluster.ShutdownPeer(followerPeers[1]));
    WriteVerifyNotAvailable(leaderPeer, logicPoolId, copysetId, chunkId, length,
                            ch++, 2);

    // 3. Hang up the leader
    ASSERT_EQ(0, cluster.ShutdownPeer(leaderPeer));
    Peer oldLeader = leaderPeer;

    // 4. Pull up 2 followers
    ASSERT_EQ(0, cluster.StartPeer(followerPeers[0],
                                   PeerCluster::PeerToId(followerPeers[0])));
    ASSERT_EQ(0, cluster.StartPeer(followerPeers[1],
                                   PeerCluster::PeerToId(followerPeers[1])));
    ASSERT_EQ(0, cluster.WaitLeader(&leaderPeer));
    ASSERT_EQ(0, leaderId.parse(leaderPeer.address()));

    // Log truncation
    ReadNotVerify(leaderPeer, logicPoolId, copysetId, chunkId, length, ch - 1,
                  2);

    WriteThenReadVerify(leaderPeer, logicPoolId, copysetId, chunkId, length,
                        ch++, loop);

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
 * Verify the replication group of three nodes, and test copying logs to
 * followers who fall behind multiple terms
 * 1. Create a replication group of 3 members, wait for the leader to generate,
 * write the data, and then read it out for verification
 * 2. Hang up a follower
 * 3. Hang up the leader and wait for 2 ETs to restart
 * 4. Hang up the leader and wait for 2 ETs to restart
 * 3. Pull up the hanging follower
 */
TEST_F(RaftLogReplicationTest, ThreeNodeLogReplicationToOldFollwer) {
    LogicPoolID logicPoolId = 2;
    CopysetID copysetId = 100001;
    uint64_t chunkId = 1;
    int length = kOpRequestAlignSize;
    char ch = 'a';
    int loop = 10;

    // 1. Start a replication group of 3 members
    PeerId leaderId;
    Peer leaderPeer;
    std::vector<Peer> peers;
    peers.push_back(peer1);
    peers.push_back(peer2);
    peers.push_back(peer3);
    PeerCluster cluster("InitShutdown-cluster", logicPoolId, copysetId, peers,
                        params, paramsIndexs);
    ASSERT_EQ(0, cluster.StartFakeTopoloyService(kFakeMdsAddr));
    cluster.SetElectionTimeoutMs(electionTimeoutMs);
    cluster.SetsnapshotIntervalS(snapshotIntervalS);
    ASSERT_EQ(0, cluster.StartPeer(peer1, PeerCluster::PeerToId(peer1)));
    ASSERT_EQ(0, cluster.StartPeer(peer2, PeerCluster::PeerToId(peer2)));
    ASSERT_EQ(0, cluster.StartPeer(peer3, PeerCluster::PeerToId(peer3)));

    ASSERT_EQ(0, cluster.WaitLeader(&leaderPeer));
    ASSERT_EQ(0, leaderId.parse(leaderPeer.address()));

    WriteThenReadVerify(leaderPeer, logicPoolId, copysetId, chunkId, length,
                        ch++, loop);

    // 2. Hang up 1 Follower
    std::vector<Peer> followerPeers;
    PeerCluster::GetFollwerPeers(peers, leaderPeer, &followerPeers);
    ASSERT_GE(followerPeers.size(), 1);
    ASSERT_EQ(0, cluster.ShutdownPeer(followerPeers[0]));
    WriteThenReadVerify(leaderPeer, logicPoolId, copysetId, chunkId, length,
                        ch++, loop);

    // 3. Hang up the leader and wait for 2 ETs to restart
    ASSERT_EQ(0, cluster.ShutdownPeer(leaderPeer));
    ::usleep(1000 * electionTimeoutMs * 2);
    ASSERT_EQ(0,
              cluster.StartPeer(leaderPeer, PeerCluster::PeerToId(leaderPeer)));
    ASSERT_EQ(0, cluster.WaitLeader(&leaderPeer));
    ASSERT_EQ(0, leaderId.parse(leaderPeer.address()));
    WriteThenReadVerify(leaderPeer, logicPoolId, copysetId, chunkId, length,
                        ch++, loop);

    // 4. Hang up the leader and wait for 2 ETs to restart
    ASSERT_EQ(0, cluster.ShutdownPeer(leaderPeer));
    ::usleep(1000 * electionTimeoutMs * 2);
    ASSERT_EQ(0,
              cluster.StartPeer(leaderPeer, PeerCluster::PeerToId(leaderPeer)));
    ASSERT_EQ(0, cluster.WaitLeader(&leaderPeer));
    ASSERT_EQ(0, leaderId.parse(leaderPeer.address()));
    WriteThenReadVerify(leaderPeer, logicPoolId, copysetId, chunkId, length,
                        ch++, loop);

    // 5. Pull up the hanging follower
    ASSERT_EQ(0, cluster.StartPeer(followerPeers[0],
                                   PeerCluster::PeerToId(followerPeers[0])));
    ASSERT_EQ(0, cluster.WaitLeader(&leaderPeer));
    WriteThenReadVerify(leaderPeer, logicPoolId, copysetId, chunkId, length,
                        ch++, loop);

    // Wait a little longer to ensure successful installation of the snapshot
    ::usleep(1.3 * waitMultiReplicasBecomeConsistent * 1000);
    CopysetStatusVerify(peers, logicPoolId, copysetId, 2);
}

/**
 * Verify replication group log replication for 4 members
 * 1. 4 members started normally
 * 2. Hang up the leader
 * 3. Pull up the leader
 * 4. Hang 1 follower
 * 5. Follower, pull it up
 * 6. Hang 2 followers
 * 7. Pull up 1 follower
 * 8. Hang up the leader
 * 9. Pull up the leader from the previous step
 * 10. Hang up the leader and two followers
 * 11. Pull up one by one
 * 12.  Hang up three followers
 * 13.  Pull up one by one
 */
TEST_F(RaftLogReplicationTest, FourNodeKill) {
    LogicPoolID logicPoolId = 2;
    CopysetID copysetId = 100001;
    uint64_t chunkId = 1;
    int length = kOpRequestAlignSize;
    char ch = 'a';
    int loop = 10;

    // 1. Start a replication group of 4 members
    PeerId leaderId;
    Peer leaderPeer;
    std::vector<Peer> peers;
    peers.push_back(peer1);
    peers.push_back(peer2);
    peers.push_back(peer3);
    peers.push_back(peer4);
    PeerCluster cluster("InitShutdown-cluster", logicPoolId, copysetId, peers,
                        params, paramsIndexs);
    ASSERT_EQ(0, cluster.StartFakeTopoloyService(kFakeMdsAddr));
    cluster.SetElectionTimeoutMs(electionTimeoutMs);
    cluster.SetsnapshotIntervalS(snapshotIntervalS);
    ASSERT_EQ(0, cluster.StartPeer(peer1, PeerCluster::PeerToId(peer1)));
    ASSERT_EQ(0, cluster.StartPeer(peer2, PeerCluster::PeerToId(peer2)));
    ASSERT_EQ(0, cluster.StartPeer(peer3, PeerCluster::PeerToId(peer3)));
    ASSERT_EQ(0, cluster.StartPeer(peer4, PeerCluster::PeerToId(peer4)));

    ASSERT_EQ(0, cluster.WaitLeader(&leaderPeer));
    ASSERT_EQ(0, leaderId.parse(leaderPeer.address()));

    WriteThenReadVerify(leaderPeer, logicPoolId, copysetId, chunkId, length,
                        ch++,  // a
                        loop);

    // 2. Hang up the leader
    ASSERT_EQ(0, cluster.ShutdownPeer(leaderPeer));
    ReadVerifyNotAvailable(leaderPeer, logicPoolId, copysetId, chunkId, length,
                           ch - 1, 1);

    Peer newLeader;
    ASSERT_EQ(0, cluster.WaitLeader(&newLeader));
    ASSERT_EQ(0, leaderId.parse(newLeader.address()));
    WriteThenReadVerify(newLeader, logicPoolId, copysetId, chunkId, length,
                        ch++,  // b
                        loop);

    // 3. Pull up the old leader
    ASSERT_EQ(0,
              cluster.StartPeer(leaderPeer, PeerCluster::PeerToId(leaderPeer)));
    ASSERT_EQ(0, cluster.WaitLeader(&newLeader));
    WriteThenReadVerify(newLeader, logicPoolId, copysetId, chunkId, length,
                        ch++,  // c
                        loop);

    // 4. Hang 1 follower
    std::vector<Peer> followerPeers1;
    PeerCluster::GetFollwerPeers(peers, newLeader, &followerPeers1);
    ASSERT_GE(followerPeers1.size(), 3);
    ASSERT_EQ(0, cluster.ShutdownPeer(followerPeers1[0]));
    WriteThenReadVerify(newLeader, logicPoolId, copysetId, chunkId, length,
                        ch++,  // d
                        loop);

    // 5. Pull up the follower
    ASSERT_EQ(0, cluster.StartPeer(followerPeers1[0],
                                   PeerCluster::PeerToId(followerPeers1[0])));
    ASSERT_EQ(0, cluster.WaitLeader(&newLeader));
    WriteThenReadVerify(newLeader, logicPoolId, copysetId, chunkId, length,
                        ch++,  // e
                        loop);

    // 6. Hang 2 followers
    std::vector<Peer> followerPeers2;
    PeerCluster::GetFollwerPeers(peers, newLeader, &followerPeers2);
    ASSERT_GE(followerPeers2.size(), 3);
    ASSERT_EQ(0, cluster.ShutdownPeer(followerPeers2[0]));
    ASSERT_EQ(0, cluster.ShutdownPeer(followerPeers2[1]));
    WriteVerifyNotAvailable(newLeader, logicPoolId, copysetId, chunkId, length,
                            ch++,  // f
                            1);

    // 7. Pull up 1 follower
    ASSERT_EQ(0, cluster.StartPeer(followerPeers2[0],
                                   PeerCluster::PeerToId(followerPeers2[0])));
    ASSERT_EQ(0, cluster.WaitLeader(&leaderPeer));
    ASSERT_EQ(0, leaderId.parse(leaderPeer.address()));
    WriteThenReadVerify(leaderPeer, logicPoolId, copysetId, chunkId, length,
                        ch++,  // g
                        loop);

    // 8. Hang up the leader
    ASSERT_EQ(0, cluster.ShutdownPeer(leaderPeer));
    ReadVerifyNotAvailable(leaderPeer, logicPoolId, copysetId, chunkId, length,
                           ch - 1, 1);

    // 9. Pull up the leader from the previous step
    ASSERT_EQ(0,
              cluster.StartPeer(leaderPeer, PeerCluster::PeerToId(leaderPeer)));
    ASSERT_EQ(0, cluster.WaitLeader(&leaderPeer));
    ASSERT_EQ(0, leaderId.parse(leaderPeer.address()));
    WriteThenReadVerify(leaderPeer, logicPoolId, copysetId, chunkId, length,
                        ch++,  // h
                        loop);

    // 10. Hang up the leader and two followers
    ASSERT_EQ(0, cluster.ShutdownPeer(leaderPeer));
    Peer shutdownFollower;
    if (leaderPeer.address() != followerPeers2[0].address()) {
        shutdownFollower = followerPeers2[0];
    } else {
        shutdownFollower = followerPeers2[2];
    }
    ASSERT_EQ(0, cluster.ShutdownPeer(shutdownFollower));
    ReadVerifyNotAvailable(leaderPeer, logicPoolId, copysetId, chunkId, length,
                           ch - 1, 1);

    ::usleep(1000 * electionTimeoutMs * 2);
    // 11. Pull up one by one
    ASSERT_EQ(0,
              cluster.StartPeer(leaderPeer, PeerCluster::PeerToId(leaderPeer)));
    ASSERT_EQ(-1, cluster.WaitLeader(&leaderPeer));
    ReadVerifyNotAvailable(leaderPeer, logicPoolId, copysetId, chunkId, length,
                           ch - 1, 1);
    ASSERT_EQ(0, cluster.StartPeer(shutdownFollower,
                                   PeerCluster::PeerToId(shutdownFollower)));
    ASSERT_EQ(0, cluster.WaitLeader(&leaderPeer));
    ASSERT_EQ(0, leaderId.parse(leaderPeer.address()));
    WriteThenReadVerify(leaderPeer, logicPoolId, copysetId, chunkId, length,
                        ch++,  // i
                        loop);
    ASSERT_EQ(0, cluster.StartPeer(followerPeers2[1],
                                   PeerCluster::PeerToId(followerPeers2[1])));
    ASSERT_EQ(0, cluster.WaitLeader(&leaderPeer));
    WriteThenReadVerify(leaderPeer, logicPoolId, copysetId, chunkId, length,
                        ch++,  // i
                        loop);

    // 12. Hang up three followers
    std::vector<Peer> followerPeers3;
    PeerCluster::GetFollwerPeers(peers, leaderPeer, &followerPeers3);
    ASSERT_GE(followerPeers3.size(), 3);
    ASSERT_EQ(0, cluster.ShutdownPeer(followerPeers3[0]));
    ASSERT_EQ(0, cluster.ShutdownPeer(followerPeers3[1]));
    ASSERT_EQ(0, cluster.ShutdownPeer(followerPeers3[2]));
    WriteVerifyNotAvailable(leaderPeer, logicPoolId, copysetId, chunkId, length,
                            ch++,  // j
                            1);

    ::usleep(1000 * electionTimeoutMs * 2);
    // 13. Pull up one by one
    ASSERT_EQ(0, cluster.StartPeer(followerPeers3[0],
                                   PeerCluster::PeerToId(followerPeers3[0])));
    ASSERT_EQ(-1, cluster.WaitLeader(&leaderPeer));
    ReadVerifyNotAvailable(leaderPeer, logicPoolId, copysetId, chunkId, length,
                           ch - 1, 1);

    ASSERT_EQ(0, cluster.StartPeer(followerPeers3[1],
                                   PeerCluster::PeerToId(followerPeers3[1])));
    ASSERT_EQ(0, cluster.WaitLeader(&leaderPeer));
    WriteThenReadVerify(leaderPeer, logicPoolId, copysetId, chunkId, length,
                        ch++,  // k
                        loop);

    ASSERT_EQ(0, cluster.StartPeer(followerPeers3[2],
                                   PeerCluster::PeerToId(followerPeers3[2])));
    ASSERT_EQ(0, cluster.WaitLeader(&leaderPeer));
    WriteThenReadVerify(leaderPeer, logicPoolId, copysetId, chunkId, length,
                        ch++, loop);

    ::usleep(waitMultiReplicasBecomeConsistent * 1000);
    CopysetStatusVerify(peers, logicPoolId, copysetId, 2);
}

/**
 * Verify replication group log replication for 4 members
 * 1. 4 members started normally
 * 2. Hang leader
 * 3. Restore leader
 * 4. Hang1, a follower
 * 5. Restore follower
 * 6. Hang 2 followers
 * 7. Restore 1 follower
 * 8. Hangleader
 * 9. The leader of the previous step of hang
 * 10. Hang leader and two followers
 * 11. Restore one by one
 * 12.  Hang 3 followers
 * 13.  Restore one by one
 */
TEST_F(RaftLogReplicationTest, FourNodeHang) {
    LogicPoolID logicPoolId = 2;
    CopysetID copysetId = 100001;
    uint64_t chunkId = 1;
    int length = kOpRequestAlignSize;
    char ch = 'a';
    int loop = 10;

    // 1. Start a replication group of 4 members
    PeerId leaderId;
    Peer leaderPeer;
    std::vector<Peer> peers;
    peers.push_back(peer1);
    peers.push_back(peer2);
    peers.push_back(peer3);
    peers.push_back(peer4);
    PeerCluster cluster("InitShutdown-cluster", logicPoolId, copysetId, peers,
                        params, paramsIndexs);
    ASSERT_EQ(0, cluster.StartFakeTopoloyService(kFakeMdsAddr));
    cluster.SetElectionTimeoutMs(electionTimeoutMs);
    cluster.SetsnapshotIntervalS(snapshotIntervalS);
    ASSERT_EQ(0, cluster.StartPeer(peer1, PeerCluster::PeerToId(peer1)));
    ASSERT_EQ(0, cluster.StartPeer(peer2, PeerCluster::PeerToId(peer2)));
    ASSERT_EQ(0, cluster.StartPeer(peer3, PeerCluster::PeerToId(peer3)));
    ASSERT_EQ(0, cluster.StartPeer(peer4, PeerCluster::PeerToId(peer4)));

    ASSERT_EQ(0, cluster.WaitLeader(&leaderPeer));
    ASSERT_EQ(0, leaderId.parse(leaderPeer.address()));

    WriteThenReadVerify(leaderPeer, logicPoolId, copysetId, chunkId, length,
                        ch++,  // a
                        loop);

    // 2. hang leader
    ASSERT_EQ(0, cluster.HangPeer(leaderPeer));
    Peer oldLeader = leaderPeer;
    ReadVerifyNotAvailable(leaderPeer, logicPoolId, copysetId, chunkId, length,
                           ch - 1, 1);
    Peer newLeader;
    ASSERT_EQ(0, cluster.WaitLeader(&newLeader));
    ASSERT_EQ(0, leaderId.parse(newLeader.address()));
    ASSERT_STRNE(oldLeader.address().c_str(), newLeader.address().c_str());
    WriteThenReadVerify(newLeader, logicPoolId, copysetId, chunkId, length,
                        ch++,  // b
                        loop);

    // 3. Restore old leader
    ASSERT_EQ(0, cluster.SignalPeer(oldLeader));
    WriteThenReadVerify(newLeader, logicPoolId, copysetId, chunkId, length,
                        ch++,  // c
                        loop);

    // 4. Hang 1, one follower
    std::vector<Peer> followerPeers1;
    PeerCluster::GetFollwerPeers(peers, newLeader, &followerPeers1);
    ASSERT_GE(followerPeers1.size(), 1);
    ASSERT_EQ(0, cluster.HangPeer(followerPeers1[0]));
    WriteThenReadVerify(newLeader, logicPoolId, copysetId, chunkId, length,
                        ch++,  // d
                        loop);

    // 5. Restore follower
    ASSERT_EQ(0, cluster.SignalPeer(followerPeers1[0]));
    WriteThenReadVerify(newLeader, logicPoolId, copysetId, chunkId, length,
                        ch++,  // e
                        loop);

    // 6. Hang 2 followers
    std::vector<Peer> followerPeers2;
    PeerCluster::GetFollwerPeers(peers, newLeader, &followerPeers2);
    ASSERT_GE(followerPeers2.size(), 3);
    ASSERT_EQ(0, cluster.HangPeer(followerPeers2[0]));
    ASSERT_EQ(0, cluster.HangPeer(followerPeers2[1]));
    WriteVerifyNotAvailable(newLeader, logicPoolId, copysetId, chunkId, length,
                            ch++,  // f
                            1);

    // 7. Restore 1 follower
    ASSERT_EQ(0, cluster.SignalPeer(followerPeers2[0]));
    ASSERT_EQ(0, cluster.WaitLeader(&leaderPeer));
    ASSERT_EQ(0, leaderId.parse(leaderPeer.address()));
    WriteThenReadVerify(leaderPeer, logicPoolId, copysetId, chunkId, length,
                        ch++,  // g
                        loop);

    // 8. hang leader
    ASSERT_EQ(0, cluster.HangPeer(leaderPeer));
    ReadVerifyNotAvailable(leaderPeer, logicPoolId, copysetId, chunkId, length,
                           ch - 1, 1);

    // 9. Restore the previous suspended leader
    ASSERT_EQ(0, cluster.SignalPeer(leaderPeer));
    ASSERT_EQ(0, cluster.WaitLeader(&leaderPeer));
    ASSERT_EQ(0, leaderId.parse(leaderPeer.address()));
    WriteThenReadVerify(leaderPeer, logicPoolId, copysetId, chunkId, length,
                        ch++,  // h
                        loop);

    // 10. Hang leader and two followers
    ASSERT_EQ(0, cluster.HangPeer(leaderPeer));
    Peer shutdownFollower;
    if (leaderPeer.address() != followerPeers2[0].address()) {
        shutdownFollower = followerPeers2[0];
    } else {
        shutdownFollower = followerPeers2[2];
    }
    ASSERT_EQ(0, cluster.HangPeer(shutdownFollower));
    ReadVerifyNotAvailable(leaderPeer, logicPoolId, copysetId, chunkId, length,
                           ch - 1, 1);

    ::usleep(1000 * electionTimeoutMs * 2);
    // 11. Restore one by one
    ASSERT_EQ(0, cluster.SignalPeer(leaderPeer));
    ASSERT_EQ(-1, cluster.WaitLeader(&leaderPeer));
    ReadVerifyNotAvailable(leaderPeer, logicPoolId, copysetId, chunkId, length,
                           ch - 1, 1);
    ASSERT_EQ(0, cluster.SignalPeer(shutdownFollower));
    ASSERT_EQ(0, cluster.WaitLeader(&leaderPeer));
    ASSERT_EQ(0, leaderId.parse(leaderPeer.address()));
    WriteThenReadVerify(leaderPeer, logicPoolId, copysetId, chunkId, length,
                        ch++,  // i
                        loop);
    ASSERT_EQ(0, cluster.SignalPeer(followerPeers2[1]));
    WriteThenReadVerify(leaderPeer, logicPoolId, copysetId, chunkId, length,
                        ch++,  // j
                        loop);

    // 12. Hang 3 followers
    std::vector<Peer> followerPeers3;
    PeerCluster::GetFollwerPeers(peers, leaderPeer, &followerPeers3);
    ASSERT_GE(followerPeers3.size(), 3);
    ASSERT_EQ(0, cluster.HangPeer(followerPeers3[0]));
    ASSERT_EQ(0, cluster.HangPeer(followerPeers3[1]));
    ASSERT_EQ(0, cluster.HangPeer(followerPeers3[2]));
    WriteVerifyNotAvailable(leaderPeer, logicPoolId, copysetId, chunkId, length,
                            ch++,  // k
                            1);

    // 13. Restore one by one
    ::usleep(1000 * electionTimeoutMs * 2);
    ASSERT_EQ(0, cluster.SignalPeer(followerPeers3[0]));
    ASSERT_EQ(-1, cluster.WaitLeader(&leaderPeer));
    ReadVerifyNotAvailable(leaderPeer, logicPoolId, copysetId, chunkId, length,
                           ch - 1, 1);

    ASSERT_EQ(0, cluster.SignalPeer(followerPeers3[1]));
    ASSERT_EQ(0, cluster.WaitLeader(&leaderPeer));
    WriteThenReadVerify(leaderPeer, logicPoolId, copysetId, chunkId, length,
                        ch++,  // l
                        loop);

    ASSERT_EQ(0, cluster.SignalPeer(followerPeers3[2]));
    WriteThenReadVerify(leaderPeer, logicPoolId, copysetId, chunkId, length,
                        ch++, loop);
}

/**
 * Verify replication group log replication for 5 members
 * 1. 5 members started normally
 * 2. Hang the leader
 * 3. Restore leader
 * 4. Hang 1 follower
 * 5. Restore follower
 * 6. Hang 2 followers
 * 7. Restore 1 follower
 * 8. Hang the leader
 * 9. Restore one-step suspended leaders
 * 10. Hang the leader and two followers
 * 11. Restore one by one
 * 12.  Hang 3 followers
 * 13.  Restore one by one
 */
TEST_F(RaftLogReplicationTest, FiveNodeKill) {
    LogicPoolID logicPoolId = 2;
    CopysetID copysetId = 100001;
    uint64_t chunkId = 1;
    int length = kOpRequestAlignSize;
    char ch = 'a';
    int loop = 10;

    // 1. Start a replication group of 5 members
    PeerId leaderId;
    Peer leaderPeer;
    std::vector<Peer> peers;
    peers.push_back(peer1);
    peers.push_back(peer2);
    peers.push_back(peer3);
    peers.push_back(peer4);
    peers.push_back(peer5);
    PeerCluster cluster("InitShutdown-cluster", logicPoolId, copysetId, peers,
                        params, paramsIndexs);
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

    WriteThenReadVerify(leaderPeer, logicPoolId, copysetId, chunkId, length,
                        ch++,  // a
                        loop);

    // 2. Hang up the leader
    ASSERT_EQ(0, cluster.ShutdownPeer(leaderPeer));
    ReadVerifyNotAvailable(leaderPeer, logicPoolId, copysetId, chunkId, length,
                           ch - 1, 1);
    Peer newLeader;
    ASSERT_EQ(0, cluster.WaitLeader(&newLeader));
    ASSERT_EQ(0, leaderId.parse(newLeader.address()));
    WriteThenReadVerify(newLeader, logicPoolId, copysetId, chunkId, length,
                        ch++,  // b
                        loop);

    // 3. Pull up the old leader
    ASSERT_EQ(0,
              cluster.StartPeer(leaderPeer, PeerCluster::PeerToId(leaderPeer)));
    ASSERT_EQ(0, cluster.WaitLeader(&leaderPeer));
    WriteThenReadVerify(newLeader, logicPoolId, copysetId, chunkId, length,
                        ch++,  // c
                        loop);

    // 4. Hang 1 follower
    std::vector<Peer> followerPeers1;
    PeerCluster::GetFollwerPeers(peers, newLeader, &followerPeers1);
    ASSERT_GE(followerPeers1.size(), 1);
    ASSERT_EQ(0, cluster.ShutdownPeer(followerPeers1[0]));
    WriteThenReadVerify(newLeader, logicPoolId, copysetId, chunkId, length,
                        ch++,  // d
                        loop);

    // 5. Follower, pull it up
    ASSERT_EQ(0, cluster.StartPeer(followerPeers1[0],
                                   PeerCluster::PeerToId(followerPeers1[0])));
    ASSERT_EQ(0, cluster.WaitLeader(&leaderPeer));
    WriteThenReadVerify(newLeader, logicPoolId, copysetId, chunkId, length,
                        ch++,  // e
                        loop);

    // 6. Hang 2 followers
    std::vector<Peer> followerPeers2;
    PeerCluster::GetFollwerPeers(peers, newLeader, &followerPeers2);
    ASSERT_GE(followerPeers2.size(), 4);
    ASSERT_EQ(0, cluster.ShutdownPeer(followerPeers2[0]));
    ASSERT_EQ(0, cluster.ShutdownPeer(followerPeers2[1]));
    WriteThenReadVerify(newLeader, logicPoolId, copysetId, chunkId, length,
                        ch++,  // f
                        loop);

    // 7. Pull up 1 follower
    ASSERT_EQ(0, cluster.StartPeer(followerPeers2[0],
                                   PeerCluster::PeerToId(followerPeers2[0])));
    ASSERT_EQ(0, cluster.WaitLeader(&leaderPeer));
    WriteThenReadVerify(newLeader, logicPoolId, copysetId, chunkId, length,
                        ch++,  // g
                        loop);

    // 8. Hang up the leader
    ASSERT_EQ(0, cluster.ShutdownPeer(newLeader));
    ReadVerifyNotAvailable(newLeader, logicPoolId, copysetId, chunkId, length,
                           ch - 1, 1);

    // 9. Pull up the leader from the previous step
    ASSERT_EQ(0,
              cluster.StartPeer(newLeader, PeerCluster::PeerToId(newLeader)));
    ASSERT_EQ(0, cluster.WaitLeader(&leaderPeer));
    ASSERT_EQ(0, leaderId.parse(leaderPeer.address()));
    WriteThenReadVerify(leaderPeer, logicPoolId, copysetId, chunkId, length,
                        ch++,  // h
                        loop);

    // 10. Hang up the leader and two followers
    ASSERT_EQ(0, cluster.ShutdownPeer(leaderPeer));
    Peer shutdownFollower;
    if (leaderPeer.address() != followerPeers2[0].address()) {
        shutdownFollower = followerPeers2[0];
    } else {
        shutdownFollower = followerPeers2[2];
    }
    ASSERT_EQ(0, cluster.ShutdownPeer(shutdownFollower));
    ReadVerifyNotAvailable(leaderPeer, logicPoolId, copysetId, chunkId, length,
                           ch - 1, 1);

    // 11. Pull up one by one
    ASSERT_EQ(0,
              cluster.StartPeer(leaderPeer, PeerCluster::PeerToId(leaderPeer)));
    ASSERT_EQ(0, cluster.WaitLeader(&leaderPeer));
    WriteThenReadVerify(leaderPeer, logicPoolId, copysetId, chunkId, length,
                        ch++,  // i
                        loop);
    ASSERT_EQ(0, cluster.StartPeer(shutdownFollower,
                                   PeerCluster::PeerToId(shutdownFollower)));
    ASSERT_EQ(0, cluster.WaitLeader(&leaderPeer));
    WriteThenReadVerify(leaderPeer, logicPoolId, copysetId, chunkId, length,
                        ch++,  // j
                        loop);
    ASSERT_EQ(0, cluster.StartPeer(followerPeers2[1],
                                   PeerCluster::PeerToId(followerPeers2[1])));
    ASSERT_EQ(0, cluster.WaitLeader(&leaderPeer));
    WriteThenReadVerify(leaderPeer, logicPoolId, copysetId, chunkId, length,
                        ch++,  // k
                        loop);

    // 12. Hang up three followers
    std::vector<Peer> followerPeers3;
    PeerCluster::GetFollwerPeers(peers, leaderPeer, &followerPeers3);
    ASSERT_GE(followerPeers3.size(), 3);
    ASSERT_EQ(0, cluster.ShutdownPeer(followerPeers3[0]));
    ASSERT_EQ(0, cluster.ShutdownPeer(followerPeers3[1]));
    ASSERT_EQ(0, cluster.ShutdownPeer(followerPeers3[2]));
    WriteVerifyNotAvailable(leaderPeer, logicPoolId, copysetId, chunkId, length,
                            ch++,  // l
                            1);

    // 13. Pull up one by one
    ASSERT_EQ(0, cluster.StartPeer(followerPeers3[0],
                                   PeerCluster::PeerToId(followerPeers3[0])));
    ASSERT_EQ(0, cluster.WaitLeader(&leaderPeer));
    WriteThenReadVerify(leaderPeer, logicPoolId, copysetId, chunkId, length,
                        ch++,  // m
                        loop);

    ASSERT_EQ(0, cluster.StartPeer(followerPeers3[1],
                                   PeerCluster::PeerToId(followerPeers3[1])));
    ASSERT_EQ(0, cluster.WaitLeader(&leaderPeer));
    WriteThenReadVerify(leaderPeer, logicPoolId, copysetId, chunkId, length,
                        ch++,  // n
                        loop);

    ASSERT_EQ(0, cluster.StartPeer(followerPeers3[2],
                                   PeerCluster::PeerToId(followerPeers3[2])));
    ASSERT_EQ(0, cluster.WaitLeader(&leaderPeer));
    WriteThenReadVerify(leaderPeer, logicPoolId, copysetId, chunkId, length,
                        ch++,  // o
                        loop);
}

/**
 * Verify replication group log replication for 5 members
 * 1. 5 members started normally
 * 2. Hang leader
 * 3. Restore leader
 * 4. Hang 1, one follower
 * 5. Restore follower
 * 6. Hang 2 followers
 * 7. Restore 1 follower
 * 8. Hang leader
 * 9. The leader of the previous step of hang
 * 10. Hang leader and two followers
 * 11. Restore one by one
 * 12.  Hang3 followers
 * 13.  Restore one by one
 */
TEST_F(RaftLogReplicationTest, FiveNodeHang) {
    LogicPoolID logicPoolId = 2;
    CopysetID copysetId = 100001;
    uint64_t chunkId = 1;
    int length = kOpRequestAlignSize;
    char ch = 'a';
    int loop = 10;

    // 1. Start a replication group of 5 members
    PeerId leaderId;
    Peer leaderPeer;
    std::vector<Peer> peers;
    peers.push_back(peer1);
    peers.push_back(peer2);
    peers.push_back(peer3);
    peers.push_back(peer4);
    peers.push_back(peer5);
    PeerCluster cluster("InitShutdown-cluster", logicPoolId, copysetId, peers,
                        params, paramsIndexs);
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

    WriteThenReadVerify(leaderPeer, logicPoolId, copysetId, chunkId, length,
                        ch++,  // a
                        loop);

    // 2. hang leader
    ASSERT_EQ(0, cluster.HangPeer(leaderPeer));
    ReadVerifyNotAvailable(leaderPeer, logicPoolId, copysetId, chunkId, length,
                           ch - 1, 1);
    Peer newLeader;
    ASSERT_EQ(0, cluster.WaitLeader(&newLeader));
    ASSERT_EQ(0, leaderId.parse(newLeader.address()));
    WriteThenReadVerify(newLeader, logicPoolId, copysetId, chunkId, length,
                        ch++,  // b
                        loop);

    // 3. Restore old leader
    ASSERT_EQ(0, cluster.SignalPeer(leaderPeer));
    WriteThenReadVerify(newLeader, logicPoolId, copysetId, chunkId, length,
                        ch++,  // c
                        loop);

    // 4. Hang 1, one follower
    std::vector<Peer> followerPeers1;
    PeerCluster::GetFollwerPeers(peers, newLeader, &followerPeers1);
    ASSERT_GE(followerPeers1.size(), 1);
    ASSERT_EQ(0, cluster.HangPeer(followerPeers1[0]));
    WriteThenReadVerify(newLeader, logicPoolId, copysetId, chunkId, length,
                        ch++,  // d
                        loop);

    // 5. Restore follower
    ASSERT_EQ(0, cluster.SignalPeer(followerPeers1[0]));
    WriteThenReadVerify(newLeader, logicPoolId, copysetId, chunkId, length,
                        ch++,  // e
                        loop);

    // 6. Hang 2 followers
    std::vector<Peer> followerPeers2;
    PeerCluster::GetFollwerPeers(peers, newLeader, &followerPeers2);
    ASSERT_GE(followerPeers2.size(), 4);
    ASSERT_EQ(0, cluster.HangPeer(followerPeers2[0]));
    ASSERT_EQ(0, cluster.HangPeer(followerPeers2[1]));
    WriteThenReadVerify(newLeader, logicPoolId, copysetId, chunkId, length,
                        ch++,  // f
                        loop);

    // 7. Restore 1 follower
    ASSERT_EQ(0, cluster.SignalPeer(followerPeers2[0]));
    WriteThenReadVerify(newLeader, logicPoolId, copysetId, chunkId, length,
                        ch++,  // g
                        loop);

    // 8. hang leader
    ASSERT_EQ(0, cluster.HangPeer(newLeader));
    ReadVerifyNotAvailable(newLeader, logicPoolId, copysetId, chunkId, length,
                           ch - 1, 1);

    // 9. Restore the previous suspended leader
    ASSERT_EQ(0, cluster.SignalPeer(newLeader));
    ASSERT_EQ(0, cluster.WaitLeader(&leaderPeer));
    ASSERT_EQ(0, leaderId.parse(leaderPeer.address()));
    WriteThenReadVerify(leaderPeer, logicPoolId, copysetId, chunkId, length,
                        ch++,  // h
                        loop);

    // 10. Hang leader and two followers
    ASSERT_EQ(0, cluster.HangPeer(leaderPeer));
    Peer shutdownFollower;
    if (leaderPeer.address() != followerPeers2[0].address()) {
        shutdownFollower = followerPeers2[0];
    } else {
        shutdownFollower = followerPeers2[2];
    }
    ASSERT_EQ(0, cluster.HangPeer(shutdownFollower));
    ReadVerifyNotAvailable(leaderPeer, logicPoolId, copysetId, chunkId, length,
                           ch - 1, 1);

    // 11. Restore one by one
    ASSERT_EQ(0, cluster.SignalPeer(leaderPeer));
    ASSERT_EQ(0, cluster.WaitLeader(&leaderPeer));
    WriteThenReadVerify(leaderPeer, logicPoolId, copysetId, chunkId, length,
                        ch++,  // i
                        loop);
    ASSERT_EQ(0, cluster.SignalPeer(shutdownFollower));
    WriteThenReadVerify(leaderPeer, logicPoolId, copysetId, chunkId, length,
                        ch++,  // j
                        loop);
    ASSERT_EQ(0, cluster.SignalPeer(followerPeers2[1]));
    WriteThenReadVerify(leaderPeer, logicPoolId, copysetId, chunkId, length,
                        ch++,  // k
                        loop);

    // 12. Hang 3 followers
    std::vector<Peer> followerPeers3;
    PeerCluster::GetFollwerPeers(peers, leaderPeer, &followerPeers3);
    ASSERT_GE(followerPeers3.size(), 3);
    ASSERT_EQ(0, cluster.HangPeer(followerPeers3[0]));
    ASSERT_EQ(0, cluster.HangPeer(followerPeers3[1]));
    ASSERT_EQ(0, cluster.HangPeer(followerPeers3[2]));
    WriteVerifyNotAvailable(leaderPeer, logicPoolId, copysetId, chunkId, length,
                            ch++,  // l
                            1);

    // 13. Restore one by one
    ASSERT_EQ(0, cluster.SignalPeer(followerPeers3[0]));
    ASSERT_EQ(0, cluster.WaitLeader(&leaderPeer));
    WriteThenReadVerify(leaderPeer, logicPoolId, copysetId, chunkId, length,
                        ch++,  // m
                        loop);

    ASSERT_EQ(0, cluster.SignalPeer(followerPeers3[1]));
    WriteThenReadVerify(leaderPeer, logicPoolId, copysetId, chunkId, length,
                        ch++,  // n
                        loop);

    ASSERT_EQ(0, cluster.SignalPeer(followerPeers3[2]));
    WriteThenReadVerify(leaderPeer, logicPoolId, copysetId, chunkId, length,
                        ch++,  // o
                        loop);
}

}  // namespace chunkserver
}  // namespace curve
