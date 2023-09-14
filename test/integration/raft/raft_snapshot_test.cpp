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
 * Created Date: Thursday November 14th 2019
 * Author: yangyaokai
 */

#include <brpc/channel.h>
#include <gtest/gtest.h>
#include <butil/at_exit.h>

#include <vector>

#include "test/integration/common/peer_cluster.h"
#include "src/chunkserver/copyset_node_manager.h"
#include "src/chunkserver/cli2.h"
#include "src/fs/fs_common.h"
#include "src/fs/local_filesystem.h"
#include "test/integration/common/config_generator.h"

namespace curve {
namespace chunkserver {

using curve::fs::LocalFileSystem;
using curve::fs::LocalFsFactory;
using curve::fs::FileSystemType;

const char kRaftSnapshotTestLogDir[] = "./runlog/RaftSnapshot";
const char* kFakeMdsAddr = "127.0.0.1:9320";
static constexpr uint32_t kOpRequestAlignSize = 4096;

static const char *raftVoteParam[4][16] = {
    {
        "chunkserver",
        "-chunkServerIp=127.0.0.1",
        "-chunkServerPort=9321",
        "-chunkServerStoreUri=local://./9321/",
        "-chunkServerMetaUri=local://./9321/chunkserver.dat",
        "-copySetUri=local://./9321/copysets",
        "-raftSnapshotUri=curve://./9321/copysets",
        "-recycleUri=local://./9321/recycler",
        "-chunkFilePoolDir=./9321/chunkfilepool/",
        "-chunkFilePoolMetaPath=./9321/chunkfilepool.meta",
        "-conf=./9321/chunkserver.conf",
        "-raft_sync_segments=true",
        "-raftLogUri=curve://./9321/copysets",
        "-walFilePoolDir=./9321/walfilepool/",
        "-walFilePoolMetaPath=./9321/walfilepool.meta",
        NULL
    },
    {
        "chunkserver",
        "-chunkServerIp=127.0.0.1",
        "-chunkServerPort=9322",
        "-chunkServerStoreUri=local://./9322/",
        "-chunkServerMetaUri=local://./9322/chunkserver.dat",
        "-copySetUri=local://./9322/copysets",
        "-raftSnapshotUri=curve://./9322/copysets",
        "-recycleUri=local://./9322/recycler",
        "-chunkFilePoolDir=./9322/chunkfilepool/",
        "-chunkFilePoolMetaPath=./9322/chunkfilepool.meta",
        "-conf=./9322/chunkserver.conf",
        "-raft_sync_segments=true",
        "-raftLogUri=curve://./9322/copysets",
        "-walFilePoolDir=./9322/walfilepool/",
        "-walFilePoolMetaPath=./9322/walfilepool.meta",
        NULL
    },
    {
        "chunkserver",
        "-chunkServerIp=127.0.0.1",
        "-chunkServerPort=9323",
        "-chunkServerStoreUri=local://./9323/",
        "-chunkServerMetaUri=local://./9323/chunkserver.dat",
        "-copySetUri=local://./9323/copysets",
        "-raftSnapshotUri=curve://./9323/copysets",
        "-recycleUri=local://./9323/recycler",
        "-chunkFilePoolDir=./9323/chunkfilepool/",
        "-chunkFilePoolMetaPath=./9323/chunkfilepool.meta",
        "-conf=./9323/chunkserver.conf",
        "-raft_sync_segments=true",
        "-raftLogUri=curve://./9323/copysets",
        "-walFilePoolDir=./9323/walfilepool/",
        "-walFilePoolMetaPath=./9323/walfilepool.meta",
        NULL
    },
    {
        "chunkserver",
        "-chunkServerIp=127.0.0.1",
        "-chunkServerPort=9324",
        "-chunkServerStoreUri=local://./9324/",
        "-chunkServerMetaUri=local://./9324/chunkserver.dat",
        "-copySetUri=local://./9324/copysets",
        "-raftSnapshotUri=curve://./9324/copysets",
        "-recycleUri=local://./9324/recycler",
        "-chunkFilePoolDir=./9324/chunkfilepool/",
        "-chunkFilePoolMetaPath=./9324/chunkfilepool.meta",
        "-conf=./9324/chunkserver.conf",
        "-raft_sync_segments=true",
        "-raftLogUri=curve://./9324/copysets",
        "-walFilePoolDir=./9324/walfilepool/",
        "-walFilePoolMetaPath=./9324/walfilepool.meta",
        NULL
    },
};

class RaftSnapshotTest : public testing::Test {
 protected:
    virtual void SetUp() {
        peer1_.set_address("127.0.0.1:9321:0");
        peer2_.set_address("127.0.0.1:9322:0");
        peer3_.set_address("127.0.0.1:9323:0");
        peer4_.set_address("127.0.0.1:9324:0");

        std::string mkdir1("mkdir ");
        mkdir1 += std::to_string(PeerCluster::PeerToId(peer1_));
        std::string mkdir2("mkdir ");
        mkdir2 += std::to_string(PeerCluster::PeerToId(peer2_));
        std::string mkdir3("mkdir ");
        mkdir3 += std::to_string(PeerCluster::PeerToId(peer3_));
        std::string mkdir4("mkdir ");
        mkdir4 += std::to_string(PeerCluster::PeerToId(peer4_));
        std::string mkdir5("mkdir ");
        mkdir5 += kRaftSnapshotTestLogDir;

        ::system(mkdir1.c_str());
        ::system(mkdir2.c_str());
        ::system(mkdir3.c_str());
        ::system(mkdir4.c_str());
        ::system(mkdir5.c_str());

        electionTimeoutMs_ = 1000;
        snapshotIntervalS_ = 20;

        ASSERT_TRUE(cg1_.Init("9321"));
        ASSERT_TRUE(cg2_.Init("9322"));
        ASSERT_TRUE(cg3_.Init("9323"));
        ASSERT_TRUE(cg4_.Init("9324"));
        cg1_.SetKV("copyset.election_timeout_ms",
                  std::to_string(electionTimeoutMs_));
        cg1_.SetKV("copyset.snapshot_interval_s",
                  std::to_string(snapshotIntervalS_));
        cg1_.SetKV("chunkserver.common.logDir",
            kRaftSnapshotTestLogDir);
        cg1_.SetKV("mds.listen.addr", kFakeMdsAddr);
        cg2_.SetKV("copyset.election_timeout_ms",
                  std::to_string(electionTimeoutMs_));
        cg2_.SetKV("copyset.snapshot_interval_s",
                  std::to_string(snapshotIntervalS_));
        cg2_.SetKV("chunkserver.common.logDir",
            kRaftSnapshotTestLogDir);
        cg2_.SetKV("mds.listen.addr", kFakeMdsAddr);
        cg3_.SetKV("copyset.election_timeout_ms",
                  std::to_string(electionTimeoutMs_));
        cg3_.SetKV("copyset.snapshot_interval_s",
                  std::to_string(snapshotIntervalS_));
        cg3_.SetKV("chunkserver.common.logDir",
            kRaftSnapshotTestLogDir);
        cg3_.SetKV("mds.listen.addr", kFakeMdsAddr);
        cg4_.SetKV("copyset.election_timeout_ms",
                  std::to_string(electionTimeoutMs_));
        cg4_.SetKV("copyset.snapshot_interval_s",
                  std::to_string(snapshotIntervalS_));
        cg4_.SetKV("chunkserver.common.logDir",
            kRaftSnapshotTestLogDir);
        cg4_.SetKV("mds.listen.addr", kFakeMdsAddr);
        ASSERT_TRUE(cg1_.Generate());
        ASSERT_TRUE(cg2_.Generate());
        ASSERT_TRUE(cg3_.Generate());
        ASSERT_TRUE(cg4_.Generate());

        paramsIndexs_[PeerCluster::PeerToId(peer1_)] = 0;
        paramsIndexs_[PeerCluster::PeerToId(peer2_)] = 1;
        paramsIndexs_[PeerCluster::PeerToId(peer3_)] = 2;
        paramsIndexs_[PeerCluster::PeerToId(peer4_)] = 3;

        params_.push_back(const_cast<char**>(raftVoteParam[0]));
        params_.push_back(const_cast<char**>(raftVoteParam[1]));
        params_.push_back(const_cast<char**>(raftVoteParam[2]));
        params_.push_back(const_cast<char**>(raftVoteParam[3]));

        // Configure default raft client option
        defaultCliOpt_.max_retry = 3;
        defaultCliOpt_.timeout_ms = 10000;
    }

    virtual void TearDown() {
        std::string rmdir1("rm -fr ");
        rmdir1 += std::to_string(PeerCluster::PeerToId(peer1_));
        std::string rmdir2("rm -fr ");
        rmdir2 += std::to_string(PeerCluster::PeerToId(peer2_));
        std::string rmdir3("rm -fr ");
        rmdir3 += std::to_string(PeerCluster::PeerToId(peer3_));
        std::string rmdir4("rm -fr ");
        rmdir4 += std::to_string(PeerCluster::PeerToId(peer4_));

        ::system(rmdir1.c_str());
        ::system(rmdir2.c_str());
        ::system(rmdir3.c_str());
        ::system(rmdir4.c_str());

        // wait for process exit
        ::usleep(100 * 1000);
    }

 public:
    Peer peer1_;
    Peer peer2_;
    Peer peer3_;
    Peer peer4_;
    CSTConfigGenerator cg1_;
    CSTConfigGenerator cg2_;
    CSTConfigGenerator cg3_;
    CSTConfigGenerator cg4_;
    int electionTimeoutMs_;
    int snapshotIntervalS_;
    braft::cli::CliOptions defaultCliOpt_;

    std::map<int, int> paramsIndexs_;
    std::vector<char **> params_;
};


/**
 *Verify continuous recovery of copyset through snapshots
 *1. Create a replication group of 3 replicas
 *2. Hang up a follower
 *3. Write data and wait for the raft snapshot to be generated
 *4. Start the failed follower and restore it through snapshot
 *5. Transfer the leader to the newly started follower and read the data for verification
 *6. Remove old leader, mainly to delete its copyset directory
 *7. Add a new peer to load data through a snapshot
 *8. Transfer leader to newly added peer, read data validation
 */
TEST_F(RaftSnapshotTest, AddPeerRecoverFromSnapshot) {
    LogicPoolID logicPoolId = 2;
    CopysetID copysetId = 100001;
    uint64_t chunkId = 1;
    uint64_t initsn = 1;
    int length = kOpRequestAlignSize;
    char ch = 'a';
    int loop = 25;

    std::vector<Peer> peers;
    peers.push_back(peer1_);
    peers.push_back(peer2_);
    peers.push_back(peer3_);

    PeerCluster cluster("ThreeNode-cluster",
                        logicPoolId,
                        copysetId,
                        peers,
                        params_,
                        paramsIndexs_);
    ASSERT_EQ(0, cluster.StartFakeTopoloyService(kFakeMdsAddr));
    ASSERT_EQ(0, cluster.StartPeer(peer1_, PeerCluster::PeerToId(peer1_)));
    ASSERT_EQ(0, cluster.StartPeer(peer2_, PeerCluster::PeerToId(peer2_)));
    ASSERT_EQ(0, cluster.StartPeer(peer3_, PeerCluster::PeerToId(peer3_)));

    Peer leaderPeer;
    ASSERT_EQ(0, cluster.WaitLeader(&leaderPeer));
    Peer oldLeader = leaderPeer;

    //Hang up a follower
    Peer shutdownPeer;
    if (leaderPeer.address() == peer1_.address()) {
        shutdownPeer = peer2_;
    } else {
        shutdownPeer = peer1_;
    }
    LOG(INFO) << "shutdown peer: " << shutdownPeer.address();
    LOG(INFO) << "leader peer: " << leaderPeer.address();
    ASSERT_EQ(0, cluster.ShutdownPeer(shutdownPeer));

    LOG(INFO) << "write 1 start";
    // Initiate read/write to generate a chunk file
    WriteThenReadVerify(leaderPeer,
                        logicPoolId,
                        copysetId,
                        chunkId,
                        length,
                        ch,
                        loop,
                        initsn);

    LOG(INFO) << "write 1 end";
    // wait snapshot to ensure that it can trigger a snapshot
    ::sleep(1.5*snapshotIntervalS_);

    // restart, needs to be restored from install snapshot
    ASSERT_EQ(0, cluster.StartPeer(shutdownPeer,
                                   PeerCluster::PeerToId(shutdownPeer)));

    // Wait shutdown peer recovery, and then transfer leader to it
    ::sleep(3);
    TransferLeaderAssertSuccess(&cluster, shutdownPeer, defaultCliOpt_);
    leaderPeer = shutdownPeer;
    // Read Data Validation
    ReadVerify(leaderPeer, logicPoolId, copysetId, chunkId,
               length, ch, loop);
    Configuration conf = cluster.CopysetConf();
    // Delete old leader and its directory
    butil::Status status =
        RemovePeer(logicPoolId, copysetId, conf, oldLeader, defaultCliOpt_);
    ASSERT_TRUE(status.ok());
    std::string rmdir("rm -fr ");
        rmdir += std::to_string(PeerCluster::PeerToId(oldLeader));
    ::system(rmdir.c_str());

    // Add a new peer
    ASSERT_EQ(0, cluster.StartPeer(peer4_,
                                   PeerCluster::PeerToId(peer4_)));
    status = AddPeer(logicPoolId, copysetId, conf, peer4_, defaultCliOpt_);
    ASSERT_TRUE(status.ok()) << status;
    // Transfer leader to peer4_, And read it out for verification
    TransferLeaderAssertSuccess(&cluster, peer4_, defaultCliOpt_);
    leaderPeer = peer4_;
    // Read Data Validation
    ReadVerify(leaderPeer, logicPoolId, copysetId, chunkId,
               length, ch, loop);
}

/**
 *Verify the shutdown of non leader nodes on three nodes, restart, and control the recovery from install snapshot
 * 1. Create a replication group of 3 replicas
 * 2. Wait for the leader to generate, write the data, and then read it out for verification
 * 3. Shutdown non leader
 * 4. Then sleep exceeds one snapshot interval, write read data,
 * 5. Then sleep for more than one snapshot interval and write read data; 4,5 two-step
 *    It is to ensure that at least two snapshots are taken, so that when the node restarts again, it must pass the install snapshot
 * 6. Wait for the leader to be generated, and then verify the data written before the read
 * 7. Transfer leader to shut down peer
 * 8. Verification of data written before read
 * 9. Write the data again and read it out for verification
 */
TEST_F(RaftSnapshotTest, ShutdownOnePeerRestartFromInstallSnapshot) {
    LogicPoolID logicPoolId = 2;
    CopysetID copysetId = 100001;
    uint64_t chunkId = 1;
    uint64_t initsn = 1;
    int length = kOpRequestAlignSize;
    char ch = 'a';
    int loop = 25;

    std::vector<Peer> peers;
    peers.push_back(peer1_);
    peers.push_back(peer2_);
    peers.push_back(peer3_);

    PeerCluster cluster("ThreeNode-cluster",
                        logicPoolId,
                        copysetId,
                        peers,
                        params_,
                        paramsIndexs_);
    ASSERT_EQ(0, cluster.StartFakeTopoloyService(kFakeMdsAddr));
    ASSERT_EQ(0, cluster.StartPeer(peer1_, PeerCluster::PeerToId(peer1_)));
    ASSERT_EQ(0, cluster.StartPeer(peer2_, PeerCluster::PeerToId(peer2_)));
    ASSERT_EQ(0, cluster.StartPeer(peer3_, PeerCluster::PeerToId(peer3_)));

    Peer leaderPeer;
    ASSERT_EQ(0, cluster.WaitLeader(&leaderPeer));

    LOG(INFO) << "write 1 start";
    // Initiate read/write to generate a chunk file
    WriteThenReadVerify(leaderPeer,
                        logicPoolId,
                        copysetId,
                        chunkId,
                        length,
                        ch,
                        loop,
                        initsn);

    LOG(INFO) << "write 1 end";
    // The operations between replicas within the raft are not all synchronized, and there may be outdated replica operations
    // So take a nap first to prevent concurrent statistics of file information
    ::sleep(2);

    // shutdown a certain follower
    Peer shutdownPeer;
    if (leaderPeer.address() == peer1_.address()) {
        shutdownPeer = peer2_;
    } else {
        shutdownPeer = peer1_;
    }
    LOG(INFO) << "shutdown peer: " << shutdownPeer.address();
    LOG(INFO) << "leader peer: " << leaderPeer.address();
    ASSERT_EQ(0, cluster.ShutdownPeer(shutdownPeer));

    // wait snapshot to ensure that it can trigger a snapshot
    // In addition, by increasing the chunk version number, trigger the chunk file to generate a snapshot file
    ::sleep(1.5*snapshotIntervalS_);
    // Initiate read/write again
    LOG(INFO) << "write 2 start";
    WriteThenReadVerify(leaderPeer,
                        logicPoolId,
                        copysetId,
                        chunkId,
                        length,
                        ch + 1,
                        loop,
                        initsn + 1);
    LOG(INFO) << "write 2 end";
    // Verify the correctness of chunk snapshot data
    ReadSnapshotVerify(leaderPeer,
                       logicPoolId,
                       copysetId,
                       chunkId,
                       length,
                       ch,
                       loop);

    // wait snapshot to ensure that it can trigger a snapshot
    ::sleep(1.5*snapshotIntervalS_);

    // restart, needs to be restored from install snapshot
    ASSERT_EQ(0, cluster.StartPeer(shutdownPeer,
                                   PeerCluster::PeerToId(shutdownPeer)));
    ASSERT_EQ(0, cluster.WaitLeader(&leaderPeer));

    LOG(INFO) << "write 3 start";
    // Initiate read/write again
    WriteThenReadVerify(leaderPeer,
                        logicPoolId,
                        copysetId,
                        chunkId,
                        length,
                        ch + 2,
                        loop,
                        initsn + 1);

    LOG(INFO) << "write 3 end";

    // Wait shutdown peer recovery, and then transfer leader to it
    ::sleep(3);
    TransferLeaderAssertSuccess(&cluster, shutdownPeer, defaultCliOpt_);
    leaderPeer = shutdownPeer;
    ReadVerify(leaderPeer, logicPoolId, copysetId, chunkId,
               length, ch + 2, loop);
    ReadSnapshotVerify(leaderPeer, logicPoolId, copysetId, chunkId,
                       length, ch, loop);
}

/**
 *Verify the shutdown of non leader nodes on three nodes, restart, and control the recovery from install snapshot
 * 1. Create a replication group of 3 replicas
 * 2. Wait for the leader to generate, write the data, and update the write version to generate a chunk snapshot
 * 3. Shutdown non leader
 * 4. Then the sleep exceeds one snapshot interval,
 * 5. Delete the chunk snapshot and write the data again with a new version to generate a new chunk snapshot
 * 6. Then sleep more than one snapshot interval; 4,5 two-step
 *    It is to ensure that at least two snapshots are taken, so that when the node restarts again, it must pass the install snapshot
 * 7. Wait for the leader to be generated, and then verify the data written before the read
 * 8. Transfer leader to shut down peer
 * 9. Verification of data written before read
 */
TEST_F(RaftSnapshotTest, DoCurveSnapshotAfterShutdownPeerThenRestart) {
    LogicPoolID logicPoolId = 2;
    CopysetID copysetId = 100001;
    uint64_t chunkId = 1;
    uint64_t initsn = 1;
    int length = kOpRequestAlignSize;
    char ch = 'a';
    int loop = 25;

    std::vector<Peer> peers;
    peers.push_back(peer1_);
    peers.push_back(peer2_);
    peers.push_back(peer3_);

    PeerCluster cluster("ThreeNode-cluster",
                        logicPoolId,
                        copysetId,
                        peers,
                        params_,
                        paramsIndexs_);
    ASSERT_EQ(0, cluster.StartFakeTopoloyService(kFakeMdsAddr));
    ASSERT_EQ(0, cluster.StartPeer(peer1_, PeerCluster::PeerToId(peer1_)));
    ASSERT_EQ(0, cluster.StartPeer(peer2_, PeerCluster::PeerToId(peer2_)));
    ASSERT_EQ(0, cluster.StartPeer(peer3_, PeerCluster::PeerToId(peer3_)));

    Peer leaderPeer;
    ASSERT_EQ(0, cluster.WaitLeader(&leaderPeer));

    LOG(INFO) << "write 1 start";
    // Initiate read/write to generate a chunk file
    WriteThenReadVerify(leaderPeer,
                        logicPoolId,
                        copysetId,
                        chunkId,
                        length,
                        ch,  // a
                        loop,
                        initsn);

    LOG(INFO) << "write 1 end";

    LOG(INFO) << "write 2 start";
    // Initiate read/write, generate chunk files, and generate snapshot files
    WriteThenReadVerify(leaderPeer,
                        logicPoolId,
                        copysetId,
                        chunkId,
                        length,
                        ++ch,  // b
                        loop,
                        initsn+1);  // sn = 2
    // Verify the correctness of chunk snapshot data
    ReadSnapshotVerify(leaderPeer,
                       logicPoolId,
                       copysetId,
                       chunkId,
                       length,
                       ch-1,  // a
                       loop);

    LOG(INFO) << "write 2 end";
    // The operations between replicas within the raft are not all synchronized, and there may be outdated replica operations
    // So take a nap first to prevent concurrent statistics of file information
    ::sleep(2);

    // shutdown a certain follower
    Peer shutdownPeer;
    if (leaderPeer.address() == peer1_.address()) {
        shutdownPeer = peer2_;
    } else {
        shutdownPeer = peer1_;
    }
    LOG(INFO) << "shutdown peer: " << shutdownPeer.address();
    LOG(INFO) << "leader peer: " << leaderPeer.address();
    ASSERT_EQ(0, cluster.ShutdownPeer(shutdownPeer));

    // wait snapshot to ensure that it can trigger a snapshot
    // In addition, by increasing the chunk version number, trigger the chunk file to generate a snapshot file
    ::sleep(1.5*snapshotIntervalS_);

    // Delete old snapshots
    DeleteSnapshotVerify(leaderPeer,
                         logicPoolId,
                         copysetId,
                         chunkId,
                         initsn + 1);  // csn = 2

    // Initiate read/write again
    LOG(INFO) << "write 3 start";
    WriteThenReadVerify(leaderPeer,
                        logicPoolId,
                        copysetId,
                        chunkId,
                        length,
                        ++ch,  // c
                        loop,
                        initsn + 2);  // sn = 3
    LOG(INFO) << "write 3 end";
    // Verify the correctness of chunk snapshot data
    ReadSnapshotVerify(leaderPeer,
                       logicPoolId,
                       copysetId,
                       chunkId,
                       length,
                       ch-1,  // b
                       loop);

    // wait snapshot to ensure that it can trigger a snapshot
    ::sleep(1.5*snapshotIntervalS_);

    // restart, needs to be restored from install snapshot
    ASSERT_EQ(0, cluster.StartPeer(shutdownPeer,
                                   PeerCluster::PeerToId(shutdownPeer)));
    ASSERT_EQ(0, cluster.WaitLeader(&leaderPeer));

    // Wait shutdown peer recovery, and then transfer leader to it
    ::sleep(3);
    TransferLeaderAssertSuccess(&cluster, shutdownPeer, defaultCliOpt_);
    leaderPeer = shutdownPeer;
    ReadVerify(leaderPeer, logicPoolId, copysetId, chunkId,
               length, ch, loop);
    ReadSnapshotVerify(leaderPeer, logicPoolId, copysetId, chunkId,
                       length, ch-1, loop);
}

/**
 * During the process of verifying the curve snapshot dump, if there are multiple copysets in the chunkserver,
 * 1. Create a replication group of 3 replicas
 * 2. Generate a new copyset for each replication group's chunkserver and use it as a subsequent operation object
 * 3. Wait for the leader to generate and write data
 * 4. If the sleep exceeds one snapshot interval, ensure that a raft snapshot is generated
 * 5. Update the write version to generate a chunk snapshot
 * 6. Then the sleep exceeds one snapshot interval to ensure that a raft snapshot is generated
 * 7. Shutdown non leader
 * 8. Add a new node to AddPeer and restore it by loading a snapshot, then remove the shutdown peer
 * 9. Switch the leader to the newly added peer
 * 10. Wait for the leader to be generated, then read the data and chunk snapshot generated before validation
 */
TEST_F(RaftSnapshotTest, AddPeerWhenDoingCurveSnapshotWithMultiCopyset) {
    LogicPoolID logicPoolId = 2;
    CopysetID copysetId = 100001;
    uint64_t chunkId = 1;
    uint64_t initsn = 1;
    int length = kOpRequestAlignSize;
    char ch = 'a';
    int loop = 25;

    std::vector<Peer> peers;
    peers.push_back(peer1_);
    peers.push_back(peer2_);
    peers.push_back(peer3_);

    PeerCluster cluster("ThreeNode-cluster",
                        logicPoolId,
                        copysetId,
                        peers,
                        params_,
                        paramsIndexs_);
    ASSERT_EQ(0, cluster.StartFakeTopoloyService(kFakeMdsAddr));
    ASSERT_EQ(0, cluster.StartPeer(peer1_, PeerCluster::PeerToId(peer1_)));
    ASSERT_EQ(0, cluster.StartPeer(peer2_, PeerCluster::PeerToId(peer2_)));
    ASSERT_EQ(0, cluster.StartPeer(peer3_, PeerCluster::PeerToId(peer3_)));

    // Create a new copyset
    LOG(INFO) << "create new copyset.";
    ++copysetId;
    int ret = cluster.CreateCopyset(logicPoolId, copysetId, peer1_, peers);
    ASSERT_EQ(0, ret);
    ret = cluster.CreateCopyset(logicPoolId, copysetId, peer2_, peers);
    ASSERT_EQ(0, ret);
    ret = cluster.CreateCopyset(logicPoolId, copysetId, peer3_, peers);
    ASSERT_EQ(0, ret);

    // Use the new copyset as the operand
    cluster.SetWorkingCopyset(copysetId);

    Peer leaderPeer;
    ASSERT_EQ(0, cluster.WaitLeader(&leaderPeer));

    LOG(INFO) << "write 1 start";
    // Initiate read/write to generate a chunk file
    WriteThenReadVerify(leaderPeer,
                        logicPoolId,
                        copysetId,
                        chunkId,
                        length,
                        ch,  // a
                        loop,
                        initsn);

    LOG(INFO) << "write 1 end";

    // Wait snapshot to ensure that it can trigger a snapshot
    ::sleep(1.5*snapshotIntervalS_);

    LOG(INFO) << "write 2 start";
    // Initiate read/write, generate chunk files, and generate snapshot files
    WriteThenReadVerify(leaderPeer,
                        logicPoolId,
                        copysetId,
                        chunkId,
                        length,
                        ++ch,  // b
                        loop,
                        initsn+1);  // sn = 2
    // Verify the correctness of chunk snapshot data
    ReadSnapshotVerify(leaderPeer,
                       logicPoolId,
                       copysetId,
                       chunkId,
                       length,
                       ch-1,  // a
                       loop);

    LOG(INFO) << "write 2 end";
    // The operations between replicas within the raft are not all synchronized, and there may be outdated replica operations
    // So take a nap first to prevent concurrent statistics of file information
    ::sleep(2);

    // Wait snapshot to ensure that it can trigger a snapshot
    // Ensure that the newly added peer is installed by downloading the snapshot by taking at least two snapshots
    ::sleep(1.5*snapshotIntervalS_);

    // Shutdown a certain follower
    Peer shutdownPeer;
    if (leaderPeer.address() == peer1_.address()) {
        shutdownPeer = peer2_;
    } else {
        shutdownPeer = peer1_;
    }
    LOG(INFO) << "shutdown peer: " << shutdownPeer.address();
    LOG(INFO) << "leader peer: " << leaderPeer.address();
    ASSERT_EQ(0, cluster.ShutdownPeer(shutdownPeer));

    // Add a new peer and remove the shutdown peer
    Configuration conf = cluster.CopysetConf();
    ASSERT_EQ(0, cluster.StartPeer(peer4_,
                                   PeerCluster::PeerToId(peer4_)));
    butil::Status status =
        AddPeer(logicPoolId, copysetId, conf, peer4_, defaultCliOpt_);
    ASSERT_TRUE(status.ok());

    // Delete old leader and its directory
    status =
        RemovePeer(logicPoolId, copysetId, conf, shutdownPeer, defaultCliOpt_);
    ASSERT_TRUE(status.ok());
    std::string rmdir("rm -fr ");
        rmdir += std::to_string(PeerCluster::PeerToId(shutdownPeer));
    ::system(rmdir.c_str());

    // Transfer leader to peer4_, And read it out for verification
    TransferLeaderAssertSuccess(&cluster, peer4_, defaultCliOpt_);
    leaderPeer = peer4_;
    // Read Data Validation
    ReadVerify(leaderPeer, logicPoolId, copysetId, chunkId,
               length, ch, loop);
    ReadSnapshotVerify(leaderPeer, logicPoolId, copysetId, chunkId,
                       length, ch-1, loop);
}

}  // namespace chunkserver
}  // namespace curve
