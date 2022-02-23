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

        // 配置默认raft client option
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
 * 验证连续通过快照恢复copyset
 * 1.创建3个副本的复制组
 * 2.挂掉一个follower
 * 3.写入数据，并等待raft snapshot 产生
 * 4.启动挂掉的follower，使其通过snapshot恢复
 * 5.transfer leader到刚启动的follower，读数据验证
 * 6.remove old leader，主要为了删除其copyset目录
 * 7.添加新的peer，使其通过快照加载数据
 * 8.transfer leader到新加入的peer，读数据验证
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

    // 挂掉一个follower
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
    // 发起 read/write,产生chunk文件
    WriteThenReadVerify(leaderPeer,
                        logicPoolId,
                        copysetId,
                        chunkId,
                        length,
                        ch,
                        loop,
                        initsn);

    LOG(INFO) << "write 1 end";
    // wait snapshot,保证能够触发打快照
    ::sleep(1.5*snapshotIntervalS_);

    // restart, 需要从 install snapshot 恢复
    ASSERT_EQ(0, cluster.StartPeer(shutdownPeer,
                                   PeerCluster::PeerToId(shutdownPeer)));

    // Wait shutdown peer recovery, and then transfer leader to it
    ::sleep(3);
    TransferLeaderAssertSuccess(&cluster, shutdownPeer, defaultCliOpt_);
    leaderPeer = shutdownPeer;
    // 读数据验证
    ReadVerify(leaderPeer, logicPoolId, copysetId, chunkId,
               length, ch, loop);
    Configuration conf = cluster.CopysetConf();
    // 删除旧leader及其目录
    butil::Status status =
        RemovePeer(logicPoolId, copysetId, conf, oldLeader, defaultCliOpt_);
    ASSERT_TRUE(status.ok());
    std::string rmdir("rm -fr ");
        rmdir += std::to_string(PeerCluster::PeerToId(oldLeader));
    ::system(rmdir.c_str());

    // 添加新的peer
    ASSERT_EQ(0, cluster.StartPeer(peer4_,
                                   PeerCluster::PeerToId(peer4_)));
    status = AddPeer(logicPoolId, copysetId, conf, peer4_, defaultCliOpt_);
    ASSERT_TRUE(status.ok()) << status;
    // transfer leader 到peer4_，并读出来验证
    TransferLeaderAssertSuccess(&cluster, peer4_, defaultCliOpt_);
    leaderPeer = peer4_;
    // 读数据验证
    ReadVerify(leaderPeer, logicPoolId, copysetId, chunkId,
               length, ch, loop);
}

/**
 * 验证3个节点的关闭非 leader 节点，重启，控制让其从 install snapshot 恢复
 * 1. 创建3个副本的复制组
 * 2. 等待 leader 产生，write 数据，然后 read 出来验证一遍
 * 3. shutdown 非 leader
 * 4. 然后 sleep 超过一个 snapshot interval，write read 数据,
 * 5. 然后再 sleep 超过一个 snapshot interval，write read 数据；4,5两步
 *    是为了保证打至少两次快照，这样，节点再重启的时候必须通过 install snapshot
 * 6. 等待 leader 产生，然后 read 之前写入的数据验证一遍
 * 7. transfer leader 到shut down 的peer 上
 * 8. 在 read 之前写入的数据验证
 * 9. 再 write 数据，再 read 出来验证一遍
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
    // 发起 read/write,产生chunk文件
    WriteThenReadVerify(leaderPeer,
                        logicPoolId,
                        copysetId,
                        chunkId,
                        length,
                        ch,
                        loop,
                        initsn);

    LOG(INFO) << "write 1 end";
    // raft内副本之间的操作并不是全部同步的，可能存在落后的副本操作
    // 所以先睡一会，防止并发统计文件信息
    ::sleep(2);

    // shutdown 某个follower
    Peer shutdownPeer;
    if (leaderPeer.address() == peer1_.address()) {
        shutdownPeer = peer2_;
    } else {
        shutdownPeer = peer1_;
    }
    LOG(INFO) << "shutdown peer: " << shutdownPeer.address();
    LOG(INFO) << "leader peer: " << leaderPeer.address();
    ASSERT_EQ(0, cluster.ShutdownPeer(shutdownPeer));

    // wait snapshot, 保证能够触发打快照
    // 此外通过增加chunk版本号，触发chunk文件产生快照文件
    ::sleep(1.5*snapshotIntervalS_);
    // 再次发起 read/write
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
    // 验证chunk快照数据正确性
    ReadSnapshotVerify(leaderPeer,
                       logicPoolId,
                       copysetId,
                       chunkId,
                       length,
                       ch,
                       loop);

    // wait snapshot, 保证能够触发打快照
    ::sleep(1.5*snapshotIntervalS_);

    // restart, 需要从 install snapshot 恢复
    ASSERT_EQ(0, cluster.StartPeer(shutdownPeer,
                                   PeerCluster::PeerToId(shutdownPeer)));
    ASSERT_EQ(0, cluster.WaitLeader(&leaderPeer));

    LOG(INFO) << "write 3 start";
    // 再次发起 read/write
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
 * 验证3个节点的关闭非 leader 节点，重启，控制让其从 install snapshot 恢复
 * 1. 创建3个副本的复制组
 * 2. 等待 leader 产生，write 数据，并更新写版本，产生chunk快照
 * 3. shutdown 非 leader
 * 4. 然后 sleep 超过一个 snapshot interval，
 * 5. 删除chunk快照，再次用新版本write 数据,产生新的chunk快照
 * 6. 然后再 sleep 超过一个 snapshot interval；4,5两步
 *    是为了保证打至少两次快照，这样，节点再重启的时候必须通过 install snapshot
 * 7. 等待 leader 产生，然后 read 之前写入的数据验证一遍
 * 8. transfer leader 到shut down 的peer 上
 * 9. 在 read 之前写入的数据验证
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
    // 发起 read/write,产生chunk文件
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
    // 发起 read/write,产生chunk文件,并产生快照文件
    WriteThenReadVerify(leaderPeer,
                        logicPoolId,
                        copysetId,
                        chunkId,
                        length,
                        ++ch,  // b
                        loop,
                        initsn+1);  // sn = 2
    // 验证chunk快照数据正确性
    ReadSnapshotVerify(leaderPeer,
                       logicPoolId,
                       copysetId,
                       chunkId,
                       length,
                       ch-1,  // a
                       loop);

    LOG(INFO) << "write 2 end";
    // raft内副本之间的操作并不是全部同步的，可能存在落后的副本操作
    // 所以先睡一会，防止并发统计文件信息
    ::sleep(2);

    // shutdown 某个follower
    Peer shutdownPeer;
    if (leaderPeer.address() == peer1_.address()) {
        shutdownPeer = peer2_;
    } else {
        shutdownPeer = peer1_;
    }
    LOG(INFO) << "shutdown peer: " << shutdownPeer.address();
    LOG(INFO) << "leader peer: " << leaderPeer.address();
    ASSERT_EQ(0, cluster.ShutdownPeer(shutdownPeer));

    // wait snapshot, 保证能够触发打快照
    // 此外通过增加chunk版本号，触发chunk文件产生快照文件
    ::sleep(1.5*snapshotIntervalS_);

    // 删除旧的快照
    DeleteSnapshotVerify(leaderPeer,
                         logicPoolId,
                         copysetId,
                         chunkId,
                         initsn + 1);  // csn = 2

    // 再次发起 read/write
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
    // 验证chunk快照数据正确性
    ReadSnapshotVerify(leaderPeer,
                       logicPoolId,
                       copysetId,
                       chunkId,
                       length,
                       ch-1,  // b
                       loop);

    // wait snapshot, 保证能够触发打快照
    ::sleep(1.5*snapshotIntervalS_);

    // restart, 需要从 install snapshot 恢复
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
 * 验证curve快照转储过程当中，chunkserver存在多个copyset情况下，
 * 1. 创建3个副本的复制组
 * 2. 为每个复制组的chunkserver生成新的copyset，并作为后续操作对象
 * 3. 等待 leader 产生，write 数据
 * 4. sleep 超过一个 snapshot interval，确保产生raft快照
 * 5. 更新写版本，产生chunk快照
 * 6. 然后 sleep 超过一个 snapshot interval，确保产生raft快照
 * 7. shutdown 非 leader
 * 8. AddPeer添加一个新节点使其通过加载快照恢复，然后remove掉shutdown的peer
 * 9. 切换leader到新添加的peer
 * 10. 等待 leader 产生，然后 read 之前产生的数据和chunk快照进行验证
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

    // 创建新的copyset
    LOG(INFO) << "create new copyset.";
    ++copysetId;
    int ret = cluster.CreateCopyset(logicPoolId, copysetId, peer1_, peers);
    ASSERT_EQ(0, ret);
    ret = cluster.CreateCopyset(logicPoolId, copysetId, peer2_, peers);
    ASSERT_EQ(0, ret);
    ret = cluster.CreateCopyset(logicPoolId, copysetId, peer3_, peers);
    ASSERT_EQ(0, ret);

    // 使用新的copyset作为操作对象
    cluster.SetWorkingCopyset(copysetId);

    Peer leaderPeer;
    ASSERT_EQ(0, cluster.WaitLeader(&leaderPeer));

    LOG(INFO) << "write 1 start";
    // 发起 read/write,产生chunk文件
    WriteThenReadVerify(leaderPeer,
                        logicPoolId,
                        copysetId,
                        chunkId,
                        length,
                        ch,  // a
                        loop,
                        initsn);

    LOG(INFO) << "write 1 end";

    // wait snapshot, 保证能够触发打快照
    ::sleep(1.5*snapshotIntervalS_);

    LOG(INFO) << "write 2 start";
    // 发起 read/write,产生chunk文件,并产生快照文件
    WriteThenReadVerify(leaderPeer,
                        logicPoolId,
                        copysetId,
                        chunkId,
                        length,
                        ++ch,  // b
                        loop,
                        initsn+1);  // sn = 2
    // 验证chunk快照数据正确性
    ReadSnapshotVerify(leaderPeer,
                       logicPoolId,
                       copysetId,
                       chunkId,
                       length,
                       ch-1,  // a
                       loop);

    LOG(INFO) << "write 2 end";
    // raft内副本之间的操作并不是全部同步的，可能存在落后的副本操作
    // 所以先睡一会，防止并发统计文件信息
    ::sleep(2);

    // wait snapshot, 保证能够触发打快照
    // 通过至少两次快照，保证新加的peer通过下载快照安装
    ::sleep(1.5*snapshotIntervalS_);

    // shutdown 某个follower
    Peer shutdownPeer;
    if (leaderPeer.address() == peer1_.address()) {
        shutdownPeer = peer2_;
    } else {
        shutdownPeer = peer1_;
    }
    LOG(INFO) << "shutdown peer: " << shutdownPeer.address();
    LOG(INFO) << "leader peer: " << leaderPeer.address();
    ASSERT_EQ(0, cluster.ShutdownPeer(shutdownPeer));

    // 添加新的peer，并移除shutdown的peer
    Configuration conf = cluster.CopysetConf();
    ASSERT_EQ(0, cluster.StartPeer(peer4_,
                                   PeerCluster::PeerToId(peer4_)));
    butil::Status status =
        AddPeer(logicPoolId, copysetId, conf, peer4_, defaultCliOpt_);
    ASSERT_TRUE(status.ok());

    // 删除旧leader及其目录
    status =
        RemovePeer(logicPoolId, copysetId, conf, shutdownPeer, defaultCliOpt_);
    ASSERT_TRUE(status.ok());
    std::string rmdir("rm -fr ");
        rmdir += std::to_string(PeerCluster::PeerToId(shutdownPeer));
    ::system(rmdir.c_str());

    // transfer leader 到peer4_，并读出来验证
    TransferLeaderAssertSuccess(&cluster, peer4_, defaultCliOpt_);
    leaderPeer = peer4_;
    // 读数据验证
    ReadVerify(leaderPeer, logicPoolId, copysetId, chunkId,
               length, ch, loop);
    ReadSnapshotVerify(leaderPeer, logicPoolId, copysetId, chunkId,
                       length, ch-1, loop);
}

}  // namespace chunkserver
}  // namespace curve
