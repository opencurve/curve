/*
 * Project: curve
 * File Created: Thursday, 13th June 2019 4:14:55 pm
 * Author: tongguangxun
 * Copyright (c)￼ 2018 netease
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

static char *raftVoteParam[3][13] = {
    {
        "chunkserver",
        "-chunkServerIp=127.0.0.1",
        "-chunkServerPort=9321",
        "-chunkServerStoreUri=local://./9321/",
        "-chunkServerMetaUri=local://./9321/chunkserver.dat",
        "-copySetUri=local://./9321/copysets",
        "-recycleUri=local://./9321/recycler",
        "-chunkFilePoolDir=./9321/chunkfilepool/",
        "-chunkFilePoolMetaPath=./9321/chunkfilepool.meta",
        "-conf=./9321/chunkserver.conf",
        "-raft_sync_segments=true",
        NULL
    },
    {
        "chunkserver",
        "-chunkServerIp=127.0.0.1",
        "-chunkServerPort=9322",
        "-chunkServerStoreUri=local://./9322/",
        "-chunkServerMetaUri=local://./9322/chunkserver.dat",
        "-copySetUri=local://./9322/copysets",
        "-recycleUri=local://./9322/recycler",
        "-chunkFilePoolDir=./9322/chunkfilepool/",
        "-chunkFilePoolMetaPath=./9322/chunkfilepool.meta",
        "-conf=./9322/chunkserver.conf",
        "-raft_sync_segments=true",
        NULL
    },
    {
        "chunkserver",
        "-chunkServerIp=127.0.0.1",
        "-chunkServerPort=9323",
        "-chunkServerStoreUri=local://./9323/",
        "-chunkServerMetaUri=local://./9323/chunkserver.dat",
        "-copySetUri=local://./9323/copysets",
        "-recycleUri=local://./9323/recycler",
        "-chunkFilePoolDir=./9323/chunkfilepool/",
        "-chunkFilePoolMetaPath=./9323/chunkfilepool.meta",
        "-conf=./9323/chunkserver.conf",
        "-raft_sync_segments=true",
        NULL
    },
};

class RaftSnapshotTest : public testing::Test {
 protected:
    virtual void SetUp() {
        peer1.set_address("127.0.0.1:9321:0");
        peer2.set_address("127.0.0.1:9322:0");
        peer3.set_address("127.0.0.1:9323:0");

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

        ASSERT_TRUE(cg1.Init("9321"));
        ASSERT_TRUE(cg2.Init("9322"));
        ASSERT_TRUE(cg3.Init("9323"));
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
};


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
    int snapshotTimeoutS = 2;

    std::vector<Peer> peers;
    peers.push_back(peer1);
    peers.push_back(peer2);
    peers.push_back(peer3);

    PeerCluster cluster("ThreeNode-cluster",
                        logicPoolId,
                        copysetId,
                        peers,
                        params,
                        paramsIndexs);
    cluster.SetElectionTimeoutMs(electionTimeoutMs);
    cluster.SetsnapshotIntervalS(snapshotTimeoutS);
    ASSERT_EQ(0, cluster.StartPeer(peer1, PeerCluster::PeerToId(peer1)));
    ASSERT_EQ(0, cluster.StartPeer(peer2, PeerCluster::PeerToId(peer2)));
    ASSERT_EQ(0, cluster.StartPeer(peer3, PeerCluster::PeerToId(peer3)));

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
    ::sleep(1*snapshotTimeoutS);

    // shutdown 某个follower
    Peer shutdownPeer;
    if (leaderPeer.address() == peer1.address()) {
        shutdownPeer = peer2;
    } else {
        shutdownPeer = peer1;
    }
    LOG(INFO) << "shutdown peer: " << shutdownPeer.address();
    LOG(INFO) << "leader peer: " << leaderPeer.address();
    ASSERT_EQ(0, cluster.ShutdownPeer(shutdownPeer));

    // wait snapshot, 保证能够触发打快照
    // 此外通过增加chunk版本号，触发chunk文件产生快照文件
    ::sleep(1.5*snapshotTimeoutS);
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
    ::sleep(1.5*snapshotTimeoutS);

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
                                shutdownPeer,
                                options);
        if (0 == status.error_code()) {
            cluster.WaitLeader(&leaderPeer);
            if (leaderPeer.address() == shutdownPeer.address()) {
                break;
            }
        }
        ::sleep(1);
    }

    ASSERT_STREQ(shutdownPeer.address().c_str(), leaderPeer.address().c_str());

    ReadVerify(leaderPeer, logicPoolId, copysetId, chunkId,
               length, ch + 2, loop);
    ReadSnapshotVerify(leaderPeer, logicPoolId, copysetId, chunkId,
                       length, ch, loop);
}

}  // namespace chunkserver
}  // namespace curve
