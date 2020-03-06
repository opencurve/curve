/*
 * Project: curve
 * Created Date: 2020-03-03
 * Author: qinyi
 * Copyright (c) 2020 netease
 */

#include <brpc/channel.h>
#include <butil/at_exit.h>
#include <gtest/gtest.h>

#include <vector>

#include "test/chunkserver/datastore/chunkfilepool_helper.h"
#include "test/integration/common/chunkservice_op.h"
#include "test/integration/common/config_generator.h"
#include "test/integration/common/peer_cluster.h"

namespace curve {
namespace chunkserver {

using curve::common::Thread;
using curve::fs::FileSystemType;
using curve::fs::LocalFileSystem;
using curve::fs::LocalFsFactory;

#define BASIC_TEST_CHUNK_SERVER_PORT "9078"

static char *chunkServerParams[1][13] = {
    { "chunkserver", "-chunkServerIp=127.0.0.1",
      "-chunkServerPort=" BASIC_TEST_CHUNK_SERVER_PORT,
      "-chunkServerStoreUri=local://./" BASIC_TEST_CHUNK_SERVER_PORT "/",
      "-chunkServerMetaUri=local://./" BASIC_TEST_CHUNK_SERVER_PORT
      "/chunkserver.dat",
      "-copySetUri=local://./" BASIC_TEST_CHUNK_SERVER_PORT "/copysets",
      "-recycleUri=local://./" BASIC_TEST_CHUNK_SERVER_PORT "/recycler",
      "-chunkFilePoolDir=./" BASIC_TEST_CHUNK_SERVER_PORT "/chunkfilepool/",
      "-chunkFilePoolMetaPath=./" BASIC_TEST_CHUNK_SERVER_PORT
      "/chunkfilepool.meta",
      "-conf=./" BASIC_TEST_CHUNK_SERVER_PORT "/chunkserver.conf",
      "-raft_sync_segments=true", NULL },
};

butil::AtExitManager atExitManager;
const int kChunkNum = 10;
const ChunkSizeType kChunkSize = 16 * 1024 * 1024;
const PageSizeType kPageSize = kOpRequestAlignSize;

class ChunkServerIoTest : public testing::Test {
 protected:
    virtual void SetUp() {
        peer1_.set_address("127.0.0.1:" BASIC_TEST_CHUNK_SERVER_PORT ":0");
        leaderPeer_.set_address(peer1_.address());
        peers_.push_back(peer1_);
        leaderId_.parse(peer1_.address());

        std::string mkdir1("mkdir ");
        mkdir1 += std::to_string(PeerCluster::PeerToId(peer1_));
        ::system(mkdir1.c_str());

        electionTimeoutMs_ = 3000;
        snapshotIntervalS_ = 60;
        logicPoolId_ = 1;
        copysetId_ = 1;

        ASSERT_TRUE(cg1_.Init(BASIC_TEST_CHUNK_SERVER_PORT));
        cg1_.SetKV("copyset.election_timeout_ms", "3000");
        cg1_.SetKV("copyset.snapshot_interval_s", "60");
        cg1_.SetKV("chunkfilepool.enable_get_chunk_from_pool", "true");
        ASSERT_TRUE(cg1_.Generate());

        paramsIndexs_[PeerCluster::PeerToId(peer1_)] = 0;
        params_.push_back(chunkServerParams[0]);

        // 初始化chunkfilepool，这里会预先分配一些chunk
        lfs_ = LocalFsFactory::CreateFs(FileSystemType::EXT4, "");
        poolDir_ = "./" + std::to_string(PeerCluster::PeerToId(peer1_)) +
                   "/chunkfilepool/";
        metaDir_ = "./" + std::to_string(PeerCluster::PeerToId(peer1_)) +
                   "/chunkfilepool.meta";
        ChunkfilePoolHelper::PersistEnCodeMetaInfo(lfs_, kChunkSize, kPageSize,
                                                   poolDir_, metaDir_);
        allocateChunk(lfs_, kChunkNum, poolDir_);
    }

    virtual void TearDown() {
        std::string rmdir1("rm -fr ");
        rmdir1 += std::to_string(PeerCluster::PeerToId(peer1_));

        ::system(rmdir1.c_str());

        //  等待进程结束
        ::usleep(100 * 1000);
    }

    int InitCluster(PeerCluster *cluster) {
        PeerId leaderId;
        Peer leaderPeer;
        cluster->SetElectionTimeoutMs(electionTimeoutMs_);
        cluster->SetsnapshotIntervalS(snapshotIntervalS_);
        if (cluster->StartPeer(peer1_, PeerCluster::PeerToId(peer1_))) {
            LOG(ERROR) << "StartPeer failed";
            return -1;
        }

        // 等待leader产生
        if (cluster->WaitLeader(&leaderPeer_)) {
            LOG(ERROR) << "WaiteLeader failed";
            return -1;
        }

        if (leaderId_.parse(leaderPeer_.address())) {
            LOG(ERROR) << "parse leaderPeer address failed";
            return -1;
        }

        if (strcmp(peer1_.address().c_str(), leaderId_.to_string().c_str())) {
            LOG(ERROR) << "check leaderId failed";
            return -1;
        }

        return 0;
    }

 public:
    std::vector<Peer> peers_;
    Peer leaderPeer_;
    LogicPoolID logicPoolId_;
    CopysetID copysetId_;

    std::map<int, int> paramsIndexs_;
    std::vector<char **> params_;

 private:
    Peer peer1_;
    CSTConfigGenerator cg1_;
    PeerId leaderId_;

    int electionTimeoutMs_;
    int snapshotIntervalS_;

    std::string poolDir_;
    std::string metaDir_;
    std::shared_ptr<LocalFileSystem> lfs_;
};

TEST_F(ChunkServerIoTest, BasicIO) {
    uint64_t chunkId = 1;
    off_t offset = 0;
    int length = kOpRequestAlignSize;
    int ret = 0;
    const SequenceNum sn1 = 1;
    std::string data(length * 4, 0);
    // 用例初始化时chunk文件以'0'填充，因此这里同样以'0'填充
    std::string chunkData(kChunkSize, '0');
    std::string leader = "";
    struct ChunkServiceOpConf opConf = { &leaderPeer_, logicPoolId_, copysetId_,
                                         2000 };
    ChunkServiceVerify *verify = new ChunkServiceVerify(&opConf);

    PeerCluster cluster("InitShutdown-cluster", logicPoolId_, copysetId_,
                        peers_, params_, paramsIndexs_);
    ASSERT_EQ(0, InitCluster(&cluster));

    /* 场景一：新建的文件，Chunk文件不存在 */
    ASSERT_EQ(0, verify->VerifyReadChunk(chunkId, sn1, 0, length, nullptr));
    ASSERT_EQ(0, verify->VerifyGetChunkInfo(chunkId, NULL_SN, NULL_SN, leader));
    ASSERT_EQ(0, verify->VerifyDeleteChunk(chunkId, sn1));

    /* 场景二：通过WriteChunk产生chunk文件后操作 */
    data.assign(length, 'a');
    ASSERT_EQ(0, verify->VerifyWriteChunk(chunkId, sn1, 0, 4096, data.c_str(),
                                          &chunkData));
    ASSERT_EQ(0, verify->VerifyGetChunkInfo(chunkId, sn1, NULL_SN, leader));
    ASSERT_EQ(0, verify->VerifyReadChunk(chunkId, sn1, 0, 4096, &chunkData));
    ASSERT_EQ(0, verify->VerifyReadChunk(chunkId, sn1, kChunkSize - 4096, 4096,
                                         nullptr));
    data.assign(length, 'b');
    ASSERT_EQ(0, verify->VerifyWriteChunk(chunkId, sn1, 0, 4096, data.c_str(),
                                          &chunkData));
    ASSERT_EQ(0, verify->VerifyReadChunk(chunkId, sn1, 0, 12288, &chunkData));
    data.assign(length, 'c');
    ASSERT_EQ(0, verify->VerifyWriteChunk(chunkId, sn1, 4096, 4096,
                                          data.c_str(), &chunkData));
    ASSERT_EQ(0, verify->VerifyReadChunk(chunkId, sn1, 0, 12288, &chunkData));
    data.assign(length * 2, 'd');
    ASSERT_EQ(0, verify->VerifyWriteChunk(chunkId, sn1, 4096, 8192,
                                          data.c_str(), &chunkData));
    ASSERT_EQ(0, verify->VerifyReadChunk(chunkId, sn1, 0, 12288, &chunkData));

    /* 场景三：用户删除文件 */
    ASSERT_EQ(0, verify->VerifyDeleteChunk(chunkId, sn1));
    ASSERT_EQ(0, verify->VerifyGetChunkInfo(chunkId, NULL_SN, NULL_SN, leader));
}

}  // namespace chunkserver
}  // namespace curve
