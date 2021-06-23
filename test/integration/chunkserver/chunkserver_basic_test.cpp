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
 * Created Date: 2020-03-03
 * Author: qinyi
 */

#include <brpc/channel.h>
#include <butil/at_exit.h>
#include <gtest/gtest.h>

#include <vector>
#include <memory>

#include "test/chunkserver/datastore/filepool_helper.h"
#include "test/integration/common/chunkservice_op.h"
#include "test/integration/common/config_generator.h"
#include "test/integration/common/peer_cluster.h"

namespace curve {
namespace chunkserver {

using curve::common::Thread;
using curve::fs::FileSystemType;
using curve::fs::LocalFileSystem;
using curve::fs::LocalFsFactory;

const uint64_t kMB = 1024 * 1024;
const ChunkSizeType CHUNK_SIZE = 16 * kMB;


#define BASIC_TEST_CHUNK_SERVER_PORT "9078"
#define KB 1024

static char *chunkServerParams[1][16] = {
    { "chunkserver", "-chunkServerIp=127.0.0.1",
      "-chunkServerPort=" BASIC_TEST_CHUNK_SERVER_PORT,
      "-chunkServerStoreUri=local://./" BASIC_TEST_CHUNK_SERVER_PORT "/",
      "-chunkServerMetaUri=local://./" BASIC_TEST_CHUNK_SERVER_PORT
      "/chunkserver.dat",
      "-copySetUri=local://./" BASIC_TEST_CHUNK_SERVER_PORT "/copysets",
      "-raftSnapshotUri=curve://./" BASIC_TEST_CHUNK_SERVER_PORT "/copysets",
      "-raftLogUri=curve://./" BASIC_TEST_CHUNK_SERVER_PORT "/copysets",
      "-recycleUri=local://./" BASIC_TEST_CHUNK_SERVER_PORT "/recycler",
      "-chunkFilePoolDir=./" BASIC_TEST_CHUNK_SERVER_PORT "/chunkfilepool/",
      "-chunkFilePoolMetaPath=./" BASIC_TEST_CHUNK_SERVER_PORT
      "/chunkfilepool.meta",
      "-walFilePoolDir=./" BASIC_TEST_CHUNK_SERVER_PORT "/walfilepool/",
      "-walFilePoolMetaPath=./" BASIC_TEST_CHUNK_SERVER_PORT
      "/walfilepool.meta",
      "-conf=./" BASIC_TEST_CHUNK_SERVER_PORT "/chunkserver.conf",
      "-raft_sync_segments=true", NULL },
};

butil::AtExitManager atExitManager;
const int kChunkNum = 10;
const ChunkSizeType kChunkSize = 16 * 1024 * 1024;
const uint32_t kOpRequestAlignSize = 4096;
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
        externalIp_ = butil::my_ip_cstr();
        cg1_.SetKV("global.external_ip", externalIp_);
        cg1_.SetKV("global.enable_external_server", "true");
        ASSERT_TRUE(cg1_.Generate());

        paramsIndexs_[PeerCluster::PeerToId(peer1_)] = 0;
        params_.push_back(chunkServerParams[0]);

        // 初始化chunkfilepool，这里会预先分配一些chunk
        lfs_ = LocalFsFactory::CreateFs(FileSystemType::EXT4, "");
        poolDir_ = "./" + std::to_string(PeerCluster::PeerToId(peer1_)) +
                   "/chunkfilepool/";
        metaDir_ = "./" + std::to_string(PeerCluster::PeerToId(peer1_)) +
                   "/chunkfilepool.meta";
        FilePoolHelper::PersistEnCodeMetaInfo(lfs_, kChunkSize, kPageSize,
                                                   poolDir_, metaDir_);
        allocateChunk(lfs_, kChunkNum, poolDir_, kChunkSize);
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

    void TestBasicIO(std::shared_ptr<ChunkServiceVerify> verify) {
        uint64_t chunkId = 1;
        off_t offset = 0;
        int length = kOpRequestAlignSize;
        int ret = 0;
        const SequenceNum sn1 = 1;
        std::string data(length * 4, 0);
        // 用例初始化时chunk文件以'0'填充，因此这里同样以'0'填充
        std::string chunkData(kChunkSize, '0');
        std::string leader = "";
        PeerCluster cluster("InitShutdown-cluster", logicPoolId_, copysetId_,
                        peers_, params_, paramsIndexs_);
        ASSERT_EQ(0, InitCluster(&cluster));

        /* 场景一：新建的文件，Chunk文件不存在 */
        ASSERT_EQ(0, verify->VerifyReadChunk(chunkId, sn1, 0, length, nullptr));
        ASSERT_EQ(0, verify->VerifyGetChunkInfo(
                                chunkId, NULL_SN, NULL_SN, leader));
        ASSERT_EQ(0, verify->VerifyDeleteChunk(chunkId, sn1));

        /* 场景二：通过WriteChunk产生chunk文件后操作 */
        data.assign(length, 'a');
        ASSERT_EQ(0, verify->VerifyWriteChunk(chunkId, sn1, 0, 4 * KB,
                                             data.c_str(), &chunkData));
        ASSERT_EQ(0, verify->VerifyGetChunkInfo(chunkId, sn1, NULL_SN, leader));
        ASSERT_EQ(0, verify->VerifyReadChunk(
                                chunkId, sn1, 0, 4 * KB, &chunkData));
        ASSERT_EQ(0, verify->VerifyReadChunk(chunkId, sn1, kChunkSize - 4 * KB,
                                            4 * KB, nullptr));
        data.assign(length, 'b');
        ASSERT_EQ(0, verify->VerifyWriteChunk(chunkId, sn1, 0, 4 * KB,
                                             data.c_str(), &chunkData));
        ASSERT_EQ(0,
              verify->VerifyReadChunk(chunkId, sn1, 0, 12 * KB, &chunkData));
        data.assign(length, 'c');
        ASSERT_EQ(0, verify->VerifyWriteChunk(chunkId, sn1, 4 * KB, 4 * KB,
                                             data.c_str(), &chunkData));
        ASSERT_EQ(0,
              verify->VerifyReadChunk(chunkId, sn1, 0, 12 * KB, &chunkData));
        data.assign(length * 2, 'd');
        ASSERT_EQ(0, verify->VerifyWriteChunk(chunkId, sn1, 4 * KB, 8 * KB,
                                             data.c_str(), &chunkData));
        ASSERT_EQ(0,
              verify->VerifyReadChunk(chunkId, sn1, 0, 12 * KB, &chunkData));

        /* 场景三：用户删除文件 */
        ASSERT_EQ(0, verify->VerifyDeleteChunk(chunkId, sn1));
        ASSERT_EQ(0, verify->VerifyGetChunkInfo(
                                chunkId, NULL_SN, NULL_SN, leader));
    }

    void TestSnapshotIO(std::shared_ptr<ChunkServiceVerify> verify) {
        const uint64_t chunk1 = 1;
        const uint64_t chunk2 = 2;
        const SequenceNum sn1 = 1;
        const SequenceNum sn2 = 2;
        const SequenceNum sn3 = 3;
        off_t offset = 0;
        int length = kOpRequestAlignSize;
        int ret = 0;
        std::string data(length * 4, 0);
        std::string chunkData1a(kChunkSize, 0);  // chunk1版本1预期数据
        std::string chunkData1b(kChunkSize, 0);  // chunk1版本2预期数据
        std::string chunkData1c(kChunkSize, 0);  // chunk1版本3预期数据
        std::string chunkData2(kChunkSize, 0);   // chunk2预期数据
        std::string leader = "";
        PeerCluster cluster("InitShutdown-cluster", logicPoolId_, copysetId_,
                        peers_, params_, paramsIndexs_);
        ASSERT_EQ(0, InitCluster(&cluster));

        // 构造初始环境
        // 写chunk1产生chunk1，chunk1版本为1，chunk2开始不存在。
        data.assign(length, 'a');
        ASSERT_EQ(0, verify->VerifyWriteChunk(chunk1, sn1, 0, 12 * KB,
                                             data.c_str(), &chunkData1a));

        /*
        *  场景一：第一次给文件打快照
        */
        chunkData1b.assign(chunkData1a);  // 模拟对chunk1数据进行COW
        data.assign(length, 'b');
        ASSERT_EQ(0, verify->VerifyWriteChunk(chunk1, sn2, 4 * KB, 4 * KB,
                                             data.c_str(), &chunkData1b));
        // 重复写入同一区域，用于验证不会重复cow
        data.assign(length, 'c');
        ASSERT_EQ(0, verify->VerifyWriteChunk(chunk1, sn2, 4 * KB, 4 * KB,
                                             data.c_str(), &chunkData1b));

        // 读取chunk1快照，预期读到版本1数据
        ASSERT_EQ(0, verify->VerifyReadChunkSnapshot(chunk1, sn1, 0, 12 * KB,
                                                    &chunkData1a));

        // chunk1写[0, 4KB]
        data.assign(length, 'd');
        ASSERT_EQ(0, verify->VerifyWriteChunk(chunk1, sn2, 0, 4 * KB,
                                             data.c_str(), &chunkData1b));
        // chunk1写[4KB, 16KB]
        data.assign(length, 'e');
        ASSERT_EQ(0, verify->VerifyWriteChunk(chunk1, sn2, 4 * KB, 12 * KB,
                                             data.c_str(), &chunkData1b));

        // 获取chunk1信息，预期其版本为2，快照版本为1，
        ASSERT_EQ(0, verify->VerifyGetChunkInfo(chunk1, sn2, sn1, leader));

        // chunk1读[0, 12KB], 预期读到版本2数据
        ASSERT_EQ(0,
              verify->VerifyReadChunk(chunk1, sn2, 0, 12 * KB, &chunkData1b));

        // 读取chunk1的快照, 预期读到版本1数据
        ASSERT_EQ(0, verify->VerifyReadChunkSnapshot(chunk1, sn1, 0, 12 * KB,
                                                    &chunkData1a));

        // 读取chunk2的快照, 预期chunk不存在
        ASSERT_EQ(0, verify->VerifyReadChunkSnapshot(
                                chunk2, sn1, 0, 12 * KB, nullptr));

        /*
         *  场景二：第一次快照结束，删除快照
        */
        // 删除chunk1快照
        ASSERT_EQ(CHUNK_OP_STATUS_SUCCESS,
              verify->VerifyDeleteChunkSnapshotOrCorrectSn(chunk1, sn2));
        // 获取chunk1信息，预期其版本为2，无快照版本
        ASSERT_EQ(0, verify->VerifyGetChunkInfo(chunk1, sn2, NULL_SN, leader));

        // 删chunk2快照，预期成功
        ASSERT_EQ(CHUNK_OP_STATUS_SUCCESS,
              verify->VerifyDeleteChunkSnapshotOrCorrectSn(chunk2, sn2));

        // chunk2写[0, 8KB]
        data.assign(length, 'f');
        ASSERT_EQ(0, verify->VerifyWriteChunk(chunk2, sn2, 0, 8 * KB,
                                             data.c_str(), &chunkData2));
        // 获取chunk2信息，预期其版本为2，无快照版本
        ASSERT_EQ(0, verify->VerifyGetChunkInfo(chunk2, sn2, NULL_SN, leader));

        /*
        *  场景三：第二次打快照
        */
        // chunk1写[0, 8KB]
        chunkData1c.assign(chunkData1b);  // 模拟对chunk1数据进行COW
        data.assign(length, 'g');
        ASSERT_EQ(0, verify->VerifyWriteChunk(chunk1, sn3, 0, 8 * KB,
                                             data.c_str(), &chunkData1c));
        // 获取chunk1信息，预期其版本为3，快照版本为2
        ASSERT_EQ(0, verify->VerifyGetChunkInfo(chunk1, sn3, sn2, leader));

        // 读取chunk1的快照, 预期读到版本2数据
        ASSERT_EQ(0, verify->VerifyReadChunkSnapshot(chunk1, sn2, 0, 12 * KB,
                                                    &chunkData1b));

        // 读取chunk2的快照, 预期读到版本2数据
        ASSERT_EQ(0, verify->VerifyReadChunkSnapshot(chunk2, sn2, 0, 8 * KB,
                                                    &chunkData2));

        // 删除chunk1文件，预期成功，本地快照存在的情况下，会将快照也一起删除
        ASSERT_EQ(CHUNK_OP_STATUS_SUCCESS,
                        verify->VerifyDeleteChunk(chunk1, sn3));

        /*
        *  场景四：第二次快照结束，删除快照
        */
        // 删除chunk1快照，因为chunk1及其快照上一步已经删除，预期成功
        ASSERT_EQ(CHUNK_OP_STATUS_SUCCESS,
              verify->VerifyDeleteChunkSnapshotOrCorrectSn(chunk1, sn3));
        // 获取chunk1信息，预期不存在
        ASSERT_EQ(0, verify->VerifyGetChunkInfo(
                                    chunk1, NULL_SN, NULL_SN, leader));

        // 删除chunk2快照，预期成功
        ASSERT_EQ(CHUNK_OP_STATUS_SUCCESS,
              verify->VerifyDeleteChunkSnapshotOrCorrectSn(chunk2, sn3));
        // 获取chunk2信息，预期其版本为2，无快照版本
        ASSERT_EQ(0, verify->VerifyGetChunkInfo(chunk2, sn2, NULL_SN, leader));

        // chunk2写[0, 4KB]
        data.assign(length, 'h');
        ASSERT_EQ(0, verify->VerifyWriteChunk(chunk2, sn3, 0, 4 * KB,
                                             data.c_str(), &chunkData2));
        // 获取chunk2信息，预期其版本为3，无快照版本
        ASSERT_EQ(0, verify->VerifyGetChunkInfo(chunk2, sn3, NULL_SN, leader));

        // chunk2写[0, 4KB]
        data.assign(length, 'i');
        ASSERT_EQ(0, verify->VerifyWriteChunk(chunk2, sn3, 0, 4 * KB,
                                              data.c_str(), &chunkData2));
        // 获取chunk2信息，预期其版本为3，无快照版本
        ASSERT_EQ(0, verify->VerifyGetChunkInfo(chunk2, sn3, NULL_SN, leader));

        /*
        *  场景五：用户删除文件
        */
        // 删除chunk1，已不存在，预期成功
        ASSERT_EQ(CHUNK_OP_STATUS_SUCCESS,
                                verify->VerifyDeleteChunk(chunk1, sn3));
        // 获取chunk1信息，预期不存在
        ASSERT_EQ(0, verify->VerifyGetChunkInfo(
                                chunk1, NULL_SN, NULL_SN, leader));
        // 删除chunk2，预期成功
        ASSERT_EQ(CHUNK_OP_STATUS_SUCCESS,
                                verify->VerifyDeleteChunk(chunk2, sn3));
        // 获取chunk2信息，预期不存在
        ASSERT_EQ(0, verify->VerifyGetChunkInfo(
                                chunk2, NULL_SN, NULL_SN, leader));
    }

 public:
    std::vector<Peer> peers_;
    Peer leaderPeer_;
    LogicPoolID logicPoolId_;
    CopysetID copysetId_;

    std::map<int, int> paramsIndexs_;
    std::vector<char **> params_;
    std::string externalIp_;

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

/*
 *
 *
 */
TEST_F(ChunkServerIoTest, BasicIO) {
    struct ChunkServiceOpConf opConf = { &leaderPeer_, logicPoolId_, copysetId_,
                                         2000 };
    auto verify = std::make_shared<ChunkServiceVerify>(&opConf);
    TestBasicIO(verify);
}

TEST_F(ChunkServerIoTest, BasicIO_from_external_ip) {
    Peer exPeer;
    exPeer.set_address(externalIp_ + ":" + BASIC_TEST_CHUNK_SERVER_PORT + ":0");

    struct ChunkServiceOpConf opConf = { &exPeer, logicPoolId_, copysetId_,
                                         2000 };
    auto verify = std::make_shared<ChunkServiceVerify>(&opConf);
    TestBasicIO(verify);
}

TEST_F(ChunkServerIoTest, SnapshotIO) {
    struct ChunkServiceOpConf opConf = { &leaderPeer_, logicPoolId_, copysetId_,
                                         2000 };
    auto verify = std::make_shared<ChunkServiceVerify>(&opConf);
    TestSnapshotIO(verify);
}

TEST_F(ChunkServerIoTest, SnapshotIO_from_external_ip) {
    Peer exPeer;
    exPeer.set_address(externalIp_ + ":" + BASIC_TEST_CHUNK_SERVER_PORT + ":0");
    struct ChunkServiceOpConf opConf = { &exPeer, logicPoolId_, copysetId_,
                                         2000 };
    auto verify = std::make_shared<ChunkServiceVerify>(&opConf);
    TestSnapshotIO(verify);
}

}  // namespace chunkserver
}  // namespace curve
