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
static constexpr uint32_t kOpRequestAlignSize = 4096;

#define BASIC_TEST_CHUNK_SERVER_PORT "9078"
#define KB 1024

const char* kFakeMdsAddr = "127.0.0.1:9079";

static const char *chunkServerParams[1][16] = {
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
        cg1_.SetKV("mds.listen.addr", kFakeMdsAddr);
        cg1_.SetKV("global.meta_page_size", "4096");
        cg1_.SetKV("global.block_size", "4096");
        ASSERT_TRUE(cg1_.Generate());

        paramsIndexs_[PeerCluster::PeerToId(peer1_)] = 0;
        params_.push_back(const_cast<char**>(chunkServerParams[0]));

        //Initialize chunkfilepool, where some chunks will be pre allocated
        lfs_ = LocalFsFactory::CreateFs(FileSystemType::EXT4, "");
        poolDir_ = "./" + std::to_string(PeerCluster::PeerToId(peer1_)) +
                   "/chunkfilepool/";
        metaDir_ = "./" + std::to_string(PeerCluster::PeerToId(peer1_)) +
                   "/chunkfilepool.meta";

        FilePoolMeta meta(kChunkSize, kPageSize, poolDir_);
        FilePoolHelper::PersistEnCodeMetaInfo(lfs_, meta, metaDir_);
        allocateChunk(lfs_, kChunkNum, poolDir_, kChunkSize);
    }

    virtual void TearDown() {
        std::string rmdir1("rm -fr ");
        rmdir1 += std::to_string(PeerCluster::PeerToId(peer1_));

        ::system(rmdir1.c_str());

        //Waiting for the process to end
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

        //Waiting for the leader to be generated
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
        int length = kOpRequestAlignSize;
        const SequenceNum sn1 = 1;
        std::string data(length * 4, 0);
        // Now we will zeroing chunk file, even though it fill '0' in start
        std::string chunkData(kChunkSize, '\0');

        std::string leader = "";
        PeerCluster cluster("InitShutdown-cluster", logicPoolId_, copysetId_,
                        peers_, params_, paramsIndexs_);
        ASSERT_EQ(0, cluster.StartFakeTopoloyService(kFakeMdsAddr));
        ASSERT_EQ(0, InitCluster(&cluster));

        /*Scenario 1: Newly created file, Chunk file does not exist*/
        ASSERT_EQ(0, verify->VerifyReadChunk(chunkId, sn1, 0, length, nullptr));
        ASSERT_EQ(0, verify->VerifyGetChunkInfo(
                                chunkId, NULL_SN, NULL_SN, leader));
        ASSERT_EQ(0, verify->VerifyDeleteChunk(chunkId, sn1));

        /*Scenario 2: After generating a chunk file through WriteChunk, perform the operation*/
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

        /*Scenario 3: User deletes files*/
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
        int length = kOpRequestAlignSize;
        std::string data(length * 4, 0);
        std::string chunkData1a(kChunkSize, 0);  //Chunk1 version 1 expected data
        std::string chunkData1b(kChunkSize, 0);  //Chunk1 version 2 expected data
        std::string chunkData1c(kChunkSize, 0);  //Chunk1 version 3 expected data
        std::string chunkData2(kChunkSize, 0);   //Chunk2 expected data
        std::string leader = "";
        PeerCluster cluster("InitShutdown-cluster", logicPoolId_, copysetId_,
                        peers_, params_, paramsIndexs_);
        ASSERT_EQ(0, cluster.StartFakeTopoloyService(kFakeMdsAddr));
        ASSERT_EQ(0, InitCluster(&cluster));

        //Construct initial environment
        //Writing chunk1 generates chunk1, which is version 1 and does not exist at the beginning of chunk2.
        data.assign(length, 'a');
        ASSERT_EQ(0, verify->VerifyWriteChunk(chunk1, sn1, 0, 12 * KB,
                                             data.c_str(), &chunkData1a));

        /*
        *Scenario 1: Taking a snapshot of a file for the first time
        */
        chunkData1b.assign(chunkData1a);  //Simulate COW on chunk1 data
        data.assign(length, 'b');
        ASSERT_EQ(0, verify->VerifyWriteChunk(chunk1, sn2, 4 * KB, 4 * KB,
                                             data.c_str(), &chunkData1b));
        //Write repeatedly to the same area to verify that there will be no duplicate rows
        data.assign(length, 'c');
        ASSERT_EQ(0, verify->VerifyWriteChunk(chunk1, sn2, 4 * KB, 4 * KB,
                                             data.c_str(), &chunkData1b));

        //Reading chunk1 snapshot, expected to read version 1 data
        ASSERT_EQ(0, verify->VerifyReadChunkSnapshot(chunk1, sn1, 0, 12 * KB,
                                                    &chunkData1a));

        //Chunk1 write [0, 4KB]
        data.assign(length, 'd');
        ASSERT_EQ(0, verify->VerifyWriteChunk(chunk1, sn2, 0, 4 * KB,
                                             data.c_str(), &chunkData1b));
        //Chunk1 write [4KB, 16KB]
        data.assign(length, 'e');
        ASSERT_EQ(0, verify->VerifyWriteChunk(chunk1, sn2, 4 * KB, 12 * KB,
                                             data.c_str(), &chunkData1b));

        //Obtain chunk1 information, with expected version 2 and snapshot version 1,
        ASSERT_EQ(0, verify->VerifyGetChunkInfo(chunk1, sn2, sn1, leader));

        //Chunk1 read [0, 12KB], expected to read version 2 data
        ASSERT_EQ(0,
              verify->VerifyReadChunk(chunk1, sn2, 0, 12 * KB, &chunkData1b));

        //Reading snapshot of chunk1, expected to read version 1 data
        ASSERT_EQ(0, verify->VerifyReadChunkSnapshot(chunk1, sn1, 0, 12 * KB,
                                                    &chunkData1a));

        //Reading snapshot of chunk2, expected chunk not to exist
        ASSERT_EQ(0, verify->VerifyReadChunkSnapshot(
                                chunk2, sn1, 0, 12 * KB, nullptr));

        /*
         *Scenario 2: The first snapshot ends and the snapshot is deleted
        */
        //Delete chunk1 snapshot
        ASSERT_EQ(CHUNK_OP_STATUS_SUCCESS,
              verify->VerifyDeleteChunkSnapshotOrCorrectSn(chunk1, sn2));
        //Obtain chunk1 information, expect its version to be 2, no snapshot version
        ASSERT_EQ(0, verify->VerifyGetChunkInfo(chunk1, sn2, NULL_SN, leader));

        //Delete chunk2 snapshot, expected success
        ASSERT_EQ(CHUNK_OP_STATUS_SUCCESS,
              verify->VerifyDeleteChunkSnapshotOrCorrectSn(chunk2, sn2));

        //Chunk2 write [0, 8KB]
        data.assign(length, 'f');
        ASSERT_EQ(0, verify->VerifyWriteChunk(chunk2, sn2, 0, 8 * KB,
                                             data.c_str(), &chunkData2));
        //Obtain chunk2 information, expect its version to be 2, no snapshot version
        ASSERT_EQ(0, verify->VerifyGetChunkInfo(chunk2, sn2, NULL_SN, leader));

        /*
        *Scenario 3: Taking a second snapshot
        */
        //Chunk1 write [0, 8KB]
        chunkData1c.assign(chunkData1b);  //Simulate COW on chunk1 data
        data.assign(length, 'g');
        ASSERT_EQ(0, verify->VerifyWriteChunk(chunk1, sn3, 0, 8 * KB,
                                             data.c_str(), &chunkData1c));
        //Obtain chunk1 information, expect its version to be 3 and snapshot version to be 2
        ASSERT_EQ(0, verify->VerifyGetChunkInfo(chunk1, sn3, sn2, leader));

        //Reading snapshot of chunk1, expected to read version 2 data
        ASSERT_EQ(0, verify->VerifyReadChunkSnapshot(chunk1, sn2, 0, 12 * KB,
                                                    &chunkData1b));

        //Reading snapshot of chunk2, expected to read version 2 data
        ASSERT_EQ(0, verify->VerifyReadChunkSnapshot(chunk2, sn2, 0, 8 * KB,
                                                    &chunkData2));

        //Delete chunk1 file, expected success. If the local snapshot exists, the snapshot will also be deleted together
        ASSERT_EQ(CHUNK_OP_STATUS_SUCCESS,
                        verify->VerifyDeleteChunk(chunk1, sn3));

        /*
        *Scenario 4: The second snapshot ends and the snapshot is deleted
        */
        //Delete chunk1 snapshot because chunk1 and its snapshot have been deleted in the previous step and are expected to succeed
        ASSERT_EQ(CHUNK_OP_STATUS_SUCCESS,
              verify->VerifyDeleteChunkSnapshotOrCorrectSn(chunk1, sn3));
        //Obtaining chunk1 information, expected not to exist
        ASSERT_EQ(0, verify->VerifyGetChunkInfo(
                                    chunk1, NULL_SN, NULL_SN, leader));

        //Delete chunk2 snapshot, expected success
        ASSERT_EQ(CHUNK_OP_STATUS_SUCCESS,
              verify->VerifyDeleteChunkSnapshotOrCorrectSn(chunk2, sn3));
        //Obtain chunk2 information, expect its version to be 2, no snapshot version
        ASSERT_EQ(0, verify->VerifyGetChunkInfo(chunk2, sn2, NULL_SN, leader));

        //Chunk2 write [0, 4KB]
        data.assign(length, 'h');
        ASSERT_EQ(0, verify->VerifyWriteChunk(chunk2, sn3, 0, 4 * KB,
                                             data.c_str(), &chunkData2));
        //Obtain chunk2 information, expect its version to be 3, no snapshot version
        ASSERT_EQ(0, verify->VerifyGetChunkInfo(chunk2, sn3, NULL_SN, leader));

        //Chunk2 write [0, 4KB]
        data.assign(length, 'i');
        ASSERT_EQ(0, verify->VerifyWriteChunk(chunk2, sn3, 0, 4 * KB,
                                              data.c_str(), &chunkData2));
        //Obtain chunk2 information, expect its version to be 3, no snapshot version
        ASSERT_EQ(0, verify->VerifyGetChunkInfo(chunk2, sn3, NULL_SN, leader));

        /*
        *Scenario 5: User deletes files
        */
        //Delete chunk1, it no longer exists, expected success
        ASSERT_EQ(CHUNK_OP_STATUS_SUCCESS,
                                verify->VerifyDeleteChunk(chunk1, sn3));
        //Obtaining chunk1 information, expected not to exist
        ASSERT_EQ(0, verify->VerifyGetChunkInfo(
                                chunk1, NULL_SN, NULL_SN, leader));
        //Delete chunk2, expected success
        ASSERT_EQ(CHUNK_OP_STATUS_SUCCESS,
                                verify->VerifyDeleteChunk(chunk2, sn3));
        //Obtaining chunk2 information, expected not to exist
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
