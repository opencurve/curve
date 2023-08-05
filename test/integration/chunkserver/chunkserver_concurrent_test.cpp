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
 * Created Date: 19-07-12
 * Author: wudemiao
 */

#include <brpc/channel.h>
#include <gtest/gtest.h>
#include <butil/at_exit.h>

#include <vector>

#include "src/chunkserver/copyset_node_manager.h"
#include "src/chunkserver/cli.h"
#include "src/fs/fs_common.h"
#include "src/fs/local_filesystem.h"
#include "src/common/concurrent/concurrent.h"
#include "test/integration/common/peer_cluster.h"
#include "test/chunkserver/datastore/filepool_helper.h"
#include "test/integration/common/config_generator.h"

namespace curve {
namespace chunkserver {

using curve::fs::LocalFileSystem;
using curve::fs::LocalFsFactory;
using curve::fs::FileSystemType;
using curve::common::Thread;

static const char* kFakeMdsAddr = "127.0.0.1:9329";

constexpr uint32_t kOpRequestAlignSize = 4096;

static const char *chunkConcurrencyParams1[1][16] = {
    {
        "chunkserver",
        "-chunkServerIp=127.0.0.1",
        "-chunkServerPort=9076",
        "-chunkServerStoreUri=local://./9076/",
        "-chunkServerMetaUri=local://./9076/chunkserver.dat",
        "-copySetUri=local://./9076/copysets",
        "-raftSnapshotUri=curve://./9076/copysets",
        "-raftLogUri=curve://./9076/copysets",
        "-recycleUri=local://./9076/recycler",
        "-chunkFilePoolDir=./9076/chunkfilepool/",
        "-chunkFilePoolMetaPath=./9076/chunkfilepool.meta",
        "-walFilePoolDir=./9076/walfilepool/",
        "-walFilePoolMetaPath=./9076/walfilepool.meta",
        "-conf=./9076/chunkserver.conf",
        "-raft_sync_segments=true",
        NULL
    },
};

static const char *chunkConcurrencyParams2[1][16] = {
    {
        "chunkserver",
        "-chunkServerIp=127.0.0.1",
        "-chunkServerPort=9077",
        "-chunkServerStoreUri=local://./9077/",
        "-chunkServerMetaUri=local://./9077/chunkserver.dat",
        "-copySetUri=local://./9077/copysets",
        "-raftSnapshotUri=curve://./9077/copysets",
        "-raftLogUri=curve://./9077/copysets",
        "-recycleUri=local://./9077/recycler",
        "-chunkFilePoolDir=./9077/chunkfilepool/",
        "-chunkFilePoolMetaPath=./9077/chunkfilepool.meta",
        "-walFilePoolDir=./9077/walfilepool/",
        "-walFilePoolMetaPath=./9077/walfilepool.meta",
        "-conf=./9077/chunkserver.conf",
        "-raft_sync_segments=true",
        NULL
    },
};

butil::AtExitManager atExitManager;
const int kChunkNum = 10;
const ChunkSizeType kChunkSize = 16 * 1024 * 1024;
const PageSizeType kPageSize = kOpRequestAlignSize;

// chunk不从FilePool获取的chunkserver并发测试
class ChunkServerConcurrentNotFromFilePoolTest : public testing::Test {
 protected:
    virtual void SetUp() {
        peer1.set_address("127.0.0.1:9076:0");
        leaderPeer.set_address(peer1.address());
        peers.push_back(peer1);
        leaderId.parse(peer1.address());

        std::string mkdir1("mkdir ");
        mkdir1 += std::to_string(PeerCluster::PeerToId(peer1));

        ::system(mkdir1.c_str());

        electionTimeoutMs = 3000;
        snapshotIntervalS = 60;

        ASSERT_TRUE(cg1.Init("9076"));
        cg1.SetKV("copyset.election_timeout_ms", "3000");
        cg1.SetKV("copyset.snapshot_interval_s", "60");
        cg1.SetKV("mds.listen.addr", kFakeMdsAddr);
        cg1.SetKV("global.block_size", "4096");
        cg1.SetKV("global.meta_page_size", "4096");
        ASSERT_TRUE(cg1.Generate());

        logicPoolId = 1;
        copysetId = 1;

        paramsIndexs[PeerCluster::PeerToId(peer1)] = 0;

        params.push_back(const_cast<char**>(chunkConcurrencyParams1[0]));
    }
    virtual void TearDown() {
        std::string rmdir1("rm -fr ");
        rmdir1 += std::to_string(PeerCluster::PeerToId(peer1));

        ::system(rmdir1.c_str());

        // wait for process exit
        ::usleep(100 * 1000);
    }

    void InitCluster(PeerCluster *cluster) {
        PeerId leaderId;
        Peer leaderPeer;
        cluster->SetElectionTimeoutMs(electionTimeoutMs);
        cluster->SetsnapshotIntervalS(snapshotIntervalS);
        ASSERT_EQ(0, cluster->StartPeer(peer1, PeerCluster::PeerToId(peer1)));

        // 等待leader产生
        ASSERT_EQ(0, cluster->WaitLeader(&leaderPeer));
        ASSERT_EQ(0, leaderId.parse(leaderPeer.address()));
        ASSERT_STREQ(peer1.address().c_str(), leaderId.to_string().c_str());
    }

 public:
    Peer peer1;
    CSTConfigGenerator cg1;
    std::vector<Peer> peers;
    PeerId leaderId;
    Peer leaderPeer;
    int electionTimeoutMs;
    int snapshotIntervalS;

    LogicPoolID logicPoolId;
    CopysetID copysetId;

    std::map<int, int> paramsIndexs;
    std::vector<char **> params;
};

// chunk从FilePool获取的chunkserver并发测试
class ChunkServerConcurrentFromFilePoolTest : public testing::Test {
 protected:
    virtual void SetUp() {
        peer1.set_address("127.0.0.1:9077:0");
        leaderPeer.set_address(peer1.address());
        peers.push_back(peer1);
        leaderId.parse(peer1.address());

        std::string mkdir1("mkdir ");
        mkdir1 += std::to_string(PeerCluster::PeerToId(peer1));

        ::system(mkdir1.c_str());

        electionTimeoutMs = 3000;
        snapshotIntervalS = 60;


        ASSERT_TRUE(cg1.Init("9077"));
        cg1.SetKV("copyset.election_timeout_ms", "3000");
        cg1.SetKV("copyset.snapshot_interval_s", "60");
        cg1.SetKV("chunkfilepool.enable_get_chunk_from_pool", "true");
        cg1.SetKV("mds.listen.addr", kFakeMdsAddr);
        ASSERT_TRUE(cg1.Generate());

        logicPoolId = 1;
        copysetId = 1;

        paramsIndexs[PeerCluster::PeerToId(peer1)] = 0;

        params.push_back(const_cast<char**>(chunkConcurrencyParams2[0]));

        // 初始化FilePool，这里会预先分配一些chunk
        lfs = LocalFsFactory::CreateFs(FileSystemType::EXT4, "");
        poolDir = "./"
            + std::to_string(PeerCluster::PeerToId(peer1))
            + "/chunkfilepool/";
        metaDir = "./"
            + std::to_string(PeerCluster::PeerToId(peer1))
            + "/chunkfilepool.meta";

        FilePoolMeta meta(kChunkSize, kPageSize, poolDir);
        FilePoolHelper::PersistEnCodeMetaInfo(lfs, meta, metaDir);

        // There maybe one chunk in cleaning, so you should allocate
        // (kChunkNum + 1) chunks in start if you want to use kChunkNum chunks.
        // This situation will not occur in the production environment
        allocateChunk(lfs, kChunkNum+1, poolDir, kChunkSize);
    }
    virtual void TearDown() {
        std::string rmdir1("rm -fr ");
        rmdir1 += std::to_string(PeerCluster::PeerToId(peer1));

        ::system(rmdir1.c_str());

        // wait for process exit
        ::usleep(100 * 1000);
    }
    void InitCluster(PeerCluster *cluster) {
        PeerId leaderId;
        Peer leaderPeer;
        cluster->SetElectionTimeoutMs(electionTimeoutMs);
        cluster->SetsnapshotIntervalS(snapshotIntervalS);
        ASSERT_EQ(0, cluster->StartPeer(peer1, PeerCluster::PeerToId(peer1)));

        // 等待leader产生
        ASSERT_EQ(0, cluster->WaitLeader(&leaderPeer));
        ASSERT_EQ(0, leaderId.parse(leaderPeer.address()));
        ASSERT_STREQ(peer1.address().c_str(), leaderId.to_string().c_str());
    }

 public:
    Peer peer1;
    CSTConfigGenerator cg1;
    std::vector<Peer> peers;
    PeerId leaderId;
    Peer leaderPeer;
    int  electionTimeoutMs;
    int  snapshotIntervalS;

    LogicPoolID logicPoolId;
    CopysetID   copysetId;

    std::map<int, int>   paramsIndexs;
    std::vector<char **> params;

    std::string poolDir;
    std::string metaDir;
    std::shared_ptr<LocalFileSystem>  lfs;
};

// 写chunk
int WriteChunk(Peer leader,
               LogicPoolID logicPoolId,
               CopysetID copysetId,
               ChunkID chunkId,
               off_t offset,
               size_t len,
               const char *data,
               const int sn = 1) {
    PeerId leaderId(leader.address());
    brpc::Channel channel;
    channel.Init(leaderId.addr, NULL);
    ChunkService_Stub stub(&channel);

    brpc::Controller cntl;
    cntl.set_timeout_ms(2000);
    ChunkRequest request;
    ChunkResponse response;
    request.set_optype(CHUNK_OP_TYPE::CHUNK_OP_WRITE);
    request.set_logicpoolid(logicPoolId);
    request.set_copysetid(copysetId);
    request.set_chunkid(chunkId);
    request.set_sn(sn);
    request.set_offset(offset);
    request.set_size(len);
    cntl.request_attachment().append(data, len);
    stub.WriteChunk(&cntl, &request, &response, nullptr);

    if (cntl.Failed()) {
        LOG(INFO) << "write failed: " << cntl.ErrorText();
        return -1;
    }

    if (response.status() != CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS) {
        LOG(INFO) << "write failed: "
                  << CHUNK_OP_STATUS_Name(response.status());
        return -1;
    }

    return 0;
}

// 随机选择一个chunk的随机offset进行read
void RandReadChunk(Peer leader,
                   LogicPoolID logicPoolId,
                   CopysetID copysetId,
                   ChunkID chunkIdRange,
                   const int loop,
                   const int sn = 1) {
    int ret = 0;
    uint64_t appliedIndex = 1;
    PeerId leaderId(leader.address());
    brpc::Channel channel;
    channel.Init(leaderId.addr, NULL);
    ChunkService_Stub stub(&channel);

    for (int i = 0; i < loop; ++i) {
        // 随机选择一个chunk
        ChunkID chunkId = butil::fast_rand_less_than(chunkIdRange);
        chunkId += 1;

        brpc::Controller cntl;
        cntl.set_timeout_ms(3000);
        ChunkRequest request;
        ChunkResponse response;
        request.set_optype(CHUNK_OP_TYPE::CHUNK_OP_READ);
        request.set_logicpoolid(logicPoolId);
        request.set_copysetid(copysetId);
        request.set_chunkid(chunkId);
        request.set_sn(sn);
        request.set_size(kOpRequestAlignSize);
        request.set_appliedindex(appliedIndex);

        // 随机选择一个offset
        uint64_t pageIndex = butil::fast_rand_less_than(kChunkSize / kPageSize);
        request.set_offset(pageIndex * kPageSize);

        stub.ReadChunk(&cntl, &request, &response, nullptr);

        if (cntl.Failed()) {
            LOG(INFO) << "read failed: " << cntl.ErrorText();
            ret = -1;
        }

        if (response.status() != CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS &&
            response.status() != CHUNK_OP_STATUS::CHUNK_OP_STATUS_CHUNK_NOTEXIST) {  //NOLINT
            LOG(INFO) << "read failed: "
                      << CHUNK_OP_STATUS_Name(response.status());
            ret = -1;
        }

        ASSERT_EQ(0, ret);
    }
}

// 随机选择一个chunk的随机offset进行write
void RandWriteChunk(Peer leader,
                    LogicPoolID logicPoolId,
                    CopysetID copysetId,
                    ChunkID chunkIdRange,
                    const int loop,
                    const int sn = 1) {
    int ret = 0;
    char data[kOpRequestAlignSize] = {'a'};
    int length = kOpRequestAlignSize;

    PeerId leaderId(leader.address());
    brpc::Channel channel;
    channel.Init(leaderId.addr, NULL);
    ChunkService_Stub stub(&channel);

    for (int i = 0; i < loop; ++i) {
        // 随机选择一个chunk
        ChunkID chunkId = butil::fast_rand_less_than(chunkIdRange);
        chunkId += 1;

        brpc::Controller cntl;
        cntl.set_timeout_ms(4000);
        ChunkRequest request;
        ChunkResponse response;
        request.set_optype(CHUNK_OP_TYPE::CHUNK_OP_WRITE);
        request.set_logicpoolid(logicPoolId);
        request.set_copysetid(copysetId);
        request.set_chunkid(chunkId);
        request.set_sn(sn);
        request.set_size(kOpRequestAlignSize);
        cntl.request_attachment().append(data, length);

        // 随机选择一个offset
        uint64_t pageIndex = butil::fast_rand_less_than(kChunkSize / kPageSize);
        request.set_offset(pageIndex * kPageSize);

        stub.WriteChunk(&cntl, &request, &response, nullptr);

        if (cntl.Failed()) {
            LOG(INFO) << "write failed: " << cntl.ErrorText();
            ret = -1;
        }

        if (response.status() != CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS) {
            LOG(INFO) << "write failed: "
                      << CHUNK_OP_STATUS_Name(response.status());
            ret = -1;
        }

        ASSERT_EQ(0, ret);
    }
}

// 随机选择一个chunk删除
void RandDeleteChunk(Peer leader,
                     LogicPoolID logicPoolId,
                     CopysetID copysetId,
                     ChunkID chunkIdRange,
                     const int loop) {
    int ret = 0;

    PeerId leaderId(leader.address());
    brpc::Channel channel;
    channel.Init(leaderId.addr, NULL);
    ChunkService_Stub stub(&channel);

    for (int i = 0; i < loop; ++i) {
        // 随机选择一个chunk
        ChunkID chunkId = butil::fast_rand_less_than(chunkIdRange);
        chunkId += 1;

        brpc::Controller cntl;
        cntl.set_timeout_ms(1500);
        ChunkRequest request;
        ChunkResponse response;
        request.set_optype(CHUNK_OP_TYPE::CHUNK_OP_DELETE);
        request.set_logicpoolid(logicPoolId);
        request.set_copysetid(copysetId);
        request.set_chunkid(chunkId);
        request.set_sn(1);
        stub.DeleteChunk(&cntl, &request, &response, nullptr);

        if (cntl.Failed()) {
            LOG(INFO) << "delete failed: " << cntl.ErrorText();
            ret = -1;
        }

        if (response.status() != CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS) {
            LOG(INFO) << "delete failed: "
                      << CHUNK_OP_STATUS_Name(response.status());
            ret = -1;
        }

        ASSERT_EQ(0, ret);
    }
}

// 创建clone chunk
void CreateCloneChunk(Peer leader,
                      LogicPoolID logicPoolId,
                      CopysetID copysetId,
                      ChunkID start,
                      ChunkID end) {
    int ret = 0;
    SequenceNum sn = 2;
    SequenceNum correctedSn = 1;
    std::string location = "test@s3";

    PeerId leaderId(leader.address());
    brpc::Channel channel;
    channel.Init(leaderId.addr, NULL);
    ChunkService_Stub stub(&channel);
    for (int i = start; i <= end; ++i) {
        brpc::Controller cntl;
        cntl.set_timeout_ms(2000);
        ChunkRequest request;
        ChunkResponse response;
        request.set_optype(CHUNK_OP_TYPE::CHUNK_OP_CREATE_CLONE);
        request.set_logicpoolid(logicPoolId);
        request.set_copysetid(copysetId);
        request.set_chunkid(i);
        request.set_sn(sn);
        request.set_correctedsn(correctedSn);
        request.set_location(location);
        request.set_size(kChunkSize);
        stub.CreateCloneChunk(&cntl, &request, &response, nullptr);

        if (cntl.Failed() && cntl.ErrorCode() != brpc::ERPCTIMEDOUT) {
            LOG(INFO) << "create clone chunk failed: " << cntl.ErrorCode()
                      << " " << cntl.ErrorText();
            ret = -1;
        }

        if (response.status() != CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS) {
            LOG(INFO) << "create clone chunk failed: "
                      << CHUNK_OP_STATUS_Name(response.status());
            ret = -1;
        }

        ASSERT_EQ(0, ret);
    }
}

/**
 * chunk不是事先在FilePool分配好的
 */

// 多线程并发随机读同一个chunk
TEST_F(ChunkServerConcurrentNotFromFilePoolTest, RandReadOneChunk) {
    uint64_t chunkId = 1;
    off_t offset = 0;
    int length = kOpRequestAlignSize;
    std::string data(kOpRequestAlignSize, 'a');
    const int kThreadNum = 10;
    const int kMaxLoop = 200;
    ChunkID chunkIdRange = 1;
    const int sn = 1;

    // 1. 启动一个成员的复制组
    PeerCluster cluster("InitShutdown-cluster",
                        logicPoolId,
                        copysetId,
                        peers,
                        params,
                        paramsIndexs);
    ASSERT_EQ(0, cluster.StartFakeTopoloyService(kFakeMdsAddr));
    InitCluster(&cluster);


    // 2. 对chunk发起一次写，保证chunk已经产生
    ASSERT_EQ(0, WriteChunk(leaderPeer,
                            logicPoolId,
                            copysetId,
                            chunkId,
                            offset,
                            length,
                            data.c_str(),
                            sn));

    // 3. 起多个线程执行随机read chunk
    std::vector<Thread> threads;
    for (int i = 0; i < kThreadNum; ++i) {
        threads.push_back(Thread(RandReadChunk,
                                 leaderPeer,
                                 logicPoolId,
                                 copysetId,
                                 chunkIdRange,
                                 kMaxLoop,
                                 sn));
    }

    for (int j = 0; j < kThreadNum; ++j) {
        threads[j].join();
    }
}

// 多线程并发随机写同一个chunk
TEST_F(ChunkServerConcurrentNotFromFilePoolTest, RandWriteOneChunk) {
    const int kThreadNum = 10;
    const int kMaxLoop = 200;
    ChunkID chunkIdRange = 1;
    const int sn = 1;

    // 1. 启动一个成员的复制组
    PeerCluster cluster("InitShutdown-cluster",
                        logicPoolId,
                        copysetId,
                        peers,
                        params,
                        paramsIndexs);
    ASSERT_EQ(0, cluster.StartFakeTopoloyService(kFakeMdsAddr));
    InitCluster(&cluster);

    // 2. 起多个线程执行随机write chunk
    std::vector<Thread> threads;
    for (int i = 0; i < kThreadNum; ++i) {
        threads.push_back(Thread(RandWriteChunk,
                                 leaderPeer,
                                 logicPoolId,
                                 copysetId,
                                 chunkIdRange,
                                 kMaxLoop,
                                 sn));
    }

    for (int j = 0; j < kThreadNum; ++j) {
        threads[j].join();
    }
}

// 多线程并发写同一个chunk同一个offset
TEST_F(ChunkServerConcurrentNotFromFilePoolTest, WriteOneChunkOnTheSameOffset) {   //NOLINT
    const int kThreadNum = 10;
    std::vector<string> datas;
    ChunkID chunkId = 1;
    off_t offset = 0;
    int length = 2 * kOpRequestAlignSize;
    const int sn = 1;

    // 1. 启动一个成员的复制组
    PeerCluster cluster("InitShutdown-cluster",
                        logicPoolId,
                        copysetId,
                        peers,
                        params,
                        paramsIndexs);
    ASSERT_EQ(0, cluster.StartFakeTopoloyService(kFakeMdsAddr));
    InitCluster(&cluster);

    // 2. 起多个线程执行随机write chunk
    std::vector<Thread> threads;
    for (int i = 0; i < kThreadNum; ++i) {
        std::string data(length, 'a' + i);
        datas.push_back(data);
        threads.push_back(Thread(WriteChunk,
                                 leaderPeer,
                                 logicPoolId,
                                 copysetId,
                                 chunkId,
                                 offset,
                                 length,
                                 datas[i].c_str(),
                                 sn));
    }

    for (int j = 0; j < kThreadNum; ++j) {
        threads[j].join();
    }

    // 3. 将数据read出来验证
    brpc::Channel channel;
    channel.Init(leaderId.addr, NULL);
    ChunkService_Stub stub(&channel);
    brpc::Controller cntl;
    cntl.set_timeout_ms(3000);
    ChunkRequest request;
    ChunkResponse response;
    request.set_optype(CHUNK_OP_TYPE::CHUNK_OP_READ);
    request.set_logicpoolid(logicPoolId);
    request.set_copysetid(copysetId);
    request.set_chunkid(chunkId);
    request.set_sn(1);
    request.set_size(length);
    request.set_appliedindex(1);
    request.set_offset(offset);
    stub.ReadChunk(&cntl, &request, &response, nullptr);

    ASSERT_FALSE(cntl.Failed());
    ASSERT_EQ(response.status(), CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS);

    std::string result = cntl.response_attachment().to_string();
    ASSERT_EQ(length, result.size());

    // 读出来的数据的字符>='a' 且<= 'a' + kThreadNum - 1
    ASSERT_GE(result[0] - 'a', 0);
    ASSERT_LE(result[0] - 'a', kThreadNum - 1);
    for (int i = 1; i < length - 1; ++i) {
        ASSERT_EQ(result[0], result[i]);
    }
}

// 多线程并发随机读写同一个chunk
TEST_F(ChunkServerConcurrentNotFromFilePoolTest, RandReadWriteOneChunk) {
    off_t offset = 0;
    int length = kOpRequestAlignSize;
    std::string data(kOpRequestAlignSize, 'a');
    const int kThreadNum = 10;
    const int kMaxLoop = 200;
    ChunkID chunkIdRange = 1;
    const int sn = 1;

    // 1. 启动一个成员的复制组
    PeerCluster cluster("InitShutdown-cluster",
                        logicPoolId,
                        copysetId,
                        peers,
                        params,
                        paramsIndexs);
    ASSERT_EQ(0, cluster.StartFakeTopoloyService(kFakeMdsAddr));
    InitCluster(&cluster);

    // 2. 对chunk发起一次写，保证chunk已经产生
    for (int k = 1; k < chunkIdRange + 1; ++k) {
        ASSERT_EQ(0, WriteChunk(leaderPeer,
                                logicPoolId,
                                copysetId,
                                k,
                                offset,
                                length,
                                data.c_str(),
                                sn));
    }

    // 3. 起多个线程执行随机read write chunk
    std::vector<Thread> threads;
    for (int i = 0; i < kThreadNum; ++i) {
        int read = butil::fast_rand_less_than(2);
        if (read) {
            // 起read线程
            threads.push_back(Thread(RandReadChunk,
                                     leaderPeer,
                                     logicPoolId,
                                     copysetId,
                                     chunkIdRange,
                                     kMaxLoop,
                                     sn));
        } else {
            // 起write线程
            threads.push_back(Thread(RandWriteChunk,
                                     leaderPeer,
                                     logicPoolId,
                                     copysetId,
                                     chunkIdRange,
                                     kMaxLoop,
                                     sn));
        }
    }

    for (int j = 0; j < kThreadNum; ++j) {
        threads[j].join();
    }
}

// 多线程并发读不同的chunk
TEST_F(ChunkServerConcurrentNotFromFilePoolTest, RandReadMultiChunk) {
    off_t offset = 0;
    int length = kOpRequestAlignSize;
    std::string data(kOpRequestAlignSize, 'a');
    const int kThreadNum = 10;
    const int kMaxLoop = 200;
    ChunkID chunkIdRange = kChunkNum;
    const int sn = 1;

    // 1. 启动一个成员的复制组
    PeerCluster cluster("InitShutdown-cluster",
                        logicPoolId,
                        copysetId,
                        peers,
                        params,
                        paramsIndexs);
    ASSERT_EQ(0, cluster.StartFakeTopoloyService(kFakeMdsAddr));
    InitCluster(&cluster);

    // 2. 对chunk发起一次写，保证chunk已经产生
    for (int k = 1; k < chunkIdRange + 1; ++k) {
        ASSERT_EQ(0, WriteChunk(leaderPeer,
                                logicPoolId,
                                copysetId,
                                k,
                                offset,
                                length,
                                data.c_str(),
                                sn));
    }

    // 3. 起多个线程执行随机read chunk
    std::vector<Thread> threads;
    for (int i = 0; i < kThreadNum; ++i) {
        threads.push_back(Thread(RandReadChunk,
                                 leaderPeer,
                                 logicPoolId,
                                 copysetId,
                                 chunkIdRange,
                                 kMaxLoop,
                                 sn));
    }

    for (int j = 0; j < kThreadNum; ++j) {
        threads[j].join();
    }
}

// 多线程并发读不同的chunk，注意这些chunk都还没有被写过
TEST_F(ChunkServerConcurrentNotFromFilePoolTest, RandReadMultiNotExistChunk) {  //NOLINT
    const int kThreadNum = 10;
    const int kMaxLoop = 200;
    ChunkID chunkIdRange = kChunkNum;
    const int sn = 1;

    // 1. 启动一个成员的复制组
    PeerCluster cluster("InitShutdown-cluster",
                        logicPoolId,
                        copysetId,
                        peers,
                        params,
                        paramsIndexs);
    ASSERT_EQ(0, cluster.StartFakeTopoloyService(kFakeMdsAddr));
    InitCluster(&cluster);

    // 2. 起多个线程执行随机read chunk
    std::vector<Thread> threads;
    for (int i = 0; i < kThreadNum; ++i) {
        threads.push_back(Thread(RandReadChunk,
                                 leaderPeer,
                                 logicPoolId,
                                 copysetId,
                                 chunkIdRange,
                                 kMaxLoop,
                                 sn));
    }

    for (int j = 0; j < kThreadNum; ++j) {
        threads[j].join();
    }
}

// 多线程并发随机写同多个chunk
TEST_F(ChunkServerConcurrentNotFromFilePoolTest, RandWriteMultiChunk) {
    off_t offset = 0;
    int length = kOpRequestAlignSize;
    std::string data(kOpRequestAlignSize, 'a');
    const int kThreadNum = 10;
    const int kMaxLoop = 200;
    ChunkID chunkIdRange = kChunkNum;
    const int sn = 1;

    // 1. 启动一个成员的复制组
    PeerCluster cluster("InitShutdown-cluster",
                        logicPoolId,
                        copysetId,
                        peers,
                        params,
                        paramsIndexs);
    ASSERT_EQ(0, cluster.StartFakeTopoloyService(kFakeMdsAddr));
    InitCluster(&cluster);

    // 2. 对chunk发起一次写，保证chunk已经产生，避免下面同时从
    //    chunkfile pool生成new chunk导致write 超时失败
    for (int k = 1; k < chunkIdRange + 1; ++k) {
        ASSERT_EQ(0, WriteChunk(leaderPeer,
                                logicPoolId,
                                copysetId,
                                k,
                                offset,
                                length,
                                data.c_str(),
                                sn));
    }

    // 4. 起多个线程执行随机write chunk
    std::vector<Thread> threads;
    for (int i = 0; i < kThreadNum; ++i) {
        threads.push_back(Thread(RandWriteChunk,
                                 leaderPeer,
                                 logicPoolId,
                                 copysetId,
                                 chunkIdRange,
                                 kMaxLoop,
                                 sn));
    }

    for (int j = 0; j < kThreadNum; ++j) {
        threads[j].join();
    }
}

// 多线程并发随机读写同多个chunk
TEST_F(ChunkServerConcurrentNotFromFilePoolTest, RandReadWriteMultiChunk) {
    std::string data(kOpRequestAlignSize, 'a');
    const int kThreadNum = 10;
    const int kMaxLoop = 200;
    ChunkID chunkIdRange = kChunkNum;
    const int sn = 1;

    // 1. 启动一个成员的复制组
    PeerCluster cluster("InitShutdown-cluster",
                        logicPoolId,
                        copysetId,
                        peers,
                        params,
                        paramsIndexs);
    ASSERT_EQ(0, cluster.StartFakeTopoloyService(kFakeMdsAddr));
    InitCluster(&cluster);

    // 2. 起多个线程执行随机read write chunk
    std::vector<Thread> threads;
    for (int i = 0; i < kThreadNum; ++i) {
        int read = butil::fast_rand_less_than(2);
        if (read) {
            // 起read线程
            threads.push_back(Thread(RandReadChunk,
                                     leaderPeer,
                                     logicPoolId,
                                     copysetId,
                                     chunkIdRange,
                                     kMaxLoop,
                                     sn));
        } else {
            // 起write线程
            threads.push_back(Thread(RandWriteChunk,
                                     leaderPeer,
                                     logicPoolId,
                                     copysetId,
                                     chunkIdRange,
                                     kMaxLoop,
                                     sn));
        }
    }

    for (int j = 0; j < kThreadNum; ++j) {
        threads[j].join();
    }
}

// 多线程并发删除不同的chunk
TEST_F(ChunkServerConcurrentNotFromFilePoolTest, DeleteMultiChunk) {
    off_t offset = 0;
    int length = kOpRequestAlignSize;
    std::string data(kOpRequestAlignSize, 'a');
    const int kThreadNum = 10;
    const int kMaxLoop = 200;
    ChunkID chunkIdRange = kChunkNum;
    const int sn = 1;

    // 1. 启动一个成员的复制组
    PeerCluster cluster("InitShutdown-cluster",
                        logicPoolId,
                        copysetId,
                        peers,
                        params,
                        paramsIndexs);
    ASSERT_EQ(0, cluster.StartFakeTopoloyService(kFakeMdsAddr));
    InitCluster(&cluster);

    // 2. 对chunk发起一次写，保证chunk已经产生
    for (int k = 1; k < chunkIdRange + 1; ++k) {
        ASSERT_EQ(0, WriteChunk(leaderPeer,
                                logicPoolId,
                                copysetId,
                                k,
                                offset,
                                length,
                                data.c_str(),
                                sn));
    }

    // 3. 起多个线程执行随机delete chunk
    std::vector<Thread> threads;
    for (int i = 0; i < kThreadNum; ++i) {
        // 起delete线程
        threads.push_back(Thread(RandDeleteChunk,
                                 leaderPeer,
                                 logicPoolId,
                                 copysetId,
                                 chunkIdRange,
                                 kMaxLoop));
    }

    for (int j = 0; j < kThreadNum; ++j) {
        threads[j].join();
    }
}

// 多线程并发create clone不同的chunk
TEST_F(ChunkServerConcurrentNotFromFilePoolTest, CreateCloneMultiChunk) {
    const int kThreadNum = 10;
    ChunkID chunkIdRange = kChunkNum;

    // 1. 启动一个成员的复制组
    PeerCluster cluster("InitShutdown-cluster",
                        logicPoolId,
                        copysetId,
                        peers,
                        params,
                        paramsIndexs);
    ASSERT_EQ(0, cluster.StartFakeTopoloyService(kFakeMdsAddr));
    InitCluster(&cluster);

    // 2. 起多个线程执行随机create clone chunk
    std::vector<Thread> threads;
    int chunksPerThread = chunkIdRange / kThreadNum;
    for (int i = 0; i < kThreadNum; ++i) {
        threads.push_back(Thread(CreateCloneChunk,
                                 leaderPeer,
                                 logicPoolId,
                                 copysetId,
                                 i * chunksPerThread + 1,
                                 (i + 1) * chunksPerThread));
    }

    for (int j = 0; j < kThreadNum; ++j) {
        threads[j].join();
    }
}

/**
 * chunk是事先在FilePool分配好的
 */

// 多线程并发随机读同一个chunk
TEST_F(ChunkServerConcurrentFromFilePoolTest, RandReadOneChunk) {
    uint64_t chunkId = 1;
    off_t offset = 0;
    int length = kOpRequestAlignSize;
    std::string data(kOpRequestAlignSize, 'a');
    const int kThreadNum = 10;
    const int kMaxLoop = 200;
    ChunkID chunkIdRange = 1;
    const int sn = 1;

    // 1. 启动一个成员的复制组
    PeerCluster cluster("InitShutdown-cluster",
                        logicPoolId,
                        copysetId,
                        peers,
                        params,
                        paramsIndexs);
    ASSERT_EQ(0, cluster.StartFakeTopoloyService(kFakeMdsAddr));
    InitCluster(&cluster);

    // 2. 对chunk发起一次写，保证chunk已经产生
    ASSERT_EQ(0, WriteChunk(leaderPeer,
                            logicPoolId,
                            copysetId,
                            chunkId,
                            offset,
                            length,
                            data.c_str(),
                            sn));

    // 3. 起多个线程执行随机read chunk
    std::vector<Thread> threads;
    for (int i = 0; i < kThreadNum; ++i) {
        threads.push_back(Thread(RandReadChunk,
                                 leaderPeer,
                                 logicPoolId,
                                 copysetId,
                                 chunkIdRange,
                                 kMaxLoop,
                                 sn));
    }

    for (int j = 0; j < kThreadNum; ++j) {
        threads[j].join();
    }
}

// 多线程并发随机写同一个chunk
TEST_F(ChunkServerConcurrentFromFilePoolTest, RandWriteOneChunk) {
    const int kThreadNum = 10;
    const int kMaxLoop = 200;
    ChunkID chunkIdRange = 1;
    const int sn = 1;

    // 1. 启动一个成员的复制组
    PeerCluster cluster("InitShutdown-cluster",
                        logicPoolId,
                        copysetId,
                        peers,
                        params,
                        paramsIndexs);
    ASSERT_EQ(0, cluster.StartFakeTopoloyService(kFakeMdsAddr));
    InitCluster(&cluster);

    // 2. 起多个线程执行随机write chunk
    std::vector<Thread> threads;
    for (int i = 0; i < kThreadNum; ++i) {
        threads.push_back(Thread(RandWriteChunk,
                                 leaderPeer,
                                 logicPoolId,
                                 copysetId,
                                 chunkIdRange,
                                 kMaxLoop,
                                 sn));
    }

    for (int j = 0; j < kThreadNum; ++j) {
        threads[j].join();
    }
}

// 多线程并发写同一个chunk同一个offset
TEST_F(ChunkServerConcurrentFromFilePoolTest, WriteOneChunkOnTheSameOffset) {   //NOLINT
    const int kThreadNum = 10;
    std::vector<string> datas;
    ChunkID chunkId = 1;
    off_t offset = 0;
    int length = 2 * kOpRequestAlignSize;
    const int sn = 1;

    // 1. 启动一个成员的复制组
    PeerCluster cluster("InitShutdown-cluster",
                        logicPoolId,
                        copysetId,
                        peers,
                        params,
                        paramsIndexs);
    ASSERT_EQ(0, cluster.StartFakeTopoloyService(kFakeMdsAddr));
    InitCluster(&cluster);

    // 2. 起多个线程执行随机write chunk
    std::vector<Thread> threads;
    for (int i = 0; i < kThreadNum; ++i) {
        std::string data(length, 'a' + i);
        datas.push_back(data);
        threads.push_back(Thread(WriteChunk,
                                 leaderPeer,
                                 logicPoolId,
                                 copysetId,
                                 chunkId,
                                 offset,
                                 length,
                                 datas[i].c_str(),
                                 sn));
    }

    for (int j = 0; j < kThreadNum; ++j) {
        threads[j].join();
    }

    // 4. 将数据read出来验证
    brpc::Channel channel;
    channel.Init(leaderId.addr, NULL);
    ChunkService_Stub stub(&channel);
    brpc::Controller cntl;
    cntl.set_timeout_ms(3000);
    ChunkRequest request;
    ChunkResponse response;
    request.set_optype(CHUNK_OP_TYPE::CHUNK_OP_READ);
    request.set_logicpoolid(logicPoolId);
    request.set_copysetid(copysetId);
    request.set_chunkid(chunkId);
    request.set_sn(1);
    request.set_size(length);
    request.set_appliedindex(1);
    request.set_offset(offset);
    stub.ReadChunk(&cntl, &request, &response, nullptr);

    ASSERT_FALSE(cntl.Failed());
    ASSERT_EQ(response.status(), CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS);

    std::string result = cntl.response_attachment().to_string();
    ASSERT_EQ(length, result.size());

    // 读出来的数据的字符>='a' 且<= 'a' + kThreadNum - 1
    ASSERT_GE(result[0] - 'a', 0);
    ASSERT_LE(result[0] - 'a', kThreadNum - 1);
    for (int i = 1; i < length - 1; ++i) {
        ASSERT_EQ(result[0], result[i]);
    }
}

// 多线程并发随机读写同一个chunk
TEST_F(ChunkServerConcurrentFromFilePoolTest, RandReadWriteOneChunk) {
    std::string data(kOpRequestAlignSize, 'a');
    const int kThreadNum = 10;
    const int kMaxLoop = 200;
    ChunkID chunkIdRange = 1;
    const int sn = 1;

    // 1. 启动一个成员的复制组
    PeerCluster cluster("InitShutdown-cluster",
                        logicPoolId,
                        copysetId,
                        peers,
                        params,
                        paramsIndexs);
    ASSERT_EQ(0, cluster.StartFakeTopoloyService(kFakeMdsAddr));
    InitCluster(&cluster);

    // 2. 起多个线程执行随机read write chunk
    std::vector<Thread> threads;
    for (int i = 0; i < kThreadNum; ++i) {
        int read = butil::fast_rand_less_than(2);
        if (read) {
            // 起read线程
            threads.push_back(Thread(RandReadChunk,
                                     leaderPeer,
                                     logicPoolId,
                                     copysetId,
                                     chunkIdRange,
                                     kMaxLoop,
                                     sn));
        } else {
            // 起write线程
            threads.push_back(Thread(RandWriteChunk,
                                     leaderPeer,
                                     logicPoolId,
                                     copysetId,
                                     chunkIdRange,
                                     kMaxLoop,
                                     sn));
        }
    }

    for (int j = 0; j < kThreadNum; ++j) {
        threads[j].join();
    }
}

// 多线程并发读不同的chunk
TEST_F(ChunkServerConcurrentFromFilePoolTest, RandReadMultiChunk) {
    off_t offset = 0;
    int length = kOpRequestAlignSize;
    std::string data(kOpRequestAlignSize, 'a');
    const int kThreadNum = 10;
    const int kMaxLoop = 200;
    ChunkID chunkIdRange = kChunkNum;
    const int sn = 1;

    // 1. 启动一个成员的复制组
    PeerCluster cluster("InitShutdown-cluster",
                        logicPoolId,
                        copysetId,
                        peers,
                        params,
                        paramsIndexs);
    ASSERT_EQ(0, cluster.StartFakeTopoloyService(kFakeMdsAddr));
    InitCluster(&cluster);

    // 2. 对chunk发起一次写，保证chunk已经产生
    for (int k = 1; k < chunkIdRange + 1; ++k) {
        ASSERT_EQ(0, WriteChunk(leaderPeer,
                                logicPoolId,
                                copysetId,
                                k,
                                offset,
                                length,
                                data.c_str(),
                                sn));
    }

    // 4. 起多个线程执行随机read chunk
    std::vector<Thread> threads;
    for (int i = 0; i < kThreadNum; ++i) {
        threads.push_back(Thread(RandReadChunk,
                                 leaderPeer,
                                 logicPoolId,
                                 copysetId,
                                 chunkIdRange,
                                 kMaxLoop,
                                 sn));
    }

    for (int j = 0; j < kThreadNum; ++j) {
        threads[j].join();
    }
}

// 多线程并发读不同的chunk，注意这些chunk都还没有被写过
TEST_F(ChunkServerConcurrentFromFilePoolTest, RandReadMultiNotExistChunk) {
    const int kThreadNum = 10;
    const int kMaxLoop = 200;
    ChunkID chunkIdRange = kChunkNum;
    const int sn = 1;

    // 1. 启动一个成员的复制组
    PeerCluster cluster("InitShutdown-cluster",
                        logicPoolId,
                        copysetId,
                        peers,
                        params,
                        paramsIndexs);
    ASSERT_EQ(0, cluster.StartFakeTopoloyService(kFakeMdsAddr));
    InitCluster(&cluster);

    // 2. 起多个线程执行随机read chunk
    std::vector<Thread> threads;
    for (int i = 0; i < kThreadNum; ++i) {
        threads.push_back(Thread(RandReadChunk,
                                 leaderPeer,
                                 logicPoolId,
                                 copysetId,
                                 chunkIdRange,
                                 kMaxLoop,
                                 sn));
    }

    for (int j = 0; j < kThreadNum; ++j) {
        threads[j].join();
    }
}

// 多线程并发随机写同多个chunk
TEST_F(ChunkServerConcurrentFromFilePoolTest, RandWriteMultiChunk) {
    std::string data(kOpRequestAlignSize, 'a');
    const int kThreadNum = 10;
    const int kMaxLoop = 200;
    ChunkID chunkIdRange = kChunkNum;
    const int sn = 1;

    // 1. 启动一个成员的复制组
    PeerCluster cluster("InitShutdown-cluster",
                        logicPoolId,
                        copysetId,
                        peers,
                        params,
                        paramsIndexs);
    ASSERT_EQ(0, cluster.StartFakeTopoloyService(kFakeMdsAddr));
    InitCluster(&cluster);

    // 2. 起多个线程执行随机write chunk
    std::vector<Thread> threads;
    for (int i = 0; i < kThreadNum; ++i) {
        threads.push_back(Thread(RandWriteChunk,
                                 leaderPeer,
                                 logicPoolId,
                                 copysetId,
                                 chunkIdRange,
                                 kMaxLoop,
                                 sn));
    }

    for (int j = 0; j < kThreadNum; ++j) {
        threads[j].join();
    }
}

// 多线程并发随机读写同多个chunk
TEST_F(ChunkServerConcurrentFromFilePoolTest, RandReadWriteMultiChunk) {
    std::string data(kOpRequestAlignSize, 'a');
    const int kThreadNum = 10;
    const int kMaxLoop = 200;
    ChunkID chunkIdRange = kChunkNum;
    const int sn = 1;

    // 1. 启动一个成员的复制组
    PeerCluster cluster("InitShutdown-cluster",
                        logicPoolId,
                        copysetId,
                        peers,
                        params,
                        paramsIndexs);
    ASSERT_EQ(0, cluster.StartFakeTopoloyService(kFakeMdsAddr));
    InitCluster(&cluster);

    // 2. 起多个线程执行随机read write chunk
    std::vector<Thread> threads;
    for (int i = 0; i < kThreadNum; ++i) {
        int read = butil::fast_rand_less_than(2);
        if (read) {
            // 起read线程
            threads.push_back(Thread(RandReadChunk,
                                     leaderPeer,
                                     logicPoolId,
                                     copysetId,
                                     chunkIdRange,
                                     kMaxLoop,
                                     sn));
        } else {
            // 起write线程
            threads.push_back(Thread(RandWriteChunk,
                                     leaderPeer,
                                     logicPoolId,
                                     copysetId,
                                     chunkIdRange,
                                     kMaxLoop,
                                     sn));
        }
    }

    for (int j = 0; j < kThreadNum; ++j) {
        threads[j].join();
    }
}

// 多线程并发删除不同的chunk
TEST_F(ChunkServerConcurrentFromFilePoolTest, DeleteMultiChunk) {
    off_t offset = 0;
    int length = kOpRequestAlignSize;
    std::string data(kOpRequestAlignSize, 'a');
    const int kThreadNum = 10;
    const int kMaxLoop = 200;
    ChunkID chunkIdRange = kChunkNum;
    const int sn = 1;

    // 1. 启动一个成员的复制组
    PeerCluster cluster("InitShutdown-cluster",
                        logicPoolId,
                        copysetId,
                        peers,
                        params,
                        paramsIndexs);
    ASSERT_EQ(0, cluster.StartFakeTopoloyService(kFakeMdsAddr));
    InitCluster(&cluster);

    // 2. 对chunk发起一次写，保证chunk已经产生
    for (int k = 1; k < chunkIdRange + 1; ++k) {
        ASSERT_EQ(0, WriteChunk(leaderPeer,
                                logicPoolId,
                                copysetId,
                                k,
                                offset,
                                length,
                                data.c_str(),
                                sn));
    }

    // 3. 起多个线程执行随机delete chunk
    std::vector<Thread> threads;
    for (int i = 0; i < kThreadNum; ++i) {
        // 起delete线程
        threads.push_back(Thread(RandDeleteChunk,
                                 leaderPeer,
                                 logicPoolId,
                                 copysetId,
                                 chunkIdRange,
                                 kMaxLoop));
    }

    for (int j = 0; j < kThreadNum; ++j) {
        threads[j].join();
    }
}

// 多线程并发create clone不同的chunk
TEST_F(ChunkServerConcurrentFromFilePoolTest, CreateCloneMultiChunk) {
    const int kThreadNum = 10;
    ChunkID chunkIdRange = kChunkNum;

    // 1. 启动一个成员的复制组
    PeerCluster cluster("InitShutdown-cluster",
                        logicPoolId,
                        copysetId,
                        peers,
                        params,
                        paramsIndexs);
    ASSERT_EQ(0, cluster.StartFakeTopoloyService(kFakeMdsAddr));
    InitCluster(&cluster);

    // 2. 起多个线程执行随机create clone chunk
    std::vector<Thread> threads;
    int chunksPerThread = chunkIdRange / kThreadNum;
    for (int i = 0; i < kThreadNum; ++i) {
        threads.push_back(Thread(CreateCloneChunk,
                                 leaderPeer,
                                 logicPoolId,
                                 copysetId,
                                 i * chunksPerThread + 1,
                                 (i + 1) * chunksPerThread));
    }

    for (int j = 0; j < kThreadNum; ++j) {
        threads[j].join();
    }
}

// 多线程并发随机读写同多个chunk，同事伴随这并发的COW
TEST_F(ChunkServerConcurrentFromFilePoolTest, RandWriteMultiChunkWithCOW) {
    off_t offset = 0;
    int length = kOpRequestAlignSize;
    std::string data(kOpRequestAlignSize, 'a');
    const int kThreadNum = 10;
    const int kMaxLoop = 200;
    ChunkID chunkIdRange = kChunkNum / 2;
    int sn = 1;

    // 1. 启动一个成员的复制组
    PeerCluster cluster("InitShutdown-cluster",
                        logicPoolId,
                        copysetId,
                        peers,
                        params,
                        paramsIndexs);
    ASSERT_EQ(0, cluster.StartFakeTopoloyService(kFakeMdsAddr));
    InitCluster(&cluster);

    // 2. 用低版本的sn写一遍chunk
    for (int k = 1; k <= chunkIdRange; ++k) {
        ASSERT_EQ(0, WriteChunk(leaderPeer,
                                logicPoolId,
                                copysetId,
                                k,
                                offset,
                                length,
                                data.c_str(),
                                sn));
    }
    // sn加1，保证后面的write会产生COW
    sn += 1;

    // 3. 起多个线程执行随机read write chunk
    std::vector<Thread> threads;
    for (int i = 0; i < kThreadNum; ++i) {
        int read = butil::fast_rand_less_than(10);
        if (read <= 1) {
            // 起read线程，20%概率
            threads.push_back(Thread(RandReadChunk,
                                     leaderPeer,
                                     logicPoolId,
                                     copysetId,
                                     chunkIdRange,
                                     kMaxLoop,
                                     sn));
        } else {
            // 起write线程
            threads.push_back(Thread(RandWriteChunk,
                                     leaderPeer,
                                     logicPoolId,
                                     copysetId,
                                     chunkIdRange,
                                     kMaxLoop,
                                     sn));
        }
    }

    for (int j = 0; j < kThreadNum; ++j) {
        threads[j].join();
    }
}

}  // namespace chunkserver
}  // namespace curve
