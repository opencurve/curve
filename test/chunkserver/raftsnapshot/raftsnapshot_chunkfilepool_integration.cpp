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
 * File Created: Thursday, 13th June 2019 4:14:55 pm
 * Author: tongguangxun
 */

#include <brpc/channel.h>
#include <gtest/gtest.h>
#include <butil/at_exit.h>

#include <vector>

#include "test/chunkserver/chunkserver_test_util.h"
#include "src/chunkserver/copyset_node_manager.h"
#include "src/chunkserver/cli.h"
#include "src/fs/fs_common.h"
#include "src/fs/local_filesystem.h"

namespace curve {
namespace chunkserver {

using curve::fs::LocalFileSystem;
using curve::fs::LocalFsFactory;
using curve::fs::FileSystemType;

static constexpr uint32_t kOpRequestAlignSize = 4096;

class RaftSnapFilePoolTest : public testing::Test {
 protected:
    virtual void SetUp() {
        ASSERT_EQ(0, peer1.parse("127.0.0.1:9060:0"));
        ASSERT_EQ(0, peer2.parse("127.0.0.1:9061:0"));
        ASSERT_EQ(0, peer3.parse("127.0.0.1:9062:0"));
        ASSERT_EQ(0, peer4.parse("127.0.0.1:9063:0"));
        Exec(TestCluster::RemoveCopysetDirCmd(peer1).c_str());
        Exec(TestCluster::RemoveCopysetDirCmd(peer2).c_str());
        Exec(TestCluster::RemoveCopysetDirCmd(peer3).c_str());
        Exec(TestCluster::RemoveCopysetDirCmd(peer4).c_str());

        electionTimeoutMs = 3000;
        snapshotIntervalS = 60;
    }
    virtual void TearDown() {
        Exec(TestCluster::RemoveCopysetDirCmd(peer1).c_str());
        Exec(TestCluster::RemoveCopysetDirCmd(peer2).c_str());
        Exec(TestCluster::RemoveCopysetDirCmd(peer3).c_str());
        Exec(TestCluster::RemoveCopysetDirCmd(peer4).c_str());
        ::usleep(100*1000);
    }

 public:
    PeerId peer1;
    PeerId peer2;
    PeerId peer3;
    PeerId peer4;
    int electionTimeoutMs;
    int snapshotIntervalS;
};

/**
 *TODO (wudemiao) will further abstract I/O and verification in the later stage
 */

/**
 *Normal I/O verification, write it in first, then read it out for verification
 * @param leaderId Primary ID
 * @param logicPoolId Logical Pool ID
 * @param copysetId Copy Group ID
 * @param chunkId chunk id
 * @param length The length of each IO
 * @param fillCh Characters filled in each IO
 * @param loopThe number of times repeatedly initiates IO
 */
static void WriteThenReadVerify(PeerId leaderId,
                                LogicPoolID logicPoolId,
                                CopysetID copysetId,
                                ChunkID chunkId,
                                int length,
                                char fillCh,
                                int loop) {
    brpc::Channel* channel = new brpc::Channel;
    uint64_t sn = 1;
    ASSERT_EQ(0, channel->Init(leaderId.addr, NULL));
    for (int i = 0; i < loop; ++i) {
        // write

        brpc::Controller cntl;
        cntl.set_timeout_ms(5000);
        ChunkRequest request;
        ChunkResponse response;
        request.set_optype(CHUNK_OP_TYPE::CHUNK_OP_WRITE);
        request.set_logicpoolid(logicPoolId);
        request.set_copysetid(copysetId);
        request.set_chunkid(chunkId);
        request.set_offset(length*i);
        request.set_size(length);
        request.set_sn(sn);
        cntl.request_attachment().resize(length, fillCh);
        ChunkService_Stub stub(channel);
        stub.WriteChunk(&cntl, &request, &response, nullptr);
        LOG_IF(INFO, cntl.Failed()) << "error msg: "
                                    << cntl.ErrorCode() << " : "
                                    << cntl.ErrorText();
        ASSERT_FALSE(cntl.Failed());
        if (response.status() ==
            CHUNK_OP_STATUS::CHUNK_OP_STATUS_REDIRECTED) {
            std::string redirect = response.redirect();
            leaderId.parse(redirect);
            delete channel;
            channel = new brpc::Channel;
            ASSERT_EQ(0, channel->Init(leaderId.addr, NULL));
            continue;
        }
        ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS,
                    response.status());

        // read
        {
            brpc::Controller cntl;
            cntl.set_timeout_ms(5000);
            ChunkRequest request;
            ChunkResponse response;
            request.set_optype(CHUNK_OP_TYPE::CHUNK_OP_READ);
            request.set_logicpoolid(logicPoolId);
            request.set_copysetid(copysetId);
            request.set_chunkid(chunkId);
            request.set_offset(length*i);
            request.set_size(length);
            request.set_sn(sn);
            stub.ReadChunk(&cntl, &request, &response, nullptr);
            LOG_IF(INFO, cntl.Failed()) << "error msg: "
                                        << cntl.ErrorCode() << " : "
                                        << cntl.ErrorText();
            ASSERT_FALSE(cntl.Failed());
            ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS,
                      response.status());
            std::string expectRead(length, fillCh);
            ASSERT_STREQ(expectRead.c_str(),
                         cntl.response_attachment().to_string().c_str());
        }
    }
}

/**
 *Normal I/O verification, read data verification
 * @param leaderId Primary ID
 * @param logicPoolId Logical Pool ID
 * @param copysetId Copy Group ID
 * @param chunkId chunk id
 * @param length The length of each IO
 * @param fillCh Characters filled in each IO
 * @param loopThe number of times repeatedly initiates IO
 */
static void ReadVerify(PeerId leaderId,
                       LogicPoolID logicPoolId,
                       CopysetID copysetId,
                       ChunkID chunkId,
                       int length,
                       char fillCh,
                       int loop) {
    brpc::Channel channel;
    uint64_t sn = 1;
    ASSERT_EQ(0, channel.Init(leaderId.addr, NULL));
    ChunkService_Stub stub(&channel);
    for (int i = 0; i < loop; ++i) {
        brpc::Controller cntl;
        cntl.set_timeout_ms(5000);
        ChunkRequest request;
        ChunkResponse response;
        request.set_optype(CHUNK_OP_TYPE::CHUNK_OP_READ);
        request.set_logicpoolid(logicPoolId);
        request.set_copysetid(copysetId);
        request.set_chunkid(chunkId);
        request.set_offset(length*i);
        request.set_size(length);
        request.set_sn(sn);
        stub.ReadChunk(&cntl, &request, &response, nullptr);
        LOG_IF(INFO, cntl.Failed()) << "error msg: "
                                    << cntl.ErrorCode() << " : "
                                    << cntl.ErrorText();
        ASSERT_FALSE(cntl.Failed());
        ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS,
                  response.status());
        std::string expectRead(length, fillCh);
        ASSERT_STREQ(expectRead.c_str(),
                     cntl.response_attachment().to_string().c_str());
    }
}

/**
 *Verify the shutdown of non leader nodes on three nodes, restart, and control the recovery from install snapshot
 *1 Create a replication group of 3 replicas
 *2 Wait for the leader to generate, write the data, and then read it out for verification
 *3 Shutdown non leader
 *4 Then sleep exceeds one snapshot interval and write read data
 *5 Then sleep for more than one snapshot interval and write read data; 4,5 two-step
 *  It is to ensure that at least two snapshots are taken, so that when the node restarts again, it must pass the install snapshot,
 *  Because the log has been deleted, the data of the install snapshot is taken from the FilePool
 *6 Wait for the leader to be generated, and then verify the data written before the read
 *7 Transfer leader to shut down peer
 *8 Verification of data written before read
 *9 Write the data again and read it out for verification
 */
TEST_F(RaftSnapFilePoolTest, ShutdownOnePeerRestartFromInstallSnapshot) {
    LogicPoolID logicPoolId = 2;
    CopysetID copysetId = 100001;
    uint64_t chunkId = 1;
    int length = kOpRequestAlignSize;
    char ch = 'a';
    int loop = 25;
    int snapshotTimeoutS = 2;

    std::vector<PeerId> peers;
    peers.push_back(peer1);
    peers.push_back(peer2);
    peers.push_back(peer3);

    TestCluster cluster("ThreeNode-cluster", logicPoolId, copysetId, peers);
    cluster.SetElectionTimeoutMs(electionTimeoutMs);
    cluster.SetsnapshotIntervalS(snapshotTimeoutS);
    ASSERT_EQ(0, cluster.StartPeer(peer1, false, true, true));
    ASSERT_EQ(0, cluster.StartPeer(peer2, false, true, true));
    ASSERT_EQ(0, cluster.StartPeer(peer3, false, true, true));

    //Waiting for FilePool creation to succeed
    std::this_thread::sleep_for(std::chrono::seconds(60));
    PeerId leaderId;
    ASSERT_EQ(0, cluster.WaitLeader(&leaderId));

    //Obtain the pool capacity of FilePool for three chunkservers
    std::shared_ptr<LocalFileSystem> fs(LocalFsFactory::CreateFs(
                                        FileSystemType::EXT4, ""));
    std::vector<std::string> Peer1ChunkPoolSize;
    std::vector<std::string> Peer2ChunkPoolSize;
    std::vector<std::string> Peer3ChunkPoolSize;
    std::string copysetdir1, copysetdir2, copysetdir3;
    butil::string_printf(&copysetdir1,
                         "./%s-%d-%d",
                         butil::ip2str(peer1.addr.ip).c_str(),
                         peer1.addr.port,
                         0);
    butil::string_printf(&copysetdir2,
                         "./%s-%d-%d",
                         butil::ip2str(peer2.addr.ip).c_str(),
                         peer2.addr.port,
                         0);
    butil::string_printf(&copysetdir3,
                         "./%s-%d-%d",
                         butil::ip2str(peer3.addr.ip).c_str(),
                         peer3.addr.port,
                         0);

    fs->List(copysetdir1+"/chunkfilepool", &Peer1ChunkPoolSize);
    fs->List(copysetdir2+"/chunkfilepool", &Peer2ChunkPoolSize);
    fs->List(copysetdir3+"/chunkfilepool", &Peer3ChunkPoolSize);

    //Currently, only chunk files are retrieved from FilePool
    //Raft snapshot meta and conf epoch files are created directly from the file system
    ASSERT_EQ(20, Peer1ChunkPoolSize.size());
    ASSERT_EQ(20, Peer2ChunkPoolSize.size());
    ASSERT_EQ(20, Peer3ChunkPoolSize.size());

    LOG(INFO) << "write 1 start";
    //Initiate read/write, writing data will trigger chunkserver to fetch chunks from FilePool
    WriteThenReadVerify(leaderId,
                        logicPoolId,
                        copysetId,
                        chunkId,
                        length,
                        ch,
                        loop);

    LOG(INFO) << "write 1 end";
    //The operations between replicas within the raft are not all synchronized, and there may be outdated replica operations
    //So take a nap first to prevent concurrent statistics of file information
    ::sleep(1*snapshotTimeoutS);

    Peer1ChunkPoolSize.clear();
    Peer2ChunkPoolSize.clear();
    Peer3ChunkPoolSize.clear();
    fs->List(copysetdir1+"/chunkfilepool", &Peer1ChunkPoolSize);
    fs->List(copysetdir2+"/chunkfilepool", &Peer2ChunkPoolSize);
    fs->List(copysetdir3+"/chunkfilepool", &Peer3ChunkPoolSize);

    //After writing the data, ChunkFilePool has one less capacity
    ASSERT_EQ(19, Peer1ChunkPoolSize.size());
    ASSERT_EQ(19, Peer2ChunkPoolSize.size());
    ASSERT_EQ(19, Peer3ChunkPoolSize.size());

    //Shutdown a non leader peer
    PeerId shutdownPeerid;
    if (0 == ::strcmp(leaderId.to_string().c_str(),
                      peer1.to_string().c_str())) {
        shutdownPeerid = peer2;
    } else {
        shutdownPeerid = peer1;
    }
    LOG(INFO) << "shutdown peer: " << shutdownPeerid.to_string();
    LOG(INFO) << "leader peer: " << leaderId.to_string();
    ASSERT_NE(0, ::strcmp(shutdownPeerid.to_string().c_str(),
                          leaderId.to_string().c_str()));
    ASSERT_EQ(0, cluster.ShutdownPeer(shutdownPeerid));

    //Wait snapshot to ensure that it can trigger a snapshot
    //This time taking a snapshot, raft will take a file from FilePool as the snapshot file
    //Then the last snapshot file will be deleted, and the deleted files will be recycled to FilePool
    //So overall, this write will only cause the datastore to retrieve files from FilePool
    //But I took one snapshot and put it back again
    ::sleep(1.5*snapshotTimeoutS);
    //Initiate read/write again
    LOG(INFO) << "write 2 start";
    WriteThenReadVerify(leaderId,
                        logicPoolId,
                        copysetId,
                        chunkId,
                        length,
                        ch + 1,
                        loop);
    LOG(INFO) << "write 2 end";

    ::sleep(1*snapshotTimeoutS);

    Peer1ChunkPoolSize.clear();
    Peer2ChunkPoolSize.clear();
    Peer3ChunkPoolSize.clear();
    fs->List(copysetdir1+"/chunkfilepool", &Peer1ChunkPoolSize);
    fs->List(copysetdir2+"/chunkfilepool", &Peer2ChunkPoolSize);
    fs->List(copysetdir3+"/chunkfilepool", &Peer3ChunkPoolSize);

    //After writing the data, the FilePool capacity is reduced by one
    ASSERT_EQ(19, Peer1ChunkPoolSize.size());
    ASSERT_EQ(19, Peer2ChunkPoolSize.size());
    ASSERT_EQ(19, Peer3ChunkPoolSize.size());

    //Wait snapshot to ensure that it can trigger a snapshot
    //This time taking a snapshot, raft will take a file from FilePool as the snapshot file
    //Then the last snapshot file will be deleted, and the deleted files will be recycled to FilePool
    //So overall, this write will only cause the datastore to retrieve files from FilePool
    //But I took one snapshot and put it back again
    ::sleep(1.5*snapshotTimeoutS);
    //Initiate read/write again
    LOG(INFO) << "write 3 start";
    //Add a chunk to remove another chunk from the chunkserver side
    WriteThenReadVerify(leaderId,
                        logicPoolId,
                        copysetId,
                        chunkId + 1,
                        length,
                        ch + 2,
                        loop);
    LOG(INFO) << "write 3 end";

    ::sleep(snapshotTimeoutS);
    Peer1ChunkPoolSize.clear();
    Peer2ChunkPoolSize.clear();
    Peer3ChunkPoolSize.clear();
    fs->List(copysetdir1+"/chunkfilepool", &Peer1ChunkPoolSize);
    fs->List(copysetdir2+"/chunkfilepool", &Peer2ChunkPoolSize);
    fs->List(copysetdir3+"/chunkfilepool", &Peer3ChunkPoolSize);

    LOG(INFO) << "chunk pool1 size = " << Peer1ChunkPoolSize.size();
    LOG(INFO) << "chunk pool2 size = " << Peer2ChunkPoolSize.size();
    LOG(INFO) << "chunk pool3 size = " << Peer3ChunkPoolSize.size();

    //After writing the data, the FilePool capacity is reduced by one
    if (shutdownPeerid == peer1) {
        ASSERT_EQ(19, Peer1ChunkPoolSize.size());
        ASSERT_EQ(18, Peer2ChunkPoolSize.size());
    } else {
        ASSERT_EQ(18, Peer1ChunkPoolSize.size());
        ASSERT_EQ(19, Peer2ChunkPoolSize.size());
    }
    ASSERT_EQ(18, Peer3ChunkPoolSize.size());

    //Restart, needs to be restored from install snapshot
    ASSERT_EQ(0, cluster.StartPeer(shutdownPeerid, false, true, false));
    ASSERT_EQ(0, cluster.WaitLeader(&leaderId));

    //Read it out and verify it again
    ReadVerify(leaderId, logicPoolId, copysetId, chunkId + 1,
               length, ch + 2, loop);
    LOG(INFO) << "write 4 start";
    //Initiate read/write again
    WriteThenReadVerify(leaderId,
                        logicPoolId,
                        copysetId,
                        chunkId,
                        length,
                        ch + 3,
                        loop);

    LOG(INFO) << "write 4 end";

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
                                shutdownPeerid,
                                options);
        if (0 == status.error_code()) {
            cluster.WaitLeader(&leaderId);
            if (leaderId == shutdownPeerid) {
                break;
            }
        }
        ::sleep(1);
    }

    ASSERT_EQ(0, ::strcmp(leaderId.to_string().c_str(),
                          shutdownPeerid.to_string().c_str()));

    ::sleep(5*snapshotTimeoutS);
    Peer1ChunkPoolSize.clear();
    Peer2ChunkPoolSize.clear();
    Peer3ChunkPoolSize.clear();
    fs->List(copysetdir1+"/chunkfilepool", &Peer1ChunkPoolSize);
    fs->List(copysetdir2+"/chunkfilepool", &Peer2ChunkPoolSize);
    fs->List(copysetdir3+"/chunkfilepool", &Peer3ChunkPoolSize);

    LOG(INFO) << "chunk pool1 size = " << Peer1ChunkPoolSize.size();
    LOG(INFO) << "chunk pool2 size = " << Peer2ChunkPoolSize.size();
    LOG(INFO) << "chunk pool3 size = " << Peer3ChunkPoolSize.size();

    //The current raftsnapshot file system only accesses chunk files
    //The meta file follows the original logic and is created directly through the file system, so only two chunks are extracted here
    ASSERT_EQ(18, Peer1ChunkPoolSize.size());
    ASSERT_EQ(18, Peer2ChunkPoolSize.size());
    ASSERT_EQ(18, Peer3ChunkPoolSize.size());
}

}  // namespace chunkserver
}  // namespace curve
