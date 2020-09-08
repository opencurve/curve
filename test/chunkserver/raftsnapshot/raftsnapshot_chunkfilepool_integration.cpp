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
 * TODO(wudemiao) 后期将发 I/O 和验证再抽象一下
 */

/**
 * 正常 I/O 验证，先写进去，再读出来验证
 * @param leaderId      主的 id
 * @param logicPoolId   逻辑池 id
 * @param copysetId 复制组 id
 * @param chunkId   chunk id
 * @param length    每次 IO 的 length
 * @param fillCh    每次 IO 填充的字符
 * @param loop      重复发起 IO 的次数
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
 * 正常 I/O 验证，read 数据验证
 * @param leaderId      主的 id
 * @param logicPoolId   逻辑池 id
 * @param copysetId 复制组 id
 * @param chunkId   chunk id
 * @param length    每次 IO 的 length
 * @param fillCh    每次 IO 填充的字符
 * @param loop      重复发起 IO 的次数
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
 * 验证3个节点的关闭非 leader 节点，重启，控制让其从 install snapshot 恢复
 * 1. 创建3个副本的复制组
 * 2. 等待 leader 产生，write 数据，然后 read 出来验证一遍
 * 3. shutdown 非 leader
 * 4. 然后 sleep 超过一个 snapshot interval，write read 数据
 * 5. 然后再 sleep 超过一个 snapshot interval，write read 数据；4,5两步
 *    是为了保证打至少两次快照，这样，节点再重启的时候必须通过 install snapshot,
 *    因为 log 已经被删除了, install snapshot的数据从FilePool中取文件
 * 6. 等待 leader 产生，然后 read 之前写入的数据验证一遍
 * 7. transfer leader 到shut down 的peer 上
 * 8. 在 read 之前写入的数据验证
 * 9. 再 write 数据，再 read 出来验证一遍
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

    // 等待FilePool创建成功
    std::this_thread::sleep_for(std::chrono::seconds(60));
    PeerId leaderId;
    ASSERT_EQ(0, cluster.WaitLeader(&leaderId));

    // 获取三个chunkserver的FilePool的pool容量
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

    // 目前只有chunk文件才会从FilePool中取
    // raft snapshot meta 和 conf epoch文件直接从文件系统创建
    ASSERT_EQ(20, Peer1ChunkPoolSize.size());
    ASSERT_EQ(20, Peer2ChunkPoolSize.size());
    ASSERT_EQ(20, Peer3ChunkPoolSize.size());

    LOG(INFO) << "write 1 start";
    // 发起 read/write， 写数据会触发chunkserver从FilePool取chunk
    WriteThenReadVerify(leaderId,
                        logicPoolId,
                        copysetId,
                        chunkId,
                        length,
                        ch,
                        loop);

    LOG(INFO) << "write 1 end";
    // raft内副本之间的操作并不是全部同步的，可能存在落后的副本操作
    // 所以先睡一会，防止并发统计文件信息
    ::sleep(1*snapshotTimeoutS);

    Peer1ChunkPoolSize.clear();
    Peer2ChunkPoolSize.clear();
    Peer3ChunkPoolSize.clear();
    fs->List(copysetdir1+"/chunkfilepool", &Peer1ChunkPoolSize);
    fs->List(copysetdir2+"/chunkfilepool", &Peer2ChunkPoolSize);
    fs->List(copysetdir3+"/chunkfilepool", &Peer3ChunkPoolSize);

    // 写完数据后，ChunkFilePool容量少一个
    ASSERT_EQ(19, Peer1ChunkPoolSize.size());
    ASSERT_EQ(19, Peer2ChunkPoolSize.size());
    ASSERT_EQ(19, Peer3ChunkPoolSize.size());

    // shutdown 某个非 leader 的 peer
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

    // wait snapshot, 保证能够触发打快照
    // 本次打快照，raft会从FilePool取一个文件作为快照文件
    // 然后会把上一次的快照文件删除，删除过的文件会被回收到FilePool
    // 所以总体上本次写入只会导致datastore从FilePool取文件
    // 但是快照取了一个又放回去了一个
    ::sleep(1.5*snapshotTimeoutS);
    // 再次发起 read/write
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

    // 写完数据后，FilePool容量少一个
    ASSERT_EQ(19, Peer1ChunkPoolSize.size());
    ASSERT_EQ(19, Peer2ChunkPoolSize.size());
    ASSERT_EQ(19, Peer3ChunkPoolSize.size());

    // wait snapshot, 保证能够触发打快照
    // 本次打快照，raft会从FilePool取一个文件作为快照文件
    // 然后会把上一次的快照文件删除，删除过的文件会被回收到FilePool
    // 所以总体上本次写入只会导致datastore从FilePool取文件
    // 但是快照取了一个又放回去了一个
    ::sleep(1.5*snapshotTimeoutS);
    // 再次发起 read/write
    LOG(INFO) << "write 3 start";
    // 增加chunkid，使chunkserver端的chunk又被取走一个
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

    // 写完数据后，FilePool容量少一个
    if (shutdownPeerid == peer1) {
        ASSERT_EQ(19, Peer1ChunkPoolSize.size());
        ASSERT_EQ(18, Peer2ChunkPoolSize.size());
    } else {
        ASSERT_EQ(18, Peer1ChunkPoolSize.size());
        ASSERT_EQ(19, Peer2ChunkPoolSize.size());
    }
    ASSERT_EQ(18, Peer3ChunkPoolSize.size());

    // restart, 需要从 install snapshot 恢复
    ASSERT_EQ(0, cluster.StartPeer(shutdownPeerid, false, true, false));
    ASSERT_EQ(0, cluster.WaitLeader(&leaderId));

    // 读出来验证一遍
    ReadVerify(leaderId, logicPoolId, copysetId, chunkId + 1,
               length, ch + 2, loop);
    LOG(INFO) << "write 4 start";
    // 再次发起 read/write
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

    // 当前的raftsnapshot filesystem只存取chunk文件
    // meta文件遵守原有逻辑，直接通过文件系统创建，所以这里只有两个chunk被取出
    ASSERT_EQ(18, Peer1ChunkPoolSize.size());
    ASSERT_EQ(18, Peer2ChunkPoolSize.size());
    ASSERT_EQ(18, Peer3ChunkPoolSize.size());
}

}  // namespace chunkserver
}  // namespace curve
