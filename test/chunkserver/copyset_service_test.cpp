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
 * Created Date: 18-8-27
 * Author: wudemiao
 */

#include <gtest/gtest.h>
#include <gflags/gflags.h>
#include <bthread/bthread.h>
#include <brpc/channel.h>
#include <brpc/controller.h>
#include <brpc/server.h>

#include <cstdint>

#include "src/chunkserver/trash.h"
#include "src/chunkserver/copyset_node.h"
#include "src/chunkserver/copyset_node_manager.h"
#include "src/chunkserver/cli.h"
#include "proto/copyset.pb.h"
#include "proto/chunk.pb.h"

namespace curve {
namespace chunkserver {

using curve::fs::FileSystemType;

static std::string Exec(const char *cmd) {
    FILE *pipe = popen(cmd, "r");
    if (!pipe) return "ERROR";
    char buffer[4096];
    std::string result = "";
    while (!feof(pipe)) {
        if (fgets(buffer, 1024, pipe) != NULL)
            result += buffer;
    }
    pclose(pipe);
    return result;
}

class CopysetServiceTest : public testing::Test {
 public:
    void SetUp() {
        testDir = "CopysetServiceTestData";
        rmCmd = "rm -rf CopysetServiceTestData trash";
        copysetDir = "local://./CopysetServiceTestData";
        copysetDirPattern = "local://./CopysetServiceTestData/%d";
        Exec(rmCmd.c_str());

        // prepare trash
        TrashOptions opt;
        opt.trashPath = "local://./trash";
        opt.localFileSystem =
            LocalFsFactory::CreateFs(FileSystemType::EXT4, "");
        trash_ = std::make_shared<Trash>();
        trash_->Init(opt);
    }

    void TearDown() {
        Exec(rmCmd.c_str());
    }

 protected:
    std::string testDir;
    std::string rmCmd;
    std::string copysetDir;
    std::string copysetDirPattern;
    std::shared_ptr<Trash> trash_;
};

butil::AtExitManager atExitManager;

TEST_F(CopysetServiceTest, basic) {
    CopysetNodeManager *copysetNodeManager = &CopysetNodeManager::GetInstance();
    LogicPoolID logicPoolId = 1;
    CopysetID copysetId = 100002;
    std::string ip = "127.0.0.1";
    uint32_t port = 9040;

    brpc::Server server;
    butil::EndPoint addr(butil::IP_ANY, port);

    ASSERT_EQ(0, copysetNodeManager->AddService(&server, addr));
    ASSERT_EQ(0, server.Start(port, NULL));

    std::shared_ptr<LocalFileSystem> fs(LocalFsFactory::CreateFs(FileSystemType::EXT4, ""));    //NOLINT
    ASSERT_TRUE(nullptr != fs);

    butil::string_printf(&copysetDir, copysetDirPattern.c_str(), port);
    CopysetNodeOptions copysetNodeOptions;
    copysetNodeOptions.ip = ip;
    copysetNodeOptions.port = port;
    copysetNodeOptions.snapshotIntervalS = 30;
    copysetNodeOptions.catchupMargin = 50;
    copysetNodeOptions.chunkDataUri = copysetDir;
    copysetNodeOptions.chunkSnapshotUri = copysetDir;
    copysetNodeOptions.logUri = copysetDir;
    copysetNodeOptions.raftMetaUri = copysetDir;
    copysetNodeOptions.raftSnapshotUri = copysetDir;
    copysetNodeOptions.concurrentapply = new ConcurrentApplyModule();
    copysetNodeOptions.localFileSystem = fs;
    copysetNodeOptions.chunkFilePool =
        std::make_shared<FilePool>(fs);
    copysetNodeOptions.trash = trash_;
    copysetNodeOptions.enableOdsyncWhenOpenChunkFile = true;
    ASSERT_EQ(0, copysetNodeManager->Init(copysetNodeOptions));
    ASSERT_EQ(0, copysetNodeManager->Run());

    brpc::Channel channel;
    PeerId peerId("127.0.0.1:9040:0");
    if (channel.Init(peerId.addr, NULL) != 0) {
        LOG(FATAL) << "Fail to init channel to " << peerId.addr;
    }

    /* 测试创建一个新的 copyset */
    CopysetService_Stub stub(&channel);
    {
        brpc::Controller cntl;
        cntl.set_timeout_ms(3000);

        CopysetRequest request;
        CopysetResponse response;
        request.set_logicpoolid(logicPoolId);
        request.set_copysetid(copysetId);
        request.add_peerid("127.0.0.1:9040:0");
        request.add_peerid("127.0.0.1:9041:0");
        request.add_peerid("127.0.0.1:9042:0");
        stub.CreateCopysetNode(&cntl, &request, &response, nullptr);
        if (cntl.Failed()) {
            std::cout << cntl.ErrorText() << std::endl;
        }
        ASSERT_EQ(response.status(),
                  COPYSET_OP_STATUS::COPYSET_OP_STATUS_SUCCESS);
    }

    /* 测试创建一个重复 copyset */
    {
        brpc::Controller cntl;
        cntl.set_timeout_ms(3000);

        CopysetRequest request;
        CopysetResponse response;
        request.set_logicpoolid(logicPoolId);
        request.set_copysetid(copysetId);
        request.add_peerid("127.0.0.1:9040:0");
        request.add_peerid("127.0.0.1:9041:0");
        request.add_peerid("127.0.0.1:9042:0");
        stub.CreateCopysetNode(&cntl, &request, &response, nullptr);
        if (cntl.Failed()) {
            std::cout << cntl.ErrorText() << std::endl;
        }
        ASSERT_EQ(COPYSET_OP_STATUS::COPYSET_OP_STATUS_EXIST,
                  response.status());
    }

    /* 非法参数测试 */
    {
        brpc::Controller cntl;
        cntl.set_timeout_ms(3000);

        CopysetRequest request;
        CopysetResponse response;
        request.set_logicpoolid(logicPoolId + 1);
        request.set_copysetid(copysetId + 1);
        request.add_peerid("127.0.0.1");
        request.add_peerid("127.0.0.1:9041:0");
        request.add_peerid("127.0.0.1:9042:0");
        stub.CreateCopysetNode(&cntl, &request, &response, nullptr);
        if (cntl.Failed()) {
            std::cout << cntl.ErrorText() << std::endl;
        }
        ASSERT_EQ(cntl.ErrorCode(), EINVAL);
    }

    // TEST CASES: remove copyset node
    {
        brpc::Controller cntl;
        CopysetRequest request;
        CopysetResponse response;
        CopysetStatusRequest statusReq;
        CopysetStatusResponse statusResp;
        cntl.set_timeout_ms(3000);

        // CASE 1: copyset is healthy
        request.set_logicpoolid(logicPoolId);
        request.set_copysetid(copysetId);
        stub.DeleteBrokenCopyset(&cntl, &request, &response, nullptr);
        ASSERT_FALSE(cntl.Failed());
        ASSERT_EQ(response.status(), COPYSET_OP_STATUS_COPYSET_IS_HEALTHY);

        // CASE 2: copyset is not exist -> delete failed
        cntl.Reset();
        request.set_logicpoolid(logicPoolId);
        request.set_copysetid(copysetId + 1);
        stub.DeleteBrokenCopyset(&cntl, &request, &response, nullptr);
        ASSERT_FALSE(cntl.Failed());
        ASSERT_EQ(response.status(), COPYSET_OP_STATUS_FAILURE_UNKNOWN);

        // CASE 3: delete broken copyset success
        ASSERT_TRUE(copysetNodeManager->
                    DeleteCopysetNode(logicPoolId, copysetId));
        cntl.Reset();
        request.set_logicpoolid(logicPoolId);
        request.set_copysetid(copysetId);
        stub.DeleteBrokenCopyset(&cntl, &request, &response, nullptr);
        ASSERT_FALSE(cntl.Failed());
        ASSERT_EQ(response.status(), COPYSET_OP_STATUS_SUCCESS);
    }

    ASSERT_EQ(0, server.Stop(0));
    ASSERT_EQ(0, server.Join());
}

TEST_F(CopysetServiceTest, basic2) {
    /********************* 设置初始环境 ***********************/
    CopysetNodeManager *copysetNodeManager = &CopysetNodeManager::GetInstance();
    LogicPoolID logicPoolId = 2;
    CopysetID copysetId = 100003;
    std::string ip = "127.0.0.1";
    uint32_t port = 9040;

    brpc::Server server;
    butil::EndPoint addr(butil::IP_ANY, port);

    ASSERT_EQ(0, copysetNodeManager->AddService(&server, addr));
    ASSERT_EQ(0, server.Start(port, NULL));

    std::shared_ptr<LocalFileSystem> fs(LocalFsFactory::CreateFs(FileSystemType::EXT4, ""));    //NOLINT
    ASSERT_TRUE(nullptr != fs);

    butil::string_printf(&copysetDir, copysetDirPattern.c_str(), port);
    CopysetNodeOptions copysetNodeOptions;
    copysetNodeOptions.ip = ip;
    copysetNodeOptions.port = port;
    copysetNodeOptions.snapshotIntervalS = 30;
    copysetNodeOptions.catchupMargin = 50;
    copysetNodeOptions.chunkDataUri = copysetDir;
    copysetNodeOptions.chunkSnapshotUri = copysetDir;
    copysetNodeOptions.logUri = copysetDir;
    copysetNodeOptions.raftMetaUri = copysetDir;
    copysetNodeOptions.raftSnapshotUri = copysetDir;
    copysetNodeOptions.concurrentapply = new ConcurrentApplyModule();
    copysetNodeOptions.localFileSystem = fs;
    copysetNodeOptions.chunkFilePool =
        std::make_shared<FilePool>(fs);
    copysetNodeOptions.enableOdsyncWhenOpenChunkFile = true;
    ASSERT_EQ(0, copysetNodeManager->Init(copysetNodeOptions));
    ASSERT_EQ(0, copysetNodeManager->Run());

    brpc::Channel channel;
    PeerId peerId("127.0.0.1:9040:0");
    if (channel.Init(peerId.addr, NULL) != 0) {
        LOG(FATAL) << "Fail to init channel to " << peerId.addr;
    }

    /********************** 跑测试cases ************************/

    /* 测试创建一个新的 copyset */
    CopysetService_Stub stub(&channel);
    {
        brpc::Controller cntl;
        cntl.set_timeout_ms(3000);

        CopysetRequest2 request;
        CopysetResponse2 response;
        Copyset *copyset;
        copyset = request.add_copysets();
        copyset->set_logicpoolid(logicPoolId);
        copyset->set_copysetid(copysetId);
        Peer *peer1 = copyset->add_peers();
        peer1->set_address("127.0.0.1:9040:0");
        Peer *peer2 = copyset->add_peers();
        peer2->set_address("127.0.0.1:9041:0");
        Peer *peer3 = copyset->add_peers();
        peer3->set_address("127.0.0.1:9042:0");

        stub.CreateCopysetNode2(&cntl, &request, &response, nullptr);
        if (cntl.Failed()) {
            std::cout << cntl.ErrorText() << std::endl;
        }
        ASSERT_EQ(response.status(),
                  COPYSET_OP_STATUS::COPYSET_OP_STATUS_SUCCESS);
    }

    /* 测试创建一个重复 copyset */
    {
        brpc::Controller cntl;
        cntl.set_timeout_ms(3000);

        CopysetRequest2 request;
        CopysetResponse2 response;
        Copyset *copyset;
        copyset = request.add_copysets();
        copyset->set_logicpoolid(logicPoolId);
        copyset->set_copysetid(copysetId);
        Peer *peer1 = copyset->add_peers();
        peer1->set_address("127.0.0.1:9040:0");
        Peer *peer2 = copyset->add_peers();
        peer2->set_address("127.0.0.1:9041:0");
        Peer *peer3 = copyset->add_peers();
        peer3->set_address("127.0.0.1:9042:0");

        stub.CreateCopysetNode2(&cntl, &request, &response, nullptr);
        if (cntl.Failed()) {
            std::cout << cntl.ErrorText() << std::endl;
        }
        ASSERT_EQ(COPYSET_OP_STATUS::COPYSET_OP_STATUS_EXIST,
                  response.status());
    }

    /* 创建多个copyset */
    {
        brpc::Controller cntl;
        cntl.set_timeout_ms(3000);

        CopysetRequest2 request;
        CopysetResponse2 response;

        // 准备第1个copyset
        {
            Copyset *copyset;
            copyset = request.add_copysets();
            copyset->set_logicpoolid(logicPoolId);
            copyset->set_copysetid(copysetId + 1);
            Peer *peer1 = copyset->add_peers();
            peer1->set_address("127.0.0.1:9040:0");
            Peer *peer2 = copyset->add_peers();
            peer2->set_address("127.0.0.1:9041:0");
            Peer *peer3 = copyset->add_peers();
            peer3->set_address("127.0.0.1:9042:0");
        }

        // 准备第2个copyset
        {
            Copyset *copyset;
            copyset = request.add_copysets();
            copyset->set_logicpoolid(logicPoolId);
            copyset->set_copysetid(copysetId + 2);
            Peer *peer1 = copyset->add_peers();
            peer1->set_address("127.0.0.1:9040:0");
            Peer *peer2 = copyset->add_peers();
            peer2->set_address("127.0.0.1:9041:0");
            Peer *peer3 = copyset->add_peers();
            peer3->set_address("127.0.0.1:9042:0");
        }

        stub.CreateCopysetNode2(&cntl, &request, &response, nullptr);
        if (cntl.Failed()) {
            std::cout << cntl.ErrorText() << std::endl;
        }
        ASSERT_EQ(response.status(),
                  COPYSET_OP_STATUS::COPYSET_OP_STATUS_SUCCESS);
    }

    // get status
    {
        // 创建一个copyset
        {
            brpc::Controller cntl;
            cntl.set_timeout_ms(3000);
            CopysetRequest2 request;
            CopysetResponse2 response;

            Copyset *copyset;
            copyset = request.add_copysets();
            copyset->set_logicpoolid(logicPoolId);
            copyset->set_copysetid(copysetId + 3);
            Peer *peer1 = copyset->add_peers();
            peer1->set_address("127.0.0.1:9040:0");

            stub.CreateCopysetNode2(&cntl, &request, &response, nullptr);
            if (cntl.Failed()) {
                std::cout << cntl.ErrorText() << std::endl;
            }
            ASSERT_EQ(response.status(),
                      COPYSET_OP_STATUS::COPYSET_OP_STATUS_SUCCESS);
        }

        // 睡眠等待leader产生
        ::usleep(2 * 1000 * 1000);

        {
            // query hash为false
            std::string peerStr("127.0.0.1:9040:0");
            brpc::Controller cntl;
            cntl.set_timeout_ms(3000);
            CopysetStatusRequest request;
            CopysetStatusResponse response;
            request.set_logicpoolid(logicPoolId);
            request.set_copysetid(copysetId + 3);
            Peer *peer = new Peer();
            request.set_allocated_peer(peer);
            peer->set_address(peerStr);
            request.set_queryhash(false);

            stub.GetCopysetStatus(&cntl, &request, &response, nullptr);
            if (cntl.Failed()) {
                std::cout << cntl.ErrorText() << std::endl;
            }

            ASSERT_EQ(response.status(),
                      COPYSET_OP_STATUS::COPYSET_OP_STATUS_SUCCESS);
            ASSERT_EQ(braft::STATE_LEADER, response.state());
            ASSERT_STREQ(peerStr.c_str(), response.leader().address().c_str());
            ASSERT_EQ(false, response.readonly());
            ASSERT_EQ(2, response.term());
            ASSERT_EQ(1, response.committedindex());
            ASSERT_EQ(1, response.knownappliedindex());
            ASSERT_EQ(0, response.pendingindex());
            ASSERT_EQ(0, response.pendingqueuesize());
            ASSERT_EQ(0, response.applyingindex());
            ASSERT_EQ(1, response.firstindex());
            ASSERT_EQ(1, response.lastindex());
            ASSERT_EQ(1, response.diskindex());
            ASSERT_EQ(1, response.epoch());
            ASSERT_FALSE(response.has_hash());
        }
        {
            // query hash为true
            std::string peerStr("127.0.0.1:9040:0");
            brpc::Controller cntl;
            cntl.set_timeout_ms(3000);
            CopysetStatusRequest request;
            CopysetStatusResponse response;
            request.set_logicpoolid(logicPoolId);
            request.set_copysetid(copysetId + 3);
            Peer *peer = new Peer();
            request.set_allocated_peer(peer);
            peer->set_address(peerStr);
            request.set_queryhash(true);

            stub.GetCopysetStatus(&cntl, &request, &response, nullptr);
            if (cntl.Failed()) {
                std::cout << cntl.ErrorText() << std::endl;
            }

            ASSERT_EQ(response.status(),
                      COPYSET_OP_STATUS::COPYSET_OP_STATUS_SUCCESS);
            ASSERT_EQ(braft::STATE_LEADER, response.state());
            ASSERT_STREQ(peerStr.c_str(), response.leader().address().c_str());
            ASSERT_EQ(false, response.readonly());
            ASSERT_EQ(2, response.term());
            ASSERT_EQ(1, response.committedindex());
            ASSERT_EQ(1, response.knownappliedindex());
            ASSERT_EQ(0, response.pendingindex());
            ASSERT_EQ(0, response.pendingqueuesize());
            ASSERT_EQ(0, response.applyingindex());
            ASSERT_EQ(1, response.firstindex());
            ASSERT_EQ(1, response.lastindex());
            ASSERT_EQ(1, response.diskindex());
            ASSERT_EQ(1, response.epoch());
            ASSERT_TRUE(response.has_hash());
            ASSERT_EQ("0", response.hash());
        }
    }

    ASSERT_EQ(0, server.Stop(0));
    ASSERT_EQ(0, server.Join());
}

}  // namespace chunkserver
}  // namespace curve

