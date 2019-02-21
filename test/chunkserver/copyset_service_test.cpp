/*
 * Project: curve
 * Created Date: 18-8-27
 * Author: wudemiao
 * Copyright (c) 2018 netease
 */

#include <gtest/gtest.h>
#include <gflags/gflags.h>
#include <bthread/bthread.h>
#include <brpc/channel.h>
#include <brpc/controller.h>
#include <brpc/server.h>

#include <cstdint>

#include "src/chunkserver/copyset_node.h"
#include "src/chunkserver/copyset_node_manager.h"
#include "src/chunkserver/cli.h"
#include "proto/copyset.pb.h"

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
        Exec("rm -rf data");
    }
    void TearDown() {
        Exec("rm -rf data");
    }
};

butil::AtExitManager atExitManager;

TEST_F(CopysetServiceTest, basic) {
    CopysetNodeManager *copysetNodeManager = &CopysetNodeManager::GetInstance();
    LogicPoolID logicPoolId = 1;
    CopysetID copysetId = 100002;
    std::string ip = "127.0.0.1";
    uint32_t port = 9200;
    std::string copysetDir = "local://./data";

    brpc::Server server;
    butil::EndPoint addr(butil::IP_ANY, port);

    ASSERT_EQ(0, copysetNodeManager->AddService(&server, addr));
    ASSERT_EQ(0, server.Start(port, NULL));

    std::shared_ptr<LocalFileSystem> fs(LocalFsFactory::CreateFs(FileSystemType::EXT4, ""));    //NOLINT
    ASSERT_TRUE(nullptr != fs);

    butil::string_printf(&copysetDir, "local://./data/%d", port);
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
    copysetNodeOptions.chunkfilePool =
        std::make_shared<ChunkfilePool>(fs);
    copysetNodeManager->Init(copysetNodeOptions);

    brpc::Channel channel;
    PeerId peerId("127.0.0.1:9200:0");
    if (channel.Init(peerId.addr, NULL) != 0) {
        LOG(FATAL) << "Fail to init channel to " << peerId.addr;
    }

    /* 测试创建一个新的 copyset */
    CopysetService_Stub stub(&channel);
    {
        brpc::Controller cntl;
        cntl.set_timeout_ms(100);

        CopysetRequest request;
        CopysetResponse response;
        request.set_logicpoolid(logicPoolId);
        request.set_copysetid(copysetId);
        request.add_peerid("127.0.0.1:9200:0");
        request.add_peerid("127.0.0.1:9201:0");
        request.add_peerid("127.0.0.1:9202:0");
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
        cntl.set_timeout_ms(100);

        CopysetRequest request;
        CopysetResponse response;
        request.set_logicpoolid(logicPoolId);
        request.set_copysetid(copysetId);
        request.add_peerid("127.0.0.1:9200:0");
        request.add_peerid("127.0.0.1:9201:0");
        request.add_peerid("127.0.0.1:9202:0");
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
        cntl.set_timeout_ms(100);

        CopysetRequest request;
        CopysetResponse response;
        request.set_logicpoolid(logicPoolId + 1);
        request.set_copysetid(copysetId + 1);
        request.add_peerid("127.0.0.1");
        request.add_peerid("127.0.0.1:9201:0");
        request.add_peerid("127.0.0.1:9202:0");
        stub.CreateCopysetNode(&cntl, &request, &response, nullptr);
        if (cntl.Failed()) {
            std::cout << cntl.ErrorText() << std::endl;
        }
        ASSERT_EQ(cntl.ErrorCode(), EINVAL);
    }

    ASSERT_EQ(0, server.Stop(0));
    ASSERT_EQ(0, server.Join());
}

}  // namespace chunkserver
}  // namespace curve

