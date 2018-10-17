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
#include "src/sfs/sfsMock.h"

namespace curve {
namespace chunkserver {

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

// butil::AtExitManager atExitManager;

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
        cntl.set_timeout_ms(10);

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

    /* 测试创建一个新的 copyset */
    {
        brpc::Controller cntl;
        cntl.set_timeout_ms(2);

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

    ASSERT_EQ(0, server.Stop(0));
    ASSERT_EQ(0, server.Join());
}

}  // namespace chunkserver
}  // namespace curve

