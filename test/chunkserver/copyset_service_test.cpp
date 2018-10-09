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

#include "src/chunkserver/copyset_node.h"
#include "src/chunkserver/copyset_node_manager.h"
#include "src/chunkserver/cli.h"
#include "proto/copyset.pb.h"
#include "src/sfs/sfsMock.h"

curve::sfs::LocalFileSystem * curve::sfs::LocalFsFactory::localFs_ = nullptr;

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

TEST_F(CopysetServiceTest, basic) {
    CopysetNodeManager *copysetNodeManager = &CopysetNodeManager::GetInstance();
    LogicPoolID logicPoolId = 1;
    CopysetID copysetId = 100002;
    std::string ip = "127.0.0.1";
    uint32_t port = 8200;
    std::string copysetDir = "local://./data";
    std::string initConf = "127.0.0.1:8200:0,127.0.0.1:8201:0";

    brpc::Server server;
    butil::EndPoint addr(butil::IP_ANY, port);
    ASSERT_EQ(0, copysetNodeManager->AddService(&server, addr));

    if (server.Start(port, NULL) != 0) {
        LOG(ERROR) << "Fail to start Server";
        return;
    }

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

    Configuration conf;
    if (conf.parse_from(initConf) != 0) {
        LOG(ERROR) << "Fail to parse configuration `" << initConf << '\'';
        return;
    }
    copysetNodeManager->Init(copysetNodeOptions);

    brpc::Channel channel;
    PeerId peerId("127.0.0.1:8200:0");
    if (channel.Init(peerId.addr, NULL) != 0) {
        LOG(FATAL) << "Fail to init channel to " << peerId.addr;
    }

    // 测试创建一个新的 copyset
    CopysetService_Stub stub(&channel);
    {
        brpc::Controller cntl;
        cntl.set_timeout_ms(10);

        CopysetRequest request;
        CopysetResponse response;
        request.set_logicpoolid(logicPoolId);
        request.set_copysetid(copysetId);
        request.add_conf("127.0.0.1:8200:0");
        request.add_conf("127.0.0.1:8201:0");
        stub.CreateCopysetNode(&cntl, &request, &response, nullptr);
        if (cntl.Failed()) {
            std::cout << cntl.ErrorText() << std::endl;
        }
        ASSERT_TRUE(COPYSET_OP_STATUS::COPYSET_OP_STATUS_SUCCESS == response.status());
    }

    // 测试创建一个已经存在的 copyset
    {
        brpc::Controller cntl;
        cntl.set_timeout_ms(2);

        CopysetRequest request;
        CopysetResponse response;
        request.set_logicpoolid(logicPoolId);
        request.set_copysetid(copysetId);
        request.add_conf("127.0.0.1:8200:0");
        request.add_conf("127.0.0.1:8201:0");
        stub.CreateCopysetNode(&cntl, &request, &response, nullptr);
        if (cntl.Failed()) {
            std::cout << cntl.ErrorText() << std::endl;
        }
        ASSERT_EQ(COPYSET_OP_STATUS::COPYSET_OP_STATUS_EXIST, response.status());
    }
}

}  // namespace chunkserver
}  // namespace curve

