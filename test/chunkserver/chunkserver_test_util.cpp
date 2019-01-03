/*
 * Project: curve
 * Created Date: 18-11-25
 * Author: wudemiao
 * Copyright (c) 2018 netease
 */

#include "test/chunkserver/chunkserver_test_util.h"

#include <glog/logging.h>
#include <bthread/bthread.h>
#include <brpc/channel.h>
#include <brpc/controller.h>
#include <brpc/server.h>

#include <string>

#include "src/chunkserver/copyset_node.h"
#include "src/chunkserver/copyset_node_manager.h"
#include "src/chunkserver/cli.h"

namespace curve {
namespace chunkserver {

std::string Exec(const char *cmd) {
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

int StartChunkserver(const char *ip,
                     int port,
                     const char *copysetdir,
                     const char *confs,
                     const int snapshotInterval,
                     const int electionTimeoutMs) {
    LOG(INFO) << "Going to start chunk server";

    /* Generally you only need one Server. */
    brpc::Server server;
    butil::EndPoint addr(butil::IP_ANY, port);
    if (0 != CopysetNodeManager::GetInstance().AddService(&server, addr)) {
        LOG(ERROR) << "Fail to add rpc service";
        return -1;
    }
    if (server.Start(port, NULL) != 0) {
        LOG(ERROR) << "Fail to start Server, port: " << port << ", errno: "
                   << errno << ", " << strerror(errno);
        return -1;
    }

    const uint32_t kMaxChunkSize = 16 * 1024 * 1024;
    CopysetNodeOptions copysetNodeOptions;
    copysetNodeOptions.ip = ip;
    copysetNodeOptions.port = port;
    copysetNodeOptions.electionTimeoutMs = electionTimeoutMs;
    copysetNodeOptions.snapshotIntervalS = snapshotInterval;
    copysetNodeOptions.catchupMargin = 50;
    copysetNodeOptions.chunkDataUri = copysetdir;
    copysetNodeOptions.chunkSnapshotUri = copysetdir;
    copysetNodeOptions.logUri = copysetdir;
    copysetNodeOptions.raftMetaUri = copysetdir;
    copysetNodeOptions.raftSnapshotUri = copysetdir;
    copysetNodeOptions.copysetNodeManager =
        &CopysetNodeManager::GetInstance();   //NOLINT
    copysetNodeOptions.maxChunkSize = kMaxChunkSize;

    Configuration conf;
    if (conf.parse_from(confs) != 0) {
        LOG(ERROR) << "Fail to parse configuration `" << confs << '\'';
        return -1;
    }

    LogicPoolID logicPoolId = 1;
    CopysetID copysetId = 100001;
    CopysetNodeManager::GetInstance().Init(copysetNodeOptions);
    CHECK(CopysetNodeManager::GetInstance().CreateCopysetNode(logicPoolId,
                                                              copysetId,
                                                              conf));

    /* Wait until 'CTRL-C' is pressed. then Stop() and Join() the service */
    while (!brpc::IsAskedToQuit()) {
        sleep(1);
    }
    LOG(INFO) << "server test service is going to quit";
    CopysetNodeManager::GetInstance().DeleteCopysetNode(logicPoolId, copysetId);
    server.Stop(0);
    server.Join();
}

butil::Status WaitLeader(const LogicPoolID &logicPoolId,
                         const CopysetID &copysetId,
                         const Configuration &conf,
                         PeerId *leaderId,
                         int electionTimeoutMs) {
    butil::Status status;
    const int kMaxLoop = (5 * electionTimeoutMs) / 100;
    for (int i = 0; i < kMaxLoop; ++i) {
        status = GetLeader(logicPoolId, copysetId, conf, leaderId);
        if (status.ok()) {
            /**
             * 等待 flush noop entry
             */
            ::usleep(electionTimeoutMs * 1000);
            return status;
        } else {
            LOG(WARNING) << "Get leader failed, " << status.error_str();
            usleep(100 * 1000);
        }
    }
}

}  // namespace chunkserver
}  // namespace curve
