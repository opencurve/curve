/*
 * Project: curve
 * Created Date: 18-8-24
 * Author: wudemiao
 * Copyright (c) 2018 netease
 */

#include <gflags/gflags.h>
#include <butil/at_exit.h>
#include <brpc/server.h>

#include "src/chunkserver/copyset_node_manager.h"
#include "src/chunkserver/copyset_node.h"
#include "src/chunkserver/chunk_service.h"
#include "src/sfs/sfsMock.h"

using curve::chunkserver::CopysetNodeOptions;
using curve::chunkserver::Configuration;
using curve::chunkserver::CopysetNodeManager;
using curve::chunkserver::LogicPoolID;
using curve::chunkserver::CopysetID;

DEFINE_string(ip, "127.0.0.1", "Initial configuration of the replication group");
DEFINE_string(copyset_dir, "local://.", "copyset data dir");
DEFINE_int32(port, 8200, "Listen port of this peer");
DEFINE_string(conf,
              "127.0.0.1:8200:0,127.0.0.1:8201:0,127.0.0.1:8202:0",
              "Initial configuration of the replication group");


curve::sfs::LocalFileSystem * curve::sfs::LocalFsFactory::localFs_ = nullptr;

int main(int argc, char *argv[]) {
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    butil::AtExitManager atExitManager;

    // Generally you only need one Server.
    brpc::Server server;
    butil::EndPoint addr(butil::IP_ANY, FLAGS_port);
    if (0 != CopysetNodeManager::GetInstance().AddService(&server, addr)) {
        LOG(ERROR) << "Fail to add rpc service";
        return -1;
    }

    if (server.Start(FLAGS_port, NULL) != 0) {
        LOG(ERROR) << "Fail to start Server";
        return -1;
    }

    CopysetNodeOptions copysetNodeOptions;
    copysetNodeOptions.ip = FLAGS_ip;
    copysetNodeOptions.port = FLAGS_port;
    copysetNodeOptions.snapshotIntervalS = 30;
    copysetNodeOptions.catchupMargin = 50;
    copysetNodeOptions.chunkDataUri = FLAGS_copyset_dir;
    copysetNodeOptions.chunkSnapshotUri = FLAGS_copyset_dir;
    copysetNodeOptions.logUri = FLAGS_copyset_dir;
    copysetNodeOptions.raftMetaUri = FLAGS_copyset_dir;
    copysetNodeOptions.raftSnapshotUri = FLAGS_copyset_dir;
    copysetNodeOptions.copysetNodeManager = &CopysetNodeManager::GetInstance();

    Configuration conf;
    if (conf.parse_from(FLAGS_conf) != 0) {
        LOG(ERROR) << "Fail to parse configuration `" << FLAGS_conf << '\'';
        return -1;
    }

    LogicPoolID logicPoolId = 1;
    CopysetID copysetId = 100001;
    CopysetNodeManager::GetInstance().Init(copysetNodeOptions);
    CHECK(CopysetNodeManager::GetInstance().CreateCopysetNode(logicPoolId, copysetId, conf));


    // Wait until 'CTRL-C' is pressed. then Stop() and Join() the service
    while (!brpc::IsAskedToQuit()) {
        sleep(1);
    }

    LOG(INFO) << "server test service is going to quit";
    CopysetNodeManager::GetInstance().DeleteCopysetNode(logicPoolId, copysetId);
    server.Stop(0);
    server.Join();

    return 0;
}
