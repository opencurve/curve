/*
 * Project: curve
 * Created Date: 18-8-24
 * Author: wudemiao
 * Copyright (c) 2018 netease
 */

#include <glog/logging.h>
#include <gflags/gflags.h>
#include <butil/at_exit.h>
#include <brpc/server.h>

#include "src/chunkserver/copyset_node_manager.h"
#include "src/chunkserver/copyset_node.h"
#include "src/chunkserver/chunk_service.h"
#include "src/fs/fs_common.h"
#include "src/fs/local_filesystem.h"
#include "src/chunkserver/concurrent_apply.h"
#include "src/chunkserver/datastore/chunkfile_pool.h"
#include "src/chunkserver/chunkserverStorage/chunkserver_adaptor_util.h"

using curve::chunkserver::CopysetNodeOptions;
using curve::chunkserver::Configuration;
using curve::chunkserver::CopysetNodeManager;
using curve::chunkserver::ChunkfilePool;
using curve::chunkserver::ChunkfilePoolOptions;
using curve::chunkserver::ConcurrentApplyModule;
using curve::chunkserver::FsAdaptorUtil;
using curve::chunkserver::LogicPoolID;
using curve::chunkserver::CopysetID;
using curve::fs::LocalFileSystem;
using curve::fs::LocalFsFactory;
using curve::fs::FileSystemType;

DEFINE_string(ip,
              "127.0.0.1",
              "Initial configuration of the replication group");
DEFINE_int32(port, 8200, "Listen port of this peer");
DEFINE_string(copyset_dir, "local://./0/", "copyset data dir");
DEFINE_string(conf,
              "127.0.0.1:8200:0,127.0.0.1:8201:0,127.0.0.1:8202:0",
              "Initial configuration of the replication group");
DEFINE_int32(election_timeout_ms, 1000, "election timeout");
DEFINE_int32(snapshot_interval_s, 5, "snapshot interval");
DEFINE_int32(catchup_margin, 100, "catchup margin");
DEFINE_int32(logic_pool_id, 2, "logic pool id");
DEFINE_int32(copyset_id, 10001, "copyset id");

butil::AtExitManager atExitManager;

int main(int argc, char *argv[]) {
    gflags::ParseCommandLineFlags(&argc, &argv, true);

    /* Generally you only need one Server. */
    brpc::Server server;
    butil::EndPoint addr(butil::IP_ANY, FLAGS_port);
    if (0 != CopysetNodeManager::GetInstance().AddService(&server, addr)) {
        LOG(ERROR) << "Fail to add rpc service";
        return -1;
    }

    if (server.Start(FLAGS_port, NULL) != 0) {
        LOG(ERROR) << "Fail to start Server: "
                   << errno << ", " << strerror(errno);
        return -1;
    }

    std::shared_ptr<LocalFileSystem>
        fs(LocalFsFactory::CreateFs(FileSystemType::EXT4, ""));
    const uint32_t kMaxChunkSize = 16 * 1024 * 1024;
    CopysetNodeOptions copysetNodeOptions;
    copysetNodeOptions.ip = FLAGS_ip;
    copysetNodeOptions.port = FLAGS_port;
    copysetNodeOptions.snapshotIntervalS = FLAGS_snapshot_interval_s;
    copysetNodeOptions.electionTimeoutMs = FLAGS_election_timeout_ms;
    copysetNodeOptions.catchupMargin = FLAGS_catchup_margin;
    copysetNodeOptions.chunkDataUri = FLAGS_copyset_dir;
    copysetNodeOptions.chunkSnapshotUri = FLAGS_copyset_dir;
    copysetNodeOptions.logUri = FLAGS_copyset_dir;
    copysetNodeOptions.raftMetaUri = FLAGS_copyset_dir;
    copysetNodeOptions.raftSnapshotUri = FLAGS_copyset_dir;
    copysetNodeOptions.pageSize = 4 * 1024;
    copysetNodeOptions.maxChunkSize = kMaxChunkSize;

    copysetNodeOptions.concurrentapply = new ConcurrentApplyModule();
    copysetNodeOptions.localFileSystem = fs;

    std::string chunkDataDir;
    std::string
        protocol = FsAdaptorUtil::ParserUri(FLAGS_copyset_dir, &chunkDataDir);
    if (protocol.empty()) {
        LOG(FATAL) << "not support chunk data uri's protocol"
                   << " error chunkDataDir is: " << chunkDataDir;
    }

    copysetNodeOptions.chunkfilePool = std::make_shared<ChunkfilePool>(fs);
    if (nullptr == copysetNodeOptions.chunkfilePool) {
        LOG(FATAL) << "new chunfilepool failed";
    }
    ChunkfilePoolOptions cfop;
    ::memcpy(cfop.chunkFilePoolDir, chunkDataDir.c_str(), chunkDataDir.size());
    cfop.getChunkFromPool = false;
    cfop.retryTimes = 3;
    cfop.metaPageSize = 4 * 1024;
    cfop.chunkSize = kMaxChunkSize;
    if (false == copysetNodeOptions.chunkfilePool->Initialize(cfop)) {
        LOG(FATAL) << "chunfilepool init failed";
    } else {
        LOG(INFO) << "chunfilepool init success";
    }

    LOG_IF(FATAL, false == copysetNodeOptions.concurrentapply->Init(2, 1))
    << "Failed to init concurrent apply module";

    Configuration conf;
    if (conf.parse_from(FLAGS_conf) != 0) {
        LOG(ERROR) << "Fail to parse configuration `" << FLAGS_conf << '\'';
        return -1;
    }

    LogicPoolID logicPoolId = 1;
    CopysetID copysetId = 100001;
    CopysetNodeManager::GetInstance().Init(copysetNodeOptions);
    CopysetNodeManager::GetInstance().CreateCopysetNode(FLAGS_logic_pool_id,
                                                        FLAGS_copyset_id,
                                                        conf);

    /* Wait until 'CTRL-C' is pressed. then Stop() and Join() the service */
    while (!brpc::IsAskedToQuit()) {
        sleep(1);
    }

    LOG(INFO) << "server test service is going to quit";
    CopysetNodeManager::GetInstance().DeleteCopysetNode(logicPoolId, copysetId);

    server.Stop(0);
    server.Join();

    return 0;
}
