/*
 * Copyright (C) 2018 NetEase Inc. All rights reserved.
 * Project: Curve
 * 
 * History: 
 *          2018/08/30  Wenyu Zhou   Initial version
 */

#include "src/chunkserver/chunkserver.h"

#include <gflags/gflags.h>

#include "include/chunkserver/chunkserver_common.h"

// TODO(wenyu): add more command line arguments
DEFINE_string(conf, "ChunkServer.conf", "Path of configuration file");

namespace curve {
namespace chunkserver {

void ChunkServer::InitCopysetNodeOptions() {
    copysetNodeOptions_.ip = conf_.GetStringValue("global.ip");
    copysetNodeOptions_.port = conf_.GetIntValue("global.port");
    copysetNodeOptions_.snapshotIntervalS =
        conf_.GetIntValue("copyset.snapshot_interval");
    copysetNodeOptions_.catchupMargin =
        conf_.GetIntValue("copyset.catchup_margin");
    copysetNodeOptions_.chunkDataUri =
        conf_.GetStringValue("copyset.chunk_data_uri");
    copysetNodeOptions_.chunkSnapshotUri =
        conf_.GetStringValue("copyset.chunk_data_uri");
    copysetNodeOptions_.logUri = conf_.GetStringValue("copyset.raft_log_uri");
    copysetNodeOptions_.raftMetaUri =
        conf_.GetStringValue("copyset.raft_meta_uri");
    copysetNodeOptions_.raftSnapshotUri =
        conf_.GetStringValue("copyset.raft_snapshot_uri");
    copysetNodeOptions_.maxChunkSize =
        conf_.GetIntValue("global.chunk_size");
    copysetNodeOptions_.copysetNodeManager = &copysetNodeManager_;
}

void ChunkServer::InitQosOptions() {
    qosOptions_.copysetNodeManager = &copysetNodeManager_;
}

void ChunkServer::InitIntegrityOptions() {
    // TODO(wenyu): to be implement
}

void ChunkServer::InitServiceOptions() {
    serviceOptions_.ip = conf_.GetStringValue("global.ip");
    serviceOptions_.port = conf_.GetIntValue("global.port");
    serviceOptions_.chunkserver = this;
    serviceOptions_.copysetNodeManager = &copysetNodeManager_;
}

int ChunkServer::Init(int argc, char **argv) {
    LOG(INFO) << "Starting to Initialize ChunkServer.";

    ParseCommandLineFlags(argc, argv);

    LOG(INFO) << "Loading Configuration.";
    conf_.SetConfigPath(FLAGS_conf);
    // FIXME: may also log into syslog
    LOG_IF(FATAL, !conf_.LoadConfig())
    << "Failed to open config file: " << conf_.GetConfigPath();
    ReplaceConfigWithCmdFlags();

    LOG(INFO) << "Initializing ChunkServer modules";

    InitCopysetNodeOptions();
    // copysetNodeManager_ = &kCopysetNodeManager;
    LOG_IF(FATAL, copysetNodeManager_.Init(copysetNodeOptions_) != 0)
    << "Failed to initialize CopysetNodeManager.";

    InitQosOptions();
    LOG_IF(FATAL, qosManager_.Init(qosOptions_) != 0)
    << "Failed to initialize QosManager.";

    InitIntegrityOptions();
    LOG_IF(FATAL, integrity_.Init(integrityOptions_) != 0)
    << "Failed to initialize integrity manager.";

    InitServiceOptions();
    LOG_IF(FATAL, serviceManager_.Init(serviceOptions_) != 0)
    << "Failed to initialize ServiceManager.";

    bool createTestCopyset = conf_.GetBoolValue("test.create_testcopyset");
    if (createTestCopyset) {
        LogicPoolID poolId = conf_.GetIntValue("test.testcopyset_poolid");
        CopysetID copysetId = conf_.GetIntValue("test.testcopyset_copysetid");
        braft::Configuration conf;
        conf.parse_from(conf_.GetStringValue("test.testcopyset_conf"));
        copysetNodeManager_.CreateCopysetNode(poolId, copysetId, conf);

        LOG(INFO)
        << "Created test copyset: <" << poolId << "," << copysetId << ">";
    }

    toStop = false;

    return 0;
}

int ChunkServer::Fini() {
    LOG(INFO) << "ChunkServer is going to quit.";

    bool createTestCopyset = conf_.GetBoolValue("test.create_testcopyset");
    if (createTestCopyset) {
        LogicPoolID poolId = conf_.GetIntValue("test.testcopyset_poolid");
        CopysetID copysetId = conf_.GetIntValue("test.testcopyset_copysetid");
        copysetNodeManager_.DeleteCopysetNode(poolId, copysetId);
        LOG(INFO)
        << "Test copyset removed: <" << poolId << "," << copysetId << ">";
    }

    LOG_IF(ERROR, integrity_.Fini() != 0)
    << "Failed to shutdown integrity manager.";
    LOG_IF(ERROR, qosManager_.Fini() != 0)
    << "Failed to shutdown QosManager.";
    LOG_IF(ERROR, copysetNodeManager_.Fini() != 0)
    << "Failed to shutdown CopysetNodeManager.";
    LOG_IF(ERROR, serviceManager_.Fini() != 0)
    << "Failed to shutdown ServiceManager.";

    // We don't save config yet
    // conf_.SaveConfig();

    return 0;
}

int ChunkServer::Run() {
    LOG(INFO) << "ChunkServer starts.";

    LOG_IF(FATAL, serviceManager_.Run() != 0)
    << "Failed to start ServiceManager.";
    LOG_IF(FATAL, copysetNodeManager_.Run() != 0)
    << "Failed to start CopysetNodeManager.";
    LOG_IF(FATAL, qosManager_.Run() != 0)
    << "Failed to start QosManager.";
    LOG_IF(FATAL, integrity_.Run() != 0)
    << "Failed to start integrity manager.";

    // TODO(wenyu): daemonize it

    while (!toStop) {
        sleep(1);
    }

    return 0;
}

int ChunkServer::ParseCommandLineFlags(int argc, char **argv) {
    gflags::ParseCommandLineFlags(&argc, &argv, true);

    return 0;
}

int ChunkServer::ReplaceConfigWithCmdFlags() {
    /**
     * TODO(wenyu): replace config items which provided in commandline flags,
     * so commandline flags have higher priority
     */
    return 0;
}

}  // namespace chunkserver
}  // namespace curve
