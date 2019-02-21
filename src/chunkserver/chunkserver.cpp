/*
 * Copyright (C) 2018 NetEase Inc. All rights reserved.
 * Project: Curve
 * 
 * History: 
 *          2018/08/30  Wenyu Zhou   Initial version
 */

#include "src/chunkserver/chunkserver.h"

#include <gflags/gflags.h>

#include <string>

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
    copysetNodeOptions_.pageSize =
        conf_.GetIntValue("global.meta_page_size");
    copysetNodeOptions_.concurrentapply = &concurrentapply_;
    // TODO(wudemiao): 下面几个参数放在配置文件里面
    std::shared_ptr<LocalFileSystem> fs(LocalFsFactory::CreateFs(FileSystemType::EXT4, ""));    //NOLINT
    copysetNodeOptions_.localFileSystem = fs;
    copysetNodeOptions_.chunkfilePool
        = std::make_shared<ChunkfilePool>(fs);       //NOLINT
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
    LOG_IF(FATAL, copysetNodeManager_.Init(copysetNodeOptions_) != 0)
    << "Failed to initialize CopysetNodeManager.";

    LOG_IF(FATAL, false == concurrentapply_.Init(conf_.GetIntValue("concurrentapply.size"),            // NOLINT
                                                 conf_.GetIntValue("concurrentapply.queuedepth")))     // NOLINT
        << "Failed to initialize concurrentapply module!";

    ChunkfilePoolOptions options;
    options.chunkSize = conf_.GetIntValue("global.chunk_size");
    options.metaPageSize = conf_.GetIntValue("global.meta_page_size");

    options.retryTimes = conf_.GetIntValue("chunkfilepool.retry_times");
    options.cpMetaFileSize
        = conf_.GetIntValue("chunkfilepool.cpmeta_file_size");
    options.getChunkFromPool
        = conf_.GetBoolValue("chunkfilepool.enable_get_chunk_from_pool");
    if (options.getChunkFromPool == false) {
        std::string chunkFilePoolUri
            = conf_.GetStringValue("chunkfilepool.chunk_file_pool_dir");
        ::memcpy(options.chunkFilePoolDir,
                 chunkFilePoolUri.c_str(),
                 chunkFilePoolUri.size());
    } else {
        std::string metaUri
            = conf_.GetStringValue("chunkfilepool.meta_path");
        ::memcpy(options.metaPath,
                 metaUri.c_str(),
                 metaUri.size());
    }
    LOG_IF(FATAL, false == copysetNodeOptions_.chunkfilePool->Initialize(options))  //NOLINT
        << "Failed to init chunk file pool";

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
    concurrentapply_.Stop();

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

int ChunkServer::Stop() {
    toStop = true;

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
