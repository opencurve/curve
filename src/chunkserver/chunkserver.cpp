/*
 * Project: curve
 * Created Date: Thur May 9th 2019
 * Author: lixiaocui
 * Copyright (c) 2018 netease
 */

#include <glog/logging.h>

#include <butil/endpoint.h>
#include <braft/file_service.h>
#include <braft/builtin_service_impl.h>
#include <braft/raft_service.h>

#include "src/chunkserver/chunkserver.h"
#include "src/chunkserver/copyset_service.h"
#include "src/chunkserver/chunk_service.h"
#include "src/chunkserver/braft_cli_service.h"
#include "src/chunkserver/chunkserver_helper.h"
#include "src/chunkserver/chunkserverStorage/chunkserver_adaptor_util.h"

using ::curve::fs::LocalFileSystem;
using ::curve::fs::LocalFsFactory;
using ::curve::fs::FileSystemType;

DEFINE_string(conf, "ChunkServer.conf", "Path of configuration file");

namespace curve {
namespace chunkserver {
int ChunkServer::Run(int argc, char** argv) {
    gflags::ParseCommandLineFlags(&argc, &argv, true);

    // ==========================加载配置项===============================//
    LOG(INFO) << "Loading Configuration.";
    std::string confPath = FLAGS_conf.c_str();
    common::Configuration conf;
    conf.SetConfigPath(confPath);
    if (!conf.LoadConfig()) {
        LOG(ERROR) << "load chunkserver configuration fail, conf path = "
                   << confPath;
        return -1;
    }

    // ============================初始化各模块==========================//
    LOG(INFO) << "Initializing ChunkServer modules";
    // 初始化并发持久模块
    ConcurrentApplyModule concurrentapply;
    int size = conf.GetIntValue("concurrentapply.size");
    int qdepth = conf.GetIntValue("concurrentapply.queuedepth");
    LOG_IF(FATAL, false == concurrentapply.Init(size, qdepth))
        << "Failed to initialize concurrentapply module!";

    // 初始化本地文件系统
    std::shared_ptr<LocalFileSystem> fs(
        LocalFsFactory::CreateFs(FileSystemType::EXT4, ""));

    // 初始化chunk文件池
    ChunkfilePoolOptions chunkFilePoolOptions;
    InitChunkFilePoolOptions(&conf, &chunkFilePoolOptions);
    std::shared_ptr<ChunkfilePool> chunkfilePool =
        std::make_shared<ChunkfilePool>(fs);
    LOG_IF(FATAL, false == chunkfilePool->Initialize(chunkFilePoolOptions))
        << "Failed to init chunk file pool";

    // 远端拷贝管理模块选项
    CopyerOptions copyerOptions;
    InitCopyerOptions(&conf, &copyerOptions);
    auto curveClient = std::make_shared<FileClient>();
    auto s3Adapter = std::make_shared<S3Adapter>();
    auto copyer = std::make_shared<OriginCopyer>(curveClient, s3Adapter);
    LOG_IF(FATAL, copyer->Init(copyerOptions) != 0)
        << "Failed to initialize clone copyer.";

    // 克隆管理模块初始化
    CloneOptions cloneOptions;
    InitCloneOptions(&conf, &cloneOptions);
    uint32_t sliceSize = conf.GetIntValue("clone.slice_size");
    cloneOptions.core = std::make_shared<CloneCore>(sliceSize, copyer);
    LOG_IF(FATAL, cloneManager_.Init(cloneOptions) != 0)
        << "Failed to initialize clone manager.";

    // 初始化注册模块
    RegisterOptions registerOptions;
    InitRegisterOptions(&conf, &registerOptions);
    registerOptions.fs = fs;
    Register registerMDS(registerOptions);
    ChunkServerMetadata metadata;
    // 从本地获取meta
    std::string metaPath = FsAdaptorUtil::GetPathFromUri(
        registerOptions.chunkserverMetaUri).c_str();
    if (fs->FileExists(metaPath)) {
        LOG_IF(FATAL, GetChunkServerMetaFromLocal(
                            registerOptions.chunserverStoreUri,
                            registerOptions.chunkserverMetaUri,
                            registerOptions.fs, &metadata) != 0)
            << "Failed to register to MDS.";
    } else {
        // 如果本地获取不到，向mds注册
        LOG(INFO) << "meta file "
                  << metaPath << " do not exist, register to mds";
        LOG_IF(FATAL, registerMDS.RegisterToMDS(&metadata) != 0)
            << "Failed to register to MDS.";
    }

    // 初始化复制组管理模块
    CopysetNodeOptions copysetNodeOptions;
    InitCopysetNodeOptions(&conf, &copysetNodeOptions);
    copysetNodeOptions.concurrentapply = &concurrentapply;
    copysetNodeOptions.chunkfilePool = chunkfilePool;
    copysetNodeOptions.localFileSystem = fs;
    butil::ip_t ip;
    if (butil::str2ip(copysetNodeOptions.ip.c_str(), &ip) < 0) {
        LOG(FATAL) << "Invalid server IP provided: " << copysetNodeOptions.ip;
        return -1;
    }
    butil::EndPoint endPoint = butil::EndPoint(ip, copysetNodeOptions.port);
    if (!braft::NodeManager::GetInstance()->server_exists(endPoint)) {
        braft::NodeManager::GetInstance()->add_address(endPoint);
    }
    LOG_IF(FATAL, copysetNodeManager_.Init(copysetNodeOptions) != 0)
        << "Failed to initialize CopysetNodeManager.";
    LOG_IF(FATAL, copysetNodeManager_.ReloadCopysets() != 0)
        << "CopysetNodeManager Failed to reload copyset.";


    // 心跳模块初始化
    HeartbeatOptions heartbeatOptions;
    InitHeartbeatOptions(&conf, &heartbeatOptions);
    heartbeatOptions.copysetNodeManager = &copysetNodeManager_;
    heartbeatOptions.fs = fs;
    heartbeatOptions.chunkserverId = metadata.id();
    heartbeatOptions.chunkserverToken = metadata.token();
    LOG_IF(FATAL, heartbeat_.Init(heartbeatOptions) != 0)
        << "Failed to init Heartbeat manager.";

    // =======================启动各模块==================================//
    LOG(INFO) << "ChunkServer starts.";

    LOG_IF(FATAL, copysetNodeManager_.Run() != 0)
        << "Failed to start CopysetNodeManager.";
    LOG_IF(FATAL, heartbeat_.Run() != 0)
        << "Failed to start heartbeat manager.";
    LOG_IF(FATAL, cloneManager_.Run() != 0)
        << "Failed to start clone manager.";

    // ========================添加rpc服务===============================//
    // TODO(lixiaocui): rpc中各接口添加上延迟metric
    brpc::Server server;

    // copyset service
    CopysetServiceImpl copysetService(&copysetNodeManager_);
    int ret = server.AddService(&copysetService,
                        brpc::SERVER_DOESNT_OWN_SERVICE);
    CHECK(0 == ret) << "Fail to add CopysetService";

    // chunk service
    ChunkServiceOptions chunkServiceOptions;
    chunkServiceOptions.copysetNodeManager = &copysetNodeManager_;
    chunkServiceOptions.cloneManager = &cloneManager_;
    ChunkServiceImpl chunkService(chunkServiceOptions);
    ret = server.AddService(&chunkService,
                        brpc::SERVER_DOESNT_OWN_SERVICE);
    CHECK(0 == ret) << "Fail to add ChunkService";

    // braftclient service
    BRaftCliServiceImpl braftCliService;
    ret = server.AddService(&braftCliService,
                        brpc::SERVER_DOESNT_OWN_SERVICE);
    CHECK(0 == ret) << "Fail to add BRaftCliService";

    // raft service
    braft::RaftServiceImpl raftService(endPoint);
    ret = server.AddService(&raftService,
        brpc::SERVER_DOESNT_OWN_SERVICE);
    CHECK(0 == ret) << "Fail to add RaftService";

    // raft stat service
    braft::RaftStatImpl raftStatService;
    ret = server.AddService(&raftStatService,
        brpc::SERVER_DOESNT_OWN_SERVICE);
    CHECK(0 == ret) << "Fail to add RaftStatService";

    // braft file service
    ret = server.AddService(braft::file_service(),
        brpc::SERVER_DOESNT_OWN_SERVICE);
    CHECK(0 == ret) << "Fail to add FileService";


    // 启动rpc service
    LOG(INFO) << "RPC server is going to serve on: "
              << copysetNodeOptions.ip << ":" << copysetNodeOptions.port;
    if (server.Start(endPoint, NULL) != 0) {
        LOG(ERROR) << "Fail to start RPC Server";
        return -1;
    }

    toStop_ = false;
    while (!toStop_ && !brpc::IsAskedToQuit()) {
        sleep(1);
    }

    LOG(INFO) << "ChunkServer is going to quit.";
    LOG_IF(ERROR, heartbeat_.Fini() != 0)
        << "Failed to shutdown heartbeat manager.";
    LOG_IF(ERROR, cloneManager_.Fini() != 0)
        << "Failed to shutdown clone manager.";
    LOG_IF(ERROR, copyer->Fini() != 0)
        << "Failed to shutdown clone copyer.";
    LOG_IF(ERROR, copysetNodeManager_.Fini() != 0)
        << "Failed to shutdown CopysetNodeManager.";
    concurrentapply.Stop();
    return 0;
}

void ChunkServer::Stop() {
    toStop_ = true;
}

void ChunkServer::InitChunkFilePoolOptions(
    common::Configuration *conf, ChunkfilePoolOptions *chunkFilePoolOptions) {
    chunkFilePoolOptions->chunkSize =
        conf->GetIntValue("global.chunk_size");
    chunkFilePoolOptions->metaPageSize =
        conf->GetIntValue("global.meta_page_size");
    chunkFilePoolOptions->cpMetaFileSize
        = conf->GetIntValue("chunkfilepool.cpmeta_file_size");
    chunkFilePoolOptions->getChunkFromPool
        = conf->GetBoolValue("chunkfilepool.enable_get_chunk_from_pool");

    if (chunkFilePoolOptions->getChunkFromPool == false) {
        std::string chunkFilePoolUri
            = conf->GetStringValue("chunkfilepool.chunk_file_pool_dir");
        ::memcpy(chunkFilePoolOptions->chunkFilePoolDir,
                 chunkFilePoolUri.c_str(),
                 chunkFilePoolUri.size());
    } else {
        std::string metaUri
            = conf->GetStringValue("chunkfilepool.meta_path");
        ::memcpy(chunkFilePoolOptions->metaPath,
                 metaUri.c_str(),
                 metaUri.size());
    }
}

void ChunkServer::InitCopysetNodeOptions(
    common::Configuration *conf, CopysetNodeOptions *copysetNodeOptions) {
    copysetNodeOptions->ip = conf->GetStringValue("global.ip");
    copysetNodeOptions->port = conf->GetIntValue("global.port");
    if (copysetNodeOptions->port <= 0 || copysetNodeOptions->port >= 65535) {
        LOG(FATAL) << "Invalid server port provided: "
                   << copysetNodeOptions->port;
    }
    copysetNodeOptions->snapshotIntervalS =
        conf->GetIntValue("copyset.snapshot_interval");
    copysetNodeOptions->catchupMargin =
        conf->GetIntValue("copyset.catchup_margin");
    copysetNodeOptions->chunkDataUri =
        conf->GetStringValue("copyset.chunk_data_uri");
    copysetNodeOptions->chunkSnapshotUri =
        conf->GetStringValue("copyset.chunk_data_uri");
    copysetNodeOptions->logUri = conf->GetStringValue("copyset.raft_log_uri");
    copysetNodeOptions->raftMetaUri =
        conf->GetStringValue("copyset.raft_meta_uri");
    copysetNodeOptions->raftSnapshotUri =
        conf->GetStringValue("copyset.raft_snapshot_uri");
    copysetNodeOptions->recyclerUri =
        conf->GetStringValue("copyset.recycler_uri");
    copysetNodeOptions->maxChunkSize =
        conf->GetIntValue("global.chunk_size");
    copysetNodeOptions->pageSize =
        conf->GetIntValue("global.meta_page_size");
}

void ChunkServer::InitCopyerOptions(
    common::Configuration *conf, CopyerOptions *copyerOptions) {
    copyerOptions->curveUser.owner =
        conf->GetStringValue("curve.root_username");
    copyerOptions->curveUser.password =
        conf->GetStringValue("curve.root_password");
    copyerOptions->curveConf = conf->GetStringValue("curve.config_path");
    copyerOptions->s3Conf = conf->GetStringValue("s3.config_path");
}

void ChunkServer::InitCloneOptions(
    common::Configuration *conf, CloneOptions *cloneOptions) {
    cloneOptions->threadNum = conf->GetIntValue("clone.thread_num");
    cloneOptions->queueCapacity = conf->GetIntValue("clone.queue_depth");
}

void ChunkServer::InitHeartbeatOptions(
    common::Configuration *conf, HeartbeatOptions *heartbeatOptions) {
    heartbeatOptions->dataUri = conf->GetStringValue("copyset.chunk_data_uri");
    heartbeatOptions->ip = conf->GetStringValue("global.ip");
    heartbeatOptions->port = conf->GetIntValue("global.port");
    heartbeatOptions->mdsIp = conf->GetStringValue("mds.ip");
    heartbeatOptions->mdsPort = conf->GetIntValue("mds.port");
    heartbeatOptions->interval = conf->GetIntValue("mds.heartbeat_interval");
    heartbeatOptions->timeout = conf->GetIntValue("mds.heartbeat_timeout");
}

void ChunkServer::InitRegisterOptions(
    common::Configuration *conf, RegisterOptions *registerOptions) {
    registerOptions->mdsIp = conf->GetStringValue("mds.ip");
    registerOptions->mdsPort = conf->GetIntValue("mds.port");
    if (registerOptions->mdsPort <= 0 || registerOptions->mdsPort >= 65535) {
        LOG(FATAL) << "Invalid MDS port provided: " << registerOptions->mdsPort;
    }
    registerOptions->chunkserverIp = conf->GetStringValue("global.ip");
    registerOptions->chunkserverPort = conf->GetIntValue("global.port");
    registerOptions->chunserverStoreUri =
        conf->GetStringValue("chunkserver.stor_uri");
    registerOptions->chunkserverMetaUri =
        conf->GetStringValue("chunkserver.meta_uri");
    registerOptions->chunkserverDiskType =
        conf->GetStringValue("chunkserver.disk_type");
    registerOptions->registerRetries =
        conf->GetIntValue("mds.register_retries");
    registerOptions->registerTimeout =
        conf->GetIntValue("mds.register_timeout");
}

int ChunkServer::GetChunkServerMetaFromLocal(
    const std::string &storeUri,
    const std::string &metaUri,
    const std::shared_ptr<LocalFileSystem> &fs,
    ChunkServerMetadata *metadata) {
    std::string proto =
        FsAdaptorUtil::GetProtocolFromUri(storeUri);
    if (proto != "local") {
        LOG(ERROR) << "Datastore protocal " << proto << " is not supported yet";
        return -1;
    }
    // 从配置文件中获取chunkserver元数据的文件路径
    proto = FsAdaptorUtil::GetProtocolFromUri(metaUri);
    if (proto != "local") {
        LOG(ERROR) << "Chunkserver meta protocal "
                   << proto << " is not supported yet";
        return -1;
    }
    // 元数据文件已经存在
    if (fs->FileExists(FsAdaptorUtil::GetPathFromUri(metaUri).c_str())) {
        // 获取文件内容
        if (ReadChunkServerMeta(fs, metaUri, metadata) != 0) {
            LOG(ERROR) << "Fail to read persisted chunkserver meta data";
            return -1;
        }

        LOG(INFO) << "Found persisted chunkserver data, skipping registration,"
                  << " chunkserver id: " << metadata->id()
                  << ", token: " << metadata->token();
        return 0;
    }
    return -1;
}

int ChunkServer::ReadChunkServerMeta(const std::shared_ptr<LocalFileSystem> &fs,
    const std::string &metaUri, ChunkServerMetadata *metadata) {
    int fd;
    std::string metaFile =
        FsAdaptorUtil::GetPathFromUri(metaUri);

    fd = fs->Open(metaFile.c_str(), O_RDONLY);
    if (fd < 0) {
        LOG(ERROR) << "Failed to open Chunkserver metadata file " << metaFile;
        return -1;
    }

    #define METAFILE_MAX_SIZE  4096
    uint32_t size;
    char json[METAFILE_MAX_SIZE] = {0};

    size = fs->Read(fd, json, 0, METAFILE_MAX_SIZE);
    if (size < 0) {
        LOG(ERROR) << "Failed to read Chunkserver metadata file";
        return -1;
    } else if (size >= METAFILE_MAX_SIZE) {
        LOG(ERROR) << "Chunkserver metadata file is too large: " << size;
        return -1;
    }
    if (fs->Close(fd)) {
        LOG(ERROR) << "Failed to close chunkserver metadata file";
        return -1;
    }

    if (!ChunkServerMetaHelper::DecodeChunkServerMeta(json, metadata)) {
        LOG(ERROR) << "Failed to decode chunkserver meta: " << json;
        return -1;
    }

    return 0;
}

}  // namespace chunkserver
}  // namespace curve

