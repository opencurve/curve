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

#include <memory>

#include "src/chunkserver/chunkserver.h"
#include "src/chunkserver/chunkserver_metrics.h"
#include "src/chunkserver/copyset_service.h"
#include "src/chunkserver/chunk_service.h"
#include "src/chunkserver/braft_cli_service.h"
#include "src/chunkserver/braft_cli_service2.h"
#include "src/chunkserver/chunkserver_helper.h"
#include "src/chunkserver/chunkserverStorage/chunkserver_adaptor_util.h"

using ::curve::fs::LocalFileSystem;
using ::curve::fs::LocalFileSystemOption;
using ::curve::fs::LocalFsFactory;
using ::curve::fs::FileSystemType;

DEFINE_string(conf, "ChunkServer.conf", "Path of configuration file");
DEFINE_string(chunkServerIp, "127.0.0.1", "chunkserver ip");
DEFINE_int32(chunkServerPort, 8200, "chunkserver port");
DEFINE_string(chunkServerStoreUri, "local://./0/", "chunkserver store uri");
DEFINE_string(chunkServerMetaUri,
    "local://./0/chunkserver.dat", "chunnkserver meata uri");
DEFINE_string(copySetUri, "local://./0/copysets", "copyset data uri");
DEFINE_string(recycleUri, "local://./0/recycler" , "recycle uri");
DEFINE_string(chunkFilePoolDir, "./0/", "chunk file pool location");
DEFINE_string(chunkFilePoolMetaPath,
    "./chunkfilepool.meta", "chunk file pool meta path");

namespace curve {
namespace chunkserver {
int ChunkServer::Run(int argc, char** argv) {
    gflags::ParseCommandLineFlags(&argc, &argv, true);

    // ==========================加载配置项===============================//
    LOG(INFO) << "Loading Configuration.";
    common::Configuration conf;
    conf.SetConfigPath(FLAGS_conf.c_str());

    // 在从配置文件获取
    LOG_IF(FATAL, !conf.LoadConfig())
        << "load chunkserver configuration fail, conf path = "
        << conf.GetConfigPath();
    // 命令行可以覆盖配置文件中的参数
    LoadConfigFromCmdline(&conf);

    // ============================初始化各模块==========================//
    LOG(INFO) << "Initializing ChunkServer modules";
    // 初始化并发持久模块
    ConcurrentApplyModule concurrentapply;
    int size;
    LOG_IF(FATAL, !conf.GetIntValue("concurrentapply.size", &size));
    int qdepth;
    LOG_IF(FATAL, !conf.GetIntValue("concurrentapply.queuedepth", &qdepth));
    LOG_IF(FATAL, false == concurrentapply.Init(size, qdepth))
        << "Failed to initialize concurrentapply module!";

    // 初始化本地文件系统
    std::shared_ptr<LocalFileSystem> fs(
        LocalFsFactory::CreateFs(FileSystemType::EXT4, ""));
    LocalFileSystemOption lfsOption;
    LOG_IF(FATAL, !conf.GetBoolValue(
        "fs.enable_renameat2", &lfsOption.enableRenameat2));
    LOG_IF(FATAL, 0 != fs->Init(lfsOption))
        << "Failed to initialize local filesystem module!";


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
    auto copyer = std::make_shared<OriginCopyer>();
    LOG_IF(FATAL, copyer->Init(copyerOptions) != 0)
        << "Failed to initialize clone copyer.";

    // 克隆管理模块初始化
    CloneOptions cloneOptions;
    InitCloneOptions(&conf, &cloneOptions);
    uint32_t sliceSize;
    LOG_IF(FATAL, !conf.GetUInt32Value("clone.slice_size", &sliceSize));
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

    // trash模块初始化
    TrashOptions trashOptions;
    InitTrashOptions(&conf, &trashOptions);
    trashOptions.localFileSystem = fs;
    trashOptions.chunkfilePool = chunkfilePool;
    trash_ = std::make_shared<Trash>();
    LOG_IF(FATAL, trash_->Init(trashOptions) != 0)
        << "Failed to init Trash";

    // 初始化复制组管理模块
    CopysetNodeOptions copysetNodeOptions;
    InitCopysetNodeOptions(&conf, &copysetNodeOptions);
    copysetNodeOptions.concurrentapply = &concurrentapply;
    copysetNodeOptions.chunkfilePool = chunkfilePool;
    copysetNodeOptions.localFileSystem = fs;
    copysetNodeOptions.trash = trash_;

    // install snapshot的带宽限制
    int snapshotThroughputBytes;
    LOG_IF(FATAL,
           !conf.GetIntValue("chunkserver.snapshot_throttle_throughput_bytes",
                             &snapshotThroughputBytes));
    /**
     * checkCycles是为了更精细的进行带宽控制，以snapshotThroughputBytes=100MB，
     * checkCycles=10为例，它可以保证每1/10秒的带宽是10MB，且不累积，例如第1个
     * 1/10秒的带宽是10MB，但是就过期了，在第2个1/10秒依然只能用10MB的带宽，而
     * 不是20MB的带宽
     */
    int checkCycles;
    LOG_IF(FATAL,
           !conf.GetIntValue("chunkserver.snapshot_throttle_check_cycles",
                             &checkCycles));
    scoped_refptr<SnapshotThrottle> snapshotThrottle
        = new ThroughputSnapshotThrottle(snapshotThroughputBytes, checkCycles);
    snapshotThrottle_ = snapshotThrottle;
    copysetNodeOptions.snapshotThrottle = &snapshotThrottle_;

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

    // 初始化 metric 收集模块
    ChunkServerMetricOptions metricOptions;
    InitMetricOptions(&conf, &metricOptions);
    ChunkServerMetric* metric = ChunkServerMetric::GetInstance();
    LOG_IF(FATAL, metric->Init(metricOptions) != 0)
        << "Failed to init chunkserver metric.";
    metric->MonitorCopysetManager(&copysetNodeManager_);
    metric->MonitorChunkFilePool(chunkfilePool.get());
    metric->UpdateConfigMetric(conf);

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
    LOG_IF(FATAL, trash_->Run() != 0)
        << "Failed to start trash.";

    // ========================添加rpc服务===============================//
    // TODO(lixiaocui): rpc中各接口添加上延迟metric
    brpc::Server server;

    // copyset service
    CopysetServiceImpl copysetService(&copysetNodeManager_);
    int ret = server.AddService(&copysetService,
                        brpc::SERVER_DOESNT_OWN_SERVICE);
    CHECK(0 == ret) << "Fail to add CopysetService";

    // inflight throttle
    int maxInflight;
    LOG_IF(FATAL,
           !conf.GetIntValue("copyset.max_inflight_requests",
                             &maxInflight));
    std::shared_ptr<InflightThrottle> inflightThrottle
        = std::make_shared<InflightThrottle>(maxInflight);
    CHECK(nullptr != inflightThrottle) << "new inflight throttle failed";

    // chunk service
    ChunkServiceOptions chunkServiceOptions;
    chunkServiceOptions.copysetNodeManager = &copysetNodeManager_;
    chunkServiceOptions.cloneManager = &cloneManager_;
    chunkServiceOptions.inflightThrottle = inflightThrottle;
    ChunkServiceImpl chunkService(chunkServiceOptions);
    ret = server.AddService(&chunkService,
                        brpc::SERVER_DOESNT_OWN_SERVICE);
    CHECK(0 == ret) << "Fail to add ChunkService";

    // braftclient service
    BRaftCliServiceImpl braftCliService;
    ret = server.AddService(&braftCliService,
                        brpc::SERVER_DOESNT_OWN_SERVICE);
    CHECK(0 == ret) << "Fail to add BRaftCliService";

    // braftclient service
    BRaftCliServiceImpl2 braftCliService2;
    ret = server.AddService(&braftCliService2,
                        brpc::SERVER_DOESNT_OWN_SERVICE);
    CHECK(0 == ret) << "Fail to add BRaftCliService2";

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
    LOG_IF(ERROR, trash_->Fini() != 0)
        << "Failed to shutdown trash.";
    concurrentapply.Stop();
    return 0;
}

void ChunkServer::Stop() {
    toStop_ = true;
}

void ChunkServer::InitChunkFilePoolOptions(
    common::Configuration *conf, ChunkfilePoolOptions *chunkFilePoolOptions) {
    LOG_IF(FATAL, !conf->GetUInt32Value("global.chunk_size",
        &chunkFilePoolOptions->chunkSize));
    LOG_IF(FATAL, !conf->GetUInt32Value("global.meta_page_size",
        &chunkFilePoolOptions->metaPageSize));
    LOG_IF(FATAL, !conf->GetUInt32Value("chunkfilepool.cpmeta_file_size",
        &chunkFilePoolOptions->cpMetaFileSize));
    LOG_IF(FATAL, !conf->GetBoolValue(
        "chunkfilepool.enable_get_chunk_from_pool",
        &chunkFilePoolOptions->getChunkFromPool));

    if (chunkFilePoolOptions->getChunkFromPool == false) {
        std::string chunkFilePoolUri;
        LOG_IF(FATAL, !conf->GetStringValue(
            "chunkfilepool.chunk_file_pool_dir", &chunkFilePoolUri));
        ::memcpy(chunkFilePoolOptions->chunkFilePoolDir,
                 chunkFilePoolUri.c_str(),
                 chunkFilePoolUri.size());
    } else {
        std::string metaUri;
        LOG_IF(FATAL, !conf->GetStringValue(
            "chunkfilepool.meta_path", &metaUri));
        ::memcpy(
            chunkFilePoolOptions->metaPath, metaUri.c_str(), metaUri.size());
    }
}

void ChunkServer::InitCopysetNodeOptions(
    common::Configuration *conf, CopysetNodeOptions *copysetNodeOptions) {
    LOG_IF(FATAL, !conf->GetStringValue("global.ip", &copysetNodeOptions->ip));
    LOG_IF(FATAL, !conf->GetUInt32Value(
        "global.port", &copysetNodeOptions->port));
    if (copysetNodeOptions->port <= 0 || copysetNodeOptions->port >= 65535) {
        LOG(FATAL) << "Invalid server port provided: "
                   << copysetNodeOptions->port;
    }

    LOG_IF(FATAL, !conf->GetIntValue("copyset.snapshot_interval_s",
        &copysetNodeOptions->snapshotIntervalS));
    LOG_IF(FATAL, !conf->GetIntValue("copyset.catchup_margin",
        &copysetNodeOptions->catchupMargin));
    LOG_IF(FATAL, !conf->GetStringValue("copyset.chunk_data_uri",
        &copysetNodeOptions->chunkDataUri));
    LOG_IF(FATAL, !conf->GetStringValue("copyset.chunk_data_uri",
        &copysetNodeOptions->chunkSnapshotUri));
    LOG_IF(FATAL, !conf->GetStringValue("copyset.raft_log_uri",
        &copysetNodeOptions->logUri));
    LOG_IF(FATAL, !conf->GetStringValue("copyset.raft_meta_uri",
        &copysetNodeOptions->raftMetaUri));
    LOG_IF(FATAL, !conf->GetStringValue("copyset.raft_snapshot_uri",
        &copysetNodeOptions->raftSnapshotUri));
    LOG_IF(FATAL, !conf->GetStringValue("copyset.recycler_uri",
        &copysetNodeOptions->recyclerUri));
    LOG_IF(FATAL, !conf->GetUInt32Value("global.chunk_size",
        &copysetNodeOptions->maxChunkSize));
    LOG_IF(FATAL, !conf->GetUInt32Value("global.meta_page_size",
        &copysetNodeOptions->pageSize));
}

void ChunkServer::InitCopyerOptions(
    common::Configuration *conf, CopyerOptions *copyerOptions) {
    LOG_IF(FATAL, !conf->GetStringValue("curve.root_username",
        &copyerOptions->curveUser.owner));
    LOG_IF(FATAL, !conf->GetStringValue("curve.root_password",
        &copyerOptions->curveUser.password));
    LOG_IF(FATAL, !conf->GetStringValue("curve.config_path",
        &copyerOptions->curveConf));
    LOG_IF(FATAL,
        !conf->GetStringValue("s3.config_path", &copyerOptions->s3Conf));
    bool disableCurveClient = false;
    bool disableS3Adapter = false;
    LOG_IF(FATAL, !conf->GetBoolValue("clone.disable_curve_client",
        &disableCurveClient));
    LOG_IF(FATAL, !conf->GetBoolValue("clone.disable_s3_adapter",
        &disableS3Adapter));

    if (disableCurveClient) {
        copyerOptions->curveClient = nullptr;
    } else {
        copyerOptions->curveClient = std::make_shared<FileClient>();
    }

    if (disableS3Adapter) {
        copyerOptions->s3Client = nullptr;
    } else {
        copyerOptions->s3Client = std::make_shared<S3Adapter>();
    }
}

void ChunkServer::InitCloneOptions(
    common::Configuration *conf, CloneOptions *cloneOptions) {
    LOG_IF(FATAL, !conf->GetUInt32Value("clone.thread_num",
        &cloneOptions->threadNum));
    LOG_IF(FATAL, !conf->GetUInt32Value("clone.queue_depth",
        &cloneOptions->queueCapacity));
}

void ChunkServer::InitHeartbeatOptions(
    common::Configuration *conf, HeartbeatOptions *heartbeatOptions) {
    LOG_IF(FATAL, !conf->GetStringValue("chunkserver.stor_uri",
        &heartbeatOptions->storeUri));
    LOG_IF(FATAL, !conf->GetStringValue("global.ip", &heartbeatOptions->ip));
    LOG_IF(FATAL, !conf->GetUInt32Value("global.port",
        &heartbeatOptions->port));
    LOG_IF(FATAL, !conf->GetStringValue("mds.ip", &heartbeatOptions->mdsIp));
    LOG_IF(FATAL, !conf->GetUInt32Value("mds.port",
        &heartbeatOptions->mdsPort));
    LOG_IF(FATAL, !conf->GetUInt32Value("mds.heartbeat_interval",
        &heartbeatOptions->interval));
    LOG_IF(FATAL, !conf->GetUInt32Value("mds.heartbeat_timeout",
        &heartbeatOptions->timeout));
}

void ChunkServer::InitRegisterOptions(
    common::Configuration *conf, RegisterOptions *registerOptions) {
    LOG_IF(FATAL, !conf->GetStringValue("mds.ip", &registerOptions->mdsIp));
    LOG_IF(FATAL, !conf->GetIntValue("mds.port", &registerOptions->mdsPort));
    if (registerOptions->mdsPort <= 0 || registerOptions->mdsPort >= 65535) {
        LOG(FATAL) << "Invalid MDS port provided: " << registerOptions->mdsPort;
    }
    LOG_IF(FATAL, !conf->GetStringValue("global.ip",
        &registerOptions->chunkserverIp));
    LOG_IF(FATAL, !conf->GetIntValue("global.port",
        &registerOptions->chunkserverPort));
    LOG_IF(FATAL, !conf->GetStringValue("chunkserver.stor_uri",
        &registerOptions->chunserverStoreUri));
    LOG_IF(FATAL, !conf->GetStringValue("chunkserver.meta_uri",
        &registerOptions->chunkserverMetaUri));
    LOG_IF(FATAL, !conf->GetStringValue("chunkserver.disk_type",
        &registerOptions->chunkserverDiskType));
    LOG_IF(FATAL, !conf->GetIntValue("mds.register_retries",
        &registerOptions->registerRetries));
    LOG_IF(FATAL, !conf->GetIntValue("mds.register_timeout",
        &registerOptions->registerTimeout));
}

void ChunkServer::InitTrashOptions(
    common::Configuration *conf, TrashOptions *trashOptions) {
    LOG_IF(FATAL, !conf->GetStringValue(
        "copyset.recycler_uri", &trashOptions->trashPath));
    LOG_IF(FATAL, !conf->GetIntValue(
        "trash.expire_afterSec", &trashOptions->expiredAfterSec));
    LOG_IF(FATAL, !conf->GetIntValue(
        "trash.scan_periodSec", &trashOptions->scanPeriodSec));
}

void ChunkServer::InitMetricOptions(
    common::Configuration *conf, ChunkServerMetricOptions *metricOptions) {
    LOG_IF(FATAL, !conf->GetUInt32Value(
        "global.port", &metricOptions->port));
    LOG_IF(FATAL, !conf->GetStringValue(
        "global.ip", &metricOptions->ip));
    LOG_IF(FATAL, !conf->GetBoolValue(
        "metric.onoff", &metricOptions->collectMetric));
}

void ChunkServer::LoadConfigFromCmdline(common::Configuration *conf) {
    // 如果命令行有设置, 命令行覆盖配置文件中的字段
    google::CommandLineFlagInfo info;
    if (GetCommandLineFlagInfo("chunkServerIp", &info) && !info.is_default) {
        conf->SetStringValue("global.ip", FLAGS_chunkServerIp);
    }

    if (GetCommandLineFlagInfo("chunkServerPort", &info) && !info.is_default) {
        conf->SetIntValue("global.port", FLAGS_chunkServerPort);
    }

    if (GetCommandLineFlagInfo("chunkServerStoreUri", &info) &&
        !info.is_default) {
        conf->SetStringValue("chunkserver.stor_uri", FLAGS_chunkServerStoreUri);
    }

    if (GetCommandLineFlagInfo("chunkServerMetaUri", &info) &&
        !info.is_default) {
        conf->SetStringValue("chunkserver.meta_uri", FLAGS_chunkServerMetaUri);
    }

    if (GetCommandLineFlagInfo("copySetUri", &info) && !info.is_default) {
        conf->SetStringValue("copyset.chunk_data_uri", FLAGS_copySetUri);
        conf->SetStringValue("copyset.raft_log_uri", FLAGS_copySetUri);
        conf->SetStringValue("copyset.raft_snapshot_uri", FLAGS_copySetUri);
        conf->SetStringValue("copyset.raft_meta_uri", FLAGS_copySetUri);
    }

    if (GetCommandLineFlagInfo("recycleUri", &info) &&
        !info.is_default) {
        conf->SetStringValue("copyset.recycler_uri", FLAGS_recycleUri);
    }

    if (GetCommandLineFlagInfo("chunkFilePoolDir", &info) &&
        !info.is_default) {
        conf->SetStringValue(
            "chunkfilepool.chunk_file_pool_dir", FLAGS_chunkFilePoolDir);
    }

    if (GetCommandLineFlagInfo("chunkFilePoolMetaPath", &info) &&
        !info.is_default) {
        conf->SetStringValue(
            "chunkfilepool.meta_path", FLAGS_chunkFilePoolMetaPath);
    }
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

