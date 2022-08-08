/*
 *  Copyright (c) 2020 NetEase Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

/*
 * Project: curve
 * Created Date: Thur May 9th 2019
 * Author: lixiaocui
 */

#include <glog/logging.h>

#include <butil/endpoint.h>
#include <braft/builtin_service_impl.h>
#include <braft/raft_service.h>
#include <braft/storage.h>

#include <memory>

#include "src/chunkserver/chunkserver.h"
#include "src/chunkserver/chunkserver_metrics.h"
#include "src/chunkserver/chunkserver_service.h"
#include "src/chunkserver/copyset_service.h"
#include "src/chunkserver/chunk_service.h"
#include "src/chunkserver/braft_cli_service.h"
#include "src/chunkserver/braft_cli_service2.h"
#include "src/chunkserver/chunkserver_helper.h"
#include "src/common/uri_parser.h"
#include "src/chunkserver/raftsnapshot/curve_snapshot_attachment.h"
#include "src/chunkserver/raftsnapshot/curve_file_service.h"
#include "src/chunkserver/raftsnapshot/curve_snapshot_storage.h"
#include "src/chunkserver/raftlog/curve_segment_log_storage.h"
#include "src/common/curve_version.h"

using ::curve::fs::LocalFileSystem;
using ::curve::fs::LocalFileSystemOption;
using ::curve::fs::LocalFsFactory;
using ::curve::fs::FileSystemType;
using ::curve::chunkserver::concurrent::ConcurrentApplyModule;
using ::curve::common::UriParser;

DEFINE_string(conf, "ChunkServer.conf", "Path of configuration file");
DEFINE_string(chunkServerIp, "127.0.0.1", "chunkserver ip");
DEFINE_bool(enableExternalServer, false, "start external server or not");
DEFINE_string(chunkServerExternalIp, "127.0.0.1", "chunkserver external ip");
DEFINE_int32(chunkServerPort, 8200, "chunkserver port");
DEFINE_string(chunkServerStoreUri, "local://./0/", "chunkserver store uri");
DEFINE_string(chunkServerMetaUri,
    "local://./0/chunkserver.dat", "chunnkserver meata uri");
DEFINE_string(copySetUri, "local://./0/copysets", "copyset data uri");
DEFINE_string(raftSnapshotUri, "curve://./0/copysets", "raft snapshot uri");
DEFINE_string(raftLogUri, "curve://./0/copysets", "raft log uri");
DEFINE_string(recycleUri, "local://./0/recycler" , "recycle uri");
DEFINE_string(chunkFilePoolDir, "./0/", "chunk file pool location");
DEFINE_string(chunkFilePoolMetaPath,
    "./chunkfilepool.meta", "chunk file pool meta path");
DEFINE_string(logPath, "./0/chunkserver.log-", "log file path");
DEFINE_string(mdsListenAddr, "127.0.0.1:6666", "mds listen addr");
DEFINE_bool(enableChunkfilepool, true, "enable chunkfilepool");
DEFINE_uint32(copysetLoadConcurrency, 5, "copyset load concurrency");
DEFINE_bool(enableWalfilepool, true, "enable WAL filepool");
DEFINE_string(walFilePoolDir, "./0/", "WAL filepool location");
DEFINE_string(walFilePoolMetaPath, "./walfilepool.meta",
                                    "WAL filepool meta path");

const char* kProtocalCurve = "curve";

namespace curve {
namespace chunkserver {

int ChunkServer::Run(int argc, char** argv) {
    gflags::ParseCommandLineFlags(&argc, &argv, true);

    RegisterCurveSegmentLogStorageOrDie();

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

    // 初始化日志模块
    google::InitGoogleLogging(argv[0]);

    // 打印参数
    conf.PrintConfig();
    curve::common::ExposeCurveVersion();

    // ============================初始化各模块==========================//
    LOG(INFO) << "Initializing ChunkServer modules";

    LOG_IF(FATAL, !conf.GetUInt32Value("global.min_io_alignment",
                                       &FLAGS_minIoAlignment))
        << "Failed to get global.min_io_alignment";
    LOG_IF(FATAL, !common::is_aligned(FLAGS_minIoAlignment, 512))
        << "minIoAlignment should align to 512";

    // 优先初始化 metric 收集模块
    ChunkServerMetricOptions metricOptions;
    InitMetricOptions(&conf, &metricOptions);
    ChunkServerMetric* metric = ChunkServerMetric::GetInstance();
    LOG_IF(FATAL, metric->Init(metricOptions) != 0)
        << "Failed to init chunkserver metric.";

    // 初始化并发持久模块
    ConcurrentApplyModule concurrentapply;
    ConcurrentApplyOption concurrentApplyOptions;
    InitConcurrentApplyOptions(&conf, &concurrentApplyOptions);
    LOG_IF(FATAL, false == concurrentapply.Init(concurrentApplyOptions))
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
    FilePoolOptions chunkFilePoolOptions;
    InitChunkFilePoolOptions(&conf, &chunkFilePoolOptions);
    std::shared_ptr<FilePool> chunkfilePool =
            std::make_shared<FilePool>(fs);
    LOG_IF(FATAL, false == chunkfilePool->Initialize(chunkFilePoolOptions))
        << "Failed to init chunk file pool";

    // Init Wal file pool
    std::string raftLogUri;
    LOG_IF(FATAL, !conf.GetStringValue("copyset.raft_log_uri", &raftLogUri));
    std::string raftLogProtocol = UriParser::GetProtocolFromUri(raftLogUri);
    std::shared_ptr<FilePool> walFilePool = nullptr;
    bool useChunkFilePoolAsWalPool = true;
    if (raftLogProtocol == kProtocalCurve) {
        LOG_IF(FATAL, !conf.GetBoolValue(
            "walfilepool.use_chunk_file_pool",
            &useChunkFilePoolAsWalPool));

        if (!useChunkFilePoolAsWalPool) {
            FilePoolOptions walFilePoolOptions;
            InitWalFilePoolOptions(&conf, &walFilePoolOptions);
            walFilePool = std::make_shared<FilePool>(fs);
            LOG_IF(FATAL, false == walFilePool->Initialize(walFilePoolOptions))
                << "Failed to init wal file pool";
            LOG(INFO) << "initialize walpool success.";
        } else {
            walFilePool = chunkfilePool;
            LOG(INFO) << "initialize to use chunkfilePool as walpool success.";
        }
    }

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
    bool enablePaste = false;
    LOG_IF(FATAL, !conf.GetBoolValue("clone.enable_paste", &enablePaste));
    cloneOptions.core =
        std::make_shared<CloneCore>(sliceSize, enablePaste, copyer);
    LOG_IF(FATAL, cloneManager_.Init(cloneOptions) != 0)
        << "Failed to initialize clone manager.";

    // 初始化注册模块
    RegisterOptions registerOptions;
    InitRegisterOptions(&conf, &registerOptions);
    registerOptions.fs = fs;
    Register registerMDS(registerOptions);
    ChunkServerMetadata metadata;
    ChunkServerMetadata localMetadata;
    // 从本地获取meta
    std::string metaPath = UriParser::GetPathFromUri(
        registerOptions.chunkserverMetaUri).c_str();

    auto epochMap = std::make_shared<EpochMap>();
    if (fs->FileExists(metaPath)) {
        LOG_IF(FATAL, GetChunkServerMetaFromLocal(
                            registerOptions.chunserverStoreUri,
                            registerOptions.chunkserverMetaUri,
                            registerOptions.fs, &localMetadata) != 0)
            << "Failed to GetChunkServerMetaFromLocal.";
        LOG_IF(FATAL, registerMDS.RegisterToMDS(
            &localMetadata, &metadata, epochMap) != 0)
            << "Failed to register to MDS.";
    } else {
        // 如果本地获取不到，向mds注册
        LOG(INFO) << "meta file "
                  << metaPath << " do not exist, register to mds";
        LOG_IF(FATAL, registerMDS.RegisterToMDS(
            nullptr, &metadata, epochMap) != 0)
            << "Failed to register to MDS.";
    }

    // trash模块初始化
    TrashOptions trashOptions;
    InitTrashOptions(&conf, &trashOptions);
    trashOptions.localFileSystem = fs;
    trashOptions.chunkFilePool = chunkfilePool;
    trashOptions.walPool = walFilePool;
    trash_ = std::make_shared<Trash>();
    LOG_IF(FATAL, trash_->Init(trashOptions) != 0)
        << "Failed to init Trash";

    // 初始化复制组管理模块
    CopysetNodeOptions copysetNodeOptions;
    InitCopysetNodeOptions(&conf, &copysetNodeOptions);
    copysetNodeOptions.concurrentapply = &concurrentapply;
    copysetNodeOptions.chunkFilePool = chunkfilePool;
    copysetNodeOptions.walFilePool = walFilePool;
    copysetNodeOptions.localFileSystem = fs;
    copysetNodeOptions.trash = trash_;
    if (nullptr != walFilePool) {
        FilePoolOptions poolOpt = walFilePool->GetFilePoolOpt();
        uint32_t maxWalSegmentSize = poolOpt.fileSize + poolOpt.metaPageSize;
        copysetNodeOptions.maxWalSegmentSize = maxWalSegmentSize;
    }

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
    // 注册curve snapshot storage
    RegisterCurveSnapshotStorageOrDie();
    CurveSnapshotStorage::set_server_addr(endPoint);
    copysetNodeManager_ = &CopysetNodeManager::GetInstance();
    LOG_IF(FATAL, copysetNodeManager_->Init(copysetNodeOptions) != 0)
        << "Failed to initialize CopysetNodeManager.";

    // init scan model
    ScanManagerOptions scanOpts;
    InitScanOptions(&conf, &scanOpts);
    scanOpts.copysetNodeManager = copysetNodeManager_;
    LOG_IF(FATAL, scanManager_.Init(scanOpts) != 0)
        << "Failed to init scan manager.";

    // 心跳模块初始化
    HeartbeatOptions heartbeatOptions;
    InitHeartbeatOptions(&conf, &heartbeatOptions);
    heartbeatOptions.copysetNodeManager = copysetNodeManager_;
    heartbeatOptions.fs = fs;
    heartbeatOptions.chunkserverId = metadata.id();
    heartbeatOptions.chunkserverToken = metadata.token();
    heartbeatOptions.scanManager = &scanManager_;
    LOG_IF(FATAL, heartbeat_.Init(heartbeatOptions) != 0)
        << "Failed to init Heartbeat manager.";

    // 监控部分模块的metric指标
    metric->MonitorTrash(trash_.get());
    metric->MonitorChunkFilePool(chunkfilePool.get());
    if (raftLogProtocol == kProtocalCurve && !useChunkFilePoolAsWalPool) {
        metric->MonitorWalFilePool(walFilePool.get());
    }
    metric->ExposeConfigMetric(&conf);

    // ========================添加rpc服务===============================//
    // TODO(lixiaocui): rpc中各接口添加上延迟metric
    brpc::Server server;
    brpc::Server externalServer;
    // We need call braft::add_service to add endPoint to braft::NodeManager
    braft::add_service(&server, endPoint);

    // copyset service
    CopysetServiceImpl copysetService(copysetNodeManager_);
    int ret = server.AddService(&copysetService,
                        brpc::SERVER_DOESNT_OWN_SERVICE);
    CHECK(0 == ret) << "Fail to add CopysetService";

    // inflight throttle
    int maxInflight;
    LOG_IF(FATAL,
           !conf.GetIntValue("chunkserver.max_inflight_requests",
                             &maxInflight));
    std::shared_ptr<InflightThrottle> inflightThrottle
        = std::make_shared<InflightThrottle>(maxInflight);
    CHECK(nullptr != inflightThrottle) << "new inflight throttle failed";

    // chunk service
    ChunkServiceOptions chunkServiceOptions;
    chunkServiceOptions.copysetNodeManager = copysetNodeManager_;
    chunkServiceOptions.cloneManager = &cloneManager_;
    chunkServiceOptions.inflightThrottle = inflightThrottle;

    ChunkServiceImpl chunkService(chunkServiceOptions, epochMap);
    ret = server.AddService(&chunkService,
                        brpc::SERVER_DOESNT_OWN_SERVICE);
    CHECK(0 == ret) << "Fail to add ChunkService";

    // We need to replace braft::CliService with our own implementation
    auto service = server.FindServiceByName("CliService");
    ret = server.RemoveService(service);
    CHECK(0 == ret) << "Fail to remove braft::CliService";
    BRaftCliServiceImpl braftCliService;
    ret = server.AddService(&braftCliService,
                        brpc::SERVER_DOESNT_OWN_SERVICE);
    CHECK(0 == ret) << "Fail to add BRaftCliService";

    // braftclient service
    BRaftCliServiceImpl2 braftCliService2;
    ret = server.AddService(&braftCliService2,
                        brpc::SERVER_DOESNT_OWN_SERVICE);
    CHECK(0 == ret) << "Fail to add BRaftCliService2";

    // We need to replace braft::FileServiceImpl with our own implementation
    service = server.FindServiceByName("FileService");
    ret = server.RemoveService(service);
    CHECK(0 == ret) << "Fail to remove braft::FileService";
    kCurveFileService.set_snapshot_attachment(new CurveSnapshotAttachment(fs));
    ret = server.AddService(&kCurveFileService,
        brpc::SERVER_DOESNT_OWN_SERVICE);
    CHECK(0 == ret) << "Fail to add CurveFileService";

    // chunkserver service
    ChunkServerServiceImpl chunkserverService(copysetNodeManager_);
    ret = server.AddService(&chunkserverService,
        brpc::SERVER_DOESNT_OWN_SERVICE);
    CHECK(0 == ret) << "Fail to add ChunkServerService";

    // scan copyset service
    ScanServiceImpl scanCopysetService(&scanManager_);
    ret = server.AddService(&scanCopysetService,
        brpc::SERVER_DOESNT_OWN_SERVICE);
    CHECK(0 == ret) << "Fail to add ScanCopysetService";

    // 启动rpc service
    LOG(INFO) << "Internal server is going to serve on: "
              << copysetNodeOptions.ip << ":" << copysetNodeOptions.port;
    if (server.Start(endPoint, NULL) != 0) {
        LOG(ERROR) << "Fail to start Internal Server";
        return -1;
    }
    /* 启动external server
       external server用于向client和工具等外部提供服务
       区别于mds和chunkserver之间的通信*/
    if (registerOptions.enableExternalServer) {
        ret = externalServer.AddService(&copysetService,
                        brpc::SERVER_DOESNT_OWN_SERVICE);
        CHECK(0 == ret) << "Fail to add CopysetService at external server";
        ret = externalServer.AddService(&chunkService,
                        brpc::SERVER_DOESNT_OWN_SERVICE);
        CHECK(0 == ret) << "Fail to add ChunkService at external server";
        ret = externalServer.AddService(&braftCliService,
                        brpc::SERVER_DOESNT_OWN_SERVICE);
        CHECK(0 == ret) << "Fail to add BRaftCliService at external server";
        ret = externalServer.AddService(&braftCliService2,
                        brpc::SERVER_DOESNT_OWN_SERVICE);
        CHECK(0 == ret) << "Fail to add BRaftCliService2 at external server";
        braft::RaftStatImpl raftStatService;
        ret = externalServer.AddService(&raftStatService,
                        brpc::SERVER_DOESNT_OWN_SERVICE);
        CHECK(0 == ret) << "Fail to add RaftStatService at external server";
        std::string externalAddr = registerOptions.chunkserverExternalIp + ":" +
                                std::to_string(registerOptions.chunkserverPort);
        LOG(INFO) << "External server is going to serve on: " << externalAddr;
        if (externalServer.Start(externalAddr.c_str(), NULL) != 0) {
            LOG(ERROR) << "Fail to start External Server";
            return -1;
        }
    }

    // =======================启动各模块==================================//
    LOG(INFO) << "ChunkServer starts.";
    /**
     * 将模块启动放到rpc 服务启动后面，主要是为了解决内存增长的问题
     * 控制并发恢复的copyset数量，copyset恢复需要依赖rpc服务先启动
     */
    LOG_IF(FATAL, trash_->Run() != 0)
        << "Failed to start trash.";
    LOG_IF(FATAL, cloneManager_.Run() != 0)
        << "Failed to start clone manager.";
    LOG_IF(FATAL, heartbeat_.Run() != 0)
        << "Failed to start heartbeat manager.";
    LOG_IF(FATAL, copysetNodeManager_->Run() != 0)
        << "Failed to start CopysetNodeManager.";
    LOG_IF(FATAL, scanManager_.Run() != 0)
        << "Failed to start scan manager.";
    LOG_IF(FATAL, !chunkfilePool->StartCleaning())
        << "Failed to start file pool clean worker.";

    // =======================等待进程退出==================================//
    while (!brpc::IsAskedToQuit()) {
        bthread_usleep(1000000L);
    }
    // scanmanager stop maybe need a little while, so stop it first before stop service  NOLINT
    LOG(INFO) << "ChunkServer is going to quit.";
    LOG_IF(ERROR, scanManager_.Fini() != 0)
        << "Failed to shutdown scan manager.";

    if (registerOptions.enableExternalServer) {
        externalServer.Stop(0);
        externalServer.Join();
    }

    server.Stop(0);
    server.Join();

    LOG_IF(ERROR, heartbeat_.Fini() != 0)
        << "Failed to shutdown heartbeat manager.";
    LOG_IF(ERROR, copysetNodeManager_->Fini() != 0)
        << "Failed to shutdown CopysetNodeManager.";
    LOG_IF(ERROR, cloneManager_.Fini() != 0)
        << "Failed to shutdown clone manager.";
    LOG_IF(ERROR, copyer->Fini() != 0)
        << "Failed to shutdown clone copyer.";
    LOG_IF(ERROR, trash_->Fini() != 0)
        << "Failed to shutdown trash.";
    LOG_IF(ERROR, !chunkfilePool->StopCleaning())
        << "Failed to shutdown file pool clean worker.";
    concurrentapply.Stop();

    google::ShutdownGoogleLogging();
    return 0;
}

void ChunkServer::Stop() {
    brpc::AskToQuit();
}



void ChunkServer::InitChunkFilePoolOptions(
    common::Configuration *conf, FilePoolOptions *chunkFilePoolOptions) {
    LOG_IF(FATAL, !conf->GetUInt32Value("global.chunk_size",
        &chunkFilePoolOptions->fileSize));
    LOG_IF(FATAL, !conf->GetUInt32Value("global.meta_page_size",
        &chunkFilePoolOptions->metaPageSize));
    LOG_IF(FATAL, !conf->GetUInt32Value("chunkfilepool.cpmeta_file_size",
        &chunkFilePoolOptions->metaFileSize));
    LOG_IF(FATAL, !conf->GetBoolValue(
        "chunkfilepool.enable_get_chunk_from_pool",
        &chunkFilePoolOptions->getFileFromPool));

    if (chunkFilePoolOptions->getFileFromPool == false) {
        std::string chunkFilePoolUri;
        LOG_IF(FATAL, !conf->GetStringValue(
            "chunkfilepool.chunk_file_pool_dir", &chunkFilePoolUri));
        ::memcpy(chunkFilePoolOptions->filePoolDir,
                 chunkFilePoolUri.c_str(),
                 chunkFilePoolUri.size());
    } else {
        std::string metaUri;
        LOG_IF(FATAL, !conf->GetStringValue(
            "chunkfilepool.meta_path", &metaUri));
        ::memcpy(
            chunkFilePoolOptions->metaPath, metaUri.c_str(), metaUri.size());
        LOG_IF(FATAL, !conf->GetBoolValue("chunkfilepool.clean.enable",
            &chunkFilePoolOptions->needClean));
        LOG_IF(FATAL, !conf->GetUInt32Value("chunkfilepool.clean.bytes_per_write",  // NOLINT
            &chunkFilePoolOptions->bytesPerWrite));
        LOG_IF(FATAL, !conf->GetUInt32Value("chunkfilepool.clean.throttle_iops",
            &chunkFilePoolOptions->iops4clean));

        if (0 == chunkFilePoolOptions->bytesPerWrite
            || chunkFilePoolOptions->bytesPerWrite > 1 * 1024 * 1024
            || 0 != chunkFilePoolOptions->bytesPerWrite % 4096) {
            LOG(FATAL) << "The bytesPerWrite must be in [1, 1048576] "
                       << "and should be aligned to 4K, "
                       << "but now is: " << chunkFilePoolOptions->bytesPerWrite;
        }
    }
}

void ChunkServer::InitConcurrentApplyOptions(common::Configuration *conf,
        ConcurrentApplyOption *concurrentApplyOptions) {
    LOG_IF(FATAL, !conf->GetIntValue(
        "rconcurrentapply.size", &concurrentApplyOptions->rconcurrentsize));
    LOG_IF(FATAL, !conf->GetIntValue(
        "wconcurrentapply.size", &concurrentApplyOptions->wconcurrentsize));
    LOG_IF(FATAL, !conf->GetIntValue(
        "rconcurrentapply.queuedepth", &concurrentApplyOptions->rqueuedepth));
    LOG_IF(FATAL, !conf->GetIntValue(
        "wconcurrentapply.queuedepth", &concurrentApplyOptions->wqueuedepth));
}

void ChunkServer::InitWalFilePoolOptions(
    common::Configuration *conf, FilePoolOptions *walPoolOptions) {
    LOG_IF(FATAL, !conf->GetUInt32Value("walfilepool.segment_size",
        &walPoolOptions->fileSize));
    LOG_IF(FATAL, !conf->GetUInt32Value("walfilepool.metapage_size",
        &walPoolOptions->metaPageSize));
    LOG_IF(FATAL, !conf->GetUInt32Value("walfilepool.meta_file_size",
        &walPoolOptions->metaFileSize));
    LOG_IF(FATAL, !conf->GetBoolValue(
        "walfilepool.enable_get_segment_from_pool",
        &walPoolOptions->getFileFromPool));

    if (walPoolOptions->getFileFromPool == false) {
        std::string filePoolUri;
        LOG_IF(FATAL, !conf->GetStringValue(
            "walfilepool.file_pool_dir", &filePoolUri));
        ::memcpy(walPoolOptions->filePoolDir,
                 filePoolUri.c_str(),
                 filePoolUri.size());
    } else {
        std::string metaUri;
        LOG_IF(FATAL, !conf->GetStringValue(
            "walfilepool.meta_path", &metaUri));
        ::memcpy(
            walPoolOptions->metaPath, metaUri.c_str(), metaUri.size());
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

    LOG_IF(FATAL, !conf->GetIntValue("copyset.election_timeout_ms",
        &copysetNodeOptions->electionTimeoutMs));
    LOG_IF(FATAL, !conf->GetIntValue("copyset.snapshot_interval_s",
        &copysetNodeOptions->snapshotIntervalS));
    LOG_IF(FATAL, !conf->GetIntValue("copyset.catchup_margin",
        &copysetNodeOptions->catchupMargin));
    LOG_IF(FATAL, !conf->GetStringValue("copyset.chunk_data_uri",
        &copysetNodeOptions->chunkDataUri));
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
    LOG_IF(FATAL, !conf->GetUInt32Value("global.location_limit",
        &copysetNodeOptions->locationLimit));
    LOG_IF(FATAL, !conf->GetUInt32Value("global.meta_page_size",
        &copysetNodeOptions->pageSize));
    LOG_IF(FATAL, !conf->GetUInt32Value("copyset.load_concurrency",
        &copysetNodeOptions->loadConcurrency));
    LOG_IF(FATAL, !conf->GetUInt32Value("copyset.check_retrytimes",
        &copysetNodeOptions->checkRetryTimes));
    LOG_IF(FATAL, !conf->GetUInt32Value("copyset.finishload_margin",
        &copysetNodeOptions->finishLoadMargin));
    LOG_IF(FATAL, !conf->GetUInt32Value("copyset.check_loadmargin_interval_ms",
        &copysetNodeOptions->checkLoadMarginIntervalMs));

    LOG_IF(FATAL, !conf->GetBoolValue(
        "copyset.enable_odsync_when_open_chunkfile",
        &copysetNodeOptions->enableOdsyncWhenOpenChunkFile));
    if (!copysetNodeOptions->enableOdsyncWhenOpenChunkFile) {
        LOG_IF(FATAL, !conf->GetUInt32Value("copyset.synctimer_interval_ms",
            &copysetNodeOptions->syncTimerIntervalMs));
        LOG_IF(FATAL, !conf->GetUInt32Value("copyset.check_syncing_interval_ms",
            &copysetNodeOptions->checkSyncingIntervalMs));
    }
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
    LOG_IF(FATAL, !conf->GetUInt64Value("curve.curve_file_timeout_s",
        &copyerOptions->curveFileTimeoutSec));

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

void ChunkServer::InitScanOptions(
    common::Configuration *conf, ScanManagerOptions *scanOptions) {
    LOG_IF(FATAL, !conf->GetUInt32Value("copyset.scan_interval_sec",
        &scanOptions->intervalSec));
    LOG_IF(FATAL, !conf->GetUInt64Value("copyset.scan_size_byte",
        &scanOptions->scanSize));
    LOG_IF(FATAL, !conf->GetUInt32Value("global.meta_page_size",
        &scanOptions->chunkMetaPageSize));
    LOG_IF(FATAL, !conf->GetUInt64Value("copyset.scan_rpc_timeout_ms",
        &scanOptions->timeoutMs));
    LOG_IF(FATAL, !conf->GetUInt32Value("copyset.scan_rpc_retry_times",
        &scanOptions->retry));
    LOG_IF(FATAL, !conf->GetUInt64Value("copyset.scan_rpc_retry_interval_us",
        &scanOptions->retryIntervalUs));
}

void ChunkServer::InitHeartbeatOptions(
    common::Configuration *conf, HeartbeatOptions *heartbeatOptions) {
    LOG_IF(FATAL, !conf->GetStringValue("chunkserver.stor_uri",
        &heartbeatOptions->storeUri));
    LOG_IF(FATAL, !conf->GetStringValue("global.ip", &heartbeatOptions->ip));
    LOG_IF(FATAL, !conf->GetUInt32Value("global.port",
        &heartbeatOptions->port));
    LOG_IF(FATAL, !conf->GetStringValue("mds.listen.addr",
        &heartbeatOptions->mdsListenAddr));
    LOG_IF(FATAL, !conf->GetUInt32Value("mds.heartbeat_interval",
        &heartbeatOptions->intervalSec));
    LOG_IF(FATAL, !conf->GetUInt32Value("mds.heartbeat_timeout",
        &heartbeatOptions->timeout));
}

void ChunkServer::InitRegisterOptions(
    common::Configuration *conf, RegisterOptions *registerOptions) {
    LOG_IF(FATAL, !conf->GetStringValue("mds.listen.addr",
        &registerOptions->mdsListenAddr));
    LOG_IF(FATAL, !conf->GetStringValue("global.ip",
        &registerOptions->chunkserverInternalIp));
    LOG_IF(FATAL, !conf->GetBoolValue("global.enable_external_server",
        &registerOptions->enableExternalServer));
    LOG_IF(FATAL, !conf->GetStringValue("global.external_ip",
        &registerOptions->chunkserverExternalIp));
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
    } else {
        LOG(FATAL)
        << "chunkServerIp must be set when run chunkserver in command.";
    }
    if (GetCommandLineFlagInfo("enableExternalServer", &info) &&
                                                            !info.is_default) {
        conf->SetBoolValue(
            "global.enable_external_server", FLAGS_enableExternalServer);
    }
    if (GetCommandLineFlagInfo("chunkServerExternalIp", &info) &&
                                                            !info.is_default) {
        conf->SetStringValue("global.external_ip", FLAGS_chunkServerExternalIp);
    }

    if (GetCommandLineFlagInfo("chunkServerPort", &info) && !info.is_default) {
        conf->SetIntValue("global.port", FLAGS_chunkServerPort);
    } else {
        LOG(FATAL)
        << "chunkServerPort must be set when run chunkserver in command.";
    }

    if (GetCommandLineFlagInfo("chunkServerStoreUri", &info) &&
        !info.is_default) {
        conf->SetStringValue("chunkserver.stor_uri", FLAGS_chunkServerStoreUri);
    } else {
        LOG(FATAL)
        << "chunkServerStoreUri must be set when run chunkserver in command.";
    }

    if (GetCommandLineFlagInfo("chunkServerMetaUri", &info) &&
        !info.is_default) {
        conf->SetStringValue("chunkserver.meta_uri", FLAGS_chunkServerMetaUri);
    } else {
        LOG(FATAL)
        << "chunkServerMetaUri must be set when run chunkserver in command.";
    }

    if (GetCommandLineFlagInfo("copySetUri", &info) && !info.is_default) {
        conf->SetStringValue("copyset.chunk_data_uri", FLAGS_copySetUri);
        conf->SetStringValue("copyset.raft_log_uri", FLAGS_copySetUri);
        conf->SetStringValue("copyset.raft_snapshot_uri", FLAGS_copySetUri);
        conf->SetStringValue("copyset.raft_meta_uri", FLAGS_copySetUri);
    } else {
        LOG(FATAL)
        << "copySetUri must be set when run chunkserver in command.";
    }
    if (GetCommandLineFlagInfo("raftSnapshotUri", &info) && !info.is_default) {
        conf->SetStringValue(
                            "copyset.raft_snapshot_uri", FLAGS_raftSnapshotUri);
    } else {
        LOG(FATAL)
        << "raftSnapshotUri must be set when run chunkserver in command.";
    }
    if (GetCommandLineFlagInfo("raftLogUri", &info) && !info.is_default) {
        conf->SetStringValue(
                            "copyset.raft_log_uri", FLAGS_raftLogUri);
    } else {
        LOG(FATAL)
        << "raftLogUri must be set when run chunkserver in command.";
    }

    if (GetCommandLineFlagInfo("recycleUri", &info) &&
        !info.is_default) {
        conf->SetStringValue("copyset.recycler_uri", FLAGS_recycleUri);
    } else {
        LOG(FATAL)
        << "recycleUri must be set when run chunkserver in command.";
    }

    if (GetCommandLineFlagInfo("chunkFilePoolDir", &info) &&
        !info.is_default) {
        conf->SetStringValue(
            "chunkfilepool.chunk_file_pool_dir", FLAGS_chunkFilePoolDir);
    } else {
        LOG(FATAL)
        << "chunkFilePoolDir must be set when run chunkserver in command.";
    }

    if (GetCommandLineFlagInfo("chunkFilePoolMetaPath", &info) &&
        !info.is_default) {
        conf->SetStringValue(
            "chunkfilepool.meta_path", FLAGS_chunkFilePoolMetaPath);
    } else {
        LOG(FATAL)
        << "chunkFilePoolMetaPath must be set when run chunkserver in command.";
    }

    if (GetCommandLineFlagInfo("walFilePoolDir", &info) &&
        !info.is_default) {
        conf->SetStringValue(
            "walfilepool.file_pool_dir", FLAGS_walFilePoolDir);
    } else {
        LOG(FATAL)
        << "walFilePoolDir must be set when run chunkserver in command.";
    }

    if (GetCommandLineFlagInfo("walFilePoolMetaPath", &info) &&
        !info.is_default) {
        conf->SetStringValue(
            "walfilepool.meta_path", FLAGS_walFilePoolMetaPath);
    } else {
        LOG(FATAL)
        << "walFilePoolMetaPath must be set when run chunkserver in command.";
    }

    if (GetCommandLineFlagInfo("mdsListenAddr", &info) && !info.is_default) {
        conf->SetStringValue("mds.listen.addr", FLAGS_mdsListenAddr);
    }

    // 设置日志存放文件夹
    if (FLAGS_log_dir.empty()) {
        if (!conf->GetStringValue("chunkserver.common.logDir", &FLAGS_log_dir)) {  // NOLINT
            LOG(WARNING) << "no chunkserver.common.logDir in " << FLAGS_conf
                         << ", will log to /tmp";
        }
    }

    if (GetCommandLineFlagInfo("enableChunkfilepool", &info) &&
        !info.is_default) {
        conf->SetBoolValue("chunkfilepool.enable_get_chunk_from_pool",
            FLAGS_enableChunkfilepool);
    }

    if (GetCommandLineFlagInfo("enableWalfilepool", &info) &&
        !info.is_default) {
        conf->SetBoolValue("walfilepool.enable_get_segment_from_pool",
            FLAGS_enableWalfilepool);
    }

    if (GetCommandLineFlagInfo("copysetLoadConcurrency", &info) &&
        !info.is_default) {
        conf->SetIntValue("copyset.load_concurrency",
            FLAGS_copysetLoadConcurrency);
    }

    if (GetCommandLineFlagInfo("minIoAlignment", &info) && !info.is_default) {
        conf->SetUInt32Value("global.min_io_alignment", FLAGS_minIoAlignment);
    }
}

int ChunkServer::GetChunkServerMetaFromLocal(
    const std::string &storeUri,
    const std::string &metaUri,
    const std::shared_ptr<LocalFileSystem> &fs,
    ChunkServerMetadata *metadata) {
    std::string proto = UriParser::GetProtocolFromUri(storeUri);
    if (proto != "local") {
        LOG(ERROR) << "Datastore protocal " << proto << " is not supported yet";
        return -1;
    }
    // 从配置文件中获取chunkserver元数据的文件路径
    proto = UriParser::GetProtocolFromUri(metaUri);
    if (proto != "local") {
        LOG(ERROR) << "Chunkserver meta protocal "
                   << proto << " is not supported yet";
        return -1;
    }
    // 元数据文件已经存在
    if (fs->FileExists(UriParser::GetPathFromUri(metaUri).c_str())) {
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
    std::string metaFile = UriParser::GetPathFromUri(metaUri);

    fd = fs->Open(metaFile.c_str(), O_RDONLY);
    if (fd < 0) {
        LOG(ERROR) << "Failed to open Chunkserver metadata file " << metaFile;
        return -1;
    }

    #define METAFILE_MAX_SIZE  4096
    int size;
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
