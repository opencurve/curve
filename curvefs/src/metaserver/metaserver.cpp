/*
 *  Copyright (c) 2021 NetEase Inc.
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
 * Created Date: 2021-05-19
 * Author: chenwei
 */

#include "curvefs/src/metaserver/metaserver.h"

#include <brpc/channel.h>
#include <brpc/server.h>
#include <fcntl.h>
#include <gflags/gflags.h>
#include <glog/logging.h>
#include <braft/builtin_service_impl.h>
#include <unistd.h>
#include "absl/memory/memory.h"
#include "curvefs/src/metaserver/copyset/copyset_service.h"
#include "curvefs/src/metaserver/metaserver_service.h"
#include "curvefs/src/metaserver/register.h"
#include "curvefs/src/metaserver/s3compact_manager.h"
#include "curvefs/src/metaserver/trash_manager.h"
#include "curvefs/src/metaserver/storage/storage.h"
#include "src/common/crc32.h"
#include "src/common/curve_version.h"
#include "src/common/s3_adapter.h"
#include "src/common/string_util.h"

namespace braft {

DECLARE_bool(raft_sync);
DECLARE_bool(raft_sync_meta);
DECLARE_bool(raft_sync_segments);
DECLARE_bool(raft_use_fsync_rather_than_fdatasync);
DECLARE_int32(raft_max_install_snapshot_tasks_num);

}  // namespace braft

namespace brpc {
DECLARE_bool(graceful_quit_on_sigterm);
}  // namespace brpc

namespace curvefs {
namespace metaserver {

using ::curve::fs::FileSystemType;
using ::curve::fs::LocalFsFactory;
using ::curve::fs::LocalFileSystemOption;

using ::curvefs::metaserver::copyset::ApplyQueueOption;
using ::curvefs::metaserver::storage::GetStorageInstance;

void Metaserver::InitOptions(std::shared_ptr<Configuration> conf) {
    conf_ = conf;
    conf_->GetValueFatalIfFail("global.ip", &options_.ip);
    conf_->GetValueFatalIfFail("global.port", &options_.port);
    conf_->GetValueFatalIfFail("global.external_ip", &options_.externalIp);
    conf_->GetValueFatalIfFail("global.external_port", &options_.externalPort);
    conf_->GetBoolValue("global.enable_external_server",
        &options_.enableExternalServer);

    LOG(INFO) << "Init metaserver option, options_.ip = " << options_.ip
              << ", options_.port = " << options_.port
              << ", options_.externalIp = " << options_.externalIp
              << ", options_.externalPort = " << options_.externalPort
              << ", options_.enableExternalServer = "
              << options_.enableExternalServer;

    std::string value;
    conf_->GetValueFatalIfFail("bthread.worker_count", &value);
    if (value == "auto") {
        options_.bthreadWorkerCount = -1;
    } else if (!curve::common::StringToInt(value,
                                           &options_.bthreadWorkerCount)) {
        LOG(WARNING)
            << "Parse bthread.worker_count to int failed, string value: "
            << value;
    }

    InitBRaftFlags(conf);
}

void Metaserver::InitRegisterOptions() {
    conf_->GetValueFatalIfFail("mds.listen.addr",
                               &registerOptions_.mdsListenAddr);
    conf_->GetValueFatalIfFail("global.ip",
                               &registerOptions_.metaserverInternalIp);
    conf_->GetValueFatalIfFail("global.external_ip",
                               &registerOptions_.metaserverExternalIp);
    conf_->GetValueFatalIfFail("global.port",
                               &registerOptions_.metaserverInternalPort);
    conf_->GetValueFatalIfFail("global.external_port",
                               &registerOptions_.metaserverExternalPort);
    conf_->GetValueFatalIfFail("mds.register_retries",
                               &registerOptions_.registerRetries);
    conf_->GetValueFatalIfFail("mds.register_timeoutMs",
                               &registerOptions_.registerTimeout);
}

void Metaserver::InitLocalFileSystem() {
    LocalFileSystemOption option;

    localFileSystem_ = LocalFsFactory::CreateFs(FileSystemType::EXT4, "");
    LOG_IF(FATAL, 0 != localFileSystem_->Init(option))
        << "Failed to initialize local filesystem";
}

void InitS3Option(const std::shared_ptr<Configuration>& conf,
                  S3ClientAdaptorOption* s3Opt) {
    LOG_IF(FATAL, !conf->GetUInt64Value("s3.batchsize", &s3Opt->batchSize));
    LOG_IF(FATAL, !conf->GetBoolValue("s3.enableDeleteObjects",
                                      &s3Opt->enableDeleteObjects));
}

void Metaserver::InitPartitionOption(std::shared_ptr<S3ClientAdaptor> s3Adaptor,
                              std::shared_ptr<MdsClient> mdsClient,
                         PartitionCleanOption* partitionCleanOption) {
    LOG_IF(FATAL, !conf_->GetUInt32Value("partition.clean.scanPeriodSec",
                                         &partitionCleanOption->scanPeriodSec));
    LOG_IF(FATAL,
           !conf_->GetUInt32Value("partition.clean.inodeDeletePeriodMs",
                                  &partitionCleanOption->inodeDeletePeriodMs));
    partitionCleanOption->s3Adaptor = s3Adaptor;
    partitionCleanOption->mdsClient = mdsClient;
}

void Metaserver::Init() {
    TrashOption trashOption;
    trashOption.InitTrashOptionFromConf(conf_);

    // init mds client
    mdsBase_ = new MDSBaseClient();
    ::curvefs::client::common::InitMdsOption(conf_.get(), &mdsOptions_);
    mdsClient_ = std::make_shared<MdsClientImpl>();
    mdsClient_->Init(mdsOptions_, mdsBase_);

    s3Adaptor_ = std::make_shared<S3ClientAdaptorImpl>();

    S3ClientAdaptorOption s3ClientAdaptorOption;
    InitS3Option(conf_, &s3ClientAdaptorOption);
    curve::common::S3AdapterOption s3AdaptorOption;
    ::curve::common::InitS3AdaptorOptionExceptS3InfoOption(conf_.get(),
                                                         &s3AdaptorOption);
    auto s3Client_ = new S3ClientImpl;
    s3Client_->SetAdaptor(std::make_shared<curve::common::S3Adapter>());
    s3Client_->Init(s3AdaptorOption);
    // s3Adaptor_ own the s3Client_, and will delete it when destruct.
    s3Adaptor_->Init(s3ClientAdaptorOption, s3Client_);
    trashOption.s3Adaptor = s3Adaptor_;
    trashOption.mdsClient = mdsClient_;
    TrashManager::GetInstance().Init(trashOption);

    // NOTE: Do not arbitrarily adjust the order, there are dependencies
    //       between different modules
    InitLocalFileSystem();
    InitStorage();
    InitCopysetNodeManager();

    // get metaserver id and token before heartbeat
    GetMetaserverDataByLoadOrRegister();

    InitHeartbeat();
    InitInflightThrottle();

    S3CompactManager::GetInstance().Init(conf_);

    PartitionCleanOption partitionCleanOption;
    InitPartitionOption(s3Adaptor_, mdsClient_, &partitionCleanOption);
    PartitionCleanManager::GetInstance().Init(partitionCleanOption);

    inited_ = true;
}

void Metaserver::GetMetaserverDataByLoadOrRegister() {
    std::string metaFilePath;
    conf_->GetValueFatalIfFail("metaserver.meta_file_path", &metaFilePath);
    if (localFileSystem_->FileExists(metaFilePath)) {
        // get metaserver from load
        LOG_IF(FATAL, LoadMetaserverMeta(metaFilePath, &metadata_) != 0)
            << "load metaserver meta fail, path = " << metaFilePath;
    } else {
        // register metaserver to mds
        InitRegisterOptions();
        Register registerMDS(registerOptions_);
        LOG(INFO) << "register metaserver to mds";
        LOG_IF(FATAL, registerMDS.RegisterToMDS(&metadata_) != 0)
            << "Failed to register metaserver to MDS.";

        LOG_IF(FATAL, PersistMetaserverMeta(metaFilePath, &metadata_) != 0)
            << "persist metadata meta to file fail, path = " << metaFilePath;
        LOG(INFO) << "metaserver " << metadata_.ShortDebugString();
    }
}

int Metaserver::PersistMetaserverMeta(std::string path,
                                      MetaServerMetadata* metadata) {
    std::string tempData;
    metadata->set_checksum(0);
    bool ret = metadata->SerializeToString(&tempData);
    if (!ret) {
        LOG(ERROR) << "convert MetaServerMetadata to string fail";
        return -1;
    }

    uint32_t crc = curve::common::CRC32(0, tempData.c_str(), tempData.length());
    metadata->set_checksum(crc);

    std::string data;
    ret = metadata->SerializeToString(&data);
    if (!ret) {
        LOG(ERROR) << "convert MetaServerMetadata to string fail";
        return -1;
    }

    return PersistDataToLocalFile(localFileSystem_, path, data);
}

int Metaserver::LoadMetaserverMeta(const std::string& metaFilePath,
                                   MetaServerMetadata* metadata) {
    std::string data;
    int ret = LoadDataFromLocalFile(localFileSystem_, metaFilePath, &data);
    if (ret != 0) {
        LOG(ERROR) << "load metaserver meta from file fail, path = "
                   << metaFilePath;
        return ret;
    }

    LOG(INFO) << "load data from file, path = " <<  metaFilePath
              << ", len = " << data.length()
              << ", data = " << data;

    bool ret1 = metadata->ParseFromString(data);
    if (!ret1) {
        LOG(ERROR) << "parse metaserver meta from string fail, data = " << data;
        return -1;
    }

    uint32_t crcFromFile = metadata->checksum();
    std::string tempData;
    metadata->set_checksum(0);
    bool ret2 = metadata->SerializeToString(&tempData);
    if (!ret2) {
        LOG(ERROR) << "convert MetaServerMetadata to string fail";
        return -1;
    }

    uint32_t crc = curve::common::CRC32(0, tempData.c_str(), tempData.length());
    if (crc != crcFromFile) {
        LOG(ERROR) << "crc is mismatch";
        return -1;
    }

    return 0;
}

int Metaserver::LoadDataFromLocalFile(std::shared_ptr<LocalFileSystem> fs,
                                      const std::string& localPath,
                                      std::string* data) {
    if (!fs->FileExists(localPath)) {
        LOG(ERROR) << "get data from local file fail, path = " << localPath;
        return -1;
    }

    int fd = fs->Open(localPath.c_str(), O_RDONLY);
    if (fd < 0) {
        LOG(ERROR) << "Fail to open local file for write, path = " << localPath;
        return -1;
    }

#define METAFILE_MAX_SIZE 4096
    char buf[METAFILE_MAX_SIZE];
    int readCount = fs->Read(fd, buf, 0, METAFILE_MAX_SIZE);
    if (readCount <= 0) {
        LOG(ERROR) << "Failed to read data from file, path = " << localPath
                   << ", readCount = " << readCount;
        return -1;
    }

    if (fs->Close(fd)) {
        LOG(ERROR) << "Failed to close file, path = " << localPath;
        return -1;
    }

    *data = std::string(buf, readCount);
    return 0;
}

int Metaserver::PersistDataToLocalFile(std::shared_ptr<LocalFileSystem> fs,
                                       const std::string& localPath,
                                       const std::string& data) {
    LOG(INFO) << "persist data to file, path  = " << localPath
              << ", data len = " << data.length()
              << ", data = " << data;
    int fd = fs->Open(localPath.c_str(), O_RDWR | O_CREAT);
    if (fd < 0) {
        LOG(ERROR) << "Fail to open local file for write, path = " << localPath;
        return -1;
    }

    int writtenCount = fs->Write(fd, data.c_str(), 0, data.size());
    if (writtenCount < 0 || static_cast<size_t>(writtenCount) != data.size()) {
        LOG(ERROR) << "Failed to write data to file, path = " << localPath
                   << ", writtenCount = " << writtenCount
                   << ", data size = " << data.size();
        return -1;
    }

    if (fs->Close(fd)) {
        LOG(ERROR) << "Failed to close file, path = " << localPath;
        return -1;
    }

    return 0;
}

void Metaserver::Run() {
    if (!inited_) {
        LOG(ERROR) << "Metaserver not inited yet!";
        return;
    }

    TrashManager::GetInstance().Run();

    // start heartbeat
    LOG_IF(FATAL, heartbeat_.Run() != 0)
        << "Failed to start heartbeat manager.";

    // set metaserver version in metric
    curve::common::ExposeCurveVersion();

    PartitionCleanManager::GetInstance().Run();

    // add internal server
    server_ = absl::make_unique<brpc::Server>();
    metaService_ = absl::make_unique<MetaServerServiceImpl>(
        copysetNodeManager_, inflightThrottle_.get());
    copysetService_ =
        absl::make_unique<CopysetServiceImpl>(copysetNodeManager_);
    raftCliService2_ = absl::make_unique<RaftCliService2>(copysetNodeManager_);

    // add metaserver service
    LOG_IF(FATAL, server_->AddService(metaService_.get(),
                                      brpc::SERVER_DOESNT_OWN_SERVICE) != 0)
        << "add metaserverService error";
    LOG_IF(FATAL, server_->AddService(copysetService_.get(),
                                      brpc::SERVER_DOESNT_OWN_SERVICE) != 0)
        << "add copysetservice error";

    butil::ip_t ip;
    LOG_IF(FATAL, 0 != butil::str2ip(options_.ip.c_str(), &ip))
        << "convert " << options_.ip << " to ip failed";
    butil::EndPoint listenAddr(ip, options_.port);

    // add raft-related service
    copysetNodeManager_->AddService(server_.get(), listenAddr);

    // start internal rpc server
    brpc::ServerOptions option;
    if (options_.bthreadWorkerCount != -1) {
        option.num_threads = options_.bthreadWorkerCount;
    }
    LOG_IF(FATAL, server_->Start(listenAddr, &option) != 0)
        << "start internal brpc server error";

    // add external server
    if (options_.enableExternalServer) {
        LOG(INFO) << "metaserver enable external server, options_.externalIp = "
                  << options_.externalIp << ", options_.externalPort = "
                  << options_.externalPort;
        externalServer_ = absl::make_unique<brpc::Server>();
        LOG_IF(FATAL, externalServer_->AddService(metaService_.get(),
            brpc::SERVER_DOESNT_OWN_SERVICE) != 0)
            << "add metaserverService error";
        LOG_IF(FATAL, externalServer_->AddService(copysetService_.get(),
            brpc::SERVER_DOESNT_OWN_SERVICE) != 0)
            << "add copysetService error";
        LOG_IF(FATAL, externalServer_->AddService(raftCliService2_.get(),
            brpc::SERVER_DOESNT_OWN_SERVICE) != 0)
            << "add raftCliService2 error";
        LOG_IF(FATAL, externalServer_->AddService(new braft::RaftStatImpl{},
            brpc::SERVER_OWNS_SERVICE) != 0)
            << "add raftStatService error";

        butil::ip_t ip;
        LOG_IF(FATAL, 0 != butil::str2ip(options_.externalIp.c_str(), &ip))
            << "convert " << options_.externalIp << " to ip failed";
        butil::EndPoint listenAddr(ip, options_.externalPort);
        // start external rpc server
        LOG_IF(FATAL, externalServer_->Start(listenAddr, &option) != 0)
            << "start external brpc server error";
    }

    // try start s3compact wq
    LOG_IF(FATAL, S3CompactManager::GetInstance().Run() != 0);
    running_ = true;

    // start copyset node manager
    LOG_IF(FATAL, !copysetNodeManager_->Start())
        << "Failed to start copyset node manager";

    brpc::FLAGS_graceful_quit_on_sigterm = true;
    server_->RunUntilAskedToQuit();
}

void Metaserver::Stop() {
    if (!running_) {
        LOG(WARNING) << "Metaserver is not running";
        return;
    }

    LOG(INFO) << "MetaServer is going to quit";
    if (options_.enableExternalServer) {
        externalServer_->Stop(0);
        externalServer_->Join();
    }
    server_->Stop(0);
    server_->Join();

    PartitionCleanManager::GetInstance().Fini();

    LOG_IF(ERROR, heartbeat_.Fini() != 0);

    TrashManager::GetInstance().Fini();
    LOG_IF(ERROR, !copysetNodeManager_->Stop())
        << "Failed to stop copyset node manager";

    S3CompactManager::GetInstance().Stop();

    LOG_IF(ERROR, !GetStorageInstance()->Close())
        << "Failed to close storage.";

    LOG(INFO) << "MetaServer stopped success";
}

void Metaserver::InitHeartbeatOptions() {
    LOG_IF(FATAL, !conf_->GetStringValue("copyset.data_uri",
                                         &heartbeatOptions_.storeUri));
    LOG_IF(FATAL, !conf_->GetStringValue("global.ip", &heartbeatOptions_.ip));
    LOG_IF(FATAL,
           !conf_->GetUInt32Value("global.port", &heartbeatOptions_.port));
    LOG_IF(FATAL, !conf_->GetStringValue("mds.listen.addr",
                                         &heartbeatOptions_.mdsListenAddr));
    LOG_IF(FATAL, !conf_->GetUInt32Value("mds.heartbeat_intervalSec",
                                         &heartbeatOptions_.intervalSec));
    LOG_IF(FATAL, !conf_->GetUInt32Value("mds.heartbeat_timeoutMs",
                                         &heartbeatOptions_.timeout));
}

void Metaserver::InitHeartbeat() {
    InitHeartbeatOptions();
    heartbeatOptions_.copysetNodeManager = copysetNodeManager_;
    heartbeatOptions_.metaserverId = metadata_.id();
    heartbeatOptions_.metaserverToken = metadata_.token();
    heartbeatOptions_.fs = localFileSystem_;
    LOG_IF(FATAL, heartbeat_.Init(heartbeatOptions_) != 0)
        << "Failed to init Heartbeat manager.";
}

void Metaserver::InitStorage() {
    LOG_IF(FATAL, !conf_->GetStringValue("storage.type",
                                         &storageOptions_.type));
    LOG_IF(FATAL,
        storageOptions_.type != "memory" && storageOptions_.type != "rocksdb")
        << "Invalid storage type: " << storageOptions_.type;
    LOG_IF(FATAL, !conf_->GetUInt64Value("storage.max_memory_quota_bytes",
                                         &storageOptions_.maxMemoryQuotaBytes));
    LOG_IF(FATAL, !conf_->GetUInt64Value("storage.max_disk_quota_bytes",
                                         &storageOptions_.maxDiskQuotaBytes));
    LOG_IF(FATAL, !conf_->GetStringValue("storage.data_dir",
                                         &storageOptions_.dataDir));
    LOG_IF(FATAL, !conf_->GetBoolValue("storage.memory.compression",
                                       &storageOptions_.compression));
    LOG_IF(FATAL, !conf_->GetUInt64Value(
        "storage.rocksdb.unordered_write_buffer_size",
        &storageOptions_.unorderedWriteBufferSize));
    LOG_IF(FATAL, !conf_->GetUInt64Value(
        "storage.rocksdb.unordered_max_write_buffer_number",
        &storageOptions_.unorderedMaxWriteBufferNumber));
    LOG_IF(FATAL, !conf_->GetUInt64Value(
        "storage.rocksdb.ordered_write_buffer_size",
        &storageOptions_.orderedWriteBufferSize));
    LOG_IF(FATAL, !conf_->GetUInt64Value(
        "storage.rocksdb.ordered_max_write_buffer_number",
        &storageOptions_.orderedMaxWriteBufferNumber));
    LOG_IF(FATAL, !conf_->GetUInt64Value(
        "storage.rocksdb.block_cache_capacity",
        &storageOptions_.blockCacheCapacity));
    LOG_IF(FATAL, !conf_->GetUInt64Value(
        "storage.s3_meta_inside_inode.limit_size",
        &storageOptions_.s3MetaLimitSizeInsideInode));

    bool succ = ::curvefs::metaserver::storage::InitStorage(storageOptions_);
    LOG_IF(FATAL, !succ) << "Init storage failed";
}

void Metaserver::InitCopysetNodeManager() {
    InitCopysetNodeOptions();

    copysetNodeManager_ = &CopysetNodeManager::GetInstance();
    LOG_IF(FATAL, !copysetNodeManager_->Init(copysetNodeOptions_))
        << "Failed to initialize CopysetNodeManager";
}

void Metaserver::InitCopysetNodeOptions() {
    LOG_IF(FATAL, !conf_->GetStringValue("global.ip", &copysetNodeOptions_.ip));
    LOG_IF(FATAL,
           !conf_->GetUInt32Value("global.port", &copysetNodeOptions_.port));

    LOG_IF(FATAL,
           copysetNodeOptions_.port <= 0 || copysetNodeOptions_.port >= 65535)
        << "Invalid server port: " << copysetNodeOptions_.port;

    LOG_IF(FATAL, !conf_->GetStringValue("copyset.data_uri",
                                         &copysetNodeOptions_.dataUri));
    LOG_IF(FATAL,
           !conf_->GetIntValue(
               "copyset.election_timeout_ms",
               &copysetNodeOptions_.raftNodeOptions.election_timeout_ms));
    LOG_IF(FATAL,
           !conf_->GetIntValue(
               "copyset.snapshot_interval_s",
               &copysetNodeOptions_.raftNodeOptions.snapshot_interval_s));
    LOG_IF(FATAL, !conf_->GetIntValue(
                      "copyset.catchup_margin",
                      &copysetNodeOptions_.raftNodeOptions.catchup_margin));
    LOG_IF(FATAL, !conf_->GetStringValue(
                      "copyset.raft_log_uri",
                      &copysetNodeOptions_.raftNodeOptions.log_uri));
    LOG_IF(FATAL, !conf_->GetStringValue(
                      "copyset.raft_meta_uri",
                      &copysetNodeOptions_.raftNodeOptions.raft_meta_uri));
    LOG_IF(FATAL, !conf_->GetStringValue(
                      "copyset.raft_snapshot_uri",
                      &copysetNodeOptions_.raftNodeOptions.snapshot_uri));
    LOG_IF(FATAL, !conf_->GetUInt32Value("copyset.load_concurrency",
                                         &copysetNodeOptions_.loadConcurrency));
    LOG_IF(FATAL, !conf_->GetUInt32Value("copyset.check_retrytimes",
                                         &copysetNodeOptions_.checkRetryTimes));
    LOG_IF(FATAL,
           !conf_->GetUInt32Value("copyset.finishload_margin",
                                  &copysetNodeOptions_.finishLoadMargin));
    LOG_IF(FATAL, !conf_->GetUInt32Value(
                      "copyset.check_loadmargin_interval_ms",
                      &copysetNodeOptions_.checkLoadMarginIntervalMs));

    LOG_IF(FATAL, !conf_->GetUInt32Value(
                      "applyqueue.worker_count",
                      &copysetNodeOptions_.applyQueueOption.workerCount));
    LOG_IF(FATAL, !conf_->GetUInt32Value(
                      "applyqueue.queue_depth",
                      &copysetNodeOptions_.applyQueueOption.queueDepth));

    LOG_IF(FATAL,
           !conf_->GetStringValue("copyset.trash.uri",
                                  &copysetNodeOptions_.trashOptions.trashUri));
    LOG_IF(FATAL, !conf_->GetUInt32Value(
                      "copyset.trash.expired_aftersec",
                      &copysetNodeOptions_.trashOptions.expiredAfterSec));
    LOG_IF(FATAL, !conf_->GetUInt32Value(
                      "copyset.trash.scan_periodsec",
                      &copysetNodeOptions_.trashOptions.scanPeriodSec));

    CHECK(localFileSystem_);
    copysetNodeOptions_.localFileSystem = localFileSystem_.get();
}

void Metaserver::InitInflightThrottle() {
    uint64_t maxInflight = 0;
    LOG_IF(FATAL, !conf_->GetUInt64Value("service.max_inflight_request",
                                         &maxInflight));

    inflightThrottle_ = absl::make_unique<InflightThrottle>(maxInflight);
}

struct TakeValueFromConfIfCmdNotSet {
    template <typename T>
    void operator()(const std::shared_ptr<Configuration>& conf,
                    const std::string& cmdName, const std::string& confName,
                    T* value) {
        using ::google::CommandLineFlagInfo;
        using ::google::GetCommandLineFlagInfo;

        CommandLineFlagInfo info;
        if (GetCommandLineFlagInfo(cmdName.c_str(), &info) && info.is_default) {
            conf->GetValueFatalIfFail(confName, value);
        }
    }
};

void Metaserver::InitBRaftFlags(const std::shared_ptr<Configuration>& conf) {
    TakeValueFromConfIfCmdNotSet dummy;
    dummy(conf, "raft_sync", "braft.raft_sync", &braft::FLAGS_raft_sync);
    dummy(conf, "raft_sync_meta", "braft.raft_sync_meta",
          &braft::FLAGS_raft_sync_meta);
    dummy(conf, "raft_sync_segments", "braft.raft_sync_segments",
          &braft::FLAGS_raft_sync_segments);
    dummy(conf, "raft_use_fsync_rather_than_fdatasync",
          "braft.raft_use_fsync_rather_than_fdatasync",
          &braft::FLAGS_raft_use_fsync_rather_than_fdatasync);
    dummy(conf, "raft_max_install_snapshot_tasks_num",
          "braft.raft_max_install_snapshot_tasks_num",
          &braft::FLAGS_raft_max_install_snapshot_tasks_num);
}

}  // namespace metaserver
}  // namespace curvefs
