/*
 * Copyright (C) 2018 NetEase Inc. All rights reserved.
 * Project: Curve
 *
 * History:
 *          2018/08/30  Wenyu Zhou   Initial version
 */

#include "src/chunkserver/chunkserver.h"

#include <gflags/gflags.h>
#include <dirent.h>
#include <json2pb/pb_to_json.h>
#include <json2pb/json_to_pb.h>

#include <string>
#include <vector>

#include "proto/topology.pb.h"
#include "include/chunkserver/chunkserver_common.h"
#include "src/chunkserver/chunkserverStorage/chunkserver_adaptor_util.h"

const uint32_t TOKEN_SIZE = 128;

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
    copysetNodeOptions_.recyclerUri =
        conf_.GetStringValue("copyset.recycler_uri");
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

void ChunkServer::InitChunkFilePoolOptions() {
    chunkFilePoolOptions_.chunkSize =
        conf_.GetIntValue("global.chunk_size");
    chunkFilePoolOptions_.metaPageSize =
        conf_.GetIntValue("global.meta_page_size");
    chunkFilePoolOptions_.cpMetaFileSize
        = conf_.GetIntValue("chunkfilepool.cpmeta_file_size");
    chunkFilePoolOptions_.getChunkFromPool
        = conf_.GetBoolValue("chunkfilepool.enable_get_chunk_from_pool");

    if (chunkFilePoolOptions_.getChunkFromPool == false) {
        std::string chunkFilePoolUri
            = conf_.GetStringValue("chunkfilepool.chunk_file_pool_dir");
        ::memcpy(chunkFilePoolOptions_.chunkFilePoolDir,
                 chunkFilePoolUri.c_str(),
                 chunkFilePoolUri.size());
    } else {
        std::string metaUri
            = conf_.GetStringValue("chunkfilepool.meta_path");
        ::memcpy(chunkFilePoolOptions_.metaPath,
                 metaUri.c_str(),
                 metaUri.size());
    }
}

void ChunkServer::InitHeartbeatOptions() {
    heartbeatOptions_.copysetNodeManager = &copysetNodeManager_;
    heartbeatOptions_.chunkserverId = metadata_.id();
    heartbeatOptions_.chunkserverToken = metadata_.token();
    heartbeatOptions_.dataUri = conf_.GetStringValue("copyset.chunk_data_uri");
    heartbeatOptions_.ip = conf_.GetStringValue("global.ip");
    heartbeatOptions_.port = conf_.GetIntValue("global.port");
    heartbeatOptions_.mdsIp = conf_.GetStringValue("mds.ip");
    heartbeatOptions_.mdsPort = conf_.GetIntValue("mds.port");
    heartbeatOptions_.interval = conf_.GetIntValue("mds.heartbeat_interval");
    heartbeatOptions_.timeout = conf_.GetIntValue("mds.heartbeat_timeout");
    heartbeatOptions_.fs = fs_;
}

void ChunkServer::InitServiceOptions() {
    serviceOptions_.ip = conf_.GetStringValue("global.ip");
    serviceOptions_.port = conf_.GetIntValue("global.port");
    serviceOptions_.chunkserver = this;
    serviceOptions_.copysetNodeManager = &copysetNodeManager_;
}

int ChunkServer::PersistChunkServerMeta() {
    int fd;
    std::string metaFile = FsAdaptorUtil::GetPathFromUri(metaUri_);

    std::string metaStr;
    std::string err;
    json2pb::Pb2JsonOptions opt;
    opt.bytes_to_base64 = true;
    opt.enum_option = json2pb::OUTPUT_ENUM_BY_NUMBER;

    if (!json2pb::ProtoMessageToJson(metadata_, &metaStr, opt, &err)) {
        LOG(ERROR) << "Failed to convert chunkserver meta data for persistence,"
                   << " error: " << err.c_str();
        return -1;
    }

    fd = fs_->Open(metaFile.c_str(), O_RDWR | O_CREAT);
    if (fd < 0) {
        LOG(ERROR) << "Fail to open chunkserver metadata file for write";
        return -1;
    }

    if (fs_->Write(fd, metaStr.c_str(), 0, metaStr.size()) < metaStr.size()) {
        LOG(ERROR) << "Failed to write chunkserver metadata file";
        return -1;
    }
    if (fs_->Close(fd)) {
        LOG(ERROR) << "Failed to close chunkserver metadata file";
        return -1;
    }

    return 0;
}

int ChunkServer::ReadChunkServerMeta() {
    int fd;
    std::string metaFile = FsAdaptorUtil::GetPathFromUri(metaUri_);

    fd = fs_->Open(metaFile.c_str(), O_RDONLY);
    if (fd < 0) {
        LOG(ERROR) << "Failed to open Chunkserver metadata file " << metaFile;
        return -1;
    }

#define METAFILE_MAX_SIZE  4096
    uint32_t size;
    char json[METAFILE_MAX_SIZE] = {0};

    size = fs_->Read(fd, json, 0, METAFILE_MAX_SIZE);
    if (size < 0) {
        LOG(ERROR) << "Failed to read Chunkserver metadata file";
        return -1;
    } else if (size >= METAFILE_MAX_SIZE) {
        LOG(ERROR) << "Chunkserver metadata file is too large: " << size;
        return -1;
    }
    if (fs_->Close(fd)) {
        LOG(ERROR) << "Failed to close chunkserver metadata file";
        return -1;
    }

    std::string jsonStr(json);
    std::string err;
    json2pb::Json2PbOptions opt;
    opt.base64_to_bytes = true;

    if (!json2pb::JsonToProtoMessage(jsonStr, &metadata_, opt, &err)) {
        LOG(ERROR) << "Failed to convert chunkserver meta data read,"
                   << " error: " << err.c_str();
        return -1;
    }

    if (!ValidateMetaData()) {
        LOG(ERROR) << "Failed to validate Chunkserver metadata.";
        return -1;
    }

    return 0;
}

uint32_t ChunkServer::MetadataCrc() {
    uint32_t crc = 0;
    uint32_t ver = metadata_.version();
    uint32_t id = metadata_.id();
    const char* token = metadata_.token().c_str();

    crc = curve::common::CRC32(crc, reinterpret_cast<char*>(&ver), sizeof(ver));
    crc = curve::common::CRC32(crc, reinterpret_cast<char*>(&id), sizeof(id));
    crc = curve::common::CRC32(crc, token, metadata_.token().size());

    return crc;
}

bool ChunkServer::ValidateMetaData() {
    if (MetadataCrc() != metadata_.checksum()) {
        LOG(ERROR) << "ChunkServer persisted metadata CRC dismatch.";
        return false;
    } else if (metadata_.version() != CURRENT_METADATA_VERSION) {
        LOG(ERROR) << "ChunkServer metadata version dismatch, stored: "
                   << metadata_.version()
                   << ", expected: " << CURRENT_METADATA_VERSION;
        return false;
    }

    return true;
}

bool ChunkServer::ChunkServerIdPersisted() {
    std::string metaFile = FsAdaptorUtil::GetPathFromUri(metaUri_);

    if (!fs_->FileExists(metaFile.c_str())) {
        return false;
    }

    return true;
}

int ChunkServer::RegisterToMetaServer() {
    std::string mdsIpStr = conf_.GetStringValue("mds.ip");
    int mdsPort = conf_.GetIntValue("mds.port");

    butil::ip_t mdsIp;
    if (butil::str2ip(mdsIpStr.c_str(), &mdsIp) < 0) {
        LOG(ERROR) << "Invalid MDS IP provided: " << mdsIpStr;
        return -1;
    }
    if (mdsPort <= 0 || mdsPort >= 65535) {
        LOG(ERROR) << "Invalid MDS port provided: " << mdsPort;
        return -1;
    }
    mdsEp_ = butil::EndPoint(mdsIp, mdsPort);
    LOG(INFO) << "MDS address is  " << mdsIpStr << ":" << mdsPort;

    std::string proto;

    storUri_ = conf_.GetStringValue("chunkserver.stor_uri");
    proto = FsAdaptorUtil::GetProtocolFromUri(storUri_);
    if (proto != "local") {
        LOG(ERROR) << "Datastore protocal " << proto << " is not supported yet";
        return -1;
    }

    metaUri_ = conf_.GetStringValue("chunkserver.meta_uri");
    proto = FsAdaptorUtil::GetProtocolFromUri(metaUri_);

    if (ChunkServerIdPersisted()) {
        if (ReadChunkServerMeta() != 0) {
            LOG(ERROR) << "Fail to read persisted chunkserver meta data";
            return -1;
        }

        LOG(INFO) << "Found persisted chunkserver data, skipping registration,"
                  << " chunkserver id: " << metadata_.id()
                  << ", token: " << metadata_.token();
        return 0;
    }

    curve::mds::topology::ChunkServerRegistRequest req;
    curve::mds::topology::ChunkServerRegistResponse resp;

    req.set_disktype(conf_.GetStringValue("chunkserver.disk_type"));
    req.set_diskpath(storUri_);
    req.set_hostip(serviceOptions_.ip);
    req.set_port(serviceOptions_.port);

    LOG(INFO) << "Registering to MDS " << mdsEp_;
    int retries = conf_.GetIntValue("mds.register_retries");
    int timeout = conf_.GetIntValue("mds.register_timeout");
    while (retries >= 0) {
        brpc::Channel channel;
        brpc::Controller cntl;

        cntl.set_timeout_ms(timeout);

        if (channel.Init(mdsEp_, NULL) != 0) {
            LOG(ERROR) << "Fail to init channel to MDS " << mdsEp_;
        }
        curve::mds::topology::TopologyService_Stub stub(&channel);

        stub.RegistChunkServer(&cntl, &req, &resp, nullptr);

        if (!cntl.Failed() && resp.statuscode() == 0) {
            break;
        } else {
            LOG(ERROR) << "Fail to register to MDS " << mdsEp_ << ","
                       << " cntl error: " << cntl.ErrorText() << ","
                       << " statusCode: " << resp.statuscode() << ","
                       << " going to sleep and try again.";
            sleep(1);
            --retries;
        }
    }

    if (retries <= 0) {
        LOG(ERROR) << "Fail to register to MDS " << mdsEp_
                   << " for " << conf_.GetIntValue("mds.register_retries")
                   << " times.";
        return -1;
    }

    metadata_.set_version(CURRENT_METADATA_VERSION);
    metadata_.set_id(resp.chunkserverid());
    metadata_.set_token(resp.token());
    metadata_.set_checksum(MetadataCrc());

    LOG(INFO) << "Successfully registered to MDS,"
              << " chunkserver id: " << metadata_.id() << ","
              << " token: " << metadata_.token() << ","
              << " persisting them to local storage.";

    if (PersistChunkServerMeta() < 0) {
        LOG(ERROR) << "Failed to persist chunkserver meta data";
        return -1;
    }

    return 0;
}

int ChunkServer::ReloadCopysets() {
    std::string datadir;

    datadir = FsAdaptorUtil::GetPathFromUri(copysetNodeOptions_.chunkDataUri);
    if (!fs_->DirExists(datadir)) {
        LOG(INFO) << "Failed to access data directory:  " << datadir
                   << ", assume it is an uninitialized datastore";
        return 0;
    }

    vector<std::string> items;
    if (fs_->List(datadir, &items) != 0) {
        LOG(ERROR) << "Failed to get copyset list from data directory "
                   << datadir;
        return -1;
    }

    vector<std::string>::iterator it = items.begin();
    for (; it != items.end(); ++it) {
        if (*it == "." || *it == "..") {
            continue;
        }

        LOG(INFO) << "Found copyset dir " << *it;

        uint64_t groupId = std::stoul(*it);
        uint64_t poolId = GetPoolID(groupId);
        uint64_t copysetId = GetCopysetID(groupId);
        LOG(INFO) << "Parsed groupid " << groupId
                  << "as " << ToGroupIdStr(poolId, copysetId);

        braft::Configuration conf;
        if (!copysetNodeManager_.CreateCopysetNode(poolId, copysetId, conf)) {
            LOG(ERROR) << "Failed to recreate copyset: <"
                      << poolId << "," << copysetId << ">";
        }

        LOG(INFO) << "Created copyset: <" << poolId << "," << copysetId << ">";
    }

    return 0;
}

int ChunkServer::InitConfig(const std::string& confPath) {
    LOG(INFO) << "Loading Configuration.";
    conf_.SetConfigPath(confPath);
    // FIXME(wenyu): may also log into syslog
    if (!conf_.LoadConfig()) {
        return -1;
    }

    return ReplaceConfigWithCmdFlags();
}

int ChunkServer::Init(int argc, char **argv) {
    LOG(INFO) << "Starting to Initialize ChunkServer.";

    ParseCommandLineFlags(argc, argv);

    LOG_IF(FATAL, InitConfig(FLAGS_conf))
        << "Failed to open config file: " << conf_.GetConfigPath();

    LOG(INFO) << "Initializing ChunkServer modules";

    InitCopysetNodeOptions();
    LOG_IF(FATAL, copysetNodeManager_.Init(copysetNodeOptions_) != 0)
        << "Failed to initialize CopysetNodeManager.";

    fs_ = copysetNodeOptions_.localFileSystem;

    int size = conf_.GetIntValue("concurrentapply.size");
    int qdepth = conf_.GetIntValue("concurrentapply.queuedepth");
    LOG_IF(FATAL, false == concurrentapply_.Init(size, qdepth))
        << "Failed to initialize concurrentapply module!";

    bool ret;
    InitChunkFilePoolOptions();
    ret = copysetNodeOptions_.chunkfilePool->Initialize(chunkFilePoolOptions_);
    LOG_IF(FATAL, false == ret)
        << "Failed to init chunk file pool";

    InitServiceOptions();
    LOG_IF(FATAL, serviceManager_.Init(serviceOptions_) != 0)
        << "Failed to initialize ServiceManager.";

    LOG_IF(FATAL, RegisterToMetaServer() != 0) << "Failed to register to MDS.";

    LOG_IF(FATAL, ReloadCopysets() != 0) << "Failed to recreate copysets.";

    InitHeartbeatOptions();
    LOG_IF(FATAL, heartbeat_.Init(heartbeatOptions_) != 0)
        << "Failed to init Heartbeat manager.";

    toStop = false;

    return 0;
}

int ChunkServer::Fini() {
    LOG(INFO) << "ChunkServer is going to quit.";

    LOG_IF(ERROR, heartbeat_.Fini() != 0)
        << "Failed to shutdown heartbeat manager.";
    LOG_IF(ERROR, copysetNodeManager_.Fini() != 0)
        << "Failed to shutdown CopysetNodeManager.";
    LOG_IF(ERROR, serviceManager_.Fini() != 0)
        << "Failed to shutdown ServiceManager.";
    concurrentapply_.Stop();

    return 0;
}

int ChunkServer::Run() {
    LOG(INFO) << "ChunkServer starts.";

    LOG_IF(FATAL, serviceManager_.Run() != 0)
        << "Failed to start ServiceManager.";
    LOG_IF(FATAL, copysetNodeManager_.Run() != 0)
        << "Failed to start CopysetNodeManager.";
    LOG_IF(FATAL, heartbeat_.Run() != 0)
        << "Failed to start heartbeat manager.";

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
