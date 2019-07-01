/*
 * Project: curve
 * Created Date: Monday June 10th 2019
 * Author: yangyaokai
 * Copyright (c) 2019 netease
 */

#include "src/chunkserver/chunkserver_metrics.h"
#include <vector>
#include <map>

#include "src/chunkserver/copyset_node_manager.h"
#include "src/chunkserver/passive_getfn.h"

namespace curve {
namespace chunkserver {

IOMetric::IOMetric()
    : rps_(&reqNum_, 1)
    , iops_(&ioNum_, 1)
    , eps_(&errorNum_, 1)
    , bps_(&ioBytes_, 1) {}

IOMetric::~IOMetric() {}

int IOMetric::Init(const std::string& prefix) {
    // 暴露所有的metric
    if (reqNum_.expose_as(prefix, "request_num") != 0) {
        LOG(ERROR) << "expose request num failed.";
        return -1;
    }
    if (ioNum_.expose_as(prefix, "io_num") != 0) {
        LOG(ERROR) << "expose io num failed.";
        return -1;
    }
    if (ioBytes_.expose_as(prefix, "io_bytes") != 0) {
        LOG(ERROR) << "expose io bytes failed.";
        return -1;
    }
    if (errorNum_.expose_as(prefix, "error_num") != 0) {
        LOG(ERROR) << "expose error num failed.";
        return -1;
    }
    if (latencyRecorder_.expose(prefix, "lat") != 0) {
        LOG(ERROR) << "expose latency recorder failed.";
        return -1;
    }
    if (sizeRecorder_.expose(prefix, "io_size") != 0) {
        LOG(ERROR) << "expose size recorder failed.";
        return -1;
    }
    if (rps_.expose_as(prefix, "rps") != 0) {
        LOG(ERROR) << "expose rps failed.";
        return -1;
    }
    if (iops_.expose_as(prefix, "iops") != 0) {
        LOG(ERROR) << "expose iops failed.";
        return -1;
    }
    if (bps_.expose_as(prefix, "bps") != 0) {
        LOG(ERROR) << "expose bps failed.";
        return -1;
    }
    if (eps_.expose_as(prefix, "eps") != 0) {
        LOG(ERROR) << "expose eps failed.";
        return -1;
    }
    return 0;
}

void IOMetric::OnRequest() {
    reqNum_ << 1;
}

void IOMetric::OnResponse(size_t size, int64_t latUs, bool hasError) {
    if (!hasError) {
        ioNum_ << 1;
        sizeRecorder_ << size;
        ioBytes_ << size;
        latencyRecorder_ << latUs;
    } else {
        errorNum_ << 1;
    }
}

int CSCopysetMetric::Init(const LogicPoolID& logicPoolId,
                          const CopysetID& copysetId) {
    logicPoolId_ = logicPoolId;
    copysetId_ = copysetId;
    std::string readPrefix = Prefix() + "_read";
    std::string writePrefix = Prefix() + "_write";
    readMetric_ = std::make_shared<IOMetric>();
    writeMetric_ = std::make_shared<IOMetric>();
    if (readMetric_->Init(readPrefix) != 0) {
        LOG(ERROR) << "Init Copyset ("
                   << logicPoolId << "," << copysetId << ")"
                   << " metric failed : init read metric failed."
                   << " prefix = " << readPrefix;
        return -1;
    }
    if (writeMetric_->Init(writePrefix) != 0) {
        LOG(ERROR) << "Init Copyset ("
                   << logicPoolId << "," << copysetId << ")"
                   << " metric failed : init write metric failed."
                   << " prefix = " << writePrefix;
        return -1;
    }
    return 0;
}

void CSCopysetMetric::OnRequestWrite() {
    if (writeMetric_ != nullptr) {
        writeMetric_->OnRequest();
    }
}

void CSCopysetMetric::OnRequestRead() {
    if (readMetric_ != nullptr) {
        readMetric_->OnRequest();
    }
}

void CSCopysetMetric::OnResponseWrite(size_t size,
                                      int64_t latUs,
                                      bool hasError) {
    if (writeMetric_ != nullptr) {
        writeMetric_->OnResponse(size, latUs, hasError);
    }
}

void CSCopysetMetric::OnResponseRead(size_t size,
                                       int64_t latUs,
                                       bool hasError) {
    if (readMetric_ != nullptr) {
        readMetric_->OnResponse(size, latUs, hasError);
    }
}

void CSCopysetMetric::MonitorDataStore(CSDataStore* datastore) {
    std::string chunkCountPrefix = Prefix() + "_datastore_chunk_count";
    std::string snapshotCountPrefix = Prefix() + "_datastore_snapshot_count";
    chunkCount_ = std::make_shared<bvar::PassiveStatus<uint32_t>>(
        chunkCountPrefix, getDatastoreChunkCountFunc, datastore);
    snapshotCount_ = std::make_shared<bvar::PassiveStatus<uint32_t>>(
        snapshotCountPrefix, getDatastoreSnapshotCountFunc, datastore);
}

ChunkServerMetric::ChunkServerMetric()
    : hasInited_(false)
    , readMetric_(nullptr)
    , writeMetric_(nullptr)
    , leaderCount_(nullptr)
    , chunkLeft_(nullptr) {}

ChunkServerMetric* ChunkServerMetric::self_ = nullptr;

ChunkServerMetric* ChunkServerMetric::GetInstance() {
    // chunkserver metric 在chunkserver启动时初始化创建
    // 因此创建的时候不会存在竞争，不需要锁保护
    if (self_ == nullptr) {
        self_ = new ChunkServerMetric;
    }
    return self_;
}

int ChunkServerMetric::Init(const ChunkServerMetricOptions& option) {
    if (hasInited_) {
        LOG(WARNING) << "chunkserver metric has inited.";
        return 0;
    }
    option_ = option;

    if (!option_.collectMetric) {
        LOG(WARNING) << "chunkserver collect metric option is off.";
        hasInited_ = true;
        return 0;
    }

    // 初始化io统计项metric
    std::string readPrefix = Prefix() + "_read";
    std::string writePrefix = Prefix() + "_write";
    readMetric_ = std::make_shared<IOMetric>();
    writeMetric_ = std::make_shared<IOMetric>();
    if (readMetric_->Init(readPrefix) != 0) {
        LOG(ERROR) << "Init read metric failed."
                   << " prefix = " << readPrefix;
        return -1;
    }
    if (writeMetric_->Init(writePrefix) != 0) {
        LOG(ERROR) << "Init write metric failed."
                   << " prefix = " << writePrefix;
        return -1;
    }

    std::string leaderCountPrefix = Prefix() + "_leader_count";
    leaderCount_ = std::make_shared<bvar::Adder<uint32_t>>(leaderCountPrefix);

    hasInited_ = true;
    LOG(INFO) << "Init chunkserver metric success.";
    return 0;
}

int ChunkServerMetric::Fini() {
    // 释放资源，从而将暴露的metric从全局的map中移除
    readMetric_ = nullptr;
    writeMetric_ = nullptr;
    leaderCount_ = nullptr;
    chunkLeft_ = nullptr;
    copysetMetricMap_.clear();
    configMetric_.clear();
    hasInited_ = false;
    return 0;
}

int ChunkServerMetric::CreateCopysetMetric(const LogicPoolID& logicPoolId,
                                           const CopysetID& copysetId) {
    if (!option_.collectMetric) {
        return 0;
    }

    GroupId groupId = ToGroupId(logicPoolId, copysetId);
    WriteLockGuard lockGuard(rwLock_);
    auto it = copysetMetricMap_.find(groupId);
    if (it != copysetMetricMap_.end()) {
        LOG(ERROR) << "Create Copyset ("
                   << logicPoolId << "," << copysetId << ")"
                   << " metric failed : is already exists.";
        return -1;
    }

    CopysetMetricPtr copysetMetric = std::make_shared<CSCopysetMetric>();
    int ret = copysetMetric->Init(logicPoolId, copysetId);
    if (ret < 0) {
        LOG(ERROR) << "Create Copyset ("
                   << logicPoolId << "," << copysetId << ")"
                   << " metric failed : init failed.";
        return -1;
    }
    copysetMetricMap_[groupId] = copysetMetric;
    return 0;
}

CopysetMetricPtr ChunkServerMetric::GetCopysetMetric(
    const LogicPoolID& logicPoolId, const CopysetID& copysetId) {
    if (!option_.collectMetric) {
        return nullptr;
    }

    GroupId groupId = ToGroupId(logicPoolId, copysetId);
    ReadLockGuard lockGuard(rwLock_);
    auto it = copysetMetricMap_.find(groupId);
    if (it == copysetMetricMap_.end()) {
        return nullptr;
    }
    return it->second;
}

int ChunkServerMetric::RemoveCopysetMetric(const LogicPoolID& logicPoolId,
                                           const CopysetID& copysetId) {
    GroupId groupId = ToGroupId(logicPoolId, copysetId);
    WriteLockGuard lockGuard(rwLock_);
    auto it = copysetMetricMap_.find(groupId);
    if (it != copysetMetricMap_.end()) {
        copysetMetricMap_.erase(it);
    }
    return 0;
}

void ChunkServerMetric::OnRequestWrite(const LogicPoolID& logicPoolId,
                                       const CopysetID& copysetId) {
    if (!option_.collectMetric) {
        return;
    }

    CopysetMetricPtr cpMetric = GetCopysetMetric(logicPoolId, copysetId);
    if (cpMetric != nullptr) {
        cpMetric->OnRequestWrite();
    }
    if (writeMetric_ != nullptr) {
        writeMetric_->OnRequest();
    }
}

void ChunkServerMetric::OnRequestRead(const LogicPoolID& logicPoolId,
                                      const CopysetID& copysetId) {
    if (!option_.collectMetric) {
        return;
    }

    CopysetMetricPtr cpMetric = GetCopysetMetric(logicPoolId, copysetId);
    if (cpMetric != nullptr) {
        cpMetric->OnRequestRead();
    }
    if (readMetric_ != nullptr) {
        readMetric_->OnRequest();
    }
}


void ChunkServerMetric::OnResponseWrite(const LogicPoolID& logicPoolId,
                                        const CopysetID& copysetId,
                                        size_t size,
                                        int64_t latUs,
                                        bool hasError) {
    if (!option_.collectMetric) {
        return;
    }

    CopysetMetricPtr cpMetric = GetCopysetMetric(logicPoolId, copysetId);
    if (cpMetric != nullptr) {
        cpMetric->OnResponseWrite(size, latUs, hasError);
    }
    if (writeMetric_ != nullptr) {
        writeMetric_->OnResponse(size, latUs, hasError);
    }
}

void ChunkServerMetric::OnResponseRead(const LogicPoolID& logicPoolId,
                                        const CopysetID& copysetId,
                                        size_t size,
                                        int64_t latUs,
                                        bool hasError) {
    if (!option_.collectMetric) {
        return;
    }

    CopysetMetricPtr cpMetric = GetCopysetMetric(logicPoolId, copysetId);
    if (cpMetric != nullptr) {
        cpMetric->OnResponseRead(size, latUs, hasError);
    }
    if (readMetric_ != nullptr) {
        readMetric_->OnResponse(size, latUs, hasError);
    }
}

void ChunkServerMetric::MonitorChunkFilePool(ChunkfilePool* chunkfilePool) {
    if (!option_.collectMetric) {
        return;
    }

    std::string chunkLeftPrefix = Prefix() + "_chunkfilepool_left";
    chunkLeft_ = std::make_shared<bvar::PassiveStatus<uint32_t>>(
        chunkLeftPrefix, getChunkLeftFunc, chunkfilePool);
}

void ChunkServerMetric::IncreaseLeaderCount() {
    if (!option_.collectMetric) {
        return;
    }

    *leaderCount_ << 1;
}

void ChunkServerMetric::DecreaseLeaderCount() {
    if (!option_.collectMetric) {
        return;
    }

    *leaderCount_ << -1;
}

void ChunkServerMetric::UpdateConfigMetric(const common::Configuration& conf) {
    if (!option_.collectMetric) {
        return;
    }

    std::string prefix = Prefix() + "_config";
    std::map<std::string, std::string> configs = conf.ListConfig();
    for (auto& config : configs) {
        std::string configKey = config.first;
        std::string configValue = config.second;
        auto it = configMetric_.find(configKey);
        // 如果配置项不存在，则新建配置项
        if (it == configMetric_.end()) {
            ConfigItemPtr configItem =
                std::make_shared<bvar::Status<std::string>>(prefix,
                                                            configKey,
                                                            nullptr);
            configMetric_[configKey] = configItem;
        }
        // 更新配置项
        configMetric_[configKey]->set_value(configValue);
    }
}

}  // namespace chunkserver
}  // namespace curve

