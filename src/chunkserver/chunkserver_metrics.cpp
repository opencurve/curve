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
 * Created Date: Monday June 10th 2019
 * Author: yangyaokai
 */

#include "src/chunkserver/chunkserver_metrics.h"
#include <vector>
#include <map>

#include "src/chunkserver/copyset_node_manager.h"
#include "src/chunkserver/passive_getfn.h"

namespace curve {
namespace chunkserver {

IOMetric::IOMetric()
    : rps_(&reqNum_, 1), iops_(&ioNum_, 1), eps_(&errorNum_, 1),
      bps_(&ioBytes_, 1) {}

IOMetric::~IOMetric() {}

int IOMetric::Init(const std::string &prefix) {
    // Expose all metrics
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

void IOMetric::OnRequest() { reqNum_ << 1; }

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


int CSIOMetric::Init(const std::string &prefix) {
    // Initialize IO statistics item metric
    std::string readPrefix = prefix + "_read";
    std::string writePrefix = prefix + "_write";
    std::string recoverPrefix = prefix + "_recover";
    std::string pastePrefix = prefix + "_paste";
    std::string downloadPrefix = prefix + "_download";
    readMetric_ = std::make_shared<IOMetric>();
    writeMetric_ = std::make_shared<IOMetric>();
    recoverMetric_ = std::make_shared<IOMetric>();
    pasteMetric_ = std::make_shared<IOMetric>();
    downloadMetric_ = std::make_shared<IOMetric>();
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
    if (recoverMetric_->Init(recoverPrefix) != 0) {
        LOG(ERROR) << "Init recover metric failed."
                   << " prefix = " << recoverPrefix;
        return -1;
    }
    if (pasteMetric_->Init(pastePrefix) != 0) {
        LOG(ERROR) << "Init paste metric failed."
                   << " prefix = " << pastePrefix;
        return -1;
    }
    if (downloadMetric_->Init(downloadPrefix) != 0) {
        LOG(ERROR) << "Init download metric failed."
                   << " prefix = " << downloadPrefix;
        return -1;
    }
    return 0;
}

void CSIOMetric::Fini() {
    readMetric_ = nullptr;
    writeMetric_ = nullptr;
    recoverMetric_ = nullptr;
    pasteMetric_ = nullptr;
    downloadMetric_ = nullptr;
}

void CSIOMetric::OnRequest(CSIOMetricType type) {
    IOMetricPtr ioMetric = GetIOMetric(type);
    if (ioMetric != nullptr) {
        ioMetric->OnRequest();
    }
}

void CSIOMetric::OnResponse(CSIOMetricType type, size_t size, int64_t latUs,
                            bool hasError) {
    IOMetricPtr ioMetric = GetIOMetric(type);
    if (ioMetric != nullptr) {
        ioMetric->OnResponse(size, latUs, hasError);
    }
}

IOMetricPtr CSIOMetric::GetIOMetric(CSIOMetricType type) {
    IOMetricPtr result = nullptr;
    switch (type) {
    case CSIOMetricType::READ_CHUNK:
        result = readMetric_;
        break;
    case CSIOMetricType::WRITE_CHUNK:
        result = writeMetric_;
        break;
    case CSIOMetricType::RECOVER_CHUNK:
        result = recoverMetric_;
        break;
    case CSIOMetricType::PASTE_CHUNK:
        result = pasteMetric_;
        break;
    case CSIOMetricType::DOWNLOAD:
        result = downloadMetric_;
        break;
    default:
        result = nullptr;
        break;
    }
    return result;
}

int CSCopysetMetric::Init(const LogicPoolID &logicPoolId,
                          const CopysetID &copysetId) {
    logicPoolId_ = logicPoolId;
    copysetId_ = copysetId;
    int ret = ioMetrics_.Init(Prefix());
    if (ret < 0) {
        LOG(ERROR) << "Init Copyset (" << logicPoolId << "," << copysetId << ")"
                   << " metric failed.";
        return -1;
    }
    return 0;
}

void CSCopysetMetric::MonitorDataStore(CSDataStore *datastore) {
    std::string chunkCountPrefix = Prefix() + "_chunk_count";
    std::string snapshotCountPrefix = Prefix() + "snapshot_count";
    std::string cloneChunkCountPrefix = Prefix() + "_clonechunk_count";
    chunkCount_ = std::make_shared<bvar::PassiveStatus<uint32_t>>(
        chunkCountPrefix, GetDatastoreChunkCountFunc, datastore);
    snapshotCount_ = std::make_shared<bvar::PassiveStatus<uint32_t>>(
        snapshotCountPrefix, GetDatastoreSnapshotCountFunc, datastore);
    cloneChunkCount_ = std::make_shared<bvar::PassiveStatus<uint32_t>>(
        cloneChunkCountPrefix, GetDatastoreCloneChunkCountFunc, datastore);
}

void CSCopysetMetric::MonitorCurveSegmentLogStorage(
    CurveSegmentLogStorage *logStorage) {
    std::string walSegmentCountPrefix = Prefix() + "_walsegment_count";
    walSegmentCount_ = std::make_shared<bvar::PassiveStatus<uint32_t>>(
        walSegmentCountPrefix, GetLogStorageWalSegmentCountFunc, logStorage);
}

ChunkServerMetric::ChunkServerMetric()
    : hasInited_(false), leaderCount_(nullptr), chunkLeft_(nullptr),
      walSegmentLeft_(nullptr), chunkTrashed_(nullptr), chunkCount_(nullptr),
      walSegmentCount_(nullptr), snapshotCount_(nullptr),
      cloneChunkCount_(nullptr) {}

ChunkServerMetric *ChunkServerMetric::self_ = nullptr;

ChunkServerMetric *ChunkServerMetric::GetInstance() {
    // Chunkserver metric initializes creation when chunkserver starts
    // Therefore, there will be no competition during creation and lock protection is not required
    if (self_ == nullptr) {
        self_ = new ChunkServerMetric;
    }
    return self_;
}

int ChunkServerMetric::Init(const ChunkServerMetricOptions &option) {
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

    // Initialize IO statistics item metric
    int ret = ioMetrics_.Init(Prefix());
    if (ret < 0) {
        LOG(ERROR) << "Init chunkserver metric failed.";
        return -1;
    }

    // Initialize resource statistics
    std::string leaderCountPrefix = Prefix() + "_leader_count";
    leaderCount_ = std::make_shared<bvar::Adder<uint32_t>>(leaderCountPrefix);

    std::string chunkCountPrefix = Prefix() + "_chunk_count";
    chunkCount_ = std::make_shared<bvar::PassiveStatus<uint32_t>>(
        chunkCountPrefix, GetTotalChunkCountFunc, this);

    std::string walSegmentCountPrefix = Prefix() + "_walsegment_count";
    walSegmentCount_ = std::make_shared<bvar::PassiveStatus<uint32_t>>(
        walSegmentCountPrefix, GetTotalWalSegmentCountFunc, this);

    std::string snapshotCountPrefix = Prefix() + "_snapshot_count";
    snapshotCount_ = std::make_shared<bvar::PassiveStatus<uint32_t>>(
        snapshotCountPrefix, GetTotalSnapshotCountFunc, this);

    std::string cloneChunkCountPrefix = Prefix() + "_clonechunk_count";
    cloneChunkCount_ = std::make_shared<bvar::PassiveStatus<uint32_t>>(
        cloneChunkCountPrefix, GetTotalCloneChunkCountFunc, this);

    hasInited_ = true;
    LOG(INFO) << "Init chunkserver metric success.";
    return 0;
}

int ChunkServerMetric::Fini() {
    // Release resources to remove exposed metrics from the global map
    ioMetrics_.Fini();
    leaderCount_ = nullptr;
    chunkLeft_ = nullptr;
    walSegmentLeft_ = nullptr;
    chunkTrashed_ = nullptr;
    chunkCount_ = nullptr;
    snapshotCount_ = nullptr;
    cloneChunkCount_ = nullptr;
    walSegmentCount_ = nullptr;
    copysetMetricMap_.Clear();
    hasInited_ = false;
    return 0;
}

int ChunkServerMetric::CreateCopysetMetric(const LogicPoolID &logicPoolId,
                                           const CopysetID &copysetId) {
    if (!option_.collectMetric) {
        return 0;
    }

    GroupId groupId = ToGroupId(logicPoolId, copysetId);
    bool exist = copysetMetricMap_.Exist(groupId);
    if (exist) {
        LOG(ERROR) << "Create Copyset (" << logicPoolId << "," << copysetId
                   << ")"
                   << " metric failed : is already exists.";
        return -1;
    }

    CopysetMetricPtr copysetMetric = std::make_shared<CSCopysetMetric>();
    int ret = copysetMetric->Init(logicPoolId, copysetId);
    if (ret < 0) {
        LOG(ERROR) << "Create Copyset (" << logicPoolId << "," << copysetId
                   << ")"
                   << " metric failed : init failed.";
        return -1;
    }

    copysetMetricMap_.Add(groupId, copysetMetric);
    return 0;
}

CopysetMetricPtr
ChunkServerMetric::GetCopysetMetric(const LogicPoolID &logicPoolId,
                                    const CopysetID &copysetId) {
    if (!option_.collectMetric) {
        return nullptr;
    }

    GroupId groupId = ToGroupId(logicPoolId, copysetId);
    return copysetMetricMap_.Get(groupId);
}

int ChunkServerMetric::RemoveCopysetMetric(const LogicPoolID &logicPoolId,
                                           const CopysetID &copysetId) {
    GroupId groupId = ToGroupId(logicPoolId, copysetId);
    // Save the copyset metric here first, and then release it after removing it
    // Prevent operating metrics within read write locks, resulting in deadlocks
    auto metric = copysetMetricMap_.Get(groupId);
    copysetMetricMap_.Remove(groupId);
    return 0;
}

void ChunkServerMetric::OnRequest(const LogicPoolID &logicPoolId,
                                  const CopysetID &copysetId,
                                  CSIOMetricType type) {
    if (!option_.collectMetric) {
        return;
    }

    CopysetMetricPtr cpMetric = GetCopysetMetric(logicPoolId, copysetId);
    if (cpMetric != nullptr) {
        cpMetric->OnRequest(type);
    }
    ioMetrics_.OnRequest(type);
}

void ChunkServerMetric::OnResponse(const LogicPoolID &logicPoolId,
                                   const CopysetID &copysetId,
                                   CSIOMetricType type, size_t size,
                                   int64_t latUs, bool hasError) {
    if (!option_.collectMetric) {
        return;
    }

    CopysetMetricPtr cpMetric = GetCopysetMetric(logicPoolId, copysetId);
    if (cpMetric != nullptr) {
        cpMetric->OnResponse(type, size, latUs, hasError);
    }
    ioMetrics_.OnResponse(type, size, latUs, hasError);
}

void ChunkServerMetric::MonitorChunkFilePool(FilePool *chunkFilePool) {
    if (!option_.collectMetric) {
        return;
    }

    std::string chunkLeftPrefix = Prefix() + "_chunkfilepool_left";
    chunkLeft_ = std::make_shared<bvar::PassiveStatus<uint32_t>>(
        chunkLeftPrefix, GetChunkLeftFunc, chunkFilePool);
}

void ChunkServerMetric::MonitorWalFilePool(FilePool *walFilePool) {
    if (!option_.collectMetric) {
        return;
    }

    std::string walSegmentLeftPrefix = Prefix() + "_walfilepool_left";
    walSegmentLeft_ = std::make_shared<bvar::PassiveStatus<uint32_t>>(
        walSegmentLeftPrefix, GetWalSegmentLeftFunc, walFilePool);
}

void ChunkServerMetric::MonitorTrash(Trash *trash) {
    if (!option_.collectMetric) {
        return;
    }

    std::string chunkTrashedPrefix = Prefix() + "_chunk_trashed";
    chunkTrashed_ = std::make_shared<bvar::PassiveStatus<uint32_t>>(
        chunkTrashedPrefix, GetChunkTrashedFunc, trash);
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

void ChunkServerMetric::ExposeConfigMetric(common::Configuration *conf) {
    if (!option_.collectMetric) {
        return;
    }

    std::string exposeName = Prefix() + "_config";
    conf->ExposeMetric(exposeName);
}

}  // namespace chunkserver
}  // namespace curve
