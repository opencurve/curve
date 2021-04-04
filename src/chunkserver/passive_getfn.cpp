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
 * Created Date: Tuesday June 18th 2019
 * Author: yangyaokai
 */

#include "src/chunkserver/passive_getfn.h"

#include <memory>
#include <vector>

namespace curve {
namespace chunkserver {

uint32_t GetChunkLeftFunc(void* arg) {
    FilePool* chunkFilePool = reinterpret_cast<FilePool*>(arg);
    uint32_t chunkLeft = 0;
    if (chunkFilePool != nullptr) {
        FilePoolState poolState = chunkFilePool->GetState();
        chunkLeft = poolState.preallocatedChunksLeft;
    }
    return chunkLeft;
}

uint32_t GetWalSegmentLeftFunc(void* arg) {
    FilePool* walFilePool = reinterpret_cast<FilePool*>(arg);
    uint32_t segmentLeft = 0;
    if (walFilePool != nullptr) {
        FilePoolState poolState = walFilePool->GetState();
        segmentLeft = poolState.preallocatedChunksLeft;
    }
    return segmentLeft;
}

uint32_t GetDatastoreChunkCountFunc(void* arg) {
    CSDataStore* dataStore = reinterpret_cast<CSDataStore*>(arg);
    uint32_t chunkCount = 0;
    if (dataStore != nullptr) {
        DataStoreStatus status = dataStore->GetStatus();
        chunkCount = status.chunkFileCount;
    }
    return chunkCount;
}

uint32_t GetLogStorageWalSegmentCountFunc(void* arg) {
    CurveSegmentLogStorage* logStorage
        = reinterpret_cast<CurveSegmentLogStorage*>(arg);
    uint32_t walSegmentCount = 0;
    if (nullptr != logStorage) {
        walSegmentCount = logStorage->GetStatus().walSegmentFileCount;
    }
    return walSegmentCount;
}

uint32_t GetDatastoreSnapshotCountFunc(void* arg) {
    CSDataStore* dataStore = reinterpret_cast<CSDataStore*>(arg);
    uint32_t snapshotCount = 0;
    if (dataStore != nullptr) {
        DataStoreStatus status = dataStore->GetStatus();
        snapshotCount = status.snapshotCount;
    }
    return snapshotCount;
}

uint32_t GetDatastoreCloneChunkCountFunc(void* arg) {
    CSDataStore* dataStore = reinterpret_cast<CSDataStore*>(arg);
    uint32_t cloneChunkCount  = 0;
    if (dataStore != nullptr) {
        DataStoreStatus status = dataStore->GetStatus();
        cloneChunkCount = status.cloneChunkCount;
    }
    return cloneChunkCount;
}

uint32_t GetChunkTrashedFunc(void* arg) {
    Trash* trash = reinterpret_cast<Trash*>(arg);
    uint32_t chunkTrashed = 0;
    if (trash != nullptr) {
        chunkTrashed = trash->GetChunkNum();
    }
    return chunkTrashed;
}

uint32_t GetTotalChunkCountFunc(void* arg) {
    uint32_t chunkCount = 0;
    ChunkServerMetric* csMetric = reinterpret_cast<ChunkServerMetric*>(arg);
    auto copysetMetricMap = csMetric->GetCopysetMetricMap()->GetMap();
    for (auto metricPair : copysetMetricMap) {
        chunkCount += metricPair.second->GetChunkCount();
    }
    return chunkCount;
}

uint32_t GetTotalWalSegmentCountFunc(void* arg) {
    uint32_t walSegmentCount = 0;
    ChunkServerMetric* csMetric = reinterpret_cast<ChunkServerMetric*>(arg);
    auto copysetMetricMap = csMetric->GetCopysetMetricMap()->GetMap();
    for (auto metricPair : copysetMetricMap) {
        walSegmentCount += metricPair.second->GetWalSegmentCount();
    }
    return walSegmentCount;
}

uint32_t GetTotalSnapshotCountFunc(void* arg) {
    uint32_t snapshotCount = 0;
    ChunkServerMetric* csMetric = reinterpret_cast<ChunkServerMetric*>(arg);
    auto copysetMetricMap = csMetric->GetCopysetMetricMap()->GetMap();
    for (auto metricPair : copysetMetricMap) {
        snapshotCount += metricPair.second->GetSnapshotCount();
    }
    return snapshotCount;
}

uint32_t GetTotalCloneChunkCountFunc(void* arg) {
    uint32_t cloneChunkCount = 0;
    ChunkServerMetric* csMetric = reinterpret_cast<ChunkServerMetric*>(arg);
    auto copysetMetricMap = csMetric->GetCopysetMetricMap()->GetMap();
    for (auto metricPair : copysetMetricMap) {
        cloneChunkCount += metricPair.second->GetCloneChunkCount();
    }
    return cloneChunkCount;
}

}  // namespace chunkserver
}  // namespace curve
