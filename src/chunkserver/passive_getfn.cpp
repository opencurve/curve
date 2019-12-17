/*
 * Project: curve
 * Created Date: Tuesday June 18th 2019
 * Author: yangyaokai
 * Copyright (c) 2019 netease
 */

#include "src/chunkserver/passive_getfn.h"

#include <memory>
#include <vector>

namespace curve {
namespace chunkserver {

uint32_t GetChunkLeftFunc(void* arg) {
    ChunkfilePool* chunkfilePool = reinterpret_cast<ChunkfilePool*>(arg);
    uint32_t chunkLeft = 0;
    if (chunkfilePool != nullptr) {
        ChunkFilePoolState poolState = chunkfilePool->GetState();
        chunkLeft = poolState.preallocatedChunksLeft;
    }
    return chunkLeft;
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
    CopysetNodeManager* nodeMgr = &CopysetNodeManager::GetInstance();
    std::vector<std::shared_ptr<CopysetNode>> nodes;
    nodeMgr->GetAllCopysetNodes(&nodes);
    for (auto node : nodes) {
        auto dataStore = node->GetDataStore();
        chunkCount += GetDatastoreChunkCountFunc(dataStore.get());
    }
    return chunkCount;
}

uint32_t GetTotalSnapshotCountFunc(void* arg) {
    uint32_t snapshotCount = 0;
    CopysetNodeManager* nodeMgr = &CopysetNodeManager::GetInstance();
    std::vector<std::shared_ptr<CopysetNode>> nodes;
    nodeMgr->GetAllCopysetNodes(&nodes);
    for (auto node : nodes) {
        auto dataStore = node->GetDataStore();
        snapshotCount += GetDatastoreSnapshotCountFunc(dataStore.get());
    }
    return snapshotCount;
}

uint32_t GetTotalCloneChunkCountFunc(void* arg) {
    uint32_t cloneChunkCount = 0;
    CopysetNodeManager* nodeMgr = &CopysetNodeManager::GetInstance();
    std::vector<std::shared_ptr<CopysetNode>> nodes;
    nodeMgr->GetAllCopysetNodes(&nodes);
    for (auto node : nodes) {
        auto dataStore = node->GetDataStore();
        cloneChunkCount += GetDatastoreCloneChunkCountFunc(dataStore.get());
    }
    return cloneChunkCount;
}

}  // namespace chunkserver
}  // namespace curve
