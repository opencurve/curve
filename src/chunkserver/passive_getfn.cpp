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

uint32_t getChunkLeftFunc(void* arg) {
    ChunkfilePool* chunkfilePool = reinterpret_cast<ChunkfilePool*>(arg);
    uint32_t chunkLeft = 0;
    if (chunkfilePool != nullptr) {
        ChunkFilePoolState poolState = chunkfilePool->GetState();
        chunkLeft = poolState.preallocatedChunksLeft;
    }
    return chunkLeft;
}

uint32_t getDatastoreChunkCountFunc(void* arg) {
    CSDataStore* dataStore = reinterpret_cast<CSDataStore*>(arg);
    uint32_t chunkCount = 0;
    if (dataStore != nullptr) {
        DataStoreStatus status = dataStore->GetStatus();
        chunkCount = status.chunkFileCount;
    }
    return chunkCount;
}

uint32_t getDatastoreSnapshotCountFunc(void* arg) {
    CSDataStore* dataStore = reinterpret_cast<CSDataStore*>(arg);
    uint32_t snapshotCount = 0;
    if (dataStore != nullptr) {
        DataStoreStatus status = dataStore->GetStatus();
        snapshotCount = status.snapshotCount;
    }
    return snapshotCount;
}

uint32_t getDatastoreCloneChunkCountFunc(void* arg) {
    CSDataStore* dataStore = reinterpret_cast<CSDataStore*>(arg);
    uint32_t cloneChunkCount  = 0;
    if (dataStore != nullptr) {
        DataStoreStatus status = dataStore->GetStatus();
        cloneChunkCount = status.cloneChunkCount;
    }
    return cloneChunkCount;
}

uint32_t getChunkTrashedFunc(void* arg) {
    Trash* trash = reinterpret_cast<Trash*>(arg);
    uint32_t chunkTrashed = 0;
    if (trash != nullptr) {
        chunkTrashed = trash->GetChunkNum();
    }
    return chunkTrashed;
}

}  // namespace chunkserver
}  // namespace curve
