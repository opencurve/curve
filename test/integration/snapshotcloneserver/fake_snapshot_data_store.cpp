/*
 * Project: curve
 * Created Date: Wed Dec 04 2019
 * Author: xuchaojie
 * Copyright (c) 2019 netease
 */

#include <fiu-control.h>
#include <fiu.h>

#include <memory>

#include "test/integration/snapshotcloneserver/fake_snapshot_data_store.h"

namespace curve {
namespace snapshotcloneserver {

int FakeSnapshotDataStore::Init(const std::string &path) {
    return 0;
}

int FakeSnapshotDataStore::PutChunkIndexData(const ChunkIndexDataName &name,
        const ChunkIndexData &meta) {
    fiu_return_on(
        "test/integration/snapshotcloneserver/FakeSnapshotDataStore.PutChunkIndexData", -1);  // NOLINT
    indexDataMap_.emplace(name.ToIndexDataChunkKey(), meta);
    return 0;
}

int FakeSnapshotDataStore::GetChunkIndexData(const ChunkIndexDataName &name,
        ChunkIndexData *meta) {
    fiu_return_on(
        "test/integration/snapshotcloneserver/FakeSnapshotDataStore.GetChunkIndexData", -1);  // NOLINT
    std::string key = name.ToIndexDataChunkKey();
    *meta = indexDataMap_[key];
    return 0;
}

int FakeSnapshotDataStore::DeleteChunkIndexData(
    const ChunkIndexDataName &name) {
    fiu_return_on(
        "test/integration/snapshotcloneserver/FakeSnapshotDataStore.DeleteChunkIndexData", -1);  // NOLINT
    std::string key = name.ToIndexDataChunkKey();
    indexDataMap_.erase(key);
    return 0;
}

bool FakeSnapshotDataStore::ChunkIndexDataExist(
    const ChunkIndexDataName &name) {
    std::string key = name.ToIndexDataChunkKey();
    return indexDataMap_.find(key) != indexDataMap_.end();
}

int FakeSnapshotDataStore::DeleteChunkData(const ChunkDataName &name) {
    fiu_return_on(
        "test/integration/snapshotcloneserver/FakeSnapshotDataStore.DeleteChunkData", -1);  // NOLINT
    chunkData_.erase(name.ToDataChunkKey());
    return 0;
}

bool FakeSnapshotDataStore::ChunkDataExist(const ChunkDataName &name) {
    return chunkData_.find(name.ToDataChunkKey()) != chunkData_.end();
}

int FakeSnapshotDataStore::DataChunkTranferInit(const ChunkDataName &name,
        std::shared_ptr<TransferTask> task) {
    return 0;
}

int FakeSnapshotDataStore::DataChunkTranferAddPart(const ChunkDataName &name,
        std::shared_ptr<TransferTask> task,
        int partNum,
        int partSize,
        const char* buf) {
    return 0;
}

int FakeSnapshotDataStore::DataChunkTranferComplete(const ChunkDataName &name,
        std::shared_ptr<TransferTask> task) {
    fiu_return_on(
        "test/integration/snapshotcloneserver/FakeSnapshotDataStore.DataChunkTranferComplete", -1);  // NOLINT
    chunkData_.insert(name.ToDataChunkKey());
    return 0;
}

int FakeSnapshotDataStore::DataChunkTranferAbort(const ChunkDataName &name,
        std::shared_ptr<TransferTask> task) {
    return 0;
}

}  // namespace snapshotcloneserver
}  // namespace curve
