/*
 * Project: curve
 * Created Date: Wed Dec 04 2019
 * Author: xuchaojie
 * Copyright (c) 2019 netease
 */

#ifndef TEST_INTEGRATION_SNAPSHOTCLONESERVER_FAKE_SNAPSHOT_DATA_STORE_H_
#define TEST_INTEGRATION_SNAPSHOTCLONESERVER_FAKE_SNAPSHOT_DATA_STORE_H_

#include <string>
#include <set>
#include <map>
#include <memory>

#include "src/snapshotcloneserver/snapshot/snapshot_data_store.h"

namespace curve {
namespace snapshotcloneserver {

class FakeSnapshotDataStore : public SnapshotDataStore {
 public:
    int Init(const std::string &path) override;
    int PutChunkIndexData(const ChunkIndexDataName &name,
                        const ChunkIndexData &meta) override;
    int GetChunkIndexData(const ChunkIndexDataName &name,
                          ChunkIndexData *meta) override;
    int DeleteChunkIndexData(const ChunkIndexDataName &name) override;
    bool ChunkIndexDataExist(const ChunkIndexDataName &name) override;

    int DeleteChunkData(const ChunkDataName &name) override;
    bool ChunkDataExist(const ChunkDataName &name) override;

    int DataChunkTranferInit(const ChunkDataName &name,
                            std::shared_ptr<TransferTask> task) override;
    int DataChunkTranferAddPart(const ChunkDataName &name,
                                        std::shared_ptr<TransferTask> task,
                                        int partNum,
                                        int partSize,
                                        const char* buf) override;
    int DataChunkTranferComplete(const ChunkDataName &name,
                                std::shared_ptr<TransferTask> task) override;
    int DataChunkTranferAbort(const ChunkDataName &name,
                               std::shared_ptr<TransferTask> task) override;

 private:
    std::map<std::string, ChunkIndexData> indexDataMap_;
    std::set<std::string> chunkData_;
};

}  // namespace snapshotcloneserver
}  // namespace curve

#endif  // TEST_INTEGRATION_SNAPSHOTCLONESERVER_FAKE_SNAPSHOT_DATA_STORE_H_
