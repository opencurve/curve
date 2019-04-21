/*************************************************************************
> File Name: snapshot_data_store_s3.h
> Author:
> Created Time: Fri Dec 14 18:28:10 2018
> Copyright (c) 2018 netease
 ************************************************************************/

#ifndef SRC_SNAPSHOTCLONESERVER_SNAPSHOT_SNAPSHOT_DATA_STORE_S3_H_
#define SRC_SNAPSHOTCLONESERVER_SNAPSHOT_SNAPSHOT_DATA_STORE_S3_H_

#include <map>
#include <vector>
#include <list>
#include <string>
#include "src/snapshotcloneserver/snapshot/snapshot_data_store.h"
#include "src/common/s3_adapter.h"

using ::curve::common::S3Adapter;
namespace curve {
namespace snapshotcloneserver {

class S3SnapshotDataStore : public SnapshotDataStore {
 public:
     S3SnapshotDataStore() {
        s3Adapter_ = std::make_shared<S3Adapter>();
     }
    ~S3SnapshotDataStore() {}
    int Init(const std::string &path) override;
    int PutChunkIndexData(const ChunkIndexDataName &name,
                        const ChunkIndexData &meta) override;
    int GetChunkIndexData(const ChunkIndexDataName &name,
                          ChunkIndexData *meta) override;
    int DeleteChunkIndexData(const ChunkIndexDataName &name) override;
    bool ChunkIndexDataExist(const ChunkIndexDataName &name) override;
    // int PutChunkData(const ChunkDataName &name,
    //                const ChunkData &data) override;
    // int GetChunkData(const ChunkDataName &name,
    //                ChunkData *data) override;
    int DeleteChunkData(const ChunkDataName &name) override;
    bool ChunkDataExist(const ChunkDataName &name) override;
/*  nos暂时不支持，后续增加
    int SetSnapshotFlag(const ChunkIndexDataName &name, int flag) override;
    int GetSnapshotFlag(const ChunkIndexDataName &name) override;
*/
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
     void SetAdapter(std::shared_ptr<S3Adapter> adapter) {
         s3Adapter_ = adapter;
     }
     std::shared_ptr<S3Adapter> GetAdapter(void) {
         return s3Adapter_;
     }

 private:
    std::shared_ptr<curve::common::S3Adapter> s3Adapter_;
};

}   // namespace snapshotcloneserver
}   // namespace curve

#endif  // SRC_SNAPSHOTCLONESERVER_SNAPSHOT_SNAPSHOT_DATA_STORE_S3_H_
