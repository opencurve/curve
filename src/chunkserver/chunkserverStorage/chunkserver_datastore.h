/*
 * Project: curve
 * File Created: Wednesday, 5th September 2018 8:04:38 pm
 * Author: tongguangxun
 * Copyright (c) 2018 NetEase
 */

#ifndef SRC_CHUNKSERVER_CHUNKSERVERSTORAGE_CHUNKSERVER_DATASTORE_H_
#define SRC_CHUNKSERVER_CHUNKSERVERSTORAGE_CHUNKSERVER_DATASTORE_H_

#include <string>
#include <unordered_map>

#include "include/curve_compiler_specific.h"
#include "include/chunkserver/chunkserver_common.h"
#include "src/chunkserver/chunkserverStorage/chunkserver_datastore_executor.h"
#include "src/chunkserver/chunkserverStorage/chunkserver_datastore_interface.h"

namespace curve {
namespace chunkserver {
class CURVE_CACHELINE_ALIGNMENT CSDataStore : public CSDataStoreInterface {
 public:
    CSDataStore();
    ~CSDataStore();

    using CSDataStoreExecutorPtr  = std::shared_ptr<CSDataStoreExecutor>;
    /**
     * chunk operation
     */
    bool DeleteChunk(ChunkID id) override;
    // return int tell caller, the buf length
    bool ReadChunk(ChunkID id,
                    char * buf,
                    off_t offset,
                    size_t* length) override;
    bool WriteChunk(ChunkID id,
                    const char * buf,
                    off_t offset,
                    size_t length) override;
    int AioReadChunk(ChunkID id,
                    char * buf,
                    off_t offset,
                    size_t length,
                    std::function<void(void*)> callback) override;
    int AioWriteChunk(ChunkID id,
                    char * buf,
                    off_t offset,
                    size_t length,
                    std::function<void(void*)> callback) override;

    int ValidateChunk(ChunkID id) override;

    /**
     * snapshort operation
     */
    bool CreateSnapshot(ChunkID cid, SnapshotID sid) override;
    bool DeleteSnapshot(ChunkID cid, SnapshotID sid) override;
    int ReadSnapshot(ChunkID cid,
                    SnapshotID sid,
                    char* buff,
                    off_t offset,
                    size_t length) override;
    bool RevertSnapshot2ID(ChunkID cid, SnapshotID targetID) override;

    bool Initialize(std::shared_ptr<CSSfsAdaptor> fsadaptor,
                    std::string copysetdir);
    void UnInitialize();

 private:
    bool InitInternal(std::shared_ptr<CSSfsAdaptor> fsadaptor,
                      std::string copysetdir);
    void UnInitInternal();
    inline std::string ChunkID2FileName(ChunkID cid);

 private:
    bool        isInited_;
    std::string copysetDir_;
    std::string deviceID_;
    std::string storagePrefix_;
    std::shared_ptr<CSSfsAdaptor>   sfsptr_;
    std::unordered_map<ChunkID, CSDataStoreExecutorPtr>   copysetStoragePool_;
};

}  // namespace chunkserver
}  // namespace curve

#endif  // SRC_CHUNKSERVER_CHUNKSERVERSTORAGE_CHUNKSERVER_DATASTORE_H_
