/*
 * Project: curve
 * File Created: Thursday, 6th September 2018 10:49:30 am
 * Author: tongguangxun
 * Copyright (c) 2018 NetEase
 */
#ifndef SRC_CHUNKSERVER_CHUNKSERVERSTORAGE_CHUNKSERVER_DATASTORE_EXECUTOR_H_
#define SRC_CHUNKSERVER_CHUNKSERVERSTORAGE_CHUNKSERVER_DATASTORE_EXECUTOR_H_

#include <string>
#include <atomic>
#include <functional>

#include "include/curve_compiler_specific.h"
#include "include/chunkserver/chunkserver_common.h"
#include "src/chunkserver/chunkserverStorage/chunkserver_sfs_adaptor.h"

namespace curve {
namespace chunkserver {
class CURVE_CACHELINE_ALIGNMENT CSDataStoreExecutor {
 public:
    CSDataStoreExecutor(std::shared_ptr<CSSfsAdaptor>, std::string path);
    ~CSDataStoreExecutor();

    /**
     * chunk operation
     */
    bool DeleteChunk();
    bool ReadChunk(char * buf,
                    off_t offset,
                    size_t* length);
    bool WriteChunk(const char * buf,
                    off_t offset,
                    size_t length);
    int AioReadChunk(char * buf,
                    off_t offset,
                    size_t length,
                    std::function<void(void*)> callback);
    int AioWriteChunk(char * buf,
                    off_t offset,
                    size_t length,
                    std::function<void(void*)> callback);

    int ValidateChunk();

    bool CreateSnapshot(SnapshotID sid);
    bool DeleteSnapshot(SnapshotID sid);
    int ReadSnapshot(SnapshotID sid,
                    char* buff,
                    off_t offset,
                    size_t length);
    bool RevertSnapshot2ID(SnapshotID targetID);

 private:
    CSSfsAdaptor::fd_t      fd_;
    std::string             filePath_;
    std::shared_ptr<CSSfsAdaptor> lfs_;
};
}  // namespace chunkserver
}  // namespace curve

#endif  // SRC_CHUNKSERVER_CHUNKSERVERSTORAGE_CHUNKSERVER_DATASTORE_EXECUTOR_H_
