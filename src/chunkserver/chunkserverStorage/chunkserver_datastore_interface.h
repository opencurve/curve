/*
 * Project: curve
 * File Created: Wednesday, 5th September 2018 8:03:38 pm
 * Author: tongguangxun
 * Copyright (c) 2018 NetEase
 */

#ifndef CURVE_CHUNKSERVER_CHUNKSERVER_DATASTORE_INTERFACE_H
#define CURVE_CHUNKSERVER_CHUNKSERVER_DATASTORE_INTERFACE_H

#include <string>
#include <functional>

#include "src/chunkserver/chunkserverStorage/chunkserver_adaptor_util.h"
#include "include/chunkserver/chunkserver_common.h"

namespace curve {
namespace chunkserver {

class CSDataStoreInterface {
 public:
    CSDataStoreInterface() {}

    virtual ~CSDataStoreInterface() {}

    /**
     * chunk operation
     */
    virtual bool DeleteChunk(ChunkID id) = 0;
    virtual bool ReadChunk(ChunkID id,
                            char * buf,
                            off_t offset,
                            size_t* length) = 0;
    virtual bool WriteChunk(ChunkID id,
                            const char * buf,
                            off_t offset,
                            size_t length) = 0;
    virtual int AioReadChunk(ChunkID id,
                            char * buf,
                            off_t offset,
                            size_t length,
                            std::function<void(void*)> callback) = 0;
    virtual int AioWriteChunk(ChunkID id,
                            char * buf,
                            off_t offset,
                            size_t length,
                            std::function<void(void*)> callback) = 0;
    virtual int ValidateChunk(ChunkID id) = 0;

    /**
     * snapshort operation
     */
    virtual bool CreateSnapshot(ChunkID cid, SnapshotID sid) = 0;
    virtual bool DeleteSnapshot(ChunkID cid, SnapshotID sid) = 0;
    virtual int ReadSnapshot(ChunkID cid,
                            SnapshotID sid,
                            char* buff,
                            off_t offset,
                            size_t length) = 0;
    virtual bool RevertSnapshot2ID(ChunkID cid, SnapshotID targetID) = 0;
};
}  // namespace chunkserver
}  // namespace curve

#endif  // ! CURVE_CHUNKSERVER_CHUNKSERVER_DATASTORE_INTERFACE_H
