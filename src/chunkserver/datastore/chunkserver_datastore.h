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
 * File Created: Wednesday, 5th September 2018 8:04:38 pm
 * Author: yangyaokai
 */

#ifndef SRC_CHUNKSERVER_DATASTORE_CHUNKSERVER_DATASTORE_H_
#define SRC_CHUNKSERVER_DATASTORE_CHUNKSERVER_DATASTORE_H_

#include <bvar/bvar.h>
#include <glog/logging.h>
#include <butil/iobuf.h>
#include <string>
#include <vector>
#include <unordered_map>
#include <memory>
#include <condition_variable>

#include "include/curve_compiler_specific.h"
#include "include/chunkserver/chunkserver_common.h"
#include "src/common/concurrent/rw_lock.h"
#include "src/common/concurrent/concurrent.h"
#include "src/chunkserver/datastore/define.h"
#include "src/chunkserver/datastore/chunkserver_chunkfile.h"
#include "src/chunkserver/datastore/file_pool.h"
#include "src/fs/local_filesystem.h"

namespace curve {
namespace chunkserver {
using curve::fs::LocalFileSystem;
using ::curve::common::Atomic;
using CSChunkFilePtr = std::shared_ptr<CSChunkFile>;
using CSSnapshotPtr = std::shared_ptr<CSSnapshot>;

inline void TrivialDeleter(void* ptr) {}

/**
 * DataStore configuration parameters
 * baseDir: Directory path managed by DataStore
 * chunkSize: The size of the chunk file or snapshot file in the DataStore
 * blockSize: the size of the smallest read-write unit
 * metaPageSize: meta page size for chunk
 */
struct DataStoreOptions {
    std::string                         baseDir;
    ChunkSizeType                       chunkSize;
    ChunkSizeType                       blockSize;
    PageSizeType                        metaPageSize;
    uint32_t                            locationLimit;
    bool                                enableOdsyncWhenOpenChunkFile;
};

/**
 * The internal state of the DataStore, used to return to the upper layer
 * chunkFileCount: the number of chunks in the DataStore
 * snapshotCount: the number of snapshots in the DataStore
 * cloneChunkCount: the number of clone chunks
 */
struct DataStoreStatus {
    uint32_t chunkFileCount;
    uint32_t snapshotCount;
    uint32_t cloneChunkCount;
    DataStoreStatus() : chunkFileCount(0)
                    , snapshotCount(0)
                    , cloneChunkCount(0) {}
};

/**
 * Internal status information of DataStore
 * chunkFileCount: the number of chunks in the DataStore
 * snapshotCount: the number of snapshots in the DataStore
 * cloneChunkCount: the number of clone chunks
 */
struct DataStoreMetric {
    bvar::Adder<uint32_t> chunkFileCount;
    bvar::Adder<uint32_t> snapshotCount;
    bvar::Adder<uint32_t> cloneChunkCount;
};
using DataStoreMetricPtr = std::shared_ptr<DataStoreMetric>;

using ChunkMap = std::unordered_map<ChunkID, CSChunkFilePtr>;
// For the mapping from chunkid to chunkfile,
// use read-write lock to protect the map operation
class CSMetaCache {
 public:
    CSMetaCache() : cvar_(nullptr),
        sumChunkRate_(std::make_shared<std::atomic<uint64_t>>()) {}
    virtual ~CSMetaCache() {}

    ChunkMap GetMap() {
        ReadLockGuard readGuard(rwLock_);
        return chunkMap_;
    }

    CSChunkFilePtr Get(ChunkID id) {
        ReadLockGuard readGuard(rwLock_);
        if (chunkMap_.find(id) == chunkMap_.end()) {
            return nullptr;
        }
        return chunkMap_[id];
    }

    CSChunkFilePtr Set(ChunkID id, CSChunkFilePtr chunkFile) {
        WriteLockGuard writeGuard(rwLock_);
       // When two write requests are concurrently created to create a chunk
       // file, return the first set chunkFile
        if (chunkMap_.find(id) == chunkMap_.end()) {
            chunkFile->SetSyncInfo(sumChunkRate_, cvar_);
            chunkMap_[id] = chunkFile;
        }
        return chunkMap_[id];
    }

    void Remove(ChunkID id) {
        WriteLockGuard writeGuard(rwLock_);
        if (chunkMap_.find(id) != chunkMap_.end()) {
            chunkMap_.erase(id);
        }
    }

    void Clear() {
        WriteLockGuard writeGuard(rwLock_);
        chunkMap_.clear();
    }

    void SetCondPtr(std::shared_ptr<std::condition_variable> cond) {
        cvar_ = cond;
    }

    void SetSyncChunkLimits(const uint64_t limits, const uint64_t threshold) {
        CSChunkFile::syncChunkLimits_ = limits;
        CSChunkFile::syncThreshold_ = threshold;
    }

 private:
    std::shared_ptr<std::condition_variable> cvar_;
    // sum of all chunks rate
    std::shared_ptr<std::atomic<uint64_t>> sumChunkRate_;
    RWLock      rwLock_;
    ChunkMap    chunkMap_;
};

using CloneMap = std::map<uint64_t, CSChunkFilePtr>;

struct CloneMapInfo {
    RWLock rwLock_;
    CloneMap map_;
};

using CloneMapInfoPtr = std::shared_ptr<CloneMapInfo>;
using ChunkCloneMap = std::unordered_map<ChunkID, CloneMapInfoPtr>;


class CSMetaCloneCache {

public:
    CSMetaCloneCache() {}
    virtual ~CSMetaCloneCache() {}

    ChunkCloneMap& GetMap() {
        ReadLockGuard readGuard(rwLock_);
        return chunkCloneMap_;
    }

    CSChunkFilePtr Get(ChunkID id, uint64_t cloneNo) {
        ReadLockGuard readGuard(rwLock_);
        auto it = chunkCloneMap_.find(id);
        if (it == chunkCloneMap_.end()) {
            return nullptr;
        }

        CloneMapInfoPtr cinfo = it->second;
        ReadLockGuard c_readGuard(cinfo->rwLock_);
        auto iter = cinfo->map_.find(cloneNo);
        if (iter == cinfo->map_.end()) {
            return nullptr;
        }

        return iter->second;
    }

    CSChunkFilePtr Set(ChunkID id, uint64_t cloneNo, CSChunkFilePtr cfile) {
        CloneMapInfoPtr cinfo = nullptr;
        WriteLockGuard wGuard(rwLock_);
        auto iter = chunkCloneMap_.find(id);
        if (iter == chunkCloneMap_.end()) {
            cinfo = std::make_shared<CloneMapInfo>();
            chunkCloneMap_[id] = cinfo;
        } else {
            cinfo = iter->second;
        }

        WriteLockGuard wCGuard(cinfo->rwLock_);
        auto it = cinfo->map_.find(cloneNo);

        if (it == cinfo->map_.end()) {
            cinfo->map_.insert(std::pair<uint64_t, CSChunkFilePtr>(cloneNo, cfile));
        } else {
            it->second = nullptr;
            it->second = cfile;
        }

        return cinfo->map_[cloneNo];
    }

    void Remove(ChunkID id, uint64_t cloneNo) {
        CloneMapInfoPtr cinfo = nullptr;
        WriteLockGuard writeGuard(rwLock_);
        auto it = chunkCloneMap_.find(id);
        if (it != chunkCloneMap_.end()) {
            cinfo = it->second;
            WriteLockGuard wCGuard(cinfo->rwLock_);
            auto iter = cinfo->map_.find(cloneNo);
            if (iter != cinfo->map_.end()) {
                cinfo->map_.erase(iter);
            }
        }
    }

    void Remove(ChunkID id) {
        WriteLockGuard writeGuard(rwLock_);
        if (chunkCloneMap_.find(id) != chunkCloneMap_.end()) {
            chunkCloneMap_.erase(id);
        }
    }

    void Clear() {
        WriteLockGuard writeGuard(rwLock_);
        chunkCloneMap_.clear();
    }

private:
    RWLock rwLock_;
    ChunkCloneMap chunkCloneMap_;

};

struct CloneListInfo {
    ChunkID chunkid;
    uint64_t cloneNo;
};

//used to speed the on_snapshot_save process
//just record the cloneno and the chunkid list
class CSCloneFileMap {
public:
    CSCloneFileMap() {}
    virtual ~CSCloneFileMap() {}

    void Insert(ChunkID chunkid, uint64_t cloneNo) {
        WriteLockGuard writeGuard(rwLock_);
        cloneFileMap_[chunkid] = cloneNo;
    }

    void Remove(ChunkID chunkid) {
        WriteLockGuard writeGuard(rwLock_);
        if (cloneFileMap_.find(chunkid) != cloneFileMap_.end()) {
            cloneFileMap_.erase(chunkid);
        }
    }

    void Clear() {
        WriteLockGuard writeGuard(rwLock_);
        cloneFileMap_.clear();
    }

    //get std::vector<CloneListInfo> from cloneFileMap_
    void GetCloneListInfo(std::vector<CloneListInfo>& chunklist) {
        ReadLockGuard readGuard(rwLock_);
        chunklist.reserve(cloneFileMap_.size());
        for (auto it = cloneFileMap_.begin(); it != cloneFileMap_.end(); it++) {
            CloneListInfo info;
            info.chunkid = it->first;
            info.cloneNo = it->second;
            chunklist.push_back(info);
        }
        return;
    }

private:
    RWLock rwLock_;
    std::map<uint64_t, uint64_t> cloneFileMap_; //chunkid --> cloneno
};

class CSDataStore {
 public:
    // for ut mock
    CSDataStore() {}

    CSDataStore(std::shared_ptr<LocalFileSystem> lfs,
                std::shared_ptr<FilePool> chunkFilePool,
                const DataStoreOptions& options);
    virtual ~CSDataStore();
    /**
     * Called when copyset is initialized
     * During initialization, all files in the current copyset directory are
     * traversed, metapage is read and loaded into metacache
     * @return: return true on success, false on failure
     */
    virtual bool Initialize();
    /**
     * Delete the current chunk file
     * @param id: the id of the chunk to be deleted
     * @param sn: used to record trace, if sn<chunk sn, delete is not allowed
     * @return: return error code
     */
    virtual CSErrorCode DeleteChunk(ChunkID id, SequenceNum sn, std::shared_ptr<SnapContext> ctx = nullptr);
    /**
     * Delete snapshots generated during this dump or before
     * If no snapshot is generated during the dump, modify the correctedSn
     * of the chunk
     * @param id: the chunk id of the snapshot to be deleted
     * @param correctedSn: the sequence number that needs to be corrected
     * If the snapshot does not exist, you need to modify the correctedSn
     * of the chunk to this parameter value
     * @return: return error code
     */
    virtual CSErrorCode DeleteSnapshotChunk(
        ChunkID id, SequenceNum snapSn, std::shared_ptr<SnapContext> ctx = nullptr);
    /**
     * Read the contents of the current chunk
     * @param id: the chunk id to be read
     * @param sn: used to record trace, not used in actual logic processing,
     *             indicating the sequence number of the current user file
     * @param buf: the content of the data read
     * @param offset: the logical offset of the data requested to be read in the chunk
     * @param length: the length of the data requested to be read
     * @return: return error code
     */
    virtual CSErrorCode ReadChunk(ChunkID id,
                                  SequenceNum sn,
                                  char * buf,
                                  off_t offset,
                                  size_t length);
    CSErrorCode ReadChunk(ChunkID id,
                                SequenceNum sn,
                                char * buf,
                                off_t offset,
                                size_t length,
                                std::unique_ptr<CloneContext>& ctx);

    /**
     * Read the data of the specified sequence, it may read the current
     * chunk file, or it may read the snapshot file
     * @param id: the chunk id to be read
     * @param sn: the sequence number of the chunk to be read
     * @param buf: the content of the data read
     * @param offset: the logical offset of the data requested
     *                to be read in the chunk
     * @param length: the length of the data requested to be read
     * @return: return error code
     */
    virtual CSErrorCode ReadSnapshotChunk(ChunkID id,
                                          SequenceNum sn,
                                          char * buf,
                                          off_t offset,
                                          size_t length,
                                          std::shared_ptr<SnapContext> ctx = nullptr);

    CSErrorCode ReadSnapshotChunk(ChunkID id,
                                SequenceNum sn,
                                char * buf,
                                off_t offset,
                                size_t length,
                                std::shared_ptr<SnapContext> ctx,
                                std::unique_ptr<CloneContext>& cloneCtx);

    /**
     * Write data
     * @param id: the chunk id to be written
     * @param sn: The sequence number of the user file when the current
     *            write request is issued
     * @param buf: the content of the data to be written
     * @param offset: the offset address requested to write
     * @param length: the length of the data requested to be written
     * @param cost: the actual number of IOs generated, used for QOS control
     * @param cloneSource: indicates the address of the clone from curvefs
     * @return: return error code
     */
    virtual CSErrorCode WriteChunk(ChunkID id,
                                SequenceNum sn,
                                const butil::IOBuf& buf,
                                off_t offset,
                                size_t length,
                                uint64_t chunkIndex,
                                uint64_t fileID,
                                uint32_t* cost,
                                std::shared_ptr<SnapContext> ctx,
                                const std::string & cloneSourceLocation = "");

    //WriteChunk interface for the clone chunk
    CSErrorCode WriteChunk (ChunkID id, 
                                SequenceNum sn,
                                const butil::IOBuf& buf, 
                                off_t offset, 
                                size_t length,
                                uint64_t chunkIndex,
                                uint64_t fileID,
                                uint32_t* cost, 
                                std::shared_ptr<SnapContext> ctx, 
                                std::unique_ptr<CloneContext>& cloneCtx);

    //FlattenChunk interface for the clone chunk
    CSErrorCode FlattenChunk (ChunkID id, SequenceNum sn,
                              off_t offset, size_t length,
                              std::unique_ptr<CloneContext>& cloneCtx);

    virtual CSErrorCode SyncChunk(ChunkID id);


    // Deprecated, only use for unit & integration test
    virtual CSErrorCode WriteChunk(
        ChunkID id, SequenceNum sn, const char *buf, off_t offset,
        size_t length, uint64_t chunkIndex, uint64_t fileID, uint32_t *cost,
        const std::string &cloneSourceLocation = "") {
        butil::IOBuf data;
        data.append_user_data(const_cast<char*>(buf), length, TrivialDeleter);

        return WriteChunk(id, sn, data, offset, length, chunkIndex, fileID, cost,
                          SnapContext::build_empty(), cloneSourceLocation);
    }

    // Deprecated, only use for unit & integration test
    virtual CSErrorCode WriteChunk(
        ChunkID id, SequenceNum sn, const char *buf, off_t offset,
        size_t length, uint64_t chunkIndex, uint64_t fileID, uint32_t *cost,std::shared_ptr<SnapContext> ctx,
        const std::string &cloneSourceLocation = "") {
        butil::IOBuf data;
        data.append_user_data(const_cast<char*>(buf), length, TrivialDeleter);

        return WriteChunk(id, sn, data, offset, length, chunkIndex, fileID, cost,
                          ctx, cloneSourceLocation);
    }

    /**
     * Create a cloned Chunk, record the data source location information
     * in the chunk
     * The interface needs to be idempotent, and repeated creation with the
     * same parameters will return success
     * If the Chunk already exists and the information of the Chunk does not
     * match the parameters, it will return failure
     * @param id: the chunk id to be created
     * @param sn: the sequence number of the chunk to be created
     * @param correctedSn: modify the correctedSn of the chunk
     * @param size: the chunk size to be created
     * @param location: data source location information
     * @return: return error code
     */
    virtual CSErrorCode CreateCloneChunk(ChunkID id,
                                         SequenceNum sn,
                                         SequenceNum correctedSn,
                                         ChunkSizeType size,
                                         const string& location);
    /**
     * Write the data copied from the source to the local without overwriting
     * the written data area
     * @param id: the chunk id to be written
     * @param buf: the content of the data to be written
     * @param offset: the offset address requested to write
     * @param length: the length of the data requested to be written
     * @return: return error code
     */
    virtual CSErrorCode PasteChunk(ChunkID id,
                                   const char* buf,
                                   off_t offset,
                                   size_t length);
    /**
     * Get detailed information about Chunk
     * @param id: the id of the chunk requested
     * @param chunkInfo: detailed information about chunk
     */
    virtual CSErrorCode GetChunkInfo(ChunkID id,
                                     CSChunkInfo* chunkInfo);

    CSErrorCode GetCloneInfo(ChunkID id, uint64_t& virtualId, uint64_t& cloneNo);

    void GetCloneInfoList(std::vector<CloneListInfo>& cloneInfoList) {
        cloneFileMap_.GetCloneListInfo(cloneInfoList);
    }

    /**
     * Get the hash value of Chunk
     * @param id[in]: chunk id
     * @param hash[out]: chunk hash value
     * @return: return error code
     */
    virtual CSErrorCode GetChunkHash(ChunkID id,
                                     off_t offset,
                                     size_t length,
                                     std::string* hash);
    /**
     * Get internal statistics of DataStore
     * @return: internal statistics of datastore
     */
    virtual DataStoreStatus GetStatus();

    void SetCacheCondPtr(std::shared_ptr<std::condition_variable> cond) {
        metaCache_.SetCondPtr(cond);
    }

    void SetCacheLimits(const uint64_t limit, const uint64_t threshold) {
        metaCache_.SetSyncChunkLimits(limit, threshold);
    }

    virtual ChunkMap GetChunkMap();

    static struct CloneInfos getParentClone (std::vector<struct CloneInfos>& clones, uint64_t cloneNo);
    static struct CloneInfos getParentClone (std::vector<struct CloneInfos>& clones, std::vector<struct CloneInfos>::iterator& ptr);

    static void searchChunkForObj (SequenceNum sn, 
                            std::vector<File_ObjectInfoPtr>& objInfos, 
                            uint32_t beginIndex, uint32_t endIndex, 
                            std::unique_ptr<CloneContext>& ctx,
                            CSDataStore& datastore,
                            bool isWrite);

    CSChunkFilePtr GetCloneCache(ChunkID virtualid, uint64_t cloneno);

    static CSErrorCode ReadByObjInfo (CSChunkFilePtr fileptr, char* buf, ObjectInfo& objinfo);

    static void SplitDataIntoObjs (SequenceNum sn,
                            std::vector<File_ObjectInfoPtr>& objInfos, 
                            off_t offset, 
                            size_t length,
                            std::unique_ptr<CloneContext>& ctx,
                            CSDataStore& datastore,
                            bool isWrite = false);


    CSErrorCode CreateChunkFile(const ChunkOptions & ops,
                                CSChunkFilePtr* chunkFile);
    
    CSChunkFilePtr GetChunkFile(ChunkID id);

 private:
    CSErrorCode loadChunkFile(ChunkID id);

 private:
    // The size of each chunk
    ChunkSizeType chunkSize_;
    // page size, which is the smallest atomic read and write unit
    ChunkSizeType blockSize_;
    PageSizeType metaPageSize_;
    // clone chunk location length limit
    uint32_t locationLimit_;
    // datastore management directory
    std::string baseDir_;
    // the mapping of chunkid->chunkfile
    CSMetaCache metaCache_;
    // the mapping of chunkid, clondNo -->chunkfile
    CSMetaCloneCache cloneCache_;
    // the mapping of chunkid --> cloneno
    CSCloneFileMap cloneFileMap_;
    // chunkfile pool, rely on this pool to create and recycle chunk files
    // or snapshot files
    std::shared_ptr<FilePool> chunkFilePool_;
    // local file system
    std::shared_ptr<LocalFileSystem> lfs_;
    // internal statistics of datastore
    DataStoreMetricPtr metric_;
    // enable O_DSYNC When Open ChunkFile
    bool enableOdsyncWhenOpenChunkFile_;
};

}  // namespace chunkserver
}  // namespace curve

#endif  // SRC_CHUNKSERVER_DATASTORE_CHUNKSERVER_DATASTORE_H_
