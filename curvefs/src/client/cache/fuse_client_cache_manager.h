/*
 *  Copyright (c) 2021 NetEase Inc.
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
 * Created Date: 21-8-18
 * Author: huyao
 */
#ifndef CURVEFS_SRC_CLIENT_CACHE_FUSE_CLIENT_CACHE_MANAGER_H_
#define CURVEFS_SRC_CLIENT_CACHE_FUSE_CLIENT_CACHE_MANAGER_H_

#include <algorithm>
#include <cstring>
#include <list>
#include <map>
#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>
#include <set>

#include "curvefs/proto/metaserver.pb.h"
#include "curvefs/src/client/error_code.h"
#include "curvefs/src/client/s3/client_s3.h"
#include "src/common/concurrent/concurrent.h"
#include "src/common/concurrent/task_thread_pool.h"
#include "curvefs/src/client/kvclient/kvclient_manager.h"
#include "curvefs/src/client/inode_wrapper.h"

using curve::common::ReadLockGuard;
using curve::common::RWLock;
using curve::common::WriteLockGuard;

namespace curvefs {
namespace client {

class StorageAdaptor;
class ChunkCacheManager;
class FileCacheManager;
class FsCacheManager;
class DataCache;
using FileCacheManagerPtr = std::shared_ptr<FileCacheManager>;
using ChunkCacheManagerPtr = std::shared_ptr<ChunkCacheManager>;
using DataCachePtr = std::shared_ptr<DataCache>;
using WeakDataCachePtr = std::weak_ptr<DataCache>;
using curve::common::TaskThreadPool;
using curvefs::metaserver::Inode;
using curvefs::metaserver::ChunkInfo;
using curvefs::metaserver::ChunkInfoList;

enum CacheType { Write = 1, Read = 2 };

enum class CachePolicy {
    NCache,
    RCache,
    WRCache,
};

struct ReadRequest {
    uint64_t index;
    uint64_t chunkPos;
    uint64_t len;
    uint64_t bufOffset;

    std::string DebugString() const {
        std::ostringstream os;
        os << "ReadRequest ( index = " << index << ", chunkPos = " << chunkPos
           << ", len = " << len << ", bufOffset = " << bufOffset << " )";
        return os.str();
    }
};

struct UperFlushRequest {
    uint64_t inodeId;
    const char *buf;
    uint64_t length;
    uint64_t offset;   // offset at inode
    uint64_t chunkId;
    uint64_t chunkPos;
    bool sync;
};

struct  UperReadRequest {
    std::vector<ReadRequest> requests;
    uint64_t inodeId;
    std::shared_ptr<InodeWrapper> inodeWapper;
    char *buf;
};

struct ObjectChunkInfo {
    // ChunkInfo ChunkInfo;
    ChunkInfo chunkInfo;
    uint64_t objectOffset;  // s3 object's begin in the block
};

struct PageData {
    uint64_t index;
    char *data;
};
using PageDataMap = std::map<uint64_t, PageData *>;

enum DataCacheStatus {
    Dirty = 1,
    Flush = 2,
};

class DataCache : public std::enable_shared_from_this<DataCache> {
 public:
    DataCache(StorageAdaptor *s3ClientAdaptor,
              ChunkCacheManagerPtr chunkCacheManager, uint64_t chunkPos,
              uint64_t len, const char *data,
              std::shared_ptr<KVClientManager> kvClientManager);
    virtual ~DataCache() {
        auto iter = dataMap_.begin();
        for (; iter != dataMap_.end(); iter++) {
            auto pageIter = iter->second.begin();
            for (; pageIter != iter->second.end(); pageIter++) {
                delete[] pageIter->second->data;
                delete pageIter->second;
            }
        }
    }

    virtual void Write(uint64_t chunkPos, uint64_t len, const char *data,
               const std::vector<DataCachePtr> &mergeDataCacheVer);
    virtual void Truncate(uint64_t size);
    uint64_t GetChunkPos() { return chunkPos_; }
    uint64_t GetLen() { return len_; }
    PageData *GetPageData(uint64_t blockIndex, uint64_t pageIndex) {
        PageDataMap &pdMap = dataMap_[blockIndex];
        if (pdMap.count(pageIndex)) {
            return pdMap[pageIndex];
        }
        return nullptr;
    }

    void ErasePageData(uint64_t blockIndex, uint64_t pageIndex) {
        curve::common::LockGuard lg(mtx_);
        PageDataMap &pdMap = dataMap_[blockIndex];
        auto iter = pdMap.find(pageIndex);
        if (iter != pdMap.end()) {
            pdMap.erase(iter);
        }
        if (pdMap.empty()) {
            dataMap_.erase(blockIndex);
        }
    }

    uint64_t GetActualLen() { return actualLen_; }

    virtual CURVEFS_ERROR Flush(uint64_t inodeId, bool toS3 = false);
    void Release();
    bool IsDirty() {
        return status_.load(std::memory_order_acquire) ==
               DataCacheStatus::Dirty;
    }
    virtual bool CanFlush(bool force);
    bool InReadCache() const {
        return inReadCache_.load(std::memory_order_acquire);
    }

    void SetReadCacheState(bool inCache) {
        inReadCache_.store(inCache, std::memory_order_release);
    }

    void Lock() {
        mtx_.lock();
    }

    void UnLock() {
        mtx_.unlock();
    }
    void CopyDataCacheToBuf(uint64_t offset, uint64_t len, char *data);
    void MergeDataCacheToDataCache(DataCachePtr mergeDataCache,
                                   uint64_t dataOffset, uint64_t len);

 private:
    void PrepareChunkInfo(uint64_t chunkId, uint64_t offset,
        uint64_t len, ChunkInfo *info);
    void CopyBufToDataCache(uint64_t dataCachePos, uint64_t len,
                             const char *data);
    void AddDataBefore(uint64_t len, const char *data);

    CachePolicy GetCachePolicy(bool toS3);

 private:
    StorageAdaptor *storageAdaptor_;
    ChunkCacheManagerPtr chunkCacheManager_;
    uint64_t chunkPos_;  // useful chunkPos
    uint64_t len_;  // useful len
    uint64_t actualChunkPos_;  // after alignment the actual chunkPos
    uint64_t actualLen_;  // after alignment the actual len
    curve::common::Mutex mtx_;
    uint64_t createTime_;
    std::atomic<int> status_;
    std::atomic<bool> inReadCache_;
    std::map<uint64_t, PageDataMap> dataMap_;  // first is block index

    std::shared_ptr<KVClientManager> kvClientManager_;
};

class ChunkCacheManager
    : public std::enable_shared_from_this<ChunkCacheManager> {
 public:
    ChunkCacheManager(uint64_t index, StorageAdaptor *s3ClientAdaptor,
                      std::shared_ptr<KVClientManager> kvClientManager)
        : index_(index), storageAdaptor_(s3ClientAdaptor),
          flushingDataCache_(nullptr),
          kvClientManager_(std::move(kvClientManager)) {}
    virtual ~ChunkCacheManager() = default;
    virtual void ReadChunk(uint64_t index, uint64_t chunkPos, uint64_t readLen,
                   char *dataBuf, uint64_t dataBufOffset,
                   std::vector<ReadRequest> *requests);
    virtual void WriteNewDataCache(StorageAdaptor *s3ClientAdaptor,
                                   uint32_t chunkPos, uint32_t len,
                                   const char *data);
    virtual void AddReadDataCache(DataCachePtr dataCache);
    virtual DataCachePtr
    FindWriteableDataCache(uint64_t pos, uint64_t len,
                           std::vector<DataCachePtr> *mergeDataCacheVer,
                           uint64_t inodeId);
    virtual void ReadByWriteCache(uint64_t chunkPos, uint64_t readLen,
                                  char *dataBuf, uint64_t dataBufOffset,
                                  std::vector<ReadRequest> *requests);
    virtual void ReadByReadCache(uint64_t chunkPos, uint64_t readLen,
                                 char *dataBuf, uint64_t dataBufOffset,
                                 std::vector<ReadRequest> *requests);
    virtual void ReadByFlushData(uint64_t chunkPos, uint64_t readLen,
                                 char *dataBuf, uint64_t dataBufOffset,
                                 std::vector<ReadRequest> *requests);
    virtual CURVEFS_ERROR Flush(uint64_t inodeId, bool force,
                                bool toS3 = false);
    uint64_t GetIndex() { return index_; }
    bool IsEmpty() {
        ReadLockGuard writeCacheLock(rwLockChunk_);
        return (dataWCacheMap_.empty() && dataRCacheMap_.empty());
    }
    virtual void ReleaseReadDataCache(uint64_t key);
    virtual void ReleaseCache();
    void TruncateCache(uint64_t chunkPos);
    void UpdateWriteCacheMap(uint64_t oldChunkPos, DataCache *dataCache);
    // for unit test
    void AddWriteDataCacheForTest(DataCachePtr dataCache);
    void ReleaseCacheForTest() {
        {
            WriteLockGuard writeLockGuard(rwLockWrite_);
            dataWCacheMap_.clear();
        }
        WriteLockGuard writeLockGuard(rwLockRead_);
        dataRCacheMap_.clear();
    }
 public:
    RWLock rwLockChunk_;  //  for read write chunk
    RWLock rwLockWrite_;  //  for dataWCacheMap_

 private:
    void ReleaseWriteDataCache(const DataCachePtr &dataCache);
    void TruncateWriteCache(uint64_t chunkPos);
    void TruncateReadCache(uint64_t chunkPos);
    bool IsFlushDataEmpty() {
        return flushingDataCache_ == nullptr;
    }
 private:
    uint64_t index_;
    std::map<uint64_t, DataCachePtr> dataWCacheMap_;  // first is pos in chunk
    std::map<uint64_t, std::list<DataCachePtr>::iterator>
        dataRCacheMap_;  // first is pos in chunk

    RWLock rwLockRead_;  //  for read cache
    StorageAdaptor *storageAdaptor_;
    curve::common::Mutex flushMtx_;
    DataCachePtr flushingDataCache_;
    curve::common::Mutex flushingDataCacheMtx_;

    std::shared_ptr<KVClientManager> kvClientManager_;
};

class FileCacheManager {
 public:
    FileCacheManager(uint32_t fsid, uint64_t inode,
      StorageAdaptor *s3ClientAdaptor,
      std::shared_ptr<KVClientManager> kvClientManager,
      std::shared_ptr<TaskThreadPool<>> threadPool)
        : fsId_(fsid), inode_(inode), storageAdaptor_(s3ClientAdaptor),
          kvClientManager_(std::move(kvClientManager)),
          readTaskPool_(threadPool) {}
    FileCacheManager() = default;
    ~FileCacheManager() = default;

    ChunkCacheManagerPtr FindOrCreateChunkCacheManager(uint64_t index);

    void ReleaseCache();

    virtual void TruncateCache(uint64_t offset, uint64_t fileSize);

    virtual CURVEFS_ERROR Flush(bool force, bool toS3 = false);

    virtual int Write(uint64_t offset, uint64_t length, const char *dataBuf);

    virtual int Read(uint64_t inodeId, uint64_t offset, uint64_t length,
                     char *dataBuf);

    // for test

    bool IsEmpty() { return chunkCacheMap_.empty(); }

    uint64_t GetInodeId() const { return inode_; }

    void SetChunkCacheManagerForTest(uint64_t index,
                                     ChunkCacheManagerPtr chunkCacheManager) {
        WriteLockGuard writeLockGuard(rwLock_);

        auto ret = chunkCacheMap_.emplace(index, chunkCacheManager);
        assert(ret.second);
        (void)ret;
    }

 private:
    void WriteChunk(uint64_t index, uint64_t chunkPos, uint64_t writeLen,
                    const char *dataBuf);

    // GetChunkLoc: get chunk info according to offset
    void GetChunkLoc(uint64_t offset, uint64_t *index, uint64_t *chunkPos,
                     uint64_t *chunkSize);

    // GetBlockLoc: get block info according to offset
    void GetBlockLoc(uint64_t offset, uint64_t *chunkIndex, uint64_t *chunkPos,
                     uint64_t *blockIndex, uint64_t *blockPos);

    // read data from memory read/write cache
    void ReadFromMemCache(uint64_t offset, uint64_t length, char *dataBuf,
                          uint64_t *actualReadLen,
                          std::vector<ReadRequest> *memCacheMissRequest);

/*  whs
    enum class ReadStatus {
        OK = 0,
        S3_READ_FAIL = -1,
        S3_NOT_EXIST = -2,
    };

    ReadStatus toReadStatus(const int retCode) {
        ReadStatus st = ReadStatus::OK;
        if (retCode < 0) {
            st = (retCode == -2) ? ReadStatus::S3_NOT_EXIST
                                 : ReadStatus::S3_READ_FAIL;
        }
        return st;
    }
*/
 private:
    friend class AsyncPrefetchCallback;

    uint64_t fsId_;
    uint64_t inode_;
    std::map<uint64_t, ChunkCacheManagerPtr> chunkCacheMap_;  // first is index
    RWLock rwLock_;
    curve::common::Mutex mtx_;
    StorageAdaptor *storageAdaptor_;

    std::shared_ptr<KVClientManager> kvClientManager_;
    std::shared_ptr<TaskThreadPool<>> readTaskPool_;
};

class FsCacheManager {
 public:
    FsCacheManager(StorageAdaptor *s3ClientAdaptor,
                   uint64_t readCacheMaxByte, uint64_t writeCacheMaxByte,
                   uint32_t readCacheThreads,
                   std::shared_ptr<KVClientManager> kvClientManager)
        : lruByte_(0), wDataCacheNum_(0), wDataCacheByte_(0),
          readCacheMaxByte_(readCacheMaxByte),
          writeCacheMaxByte_(writeCacheMaxByte),
          storageAdaptor_(s3ClientAdaptor), isWaiting_(false),
          kvClientManager_(std::move(kvClientManager)) {
            LOG(INFO) << "whs read task pool start. " << readCacheThreads;
            readTaskPool_->Start(readCacheThreads);
            LOG(INFO) << "whs read task pool start end";
    }

    FsCacheManager() = default;
    virtual ~FsCacheManager() {
        LOG(INFO) << "whs read task pool stop";
        readTaskPool_->Stop();
        LOG(INFO) << "whs read task pool stop end";
    }

    virtual FileCacheManagerPtr FindFileCacheManager(uint64_t inodeId);
    virtual FileCacheManagerPtr FindOrCreateFileCacheManager(uint64_t fsId,
                                                     uint64_t inodeId);
    void ReleaseFileCacheManager(uint64_t inodeId);

    bool Set(DataCachePtr dataCache,
             std::list<DataCachePtr>::iterator *outIter);
    bool Delete(std::list<DataCachePtr>::iterator iter);
    void Get(std::list<DataCachePtr>::iterator iter);

    CURVEFS_ERROR FsSync(bool force);
    uint64_t GetDataCacheNum() {
        return wDataCacheNum_.load(std::memory_order_relaxed);
    }

    virtual uint64_t GetDataCacheSize() {
        return wDataCacheByte_.load(std::memory_order_relaxed);
    }

    virtual uint64_t GetDataCacheMaxSize() {
        return writeCacheMaxByte_;
    }

    void WaitFlush() {
        std::unique_lock<std::mutex> lk(mutex_);
        isWaiting_ = true;
        cond_.wait(lk);
    }

    void FlushSignal() {
        std::lock_guard<std::mutex> lk(mutex_);
        if (isWaiting_) {
            isWaiting_ = false;
            cond_.notify_all();
        }
    }

    bool WriteCacheIsFull() {
        if (writeCacheMaxByte_ <= 0)
            return true;
        return wDataCacheByte_.load(std::memory_order_relaxed) >
               writeCacheMaxByte_;
    }

    virtual uint64_t MemCacheRatio() {
        return 100 * wDataCacheByte_.load(std::memory_order_relaxed) /
               writeCacheMaxByte_;
    }

    uint64_t GetLruByte() {
        std::lock_guard<std::mutex> lk(lruMtx_);
        return lruByte_;
    }

    void SetFileCacheManagerForTest(uint64_t inodeId,
                                    FileCacheManagerPtr fileCacheManager) {
        WriteLockGuard writeLockGuard(rwLock_);

        auto ret = fileCacheManagerMap_.emplace(inodeId, fileCacheManager);
        assert(ret.second);
        (void)ret;
    }

    std::shared_ptr<TaskThreadPool<>> GetReadTaskPool() {
        return readTaskPool_;
    }

    void DataCacheNumInc();
    void DataCacheNumFetchSub(uint64_t v);
    void DataCacheByteInc(uint64_t v);
    void DataCacheByteDec(uint64_t v);

 private:
    class ReadCacheReleaseExecutor {
     public:
        ReadCacheReleaseExecutor();
        ~ReadCacheReleaseExecutor();

        void Stop();

        void Release(std::list<DataCachePtr>* caches);

     private:
        void ReleaseCache();

     private:
        std::mutex mtx_;
        std::condition_variable cond_;
        std::list<DataCachePtr> retired_;
        std::atomic<bool> running_;
        std::thread t_;
    };

 private:
    std::unordered_map<uint64_t, FileCacheManagerPtr>
        fileCacheManagerMap_;  // first is inodeid
    RWLock rwLock_;
    std::mutex lruMtx_;

    std::list<DataCachePtr> lruReadDataCacheList_;
    uint64_t lruByte_;
    std::atomic<uint64_t> wDataCacheNum_;
    std::atomic<uint64_t> wDataCacheByte_;
    uint64_t readCacheMaxByte_;
    uint64_t writeCacheMaxByte_;
    StorageAdaptor *storageAdaptor_;
    bool isWaiting_;
    std::mutex mutex_;
    std::condition_variable cond_;

    ReadCacheReleaseExecutor releaseReadCache_;

    std::shared_ptr<KVClientManager> kvClientManager_;

    std::shared_ptr<TaskThreadPool<>> readTaskPool_ =
        std::make_shared<TaskThreadPool<>>();
};

}  // namespace client
}  // namespace curvefs

#endif  // CURVEFS_SRC_CLIENT_CACHE_FUSE_CLIENT_CACHE_MANAGER_H_
