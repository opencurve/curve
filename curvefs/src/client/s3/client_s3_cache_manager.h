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
#ifndef CURVEFS_SRC_CLIENT_S3_CLIENT_S3_CACHE_MANAGER_H_
#define CURVEFS_SRC_CLIENT_S3_CLIENT_S3_CACHE_MANAGER_H_

#include <algorithm>
#include <cstring>
#include <list>
#include <map>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>
#include <set>
#include <utility>

#include "curvefs/proto/metaserver.pb.h"
#include "curvefs/src/client/error_code.h"
#include "curvefs/src/client/s3/client_s3.h"
#include "curvefs/src/client/common/common.h"
#include "src/common/concurrent/concurrent.h"
#include "src/common/timeutility.h"
#include "curvefs/src/client/metric/client_metric.h"
#include "curvefs/src/client/kvclient/kvclient_manager.h"
#include "curvefs/src/client/kvclient/kvclient.h"

using curve::common::ReadLockGuard;
using curve::common::RWLock;
using curve::common::WriteLockGuard;

namespace curvefs {
namespace client {

class S3ClientAdaptorImpl;
class ChunkCacheManager;
class FileCacheManager;
class FsCacheManager;
class DataCache;
class S3ReadRequest;
using FileCacheManagerPtr = std::shared_ptr<FileCacheManager>;
using ChunkCacheManagerPtr = std::shared_ptr<ChunkCacheManager>;
using KvClientManagerPtr = std::unique_ptr<KvClientManager<KvClient>>;
using DataCachePtr = std::shared_ptr<DataCache>;
using WeakDataCachePtr = std::weak_ptr<DataCache>;
using curve::common::GetObjectAsyncCallBack;
using curve::common::PutObjectAsyncCallBack;
using curve::common::S3Adapter;
using curvefs::metaserver::Inode;
using curvefs::metaserver::S3ChunkInfo;
using curvefs::metaserver::S3ChunkInfoList;

enum CacheType { Write = 1, Read = 2 };
struct ReadRequest {
    uint64_t index;
    uint64_t chunkPos;
    uint64_t len;
    uint64_t bufOffset;
};

struct S3ReadRequest {
    uint64_t chunkId;
    uint64_t offset;  // file offset
    uint64_t len;
    uint64_t objectOffset;  // s3 object's begin in the block
    uint64_t readOffset;    // read buf offset
    uint64_t fsId;
    uint64_t inodeId;
    uint64_t compaction;
};

struct ObjectChunkInfo {
    S3ChunkInfo s3ChunkInfo;
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
    DataCache(S3ClientAdaptorImpl *s3ClientAdaptor,
              KvClientManagerPtr kvClientManagerPtr,
              ChunkCacheManagerPtr chunkCacheManager, uint64_t chunkPos,
              uint64_t len, const char *data);
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
    void PrepareS3ChunkInfo(uint64_t chunkId, uint64_t offset,
        uint64_t len, S3ChunkInfo *info);
    void CopyBufToDataCache(uint64_t dataCachePos, uint64_t len,
                             const char *data);
    void AddDataBefore(uint64_t len, const char *data);

    CURVEFS_ERROR PrepareFlushTasks(
        uint64_t inodeId, char *data,
        std::vector<std::shared_ptr<PutObjectAsyncContext>> *s3Tasks,
        std::vector<std::shared_ptr<SetKVCacheTask>> *kvCacheTasks,
        uint64_t *chunkId, uint64_t *writeOffset);

    void FlushTaskExecute(
        bool useDiskCache,
        std::vector<std::shared_ptr<PutObjectAsyncContext>> &s3Tasks,
        std::vector<std::shared_ptr<SetKVCacheTask>> &kvCacheTasks);

 private:
    S3ClientAdaptorImpl *s3ClientAdaptor_;
    KvClientManagerPtr kvCacheClientManager_;
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
};

class S3ReadResponse {
 public:
    explicit S3ReadResponse(char *data, uint64_t length)
        : data_(data), len_(length) {}

    char *GetDataBuf() { return data_; }

    uint64_t GetBufLen() { return len_; }

 private:
    char *data_;
    uint64_t len_;
};

class ChunkCacheManager
    : public std::enable_shared_from_this<ChunkCacheManager> {
 public:
    ChunkCacheManager(uint64_t index, S3ClientAdaptorImpl *s3ClientAdaptor)
        : index_(index), s3ClientAdaptor_(s3ClientAdaptor),
          flushingDataCache_(nullptr) {}
    virtual ~ChunkCacheManager() = default;
    void ReadChunk(uint64_t index, uint64_t chunkPos, uint64_t readLen,
                   char *dataBuf, uint64_t dataBufOffset,
                   std::vector<ReadRequest> *requests);
    virtual void WriteNewDataCache(S3ClientAdaptorImpl *s3ClientAdaptor,
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
    S3ClientAdaptorImpl *s3ClientAdaptor_;
    curve::common::Mutex flushMtx_;
    DataCachePtr flushingDataCache_;
    curve::common::Mutex flushingDataCacheMtx_;
};

class FileCacheManager {
 public:
    FileCacheManager(uint32_t fsid, uint64_t inode,
                     S3ClientAdaptorImpl *s3ClientAdaptor)
        : fsId_(fsid), inode_(inode), s3ClientAdaptor_(s3ClientAdaptor) {}
    FileCacheManager() {}
    ChunkCacheManagerPtr FindOrCreateChunkCacheManager(uint64_t index);
    void ReleaseCache();
    virtual void TruncateCache(uint64_t offset, uint64_t fileSize);
    virtual CURVEFS_ERROR Flush(bool force, bool toS3 = false);
    virtual int Write(uint64_t offset, uint64_t length, const char *dataBuf);
    virtual int Read(uint64_t inodeId, uint64_t offset, uint64_t length,
                     char *dataBuf);
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
    void GenerateS3Request(ReadRequest request,
                           const S3ChunkInfoList &s3ChunkInfoList,
                           char *dataBuf, std::vector<S3ReadRequest> *requests,
                           uint64_t fsId, uint64_t inodeId);
    int ReadFromS3(const std::vector<S3ReadRequest> &requests,
                            std::vector<S3ReadResponse> *responses,
                            char* dataBuf, uint64_t fileLen);
    void PrefetchS3Objs(
        const std::vector<std::pair<std::string, uint64_t>> &prefetchObjs);
    void HandleReadRequest(const ReadRequest &request,
                           const S3ChunkInfo &s3ChunkInfo,
                           std::vector<ReadRequest> *addReadRequests,
                           std::vector<uint64_t> *deletingReq,
                           std::vector<S3ReadRequest> *requests, char *dataBuf,
                           uint64_t fsId, uint64_t inodeId);
    int HandleReadRequest(const std::vector<S3ReadRequest> &requests,
                          std::vector<S3ReadResponse> *responses,
                          uint64_t fileLen);

 private:
    friend class AsyncPrefetchCallback;

    uint64_t fsId_;
    uint64_t inode_;
    std::map<uint64_t, ChunkCacheManagerPtr> chunkCacheMap_;  // first is index
    RWLock rwLock_;
    curve::common::Mutex mtx_;
    S3ClientAdaptorImpl *s3ClientAdaptor_;
    curve::common::Mutex downloadMtx_;
    std::set<std::string> downloadingObj_;
};

class FsCacheManager {
 public:
    FsCacheManager(S3ClientAdaptorImpl *s3ClientAdaptor,
                   uint64_t readCacheMaxByte, uint64_t writeCacheMaxByte)
        : lruByte_(0), wDataCacheNum_(0), wDataCacheByte_(0),
          readCacheMaxByte_(readCacheMaxByte),
          writeCacheMaxByte_(writeCacheMaxByte),
          s3ClientAdaptor_(s3ClientAdaptor), isWaiting_(false) {}
    FsCacheManager() {}
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
    S3ClientAdaptorImpl *s3ClientAdaptor_;
    bool isWaiting_;
    std::mutex mutex_;
    std::condition_variable cond_;

    ReadCacheReleaseExecutor releaseReadCache_;
};

}  // namespace client
}  // namespace curvefs

#endif  // CURVEFS_SRC_CLIENT_S3_CLIENT_S3_CACHE_MANAGER_H_
