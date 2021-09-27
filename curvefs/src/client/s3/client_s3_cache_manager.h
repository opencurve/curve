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

#include "curvefs/proto/metaserver.pb.h"
#include "curvefs/src/client/error_code.h"
#include "curvefs/src/client/s3/client_s3.h"
#include "src/common/concurrent/concurrent.h"
#include "src/common/timeutility.h"

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
using DataCachePtr = std::shared_ptr<DataCache>;
using curve::common::GetObjectAsyncCallBack;
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
    uint64_t objectOffset; // s3 object's offset
    uint64_t readOffset; // read buf offset        
};
#if 0
class S3ReadRequest {
 public:
    S3ReadRequest() {}
    virtual ~S3ReadRequest() {}
    void SetS3ChunkInfo(S3ChunkInfo chunkInfo) {
        chunkInfo_ = chunkInfo;
        readOffset_ = 0;
    }

    S3ChunkInfo& GetS3ChunkInfo() {
        return chunkInfo_;
    }

    const S3ChunkInfo GetS3ChunkInfo() const {
        return chunkInfo_;
    }

    void SetReadOffset(uint64_t readOffset) {
        readOffset_ = readOffset;
    }

    uint64_t GetReadOffset() {
        return readOffset_;
    }

    const uint64_t GetReadOffset() const {
        return readOffset_;
    }

 private:
    S3ChunkInfo chunkInfo_;
    uint64_t readOffset_;  // read buf offset
};
#endif
class DataCache {
 public:
    DataCache(S3ClientAdaptorImpl* s3ClientAdaptor,
              ChunkCacheManager* chunkCacheManager, uint64_t chunkPos,
              uint64_t len, const char* data)
        : s3ClientAdaptor_(s3ClientAdaptor),
          chunkCacheManager_(chunkCacheManager),
          chunkPos_(chunkPos),
          len_(len) {
        data_ = new char[len];
        memcpy(data_, data, len);
        createTime_ = ::curve::common::TimeUtility::GetTimeofDaySec();
        dirty_.exchange(true, std::memory_order_acq_rel);
    }
    DataCache(S3ClientAdaptorImpl* s3ClientAdaptor,
              ChunkCacheManager* chunkCacheManager, uint64_t chunkPos,
              uint64_t len)
        : s3ClientAdaptor_(s3ClientAdaptor),
          chunkCacheManager_(chunkCacheManager),
          chunkPos_(chunkPos),
          len_(len) {
        data_ = new char[len];
        createTime_ = ::curve::common::TimeUtility::GetTimeofDaySec();
        dirty_.exchange(false, std::memory_order_acq_rel);
    }
    virtual ~DataCache() {
        delete data_;
        data_ = nullptr;
    }

    void Write(uint64_t chunkPos, uint64_t len, const char* data,
               const std::vector<DataCachePtr>& mergeDataCacheVer);
    uint64_t GetChunkPos() {
        return chunkPos_;
    }
    uint64_t GetLen() {
        return len_;
    }

    char* GetData() {
        return data_;
    }

    CURVEFS_ERROR Flush(uint64_t inodeId, bool force);
    void Release();
    bool IsDirty() {
        return dirty_.load(std::memory_order_acquire);    
    }
 private:
    std::string GenerateObjectName(uint64_t chunkId, uint64_t blockIndex);
    void UpdateInodeChunkInfo(S3ChunkInfoList* s3ChunkInfoList,
                              uint64_t chunkId, uint64_t offset, uint64_t len);

 private:
    S3ClientAdaptorImpl* s3ClientAdaptor_;
    ChunkCacheManager* chunkCacheManager_;
    uint64_t chunkPos_;
    uint64_t len_;
    char* data_;
    curve::common::Mutex mtx_;
    uint64_t createTime_;
    std::atomic<bool> dirty_;
};

class S3ReadResponse {
 public:
    explicit S3ReadResponse(DataCachePtr dataCache) : dataCache_(dataCache) {}
    virtual ~S3ReadResponse() {}

    char* GetDataBuf() {
        return dataCache_->GetData();
    }

    void SetReadOffset(uint64_t readOffset) {
        readOffset_ = readOffset;
    }

    uint64_t GetReadOffset() {
        return readOffset_;
    }

    uint64_t GetBufLen() {
        return dataCache_->GetLen();
    }

    DataCachePtr GetDataCache() {
        return dataCache_;
    }

 private:
    uint64_t readOffset_;
    DataCachePtr dataCache_;
};

class ChunkCacheManager {
 public:
    ChunkCacheManager(uint64_t index, S3ClientAdaptorImpl* s3ClientAdaptor)
        : index_(index), s3ClientAdaptor_(s3ClientAdaptor) {}
    DataCachePtr CreateWriteDataCache(S3ClientAdaptorImpl* s3ClientAdaptor,
                                      uint32_t chunkPos, uint32_t len,
                                      const char* data);
    void AddReadDataCache(DataCachePtr dataCache);
    DataCachePtr FindWriteableDataCache(
        uint64_t pos, uint64_t len,
        std::vector<DataCachePtr>* mergeDataCacheVer);
    void ReadByWriteCache(uint64_t chunkPos, uint64_t readLen, char* dataBuf,
                          uint64_t dataBufOffset,
                          std::vector<ReadRequest>* requests);
    void ReadByReadCache(uint64_t chunkPos, uint64_t readLen, char* dataBuf,
                         uint64_t dataBufOffset,
                         std::vector<ReadRequest>* requests);
    CURVEFS_ERROR Flush(uint64_t inodeId, bool force);
    uint64_t GetIndex() {
        return index_;
    }
    bool IsEmpty() {
        return (dataWCacheMap_.empty() && dataRCacheMap_.empty());
    }

    void ReleaseReadDataCache(uint64_t key);
    void ReleaseCache(S3ClientAdaptorImpl* s3ClientAdaptor);
    // void UpdateReadCache(DataCachePtr dataCache, CacheType type);

 private:
    uint64_t index_;
    std::map<uint64_t, DataCachePtr> dataWCacheMap_;  // first is pos in chunk
    std::map<uint64_t, std::list<DataCachePtr>::iterator>
        dataRCacheMap_;   // first is pos in chunk
    RWLock rwLockWrite_;  //  for write cache
    RWLock rwLockRead_;   //  for read cache
    S3ClientAdaptorImpl* s3ClientAdaptor_;
    curve::common::Mutex flushMtx_;
};

class FileCacheManager {
 public:
    FileCacheManager(uint32_t fsid, uint64_t inode,
                     S3ClientAdaptorImpl* s3ClientAdaptor)
        : fsId_(fsid), inode_(inode), s3ClientAdaptor_(s3ClientAdaptor) {}
    ChunkCacheManagerPtr FindChunkCacheManager(uint64_t index);
    // ChunkCacheManagerPtr CreateChunkCacheManager(uint64_t index);
    ChunkCacheManagerPtr FindOrCreateChunkCacheManager(uint64_t index);
    void ReleaseChunkCacheManager(uint64_t index);
    void ReleaseCache();
    //CURVEFS_ERROR Flush(uint64_t inodeId, bool force);
    CURVEFS_ERROR Flush(bool force);
    int Write(uint64_t offset, uint64_t length, const char* dataBuf);
    int Read(Inode* inode, uint64_t offset, uint64_t length, char* dataBuf);
    bool IsEmpty() {
        return chunkCacheMap_.empty();
    }

 private:
    void WriteChunk(uint64_t index, uint64_t chunkPos, uint64_t writeLen,
                    const char* dataBuf);
    void ReadChunk(uint64_t index, uint64_t chunkPos, uint64_t readLen,
                   char* dataBuf, uint64_t dataBufOffset,
                   std::vector<ReadRequest>* requests);
    void GenerateS3Request(ReadRequest request, S3ChunkInfoList s3ChunkInfoList,
                           char* dataBuf, std::vector<S3ReadRequest>* requests);
    int HandleReadRequest(const std::vector<S3ReadRequest>& requests,
                          std::vector<S3ReadResponse>* responses);
    std::vector<S3ChunkInfo> GetReadChunks(
        const S3ChunkInfoList& s3ChunkInfoList);
    std::vector<S3ChunkInfo> SortByOffset(std::vector<S3ChunkInfo> chunks);
    std::vector<S3ChunkInfo> CutOverLapChunks(const S3ChunkInfo& newChunk,
                                              const S3ChunkInfo& oldChunk);

 private:
    uint64_t fsId_;
    uint64_t inode_;
    std::map<uint64_t, ChunkCacheManagerPtr> chunkCacheMap_;  // first is index
    RWLock rwLock_;
    curve::common::Mutex mtx_;
    S3ClientAdaptorImpl* s3ClientAdaptor_;
};

class FsCacheManager {
 public:
    explicit FsCacheManager(S3ClientAdaptorImpl* s3ClientAdaptor,
                            uint64_t lruCapacity, uint64_t writeCacheMaxByte)
        : lruCapacity_(lruCapacity),
          wDataCacheNum_(0),
          wDataCacheByte_(0),
          writeCacheMaxByte_(writeCacheMaxByte),
          s3ClientAdaptor_(s3ClientAdaptor),
          isWaiting_(false) {}
    FileCacheManagerPtr FindFileCacheManager(uint64_t inodeId);
    FileCacheManagerPtr FindOrCreateFileCacheManager(uint64_t fsId,
                                                     uint64_t inodeId);
    void ReleaseFileCacheManager(uint64_t inodeId);
    std::list<DataCachePtr>::iterator Set(DataCachePtr dataCache);
    void Delete(std::list<DataCachePtr>::iterator iter);
    void Get(std::list<DataCachePtr>::iterator iter);
    CURVEFS_ERROR FsSync(bool force);
    void BackGroundFlush();
    uint64_t GetDataCacheNum() {
        return wDataCacheNum_.load(std::memory_order_relaxed);
    }

    void DataCacheNumInc() {
        wDataCacheNum_.fetch_add(1, std::memory_order_relaxed);
    }

    void DataCacheNumFetchSub(uint64_t v) {
        assert(wDataCacheNum_.load(std::memory_order_relaxed) >= v);
        wDataCacheNum_.fetch_sub(v, std::memory_order_relaxed);
    }

    void DataCacheByteInc(uint64_t v) {
        LOG(INFO) << "DataCacheByteInc() v:" << v << ",wDataCacheByte:"
                  << wDataCacheByte_.load(std::memory_order_relaxed);
        wDataCacheByte_.fetch_add(v, std::memory_order_relaxed);
    }

    void DataCacheByteDec(uint64_t v) {
        LOG(INFO) << "DataCacheByteDec() v:" << v << ",wDataCacheByte:"
                  << wDataCacheByte_.load(std::memory_order_relaxed);
        assert(wDataCacheByte_.load(std::memory_order_relaxed) >= v);
        wDataCacheByte_.fetch_sub(v, std::memory_order_relaxed);
    }

    void WaitFlush() {
        isWaiting_.exchange(true, std::memory_order_acq_rel);
        std::unique_lock<std::mutex> lk(mutex_);
        cond_.wait(lk);
    }

    void FlushSignal() {
        if (isWaiting_.load(std::memory_order_acquire)) {
            isWaiting_.exchange(false, std::memory_order_acq_rel);
            std::lock_guard<std::mutex> lk(mutex_);
            cond_.notify_all();
        }
        return;
    }

    bool WriteCacheIsFull() {
        return wDataCacheByte_.load(std::memory_order_relaxed) >
               wDataCacheByte_;
    }

 private:
    std::unordered_map<uint64_t, FileCacheManagerPtr>
        fileCacheManagerMap_;  // first is inodeid
    RWLock rwLock_;
    RWLock rwLockLru_;
    std::list<DataCachePtr> lruReadDataCacheList_;
    uint64_t lruCapacity_;
    std::atomic<uint64_t> wDataCacheNum_;
    std::atomic<uint64_t> wDataCacheByte_;
    uint64_t writeCacheMaxByte_;
    S3ClientAdaptorImpl* s3ClientAdaptor_;
    std::atomic<bool> isWaiting_;
    std::mutex mutex_;
    std::condition_variable cond_;
};

}  // namespace client
}  // namespace curvefs

#endif  // CURVEFS_SRC_CLIENT_S3_CLIENT_S3_CACHE_MANAGER_H_
