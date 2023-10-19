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

#include "curvefs/src/client/s3/client_s3_cache_manager.h"

#include <bvar/bvar.h>
#include <sys/types.h>

#include <utility>

#include "absl/cleanup/cleanup.h"
#include "absl/synchronization/blocking_counter.h"
#include "curvefs/src/client/kvclient/kvclient_manager.h"
#include "curvefs/src/client/metric/client_metric.h"
#include "curvefs/src/client/s3/client_s3_adaptor.h"
#include "curvefs/src/common/s3util.h"
#include "src/common/s3_adapter.h"

namespace curvefs {
namespace client {
namespace common {
DECLARE_bool(enableCto);
}  // namespace common
}  // namespace client
}  // namespace curvefs

using ::curvefs::client::metric::S3MultiManagerMetric;
static S3MultiManagerMetric *g_s3MultiManagerMetric =
    new S3MultiManagerMetric();

namespace curvefs {
namespace client {

void FsCacheManager::DataCacheNumInc() {
    g_s3MultiManagerMetric->writeDataCacheNum << 1;
    VLOG(9) << "DataCacheNumInc() v: 1,wDataCacheNum:"
            << wDataCacheNum_.load(std::memory_order_relaxed);
    wDataCacheNum_.fetch_add(1, std::memory_order_relaxed);
}

void FsCacheManager::DataCacheNumFetchSub(uint64_t v) {
    g_s3MultiManagerMetric->writeDataCacheNum << -1 * v;
    VLOG(9) << "DataCacheNumFetchSub() v:" << v << ",wDataCacheNum_:"
            << wDataCacheNum_.load(std::memory_order_relaxed);
    assert(wDataCacheNum_.load(std::memory_order_relaxed) >= v);
    wDataCacheNum_.fetch_sub(v, std::memory_order_relaxed);
}

void FsCacheManager::DataCacheByteInc(uint64_t v) {
    g_s3MultiManagerMetric->writeDataCacheByte << v;
    VLOG(9) << "DataCacheByteInc() v:" << v << ",wDataCacheByte:"
            << wDataCacheByte_.load(std::memory_order_relaxed);
    wDataCacheByte_.fetch_add(v, std::memory_order_relaxed);
}

void FsCacheManager::DataCacheByteDec(uint64_t v) {
    g_s3MultiManagerMetric->writeDataCacheByte << -1 * v;
    VLOG(9) << "DataCacheByteDec() v:" << v << ",wDataCacheByte:"
            << wDataCacheByte_.load(std::memory_order_relaxed);
    assert(wDataCacheByte_.load(std::memory_order_relaxed) >= v);
    wDataCacheByte_.fetch_sub(v, std::memory_order_relaxed);
}

FileCacheManagerPtr FsCacheManager::FindFileCacheManager(uint64_t inodeId) {
    ReadLockGuard readLockGuard(rwLock_);

    auto it = fileCacheManagerMap_.find(inodeId);
    if (it != fileCacheManagerMap_.end()) {
        return it->second;
    }

    return nullptr;
}

FileCacheManagerPtr
FsCacheManager::FindOrCreateFileCacheManager(uint64_t fsId, uint64_t inodeId) {
    WriteLockGuard writeLockGuard(rwLock_);

    auto it = fileCacheManagerMap_.find(inodeId);
    if (it != fileCacheManagerMap_.end()) {
        return it->second;
    }

    FileCacheManagerPtr fileCacheManager = std::make_shared<FileCacheManager>(
        fsId, inodeId, s3ClientAdaptor_, kvClientManager_, readTaskPool_);
    auto ret = fileCacheManagerMap_.emplace(inodeId, fileCacheManager);
    g_s3MultiManagerMetric->fileManagerNum << 1;
    assert(ret.second);
    (void)ret;
    return fileCacheManager;
}

void FsCacheManager::ReleaseFileCacheManager(uint64_t inodeId) {
    WriteLockGuard writeLockGuard(rwLock_);

    auto iter = fileCacheManagerMap_.find(inodeId);
    if (iter == fileCacheManagerMap_.end()) {
        VLOG(1) << "ReleaseFileCacheManager, do not find file cache manager of "
                   "inode: "
                << inodeId;
        return;
    }

    fileCacheManagerMap_.erase(iter);
    g_s3MultiManagerMetric->fileManagerNum << -1;
    return;
}

bool FsCacheManager::Set(DataCachePtr dataCache,
                         std::list<DataCachePtr>::iterator *outIter) {
    std::lock_guard<std::mutex> lk(lruMtx_);
    VLOG(3) << "lru current byte:" << lruByte_
            << ",lru max byte:" << readCacheMaxByte_
            << ", dataCache len:" << dataCache->GetLen();
    if (readCacheMaxByte_ == 0) {
        return false;
    }
    // trim cache without consider dataCache's size, because its size is
    // expected to be very smaller than `readCacheMaxByte_`
    if (lruByte_ >= readCacheMaxByte_) {
        uint64_t retiredBytes = 0;
        auto iter = lruReadDataCacheList_.end();

        while (lruByte_ >= readCacheMaxByte_) {
            --iter;
            auto &trim = *iter;
            trim->SetReadCacheState(false);
            lruByte_ -= trim->GetActualLen();
            retiredBytes += trim->GetActualLen();
        }

        std::list<DataCachePtr> retired;
        retired.splice(retired.end(), lruReadDataCacheList_, iter,
                       lruReadDataCacheList_.end());

        VLOG(3) << "lru release " << retiredBytes << " bytes, retired "
                << retired.size() << " data cache";

        releaseReadCache_.Release(&retired);
    }

    lruByte_ += dataCache->GetActualLen();
    dataCache->SetReadCacheState(true);
    lruReadDataCacheList_.push_front(std::move(dataCache));
    *outIter = lruReadDataCacheList_.begin();
    return true;
}

void FsCacheManager::Get(std::list<DataCachePtr>::iterator iter) {
    std::lock_guard<std::mutex> lk(lruMtx_);

    if (!(*iter)->InReadCache()) {
        return;
    }

    lruReadDataCacheList_.splice(lruReadDataCacheList_.begin(),
                                 lruReadDataCacheList_, iter);
}

bool FsCacheManager::Delete(std::list<DataCachePtr>::iterator iter) {
    std::lock_guard<std::mutex> lk(lruMtx_);

    if (!(*iter)->InReadCache()) {
        return false;
    }

    (*iter)->SetReadCacheState(false);
    lruByte_ -= (*iter)->GetActualLen();
    lruReadDataCacheList_.erase(iter);
    return true;
}

CURVEFS_ERROR FsCacheManager::FsSync(bool force) {
    CURVEFS_ERROR ret;
    std::unordered_map<uint64_t, FileCacheManagerPtr> tmp;
    {
        WriteLockGuard writeLockGuard(rwLock_);
        tmp = fileCacheManagerMap_;
    }

    auto iter = tmp.begin();
    for (; iter != tmp.end(); iter++) {
        ret = iter->second->Flush(force);
        if (ret == CURVEFS_ERROR::OK) {
            WriteLockGuard writeLockGuard(rwLock_);
            auto iter1 = fileCacheManagerMap_.find(iter->first);
            if (iter1 == fileCacheManagerMap_.end()) {
                VLOG(1) << "FsSync, chunk cache for inodeid: " << iter->first
                        << " is removed";
                continue;
            } else {
                VLOG(9) << "FileCacheManagerPtr count:"
                        << iter1->second.use_count()
                        << ", inodeId:" << iter1->first;
                // tmp and fileCacheManagerMap_ has this FileCacheManagerPtr, so
                // count is 2 if count more than 2, this mean someone thread has
                // this FileCacheManagerPtr
                // TODO(@huyao) https://github.com/opencurve/curve/issues/1473
                if ((iter1->second->IsEmpty()) &&
                    (iter1->second.use_count() <= 2)) {
                    VLOG(9) << "Release FileCacheManager, inode id: "
                            << iter1->second->GetInodeId();
                    fileCacheManagerMap_.erase(iter1);
                    g_s3MultiManagerMetric->fileManagerNum << -1;
                }
            }
        } else if (ret == CURVEFS_ERROR::NOTEXIST) {
            iter->second->ReleaseCache();
            WriteLockGuard writeLockGuard(rwLock_);
            auto iter1 = fileCacheManagerMap_.find(iter->first);
            if (iter1 != fileCacheManagerMap_.end()) {
                VLOG(9) << "Release FileCacheManager, inode id: "
                        << iter1->second->GetInodeId();
                fileCacheManagerMap_.erase(iter1);
                g_s3MultiManagerMetric->fileManagerNum << -1;
            }
        } else {
            LOG(ERROR) << "fs fssync error, ret: " << ret;
            return ret;
        }
    }

    return CURVEFS_ERROR::OK;
}

int FileCacheManager::Write(uint64_t offset, uint64_t length,
                            const char *dataBuf) {
    uint64_t chunkSize = s3ClientAdaptor_->GetChunkSize();
    uint64_t index = offset / chunkSize;
    uint64_t chunkPos = offset % chunkSize;
    uint64_t writeLen = 0;
    uint64_t writeOffset = 0;

    while (length > 0) {
        if (chunkPos + length > chunkSize) {
            writeLen = chunkSize - chunkPos;
        } else {
            writeLen = length;
        }

        WriteChunk(index, chunkPos, writeLen, (dataBuf + writeOffset));

        length -= writeLen;
        index++;
        writeOffset += writeLen;
        chunkPos = (chunkPos + writeLen) % chunkSize;
    }
    return writeOffset;
}

void FileCacheManager::WriteChunk(uint64_t index, uint64_t chunkPos,
                                  uint64_t writeLen, const char *dataBuf) {
    VLOG(9) << "WriteChunk start, index: " << index
            << ", chunkPos: " << chunkPos;
    ChunkCacheManagerPtr chunkCacheManager =
        FindOrCreateChunkCacheManager(index);
    WriteLockGuard writeLockGuard(chunkCacheManager->rwLockChunk_);  // todo
    std::vector<DataCachePtr> mergeDataCacheVer;
    DataCachePtr dataCache = chunkCacheManager->FindWriteableDataCache(
        chunkPos, writeLen, &mergeDataCacheVer, inode_);
    if (dataCache) {
        dataCache->Write(chunkPos, writeLen, dataBuf, mergeDataCacheVer);
    } else {
        chunkCacheManager->WriteNewDataCache(s3ClientAdaptor_, chunkPos,
                                             writeLen, dataBuf);
    }
    VLOG(9) << "WriteChunk end, index: " << index << ", chunkPos: " << chunkPos;
    return;
}

ChunkCacheManagerPtr
FileCacheManager::FindOrCreateChunkCacheManager(uint64_t index) {
    WriteLockGuard writeLockGuard(rwLock_);

    auto it = chunkCacheMap_.find(index);
    if (it != chunkCacheMap_.end()) {
        return it->second;
    }

    ChunkCacheManagerPtr chunkCacheManager =
        std::make_shared<ChunkCacheManager>(index, s3ClientAdaptor_,
                                            kvClientManager_);
    auto ret = chunkCacheMap_.emplace(index, chunkCacheManager);
    g_s3MultiManagerMetric->chunkManagerNum << 1;
    assert(ret.second);
    (void)ret;
    return chunkCacheManager;
}

void FileCacheManager::GetChunkLoc(uint64_t offset, uint64_t *index,
                                   uint64_t *chunkPos, uint64_t *chunkSize) {
    *chunkSize = s3ClientAdaptor_->GetChunkSize();
    *index = offset / *chunkSize;
    *chunkPos = offset % *chunkSize;
}

void FileCacheManager::GetBlockLoc(uint64_t offset, uint64_t *chunkIndex,
                                   uint64_t *chunkPos, uint64_t *blockIndex,
                                   uint64_t *blockPos) {
    uint64_t chunkSize = 0;
    uint64_t blockSize = s3ClientAdaptor_->GetBlockSize();
    GetChunkLoc(offset, chunkIndex, chunkPos, &chunkSize);

    *blockIndex = offset % chunkSize / blockSize;
    *blockPos = offset % chunkSize % blockSize;
}

void FileCacheManager::ReadFromMemCache(
    uint64_t offset, uint64_t length, char *dataBuf, uint64_t *actualReadLen,
    std::vector<ReadRequest> *memCacheMissRequest) {

    uint64_t index = 0, chunkPos = 0, chunkSize = 0;
    GetChunkLoc(offset, &index, &chunkPos, &chunkSize);

    uint64_t currentReadLen = 0;
    uint64_t dataBufferOffset = 0;
    while (length > 0) {
        // |--------------------------------|
        // 0                             chunksize
        //                chunkPos                   length + chunkPos
        //                   |-------------------------|
        //                   |--------------|
        //                    currentReadLen
        currentReadLen =
            chunkPos + length > chunkSize ? chunkSize - chunkPos : length;


        // 1. read from local memory cache
        // 2. generate cache miss request
        ChunkCacheManagerPtr chunkCacheManager =
            FindOrCreateChunkCacheManager(index);
        std::vector<ReadRequest> tmpMissRequests;
        chunkCacheManager->ReadChunk(index, chunkPos, currentReadLen, dataBuf,
                                     dataBufferOffset, &tmpMissRequests);
        memCacheMissRequest->insert(memCacheMissRequest->end(),
                                    tmpMissRequests.begin(),
                                    tmpMissRequests.end());

        length -= currentReadLen;            // left length
        index++;                             // next index
        dataBufferOffset += currentReadLen;  // next data buffer offset
        chunkPos = (chunkPos + currentReadLen) % chunkSize;  // next chunkPos
    }

    *actualReadLen = dataBufferOffset;

    VLOG_IF(3, memCacheMissRequest->empty()) << "great! memory cache all hit.";
}

int FileCacheManager::GenerateKVRequest(
    const std::shared_ptr<InodeWrapper> &inodeWrapper,
    const std::vector<ReadRequest> &readRequest, char *dataBuf,
    std::vector<S3ReadRequest> *kvRequest) {

    ::curve::common::UniqueLock lgGuard = inodeWrapper->GetUniqueLock();
    const Inode *inode = inodeWrapper->GetInodeLocked();
    const auto *s3chunkinfo = inodeWrapper->GetChunkInfoMap();
    VLOG(9) << "process inode: " << inode->DebugString();
    for_each(
        readRequest.begin(), readRequest.end(), [&](const ReadRequest &req) {
            VLOG(6) << req.DebugString();
            auto infoIter = s3chunkinfo->find(req.index);
            if (infoIter == s3chunkinfo->end()) {
                VLOG(6) << "inode = " << inode->inodeid()
                        << " s3chunkinfo do not find index = " << req.index;
                memset(dataBuf + req.bufOffset, 0, req.len);
                return;
            } else {
                std::vector<S3ReadRequest> tmpKVRequests;
                GenerateS3Request(req, infoIter->second, dataBuf,
                                  &tmpKVRequests, inode->fsid(),
                                  inode->inodeid());
                kvRequest->insert(kvRequest->end(), tmpKVRequests.begin(),
                                  tmpKVRequests.end());
            }
        });

    VLOG(9) << "process inode: " << S3ReadRequestVecDebugString(*kvRequest)
            << " ok";

    return 0;
}

int FileCacheManager::HandleReadS3NotExist(
    uint32_t retry, const std::shared_ptr<InodeWrapper> &inodeWrapper) {
    uint32_t maxIntervalMs =
        s3ClientAdaptor_->GetMaxReadRetryIntervalMs();  // hardcode, fixme
    uint32_t retryIntervalMs = s3ClientAdaptor_->GetReadRetryIntervalMs();

    if (retry == 1) {
        curve::common::UniqueLock lgGuard = inodeWrapper->GetUniqueLock();
        if (CURVEFS_ERROR::OK != inodeWrapper->RefreshS3ChunkInfo()) {
            LOG(ERROR) << "refresh inode = " << inodeWrapper->GetInodeId()
                       << " fail";
            return -1;
        }
    } else if (retry * retryIntervalMs < maxIntervalMs) {
        LOG(WARNING) << "read inode = " << inodeWrapper->GetInodeId()
                     << " retry = " << retry;
        bthread_usleep(retryIntervalMs * retry * 1000);
    } else {
        LOG(WARNING) << "read inode = " << inodeWrapper->GetInodeId()
                     << " retry = " << retry
                     << ", reach max interval = " << maxIntervalMs << " ms";
        bthread_usleep(maxIntervalMs * 1000);
    }

    return 0;
}

int FileCacheManager::Read(uint64_t inodeId, uint64_t offset, uint64_t length,
                           char *dataBuf) {
    // 1. read from memory cache
    uint64_t actualReadLen = 0;
    std::vector<ReadRequest> memCacheMissRequest;
    ReadFromMemCache(offset, length, dataBuf, &actualReadLen,
                     &memCacheMissRequest);
    if (memCacheMissRequest.empty()) {
        return actualReadLen;
    }


    // 2. read from localcache and remote cluster
    std::shared_ptr<InodeWrapper> inodeWrapper;
    auto inodeManager = s3ClientAdaptor_->GetInodeCacheManager();
    if (CURVEFS_ERROR::OK != inodeManager->GetInode(inodeId, inodeWrapper)) {
        LOG(ERROR) << "get inode = " << inodeId << " fail";
        return -1;
    }

    uint32_t retry = 0;
    do {
        // generate kv request
        std::vector<S3ReadRequest> kvRequests;
        GenerateKVRequest(inodeWrapper, memCacheMissRequest, dataBuf,
                          &kvRequests);

        // read from kv cluster (localcache -> remote kv cluster -> s3)
        // localcache/remote kv cluster fail will not return error code.
        // Failure to read from s3 will eventually return failure.
        ReadStatus ret =
            ReadKVRequest(kvRequests, dataBuf, inodeWrapper->GetLength());
        if (ret == ReadStatus::OK) {
            break;
        }

        if (ret == ReadStatus::S3_NOT_EXIST) {
            if (0 != HandleReadS3NotExist(retry++, inodeWrapper)) {
                return -1;
            }
        } else {
            LOG(INFO) << "read inode = " << inodeId
                      << " from s3 failed, ret = " << static_cast<int>(ret);
            return static_cast<int>(ret);
        }
    } while (1);

    return actualReadLen;
}

bool FileCacheManager::ReadKVRequestFromLocalCache(const std::string &name,
                                                   char *databuf,
                                                   uint64_t offset,
                                                   uint64_t len) {
    uint64_t start = butil::cpuwide_time_us();

    bool mayCached = s3ClientAdaptor_->HasDiskCache() &&
                     s3ClientAdaptor_->GetDiskCacheManager()->IsCached(name);
    if (!mayCached) {
        return false;
    }

    if (0 > s3ClientAdaptor_->GetDiskCacheManager()->Read(name, databuf, offset,
                                                          len)) {
        LOG(WARNING) << "object " << name << " not cached in disk";
        return false;
    }

    if (s3ClientAdaptor_->s3Metric_) {
        curve::client::CollectMetrics(
            &s3ClientAdaptor_->s3Metric_->adaptorReadS3, len,
            butil::cpuwide_time_us() - start);
        curve::client::CollectMetrics(
            &s3ClientAdaptor_->s3Metric_->readFromDiskCache, len,
            butil::cpuwide_time_us() - start);
    }
    return true;
}

bool FileCacheManager::ReadKVRequestFromRemoteCache(const std::string &name,
                                                    char *databuf,
                                                    uint64_t offset,
                                                    uint64_t length) {
    if (!kvClientManager_) {
        return false;
    }

    CountDownEvent event(1);
    GetKVCacheDone cb = [&](const std::shared_ptr<GetKVCacheTask>& task) {
        if (task->res && s3ClientAdaptor_->s3Metric_ != nullptr) {
            curve::client::CollectMetrics(
                &s3ClientAdaptor_->s3Metric_->readFromKVCache, task->length,
                task->timer.u_elapsed());
        }
        event.Signal();
        return;
    };

    auto task =
        std::make_shared<GetKVCacheTask>(name, databuf, offset, length, cb);
    kvClientManager_->Get(task);
    event.Wait();

    return task->res;
}

bool FileCacheManager::ReadKVRequestFromS3(const std::string &name,
                                           char *databuf, uint64_t offset,
                                           uint64_t length, int *ret) {
    uint64_t start = butil::cpuwide_time_us();
    *ret = s3ClientAdaptor_->GetS3Client()->Download(name, databuf, offset,
                                                     length);
    if (*ret < 0) {
        LOG(ERROR) << "object " << name << " read from s3 fail, ret = " << *ret;
        return false;
    }

    if (s3ClientAdaptor_->s3Metric_) {
        curve::client::CollectMetrics(
            &s3ClientAdaptor_->s3Metric_->adaptorReadS3, length,
            butil::cpuwide_time_us() - start);
        curve::client::CollectMetrics(&s3ClientAdaptor_->s3Metric_->readFromS3,
                                      length, butil::cpuwide_time_us() - start);
    }

    return true;
}

FileCacheManager::ReadStatus
FileCacheManager::ReadKVRequest(const std::vector<S3ReadRequest> &kvRequests,
                                char *dataBuf, uint64_t fileLen) {
    absl::BlockingCounter counter(kvRequests.size());
    std::once_flag cancelFlag;
    std::atomic<bool> isCanceled{false};
    std::atomic<int> retCode{0};

    for (const auto &req : kvRequests) {
        readTaskPool_->Enqueue([&]() {
            auto defer = absl::MakeCleanup([&]() { counter.DecrementCount(); });
            if (isCanceled) {
                LOG(WARNING) << "kv request is canceled " << req.DebugString();
                return;
            }
            ProcessKVRequest(req, dataBuf, fileLen, cancelFlag, isCanceled,
                             retCode);
        });
    }

    counter.Wait();
    return toReadStatus(retCode.load());
}

void FileCacheManager::ProcessKVRequest(const S3ReadRequest &req, char *dataBuf,
                                        uint64_t fileLen,
                                        std::once_flag &cancelFlag,
                                        std::atomic<bool> &isCanceled,
                                        std::atomic<int> &retCode) {
    VLOG(6) << "read from kv request " << req.DebugString();
    uint64_t chunkIndex = 0;
    uint64_t chunkPos = 0;
    uint64_t blockIndex = 0;
    uint64_t blockPos = 0;
    const uint64_t chunkSize = s3ClientAdaptor_->GetChunkSize();
    const uint64_t blockSize = s3ClientAdaptor_->GetBlockSize();
    const uint32_t objectPrefix = s3ClientAdaptor_->GetObjectPrefix();
    GetBlockLoc(req.offset, &chunkIndex, &chunkPos, &blockIndex, &blockPos);

    // prefetch
    if (s3ClientAdaptor_->HasDiskCache()) {
        PrefetchForBlock(req, fileLen, blockSize, chunkSize, blockIndex);
    }

    // read request
    // |--------------------------------|----------------------------------|
    // 0                             blockSize                   2*blockSize
    //                blockPos                   length + blockPos
    //                   |-------------------------|
    //                   |--------------|
    //                    currentReadLen
    uint64_t length = req.len;
    uint64_t currentReadLen = 0;
    uint64_t readBufOffset = 0;
    uint64_t objectOffset = req.objectOffset;

    while (length > 0) {
        currentReadLen =
            length + blockPos > blockSize ? blockSize - blockPos : length;
        assert(blockPos >= objectOffset);
        std::string name = curvefs::common::s3util::GenObjName(
            req.chunkId, blockIndex, req.compaction, req.fsId, req.inodeId,
            objectPrefix);
        char *currentBuf = dataBuf + req.readOffset + readBufOffset;

        // read from localcache -> remotecache -> s3
        do {
            if (ReadKVRequestFromLocalCache(name, currentBuf,
                                            blockPos - objectOffset,
                                            currentReadLen)) {
                VLOG(9) << "read " << name << " from local cache ok";
                break;
            }

            if (ReadKVRequestFromRemoteCache(name, currentBuf,
                                             blockPos - objectOffset,
                                             currentReadLen)) {
                VLOG(9) << "read " << name << " from remote cache ok";
                break;
            }

            int ret = 0;
            if (ReadKVRequestFromS3(name, currentBuf, blockPos - objectOffset,
                                    currentReadLen, &ret)) {
                VLOG(9) << "read " << name << " from s3 ok";
                break;
            }

            LOG(ERROR) << "read " << name << " fail";
            // make sure variable is set only once
            std::call_once(cancelFlag, [&]() {
                isCanceled.store(true);
                retCode.store(ret);
            });
            return;
        } while (false);

        // update param
        {
            length -= currentReadLen;         // Remaining read data length
            readBufOffset += currentReadLen;  // next read offset
            blockIndex++;
            blockPos = (blockPos + currentReadLen) % blockSize;
            objectOffset = 0;
        }
    }

    // add data to memory read cache
    if (!curvefs::client::common::FLAGS_enableCto) {
        auto chunkCacheManager = FindOrCreateChunkCacheManager(chunkIndex);
        WriteLockGuard writeLockGuard(chunkCacheManager->rwLockChunk_);
        DataCachePtr dataCache = std::make_shared<DataCache>(
            s3ClientAdaptor_, chunkCacheManager, chunkPos, req.len,
            dataBuf + req.readOffset, kvClientManager_);
        chunkCacheManager->AddReadDataCache(dataCache);
    }
}

void FileCacheManager::PrefetchForBlock(const S3ReadRequest &req,
                                        uint64_t fileLen, uint64_t blockSize,
                                        uint64_t chunkSize,
                                        uint64_t startBlockIndex) {
    uint32_t prefetchBlocks = s3ClientAdaptor_->GetPrefetchBlocks();
    uint32_t objectPrefix = s3ClientAdaptor_->GetObjectPrefix();
    std::vector<std::pair<std::string, uint64_t>> prefetchObjs;

    uint64_t blockIndex = startBlockIndex;
    for (uint32_t i = 0; i < prefetchBlocks; i++) {
        std::string name = curvefs::common::s3util::GenObjName(
            req.chunkId, blockIndex, req.compaction,
            req.fsId, req.inodeId, objectPrefix);
        uint64_t maxReadLen = (blockIndex + 1) * blockSize;
        uint64_t needReadLen =
            maxReadLen > fileLen ? fileLen - blockIndex * blockSize : blockSize;

        prefetchObjs.push_back(std::make_pair(name, needReadLen));

        blockIndex++;
        if (maxReadLen > fileLen || blockIndex >= chunkSize / blockSize) {
            break;
        }
    }

    PrefetchS3Objs(prefetchObjs);
}

class AsyncPrefetchCallback {
 public:
    AsyncPrefetchCallback(uint64_t inode, S3ClientAdaptorImpl *s3Client)
        : inode_(inode), s3Client_(s3Client) {}

    void operator()(const S3Adapter *,
                    const std::shared_ptr<GetObjectAsyncContext> &context) {
        VLOG(9) << "prefetch end: " << context->key << ", len " << context->len
                << "actual len: " << context->actualLen;
        std::unique_ptr<char[]> guard(context->buf);
        auto fileCache =
            s3Client_->GetFsCacheManager()->FindFileCacheManager(inode_);

        if (!fileCache) {
            VLOG(3) << "prefetch inode: " << inode_
                    << " end, but file cache is released, key: "
                    << context->key;
            return;
        }

        if (context->retCode < 0) {
            LOG(WARNING) << "prefetch failed, key: " << context->key;
            return;
        }
        if (s3Client_->s3Metric_ != nullptr) {
            // read from s3
            metric::AsyncContextCollectMetrics(s3Client_->s3Metric_, context);
        }

        uint64_t start = butil::cpuwide_time_us();
        int ret = s3Client_->GetDiskCacheManager()->WriteReadDirect(
            context->key, context->buf, context->actualLen);
        if (ret < 0) {
            LOG_EVERY_SECOND(INFO)
                << "write read directly failed, key: " << context->key;
        }
        if (s3Client_->s3Metric_ != nullptr) {
            // prefetch to disk
            curve::client::CollectMetrics(
                &s3Client_->s3Metric_->writeToDiskCache, context->actualLen,
                butil::cpuwide_time_us() - start);
        }
        {
            curve::common::LockGuard lg(fileCache->downloadMtx_);
            fileCache->downloadingObj_.erase(context->key);
        }
    }

 private:
    const uint64_t inode_;
    S3ClientAdaptorImpl *s3Client_;
};

void FileCacheManager::PrefetchS3Objs(
    const std::vector<std::pair<std::string, uint64_t>> &prefetchObjs) {
    for (auto &obj : prefetchObjs) {
        std::string name = obj.first;
        uint64_t readLen = obj.second;
        curve::common::LockGuard lg(downloadMtx_);
        if (downloadingObj_.find(name) != downloadingObj_.end()) {
            VLOG(9) << "obj is already in downloading: " << name
                    << ", size: " << downloadingObj_.size();
            continue;
        }
        if (s3ClientAdaptor_->GetDiskCacheManager()->IsCached(name)) {
            VLOG(9) << "downloading is exist in cache: " << name
                    << ", size: " << downloadingObj_.size();
            continue;
        }
        VLOG(9) << "download start: " << name
                << ", size: " << downloadingObj_.size();
        downloadingObj_.emplace(name);

        auto inode = inode_;
        auto s3ClientAdaptor = s3ClientAdaptor_;
        auto task = [name, inode, s3ClientAdaptor, readLen]() {
            char *dataCacheS3 = new char[readLen];
            auto context = std::make_shared<GetObjectAsyncContext>(
                name, dataCacheS3, 0, readLen,
                AsyncPrefetchCallback{inode, s3ClientAdaptor},
                curve::common::ContextType::S3);
            VLOG(9) << "prefetch start: " << context->key
                    << ", len: " << context->len;
            s3ClientAdaptor->GetS3Client()->DownloadAsync(context);
        };
        s3ClientAdaptor_->PushAsyncTask(task);
    }
    return;
}

void FileCacheManager::HandleReadRequest(
    const ReadRequest &request, const S3ChunkInfo &s3ChunkInfo,
    std::vector<ReadRequest> *addReadRequests,
    std::vector<uint64_t> *deletingReq, std::vector<S3ReadRequest> *requests,
    char *dataBuf, uint64_t fsId, uint64_t inodeId) {
    uint64_t blockSize = s3ClientAdaptor_->GetBlockSize();
    uint64_t chunkSize = s3ClientAdaptor_->GetChunkSize();
    S3ReadRequest s3Request;
    uint64_t s3ChunkInfoOffset = s3ChunkInfo.offset();
    uint64_t s3ChunkInfoLen = s3ChunkInfo.len();
    uint64_t fileOffset = request.index * chunkSize + request.chunkPos;
    uint64_t length = request.len;
    uint64_t bufOffset = request.bufOffset;
    uint64_t readOffset = 0;

    VLOG(9) << "HandleReadRequest request index:" << request.index
            << ",chunkPos:" << request.chunkPos << ",len:" << request.len
            << ",bufOffset:" << request.bufOffset;
    VLOG(9) << "HandleReadRequest s3info chunkid:" << s3ChunkInfo.chunkid()
            << ",offset:" << s3ChunkInfoOffset << ",len:" << s3ChunkInfoLen
            << ",compaction:" << s3ChunkInfo.compaction()
            << ",zero:" << s3ChunkInfo.zero();
    /*
             -----             read block
                    ------     S3ChunkInfo
    */
    if (fileOffset + length <= s3ChunkInfoOffset) {
        return;
        /*
             -----              ------------   read block           -
                ------             -----       S3ChunkInfo
        */
    } else if ((s3ChunkInfoOffset > fileOffset) &&
               (s3ChunkInfoOffset < fileOffset + length)) {
        ReadRequest splitRequest;
        splitRequest.index = request.index;
        splitRequest.chunkPos = request.chunkPos;
        splitRequest.len = s3ChunkInfoOffset - fileOffset;
        splitRequest.bufOffset = bufOffset;
        addReadRequests->emplace_back(splitRequest);
        deletingReq->emplace_back(request.chunkPos);
        readOffset += splitRequest.len;
        /*
             -----                 read block           -
                ------             S3ChunkInfo
        */
        if (fileOffset + length <= s3ChunkInfoOffset + s3ChunkInfoLen) {
            if (s3ChunkInfo.zero()) {
                memset(static_cast<char *>(dataBuf) + bufOffset + readOffset, 0,
                       fileOffset + length - s3ChunkInfoOffset);
            } else {
                s3Request.chunkId = s3ChunkInfo.chunkid();
                s3Request.offset = s3ChunkInfoOffset;
                s3Request.len = fileOffset + length - s3ChunkInfoOffset;
                s3Request.objectOffset =
                    s3ChunkInfoOffset % chunkSize % blockSize;
                s3Request.readOffset = bufOffset + readOffset;
                s3Request.compaction = s3ChunkInfo.compaction();
                s3Request.fsId = fsId;
                s3Request.inodeId = inodeId;
                requests->push_back(s3Request);
            }
            /*
                                 ------------   read block           -
                                    -----       S3ChunkInfo
            */
        } else {
            if (s3ChunkInfo.zero()) {
                memset(static_cast<char *>(dataBuf) + bufOffset + readOffset, 0,
                       s3ChunkInfoLen);
            } else {
                s3Request.chunkId = s3ChunkInfo.chunkid();
                s3Request.offset = s3ChunkInfoOffset;
                s3Request.len = s3ChunkInfoLen;
                s3Request.objectOffset =
                    s3ChunkInfoOffset % chunkSize % blockSize;
                s3Request.readOffset = bufOffset + readOffset;
                s3Request.compaction = s3ChunkInfo.compaction();
                s3Request.fsId = fsId;
                s3Request.inodeId = inodeId;
                requests->push_back(s3Request);
            }
            ReadRequest splitRequest;

            readOffset += s3ChunkInfoLen;
            splitRequest.index = request.index;
            splitRequest.chunkPos = request.chunkPos + readOffset;
            splitRequest.len =
                fileOffset + length - (s3ChunkInfoOffset + s3ChunkInfoLen);
            splitRequest.bufOffset = bufOffset + readOffset;
            addReadRequests->emplace_back(splitRequest);
        }
        /*
              ----                      ---------   read block
            ----------                --------      S3ChunkInfo
        */
    } else if ((s3ChunkInfoOffset <= fileOffset) &&
               (s3ChunkInfoOffset + s3ChunkInfoLen > fileOffset)) {
        deletingReq->emplace_back(request.chunkPos);
        /*
              ----                    read block
            ----------                S3ChunkInfo
        */
        if (fileOffset + length <= s3ChunkInfoOffset + s3ChunkInfoLen) {
            if (s3ChunkInfo.zero()) {
                memset(static_cast<char *>(dataBuf) + bufOffset + readOffset, 0,
                       length);
            } else {
                s3Request.chunkId = s3ChunkInfo.chunkid();
                s3Request.offset = fileOffset;
                s3Request.len = length;
                if (fileOffset / blockSize == s3ChunkInfoOffset / blockSize) {
                    s3Request.objectOffset =
                        s3ChunkInfoOffset % chunkSize % blockSize;
                } else {
                    s3Request.objectOffset = 0;
                }
                s3Request.readOffset = bufOffset + readOffset;
                s3Request.compaction = s3ChunkInfo.compaction();
                s3Request.fsId = fsId;
                s3Request.inodeId = inodeId;
                requests->push_back(s3Request);
            }
            /*
                                      ---------   read block
                                    --------      S3ChunkInfo
            */
        } else {
            if (s3ChunkInfo.zero()) {
                memset(static_cast<char *>(dataBuf) + bufOffset + readOffset, 0,
                       s3ChunkInfoOffset + s3ChunkInfoLen - fileOffset);
            } else {
                s3Request.chunkId = s3ChunkInfo.chunkid();
                s3Request.offset = fileOffset;
                s3Request.len = s3ChunkInfoOffset + s3ChunkInfoLen - fileOffset;
                if (fileOffset / blockSize == s3ChunkInfoOffset / blockSize) {
                    s3Request.objectOffset =
                        s3ChunkInfoOffset % chunkSize % blockSize;
                } else {
                    s3Request.objectOffset = 0;
                }
                s3Request.readOffset = bufOffset + readOffset;
                s3Request.compaction = s3ChunkInfo.compaction();
                s3Request.fsId = fsId;
                s3Request.inodeId = inodeId;
                requests->push_back(s3Request);
            }
            readOffset += s3ChunkInfoOffset + s3ChunkInfoLen - fileOffset;
            ReadRequest splitRequest;
            splitRequest.index = request.index;
            splitRequest.chunkPos = request.chunkPos + s3ChunkInfoOffset +
                                    s3ChunkInfoLen - fileOffset;
            splitRequest.len =
                fileOffset + length - (s3ChunkInfoOffset + s3ChunkInfoLen);
            splitRequest.bufOffset = bufOffset + readOffset;
            addReadRequests->emplace_back(splitRequest);
        }
        /*
                    -----  read block
            ----           S3ChunkInfo
        do nothing
        */
    } else {
    }
}


void FileCacheManager::GenerateS3Request(ReadRequest request,
                                         const S3ChunkInfoList &s3ChunkInfoList,
                                         char *dataBuf,
                                         std::vector<S3ReadRequest> *requests,
                                         uint64_t fsId, uint64_t inodeId) {
    // first is chunkPos, user read request is split into multiple,
    // and emplace in the readRequests;
    std::map<uint64_t, ReadRequest> readRequests;

    VLOG(9) << "GenerateS3Request start request index:" << request.index
            << ",chunkPos:" << request.chunkPos << ",len:" << request.len
            << ",bufOffset:" << request.bufOffset;
    readRequests.emplace(request.chunkPos, request);
    for (int i = s3ChunkInfoList.s3chunks_size() - 1; i >= 0; i--) {
        const S3ChunkInfo &s3ChunkInfo = s3ChunkInfoList.s3chunks(i);
        // readRequests is split by current s3ChunkInfo, emplace_back to the
        // addReadRequests
        std::vector<ReadRequest> addReadRequests;
        // if readRequest is split to one or two, old readRequest should be
        // delete,
        std::vector<uint64_t> deletingReq;
        for (auto readRequestIter = readRequests.begin();
             readRequestIter != readRequests.end(); readRequestIter++) {
            HandleReadRequest(readRequestIter->second, s3ChunkInfo,
                              &addReadRequests, &deletingReq, requests, dataBuf,
                              fsId, inodeId);
        }

        for (auto iter = deletingReq.begin(); iter != deletingReq.end();
             iter++) {
            readRequests.erase(*iter);
        }

        for (auto addIter = addReadRequests.begin();
             addIter != addReadRequests.end(); addIter++) {
            auto ret = readRequests.emplace(addIter->chunkPos, *addIter);
            if (!ret.second) {
                LOG(ERROR) << "read request emplace failed. chunkPos:"
                           << addIter->chunkPos << ",len:" << addIter->len
                           << ",index:" << addIter->index
                           << ",bufOffset:" << addIter->bufOffset;
            }
        }

        if (readRequests.empty()) {
            VLOG(6) << "readRequests has hit s3ChunkInfos.";
            break;
        }
    }

    for (auto emptyIter = readRequests.begin(); emptyIter != readRequests.end();
         emptyIter++) {
        VLOG(9) << "empty buf index:" << emptyIter->second.index
                << ", chunkPos:" << emptyIter->second.chunkPos
                << ", len:" << emptyIter->second.len
                << ", bufOffset:" << emptyIter->second.bufOffset;
        memset(dataBuf + emptyIter->second.bufOffset, 0, emptyIter->second.len);
    }

    auto s3RequestIter = requests->begin();
    for (; s3RequestIter != requests->end(); s3RequestIter++) {
        VLOG(9) << "s3Request chunkid:" << s3RequestIter->chunkId
                << ",offset:" << s3RequestIter->offset
                << ",len:" << s3RequestIter->len
                << ",objectOffset:" << s3RequestIter->objectOffset
                << ",readOffset:" << s3RequestIter->readOffset
                << ",fsid:" << s3RequestIter->fsId
                << ",inodeId:" << s3RequestIter->inodeId
                << ",compaction:" << s3RequestIter->compaction;
    }

    return;
}


void FileCacheManager::ReleaseCache() {
    WriteLockGuard writeLockGuard(rwLock_);

    uint64_t chunNum = chunkCacheMap_.size();
    for (auto &chunk : chunkCacheMap_) {
        chunk.second->ReleaseCache();
    }

    chunkCacheMap_.clear();
    g_s3MultiManagerMetric->chunkManagerNum
        << -1 * static_cast<int64_t>(chunNum);
    return;
}

void FileCacheManager::TruncateCache(uint64_t offset, uint64_t fileSize) {
    uint64_t chunkSize = s3ClientAdaptor_->GetChunkSize();
    uint64_t chunkIndex = offset / chunkSize;
    uint64_t chunkPos = offset % chunkSize;
    int chunkLen = 0;
    uint64_t truncateLen = fileSize - offset;
    //  Truncate processing according to chunk polling
    while (truncateLen > 0) {
        if (chunkPos + truncateLen > chunkSize) {
            chunkLen = chunkSize - chunkPos;
        } else {
            chunkLen = truncateLen;
        }
        ChunkCacheManagerPtr chunkCacheManager =
            FindOrCreateChunkCacheManager(chunkIndex);
        chunkCacheManager->TruncateCache(chunkPos);
        truncateLen -= chunkLen;
        chunkIndex++;
        chunkPos = (chunkPos + chunkLen) % chunkSize;
    }

    return;
}

CURVEFS_ERROR FileCacheManager::Flush(bool force, bool toS3) {
    (void)toS3;
    // Todo: concurrent flushes within one file
    // instead of multiple file flushes may be better
    CURVEFS_ERROR ret = CURVEFS_ERROR::OK;
    std::map<uint64_t, ChunkCacheManagerPtr> tmp;
    {
        WriteLockGuard writeLockGuard(rwLock_);
        tmp = chunkCacheMap_;
    }

    std::atomic<uint64_t> pendingReq(0);
    curve::common::CountDownEvent cond(1);
    FlushChunkCacheCallBack cb =
        [&](const std::shared_ptr<FlushChunkCacheContext> &context) {
            ret = context->retCode;
            if (context->retCode != CURVEFS_ERROR::OK) {
                LOG(ERROR) << "fileCacheManager Flush error, ret:" << ret
                           << ", inode: " << context->inode << ", chunkIndex: "
                           << context->chunkCacheManptr->GetIndex();
                cond.Signal();
                return;
            }

            {
                WriteLockGuard writeLockGuard(rwLock_);
                auto iter1 =
                    chunkCacheMap_.find(context->chunkCacheManptr->GetIndex());
                if (iter1 != chunkCacheMap_.end() && iter1->second->IsEmpty() &&
                    (iter1->second.use_count() <= 3)) {
                    // tmp、chunkCacheMap_ and context->chunkCacheManptr has
                    // this ChunkCacheManagerPtr, so count is 3 if count more
                    // than 3, this mean someone thread has this
                    // ChunkCacheManagerPtr
                    VLOG(6)
                        << "ChunkCacheManagerPtr count:"
                        << context->chunkCacheManptr.use_count()
                        << ",inode:" << inode_
                        << ", index:" << context->chunkCacheManptr->GetIndex()
                        << "erase iter: " << iter1->first;
                    chunkCacheMap_.erase(iter1);
                    g_s3MultiManagerMetric->chunkManagerNum << -1;
                }
            }

            if (pendingReq.fetch_sub(1, std::memory_order_seq_cst) == 1) {
                VLOG(9) << "pendingReq is over";
                cond.Signal();
            }
        };
    std::vector<std::shared_ptr<FlushChunkCacheContext>> flushTasks;
    auto iter = tmp.begin();
    VLOG(6) << "flush size is: " << tmp.size() << ",inodeId:" << inode_;
    for (; iter != tmp.end(); iter++) {
        auto context = std::make_shared<FlushChunkCacheContext>();
        context->inode = inode_;
        context->cb = cb;
        context->force = force;
        context->chunkCacheManptr = iter->second;
        flushTasks.emplace_back(context);
    }
    pendingReq.fetch_add(flushTasks.size(), std::memory_order_seq_cst);
    if (pendingReq.load(std::memory_order_seq_cst)) {
        VLOG(6) << "wait for pendingReq";
        for (auto iter = flushTasks.begin(); iter != flushTasks.end(); ++iter) {
            s3ClientAdaptor_->Enqueue(*iter);
        }
        cond.Wait();
    }
    VLOG(6) << "file cache flush over";
    return ret;
}

void ChunkCacheManager::ReadChunk(uint64_t index, uint64_t chunkPos,
                                  uint64_t readLen, char *dataBuf,
                                  uint64_t dataBufOffset,
                                  std::vector<ReadRequest> *requests) {
    (void)index;
    ReadLockGuard readLockGuard(rwLockChunk_);
    std::vector<ReadRequest> cacheMissWriteRequests, cacheMissFlushDataRequest;
    // read by write cache
    ReadByWriteCache(chunkPos, readLen, dataBuf, dataBufOffset,
                     &cacheMissWriteRequests);

    // read by flushing data cache
    flushingDataCacheMtx_.lock();
    if (!IsFlushDataEmpty()) {
        // read by flushing data cache
        for (auto request : cacheMissWriteRequests) {
            std::vector<ReadRequest> tmpRequests;
            ReadByFlushData(request.chunkPos, request.len, dataBuf,
                            request.bufOffset, &tmpRequests);
            cacheMissFlushDataRequest.insert(cacheMissFlushDataRequest.end(),
                                             tmpRequests.begin(),
                                             tmpRequests.end());
        }
        flushingDataCacheMtx_.unlock();

        // read by read cache
        for (auto request : cacheMissFlushDataRequest) {
            std::vector<ReadRequest> tmpRequests;
            ReadByReadCache(request.chunkPos, request.len, dataBuf,
                            request.bufOffset, &tmpRequests);
            requests->insert(requests->end(), tmpRequests.begin(),
                             tmpRequests.end());
        }
        return;
    }
    flushingDataCacheMtx_.unlock();

    // read by read cache
    for (auto request : cacheMissWriteRequests) {
        std::vector<ReadRequest> tmpRequests;
        ReadByReadCache(request.chunkPos, request.len, dataBuf,
                        request.bufOffset, &tmpRequests);
        requests->insert(requests->end(), tmpRequests.begin(),
                         tmpRequests.end());
    }

    return;
}

void ChunkCacheManager::ReadByWriteCache(uint64_t chunkPos, uint64_t readLen,
                                         char *dataBuf, uint64_t dataBufOffset,
                                         std::vector<ReadRequest> *requests) {
    ReadLockGuard readLockGuard(rwLockWrite_);

    VLOG(6) << "ReadByWriteCache chunkPos:" << chunkPos
            << ",readLen:" << readLen << ",dataBufOffset:" << dataBufOffset;
    if (dataWCacheMap_.empty()) {
        VLOG(9) << "dataWCacheMap_ is empty";
        ReadRequest request;
        request.index = index_;
        request.len = readLen;
        request.chunkPos = chunkPos;
        request.bufOffset = dataBufOffset;
        requests->emplace_back(request);
        return;
    }

    auto iter = dataWCacheMap_.upper_bound(chunkPos);
    if (iter != dataWCacheMap_.begin()) {
        --iter;
    }

    for (; iter != dataWCacheMap_.end(); iter++) {
        ReadRequest request;
        uint64_t dcChunkPos = iter->second->GetChunkPos();
        uint64_t dcLen = iter->second->GetLen();
        VLOG(6) << "ReadByWriteCache chunkPos:" << chunkPos
                << ",readLen:" << readLen << ",dcChunkPos:" << dcChunkPos
                << ",dcLen:" << dcLen << ",first:" << iter->first;
        assert(iter->first == iter->second->GetChunkPos());
        if (chunkPos + readLen <= dcChunkPos) {
            break;
        } else if ((chunkPos + readLen > dcChunkPos) &&
                   (chunkPos < dcChunkPos)) {
            request.len = dcChunkPos - chunkPos;
            request.chunkPos = chunkPos;
            request.index = index_;
            request.bufOffset = dataBufOffset;
            VLOG(6) << "request: index:" << index_ << ",chunkPos:" << chunkPos
                    << ",len:" << request.len << ",bufOffset:" << dataBufOffset;
            requests->emplace_back(request);
            /*
                 -----               ReadData
                    ------           DataCache
            */
            if (chunkPos + readLen <= dcChunkPos + dcLen) {
                iter->second->CopyDataCacheToBuf(
                    0, chunkPos + readLen - dcChunkPos,
                    dataBuf + request.len + dataBufOffset);
                readLen = 0;
                break;
                /*
                     -----------         ReadData
                        ------           DataCache
                */
            } else {
                iter->second->CopyDataCacheToBuf(
                    0, dcLen, dataBuf + request.len + dataBufOffset);
                readLen = chunkPos + readLen - (dcChunkPos + dcLen);
                dataBufOffset = dcChunkPos + dcLen - chunkPos + dataBufOffset;
                chunkPos = dcChunkPos + dcLen;
            }
        } else if ((chunkPos >= dcChunkPos) &&
                   (chunkPos < dcChunkPos + dcLen)) {
            /*
                     ----              ReadData
                   ---------           DataCache
            */
            if (chunkPos + readLen <= dcChunkPos + dcLen) {
                iter->second->CopyDataCacheToBuf(chunkPos - dcChunkPos, readLen,
                                                 dataBuf + dataBufOffset);
                readLen = 0;
                break;
                /*
                         ----------              ReadData
                       ---------                DataCache
                */
            } else {
                iter->second->CopyDataCacheToBuf(chunkPos - dcChunkPos,
                                                 dcChunkPos + dcLen - chunkPos,
                                                 dataBuf + dataBufOffset);
                readLen = chunkPos + readLen - dcChunkPos - dcLen;
                dataBufOffset = dcChunkPos + dcLen - chunkPos + dataBufOffset;
                chunkPos = dcChunkPos + dcLen;
            }
        } else {
            continue;
        }
    }

    if (readLen > 0) {
        ReadRequest request;
        request.index = index_;
        request.len = readLen;
        request.chunkPos = chunkPos;
        request.bufOffset = dataBufOffset;
        requests->emplace_back(request);
    }
    return;
}

void ChunkCacheManager::ReadByReadCache(uint64_t chunkPos, uint64_t readLen,
                                        char *dataBuf, uint64_t dataBufOffset,
                                        std::vector<ReadRequest> *requests) {
    ReadLockGuard readLockGuard(rwLockRead_);

    VLOG(9) << "ReadByReadCache chunkPos:" << chunkPos << ",readLen:" << readLen
            << ",dataBufOffset:" << dataBufOffset;
    if (dataRCacheMap_.empty()) {
        VLOG(9) << "dataRCacheMap_ is empty";
        ReadRequest request;
        request.index = index_;
        request.len = readLen;
        request.chunkPos = chunkPos;
        request.bufOffset = dataBufOffset;
        requests->emplace_back(request);
        return;
    }

    auto iter = dataRCacheMap_.upper_bound(chunkPos);
    if (iter != dataRCacheMap_.begin()) {
        --iter;
    }

    for (; iter != dataRCacheMap_.end(); ++iter) {
        DataCachePtr &dataCache = (*iter->second);
        ReadRequest request;
        uint64_t dcChunkPos = dataCache->GetChunkPos();
        uint64_t dcLen = dataCache->GetLen();

        VLOG(9) << "ReadByReadCache chunkPos:" << chunkPos
                << ",readLen:" << readLen << ",dcChunkPos:" << dcChunkPos
                << ",dcLen:" << dcLen << ",dataBufOffset:" << dataBufOffset;
        if (chunkPos + readLen <= dcChunkPos) {
            break;
        } else if ((chunkPos + readLen > dcChunkPos) &&
                   (chunkPos < dcChunkPos)) {
            s3ClientAdaptor_->GetFsCacheManager()->Get(iter->second);
            request.len = dcChunkPos - chunkPos;
            request.chunkPos = chunkPos;
            request.index = index_;
            request.bufOffset = dataBufOffset;
            VLOG(9) << "request: index:" << index_ << ",chunkPos:" << chunkPos
                    << ",len:" << request.len << ",bufOffset:" << dataBufOffset;
            requests->emplace_back(request);
            /*
                 -----               ReadData
                    ------           DataCache
            */
            if (chunkPos + readLen <= dcChunkPos + dcLen) {
                dataCache->CopyDataCacheToBuf(
                    0, chunkPos + readLen - dcChunkPos,
                    dataBuf + request.len + dataBufOffset);
                readLen = 0;
                break;
                /*
                     -----------         ReadData
                        ------           DataCache
                */
            } else {
                dataCache->CopyDataCacheToBuf(
                    0, dcLen, dataBuf + request.len + dataBufOffset);
                readLen = chunkPos + readLen - (dcChunkPos + dcLen);
                dataBufOffset = dcChunkPos + dcLen - chunkPos + dataBufOffset;
                chunkPos = dcChunkPos + dcLen;
            }
        } else if ((chunkPos >= dcChunkPos) &&
                   (chunkPos < dcChunkPos + dcLen)) {
            s3ClientAdaptor_->GetFsCacheManager()->Get(iter->second);
            /*
                     ----              ReadData
                   ---------           DataCache
            */
            if (chunkPos + readLen <= dcChunkPos + dcLen) {
                dataCache->CopyDataCacheToBuf(chunkPos - dcChunkPos, readLen,
                                              dataBuf + dataBufOffset);
                readLen = 0;
                break;
                /*
                         ----------              ReadData
                       ---------                DataCache
                */
            } else {
                dataCache->CopyDataCacheToBuf(chunkPos - dcChunkPos,
                                              dcChunkPos + dcLen - chunkPos,
                                              dataBuf + dataBufOffset);
                readLen = chunkPos + readLen - dcChunkPos - dcLen;
                dataBufOffset = dcChunkPos + dcLen - chunkPos + dataBufOffset;
                chunkPos = dcChunkPos + dcLen;
            }
        }
    }

    if (readLen > 0) {
        ReadRequest request;
        request.index = index_;
        request.len = readLen;
        request.chunkPos = chunkPos;
        request.bufOffset = dataBufOffset;
        VLOG(9) << "request: index:" << index_ << ",chunkPos:" << chunkPos
                << ",len:" << request.len << ",bufOffset:" << dataBufOffset;
        requests->emplace_back(request);
    }
    return;
}

void ChunkCacheManager::ReadByFlushData(uint64_t chunkPos, uint64_t readLen,
                                        char *dataBuf, uint64_t dataBufOffset,
                                        std::vector<ReadRequest> *requests) {
    uint64_t dcChunkPos = flushingDataCache_->GetChunkPos();
    uint64_t dcLen = flushingDataCache_->GetLen();
    ReadRequest request;
    VLOG(9) << "ReadByFlushData chunkPos: " << chunkPos
            << ", readLen: " << readLen << ", dcChunkPos: " << dcChunkPos
            << ", dcLen: " << dcLen;
    if (chunkPos + readLen <= dcChunkPos) {
        request.index = index_;
        request.len = readLen;
        request.chunkPos = chunkPos;
        request.bufOffset = dataBufOffset;
        requests->emplace_back(request);
        return;
    } else if ((chunkPos + readLen > dcChunkPos) && (chunkPos < dcChunkPos)) {
        request.len = dcChunkPos - chunkPos;
        request.chunkPos = chunkPos;
        request.index = index_;
        request.bufOffset = dataBufOffset;
        VLOG(6) << "request: index:" << index_ << ",chunkPos:" << chunkPos
                << ",len:" << request.len << ",bufOffset:" << dataBufOffset;
        requests->emplace_back(request);
        /*
             -----               ReadData
                ------           DataCache
        */
        if (chunkPos + readLen <= dcChunkPos + dcLen) {
            flushingDataCache_->CopyDataCacheToBuf(
                0, chunkPos + readLen - dcChunkPos,
                dataBuf + request.len + dataBufOffset);
            readLen = 0;
            return;
            /*
                 -----------         ReadData
                    ------           DataCache
            */
        } else {
            flushingDataCache_->CopyDataCacheToBuf(
                0, dcLen, dataBuf + request.len + dataBufOffset);
            readLen = chunkPos + readLen - (dcChunkPos + dcLen);
            dataBufOffset = dcChunkPos + dcLen - chunkPos + dataBufOffset;
            chunkPos = dcChunkPos + dcLen;
        }
    } else if ((chunkPos >= dcChunkPos) && (chunkPos < dcChunkPos + dcLen)) {
        /*
                 ----              ReadData
               ---------           DataCache
        */
        if (chunkPos + readLen <= dcChunkPos + dcLen) {
            flushingDataCache_->CopyDataCacheToBuf(
                chunkPos - dcChunkPos, readLen, dataBuf + dataBufOffset);
            readLen = 0;
            return;
            /*
                     ----------              ReadData
                   ---------                DataCache
            */
        } else {
            flushingDataCache_->CopyDataCacheToBuf(
                chunkPos - dcChunkPos, dcChunkPos + dcLen - chunkPos,
                dataBuf + dataBufOffset);
            readLen = chunkPos + readLen - dcChunkPos - dcLen;
            dataBufOffset = dcChunkPos + dcLen - chunkPos + dataBufOffset;
            chunkPos = dcChunkPos + dcLen;
        }
    }

    if (readLen > 0) {
        request.index = index_;
        request.len = readLen;
        request.chunkPos = chunkPos;
        request.bufOffset = dataBufOffset;
        requests->emplace_back(request);
    }
    return;
}

DataCachePtr ChunkCacheManager::FindWriteableDataCache(
    uint64_t chunkPos, uint64_t len,
    std::vector<DataCachePtr> *mergeDataCacheVer, uint64_t inodeId) {
    WriteLockGuard writeLockGuard(rwLockWrite_);

    auto iter = dataWCacheMap_.upper_bound(chunkPos);
    if (iter != dataWCacheMap_.begin()) {
        --iter;
    }
    for (; iter != dataWCacheMap_.end(); iter++) {
        VLOG(12) << "FindWriteableDataCache chunkPos:"
                 << iter->second->GetChunkPos()
                 << ",len:" << iter->second->GetLen() << ",inodeId: " << inodeId
                 << ",chunkIndex:" << index_;
        assert(iter->first == iter->second->GetChunkPos());
        if (((chunkPos + len) >= iter->second->GetChunkPos()) &&
            (chunkPos <=
             iter->second->GetChunkPos() + iter->second->GetLen())) {
            DataCachePtr dataCache = iter->second;
            std::vector<uint64_t> waitDelVec;
            while (1) {
                iter++;
                if (iter == dataWCacheMap_.end()) {
                    break;
                }
                if ((chunkPos + len) < iter->second->GetChunkPos()) {
                    break;
                }

                mergeDataCacheVer->emplace_back(iter->second);
                waitDelVec.push_back(iter->first);
            }

            std::vector<uint64_t>::iterator iterDel = waitDelVec.begin();
            for (; iterDel != waitDelVec.end(); ++iterDel) {
                auto iter = dataWCacheMap_.find(*iterDel);
                VLOG(9) << "delete data cache chunkPos:"
                        << iter->second->GetChunkPos()
                        << ", len:" << iter->second->GetLen()
                        << ",inode:" << inodeId << ",chunkIndex:" << index_;
                s3ClientAdaptor_->GetFsCacheManager()->DataCacheNumFetchSub(1);
                VLOG(9) << "FindWriteableDataCache() DataCacheByteDec1 len:"
                        << iter->second->GetLen();
                s3ClientAdaptor_->GetFsCacheManager()->DataCacheByteDec(
                    iter->second->GetActualLen());
                dataWCacheMap_.erase(iter);
            }
            return dataCache;
        }
    }
    return nullptr;
}


void ChunkCacheManager::WriteNewDataCache(S3ClientAdaptorImpl *s3ClientAdaptor,
                                          uint32_t chunkPos, uint32_t len,
                                          const char *data) {
    DataCachePtr dataCache =
        std::make_shared<DataCache>(s3ClientAdaptor, this->shared_from_this(),
                                    chunkPos, len, data, kvClientManager_);
    VLOG(9) << "WriteNewDataCache chunkPos:" << chunkPos << ", len:" << len
            << ", new len:" << dataCache->GetLen() << ",chunkIndex:" << index_;
    WriteLockGuard writeLockGuard(rwLockWrite_);

    auto ret = dataWCacheMap_.emplace(chunkPos, dataCache);
    if (!ret.second) {
        LOG(ERROR) << "dataCache emplace failed.";
        return;
    }
    s3ClientAdaptor_->FsSyncSignalAndDataCacheInc();
    s3ClientAdaptor_->GetFsCacheManager()->DataCacheByteInc(
        dataCache->GetActualLen());
    return;
}

void ChunkCacheManager::AddReadDataCache(DataCachePtr dataCache) {
    uint64_t chunkPos = dataCache->GetChunkPos();
    uint64_t len = dataCache->GetLen();
    WriteLockGuard writeLockGuard(rwLockRead_);
    std::vector<uint64_t> deleteKeyVec;
    auto iter = dataRCacheMap_.begin();
    for (; iter != dataRCacheMap_.end(); iter++) {
        if (chunkPos + len <= iter->first) {
            break;
        }
        std::list<DataCachePtr>::iterator dcpIter = iter->second;
        uint64_t dcChunkPos = (*dcpIter)->GetChunkPos();
        uint64_t dcLen = (*dcpIter)->GetLen();
        if ((chunkPos + len > dcChunkPos) && (chunkPos < dcChunkPos + dcLen)) {
            VLOG(9) << "read cache chunkPos:" << chunkPos << ",len:" << len
                    << "is overlap with datacache chunkPos:" << dcChunkPos
                    << ",len:" << dcLen << ", index:" << index_;
            deleteKeyVec.emplace_back(dcChunkPos);
        }
    }
    for (auto key : deleteKeyVec) {
        auto iter = dataRCacheMap_.find(key);
        std::list<DataCachePtr>::iterator dcpIter = iter->second;
        uint64_t actualLen = (*dcpIter)->GetActualLen();
        if (s3ClientAdaptor_->GetFsCacheManager()->Delete(dcpIter)) {
            g_s3MultiManagerMetric->readDataCacheNum << -1;
            g_s3MultiManagerMetric->readDataCacheByte
                << -1 * static_cast<int64_t>(actualLen);
            dataRCacheMap_.erase(iter);
        }
    }
    std::list<DataCachePtr>::iterator outIter;
    bool ret = s3ClientAdaptor_->GetFsCacheManager()->Set(dataCache, &outIter);
    if (ret) {
        g_s3MultiManagerMetric->readDataCacheNum << 1;
        g_s3MultiManagerMetric->readDataCacheByte << dataCache->GetActualLen();
        dataRCacheMap_.emplace(chunkPos, outIter);
    }
}

void ChunkCacheManager::ReleaseReadDataCache(uint64_t key) {
    WriteLockGuard writeLockGuard(rwLockRead_);

    auto iter = dataRCacheMap_.find(key);
    if (iter == dataRCacheMap_.end()) {
        return;
    }
    g_s3MultiManagerMetric->readDataCacheNum << -1;
    g_s3MultiManagerMetric->readDataCacheByte
        << -1 * (*(iter->second))->GetActualLen();
    dataRCacheMap_.erase(iter);
    return;
}

void ChunkCacheManager::ReleaseCache() {
    {
        WriteLockGuard writeLockGuard(rwLockWrite_);

        for (auto &dataWCache : dataWCacheMap_) {
            s3ClientAdaptor_->GetFsCacheManager()->DataCacheNumFetchSub(1);
            s3ClientAdaptor_->GetFsCacheManager()->DataCacheByteDec(
                dataWCache.second->GetActualLen());
        }
        dataWCacheMap_.clear();
        s3ClientAdaptor_->GetFsCacheManager()->FlushSignal();
    }
    WriteLockGuard writeLockGuard(rwLockRead_);
    for (auto iter = dataRCacheMap_.begin(); iter != dataRCacheMap_.end();) {
        if (s3ClientAdaptor_->GetFsCacheManager()->Delete(iter->second)) {
            g_s3MultiManagerMetric->readDataCacheNum << -1;
            g_s3MultiManagerMetric->readDataCacheByte
                << -1 * (*(iter->second))->GetActualLen();
            iter = dataRCacheMap_.erase(iter);
        } else {
            ++iter;
        }
    }
}

void ChunkCacheManager::TruncateCache(uint64_t chunkPos) {
    WriteLockGuard writeLockGuard(rwLockChunk_);

    TruncateWriteCache(chunkPos);
    TruncateReadCache(chunkPos);
}

void ChunkCacheManager::TruncateWriteCache(uint64_t chunkPos) {
    WriteLockGuard writeLockGuard(rwLockWrite_);
    auto rIter = dataWCacheMap_.rbegin();
    for (; rIter != dataWCacheMap_.rend();) {
        uint64_t dcChunkPos = rIter->second->GetChunkPos();
        uint64_t dcLen = rIter->second->GetLen();
        uint64_t dcActualLen = rIter->second->GetActualLen();
        if (dcChunkPos >= chunkPos) {
            s3ClientAdaptor_->GetFsCacheManager()->DataCacheNumFetchSub(1);
            s3ClientAdaptor_->GetFsCacheManager()->DataCacheByteDec(
                dcActualLen);
            dataWCacheMap_.erase(next(rIter).base());
        } else if ((dcChunkPos < chunkPos) &&
                   ((dcChunkPos + dcLen) > chunkPos)) {
            rIter->second->Truncate(chunkPos - dcChunkPos);
            s3ClientAdaptor_->GetFsCacheManager()->DataCacheByteDec(
                dcActualLen - rIter->second->GetActualLen());
            break;
        } else {
            break;
        }
    }
}

void ChunkCacheManager::TruncateReadCache(uint64_t chunkPos) {
    WriteLockGuard writeLockGuard(rwLockRead_);
    auto rIter = dataRCacheMap_.rbegin();
    for (; rIter != dataRCacheMap_.rend();) {
        uint64_t dcChunkPos = (*rIter->second)->GetChunkPos();
        uint64_t dcLen = (*rIter->second)->GetLen();
        uint64_t dcActualLen = (*rIter->second)->GetActualLen();
        if ((dcChunkPos + dcLen) > chunkPos) {
            if (s3ClientAdaptor_->GetFsCacheManager()->Delete(rIter->second)) {
                g_s3MultiManagerMetric->readDataCacheNum << -1;
                g_s3MultiManagerMetric->readDataCacheByte
                    << -1 * static_cast<int64_t>(dcActualLen);
                dataRCacheMap_.erase(next(rIter).base());
            }
        } else {
            break;
        }
    }
}

void ChunkCacheManager::ReleaseWriteDataCache(const DataCachePtr &dataCache) {
    s3ClientAdaptor_->GetFsCacheManager()->DataCacheNumFetchSub(1);
    VLOG(9) << "chunk flush DataCacheByteDec len:" << dataCache->GetActualLen();
    s3ClientAdaptor_->GetFsCacheManager()->DataCacheByteDec(
        dataCache->GetActualLen());
    if (!s3ClientAdaptor_->GetFsCacheManager()->WriteCacheIsFull()) {
        VLOG(9) << "write cache is not full, signal wait.";
        s3ClientAdaptor_->GetFsCacheManager()->FlushSignal();
    }
}

CURVEFS_ERROR ChunkCacheManager::Flush(uint64_t inodeId, bool force,
                                       bool toS3) {
    curve::common::LockGuard lg(flushMtx_);
    CURVEFS_ERROR ret = CURVEFS_ERROR::OK;
    // DataCachePtr dataCache;
    while (1) {
        bool isFlush = false;
        {
            WriteLockGuard writeLockGuard(rwLockChunk_);

            auto iter = dataWCacheMap_.begin();
            while (iter != dataWCacheMap_.end()) {
                if (iter->second->CanFlush(force)) {
                    {
                        curve::common::LockGuard lg(flushingDataCacheMtx_);
                        flushingDataCache_ = std::move(iter->second);
                    }
                    dataWCacheMap_.erase(iter);
                    isFlush = true;
                    break;
                } else {
                    iter++;
                }
            }
        }
        if (isFlush) {
            VLOG(9) << "Flush datacache chunkPos:"
                    << flushingDataCache_->GetChunkPos()
                    << ",len:" << flushingDataCache_->GetLen()
                    << ",inodeId:" << inodeId << ",chunkIndex:" << index_;
            assert(flushingDataCache_->IsDirty());
            do {
                ret = flushingDataCache_->Flush(inodeId, toS3);
                if (ret == CURVEFS_ERROR::NOTEXIST) {
                    LOG(WARNING) << "dataCache flush failed. ret:" << ret
                                 << ",index:" << index_ << ",data chunkpos:"
                                 << flushingDataCache_->GetChunkPos();
                    ReleaseWriteDataCache(flushingDataCache_);
                    break;
                } else if (ret == CURVEFS_ERROR::INTERNAL) {
                    LOG(WARNING) << "dataCache flush failed. ret:" << ret
                                 << ",index:" << index_ << ",data chunkpos:"
                                 << flushingDataCache_->GetChunkPos()
                                 << ", should retry.";
                    ::sleep(3);
                    continue;
                }
                VLOG(9) << "ReleaseWriteDataCache chunkPos:"
                        << flushingDataCache_->GetChunkPos()
                        << ",len:" << flushingDataCache_->GetLen()
                        << ",inodeId:" << inodeId << ",chunkIndex:" << index_;
                if (!curvefs::client::common::FLAGS_enableCto) {
                    WriteLockGuard lockGuard(rwLockChunk_);
                    AddReadDataCache(flushingDataCache_);
                }
                ReleaseWriteDataCache(flushingDataCache_);
            } while (ret != CURVEFS_ERROR::OK);
            {
                curve::common::LockGuard lg(flushingDataCacheMtx_);
                flushingDataCache_ = nullptr;
            }
        } else {
            VLOG(9) << "can not find flush datacache";
            break;
        }
    }
    return CURVEFS_ERROR::OK;
}

void ChunkCacheManager::UpdateWriteCacheMap(uint64_t oldChunkPos,
                                            DataCache *pDataCache) {
    auto iter = dataWCacheMap_.find(oldChunkPos);
    DataCachePtr datacache;
    if (iter != dataWCacheMap_.end()) {
        datacache = iter->second;
        dataWCacheMap_.erase(iter);
    } else {
        datacache = pDataCache->shared_from_this();
    }
    auto ret = dataWCacheMap_.emplace(datacache->GetChunkPos(), datacache);
    assert(ret.second);
    (void)ret;
}

void ChunkCacheManager::AddWriteDataCacheForTest(DataCachePtr dataCache) {
    WriteLockGuard writeLockGuard(rwLockWrite_);

    dataWCacheMap_.emplace(dataCache->GetChunkPos(), dataCache);
    s3ClientAdaptor_->GetFsCacheManager()->DataCacheNumInc();
    s3ClientAdaptor_->GetFsCacheManager()->DataCacheByteInc(
        dataCache->GetActualLen());
}

DataCache::DataCache(S3ClientAdaptorImpl *s3ClientAdaptor,
                     ChunkCacheManagerPtr chunkCacheManager, uint64_t chunkPos,
                     uint64_t len, const char *data,
                     std::shared_ptr<KVClientManager> kvClientManager)
    : s3ClientAdaptor_(std::move(s3ClientAdaptor)),
      chunkCacheManager_(chunkCacheManager), status_(DataCacheStatus::Dirty),
      inReadCache_(false) {
    uint64_t blockSize = s3ClientAdaptor->GetBlockSize();
    uint32_t pageSize = s3ClientAdaptor->GetPageSize();
    chunkPos_ = chunkPos;
    len_ = len;
    actualChunkPos_ = chunkPos - chunkPos % pageSize;

    uint64_t headZeroLen = chunkPos - actualChunkPos_;
    uint64_t blockIndex = chunkPos / blockSize;
    uint64_t blockPos = chunkPos % blockSize;
    uint64_t pageIndex, pagePos;
    uint64_t n, m, blockLen;
    uint64_t dataOffset = 0;
    uint64_t tailZeroLen = 0;

    while (len > 0) {
        if (blockPos + len > blockSize) {
            n = blockSize - blockPos;
        } else {
            n = len;
        }
        PageDataMap &pdMap = dataMap_[blockIndex];
        blockLen = n;
        pageIndex = blockPos / pageSize;
        pagePos = blockPos % pageSize;
        while (blockLen > 0) {
            if (pagePos + blockLen > pageSize) {
                m = pageSize - pagePos;
            } else {
                m = blockLen;
            }

            PageData *pageData = new PageData();
            pageData->data = new char[pageSize];
            memset(pageData->data, 0, pageSize);
            memcpy(pageData->data + pagePos, data + dataOffset, m);
            if (pagePos + m < pageSize) {
                tailZeroLen = pageSize - pagePos - m;
            }
            pageData->index = pageIndex;
            assert(pdMap.count(pageIndex) == 0);
            pdMap.emplace(pageIndex, pageData);
            pageIndex++;
            blockLen -= m;
            dataOffset += m;
            pagePos = (pagePos + m) % pageSize;
        }

        blockIndex++;
        len -= n;
        blockPos = (blockPos + n) % blockSize;
    }
    actualLen_ = headZeroLen + len_ + tailZeroLen;
    assert((actualLen_ % pageSize) == 0);
    assert((actualChunkPos_ % pageSize) == 0);
    createTime_ = ::curve::common::TimeUtility::GetTimeofDaySec();

    kvClientManager_ = std::move(kvClientManager);
}

void DataCache::CopyBufToDataCache(uint64_t dataCachePos, uint64_t len,
                                   const char *data) {
    uint64_t blockSize = s3ClientAdaptor_->GetBlockSize();
    uint32_t pageSize = s3ClientAdaptor_->GetPageSize();
    uint64_t pos = chunkPos_ + dataCachePos;
    uint64_t blockIndex = pos / blockSize;
    uint64_t blockPos = pos % blockSize;
    uint64_t pageIndex, pagePos;
    uint64_t n, blockLen, m;
    uint64_t dataOffset = 0;
    uint64_t addLen = 0;

    VLOG(9) << "CopyBufToDataCache() dataCachePos:" << dataCachePos
            << ", len:" << len << ", chunkPos_:" << chunkPos_
            << ", len_:" << len_;
    if (dataCachePos + len > len_) {
        len_ = dataCachePos + len;
    }
    while (len > 0) {
        if (blockPos + len > blockSize) {
            n = blockSize - blockPos;
        } else {
            n = len;
        }
        blockLen = n;
        PageDataMap &pdMap = dataMap_[blockIndex];
        PageData *pageData;
        pageIndex = blockPos / pageSize;
        pagePos = blockPos % pageSize;
        while (blockLen > 0) {
            if (pagePos + blockLen > pageSize) {
                m = pageSize - pagePos;
            } else {
                m = blockLen;
            }
            if (pdMap.count(pageIndex)) {
                pageData = pdMap[pageIndex];
            } else {
                pageData = new PageData();
                pageData->data = new char[pageSize];
                memset(pageData->data, 0, pageSize);
                pageData->index = pageIndex;
                pdMap.emplace(pageIndex, pageData);
                addLen += pageSize;
            }
            memcpy(pageData->data + pagePos, data + dataOffset, m);
            pageIndex++;
            blockLen -= m;
            dataOffset += m;
            pagePos = (pagePos + m) % pageSize;
        }

        blockIndex++;
        len -= n;
        blockPos = (blockPos + n) % blockSize;
    }
    actualLen_ += addLen;
    VLOG(9) << "chunkPos:" << chunkPos_ << ", len:" << len_
            << ",actualChunkPos_:" << actualChunkPos_
            << ",actualLen:" << actualLen_;
}

void DataCache::AddDataBefore(uint64_t len, const char *data) {
    uint64_t blockSize = s3ClientAdaptor_->GetBlockSize();
    uint32_t pageSize = s3ClientAdaptor_->GetPageSize();
    uint64_t tmpLen = len;
    uint64_t newChunkPos = chunkPos_ - len;
    uint64_t blockIndex = newChunkPos / blockSize;
    uint64_t blockPos = newChunkPos % blockSize;
    uint64_t pageIndex, pagePos;
    uint64_t n, m, blockLen;
    uint64_t dataOffset = 0;

    VLOG(9) << "AddDataBefore() len:" << len << ", len_:" << len_
            << "chunkPos:" << chunkPos_ << ",actualChunkPos:" << actualChunkPos_
            << ",len:" << len_ << ",actualLen:" << actualLen_;
    while (tmpLen > 0) {
        if (blockPos + tmpLen > blockSize) {
            n = blockSize - blockPos;
        } else {
            n = tmpLen;
        }

        PageDataMap &pdMap = dataMap_[blockIndex];
        blockLen = n;
        PageData *pageData = NULL;
        pageIndex = blockPos / pageSize;
        pagePos = blockPos % pageSize;
        while (blockLen > 0) {
            if (pagePos + blockLen > pageSize) {
                m = pageSize - pagePos;
            } else {
                m = blockLen;
            }

            if (pdMap.count(pageIndex)) {
                pageData = pdMap[pageIndex];
            } else {
                pageData = new PageData();
                pageData->data = new char[pageSize];
                memset(pageData->data, 0, pageSize);
                pageData->index = pageIndex;
                pdMap.emplace(pageIndex, pageData);
            }
            memcpy(pageData->data + pagePos, data + dataOffset, m);
            pageIndex++;
            blockLen -= m;
            dataOffset += m;
            pagePos = (pagePos + m) % pageSize;
        }
        blockIndex++;
        tmpLen -= n;
        blockPos = (blockPos + n) % blockSize;
    }
    chunkPos_ = newChunkPos;
    actualChunkPos_ = chunkPos_ - chunkPos_ % pageSize;
    len_ += len;
    if ((chunkPos_ + len_ - actualChunkPos_) % pageSize == 0) {
        actualLen_ = chunkPos_ + len_ - actualChunkPos_;
    } else {
        actualLen_ =
            ((chunkPos_ + len_ - actualChunkPos_) / pageSize + 1) * pageSize;
    }
    VLOG(9) << "chunkPos:" << chunkPos_ << ", len:" << len_
            << ",actualChunkPos_:" << actualChunkPos_
            << ",actualLen:" << actualLen_;
}

void DataCache::MergeDataCacheToDataCache(DataCachePtr mergeDataCache,
                                          uint64_t dataOffset, uint64_t len) {
    uint64_t blockSize = s3ClientAdaptor_->GetBlockSize();
    uint32_t pageSize = s3ClientAdaptor_->GetPageSize();
    uint64_t maxPageInBlock = blockSize / pageSize;
    uint64_t chunkPos = mergeDataCache->GetChunkPos() + dataOffset;
    assert(chunkPos == (chunkPos_ + len_));
    uint64_t blockIndex = chunkPos / blockSize;
    uint64_t blockPos = chunkPos % blockSize;
    uint64_t pageIndex = blockPos / pageSize;
    uint64_t pagePos = blockPos % pageSize;
    char *data = nullptr;
    PageData *mergePage = nullptr;
    PageDataMap *pdMap = &dataMap_[blockIndex];
    uint64_t n = 0;

    VLOG(9) << "MergeDataCacheToDataCache dataOffset:" << dataOffset
            << ", len:" << len << ",dataCache chunkPos:" << chunkPos_
            << ", len:" << len_
            << "mergeData chunkPos:" << mergeDataCache->GetChunkPos()
            << ", len:" << mergeDataCache->GetLen();
    assert((dataOffset + len) == mergeDataCache->GetLen());
    len_ += len;
    while (len > 0) {
        if (pageIndex == maxPageInBlock) {
            blockIndex++;
            pageIndex = 0;
            pdMap = &dataMap_[blockIndex];
        }
        mergePage = mergeDataCache->GetPageData(blockIndex, pageIndex);
        assert(mergePage);
        if (pdMap->count(pageIndex)) {
            data = (*pdMap)[pageIndex]->data;
            if (pagePos + len > pageSize) {
                n = pageSize - pagePos;
            } else {
                n = len;
            }
            VLOG(9) << "MergeDataCacheToDataCache n:" << n
                    << ", pagePos:" << pagePos;
            memcpy(data + pagePos, mergePage->data + pagePos, n);
            // mergeDataCache->ReleasePageData(blockIndex, pageIndex);
        } else {
            pdMap->emplace(pageIndex, mergePage);
            mergeDataCache->ErasePageData(blockIndex, pageIndex);
            n = pageSize;
            actualLen_ += pageSize;
            VLOG(9) << "MergeDataCacheToDataCache n:" << n;
        }

        if (len >= n) {
            len -= n;
        } else {
            len = 0;
        }
        pageIndex++;
        pagePos = 0;
    }
    VLOG(9) << "MergeDataCacheToDataCache end chunkPos:" << chunkPos_
            << ", len:" << len_ << ", actualChunkPos:" << actualChunkPos_
            << ", actualLen:" << actualLen_;
    return;
}

void DataCache::Write(uint64_t chunkPos, uint64_t len, const char *data,
                      const std::vector<DataCachePtr> &mergeDataCacheVer) {
    uint64_t addByte = 0;
    uint64_t oldSize = 0;
    VLOG(9) << "DataCache Write() chunkPos:" << chunkPos << ",len:" << len
            << ",dataCache's chunkPos:" << chunkPos_
            << ",actualChunkPos:" << actualChunkPos_
            << ",dataCache's len:" << len_ << ", actualLen:" << actualLen_;
    auto iter = mergeDataCacheVer.begin();
    for (; iter != mergeDataCacheVer.end(); iter++) {
        VLOG(9) << "mergeDataCacheVer chunkPos:" << (*iter)->GetChunkPos()
                << ", len:" << (*iter)->GetLen();
    }
    curve::common::LockGuard lg(mtx_);
    status_.store(DataCacheStatus::Dirty, std::memory_order_release);
    uint64_t oldChunkPos = chunkPos_;
    if (chunkPos <= chunkPos_) {
        /*
            ------       DataCache
         -------         WriteData
        */
        if (chunkPos + len <= chunkPos_ + len_) {
            chunkCacheManager_->rwLockWrite_.WRLock();
            oldSize = actualLen_;
            CopyBufToDataCache(0, chunkPos + len - chunkPos_,
                               data + chunkPos_ - chunkPos);
            AddDataBefore(chunkPos_ - chunkPos, data);
            addByte = actualLen_ - oldSize;
            s3ClientAdaptor_->GetFsCacheManager()->DataCacheByteInc(addByte);
            chunkCacheManager_->UpdateWriteCacheMap(oldChunkPos, this);
            chunkCacheManager_->rwLockWrite_.Unlock();
            return;
        } else {
            std::vector<DataCachePtr>::const_iterator iter =
                mergeDataCacheVer.begin();
            for (; iter != mergeDataCacheVer.end(); ++iter) {
                /*
                     ------         ------    DataCache
                  ---------------------       WriteData
                */
                if (chunkPos + len <
                    (*iter)->GetChunkPos() + (*iter)->GetLen()) {
                    chunkCacheManager_->rwLockWrite_.WRLock();
                    oldSize = actualLen_;
                    CopyBufToDataCache(0, chunkPos + len - chunkPos_,
                                       data + chunkPos_ - chunkPos);
                    MergeDataCacheToDataCache(
                        (*iter), chunkPos + len - (*iter)->GetChunkPos(),
                        (*iter)->GetChunkPos() + (*iter)->GetLen() - chunkPos -
                            len);
                    AddDataBefore(chunkPos_ - chunkPos, data);
                    addByte = actualLen_ - oldSize;
                    s3ClientAdaptor_->GetFsCacheManager()->DataCacheByteInc(
                        addByte);
                    chunkCacheManager_->UpdateWriteCacheMap(oldChunkPos, this);
                    chunkCacheManager_->rwLockWrite_.Unlock();
                    return;
                }
            }
            /*
                     ------    ------         DataCache
                  ---------------------       WriteData
            */
            chunkCacheManager_->rwLockWrite_.WRLock();
            oldSize = actualLen_;
            CopyBufToDataCache(0, chunkPos + len - chunkPos_,
                               data + chunkPos_ - chunkPos);
            AddDataBefore(chunkPos_ - chunkPos, data);
            addByte = actualLen_ - oldSize;
            s3ClientAdaptor_->GetFsCacheManager()->DataCacheByteInc(addByte);
            chunkCacheManager_->UpdateWriteCacheMap(oldChunkPos, this);
            chunkCacheManager_->rwLockWrite_.Unlock();
            return;
        }
    } else {
        /*
            --------       DataCache
             -----         WriteData
        */
        if (chunkPos + len <= chunkPos_ + len_) {
            CopyBufToDataCache(chunkPos - chunkPos_, len, data);
            return;
        } else {
            std::vector<DataCachePtr>::const_iterator iter =
                mergeDataCacheVer.begin();
            for (; iter != mergeDataCacheVer.end(); ++iter) {
                /*
                     ------         ------    DataCache
                        ----------------       WriteData
                */
                if (chunkPos + len <
                    (*iter)->GetChunkPos() + (*iter)->GetLen()) {
                    oldSize = actualLen_;

                    CopyBufToDataCache(chunkPos - chunkPos_, len, data);
                    VLOG(9) << "databuf offset:"
                            << chunkPos + len - (*iter)->GetChunkPos();
                    MergeDataCacheToDataCache(
                        (*iter), chunkPos + len - (*iter)->GetChunkPos(),
                        (*iter)->GetChunkPos() + (*iter)->GetLen() - chunkPos -
                            len);
                    addByte = actualLen_ - oldSize;
                    s3ClientAdaptor_->GetFsCacheManager()->DataCacheByteInc(
                        addByte);
                    return;
                }
            }
            /*
                     ------         ------         DataCache
                        --------------------       WriteData
            */
            oldSize = actualLen_;
            CopyBufToDataCache(chunkPos - chunkPos_, len, data);
            addByte = actualLen_ - oldSize;
            s3ClientAdaptor_->GetFsCacheManager()->DataCacheByteInc(addByte);
            return;
        }
    }
    return;
}

void DataCache::Truncate(uint64_t size) {
    uint64_t blockSize = s3ClientAdaptor_->GetBlockSize();
    uint32_t pageSize = s3ClientAdaptor_->GetPageSize();
    assert(size <= len_);

    curve::common::LockGuard lg(mtx_);
    uint64_t truncatePos = chunkPos_ + size;
    uint64_t truncateLen = len_ - size;
    uint64_t blockIndex = truncatePos / blockSize;
    uint64_t pageIndex = truncatePos % blockSize / pageSize;
    uint64_t blockPos = truncatePos % blockSize;
    int n, m, blockLen;
    while (truncateLen > 0) {
        if (blockPos + truncateLen > blockSize) {
            n = blockSize - blockPos;
        } else {
            n = truncateLen;
        }
        PageDataMap &pdMap = dataMap_[blockIndex];
        blockLen = n;
        pageIndex = blockPos / pageSize;
        uint64_t pagePos = blockPos % pageSize;
        PageData *pageData = nullptr;
        while (blockLen > 0) {
            if (pagePos + blockLen > pageSize) {
                m = pageSize - pagePos;
            } else {
                m = blockLen;
            }

            if (pagePos == 0) {
                if (pdMap.count(pageIndex)) {
                    pageData = pdMap[pageIndex];
                    delete pageData->data;
                    pdMap.erase(pageIndex);
                    actualLen_ -= pageSize;
                }
            } else {
                if (pdMap.count(pageIndex)) {
                    pageData = pdMap[pageIndex];
                    memset(pageData->data + pagePos, 0, m);
                }
            }
            pageIndex++;
            blockLen -= m;
            pagePos = (pagePos + m) % pageSize;
        }
        if (pdMap.empty()) {
            dataMap_.erase(blockIndex);
        }
        blockIndex++;
        truncateLen -= n;
        blockPos = (blockPos + n) % blockSize;
    }

    len_ = size;
    uint64_t tmpActualLen;
    if ((chunkPos_ + len_ - actualChunkPos_) % pageSize == 0) {
        tmpActualLen = chunkPos_ + len_ - actualChunkPos_;
    } else {
        tmpActualLen =
            ((chunkPos_ + len_ - actualChunkPos_) / pageSize + 1) * pageSize;
    }
    assert(tmpActualLen == actualLen_);
    (void)tmpActualLen;
    return;
}

void DataCache::Release() {
    chunkCacheManager_->ReleaseReadDataCache(chunkPos_);
}

void DataCache::CopyDataCacheToBuf(uint64_t offset, uint64_t len, char *data) {
    assert(offset + len <= len_);
    uint64_t blockSize = s3ClientAdaptor_->GetBlockSize();
    uint32_t pageSize = s3ClientAdaptor_->GetPageSize();
    uint64_t newChunkPos = chunkPos_ + offset;
    uint64_t blockIndex = newChunkPos / blockSize;
    uint64_t blockPos = newChunkPos % blockSize;
    uint64_t pagePos, pageIndex;
    uint64_t n, m, blockLen;
    uint64_t dataOffset = 0;

    VLOG(9) << "CopyDataCacheToBuf start Offset:" << offset
            << ", newChunkPos:" << newChunkPos << ",len:" << len;

    while (len > 0) {
        if (blockPos + len > blockSize) {
            n = blockSize - blockPos;
        } else {
            n = len;
        }
        blockLen = n;
        PageDataMap &pdMap = dataMap_[blockIndex];
        PageData *pageData = NULL;
        pageIndex = blockPos / pageSize;
        pagePos = blockPos % pageSize;
        while (blockLen > 0) {
            if (pagePos + blockLen > pageSize) {
                m = pageSize - pagePos;
            } else {
                m = blockLen;
            }

            assert(pdMap.count(pageIndex));
            pageData = pdMap[pageIndex];
            memcpy(data + dataOffset, pageData->data + pagePos, m);
            pageIndex++;
            blockLen -= m;
            dataOffset += m;
            pagePos = (pagePos + m) % pageSize;
        }

        blockIndex++;
        len -= n;
        blockPos = (blockPos + n) % blockSize;
    }
    VLOG(9) << "CopyDataCacheToBuf end.";
    return;
}

CURVEFS_ERROR DataCache::Flush(uint64_t inodeId, bool toS3) {
    VLOG(9) << "DataCache Flush. chunkPos=" << chunkPos_ << ", len=" << len_
            << ", chunkIndex=" << chunkCacheManager_->GetIndex()
            << ", inodeId=" << inodeId;

    // generate flush task
    std::vector<std::shared_ptr<PutObjectAsyncContext>> s3Tasks;
    std::vector<std::shared_ptr<SetKVCacheTask>> kvCacheTasks;
    char *data = new (std::nothrow) char[len_];
    if (!data) {
        LOG(ERROR) << "new data failed.";
        return CURVEFS_ERROR::INTERNAL;
    }
    CopyDataCacheToBuf(0, len_, data);
    uint64_t writeOffset = 0;
    uint64_t chunkId = 0;
    CURVEFS_ERROR ret = PrepareFlushTasks(
        inodeId, data, &s3Tasks, &kvCacheTasks, &chunkId, &writeOffset);
    if (CURVEFS_ERROR::OK != ret) {
        return ret;
    }

    // exec flush task
    FlushTaskExecute(GetCachePolicy(toS3), s3Tasks, kvCacheTasks);
    delete[] data;

    // inode ship to flush
    std::shared_ptr<InodeWrapper> inodeWrapper;
    ret = s3ClientAdaptor_->GetInodeCacheManager()->GetInode(inodeId,
                                                             inodeWrapper);
    if (ret != CURVEFS_ERROR::OK) {
        LOG(WARNING) << "get inode fail, ret:" << ret;
        status_.store(DataCacheStatus::Dirty, std::memory_order_release);
        return ret;
    }

    S3ChunkInfo info;
    uint64_t chunkIndex = chunkCacheManager_->GetIndex();
    uint64_t chunkSize = s3ClientAdaptor_->GetChunkSize();
    int64_t offset = chunkIndex * chunkSize + chunkPos_;
    PrepareS3ChunkInfo(chunkId, offset, writeOffset, &info);
    inodeWrapper->AppendS3ChunkInfo(chunkIndex, info);
    s3ClientAdaptor_->GetInodeCacheManager()->ShipToFlush(inodeWrapper);

    return CURVEFS_ERROR::OK;
}

CURVEFS_ERROR DataCache::PrepareFlushTasks(
    uint64_t inodeId, char *data,
    std::vector<std::shared_ptr<PutObjectAsyncContext>> *s3Tasks,
    std::vector<std::shared_ptr<SetKVCacheTask>> *kvCacheTasks,
    uint64_t *chunkId, uint64_t *writeOffset) {
    // allocate chunkid
    uint32_t fsId = s3ClientAdaptor_->GetFsId();
    FSStatusCode ret = s3ClientAdaptor_->AllocS3ChunkId(fsId, 1, chunkId);
    if (ret != FSStatusCode::OK) {
        LOG(ERROR) << "alloc s3 chunkid fail. ret:" << ret;
        return CURVEFS_ERROR::INTERNAL;
    }

    // generate flush task
    uint64_t blockSize = s3ClientAdaptor_->GetBlockSize();
    uint32_t objectPrefix = s3ClientAdaptor_->GetObjectPrefix();
    uint64_t blockPos = chunkPos_ % blockSize;
    uint64_t blockIndex = chunkPos_ / blockSize;
    uint64_t remainLen = len_;
    while (remainLen > 0) {
        uint64_t curentLen =
            blockPos + remainLen > blockSize ? blockSize - blockPos : remainLen;

        // generate flush to disk or s3 task
        std::string objectName = curvefs::common::s3util::GenObjName(
            *chunkId, blockIndex, 0, fsId, inodeId, objectPrefix);
        auto context = std::make_shared<PutObjectAsyncContext>(
            objectName, data + (*writeOffset), curentLen);
        // context->type and context->cb will set in FlushTaskExecute
        s3Tasks->emplace_back(context);

        // generate flush to kvcache task
        if (kvClientManager_) {
            SetKVCacheDone cb =
                [this](const std::shared_ptr<SetKVCacheTask>& setTask) {
                    if (setTask->res &&
                        s3ClientAdaptor_->s3Metric_ != nullptr) {
                        curve::client::CollectMetrics(
                            &s3ClientAdaptor_->s3Metric_->writeToKVCache,
                            setTask->length, setTask->timer.u_elapsed());
                    }
                };
            auto task = std::make_shared<SetKVCacheTask>(
                objectName, data + (*writeOffset), curentLen, cb);
            kvCacheTasks->emplace_back(task);
        }

        remainLen -= curentLen;
        blockIndex++;
        (*writeOffset) += curentLen;
        blockPos = (blockPos + curentLen) % blockSize;
    }

    return CURVEFS_ERROR::OK;
}

CachePolicy DataCache::GetCachePolicy(bool toS3) {
    const bool mayCache =
        s3ClientAdaptor_->HasDiskCache() &&
        !s3ClientAdaptor_->GetDiskCacheManager()->IsDiskCacheFull() && !toS3;

    if (s3ClientAdaptor_->IsReadCache() && mayCache) {
        return CachePolicy::RCache;
    } else if (s3ClientAdaptor_->IsReadWriteCache() && mayCache) {
        return CachePolicy::WRCache;
    } else {
        return CachePolicy::NCache;
    }
}

/**
 * @brief Decide whether to use diskcache or s3 based on the cache strategy
(note that kvcache is determined based on kvClientManager_). If the upload
fails, no matter the original type is diskcache or s3, use s3 to retry.
 *
 */
void DataCache::FlushTaskExecute(
    CachePolicy cachePolicy,
    const std::vector<std::shared_ptr<PutObjectAsyncContext>> &s3Tasks,
    const std::vector<std::shared_ptr<SetKVCacheTask>> &kvCacheTasks) {
    // callback
    std::atomic<uint64_t> s3PendingTaskCal(s3Tasks.size());
    std::atomic<uint64_t> kvPendingTaskCal(kvCacheTasks.size());
    CountDownEvent s3TaskEvent(s3PendingTaskCal);
    CountDownEvent kvTaskEvent(kvPendingTaskCal);

    PutObjectAsyncCallBack s3cb =
        [&](const std::shared_ptr<PutObjectAsyncContext>& context) {
            if (context->retCode >= 0) {
                if (s3ClientAdaptor_->s3Metric_ != nullptr) {
                    metric::AsyncContextCollectMetrics(
                        s3ClientAdaptor_->s3Metric_, context);
                }

                if (CachePolicy::RCache == cachePolicy) {
                    VLOG(9) << "write to read cache, name = " << context->key;
                    s3ClientAdaptor_->GetDiskCacheManager()->Enqueue(context,
                                                                     true);
                }

                // Don't move the if sentence to the front
                // it will cause core dumped because s3Metric_
                // will be destructed before being accessed
                s3TaskEvent.Signal();
                return;
            }

            LOG(WARNING) << "Put object failed, key: " << context->key;
            // Retry using s3 no matter what the original was
            context->type = curve::common::ContextType::S3;
            s3ClientAdaptor_->GetS3Client()->UploadAsync(context);
        };

    SetKVCacheDone kvdone = [&](const std::shared_ptr<SetKVCacheTask> &task) {
        kvTaskEvent.Signal();
        if (task->res && s3ClientAdaptor_->s3Metric_ != nullptr) {
            curve::client::CollectMetrics(
                &s3ClientAdaptor_->s3Metric_->writeToKVCache, task->length,
                task->timer.u_elapsed());
        }
        return;
    };

    // s3task execute
    if (s3PendingTaskCal.load()) {
        std::for_each(
            s3Tasks.begin(), s3Tasks.end(),
            [&](const std::shared_ptr<PutObjectAsyncContext> &context) {
                context->cb = s3cb;
                if (CachePolicy::WRCache == cachePolicy) {
                    context->type = curve::common::ContextType::Disk;
                    s3ClientAdaptor_->GetDiskCacheManager()->Enqueue(context);
                } else {
                    context->type = curve::common::ContextType::S3;
                    s3ClientAdaptor_->GetS3Client()->UploadAsync(context);
                }
            });
    }
    // kvtask execute
    if (kvClientManager_ && kvPendingTaskCal.load()) {
        std::for_each(kvCacheTasks.begin(), kvCacheTasks.end(),
                      [&](const std::shared_ptr<SetKVCacheTask> &task) {
                          task->done = kvdone;
                          kvClientManager_->Set(task);
                      });
        kvTaskEvent.Wait();
    }

    s3TaskEvent.Wait();
}

void DataCache::PrepareS3ChunkInfo(uint64_t chunkId, uint64_t offset,
                                   uint64_t len, S3ChunkInfo *info) {
    info->set_chunkid(chunkId);
    info->set_compaction(0);
    info->set_offset(offset);
    info->set_len(len);
    info->set_size(len);
    info->set_zero(false);
    VLOG(6) << "UpdateInodeChunkInfo chunkId:" << chunkId
            << ",offset:" << offset << ", len:" << len;
    return;
}

bool DataCache::CanFlush(bool force) {
    if (force) {
        return true;
    }

    uint64_t chunkSize = s3ClientAdaptor_->GetChunkSize();
    uint64_t now = ::curve::common::TimeUtility::GetTimeofDaySec();
    uint32_t flushIntervalSec = s3ClientAdaptor_->GetFlushInterval();

    if (len_ == chunkSize) {
        return true;
    } else if (now < (createTime_ + flushIntervalSec)) {
        return false;
    }

    return true;
}

FsCacheManager::ReadCacheReleaseExecutor::ReadCacheReleaseExecutor()
    : running_(true) {
    t_ = std::thread{&FsCacheManager::ReadCacheReleaseExecutor::ReleaseCache,
                     this};
}

void FsCacheManager::ReadCacheReleaseExecutor::ReleaseCache() {
    while (running_) {
        std::list<DataCachePtr> tmp;

        {
            std::unique_lock<std::mutex> lk(mtx_);
            cond_.wait(lk, [&]() { return !retired_.empty() || !running_; });

            if (!running_) {
                return;
            }

            tmp.swap(retired_);
        }

        for (auto &c : tmp) {
            c->Release();
            c.reset();
        }

        VLOG(9) << "released " << tmp.size() << " data caches";
    }
}

void FsCacheManager::ReadCacheReleaseExecutor::Stop() {
    std::lock_guard<std::mutex> lk(mtx_);
    running_ = false;
    cond_.notify_one();
}

FsCacheManager::ReadCacheReleaseExecutor::~ReadCacheReleaseExecutor() {
    Stop();
    t_.join();
    LOG(INFO) << "ReadCacheReleaseExecutor stopped";
}

void FsCacheManager::ReadCacheReleaseExecutor::Release(
    std::list<DataCachePtr> *caches) {
    std::lock_guard<std::mutex> lk(mtx_);
    retired_.splice(retired_.end(), *caches);
    cond_.notify_one();
}

}  // namespace client
}  // namespace curvefs
