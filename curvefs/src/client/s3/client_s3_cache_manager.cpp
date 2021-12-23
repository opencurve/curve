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

#include <utility>

#include "curvefs/src/client/s3/client_s3_adaptor.h"
#include "curvefs/src/common/s3util.h"

namespace curvefs {
namespace client {

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

    FileCacheManagerPtr fileCacheManager =
        std::make_shared<FileCacheManager>(fsId, inodeId, s3ClientAdaptor_);
    auto ret = fileCacheManagerMap_.emplace(inodeId, fileCacheManager);
    assert(ret.second);
    (void)ret;
    return fileCacheManager;
}

void FsCacheManager::ReleaseFileCacheManager(uint64_t inodeId) {
    WriteLockGuard writeLockGuard(rwLock_);

    auto iter = fileCacheManagerMap_.find(inodeId);
    assert(iter != fileCacheManagerMap_.end());

    fileCacheManagerMap_.erase(iter);
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
            auto& trim = *iter;
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

void FsCacheManager::Delete(std::list<DataCachePtr>::iterator iter) {
    std::lock_guard<std::mutex> lk(lruMtx_);

    if (!(*iter)->InReadCache()) {
        return;
    }

    (*iter)->SetReadCacheState(false);
    lruByte_ -= (*iter)->GetActualLen();
    lruReadDataCacheList_.erase(iter);
}

CURVEFS_ERROR FsCacheManager::FsSync(bool force) {
    CURVEFS_ERROR ret;
    std::unordered_map<uint64_t, FileCacheManagerPtr> tmp;
    {
        WriteLockGuard writeLockGuard(rwLock_);
        tmp = fileCacheManagerMap_;
    }
    auto iter = tmp.begin();
    VLOG(3) << "FsSync force:" << force;
    for (; iter != tmp.end(); iter++) {
        ret = iter->second->Flush(force);
        if (ret != CURVEFS_ERROR::OK) {
            LOG(ERROR) << "fs fssync error, ret:" << ret;
            return ret;
        }

        {
            WriteLockGuard writeLockGuard(rwLock_);
            auto iter1 = fileCacheManagerMap_.find(iter->first);
            VLOG(9) << "FileCacheManagerPtr count:"
                    << iter1->second.use_count();
            // tmp and fileCacheManagerMap_ has this FileCacheManagerPtr, so
            // count is 2 if count more than 2, this mean someone thread has
            // this FileCacheManagerPtr
            if ((iter1->second->IsEmpty()) &&
                (2 == iter1->second.use_count())) {
                VLOG(9) << "Release FileCacheManager, inode id: "
                        << iter1->second->GetInodeId();
                fileCacheManagerMap_.erase(iter1);
            }
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
    VLOG(9) << "WriteChunk end, index: " << index
            << ", chunkPos: " << chunkPos;
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
        std::make_shared<ChunkCacheManager>(index, s3ClientAdaptor_);
    auto ret = chunkCacheMap_.emplace(index, chunkCacheManager);
    assert(ret.second);
    (void)ret;
    return chunkCacheManager;
}

int FileCacheManager::Read(uint64_t inodeId, uint64_t offset, uint64_t length,
                           char *dataBuf) {
    uint64_t chunkSize = s3ClientAdaptor_->GetChunkSize();
    uint64_t index = offset / chunkSize;
    uint64_t chunkPos = offset % chunkSize;
    uint64_t readLen = 0;
    int ret = 0;
    uint64_t readOffset = 0;
    std::vector<ReadRequest> totalRequests;

    //  Find offset~len in the write and read cache,
    //  and The parts that are not in the cache are placed in the totalRequests
    while (length > 0) {
        std::vector<ReadRequest> requests;
        if (chunkPos + length > chunkSize) {
            readLen = chunkSize - chunkPos;
        } else {
            readLen = length;
        }
        ReadChunk(index, chunkPos, readLen, dataBuf, readOffset, &requests);
        totalRequests.insert(totalRequests.end(), requests.begin(),
                             requests.end());
        length -= readLen;
        index++;
        readOffset += readLen;
        chunkPos = (chunkPos + readLen) % chunkSize;
    }

    if (totalRequests.empty()) {
        VLOG(3) << "read cache is all the hits.";
        return readOffset;
    }

    std::vector<S3ReadRequest> totalS3Requests;
    auto iter = totalRequests.begin();
    uint64_t fileLen;
    {
        std::shared_ptr<InodeWrapper> inodeWrapper;
        CURVEFS_ERROR ret = s3ClientAdaptor_->GetInodeCacheManager()->GetInode(
            inodeId, inodeWrapper);
        if (ret != CURVEFS_ERROR::OK) {
            LOG(WARNING) << "get inode fail, ret:" << ret;
            return -1;
        }
        ::curve::common::UniqueLock lgGuard = inodeWrapper->GetUniqueLock();
        Inode *inode = inodeWrapper->GetMutableInodeUnlocked();
        fileLen = inode->length();
        for (; iter != totalRequests.end(); iter++) {
            VLOG(6) << "ReadRequest index:" << iter->index
                    << ",chunkPos:" << iter->chunkPos << ",len:" << iter->len
                    << ",bufOffset:" << iter->bufOffset;
            auto s3InfoListIter = inode->s3chunkinfomap().find(iter->index);
            if (s3InfoListIter == inode->s3chunkinfomap().end()) {
                VLOG(6) << "s3infolist is not found.index:" << iter->index;
                memset(dataBuf + iter->bufOffset, 0, iter->len);
                continue;
            }
            std::vector<S3ReadRequest> s3Requests;
            GenerateS3Request(*iter, s3InfoListIter->second, dataBuf,
                              &s3Requests, inode->fsid(), inode->inodeid());
            totalS3Requests.insert(totalS3Requests.end(), s3Requests.begin(),
                                   s3Requests.end());
        }
    }
    if (totalS3Requests.empty()) {
        VLOG(6) << "s3 has not data to read.";
        return readOffset;
    }

    uint32_t i;
    for (i = 0; i < totalS3Requests.size(); i++) {
        S3ReadRequest &tmp_req = totalS3Requests[i];
        VLOG(9) << "S3ReadRequest chunkid:" << tmp_req.chunkId
                << ",offset:" << tmp_req.offset << ",len:" << tmp_req.len
                << ",objectOffset:" << tmp_req.objectOffset
                << ",readOffset:" << tmp_req.readOffset
                << ",compaction:" << tmp_req.compaction
                << ",fsid:" << tmp_req.fsId << ",inodeId:" << tmp_req.inodeId;
    }

    std::vector<S3ReadResponse> responses;

    ret = ReadFromS3(totalS3Requests, &responses, fileLen);
    if (ret < 0) {
        LOG(ERROR) << "read from s3 failed. ret:" << ret;
        return ret;
    }

    auto repIter = responses.begin();
    for (; repIter != responses.end(); repIter++) {
        VLOG(6) << "readOffset:" << repIter->GetReadOffset()
                << ",bufLen:" << repIter->GetBufLen();
        memcpy(dataBuf + repIter->GetReadOffset(), repIter->GetDataBuf(),
               repIter->GetBufLen());
    }

    return readOffset;
}

int FileCacheManager::ReadFromS3(const std::vector<S3ReadRequest> &requests,
                                 std::vector<S3ReadResponse> *responses,
                                 uint64_t fileLen) {
    uint64_t chunkSize = s3ClientAdaptor_->GetChunkSize();
    uint64_t blockSize = s3ClientAdaptor_->GetBlockSize();
    std::vector<S3ReadRequest>::const_iterator iter = requests.begin();
    std::atomic<uint64_t> pendingReq(0);
    curve::common::CountDownEvent cond(1);
    bool async = false;
    // first is chunkIndex, second is vector chunkPos
    std::map<uint64_t, std::vector<uint64_t>> dataCacheMap;
    GetObjectAsyncCallBack cb =
        [&](const S3Adapter *adapter,
            const std::shared_ptr<GetObjectAsyncContext> &context) {
            if (context->retCode == 0) {
                pendingReq.fetch_sub(1, std::memory_order_relaxed);
                cond.Signal();
                return;
            }

            LOG(WARNING) << "Get Object failed, key: " << context->key
                         << ", offset: " << context->offset;
            s3ClientAdaptor_->GetS3Client()->DownloadAsync(context);
        };

    for (; iter != requests.end(); iter++) {
        uint64_t blockIndex = iter->offset % chunkSize / blockSize;
        uint64_t blockPos = iter->offset % chunkSize % blockSize;
        uint64_t chunkIndex = iter->offset / chunkSize;
        uint64_t chunkPos = iter->offset % chunkSize;
        uint64_t len = iter->len;
        uint64_t n = 0;
        uint64_t readOffset = 0;
        uint64_t objectOffset = iter->objectOffset;

        std::vector<uint64_t> &dataCacheVec = dataCacheMap[chunkIndex];
        dataCacheVec.push_back(chunkPos);
        S3ReadResponse response(len);
        if (!response.GetDataBuf()) {
            return -1;
        }
        VLOG(6) << "HandleReadRequest blockPos:" << blockPos << ",len:" << len
                << ",blockIndex:" << blockIndex
                << ",objectOffset:" << objectOffset << ",chunkid"
                << iter->chunkId << ",fsid" << iter->fsId
                << ",inodeId:" << iter->inodeId;
        // prefetch read
        if (s3ClientAdaptor_->HasDiskCache()) {
            uint64_t blockIndexTmp = blockIndex;
            // the counts of blocks that need prefetch
            uint32_t prefetchBlocks = s3ClientAdaptor_->GetPrefetchBlocks();
            std::vector<std::string> prefetchObjs;
            for (int count = 0; count < prefetchBlocks; count++) {
                std::string name = curvefs::common::s3util::GenObjName(
                    iter->chunkId, blockIndexTmp, iter->compaction, iter->fsId,
                    iter->inodeId);
                prefetchObjs.push_back(name);
                blockIndexTmp++;
                uint64_t readLen = blockIndexTmp * blockSize;
                if ((readLen > fileLen) ||
                    (blockIndexTmp >= chunkSize / blockSize)) {
                    VLOG(6) << "end, redLen :" << readLen
                            << ", fileLen: " << fileLen << ", blockIndexTmp "
                            << blockIndexTmp;
                    break;
                }
            }
            // prefetch object from s3
            PrefetchS3Objs(prefetchObjs);
        }
        while (len > 0) {
            if (blockPos + len > blockSize) {
                n = blockSize - blockPos;
            } else {
                n = len;
            }
            assert(blockPos >= objectOffset);
            std::string name = curvefs::common::s3util::GenObjName(
                iter->chunkId, blockIndex, iter->compaction, iter->fsId,
                iter->inodeId);
            if (async) {
                VLOG(9) << "async read s3";
                auto context = std::make_shared<GetObjectAsyncContext>();
                context->key = name;
                context->buf = response.GetDataBuf() + readOffset;
                context->offset = blockPos - objectOffset;
                context->len = n;
                context->cb = cb;
                pendingReq.fetch_add(1, std::memory_order_relaxed);
                s3ClientAdaptor_->GetS3Client()->DownloadAsync(context);
            } else {
                VLOG(9) << "sync read s3";
                int ret = 0;
                if (s3ClientAdaptor_->HasDiskCache() &&
                    s3ClientAdaptor_->GetDiskCacheManager()->IsCached(name)) {
                    VLOG(9) << "cached in disk: " << name;
                    ret = s3ClientAdaptor_->GetDiskCacheManager()->Read(
                        name, response.GetDataBuf() + readOffset,
                        blockPos - objectOffset, n);
                } else {
                    VLOG(9) << "not cached in disk: " << name;
                    ret = s3ClientAdaptor_->GetS3Client()->Download(
                        name, response.GetDataBuf() + readOffset,
                        blockPos - objectOffset, n);
                }
                if (ret < 0) {
                    LOG(ERROR) << "get obj failed, name is: " << name
                               << ", offset is: " << blockPos
                               << ", objoffset is: " << objectOffset
                               << ", len: " << n << ", ret is: " << ret;
                    return ret;
                }
            }
            len -= n;
            readOffset += n;
            blockIndex++;
            blockPos = (blockPos + n) % blockSize;
            objectOffset = 0;
        }
        response.SetReadOffset(iter->readOffset);
        VLOG(6) << "response readOffset:" << response.GetReadOffset()
                << ",response len:" << response.GetBufLen()
                << ",bufLen:" << readOffset;
        responses->emplace_back(std::move(response));
    }

    while (pendingReq.load(std::memory_order_acquire)) {
        cond.Wait();
    }
    uint32_t i = 0;
    for (auto &dataCacheMapIter : dataCacheMap) {
        ChunkCacheManagerPtr chunkCacheManager =
            FindOrCreateChunkCacheManager(dataCacheMapIter.first);
        std::vector<uint64_t> &DataCacheVec = dataCacheMapIter.second;
        WriteLockGuard writeLockGuard(chunkCacheManager->rwLockChunk_);
        for (auto &chunkPos : DataCacheVec) {
            DataCachePtr dataCache = std::make_shared<DataCache>(
            s3ClientAdaptor_, chunkCacheManager.get(), chunkPos,
            (*responses)[i].GetBufLen(), (*responses)[i].GetDataBuf());
            chunkCacheManager->AddReadDataCache(dataCache);
            i++;
        }
    }

    return 0;
}

class AsyncPrefetchCallback {
 public:
    AsyncPrefetchCallback(uint64_t inode, S3ClientAdaptorImpl *s3Client)
        : inode_(inode), s3Client_(s3Client) {}

    void operator()(const S3Adapter *,
                    const std::shared_ptr<GetObjectAsyncContext> &context) {
        VLOG(9) << "prefetch end: " << context->key << ", len " << context->len;

        std::unique_ptr<char[]> guard(context->buf);
        auto fileCache =
            s3Client_->GetFsCacheManager()->FindFileCacheManager(inode_);

        if (!fileCache) {
            VLOG(3) << "prefetch inode: " << inode_
                    << " end, but file cache is released, key: "
                    << context->key;
            return;
        }

        if (context->retCode != 0) {
            LOG(WARNING) << "prefetch failed, key: " << context->key;
            return;
        }

        int ret = s3Client_->GetDiskCacheManager()->WriteReadDirect(
            context->key, context->buf, context->len);
        LOG_IF(ERROR, ret < 0)
            << "write read directly failed, key: " << context->key;

        {
            curve::common::LockGuard lg(fileCache->downloadMtx_);
            fileCache->downloadingObj_.erase(context->key);
        }
    }

 private:
    const uint64_t inode_;
    S3ClientAdaptorImpl *s3Client_;
};

void FileCacheManager::PrefetchS3Objs(std::vector<std::string> prefetchObjs) {
    uint64_t blockSize = s3ClientAdaptor_->GetBlockSize();
    for (auto &obj : prefetchObjs) {
        std::string name = obj;
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
        auto task = [name, inode, s3ClientAdaptor, blockSize]() {
            char *dataCacheS3 = new char[blockSize];
            auto context = std::make_shared<GetObjectAsyncContext>();
            context->key = name;
            context->buf = dataCacheS3;
            context->offset = 0;
            context->len = blockSize;
            context->cb = AsyncPrefetchCallback{inode, s3ClientAdaptor};
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

void FileCacheManager::ReadChunk(uint64_t index, uint64_t chunkPos,
                                 uint64_t readLen, char *dataBuf,
                                 uint64_t dataBufOffset,
                                 std::vector<ReadRequest> *requests) {
    ChunkCacheManagerPtr chunkCacheManager =
        FindOrCreateChunkCacheManager(index);
    ReadLockGuard readLockGuard(chunkCacheManager->rwLockChunk_);
    std::vector<ReadRequest> cacheMissRequests;
    chunkCacheManager->ReadByWriteCache(chunkPos, readLen, dataBuf,
                                        dataBufOffset, &cacheMissRequests);
    for (auto request : cacheMissRequests) {
        std::vector<ReadRequest> tmpRequests;
        chunkCacheManager->ReadByReadCache(request.chunkPos, request.len,
                                           dataBuf, request.bufOffset,
                                           &tmpRequests);
        requests->insert(requests->end(), tmpRequests.begin(),
                         tmpRequests.end());
    }
    return;
}

void FileCacheManager::ReleaseCache() {
    WriteLockGuard writeLockGuard(rwLock_);
    auto iter = chunkCacheMap_.begin();

    for (; iter != chunkCacheMap_.end(); iter++) {
        iter->second->ReleaseCache(s3ClientAdaptor_);
    }

    chunkCacheMap_.clear();
    return;
}

CURVEFS_ERROR FileCacheManager::Flush(bool force) {
    CURVEFS_ERROR ret;
    std::map<uint64_t, ChunkCacheManagerPtr> tmp;
    {
        WriteLockGuard writeLockGuard(rwLock_);
        tmp = chunkCacheMap_;
    }
    auto iter = tmp.begin();

    for (; iter != tmp.end(); iter++) {
        ret = iter->second->Flush(inode_, force);
        if (ret != CURVEFS_ERROR::OK) {
            LOG(ERROR) << "fileCacheManager Flush error, ret:" << ret
                       << ",chunkIndex:" << iter->second->GetIndex();
            return ret;
        }

        {
            WriteLockGuard writeLockGuard(rwLock_);
            auto iter1 = chunkCacheMap_.find(iter->first);
            VLOG(9) << "ChunkCacheManagerPtr count:"
                    << iter1->second.use_count();
            // tmp and chunkCacheMap_ has this ChunkCacheManagerPtr, so count is
            // 2 if count more than 2, this mean someone thread has this
            // ChunkCacheManagerPtr
            if (iter1->second->IsEmpty() && (2 == iter1->second.use_count())) {
                VLOG(9) << "chunkCacheMap_ erase.";
                chunkCacheMap_.erase(iter1);
            }
        }
    }
    return CURVEFS_ERROR::OK;
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
                iter->second->CopyDataCacheToBuf(
                    chunkPos - dcChunkPos, readLen, dataBuf + dataBufOffset);
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
                 << ",len:" << iter->second->GetLen() << ",inodeId:" << inodeId
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
                iter->second->SetDelete();
                waitDelVec.push_back(iter->first);
            }

            std::vector<uint64_t>::iterator iterDel = waitDelVec.begin();
            for (; iterDel != waitDelVec.end(); iterDel++) {
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
        std::make_shared<DataCache>(s3ClientAdaptor, this, chunkPos, len, data);
    VLOG(9) << "WriteNewDataCache chunkPos:" << chunkPos << ", len:" << len
            << ", new len:" << dataCache->GetLen()
            << ",chunkIndex:" << index_;
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
        s3ClientAdaptor_->GetFsCacheManager()->Delete(dcpIter);
        dataRCacheMap_.erase(iter);
    }
    std::list<DataCachePtr>::iterator outIter;
    bool ret =
        s3ClientAdaptor_->GetFsCacheManager()->Set(dataCache, &outIter);
    if (ret) {
        dataRCacheMap_.emplace(chunkPos, outIter);
    }
}

void ChunkCacheManager::ReleaseReadDataCache(uint64_t key) {
    WriteLockGuard writeLockGuard(rwLockRead_);

    auto iter = dataRCacheMap_.find(key);
    if (iter == dataRCacheMap_.end()) {
        return;
    }

    dataRCacheMap_.erase(iter);
    return;
}

void ChunkCacheManager::ReleaseCache(S3ClientAdaptorImpl *s3ClientAdaptor) {
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
    auto iter = dataRCacheMap_.begin();
    for (; iter != dataRCacheMap_.end(); iter++) {
        s3ClientAdaptor->GetFsCacheManager()->Delete(iter->second);
    }
    dataRCacheMap_.clear();
}

void ChunkCacheManager::ReleaseWriteDataCache(const DataCachePtr &dataCache) {
    rwLockWrite_.WRLock();
    uint64_t key = dataCache->GetChunkPos();
    if (dataWCacheMap_.count(key)) {
        dataWCacheMap_.erase(key);
        rwLockWrite_.Unlock();
        s3ClientAdaptor_->GetFsCacheManager()->DataCacheNumFetchSub(1);
        VLOG(9) << "chunk flush DataCacheByteDec len:"
                << dataCache->GetActualLen();
        s3ClientAdaptor_->GetFsCacheManager()->DataCacheByteDec(
            dataCache->GetActualLen());
        if (!s3ClientAdaptor_->GetFsCacheManager()->WriteCacheIsFull()) {
            VLOG(9) << "write cache is not full, signal wait.";
            s3ClientAdaptor_->GetFsCacheManager()->FlushSignal();
        }
    } else {
        VLOG(9) << "ReleaseWriteDataCache is deleted key:" << key;
        rwLockWrite_.Unlock();
    }
}

CURVEFS_ERROR ChunkCacheManager::Flush(uint64_t inodeId, bool force) {
    std::map<uint64_t, DataCachePtr> tmp;
    curve::common::LockGuard lg(flushMtx_);
    CURVEFS_ERROR ret;
    {
        WriteLockGuard writeLockGuard(rwLockWrite_);
        tmp = dataWCacheMap_;
    }
    auto iter = tmp.begin();
    for (; iter != tmp.end(); iter++) {
        VLOG(9) << "Flush datacache chunkPos:" << iter->second->GetChunkPos()
                << ",len:" << iter->second->GetLen() << ",inodeId:" << inodeId
                << ",chunkIndex:" << index_;
        assert(iter->second->IsDirty());
        ret = iter->second->Flush(inodeId, force);
        if ((ret != CURVEFS_ERROR::OK) && (ret != CURVEFS_ERROR::NOFLUSH) &&
            ret != CURVEFS_ERROR::NOTEXIST) {
            LOG(WARNING) << "dataCache flush failed. ret:" << ret
                         << ",index:" << index_
                         << ",data chunkpos:" << iter->second->GetChunkPos();
            return ret;
        }
        WriteLockGuard lockGuard(rwLockChunk_);
        if (ret == CURVEFS_ERROR::OK) {
            if (!iter->second->IsDirty()) {
                VLOG(9) << "ReleaseWriteDataCache chunkPos:"
                        << iter->second->GetChunkPos()
                        << ",len:" << iter->second->GetLen()
                        << ",inodeId:" << inodeId << ",chunkIndex:" << index_;
                AddReadDataCache(iter->second);
                ReleaseWriteDataCache(iter->second);
            } else {
                VLOG(6) << "data cache is dirty.";
            }
        } else if (ret == CURVEFS_ERROR::NOTEXIST) {
            VLOG(9) << "ReleaseWriteDataCache chunkPos:"
                    << iter->second->GetChunkPos()
                    << ",len:" << iter->second->GetLen()
                    << ",inodeId:" << inodeId << ",chunkIndex:" << index_;
            ReleaseWriteDataCache(iter->second);
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

DataCache::DataCache(S3ClientAdaptorImpl *s3ClientAdaptor,
                     ChunkCacheManager *chunkCacheManager, uint64_t chunkPos,
                     uint64_t len, const char *data)
    : s3ClientAdaptor_(s3ClientAdaptor), chunkCacheManager_(chunkCacheManager),
      dirty_(true), delete_(false), inReadCache_(false) {
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
    PageData *meragePage = nullptr;
    PageDataMap *pdMap = &dataMap_[blockIndex];
    int n = 0;

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
        meragePage = mergeDataCache->GetPageData(blockIndex, pageIndex);
        assert(meragePage);
        if (pdMap->count(pageIndex)) {
            data = (*pdMap)[pageIndex]->data;
            if (pagePos + len > pageSize) {
                n = pageSize - pagePos;
            } else {
                n = len;
            }
            VLOG(9) << "MergeDataCacheToDataCache n:" << n
                    << ", pagePos:" << pagePos;
            memcpy(data + pagePos, meragePage->data + pagePos, n);
            // mergeDataCache->ReleasePageData(blockIndex, pageIndex);
        } else {
            pdMap->emplace(pageIndex, meragePage);
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
    uint64_t totalSize = 0;
    uint64_t addByte = 0;
    uint64_t oldSize = 0;
    uint32_t pageSize = s3ClientAdaptor_->GetPageSize();
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
    dirty_.exchange(true, std::memory_order_acq_rel);
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
            for (; iter != mergeDataCacheVer.end(); iter++) {
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
            for (; iter != mergeDataCacheVer.end(); iter++) {
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

CURVEFS_ERROR DataCache::Flush(uint64_t inodeId, bool force) {
    uint64_t blockSize = s3ClientAdaptor_->GetBlockSize();
    uint64_t chunkSize = s3ClientAdaptor_->GetChunkSize();
    uint32_t flushIntervalSec = s3ClientAdaptor_->GetFlushInterval();
    uint32_t fsId = s3ClientAdaptor_->GetFsId();
    uint64_t blockPos;
    uint64_t blockIndex;
    uint64_t chunkIndex = chunkCacheManager_->GetIndex();
    uint64_t offset;
    uint64_t tmpLen;
    uint64_t n = 0;
    std::string objectName;
    uint32_t writeOffset = 0;
    bool isFlush = true;
    uint64_t chunkId;
    uint64_t now = ::curve::common::TimeUtility::GetTimeofDaySec();
    char *data = nullptr;
    curve::common::CountDownEvent cond(1);
    std::atomic<uint64_t> pendingReq(0);
    FSStatusCode ret;

    mtx_.lock();
    if (!force) {
        if (len_ == chunkSize) {
            isFlush = true;
        } else if (now < (createTime_ + flushIntervalSec)) {
            isFlush = false;
        }
    }
    VLOG(9) << "now:" << now << ",createTime:" << createTime_
            << ",flushIntervalSec:" << flushIntervalSec
            << ",isFlush:" << isFlush << ",chunkPos:" << chunkPos_
            << ", len:" << len_ << ", inodeId:" << inodeId
            << ",chunkIndex:" << chunkIndex;
    if (delete_.load(std::memory_order_acquire)) {
        VLOG(9) << "datacache is deleted chunkPos:" << chunkPos_
                << ", len:" << len_ << ", inodeId:" << inodeId
                << ",chunkIndex:" << chunkIndex;
        mtx_.unlock();
        return CURVEFS_ERROR::NOFLUSH;
    }

    if (isFlush) {
        tmpLen = len_;
        blockPos = chunkPos_ % blockSize;
        blockIndex = chunkPos_ / blockSize;
        offset = chunkIndex * chunkSize + chunkPos_;

        ret = s3ClientAdaptor_->AllocS3ChunkId(fsId, &chunkId);
        if (ret != FSStatusCode::OK) {
            LOG(ERROR) << "alloc s3 chunkid fail. ret:" << ret;
            mtx_.unlock();
            return CURVEFS_ERROR::INTERNAL;
        }
        data = new (std::nothrow) char[len_];
        if (!data) {
            LOG(ERROR) << "new data failed.";
            mtx_.unlock();
            return CURVEFS_ERROR::INTERNAL;
        }
        CopyDataCacheToBuf(0, len_, data);
        dirty_.store(false, std::memory_order_release);
        mtx_.unlock();

        VLOG(9) << "start datacache flush, chunkId:" << chunkId
                << ", inodeId:" << inodeId
                << ",Len:" << tmpLen << ",blockPos:" << blockPos
                << ",blockIndex:" << blockIndex;
        PutObjectAsyncCallBack cb =
            [&](const std::shared_ptr<PutObjectAsyncContext> &context) {
                if (context->retCode == 0) {
                    if (pendingReq.fetch_sub(1) == 1) {
                        VLOG(9) << "pendingReq is over";
                        cond.Signal();
                    }
                    VLOG(9) << "PutObjectAsyncCallBack: " << context->key
                            << " pendingReq is: " << pendingReq;
                    return;
                }

                LOG(WARNING) << "Put object failed, key: " << context->key;
                s3ClientAdaptor_->GetS3Client()->UploadAsync(context);
            };

        std::vector<std::shared_ptr<PutObjectAsyncContext>> uploadTasks;
        bool useDiskCache = s3ClientAdaptor_->IsReadWriteCache() &&
        !s3ClientAdaptor_->GetDiskCacheManager()->IsDiskCacheFull();
        while (tmpLen > 0) {
            if (blockPos + tmpLen > blockSize) {
                n = blockSize - blockPos;
            } else {
                n = tmpLen;
            }

            objectName = curvefs::common::s3util::GenObjName(
                chunkId, blockIndex, 0, fsId, inodeId);
            int ret = 0;
            if (useDiskCache) {
                ret = s3ClientAdaptor_->GetDiskCacheManager()->Write(
                    objectName, data + writeOffset, n);
            } else {
                auto context = std::make_shared<PutObjectAsyncContext>();
                context->key = objectName;
                context->buffer = data + writeOffset;
                context->bufferSize = n;
                context->cb = cb;
                uploadTasks.emplace_back(context);
            }
            if (ret < 0) {
                LOG(ERROR) << "write object fail. object: " << objectName;
                delete[] data;
                dirty_.store(true, std::memory_order_release);
                return CURVEFS_ERROR::INTERNAL;
            }
            tmpLen -= n;
            blockIndex++;
            writeOffset += n;
            blockPos = (blockPos + n) % blockSize;
        }
        if (!useDiskCache) {
            pendingReq.fetch_add(uploadTasks.size(), std::memory_order_seq_cst);
            VLOG(9) << "pendingReq init: " << pendingReq;
            for (auto iter = uploadTasks.begin(); iter != uploadTasks.end();
                 ++iter) {
                VLOG(9) << "upload start: " << (*iter)->key
                        << " len : " << (*iter)->bufferSize;
                s3ClientAdaptor_->GetS3Client()->UploadAsync(*iter);
            }
        }

        if (pendingReq.load(std::memory_order_seq_cst)) {
            VLOG(9) << "wait for pendingReq";
            cond.Wait();
        }

        delete[] data;
        VLOG(9) << "update inode start, chunkId:" << chunkId
                << ",offset:" << offset << ",len:" << writeOffset
                << ",inodeId:" << inodeId << ",chunkIndex:" << chunkIndex;
        {
            std::shared_ptr<InodeWrapper> inodeWrapper;
            CURVEFS_ERROR ret =
                s3ClientAdaptor_->GetInodeCacheManager()->GetInode(
                    inodeId, inodeWrapper);
            if (ret != CURVEFS_ERROR::OK) {
                LOG(WARNING) << "get inode fail, ret:" << ret;
                dirty_.store(true, std::memory_order_release);
                return ret;
            }
            ::curve::common::UniqueLock lgGuard = inodeWrapper->GetUniqueLock();
            Inode *inode = inodeWrapper->GetMutableInodeUnlocked();
            auto s3ChunkInfoMap = inode->mutable_s3chunkinfomap();
            auto s3chunkInfoListIter = s3ChunkInfoMap->find(chunkIndex);
            if (s3chunkInfoListIter == s3ChunkInfoMap->end()) {
                S3ChunkInfoList s3ChunkInfoList;
                UpdateInodeChunkInfo(&s3ChunkInfoList, chunkId, offset,
                                     writeOffset);
                s3ChunkInfoMap->insert({chunkIndex, s3ChunkInfoList});
            } else {
                S3ChunkInfoList &s3ChunkInfoList = s3chunkInfoListIter->second;
                UpdateInodeChunkInfo(&s3ChunkInfoList, chunkId, offset,
                                     writeOffset);
            }
            s3ClientAdaptor_->GetInodeCacheManager()->ShipToFlush(inodeWrapper);
        }

        VLOG(9) << "data flush end, inodeId: " << inodeId;
        return CURVEFS_ERROR::OK;
    }
    mtx_.unlock();
    return CURVEFS_ERROR::NOFLUSH;
}

void DataCache::UpdateInodeChunkInfo(S3ChunkInfoList *s3ChunkInfoList,
                                     uint64_t chunkId, uint64_t offset,
                                     uint64_t len) {
    S3ChunkInfo *tmp = s3ChunkInfoList->add_s3chunks();

    tmp->set_chunkid(chunkId);
    tmp->set_compaction(0);
    tmp->set_offset(offset);
    tmp->set_len(len);
    tmp->set_size(len);
    tmp->set_zero(false);
    VLOG(6) << "UpdateInodeChunkInfo chunkId:" << chunkId
            << ",offset:" << offset << ", len:" << len
            << ",s3chunks size:" << s3ChunkInfoList->s3chunks_size();
    return;
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

        for (auto& c : tmp) {
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
    std::list<DataCachePtr>* caches) {
    std::lock_guard<std::mutex> lk(mtx_);
    retired_.splice(retired_.end(), *caches);
    cond_.notify_one();
}

}  // namespace client
}  // namespace curvefs
