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
    fileCacheManagerMap_.emplace(inodeId, fileCacheManager);

    return fileCacheManager;
}

void FsCacheManager::ReleaseFileCacheManager(uint64_t inodeId) {
    WriteLockGuard writeLockGuard(rwLock_);

    auto iter = fileCacheManagerMap_.find(inodeId);
    assert(iter != fileCacheManagerMap_.end());

    fileCacheManagerMap_.erase(iter);
    return;
}

std::list<DataCachePtr>::iterator FsCacheManager::Set(DataCachePtr dataCache) {
    WriteLockGuard writeLockGuard(rwLockLru_);

    VLOG(3) << "lru current byte:" << lruByte_
            << ",lru max byte:" << readCacheMaxByte_;

    // trim cache without consider dataCache's size, because its size is
    // expected to be very smaller than `readCacheMaxByte_`
    while (lruByte_ >= readCacheMaxByte_) {
        auto &trimDataCache = lruReadDataCacheList_.back();
        VLOG(3) << "lru release data cache, size: " << trimDataCache->GetLen();
        lruByte_ -= trimDataCache->GetLen();
        trimDataCache->Release();
        lruReadDataCacheList_.pop_back();
    }
    lruByte_ += dataCache->GetLen();
    lruReadDataCacheList_.push_front(std::move(dataCache));
    return lruReadDataCacheList_.begin();
}

void FsCacheManager::Get(std::list<DataCachePtr>::iterator iter) {
    WriteLockGuard writeLockGuard(rwLockLru_);

    lruReadDataCacheList_.splice(lruReadDataCacheList_.begin(),
                                 lruReadDataCacheList_, iter);
    return;
}

void FsCacheManager::Delete(std::list<DataCachePtr>::iterator iter) {
    WriteLockGuard writeLockGuard(rwLockLru_);

    lruByte_ -= (*iter)->GetLen();
    lruReadDataCacheList_.erase(iter);
    return;
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
        if (iter->second->IsEmpty()) {
            WriteLockGuard writeLockGuard(rwLock_);
            auto iter1 = fileCacheManagerMap_.find(iter->first);
            if (iter1 != fileCacheManagerMap_.end()) {
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
    // todo curve::common::LockGuard lg(mtx_);
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
    ChunkCacheManagerPtr chunkCacheManager =
        FindOrCreateChunkCacheManager(index);
    curve::common::LockGuard lg(chunkCacheManager->mtx_);  // todo
    std::vector<DataCachePtr> mergeDataCacheVer;
    DataCachePtr dataCache = chunkCacheManager->FindWriteableDataCache(
        chunkPos, writeLen, &mergeDataCacheVer);
    if (dataCache) {
        dataCache->Write(chunkPos, writeLen, dataBuf, mergeDataCacheVer);
    } else {
        chunkCacheManager->CreateWriteDataCache(s3ClientAdaptor_, chunkPos,
                                                writeLen, dataBuf);
    }

    return;
}

ChunkCacheManagerPtr FileCacheManager::FindChunkCacheManager(uint64_t index) {
    ReadLockGuard readLockGuard(rwLock_);

    auto it = chunkCacheMap_.find(index);
    if (it != chunkCacheMap_.end()) {
        return it->second;
    }

    return nullptr;
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
    chunkCacheMap_.emplace(index, chunkCacheManager);

    return chunkCacheManager;
}

int FileCacheManager::Read(Inode *inode, uint64_t offset, uint64_t length,
                           char *dataBuf) {
    uint64_t chunkSize = s3ClientAdaptor_->GetChunkSize();
    uint64_t index = offset / chunkSize;
    uint64_t chunkPos = offset % chunkSize;
    uint64_t readLen = 0;
    int ret = 0;
    uint64_t readOffset = 0;

    std::vector<ReadRequest> totalRequests;
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
        GenerateS3Request(*iter, s3InfoListIter->second, dataBuf, &s3Requests,
                          inode->fsid(), inode->inodeid());
        totalS3Requests.insert(totalS3Requests.end(), s3Requests.begin(),
                               s3Requests.end());
    }

    if (totalS3Requests.empty()) {
        VLOG(6) << "s3 has not data to read.";
        return readOffset;
    }

    uint32_t i;
    for (i = 0; i < totalS3Requests.size(); i++) {
        S3ReadRequest &tmp_req = totalS3Requests[i];
        VLOG(6) << "S3ReadRequest chunkid:" << tmp_req.chunkId
                << ",offset:" << tmp_req.offset << ",len:" << tmp_req.len
                << ",objectOffset:" << tmp_req.objectOffset
                << ",readOffset:" << tmp_req.readOffset
                << ",compaction:" << tmp_req.compaction
                << ",fsid:" << tmp_req.fsId
                << ",inodeid:" << tmp_req.inodeId;
    }

    std::vector<S3ReadResponse> responses;

    ret = HandleReadRequest(totalS3Requests,
      &responses, inode->length());
    if (ret < 0) {
        LOG(ERROR) << "handle read request fail:" << ret;
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

int FileCacheManager::HandleReadRequest(
    const std::vector<S3ReadRequest> &requests,
    std::vector<S3ReadResponse> *responses, uint64_t fileLen) {
    uint64_t chunkSize = s3ClientAdaptor_->GetChunkSize();
    uint64_t blockSize = s3ClientAdaptor_->GetBlockSize();
    (*responses).reserve(requests.size());
    std::vector<S3ReadRequest>::const_iterator iter = requests.begin();
    std::atomic<uint64_t> pendingReq(0);
    curve::common::CountDownEvent cond(1);
    bool async = false;
    std::vector<std::pair<ChunkCacheManagerPtr, DataCachePtr>> DataCacheVec;

    GetObjectAsyncCallBack cb =
        [&](const S3Adapter *adapter,
            const std::shared_ptr<GetObjectAsyncContext> &context) {
            //  todo failed branch
            pendingReq.fetch_sub(1, std::memory_order_relaxed);
            cond.Signal();
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
        ChunkCacheManagerPtr chunkCacheManager =
            FindOrCreateChunkCacheManager(chunkIndex);

        DataCachePtr dataCache = std::make_shared<DataCache>(
            s3ClientAdaptor_, chunkCacheManager.get(), chunkPos, len);
        DataCacheVec.push_back(std::make_pair(chunkCacheManager, dataCache));
        S3ReadResponse response(dataCache);
        VLOG(6) << "HandleReadRequest blockPos:" << blockPos << ",len:" << len
                << ",blockIndex:" << blockIndex
                << ",objectOffset:" << objectOffset << ",chunkid"
                << iter->chunkId << ",fsid" << iter->fsId
                << ",inodeid:" << iter->inodeId;
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
                  (blockIndexTmp >= chunkSize/blockSize)) {
                    VLOG(6) << "end, redLen :" << readLen
                              << ", fileLen: " << fileLen
                              << ", blockIndexTmp " << blockIndexTmp;
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
                    if (ret < 0) {
                        LOG(ERROR) << "download name:" << name
                                   << " offset:" << blockPos << " len:" << n
                                   << "fail:" << ret;
                        return ret;
                    }
                }
            }
            len -= n;
            readOffset += n;
            blockIndex++;
            blockPos = (blockPos + n) % blockSize;
            objectOffset = 0;
        }
        response.SetReadOffset(iter->readOffset);
        responses->emplace_back(response);

        VLOG(6) << "response readOffset:" << response.GetReadOffset()
                << ",bufLen:" << readOffset;
    }

    while (pendingReq.load(std::memory_order_acquire)) {
        cond.Wait();
    }

    for (auto &dataCache : DataCacheVec) {
        VLOG(9) << "add read data cache start";
        dataCache.first->AddReadDataCache(dataCache.second);
        VLOG(9) << "add read data cache end";
    }

    return 0;
}

void FileCacheManager::PrefetchS3Objs(std::vector<std::string> prefetchObjs) {
    uint64_t blockSize = s3ClientAdaptor_->GetBlockSize();
     GetObjectAsyncCallBack cb =
        [&](const S3Adapter *adapter,
            const std::shared_ptr<GetObjectAsyncContext> &context) {
            VLOG(9) << "prefetch end: " << context->key
                       << ", len: " << context->len;
            if (s3ClientAdaptor_->GetDiskCacheManager()->WriteReadDirect(
                context->key, context->buf, context->len) < 0) {
                LOG(ERROR) << "write read directly failed";
            }
            delete context->buf;
            curve::common::LockGuard lg(downloadMtx_);
            downloadingObj_.erase(context->key);
    };
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
        auto task = [=]() {
            char* dataCacheS3 = new char[blockSize];
            if (dataCacheS3 == nullptr) {
                LOG(ERROR) << "malloc failed, size = " << blockSize;
                return;
            }
            memset(dataCacheS3, 0, blockSize);
            auto context = std::make_shared<GetObjectAsyncContext>();
            context->key = name;
            context->buf = dataCacheS3;
            context->offset = 0;
            context->len = blockSize;
            context->cb = cb;
            VLOG(9) << "prefetch start: " << context->key
                       << ", len: " << context->len;
            s3ClientAdaptor_->GetS3Client()->DownloadAsync(context);
        };
        s3ClientAdaptor_->PushAsyncTask(task);
    }
    return;
}

void FileCacheManager::GenerateS3Request(ReadRequest request,
                                         const S3ChunkInfoList& s3ChunkInfoList,
                                         char* dataBuf,
                                         std::vector<S3ReadRequest>* requests,
                                         uint64_t fsId,
                                         uint64_t inodeId) {
    uint64_t blockSize = s3ClientAdaptor_->GetBlockSize();
    std::vector<ObjectChunkInfo> chunks =
        GetReadChunks(s3ChunkInfoList, blockSize);
    std::vector<ObjectChunkInfo> sortChunks =
        SortByOffset(std::move(chunks));
    char* buf = dataBuf + request.bufOffset;
    uint64_t chunkSize = s3ClientAdaptor_->GetChunkSize();
    uint64_t offset = request.index * chunkSize + request.chunkPos;
    uint64_t length = request.len;
    uint32_t i = 0;
    ObjectChunkInfo tmp;
    uint64_t readOffset = 0;
    uint64_t chunkInfoOffset;
    uint64_t chunkInfoLen;
    VLOG(6) << "GenerateS3Request ReadRequest index:" << request.index
              << ",chunkPos:" << request.chunkPos << ",len:" << request.len
              << ",bufOffset:" << request.bufOffset
              << ",sortchunks size:" << sortChunks.size();
    while (length > 0) {
        S3ReadRequest s3Request;
        if (i == sortChunks.size()) {
            memset(static_cast<char *>(buf) + readOffset, 0, length);
            break;
        }
        tmp = sortChunks[i];
        chunkInfoOffset = tmp.s3ChunkInfo.offset();
        chunkInfoLen = tmp.s3ChunkInfo.len();
        VLOG(6) << "GenerateS3Request S3ChunkInfo chunkId:"
                  << tmp.s3ChunkInfo.chunkid()
                  << ",compaction:" << tmp.s3ChunkInfo.compaction()
                  << ",offset:" << chunkInfoOffset << ",len:" << chunkInfoLen
                  << ",zero:" << tmp.s3ChunkInfo.zero()
                  << ",objectOffset:" << tmp.objectOffset
                  << ",offset:" << offset
                  << ",len:" << length
                  << ",i:" << i;
        /*
        -----    read block
               ------  S3ChunkInfo
        */
        if (offset + length <= chunkInfoOffset) {
            memset(buf + readOffset, 0, length);
            break;
            /*
                   -----              ------------   read block           -
                      ------             -----       S3ChunkInfo
            */
        } else if ((chunkInfoOffset >= offset) &&
                   (chunkInfoOffset < offset + length)) {
            int n = chunkInfoOffset - offset;
            memset(static_cast<char*>(buf) + readOffset, 0, n);
            offset = chunkInfoOffset;
            readOffset += n;
            length -= n;

            if (offset + length <= chunkInfoOffset + chunkInfoLen) {
                if (tmp.s3ChunkInfo.zero()) {
                    memset(static_cast<char*>(buf) + readOffset, 0, length);
                } else {
                    s3Request.chunkId = tmp.s3ChunkInfo.chunkid();
                    s3Request.offset = offset;
                    s3Request.len = length;
                    s3Request.objectOffset = tmp.objectOffset;
                    s3Request.readOffset = request.bufOffset + readOffset;
                    s3Request.compaction = tmp.s3ChunkInfo.compaction();
                    s3Request.fsId = fsId;
                    s3Request.inodeId = inodeId;
                    requests->push_back(s3Request);
                }
                readOffset += length;
                length = 0;
            } else {
                if (tmp.s3ChunkInfo.zero()) {
                    memset(static_cast<char*>(buf) + readOffset,
                    0, chunkInfoLen);
                } else {
                    s3Request.chunkId = tmp.s3ChunkInfo.chunkid();
                    s3Request.offset = chunkInfoOffset;
                    s3Request.len = chunkInfoLen;
                    s3Request.objectOffset = tmp.objectOffset;
                    s3Request.readOffset = request.bufOffset + readOffset;
                    s3Request.compaction = tmp.s3ChunkInfo.compaction();
                    s3Request.fsId = fsId;
                    s3Request.inodeId = inodeId;
                    requests->push_back(s3Request);
                }
                readOffset += chunkInfoLen;
                length -= chunkInfoLen;
                offset += chunkInfoLen;
            }
            /*
                     ----                      ---------   read block
                   ----------                --------      S3ChunkInfo
            */
        } else if ((chunkInfoOffset < offset) &&
                   (chunkInfoOffset + chunkInfoLen > offset)) {
            if (offset + length <= chunkInfoOffset + chunkInfoLen) {
                if (tmp.s3ChunkInfo.zero()) {
                    memset(static_cast<char*>(buf) + readOffset, 0, length);
                } else {
                    s3Request.chunkId = tmp.s3ChunkInfo.chunkid();
                    s3Request.offset = offset;
                    s3Request.len = length;
                    if ((offset / blockSize) == (chunkInfoOffset / blockSize)) {
                        s3Request.objectOffset = tmp.objectOffset;
                    } else {
                        s3Request.objectOffset = 0;
                    }
                    s3Request.readOffset = request.bufOffset + readOffset;
                    s3Request.compaction = tmp.s3ChunkInfo.compaction();
                    s3Request.fsId = fsId;
                    s3Request.inodeId = inodeId;
                    requests->push_back(s3Request);
                }
                readOffset += length;
                length = 0;
            } else {
                if (tmp.s3ChunkInfo.zero()) {
                    memset(static_cast<char*>(buf) + readOffset, 0,
                           chunkInfoOffset + chunkInfoLen - offset);
                } else {
                    s3Request.chunkId = tmp.s3ChunkInfo.chunkid();
                    s3Request.offset = offset;
                    s3Request.len = chunkInfoOffset + chunkInfoLen - offset;
                    if ((offset / blockSize) == (chunkInfoOffset / blockSize)) {
                        s3Request.objectOffset = tmp.objectOffset;
                    } else {
                        s3Request.objectOffset = 0;
                    }
                    s3Request.readOffset = request.bufOffset + readOffset;
                    s3Request.compaction = tmp.s3ChunkInfo.compaction();
                    s3Request.fsId = fsId;
                    s3Request.inodeId = inodeId;
                    requests->push_back(s3Request);
                }
                length -= chunkInfoOffset + chunkInfoLen - offset;
                readOffset += chunkInfoOffset + chunkInfoLen - offset;
                offset = chunkInfoOffset + chunkInfoLen;
            }
            /*
                           -----  read block
                   ----           S3ChunkInfo
                   do nothing
            */
        } else {
            // NOLINT
        }
        i++;
    }

    return;
}

std::vector<ObjectChunkInfo> FileCacheManager::GetReadChunks(
    const S3ChunkInfoList& s3ChunkInfoList,
    uint64_t blockSize) {
    S3ChunkInfo tmp;
    ObjectChunkInfo chunkTmp;
    std::vector<ObjectChunkInfo> chunks;
    VLOG(9) << "chunk size:" << s3ChunkInfoList.s3chunks_size();
    for (int i = 0; i < s3ChunkInfoList.s3chunks_size(); i++) {
        tmp = s3ChunkInfoList.s3chunks(i);
        std::vector<S3ChunkInfo> addChunks;
        std::vector<int> waitingDel;
        for (uint32_t j = 0; j < chunks.size(); j++) {
            chunkTmp = chunks[j];
            // overlap, must cut old chunk
            if ((tmp.offset() < (chunkTmp.s3ChunkInfo.offset() +
                chunkTmp.s3ChunkInfo.len())) &&
                (chunkTmp.s3ChunkInfo.offset() < (tmp.offset() + tmp.len()))) {
                addChunks = CutOverLapChunks(tmp, chunkTmp.s3ChunkInfo);
                waitingDel.push_back(j);
            }
        }

        std::vector<int>::iterator iter = waitingDel.begin();
        for (; iter != waitingDel.end(); iter++) {
            chunks.erase(chunks.begin() + *iter);
        }
        std::vector<S3ChunkInfo>::iterator chunkIter = addChunks.begin();
        for (; chunkIter != addChunks.end(); chunkIter++) {
            ObjectChunkInfo addChunk;
            addChunk.s3ChunkInfo = *chunkIter;
            if (addChunk.s3ChunkInfo.offset() % blockSize ==
            chunkTmp.s3ChunkInfo.offset() % blockSize) {
                addChunk.objectOffset =
                    chunkTmp.s3ChunkInfo.offset() % blockSize;
            }  else {
                addChunk.objectOffset = 0;
            }
            chunks.push_back(addChunk);
        }
        chunkTmp.s3ChunkInfo = tmp;
        chunkTmp.objectOffset = tmp.offset() % blockSize;
        chunks.push_back(chunkTmp);
    }

    return chunks;
}

std::vector<ObjectChunkInfo> FileCacheManager::SortByOffset(
    std::vector<ObjectChunkInfo> chunks) {
    std::sort(chunks.begin(), chunks.end(),
    [](ObjectChunkInfo a, ObjectChunkInfo b) {
        return a.s3ChunkInfo.offset() < b.s3ChunkInfo.offset();
    });
    return chunks;
}

std::vector<S3ChunkInfo>
FileCacheManager::CutOverLapChunks(const S3ChunkInfo &newChunk,
                                   const S3ChunkInfo &old) {
    assert(newChunk.chunkid() >= old.chunkid());
    std::vector<S3ChunkInfo> result;
    S3ChunkInfo tmp;
    if (newChunk.offset() > old.offset() &&
        newChunk.offset() < old.offset() + old.len()) {
        /*
            -----     old
              ------  new
       */
        if (newChunk.offset() + newChunk.len() >= old.offset() + old.len()) {
            tmp.set_chunkid(old.chunkid());
            tmp.set_compaction(old.compaction());
            tmp.set_offset(old.offset());
            tmp.set_len(newChunk.offset() - old.offset());
            tmp.set_size(newChunk.offset() - old.offset());
            tmp.set_zero(old.zero());
            result.push_back(tmp);
            /*
                 ----------     old
                   ------       new
            */
        } else {
            tmp.set_chunkid(old.chunkid());
            tmp.set_compaction(old.compaction());
            tmp.set_offset(old.offset());
            tmp.set_len(newChunk.offset() - old.offset());
            tmp.set_size(newChunk.offset() - old.offset());
            tmp.set_zero(old.zero());
            result.push_back(tmp);
            tmp.set_chunkid(old.chunkid());
            tmp.set_compaction(old.compaction());
            tmp.set_offset(newChunk.offset() + newChunk.len());
            tmp.set_len(old.offset() + old.len() - newChunk.offset() -
                        newChunk.len());
            tmp.set_size(old.offset() + old.len() - newChunk.offset() -
                         newChunk.len());
            tmp.set_zero(old.zero());
            result.push_back(tmp);
        }
        /*
                      -----     old
                   ----------   new
        */
    } else if (newChunk.offset() <= old.offset() &&
               newChunk.offset() + newChunk.len() >= old.offset() + old.len()) {
        return result;
        /*
                      --------  old
                   -------      new
        */
    } else {
        tmp.set_chunkid(old.chunkid());
        tmp.set_compaction(old.compaction());
        tmp.set_offset(newChunk.offset() + newChunk.len());
        tmp.set_len(old.offset() + old.len() - newChunk.offset() -
                    newChunk.len());
        tmp.set_size(old.offset() + old.len() - newChunk.offset() -
                     newChunk.len());
        tmp.set_zero(old.zero());
        result.push_back(tmp);
    }

    return result;
}

void FileCacheManager::ReadChunk(uint64_t index, uint64_t chunkPos,
                                 uint64_t readLen, char *dataBuf,
                                 uint64_t dataBufOffset,
                                 std::vector<ReadRequest> *requests) {
    ChunkCacheManagerPtr chunkCacheManager =
        FindOrCreateChunkCacheManager(index);
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
    auto iter = chunkCacheMap_.begin();
    WriteLockGuard writeLockGuard(rwLock_);

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
            if (iter1->second->IsEmpty()) {
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
                << ",dcLen:" << dcLen;
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
            char *cacheData = iter->second->GetData();
            /*
                 -----               ReadData
                    ------           DataCache
            */
            if (chunkPos + readLen <= dcChunkPos + dcLen) {
                memcpy(dataBuf + request.len + dataBufOffset, cacheData,
                       chunkPos + readLen - dcChunkPos);
                readLen = 0;
                break;
                /*
                     -----------         ReadData
                        ------           DataCache
                */
            } else {
                memcpy(dataBuf + request.len + dataBufOffset, cacheData, dcLen);
                readLen = chunkPos + readLen - (dcChunkPos + dcLen);
                dataBufOffset = dcChunkPos + dcLen - chunkPos + dataBufOffset;
                chunkPos = dcChunkPos + dcLen;
            }
        } else if ((chunkPos >= dcChunkPos) &&
                   (chunkPos < dcChunkPos + dcLen)) {
            char *cacheData = iter->second->GetData();
            /*
                     ----              ReadData
                   ---------           DataCache
            */
            if (chunkPos + readLen <= dcChunkPos + dcLen) {
                memcpy(dataBuf + dataBufOffset,
                       cacheData + chunkPos - dcChunkPos, readLen);
                readLen = 0;
                break;
                /*
                         ----------              ReadData
                       ---------                DataCache
                */
            } else {
                memcpy(dataBuf + dataBufOffset,
                       cacheData + chunkPos - dcChunkPos,
                       dcChunkPos + dcLen - chunkPos);
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

    for (; iter != dataRCacheMap_.end(); iter++) {
        ReadRequest request;
        std::list<DataCachePtr>::iterator dcpIter = iter->second;
        uint64_t dcChunkPos = (*dcpIter)->GetChunkPos();
        uint64_t dcLen = (*dcpIter)->GetLen();
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
            char *cacheData = (*dcpIter)->GetData();
            /*
                 -----               ReadData
                    ------           DataCache
            */
            if (chunkPos + readLen <= dcChunkPos + dcLen) {
                memcpy(dataBuf + request.len + dataBufOffset, cacheData,
                       chunkPos + readLen - dcChunkPos);
                readLen = 0;
                break;
                /*
                     -----------         ReadData
                        ------           DataCache
                */
            } else {
                memcpy(dataBuf + request.len + dataBufOffset, cacheData, dcLen);
                readLen = chunkPos + readLen - (dcChunkPos + dcLen);
                dataBufOffset = dcChunkPos + dcLen - chunkPos + dataBufOffset;
                chunkPos = dcChunkPos + dcLen;
            }
        } else if ((chunkPos >= dcChunkPos) &&
                   (chunkPos < dcChunkPos + dcLen)) {
            char *cacheData = (*dcpIter)->GetData();
            s3ClientAdaptor_->GetFsCacheManager()->Get(iter->second);
            /*
                     ----              ReadData
                   ---------           DataCache
            */
            if (chunkPos + readLen <= dcChunkPos + dcLen) {
                memcpy(dataBuf + dataBufOffset,
                       cacheData + chunkPos - dcChunkPos, readLen);
                readLen = 0;
                break;
                /*
                         ----------              ReadData
                       ---------                DataCache
                */
            } else {
                memcpy(dataBuf + dataBufOffset,
                       cacheData + chunkPos - dcChunkPos,
                       dcChunkPos + dcLen - chunkPos);
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
        VLOG(9) << "request: index:" << index_ << ",chunkPos:" << chunkPos
                << ",len:" << request.len << ",bufOffset:" << dataBufOffset;
        requests->emplace_back(request);
    }
    return;
}

DataCachePtr ChunkCacheManager::FindWriteableDataCache(
    uint64_t chunkPos, uint64_t len,
    std::vector<DataCachePtr> *mergeDataCacheVer) {
    // todo performance optimization
    WriteLockGuard writeLockGuard(rwLockWrite_);

    auto iter = dataWCacheMap_.upper_bound(chunkPos);
    if (iter != dataWCacheMap_.begin()) {
        --iter;
    }
    for (; iter != dataWCacheMap_.end(); iter++) {
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
            for (; iterDel != waitDelVec.end(); iterDel++) {
                auto iter = dataWCacheMap_.find(*iterDel);

                s3ClientAdaptor_->GetFsCacheManager()->DataCacheNumFetchSub(1);
                VLOG(9) << "FindWriteableDataCache() DataCacheByteDec1 len:"
                        << iter->second->GetLen();
                s3ClientAdaptor_->GetFsCacheManager()->DataCacheByteDec(
                    iter->second->GetLen());
                dataWCacheMap_.erase(iter);
            }
            return dataCache;
        }
    }
    return nullptr;
}

DataCachePtr
ChunkCacheManager::CreateWriteDataCache(S3ClientAdaptorImpl *s3ClientAdaptor,
                                        uint32_t chunkPos, uint32_t len,
                                        const char *data) {
    DataCachePtr dataCache =
        std::make_shared<DataCache>(s3ClientAdaptor, this, chunkPos, len, data);
    VLOG(9) << "CreateWriteDataCache chunkPos:" << chunkPos << ", len:" << len;
    WriteLockGuard writeLockGuard(rwLockWrite_);

    dataWCacheMap_.emplace(chunkPos, dataCache);
    s3ClientAdaptor_->FsSyncSignalAndDataCacheInc();
    s3ClientAdaptor_->GetFsCacheManager()->DataCacheByteInc(len);
    return dataCache;
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
                    << ",len:" << dcLen;
            deleteKeyVec.emplace_back(dcChunkPos);
        }
    }

    for (auto key : deleteKeyVec) {
        auto iter = dataRCacheMap_.find(key);
        std::list<DataCachePtr>::iterator dcpIter = iter->second;
        s3ClientAdaptor_->GetFsCacheManager()->Delete(dcpIter);
        dataRCacheMap_.erase(iter);
    }

    std::list<DataCachePtr>::iterator newIter =
        s3ClientAdaptor_->GetFsCacheManager()->Set(dataCache);
    dataRCacheMap_.emplace(chunkPos, newIter);

    return;
}

void ChunkCacheManager::ReleaseReadDataCache(uint64_t key) {
    //  WriteLockGuard writeLockGuard(rwLockRead_);

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
                dataWCache.second->GetLen());
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

void ChunkCacheManager::ReleaseWriteDataCache(uint64_t key,
                                              uint64_t dataCacheLen) {
    rwLockWrite_.WRLock();
    if (dataWCacheMap_.count(key)) {
        dataWCacheMap_.erase(key);
        rwLockWrite_.Unlock();
        s3ClientAdaptor_->GetFsCacheManager()->DataCacheNumFetchSub(1);
        VLOG(9) << "chunk flush DataCacheByteDec1 len:" << dataCacheLen;
        s3ClientAdaptor_->GetFsCacheManager()->DataCacheByteDec(dataCacheLen);
        if (!s3ClientAdaptor_->GetFsCacheManager()->WriteCacheIsFull()) {
            VLOG(9) << "write cache is not full, signal wait.";
            s3ClientAdaptor_->GetFsCacheManager()->FlushSignal();
        }
    } else {
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
    auto size = tmp.size();
    auto iter = tmp.begin();
    for (; iter != tmp.end(); iter++) {
        VLOG(9) << "Flush datacache len:" << iter->second->GetLen();
        assert(iter->second->IsDirty());
        ret = iter->second->Flush(inodeId, force);
        if ((ret != CURVEFS_ERROR::OK) && (ret != CURVEFS_ERROR::NOFLUSH) &&
            ret != CURVEFS_ERROR::NOTEXIST) {
            LOG(WARNING) << "dataCache flush failed. ret:" << ret
                         << ",index:" << index_
                         << ",data chunkpos:" << iter->second->GetChunkPos();
            return ret;
        }
        if (ret == CURVEFS_ERROR::OK) {
            if (!iter->second->IsDirty()) {
                AddReadDataCache(iter->second);
                ReleaseWriteDataCache(iter->first, iter->second->GetLen());
            } else {
                VLOG(6) << "data cache is dirty.";
            }
        } else if (ret == CURVEFS_ERROR::NOTEXIST) {
            ReleaseWriteDataCache(iter->first, iter->second->GetLen());
        }
    }

    return CURVEFS_ERROR::OK;
}

void DataCache::Write(uint64_t chunkPos, uint64_t len, const char *data,
                      const std::vector<DataCachePtr> &mergeDataCacheVer) {
    uint64_t totalSize = 0;
    uint64_t addByte = 0;
    VLOG(9) << "DataCache Write() chunkPos:" << chunkPos << ",len:" << len
            << ",dataCache's chunkPos:" << chunkPos_
            << ",dataCache's len:" << len_;
    curve::common::LockGuard lg(mtx_);
    dirty_.exchange(true, std::memory_order_acq_rel);
    if (chunkPos <= chunkPos_) {
        /*
            ------       DataCache
         -------         WriteData
        */
        if (chunkPos + len <= chunkPos_ + len_) {
            totalSize = chunkPos_ + len_ - chunkPos;
            addByte = totalSize - len_;
            s3ClientAdaptor_->GetFsCacheManager()->DataCacheByteInc(addByte);
            char *newDatabuf = new char[totalSize];
            memcpy(newDatabuf, data, len);
            memcpy(newDatabuf + len, data_ + (chunkPos + len - chunkPos_),
                   totalSize - len);
            Swap(newDatabuf, totalSize);
            chunkPos_ = chunkPos;
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
                    totalSize =
                        (*iter)->GetChunkPos() + (*iter)->GetLen() - chunkPos;
                    addByte = totalSize - len_;
                    s3ClientAdaptor_->GetFsCacheManager()->DataCacheByteInc(
                        addByte);
                    char *newDatabuf = new char[totalSize];
                    memcpy(newDatabuf, data, len);
                    memcpy(newDatabuf + len,
                           (*iter)->GetData() +
                               (chunkPos + len - (*iter)->GetChunkPos()),
                           totalSize - len);
                    Swap(newDatabuf, totalSize);
                    chunkPos_ = chunkPos;
                    return;
                }
            }
            /*
                     ------    ------         DataCache
                  ---------------------       WriteData
            */
            totalSize = len;
            addByte = totalSize - len_;
            s3ClientAdaptor_->GetFsCacheManager()->DataCacheByteInc(addByte);
            char *newDatabuf = new char[totalSize];
            memcpy(newDatabuf, data, len);
            Swap(newDatabuf, totalSize);
            chunkPos_ = chunkPos;
            return;
        }
    } else {
        /*
            --------       DataCache
             -----         WriteData
        */
        if (chunkPos + len <= chunkPos_ + len_) {
            memcpy(data_ + chunkPos - chunkPos_, data, len);
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
                    totalSize =
                        (*iter)->GetChunkPos() + (*iter)->GetLen() - chunkPos_;
                    addByte = totalSize - len_;
                    s3ClientAdaptor_->GetFsCacheManager()->DataCacheByteInc(
                        addByte);
                    char *newDatabuf = new char[totalSize];
                    memcpy(newDatabuf, data_, chunkPos - chunkPos_);
                    memcpy(newDatabuf + chunkPos - chunkPos_, data, len);
                    memcpy(newDatabuf + chunkPos - chunkPos_ + len,
                           (*iter)->GetData() + chunkPos + len -
                               (*iter)->GetChunkPos(),
                           (*iter)->GetChunkPos() + (*iter)->GetLen() -
                               chunkPos - len);
                    Swap(newDatabuf, totalSize);
                    return;
                }
            }
            /*
                     ------         ------         DataCache
                        --------------------       WriteData
            */
            totalSize = chunkPos - chunkPos_ + len;
            addByte = totalSize - len_;
            s3ClientAdaptor_->GetFsCacheManager()->DataCacheByteInc(addByte);
            char *newDatabuf = new char[totalSize];
            memcpy(newDatabuf, data_, chunkPos - chunkPos_);
            memcpy(newDatabuf + chunkPos - chunkPos_, data, len);
            Swap(newDatabuf, totalSize);
            return;
        }
    }

    return;
}

void DataCache::Release() {
    chunkCacheManager_->ReleaseReadDataCache(chunkPos_);

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
    char *data;
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
            << ",isFlush:" << isFlush;

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
        data = new char[len_];
        memcpy(data, data_, len_);
        dirty_.store(false, std::memory_order_release);
        mtx_.unlock();

        VLOG(9) << "start datacache flush, chunkId:" << chunkId
                << ",Len:" << tmpLen << ",blockPos:" << blockPos
                << ",blockIndex:" << blockIndex;
        PutObjectAsyncCallBack cb =
                [&](const std::shared_ptr<PutObjectAsyncContext> &context) {
                if (pendingReq.fetch_sub(1) == 1) {
                    VLOG(9) << "pendingReq is over";
                    cond.Signal();
                }
                VLOG(9) << "PutObjectAsyncCallBack: " << context->key
                          << " pendingReq is: " << pendingReq;
        };
        std::vector<std::shared_ptr<PutObjectAsyncContext>> uploadTasks;
        while (tmpLen > 0) {
            if (blockPos + tmpLen > blockSize) {
                n = blockSize - blockPos;
            } else {
                n = tmpLen;
            }

            objectName = curvefs::common::s3util::GenObjName(
                chunkId, blockIndex, 0, fsId, inodeId);
            int ret = 0;
            if (s3ClientAdaptor_->IsReadWriteCache()) {
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
                delete data;
                dirty_.store(true, std::memory_order_release);
                return CURVEFS_ERROR::INTERNAL;
            }
            tmpLen -= n;
            blockIndex++;
            writeOffset += n;
            blockPos = (blockPos + n) % blockSize;
        }
        if (!s3ClientAdaptor_->IsReadWriteCache()) {
            pendingReq.fetch_add(uploadTasks.size(),
                    std::memory_order_seq_cst);
            VLOG(9) << "pendingReq init: " << pendingReq;
            for (auto iter = uploadTasks.begin();
                iter != uploadTasks.end(); ++iter) {
                VLOG(9) << "upload start: " << (*iter)->key
                        << " len : " << (*iter)->bufferSize;
                s3ClientAdaptor_->GetS3Client()->UploadAsync(*iter);
            }
        }

        while (pendingReq.load(std::memory_order_seq_cst)) {
            VLOG(9) << "wait for pendingReq";
            cond.Wait();
        }

        delete data;
        VLOG(9) << "update inode start, chunkId:" << chunkId
                << ",offset:" << offset << ",len:" << writeOffset
                << ",inodeId:" << inodeId << ",chunkIndex:" << chunkIndex;
        {
            std::shared_ptr<InodeWrapper> inodeWrapper;
            CURVEFS_ERROR ret =
                s3ClientAdaptor_->GetInodeCacheManager()->GetInode(
                    inodeId, inodeWrapper);
            if (ret != CURVEFS_ERROR::OK) {
                LOG(ERROR) << "get inode fail, ret:" << ret;
                dirty_.store(true, std::memory_order_release);
                return ret;
            }
            ::curve::common::UniqueLock lgGuard = inodeWrapper->GetUniqueLock();
            Inode inode = inodeWrapper->GetInodeUnlocked();
            auto s3ChunkInfoMap = inode.mutable_s3chunkinfomap();
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
            inodeWrapper->SwapInode(&inode);
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

}  // namespace client
}  // namespace curvefs
