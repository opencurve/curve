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

namespace curvefs {
namespace client {

std::string GenerateObjectName(uint64_t chunkId, uint64_t blockIndex) {
    std::ostringstream oss;
    oss << chunkId << "_" << blockIndex << "_0";
    return oss.str();
}

FileCacheManagerPtr FsCacheManager::FindFileCacheManager(uint64_t inodeId) {
    ReadLockGuard readLockGuard(rwLock_);

    auto it = fileCacheManagerMap_.find(inodeId);
    if (it != fileCacheManagerMap_.end()) {
        return it->second;
    }

    return nullptr;
}

FileCacheManagerPtr FsCacheManager::FindOrCreateFileCacheManager(
    uint64_t fsId, uint64_t inodeId) {
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

    if (lruReadDataCacheList_.size() == lruCapacity_) {
        auto trimDataCache = lruReadDataCacheList_.back();
        lruReadDataCacheList_.pop_back();
        trimDataCache->Release();
    }
    lruReadDataCacheList_.push_front(dataCache);
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
    lruReadDataCacheList_.erase(iter);
    return;
}

CURVEFS_ERROR FsCacheManager::FsSync(bool force) {
    CURVEFS_ERROR ret;
    WriteLockGuard writeLockGuard(rwLock_);
    auto iter = fileCacheManagerMap_.begin();

    LOG(INFO) << "FsSync force:" << force;
    for (; iter != fileCacheManagerMap_.end(); iter++) {
        ret = iter->second->Flush(force);
        if (ret != CURVEFS_ERROR::OK) {
            LOG(ERROR) << "fs fssync error, ret:" << ret;
            return ret;
        }
        if (iter->second->IsEmpty()) {
            fileCacheManagerMap_.erase(iter);
        }
    }

    return CURVEFS_ERROR::OK;
}

int FileCacheManager::Write(uint64_t offset, uint64_t length,
                            const char* dataBuf) {
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
                                  uint64_t writeLen, const char* dataBuf) {
    ChunkCacheManagerPtr chunkCacheManager =
        FindOrCreateChunkCacheManager(index);

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

ChunkCacheManagerPtr FileCacheManager::FindOrCreateChunkCacheManager(
    uint64_t index) {
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

int FileCacheManager::Read(Inode* inode, uint64_t offset, uint64_t length,
                           char* dataBuf) {
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

        ReadChunk(index, chunkPos, readLen, dataBuf + readOffset, readOffset,
                  &requests);
        totalRequests.insert(totalRequests.end(), requests.begin(),
                             requests.end());
        length -= readLen;
        index++;
        readOffset += readLen;
        chunkPos = (chunkPos + readLen) % chunkSize;
    }

    std::vector<S3ReadRequest> totalS3Requests;
    auto iter = totalRequests.begin();

    for (; iter != totalRequests.end(); iter++) {
        auto s3InfoListIter = inode->s3chunkinfomap().find(iter->index);
        if (s3InfoListIter == inode->s3chunkinfomap().end()) {
            memset(dataBuf + iter->bufOffset, 0, iter->len);
            continue;
        }
        std::vector<S3ReadRequest> s3Requests;
        GenerateS3Request(*iter, s3InfoListIter->second, dataBuf, &s3Requests);
        totalS3Requests.insert(totalS3Requests.end(), s3Requests.begin(),
                               s3Requests.end());
    }
    uint32_t i;
    for (i = 0; i < totalS3Requests.size(); i++) {
        S3ReadRequest& tmp_req = totalS3Requests[i];
        LOG(INFO) << "S3ReadRequest readoffset:" << tmp_req.GetReadOffset()
                  << ",offset:" << tmp_req.GetS3ChunkInfo().offset()
                  << ",len:" << tmp_req.GetS3ChunkInfo().len();
    }

    std::vector<S3ReadResponse> responses;

    ret = HandleReadRequest(totalS3Requests, &responses);
    if (ret < 0) {
        LOG(ERROR) << "handle read request fail:" << ret;
        return ret;
    }

    auto repIter = responses.begin();
    for (; repIter != responses.end(); repIter++) {
        LOG(INFO) << "readOffset:" << repIter->GetReadOffset()
                  << ",bufLen:" << repIter->GetBufLen();
        memcpy(dataBuf + repIter->GetReadOffset(), repIter->GetDataBuf(),
               repIter->GetBufLen());
    }

    return readOffset;
}

int FileCacheManager::HandleReadRequest(
    const std::vector<S3ReadRequest>& requests,
    std::vector<S3ReadResponse>* responses) {
    uint64_t chunkSize = s3ClientAdaptor_->GetChunkSize();
    uint64_t blockSize = s3ClientAdaptor_->GetBlockSize();
    (*responses).reserve(requests.size());
    std::vector<S3ReadRequest>::const_iterator iter = requests.begin();
    std::atomic<uint64_t> pendingReq(0);
    curve::common::CountDownEvent cond(1);
    bool async = false;
    std::vector<std::pair<ChunkCacheManagerPtr, DataCachePtr> > DataCacheVec;

    GetObjectAsyncCallBack cb =
        [&](const S3Adapter* adapter,
            const std::shared_ptr<GetObjectAsyncContext>& context) {
            //  todo failed branch
            pendingReq.fetch_sub(1, std::memory_order_relaxed);
            cond.Signal();
        };

    for (; iter != requests.end(); iter++) {
        uint64_t blockIndex =
            iter->GetS3ChunkInfo().offset() % chunkSize / blockSize;
        uint64_t blockPos =
            iter->GetS3ChunkInfo().offset() % chunkSize % blockSize;
        uint64_t chunkIndex = iter->GetS3ChunkInfo().offset() / chunkSize;
        uint64_t chunkPos = iter->GetS3ChunkInfo().offset() % chunkSize;
        uint64_t len = iter->GetS3ChunkInfo().len();
        uint64_t n = 0;
        uint64_t readOffset = 0;
        ChunkCacheManagerPtr chunkCacheManager =
            FindOrCreateChunkCacheManager(chunkIndex);
        DataCachePtr dataCache = std::make_shared<DataCache>(
            s3ClientAdaptor_, chunkCacheManager.get(), chunkPos, len);
        /*chunkCacheManager->CreateReadDataCache(
            s3ClientAdaptor_, chunkPos, len);*/
        DataCacheVec.push_back(std::make_pair(chunkCacheManager, dataCache));
        S3ReadResponse response(dataCache);

        while (len > 0) {
            if (blockPos + len > blockSize) {
                n = blockSize - blockPos;
            } else {
                n = len;
            }

            std::string name = GenerateObjectName(
                iter->GetS3ChunkInfo().chunkid(), blockIndex);
            if (async) {
                LOG(INFO) << "async read s3";
                auto context = std::make_shared<GetObjectAsyncContext>();
                context->key = name;
                context->buf = response.GetDataBuf() + readOffset;
                context->offset = blockPos;
                context->len = n;
                context->cb = cb;

                pendingReq.fetch_add(1, std::memory_order_relaxed);
                s3ClientAdaptor_->GetS3Client()->DownloadAsync(context);
            } else {
                LOG(INFO) << "sync read s3";
                int ret = s3ClientAdaptor_->GetS3Client()->Download(
                    name, response.GetDataBuf() + readOffset, blockPos, n);
                if (ret < 0) {
                    LOG(ERROR)
                        << "download name:" << name << " offset:" << blockPos
                        << " len:" << n << "fail:" << ret;
                    return ret;
                }
            }

            len -= n;
            readOffset += n;
            blockIndex++;
            blockPos = (blockPos + n) % blockSize;
        }
        response.SetReadOffset(iter->GetReadOffset());
        responses->emplace_back(response);

        LOG(INFO) << "response readOffset:" << response.GetReadOffset()
                  << ",bufLen:" << readOffset;
    }

    while (pendingReq.load(std::memory_order_acquire)) {
        cond.Wait();
    }

    for (auto dataCache : DataCacheVec) {
        dataCache.first->AddReadDataCache(dataCache.second);
    }
    return 0;
}

void FileCacheManager::GenerateS3Request(ReadRequest request,
                                         S3ChunkInfoList s3ChunkInfoList,
                                         char* dataBuf,
                                         std::vector<S3ReadRequest>* requests) {
    std::vector<S3ChunkInfo> chunks = GetReadChunks(s3ChunkInfoList);
    std::vector<S3ChunkInfo> sortChunks = SortByOffset(chunks);
    char* buf = dataBuf + request.bufOffset;
    uint64_t chunkSize = s3ClientAdaptor_->GetChunkSize();
    uint64_t offset = request.index * chunkSize + request.chunkPos;
    uint64_t length = request.len;
    uint32_t i = 0;
    S3ChunkInfo tmp;
    uint64_t readOffset = 0;

    while (length > 0) {
        S3ReadRequest s3Request;
        if (i == sortChunks.size()) {
            memset(static_cast<char*>(buf) + readOffset, 0, length);
            break;
        }
        tmp = sortChunks[i];

        /*
        -----    read block
               ------  S3ChunkInfo
        */
        if (offset + length <= tmp.offset()) {
            memset(buf + readOffset, 0, length);
            break;
            /*
                   -----              ------------   read block           -
                      ------             -----       S3ChunkInfo
            */
        } else if ((tmp.offset() >= offset) &&
                   (tmp.offset() < offset + length)) {
            int n = tmp.offset() - offset;
            memset(static_cast<char*>(buf) + readOffset, 0, n);
            offset = tmp.offset();
            readOffset += n;
            length -= n;

            if (offset + length <= tmp.offset() + tmp.len()) {
                if (tmp.zero()) {
                    memset(static_cast<char*>(buf) + readOffset, 0, length);
                } else {
                    s3Request.SetS3ChunkInfo(tmp);
                    s3Request.GetS3ChunkInfo().set_offset(offset);
                    s3Request.GetS3ChunkInfo().set_len(length);
                    s3Request.SetReadOffset(request.bufOffset + readOffset);
                    requests->push_back(s3Request);
                }
                readOffset += length;
                length = 0;
            } else {
                if (tmp.zero()) {
                    memset(static_cast<char*>(buf) + readOffset, 0, tmp.len());
                } else {
                    s3Request.SetS3ChunkInfo(tmp);
                    s3Request.SetReadOffset(request.bufOffset + readOffset);
                    requests->push_back(s3Request);
                }
                readOffset += tmp.len();
                length -= tmp.len();
                offset += tmp.len();
            }
            /*
                     ----                      ---------   read block
                   ----------                --------      S3ChunkInfo
            */
        } else if ((tmp.offset() < offset) &&
                   (tmp.offset() + tmp.len() > offset)) {
            if (offset + length <= tmp.offset() + tmp.len()) {
                if (tmp.zero()) {
                    memset(static_cast<char*>(buf) + readOffset, 0, length);
                } else {
                    s3Request.SetS3ChunkInfo(tmp);
                    s3Request.GetS3ChunkInfo().set_offset(offset);
                    s3Request.GetS3ChunkInfo().set_len(length);
                    s3Request.SetReadOffset(request.bufOffset + readOffset);
                    requests->push_back(s3Request);
                }
                readOffset += length;
                length = 0;
            } else {
                if (tmp.zero()) {
                    memset(static_cast<char*>(buf) + readOffset, 0,
                           tmp.offset() + tmp.len() - offset);
                } else {
                    s3Request.SetS3ChunkInfo(tmp);
                    s3Request.GetS3ChunkInfo().set_offset(offset);
                    s3Request.GetS3ChunkInfo().set_len(tmp.offset() +
                                                       tmp.len() - offset);
                    s3Request.SetReadOffset(request.bufOffset + readOffset);
                    requests->push_back(s3Request);
                }
                offset = tmp.offset() + tmp.len();
                length -= s3Request.GetS3ChunkInfo().len();
                readOffset += s3Request.GetS3ChunkInfo().len();
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

std::vector<S3ChunkInfo> FileCacheManager::GetReadChunks(
    const S3ChunkInfoList& s3ChunkInfoList) {
    S3ChunkInfo tmp, chunkTmp;
    std::vector<S3ChunkInfo> chunks;

    for (int i = 0; i < s3ChunkInfoList.s3chunks_size(); ++i) {
        tmp = s3ChunkInfoList.s3chunks(i);
        std::vector<S3ChunkInfo> addChunks;
        std::vector<int> waitingDel;
        for (uint32_t j = 0; j < chunks.size(); j++) {
            chunkTmp = chunks[j];
            // overlap, must cut old chunk
            if ((tmp.offset() < (chunkTmp.offset() + chunkTmp.len())) &&
                (chunkTmp.offset() < (tmp.offset() + tmp.len()))) {
                addChunks = CutOverLapChunks(tmp, chunkTmp);
                waitingDel.push_back(j);
            }
        }

        std::vector<int>::iterator iter = waitingDel.begin();
        for (; iter != waitingDel.end(); iter++) {
            chunks.erase(chunks.begin() + *iter);
        }
        std::vector<S3ChunkInfo>::iterator chunkIter = addChunks.begin();
        for (; chunkIter != addChunks.end(); chunkIter++) {
            chunks.push_back(*chunkIter);
        }
        chunks.push_back(tmp);
    }

    return chunks;
}

std::vector<S3ChunkInfo> FileCacheManager::SortByOffset(
    std::vector<S3ChunkInfo> chunks) {
    std::sort(chunks.begin(), chunks.end(), [](S3ChunkInfo a, S3ChunkInfo b) {
        return a.offset() < b.offset();
    });
    return chunks;
}

std::vector<S3ChunkInfo> FileCacheManager::CutOverLapChunks(
    const S3ChunkInfo& newChunk, const S3ChunkInfo& old) {
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
                                 uint64_t readLen, char* dataBuf,
                                 uint64_t dataBufOffset,
                                 std::vector<ReadRequest>* requests) {
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

CURVEFS_ERROR FileCacheManager::Flush(Inode* inode, bool force) {
    CURVEFS_ERROR ret;
    std::map<uint64_t, ChunkCacheManagerPtr> tmp;
    {
        WriteLockGuard writeLockGuard(rwLock_);
        tmp = chunkCacheMap_;
    }
    auto iter = tmp.begin();

    for (; iter != tmp.end(); iter++) {
        ret = iter->second->Flush(inode, force);
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

CURVEFS_ERROR FileCacheManager::Flush(bool force) {
    CURVEFS_ERROR ret;
    std::shared_ptr<InodeWrapper> inodeWrapper;
    ret = s3ClientAdaptor_->GetInodeCacheManager()->GetInode(inode_,
                                                             inodeWrapper);
    if (ret != CURVEFS_ERROR::OK) {
        LOG(ERROR) << "get inode fail, ret:" << ret;
        return ret;
    }
    ::curve::common::UniqueLock lgGuard = inodeWrapper->GetUniqueLock();
    Inode inode = inodeWrapper->GetInodeUnlocked();

    ret = Flush(&inode, force);
    if (ret != CURVEFS_ERROR::OK) {
        LOG(ERROR) << "file flush fail, ret:" << ret;
    }
    inodeWrapper->SwapInode(&inode);

    return ret;
}

void ChunkCacheManager::ReadByWriteCache(uint64_t chunkPos, uint64_t readLen,
                                         char* dataBuf, uint64_t dataBufOffset,
                                         std::vector<ReadRequest>* requests) {
    ReadLockGuard readLockGuard(rwLockWrite_);

    if (dataWCacheMap_.empty()) {
        ReadRequest request;
        request.index = index_;
        request.len = readLen;
        request.chunkPos = chunkPos;
        request.bufOffset = dataBufOffset;
        requests->emplace_back(request);
        return;
    }
    std::map<uint64_t, DataCachePtr>::iterator iter = dataWCacheMap_.begin();
    for (; iter != dataWCacheMap_.end(); iter++) {
        ReadRequest request;
        uint64_t dcChunkPos = iter->second->GetChunkPos();
        uint64_t dcLen = iter->second->GetLen();

        if (chunkPos + readLen <= dcChunkPos) {
            break;
        } else if ((chunkPos + readLen > dcChunkPos) &&
                   (chunkPos < dcChunkPos)) {
            request.len = dcChunkPos - chunkPos;
            request.chunkPos = chunkPos;
            request.index = index_;
            request.bufOffset = dataBufOffset;
            requests->emplace_back(request);
            char* cacheData = iter->second->GetData();
            /*
                 -----               ReadData
                    ------           DataCache
            */
            if (chunkPos + readLen <= dcChunkPos + dcLen) {
                memcpy(dataBuf + request.len, cacheData,
                       chunkPos + readLen - dcChunkPos);
                readLen = 0;
                break;
                /*
                     -----------         ReadData
                        ------           DataCache
                */
            } else {
                memcpy(dataBuf + request.len, cacheData, dcLen);
                readLen = chunkPos + readLen - (dcChunkPos + dcLen);
                dataBufOffset = dcChunkPos + dcLen - chunkPos + dataBufOffset;
                chunkPos = dcChunkPos + dcLen;
            }
        } else if ((chunkPos >= dcChunkPos) &&
                   (chunkPos < dcChunkPos + dcLen)) {
            char* cacheData = iter->second->GetData();
            /*
                     ----              ReadData
                   ---------           DataCache
            */
            if (chunkPos + readLen <= dcChunkPos + dcLen) {
                memcpy(dataBuf, cacheData + chunkPos - dcChunkPos, readLen);
                readLen = 0;
                break;
                /*
                         ----------              ReadData
                       ---------                DataCache
                */
            } else {
                memcpy(dataBuf, cacheData + chunkPos - dcChunkPos,
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
                                        char* dataBuf, uint64_t dataBufOffset,
                                        std::vector<ReadRequest>* requests) {
    ReadLockGuard readLockGuard(rwLockRead_);

    if (dataRCacheMap_.empty()) {
        ReadRequest request;
        request.index = index_;
        request.len = readLen;
        request.chunkPos = chunkPos;
        request.bufOffset = dataBufOffset;
        requests->emplace_back(request);
        return;
    }
    std::map<uint64_t, std::list<DataCachePtr>::iterator>::iterator iter =
        dataRCacheMap_.begin();
    for (; iter != dataRCacheMap_.end(); iter++) {
        ReadRequest request;
        std::list<DataCachePtr>::iterator dcpIter = iter->second;
        uint64_t dcChunkPos = (*dcpIter)->GetChunkPos();
        uint64_t dcLen = (*dcpIter)->GetLen();

        if (chunkPos + readLen <= dcChunkPos) {
            break;
        } else if ((chunkPos + readLen > dcChunkPos) &&
                   (chunkPos < dcChunkPos)) {
            s3ClientAdaptor_->GetFsCacheManager()->Get(iter->second);
            request.len = dcChunkPos - chunkPos;
            request.chunkPos = chunkPos;
            request.index = index_;
            request.bufOffset = dataBufOffset;
            requests->emplace_back(request);
            char* cacheData = (*dcpIter)->GetData();
            /*
                 -----               ReadData
                    ------           DataCache
            */
            if (chunkPos + readLen <= dcChunkPos + dcLen) {
                memcpy(dataBuf + request.len, cacheData,
                       chunkPos + readLen - dcChunkPos);
                readLen = 0;
                break;
                /*
                     -----------         ReadData
                        ------           DataCache
                */
            } else {
                memcpy(dataBuf + request.len, cacheData, dcLen);
                readLen = chunkPos + readLen - (dcChunkPos + dcLen);
                dataBufOffset = dcChunkPos + dcLen - chunkPos + dataBufOffset;
                chunkPos = dcChunkPos + dcLen;
            }
        } else if ((chunkPos >= dcChunkPos) &&
                   (chunkPos < dcChunkPos + dcLen)) {
            char* cacheData = (*dcpIter)->GetData();
            s3ClientAdaptor_->GetFsCacheManager()->Get(iter->second);
            /*
                     ----              ReadData
                   ---------           DataCache
            */
            if (chunkPos + readLen <= dcChunkPos + dcLen) {
                memcpy(dataBuf, cacheData + chunkPos - dcChunkPos, readLen);
                readLen = 0;
                break;
                /*
                         ----------              ReadData
                       ---------                DataCache
                */
            } else {
                memcpy(dataBuf, cacheData + chunkPos - dcChunkPos,
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

DataCachePtr ChunkCacheManager::FindWriteableDataCache(
    uint64_t chunkPos, uint64_t len,
    std::vector<DataCachePtr>* mergeDataCacheVer) {
    // todo performance optimization
    WriteLockGuard writeLockGuard(rwLockWrite_);

    auto iter = dataWCacheMap_.upper_bound(chunkPos);
    if (iter != dataWCacheMap_.begin()) {
        --iter;
    }
    for (; iter != dataWCacheMap_.end(); iter++) {
        LOG(INFO) << "FindWriteableDataCache pos:" << chunkPos
                  << ", len:" << len
                  << ", datacache pos:" << iter->second->GetChunkPos()
                  << ", datacache len:" << iter->second->GetLen();
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
                dataWCacheMap_.erase(iter);
                s3ClientAdaptor_->GetFsCacheManager()->DataCacheNumFetchSub(1);
            }
            return dataCache;
        }
    }
    return nullptr;
}

DataCachePtr ChunkCacheManager::CreateWriteDataCache(
    S3ClientAdaptorImpl* s3ClientAdaptor, uint32_t chunkPos, uint32_t len,
    const char* data) {
    DataCachePtr dataCache =
        std::make_shared<DataCache>(s3ClientAdaptor, this, chunkPos, len, data);
    LOG(INFO) << "CreateWriteDataCache chunkPos:" << chunkPos
              << ", len:" << len;
    WriteLockGuard writeLockGuard(rwLockWrite_);

    dataWCacheMap_.emplace(chunkPos, dataCache);
    s3ClientAdaptor_->GetFsCacheManager()->DataCacheNumInc();
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
            LOG(INFO) << "read cache chunkPos:" << chunkPos << ",len:" << len
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
    WriteLockGuard writeLockGuard(rwLockRead_);

    auto iter = dataRCacheMap_.find(key);
    if (iter == dataRCacheMap_.end()) {
        return;
    }

    dataRCacheMap_.erase(iter);
    return;
}

void ChunkCacheManager::ReleaseCache(S3ClientAdaptorImpl* s3ClientAdaptor) {
    {
        WriteLockGuard writeLockGuard(rwLockWrite_);
        auto size = dataWCacheMap_.size();
        dataWCacheMap_.clear();
        if (size > 0) {
            s3ClientAdaptor_->GetFsCacheManager()->DataCacheNumFetchSub(size);
        }
    }
    WriteLockGuard writeLockGuard(rwLockRead_);
    auto iter = dataRCacheMap_.begin();
    for (; iter != dataRCacheMap_.end(); iter++) {
        s3ClientAdaptor->GetFsCacheManager()->Delete(iter->second);
    }
    dataRCacheMap_.clear();
}

CURVEFS_ERROR ChunkCacheManager::Flush(Inode* inode, bool force) {
    std::map<uint64_t, DataCachePtr> tmp;
    CURVEFS_ERROR ret;
    {
        WriteLockGuard writeLockGuard(rwLockWrite_);
        dataWCacheMap_.swap(tmp);
    }
    auto size = tmp.size();
    auto iter = tmp.begin();
    for (; iter != tmp.end(); iter++) {
        ret = iter->second->Flush(inode, force);
        if ((ret != CURVEFS_ERROR::OK) && (ret != CURVEFS_ERROR::NOFLUSH)) {
            LOG(INFO) << "dataCache flush failed. ret:" << ret
                      << ",index:" << index_
                      << ",data chunkpos:" << iter->second->GetChunkPos();
            if (dataWCacheMap_.empty()) {
                WriteLockGuard writeLockGuard(rwLockWrite_);
                dataWCacheMap_.swap(tmp);
            } else {
                // todo
                s3ClientAdaptor_->GetFsCacheManager()->DataCacheNumFetchSub(
                    size);
            }
            return ret;
        }
        if (ret == CURVEFS_ERROR::OK) {
            AddReadDataCache(iter->second);
        }
        s3ClientAdaptor_->GetFsCacheManager()->DataCacheNumFetchSub(1);
    }

    return CURVEFS_ERROR::OK;
}

void DataCache::Write(uint64_t chunkPos, uint64_t len, const char* data,
                      const std::vector<DataCachePtr>& mergeDataCacheVer) {
    uint64_t total_size = 0;

    LOG(INFO) << "DataCache Write() chunkPos:" << chunkPos << ",len:" << len
              << ",dataCache's chunkPos:" << chunkPos_
              << ",dataCache's len:" << len_;
    curve::common::LockGuard lg(mtx_);
    if (chunkPos <= chunkPos_) {
        /*
            ------       DataCache
         -------         WriteData
        */
        if (chunkPos + len <= chunkPos_ + len_) {
            total_size = chunkPos_ + len_ - chunkPos;
            char* newDatabuf = new char[total_size];
            memcpy(newDatabuf, data, len);
            memcpy(newDatabuf + len, data_, total_size - len);
            char* tmp = data_;
            data_ = newDatabuf;
            delete tmp;
            len_ = total_size;
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
                    total_size =
                        (*iter)->GetChunkPos() + (*iter)->GetLen() - chunkPos;
                    char* newDatabuf = new char[total_size];
                    memcpy(newDatabuf, data, len);
                    memcpy(newDatabuf + len,
                           (*iter)->GetData() +
                               (chunkPos + len - (*iter)->GetChunkPos()),
                           total_size - len);
                    char* tmp = data_;
                    data_ = newDatabuf;
                    delete tmp;
                    len_ = total_size;
                    chunkPos_ = chunkPos;
                    return;
                }
            }
            /*
                     ------    ------         DataCache
                  ---------------------       WriteData
            */
            total_size = len;
            char* newDatabuf = new char[total_size];
            memcpy(newDatabuf, data, len);
            delete data_;
            data_ = newDatabuf;
            len_ = total_size;
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
                    total_size =
                        (*iter)->GetChunkPos() + (*iter)->GetLen() - chunkPos_;
                    char* newDatabuf = new char[total_size];
                    memcpy(newDatabuf, data_, chunkPos - chunkPos_);
                    memcpy(newDatabuf + chunkPos - chunkPos_, data, len);
                    memcpy(newDatabuf + chunkPos - chunkPos_ + len,
                           (*iter)->GetData() + chunkPos + len -
                               (*iter)->GetChunkPos(),
                           (*iter)->GetChunkPos() + (*iter)->GetLen() -
                               chunkPos - len);
                    char* tmp = data_;
                    data_ = newDatabuf;
                    delete tmp;
                    len_ = total_size;
                    return;
                }
            }
            /*
                     ------         ------         DataCache
                        --------------------       WriteData
            */
            total_size = chunkPos - chunkPos_ + len;
            char* newDatabuf = new char[total_size];
            memcpy(newDatabuf, data_, chunkPos - chunkPos_);
            memcpy(newDatabuf + chunkPos - chunkPos_, data, len);
            char* tmp = data_;
            data_ = newDatabuf;
            delete tmp;
            len_ = total_size;
            return;
        }
    }

    return;
}

void DataCache::Release() {
    chunkCacheManager_->ReleaseReadDataCache(chunkPos_);

    return;
}

std::string DataCache::GenerateObjectName(uint64_t chunkId,
                                          uint64_t blockIndex) {
    std::ostringstream oss;
    oss << chunkId << "_" << blockIndex << "_0";
    return oss.str();
}

CURVEFS_ERROR DataCache::Flush(Inode* inode, bool force) {
    uint64_t blockSize = s3ClientAdaptor_->GetBlockSize();
    uint64_t chunkSize = s3ClientAdaptor_->GetChunkSize();
    uint32_t flushInterval = s3ClientAdaptor_->GetFlushInterval();
    uint64_t fsId = inode->fsid();
    uint64_t blockPos = chunkPos_ % blockSize;
    uint64_t blockIndex = chunkPos_ / blockSize;
    uint64_t chunkIndex = chunkCacheManager_->GetIndex();
    uint64_t offset = chunkIndex * chunkSize + chunkPos_;
    uint64_t tmpLen = len_;
    uint64_t n = 0;
    std::string objectName;
    uint32_t writeOffset = 0;
    bool isFlush = true;
    uint64_t chunkId;
    uint64_t now = ::curve::common::TimeUtility::GetTimeofDaySec();
    if (!force) {
        if (len_ = chunkSize) {
            isFlush = true;
        } else if (now < (createTime_ + flushInterval)) {
            isFlush = false;
        }
    }

    CURVEFS_ERROR ret = s3ClientAdaptor_->AllocS3ChunkId(fsId, &chunkId);
    if (ret != CURVEFS_ERROR::OK) {
        LOG(ERROR) << "alloc s3 chunkid fail. ret:" << ret;
        return ret;
    }

    if (isFlush) {
        LOG(INFO) << "start datacache flush, chunkId:" << chunkId
                  << ",Len:" << tmpLen << ",blockPos:" << blockPos
                  << ",blockIndex:" << blockIndex;
        while (tmpLen > 0) {
            if (blockPos + tmpLen > blockSize) {
                n = blockSize - blockPos;
            } else {
                n = tmpLen;
            }

            objectName = GenerateObjectName(chunkId, blockIndex);
            int ret = s3ClientAdaptor_->GetS3Client()->Upload(
                objectName, data_ + writeOffset, n);
            if (ret < 0) {
                LOG(ERROR) << "upload object fail. object: " << objectName;
                return CURVEFS_ERROR::INTERNAL;
            }

            tmpLen -= n;
            blockIndex++;
            writeOffset += n;
            blockPos = (blockPos + n) % blockSize;
        }

        LOG(INFO) << "update inode start, chunkId:" << chunkId
                  << ",offset:" << offset << ",len:" << writeOffset
                  << ",inode:" << inode;

        auto s3ChunkInfoMap = inode->mutable_s3chunkinfomap();
        auto s3chunkInfoListIter = s3ChunkInfoMap->find(chunkIndex);
        if (s3chunkInfoListIter == s3ChunkInfoMap->end()) {
            S3ChunkInfoList s3ChunkInfoList;
            UpdateInodeChunkInfo(&s3ChunkInfoList, chunkId, offset,
                                 writeOffset);
            s3ChunkInfoMap->insert({chunkIndex, s3ChunkInfoList});
        } else {
            S3ChunkInfoList& s3ChunkInfoList = s3chunkInfoListIter->second;
            UpdateInodeChunkInfo(&s3ChunkInfoList, chunkId, offset,
                                 writeOffset);
        }

        return CURVEFS_ERROR::OK;
    }
    return CURVEFS_ERROR::NOFLUSH;
}

void DataCache::UpdateInodeChunkInfo(S3ChunkInfoList* s3ChunkInfoList,
                                     uint64_t chunkId, uint64_t offset,
                                     uint64_t len) {
    S3ChunkInfo* tmp = s3ChunkInfoList->add_s3chunks();

    tmp->set_chunkid(chunkId);
    tmp->set_compaction(0);
    tmp->set_offset(offset);
    tmp->set_len(len);
    tmp->set_size(len);

    LOG(INFO) << "UpdateInodeChunkInfo chunkId:" << chunkId
              << ",offset:" << offset << ", len:" << len
              << ",s3chunks size:" << s3ChunkInfoList->s3chunks_size();
    return;
}

}  // namespace client
}  // namespace curvefs
