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
 * Created Date: 22-4-24
 * Author: huyao （baijiaruo）
 */

#include "curvefs/src/client/s3/s3_storage.h"
#include "curvefs/src/client/cache/client_cache_manager.h"

namespace curvefs {
namespace client {

ssize_t S3Storage::Read(uint64_t ino, off_t offset, size_t len, char *data,
                        std::vector<ReadPart> *totalMiss) {
    unsigned int maxRetry = 3;  // hardcode, fixme
    unsigned int retry = 0;
    uint64_t index = offset / chunkSize_;
    uint64_t chunkPos = offset % chunkSize_;
    uint64_t readOffset = 0;
    std::shared_ptr<InodeWrapper> inodeWrapper;

    CURVEFS_ERROR r = inodeManager_->GetInode(ino, inodeWrapper);
    if (r != CURVEFS_ERROR::OK) {
        LOG(WARNING) << "get inode fail, ret:" << ret;
        return -1;
    }

    std::vector<S3ReadResponse> responses;
    while (retry < maxRetry) {
        std::vector<S3ReadRequest> totalS3Requests;
        auto iter = totalRequests.begin();
        uint64_t fileLen;
        {
            uint64_t readLen = 0;
            ::curve::common::UniqueLock lgGuard =
                inodeWrapper->GetUniqueLock();
            Inode* inode = inodeWrapper->GetMutableInodeUnlocked();
            VLOG(9) << "FileCacheManager::Read Inode: "
                        << inode->DebugString();
            while (len > 0) {
                if (chunkPos + len > chunkSize_) {
                    readLen = chunkSize_ - chunkPos;        
                } else {
                    readLen = len;
                }
                auto s3InfoListIter =
                    inode->s3chunkinfomap().find(index);
                if (s3InfoListIter == inode->s3chunkinfomap().end()) {
                    VLOG(6)
                        << "s3infolist is not found.index:" << index;
                    memset(data + readOffset, 0, readLen);
                } else {
                    std::vector<S3ReadRequest> s3Requests;
                    ReadRequest request;
                    std::vector<ReadRequest> miss;
                    request.offset = offset + readOffset;
                    request.len = readLen;
                    request.bufOffset = readOffset;
                    GenerateS3Request(request, s3InfoListIter->second, data,
                                    &s3Requests, inode->fsid(),
                                    inode->inodeid());
                    totalS3Requests.insert(totalS3Requests.end(),
                                        s3Requests.begin(),
                                        s3Requests.end());
                    totalMiss->insert(totalMiss->end(), miss.begin(), miss.end());
                }
                len -= readLen;
                index++;
                readOffset += readLen;
                chunkPos = (chunkPos + readLen) % chunkSize_;
            }
        }
        if (totalS3Requests.empty()) {
            VLOG(6) << "s3 has not data to read.";
            return readOffset;
        }

        uint32_t i;
        for (i = 0; i < totalS3Requests.size(); i++) {
            S3ReadRequest& tmp_req = totalS3Requests[i];
            VLOG(9) << "S3ReadRequest chunkid:" << tmp_req.chunkId
                    << ",offset:" << tmp_req.offset
                    << ",len:" << tmp_req.len
                    << ",objectOffset:" << tmp_req.objectOffset
                    << ",readOffset:" << tmp_req.readOffset
                    << ",compaction:" << tmp_req.compaction
                    << ",fsid:" << tmp_req.fsId
                    << ",inodeId:" << tmp_req.inodeId;
        }

        ret = ReadFromS3(totalS3Requests, &responses, fileLen);
        if (ret < 0) {
            retry++;
            responses.clear();
            if (ret != -2 || retry == maxRetry) {
                LOG(ERROR) << "read from s3 failed. ret:" << ret;
                return ret;
            } else {
                // ret -2 refs s3obj not exist
                // clear inodecache && get again
                LOG(INFO) << "inode cache maybe steal, try to get latest";
                ::curve::common::UniqueLock lgGuard =
                    inodeWrapper->GetUniqueLock();
                auto r = inodeWrapper->RefreshS3ChunkInfo();
                if (r != CURVEFS_ERROR::OK) {
                    LOG(WARNING) << "refresh inode fail, ret:" << ret;
                    return -1;
                }
            }
        } else {
            break;
        }
    }
    auto repIter = responses.begin();
    for (; repIter != responses.end(); repIter++) {
        VLOG(6) << "readOffset:" << repIter->GetReadOffset()
                << ",bufLen:" << repIter->GetBufLen();
        memcpy(dataBuf + repIter->GetReadOffset(),
            repIter->GetDataBuf(), repIter->GetBufLen());
    }
    return;
}

void S3Storage::GenerateS3Request(ReadRequest request,
                                  const S3ChunkInfoList &s3ChunkInfoList,
                                  char *data,
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
                              &addReadRequests, &deletingReq, data, requests,
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
        memset(data + emptyIter->second.bufOffset, 0, emptyIter->second.len);
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

void S3Storage::HandleReadRequest(
    const ReadRequest &request, const S3ChunkInfo &s3ChunkInfo,
    std::vector<ReadRequest> *addReadRequests,
    std::vector<uint64_t> *deletingReq, 
    char *data,
    std::vector<S3ReadRequest> *requests,
    uint64_t fsId, uint64_t inodeId) {
   //uint64_t blockSize = s3ClientAdaptor_->GetBlockSize();
    //uint64_t chunkSize = s3ClientAdaptor_->GetChunkSize();
    S3ReadRequest s3Request;
    uint64_t s3ChunkInfoOffset = s3ChunkInfo.offset();
    uint64_t s3ChunkInfoLen = s3ChunkInfo.len();
    uint64_t fileOffset = request.offset;
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
        // splitRequest.index = request.index;
        splitRequest.offset = request.offset;
        splitRequest.len = s3ChunkInfoOffset - fileOffset;
        splitRequest.bufOffset = request.bufOffset;
        addReadRequests->emplace_back(splitRequest);
        deletingReq->emplace_back(request.offset);
        readOffset += splitRequest.len;
        /*
             -----                 read block           -
                ------             S3ChunkInfo
        */
        if (fileOffset + length <= s3ChunkInfoOffset + s3ChunkInfoLen) {
            if (s3ChunkInfo.zero()) {
                memset(static_cast<char *>(data) + bufOffset + readOffset, 0,
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
                memset(static_cast<char *>(data) + bufOffset + readOffset, 0,
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
                memset(static_cast<char *>(data) + bufOffset + readOffset, 0,
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
                memset(static_cast<char *>(data) + bufOffset + readOffset, 0,
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

}  // namespace client
}  // namespace curvefs