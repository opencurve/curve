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
 * Created Date: 21-5-31
 * Author: huyao
 */

#include <brpc/channel.h>
#include <brpc/controller.h>
#include <algorithm>

#include "curvefs/src/client/s3/client_s3_adaptor.h"

namespace curvefs {

namespace client {

void S3ClientAdaptorImpl::Init(const S3ClientAdaptorOption& option,
                           S3Client *client) {
    blockSize_ = option.blockSize;
    chunkSize_ = option.chunkSize;
    metaServerEps_ = option.metaServerEps;
    allocateServerEps_ = option.allocateServerEps;
    client_ = client;
}

int S3ClientAdaptorImpl::Write(Inode *inode, uint64_t offset,
              uint64_t length, const char* buf) {
    uint64_t chunkId;
    uint64_t  version;
    uint64_t index;
    uint64_t chunkPos;
    index = offset / chunkSize_;
    chunkPos = offset % chunkSize_;
    uint64_t n;
    int64_t writeLen;
    uint64_t writeOffset = 0;
    bool append = false;
    S3ChunkInfoList* s3ChunkInfoList = inode->mutable_s3chunkinfolist();
    uint64_t totalWriteLen = 0;
    int ret = -1;
    LOG(INFO) << "write start offset:" << offset << ", len:"
              << length << "inode length:"<< inode->length();
    // first write
    if ((inode->length() == 0)) {
        version = 0;
    // overwrite
    } else if (IsOverlap(inode, offset, length)) {
        ret = UpdateInodeS3Version(inode, &version);
    // write hole or append write
    } else {
        version = s3ChunkInfoList->
            s3chunks(s3ChunkInfoList->s3chunks_size() - 1).version();
        append = IsAppendBlock(inode, offset, length);
    }
    LOG(INFO) << "write version:" << version << ",append:" << append;
    while (length > 0) {
        if (chunkPos + length > chunkSize_) {
            n = chunkSize_ - chunkPos;
        } else {
            n = length;
        }
        ret = GetChunkId(inode, index, &chunkId);
        if (ret < 0) {
            LOG(ERROR) << "get chunk id failed, ret : " << ret;
            return ret;
        }
        writeLen = WriteChunk(chunkId, version, chunkPos,
                              n, (buf + writeOffset), append);
        if (writeLen < 0) {
            LOG(INFO) << "write chunk fail: ret : " << writeLen;
            return writeLen;
        }

        UpdateInodeChunkInfo(s3ChunkInfoList, chunkId, version,
                             index * chunkSize_ + chunkPos, writeLen);

        append = false;
        length -= writeLen;
        index++;
        writeOffset += writeLen;
        totalWriteLen += writeLen;
        chunkPos = (chunkPos + n) % chunkSize_;
    }
    inode->set_version(version);
    return totalWriteLen;
}

void S3ClientAdaptorImpl::UpdateInodeChunkInfo(S3ChunkInfoList *s3ChunkInfoList,
                                           uint64_t chunkId, uint64_t version,
                                           uint64_t offset, uint64_t len) {
    S3ChunkInfo *tmp;
    bool merge = false;
    for (int i = 0; i < s3ChunkInfoList->s3chunks_size(); i++) {
        tmp = s3ChunkInfoList->mutable_s3chunks(i);
        if (chunkId != tmp->chunkid()) {
            continue;
        }
        if (version == tmp->version()) {
            if (offset == tmp->offset() + tmp->len()) {
                len += tmp->len();
                tmp->set_len(len);
                offset = tmp->offset();
                merge = true;
            }
        }
    }

    if (!merge) {
        tmp = s3ChunkInfoList->add_s3chunks();
        tmp->set_chunkid(chunkId);
        tmp->set_version(version);
        tmp->set_offset(offset);
        tmp->set_len(len);
        tmp->set_size(len);
    }

    return;
}

int S3ClientAdaptorImpl::GetChunkId(Inode *inode, uint64_t index,
                                uint64_t *chunkId) {
    S3ChunkInfo tmp;
    uint64_t tmpIndex;
    CURVEFS_ERROR retCode = CURVEFS_ERROR::UNKNOWN;
    const S3ChunkInfoList &s3chunkInfoList = inode->s3chunkinfolist();
    if (inode->length() == 0) {
        retCode = AllocS3ChunkId(inode->fsid(), chunkId);
        if (retCode != CURVEFS_ERROR::OK) {
            return -1;
        }
        LOG(INFO) << "GetChunkId():chunkid:" << *chunkId;
        return 0;
    }

    for (int i = 0; i < s3chunkInfoList.s3chunks_size(); ++i) {
        tmp = s3chunkInfoList.s3chunks(i);
        tmpIndex = tmp.offset() / chunkSize_;
        if (tmpIndex < index) {
            continue;
        } else if (tmpIndex == index) {
            *chunkId = tmp.chunkid();
            return 0;
        }
    }

    retCode = AllocS3ChunkId(inode->fsid(), chunkId);
    if (retCode != CURVEFS_ERROR::OK) {
        return -1;
    }
    LOG(INFO) << "GetChunkId():chunkid:" << *chunkId;
    return 0;
}

int S3ClientAdaptorImpl::UpdateInodeS3Version(Inode *inode, uint64_t *version) {
    brpc::Channel channel;

    if (channel.Init(metaServerEps_.c_str(), NULL) != 0) {
        LOG(ERROR) << "Fail to init channel to meta Server"
        << " for update inode version: " << metaServerEps_;
        return -1;
    }

    brpc::Controller* cntl = new brpc::Controller();
    UpdateInodeS3VersionRequest request;
    UpdateInodeS3VersionResponse response;

    // TODO(huyao): add partiton
    request.set_poolid(0);
    request.set_copysetid(0);
    request.set_partitionid(0);
    request.set_inodeid(inode->inodeid());
    request.set_fsid(inode->fsid());
    curvefs::metaserver::MetaServerService_Stub stub(&channel);

    stub.UpdateInodeS3Version(cntl, &request, &response, NULL);

    if (cntl->Failed()) {
        LOG(WARNING) << "Update inode s3 version Failed, errorcode = "
                     << cntl->ErrorCode()
                     << ", error content:" << cntl->ErrorText()
                     << ", log id = " << cntl->log_id();
        int error = cntl->ErrorCode();
        delete cntl;
        cntl = NULL;
        return error;
    }

    curvefs::metaserver::MetaStatusCode ssCode = response.statuscode();
    if (ssCode != curvefs::metaserver::MetaStatusCode::OK) {
        LOG(WARNING) << "update inode s3 version response Failed, retCode = "
                     << ssCode;
        delete cntl;
        cntl = NULL;
        return -1;
    }

    *version = response.version();
    delete cntl;
    cntl = NULL;
    return 0;
}

CURVEFS_ERROR S3ClientAdaptorImpl::AllocS3ChunkId(uint32_t fsId,
                                              uint64_t *chunkId) {
    brpc::Channel channel;

    if (channel.Init(allocateServerEps_.c_str(), NULL) != 0) {
        LOG(ERROR) << "Fail to init channel to allocate Server"
        << " for alloc chunkId: " << allocateServerEps_;
        return CURVEFS_ERROR::INTERNAL;
    }
    brpc::Controller* cntl = new brpc::Controller();
    AllocateS3ChunkRequest request;
    AllocateS3ChunkResponse response;

    request.set_fsid(fsId);
    curvefs::space::SpaceAllocService_Stub stub(&channel);

    stub.AllocateS3Chunk(cntl, &request, &response, NULL);

    if (cntl->Failed()) {
        LOG(WARNING) << "Allocate s3 chunkid Failed, errorcode = "
                     << cntl->ErrorCode()
                     << ", error content:" << cntl->ErrorText()
                     << ", log id = " << cntl->log_id();
        CURVEFS_ERROR error = static_cast<CURVEFS_ERROR>(-cntl->ErrorCode());
        delete cntl;
        cntl = NULL;
        return error;
    }

    ::curvefs::space::SpaceStatusCode ssCode = response.status();
    if (ssCode != ::curvefs::space::SpaceStatusCode::SPACE_OK) {
        LOG(WARNING) << "Allocate s3 chunkid response Failed, retCode = "
                    << ssCode;
        delete cntl;
        cntl = NULL;
        return CURVEFS_ERROR::INTERNAL;
    }

    *chunkId = response.chunkid();
    delete cntl;
    cntl = NULL;
    return CURVEFS_ERROR::OK;
}

bool S3ClientAdaptorImpl::IsOverlap(Inode *inode, uint64_t offset,
                                uint64_t length) {
    const S3ChunkInfoList& s3ChunkInfoList = inode->s3chunkinfolist();
    for (int i = 0; i < s3ChunkInfoList.s3chunks_size(); ++i) {
        const S3ChunkInfo& tmp = s3ChunkInfoList.s3chunks(i);
        LOG(INFO) << "IsOverlap() offset:" << offset << ",len:"
                  << length << ".tmp offset:" << tmp.offset()
                  << ",tmp len:" << tmp.len();
        if ((offset < (tmp.offset() + tmp.len()))
             && (tmp.offset() < (offset + length))) {
            LOG(INFO) << "IsOverlap() return true";
            return true;
        }
    }

    return false;
}

bool S3ClientAdaptorImpl::IsAppendBlock(Inode *inode, uint64_t offset,
                                    uint64_t length) {
    const S3ChunkInfoList& s3ChunkInfoList = inode->s3chunkinfolist();
    assert(!IsOverlap(inode, offset, length));

    for (int i = 0; i < s3ChunkInfoList.s3chunks_size(); ++i) {
        const S3ChunkInfo& tmp = s3ChunkInfoList.s3chunks(i);
        if ((tmp.offset() + tmp.len() == offset)
             && ((tmp.offset() / blockSize_) == (offset / blockSize_))) {
            return true;
        }
    }
    return false;
}

bool S3ClientAdaptorImpl::IsDiscontinuityInBlock(Inode *inode, uint64_t offset,
                                             uint64_t length) {
    const S3ChunkInfoList& s3ChunkInfoList = inode->s3chunkinfolist();
    for (int i = 0; i < s3ChunkInfoList.s3chunks_size(); i++) {
        const S3ChunkInfo& tmp = s3ChunkInfoList.s3chunks(i);
        if (tmp.offset() + tmp.len() == offset) {
            return false;
        }
    }
    return true;
}

std::string S3ClientAdaptorImpl::GenerateObjectName(uint64_t chunkId,
                                                uint64_t blockIndex,
                                                uint64_t version) {
    std::ostringstream oss;
    oss << chunkId << "_" << blockIndex << "_" << version;
    return oss.str();
}

uint64_t S3ClientAdaptorImpl::WriteChunk(uint64_t chunkId, uint64_t version,
                                 uint64_t pos, uint64_t length,
                                 const char* buf, bool append) {
    uint64_t blockPos = pos % blockSize_;
    uint64_t blockIndex = pos / blockSize_;
    uint64_t n = 0;
    std::string objectName;
    int ret = 0;
    uint64_t writeOffset = 0;

    LOG(INFO) << "writechunk chunkid:" << chunkId << ",version:" << version
              << ",pos:" << pos << ",len:" << length << ",append:"  << append;
    while (length > 0) {
        if (blockPos + length > blockSize_) {
            n = blockSize_ - blockPos;
        } else {
            n = length;
        }
        objectName = GenerateObjectName(chunkId, blockIndex, version);
        if (append) {
            ret = client_->Append(objectName, buf + writeOffset, n);
            if (ret < 0) {
                LOG(ERROR) << "append object fail. object: " << objectName;
                return -1;
            }
            append = false;
        } else {
            ret = client_->Upload(objectName, buf + writeOffset, n);
            if (ret < 0) {
                LOG(ERROR) << "upload object fail. object: " << objectName;
                return -1;
            }
        }

        length -= n;
        blockIndex++;
        writeOffset += n;
        blockPos = (blockPos + n) % blockSize_;
    }
    return writeOffset;
}

std::vector<S3ChunkInfo> S3ClientAdaptorImpl::CutOverLapChunks(
       const S3ChunkInfo& newChunk, const S3ChunkInfo& old) {
    assert(newChunk.version() >= old.version());
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
            tmp.set_version(old.version());
            tmp.set_offset(old.offset());
            tmp.set_len(newChunk.offset() - old.offset());
            tmp.set_size(newChunk.offset() - old.offset());
            result.push_back(tmp);
        /*
             ----------     old
               ------       new
        */
        } else {
            tmp.set_chunkid(old.chunkid());
            tmp.set_version(old.version());
            tmp.set_offset(old.offset());
            tmp.set_len(newChunk.offset() - old.offset());
            tmp.set_size(newChunk.offset() - old.offset());
            result.push_back(tmp);
            tmp.set_chunkid(old.chunkid());
            tmp.set_version(old.version());
            tmp.set_offset(newChunk.offset() + newChunk.len());
            tmp.set_len(old.offset() + old.len()
            - newChunk.offset() - newChunk.len());
            tmp.set_size(old.offset() + old.len()
            - newChunk.offset() - newChunk.len());
            result.push_back(tmp);
        }
    /*
                  -----     old
               ----------   new
    */
    } else if (newChunk.offset() <= old.offset()
      && newChunk.offset() + newChunk.len() >= old.offset() + old.len()) {
        return result;
    /*
                  --------  old
               -------      new
    */
    } else {
        tmp.set_chunkid(old.chunkid());
        tmp.set_version(old.version());
        tmp.set_offset(newChunk.offset()+ newChunk.len());
        tmp.set_len(old.offset() + old.len()
        - newChunk.offset() - newChunk.len());
        tmp.set_size(old.offset() + old.len()
        - newChunk.offset() - newChunk.len());
        result.push_back(tmp);
    }

    return result;
}

std::vector<S3ChunkInfo> S3ClientAdaptorImpl::GetReadChunks(Inode *inode) {
    S3ChunkInfo tmp, chunkTmp;
    std::vector<S3ChunkInfo> chunks;
    const S3ChunkInfoList& s3ChunkInfoList = inode->s3chunkinfolist();

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

        chunks.insert(chunks.end(), addChunks.begin(), addChunks.end());
        chunks.push_back(tmp);
    }

    return chunks;
}

std::vector<S3ChunkInfo> S3ClientAdaptorImpl::SortByOffset(
                             std::vector<S3ChunkInfo> chunks) {
    /*
    int i, j;
    int len = chunks.size();
    S3ChunkInfo tmp;
    for (i = 0; i < len -1 ; i++) {
        for (j = 0; j < len - 1 - i; j++) {
            if (chunks[j].offset() > chunks[j+1].offset()) {
                tmp = chunks[j];
                chunks[j] = chunks[j+1];
                chunks[j+1] = tmp;
            }
        }
    }*/
    std::sort(chunks.begin(), chunks.end(), [] (S3ChunkInfo a, S3ChunkInfo b) {
        return a.offset() < b.offset();
    });
    return chunks;
}

int S3ClientAdaptorImpl::Read(Inode *inode, uint64_t offset,
              uint64_t length, char* buf) {
    std::vector<S3ChunkInfo> sortChunks;
    std::vector<S3ChunkInfo> chunks = GetReadChunks(inode);
    sortChunks = SortByOffset(chunks);

    uint32_t i = 0;
    S3ChunkInfo tmp;
    uint64_t readOffset = 0;
    std::vector<S3ReadRequest> requests;
    LOG(INFO) << "read start offset:" << offset
              << ",len:" << length << ",chunksize:" << sortChunks.size();
    for (unsigned j = 0; j < sortChunks.size(); j++) {
        S3ChunkInfo tmp1 = sortChunks[j];
        LOG(INFO) << "sort chunk info chunkId:" << tmp1.chunkid()
                  << ",version:" << tmp1.version()
                  << ",offset:" << tmp1.offset()
                  << ",len:" << tmp1.len();
    }
    while (length > 0) {
        S3ReadRequest request;
        if (i == sortChunks.size()) {
            memset(static_cast<char *>(buf) + readOffset, 0, length);
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
        } else if ((tmp.offset() >= offset)
           && (tmp.offset() < offset + length)) {
            int n = tmp.offset() - offset;
            memset(static_cast<char *>(buf) + readOffset, 0, n);
            offset = tmp.offset();
            readOffset += n;
            length -= n;

            if (offset + length <= tmp.offset() + tmp.len()) {
                request.SetS3ChunkInfo(tmp);
                request.GetS3ChunkInfo().set_offset(offset);
                request.GetS3ChunkInfo().set_len(length);
                request.SetReadOffset(readOffset);
                readOffset += length;
                length = 0;
            } else {
                request.SetS3ChunkInfo(tmp);
                request.SetReadOffset(readOffset);
                readOffset +=tmp.len();
                length -= tmp.len();
                offset += tmp.len();
            }
            requests.push_back(request);
        /*
                 ----                      ---------   read block
               ----------                --------      S3ChunkInfo
        */
        } else if ((tmp.offset() < offset)
          && (tmp.offset() + tmp.len() > offset)) {
            if (offset + length <= tmp.offset() + tmp.len()) {
                request.SetS3ChunkInfo(tmp);
                request.GetS3ChunkInfo().set_offset(offset);
                request.GetS3ChunkInfo().set_len(length);
                request.SetReadOffset(readOffset);
                readOffset += length;
                length = 0;
            } else {
                request.SetS3ChunkInfo(tmp);
                request.GetS3ChunkInfo().set_offset(offset);
                request.GetS3ChunkInfo().set_len(tmp.offset()
                                       + tmp.len() - offset);
                request.SetReadOffset(readOffset);
                offset = tmp.offset() + tmp.len();
                length -= request.GetS3ChunkInfo().len();
                readOffset += request.GetS3ChunkInfo().len();
            }
            requests.push_back(request);
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

    std::vector<S3ReadResponse> responses;
    int ret = 0;
    ret = handleReadRequest(requests, &responses);
    if (ret < 0) {
        LOG(ERROR) << "handle read request fail:" << ret;
        return ret;
    }

    std::vector<S3ReadResponse>::iterator iter = responses.begin();
    for (; iter != responses.end(); iter++) {
        LOG(INFO) << "readOffset:" << iter->GetReadOffset()
                  << ",bufLen:" << iter->GetBufLen();
        strncpy(buf + iter->GetReadOffset(), iter->GetDataBuf(),
                iter->GetBufLen());
    }

    LOG(INFO) << "read over read offset:" << readOffset;
    return readOffset;
}

int S3ClientAdaptorImpl::handleReadRequest(
                        const std::vector<S3ReadRequest>& requests,
                        std::vector<S3ReadResponse>* responses) {
    (*responses).reserve(requests.size());
    std::vector<S3ReadRequest>::const_iterator iter = requests.begin();
    for (; iter != requests.end(); iter++) {
        uint64_t blockIndex =
            iter->GetS3ChunkInfo().offset() % chunkSize_ / blockSize_;
        uint64_t blockPos = iter->GetS3ChunkInfo().offset()
                            % chunkSize_ % blockSize_;
        uint64_t len = iter->GetS3ChunkInfo().len();
        uint64_t n = 0;
        uint64_t readOffset = 0;
        (*responses).emplace_back(len);
        S3ReadResponse& response = responses->back();
        while (len > 0) {
            if (blockPos + len > blockSize_) {
                n = blockSize_ - blockPos;
            } else {
                n = len;
            }

            std::string name = GenerateObjectName(
                iter->GetS3ChunkInfo().chunkid(),
                blockIndex, iter->GetS3ChunkInfo().version());
            int readLen = client_->Download(name,
            response.GetDataBuf() + readOffset, blockPos, n);
            if (readLen < 0) {
                LOG(ERROR) << "download name:" << name <<" offset:"
                << iter->GetS3ChunkInfo().offset() << " len:"
                << iter->GetS3ChunkInfo().len() << "fail:" << readLen;
                return readLen;
            }

            len -= readLen;
            readOffset += readLen;
            blockIndex++;
            blockPos = (blockPos + n) % blockSize_;
        }

        response.SetReadOffset(iter->GetReadOffset());
        response.SetBufLen(readOffset);
        LOG(INFO) << "response readOffset:" << response.GetReadOffset()
                  << ",bufLen:"<< readOffset;
    }

    return 0;
}

int S3ClientAdaptorImpl::Truncate(Inode *inode, uint64_t length) {
    return 0;
    // Todo: huyao
}

}  // namespace client
}  // namespace curvefs
