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
 * Created Date: 2021-8-13
 * Author: chengyi
 */

#include "curvefs/src/metaserver/s3/metaserver_s3_adaptor.h"
#include <list>
#include <algorithm>
#include "curvefs/src/common/s3util.h"

namespace curvefs {
namespace metaserver {
void S3ClientAdaptorImpl::Init(const S3ClientAdaptorOption &option,
                               S3Client *client) {
    blockSize_ = option.blockSize;
    chunkSize_ = option.chunkSize;
    batchSize_ = option.batchSize;
    enableDeleteObjects_ = option.enableDeleteObjects;
    objectPrefix_ = option.objectPrefix;
    client_ = client;
}

void S3ClientAdaptorImpl::Reinit(const S3ClientAdaptorOption& option,
        const std::string& ak, const std::string& sk,
        const std::string& endpoint, const std::string& bucketName) {
    blockSize_ = option.blockSize;
    chunkSize_ = option.chunkSize;
    batchSize_ = option.batchSize;
    enableDeleteObjects_ = option.enableDeleteObjects;
    objectPrefix_ = option.objectPrefix;
    client_->Reinit(ak, sk, endpoint, bucketName);
}

int S3ClientAdaptorImpl::Delete(const Inode &inode) {
    if (enableDeleteObjects_) {
        return DeleteInodeByDeleteBatchChunk(inode);
    } else {
        return DeleteInodeByDeleteSingleChunk(inode);
    }
}

int S3ClientAdaptorImpl::DeleteInodeByDeleteSingleChunk(const Inode &inode) {
    // const S3ChunkInfoList& s3ChunkInfolist = inode.s3chunkinfolist();
    auto s3ChunkInfoMap = inode.s3chunkinfomap();
    LOG(INFO) << "delete data, inode id: " << inode.inodeid()
              << ", len:" << inode.length();
    int ret = 0;
    auto iter = s3ChunkInfoMap.begin();
    for (; iter != s3ChunkInfoMap.end(); iter++) {
        S3ChunkInfoList &s3ChunkInfolist = iter->second;
        for (int i = 0; i < s3ChunkInfolist.s3chunks_size(); ++i) {
            // traverse chunks to delete blocks
            S3ChunkInfo chunkInfo = s3ChunkInfolist.s3chunks(i);
            // delete chunkInfo from client
            uint64_t fsId = inode.fsid();
            uint64_t inodeId = inode.inodeid();
            uint64_t chunkId = chunkInfo.chunkid();
            uint64_t compaction = chunkInfo.compaction();
            uint64_t chunkPos = chunkInfo.offset() % chunkSize_;
            uint64_t length = chunkInfo.len();
            int delStat = DeleteChunk(fsId, inodeId, chunkId, compaction,
                                      chunkPos, length);
            if (delStat < 0) {
                LOG(ERROR) << "delete chunk failed, status code is: " << delStat
                           << " , chunkId is " << chunkId;
                ret = -1;
            }
        }
    }
    LOG(INFO) << "delete data, inode id: " << inode.inodeid()
              << ", len:" << inode.length() << " success";

    return ret;
}

int S3ClientAdaptorImpl::DeleteChunk(uint64_t fsId, uint64_t inodeId,
                                     uint64_t chunkId, uint64_t compaction,
                                     uint64_t chunkPos, uint64_t length) {
    uint64_t blockIndex = chunkPos / blockSize_;
    uint64_t blockPos = chunkPos % blockSize_;
    int count = 0;  // blocks' number
    int ret = 0;
    while (length > blockSize_ * count - blockPos || count == 0) {
        // divide chunks to blocks, and delete these blocks
        std::string objectName = curvefs::common::s3util::GenObjName(
            chunkId, blockIndex, compaction, fsId, inodeId, objectPrefix_);
        int delStat = client_->Delete(objectName);
        if (delStat < 0) {
            // fail
            LOG(ERROR) << "delete object fail. object: " << objectName;
            ret = -1;
        } else if (delStat > 0) {  // delSat == 1
            // object is not exist
            // 1. overwrite，the object is delete by others
            // 2. last delete failed
            // 3. others
            ret = 1;
        }

        ++blockIndex;
        ++count;
    }

    return ret;
}

int S3ClientAdaptorImpl::DeleteInodeByDeleteBatchChunk(const Inode &inode) {
    auto s3ChunkInfoMap = inode.s3chunkinfomap();
    LOG(INFO) << "delete data, inode id: " << inode.inodeid()
              << ", len:" << inode.length();
    int returnCode = 0;
    auto iter = s3ChunkInfoMap.begin();
    while (iter != s3ChunkInfoMap.end()) {
        int ret =
            DeleteS3ChunkInfoList(inode.fsid(), inode.inodeid(), iter->second);
        if (ret != 0) {
            LOG(ERROR) << "delete chunk failed, ret = " << ret
                       << " , chunk index is " << iter->first;
            returnCode = -1;
            iter++;
        } else {
            iter = s3ChunkInfoMap.erase(iter);
        }
    }
    LOG(INFO) << "delete data, inode id: " << inode.inodeid()
              << ", len:" << inode.length() << " , ret = " << returnCode;

    return returnCode;
}

int S3ClientAdaptorImpl::DeleteS3ChunkInfoList(
    uint32_t fsId, uint64_t inodeId, const S3ChunkInfoList &s3ChunkInfolist) {
    std::list<std::string> objList;

    GenObjNameListForChunkInfoList(fsId, inodeId, s3ChunkInfolist, &objList);

    while (objList.size() != 0) {
        std::list<std::string> tempObjList;
        auto begin = objList.begin();
        auto end = objList.begin();
        std::advance(end, std::min(batchSize_, objList.size()));
        tempObjList.splice(tempObjList.begin(), objList, begin, end);
        int ret = client_->DeleteBatch(tempObjList);
        if (ret != 0) {
            LOG(ERROR) << "DeleteS3ChunkInfoList failed, fsId = " << fsId
                       << ", inodeId =  " << inodeId
                       << ", status code = " << ret;
            return -1;
        }
    }

    return 0;
}

void S3ClientAdaptorImpl::GenObjNameListForChunkInfoList(
    uint32_t fsId, uint64_t inodeId, const S3ChunkInfoList &s3ChunkInfolist,
    std::list<std::string> *objList) {
    for (int i = 0; i < s3ChunkInfolist.s3chunks_size(); ++i) {
        S3ChunkInfo chunkInfo = s3ChunkInfolist.s3chunks(i);
        std::list<std::string> tempObjList;
        GenObjNameListForChunkInfo(fsId, inodeId, chunkInfo, &tempObjList);

        objList->splice(objList->end(), tempObjList);
    }
    return;
}

void S3ClientAdaptorImpl::GenObjNameListForChunkInfo(
    uint32_t fsId, uint64_t inodeId, const S3ChunkInfo &chunkInfo,
    std::list<std::string> *objList) {
    uint64_t chunkId = chunkInfo.chunkid();
    uint64_t compaction = chunkInfo.compaction();
    uint64_t chunkPos = chunkInfo.offset() % chunkSize_;
    uint64_t length = chunkInfo.len();
    uint64_t blockIndex = chunkPos / blockSize_;
    uint64_t blockPos = chunkPos % blockSize_;
    VLOG(9) << "delete Chunk start, fsId = "<< fsId << ", inodeId = " << inodeId
            << ", chunk id: " << chunkId
            << ", compaction:" << compaction << ", chunkPos: " << chunkPos
            << ", length: " << length;
    int count = (length + blockPos + blockSize_ - 1) / blockSize_;
    for (int i = 0; i < count; i++) {
        // divide chunks to blocks, and delete these blocks
        std::string objectName = curvefs::common::s3util::GenObjName(
            chunkId, blockIndex, compaction, fsId, inodeId, objectPrefix_);
        objList->push_back(objectName);

        ++blockIndex;
    }
    return;
}

void S3ClientAdaptorImpl::GetS3ClientAdaptorOption(
    S3ClientAdaptorOption *option) {
    option->blockSize = blockSize_;
    option->chunkSize = chunkSize_;
    option->batchSize = batchSize_;
    option->enableDeleteObjects = enableDeleteObjects_;
    option->objectPrefix = objectPrefix_;
}

}  // namespace metaserver
}  // namespace curvefs
