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
 * File Created: Monday, 17th September 2018 4:20:58 pm
 * Author: tongguangxun
 */

#include "src/client/splitor.h"

#include <glog/logging.h>

#include <algorithm>
#include <string>
#include <vector>

#include "src/client/file_instance.h"
#include "src/client/mds_client.h"
#include "src/client/metacache_struct.h"
#include "src/client/request_closure.h"
#include "src/common/location_operator.h"

namespace curve {
namespace client {

IOSplitOption Splitor::iosplitopt_;

void Splitor::Init(const IOSplitOption& ioSplitOpt) {
    iosplitopt_ = ioSplitOpt;
    LOG(INFO) << "io splitor init success!";
}

int Splitor::IO2ChunkRequests(IOTracker* iotracker, MetaCache* metaCache,
                              std::vector<RequestContext*>* targetlist,
                              butil::IOBuf* data, off_t offset, size_t length,
                              MDSClient* mdsclient, const FInfo_t* fileInfo) {
    if (targetlist == nullptr || mdsclient == nullptr || metaCache == nullptr ||
        iotracker == nullptr || fileInfo == nullptr) {
        return -1;
    }

    if (iotracker->Optype() == OpType::WRITE && data == nullptr) {
        return -1;
    }

    targetlist->reserve(length / (iosplitopt_.fileIOSplitMaxSizeKB * 1024) + 1);

    const uint64_t chunksize = fileInfo->chunksize;
    uint64_t currentChunkIndex = offset / chunksize;
    const uint64_t endChunkIndex = (offset + length - 1) / chunksize;
    uint64_t currentRequestOffset = offset;
    const uint64_t endRequestOffest = offset + length;
    uint64_t currentChunkOffset = offset % chunksize;
    uint64_t dataOffset = 0;

    while (currentChunkIndex <= endChunkIndex) {
        const uint64_t currentChunkEndOffset =
            chunksize * (currentChunkIndex + 1);
        uint64_t requestLength =
            std::min(currentChunkEndOffset, endRequestOffest) -
            currentRequestOffset;

        DVLOG(9) << "request split"
                 << ", off = " << currentChunkOffset
                 << ", len = " << requestLength
                 << ", seqnum = " << fileInfo->seqnum
                 << ", endoff = " << endRequestOffest
                 << ", chunkendpos = " << currentChunkEndOffset
                 << ", chunksize = " << chunksize
                 << ", chunkindex = " << currentChunkIndex
                 << ", endchunkindex = " << endChunkIndex;

        if (!AssignInternal(iotracker, metaCache, targetlist, data,
                            currentChunkOffset, requestLength, mdsclient,
                            fileInfo, currentChunkIndex)) {
            LOG(ERROR)  << "request split failed"
                        << ", off = " << currentChunkOffset
                        << ", len = " << requestLength
                        << ", seqnum = " << fileInfo->seqnum
                        << ", endoff = " << endRequestOffest
                        << ", chunkendpos = " << currentChunkEndOffset
                        << ", chunksize = " << chunksize
                        << ", chunkindex = " << currentChunkIndex
                        << ", endchunkindex = " << endChunkIndex;
            return -1;
        }

        currentChunkOffset = 0;
        currentChunkIndex++;

        dataOffset += requestLength;
        currentRequestOffset += requestLength;
    }

    return 0;
}

// this offset is begin by chunk
int Splitor::SingleChunkIO2ChunkRequests(
    IOTracker* iotracker, MetaCache* metaCache,
    std::vector<RequestContext*>* targetlist, const ChunkIDInfo& idinfo,
    butil::IOBuf* data, off_t offset, uint64_t length, uint64_t seq) {
    if (targetlist == nullptr || metaCache == nullptr || iotracker == nullptr) {
        return -1;
    }

    if (iotracker->Optype() == OpType::WRITE && data == nullptr) {
        return -1;
    }

    const auto maxSplitSizeBytes = 1024 * iosplitopt_.fileIOSplitMaxSizeKB;

    uint64_t dataOffset = 0;
    uint64_t currentOffset = offset;
    uint64_t leftLength = length;
    while (leftLength > 0) {
        uint64_t requestLength = std::min(leftLength, maxSplitSizeBytes);

        RequestContext* newreqNode = RequestContext::NewInitedRequestContext();
        if (newreqNode == nullptr) {
            return -1;
        }

        if (iotracker->Optype() == OpType::WRITE) {
            auto nc = data->cutn(&(newreqNode->writeData_), requestLength);
            if (nc != requestLength) {
                LOG(ERROR) << "IOBuf::cutn failed, expected: " << requestLength
                           << ", return: " << nc;
                return -1;
            }
        }

        newreqNode->seq_         = seq;
        newreqNode->offset_      = currentOffset;
        newreqNode->rawlength_   = requestLength;
        newreqNode->optype_      = iotracker->Optype();
        newreqNode->idinfo_      = idinfo;
        newreqNode->done_->SetIOTracker(iotracker);
        targetlist->push_back(newreqNode);

        DVLOG(9) << "request split"
                 << ", off = " << currentOffset
                 << ", len = " << requestLength
                 << ", seqnum = " << seq
                 << ", chunkid = " << idinfo.cid_
                 << ", copysetid = " << idinfo.cpid_
                 << ", logicpoolid = " << idinfo.lpid_;

        leftLength -= requestLength;
        dataOffset += requestLength;
        currentOffset += requestLength;
    }

    return 0;
}

bool Splitor::AssignInternal(IOTracker* iotracker, MetaCache* metaCache,
                             std::vector<RequestContext*>* targetlist,
                             butil::IOBuf* data, off_t off, size_t len,
                             MDSClient* mdsclient, const FInfo_t* fileinfo,
                             ChunkIndex chunkidx) {
    const auto maxSplitSizeBytes = 1024 * iosplitopt_.fileIOSplitMaxSizeKB;

    ChunkIDInfo chunkIdInfo;
    MetaCacheErrorType errCode =
        metaCache->GetChunkInfoByIndex(chunkidx, &chunkIdInfo);


    if (errCode == MetaCacheErrorType::CHUNKINFO_NOT_FOUND ||
       (errCode == MetaCacheErrorType::OK && !chunkIdInfo.chunkExist &&
       iotracker->Optype() == OpType::WRITE)) {
        bool isAllocateSegment =
            iotracker->Optype() == OpType::READ ? false : true;
        if (false == GetOrAllocateSegment(
                         isAllocateSegment,
                         static_cast<uint64_t>(chunkidx) * fileinfo->chunksize,
                         mdsclient, metaCache, fileinfo, chunkidx)) {
            return false;
        }

        errCode = metaCache->GetChunkInfoByIndex(chunkidx, &chunkIdInfo);
    }

    if (errCode == MetaCacheErrorType::OK) {
        int ret = 0;
        uint64_t appliedindex_ = 0;

        // only read needs applied-index
        if (iotracker->Optype() == OpType::READ) {
            appliedindex_ = metaCache->GetAppliedIndex(chunkIdInfo.lpid_,
                                                       chunkIdInfo.cpid_);
        }

        std::vector<RequestContext*> templist;
        ret = SingleChunkIO2ChunkRequests(iotracker, metaCache, &templist,
                                          chunkIdInfo, data, off, len,
                                          fileinfo->seqnum);

        for (auto& ctx : templist) {
            ctx->appliedindex_ = appliedindex_;
            ctx->sourceInfo_ =
                CalcRequestSourceInfo(iotracker, metaCache, chunkidx);
        }

        targetlist->insert(targetlist->end(), templist.begin(),
                            templist.end());

        return ret == 0;
    }

    LOG(ERROR) << "can not find the chunk index info!"
                << ", chunk index = " << chunkidx;

    return false;
}

bool Splitor::GetOrAllocateSegment(bool allocateIfNotExist,
                                   uint64_t offset,
                                   MDSClient* mdsClient,
                                   MetaCache* metaCache,
                                   const FInfo* fileInfo,
                                   ChunkIndex chunkidx) {
    SegmentInfo segmentInfo;
    LIBCURVE_ERROR errCode = mdsClient->GetOrAllocateSegment(
        allocateIfNotExist, offset, fileInfo, &segmentInfo);

    if (errCode == LIBCURVE_ERROR::FAILED ||
        errCode == LIBCURVE_ERROR::AUTHFAIL) {
        LOG(ERROR) << "GetOrAllocateSegmen failed, filename: "
                   << fileInfo->filename << ", offset: " << offset;
        return false;
    } else if (errCode == LIBCURVE_ERROR::NOT_ALLOCATE) {
        // this chunkIdInfo(0, 0, 0) identify the unallocated chunk when read
        ChunkIDInfo chunkIdInfo(0, 0, 0);
        chunkIdInfo.chunkExist = false;
        metaCache->UpdateChunkInfoByIndex(chunkidx, chunkIdInfo);
        return true;
    }

    const auto chunksize = fileInfo->chunksize;
    uint32_t count = 0;
    for (const auto& chunkIdInfo : segmentInfo.chunkvec) {
        uint64_t chunkIdx =
            (segmentInfo.startoffset + count * chunksize) / chunksize;
        metaCache->UpdateChunkInfoByIndex(chunkIdx, chunkIdInfo);
        ++count;
    }

    std::vector<CopysetInfo> copysetInfos;
    errCode = mdsClient->GetServerList(segmentInfo.lpcpIDInfo.lpid,
                                       segmentInfo.lpcpIDInfo.cpidVec,
                                       &copysetInfos);

    if (errCode == LIBCURVE_ERROR::FAILED) {
        std::string failedCopysets;
        for (const auto& id : segmentInfo.lpcpIDInfo.cpidVec) {
            failedCopysets.append(std::to_string(id)).append(",");
        }

        LOG(ERROR) << "GetServerList failed, logicpool id: "
                   << segmentInfo.lpcpIDInfo.lpid
                   << ", copysets: " << failedCopysets;

        return false;
    }

    for (const auto& copysetInfo : copysetInfos) {
        for (const auto& peerInfo : copysetInfo.csinfos_) {
            metaCache->AddCopysetIDInfo(
                peerInfo.chunkserverID,
                CopysetIDInfo(segmentInfo.lpcpIDInfo.lpid, copysetInfo.cpid_));
        }
    }

    for (const auto& copysetInfo : copysetInfos) {
        metaCache->UpdateCopysetInfo(segmentInfo.lpcpIDInfo.lpid,
                                     copysetInfo.cpid_, copysetInfo);
    }

    return true;
}

RequestSourceInfo Splitor::CalcRequestSourceInfo(IOTracker* ioTracker,
                                                 MetaCache* metaCache,
                                                 ChunkIndex chunkIdx) {
    const FInfo* fileInfo = metaCache->GetFileInfo();
    if (fileInfo->cloneSource.empty()) {
        return {};
    }

    OpType type = ioTracker->Optype();
    if (type != OpType::READ && type != OpType::WRITE) {
        return {};
    }

    uint64_t offset = static_cast<uint64_t>(chunkIdx) * fileInfo->chunksize;

    if (offset >= fileInfo->cloneLength) {
        return {};
    }

    return {fileInfo->cloneSource, offset};
}

}   // namespace client
}   // namespace curve
