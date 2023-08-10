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
#include "src/common/fast_align.h"
#include "src/client/global_metacache.h"
#include "src/common//namespace_define.h"

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
                              MDSClient* mdsclient, const FInfo_t* fileInfo,
                              const FileEpoch_t* fEpoch) {
    if (targetlist == nullptr || mdsclient == nullptr || metaCache == nullptr ||
        iotracker == nullptr || fileInfo == nullptr) {
        return -1;
    }

    if (iotracker->Optype() == OpType::WRITE && data == nullptr) {
        return -1;
    }

    targetlist->reserve(length / (iosplitopt_.fileIOSplitMaxSizeKB * 1024) + 1);

    if (((fileInfo->stripeUnit == 0) && (fileInfo->stripeCount == 0)) ||
        fileInfo->stripeCount == 1 || iotracker->IsStripeDisabled()) {
        return SplitForNormal(iotracker, metaCache, targetlist, data, offset,
                              length, mdsclient, fileInfo, fEpoch);
    } else {
        return SplitForStripe(iotracker, metaCache, targetlist, data, offset,
                              length, mdsclient, fileInfo, fEpoch);
    }
}

// this offset is begin by chunk
int Splitor::SingleChunkIO2ChunkRequests(
    IOTracker* iotracker, MetaCache* metaCache,
    std::vector<RequestContext*>* targetlist, const ChunkIDInfo& idinfo,
    butil::IOBuf* data, off_t offset, uint64_t length, uint64_t seq, const std::vector<uint64_t>& snaps) {
    if (targetlist == nullptr || metaCache == nullptr || iotracker == nullptr) {
        return -1;
    }

    if (iotracker->Optype() == OpType::WRITE && data == nullptr) {
        return -1;
    }

    if (iotracker->Optype() == OpType::READ_SNAP) {
        auto it = std::find(snaps.begin(), snaps.end(), seq);
        if (it == snaps.end() ) {
            LOG(ERROR) << "Invalid READ_SNAP request, snap sn = " << seq
                       << ", not contained in snaps [" << Snaps2Str(snaps) << "]";
            return -1;
        }
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
        newreqNode->snaps_       = snaps;
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
                 << ", snaps = [" << Snaps2Str(newreqNode->snaps_) << "]"
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
                             MDSClient* mdsclient, const FInfo_t* fileInfo,
                             const FileEpoch_t* fEpoch,
                             ChunkIndex chunkidx) {
    const auto maxSplitSizeBytes = 1024 * iosplitopt_.fileIOSplitMaxSizeKB;

    ChunkIDInfo chunkIdInfo;
    MetaCacheErrorType errCode =
        metaCache->GetChunkInfoByIndex(chunkidx, &chunkIdInfo);

    if (NeedGetOrAllocateSegment(errCode, iotracker->Optype(), chunkIdInfo,
                                 metaCache)) {
        bool isAllocateSegment =
           (iotracker->Optype() == OpType::READ || iotracker->Optype() == OpType::READ_SNAP) ? false : true;
        if (false == GetOrAllocateSegment(
                         isAllocateSegment,
                         static_cast<uint64_t>(chunkidx) * fileInfo->chunksize,
                         mdsclient, metaCache, fileInfo, fEpoch, chunkidx)) {
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
                                          (iotracker->Optype() == OpType::READ_SNAP? fileInfo->snapSeqnum : fileInfo->seqnum), fileInfo->snaps);
        if (ret != 0) {
            LOG(ERROR) << "SingleChunkIO2ChunkRequests return ret: " << ret;
            return false;
        }

        for (auto& ctx : templist) {
            ctx->filetype_ = fileInfo->filetype;
            ctx->fileId_ = fileInfo->id;
            if (fEpoch != nullptr) {
                ctx->epoch_ = fEpoch->epoch;
            } else {
                ctx->epoch_ = 0;
            }
            ctx->appliedindex_ = appliedindex_;
            ctx->sourceInfo_ =
                CalcRequestSourceInfo(iotracker, metaCache, chunkidx);
        }
        if (fileInfo->filetype == FileType::INODE_CLONE_PAGEFILE &&
            !chunkIdInfo.cloneOrigin_.empty()) {
            ret = AssignCloneFileInfo(
                iotracker, &templist, mdsclient, 
                fileInfo, chunkidx, chunkIdInfo);
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
                                   const FileEpoch_t *fEpoch,
                                   ChunkIndex chunkidx) {
    SegmentInfo segmentInfo;
    LIBCURVE_ERROR errCode = mdsClient->GetOrAllocateSegment(
        allocateIfNotExist, offset, fileInfo, fEpoch, &segmentInfo);

    if (errCode != LIBCURVE_ERROR::OK) {
        if (errCode == LIBCURVE_ERROR::NOT_ALLOCATE) {
            // this chunkIdInfo(0, 0, 0) identify
            // the unallocated chunk when read
            ChunkIDInfo chunkIdInfo(0, 0, 0);
            chunkIdInfo.chunkExist = false;
            metaCache->UpdateChunkInfoByIndex(chunkidx, chunkIdInfo);
            return true;
        }
        if (errCode == LIBCURVE_ERROR::EPOCH_TOO_OLD) {
            LOG(WARNING) << "GetOrAllocateSegmen epoch too old, filename: "
                         << fileInfo->filename << ", offset: " << offset;
            return false;
        } else {
            LOG(ERROR) << "GetOrAllocateSegmen failed, filename: "
                       << fileInfo->filename << ", offset: " << offset;
            return false;
        }
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

int Splitor::SplitForNormal(IOTracker* iotracker, MetaCache* metaCache,
                            std::vector<RequestContext*>* targetlist,
                            butil::IOBuf* data, off_t offset, size_t length,
                            MDSClient* mdsclient, const FInfo_t* fileInfo,
                            const FileEpoch_t* fEpoch) {
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
                 << ", snaps = [" << Snaps2Str(fileInfo->snaps) << "]"
                 << ", endoff = " << endRequestOffest
                 << ", chunkendpos = " << currentChunkEndOffset
                 << ", chunksize = " << chunksize
                 << ", chunkindex = " << currentChunkIndex
                 << ", endchunkindex = " << endChunkIndex;

        if (!AssignInternal(iotracker, metaCache, targetlist, data,
                            currentChunkOffset, requestLength, mdsclient,
                            fileInfo, fEpoch, currentChunkIndex)) {
            LOG(ERROR) << "request split failed"
                       << ", off = " << currentChunkOffset
                       << ", len = " << requestLength
                       << ", seqnum = " << fileInfo->seqnum
                       << ", snaps = [" << Snaps2Str(fileInfo->snaps) << "]"
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

int Splitor::SplitForStripe(IOTracker* iotracker, MetaCache* metaCache,
                            std::vector<RequestContext*>* targetlist,
                            butil::IOBuf* data, off_t offset, size_t length,
                            MDSClient* mdsclient, const FInfo_t* fileInfo,
                            const FileEpoch_t* fEpoch) {
    const uint64_t chunksize = fileInfo->chunksize;
    const uint64_t stripeUnit = fileInfo->stripeUnit;
    const uint64_t stripeCount = fileInfo->stripeCount;
    const uint64_t stripesPerChunk = chunksize / stripeUnit;

    uint64_t cur = offset;
    uint64_t left = length;
    uint64_t curChunkIndex = 0;

    while (left > 0) {
        uint64_t blockIndex = cur / stripeUnit;
        uint64_t stripeIndex = blockIndex / stripeCount;
        uint64_t stripepos = blockIndex % stripeCount;
        uint64_t curChunkSetIndex = stripeIndex / stripesPerChunk;
        uint64_t curChunkIndex = curChunkSetIndex * stripeCount + stripepos;

        uint64_t blockInChunkStartOff =
            (stripeIndex % stripesPerChunk) * stripeUnit;
        uint64_t blockOff = cur % stripeUnit;
        uint64_t curChunkOffset = blockInChunkStartOff + blockOff;
        uint64_t requestLength = std::min((stripeUnit - blockOff), left);
        DVLOG(9) << "request splitForStripe"
                 << ", off = " << curChunkOffset
                 << ", len = " << requestLength
                 << ", seqnum = " << fileInfo->seqnum
                 << ", snaps = [" << Snaps2Str(fileInfo->snaps) << "]"
                 << ", stripeUnit = " << stripeUnit
                 << ", stripeCount = " << stripeCount
                 << ", chunksize = " << chunksize
                 << ", chunkindex = " << curChunkIndex;

        if (!AssignInternal(iotracker, metaCache, targetlist, data,
                            curChunkOffset, requestLength, mdsclient,
                            fileInfo, fEpoch, curChunkIndex)) {
            LOG(ERROR) << "request split failed"
                       << ", off = " << curChunkOffset
                       << ", len = " << requestLength
                       << ", seqnum = " << fileInfo->seqnum
                       << ", snaps = [" << Snaps2Str(fileInfo->snaps) << "]"
                       << ", chunksize = " << chunksize
                       << ", chunkindex = " << curChunkIndex;

            return -1;
        }

        left -= requestLength;
        cur += requestLength;
    }

    return 0;
}

RequestSourceInfo Splitor::CalcRequestSourceInfo(IOTracker* ioTracker,
                                                 MetaCache* metaCache,
                                                 ChunkIndex chunkIdx) {
    OpType type = ioTracker->Optype();
    if (type != OpType::READ && type != OpType::WRITE) {
        return {};
    }

    const FInfo* fileInfo = metaCache->GetFileInfo();
    if (fileInfo->filestatus == FileStatus::CloneMetaInstalled) {
        const CloneSourceInfo& sourceInfo = fileInfo->sourceInfo;
        uint64_t offset = static_cast<uint64_t>(chunkIdx) * fileInfo->chunksize;
        if (sourceInfo.IsSegmentAllocated(offset)) {
            return {sourceInfo.name, offset};
        }
    }

    return {};
}

bool Splitor::NeedGetOrAllocateSegment(MetaCacheErrorType error, OpType opType,
                                       const ChunkIDInfo& chunkInfo,
                                       const MetaCache* metaCache) {
    if (error == MetaCacheErrorType::CHUNKINFO_NOT_FOUND) {
        return true;
    } else if (error == MetaCacheErrorType::OK && !chunkInfo.chunkExist) {
        return (opType == OpType::WRITE) ||
               (opType == OpType::READ &&
                !metaCache->GetFileInfo()->openflags.exclusive);
    }

    return false;
}

int Splitor::AssignCloneFileInfo(IOTracker* iotracker,
    std::vector<RequestContext*>* targetlist,
    MDSClient* mdsclient,
    const FInfo_t* fileInfo,
    ChunkIndex chunkidx,
    const ChunkIDInfo &chunkIdInfo) {
    MetaCache* cloneOriginCache = GlobalMetaCache::GetInstance().
        GetOrNewMetaCacheInstance(chunkIdInfo.cloneOrigin_,
            fileInfo->userinfo,
            mdsclient);
    if (cloneOriginCache == nullptr) {
        // clone origin may be deleted
        ChunkIDInfo tmp(0, 0, 0);
        tmp.chunkExist = false;
        for (auto& ctx : *targetlist) {
            ctx->cfinfo_ = fileInfo->cfinfo;
            ctx->chunkIndex_ = chunkidx;
            ctx->originChunkIdInfo_ = tmp;
        }
        return 0;
    }
    ChunkIDInfo originChunkIdInfo;
    MetaCacheErrorType errCode =
        cloneOriginCache->GetChunkInfoByIndex(chunkidx, &originChunkIdInfo);

    if (NeedGetOrAllocateSegment(errCode, OpType::READ, originChunkIdInfo,
                                 cloneOriginCache)) {
        if (false == GetOrAllocateSegment(
                         false,
                         static_cast<uint64_t>(chunkidx) * fileInfo->chunksize,
                         mdsclient, cloneOriginCache, 
                         cloneOriginCache->GetFileInfo(), 
                         cloneOriginCache->GetFileEpoch(), chunkidx)) {
            return -1;
        }
        errCode = cloneOriginCache->GetChunkInfoByIndex(chunkidx, &originChunkIdInfo);
    }

    if (errCode == MetaCacheErrorType::OK) {
        for (auto& ctx : *targetlist) {
            ctx->cfinfo_ = fileInfo->cfinfo;
            ctx->chunkIndex_ = chunkidx;
            ctx->originChunkIdInfo_ = originChunkIdInfo;
        }
    } else {
        return -1;
    }

    return 0;
}

}   // namespace client
}   // namespace curve
