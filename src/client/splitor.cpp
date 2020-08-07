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

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <algorithm>
#include <vector>
#include <string>
#include "src/client/splitor.h"
#include "src/client/mds_client.h"
#include "src/client/file_instance.h"
#include "src/client/request_closure.h"
#include "src/client/metacache_struct.h"
#include "src/common/location_operator.h"

namespace curve {
namespace client {
IOSplitOPtion_t Splitor::iosplitopt_;

void Splitor::Init(IOSplitOPtion_t ioSplitOpt) {
    iosplitopt_ = ioSplitOpt;
    LOG(INFO) << "io splitor init success!";
}

int Splitor::IO2ChunkRequests(IOTracker* iotracker, MetaCache* mc,
                              std::vector<RequestContext*>* targetlist,
                              butil::IOBuf* data, off_t offset, size_t length,
                              MDSClient* mdsclient, const FInfo_t* fi) {
    if (targetlist == nullptr || mdsclient == nullptr ||
        mc == nullptr || iotracker == nullptr || fi == nullptr) {
        return -1;
    }

    if (iotracker->Optype() == OpType::WRITE && data == nullptr) {
        return -1;
    }

    targetlist->reserve(length / (iosplitopt_.fileIOSplitMaxSizeKB * 1024) + 1);

    uint64_t chunksize = fi->chunksize;

    uint64_t startchunkindex = offset / chunksize;
    uint64_t endchunkindex = (offset + length - 1) / chunksize;
    uint64_t startoffset = offset;
    uint64_t endoff = offset + length;
    uint64_t currentoff = offset % chunksize;
    uint64_t dataoff = 0;

    while (startchunkindex <= endchunkindex) {
        uint64_t off = currentoff;
        uint64_t chunkendpos = chunksize * (startchunkindex + 1);
        uint64_t pos = chunkendpos > endoff ? endoff : chunkendpos;
        uint64_t len = pos - startoffset;

        DVLOG(9) << "request split"
                 << ", off = " << off
                 << ", len = " << len
                 << ", seqnum = " << fi->seqnum
                 << ", endoff = " << endoff
                 << ", chunkendpos = " << chunkendpos
                 << ", chunksize = " << chunksize
                 << ", chunkindex = " << startchunkindex
                 << ", endchunkindex = " << endchunkindex;

        if (!AssignInternal(iotracker, mc, targetlist, data,
                            off, len, mdsclient, fi, startchunkindex)) {
            LOG(ERROR)  << "request split failed"
                        << ", off = " << off
                        << ", len = " << len
                        << ", seqnum = " << fi->seqnum
                        << ", endoff = " << endoff
                        << ", chunkendpos = " << chunkendpos
                        << ", chunksize = " << chunksize
                        << ", chunkindex = " << startchunkindex
                        << ", endchunkindex = " << endchunkindex;
            return -1;
        }

        currentoff = 0;
        startchunkindex++;

        dataoff += len;
        startoffset += len;
    }
    return 0;
}

// this offset is begin by chunk
int Splitor::SingleChunkIO2ChunkRequests(
    IOTracker* iotracker, MetaCache* mc,
    std::vector<RequestContext*>* targetlist, const ChunkIDInfo_t idinfo,
    butil::IOBuf* data, off_t offset, uint64_t length, uint64_t seq) {
    if (targetlist == nullptr || mc == nullptr || iotracker == nullptr) {
        return -1;
    }

    if (iotracker->Optype() == OpType::WRITE && data == nullptr) {
        return -1;
    }

    auto max_split_size_bytes = 1024 * iosplitopt_.fileIOSplitMaxSizeKB;

    uint64_t len = 0;
    uint64_t off = 0;
    uint64_t tempoff = offset;
    uint64_t leftlength = length;
    while (leftlength > 0) {
        tempoff += len;
        len = leftlength > max_split_size_bytes ? max_split_size_bytes : leftlength;    // NOLINT

        RequestContext* newreqNode = GetInitedRequestContext();
        if (newreqNode == nullptr) {
            return -1;
        }

        newreqNode->seq_         = seq;

        if (iotracker->Optype() == OpType::WRITE) {
            auto nc = data->cutn(&(newreqNode->writeData_), len);
            if (nc != len) {
                LOG(ERROR) << "IOBuf::cutn failed, expected: " << len
                           << ", return: " << nc;
                return -1;
            }
        }

        newreqNode->offset_      = tempoff;
        newreqNode->rawlength_   = len;
        newreqNode->optype_      = iotracker->Optype();
        newreqNode->idinfo_      = idinfo;
        newreqNode->done_->SetIOTracker(iotracker);
        targetlist->push_back(newreqNode);

        DVLOG(9) << "request split"
                 << ", off = " << tempoff
                 << ", len = " << len
                 << ", seqnum = " << seq
                 << ", chunkid = " << idinfo.cid_
                 << ", copysetid = " << idinfo.cpid_
                 << ", logicpoolid = " << idinfo.lpid_;

        leftlength -= len;
        off += len;
    }
    return 0;
}

bool Splitor::AssignInternal(IOTracker* iotracker, MetaCache* mc,
                             std::vector<RequestContext*>* targetlist,
                             butil::IOBuf* data, off_t off, size_t len,
                             MDSClient* mdsclient, const FInfo_t* fileinfo,
                             ChunkIndex chunkidx) {
    ChunkIDInfo_t chinfo;
    SegmentInfo segInfo;
    LogicalPoolCopysetIDInfo_t lpcsIDInfo;
    MetaCacheErrorType chunkidxexist =
        mc->GetChunkInfoByIndex(chunkidx, &chinfo);

    if (chunkidxexist == MetaCacheErrorType::CHUNKINFO_NOT_FOUND) {
        LIBCURVE_ERROR re = mdsclient->GetOrAllocateSegment(true,
                                        (off_t)chunkidx * fileinfo->chunksize,
                                        fileinfo,
                                        &segInfo);
        if (re != LIBCURVE_ERROR::OK) {
            LOG(ERROR) << "GetOrAllocateSegment failed! "
                       << "offset = " << chunkidx * fileinfo->chunksize;
            return false;
        } else {
            int count = 0;
            for (auto chunkidinfo : segInfo.chunkvec) {
                uint64_t index = (segInfo.startoffset +
                         count * fileinfo->chunksize) / fileinfo->chunksize;
                mc->UpdateChunkInfoByIndex(index, chunkidinfo);
                ++count;
            }

            std::vector<CopysetInfo_t> cpinfoVec;
            re = mdsclient->GetServerList(segInfo.lpcpIDInfo.lpid,
                            segInfo.lpcpIDInfo.cpidVec, &cpinfoVec);
            for (auto cpinfo : cpinfoVec) {
                for (auto peerinfo : cpinfo.csinfos_) {
                    mc->AddCopysetIDInfo(peerinfo.chunkserverID,
                        CopysetIDInfo(segInfo.lpcpIDInfo.lpid, cpinfo.cpid_));
                }
            }

            if (re == LIBCURVE_ERROR::FAILED) {
                std::string cpidstr;
                for (auto id : segInfo.lpcpIDInfo.cpidVec) {
                    cpidstr.append(std::to_string(id))
                        .append(",");
                }

                LOG(ERROR) << "GetServerList failed! "
                           << "logicpool id = " << segInfo.lpcpIDInfo.lpid
                           << ", copyset list = " << cpidstr.c_str();
                return false;
            } else {
                for (auto cpinfo : cpinfoVec) {
                    mc->UpdateCopysetInfo(segInfo.lpcpIDInfo.lpid,
                    cpinfo.cpid_, cpinfo);
                }
            }
        }

        chunkidxexist = mc->GetChunkInfoByIndex(chunkidx, &chinfo);
    }

    if (chunkidxexist == MetaCacheErrorType::OK) {
        int ret = 0;
        auto appliedindex_ = mc->GetAppliedIndex(chinfo.lpid_, chinfo.cpid_);
        std::vector<RequestContext*> templist;
        ret = SingleChunkIO2ChunkRequests(iotracker, mc, &templist, chinfo,
                                          data, off, len, fileinfo->seqnum);

        for (auto& ctx : templist) {
            ctx->appliedindex_ = appliedindex_;
            ctx->sourceInfo_ = CalcRequestSourceInfo(iotracker, mc, chunkidx);
        }

        targetlist->insert(targetlist->end(), templist.begin(),
                           templist.end());

        return ret == 0;
    }

    LOG(ERROR) << "can not find the chunk index info!"
                << ", chunk index = " << chunkidx;
    return false;
}

RequestContext* Splitor::GetInitedRequestContext() {
    RequestContext* ctx = new (std::nothrow) RequestContext();
    if (ctx && ctx->Init()) {
        return ctx;
    } else {
        LOG(ERROR) << "Allocate RequestContext Failed!";
        delete ctx;
        return nullptr;
    }
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
