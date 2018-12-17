/*
 * Project: curve
 * File Created: Tuesday, 18th September 2018 3:24:40 pm
 * Author: tongguangxun
 * Copyright (c) 2018 NetEase
 */

#ifndef CURVE_LIBCURVE_COMMON_H
#define CURVE_LIBCURVE_COMMON_H

#include <butil/endpoint.h>
#include <butil/status.h>
#include <braft/configuration.h>

#include <unistd.h>
#include <string>
#include <atomic>
#include <vector>

#include "src/client2/libcurve_define.h"

namespace curve {
namespace client {
    using ChunkID       = uint64_t;
    using CopysetID     = uint32_t;
    using LogicPoolID   = uint32_t;
    using ChunkServerID = uint32_t;
    using ChunkIndex    = uint32_t;

    using EndPoint  = butil::EndPoint;
    using PeerId    = braft::PeerId;
    using Status    = butil::Status;
    using Configuration = braft::Configuration;

    typedef struct ChunkIDInfo {
        ChunkID         cid_;
        CopysetID       cpid_;
        LogicPoolID     lpid_;
        ChunkIDInfo() {
            cid_  = 0;
            lpid_ = 0;
            cpid_ = 0;
        }

        ChunkIDInfo(ChunkID cid, LogicPoolID lpid, CopysetID cpid) {
            cid_  = cid;
            lpid_ = lpid;
            cpid_ = cpid;
        }

        ChunkIDInfo(const ChunkIDInfo& chunkinfo) {
            cid_  = chunkinfo.cid_;
            lpid_ = chunkinfo.lpid_;
            cpid_ = chunkinfo.cpid_;
        }

        ChunkIDInfo& operator=(const ChunkIDInfo& chunkinfo) {
            cid_  = chunkinfo.cid_;
            lpid_ = chunkinfo.lpid_;
            cpid_ = chunkinfo.cpid_;
            return *this;
        }

        bool Valid() {
            return lpid_ > 0 && cpid_ > 0;
        }
    } ChunkIDInfo_t;

    typedef struct SegmentInfo {
        uint32_t segmentsize;
        uint32_t chunksize;
        uint64_t startoffset;
        std::vector<ChunkIDInfo> chunkvec;
    } SegmentInfo_t;

    typedef struct ChunkInfoDetail {
        std::vector<uint64_t> chunkSn;
    } ChunkInfoDetail_t;
}   // namespace client
}   // namespace curve

#endif  // !CURVE_LIBCURVE_COMMON_H
