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

#include "src/client/libcurve_define.h"

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

// 保存每个segment的基本信息
typedef struct SegmentInfo {
    uint32_t segmentsize;
    uint32_t chunksize;
    uint64_t startoffset;
    std::vector<ChunkIDInfo> chunkvec;
} SegmentInfo_t;

// 保存每个chunk对应的版本信息
typedef struct ChunkInfoDetail {
    std::vector<uint64_t> chunkSn;
} ChunkInfoDetail_t;

typedef struct LeaseSession {
    std::string sessionID;
    std::string token;
    uint32_t leaseTime;
    uint64_t createTime;
} LeaseSession_t;

// 保存logicalpool中segment对应的copysetid信息
typedef struct LogicalPoolCopysetIDInfo {
    LogicPoolID lpid;
    std::vector<CopysetID> cpidVec;

    LogicalPoolCopysetIDInfo() {
        lpid = 0;
        cpidVec.clear();
    }
} LogicalPoolCopysetIDInfo_t;

typedef struct FInfo {
    uint64_t        id;
    char            filename[4096];
    uint64_t        parentid;
    FileType        filetype;
    uint32_t        chunksize;
    uint32_t        segmentsize;
    uint64_t        length;
    uint64_t        ctime;
    uint64_t        seqnum;

    FInfo() {
        id = 0;
        ctime = 0;
        seqnum = 0;
        length = 0;
        strcpy(filename, "");                                                           // NOLINT
        chunksize = 4 * 1024 * 1024;
        segmentsize = 1 * 1024 * 1024 * 1024ul;
    }
} FInfo_t;

}   // namespace client
}   // namespace curve

#endif  // !CURVE_LIBCURVE_COMMON_H
