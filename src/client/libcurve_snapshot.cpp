/*
 * Project: curve
 * File Created: Monday, 18th February 2019 11:20:10 am
 * Author: tongguangxun
 * Copyright (c)￼ 2018 netease
 */

#include <gflags/gflags.h>
#include <glog/logging.h>

#include <mutex>    // NOLINT
#include "src/client/client_common.h"
#include "src/client/client_config.h"
#include "src/client/libcurve_snapshot.h"
#include "include/curve_compiler_specific.h"

namespace curve {
namespace client {
SnapshotClient::SnapshotClient() {
}

int SnapshotClient::Init(ClientConfigOption_t clientopt) {
    google::SetCommandLineOption("minloglevel",
            std::to_string(clientopt.loginfo.loglevel).c_str());
    int ret = -LIBCURVE_ERROR::FAILED;
    do {
        if (mdsclient_.Initialize(clientopt.metaServerOpt)
            != LIBCURVE_ERROR::OK) {
            LOG(ERROR) << "MDSClient init failed!";
            break;
        }

        if (!iomanager4chunk_.Initialize(clientopt.ioOpt)) {
            LOG(ERROR) << "Init io context manager failed!";
            break;
        }
        ret = LIBCURVE_ERROR::OK;
    } while (0);

    return ret;
}

void SnapshotClient::UnInit() {
    iomanager4chunk_.UnInitialize();
    mdsclient_.UnInitialize();
}

int SnapshotClient::CreateSnapShot(const std::string& filename,
                                        UserInfo_t userinfo,
                                        uint64_t* seq) {
    LIBCURVE_ERROR ret = mdsclient_.CreateSnapShot(filename, userinfo, seq);
    return -ret;
}

int SnapshotClient::DeleteSnapShot(const std::string& filename,
                                        UserInfo_t userinfo,
                                        uint64_t seq) {
    LIBCURVE_ERROR ret = mdsclient_.DeleteSnapShot(filename, userinfo, seq);
    return -ret;
}

int SnapshotClient::GetSnapShot(const std::string& filename,
                                        UserInfo_t userinfo,
                                        uint64_t seq,
                                        FInfo* snapinfo) {
    LIBCURVE_ERROR ret = mdsclient_.GetSnapShot(filename, userinfo,
                                                seq, snapinfo);
    return -ret;
}

int SnapshotClient::ListSnapShot(const std::string& filename,
                                        UserInfo_t userinfo,
                                        const std::vector<uint64_t>* seq,
                                        std::vector<FInfo*>* snapif) {
    LIBCURVE_ERROR ret = mdsclient_.ListSnapShot(filename, userinfo,
                                                    seq, snapif);
    return -ret;
}

int SnapshotClient::GetSnapshotSegmentInfo(
                                        const std::string& filename,
                                        UserInfo_t userinfo,
                                        LogicalPoolCopysetIDInfo* lpcsIDInfo,
                                        uint64_t seq,
                                        uint64_t offset,
                                        SegmentInfo *segInfo) {
    int ret = mdsclient_.GetSnapshotSegmentInfo(filename, userinfo,
                                        seq, offset, segInfo);

    if (ret != LIBCURVE_ERROR::OK) {
        LOG(INFO) << "GetSnapshotSegmentInfo failed, ret = " << ret;
        return -ret;
    }

    *lpcsIDInfo = segInfo->lpcpIDInfo;
    for (auto iter : segInfo->chunkvec) {
        iomanager4chunk_.GetMetaCache()->UpdateChunkInfoByID(iter.cid_, iter);
    }

    return GetServerList(lpcsIDInfo->lpid, lpcsIDInfo->cpidVec);
}

int SnapshotClient::GetServerList(const LogicPoolID& lpid,
                                        const std::vector<CopysetID>& csid) {
    std::vector<CopysetInfo_t> cpinfoVec;
    int ret = mdsclient_.GetServerList(lpid, csid, &cpinfoVec);

    for (auto iter : cpinfoVec) {
        iomanager4chunk_.GetMetaCache()->UpdateCopysetInfo(lpid, iter.cpid_,
                                                           iter);
    }
    return -ret;
}

int SnapshotClient::CheckSnapShotStatus(const std::string& filename,
                                        UserInfo_t userinfo,
                                        uint64_t seq,
                                        FileStatus* filestatus) {
    LIBCURVE_ERROR ret = mdsclient_.CheckSnapShotStatus(filename, userinfo,
                                                        seq, filestatus);
    return -ret;
}

int SnapshotClient::CreateCloneFile(const std::string &destination,
                                        UserInfo_t userinfo,
                                        uint64_t size,
                                        uint64_t sn,
                                        uint32_t chunksize,
                                        FInfo* finfo) {
    LIBCURVE_ERROR ret = mdsclient_.CreateCloneFile(destination, userinfo, size,
                                                    sn, chunksize, finfo);
    return -ret;
}

int SnapshotClient::GetFileInfo(const std::string &filename,
                                        UserInfo_t userinfo,
                                        FInfo* fileInfo) {
    LIBCURVE_ERROR ret = mdsclient_.GetFileInfo(filename, userinfo, fileInfo);
    return -ret;
}

int SnapshotClient::GetOrAllocateSegmentInfo(bool allocate,
                                        uint64_t offset,
                                        const FInfo_t* fi,
                                        UserInfo_t userinfo,
                                        SegmentInfo *segInfo) {
    int ret = mdsclient_.GetOrAllocateSegment(allocate, userinfo,
                                        offset, fi, segInfo);

    if (ret != LIBCURVE_ERROR::OK) {
        LOG(INFO) << "GetSnapshotSegmentInfo failed, ret = " << ret;
        return -ret;
    }

    // update metacache
    int count = 0;
    for (auto iter : segInfo->chunkvec) {
        uint64_t index = (segInfo->startoffset + count * fi->chunksize )
                         / fi->chunksize;
        iomanager4chunk_.GetMetaCache()->UpdateChunkInfoByIndex(index, iter);
        ++count;
    }
    return GetServerList(segInfo->lpcpIDInfo.lpid, segInfo->lpcpIDInfo.cpidVec);
}

int SnapshotClient::RenameCloneFile(UserInfo_t userinfo,
                                        uint64_t originId,
                                        uint64_t destinationId,
                                        const std::string &origin,
                                        const std::string &destination) {
    LIBCURVE_ERROR ret = mdsclient_.RenameFile(userinfo, origin, destination,
                                               originId, destinationId);
    return -ret;
}

int SnapshotClient::CompleteCloneMeta(const std::string &destination,
                                        UserInfo_t userinfo) {
    LIBCURVE_ERROR ret = mdsclient_.CompleteCloneMeta(destination, userinfo);
    return -ret;
}

int SnapshotClient::CompleteCloneFile(const std::string &destination,
                                        UserInfo_t userinfo) {
    LIBCURVE_ERROR ret = mdsclient_.CompleteCloneFile(destination, userinfo);
    return -ret;
}

int SnapshotClient::CreateCloneChunk(const std::string &location,
                                        const ChunkIDInfo &chunkidinfo,
                                        uint64_t sn,
                                        uint64_t correntSn,
                                        uint64_t chunkSize) {
    return iomanager4chunk_.CreateCloneChunk(location, chunkidinfo,
                                             sn, correntSn, chunkSize);
}

int SnapshotClient::RecoverChunk(const ChunkIDInfo &chunkidinfo,
                                        uint64_t offset,
                                        uint64_t len) {
    return iomanager4chunk_.RecoverChunk(chunkidinfo, offset, len);
}

int SnapshotClient::ReadChunkSnapshot(ChunkIDInfo cidinfo,
                                        uint64_t seq,
                                        uint64_t offset,
                                        uint64_t len,
                                        void *buf) {
    return iomanager4chunk_.ReadSnapChunk(cidinfo, seq, offset, len, buf);
}

int SnapshotClient::DeleteChunkSnapshotOrCorrectSn(
    ChunkIDInfo cidinfo, uint64_t correctedSeq) {
    return iomanager4chunk_.DeleteSnapChunkOrCorrectSn(cidinfo, correctedSeq);
}

int SnapshotClient::GetChunkInfo(ChunkIDInfo cidinfo,
                                        ChunkInfoDetail *chunkInfo) {
    return iomanager4chunk_.GetChunkInfo(cidinfo, chunkInfo);
}
}   // namespace client
}   // namespace curve
