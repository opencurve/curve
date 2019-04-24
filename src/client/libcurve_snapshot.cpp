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
#include "include/client/libcurve_qemu.h"
#include "include/curve_compiler_specific.h"

namespace curve {
namespace client {
SnapshotClient::SnapshotClient() {
}

LIBCURVE_ERROR SnapshotClient::Init(ClientConfigOption_t clientopt) {
    google::SetCommandLineOption("minloglevel",
            std::to_string(clientopt.loginfo.loglevel).c_str());
    LIBCURVE_ERROR ret = LIBCURVE_ERROR::FAILED;
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

LIBCURVE_ERROR SnapshotClient::CreateSnapShot(const std::string& filename,
                                        UserInfo_t userinfo,
                                        uint64_t* seq) {
    return mdsclient_.CreateSnapShot(filename, userinfo, seq);
}

LIBCURVE_ERROR SnapshotClient::DeleteSnapShot(const std::string& filename,
                                        UserInfo_t userinfo,
                                        uint64_t seq) {
    return mdsclient_.DeleteSnapShot(filename, userinfo, seq);
}

LIBCURVE_ERROR SnapshotClient::GetSnapShot(const std::string& filename,
                                        UserInfo_t userinfo,
                                        uint64_t seq,
                                        FInfo* snapinfo) {
    return mdsclient_.GetSnapShot(filename, userinfo, seq, snapinfo);
}

LIBCURVE_ERROR SnapshotClient::ListSnapShot(const std::string& filename,
                                        UserInfo_t userinfo,
                                        const std::vector<uint64_t>* seq,
                                        std::vector<FInfo*>* snapif) {
    return mdsclient_.ListSnapShot(filename, userinfo, seq, snapif);
}

LIBCURVE_ERROR SnapshotClient::GetSnapshotSegmentInfo(
                                        const std::string& filename,
                                        UserInfo_t userinfo,
                                        LogicalPoolCopysetIDInfo* lpcsIDInfo,
                                        uint64_t seq,
                                        uint64_t offset,
                                        SegmentInfo *segInfo) {
    LIBCURVE_ERROR ret = mdsclient_.GetSnapshotSegmentInfo(filename, userinfo,
                                        seq, offset, segInfo);

    if (ret != LIBCURVE_ERROR::OK) {
        LOG(INFO) << "GetSnapshotSegmentInfo failed, ret = " << ret;
        return ret;
    }

    *lpcsIDInfo = segInfo->lpcpIDInfo;
    for (auto iter : segInfo->chunkvec) {
        iomanager4chunk_.GetMetaCache()->UpdateChunkInfoByID(iter.cid_, iter);
    }

    return GetServerList(lpcsIDInfo->lpid, lpcsIDInfo->cpidVec);
}

LIBCURVE_ERROR SnapshotClient::GetServerList(const LogicPoolID& lpid,
                                        const std::vector<CopysetID>& csid) {
    std::vector<CopysetInfo_t> cpinfoVec;
    LIBCURVE_ERROR ret = mdsclient_.GetServerList(lpid, csid, &cpinfoVec);

    for (auto iter : cpinfoVec) {
        iomanager4chunk_.GetMetaCache()->UpdateCopysetInfo(lpid, iter.cpid_,
                                                           iter);
    }
    return ret;
}

LIBCURVE_ERROR SnapshotClient::CheckSnapShotStatus(const std::string& filename,
                                        UserInfo_t userinfo,
                                        uint64_t seq) {
    return mdsclient_.CheckSnapShotStatus(filename, userinfo, seq);
}

LIBCURVE_ERROR SnapshotClient::CreateCloneFile(const std::string &destination,
                                        UserInfo_t userinfo,
                                        uint64_t size,
                                        uint64_t sn,
                                        uint32_t chunksize,
                                        FInfo* finfo) {
    return mdsclient_.CreateCloneFile(destination, userinfo, size,
                                      sn, chunksize, finfo);
}

LIBCURVE_ERROR SnapshotClient::GetFileInfo(const std::string &filename,
                                        UserInfo_t userinfo,
                                        FInfo* fileInfo) {
    return mdsclient_.GetFileInfo(filename, userinfo, fileInfo);
}

LIBCURVE_ERROR SnapshotClient::GetOrAllocateSegmentInfo(bool allocate,
                                        uint64_t offset,
                                        const FInfo_t* fi,
                                        UserInfo_t userinfo,
                                        SegmentInfo *segInfo) {
    LIBCURVE_ERROR ret = mdsclient_.GetOrAllocateSegment(allocate, userinfo,
                                        offset, fi, segInfo);

    if (ret != LIBCURVE_ERROR::OK) {
        LOG(INFO) << "GetSnapshotSegmentInfo failed, ret = " << ret;
        return ret;
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

LIBCURVE_ERROR SnapshotClient::RenameCloneFile(UserInfo_t userinfo,
                                        uint64_t originId,
                                        uint64_t destinationId,
                                        const std::string &origin,
                                        const std::string &destination) {
    return mdsclient_.RenameFile(userinfo, origin, destination,
                                        originId,
                                        destinationId);
}

LIBCURVE_ERROR SnapshotClient::CompleteCloneMeta(const std::string &destination,
                                        UserInfo_t userinfo) {
    return mdsclient_.CompleteCloneMeta(destination, userinfo);
}

LIBCURVE_ERROR SnapshotClient::CompleteCloneFile(const std::string &destination,
                                        UserInfo_t userinfo) {
    return mdsclient_.CompleteCloneFile(destination, userinfo);
}

LIBCURVE_ERROR SnapshotClient::CreateCloneChunk(const std::string &location,
                                        const ChunkIDInfo &chunkidinfo,
                                        uint64_t sn,
                                        uint64_t correntSn,
                                        uint64_t chunkSize) {
    int ret = iomanager4chunk_.CreateCloneChunk(location, chunkidinfo,
                                                sn, correntSn, chunkSize);
    return ret < 0 ? LIBCURVE_ERROR::FAILED : LIBCURVE_ERROR::OK;
}

LIBCURVE_ERROR SnapshotClient::RecoverChunk(const ChunkIDInfo &chunkidinfo,
                                        uint64_t offset,
                                        uint64_t len) {
    int ret = iomanager4chunk_.RecoverChunk(chunkidinfo, offset, len);
    return ret < 0 ? LIBCURVE_ERROR::FAILED : LIBCURVE_ERROR::OK;
}

LIBCURVE_ERROR SnapshotClient::ReadChunkSnapshot(ChunkIDInfo cidinfo,
                                        uint64_t seq,
                                        uint64_t offset,
                                        uint64_t len,
                                        void *buf) {
    int ret = iomanager4chunk_.ReadSnapChunk(cidinfo, seq, offset, len, buf);
    return ret < 0 ? LIBCURVE_ERROR::FAILED : LIBCURVE_ERROR::OK;
}

LIBCURVE_ERROR SnapshotClient::DeleteChunkSnapshotOrCorrectSn(
    ChunkIDInfo cidinfo, uint64_t correctedSeq) {
    int ret = iomanager4chunk_.DeleteSnapChunkOrCorrectSn(cidinfo,
                                                          correctedSeq);
    return ret < 0 ? LIBCURVE_ERROR::FAILED : LIBCURVE_ERROR::OK;
}

LIBCURVE_ERROR SnapshotClient::GetChunkInfo(ChunkIDInfo cidinfo,
                                        ChunkInfoDetail *chunkInfo) {
    int ret = iomanager4chunk_.GetChunkInfo(cidinfo, chunkInfo);
    return ret < 0 ? LIBCURVE_ERROR::FAILED : LIBCURVE_ERROR::OK;
}
}   // namespace client
}   // namespace curve
