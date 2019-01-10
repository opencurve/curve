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
            std::to_string(clientopt.loglevel).c_str());
    LIBCURVE_ERROR ret = LIBCURVE_ERROR::FAILED;
    do {
        if (mdsclient_.Initialize(clientopt.metaserveropt)
            != LIBCURVE_ERROR::OK) {
            LOG(ERROR) << "MDSClient init failed!";
            break;
        }

        if (!iomanager4chunk_.Initialize(clientopt.ioopt)) {
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

LIBCURVE_ERROR SnapshotClient::CreateSnapShot(std::string filename,
                                              uint64_t* seq) {
    return mdsclient_.CreateSnapShot(filename, seq);
}

LIBCURVE_ERROR SnapshotClient::DeleteSnapShot(std::string filename,
                                              uint64_t seq) {
    return mdsclient_.DeleteSnapShot(filename, seq);
}

LIBCURVE_ERROR SnapshotClient::GetSnapShot(std::string filename,
                            uint64_t seq,
                            FInfo* snapinfo) {
    return mdsclient_.GetSnapShot(filename, seq,  snapinfo);
}

LIBCURVE_ERROR SnapshotClient::ListSnapShot(std::string filename,
                            const std::vector<uint64_t>* seq,
                            std::vector<FInfo*>* snapif) {
    return mdsclient_.ListSnapShot(filename, seq,  snapif);
}

LIBCURVE_ERROR SnapshotClient::GetSnapshotSegmentInfo(std::string filename,
                                    LogicalPoolCopysetIDInfo* lpcsIDInfo,
                                    uint64_t seq,
                                    uint64_t offset,
                                    SegmentInfo *segInfo) {
    return mdsclient_.GetSnapshotSegmentInfo(filename,
                                            lpcsIDInfo,
                                            seq,
                                            offset,
                                            segInfo,
                                            iomanager4chunk_.GetMetaCache());
}

LIBCURVE_ERROR SnapshotClient::GetServerList(const LogicPoolID& lpid,
                            const std::vector<CopysetID>& csid) {
    return mdsclient_.GetServerList(lpid,
                                    csid,
                                    iomanager4chunk_.GetMetaCache());
}

LIBCURVE_ERROR SnapshotClient::ReadChunkSnapshot(ChunkIDInfo cidinfo,
                                                uint64_t seq,
                                                uint64_t offset,
                                                uint64_t len,
                                                void *buf) {
    int ret = iomanager4chunk_.ReadSnapChunk(cidinfo.lpid_,
                                                cidinfo.cpid_,
                                                cidinfo.cid_,
                                                seq,
                                                offset,
                                                len,
                                                buf);
    return ret < 0 ? LIBCURVE_ERROR::FAILED : LIBCURVE_ERROR::OK;
}

LIBCURVE_ERROR SnapshotClient::DeleteChunkSnapshot(ChunkIDInfo cidinfo,
                                                    uint64_t seq) {
    int ret = iomanager4chunk_.DeleteSnapChunk(cidinfo.lpid_,
                                                    cidinfo.cpid_,
                                                    cidinfo.cid_,
                                                    seq);
    return ret < 0 ? LIBCURVE_ERROR::FAILED : LIBCURVE_ERROR::OK;
}

LIBCURVE_ERROR SnapshotClient::GetChunkInfo(ChunkIDInfo cidinfo,
                                            ChunkInfoDetail *chunkInfo) {
    int ret = iomanager4chunk_.GetChunkInfo(cidinfo.lpid_,
                                            cidinfo.cpid_,
                                            cidinfo.cid_,
                                            chunkInfo);
    return ret < 0 ? LIBCURVE_ERROR::FAILED : LIBCURVE_ERROR::OK;
}
}   // namespace client
}   // namespace curve
