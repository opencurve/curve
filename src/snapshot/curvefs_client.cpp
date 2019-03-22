/*
 * Project: curve
 * Created Date: Mon Jan 07 2019
 * Author: xuchaojie
 * Copyright (c) 2018 netease
 */

#include "src/snapshot/curvefs_client.h"

using ::curve::client::LogicalPoolCopysetIDInfo;
using ::curve::client::UserInfo;

namespace curve {
namespace snapshotserver {
int CurveFsClientImpl::Init() {
    // TODO(xuchaojie): using config file instead.
    ClientConfigOption_t opt;

    opt.metaServerOpt.metaaddrvec.push_back("127.0.0.1:6666");
    opt.ioOpt.reqSchdulerOpt.queueCapacity = 4096;
    opt.ioOpt.reqSchdulerOpt.threadpoolSize = 2;
    opt.ioOpt.ioSenderOpt.failRequestOpt.opMaxRetry = 3;
    opt.ioOpt.ioSenderOpt.failRequestOpt.opRetryIntervalUs = 500;
    opt.ioOpt.metaCacheOpt.getLeaderRetry = 3;
    opt.ioOpt.ioSenderOpt.enableAppliedIndexRead = 1;
    opt.ioOpt.ioSplitOpt.ioSplitMaxSize = 64;
    opt.ioOpt.reqSchdulerOpt.ioSenderOpt = opt.ioOpt.ioSenderOpt;
    opt.loginfo.loglevel = 0;

    return client_.Init(opt);
}

int CurveFsClientImpl::UnInit() {
    client_.UnInit();
    return 0;
}

int CurveFsClientImpl::CreateSnapshot(const std::string &filename,
    const std::string &user,
    uint64_t *seq) {
    return client_.CreateSnapShot(filename, UserInfo(user, ""), seq);
}

int CurveFsClientImpl::DeleteSnapshot(const std::string &filename,
    const std::string &user,
    uint64_t seq) {
    return client_.DeleteSnapShot(filename, UserInfo(user, ""), seq);
}

int CurveFsClientImpl::GetSnapshot(
    const std::string &filename,
    const std::string &user,
    uint64_t seq,
    FInfo* snapInfo) {
    return client_.GetSnapShot(filename, UserInfo(user, ""), seq, snapInfo);
}

int CurveFsClientImpl::GetSnapshotSegmentInfo(const std::string &filename,
    const std::string &user,
    uint64_t seq,
    uint64_t offset,
    SegmentInfo *segInfo) {
    LogicalPoolCopysetIDInfo lpcsIDInfo;
    return client_.GetSnapshotSegmentInfo(
        filename, UserInfo(user, ""), &lpcsIDInfo, seq, offset, segInfo);
}

int CurveFsClientImpl::ReadChunkSnapshot(ChunkIDInfo cidinfo,
                        uint64_t seq,
                        uint64_t offset,
                        uint64_t len,
                        void *buf) {
    return client_.ReadChunkSnapshot(
        cidinfo, seq, offset, len, buf);
}

int CurveFsClientImpl::DeleteChunkSnapshot(ChunkIDInfo cidinfo,
    uint64_t seq) {
    return client_.DeleteChunkSnapshot(cidinfo, seq);
}

int CurveFsClientImpl::GetChunkInfo(
    ChunkIDInfo cidinfo,
    ChunkInfoDetail *chunkInfo) {
    return client_.GetChunkInfo(cidinfo, chunkInfo);
}

}  // namespace snapshotserver
}  // namespace curve
