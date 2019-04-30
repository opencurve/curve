/*
 * Project: curve
 * Created Date: Mon Jan 07 2019
 * Author: xuchaojie
 * Copyright (c) 2018 netease
 */

#include "src/snapshotcloneserver/common/curvefs_client.h"

using ::curve::client::LogicalPoolCopysetIDInfo;
using ::curve::client::UserInfo;

namespace curve {
namespace snapshotcloneserver {
int CurveFsClientImpl::Init(const CurveClientOptions &options) {
    ClientConfigOption_t opt;

    opt.metaServerOpt.metaaddrvec.push_back(options.mdsAddr);
    opt.ioOpt.reqSchdulerOpt.queueCapacity = options.requestQueueCap;
    opt.ioOpt.reqSchdulerOpt.threadpoolSize = options.threadNum;
    opt.ioOpt.ioSenderOpt.failRequestOpt.opMaxRetry = options.requestMaxRetry;
    opt.ioOpt.ioSenderOpt.failRequestOpt.opRetryIntervalUs
        = options.requestRetryIntervalUs;
    opt.ioOpt.metaCacheOpt.getLeaderRetry = options.getLeaderRetry;
    opt.ioOpt.ioSenderOpt.enableAppliedIndexRead = options.enableApplyIndexRead;
    opt.ioOpt.ioSplitOpt.ioSplitMaxSize = options.ioSplitSize;
    opt.ioOpt.reqSchdulerOpt.ioSenderOpt = opt.ioOpt.ioSenderOpt;
    opt.loginfo.loglevel = options.loglevel;

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

int CurveFsClientImpl::DeleteChunkSnapshotOrCorrectSn(ChunkIDInfo cidinfo,
    uint64_t correctedSeq) {
    return client_.DeleteChunkSnapshotOrCorrectSn(cidinfo, correctedSeq);
}

int CurveFsClientImpl::CheckSnapShotStatus(std::string filename,
                        std::string user,
                        uint64_t seq) {
    return client_.CheckSnapShotStatus(filename, UserInfo(user, ""), seq);
}

int CurveFsClientImpl::GetChunkInfo(
    ChunkIDInfo cidinfo,
    ChunkInfoDetail *chunkInfo) {
    return client_.GetChunkInfo(cidinfo, chunkInfo);
}

int CurveFsClientImpl::CreateCloneFile(
    const std::string &filename,
    const std::string &user,
    uint64_t size,
    uint64_t sn,
    uint32_t chunkSize,
    FInfo* fileInfo) {
    return client_.CreateCloneFile(filename, UserInfo(user, ""), size,
            sn, chunkSize, fileInfo);
}

int CurveFsClientImpl::CreateCloneChunk(
    const std::string &location,
    const ChunkIDInfo &chunkidinfo,
    uint64_t sn,
    uint64_t csn,
    uint64_t chunkSize) {
    return client_.CreateCloneChunk(location, chunkidinfo, sn, csn, chunkSize);
}

int CurveFsClientImpl::RecoverChunk(
    const ChunkIDInfo &chunkidinfo,
    uint64_t offset,
    uint64_t len) {
    return client_.RecoverChunk(chunkidinfo, offset, len);
}

int CurveFsClientImpl::CompleteCloneMeta(
    const std::string &filename,
    const std::string &user) {
    return client_.CompleteCloneMeta(filename, UserInfo(user, ""));
}

int CurveFsClientImpl::CompleteCloneFile(
    const std::string &filename,
    const std::string &user) {
    return client_.CompleteCloneFile(filename, UserInfo(user, ""));
}

int CurveFsClientImpl::GetFileInfo(
    const std::string &filename,
    const std::string &user,
    FInfo* fileInfo) {
    return client_.GetFileInfo(filename, UserInfo(user, ""), fileInfo);
}

int CurveFsClientImpl::GetOrAllocateSegmentInfo(
    bool allocate,
    uint64_t offset,
    const FInfo* fileInfo,
    const std::string user,
    SegmentInfo *segInfo) {
    return client_.GetOrAllocateSegmentInfo(allocate, offset, fileInfo,
        UserInfo(user, ""), segInfo);
}

int CurveFsClientImpl::RenameCloneFile(
    const std::string &user,
    uint64_t originId,
    uint64_t destinationId,
    const std::string &origin,
    const std::string &destination) {
    return client_.RenameCloneFile(UserInfo(user, ""), originId, destinationId,
            origin, destination);
}

}  // namespace snapshotcloneserver
}  // namespace curve

