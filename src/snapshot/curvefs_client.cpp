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

    opt.metaserveropt.metaaddrvec.push_back("127.0.0.1:6666");
    opt.ioopt.reqschopt.request_scheduler_queue_capacity = 4096;
    opt.ioopt.reqschopt.request_scheduler_threadpool_size = 2;
    opt.ioopt.iosenderopt.failreqopt.client_chunk_op_max_retry = 3;
    opt.ioopt.iosenderopt.failreqopt.client_chunk_op_retry_interval_us = 500;
    opt.ioopt.metacacheopt.get_leader_retry = 3;
    opt.ioopt.iosenderopt.enable_applied_index_read = 1;
    opt.ioopt.iosplitopt.io_split_max_size_kb = 64;
    opt.ioopt.reqschopt.iosenderopt = opt.ioopt.iosenderopt;
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
