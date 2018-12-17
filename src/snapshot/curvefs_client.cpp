/*
 * Project: curve
 * Created Date: Mon Jan 07 2019
 * Author: xuchaojie
 * Copyright (c) 2018 netease
 */

#include "src/snapshot/curvefs_client.h"

namespace curve {
namespace snapshotserver {
int CurveFsClientImpl::Init() {
    // TODO(xuchaojie): using config file instead.
    ClientConfigOption_t opt;
    opt.metaserveropt.metaaddr = "192.168.1.1:6666";
    opt.ctxslabopt.pre_allocate_context_num = 2048;
    opt.reqslabopt.request_scheduler_queue_capacity = 4096;
    opt.reqslabopt.request_scheduler_threadpool_size = 2;
    opt.failreqslab.client_chunk_op_max_retry = 3;
    opt.failreqslab.client_chunk_op_retry_interval_us = 500;
    opt.metacacheopt.get_leader_retry = 3;
    opt.ioopt.enable_applied_index_read = 1;
    opt.ioopt.io_split_max_size_kb = 64;
    opt.loglevel = 0;
    return client_.Init(opt);
}

int CurveFsClientImpl::UnInit() {
    client_.UnInit();
    return 0;
}

int CurveFsClientImpl::CreateSnapshot(const std::string &filename,
    uint64_t *seq) {
    return client_.CreateSnapShot(filename, seq);
}

int CurveFsClientImpl::DeleteSnapshot(
    const std::string &filename, uint64_t seq) {
    return client_.DeleteSnapShot(filename, seq);
}

int CurveFsClientImpl::GetSnapshot(
    const std::string &filename, uint64_t seq, FInfo* snapInfo) {
    return client_.GetSnapShot(filename, seq, snapInfo);
}

int CurveFsClientImpl::GetSnapshotSegmentInfo(const std::string &filename,
    uint64_t seq,
    uint64_t offset,
    SegmentInfo *segInfo) {
    return client_.GetSnapshotSegmentInfo(filename, seq, offset, segInfo);
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
