/*
 * Project: curve
 * Created Date: 2020-03-10
 * Author: qinyi
 * Copyright (c) 2020 netease
 */

#include "test/integration/common/chunkservice_op.h"
#include "proto/chunk.pb.h"

namespace curve {
namespace chunkserver {

const PageSizeType kPageSize = kOpRequestAlignSize;

int ChunkServiceOp::WriteChunk(struct ChunkServiceOpConf *opConf,
                               ChunkID chunkId, SequenceNum sn, off_t offset,
                               size_t len, const char *data,
                               const std::string& cloneFileSource,
                               off_t cloneFileOffset) {
    PeerId leaderId(opConf->leaderPeer->address());
    brpc::Channel channel;
    channel.Init(leaderId.addr, NULL);
    ChunkService_Stub stub(&channel);
    brpc::Controller cntl;
    cntl.set_timeout_ms(opConf->rpcTimeout);

    ChunkRequest request;
    ChunkResponse response;
    request.set_optype(CHUNK_OP_TYPE::CHUNK_OP_WRITE);
    request.set_logicpoolid(opConf->logicPoolId);
    request.set_copysetid(opConf->copysetId);
    request.set_chunkid(chunkId);
    request.set_sn(sn);
    request.set_offset(offset);
    request.set_size(len);
    if (!cloneFileSource.empty()) {
        request.set_clonefilesource(cloneFileSource);
        request.set_clonefileoffset(cloneFileOffset);
    }
    cntl.request_attachment().append(data, len);
    stub.WriteChunk(&cntl, &request, &response, nullptr);

    if (cntl.Failed()) {
        LOG(ERROR) << "write failed: " << cntl.ErrorText();
        return -1;
    }

    CHUNK_OP_STATUS status = response.status();
    LOG_IF(ERROR, status) << "write failed: " << CHUNK_OP_STATUS_Name(status);
    return status;
}

int ChunkServiceOp::ReadChunk(struct ChunkServiceOpConf *opConf,
                              ChunkID chunkId, SequenceNum sn, off_t offset,
                              size_t len, std::string *data,
                              const std::string& cloneFileSource,
                              off_t cloneFileOffset) {
    PeerId leaderId(opConf->leaderPeer->address());
    brpc::Channel channel;
    channel.Init(leaderId.addr, NULL);
    ChunkService_Stub stub(&channel);
    brpc::Controller cntl;
    cntl.set_timeout_ms(opConf->rpcTimeout);

    ChunkRequest request;
    ChunkResponse response;
    request.set_optype(CHUNK_OP_TYPE::CHUNK_OP_READ);
    request.set_logicpoolid(opConf->logicPoolId);
    request.set_copysetid(opConf->copysetId);
    request.set_chunkid(chunkId);
    request.set_sn(sn);
    request.set_offset(offset);
    request.set_size(len);
    request.set_appliedindex(1);
    if (!cloneFileSource.empty()) {
        request.set_clonefilesource(cloneFileSource);
        request.set_clonefileoffset(cloneFileOffset);
    }

    stub.ReadChunk(&cntl, &request, &response, nullptr);
    if (cntl.Failed()) {
        LOG(ERROR) << "read failed: " << cntl.ErrorText();
        return -1;
    }

    CHUNK_OP_STATUS status = response.status();
    LOG_IF(ERROR, status) << "read failed: " << CHUNK_OP_STATUS_Name(status);

    // 读成功，复制内容到data
    if (status == CHUNK_OP_STATUS_SUCCESS && data != nullptr) {
        cntl.response_attachment().copy_to(data,
                                           cntl.response_attachment().size());
    }

    return status;
}

int ChunkServiceOp::ReadChunkSnapshot(struct ChunkServiceOpConf *opConf,
                                      ChunkID chunkId, SequenceNum sn,
                                      off_t offset, size_t len,
                                      std::string *data) {
    PeerId leaderId(opConf->leaderPeer->address());
    brpc::Channel channel;
    channel.Init(leaderId.addr, NULL);
    ChunkService_Stub stub(&channel);
    brpc::Controller cntl;
    cntl.set_timeout_ms(opConf->rpcTimeout);

    ChunkRequest request;
    ChunkResponse response;
    request.set_optype(CHUNK_OP_TYPE::CHUNK_OP_READ_SNAP);
    request.set_logicpoolid(opConf->logicPoolId);
    request.set_copysetid(opConf->copysetId);
    request.set_chunkid(chunkId);
    request.set_sn(sn);
    request.set_offset(offset);
    request.set_size(len);

    stub.ReadChunkSnapshot(&cntl, &request, &response, nullptr);
    if (cntl.Failed()) {
        LOG(ERROR) << "readchunksnapshot failed: " << cntl.ErrorText();
        return -1;
    }

    CHUNK_OP_STATUS status = response.status();
    LOG_IF(ERROR, status) << "readchunksnapshot failed: "
                          << CHUNK_OP_STATUS_Name(status);

    // 读成功，复制内容到data
    if (status == CHUNK_OP_STATUS_SUCCESS && data != nullptr) {
        cntl.response_attachment().copy_to(data,
                                           cntl.response_attachment().size());
    }

    return status;
}

int ChunkServiceOp::DeleteChunk(struct ChunkServiceOpConf *opConf,
                                ChunkID chunkId, SequenceNum sn) {
    PeerId leaderId(opConf->leaderPeer->address());
    brpc::Channel channel;
    channel.Init(leaderId.addr, NULL);
    ChunkService_Stub stub(&channel);
    brpc::Controller cntl;
    cntl.set_timeout_ms(opConf->rpcTimeout);

    ChunkRequest request;
    ChunkResponse response;
    request.set_optype(CHUNK_OP_TYPE::CHUNK_OP_DELETE);
    request.set_logicpoolid(opConf->logicPoolId);
    request.set_copysetid(opConf->copysetId);
    request.set_chunkid(chunkId);
    request.set_sn(sn);
    stub.DeleteChunk(&cntl, &request, &response, nullptr);

    if (cntl.Failed()) {
        LOG(ERROR) << "delete failed: " << cntl.ErrorText();
        return -1;
    }

    CHUNK_OP_STATUS status = response.status();
    LOG_IF(ERROR, status) << "delete failed: " << CHUNK_OP_STATUS_Name(status);

    return status;
}

int ChunkServiceOp::DeleteChunkSnapshotOrCorrectSn(
    struct ChunkServiceOpConf *opConf, ChunkID chunkId, uint64_t correctedSn) {
    PeerId leaderId(opConf->leaderPeer->address());
    brpc::Channel channel;
    channel.Init(leaderId.addr, NULL);
    ChunkService_Stub stub(&channel);
    brpc::Controller cntl;
    cntl.set_timeout_ms(opConf->rpcTimeout);

    ChunkRequest request;
    ChunkResponse response;
    request.set_optype(CHUNK_OP_TYPE::CHUNK_OP_DELETE_SNAP);
    request.set_logicpoolid(opConf->logicPoolId);
    request.set_copysetid(opConf->copysetId);
    request.set_chunkid(chunkId);
    request.set_correctedsn(correctedSn);
    stub.DeleteChunkSnapshotOrCorrectSn(&cntl, &request, &response, nullptr);

    if (cntl.Failed()) {
        LOG(ERROR) << "deleteSnapshot failed: " << cntl.ErrorText();
        return -1;
    }

    CHUNK_OP_STATUS status = response.status();
    LOG_IF(ERROR, status) << "deleteSnapshot failed: "
                          << CHUNK_OP_STATUS_Name(status);

    return status;
}

int ChunkServiceOp::CreateCloneChunk(struct ChunkServiceOpConf *opConf,
                                     ChunkID chunkId,
                                     const std::string &location,
                                     uint64_t correctedSn, uint64_t sn,
                                     uint64_t chunkSize) {
    PeerId leaderId(opConf->leaderPeer->address());
    brpc::Channel channel;
    channel.Init(leaderId.addr, NULL);
    ChunkService_Stub stub(&channel);
    brpc::Controller cntl;
    cntl.set_timeout_ms(opConf->rpcTimeout);

    ChunkRequest request;
    ChunkResponse response;
    request.set_optype(CHUNK_OP_TYPE::CHUNK_OP_CREATE_CLONE);
    request.set_logicpoolid(opConf->logicPoolId);
    request.set_copysetid(opConf->copysetId);
    request.set_chunkid(chunkId);
    request.set_location(location);
    request.set_sn(sn);
    request.set_correctedsn(correctedSn);
    request.set_size(chunkSize);
    stub.CreateCloneChunk(&cntl, &request, &response, nullptr);

    if (cntl.Failed()) {
        LOG(ERROR) << "CreateCloneChunk failed: " << cntl.ErrorText();
        return -1;
    }

    CHUNK_OP_STATUS status = response.status();
    LOG_IF(ERROR, status) << "CreateCloneChunk failed: "
                          << CHUNK_OP_STATUS_Name(status);

    return status;
}

int ChunkServiceOp::RecoverChunk(struct ChunkServiceOpConf *opConf,
                                 ChunkID chunkId, off_t offset, size_t len) {
    PeerId leaderId(opConf->leaderPeer->address());
    brpc::Channel channel;
    channel.Init(leaderId.addr, NULL);
    ChunkService_Stub stub(&channel);
    brpc::Controller cntl;
    cntl.set_timeout_ms(opConf->rpcTimeout);

    ChunkRequest request;
    ChunkResponse response;
    request.set_optype(curve::chunkserver::CHUNK_OP_TYPE::CHUNK_OP_RECOVER);
    request.set_logicpoolid(opConf->logicPoolId);
    request.set_copysetid(opConf->copysetId);
    request.set_chunkid(chunkId);
    request.set_offset(offset);
    request.set_size(len);
    stub.RecoverChunk(&cntl, &request, &response, nullptr);

    if (cntl.Failed()) {
        LOG(ERROR) << "RecoverChunk failed: " << cntl.ErrorText();
        return -1;
    }

    CHUNK_OP_STATUS status = response.status();
    LOG_IF(ERROR, status != CHUNK_OP_STATUS_SUCCESS)
        << "RecoverChunk failed: " << CHUNK_OP_STATUS_Name(status);

    return status;
}

int ChunkServiceOp::GetChunkInfo(struct ChunkServiceOpConf *opConf,
                                 ChunkID chunkId, SequenceNum *curSn,
                                 SequenceNum *snapSn,
                                 std::string *redirectedLeader) {
    PeerId leaderId(opConf->leaderPeer->address());
    brpc::Channel channel;
    channel.Init(leaderId.addr, NULL);
    ChunkService_Stub stub(&channel);
    brpc::Controller cntl;
    cntl.set_timeout_ms(opConf->rpcTimeout);

    GetChunkInfoRequest request;
    GetChunkInfoResponse response;
    request.set_logicpoolid(opConf->logicPoolId);
    request.set_copysetid(opConf->copysetId);
    request.set_chunkid(chunkId);
    stub.GetChunkInfo(&cntl, &request, &response, nullptr);
    if (cntl.Failed()) {
        LOG(ERROR) << "GetChunkInfo failed: " << cntl.ErrorText();
        return -1;
    }

    CHUNK_OP_STATUS status = response.status();
    if (status == CHUNK_OP_STATUS_SUCCESS) {
        switch (response.chunksn().size()) {
        case 2:
            *snapSn = response.chunksn(1);
        case 1:
            *curSn = response.chunksn(0);
            break;
        case 0:
            return CHUNK_OP_STATUS_CHUNK_NOTEXIST;
        default:
            LOG(ERROR) << "GetChunkInfo failed, invalid chunkSn size: "
                       << response.chunksn().size();
            return -1;
        }
    }

    if (status == CHUNK_OP_STATUS_REDIRECTED) {
        *redirectedLeader = response.redirect();
    }

    LOG_IF(ERROR, status) << "GetChunkInfo failed: "
                          << CHUNK_OP_STATUS_Name(status);
    return status;
}

int ChunkServiceVerify::VerifyWriteChunk(ChunkID chunkId, SequenceNum sn,
                                         off_t offset, size_t len,
                                         const char *data, string *chunkData,
                                         const std::string& cloneFileSource,
                                         off_t cloneFileOffset) {
    int ret =
        ChunkServiceOp::WriteChunk(opConf_, chunkId, sn, offset, len, data,
                                   cloneFileSource, cloneFileOffset);

    LOG(INFO) << "Write Chunk " << chunkId << ", sn=" << sn
              << ", offset=" << offset << ", len=" << len
              << ", cloneFileSource=" << cloneFileSource
              << ", cloneFileOffset=" << cloneFileOffset << ", ret=" << ret;
    // chunk写成功，同步更新chunkData内容和existChunks_
    if (ret == CHUNK_OP_STATUS_SUCCESS && chunkData != nullptr)
        chunkData->replace(offset, len, data);
    existChunks_.insert(chunkId);

    return ret;
}

int ChunkServiceVerify::VerifyReadChunk(ChunkID chunkId, SequenceNum sn,
                                        off_t offset, size_t len,
                                        string *chunkData,
                                        const std::string& cloneFileSource,
                                        off_t cloneFileOffset) {
    std::string data(len, 0);
    bool chunk_existed = existChunks_.find(chunkId) != std::end(existChunks_);

    int ret =
        ChunkServiceOp::ReadChunk(opConf_, chunkId, sn, offset, len, &data,
                                  cloneFileSource, cloneFileOffset);
    LOG(INFO) << "Read Chunk " << chunkId << ", sn=" << sn
              << ", offset=" << offset << ", len=" << len
              << ", cloneFileSource=" << cloneFileSource
              << ", cloneFileOffset=" << cloneFileOffset << ", ret=" << ret;

    if (ret != CHUNK_OP_STATUS_SUCCESS &&
        ret != CHUNK_OP_STATUS_CHUNK_NOTEXIST) {
        return -1;
    } else if (ret == CHUNK_OP_STATUS_SUCCESS &&
               !chunk_existed &&
               cloneFileSource.empty()) {
        LOG(ERROR) << "Unexpected read success, chunk " << chunkId
                   << " should not existed";
        return -1;
    } else if (ret == CHUNK_OP_STATUS_CHUNK_NOTEXIST && chunk_existed) {
        LOG(ERROR) << "Unexpected read missing, chunk " << chunkId
                   << " must be existed";
        return -1;
    }

    // 读成功，则判断内容是否与chunkData吻合
    if (ret == CHUNK_OP_STATUS_SUCCESS && chunkData != nullptr) {
        // 查找数据有差异的位置
        uint32_t i = 0;
        while (i < len && data[i] == (*chunkData)[offset + i]) ++i;
        // 读取数据与预期相符，返回0
        if (i == len)
            return 0;

        LOG(ERROR) << "read data missmatch for chunk " << chunkId
                   << ", from offset " << offset + i << ", read "
                   << static_cast<int>(data[i]) << ", expected "
                   << static_cast<int>((*chunkData)[offset + i]);
        // 打印每个page的第一个字节
        uint32_t j = i / kPageSize * kPageSize;
        for (; j < len; j += kPageSize) {
            LOG(ERROR) << "chunk offset " << offset + j << ": read "
                       << static_cast<int>(data[j]) << ", expected "
                       << static_cast<int>((*chunkData)[offset + j]);
        }
        return -1;
    }

    return 0;
}

int ChunkServiceVerify::VerifyReadChunkSnapshot(ChunkID chunkId, SequenceNum sn,
                                                off_t offset, size_t len,
                                                string *chunkData) {
    std::string data(len, 0);
    bool chunk_existed = existChunks_.find(chunkId) != std::end(existChunks_);

    int ret = ChunkServiceOp::ReadChunkSnapshot(opConf_, chunkId, sn, offset,
                                                len, &data);
    LOG(INFO) << "Read Snapshot for Chunk " << chunkId << ", sn=" << sn
              << ", offset=" << offset << ", len=" << len << ", ret=" << ret;

    if (ret != CHUNK_OP_STATUS_SUCCESS &&
        ret != CHUNK_OP_STATUS_CHUNK_NOTEXIST) {
        return -1;
    } else if (ret == CHUNK_OP_STATUS_SUCCESS && !chunk_existed) {
        LOG(ERROR) << "Unexpected read success, chunk " << chunkId
                   << " should not existed";
        return -1;
    } else if (ret == CHUNK_OP_STATUS_CHUNK_NOTEXIST && chunk_existed) {
        LOG(ERROR) << "Unexpected read missing, chunk " << chunkId
                   << " must be existed";
        return -1;
    }

    // 读成功，则判断内容是否与chunkData吻合
    if (ret == CHUNK_OP_STATUS_SUCCESS && chunkData != nullptr) {
        // 查找数据有差异的位置
        int i = 0;
        while (i < len && data[i] == (*chunkData)[offset + i]) ++i;
        // 读取数据与预期相符，返回0
        if (i == len)
            return 0;

        LOG(ERROR) << "read data missmatch for chunk " << chunkId
                   << ", from offset " << offset + i << ", read "
                   << static_cast<int>(data[i]) << ", expected "
                   << static_cast<int>((*chunkData)[offset + i]);
        // 打印每个4KB的第一个字节
        int j = i / kPageSize * kPageSize;
        for (; j < len; j += kPageSize) {
            LOG(ERROR) << "chunk offset " << offset + j << ": read "
                       << static_cast<int>(data[j]) << ", expected "
                       << static_cast<int>((*chunkData)[offset + j]);
        }
        return -1;
    }

    return 0;
}

int ChunkServiceVerify::VerifyDeleteChunk(ChunkID chunkId, SequenceNum sn) {
    int ret = ChunkServiceOp::DeleteChunk(opConf_, chunkId, sn);
    LOG(INFO) << "Delete Chunk " << chunkId << ", sn " << sn << ", ret=" << ret;

    if (ret == CHUNK_OP_STATUS_SUCCESS)
        existChunks_.erase(chunkId);
    return ret;
}

int ChunkServiceVerify::VerifyDeleteChunkSnapshotOrCorrectSn(
    ChunkID chunkId, SequenceNum correctedSn) {
    int ret = ChunkServiceOp::DeleteChunkSnapshotOrCorrectSn(opConf_, chunkId,
                                                             correctedSn);
    LOG(INFO) << "DeleteSnapshot for Chunk " << chunkId
              << ", correctedSn=" << correctedSn << ", ret=" << ret;

    return ret;
}

int ChunkServiceVerify::VerifyCreateCloneChunk(ChunkID chunkId,
                                               const std::string &location,
                                               uint64_t correctedSn,
                                               uint64_t sn,
                                               uint64_t chunkSize) {
    int ret = ChunkServiceOp::CreateCloneChunk(opConf_, chunkId, location,
                                               correctedSn, sn, chunkSize);
    LOG(INFO) << "CreateCloneChunk for Chunk " << chunkId << ", from location "
              << location << ", correctedSn=" << correctedSn << ", sn=" << sn
              << ", chunkSize=" << chunkSize << ", ret=" << ret;

    if (ret == CHUNK_OP_STATUS_SUCCESS)
        existChunks_.insert(chunkId);

    return ret;
}

int ChunkServiceVerify::VerifyRecoverChunk(ChunkID chunkId, off_t offset,
                                           size_t len) {
    int ret = ChunkServiceOp::RecoverChunk(opConf_, chunkId, offset, len);
    LOG(INFO) << "RecoverChunk for Chunk " << chunkId << ", offset=" << offset
              << ", len=" << len << ", ret=" << ret;

    return ret;
}

int ChunkServiceVerify::VerifyGetChunkInfo(ChunkID chunkId,
                                           SequenceNum expCurSn,
                                           SequenceNum expSnapSn,
                                           string expLeader) {
    SequenceNum curSn = NULL_SN;
    SequenceNum snapSn = NULL_SN;
    string redirectedLeader = "";
    int ret = ChunkServiceOp::GetChunkInfo(opConf_, chunkId, &curSn, &snapSn,
                                           &redirectedLeader);
    LOG(INFO) << "GetChunkInfo for chunk " << chunkId << ", curSn=" << curSn
              << ", snapSn=" << snapSn << ", redirectedLeader={"
              << redirectedLeader << "}, ret=" << ret;

    bool chunk_existed = existChunks_.find(chunkId) != std::end(existChunks_);
    switch (ret) {
    case CHUNK_OP_STATUS_SUCCESS:
        // 如果curSn或snapSn与预期不符，则返回-1
        LOG_IF(ERROR, (curSn != expCurSn || snapSn != expSnapSn))
            << "GetChunkInfo for " << chunkId << " failed, curSn=" << curSn
            << ", expected " << expCurSn << "; snapSn=" << snapSn
            << ", expected " << expSnapSn;
        return (curSn != expCurSn || snapSn != expSnapSn) ? -1 : 0;

    case CHUNK_OP_STATUS_CHUNK_NOTEXIST:
        // 如果chunk预期存在，则返回-1
        LOG_IF(ERROR, chunk_existed)
            << "Unexpected GetChunkInfo NOTEXIST, chunk " << chunkId
            << " must be existed";
        return chunk_existed ? -1 : 0;

    case CHUNK_OP_STATUS_REDIRECTED:
        // 如果返回的redirectedLeader与给定的不符，则返回-1
        LOG_IF(ERROR, expLeader != redirectedLeader)
            << "GetChunkInfo failed, redirected to " << redirectedLeader
            << ", expected " << expLeader;
        return (expLeader != redirectedLeader) ? -1 : 0;

    default:
        LOG(ERROR) << "GetChunkInfo for " << chunkId << "failed, ret=" << ret;
        return -1;
    }

    LOG(ERROR) << "GetChunkInfo for " << chunkId << "failed, Illgal branch";
    return -1;
}

}  // namespace chunkserver
}  // namespace curve
