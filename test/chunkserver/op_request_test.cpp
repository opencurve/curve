/*
 * Project: curve
 * Created Date: 18-8-25
 * Author: wudemiao
 * Copyright (c) 2018 netease
 */

#include <gtest/gtest.h>
#include <butil/iobuf.h>
#include <butil/sys_byteorder.h>
#include <brpc/controller.h>

#include <string>

#include "proto/chunk.pb.h"
#include "src/chunkserver/op_request.h"

namespace curve {
namespace chunkserver {

TEST(ChunkOpRequestTest, encode) {
    LogicPoolID logicPoolId = 1;
    CopysetID copysetId = 10001;
    uint64_t chunkId = 12345;
    size_t offset = 0;
    uint32_t size = 16;
    CopysetNodeManager *copysetNodeManager = &CopysetNodeManager::GetInstance();
    // for write
    ChunkRequest request;
    request.set_optype(CHUNK_OP_TYPE::CHUNK_OP_WRITE);
    request.set_logicpoolid(logicPoolId);
    request.set_copysetid(copysetId);
    request.set_chunkid(chunkId);
    request.set_offset(offset);
    request.set_size(size);
    std::string str(size, 'a');
    brpc::Controller *cntl = new brpc::Controller();
    {
        // TODO(wudemiao)
        ChunkOpRequest chunkOpRequest(copysetNodeManager, cntl, &request, nullptr, nullptr);

        butil::IOBuf log;
        ASSERT_EQ(0, chunkOpRequest.Encode(&log));

        RequestType type = RequestType::UNKNOWN_OP;
        log.cutn(&type, sizeof(uint8_t));
        ASSERT_EQ(type, RequestType::CHUNK_OP);

        uint32_t metaSize = 0;
        log.cutn(&metaSize, sizeof(uint32_t));
        metaSize = butil::NetToHost32(metaSize);

        butil::IOBuf meta;
        log.cutn(&meta, metaSize);
        butil::IOBufAsZeroCopyInputStream wrapper(meta);
        ChunkRequest request;
        CHECK(request.ParseFromZeroCopyStream(&wrapper));
        butil::IOBuf data;
        data.swap(log);

        ASSERT_EQ(logicPoolId, request.logicpoolid());
        ASSERT_EQ(copysetId, request.copysetid());
        ASSERT_EQ(chunkId, request.chunkid());
        ASSERT_EQ(offset, request.offset());
        ASSERT_EQ(size, request.size());
    }

    // for read
    request.set_optype(CHUNK_OP_TYPE::CHUNK_OP_READ);
    request.set_logicpoolid(logicPoolId);
    request.set_copysetid(copysetId);
    request.set_chunkid(chunkId);
    request.set_offset(offset);
    request.set_size(size);
    {
        ChunkOpRequest chunkOpRequest(copysetNodeManager, cntl, &request, nullptr, nullptr);

        butil::IOBuf log;
        ASSERT_EQ(0, chunkOpRequest.Encode(&log));

        RequestType type = RequestType::UNKNOWN_OP;
        log.cutn(&type, sizeof(uint8_t));
        ASSERT_EQ(type, RequestType::CHUNK_OP);

        uint32_t metaSize = 0;
        log.cutn(&metaSize, sizeof(uint32_t));
        metaSize = butil::NetToHost32(metaSize);

        butil::IOBuf meta;
        log.cutn(&meta, metaSize);
        butil::IOBufAsZeroCopyInputStream wrapper(meta);
        ChunkRequest request;
        CHECK(request.ParseFromZeroCopyStream(&wrapper));
        butil::IOBuf data;
        data.swap(log);

        ASSERT_EQ(logicPoolId, request.logicpoolid());
        ASSERT_EQ(copysetId, request.copysetid());
        ASSERT_EQ(chunkId, request.chunkid());
        ASSERT_EQ(offset, request.offset());
        ASSERT_EQ(size, request.size());
    }

    // for detele
    request.set_optype(CHUNK_OP_TYPE::CHUNK_OP_DELETE);
    request.set_logicpoolid(logicPoolId);
    request.set_copysetid(copysetId);
    request.set_chunkid(chunkId);
    {
        ChunkOpRequest chunkOpRequest(copysetNodeManager, cntl, &request, nullptr, nullptr);

        butil::IOBuf log;
        ASSERT_EQ(0, chunkOpRequest.Encode(&log));

        RequestType type = RequestType::UNKNOWN_OP;
        log.cutn(&type, sizeof(uint8_t));
        ASSERT_EQ(type, RequestType::CHUNK_OP);

        uint32_t metaSize = 0;
        log.cutn(&metaSize, sizeof(uint32_t));
        metaSize = butil::NetToHost32(metaSize);

        butil::IOBuf meta;
        log.cutn(&meta, metaSize);
        butil::IOBufAsZeroCopyInputStream wrapper(meta);
        ChunkRequest request;
        CHECK(request.ParseFromZeroCopyStream(&wrapper));
        butil::IOBuf data;
        data.swap(log);

        ASSERT_EQ(logicPoolId, request.logicpoolid());
        ASSERT_EQ(copysetId, request.copysetid());
        ASSERT_EQ(chunkId, request.chunkid());
    }
}

TEST(ChunkSnapshotOpRequestTest, encode) {
    LogicPoolID logicPoolId = 1;
    CopysetID copysetId = 10001;
    uint64_t chunkId = 12345;
    uint64_t snapshotId = 9000;
    uint32_t size = 16;
    CopysetNodeManager *copysetNodeManager = &CopysetNodeManager::GetInstance();

    // for create
    ChunkSnapshotRequest request;
    request.set_optype(CHUNK_SNAPSHOT_OP_TYPE::CHUNK_SNAPSHOT_OP_CREATE);
    request.set_logicpoolid(logicPoolId);
    request.set_copysetid(copysetId);
    request.set_chunkid(chunkId);
    request.set_snapshotid(snapshotId);
    brpc::Controller *cntl = new brpc::Controller();
    {
        ChunkSnapshotOpRequest chunkSnapshotOpRequest(copysetNodeManager, cntl, &request, nullptr, nullptr);

        butil::IOBuf log;
        ASSERT_EQ(0, chunkSnapshotOpRequest.Encode(&log));

        RequestType type = RequestType::UNKNOWN_OP;
        log.cutn(&type, sizeof(uint8_t));
        ASSERT_EQ(type, RequestType::CHUNK_SNAPSHOT_OP);

        uint32_t metaSize = 0;
        log.cutn(&metaSize, sizeof(uint32_t));
        metaSize = butil::NetToHost32(metaSize);

        butil::IOBuf meta;
        log.cutn(&meta, metaSize);
        butil::IOBufAsZeroCopyInputStream wrapper(meta);
        ChunkSnapshotRequest request;
        CHECK(request.ParseFromZeroCopyStream(&wrapper));
        butil::IOBuf data;
        data.swap(log);

        ASSERT_EQ(logicPoolId, request.logicpoolid());
        ASSERT_EQ(copysetId, request.copysetid());
        ASSERT_EQ(chunkId, request.chunkid());
        ASSERT_EQ(snapshotId, request.snapshotid());
    }

    // for read
    request.set_optype(CHUNK_SNAPSHOT_OP_TYPE::CHUNK_SNAPSHOT_OP_READ);
    request.set_logicpoolid(logicPoolId);
    request.set_copysetid(copysetId);
    request.set_chunkid(chunkId);
    request.set_snapshotid(snapshotId);
    request.set_size(size);
    {
        ChunkSnapshotOpRequest chunkSnapshotOpRequest(copysetNodeManager, cntl, &request, nullptr, nullptr);

        butil::IOBuf log;
        ASSERT_EQ(0, chunkSnapshotOpRequest.Encode(&log));

        RequestType type = RequestType::UNKNOWN_OP;
        log.cutn(&type, sizeof(uint8_t));
        ASSERT_EQ(type, RequestType::CHUNK_SNAPSHOT_OP);

        uint32_t metaSize = 0;
        log.cutn(&metaSize, sizeof(uint32_t));
        metaSize = butil::NetToHost32(metaSize);

        butil::IOBuf meta;
        log.cutn(&meta, metaSize);
        butil::IOBufAsZeroCopyInputStream wrapper(meta);
        ChunkSnapshotRequest request;
        CHECK(request.ParseFromZeroCopyStream(&wrapper));
        butil::IOBuf data;
        data.swap(log);

        ASSERT_EQ(logicPoolId, request.logicpoolid());
        ASSERT_EQ(copysetId, request.copysetid());
        ASSERT_EQ(chunkId, request.chunkid());
        ASSERT_EQ(snapshotId, request.snapshotid());
        ASSERT_EQ(size, request.size());
    }

    // for detele
    request.set_optype(CHUNK_SNAPSHOT_OP_TYPE::CHUNK_SNAPSHOT_OP_DELETE);
    request.set_logicpoolid(logicPoolId);
    request.set_copysetid(copysetId);
    request.set_chunkid(chunkId);
    request.set_snapshotid(snapshotId);
    {
        ChunkSnapshotOpRequest chunkSnapshotOpRequest(copysetNodeManager, cntl, &request, nullptr, nullptr);

        butil::IOBuf log;
        ASSERT_EQ(0, chunkSnapshotOpRequest.Encode(&log));

        RequestType type = RequestType::UNKNOWN_OP;
        log.cutn(&type, sizeof(uint8_t));
        ASSERT_EQ(type, RequestType::CHUNK_SNAPSHOT_OP);

        uint32_t metaSize = 0;
        log.cutn(&metaSize, sizeof(uint32_t));
        metaSize = butil::NetToHost32(metaSize);

        butil::IOBuf meta;
        log.cutn(&meta, metaSize);
        butil::IOBufAsZeroCopyInputStream wrapper(meta);
        ChunkSnapshotRequest request;
        CHECK(request.ParseFromZeroCopyStream(&wrapper));
        butil::IOBuf data;
        data.swap(log);

        ASSERT_EQ(logicPoolId, request.logicpoolid());
        ASSERT_EQ(copysetId, request.copysetid());
        ASSERT_EQ(chunkId, request.chunkid());
        ASSERT_EQ(snapshotId, request.snapshotid());
    }
}

}  // namespace chunkserver
}  // namespace curve
