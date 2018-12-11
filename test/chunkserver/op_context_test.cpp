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
#include <memory>

#include "proto/chunk.pb.h"
#include "src/chunkserver/copyset_node.h"
#include "src/chunkserver/copyset_node_manager.h"
#include "src/chunkserver/op_context.h"
#include "test/chunkserver/mock_cs_data_store.h"

namespace curve {
namespace chunkserver {

using ::google::protobuf::io::ZeroCopyOutputStream;

TEST(ChunkOpContextTest, encode) {
    LogicPoolID logicPoolId = 1;
    CopysetID copysetId = 10001;
    uint64_t chunkId = 12345;
    size_t offset = 0;
    uint32_t size = 16;
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
        ChunkOpContext opCtx(cntl, &request, nullptr, nullptr);

        butil::IOBuf log;
        ASSERT_EQ(0, opCtx.Encode(&log));

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

    /* for read */
    request.set_optype(CHUNK_OP_TYPE::CHUNK_OP_READ);
    request.set_logicpoolid(logicPoolId);
    request.set_copysetid(copysetId);
    request.set_chunkid(chunkId);
    request.set_offset(offset);
    request.set_size(size);
    {
        ChunkOpContext opCtx(cntl, &request, nullptr, nullptr);

        butil::IOBuf log;
        ASSERT_EQ(0, opCtx.Encode(&log));

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

    /* for detele */
    request.set_optype(CHUNK_OP_TYPE::CHUNK_OP_DELETE);
    request.set_logicpoolid(logicPoolId);
    request.set_copysetid(copysetId);
    request.set_chunkid(chunkId);
    {
        ChunkOpContext opCtx(cntl, &request, nullptr, nullptr);

        butil::IOBuf log;
        ASSERT_EQ(0, opCtx.Encode(&log));

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
    /* encode 异常测试，null data */
    {
        ChunkRequest request;
        ChunkOpContext opCtx(cntl, &request, nullptr, nullptr);
        ASSERT_EQ(-1, opCtx.Encode(nullptr));
    }
    /* OnApply 异常 */
    /* int ChunkOpRequest::OnApply(std::shared_ptr<CopysetNode> copysetNode) */
    {
        brpc::Controller cntl;
        ChunkRequest request;
        request.set_optype(CHUNK_OP_TYPE::CHUNK_OP_UNKNOWN);
        ChunkResponse response;
        ChunkOpContext opCtx(&cntl, &request, &response, nullptr);
        ASSERT_EQ(-1, opCtx.OnApply(nullptr));
        ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_INVALID_REQUEST,
                  response.status());
    }
    /* void ChunkOpRequest::OnApply(std::shared_ptr<CopysetNode> copysetNode,
                                    butil::IOBuf *log) { */
    {
        brpc::Controller cntl;
        ChunkRequest request;
        request.set_logicpoolid(1);
        request.set_copysetid(1);
        request.set_chunkid(1);
        request.set_offset(1);
        request.set_size(1);
        request.set_optype(CHUNK_OP_TYPE::CHUNK_OP_UNKNOWN);
        ChunkResponse response;
        ChunkOpContext opCtx(&cntl, &request, &response, nullptr);
        butil::IOBuf log;
        ASSERT_EQ(0, opCtx.Encode(&log));
        RequestType type = RequestType::UNKNOWN_OP;
        log.cutn(&type, sizeof(uint8_t));
        opCtx.OnApply(nullptr, &log);
    }
    /* data store 异常测试 */
    /* int ChunkOpRequest::OnApply(std::shared_ptr<CopysetNode> copysetNode)
     * read */
    {
        brpc::Controller cntl;
        ChunkRequest request;
        LogicPoolID logicPoolID = 1;
        CopysetID copysetID = 1;
        request.set_logicpoolid(logicPoolID);
        request.set_copysetid(copysetID);
        request.set_chunkid(1);
        request.set_offset(1);
        request.set_size(1);
        request.set_optype(CHUNK_OP_TYPE::CHUNK_OP_READ);
        ChunkResponse response;
        ChunkOpContext opCtx(&cntl, &request, &response, nullptr);
        Configuration conf;
        std::unique_ptr<FakeCSDataStore> fakePtr(new FakeCSDataStore());
        std::shared_ptr<CopysetNode>
            copysetPtr = std::make_shared<CopysetNode>(logicPoolID,
                                                       copysetID,
                                                       conf,
                                                       std::move(fakePtr));
        ASSERT_EQ(-1, opCtx.OnApply(copysetPtr));
        ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_FAILURE_UNKNOWN,
                  response.status());
    }
    /* void ChunkOpRequest::OnApply(std::shared_ptr<CopysetNode> copysetNode,
                                    butil::IOBuf *log) {
       read */
    {
        brpc::Controller cntl;
        ChunkRequest request;
        LogicPoolID logicPoolID = 1;
        CopysetID copysetID = 1;
        request.set_logicpoolid(logicPoolID);
        request.set_copysetid(copysetID);
        request.set_chunkid(1);
        request.set_offset(1);
        request.set_size(1);
        request.set_optype(CHUNK_OP_TYPE::CHUNK_OP_READ);
        ChunkResponse response;
        ChunkOpContext opCtx(&cntl, &request, &response, nullptr);
        butil::IOBuf log;
        ASSERT_EQ(0, opCtx.Encode(&log));
        RequestType type = RequestType::UNKNOWN_OP;
        log.cutn(&type, sizeof(uint8_t));
        ASSERT_EQ(RequestType::CHUNK_OP, type);
        Configuration conf;
        std::unique_ptr<FakeCSDataStore> fakePtr(new FakeCSDataStore());
        std::shared_ptr<CopysetNode> copysetPtr =
            std::make_shared<CopysetNode>(logicPoolID,
                                          copysetID,
                                          conf,
                                          std::move(fakePtr));
        opCtx.OnApply(copysetPtr, &log);
    }
}

/* 死亡测试：测试 LOG(FATAL) 分支1 */
TEST(ChunkOpContextDeathTest, fatal_test1) {
    /* int ChunkOpRequest::OnApply(std::shared_ptr<CopysetNode> copysetNode)
     * write */
    brpc::Controller cntl;
    ChunkRequest request;
    LogicPoolID logicPoolID = 1;
    CopysetID copysetID = 1;
    request.set_logicpoolid(logicPoolID);
    request.set_copysetid(copysetID);
    request.set_chunkid(1);
    request.set_offset(1);
    request.set_size(1);
    request.set_optype(CHUNK_OP_TYPE::CHUNK_OP_WRITE);
    cntl.request_attachment().append("a", 1);
    ChunkResponse response;
    ChunkOpContext opCtx(&cntl, &request, &response, nullptr);
    Configuration conf;
    std::unique_ptr<FakeCSDataStore> fakePtr(new FakeCSDataStore());
    std::shared_ptr<CopysetNode> copysetPtr =
        std::make_shared<CopysetNode>(logicPoolID,
                                      copysetID,
                                      conf,
                                      std::move(fakePtr));
    EXPECT_EXIT(opCtx.OnApply(copysetPtr),
                ::testing::KilledBySignal(SIGABRT),
                "");
}

/* 死亡测试：测试 LOG(FATAL) 分支2 */
TEST(ChunkOpContextDeathTest, fatal_test2) {
    /* void ChunkOpRequest::OnApply(std::shared_ptr<CopysetNode> copysetNode,
                                    butil::IOBuf *log) {
       write */
    brpc::Controller cntl;
    ChunkRequest request;
    LogicPoolID logicPoolID = 1;
    CopysetID copysetID = 1;
    request.set_logicpoolid(logicPoolID);
    request.set_copysetid(copysetID);
    request.set_chunkid(1);
    request.set_offset(1);
    request.set_size(1);
    request.set_optype(CHUNK_OP_TYPE::CHUNK_OP_WRITE);
    cntl.request_attachment().append("a", 1);
    ChunkResponse response;
    ChunkOpContext opCtx(&cntl, &request, &response, nullptr);
    butil::IOBuf log;
    ASSERT_EQ(0, opCtx.Encode(&log));
    RequestType type = RequestType::UNKNOWN_OP;
    log.cutn(&type, sizeof(uint8_t));
    Configuration conf;
    std::unique_ptr<FakeCSDataStore> fakePtr(new FakeCSDataStore());
    std::shared_ptr<CopysetNode> copysetPtr =
        std::make_shared<CopysetNode>(logicPoolID,
                                      copysetID,
                                      conf,
                                      std::move(fakePtr));
    EXPECT_EXIT(opCtx.OnApply(copysetPtr, &log),
                ::testing::KilledBySignal(SIGABRT),
                "");
}

}  // namespace chunkserver
}  // namespace curve
