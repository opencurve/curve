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
#include "test/chunkserver/fake_datastore.h"

namespace curve {
namespace chunkserver {

using ::google::protobuf::io::ZeroCopyOutputStream;

class OpFakeClosure : public Closure {
 public:
    void Run() {}
    ~OpFakeClosure() {}
};

TEST(ChunkOpContextTest, encode) {
    LogicPoolID logicPoolId = 1;
    CopysetID copysetId = 10001;
    uint64_t chunkId = 12345;
    size_t offset = 0;
    uint32_t size = 16;
    uint64_t sn = 1;
    /* for write without COW */
    ChunkRequest request;
    request.set_optype(CHUNK_OP_TYPE::CHUNK_OP_WRITE);
    request.set_logicpoolid(logicPoolId);
    request.set_copysetid(copysetId);
    request.set_chunkid(chunkId);
    request.set_offset(offset);
    request.set_size(size);
    std::string str(size, 'a');
    brpc::Controller *cntl = new brpc::Controller();
    cntl->request_attachment().append(str.c_str(), size);
    {
        ChunkOpContext opCtx(cntl, &request, nullptr, nullptr);

        butil::IOBuf log;
        ASSERT_EQ(0, opCtx.Encode(&log));

        butil::IOBuf data;
        opCtx.Decode(log, &request, &data);

        ASSERT_EQ(CHUNK_OP_TYPE::CHUNK_OP_WRITE, request.optype());
        ASSERT_EQ(logicPoolId, request.logicpoolid());
        ASSERT_EQ(copysetId, request.copysetid());
        ASSERT_EQ(chunkId, request.chunkid());
        ASSERT_EQ(offset, request.offset());
        ASSERT_EQ(size, request.size());
        ASSERT_STREQ(str.c_str(), data.to_string().c_str());
    }
    /* for write with COW */
    request.set_optype(CHUNK_OP_TYPE::CHUNK_OP_WRITE);
    request.set_sn(sn);
    {
        ChunkOpContext opCtx(cntl, &request, nullptr, nullptr);

        butil::IOBuf log;
        ASSERT_EQ(0, opCtx.Encode(&log));

        butil::IOBuf data;
        opCtx.Decode(log, &request, &data);

        ASSERT_EQ(CHUNK_OP_TYPE::CHUNK_OP_WRITE, request.optype());
        ASSERT_EQ(logicPoolId, request.logicpoolid());
        ASSERT_EQ(copysetId, request.copysetid());
        ASSERT_EQ(chunkId, request.chunkid());
        ASSERT_EQ(offset, request.offset());
        ASSERT_EQ(size, request.size());
        ASSERT_EQ(sn, request.sn());
        ASSERT_STREQ(str.c_str(), data.to_string().c_str());
    }
    /* for read */
    request.set_optype(CHUNK_OP_TYPE::CHUNK_OP_READ);
    request.set_offset(offset);
    request.set_size(size);
    {
        ChunkOpContext opCtx(cntl, &request, nullptr, nullptr);

        butil::IOBuf log;
        ASSERT_EQ(0, opCtx.Encode(&log));

        butil::IOBuf data;
        opCtx.Decode(log, &request, &data);

        ASSERT_EQ(CHUNK_OP_TYPE::CHUNK_OP_READ, request.optype());
        ASSERT_EQ(logicPoolId, request.logicpoolid());
        ASSERT_EQ(copysetId, request.copysetid());
        ASSERT_EQ(chunkId, request.chunkid());
        ASSERT_EQ(offset, request.offset());
        ASSERT_EQ(size, request.size());
    }

    /* for detele */
    request.set_optype(CHUNK_OP_TYPE::CHUNK_OP_DELETE);
    {
        ChunkOpContext opCtx(cntl, &request, nullptr, nullptr);

        butil::IOBuf log;
        ASSERT_EQ(0, opCtx.Encode(&log));

        butil::IOBuf data;
        opCtx.Decode(log, &request, &data);

        ASSERT_EQ(CHUNK_OP_TYPE::CHUNK_OP_DELETE, request.optype());
        ASSERT_EQ(logicPoolId, request.logicpoolid());
        ASSERT_EQ(copysetId, request.copysetid());
        ASSERT_EQ(chunkId, request.chunkid());
    }
    /* for read snapshot */
    request.set_optype(CHUNK_OP_TYPE::CHUNK_OP_READ_SNAP);
    request.set_sn(sn);
    {
        ChunkOpContext opCtx(cntl, &request, nullptr, nullptr);

        butil::IOBuf log;
        ASSERT_EQ(0, opCtx.Encode(&log));

        butil::IOBuf data;
        opCtx.Decode(log, &request, &data);

        ASSERT_EQ(CHUNK_OP_TYPE::CHUNK_OP_READ_SNAP, request.optype());
        ASSERT_EQ(logicPoolId, request.logicpoolid());
        ASSERT_EQ(copysetId, request.copysetid());
        ASSERT_EQ(chunkId, request.chunkid());
        ASSERT_EQ(offset, request.offset());
        ASSERT_EQ(size, request.size());
        ASSERT_EQ(sn, request.sn());
    }
    /* for detele snapshot */
    request.set_optype(CHUNK_OP_TYPE::CHUNK_OP_DELETE_SNAP);
    {
        ChunkOpContext opCtx(cntl, &request, nullptr, nullptr);

        butil::IOBuf log;
        ASSERT_EQ(0, opCtx.Encode(&log));

        butil::IOBuf data;
        opCtx.Decode(log, &request, &data);

        ASSERT_EQ(CHUNK_OP_TYPE::CHUNK_OP_DELETE_SNAP, request.optype());
        ASSERT_EQ(logicPoolId, request.logicpoolid());
        ASSERT_EQ(copysetId, request.copysetid());
        ASSERT_EQ(chunkId, request.chunkid());
    }
    // encode 异常测试，null data
    {
        ChunkRequest request;
        ChunkOpContext opCtx(cntl, &request, nullptr, nullptr);
        ASSERT_EQ(-1, opCtx.Encode(nullptr));
    }
}

TEST(ChunkOpContextTest, OnApplyErrorTest) {
    LogicPoolID logicPoolId = 1;
    CopysetID copysetId = 10001;
    uint64_t chunkId = 12345;
    size_t offset = 0;
    uint32_t size = 16;
    uint64_t sn = 1;
    uint64_t appliedIndex = 12;

    Configuration conf;
    std::shared_ptr<CopysetNode> copysetNode =
        std::make_shared<CopysetNode>(logicPoolId, copysetId, conf);
    std::shared_ptr<LocalFileSystem> fs(LocalFsFactory::CreateFs(FileSystemType::EXT4, ""));    //NOLINT
    DataStoreOptions options;
    options.baseDir = "./test-temp";
    options.chunkSize = 16 * 1024 * 1024;
    options.pageSize = 4 * 1024;
    std::shared_ptr<FakeCSDataStore> dataStore =
        std::make_shared<FakeCSDataStore>(options, fs);
    copysetNode->SetCSDateStore(dataStore);

    // write data store error will cause fatal, so not test in here

    // read: data store error
    {
        ChunkRequest request;
        ChunkResponse response;
        request.set_optype(CHUNK_OP_TYPE::CHUNK_OP_READ);
        request.set_logicpoolid(logicPoolId);
        request.set_copysetid(copysetId);
        request.set_chunkid(chunkId);
        request.set_offset(offset);
        request.set_size(size);
        request.set_sn(sn);
        brpc::Controller *cntl = new brpc::Controller();
        ChunkOpContext opCtx(cntl, &request, &response, nullptr);
        dataStore->InjectError();
        OpFakeClosure done;
        opCtx.OnApply(copysetNode, appliedIndex, &done);
        ASSERT_FALSE(cntl->Failed());
        ASSERT_EQ(0, cntl->ErrorCode());
        ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_FAILURE_UNKNOWN,
                  response.status());
    }
    // read snapshot: data store error
    {
        ChunkRequest request;
        ChunkResponse response;
        request.set_optype(CHUNK_OP_TYPE::CHUNK_OP_READ_SNAP);
        request.set_logicpoolid(logicPoolId);
        request.set_copysetid(copysetId);
        request.set_chunkid(chunkId);
        request.set_offset(offset);
        request.set_size(size);
        request.set_sn(sn);
        brpc::Controller *cntl = new brpc::Controller();
        ChunkOpContext opCtx(cntl, &request, &response, nullptr);
        dataStore->InjectError();
        OpFakeClosure done;
        opCtx.OnApply(copysetNode, appliedIndex, &done);
        ASSERT_FALSE(cntl->Failed());
        ASSERT_EQ(0, cntl->ErrorCode());
        ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_FAILURE_UNKNOWN,
                  response.status());
    }
    // delete : data store error
    {
        ChunkRequest request;
        ChunkResponse response;
        request.set_optype(CHUNK_OP_TYPE::CHUNK_OP_DELETE);
        request.set_logicpoolid(logicPoolId);
        request.set_copysetid(copysetId);
        request.set_chunkid(chunkId);
        request.set_sn(sn);
        brpc::Controller *cntl = new brpc::Controller();
        ChunkOpContext opCtx(cntl, &request, &response, nullptr);
        dataStore->InjectError();
        OpFakeClosure done;
        opCtx.OnApply(copysetNode, appliedIndex, &done);
        ASSERT_FALSE(cntl->Failed());
        ASSERT_EQ(0, cntl->ErrorCode());
        ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_FAILURE_UNKNOWN,
                  response.status());
    }
    // delete snapshot: data store error
    {
        ChunkRequest request;
        ChunkResponse response;
        request.set_optype(CHUNK_OP_TYPE::CHUNK_OP_DELETE_SNAP);
        request.set_logicpoolid(logicPoolId);
        request.set_copysetid(copysetId);
        request.set_chunkid(chunkId);
        request.set_sn(sn);
        brpc::Controller *cntl = new brpc::Controller();
        ChunkOpContext opCtx(cntl, &request, &response, nullptr);
        dataStore->InjectError();
        OpFakeClosure done;
        opCtx.OnApply(copysetNode, appliedIndex, &done);
        ASSERT_FALSE(cntl->Failed());
        ASSERT_EQ(0, cntl->ErrorCode());
        ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_FAILURE_UNKNOWN,
                  response.status());
    }
}

TEST(ChunkOpContextTest, OnApplyFromLogTest) {
    LogicPoolID logicPoolId = 1;
    CopysetID copysetId = 10001;
    uint64_t chunkId = 12345;
    size_t offset = 0;
    uint32_t size = 16;
    uint64_t sn = 1;
    uint64_t appliedIndex = 12;

    Configuration conf;
    std::shared_ptr<CopysetNode> copysetNode =
        std::make_shared<CopysetNode>(logicPoolId, copysetId, conf);
    std::shared_ptr<LocalFileSystem> fs(LocalFsFactory::CreateFs(FileSystemType::EXT4, ""));    //NOLINT
    DataStoreOptions options;
    options.baseDir = "./test-temp";
    options.chunkSize = 16 * 1024 * 1024;
    options.pageSize = 4 * 1024;
    std::shared_ptr<FakeCSDataStore> dataStore =
        std::make_shared<FakeCSDataStore>(options, fs);
    copysetNode->SetCSDateStore(dataStore);

    // unknown op
    {
        ChunkRequest request;
        request.set_logicpoolid(1);
        request.set_copysetid(1);
        request.set_chunkid(1);
        request.set_sn(sn);
        request.set_offset(1);
        request.set_size(1);
        request.set_optype(CHUNK_OP_TYPE::CHUNK_OP_UNKNOWN);
        ChunkOpContext opCtx(nullptr, &request, nullptr, nullptr);
        butil::IOBuf data;
        opCtx.OnApplyFromLog(copysetNode, request, data, appliedIndex);
        ASSERT_FALSE(dataStore->HasInjectError());
    }

    // read
    {
        ChunkRequest request;
        LogicPoolID logicPoolID = 1;
        CopysetID copysetID = 1;
        request.set_logicpoolid(logicPoolID);
        request.set_copysetid(copysetID);
        request.set_chunkid(1);
        request.set_sn(sn);
        request.set_offset(1);
        request.set_size(1);
        request.set_optype(CHUNK_OP_TYPE::CHUNK_OP_READ);
        ChunkOpContext opCtx(nullptr, &request, nullptr, nullptr);
        butil::IOBuf data;
        opCtx.OnApplyFromLog(copysetNode, request, data, appliedIndex);
        ASSERT_FALSE(dataStore->HasInjectError());
    }
    // read snapshot
    {
        ChunkRequest request;
        LogicPoolID logicPoolID = 1;
        CopysetID copysetID = 1;
        request.set_logicpoolid(logicPoolID);
        request.set_copysetid(copysetID);
        request.set_chunkid(1);
        request.set_sn(sn);
        request.set_offset(1);
        request.set_size(1);
        request.set_optype(CHUNK_OP_TYPE::CHUNK_OP_READ_SNAP);
        ChunkOpContext opCtx(nullptr, &request, nullptr, nullptr);
        butil::IOBuf data;
        opCtx.OnApplyFromLog(copysetNode, request, data, appliedIndex);
        ASSERT_FALSE(dataStore->HasInjectError());
    }
    // delete
    {
        ChunkRequest request;
        LogicPoolID logicPoolID = 1;
        CopysetID copysetID = 1;
        request.set_logicpoolid(logicPoolID);
        request.set_copysetid(copysetID);
        request.set_chunkid(1);
        request.set_sn(sn);
        request.set_optype(CHUNK_OP_TYPE::CHUNK_OP_DELETE);
        ChunkOpContext opCtx(nullptr, &request, nullptr, nullptr);
        butil::IOBuf data;
        dataStore->InjectError();
        opCtx.OnApplyFromLog(copysetNode, request, data, appliedIndex);
        ASSERT_FALSE(dataStore->HasInjectError());
    }
    // delete snapshot
    {
        ChunkRequest request;
        LogicPoolID logicPoolID = 1;
        CopysetID copysetID = 1;
        request.set_logicpoolid(logicPoolID);
        request.set_copysetid(copysetID);
        request.set_chunkid(1);
        request.set_sn(sn);
        request.set_optype(CHUNK_OP_TYPE::CHUNK_OP_DELETE);
        ChunkOpContext opCtx(nullptr, &request, nullptr, nullptr);
        butil::IOBuf data;
        dataStore->InjectError();
        opCtx.OnApplyFromLog(copysetNode, request, data, appliedIndex);
        ASSERT_FALSE(dataStore->HasInjectError());
    }
}

}  // namespace chunkserver
}  // namespace curve
