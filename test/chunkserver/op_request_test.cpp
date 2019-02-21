/*
 * Project: curve
 * Created Date: 19-2-27
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
#include "src/chunkserver/op_request.h"
#include "test/chunkserver/mock_cs_data_store.h"

namespace curve {
namespace chunkserver {

using ::google::protobuf::io::ZeroCopyOutputStream;

class OpFakeClosure : public Closure {
 public:
    void Run() {}
    ~OpFakeClosure() {}
};

TEST(ChunkOpRequestTest, encode) {
    LogicPoolID logicPoolId = 1;
    CopysetID copysetId = 10001;
    uint64_t chunkId = 12345;
    size_t offset = 0;
    uint32_t size = 16;
    uint64_t sn = 1;

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

    Configuration conf;
    std::shared_ptr<CopysetNode> nodePtr =
        std::make_shared<CopysetNode>(logicPoolId, copysetId, conf);
    std::shared_ptr<LocalFileSystem>
        fs(LocalFsFactory::CreateFs(FileSystemType::EXT4, ""));    //NOLINT
    DataStoreOptions options;
    options.baseDir = "./test-temp";
    options.chunkSize = 16 * 1024 * 1024;
    options.pageSize = 4 * 1024;
    std::shared_ptr<FakeCSDataStore> dataStore =
        std::make_shared<FakeCSDataStore>(options, fs);
    nodePtr->SetCSDateStore(dataStore);

    {
        ChunkOpRequest *opReq
            = new WriteChunkRequest(nodePtr, cntl, &request, nullptr, nullptr);

        butil::IOBuf log;
        ASSERT_EQ(0, opReq->Encode(cntl, &request, &log));

        butil::IOBuf data;
        auto req = ChunkOpRequest::Decode(log, &request, &data);
        auto req1 = dynamic_cast<WriteChunkRequest*>(req.get());
        ASSERT_TRUE(req1 != nullptr);

        ASSERT_EQ(CHUNK_OP_TYPE::CHUNK_OP_WRITE, request.optype());
        ASSERT_EQ(logicPoolId, request.logicpoolid());
        ASSERT_EQ(copysetId, request.copysetid());
        ASSERT_EQ(chunkId, request.chunkid());
        ASSERT_EQ(offset, request.offset());
        ASSERT_EQ(size, request.size());
        ASSERT_STREQ(str.c_str(), data.to_string().c_str());
        delete opReq;
    }
    /* for write with COW */
    request.set_optype(CHUNK_OP_TYPE::CHUNK_OP_WRITE);
    request.set_sn(sn);
    {
        ChunkOpRequest *opReq
            = new WriteChunkRequest(nodePtr, cntl, &request, nullptr, nullptr);

        butil::IOBuf log;
        ASSERT_EQ(0, opReq->Encode(cntl, &request, &log));

        butil::IOBuf data;
        auto req = ChunkOpRequest::Decode(log, &request, &data);
        auto req1 = dynamic_cast<WriteChunkRequest*>(req.get());
        ASSERT_TRUE(req1 != nullptr);

        ASSERT_EQ(CHUNK_OP_TYPE::CHUNK_OP_WRITE, request.optype());
        ASSERT_EQ(logicPoolId, request.logicpoolid());
        ASSERT_EQ(copysetId, request.copysetid());
        ASSERT_EQ(chunkId, request.chunkid());
        ASSERT_EQ(offset, request.offset());
        ASSERT_EQ(size, request.size());
        ASSERT_EQ(sn, request.sn());
        ASSERT_STREQ(str.c_str(), data.to_string().c_str());
        delete opReq;
    }
    /* for read */
    request.set_optype(CHUNK_OP_TYPE::CHUNK_OP_READ);
    request.set_offset(offset);
    request.set_size(size);
    {
        ChunkOpRequest *opReq
            = new ReadChunkRequest(nodePtr, cntl, &request, nullptr, nullptr);

        butil::IOBuf log;
        ASSERT_EQ(0, opReq->Encode(cntl, &request, &log));

        butil::IOBuf data;
        auto req = ChunkOpRequest::Decode(log, &request, &data);
        auto req1 = dynamic_cast<ReadChunkRequest*>(req.get());
        ASSERT_TRUE(req1 != nullptr);

        ASSERT_EQ(CHUNK_OP_TYPE::CHUNK_OP_READ, request.optype());
        ASSERT_EQ(logicPoolId, request.logicpoolid());
        ASSERT_EQ(copysetId, request.copysetid());
        ASSERT_EQ(chunkId, request.chunkid());
        ASSERT_EQ(offset, request.offset());
        ASSERT_EQ(size, request.size());
        delete opReq;
    }

    /* for detele */
    request.set_optype(CHUNK_OP_TYPE::CHUNK_OP_DELETE);
    {
        ChunkOpRequest *opReq
            = new DeleteChunkRequest(nodePtr, cntl, &request, nullptr, nullptr);

        butil::IOBuf log;
        ASSERT_EQ(0, opReq->Encode(cntl, &request, &log));

        butil::IOBuf data;
        auto req = ChunkOpRequest::Decode(log, &request, &data);
        auto req1 = dynamic_cast<DeleteChunkRequest*>(req.get());
        ASSERT_TRUE(req1 != nullptr);

        ASSERT_EQ(CHUNK_OP_TYPE::CHUNK_OP_DELETE, request.optype());
        ASSERT_EQ(logicPoolId, request.logicpoolid());
        ASSERT_EQ(copysetId, request.copysetid());
        ASSERT_EQ(chunkId, request.chunkid());
        delete opReq;
    }
    /* for read snapshot */
    request.set_optype(CHUNK_OP_TYPE::CHUNK_OP_READ_SNAP);
    request.set_sn(sn);
    {
        ChunkOpRequest *opReq
            =
            new ReadSnapshotRequest(nodePtr, cntl, &request, nullptr, nullptr);

        butil::IOBuf log;
        ASSERT_EQ(0, opReq->Encode(cntl, &request, &log));

        butil::IOBuf data;
        auto req = ChunkOpRequest::Decode(log, &request, &data);
        auto req1 = dynamic_cast<ReadSnapshotRequest*>(req.get());
        ASSERT_TRUE(req1 != nullptr);

        ASSERT_EQ(CHUNK_OP_TYPE::CHUNK_OP_READ_SNAP, request.optype());
        ASSERT_EQ(logicPoolId, request.logicpoolid());
        ASSERT_EQ(copysetId, request.copysetid());
        ASSERT_EQ(chunkId, request.chunkid());
        ASSERT_EQ(offset, request.offset());
        ASSERT_EQ(size, request.size());
        ASSERT_EQ(sn, request.sn());
        delete opReq;
    }
    /* for detele snapshot */
    request.set_optype(CHUNK_OP_TYPE::CHUNK_OP_DELETE_SNAP);
    {
        ChunkOpRequest *opReq
            = new DeleteSnapshotRequest(nodePtr,
                                        cntl,
                                        &request,
                                        nullptr,
                                        nullptr);

        butil::IOBuf log;
        ASSERT_EQ(0, opReq->Encode(cntl, &request, &log));

        butil::IOBuf data;
        auto req = ChunkOpRequest::Decode(log, &request, &data);
        auto req1 = dynamic_cast<DeleteChunkRequest*>(req.get());
        ASSERT_TRUE(req1 != nullptr);

        ASSERT_EQ(CHUNK_OP_TYPE::CHUNK_OP_DELETE_SNAP, request.optype());
        ASSERT_EQ(logicPoolId, request.logicpoolid());
        ASSERT_EQ(copysetId, request.copysetid());
        ASSERT_EQ(chunkId, request.chunkid());
        delete opReq;
    }
    /* for unknown op */
    request.set_optype(CHUNK_OP_TYPE::CHUNK_OP_UNKNOWN);
    {
        ChunkOpRequest *opReq = new DeleteSnapshotRequest(nodePtr,
                                                          cntl,
                                                          &request,
                                                          nullptr,
                                                          nullptr);

        butil::IOBuf log;
        ASSERT_EQ(0, opReq->Encode(cntl, &request, &log));

        butil::IOBuf data;
        auto req = ChunkOpRequest::Decode(log, &request, &data);
        ASSERT_TRUE(req == nullptr);

        ASSERT_EQ(CHUNK_OP_TYPE::CHUNK_OP_UNKNOWN, request.optype());
        ASSERT_EQ(logicPoolId, request.logicpoolid());
        ASSERT_EQ(copysetId, request.copysetid());
        ASSERT_EQ(chunkId, request.chunkid());
        delete opReq;
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
    std::shared_ptr<CopysetNode> nodePtr =
        std::make_shared<CopysetNode>(logicPoolId, copysetId, conf);
    std::shared_ptr<LocalFileSystem>
        fs(LocalFsFactory::CreateFs(FileSystemType::EXT4, ""));    //NOLINT
    DataStoreOptions options;
    options.baseDir = "./test-temp";
    options.chunkSize = 16 * 1024 * 1024;
    options.pageSize = 4 * 1024;
    std::shared_ptr<FakeCSDataStore> dataStore =
        std::make_shared<FakeCSDataStore>(options, fs);
    nodePtr->SetCSDateStore(dataStore);

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
        ChunkOpRequest *opReq
            = new ReadChunkRequest(nodePtr, cntl, &request, &response, nullptr);
        dataStore->InjectError();
        OpFakeClosure done;
        opReq->OnApply(appliedIndex, &done);
        ASSERT_FALSE(cntl->Failed());
        ASSERT_EQ(0, cntl->ErrorCode());
        ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_FAILURE_UNKNOWN,
                  response.status());
        delete opReq;
        delete cntl;
    }
    // read: chunk not exist
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
        ChunkOpRequest *opReq
            = new ReadChunkRequest(nodePtr, cntl, &request, &response, nullptr);
        dataStore->InjectError(CSErrorCode::ChunkNotExistError);
        OpFakeClosure done;
        opReq->OnApply(appliedIndex, &done);
        ASSERT_FALSE(cntl->Failed());
        ASSERT_EQ(0, cntl->ErrorCode());
        ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_CHUNK_NOTEXIST,
                  response.status());
        delete opReq;
        delete cntl;
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
        ChunkOpRequest *opReq = new ReadSnapshotRequest(nodePtr,
                                                        cntl,
                                                        &request,
                                                        &response,
                                                        nullptr);
        dataStore->InjectError();
        OpFakeClosure done;
        opReq->OnApply(appliedIndex, &done);
        ASSERT_FALSE(cntl->Failed());
        ASSERT_EQ(0, cntl->ErrorCode());
        ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_FAILURE_UNKNOWN,
                  response.status());
        delete opReq;
        delete cntl;
    }
    // read snapshot: chunk not exist
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
        ChunkOpRequest *opReq = new ReadSnapshotRequest(nodePtr,
                                                        cntl,
                                                        &request,
                                                        &response,
                                                        nullptr);
        dataStore->InjectError(CSErrorCode::ChunkNotExistError);
        OpFakeClosure done;
        opReq->OnApply(appliedIndex, &done);
        ASSERT_FALSE(cntl->Failed());
        ASSERT_EQ(0, cntl->ErrorCode());
        ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_CHUNK_NOTEXIST,
                  response.status());
        delete opReq;
        delete cntl;
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
        ChunkOpRequest *opReq = new DeleteChunkRequest(nodePtr,
                                                       cntl,
                                                       &request,
                                                       &response,
                                                       nullptr);
        dataStore->InjectError();
        OpFakeClosure done;
        opReq->OnApply(appliedIndex, &done);
        ASSERT_FALSE(cntl->Failed());
        ASSERT_EQ(0, cntl->ErrorCode());
        ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_FAILURE_UNKNOWN,
                  response.status());
        delete opReq;
        delete cntl;
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
        ChunkOpRequest *opReq = new DeleteChunkRequest(nodePtr,
                                                       cntl,
                                                       &request,
                                                       &response,
                                                       nullptr);
        dataStore->InjectError();
        OpFakeClosure done;
        opReq->OnApply(appliedIndex, &done);
        ASSERT_FALSE(cntl->Failed());
        ASSERT_EQ(0, cntl->ErrorCode());
        ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_FAILURE_UNKNOWN,
                  response.status());
        delete opReq;
        delete cntl;
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
    std::shared_ptr<CopysetNode> nodePtr =
        std::make_shared<CopysetNode>(logicPoolId, copysetId, conf);
    std::shared_ptr<LocalFileSystem> fs(LocalFsFactory::CreateFs(FileSystemType::EXT4, ""));    //NOLINT
    DataStoreOptions options;
    options.baseDir = "./test-temp";
    options.chunkSize = 16 * 1024 * 1024;
    options.pageSize = 4 * 1024;
    std::shared_ptr<FakeCSDataStore> dataStore =
        std::make_shared<FakeCSDataStore>(options, fs);
    nodePtr->SetCSDateStore(dataStore);

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
        butil::IOBuf data;
        ReadChunkRequest req;
        req.OnApplyFromLog(dataStore, request, data);
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
        butil::IOBuf data;
        ReadSnapshotRequest req;
        req.OnApplyFromLog(dataStore, request, data);
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
        butil::IOBuf data;
        DeleteChunkRequest req;
        req.OnApplyFromLog(dataStore, request, data);
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
        request.set_optype(CHUNK_OP_TYPE::CHUNK_OP_DELETE_SNAP);
        butil::IOBuf data;
        DeleteSnapshotRequest req;
        req.OnApplyFromLog(dataStore, request, data);
        ASSERT_FALSE(dataStore->HasInjectError());
    }
}

}  // namespace chunkserver
}  // namespace curve
