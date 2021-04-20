/*
 *  Copyright (c) 2020 NetEase Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

/*
 * Project: curve
 * Created Date: 19-2-27
 * Author: wudemiao
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
#include "test/chunkserver/fake_datastore.h"

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
        ASSERT_EQ(0, opReq->Encode(&request,
                                   &cntl->request_attachment(),
                                   &log));

        butil::IOBuf data;
        auto req = ChunkOpRequest::Decode(log, &request, &data, 0, PeerId("127.0.0.1:9010:0"));
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
        ASSERT_EQ(0, opReq->Encode(&request,
                                   &cntl->request_attachment(),
                                   &log));

        butil::IOBuf data;
        auto req = ChunkOpRequest::Decode(log, &request, &data, 0, PeerId("127.0.0.1:9010:0"));
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

    /* for paste chunk */
    request.set_optype(CHUNK_OP_TYPE::CHUNK_OP_PASTE);
    {
        ChunkOpRequest *opReq
            = new PasteChunkInternalRequest(nodePtr,
                                            &request,
                                            nullptr,
                                            nullptr,
                                            nullptr);

        butil::IOBuf log;
        ASSERT_EQ(0, opReq->Encode(&request,
                                   &cntl->request_attachment(),
                                   &log));

        butil::IOBuf data;
        auto req = ChunkOpRequest::Decode(log, &request, &data, 0, PeerId("127.0.0.1:9010:0"));
        auto req1 = dynamic_cast<PasteChunkInternalRequest*>(req.get());
        ASSERT_TRUE(req1 != nullptr);

        ASSERT_EQ(CHUNK_OP_TYPE::CHUNK_OP_PASTE, request.optype());
        ASSERT_EQ(logicPoolId, request.logicpoolid());
        ASSERT_EQ(copysetId, request.copysetid());
        ASSERT_EQ(chunkId, request.chunkid());
        ASSERT_EQ(offset, request.offset());
        ASSERT_EQ(size, request.size());
        ASSERT_STREQ(str.c_str(), data.to_string().c_str());
        delete opReq;
    }
    /* for read */
    request.set_optype(CHUNK_OP_TYPE::CHUNK_OP_READ);
    request.set_offset(offset);
    request.set_size(size);
    {
        ChunkOpRequest *opReq
            = new ReadChunkRequest(nodePtr,
                                   nullptr,
                                   cntl,
                                   &request,
                                   nullptr,
                                   nullptr);

        butil::IOBuf log;
        ASSERT_EQ(0, opReq->Encode(&request,
                                   nullptr,
                                   &log));

        butil::IOBuf data;
        auto req = ChunkOpRequest::Decode(log, &request, &data, 0, PeerId("127.0.0.1:9010:0"));
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
        ASSERT_EQ(0, opReq->Encode(&request,
                                   nullptr,
                                   &log));

        butil::IOBuf data;
        auto req = ChunkOpRequest::Decode(log, &request, &data, 0, PeerId("127.0.0.1:9010:0"));
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
        ASSERT_EQ(0, opReq->Encode(&request,
                                   nullptr,
                                   &log));

        butil::IOBuf data;
        auto req = ChunkOpRequest::Decode(log, &request, &data, 0, PeerId("127.0.0.1:9010:0"));
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
    request.set_correctedsn(sn);
    {
        ChunkOpRequest *opReq
            = new DeleteSnapshotRequest(nodePtr,
                                        cntl,
                                        &request,
                                        nullptr,
                                        nullptr);

        butil::IOBuf log;
        ASSERT_EQ(0, opReq->Encode(&request,
                                   nullptr,
                                   &log));

        butil::IOBuf data;
        auto req = ChunkOpRequest::Decode(log, &request, &data, 0, PeerId("127.0.0.1:9010:0"));
        auto req1 = dynamic_cast<DeleteSnapshotRequest*>(req.get());
        ASSERT_TRUE(req1 != nullptr);

        ASSERT_EQ(CHUNK_OP_TYPE::CHUNK_OP_DELETE_SNAP, request.optype());
        ASSERT_EQ(logicPoolId, request.logicpoolid());
        ASSERT_EQ(copysetId, request.copysetid());
        ASSERT_EQ(chunkId, request.chunkid());
        ASSERT_EQ(sn, request.correctedsn());
        delete opReq;
    }
    /* for create clone chunk */
    request.set_optype(CHUNK_OP_TYPE::CHUNK_OP_CREATE_CLONE);
    std::string location("test@s3");
    request.set_location(location);
    request.set_size(options.chunkSize);
    request.set_sn(sn);
    {
        ChunkOpRequest *opReq
            = new CreateCloneChunkRequest(nodePtr,
                                          cntl,
                                          &request,
                                          nullptr,
                                          nullptr);

        butil::IOBuf log;
        ASSERT_EQ(0, opReq->Encode(&request,
                                   nullptr,
                                   &log));

        butil::IOBuf data;
        auto req = ChunkOpRequest::Decode(log, &request, &data, 0, PeerId("127.0.0.1:9010:0"));
        auto req1 = dynamic_cast<CreateCloneChunkRequest*>(req.get());
        ASSERT_TRUE(req1 != nullptr);

        ASSERT_EQ(CHUNK_OP_TYPE::CHUNK_OP_CREATE_CLONE, request.optype());
        ASSERT_EQ(logicPoolId, request.logicpoolid());
        ASSERT_EQ(copysetId, request.copysetid());
        ASSERT_EQ(chunkId, request.chunkid());
        ASSERT_EQ(options.chunkSize, request.size());
        ASSERT_EQ(location, request.location());
        ASSERT_EQ(sn, request.sn());
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
        ASSERT_EQ(0, opReq->Encode(&request,
                                   nullptr,
                                   &log));

        butil::IOBuf data;
        auto req = ChunkOpRequest::Decode(log, &request, &data, 0, PeerId("127.0.0.1:9010:0"));
        ASSERT_TRUE(req == nullptr);

        ASSERT_EQ(CHUNK_OP_TYPE::CHUNK_OP_UNKNOWN, request.optype());
        ASSERT_EQ(logicPoolId, request.logicpoolid());
        ASSERT_EQ(copysetId, request.copysetid());
        ASSERT_EQ(chunkId, request.chunkid());
        delete opReq;
    }
}

TEST(ChunkOpRequestTest, OnApplyErrorTest) {
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
            = new ReadChunkRequest(nodePtr,
                                   nullptr,
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
            = new ReadChunkRequest(nodePtr,
                                   nullptr,
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
        ASSERT_DEATH(opReq->OnApply(appliedIndex, &done), "");
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
        ASSERT_DEATH(opReq->OnApply(appliedIndex, &done), "");
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
        request.set_correctedsn(sn);
        brpc::Controller *cntl = new brpc::Controller();
        ChunkOpRequest *opReq = new DeleteSnapshotRequest(nodePtr,
                                                          cntl,
                                                          &request,
                                                          &response,
                                                          nullptr);
        dataStore->InjectError();
        OpFakeClosure done;
        ASSERT_DEATH(opReq->OnApply(appliedIndex, &done), "");
        delete opReq;
        delete cntl;
    }
    // delete snapshot: backward request error
    {
        ChunkRequest request;
        ChunkResponse response;
        request.set_optype(CHUNK_OP_TYPE::CHUNK_OP_DELETE_SNAP);
        request.set_logicpoolid(logicPoolId);
        request.set_copysetid(copysetId);
        request.set_chunkid(chunkId);
        request.set_correctedsn(sn);
        brpc::Controller *cntl = new brpc::Controller();
        ChunkOpRequest *opReq = new DeleteSnapshotRequest(nodePtr,
                                                          cntl,
                                                          &request,
                                                          &response,
                                                          nullptr);
        dataStore->InjectError(CSErrorCode::BackwardRequestError);
        OpFakeClosure done;
        opReq->OnApply(appliedIndex, &done);
        ASSERT_FALSE(cntl->Failed());
        ASSERT_EQ(0, cntl->ErrorCode());
        ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_BACKWARD,
                  response.status());
        delete opReq;
        delete cntl;
    }
    // write: data store error
    {
        ChunkRequest request;
        ChunkResponse response;
        request.set_optype(CHUNK_OP_TYPE::CHUNK_OP_WRITE);
        request.set_logicpoolid(logicPoolId);
        request.set_copysetid(copysetId);
        request.set_chunkid(chunkId);
        request.set_offset(offset);
        request.set_size(size);
        request.set_sn(sn);
        brpc::Controller *cntl = new brpc::Controller();
        ChunkOpRequest *opReq = new WriteChunkRequest(nodePtr,
                                                      cntl,
                                                      &request,
                                                      &response,
                                                      nullptr);
        dataStore->InjectError();
        OpFakeClosure done;
        ASSERT_DEATH(opReq->OnApply(appliedIndex, &done), "");
        delete opReq;
        delete cntl;
    }
    // write: backward request
    {
        ChunkRequest request;
        ChunkResponse response;
        request.set_optype(CHUNK_OP_TYPE::CHUNK_OP_WRITE);
        request.set_logicpoolid(logicPoolId);
        request.set_copysetid(copysetId);
        request.set_chunkid(chunkId);
        request.set_offset(offset);
        request.set_size(size);
        request.set_sn(sn);
        brpc::Controller *cntl = new brpc::Controller();
        ChunkOpRequest *opReq = new WriteChunkRequest(nodePtr,
                                                      cntl,
                                                      &request,
                                                      &response,
                                                      nullptr);
        dataStore->InjectError(CSErrorCode::BackwardRequestError);
        OpFakeClosure done;
        opReq->OnApply(appliedIndex, &done);
        ASSERT_FALSE(cntl->Failed());
        ASSERT_EQ(0, cntl->ErrorCode());
        ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_BACKWARD,
                  response.status());
        delete opReq;
        delete cntl;
    }
    // write: other failed
    {
        ChunkRequest request;
        ChunkResponse response;
        request.set_optype(CHUNK_OP_TYPE::CHUNK_OP_WRITE);
        request.set_logicpoolid(logicPoolId);
        request.set_copysetid(copysetId);
        request.set_chunkid(chunkId);
        request.set_offset(offset);
        request.set_size(size);
        request.set_sn(sn);
        brpc::Controller *cntl = new brpc::Controller();
        ChunkOpRequest *opReq = new WriteChunkRequest(nodePtr,
                                                      cntl,
                                                      &request,
                                                      &response,
                                                      nullptr);
        dataStore->InjectError(CSErrorCode::InvalidArgError);
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

TEST(ChunkOpRequestTest, OnApplyFromLogTest) {
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
        request.set_correctedsn(sn);
        request.set_optype(CHUNK_OP_TYPE::CHUNK_OP_DELETE_SNAP);
        butil::IOBuf data;
        DeleteSnapshotRequest req;
        req.OnApplyFromLog(dataStore, request, data);
        ASSERT_FALSE(dataStore->HasInjectError());
    }
}

}  // namespace chunkserver
}  // namespace curve
