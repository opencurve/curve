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
 * Created Date: Saturday March 30th 2019
 * Author: yangyaokai
 */

#include <gtest/gtest.h>
#include <gmock/gmock.h>
#include <glog/logging.h>
#include <google/protobuf/stubs/callback.h>

#include <tuple>

#include "src/chunkserver/clone_core.h"
#include "src/chunkserver/copyset_node.h"
#include "src/chunkserver/op_request.h"
#include "test/chunkserver/mock_copyset_node.h"
#include "test/chunkserver/clone/clone_test_util.h"
#include "test/chunkserver/clone/mock_clone_copyer.h"
#include "test/chunkserver/datastore/mock_datastore.h"
#include "src/fs/local_filesystem.h"

namespace curve {
namespace chunkserver {

using curve::chunkserver::CHUNK_OP_TYPE;
using curve::fs::FileSystemType;
using curve::fs::LocalFsFactory;

ACTION_TEMPLATE(SaveBraftTask, HAS_1_TEMPLATE_PARAMS(int, k),
                AND_1_VALUE_PARAMS(value)) {
    auto input = static_cast<braft::Task>(::testing::get<k>(args));
    auto output = static_cast<braft::Task *>(value);
    output->data->swap(*input.data);
    output->done = input.done;
}

const LogicPoolID LOGICPOOL_ID = 1;
const CopysetID COPYSET_ID = 1;
const ChunkID CHUNK_ID = 1;

class CloneCoreTest
    : public testing::TestWithParam<
          std::tuple<ChunkSizeType, ChunkSizeType, PageSizeType>> {
 public:
    void SetUp() {
        chunksize_ = std::get<0>(GetParam());
        blocksize_ = std::get<1>(GetParam());
        pagesize_ = std::get<1>(GetParam());

        datastore_ = std::make_shared<MockDataStore>();
        copyer_ = std::make_shared<MockChunkCopyer>();
        node_ = std::make_shared<MockCopysetNode>();
        FakeCopysetNode();
    }
    void TearDown() {
        Mock::VerifyAndClearExpectations(datastore_.get());
        Mock::VerifyAndClearExpectations(node_.get());
    }

    void FakeCopysetNode() {
        EXPECT_CALL(*node_, IsLeaderTerm()).WillRepeatedly(Return(true));
        EXPECT_CALL(*node_, GetDataStore()).WillRepeatedly(Return(datastore_));
        EXPECT_CALL(*node_, GetConcurrentApplyModule())
            .WillRepeatedly(Return(nullptr));
        EXPECT_CALL(*node_, GetAppliedIndex())
            .WillRepeatedly(Return(LAST_INDEX));
    }

    std::shared_ptr<ReadChunkRequest>
    GenerateReadRequest(CHUNK_OP_TYPE optype, off_t offset, size_t length) {
        ChunkRequest *readRequest = new ChunkRequest();
        readRequest->set_logicpoolid(LOGICPOOL_ID);
        readRequest->set_copysetid(COPYSET_ID);
        readRequest->set_chunkid(CHUNK_ID);
        readRequest->set_optype(optype);
        readRequest->set_offset(offset);
        readRequest->set_size(length);
        brpc::Controller *cntl = new brpc::Controller();
        ChunkResponse *response = new ChunkResponse();
        FakeChunkClosure *closure = new FakeChunkClosure();
        closure->SetCntl(cntl);
        closure->SetRequest(readRequest);
        closure->SetResponse(response);
        std::shared_ptr<ReadChunkRequest> req =
            std::make_shared<ReadChunkRequest>(node_, nullptr, cntl,
                                               readRequest, response, closure);
        return req;
    }

    void SetCloneParam(std::shared_ptr<ReadChunkRequest> readRequest) {
        ChunkRequest *request =
            const_cast<ChunkRequest *>(readRequest->GetChunkRequest());
        request->set_clonefilesource("/test");
        request->set_clonefileoffset(0);
    }

    void CheckTask(const braft::Task &task, off_t offset, size_t length,
                   char *buf) {
        butil::IOBuf data;
        ChunkRequest request;
        auto req = ChunkOpRequest::Decode(*task.data, &request, &data, 0,
                                          PeerId("127.0.0.1:8200:0"));
        auto preq = dynamic_cast<PasteChunkInternalRequest *>(req.get());
        ASSERT_TRUE(preq != nullptr);

        ASSERT_EQ(LOGICPOOL_ID, request.logicpoolid());
        ASSERT_EQ(COPYSET_ID, request.copysetid());
        ASSERT_EQ(CHUNK_ID, request.chunkid());
        ASSERT_EQ(offset, request.offset());
        ASSERT_EQ(length, request.size());
        ASSERT_EQ(memcmp(buf, data.to_string().c_str(), length), 0);
    }

 protected:
    ChunkSizeType chunksize_;
    ChunkSizeType blocksize_;
    PageSizeType pagesize_;

    std::shared_ptr<MockDataStore> datastore_;
    std::shared_ptr<MockCopysetNode> node_;
    std::shared_ptr<MockChunkCopyer> copyer_;
};

/**
 * Test CHUNK_OP_READ type request, requesting to read a chunk that is not a clone chunk
 * Result: Will not copy data from the remote end, directly read data from the local, and the result is returned as successful
 */
TEST_P(CloneCoreTest, ReadChunkTest1) {
    off_t offset = 0;
    size_t length = 5 * blocksize_;
    std::shared_ptr<CloneCore> core
        = std::make_shared<CloneCore>(SLICE_SIZE, true, copyer_);
    std::shared_ptr<ReadChunkRequest> readRequest
        = GenerateReadRequest(CHUNK_OP_TYPE::CHUNK_OP_READ, offset, length);
    // Will not copy data from the source
    EXPECT_CALL(*copyer_, DownloadAsync(_)).Times(0);
    // Obtain chunk information
    CSChunkInfo info;
    info.isClone = false;
    info.metaPageSize = pagesize_;
    info.chunkSize = chunksize_;
    info.blockSize = blocksize_;
    EXPECT_CALL(*datastore_, GetChunkInfo(_, _))
        .WillOnce(DoAll(SetArgPointee<1>(info), Return(CSErrorCode::Success)));
    // Reading Chunk Files
    EXPECT_CALL(*datastore_, ReadChunk(_, _, _, offset, length)).Times(1);
    // Update applied index
    EXPECT_CALL(*node_, UpdateAppliedIndex(_)).Times(1);
    // No PasteChunkRequest will be generated
    EXPECT_CALL(*node_, Propose(_)).Times(0);

    ASSERT_EQ(0, core->HandleReadRequest(readRequest, readRequest->Closure()));
    FakeChunkClosure *closure =
        reinterpret_cast<FakeChunkClosure *>(readRequest->Closure());
    ASSERT_TRUE(closure->isDone_);
    ASSERT_EQ(LAST_INDEX, closure->resContent_.appliedindex);
    ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS,
              closure->resContent_.status);
}

/**
 * Test CHUNK_OP_READ type request, the requested chunk to read is a clone chunk
 * Case1: All regions requested for reading have been written
 * Result1: Read all from local chunk
 * Case2: The requested read area has not been written
 * Result2: Read all from the source and generate a pass request
 * Case3: The requested read area has been partially written and partially unwritten
 * Result3: Read from the local chunk for regions that have been written, and read from the source for regions that have not been written, resulting in a pass request
 * Case4: The requested read area has been partially written, and the requested offset is not aligned with pagesize
 * Result4: Error returned
 */
TEST_P(CloneCoreTest, ReadChunkTest2) {
    off_t offset = 0;
    size_t length = 5 * blocksize_;
    CSChunkInfo info;
    info.isClone = true;
    info.metaPageSize = pagesize_;
    info.chunkSize = chunksize_;
    info.blockSize = blocksize_;
    info.bitmap = std::make_shared<Bitmap>(chunksize_ / blocksize_);
    std::shared_ptr<CloneCore> core
        = std::make_shared<CloneCore>(SLICE_SIZE, true, copyer_);
    // case1
    {
        info.bitmap->Set();
        // After each call to HandleReadRequest, it will be released by the closure
        std::shared_ptr<ReadChunkRequest> readRequest =
            GenerateReadRequest(CHUNK_OP_TYPE::CHUNK_OP_READ, offset, length);
        EXPECT_CALL(*copyer_, DownloadAsync(_)).Times(0);
        EXPECT_CALL(*datastore_, GetChunkInfo(_, _))
            .WillOnce(
                DoAll(SetArgPointee<1>(info), Return(CSErrorCode::Success)));
        // Reading Chunk Files
        char *chunkData = new char[length];
        memset(chunkData, 'a', length);
        EXPECT_CALL(*datastore_, ReadChunk(_, _, _, offset, length))
            .WillOnce(DoAll(SetArrayArgument<2>(chunkData, chunkData + length),
                            Return(CSErrorCode::Success)));
        // Update applied index
        EXPECT_CALL(*node_, UpdateAppliedIndex(_)).Times(1);
        // No PasteChunkRequest will be generated
        EXPECT_CALL(*node_, Propose(_)).Times(0);
        ASSERT_EQ(0,
                  core->HandleReadRequest(readRequest, readRequest->Closure()));
        FakeChunkClosure *closure =
            reinterpret_cast<FakeChunkClosure *>(readRequest->Closure());
        ASSERT_TRUE(closure->isDone_);
        ASSERT_EQ(LAST_INDEX, closure->resContent_.appliedindex);
        ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS,
                  closure->resContent_.status);
        ASSERT_EQ(
            memcmp(
                chunkData,
                closure->resContent_.attachment.to_string().c_str(),  // NOLINT
                length),
            0);
        delete[] chunkData;
    }

    // case2
    {
        info.bitmap->Clear();
        // After each call to HandleReadRequest, it will be released by the closure
        std::shared_ptr<ReadChunkRequest> readRequest =
            GenerateReadRequest(CHUNK_OP_TYPE::CHUNK_OP_READ, offset, length);
        char *cloneData = new char[length];
        memset(cloneData, 'b', length);
        EXPECT_CALL(*copyer_, DownloadAsync(_))
            .WillOnce(Invoke([&](DownloadClosure *closure) {
                brpc::ClosureGuard guard(closure);
                AsyncDownloadContext *context = closure->GetDownloadContext();
                memcpy(context->buf, cloneData, length);
            }));
        EXPECT_CALL(*datastore_, GetChunkInfo(_, _))
            .Times(2)
            .WillRepeatedly(
                DoAll(SetArgPointee<1>(info), Return(CSErrorCode::Success)));
        // Reading Chunk Files
        EXPECT_CALL(*datastore_, ReadChunk(_, _, _, offset, length)).Times(0);
        // Update applied index
        EXPECT_CALL(*node_, UpdateAppliedIndex(_)).Times(1);
        // Generate PasteChunkRequest
        braft::Task task;
        butil::IOBuf iobuf;
        task.data = &iobuf;
        EXPECT_CALL(*node_, Propose(_)).WillOnce(SaveBraftTask<0>(&task));

        ASSERT_EQ(0,
                  core->HandleReadRequest(readRequest, readRequest->Closure()));
        FakeChunkClosure *closure =
            reinterpret_cast<FakeChunkClosure *>(readRequest->Closure());
        ASSERT_TRUE(closure->isDone_);
        ASSERT_EQ(LAST_INDEX, closure->resContent_.appliedindex);
        ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS,
                  closure->resContent_.status);

        CheckTask(task, offset, length, cloneData);
        // After a normal proposal, the closure will be handed over to the concurrency layer for processing,
        // Since the node here is mock, it is necessary to proactively execute task.done.Run to release resources
        ASSERT_NE(nullptr, task.done);
        task.done->Run();
        ASSERT_EQ(
            memcmp(
                cloneData,
                closure->resContent_.attachment.to_string().c_str(),  // NOLINT
                length),
            0);
        delete[] cloneData;
    }

    // case3
    {
        info.bitmap->Clear();
        info.bitmap->Set(0, 2);
        // After each call to HandleReadRequest, it will be released by the closure
        std::shared_ptr<ReadChunkRequest> readRequest =
            GenerateReadRequest(CHUNK_OP_TYPE::CHUNK_OP_READ, offset, length);
        char *cloneData = new char[length];
        memset(cloneData, 'b', length);
        EXPECT_CALL(*copyer_, DownloadAsync(_))
            .WillOnce(Invoke([&](DownloadClosure *closure) {
                brpc::ClosureGuard guard(closure);
                AsyncDownloadContext *context = closure->GetDownloadContext();
                memcpy(context->buf, cloneData, length);
            }));
        EXPECT_CALL(*datastore_, GetChunkInfo(_, _))
            .Times(2)
            .WillRepeatedly(
                DoAll(SetArgPointee<1>(info), Return(CSErrorCode::Success)));
        // Reading Chunk Files
        char chunkData[pagesize_ +  2 * blocksize_];  // NOLINT(runtime/arrays)
        memset(chunkData, 'a', pagesize_ +  2 * blocksize_);
        EXPECT_CALL(*datastore_,
                    ReadChunk(_, _, _, 0, pagesize_ + 2 * blocksize_))
            .WillOnce(
                DoAll(SetArrayArgument<2>(
                          chunkData, chunkData + pagesize_ + 2 * blocksize_),
                      Return(CSErrorCode::Success)));
        // Update applied index
        EXPECT_CALL(*node_, UpdateAppliedIndex(_)).Times(1);
        // Generate PasteChunkRequest
        braft::Task task;
        butil::IOBuf iobuf;
        task.data = &iobuf;
        EXPECT_CALL(*node_, Propose(_)).WillOnce(SaveBraftTask<0>(&task));

        ASSERT_EQ(0,
                  core->HandleReadRequest(readRequest, readRequest->Closure()));
        FakeChunkClosure *closure =
            reinterpret_cast<FakeChunkClosure *>(readRequest->Closure());
        ASSERT_TRUE(closure->isDone_);
        ASSERT_EQ(LAST_INDEX, closure->resContent_.appliedindex);
        ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS,
                  closure->resContent_.status);

        CheckTask(task, offset, length, cloneData);
        // After a normal proposal, the closure will be handed over to the concurrency layer for processing,
        // Since the node here is mock, it is necessary to proactively execute task.done.Run to release resources
        ASSERT_NE(nullptr, task.done);
        task.done->Run();
        ASSERT_EQ(memcmp(chunkData,
                         closure->resContent_.attachment.to_string().c_str(),  // NOLINT
                         3 * blocksize_), 0);
        ASSERT_EQ(memcmp(cloneData,
                         closure->resContent_.attachment.to_string().c_str()  + 3 * blocksize_,  // NOLINT
                         2 * blocksize_), 0);
    }
    // case4
    {
        static unsigned int seed = time(nullptr);
        offset = blocksize_ + (rand_r(&seed) & 1 ? 1 : -1);
        length = 4 * blocksize_;
        info.bitmap->Clear();
        info.bitmap->Set(0, 2);
        // After each call to HandleReadRequest, it will be released by the closure
        std::shared_ptr<ReadChunkRequest> readRequest =
            GenerateReadRequest(CHUNK_OP_TYPE::CHUNK_OP_READ, offset, length);

        EXPECT_CALL(*datastore_, GetChunkInfo(_, _))
            .WillOnce(
                DoAll(SetArgPointee<1>(info), Return(CSErrorCode::Success)));
        EXPECT_CALL(*copyer_, DownloadAsync(_)).Times(0);
        // Reading Chunk Files
        EXPECT_CALL(*datastore_, ReadChunk(_, _, _, _, _)).Times(0);
        // Update applied index
        EXPECT_CALL(*node_, UpdateAppliedIndex(_)).Times(0);
        // Do not generate PasteChunkRequest
        braft::Task task;
        EXPECT_CALL(*node_, Propose(_)).Times(0);

        ASSERT_EQ(-1,
                  core->HandleReadRequest(readRequest, readRequest->Closure()));
        FakeChunkClosure *closure =
            reinterpret_cast<FakeChunkClosure *>(readRequest->Closure());
        ASSERT_TRUE(closure->isDone_);
        ASSERT_EQ(LAST_INDEX, closure->resContent_.appliedindex);
        ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_INVALID_REQUEST,
                  closure->resContent_.status);
    }
}

/**
 * Test CHUNK_OP_READ type request, the chunk requested for reading does not exist, but the request contains the source data address
 * Expected result: Download data from the source and generate a pass request
 */
TEST_P(CloneCoreTest, ReadChunkTest3) {
    off_t offset = 0;
    size_t length = 5 * blocksize_;
    CSChunkInfo info;
    info.isClone = true;
    info.metaPageSize = pagesize_;
    info.chunkSize = chunksize_;
    info.blockSize = blocksize_;
    info.bitmap = std::make_shared<Bitmap>(chunksize_ / pagesize_);
    std::shared_ptr<CloneCore> core
        = std::make_shared<CloneCore>(SLICE_SIZE, true, copyer_);

    // case1
    {
        info.bitmap->Clear();
        // After each call to HandleReadRequest, it will be released by the closure
        std::shared_ptr<ReadChunkRequest> readRequest =
            GenerateReadRequest(CHUNK_OP_TYPE::CHUNK_OP_READ, offset, length);
        SetCloneParam(readRequest);
        char *cloneData = new char[length];
        memset(cloneData, 'b', length);
        EXPECT_CALL(*copyer_, DownloadAsync(_))
            .WillOnce(Invoke([&](DownloadClosure *closure) {
                brpc::ClosureGuard guard(closure);
                AsyncDownloadContext *context = closure->GetDownloadContext();
                memcpy(context->buf, cloneData, length);
            }));
        EXPECT_CALL(*datastore_, GetChunkInfo(_, _))
            .Times(2)
            .WillRepeatedly(Return(CSErrorCode::ChunkNotExistError));
        // Reading Chunk Files
        EXPECT_CALL(*datastore_, ReadChunk(_, _, _, offset, length)).Times(0);
        // Update applied index
        EXPECT_CALL(*node_, UpdateAppliedIndex(_)).Times(1);
        // Generate PasteChunkRequest
        braft::Task task;
        butil::IOBuf iobuf;
        task.data = &iobuf;
        EXPECT_CALL(*node_, Propose(_)).WillOnce(SaveBraftTask<0>(&task));

        ASSERT_EQ(0,
                  core->HandleReadRequest(readRequest, readRequest->Closure()));
        FakeChunkClosure *closure =
            reinterpret_cast<FakeChunkClosure *>(readRequest->Closure());
        ASSERT_TRUE(closure->isDone_);
        ASSERT_EQ(LAST_INDEX, closure->resContent_.appliedindex);
        ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS,
                  closure->resContent_.status);

        CheckTask(task, offset, length, cloneData);
        // After a normal proposal, the closure will be handed over to the concurrency layer for processing,
        // Since the node here is mock, it is necessary to proactively execute task.done.Run to release resources
        ASSERT_NE(nullptr, task.done);
        task.done->Run();
        ASSERT_EQ(
            memcmp(
                cloneData,
                closure->resContent_.attachment.to_string().c_str(),  // NOLINT
                length),
            0);
        delete[] cloneData;
    }
}

/**
 * An error occurred during the execution of HandleReadRequest
 * Case1: Error in GetChunkInfo
 * Result1: Returns -1, and the response status changes to CHUNK_OP_STATUS_FAILURE_UNKNOWN
 * Case2: Error downloading
 * Result2: Returns -1, and the response status changes to CHUNK_OP_STATUS_FAILURE_UNKNOWN
 * Case3: Error in ReadChunk
 * Result3: Returns -1, and the response status changes to CHUNK_OP_STATUS_FAILURE_UNKNOWN
 */
TEST_P(CloneCoreTest, ReadChunkErrorTest) {
    off_t offset = 0;
    size_t length = 5 * blocksize_;
    CSChunkInfo info;
    info.isClone = true;
    info.metaPageSize = pagesize_;
    info.chunkSize = chunksize_;
    info.blockSize = blocksize_;
    info.bitmap = std::make_shared<Bitmap>(chunksize_ / blocksize_);
    info.bitmap->Clear();
    info.bitmap->Set(0, 2);
    std::shared_ptr<CloneCore> core =
        std::make_shared<CloneCore>(SLICE_SIZE, true, copyer_);
    // case1
    {
        std::shared_ptr<ReadChunkRequest> readRequest =
            GenerateReadRequest(CHUNK_OP_TYPE::CHUNK_OP_READ, offset, length);

        EXPECT_CALL(*datastore_, GetChunkInfo(_, _))
            .WillOnce(Return(CSErrorCode::InternalError));
        EXPECT_CALL(*copyer_, DownloadAsync(_)).Times(0);
        EXPECT_CALL(*datastore_, ReadChunk(_, _, _, _, _)).Times(0);
        EXPECT_CALL(*node_, Propose(_)).Times(0);

        ASSERT_EQ(-1,
                  core->HandleReadRequest(readRequest, readRequest->Closure()));
        FakeChunkClosure *closure =
            reinterpret_cast<FakeChunkClosure *>(readRequest->Closure());
        ASSERT_TRUE(closure->isDone_);
        ASSERT_EQ(LAST_INDEX, closure->resContent_.appliedindex);
        ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_FAILURE_UNKNOWN,
                  closure->resContent_.status);
    }
    // case2
    {
        std::shared_ptr<ReadChunkRequest> readRequest =
            GenerateReadRequest(CHUNK_OP_TYPE::CHUNK_OP_READ, offset, length);

        EXPECT_CALL(*datastore_, GetChunkInfo(_, _))
            .WillOnce(
                DoAll(SetArgPointee<1>(info), Return(CSErrorCode::Success)));
        char *cloneData = new char[length];
        memset(cloneData, 'b', length);
        EXPECT_CALL(*copyer_, DownloadAsync(_))
            .WillOnce(Invoke([&](DownloadClosure *closure) {
                brpc::ClosureGuard guard(closure);
                closure->SetFailed();
            }));
        EXPECT_CALL(*datastore_, ReadChunk(_, _, _, _, _)).Times(0);

        ASSERT_EQ(0,
                  core->HandleReadRequest(readRequest, readRequest->Closure()));
        FakeChunkClosure *closure =
            reinterpret_cast<FakeChunkClosure *>(readRequest->Closure());
        ASSERT_TRUE(closure->isDone_);
        ASSERT_EQ(LAST_INDEX, closure->resContent_.appliedindex);
        ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_FAILURE_UNKNOWN,
                  closure->resContent_.status);
        delete[] cloneData;
    }
    // case3
    {
        std::shared_ptr<ReadChunkRequest> readRequest =
            GenerateReadRequest(CHUNK_OP_TYPE::CHUNK_OP_READ, offset, length);

        EXPECT_CALL(*datastore_, GetChunkInfo(_, _))
            .Times(2)
            .WillRepeatedly(
                DoAll(SetArgPointee<1>(info), Return(CSErrorCode::Success)));
        char *cloneData = new char[length];
        memset(cloneData, 'b', length);
        EXPECT_CALL(*copyer_, DownloadAsync(_))
            .WillOnce(Invoke([&](DownloadClosure *closure) {
                brpc::ClosureGuard guard(closure);
                AsyncDownloadContext *context = closure->GetDownloadContext();
                memcpy(context->buf, cloneData, length);
            }));
        EXPECT_CALL(*datastore_, ReadChunk(_, _, _, _, _))
            .WillOnce(Return(CSErrorCode::InternalError));
        // Generate PasteChunkRequest
        braft::Task task;
        butil::IOBuf iobuf;
        task.data = &iobuf;
        EXPECT_CALL(*node_, Propose(_)).WillOnce(SaveBraftTask<0>(&task));

        ASSERT_EQ(0,
                  core->HandleReadRequest(readRequest, readRequest->Closure()));
        FakeChunkClosure *closure =
            reinterpret_cast<FakeChunkClosure *>(readRequest->Closure());
        ASSERT_TRUE(closure->isDone_);
        ASSERT_EQ(LAST_INDEX, closure->resContent_.appliedindex);
        ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_FAILURE_UNKNOWN,
                  closure->resContent_.status);

        CheckTask(task, offset, length, cloneData);
        // After a normal proposal, the closure will be handed over to the concurrency layer for processing,
        // Since the node here is mock, it is necessary to proactively execute task.done.Run to release resources
        ASSERT_NE(nullptr, task.done);
        task.done->Run();
        delete[] cloneData;
    }
}

/**
 * Test CHUNK_OP_RECOVER type request, the requested chunk is not a clone chunk
 *Result: Will not copy data remotely or read data locally, returns success directly
 */
TEST_P(CloneCoreTest, RecoverChunkTest1) {
    off_t offset = 0;
    size_t length = 5 * pagesize_;
    std::shared_ptr<CloneCore> core
        = std::make_shared<CloneCore>(SLICE_SIZE, true, copyer_);
    std::shared_ptr<ReadChunkRequest> readRequest
        = GenerateReadRequest(CHUNK_OP_TYPE::CHUNK_OP_RECOVER, offset, length);
    // Will not copy data from the sourc
    EXPECT_CALL(*copyer_, DownloadAsync(_)).Times(0);
    // Obtain chunk information
    CSChunkInfo info;
    info.isClone = false;
    info.metaPageSize = pagesize_;
    info.chunkSize = chunksize_;
    info.blockSize = blocksize_;
    EXPECT_CALL(*datastore_, GetChunkInfo(_, _))
        .WillOnce(DoAll(SetArgPointee<1>(info), Return(CSErrorCode::Success)));
    // Reading Chunk Files
    EXPECT_CALL(*datastore_, ReadChunk(_, _, _, _, _)).Times(0);
    // No PasteChunkRequest will be generated
    EXPECT_CALL(*node_, Propose(_)).Times(0);

    ASSERT_EQ(0, core->HandleReadRequest(readRequest, readRequest->Closure()));
    FakeChunkClosure *closure =
        reinterpret_cast<FakeChunkClosure *>(readRequest->Closure());
    ASSERT_TRUE(closure->isDone_);
    ASSERT_EQ(LAST_INDEX, closure->resContent_.appliedindex);
    ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS,
              closure->resContent_.status);
}

/**
 * Test CHUNK_OP_WRECOVER type request, the requested chunk is clone chunk
 * Case1: All areas requested for recovery have been written
 * Result1: Will not copy data, returns success directly
 * Case2: The requested area for recovery has not been written in whole or in part
 * Result2: Copy data from the remote end and generate a pass request
 */
TEST_P(CloneCoreTest, RecoverChunkTest2) {
    off_t offset = 0;
    size_t length = 5 * blocksize_;
    CSChunkInfo info;
    info.isClone = true;
    info.metaPageSize = pagesize_;
    info.chunkSize = chunksize_;
    info.blockSize = blocksize_;
    info.bitmap = std::make_shared<Bitmap>(chunksize_ / blocksize_);
    std::shared_ptr<CloneCore> core
        = std::make_shared<CloneCore>(SLICE_SIZE, true, copyer_);
    // case1
    {
        info.bitmap->Set();
        // After each call to HandleReadRequest, it will be released by the closure
        std::shared_ptr<ReadChunkRequest> readRequest = GenerateReadRequest(
            CHUNK_OP_TYPE::CHUNK_OP_RECOVER, offset, length);  // NOLINT
        EXPECT_CALL(*copyer_, DownloadAsync(_)).Times(0);
        EXPECT_CALL(*datastore_, GetChunkInfo(_, _))
            .WillOnce(
                DoAll(SetArgPointee<1>(info), Return(CSErrorCode::Success)));
        // Unable to read chunk files
        EXPECT_CALL(*datastore_, ReadChunk(_, _, _, _, _)).Times(0);
        // No PasteChunkRequest will be generated
        EXPECT_CALL(*node_, Propose(_)).Times(0);
        ASSERT_EQ(0,
                  core->HandleReadRequest(readRequest, readRequest->Closure()));
        FakeChunkClosure *closure =
            reinterpret_cast<FakeChunkClosure *>(readRequest->Closure());
        ASSERT_TRUE(closure->isDone_);
        ASSERT_EQ(LAST_INDEX, closure->resContent_.appliedindex);
        ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS,
                  closure->resContent_.status);
    }

    // case2
    {
        info.bitmap->Clear();
        // After each call to HandleReadRequest, it will be released by the closure
        std::shared_ptr<ReadChunkRequest> readRequest = GenerateReadRequest(
            CHUNK_OP_TYPE::CHUNK_OP_RECOVER, offset, length);  // NOLINT
        char *cloneData = new char[length];
        memset(cloneData, 'b', length);
        EXPECT_CALL(*copyer_, DownloadAsync(_))
            .WillOnce(Invoke([&](DownloadClosure *closure) {
                brpc::ClosureGuard guard(closure);
                AsyncDownloadContext *context = closure->GetDownloadContext();
                memcpy(context->buf, cloneData, length);
            }));
        EXPECT_CALL(*datastore_, GetChunkInfo(_, _))
            .WillOnce(
                DoAll(SetArgPointee<1>(info), Return(CSErrorCode::Success)));
        // Unable to read chunk files
        EXPECT_CALL(*datastore_, ReadChunk(_, _, _, offset, length)).Times(0);
        // Generate PasteChunkRequest
        braft::Task task;
        butil::IOBuf iobuf;
        task.data = &iobuf;
        EXPECT_CALL(*node_, Propose(_)).WillOnce(SaveBraftTask<0>(&task));

        ASSERT_EQ(0,
                  core->HandleReadRequest(readRequest, readRequest->Closure()));
        FakeChunkClosure *closure =
            reinterpret_cast<FakeChunkClosure *>(readRequest->Closure());
        // The closure has been forwarded to PasteRequest for processing, and the closure has not been executed yet
        ASSERT_FALSE(closure->isDone_);

        CheckTask(task, offset, length, cloneData);
        // After a normal proposal, the closure will be handed over to the concurrency layer for processing,
        // Since the node here is mock, it is necessary to proactively execute task.done.Run to release resources
        ASSERT_NE(nullptr, task.done);

        task.done->Run();
        ASSERT_TRUE(closure->isDone_);
        ASSERT_EQ(0, closure->resContent_.appliedindex);
        ASSERT_EQ(0, closure->resContent_.status);
        delete[] cloneData;
    }
}

// Case1: When reading a chunk, copy data from the remote end, but do not generate a pass request
// Case2: When recovering a chunk, copying data from the remote end will generate a pass request
TEST_P(CloneCoreTest, DisablePasteTest) {
    off_t offset = 0;
    size_t length = 5 * blocksize_;
    CSChunkInfo info;
    info.isClone = true;
    info.metaPageSize = pagesize_;
    info.chunkSize = chunksize_;
    info.blockSize = blocksize_;
    info.bitmap = std::make_shared<Bitmap>(chunksize_ / blocksize_);
    std::shared_ptr<CloneCore> core
        = std::make_shared<CloneCore>(SLICE_SIZE, false, copyer_);

    // case1
    {
        info.bitmap->Clear();
        // After each call to HandleReadRequest, it will be released by the closure
        std::shared_ptr<ReadChunkRequest> readRequest =
            GenerateReadRequest(CHUNK_OP_TYPE::CHUNK_OP_READ, offset, length);
        char *cloneData = new char[length];
        memset(cloneData, 'b', length);
        EXPECT_CALL(*copyer_, DownloadAsync(_))
            .WillOnce(Invoke([&](DownloadClosure *closure) {
                brpc::ClosureGuard guard(closure);
                AsyncDownloadContext *context = closure->GetDownloadContext();
                memcpy(context->buf, cloneData, length);
            }));
        EXPECT_CALL(*datastore_, GetChunkInfo(_, _))
            .Times(2)
            .WillRepeatedly(
                DoAll(SetArgPointee<1>(info), Return(CSErrorCode::Success)));
        // Reading Chunk Files
        EXPECT_CALL(*datastore_, ReadChunk(_, _, _, offset, length)).Times(0);
        // Update applied index
        EXPECT_CALL(*node_, UpdateAppliedIndex(_)).Times(1);

        // No paste chunk request will be generated
        EXPECT_CALL(*node_, Propose(_)).Times(0);

        ASSERT_EQ(0,
                  core->HandleReadRequest(readRequest, readRequest->Closure()));
        FakeChunkClosure *closure =
            reinterpret_cast<FakeChunkClosure *>(readRequest->Closure());
        ASSERT_TRUE(closure->isDone_);
        ASSERT_EQ(LAST_INDEX, closure->resContent_.appliedindex);
        ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS,
                  closure->resContent_.status);
        delete[] cloneData;
    }

    // case2
    {
        info.bitmap->Clear();
        // After each call to HandleReadRequest, it will be released by the closure
        std::shared_ptr<ReadChunkRequest> readRequest = GenerateReadRequest(
            CHUNK_OP_TYPE::CHUNK_OP_RECOVER, offset, length);  // NOLINT
        char *cloneData = new char[length];
        memset(cloneData, 'b', length);
        EXPECT_CALL(*copyer_, DownloadAsync(_))
            .WillOnce(Invoke([&](DownloadClosure *closure) {
                brpc::ClosureGuard guard(closure);
                AsyncDownloadContext *context = closure->GetDownloadContext();
                memcpy(context->buf, cloneData, length);
            }));
        EXPECT_CALL(*datastore_, GetChunkInfo(_, _))
            .WillOnce(
                DoAll(SetArgPointee<1>(info), Return(CSErrorCode::Success)));
        // Unable to read chunk files
        EXPECT_CALL(*datastore_, ReadChunk(_, _, _, offset, length)).Times(0);
        // Generate PasteChunkRequest
        braft::Task task;
        butil::IOBuf iobuf;
        task.data = &iobuf;
        EXPECT_CALL(*node_, Propose(_)).WillOnce(SaveBraftTask<0>(&task));

        ASSERT_EQ(0,
                  core->HandleReadRequest(readRequest, readRequest->Closure()));
        FakeChunkClosure *closure =
            reinterpret_cast<FakeChunkClosure *>(readRequest->Closure());
        // The closure has been forwarded to PasteRequest for processing, and the closure has not been executed yet
        ASSERT_FALSE(closure->isDone_);

        CheckTask(task, offset, length, cloneData);
        // After a normal proposal, the closure will be handed over to the concurrency layer for processing,
        // Since the node here is mock, it is necessary to proactively execute task.done.Run to release resources
        ASSERT_NE(nullptr, task.done);

        task.done->Run();
        ASSERT_TRUE(closure->isDone_);
        ASSERT_EQ(0, closure->resContent_.appliedindex);
        ASSERT_EQ(0, closure->resContent_.status);
        delete[] cloneData;
    }
}

INSTANTIATE_TEST_CASE_P(
    CloneCoreTest,
    CloneCoreTest,
    ::testing::Values(
        //                chunk size        block size,     metapagesize
        std::make_tuple(16U * 1024 * 1024, 4096U, 4096U),
        std::make_tuple(16U * 1024 * 1024, 4096U, 8192U),
        std::make_tuple(16U * 1024 * 1024, 512U, 8192U),
        std::make_tuple(16U * 1024 * 1024, 512U, 4096U * 4)));

}  // namespace chunkserver
}  // namespace curve
