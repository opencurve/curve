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
using curve::fs::LocalFsFactory;
using curve::fs::FileSystemType;

ACTION_TEMPLATE(SaveBraftTask,
                HAS_1_TEMPLATE_PARAMS(int, k),
                AND_1_VALUE_PARAMS(value)) {
    auto input = static_cast<braft::Task>(::testing::get<k>(args));
    auto output = static_cast<braft::Task*>(value);
    output->data->swap(*input.data);
    output->done = input.done;
}

const LogicPoolID LOGICPOOL_ID = 1;
const CopysetID COPYSET_ID = 1;
const ChunkID CHUNK_ID = 1;

class CloneCoreTest : public testing::Test {
 public:
    void SetUp() {
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
        EXPECT_CALL(*node_, IsLeaderTerm())
            .WillRepeatedly(Return(true));
        EXPECT_CALL(*node_, GetDataStore())
            .WillRepeatedly(Return(datastore_));
        EXPECT_CALL(*node_, GetConcurrentApplyModule())
            .WillRepeatedly(Return(nullptr));
        EXPECT_CALL(*node_, GetAppliedIndex())
            .WillRepeatedly(Return(LAST_INDEX));
    }

    std::shared_ptr<ReadChunkRequest> GenerateReadRequest(CHUNK_OP_TYPE optype,
                                                          off_t offset,
                                                          size_t length) {
        ChunkRequest* readRequest = new ChunkRequest();
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
            std::make_shared<ReadChunkRequest>(node_,
                                               nullptr,
                                               cntl,
                                               readRequest,
                                               response,
                                               closure);
        return req;
    }

    void SetCloneParam(std::shared_ptr<ReadChunkRequest> readRequest) {
        ChunkRequest* request =
            const_cast<ChunkRequest*>(readRequest->GetChunkRequest());
        request->set_clonefilesource("/test");
        request->set_clonefileoffset(0);
    }

    void CheckTask(const braft::Task& task,
                   off_t offset,
                   size_t length,
                   char* buf) {
        butil::IOBuf data;
        ChunkRequest request;
        auto req = ChunkOpRequest::Decode(*task.data,
                              &request, &data, 0, PeerId("0"));
        auto preq = dynamic_cast<PasteChunkInternalRequest*>(req.get());
        ASSERT_TRUE(preq != nullptr);

        ASSERT_EQ(LOGICPOOL_ID, request.logicpoolid());
        ASSERT_EQ(COPYSET_ID, request.copysetid());
        ASSERT_EQ(CHUNK_ID, request.chunkid());
        ASSERT_EQ(offset, request.offset());
        ASSERT_EQ(length, request.size());
        ASSERT_EQ(memcmp(buf, data.to_string().c_str(), length), 0);
    }

 protected:
    std::shared_ptr<MockDataStore> datastore_;
    std::shared_ptr<MockCopysetNode> node_;
    std::shared_ptr<MockChunkCopyer> copyer_;
};

/**
 * 测试CHUNK_OP_READ类型请求,请求读取的chunk不是clone chunk
 * result:不会从远端拷贝数据，直接从本地读取数据，结果返回成功
 */
TEST_F(CloneCoreTest, ReadChunkTest1) {
    off_t offset = 0;
    size_t length = 5 * PAGE_SIZE;
    std::shared_ptr<CloneCore> core
        = std::make_shared<CloneCore>(SLICE_SIZE, true, copyer_);
    std::shared_ptr<ReadChunkRequest> readRequest
        = GenerateReadRequest(CHUNK_OP_TYPE::CHUNK_OP_READ, offset, length);
    // 不会从源端拷贝数据
    EXPECT_CALL(*copyer_, DownloadAsync(_))
        .Times(0);
    // 获取chunk信息
    CSChunkInfo info;
    info.isClone = false;
    info.pageSize = PAGE_SIZE;
    EXPECT_CALL(*datastore_, GetChunkInfo(_, _))
        .WillOnce(DoAll(SetArgPointee<1>(info),
                        Return(CSErrorCode::Success)));
    // 读chunk文件
    EXPECT_CALL(*datastore_, ReadChunk(_, _, _, offset, length))
        .Times(1);
    // 更新 applied index
    EXPECT_CALL(*node_, UpdateAppliedIndex(_))
        .Times(1);
    // 不会产生PasteChunkRequest
    EXPECT_CALL(*node_, Propose(_))
        .Times(0);

    ASSERT_EQ(0, core->HandleReadRequest(readRequest,
                                          readRequest->Closure()));
    FakeChunkClosure* closure =
        reinterpret_cast<FakeChunkClosure*>(readRequest->Closure());
    ASSERT_TRUE(closure->isDone_);
    ASSERT_EQ(LAST_INDEX, closure->resContent_.appliedindex);
    ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS,
              closure->resContent_.status);
}

/**
 * 测试CHUNK_OP_READ类型请求,请求读取的chunk是clone chunk
 * case1:请求读取的区域全部被写过
 * result1:全部从本地chunk读取
 * case2:请求读取的区域都未被写过
 * result2:全部从源端读取，产生paste请求
 * case3:请求读取的区域有部分被写过，部分未被写过
 * result3:写过区域从本地chunk读取，未写过区域从源端读取，产生paste请求
 * case4:请求读取的区域部分被写过，请求的偏移未与pagesize对齐
 * result4:返回错误
 */
TEST_F(CloneCoreTest, ReadChunkTest2) {
    off_t offset = 0;
    size_t length = 5 * PAGE_SIZE;
    CSChunkInfo info;
    info.isClone = true;
    info.pageSize = PAGE_SIZE;
    info.chunkSize = CHUNK_SIZE;
    info.bitmap = std::make_shared<Bitmap>(CHUNK_SIZE / PAGE_SIZE);
    std::shared_ptr<CloneCore> core
        = std::make_shared<CloneCore>(SLICE_SIZE, true, copyer_);
    // case1
    {
        info.bitmap->Set();
        // 每次调HandleReadRequest后会被closure释放
        std::shared_ptr<ReadChunkRequest> readRequest
            = GenerateReadRequest(CHUNK_OP_TYPE::CHUNK_OP_READ, offset, length);
        EXPECT_CALL(*copyer_, DownloadAsync(_))
            .Times(0);
        EXPECT_CALL(*datastore_, GetChunkInfo(_, _))
            .WillOnce(DoAll(SetArgPointee<1>(info),
                            Return(CSErrorCode::Success)));
        // 读chunk文件
        char chunkData[length];  // NOLINT
        memset(chunkData, 'a', length);
        EXPECT_CALL(*datastore_, ReadChunk(_, _, _, offset, length))
            .WillOnce(DoAll(SetArrayArgument<2>(chunkData,
                                                chunkData + length),
                            Return(CSErrorCode::Success)));
        // 更新 applied index
        EXPECT_CALL(*node_, UpdateAppliedIndex(_))
            .Times(1);
        // 不会产生PasteChunkRequest
        EXPECT_CALL(*node_, Propose(_))
            .Times(0);
        ASSERT_EQ(0, core->HandleReadRequest(readRequest,
                                             readRequest->Closure()));
        FakeChunkClosure* closure =
            reinterpret_cast<FakeChunkClosure*>(readRequest->Closure());
        ASSERT_TRUE(closure->isDone_);
        ASSERT_EQ(LAST_INDEX, closure->resContent_.appliedindex);
        ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS,
                  closure->resContent_.status);
        ASSERT_EQ(memcmp(chunkData,
                         closure->resContent_.attachment.to_string().c_str(),  //NOLINT
                         length), 0);
    }

    // case2
    {
        info.bitmap->Clear();
        // 每次调HandleReadRequest后会被closure释放
        std::shared_ptr<ReadChunkRequest> readRequest
            = GenerateReadRequest(CHUNK_OP_TYPE::CHUNK_OP_READ, offset, length);
        char cloneData[length];  // NOLINT
        memset(cloneData, 'b', length);
        EXPECT_CALL(*copyer_, DownloadAsync(_))
            .WillOnce(Invoke([&](DownloadClosure* closure){
                brpc::ClosureGuard guard(closure);
                AsyncDownloadContext* context = closure->GetDownloadContext();
                memcpy(context->buf, cloneData, length);
            }));
        EXPECT_CALL(*datastore_, GetChunkInfo(_, _))
            .Times(2)
            .WillRepeatedly(DoAll(SetArgPointee<1>(info),
                                  Return(CSErrorCode::Success)));
        // 读chunk文件
        EXPECT_CALL(*datastore_, ReadChunk(_, _, _, offset, length))
            .Times(0);
        // 更新 applied index
        EXPECT_CALL(*node_, UpdateAppliedIndex(_))
            .Times(1);
        // 产生PasteChunkRequest
        braft::Task task;
        butil::IOBuf iobuf;
        task.data = &iobuf;
        EXPECT_CALL(*node_, Propose(_))
            .WillOnce(SaveBraftTask<0>(&task));

        ASSERT_EQ(0, core->HandleReadRequest(readRequest,
                                              readRequest->Closure()));
        FakeChunkClosure* closure =
            reinterpret_cast<FakeChunkClosure*>(readRequest->Closure());
        ASSERT_TRUE(closure->isDone_);
        ASSERT_EQ(LAST_INDEX, closure->resContent_.appliedindex);
        ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS,
                closure->resContent_.status);

        CheckTask(task, offset, length, cloneData);
        // 正常propose后，会将closure交给并发层处理，
        // 由于这里node是mock的，因此需要主动来执行task.done.Run来释放资源
        ASSERT_NE(nullptr, task.done);
        task.done->Run();
        ASSERT_EQ(memcmp(cloneData,
                         closure->resContent_.attachment.to_string().c_str(),  //NOLINT
                         length), 0);
    }

    // case3
    {
        info.bitmap->Clear();
        info.bitmap->Set(0, 2);
        // 每次调HandleReadRequest后会被closure释放
        std::shared_ptr<ReadChunkRequest> readRequest
            = GenerateReadRequest(CHUNK_OP_TYPE::CHUNK_OP_READ, offset, length);
        char cloneData[length];  // NOLINT
        memset(cloneData, 'b', length);
        EXPECT_CALL(*copyer_, DownloadAsync(_))
            .WillOnce(Invoke([&](DownloadClosure* closure){
                brpc::ClosureGuard guard(closure);
                AsyncDownloadContext* context = closure->GetDownloadContext();
                memcpy(context->buf, cloneData, length);
            }));
        EXPECT_CALL(*datastore_, GetChunkInfo(_, _))
            .Times(2)
            .WillRepeatedly(DoAll(SetArgPointee<1>(info),
                            Return(CSErrorCode::Success)));
        // 读chunk文件
        char chunkData[3 * PAGE_SIZE];
        memset(chunkData, 'a', 3 * PAGE_SIZE);
        EXPECT_CALL(*datastore_, ReadChunk(_, _, _, 0, 3 * PAGE_SIZE))
            .WillOnce(DoAll(SetArrayArgument<2>(chunkData,
                                                chunkData + 3 * PAGE_SIZE),
                            Return(CSErrorCode::Success)));
        // 更新 applied index
        EXPECT_CALL(*node_, UpdateAppliedIndex(_))
            .Times(1);
        // 产生PasteChunkRequest
        braft::Task task;
        butil::IOBuf iobuf;
        task.data = &iobuf;
        EXPECT_CALL(*node_, Propose(_))
            .WillOnce(SaveBraftTask<0>(&task));

        ASSERT_EQ(0, core->HandleReadRequest(readRequest,
                                              readRequest->Closure()));
        FakeChunkClosure* closure =
            reinterpret_cast<FakeChunkClosure*>(readRequest->Closure());
        ASSERT_TRUE(closure->isDone_);
        ASSERT_EQ(LAST_INDEX, closure->resContent_.appliedindex);
        ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS,
                  closure->resContent_.status);

        CheckTask(task, offset, length, cloneData);
        // 正常propose后，会将closure交给并发层处理，
        // 由于这里node是mock的，因此需要主动来执行task.done.Run来释放资源
        ASSERT_NE(nullptr, task.done);
        task.done->Run();
        ASSERT_EQ(memcmp(chunkData,
                         closure->resContent_.attachment.to_string().c_str(),  //NOLINT
                         3 * PAGE_SIZE), 0);
        ASSERT_EQ(memcmp(cloneData,
                         closure->resContent_.attachment.to_string().c_str()  + 3 * PAGE_SIZE,  //NOLINT
                         2 * PAGE_SIZE), 0);
    }
    // case4
    {
        offset = 1024;
        length = 4 * PAGE_SIZE;
        info.bitmap->Clear();
        info.bitmap->Set(0, 2);
        // 每次调HandleReadRequest后会被closure释放
        std::shared_ptr<ReadChunkRequest> readRequest
            = GenerateReadRequest(CHUNK_OP_TYPE::CHUNK_OP_READ, offset, length);

        EXPECT_CALL(*datastore_, GetChunkInfo(_, _))
            .WillOnce(DoAll(SetArgPointee<1>(info),
                            Return(CSErrorCode::Success)));
        EXPECT_CALL(*copyer_, DownloadAsync(_))
            .Times(0);
        // 读chunk文件
        EXPECT_CALL(*datastore_, ReadChunk(_, _, _, _, _))
            .Times(0);
        // 更新 applied index
        EXPECT_CALL(*node_, UpdateAppliedIndex(_))
            .Times(0);
        // 不产生PasteChunkRequest
        braft::Task task;
        EXPECT_CALL(*node_, Propose(_))
            .Times(0);

        ASSERT_EQ(-1, core->HandleReadRequest(readRequest,
                                               readRequest->Closure()));
        FakeChunkClosure* closure =
            reinterpret_cast<FakeChunkClosure*>(readRequest->Closure());
        ASSERT_TRUE(closure->isDone_);
        ASSERT_EQ(LAST_INDEX, closure->resContent_.appliedindex);
        ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_INVALID_REQUEST,
                  closure->resContent_.status);
    }
}

/**
 * 测试CHUNK_OP_READ类型请求,请求读取的chunk不存在，但是请求中包含源端数据地址
 * 预期结果：从源端下载数据，产生paste请求
 */
TEST_F(CloneCoreTest, ReadChunkTest3) {
    off_t offset = 0;
    size_t length = 5 * PAGE_SIZE;
    CSChunkInfo info;
    info.isClone = true;
    info.pageSize = PAGE_SIZE;
    info.chunkSize = CHUNK_SIZE;
    info.bitmap = std::make_shared<Bitmap>(CHUNK_SIZE / PAGE_SIZE);
    std::shared_ptr<CloneCore> core
        = std::make_shared<CloneCore>(SLICE_SIZE, true, copyer_);

    // case1
    {
        info.bitmap->Clear();
        // 每次调HandleReadRequest后会被closure释放
        std::shared_ptr<ReadChunkRequest> readRequest
            = GenerateReadRequest(CHUNK_OP_TYPE::CHUNK_OP_READ, offset, length);
        SetCloneParam(readRequest);
        char cloneData[length];  // NOLINT
        memset(cloneData, 'b', length);
        EXPECT_CALL(*copyer_, DownloadAsync(_))
            .WillOnce(Invoke([&](DownloadClosure* closure){
                brpc::ClosureGuard guard(closure);
                AsyncDownloadContext* context = closure->GetDownloadContext();
                memcpy(context->buf, cloneData, length);
            }));
        EXPECT_CALL(*datastore_, GetChunkInfo(_, _))
            .Times(2)
            .WillRepeatedly(Return(CSErrorCode::ChunkNotExistError));
        // 读chunk文件
        EXPECT_CALL(*datastore_, ReadChunk(_, _, _, offset, length))
            .Times(0);
        // 更新 applied index
        EXPECT_CALL(*node_, UpdateAppliedIndex(_))
            .Times(1);
        // 产生PasteChunkRequest
        braft::Task task;
        butil::IOBuf iobuf;
        task.data = &iobuf;
        EXPECT_CALL(*node_, Propose(_))
            .WillOnce(SaveBraftTask<0>(&task));

        ASSERT_EQ(0, core->HandleReadRequest(readRequest,
                                             readRequest->Closure()));
        FakeChunkClosure* closure =
            reinterpret_cast<FakeChunkClosure*>(readRequest->Closure());
        ASSERT_TRUE(closure->isDone_);
        ASSERT_EQ(LAST_INDEX, closure->resContent_.appliedindex);
        ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS,
                closure->resContent_.status);

        CheckTask(task, offset, length, cloneData);
        // 正常propose后，会将closure交给并发层处理，
        // 由于这里node是mock的，因此需要主动来执行task.done.Run来释放资源
        ASSERT_NE(nullptr, task.done);
        task.done->Run();
        ASSERT_EQ(memcmp(cloneData,
                         closure->resContent_.attachment.to_string().c_str(),  //NOLINT
                         length), 0);
    }
}

/**
 * 执行HandleReadRequest过程中出现错误
 * case1:GetChunkInfo时出错
 * result1:返回-1，response状态改为CHUNK_OP_STATUS_FAILURE_UNKNOWN
 * case2:Download时出错
 * result2:返回-1，response状态改为CHUNK_OP_STATUS_FAILURE_UNKNOWN
 * case3:ReadChunk时出错
 * result3:返回-1，response状态改为CHUNK_OP_STATUS_FAILURE_UNKNOWN
 */
TEST_F(CloneCoreTest, ReadChunkErrorTest) {
    off_t offset = 0;
    size_t length = 5 * PAGE_SIZE;
    CSChunkInfo info;
    info.isClone = true;
    info.pageSize = PAGE_SIZE;
    info.chunkSize = CHUNK_SIZE;
    info.bitmap = std::make_shared<Bitmap>(CHUNK_SIZE / PAGE_SIZE);
    info.bitmap->Clear();
    info.bitmap->Set(0, 2);
    std::shared_ptr<CloneCore> core
        = std::make_shared<CloneCore>(SLICE_SIZE, true, copyer_);
    // case1
    {
        std::shared_ptr<ReadChunkRequest> readRequest
            = GenerateReadRequest(CHUNK_OP_TYPE::CHUNK_OP_READ, offset, length);

        EXPECT_CALL(*datastore_, GetChunkInfo(_, _))
            .WillOnce(Return(CSErrorCode::InternalError));
        EXPECT_CALL(*copyer_, DownloadAsync(_))
            .Times(0);
        EXPECT_CALL(*datastore_, ReadChunk(_, _, _, _, _))
            .Times(0);
        EXPECT_CALL(*node_, Propose(_))
            .Times(0);

        ASSERT_EQ(-1, core->HandleReadRequest(readRequest,
                                               readRequest->Closure()));
        FakeChunkClosure* closure =
            reinterpret_cast<FakeChunkClosure*>(readRequest->Closure());
        ASSERT_TRUE(closure->isDone_);
        ASSERT_EQ(LAST_INDEX, closure->resContent_.appliedindex);
        ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_FAILURE_UNKNOWN,
                  closure->resContent_.status);
    }
    // case2
    {
        std::shared_ptr<ReadChunkRequest> readRequest
            = GenerateReadRequest(CHUNK_OP_TYPE::CHUNK_OP_READ, offset, length);

        EXPECT_CALL(*datastore_, GetChunkInfo(_, _))
            .WillOnce(DoAll(SetArgPointee<1>(info),
                            Return(CSErrorCode::Success)));
        char cloneData[length];  // NOLINT
        memset(cloneData, 'b', length);
        EXPECT_CALL(*copyer_, DownloadAsync(_))
            .WillOnce(Invoke([&](DownloadClosure* closure){
                brpc::ClosureGuard guard(closure);
                closure->SetFailed();
            }));
        EXPECT_CALL(*datastore_, ReadChunk(_, _, _, _, _))
            .Times(0);

        ASSERT_EQ(0, core->HandleReadRequest(readRequest,
                                              readRequest->Closure()));
        FakeChunkClosure* closure =
            reinterpret_cast<FakeChunkClosure*>(readRequest->Closure());
        ASSERT_TRUE(closure->isDone_);
        ASSERT_EQ(LAST_INDEX, closure->resContent_.appliedindex);
        ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_FAILURE_UNKNOWN,
                  closure->resContent_.status);
    }
    // case3
    {
        std::shared_ptr<ReadChunkRequest> readRequest
            = GenerateReadRequest(CHUNK_OP_TYPE::CHUNK_OP_READ, offset, length);

        EXPECT_CALL(*datastore_, GetChunkInfo(_, _))
            .Times(2)
            .WillRepeatedly(DoAll(SetArgPointee<1>(info),
                            Return(CSErrorCode::Success)));
        char cloneData[length];  // NOLINT
        memset(cloneData, 'b', length);
        EXPECT_CALL(*copyer_, DownloadAsync(_))
            .WillOnce(Invoke([&](DownloadClosure* closure){
                brpc::ClosureGuard guard(closure);
                AsyncDownloadContext* context = closure->GetDownloadContext();
                memcpy(context->buf, cloneData, length);
            }));
        EXPECT_CALL(*datastore_, ReadChunk(_, _, _, _, _))
            .WillOnce(Return(CSErrorCode::InternalError));
         // 产生PasteChunkRequest
        braft::Task task;
        butil::IOBuf iobuf;
        task.data = &iobuf;
        EXPECT_CALL(*node_, Propose(_))
            .WillOnce(SaveBraftTask<0>(&task));

        ASSERT_EQ(0, core->HandleReadRequest(readRequest,
                                              readRequest->Closure()));
        FakeChunkClosure* closure =
            reinterpret_cast<FakeChunkClosure*>(readRequest->Closure());
        ASSERT_TRUE(closure->isDone_);
        ASSERT_EQ(LAST_INDEX, closure->resContent_.appliedindex);
        ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_FAILURE_UNKNOWN,
                  closure->resContent_.status);

        CheckTask(task, offset, length, cloneData);
        // 正常propose后，会将closure交给并发层处理，
        // 由于这里node是mock的，因此需要主动来执行task.done.Run来释放资源
        ASSERT_NE(nullptr, task.done);
        task.done->Run();
    }
}

/**
 * 测试CHUNK_OP_RECOVER类型请求,请求的chunk不是clone chunk
 * result:不会从远端拷贝数据，也不会从本地读取数据，直接返回成功
 */
TEST_F(CloneCoreTest, RecoverChunkTest1) {
    off_t offset = 0;
    size_t length = 5 * PAGE_SIZE;
    std::shared_ptr<CloneCore> core
        = std::make_shared<CloneCore>(SLICE_SIZE, true, copyer_);
    std::shared_ptr<ReadChunkRequest> readRequest
        = GenerateReadRequest(CHUNK_OP_TYPE::CHUNK_OP_RECOVER, offset, length);
    // 不会从源端拷贝数据
    EXPECT_CALL(*copyer_, DownloadAsync(_))
        .Times(0);
    // 获取chunk信息
    CSChunkInfo info;
    info.isClone = false;
    info.pageSize = PAGE_SIZE;
    EXPECT_CALL(*datastore_, GetChunkInfo(_, _))
        .WillOnce(DoAll(SetArgPointee<1>(info),
                        Return(CSErrorCode::Success)));
    // 读chunk文件
    EXPECT_CALL(*datastore_, ReadChunk(_, _, _, _, _))
        .Times(0);
    // 不会产生PasteChunkRequest
    EXPECT_CALL(*node_, Propose(_))
        .Times(0);

    ASSERT_EQ(0, core->HandleReadRequest(readRequest,
                                          readRequest->Closure()));
    FakeChunkClosure* closure =
        reinterpret_cast<FakeChunkClosure*>(readRequest->Closure());
    ASSERT_TRUE(closure->isDone_);
    ASSERT_EQ(LAST_INDEX, closure->resContent_.appliedindex);
    ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS,
              closure->resContent_.status);
}

/**
 * 测试CHUNK_OP_RECOVER类型请求,请求的chunk是clone chunk
 * case1:请求恢复的区域全部被写过
 * result1:不会拷贝数据，直接返回成功
 * case2:请求恢复的区域全部或部分未被写过
 * result2:从远端拷贝数据，并产生paste请求
 */
TEST_F(CloneCoreTest, RecoverChunkTest2) {
    off_t offset = 0;
    size_t length = 5 * PAGE_SIZE;
    CSChunkInfo info;
    info.isClone = true;
    info.pageSize = PAGE_SIZE;
    info.chunkSize = CHUNK_SIZE;
    info.bitmap = std::make_shared<Bitmap>(CHUNK_SIZE / PAGE_SIZE);
    std::shared_ptr<CloneCore> core
        = std::make_shared<CloneCore>(SLICE_SIZE, true, copyer_);
    // case1
    {
        info.bitmap->Set();
        // 每次调HandleReadRequest后会被closure释放
        std::shared_ptr<ReadChunkRequest> readRequest
            = GenerateReadRequest(CHUNK_OP_TYPE::CHUNK_OP_RECOVER, offset, length); //NOLINT
        EXPECT_CALL(*copyer_, DownloadAsync(_))
            .Times(0);
        EXPECT_CALL(*datastore_, GetChunkInfo(_, _))
            .WillOnce(DoAll(SetArgPointee<1>(info),
                            Return(CSErrorCode::Success)));
        // 不会读chunk文件
        EXPECT_CALL(*datastore_, ReadChunk(_, _, _, _, _))
            .Times(0);
        // 不会产生PasteChunkRequest
        EXPECT_CALL(*node_, Propose(_))
            .Times(0);
        ASSERT_EQ(0, core->HandleReadRequest(readRequest,
                                              readRequest->Closure()));
        FakeChunkClosure* closure =
            reinterpret_cast<FakeChunkClosure*>(readRequest->Closure());
        ASSERT_TRUE(closure->isDone_);
        ASSERT_EQ(LAST_INDEX, closure->resContent_.appliedindex);
        ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS,
                  closure->resContent_.status);
    }

    // case2
    {
        info.bitmap->Clear();
        // 每次调HandleReadRequest后会被closure释放
        std::shared_ptr<ReadChunkRequest> readRequest
            = GenerateReadRequest(CHUNK_OP_TYPE::CHUNK_OP_RECOVER, offset, length);  //NOLINT
        char cloneData[length];  // NOLINT
        memset(cloneData, 'b', length);
        EXPECT_CALL(*copyer_, DownloadAsync(_))
            .WillOnce(Invoke([&](DownloadClosure* closure){
                brpc::ClosureGuard guard(closure);
                AsyncDownloadContext* context = closure->GetDownloadContext();
                memcpy(context->buf, cloneData, length);
            }));
        EXPECT_CALL(*datastore_, GetChunkInfo(_, _))
            .WillOnce(DoAll(SetArgPointee<1>(info),
                            Return(CSErrorCode::Success)));
        // 不会读chunk文件
        EXPECT_CALL(*datastore_, ReadChunk(_, _, _, offset, length))
            .Times(0);
        // 产生PasteChunkRequest
        braft::Task task;
        butil::IOBuf iobuf;
        task.data = &iobuf;
        EXPECT_CALL(*node_, Propose(_))
            .WillOnce(SaveBraftTask<0>(&task));

        ASSERT_EQ(0, core->HandleReadRequest(readRequest,
                                              readRequest->Closure()));
        FakeChunkClosure* closure =
            reinterpret_cast<FakeChunkClosure*>(readRequest->Closure());
        // closure被转交给PasteRequest处理，这里closure还未执行
        ASSERT_FALSE(closure->isDone_);

        CheckTask(task, offset, length, cloneData);
        // 正常propose后，会将closure交给并发层处理，
        // 由于这里node是mock的，因此需要主动来执行task.done.Run来释放资源
        ASSERT_NE(nullptr, task.done);

        task.done->Run();
        ASSERT_TRUE(closure->isDone_);
        ASSERT_EQ(0, closure->resContent_.appliedindex);
        ASSERT_EQ(0, closure->resContent_.status);
    }
}

// case1: read chunk时，从远端拷贝数据，但是不会产生paste请求
// case2: recover chunk时，从远端拷贝数据，会产生paste请求
TEST_F(CloneCoreTest, DisablePasteTest) {
    off_t offset = 0;
    size_t length = 5 * PAGE_SIZE;
    CSChunkInfo info;
    info.isClone = true;
    info.pageSize = PAGE_SIZE;
    info.chunkSize = CHUNK_SIZE;
    info.bitmap = std::make_shared<Bitmap>(CHUNK_SIZE / PAGE_SIZE);
    std::shared_ptr<CloneCore> core
        = std::make_shared<CloneCore>(SLICE_SIZE, false, copyer_);

    // case1
    {
        info.bitmap->Clear();
        // 每次调HandleReadRequest后会被closure释放
        std::shared_ptr<ReadChunkRequest> readRequest
            = GenerateReadRequest(CHUNK_OP_TYPE::CHUNK_OP_READ, offset, length);
        char cloneData[length];  // NOLINT
        memset(cloneData, 'b', length);
        EXPECT_CALL(*copyer_, DownloadAsync(_))
            .WillOnce(Invoke([&](DownloadClosure* closure){
                brpc::ClosureGuard guard(closure);
                AsyncDownloadContext* context = closure->GetDownloadContext();
                memcpy(context->buf, cloneData, length);
            }));
        EXPECT_CALL(*datastore_, GetChunkInfo(_, _))
            .Times(2)
            .WillRepeatedly(DoAll(SetArgPointee<1>(info),
                                  Return(CSErrorCode::Success)));
        // 读chunk文件
        EXPECT_CALL(*datastore_, ReadChunk(_, _, _, offset, length))
            .Times(0);
        // 更新 applied index
        EXPECT_CALL(*node_, UpdateAppliedIndex(_))
            .Times(1);

        // 不会产生paste chunk请求
        EXPECT_CALL(*node_, Propose(_))
            .Times(0);

        ASSERT_EQ(0, core->HandleReadRequest(readRequest,
                                             readRequest->Closure()));
        FakeChunkClosure* closure =
            reinterpret_cast<FakeChunkClosure*>(readRequest->Closure());
        ASSERT_TRUE(closure->isDone_);
        ASSERT_EQ(LAST_INDEX, closure->resContent_.appliedindex);
        ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS,
                closure->resContent_.status);
    }

    // case2
    {
        info.bitmap->Clear();
        // 每次调HandleReadRequest后会被closure释放
        std::shared_ptr<ReadChunkRequest> readRequest
            = GenerateReadRequest(CHUNK_OP_TYPE::CHUNK_OP_RECOVER, offset, length);  //NOLINT
        char cloneData[length];  // NOLINT
        memset(cloneData, 'b', length);
        EXPECT_CALL(*copyer_, DownloadAsync(_))
            .WillOnce(Invoke([&](DownloadClosure* closure){
                brpc::ClosureGuard guard(closure);
                AsyncDownloadContext* context = closure->GetDownloadContext();
                memcpy(context->buf, cloneData, length);
            }));
        EXPECT_CALL(*datastore_, GetChunkInfo(_, _))
            .WillOnce(DoAll(SetArgPointee<1>(info),
                            Return(CSErrorCode::Success)));
        // 不会读chunk文件
        EXPECT_CALL(*datastore_, ReadChunk(_, _, _, offset, length))
            .Times(0);
        // 产生PasteChunkRequest
        braft::Task task;
        butil::IOBuf iobuf;
        task.data = &iobuf;
        EXPECT_CALL(*node_, Propose(_))
            .WillOnce(SaveBraftTask<0>(&task));

        ASSERT_EQ(0, core->HandleReadRequest(readRequest,
                                              readRequest->Closure()));
        FakeChunkClosure* closure =
            reinterpret_cast<FakeChunkClosure*>(readRequest->Closure());
        // closure被转交给PasteRequest处理，这里closure还未执行
        ASSERT_FALSE(closure->isDone_);

        CheckTask(task, offset, length, cloneData);
        // 正常propose后，会将closure交给并发层处理，
        // 由于这里node是mock的，因此需要主动来执行task.done.Run来释放资源
        ASSERT_NE(nullptr, task.done);

        task.done->Run();
        ASSERT_TRUE(closure->isDone_);
        ASSERT_EQ(0, closure->resContent_.appliedindex);
        ASSERT_EQ(0, closure->resContent_.status);
    }
}

}  // namespace chunkserver
}  // namespace curve
