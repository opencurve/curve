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
 * Created Date: Wednesday April 3rd 2019
 * Author: yangyaokai
 */

#include <gtest/gtest.h>
#include <gmock/gmock.h>
#include <glog/logging.h>
#include <memory>

#include "src/chunkserver/op_request.h"
#include "test/chunkserver/clone/clone_test_util.h"
#include "test/chunkserver/clone/mock_clone_manager.h"
#include "test/chunkserver/mock_copyset_node.h"
#include "test/chunkserver/datastore/mock_datastore.h"

namespace curve {
namespace chunkserver {

using curve::chunkserver::CHUNK_OP_TYPE;
using curve::chunkserver::concurrent::ConcurrentApplyOption;

const char PEER_STRING[] = "127.0.0.1:8200:0";

class FakeConcurrentApplyModule : public ConcurrentApplyModule {
 public:
    bool Init(int concurrentsize, int queuedepth) {
        ConcurrentApplyOption opt;
        opt.wconcurrentsize = opt.rconcurrentsize = concurrentsize;
        opt.wqueuedepth = opt.rqueuedepth = queuedepth;

        return ConcurrentApplyModule::Init(opt);
    }
};

class OpRequestTest
    : public testing::TestWithParam<
          std::tuple<ChunkSizeType, ChunkSizeType, PageSizeType>> {
 public:
    void SetUp() {
        chunksize_ = std::get<0>(GetParam());
        blocksize_ = std::get<1>(GetParam());
        metapagesize_ = std::get<2>(GetParam());

        node_ = std::make_shared<MockCopysetNode>();
        datastore_ = std::make_shared<MockDataStore>();
        cloneMgr_ = std::make_shared<MockCloneManager>();
        concurrentApplyModule_ = std::make_shared<FakeConcurrentApplyModule>();
        ASSERT_TRUE(concurrentApplyModule_->Init(10, 10));
        FakeCopysetNode();
        FakeCloneManager();
    }
    void TearDown() {
    }

    void FakeCopysetNode() {
        EXPECT_CALL(*node_, IsLeaderTerm())
            .WillRepeatedly(Return(true));
        EXPECT_CALL(*node_, GetDataStore())
            .WillRepeatedly(Return(datastore_));
        EXPECT_CALL(*node_, GetConcurrentApplyModule())
            .WillRepeatedly(Return(concurrentApplyModule_.get()));
        EXPECT_CALL(*node_, GetAppliedIndex())
            .WillRepeatedly(Return(LAST_INDEX));
        PeerId peer(PEER_STRING);
        EXPECT_CALL(*node_, GetLeaderId())
            .WillRepeatedly(Return(peer));
    }

    void FakeCloneManager() {
        EXPECT_CALL(*cloneMgr_, GenerateCloneTask(_, _))
            .WillRepeatedly(Return(nullptr));
        EXPECT_CALL(*cloneMgr_, IssueCloneTask(_))
            .WillRepeatedly(Return(true));
    }

 protected:
    ChunkSizeType chunksize_;
    ChunkSizeType blocksize_;
    PageSizeType metapagesize_;

    std::shared_ptr<MockCopysetNode> node_;
    std::shared_ptr<MockDataStore> datastore_;
    std::shared_ptr<MockCloneManager> cloneMgr_;
    std::shared_ptr<FakeConcurrentApplyModule>  concurrentApplyModule_;
};

TEST_P(OpRequestTest, CreateCloneTest) {
    // 创建CreateCloneChunkRequest
    LogicPoolID logicPoolId = 1;
    CopysetID copysetId = 10001;
    uint64_t chunkId = 12345;
    uint32_t size = chunksize_;
    uint64_t sn = 1;
    string location("test@cs");
    ChunkRequest* request = new ChunkRequest();
    request->set_logicpoolid(logicPoolId);
    request->set_copysetid(copysetId);
    request->set_chunkid(chunkId);
    request->set_optype(CHUNK_OP_CREATE_CLONE);
    request->set_location(location);
    request->set_size(size);
    request->set_sn(sn);
    brpc::Controller *cntl = new brpc::Controller();
    ChunkResponse *response = new ChunkResponse();
    UnitTestClosure *closure = new UnitTestClosure();
    closure->SetCntl(cntl);
    closure->SetRequest(request);
    closure->SetResponse(response);
    std::shared_ptr<CreateCloneChunkRequest> opReq =
        std::make_shared<CreateCloneChunkRequest>(node_,
                                                  cntl,
                                                  request,
                                                  response,
                                                  closure);
    /**
     * 测试Encode/Decode
     */
    {
        butil::IOBuf log;
        ASSERT_EQ(0, opReq->Encode(request, &cntl->request_attachment(), &log));

        butil::IOBuf data;
        auto req = ChunkOpRequest::Decode(log, request, &data, 0,
                                          PeerId("127.0.0.1:8200:0"));
        auto req1 = dynamic_cast<CreateCloneChunkRequest*>(req.get());
        ASSERT_TRUE(req1 != nullptr);

        ASSERT_EQ(CHUNK_OP_TYPE::CHUNK_OP_CREATE_CLONE, request->optype());
        ASSERT_EQ(logicPoolId, request->logicpoolid());
        ASSERT_EQ(copysetId, request->copysetid());
        ASSERT_EQ(chunkId, request->chunkid());
        ASSERT_EQ(size, request->size());
        ASSERT_EQ(location, request->location());
        ASSERT_EQ(sn, request->sn());
    }
    /**
     * 测试Process
     * 用例： node_->IsLeaderTerm() == false
     * 预期： 会要求转发请求，返回CHUNK_OP_STATUS_REDIRECTED
     */
    {
        // 设置预期
        EXPECT_CALL(*node_, IsLeaderTerm())
            .WillRepeatedly(Return(false));
        // PeerId leaderId(PEER_STRING);
        // EXPECT_CALL(*node_, GetLeaderId())
        //     .WillOnce(Return(leaderId));
        EXPECT_CALL(*node_, Propose(_))
            .Times(0);

        opReq->Process();

        // 验证结果
        ASSERT_TRUE(closure->isDone_);
        ASSERT_FALSE(response->has_appliedindex());
        ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_REDIRECTED,
                  closure->response_->status());
        // ASSERT_STREQ(closure->response_->redirect().c_str(), PEER_STRING);
    }
    /**
     * 测试Process
     * 用例： node_->IsLeaderTerm() == true
     * 预期： 会调用Propose，且不会调用closure
     */
    {
        // 重置closure
        closure->Reset();

        // 设置预期
        EXPECT_CALL(*node_, IsLeaderTerm())
            .WillRepeatedly(Return(true));
        braft::Task task;
        EXPECT_CALL(*node_, Propose(_))
            .WillOnce(SaveArg<0>(&task));

        opReq->Process();

        // 验证结果
        ASSERT_FALSE(closure->isDone_);
        ASSERT_FALSE(response->has_appliedindex());
        ASSERT_FALSE(closure->response_->has_status());
        // 由于这里node是mock的，因此需要主动来执行task.done.Run来释放资源
        ASSERT_NE(nullptr, task.done);
        task.done->Run();
        ASSERT_TRUE(closure->isDone_);
    }
    /**
     * 测试OnApply
     * 用例：CreateCloneChunk成功
     * 预期：返回 CHUNK_OP_STATUS_SUCCESS ，并更新apply index
     */
    {
        // 重置closure
        closure->Reset();

        // 设置预期
        EXPECT_CALL(*datastore_, CreateCloneChunk(_, _, _, _, _))
            .WillOnce(Return(CSErrorCode::Success));
        EXPECT_CALL(*node_, UpdateAppliedIndex(_))
            .Times(1);

        opReq->OnApply(3, closure);

        // 验证结果
        ASSERT_TRUE(closure->isDone_);
        ASSERT_EQ(LAST_INDEX, response->appliedindex());
        ASSERT_TRUE(response->has_status());
        ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS,
                  closure->response_->status());
    }
    /**
     * 测试OnApply
     * 用例：CreateCloneChunk失败
     * 预期：进程退出
     */
    {
        // 重置closure
        closure->Reset();

        // 设置预期
        EXPECT_CALL(*datastore_, CreateCloneChunk(_, _, _, _, _))
            .WillRepeatedly(Return(CSErrorCode::InternalError));
        EXPECT_CALL(*node_, UpdateAppliedIndex(_))
            .Times(0);

        ASSERT_DEATH(opReq->OnApply(3, closure), "");
    }
    /**
     * 测试OnApply
     * 用例：CreateCloneChunk失败,返回其他错误
     * 预期：进程退出
     */
    {
        // 重置closure
        closure->Reset();

        // 设置预期
        EXPECT_CALL(*datastore_, CreateCloneChunk(_, _, _, _, _))
            .WillRepeatedly(Return(CSErrorCode::InvalidArgError));
        EXPECT_CALL(*node_, UpdateAppliedIndex(_))
            .Times(0);

        opReq->OnApply(3, closure);

        // 验证结果
        ASSERT_TRUE(closure->isDone_);
        ASSERT_EQ(LAST_INDEX, response->appliedindex());
        ASSERT_TRUE(response->has_status());
        ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_FAILURE_UNKNOWN,
                  closure->response_->status());
    }
    /**
     * 测试 OnApplyFromLog
     * 用例：CreateCloneChunk成功
     * 预期：无返回
     */
    {
        // 重置closure
        closure->Reset();

        // 设置预期
        EXPECT_CALL(*datastore_, CreateCloneChunk(_, _, _, _, _))
            .WillOnce(Return(CSErrorCode::Success));

        butil::IOBuf data;
        opReq->OnApplyFromLog(datastore_, *request, data);
    }
    /**
     * 测试 OnApplyFromLog
     * 用例：CreateCloneChunk失败，返回InternalError
     * 预期：进程退出
     */
    {
        // 重置closure
        closure->Reset();

        // 设置预期
        EXPECT_CALL(*datastore_, CreateCloneChunk(_, _, _, _, _))
            .WillRepeatedly(Return(CSErrorCode::InternalError));

        butil::IOBuf data;
        ASSERT_DEATH(opReq->OnApplyFromLog(datastore_, *request, data), "");
    }
    /**
     * 测试 OnApplyFromLog
     * 用例：CreateCloneChunk失败，返回其他错误
     * 预期：进程退出
     */
    {
        // 重置closure
        closure->Reset();

        // 设置预期
        EXPECT_CALL(*datastore_, CreateCloneChunk(_, _, _, _, _))
            .WillRepeatedly(Return(CSErrorCode::InvalidArgError));

        butil::IOBuf data;
        opReq->OnApplyFromLog(datastore_, *request, data);
    }
    // 释放资源
    closure->Release();
}

TEST_P(OpRequestTest, PasteChunkTest) {
    // 生成临时的readrequest
    ChunkResponse *response = new ChunkResponse();
    std::shared_ptr<ReadChunkRequest> readChunkRequest =
        std::make_shared<ReadChunkRequest>(node_,
                                           nullptr,
                                           nullptr,
                                           nullptr,
                                           response,
                                           nullptr);

    // 创建PasteChunkRequest
    LogicPoolID logicPoolId = 1;
    CopysetID copysetId = 10001;
    uint64_t chunkId = 12345;
    uint32_t offset = 0;
    uint32_t size = 16;
    ChunkRequest* request = new ChunkRequest();
    request->set_logicpoolid(logicPoolId);
    request->set_copysetid(copysetId);
    request->set_chunkid(chunkId);
    request->set_optype(CHUNK_OP_PASTE);
    request->set_offset(offset);
    request->set_size(size);
    std::string str(size, 'a');
    butil::IOBuf cloneData;
    cloneData.append(str);

    UnitTestClosure *closure = new UnitTestClosure();
    closure->SetRequest(request);
    closure->SetResponse(response);
    std::shared_ptr<PasteChunkInternalRequest> opReq =
        std::make_shared<PasteChunkInternalRequest>(node_,
                                                    request,
                                                    response,
                                                    &cloneData,
                                                    closure);
    /**
     * 测试Encode/Decode
     */
    {
        butil::IOBuf log;
        butil::IOBuf input;
        input.append(str);
        ASSERT_EQ(0, opReq->Encode(request, &input, &log));

        butil::IOBuf data;
        auto req = ChunkOpRequest::Decode(log, request, &data, 0,
                                          PeerId("127.0.0.1:8200:0"));
        auto req1 = dynamic_cast<PasteChunkInternalRequest*>(req.get());
        ASSERT_TRUE(req1 != nullptr);

        ASSERT_EQ(CHUNK_OP_TYPE::CHUNK_OP_PASTE, request->optype());
        ASSERT_EQ(logicPoolId, request->logicpoolid());
        ASSERT_EQ(copysetId, request->copysetid());
        ASSERT_EQ(chunkId, request->chunkid());
        ASSERT_EQ(offset, request->offset());
        ASSERT_EQ(size, request->size());
        ASSERT_STREQ(str.c_str(), data.to_string().c_str());
    }
    /**
     * 测试Process
     * 用例： node_->IsLeaderTerm() == false
     * 预期： 会要求转发请求，返回CHUNK_OP_STATUS_REDIRECTED
     */
    {
        // 设置预期
        EXPECT_CALL(*node_, IsLeaderTerm())
            .WillRepeatedly(Return(false));
        // PeerId leaderId(PEER_STRING);
        // EXPECT_CALL(*node_, GetLeaderId())
        //     .WillOnce(Return(leaderId));
        EXPECT_CALL(*node_, Propose(_))
            .Times(0);

        opReq->Process();

        // 验证结果
        ASSERT_TRUE(closure->isDone_);
        ASSERT_FALSE(response->has_appliedindex());
        ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_REDIRECTED,
                  response->status());
        // ASSERT_STREQ(closure->response_->redirect().c_str(), PEER_STRING);
    }
    /**
     * 测试Process
     * 用例： node_->IsLeaderTerm() == true
     * 预期： 会调用Propose，且不会调用closure
     */
    {
        // 重置closure
        closure->Reset();

        // 设置预期
        EXPECT_CALL(*node_, IsLeaderTerm())
            .WillRepeatedly(Return(true));
        braft::Task task;
        EXPECT_CALL(*node_, Propose(_))
            .WillOnce(SaveArg<0>(&task));

        opReq->Process();

        // 验证结果
        ASSERT_FALSE(closure->isDone_);
        ASSERT_FALSE(response->has_appliedindex());
        ASSERT_FALSE(response->has_status());
        // 由于这里node是mock的，因此需要主动来执行task.done.Run来释放资源
        ASSERT_NE(nullptr, task.done);
        task.done->Run();
        ASSERT_TRUE(closure->isDone_);
    }
    /**
     * 测试OnApply
     * 用例：CreateCloneChunk成功
     * 预期：返回 CHUNK_OP_STATUS_SUCCESS ，并更新apply index
     */
    {
        // 重置closure
        closure->Reset();

        // 设置预期
        EXPECT_CALL(*datastore_, PasteChunk(_, _, _, _))
            .WillOnce(Return(CSErrorCode::Success));
        EXPECT_CALL(*node_, UpdateAppliedIndex(_))
            .Times(1);

        opReq->OnApply(3, closure);

        // 验证结果
        ASSERT_TRUE(closure->isDone_);
        ASSERT_EQ(LAST_INDEX, response->appliedindex());
        ASSERT_TRUE(response->has_status());
        ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS,
                  response->status());
    }
    /**
     * 测试OnApply
     * 用例：CreateCloneChunk失败,返回InternalError
     * 预期：进程退出
     */
    {
        // 重置closure
        closure->Reset();

        // 设置预期
        EXPECT_CALL(*datastore_, PasteChunk(_, _, _, _))
            .WillRepeatedly(Return(CSErrorCode::InternalError));
        EXPECT_CALL(*node_, UpdateAppliedIndex(_))
            .Times(0);

        ASSERT_DEATH(opReq->OnApply(3, closure), "");
    }
    /**
     * 测试OnApply
     * 用例：CreateCloneChunk失败,返回其他错误
     * 预期：进程退出
     */
    {
        // 重置closure
        closure->Reset();

        // 设置预期
        EXPECT_CALL(*datastore_, PasteChunk(_, _, _, _))
            .WillRepeatedly(Return(CSErrorCode::InvalidArgError));
        EXPECT_CALL(*node_, UpdateAppliedIndex(_))
            .Times(0);

        opReq->OnApply(3, closure);

        // 验证结果
        ASSERT_TRUE(closure->isDone_);
        ASSERT_EQ(LAST_INDEX, response->appliedindex());
        ASSERT_TRUE(response->has_status());
        ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_FAILURE_UNKNOWN,
                  response->status());
    }
    /**
     * 测试 OnApplyFromLog
     * 用例：CreateCloneChunk成功
     * 预期：无返回
     */
    {
        // 重置closure
        closure->Reset();

        // 设置预期
        EXPECT_CALL(*datastore_, PasteChunk(_, _, _, _))
            .WillOnce(Return(CSErrorCode::Success));

        butil::IOBuf data;
        opReq->OnApplyFromLog(datastore_, *request, data);
    }
    /**
     * 测试 OnApplyFromLog
     * 用例：CreateCloneChunk失败,返回InternalError
     * 预期：进程退出
     */
    {
        // 重置closure
        closure->Reset();

        // 设置预期
        EXPECT_CALL(*datastore_, PasteChunk(_, _, _, _))
            .WillRepeatedly(Return(CSErrorCode::InternalError));

        butil::IOBuf data;
        ASSERT_DEATH(opReq->OnApplyFromLog(datastore_, *request, data), "");
    }
    /**
     * 测试 OnApplyFromLog
     * 用例：CreateCloneChunk失败，返回其他错误
     * 预期：进程退出
     */
    {
        // 重置closure
        closure->Reset();

        // 设置预期
        EXPECT_CALL(*datastore_, PasteChunk(_, _, _, _))
            .WillRepeatedly(Return(CSErrorCode::InvalidArgError));

        butil::IOBuf data;
        opReq->OnApplyFromLog(datastore_, *request, data);
    }
    // 释放资源
    closure->Release();
}

TEST_P(OpRequestTest, ReadChunkTest) {
    // 创建CreateCloneChunkRequest
    LogicPoolID logicPoolId = 1;
    CopysetID copysetId = 10001;
    uint64_t chunkId = 12345;
    uint32_t offset = 0;
    uint32_t length = 5 * blocksize_;
    ChunkRequest* request = new ChunkRequest();
    request->set_logicpoolid(logicPoolId);
    request->set_copysetid(copysetId);
    request->set_chunkid(chunkId);
    request->set_optype(CHUNK_OP_READ);
    request->set_offset(offset);
    request->set_size(length);
    brpc::Controller *cntl = new brpc::Controller();
    ChunkResponse *response = new ChunkResponse();
    UnitTestClosure *closure = new UnitTestClosure();
    closure->SetCntl(cntl);
    closure->SetRequest(request);
    closure->SetResponse(response);
    std::shared_ptr<ReadChunkRequest> opReq =
        std::make_shared<ReadChunkRequest>(node_,
                                           cloneMgr_.get(),
                                           cntl,
                                           request,
                                           response,
                                           closure);
    /**
     * 测试Encode/Decode
     */
    {
        butil::IOBuf log;
        ASSERT_EQ(0, opReq->Encode(request, &cntl->request_attachment(), &log));

        butil::IOBuf data;
        auto req = ChunkOpRequest::Decode(log, request, &data, 0,
                                          PeerId("127.0.0.1:8200:0"));
        auto req1 = dynamic_cast<ReadChunkRequest*>(req.get());
        ASSERT_TRUE(req1 != nullptr);

        ASSERT_EQ(CHUNK_OP_TYPE::CHUNK_OP_READ, request->optype());
        ASSERT_EQ(logicPoolId, request->logicpoolid());
        ASSERT_EQ(copysetId, request->copysetid());
        ASSERT_EQ(chunkId, request->chunkid());
        ASSERT_EQ(offset, request->offset());
        ASSERT_EQ(length, request->size());
    }
    /**
     * 测试Process
     * 用例： node_->IsLeaderTerm() == false
     * 预期： 会要求转发请求，返回CHUNK_OP_STATUS_REDIRECTED
     */
    {
        // 设置预期
        EXPECT_CALL(*node_, IsLeaderTerm())
            .WillRepeatedly(Return(false));
        // PeerId leaderId(PEER_STRING);
        // EXPECT_CALL(*node_, GetLeaderId())
        //     .WillOnce(Return(leaderId));
        EXPECT_CALL(*node_, Propose(_))
            .Times(0);

        opReq->Process();

        // 验证结果
        ASSERT_TRUE(closure->isDone_);
        ASSERT_FALSE(response->has_appliedindex());
        ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_REDIRECTED,
                  closure->response_->status());
        // ASSERT_STREQ(closure->response_->redirect().c_str(), PEER_STRING);
    }
    /**
     * 测试Process
     * 用例： node_->IsLeaderTerm() == true,
     *       请求的 apply index 大于 node的 apply index
     * 预期： 会调用Propose，且不会调用closure
     */
    {
        // 重置closure
        closure->Reset();

        request->set_appliedindex(LAST_INDEX + 1);

        // 设置预期
        EXPECT_CALL(*node_, IsLeaderTerm())
            .WillRepeatedly(Return(true));
        braft::Task task;
        EXPECT_CALL(*node_, Propose(_))
            .WillOnce(SaveArg<0>(&task));

        opReq->Process();

        // 验证结果
        ASSERT_FALSE(closure->isDone_);
        ASSERT_FALSE(response->has_appliedindex());
        ASSERT_FALSE(closure->response_->has_status());
        // 由于这里node是mock的，因此需要主动来执行task.done.Run来释放资源
        ASSERT_NE(nullptr, task.done);
        task.done->Run();
        ASSERT_TRUE(closure->isDone_);
    }

    CSChunkInfo info;
    info.isClone = true;
    info.metaPageSize = metapagesize_;
    info.chunkSize = chunksize_;
    info.blockSize = blocksize_;
    info.bitmap = std::make_shared<Bitmap>(chunksize_ / blocksize_);

    /**
     * 测试Process
     * 用例： node_->IsLeaderTerm() == true,
     *       请求的 apply index 小于等于 node的 apply index
     * 预期： 不会走一致性协议，请求提交给concurrentApplyModule_处理
     */
    {
        // 重置closure
        closure->Reset();

        request->set_appliedindex(3);

        // 设置预期
        EXPECT_CALL(*node_, IsLeaderTerm())
            .WillRepeatedly(Return(true));
        EXPECT_CALL(*node_, Propose(_))
            .Times(0);

        info.isClone = false;
        EXPECT_CALL(*datastore_, GetChunkInfo(_, _))
            .WillOnce(
                DoAll(SetArgPointee<1>(info), Return(CSErrorCode::Success)));

        char chunkData[length];  // NOLINT
        memset(chunkData, 'a', length);
        EXPECT_CALL(*datastore_, ReadChunk(_, _, _, offset, length))
            .WillOnce(DoAll(SetArrayArgument<2>(chunkData, chunkData + length),
                            Return(CSErrorCode::Success)));
        EXPECT_CALL(*node_, UpdateAppliedIndex(_)).Times(1);

        opReq->Process();

        int retry = 10;
        while (retry-- > 0) {
            if (closure->isDone_) {
                break;
            }

            ::sleep(1);
        }

        ASSERT_TRUE(closure->isDone_);
    }

    /**
     * 测试OnApply
     * 用例：请求的 chunk 不是 clone chunk
     * 预期：从本地读chunk,返回 CHUNK_OP_STATUS_SUCCESS
     */
    {
        // 重置closure
        closure->Reset();

        // 设置预期
        info.isClone = false;
        EXPECT_CALL(*datastore_, GetChunkInfo(_, _))
            .WillOnce(
                DoAll(SetArgPointee<1>(info), Return(CSErrorCode::Success)));
        // 读chunk文件
        char chunkData[length];  // NOLINT
        memset(chunkData, 'a', length);
        EXPECT_CALL(*datastore_, ReadChunk(_, _, _, offset, length))
            .WillOnce(DoAll(SetArrayArgument<2>(chunkData, chunkData + length),
                            Return(CSErrorCode::Success)));
        EXPECT_CALL(*node_, UpdateAppliedIndex(_)).Times(1);

        opReq->OnApply(3, closure);

        // 验证结果
        ASSERT_TRUE(closure->isDone_);
        ASSERT_EQ(LAST_INDEX, response->appliedindex());
        ASSERT_TRUE(response->has_status());
        ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS, response->status());
        ASSERT_EQ(
            memcmp(chunkData,
                   cntl->response_attachment().to_string().c_str(),  // NOLINT
                   length),
            0);
    }

    /**
     * 测试OnApply
     * 用例：请求的chunk是 clone chunk，请求区域的bitmap都为1
     * 预期：从本地读chunk,返回 CHUNK_OP_STATUS_SUCCESS
     */
    {
        // 重置closure
        closure->Reset();

        // 设置预期
        info.isClone = true;
        info.bitmap->Set();
        EXPECT_CALL(*datastore_, GetChunkInfo(_, _))
            .WillOnce(
                DoAll(SetArgPointee<1>(info), Return(CSErrorCode::Success)));
        // 读chunk文件
        char chunkData[length];  // NOLINT
        memset(chunkData, 'a', length);
        EXPECT_CALL(*datastore_, ReadChunk(_, _, _, offset, length))
            .WillOnce(DoAll(SetArrayArgument<2>(chunkData, chunkData + length),
                            Return(CSErrorCode::Success)));
        EXPECT_CALL(*node_, UpdateAppliedIndex(_)).Times(1);

        opReq->OnApply(3, closure);

        // 验证结果
        ASSERT_TRUE(closure->isDone_);
        ASSERT_EQ(LAST_INDEX, closure->response_->appliedindex());
        ASSERT_TRUE(response->has_status());
        ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS,
                  closure->response_->status());
        ASSERT_EQ(memcmp(chunkData,
                         closure->cntl_->response_attachment()
                             .to_string()
                             .c_str(),  // NOLINT
                         length),
                  0);
    }

    /**
     * 测试OnApply
     * 用例：请求的chunk是 clone chunk，请求区域的bitmap存在bit为0
     * 预期：将请求转发给clone manager处理
     */
    {
        // 重置closure
        closure->Reset();

        // 设置预期
        info.isClone = true;
        info.bitmap->Clear(1);
        EXPECT_CALL(*datastore_, GetChunkInfo(_, _))
            .WillOnce(DoAll(SetArgPointee<1>(info),
                            Return(CSErrorCode::Success)));
        // 读chunk文件
        EXPECT_CALL(*datastore_, ReadChunk(_, _, _, _, _))
            .Times(0);
        EXPECT_CALL(*node_, UpdateAppliedIndex(_))
            .Times(0);
        EXPECT_CALL(*cloneMgr_, GenerateCloneTask(_, _))
            .Times(1);
        EXPECT_CALL(*cloneMgr_, IssueCloneTask(_))
            .WillOnce(Return(true));

        opReq->OnApply(3, closure);

        // 验证结果
        ASSERT_FALSE(closure->isDone_);
        ASSERT_FALSE(response->has_appliedindex());
        ASSERT_FALSE(closure->response_->has_status());

        closure->Run();
        ASSERT_TRUE(closure->isDone_);
    }
    /**
     * 测试OnApply
     * 用例：GetChunkInfo 返回 ChunkNotExistError
     * 预期：请求失败,返回 CHUNK_OP_STATUS_CHUNK_NOTEXIST
     */
    {
        // 重置closure
        closure->Reset();

        // 设置预期
        EXPECT_CALL(*datastore_, GetChunkInfo(_, _))
            .WillOnce(Return(CSErrorCode::ChunkNotExistError));
        // 不会读chunk文件
        EXPECT_CALL(*datastore_, ReadChunk(_, _, _, offset, length))
            .Times(0);
        EXPECT_CALL(*node_, UpdateAppliedIndex(_))
            .Times(0);

        opReq->OnApply(3, closure);

        // 验证结果
        ASSERT_TRUE(closure->isDone_);
        ASSERT_EQ(LAST_INDEX, response->appliedindex());
        ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_CHUNK_NOTEXIST,
                  response->status());
    }
    /**
     * 测试OnApply
     * 用例：GetChunkInfo 返回 ChunkNotExistError
     *      但是请求中包含源chunk的信息
     * 预期：将请求转发给clone manager处理
     */
    {
        // 重置closure
        closure->Reset();
        request->set_clonefilesource("/test");
        request->set_clonefileoffset(0);

        // 设置预期
        EXPECT_CALL(*datastore_, GetChunkInfo(_, _))
            .WillOnce(Return(CSErrorCode::ChunkNotExistError));
        // 读chunk文件
        EXPECT_CALL(*datastore_, ReadChunk(_, _, _, _, _))
            .Times(0);
        EXPECT_CALL(*node_, UpdateAppliedIndex(_))
            .Times(0);
        EXPECT_CALL(*cloneMgr_, GenerateCloneTask(_, _))
            .Times(1);
        EXPECT_CALL(*cloneMgr_, IssueCloneTask(_))
            .WillOnce(Return(true));

        opReq->OnApply(3, closure);

        // 验证结果
        ASSERT_FALSE(closure->isDone_);
        ASSERT_FALSE(response->has_appliedindex());
        ASSERT_FALSE(closure->response_->has_status());

        closure->Run();
        ASSERT_TRUE(closure->isDone_);
    }
    /**
     * 测试OnApply
     * 用例：请求的chunk是 clone chunk，请求区域的bitmap都为1
     *      请求中包含源chunk的信息
     * 预期：从本地读chunk,返回 CHUNK_OP_STATUS_SUCCESS
     */
    {
        // 重置closure
        closure->Reset();
        request->set_clonefilesource("/test");
        request->set_clonefileoffset(0);

        // 设置预期
        info.isClone = true;
        info.bitmap->Set();
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
        EXPECT_CALL(*node_, UpdateAppliedIndex(_))
            .Times(1);

        opReq->OnApply(3, closure);

        // 验证结果
        ASSERT_TRUE(closure->isDone_);
        ASSERT_EQ(LAST_INDEX, closure->response_->appliedindex());
        ASSERT_TRUE(response->has_status());
        ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS,
                  closure->response_->status());
        ASSERT_EQ(memcmp(chunkData,
                         closure->cntl_->response_attachment().to_string().c_str(),  //NOLINT
                         length), 0);
    }
    /**
     * 测试OnApply
     * 用例：GetChunkInfo 返回 非ChunkNotExistError错误
     * 预期：请求失败,返回 CHUNK_OP_STATUS_FAILURE_UNKNOWN
     */
    {
        // 重置closure
        closure->Reset();

        // 设置预期
        EXPECT_CALL(*datastore_, GetChunkInfo(_, _))
            .WillOnce(Return(CSErrorCode::InternalError));
        // 不会读chunk文件
        EXPECT_CALL(*datastore_, ReadChunk(_, _, _, offset, length))
            .Times(0);
        EXPECT_CALL(*node_, UpdateAppliedIndex(_))
            .Times(0);

        opReq->OnApply(3, closure);

        // 验证结果
        ASSERT_TRUE(closure->isDone_);
        ASSERT_EQ(LAST_INDEX, response->appliedindex());
        ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_FAILURE_UNKNOWN,
                  response->status());
    }
    /**
     * 测试OnApply
     * 用例：读本地chunk的时候返回错误
     * 预期：请求失败,返回 CHUNK_OP_STATUS_FAILURE_UNKNOWN
     */
    {
        // 重置closure
        closure->Reset();

        // 设置预期
        info.isClone = false;
        EXPECT_CALL(*datastore_, GetChunkInfo(_, _))
            .WillRepeatedly(DoAll(SetArgPointee<1>(info),
                            Return(CSErrorCode::Success)));
        // 读chunk文件失败
        EXPECT_CALL(*datastore_, ReadChunk(_, _, _, offset, length))
            .WillRepeatedly(Return(CSErrorCode::InternalError));
        EXPECT_CALL(*node_, UpdateAppliedIndex(_))
            .Times(0);

        ASSERT_DEATH(opReq->OnApply(3, closure), "");
    }
    /**
     * 测试OnApply
     * 用例：请求的chunk是 clone chunk，请求区域的bitmap存在bit为0
     *      转发请求给clone manager时出错
     * 预期：请求失败,返回 CHUNK_OP_STATUS_FAILURE_UNKNOWN
     */
    {
        // 重置closure
        closure->Reset();

        // 设置预期
        info.isClone = true;
        info.bitmap->Clear(1);
        EXPECT_CALL(*datastore_, GetChunkInfo(_, _))
            .WillOnce(DoAll(SetArgPointee<1>(info),
                            Return(CSErrorCode::Success)));
        // 读chunk文件
        EXPECT_CALL(*datastore_, ReadChunk(_, _, _, _, _))
            .Times(0);
        EXPECT_CALL(*node_, UpdateAppliedIndex(_))
            .Times(0);
        EXPECT_CALL(*cloneMgr_, GenerateCloneTask(_, _))
            .Times(1);
        EXPECT_CALL(*cloneMgr_, IssueCloneTask(_))
            .WillOnce(Return(false));

        opReq->OnApply(3, closure);

        // 验证结果
        ASSERT_TRUE(closure->isDone_);
        ASSERT_EQ(LAST_INDEX, response->appliedindex());
        ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_FAILURE_UNKNOWN,
                  response->status());
    }
    /**
     * 测试 OnApplyFromLog
     * 预期：啥也没做
     */
    {
        // 重置closure
        closure->Reset();

        butil::IOBuf data;
        opReq->OnApplyFromLog(datastore_, *request, data);
    }
    // 释放资源
    closure->Release();
}

TEST_P(OpRequestTest, RecoverChunkTest) {
    // 创建CreateCloneChunkRequest
    LogicPoolID logicPoolId = 1;
    CopysetID copysetId = 10001;
    uint64_t chunkId = 12345;
    uint32_t offset = 0;
    uint32_t length = 5 * blocksize_;
    ChunkRequest* request = new ChunkRequest();
    request->set_logicpoolid(logicPoolId);
    request->set_copysetid(copysetId);
    request->set_chunkid(chunkId);
    request->set_optype(CHUNK_OP_RECOVER);
    request->set_offset(offset);
    request->set_size(length);
    brpc::Controller *cntl = new brpc::Controller();
    ChunkResponse *response = new ChunkResponse();
    UnitTestClosure *closure = new UnitTestClosure();
    closure->SetCntl(cntl);
    closure->SetRequest(request);
    closure->SetResponse(response);
    std::shared_ptr<ReadChunkRequest> opReq =
        std::make_shared<ReadChunkRequest>(node_,
                                           cloneMgr_.get(),
                                           cntl,
                                           request,
                                           response,
                                           closure);
    /**
     * 测试Encode/Decode
     */
    {
        butil::IOBuf log;
        ASSERT_EQ(0, opReq->Encode(request, &cntl->request_attachment(), &log));

        butil::IOBuf data;
        auto req = ChunkOpRequest::Decode(log, request, &data, 0,
                                          PeerId("127.0.0.1:8200:0"));
        auto req1 = dynamic_cast<ReadChunkRequest*>(req.get());
        ASSERT_TRUE(req1 != nullptr);

        ASSERT_EQ(CHUNK_OP_TYPE::CHUNK_OP_RECOVER, request->optype());
        ASSERT_EQ(logicPoolId, request->logicpoolid());
        ASSERT_EQ(copysetId, request->copysetid());
        ASSERT_EQ(chunkId, request->chunkid());
        ASSERT_EQ(offset, request->offset());
        ASSERT_EQ(length, request->size());
    }
    /**
     * 测试Process
     * 用例： node_->IsLeaderTerm() == false
     * 预期： 会要求转发请求，返回CHUNK_OP_STATUS_REDIRECTED
     */
    {
        // 设置预期
        EXPECT_CALL(*node_, IsLeaderTerm())
            .WillRepeatedly(Return(false));
        // PeerId leaderId(PEER_STRING);
        // EXPECT_CALL(*node_, GetLeaderId())
        //     .WillOnce(Return(leaderId));
        EXPECT_CALL(*node_, Propose(_))
            .Times(0);

        opReq->Process();

        // 验证结果
        ASSERT_TRUE(closure->isDone_);
        ASSERT_FALSE(response->has_appliedindex());
        ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_REDIRECTED,
                  closure->response_->status());
        // ASSERT_STREQ(closure->response_->redirect().c_str(), PEER_STRING);
    }

    CSChunkInfo info;
    info.isClone = true;
    info.metaPageSize = metapagesize_;
    info.chunkSize = chunksize_;
    info.blockSize = blocksize_;
    info.bitmap = std::make_shared<Bitmap>(chunksize_ / blocksize_);

    /**
     * 测试Process
     * 用例： node_->IsLeaderTerm() == true,
     *       请求的 apply index 大于 node的 apply index
     * 预期： 不会走一致性协议，请求提交给concurrentApplyModule_处理
     */
    {
        // 重置closure
        closure->Reset();

        request->set_appliedindex(LAST_INDEX + 1);

        info.isClone = false;
        EXPECT_CALL(*datastore_, GetChunkInfo(_, _))
            .WillOnce(DoAll(SetArgPointee<1>(info),
                            Return(CSErrorCode::Success)));
        // 不读chunk文件
        EXPECT_CALL(*datastore_, ReadChunk(_, _, _, offset, length))
            .Times(0);
        EXPECT_CALL(*node_, UpdateAppliedIndex(_))
            .Times(1);

        // 设置预期
        EXPECT_CALL(*node_, IsLeaderTerm())
            .WillRepeatedly(Return(true));
        EXPECT_CALL(*node_, Propose(_))
            .Times(0);

        opReq->Process();

        int retry = 10;
        while (retry-- > 0) {
            if (closure->isDone_) {
                break;
            }

            ::sleep(1);
        }

        ASSERT_TRUE(closure->isDone_);
    }

    /**
     * 测试Process
     * 用例： node_->IsLeaderTerm() == true,
     *       请求的 apply index 小于等于 node的 apply index
     * 预期： 不会走一致性协议，请求提交给concurrentApplyModule_处理
     */
    {
        // 重置closure
        closure->Reset();

        request->set_appliedindex(3);

        info.isClone = false;
        EXPECT_CALL(*datastore_, GetChunkInfo(_, _))
            .WillOnce(DoAll(SetArgPointee<1>(info),
                            Return(CSErrorCode::Success)));
        // 不读chunk文件
        EXPECT_CALL(*datastore_, ReadChunk(_, _, _, offset, length))
            .Times(0);
        EXPECT_CALL(*node_, UpdateAppliedIndex(_))
            .Times(1);

        // 设置预期
        EXPECT_CALL(*node_, IsLeaderTerm())
            .WillRepeatedly(Return(true));
        EXPECT_CALL(*node_, Propose(_))
            .Times(0);

        opReq->Process();

        int retry = 10;
        while (retry-- > 0) {
            if (closure->isDone_) {
                break;
            }

            ::sleep(1);
        }

        ASSERT_TRUE(closure->isDone_);
    }

    /**
     * 测试OnApply
     * 用例：请求的 chunk 不是 clone chunk
     * 预期：直接返回 CHUNK_OP_STATUS_SUCCESS
     */
    {
        // 重置closure
        closure->Reset();

        // 设置预期
        info.isClone = false;
        EXPECT_CALL(*datastore_, GetChunkInfo(_, _))
            .WillOnce(DoAll(SetArgPointee<1>(info),
                            Return(CSErrorCode::Success)));
        // 不读chunk文件
        EXPECT_CALL(*datastore_, ReadChunk(_, _, _, offset, length))
            .Times(0);
        EXPECT_CALL(*node_, UpdateAppliedIndex(_))
            .Times(1);

        opReq->OnApply(3, closure);

        // 验证结果
        ASSERT_TRUE(closure->isDone_);
        ASSERT_EQ(LAST_INDEX, response->appliedindex());
        ASSERT_TRUE(response->has_status());
        ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS,
                  response->status());
    }
    /**
     * 测试OnApply
     * 用例：请求的chunk是 clone chunk，请求区域的bitmap都为1
     * 预期：直接返回 CHUNK_OP_STATUS_SUCCESS
     */
    {
        // 重置closure
        closure->Reset();

        // 设置预期
        info.isClone = true;
        info.bitmap->Set();
        EXPECT_CALL(*datastore_, GetChunkInfo(_, _))
            .WillOnce(DoAll(SetArgPointee<1>(info),
                            Return(CSErrorCode::Success)));
        // 不读chunk文件
        EXPECT_CALL(*datastore_, ReadChunk(_, _, _, offset, length))
            .Times(0);
        EXPECT_CALL(*node_, UpdateAppliedIndex(_))
            .Times(1);

        opReq->OnApply(3, closure);

        // 验证结果
        ASSERT_TRUE(closure->isDone_);
        ASSERT_EQ(LAST_INDEX, closure->response_->appliedindex());
        ASSERT_TRUE(response->has_status());
        ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS,
                  closure->response_->status());
    }
    /**
     * 测试OnApply
     * 用例：请求的chunk是 clone chunk，请求区域的bitmap存在bit为0
     * 预期：将请求转发给clone manager处理
     */
    {
        // 重置closure
        closure->Reset();

        // 设置预期
        info.isClone = true;
        info.bitmap->Clear(1);
        EXPECT_CALL(*datastore_, GetChunkInfo(_, _))
            .WillOnce(DoAll(SetArgPointee<1>(info),
                            Return(CSErrorCode::Success)));
        // 读chunk文件
        EXPECT_CALL(*datastore_, ReadChunk(_, _, _, _, _))
            .Times(0);
        EXPECT_CALL(*node_, UpdateAppliedIndex(_))
            .Times(0);
        EXPECT_CALL(*cloneMgr_, GenerateCloneTask(_, _))
            .Times(1);
        EXPECT_CALL(*cloneMgr_, IssueCloneTask(_))
            .WillOnce(Return(true));

        opReq->OnApply(3, closure);

        // 验证结果
        ASSERT_FALSE(closure->isDone_);
        ASSERT_FALSE(response->has_appliedindex());
        ASSERT_FALSE(closure->response_->has_status());

        closure->Run();
        ASSERT_TRUE(closure->isDone_);
    }
    /**
     * 测试OnApply
     * 用例：GetChunkInfo 返回 ChunkNotExistError
     * 预期：请求失败,返回 CHUNK_OP_STATUS_CHUNK_NOTEXIST
     */
    {
        // 重置closure
        closure->Reset();

        // 设置预期
        EXPECT_CALL(*datastore_, GetChunkInfo(_, _))
            .WillOnce(Return(CSErrorCode::ChunkNotExistError));
        // 不会读chunk文件
        EXPECT_CALL(*datastore_, ReadChunk(_, _, _, offset, length))
            .Times(0);
        EXPECT_CALL(*node_, UpdateAppliedIndex(_))
            .Times(0);

        opReq->OnApply(3, closure);

        // 验证结果
        ASSERT_TRUE(closure->isDone_);
        ASSERT_EQ(LAST_INDEX, response->appliedindex());
        ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_CHUNK_NOTEXIST,
                  response->status());
    }
    /**
     * 测试OnApply
     * 用例：GetChunkInfo 返回 非ChunkNotExistError错误
     * 预期：请求失败,返回 CHUNK_OP_STATUS_FAILURE_UNKNOWN
     */
    {
        // 重置closure
        closure->Reset();

        // 设置预期
        EXPECT_CALL(*datastore_, GetChunkInfo(_, _))
            .WillOnce(Return(CSErrorCode::InternalError));
        // 不会读chunk文件
        EXPECT_CALL(*datastore_, ReadChunk(_, _, _, offset, length))
            .Times(0);
        EXPECT_CALL(*node_, UpdateAppliedIndex(_))
            .Times(0);

        opReq->OnApply(3, closure);

        // 验证结果
        ASSERT_TRUE(closure->isDone_);
        ASSERT_EQ(LAST_INDEX, response->appliedindex());
        ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_FAILURE_UNKNOWN,
                  response->status());
    }
    /**
     * 测试OnApply
     * 用例：请求的chunk是 clone chunk，请求区域的bitmap存在bit为0
     *      转发请求给clone manager时出错
     * 预期：请求失败,返回 CHUNK_OP_STATUS_FAILURE_UNKNOWN
     */
    {
        // 重置closure
        closure->Reset();

        // 设置预期
        info.isClone = true;
        info.bitmap->Clear(1);
        EXPECT_CALL(*datastore_, GetChunkInfo(_, _))
            .WillOnce(DoAll(SetArgPointee<1>(info),
                            Return(CSErrorCode::Success)));
        // 读chunk文件
        EXPECT_CALL(*datastore_, ReadChunk(_, _, _, _, _))
            .Times(0);
        EXPECT_CALL(*node_, UpdateAppliedIndex(_))
            .Times(0);
        EXPECT_CALL(*cloneMgr_, GenerateCloneTask(_, _))
            .Times(1);
        EXPECT_CALL(*cloneMgr_, IssueCloneTask(_))
            .WillOnce(Return(false));

        opReq->OnApply(3, closure);

        // 验证结果
        ASSERT_TRUE(closure->isDone_);
        ASSERT_EQ(LAST_INDEX, response->appliedindex());
        ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_FAILURE_UNKNOWN,
                  response->status());
    }
    /**
     * 测试 OnApplyFromLog
     * 预期：啥也没做
     */
    {
        // 重置closure
        closure->Reset();

        butil::IOBuf data;
        opReq->OnApplyFromLog(datastore_, *request, data);
    }
    // 释放资源
    closure->Release();
}

INSTANTIATE_TEST_CASE_P(
    OpRequestTest,
    OpRequestTest,
    ::testing::Values(
        //                chunk size        block size,     metapagesize
        std::make_tuple(16U * 1024 * 1024, 4096U, 4096U),
        std::make_tuple(16U * 1024 * 1024, 4096U, 8192U),
        std::make_tuple(16U * 1024 * 1024, 512U, 8192U),
        std::make_tuple(16U * 1024 * 1024, 512U, 4096U * 4)));

}  // namespace chunkserver
}  // namespace curve
