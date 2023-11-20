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

#include "src/chunkserver/op_request.h"

#include <glog/logging.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <memory>

#include "test/chunkserver/clone/clone_test_util.h"
#include "test/chunkserver/clone/mock_clone_manager.h"
#include "test/chunkserver/datastore/mock_datastore.h"
#include "test/chunkserver/mock_copyset_node.h"

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
    void TearDown() {}

    void FakeCopysetNode() {
        EXPECT_CALL(*node_, IsLeaderTerm()).WillRepeatedly(Return(true));
        EXPECT_CALL(*node_, GetDataStore()).WillRepeatedly(Return(datastore_));
        EXPECT_CALL(*node_, GetConcurrentApplyModule())
            .WillRepeatedly(Return(concurrentApplyModule_.get()));
        EXPECT_CALL(*node_, GetAppliedIndex())
            .WillRepeatedly(Return(LAST_INDEX));
        PeerId peer(PEER_STRING);
        EXPECT_CALL(*node_, GetLeaderId()).WillRepeatedly(Return(peer));
    }

    void FakeCloneManager() {
        EXPECT_CALL(*cloneMgr_, GenerateCloneTask(_, _))
            .WillRepeatedly(Return(nullptr));
        EXPECT_CALL(*cloneMgr_, IssueCloneTask(_)).WillRepeatedly(Return(true));
    }

 protected:
    ChunkSizeType chunksize_;
    ChunkSizeType blocksize_;
    PageSizeType metapagesize_;

    std::shared_ptr<MockCopysetNode> node_;
    std::shared_ptr<MockDataStore> datastore_;
    std::shared_ptr<MockCloneManager> cloneMgr_;
    std::shared_ptr<FakeConcurrentApplyModule> concurrentApplyModule_;
};

TEST_P(OpRequestTest, CreateCloneTest) {
    // Create CreateCloneChunkRequest
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
    brpc::Controller* cntl = new brpc::Controller();
    ChunkResponse* response = new ChunkResponse();
    UnitTestClosure* closure = new UnitTestClosure();
    closure->SetCntl(cntl);
    closure->SetRequest(request);
    closure->SetResponse(response);
    std::shared_ptr<CreateCloneChunkRequest> opReq =
        std::make_shared<CreateCloneChunkRequest>(node_, cntl, request,
                                                  response, closure);
    /**
     * Test Encode/Decode
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
     * Test Process
     * Scenario: node_->IsLeaderTerm() == false
     * Expected: Request to forward request and return
     * CHUNK_OP_STATUS_REDIRECTED
     */
    {
        // Set expectations
        EXPECT_CALL(*node_, IsLeaderTerm()).WillRepeatedly(Return(false));
        // PeerId leaderId(PEER_STRING);
        // EXPECT_CALL(*node_, GetLeaderId())
        //     .WillOnce(Return(leaderId));
        EXPECT_CALL(*node_, Propose(_)).Times(0);

        opReq->Process();

        // Verification results
        ASSERT_TRUE(closure->isDone_);
        ASSERT_FALSE(response->has_appliedindex());
        ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_REDIRECTED,
                  closure->response_->status());
        // ASSERT_STREQ(closure->response_->redirect().c_str(), PEER_STRING);
    }
    /**
     * Test Process
     * Scenario: node_->IsLeaderTerm() == true
     * Expected: Propose will be called and closure will not be called
     */
    {
        // Reset closure
        closure->Reset();

        // Set expectations
        EXPECT_CALL(*node_, IsLeaderTerm()).WillRepeatedly(Return(true));
        braft::Task task;
        EXPECT_CALL(*node_, Propose(_)).WillOnce(SaveArg<0>(&task));

        opReq->Process();

        // Verification results
        ASSERT_FALSE(closure->isDone_);
        ASSERT_FALSE(response->has_appliedindex());
        ASSERT_FALSE(closure->response_->has_status());
        // Since the node here is mock, it is necessary to proactively execute
        // task.done.Run to release resources
        ASSERT_NE(nullptr, task.done);
        task.done->Run();
        ASSERT_TRUE(closure->isDone_);
    }
    /**
     * test OnApply
     * case：CreateCloneChunk success
     * expect：return CHUNK_OP_STATUS_SUCCESS
     */
    {
        // reset closure
        closure->Reset();

        // set expection
        EXPECT_CALL(*datastore_, CreateCloneChunk(_, _, _, _, _))
            .WillOnce(Return(CSErrorCode::Success));

        opReq->OnApply(3, closure);

        // check result
        ASSERT_TRUE(closure->isDone_);
        ASSERT_EQ(LAST_INDEX, response->appliedindex());
        ASSERT_TRUE(response->has_status());
        ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS,
                  closure->response_->status());
    }
    /**
     * test OnApply
     * case：CreateCloneChunk failed
     * expect：process exit
     */
    {
        // reset closure
        closure->Reset();

        // check result
        EXPECT_CALL(*datastore_, CreateCloneChunk(_, _, _, _, _))
            .WillRepeatedly(Return(CSErrorCode::InternalError));

        ASSERT_DEATH(opReq->OnApply(3, closure), "");
    }
    /**
     * test OnApply
     * case：CreateCloneChunk failed, return other problems
     * expect：process exit
     */
    {
        // reset closure
        closure->Reset();

        // set expection
        EXPECT_CALL(*datastore_, CreateCloneChunk(_, _, _, _, _))
            .WillRepeatedly(Return(CSErrorCode::InvalidArgError));
        EXPECT_CALL(*node_, UpdateAppliedIndex(_)).Times(0);

        opReq->OnApply(3, closure);

        // check result
        ASSERT_TRUE(closure->isDone_);
        ASSERT_EQ(LAST_INDEX, response->appliedindex());
        ASSERT_TRUE(response->has_status());
        ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_FAILURE_UNKNOWN,
                  closure->response_->status());
    }
    /**
     * Testing OnApplyFromLog
     * Scenario: CreateCloneChunk successful
     * Expected: No return
     */
    {
        // Reset closure
        closure->Reset();

        // Set expectations
        EXPECT_CALL(*datastore_, CreateCloneChunk(_, _, _, _, _))
            .WillOnce(Return(CSErrorCode::Success));

        butil::IOBuf data;
        opReq->OnApplyFromLog(datastore_, *request, data);
    }
    /**
     * Testing OnApplyFromLog
     * Scenario: CreateCloneChunk failed, returning InternalError
     * Expected: Process Exit
     */
    {
        // Reset closure
        closure->Reset();

        // Set expectations
        EXPECT_CALL(*datastore_, CreateCloneChunk(_, _, _, _, _))
            .WillRepeatedly(Return(CSErrorCode::InternalError));

        butil::IOBuf data;
        ASSERT_DEATH(opReq->OnApplyFromLog(datastore_, *request, data), "");
    }
    /**
     * Testing OnApplyFromLog
     * Scenario: CreateCloneChunk failed with other errors returned
     * Expected: Process Exit
     */
    {
        // Reset closure
        closure->Reset();

        // Set expectations
        EXPECT_CALL(*datastore_, CreateCloneChunk(_, _, _, _, _))
            .WillRepeatedly(Return(CSErrorCode::InvalidArgError));

        butil::IOBuf data;
        opReq->OnApplyFromLog(datastore_, *request, data);
    }
    // Release resources
    closure->Release();
}

TEST_P(OpRequestTest, PasteChunkTest) {
    // Generate temporary readrequest
    ChunkResponse* response = new ChunkResponse();
    std::shared_ptr<ReadChunkRequest> readChunkRequest =
        std::make_shared<ReadChunkRequest>(node_, nullptr, nullptr, nullptr,
                                           response, nullptr);

    // Create PasteChunkRequest
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

    UnitTestClosure* closure = new UnitTestClosure();
    closure->SetRequest(request);
    closure->SetResponse(response);
    std::shared_ptr<PasteChunkInternalRequest> opReq =
        std::make_shared<PasteChunkInternalRequest>(node_, request, response,
                                                    &cloneData, closure);
    /**
     * Test Encode/Decode
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
     * Test Process
     * Scenario: node_->IsLeaderTerm() == false
     * Expected: Request to forward request and return
     * CHUNK_OP_STATUS_REDIRECTED
     */
    {
        // Set expectations
        EXPECT_CALL(*node_, IsLeaderTerm()).WillRepeatedly(Return(false));
        // PeerId leaderId(PEER_STRING);
        // EXPECT_CALL(*node_, GetLeaderId())
        //     .WillOnce(Return(leaderId));
        EXPECT_CALL(*node_, Propose(_)).Times(0);

        opReq->Process();

        // Verification results
        ASSERT_TRUE(closure->isDone_);
        ASSERT_FALSE(response->has_appliedindex());
        ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_REDIRECTED,
                  response->status());
        // ASSERT_STREQ(closure->response_->redirect().c_str(), PEER_STRING);
    }
    /**
     * Test Process
     * Scenario: node_->IsLeaderTerm() == true
     * Expected: Propose will be called and closure will not be called
     */
    {
        // Reset closure
        closure->Reset();

        // Set expectations
        EXPECT_CALL(*node_, IsLeaderTerm()).WillRepeatedly(Return(true));
        braft::Task task;
        EXPECT_CALL(*node_, Propose(_)).WillOnce(SaveArg<0>(&task));

        opReq->Process();

        // Verification results
        ASSERT_FALSE(closure->isDone_);
        ASSERT_FALSE(response->has_appliedindex());
        ASSERT_FALSE(response->has_status());
        // Since the node here is mock, it is necessary to proactively execute
        // task.done.Run to release resources
        ASSERT_NE(nullptr, task.done);
        task.done->Run();
        ASSERT_TRUE(closure->isDone_);
    }
    /**
     * Test OnApply
     * Scenario: CreateCloneChunk successful
     * Expected: return CHUNK_OP_STATUS_SUCCESS and update the apply index
     */
    {
        // Reset closure
        closure->Reset();

        // Set expectations
        EXPECT_CALL(*datastore_, PasteChunk(_, _, _, _))
            .WillOnce(Return(CSErrorCode::Success));

        opReq->OnApply(3, closure);

        // Verification results
        ASSERT_TRUE(closure->isDone_);
        ASSERT_EQ(LAST_INDEX, response->appliedindex());
        ASSERT_TRUE(response->has_status());
        ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS, response->status());
    }
    /**
     * Test OnApply
     * Scenario: CreateCloneChunk failed, returning InternalError
     * Expected: Process Exit
     */
    {
        // Reset closure
        closure->Reset();

        // Set expectations
        EXPECT_CALL(*datastore_, PasteChunk(_, _, _, _))
            .WillRepeatedly(Return(CSErrorCode::InternalError));

        ASSERT_DEATH(opReq->OnApply(3, closure), "");
    }
    /**
     * Test OnApply
     * Scenario: CreateCloneChunk failed with other errors returned
     * Expected: Process Exit
     */
    {
        // Reset closure
        closure->Reset();

        // Set expectations
        EXPECT_CALL(*datastore_, PasteChunk(_, _, _, _))
            .WillRepeatedly(Return(CSErrorCode::InvalidArgError));

        opReq->OnApply(3, closure);

        // Verification results
        ASSERT_TRUE(closure->isDone_);
        ASSERT_EQ(LAST_INDEX, response->appliedindex());
        ASSERT_TRUE(response->has_status());
        ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_FAILURE_UNKNOWN,
                  response->status());
    }
    /**
     * Testing OnApplyFromLog
     * Scenario: CreateCloneChunk successful
     * Expected: No return
     */
    {
        // Reset closure
        closure->Reset();

        // Set expectations
        EXPECT_CALL(*datastore_, PasteChunk(_, _, _, _))
            .WillOnce(Return(CSErrorCode::Success));

        butil::IOBuf data;
        opReq->OnApplyFromLog(datastore_, *request, data);
    }
    /**
     * Testing OnApplyFromLog
     * Scenario: CreateCloneChunk failed, returning InternalError
     * Expected: Process Exit
     */
    {
        // Reset closure
        closure->Reset();

        // Set expectations
        EXPECT_CALL(*datastore_, PasteChunk(_, _, _, _))
            .WillRepeatedly(Return(CSErrorCode::InternalError));

        butil::IOBuf data;
        ASSERT_DEATH(opReq->OnApplyFromLog(datastore_, *request, data), "");
    }
    /**
     * Testing OnApplyFromLog
     * Scenario: CreateCloneChunk failed with other errors returned
     * Expected: Process Exit
     */
    {
        // Reset closure
        closure->Reset();

        // Set expectations
        EXPECT_CALL(*datastore_, PasteChunk(_, _, _, _))
            .WillRepeatedly(Return(CSErrorCode::InvalidArgError));

        butil::IOBuf data;
        opReq->OnApplyFromLog(datastore_, *request, data);
    }
    // Release resources
    closure->Release();
}

TEST_P(OpRequestTest, ReadChunkTest) {
    // Create CreateCloneChunkRequest
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
    brpc::Controller* cntl = new brpc::Controller();
    ChunkResponse* response = new ChunkResponse();
    UnitTestClosure* closure = new UnitTestClosure();
    closure->SetCntl(cntl);
    closure->SetRequest(request);
    closure->SetResponse(response);
    std::shared_ptr<ReadChunkRequest> opReq =
        std::make_shared<ReadChunkRequest>(node_, cloneMgr_.get(), cntl,
                                           request, response, closure);
    /**
     * Test Encode/Decode
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
     * Test Process
     * Scenario: node_->IsLeaderTerm() == false
     * Expected: Request to forward request and return
     * CHUNK_OP_STATUS_REDIRECTED
     */
    {
        // set expection
        EXPECT_CALL(*node_, IsLeaderTerm()).WillRepeatedly(Return(false));

        EXPECT_CALL(*node_, Propose(_)).Times(0);

        opReq->Process();

        // check result
        ASSERT_TRUE(closure->isDone_);
        ASSERT_FALSE(response->has_appliedindex());
        ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_REDIRECTED,
                  closure->response_->status());
    }
    /**
     * test Process
     * case： ABA Problem, node_->IsLeaderTerm() == true
     *        lease state == LEASE_EXPIRED
     * except： redirect request, return CHUNK_OP_STATUS_REDIRECTED
     */
    {
        // reset closure
        closure->Reset();

        // set expection
        EXPECT_CALL(*node_, IsLeaderTerm()).WillRepeatedly(Return(true));

        braft::LeaderLeaseStatus status;
        status.state = braft::LEASE_EXPIRED;
        EXPECT_CALL(*node_, GetLeaderLeaseStatus(_))
            .WillOnce(SetArgPointee<0>(status));

        EXPECT_CALL(*node_, IsLeaseLeader(_)).WillOnce(Return(false));

        EXPECT_CALL(*node_, Propose(_)).Times(0);

        opReq->Process();

        // check result
        ASSERT_TRUE(closure->isDone_);
        ASSERT_FALSE(response->has_appliedindex());
        ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_REDIRECTED,
                  closure->response_->status());
    }
    /**
     * test Process
     * case： node_->IsLeaderTerm() == true,
     *        lease state == NOT_READY => log read
     * expect： invoke Propose()，and don't invoke closure()
     */
    {
        // reset closure
        closure->Reset();

        // set expection
        EXPECT_CALL(*node_, IsLeaderTerm()).WillRepeatedly(Return(true));

        braft::LeaderLeaseStatus status;
        status.state = braft::LEASE_NOT_READY;
        EXPECT_CALL(*node_, GetLeaderLeaseStatus(_))
            .WillOnce(SetArgPointee<0>(status));

        EXPECT_CALL(*node_, IsLeaseLeader(_)).WillOnce(Return(false));

        braft::Task task;
        EXPECT_CALL(*node_, Propose(_)).WillOnce(SaveArg<0>(&task));

        opReq->Process();

        // check result
        ASSERT_FALSE(closure->isDone_);
        ASSERT_FALSE(response->has_appliedindex());
        ASSERT_FALSE(closure->response_->has_status());
        // because node is mock,
        // so need proactive invoke task.done.Run() to release resource
        ASSERT_NE(nullptr, task.done);
        task.done->Run();
        ASSERT_TRUE(closure->isDone_);
    }

    /**
     * test Process(user disable lease read, all requests propose to raft)
     * case： node_->IsLeaderTerm() == true,
     *        lease state == DISABLED => log read
     * expect： invoke Propose()，and don't invoke closure()
     */
    {
        // reset closure
        closure->Reset();

        // set expection
        EXPECT_CALL(*node_, IsLeaderTerm()).WillRepeatedly(Return(true));

        braft::LeaderLeaseStatus status;
        status.state = braft::LEASE_DISABLED;
        EXPECT_CALL(*node_, GetLeaderLeaseStatus(_))
            .WillOnce(SetArgPointee<0>(status));

        EXPECT_CALL(*node_, IsLeaseLeader(_)).WillOnce(Return(false));

        braft::Task task;
        EXPECT_CALL(*node_, Propose(_)).WillOnce(SaveArg<0>(&task));

        opReq->Process();

        // check result
        ASSERT_FALSE(closure->isDone_);
        ASSERT_FALSE(response->has_appliedindex());
        ASSERT_FALSE(closure->response_->has_status());
        // because node is mock,
        // so need proactive invoke task.done.Run() to release resource
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
     * Test Process
     * Scenario: node_->IsLeaderTerm() == true,
     *          The requested application index is less than or equal to the
     * node's application index Expected: Will not follow the consistency
     * protocol, request submission to ConcurrentApplyModule_ handle
     */
    {
        // Reset closure
        closure->Reset();

        // Set expectations
        EXPECT_CALL(*node_, IsLeaderTerm()).WillRepeatedly(Return(true));

        braft::LeaderLeaseStatus status;
        status.state = braft::LEASE_VALID;
        EXPECT_CALL(*node_, GetLeaderLeaseStatus(_))
            .WillOnce(SetArgPointee<0>(status));

        EXPECT_CALL(*node_, IsLeaseLeader(_)).WillOnce(Return(true));

        EXPECT_CALL(*node_, Propose(_)).Times(0);

        info.isClone = false;
        EXPECT_CALL(*datastore_, GetChunkInfo(_, _))
            .WillOnce(
                DoAll(SetArgPointee<1>(info), Return(CSErrorCode::Success)));

        char* chunkData = new char[length];
        memset(chunkData, 'a', length);
        EXPECT_CALL(*datastore_, ReadChunk(_, _, _, offset, length))
            .WillOnce(DoAll(SetArrayArgument<2>(chunkData, chunkData + length),
                            Return(CSErrorCode::Success)));

        opReq->Process();

        int retry = 10;
        while (retry-- > 0) {
            if (closure->isDone_) {
                break;
            }

            ::sleep(1);
        }

        ASSERT_TRUE(closure->isDone_);
        delete[] chunkData;
    }

    /**
     * Test OnApply
     * Scenario: The requested chunk is not a clone chunk
     * Expected: Chunk read locally, returning CHUNK_OP_STATUS_SUCCESS
     */
    {
        // Reset closure
        closure->Reset();

        // Set expectations
        info.isClone = false;
        EXPECT_CALL(*datastore_, GetChunkInfo(_, _))
            .WillOnce(
                DoAll(SetArgPointee<1>(info), Return(CSErrorCode::Success)));
        // Reading Chunk Files
        char* chunkData = new char[length];
        memset(chunkData, 'a', length);
        EXPECT_CALL(*datastore_, ReadChunk(_, _, _, offset, length))
            .WillOnce(DoAll(SetArrayArgument<2>(chunkData, chunkData + length),
                            Return(CSErrorCode::Success)));

        opReq->OnApply(3, closure);

        // Verification results
        ASSERT_TRUE(closure->isDone_);
        ASSERT_EQ(LAST_INDEX, response->appliedindex());
        ASSERT_TRUE(response->has_status());
        ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS, response->status());
        ASSERT_EQ(
            memcmp(chunkData,
                   cntl->response_attachment().to_string().c_str(),  // NOLINT
                   length),
            0);
        delete[] chunkData;
    }

    /**
     * Test OnApply
     * Scenario: The requested chunk is a clone chunk, and the bitmaps in the
     * request area are all 1 Expected: Chunk read locally, returning
     * CHUNK_OP_STATUS_SUCCESS
     */
    {
        // Reset closure
        closure->Reset();

        // Set expectations
        info.isClone = true;
        info.bitmap->Set();
        EXPECT_CALL(*datastore_, GetChunkInfo(_, _))
            .WillOnce(
                DoAll(SetArgPointee<1>(info), Return(CSErrorCode::Success)));
        // Reading Chunk Files
        char* chunkData = new char[length];
        memset(chunkData, 'a', length);
        EXPECT_CALL(*datastore_, ReadChunk(_, _, _, offset, length))
            .WillOnce(DoAll(SetArrayArgument<2>(chunkData, chunkData + length),
                            Return(CSErrorCode::Success)));

        opReq->OnApply(3, closure);

        // Verification results
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
        delete[] chunkData;
    }

    /**
     * Test OnApply
     * Scenario: The requested chunk is a clone chunk, and the bitmap in the
     * request area has a bit of 0 Expected: Forward request to clone manager
     * for processing
     */
    {
        // Reset closure
        closure->Reset();

        // Set expectations
        info.isClone = true;
        info.bitmap->Clear(1);
        EXPECT_CALL(*datastore_, GetChunkInfo(_, _))
            .WillOnce(
                DoAll(SetArgPointee<1>(info), Return(CSErrorCode::Success)));
        // Reading Chunk Files
        EXPECT_CALL(*datastore_, ReadChunk(_, _, _, _, _)).Times(0);
        EXPECT_CALL(*cloneMgr_, GenerateCloneTask(_, _)).Times(1);
        EXPECT_CALL(*cloneMgr_, IssueCloneTask(_)).WillOnce(Return(true));

        opReq->OnApply(3, closure);

        // Verification results
        ASSERT_FALSE(closure->isDone_);
        ASSERT_FALSE(response->has_appliedindex());
        ASSERT_FALSE(closure->response_->has_status());

        closure->Run();
        ASSERT_TRUE(closure->isDone_);
    }
    /**
     * Test OnApply
     * Scenario: GetChunkInfo returns ChunkNotExistError
     * Expected: Request failed, returning CHUNK_OP_STATUS_CHUNK_NOTEXIST
     */
    {
        // Reset closure
        closure->Reset();

        // Set expectations
        EXPECT_CALL(*datastore_, GetChunkInfo(_, _))
            .WillOnce(Return(CSErrorCode::ChunkNotExistError));
        // Unable to read chunk files
        EXPECT_CALL(*datastore_, ReadChunk(_, _, _, offset, length)).Times(0);
        opReq->OnApply(3, closure);

        // Verification results
        ASSERT_TRUE(closure->isDone_);
        ASSERT_EQ(LAST_INDEX, response->appliedindex());
        ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_CHUNK_NOTEXIST,
                  response->status());
    }
    /**
     * Test OnApply
     * Scenario: GetChunkInfo returns ChunkNotExistError
     *           But the request contains information about the source chunk
     * Expected: Forward request to clone manager for processing
     */
    {
        // Reset closure
        closure->Reset();
        request->set_clonefilesource("/test");
        request->set_clonefileoffset(0);

        // Set expectations
        EXPECT_CALL(*datastore_, GetChunkInfo(_, _))
            .WillOnce(Return(CSErrorCode::ChunkNotExistError));
        // Reading Chunk Files
        EXPECT_CALL(*datastore_, ReadChunk(_, _, _, _, _)).Times(0);
        EXPECT_CALL(*cloneMgr_, GenerateCloneTask(_, _)).Times(1);
        EXPECT_CALL(*cloneMgr_, IssueCloneTask(_)).WillOnce(Return(true));

        opReq->OnApply(3, closure);

        // Verification results
        ASSERT_FALSE(closure->isDone_);
        ASSERT_FALSE(response->has_appliedindex());
        ASSERT_FALSE(closure->response_->has_status());

        closure->Run();
        ASSERT_TRUE(closure->isDone_);
    }
    /**
     * Test OnApply
     * Scenario: The requested chunk is a clone chunk, and the bitmaps in the
     * request area are all 1 The request contains information about the source
     * chunk Expected: Chunk read locally, returning CHUNK_OP_STATUS_SUCCESS
     */
    {
        // Reset closure
        closure->Reset();
        request->set_clonefilesource("/test");
        request->set_clonefileoffset(0);

        // Set expectations
        info.isClone = true;
        info.bitmap->Set();
        EXPECT_CALL(*datastore_, GetChunkInfo(_, _))
            .WillOnce(
                DoAll(SetArgPointee<1>(info), Return(CSErrorCode::Success)));
        // Reading Chunk Files
        char* chunkData = new char[length];
        memset(chunkData, 'a', length);
        EXPECT_CALL(*datastore_, ReadChunk(_, _, _, offset, length))
            .WillOnce(DoAll(SetArrayArgument<2>(chunkData, chunkData + length),
                            Return(CSErrorCode::Success)));

        opReq->OnApply(3, closure);

        // Verification results
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
        delete[] chunkData;
    }
    /**
     * Test OnApply
     * Scenario: GetChunkInfo returns a non ChunkNotExistError error
     * Expected: Request failed, returning CHUNK_OP_STATUS_FAILURE_UNKNOWN
     */
    {
        // Reset closure
        closure->Reset();

        // Set expectations
        EXPECT_CALL(*datastore_, GetChunkInfo(_, _))
            .WillOnce(Return(CSErrorCode::InternalError));
        // Unable to read chunk files
        EXPECT_CALL(*datastore_, ReadChunk(_, _, _, offset, length)).Times(0);

        opReq->OnApply(3, closure);

        // Verification results
        ASSERT_TRUE(closure->isDone_);
        ASSERT_EQ(LAST_INDEX, response->appliedindex());
        ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_FAILURE_UNKNOWN,
                  response->status());
    }
    /**
     * Test OnApply
     * Scenario: Error returned when reading local chunk
     * Expected: Request failed, returning CHUNK_OP_STATUS_FAILURE_UNKNOWN
     */
    {
        // Reset closure
        closure->Reset();

        // Set expectations
        info.isClone = false;
        EXPECT_CALL(*datastore_, GetChunkInfo(_, _))
            .WillRepeatedly(
                DoAll(SetArgPointee<1>(info), Return(CSErrorCode::Success)));
        // Failed to read chunk file
        EXPECT_CALL(*datastore_, ReadChunk(_, _, _, offset, length))
            .WillRepeatedly(Return(CSErrorCode::InternalError));

        ASSERT_DEATH(opReq->OnApply(3, closure), "");
    }
    /**
     * Test OnApply
     * Scenario: The requested chunk is a clone chunk, and the bitmap in the
     * request area has a bit of 0 Error forwarding request to clone manager
     * Expected: Request failed, returning CHUNK_OP_STATUS_FAILURE_UNKNOWN
     */
    {
        // Reset closure
        closure->Reset();

        // Set expectations
        info.isClone = true;
        info.bitmap->Clear(1);
        EXPECT_CALL(*datastore_, GetChunkInfo(_, _))
            .WillOnce(
                DoAll(SetArgPointee<1>(info), Return(CSErrorCode::Success)));
        // Reading Chunk Files
        EXPECT_CALL(*datastore_, ReadChunk(_, _, _, _, _)).Times(0);
        EXPECT_CALL(*cloneMgr_, GenerateCloneTask(_, _)).Times(1);
        EXPECT_CALL(*cloneMgr_, IssueCloneTask(_)).WillOnce(Return(false));

        opReq->OnApply(3, closure);

        // Verification results
        ASSERT_TRUE(closure->isDone_);
        ASSERT_EQ(LAST_INDEX, response->appliedindex());
        ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_FAILURE_UNKNOWN,
                  response->status());
    }
    /**
     * Testing OnApplyFromLog
     * Expected: Nothing done
     */
    {
        // Reset closure
        closure->Reset();

        butil::IOBuf data;
        opReq->OnApplyFromLog(datastore_, *request, data);
    }
    // Release resources
    closure->Release();
}

TEST_P(OpRequestTest, RecoverChunkTest) {
    // Create CreateCloneChunkRequest
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
    brpc::Controller* cntl = new brpc::Controller();
    ChunkResponse* response = new ChunkResponse();
    UnitTestClosure* closure = new UnitTestClosure();
    closure->SetCntl(cntl);
    closure->SetRequest(request);
    closure->SetResponse(response);
    std::shared_ptr<ReadChunkRequest> opReq =
        std::make_shared<ReadChunkRequest>(node_, cloneMgr_.get(), cntl,
                                           request, response, closure);
    /**
     * Test Encode/Decode
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
     * Test Process
     * Scenario: node_->IsLeaderTerm() == false
     * Expected: Request to forward request and return
     * CHUNK_OP_STATUS_REDIRECTED
     */
    {
        // Set expectations
        EXPECT_CALL(*node_, IsLeaderTerm()).WillRepeatedly(Return(false));
        // PeerId leaderId(PEER_STRING);
        // EXPECT_CALL(*node_, GetLeaderId())
        //     .WillOnce(Return(leaderId));
        EXPECT_CALL(*node_, Propose(_)).Times(0);

        opReq->Process();

        // Verification results
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
     * test Process
     * case： node_->IsLeaderTerm() == true,
     *        LeaseStatus == LEASE_VALID
     * expect： don't propose to raft，request commit to concurrentApplyModule_
     */
    {
        // Reset closure
        closure->Reset();

        info.isClone = false;
        EXPECT_CALL(*datastore_, GetChunkInfo(_, _))
            .WillOnce(
                DoAll(SetArgPointee<1>(info), Return(CSErrorCode::Success)));
        // Do not read chunk files
        EXPECT_CALL(*datastore_, ReadChunk(_, _, _, offset, length)).Times(0);

        // Set expectations
        EXPECT_CALL(*node_, IsLeaderTerm()).WillRepeatedly(Return(true));
        EXPECT_CALL(*node_, Propose(_)).Times(0);

        braft::LeaderLeaseStatus status;
        status.state = braft::LEASE_VALID;
        EXPECT_CALL(*node_, GetLeaderLeaseStatus(_))
            .WillOnce(SetArgPointee<0>(status));
        EXPECT_CALL(*node_, IsLeaseLeader(_)).WillOnce(Return(true));

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
     * Test OnApply
     * Scenario: The requested chunk is not a clone chunk
     * Expected: Directly return to CHUNK_OP_STATUS_SUCCESS
     */
    {
        // Reset closure
        closure->Reset();

        // Set expectations
        info.isClone = false;
        EXPECT_CALL(*datastore_, GetChunkInfo(_, _))
            .WillOnce(
                DoAll(SetArgPointee<1>(info), Return(CSErrorCode::Success)));
        // Do not read chunk files
        EXPECT_CALL(*datastore_, ReadChunk(_, _, _, offset, length)).Times(0);

        opReq->OnApply(3, closure);

        // Verification results
        ASSERT_TRUE(closure->isDone_);
        ASSERT_EQ(LAST_INDEX, response->appliedindex());
        ASSERT_TRUE(response->has_status());
        ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS, response->status());
    }
    /**
     * Test OnApply
     * Scenario: The requested chunk is a clone chunk, and the bitmaps in the
     * request area are all 1 Expected: Directly return to
     * CHUNK_OP_STATUS_SUCCESS
     */
    {
        // Reset closure
        closure->Reset();

        // Set expectations
        info.isClone = true;
        info.bitmap->Set();
        EXPECT_CALL(*datastore_, GetChunkInfo(_, _))
            .WillOnce(
                DoAll(SetArgPointee<1>(info), Return(CSErrorCode::Success)));
        // Do not read chunk files
        EXPECT_CALL(*datastore_, ReadChunk(_, _, _, offset, length)).Times(0);

        opReq->OnApply(3, closure);

        // Verification results
        ASSERT_TRUE(closure->isDone_);
        ASSERT_EQ(LAST_INDEX, closure->response_->appliedindex());
        ASSERT_TRUE(response->has_status());
        ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS,
                  closure->response_->status());
    }
    /**
     * Test OnApply
     * Scenario: The requested chunk is a clone chunk, and the bitmap in the
     * request area has a bit of 0 Expected: Forward request to clone manager
     * for processing
     */
    {
        // Reset closure
        closure->Reset();

        // Set expectations
        info.isClone = true;
        info.bitmap->Clear(1);
        EXPECT_CALL(*datastore_, GetChunkInfo(_, _))
            .WillOnce(
                DoAll(SetArgPointee<1>(info), Return(CSErrorCode::Success)));
        // Reading Chunk Files
        EXPECT_CALL(*datastore_, ReadChunk(_, _, _, _, _)).Times(0);
        EXPECT_CALL(*cloneMgr_, GenerateCloneTask(_, _)).Times(1);
        EXPECT_CALL(*cloneMgr_, IssueCloneTask(_)).WillOnce(Return(true));

        opReq->OnApply(3, closure);

        // Verification results
        ASSERT_FALSE(closure->isDone_);
        ASSERT_FALSE(response->has_appliedindex());
        ASSERT_FALSE(closure->response_->has_status());

        closure->Run();
        ASSERT_TRUE(closure->isDone_);
    }
    /**
     * Test OnApply
     * Scenario: GetChunkInfo returns ChunkNotExistError
     * Expected: Request failed, returning CHUNK_OP_STATUS_CHUNK_NOTEXIST
     */
    {
        // Reset closure
        closure->Reset();

        // Set expectations
        EXPECT_CALL(*datastore_, GetChunkInfo(_, _))
            .WillOnce(Return(CSErrorCode::ChunkNotExistError));
        // Unable to read chunk files
        EXPECT_CALL(*datastore_, ReadChunk(_, _, _, offset, length)).Times(0);

        opReq->OnApply(3, closure);

        // Verification results
        ASSERT_TRUE(closure->isDone_);
        ASSERT_EQ(LAST_INDEX, response->appliedindex());
        ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_CHUNK_NOTEXIST,
                  response->status());
    }
    /**
     * Test OnApply
     * Scenario: GetChunkInfo returns a non ChunkNotExistError error
     * Expected: Request failed, returning CHUNK_OP_STATUS_FAILURE_UNKNOWN
     */
    {
        // Reset closure
        closure->Reset();

        // Set expectations
        EXPECT_CALL(*datastore_, GetChunkInfo(_, _))
            .WillOnce(Return(CSErrorCode::InternalError));
        // Unable to read chunk files
        EXPECT_CALL(*datastore_, ReadChunk(_, _, _, offset, length)).Times(0);

        opReq->OnApply(3, closure);

        // Verification results
        ASSERT_TRUE(closure->isDone_);
        ASSERT_EQ(LAST_INDEX, response->appliedindex());
        ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_FAILURE_UNKNOWN,
                  response->status());
    }
    /**
     * Test OnApply
     * Scenario: The requested chunk is a clone chunk, and the bitmap in the
     * request area has a bit of 0 Error forwarding request to clone manager
     * Expected: Request failed, returning CHUNK_OP_STATUS_FAILURE_UNKNOWN
     */
    {
        // Reset closure
        closure->Reset();

        // Set expectations
        info.isClone = true;
        info.bitmap->Clear(1);
        EXPECT_CALL(*datastore_, GetChunkInfo(_, _))
            .WillOnce(
                DoAll(SetArgPointee<1>(info), Return(CSErrorCode::Success)));
        // Reading Chunk Files
        EXPECT_CALL(*datastore_, ReadChunk(_, _, _, _, _)).Times(0);
        EXPECT_CALL(*cloneMgr_, GenerateCloneTask(_, _)).Times(1);
        EXPECT_CALL(*cloneMgr_, IssueCloneTask(_)).WillOnce(Return(false));

        opReq->OnApply(3, closure);

        // Verification results
        ASSERT_TRUE(closure->isDone_);
        ASSERT_EQ(LAST_INDEX, response->appliedindex());
        ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_FAILURE_UNKNOWN,
                  response->status());
    }
    /**
     * Testing OnApplyFromLog
     * Expected: Nothing done
     */
    {
        // Reset closure
        closure->Reset();

        butil::IOBuf data;
        opReq->OnApplyFromLog(datastore_, *request, data);
    }
    // Release resources
    closure->Release();
}

INSTANTIATE_TEST_CASE_P(
    OpRequestTest, OpRequestTest,
    ::testing::Values(
        //                chunk size        block size,     metapagesize
        std::make_tuple(16U * 1024 * 1024, 4096U, 4096U),
        std::make_tuple(16U * 1024 * 1024, 4096U, 8192U),
        std::make_tuple(16U * 1024 * 1024, 512U, 8192U),
        std::make_tuple(16U * 1024 * 1024, 512U, 4096U * 4)));

}  // namespace chunkserver
}  // namespace curve
