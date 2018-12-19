/*
 * Project: curve
 * Created Date: 18-9-29
 * Author: wudemiao
 * Copyright (c) 2018 netease
 */

#include <brpc/controller.h>
#include <brpc/channel.h>
#include <brpc/server.h>
#include <gtest/gtest.h>
#include <gmock/gmock.h>

#include <thread>   //NOLINT

#include "src/client/copyset_client.h"
#include "test/client/mock_meta_cache.h"
#include "test/utils/count_down_event.h"
#include "test/client/mock_chunkservice.h"
#include "test/client/mock_request_context.h"
#include "src/client/chunk_closure.h"

namespace curve {
namespace client {

using curve::chunkserver::CHUNK_OP_STATUS;
using curve::test::CountDownEvent;

using ::testing::_;
using ::testing::Invoke;
using ::testing::Return;
using ::testing::AnyNumber;
using ::testing::DoAll;
using ::testing::SetArgPointee;
using ::testing::SetArgReferee;
using ::testing::InSequence;
using ::testing::AtLeast;
using ::testing::SaveArgPointee;
using curve::client::MetaCache;

class CopysetClientTest : public testing::Test {
 protected:
    virtual void SetUp() {
        listenAddr_ = "127.0.0.1:8200";
        server_ = new brpc::Server();
    }

    virtual void TearDown() {
        server_->Stop(0);
        server_->Join();
        delete server_;
        server_ = nullptr;
    }

 public:
    std::string listenAddr_;
    brpc::Server *server_;
};

/* TODO(wudemiao) 当前 controller 错误不能通过 mock 返回 */
int gWriteCntlFailedCode = 0;
int gReadCntlFailedCode = 0;

static void WriteChunkFunc(::google::protobuf::RpcController *controller,
                           const ::curve::chunkserver::ChunkRequest *request,
                           ::curve::chunkserver::ChunkResponse *response,
                           google::protobuf::Closure *done) {
    /* return response */
    brpc::ClosureGuard doneGuard(done);
    if (0 != gWriteCntlFailedCode) {
        brpc::Controller *cntl = dynamic_cast<brpc::Controller *>(controller);
        cntl->SetFailed(-1, "write controller error");
    }
}

static void ReadChunkFunc(::google::protobuf::RpcController *controller,
                          const ::curve::chunkserver::ChunkRequest *request,
                          ::curve::chunkserver::ChunkResponse *response,
                          google::protobuf::Closure *done) {
    brpc::ClosureGuard doneGuard(done);
    if (0 != gReadCntlFailedCode) {
        brpc::Controller *cntl = dynamic_cast<brpc::Controller *>(controller);
        cntl->SetFailed(-1, "read controller error");
    }
}

TEST_F(CopysetClientTest, normal_test) {
    MockChunkServiceImpl mockChunkService;
    ASSERT_EQ(server_->AddService(&mockChunkService,
                                  brpc::SERVER_DOESNT_OWN_SERVICE), 0);
    ASSERT_EQ(server_->Start(listenAddr_.c_str(), nullptr), 0);

    CopysetClient copysetClient;
    RequestSenderManager senderManager;
    MockMetaCache mockMetaCache;
    mockMetaCache.DelegateToFake();
    copysetClient.Init(&senderManager, &mockMetaCache);

    LogicPoolID logicPoolId = 1;
    CopysetID copysetId = 100001;
    ChunkID chunkId = 1;
    size_t len = 8;
    char buff1[8 + 1];
    char buff2[8 + 1];
    memset(buff1, 'a', 8);
    memset(buff2, 'a', 8);
    buff1[8] = '\0';
    buff2[8] = '\0';
    off_t offset = 0;

    ChunkServerID leaderId1 = 10000;
    butil::EndPoint leaderAdder1;
    std::string leaderStr1 = "127.0.0.1:8200";
    butil::str2endpoint(leaderStr1.c_str(), &leaderAdder1);
    ChunkServerID leaderId2 = 10001;
    butil::EndPoint leaderAdder2;
    std::string leaderStr2 = "127.0.0.1:8200";
    butil::str2endpoint(leaderStr2.c_str(), &leaderAdder2);
    ChunkServerID leaderId3 = 10002;
    butil::EndPoint leaderAdder3;
    std::string leaderStr3 = "127.0.0.1:8200";
    butil::str2endpoint(leaderStr3.c_str(), &leaderAdder3);
    ChunkServerID leaderId4 = 10003;
    butil::EndPoint leaderAdder4;
    std::string leaderStr4 = "127.0.0.1:8200";
    butil::str2endpoint(leaderStr4.c_str(), &leaderAdder4);

    /* write success */
    for (int i = 0; i < 10; ++i) {
        RequestContext *reqCtx = new FakeRequestContext();
        reqCtx->optype_ = OpType::WRITE;
        reqCtx->logicpoolid_ = logicPoolId;
        reqCtx->copysetid_ = copysetId;
        reqCtx->chunkid_ = chunkId;
        reqCtx->data_ = buff1;
        reqCtx->offset_ = i * 8;
        reqCtx->rawlength_ = len;

        CountDownEvent cond(1);
        RequestClosure *reqDone = new FakeRequestClosure(&cond, reqCtx);
        reqCtx->done_ = reqDone;
        ChunkResponse response;
        response.set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS);
        EXPECT_CALL(mockMetaCache, GetLeader(_, _, _, _, _)).Times(AtLeast(1))
            .WillOnce(DoAll(SetArgPointee<2>(leaderId1),
                            SetArgPointee<3>(leaderAdder1),
                            Return(0)));
        EXPECT_CALL(mockChunkService, WriteChunk(_, _, _, _)).Times(1)
            .WillOnce(DoAll(SetArgPointee<2>(response),
                            Invoke(WriteChunkFunc)));
        copysetClient.WriteChunk(logicPoolId, copysetId, chunkId,
                                 buff1, offset, len, reqDone, 0);
        cond.Wait();
        ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS,
                  reqDone->GetErrorCode());
    }
    {
        RequestContext *reqCtx = new FakeRequestContext();
        reqCtx->optype_ = OpType::WRITE;
        reqCtx->logicpoolid_ = logicPoolId;
        reqCtx->copysetid_ = copysetId;
        reqCtx->chunkid_ = chunkId;
        reqCtx->data_ = buff1;
        reqCtx->offset_ = offset;
        reqCtx->rawlength_ = len;

        CountDownEvent cond(1);
        RequestClosure *reqDone = new FakeRequestClosure(&cond, reqCtx);
        reqCtx->done_ = reqDone;
        ChunkResponse response;
        response.set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS);
        EXPECT_CALL(mockMetaCache, GetLeader(_, _, _, _, _)).Times(AtLeast(1))
            .WillOnce(DoAll(SetArgPointee<2>(leaderId1),
                            SetArgPointee<3>(leaderAdder1),
                            Return(-1)))
            .WillOnce(DoAll(SetArgPointee<2>(leaderId1),
                            SetArgPointee<3>(leaderAdder1),
                            Return(0)));
        EXPECT_CALL(mockChunkService, WriteChunk(_, _, _, _)).Times(1)
            .WillOnce(DoAll(SetArgPointee<2>(response),
                            Invoke(WriteChunkFunc)));
        copysetClient.WriteChunk(logicPoolId, copysetId, chunkId,
                                 buff1, offset, len, reqDone, 0);
        cond.Wait();
        ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS,
                  reqDone->GetErrorCode());
    }
    {
        RequestContext *reqCtx = new FakeRequestContext();
        reqCtx->optype_ = OpType::WRITE;
        reqCtx->logicpoolid_ = logicPoolId;
        reqCtx->copysetid_ = copysetId;
        reqCtx->chunkid_ = chunkId;
        reqCtx->data_ = buff1;
        reqCtx->offset_ = offset;
        reqCtx->rawlength_ = len;

        CountDownEvent cond(1);
        RequestClosure *reqDone = new FakeRequestClosure(&cond, reqCtx);
        reqCtx->done_ = reqDone;
        ChunkResponse response;
        response.set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS);
        EXPECT_CALL(mockMetaCache, GetLeader(_, _, _, _, _)).Times(AtLeast(1))
            .WillOnce(DoAll(SetArgPointee<2>(leaderId1),
                            SetArgPointee<3>(leaderAdder1),
                            Return(-1)))
            .WillOnce(DoAll(SetArgPointee<2>(leaderId1),
                            SetArgPointee<3>(leaderAdder1),
                            Return(-1)))
            .WillOnce(DoAll(SetArgPointee<2>(leaderId1),
                            SetArgPointee<3>(leaderAdder1),
                            Return(0)));
        EXPECT_CALL(mockChunkService, WriteChunk(_, _, _, _)).Times(1)
            .WillOnce(DoAll(SetArgPointee<2>(response),
                            Invoke(WriteChunkFunc)));
        copysetClient.WriteChunk(logicPoolId, copysetId, chunkId,
                                 buff1, offset, len, reqDone, 0);
        cond.Wait();
        ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS,
                  reqDone->GetErrorCode());
    }
    /* read success */
    for (int i = 0; i < 10; ++i) {
        RequestContext *reqCtx = new FakeRequestContext();
        reqCtx->optype_ = OpType::READ;
        reqCtx->logicpoolid_ = logicPoolId;
        reqCtx->copysetid_ = copysetId;
        reqCtx->chunkid_ = chunkId;
        reqCtx->data_ = buff1;
        reqCtx->offset_ = i * 8;
        reqCtx->rawlength_ = len;

        CountDownEvent cond(1);
        RequestClosure *reqDone = new FakeRequestClosure(&cond, reqCtx);
        reqCtx->done_ = reqDone;
        ChunkResponse response;
        response.set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS);
        EXPECT_CALL(mockMetaCache, GetLeader(_, _, _, _, _)).Times(AtLeast(1))
            .WillOnce(DoAll(SetArgPointee<2>(leaderId1),
                            SetArgPointee<3>(leaderAdder1),
                            Return(0)));
        EXPECT_CALL(mockChunkService, ReadChunk(_, _, _, _)).Times(1)
            .WillOnce(DoAll(SetArgPointee<2>(response),
                            Invoke(ReadChunkFunc)));
        copysetClient.ReadChunk(logicPoolId, copysetId, chunkId,
                                offset, len, 0, reqDone, 0);
        cond.Wait();
        ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS,
                  reqDone->GetErrorCode());
    }
    {
        RequestContext *reqCtx = new FakeRequestContext();
        reqCtx->optype_ = OpType::READ;
        reqCtx->logicpoolid_ = logicPoolId;
        reqCtx->copysetid_ = copysetId;
        reqCtx->chunkid_ = chunkId;
        reqCtx->data_ = buff1;
        reqCtx->offset_ = offset;
        reqCtx->rawlength_ = len;

        CountDownEvent cond(1);
        RequestClosure *reqDone = new FakeRequestClosure(&cond, reqCtx);
        reqCtx->done_ = reqDone;
        ChunkResponse response;
        response.set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS);
        EXPECT_CALL(mockMetaCache, GetLeader(_, _, _, _, _)).Times(AtLeast(1))
            .WillOnce(DoAll(SetArgPointee<2>(leaderId1),
                            SetArgPointee<3>(leaderAdder1),
                            Return(-1)))
            .WillOnce(DoAll(SetArgPointee<2>(leaderId1),
                            SetArgPointee<3>(leaderAdder1),
                            Return(0)));
        EXPECT_CALL(mockChunkService, ReadChunk(_, _, _, _)).Times(1)
            .WillOnce(DoAll(SetArgPointee<2>(response),
                            Invoke(ReadChunkFunc)));
        copysetClient.ReadChunk(logicPoolId, copysetId, chunkId,
                                offset, len, 0, reqDone, 0);
        cond.Wait();
        ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS,
                  reqDone->GetErrorCode());
    }
    {
        RequestContext *reqCtx = new FakeRequestContext();
        reqCtx->optype_ = OpType::READ;
        reqCtx->logicpoolid_ = logicPoolId;
        reqCtx->copysetid_ = copysetId;
        reqCtx->chunkid_ = chunkId;
        reqCtx->data_ = buff1;
        reqCtx->offset_ = offset;
        reqCtx->rawlength_ = len;

        CountDownEvent cond(1);
        RequestClosure *reqDone = new FakeRequestClosure(&cond, reqCtx);
        reqCtx->done_ = reqDone;
        ChunkResponse response;
        response.set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS);
        EXPECT_CALL(mockMetaCache, GetLeader(_, _, _, _, _)).Times(AtLeast(1))
            .WillOnce(DoAll(SetArgPointee<2>(leaderId1),
                            SetArgPointee<3>(leaderAdder1),
                            Return(-1)))
            .WillOnce(DoAll(SetArgPointee<2>(leaderId1),
                            SetArgPointee<3>(leaderAdder1),
                            Return(-1)))
            .WillOnce(DoAll(SetArgPointee<2>(leaderId1),
                            SetArgPointee<3>(leaderAdder1),
                            Return(0)));
        EXPECT_CALL(mockChunkService, ReadChunk(_, _, _, _)).Times(1)
            .WillOnce(DoAll(SetArgPointee<2>(response),
                            Invoke(ReadChunkFunc)));
        copysetClient.ReadChunk(logicPoolId, copysetId, chunkId,
                                offset, len, 0, reqDone, 0);
        cond.Wait();
        ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS,
                  reqDone->GetErrorCode());
    }
}

/**
 * write error testing
 */
TEST_F(CopysetClientTest, write_error_test) {
    MockChunkServiceImpl mockChunkService;
    ASSERT_EQ(server_->AddService(&mockChunkService,
                                  brpc::SERVER_DOESNT_OWN_SERVICE), 0);
    ASSERT_EQ(server_->Start(listenAddr_.c_str(), nullptr), 0);

    CopysetClient copysetClient;
    RequestSenderManager senderManager;
    MockMetaCache mockMetaCache;
    mockMetaCache.DelegateToFake();
    copysetClient.Init(&senderManager, &mockMetaCache);

    LogicPoolID logicPoolId = 1;
    CopysetID copysetId = 100001;
    ChunkID chunkId = 1;
    size_t len = 8;
    char buff1[8 + 1];
    char buff2[8 + 1];
    memset(buff1, 'a', 8);
    memset(buff2, 'a', 8);
    buff1[8] = '\0';
    buff2[8] = '\0';
    off_t offset = 0;

    ChunkServerID leaderId1 = 10000;
    butil::EndPoint leaderAdder1;
    std::string leaderStr1 = "127.0.0.1:8200";
    butil::str2endpoint(leaderStr1.c_str(), &leaderAdder1);
    ChunkServerID leaderId2 = 10001;
    butil::EndPoint leaderAdder2;
    std::string leaderStr2 = "127.0.0.1:8200";
    butil::str2endpoint(leaderStr2.c_str(), &leaderAdder2);
    ChunkServerID leaderId3 = 10002;
    butil::EndPoint leaderAdder3;
    std::string leaderStr3 = "127.0.0.1:8200";
    butil::str2endpoint(leaderStr3.c_str(), &leaderAdder3);
    ChunkServerID leaderId4 = 10003;
    butil::EndPoint leaderAdder4;
    std::string leaderStr4 = "127.0.0.1:8200";
    butil::str2endpoint(leaderStr4.c_str(), &leaderAdder4);

    /* 非法参数 */
    {
        RequestContext *reqCtx = new FakeRequestContext();
        reqCtx->optype_ = OpType::WRITE;
        reqCtx->logicpoolid_ = logicPoolId;
        reqCtx->copysetid_ = copysetId;
        reqCtx->chunkid_ = chunkId;
        reqCtx->data_ = buff1;
        reqCtx->offset_ = 0;
        reqCtx->rawlength_ = len;

        CountDownEvent cond(1);
        RequestClosure *reqDone = new FakeRequestClosure(&cond, reqCtx);
        reqCtx->done_ = reqDone;
        ChunkResponse response;
        response.set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_INVALID_REQUEST);
        EXPECT_CALL(mockMetaCache, GetLeader(_, _, _, _, _))
            .Times(AtLeast(1)).WillOnce(DoAll(SetArgPointee<2>(leaderId1),
                                              SetArgPointee<3>(leaderAdder1),
                                              Return(0)));
        EXPECT_CALL(mockChunkService, WriteChunk(_, _, _, _)).Times(1)
            .WillOnce(DoAll(SetArgPointee<2>(response),
                            Invoke(WriteChunkFunc)));
        copysetClient.WriteChunk(logicPoolId, copysetId, chunkId,
                                 buff1, offset, len, reqDone, 0);
        cond.Wait();
        ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_INVALID_REQUEST,
                  reqDone->GetErrorCode());
    }
    /* controller error */
    {
        RequestContext *reqCtx = new FakeRequestContext();
        reqCtx->optype_ = OpType::WRITE;
        reqCtx->logicpoolid_ = logicPoolId;
        reqCtx->copysetid_ = copysetId;
        reqCtx->chunkid_ = chunkId;
        reqCtx->data_ = buff1;
        reqCtx->offset_ = 0;
        reqCtx->rawlength_ = len;

        CountDownEvent cond(1);
        RequestClosure *reqDone = new FakeRequestClosure(&cond, reqCtx);
        reqCtx->done_ = reqDone;
        gWriteCntlFailedCode = -1;
        EXPECT_CALL(mockMetaCache, GetLeader(_, _, _, _, _))
            .Times(3).WillRepeatedly(DoAll(SetArgPointee<2>(leaderId1),
                                           SetArgPointee<3>(leaderAdder1),
                                           Return(0)));
        EXPECT_CALL(mockChunkService, WriteChunk(_, _, _, _)).Times(3)
            .WillRepeatedly(Invoke(WriteChunkFunc));
        copysetClient.WriteChunk(logicPoolId, copysetId, chunkId,
                                 buff1, offset, len, reqDone, 0);
        cond.Wait();
        ASSERT_NE(0, reqDone->GetErrorCode());
        gWriteCntlFailedCode = 0;
    }
    /* 其他错误 */
    {
        RequestContext *reqCtx = new FakeRequestContext();
        reqCtx->optype_ = OpType::WRITE;
        reqCtx->logicpoolid_ = logicPoolId;
        reqCtx->copysetid_ = copysetId;
        reqCtx->chunkid_ = chunkId;
        reqCtx->data_ = buff1;
        reqCtx->offset_ = 0;
        reqCtx->rawlength_ = len;

        CountDownEvent cond(1);
        RequestClosure *reqDone = new FakeRequestClosure(&cond, reqCtx);
        reqCtx->done_ = reqDone;
        ChunkResponse response;
        response.set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_FAILURE_UNKNOWN);
        EXPECT_CALL(mockMetaCache, GetLeader(_, _, _, _, _))
            .Times(3).WillRepeatedly(DoAll(SetArgPointee<2>(leaderId1),
                                           SetArgPointee<3>(leaderAdder1),
                                           Return(0)));
        EXPECT_CALL(mockChunkService, WriteChunk(_, _, _, _)).Times(3)
            .WillRepeatedly(DoAll(SetArgPointee<2>(response),
                                  Invoke(WriteChunkFunc)));
        copysetClient.WriteChunk(logicPoolId, copysetId, chunkId,
                                 buff1, offset, len, reqDone, 0);
        cond.Wait();
        ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_FAILURE_UNKNOWN,
                  reqDone->GetErrorCode());
    }
    /* 不是 leader，返回正确的 leader */
    {
        RequestContext *reqCtx = new FakeRequestContext();
        reqCtx->optype_ = OpType::WRITE;
        reqCtx->logicpoolid_ = logicPoolId;
        reqCtx->copysetid_ = copysetId;
        reqCtx->chunkid_ = chunkId;
        reqCtx->data_ = buff1;
        reqCtx->offset_ = 0;
        reqCtx->rawlength_ = len;

        CountDownEvent cond(1);
        RequestClosure *reqDone = new FakeRequestClosure(&cond, reqCtx);
        reqCtx->done_ = reqDone;
        ChunkResponse response1;
        response1.set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_REDIRECTED);
        response1.set_redirect(leaderStr2);
        ChunkResponse response2;
        response2.set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS);
        EXPECT_CALL(mockMetaCache, GetLeader(_, _, _, _, _)).Times(2)
            .WillOnce(DoAll(SetArgPointee<2>(leaderId1),
                            SetArgPointee<3>(leaderAdder1),
                            Return(0)))
            .WillOnce(DoAll(SetArgPointee<2>(leaderId2),
                            SetArgPointee<3>(leaderAdder2),
                            Return(0)));
        EXPECT_CALL(mockMetaCache, UpdateLeader(_, _, _, _)).Times(1)
            .WillOnce(Return(0));
        EXPECT_CALL(mockChunkService, WriteChunk(_, _, _, _)).Times(2)
            .WillOnce(DoAll(SetArgPointee<2>(response1),
                            Invoke(WriteChunkFunc)))
            .WillOnce(DoAll(SetArgPointee<2>(response2),
                            Invoke(WriteChunkFunc)));
        copysetClient.WriteChunk(logicPoolId, copysetId, chunkId,
                                 buff1, offset, len, reqDone, 0);
        cond.Wait();
        ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS,
                  reqDone->GetErrorCode());
    }
    /* 不是 leader，没有返回 leader，刷新 meta cache 成功 */
    {
        RequestContext *reqCtx = new FakeRequestContext();
        reqCtx->optype_ = OpType::WRITE;
        reqCtx->logicpoolid_ = logicPoolId;
        reqCtx->copysetid_ = copysetId;
        reqCtx->chunkid_ = chunkId;
        reqCtx->data_ = buff1;
        reqCtx->offset_ = 0;
        reqCtx->rawlength_ = len;

        CountDownEvent cond(1);
        RequestClosure *reqDone = new FakeRequestClosure(&cond, reqCtx);
        reqCtx->done_ = reqDone;
        ChunkResponse response1;
        response1.set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_REDIRECTED);
//        response1.set_redirect(leaderStr2);
        ChunkResponse response2;
        response2.set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS);
        EXPECT_CALL(mockMetaCache, GetLeader(_, _, _, _, _)).Times(3)
            .WillOnce(DoAll(SetArgPointee<2>(leaderId1),
                            SetArgPointee<3>(leaderAdder1),
                            Return(0)))
            .WillOnce(DoAll(SetArgPointee<2>(leaderId2),
                            SetArgPointee<3>(leaderAdder2),
                            Return(0)))
            .WillOnce(DoAll(SetArgPointee<2>(leaderId2),
                            SetArgPointee<3>(leaderAdder2),
                            Return(0)));
        EXPECT_CALL(mockChunkService, WriteChunk(_, _, _, _)).Times(2)
            .WillOnce(DoAll(SetArgPointee<2>(response1),
                            Invoke(WriteChunkFunc)))
            .WillOnce(DoAll(SetArgPointee<2>(response2),
                            Invoke(WriteChunkFunc)));
        copysetClient.WriteChunk(logicPoolId, copysetId, chunkId,
                                 buff1, offset, len, reqDone, 0);
        cond.Wait();
        ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS,
                  reqDone->GetErrorCode());
    }
    /* 不是 leader，没有返回 leader，刷新 meta cache 失败 */
    {
        RequestContext *reqCtx = new FakeRequestContext();
        reqCtx->optype_ = OpType::WRITE;
        reqCtx->logicpoolid_ = logicPoolId;
        reqCtx->copysetid_ = copysetId;
        reqCtx->chunkid_ = chunkId;
        reqCtx->data_ = buff1;
        reqCtx->offset_ = 0;
        reqCtx->rawlength_ = len;

        CountDownEvent cond(1);
        RequestClosure *reqDone = new FakeRequestClosure(&cond, reqCtx);
        reqCtx->done_ = reqDone;
        ChunkResponse response1;
        response1.set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_REDIRECTED);
//        response1.set_redirect(leaderStr2);
        ChunkResponse response2;
        response2.set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS);
        EXPECT_CALL(mockMetaCache, GetLeader(_, _, _, _, _)).Times(3)
            .WillOnce(DoAll(SetArgPointee<2>(leaderId1),
                            SetArgPointee<3>(leaderAdder1),
                            Return(0)))
            .WillOnce(DoAll(SetArgPointee<2>(leaderId2),
                            SetArgPointee<3>(leaderAdder2),
                            Return(-1)))
            .WillOnce(DoAll(SetArgPointee<2>(leaderId2),
                            SetArgPointee<3>(leaderAdder2),
                            Return(0)));
        EXPECT_CALL(mockChunkService, WriteChunk(_, _, _, _)).Times(2)
            .WillOnce(DoAll(SetArgPointee<2>(response1),
                            Invoke(WriteChunkFunc)))
            .WillOnce(DoAll(SetArgPointee<2>(response2),
                            Invoke(WriteChunkFunc)));
        copysetClient.WriteChunk(logicPoolId, copysetId, chunkId,
                                 buff1, offset, len, reqDone, 0);
        cond.Wait();
        ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS,
                  reqDone->GetErrorCode());
    }
    /* 不是 leader，但返回的是错误 leader */
    {
        RequestContext *reqCtx = new FakeRequestContext();
        reqCtx->optype_ = OpType::WRITE;
        reqCtx->logicpoolid_ = logicPoolId;
        reqCtx->copysetid_ = copysetId;
        reqCtx->chunkid_ = chunkId;
        reqCtx->data_ = buff1;
        reqCtx->offset_ = 0;
        reqCtx->rawlength_ = len;

        CountDownEvent cond(1);
        RequestClosure *reqDone = new FakeRequestClosure(&cond, reqCtx);
        reqCtx->done_ = reqDone;
        ChunkResponse response1;
        response1.set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_REDIRECTED);
        response1.set_redirect(leaderStr2);
        ChunkResponse response2;
        response2.set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_REDIRECTED);
        response2.set_redirect(leaderStr3);
        ChunkResponse response3;
        response3.set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_REDIRECTED);
        response3.set_redirect(leaderStr4);
        EXPECT_CALL(mockMetaCache, GetLeader(_, _, _, _, _)).Times(3)
            .WillOnce(DoAll(SetArgPointee<2>(leaderId1),
                            SetArgPointee<3>(leaderAdder1),
                            Return(0)))
            .WillOnce(DoAll(SetArgPointee<2>(leaderId2),
                            SetArgPointee<3>(leaderAdder2),
                            Return(0)))
            .WillOnce(DoAll(SetArgPointee<2>(leaderId3),
                            SetArgPointee<3>(leaderAdder3),
                            Return(0)));
        EXPECT_CALL(mockMetaCache, UpdateLeader(_, _, _, _)).Times(3)
            .WillRepeatedly(Return(0));
        EXPECT_CALL(mockChunkService, WriteChunk(_, _, _, _)).Times(3)
            .WillOnce(DoAll(SetArgPointee<2>(response1),
                            Invoke(WriteChunkFunc)))
            .WillOnce(DoAll(SetArgPointee<2>(response2),
                            Invoke(WriteChunkFunc)))
            .WillOnce(DoAll(SetArgPointee<2>(response3),
                            Invoke(WriteChunkFunc)));
        copysetClient.WriteChunk(logicPoolId, copysetId, chunkId,
                                 buff1, offset, len, reqDone, 0);
        cond.Wait();
        ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_REDIRECTED,
                  reqDone->GetErrorCode());
    }
    /* copyset 不存在，更新 leader 依然失败 */
    {
        RequestContext *reqCtx = new FakeRequestContext();
        reqCtx->optype_ = OpType::WRITE;
        reqCtx->logicpoolid_ = logicPoolId;
        reqCtx->copysetid_ = copysetId;
        reqCtx->chunkid_ = chunkId;
        reqCtx->data_ = buff1;
        reqCtx->offset_ = 0;
        reqCtx->rawlength_ = len;

        CountDownEvent cond(1);
        RequestClosure *reqDone = new FakeRequestClosure(&cond, reqCtx);
        reqCtx->done_ = reqDone;
        ChunkResponse response1;
        response1.set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_COPYSET_NOTEXIST);
        response1.set_redirect(leaderStr2);
        ChunkResponse response2;
        response2.set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_COPYSET_NOTEXIST);
        response2.set_redirect(leaderStr3);
        ChunkResponse response3;
        response3.set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_COPYSET_NOTEXIST);
        response3.set_redirect(leaderStr4);
        EXPECT_CALL(mockMetaCache, GetLeader(_, _, _, _, _)).Times(6)
            .WillOnce(DoAll(SetArgPointee<2>(leaderId1),
                            SetArgPointee<3>(leaderAdder1),
                            Return(0)))
            .WillOnce(DoAll(SetArgPointee<2>(leaderId2),
                            SetArgPointee<3>(leaderAdder2),
                            Return(0)))
            .WillOnce(DoAll(SetArgPointee<2>(leaderId2),
                            SetArgPointee<3>(leaderAdder2),
                            Return(0)))
            .WillOnce(DoAll(SetArgPointee<2>(leaderId3),
                            SetArgPointee<3>(leaderAdder3),
                            Return(0)))
            .WillOnce(DoAll(SetArgPointee<2>(leaderId3),
                            SetArgPointee<3>(leaderAdder3),
                            Return(0)))
            .WillOnce(DoAll(SetArgPointee<2>(leaderId4),
                            SetArgPointee<3>(leaderAdder4),
                            Return(0)));
        EXPECT_CALL(mockChunkService, WriteChunk(_, _, _, _)).Times(3)
            .WillOnce(DoAll(SetArgPointee<2>(response1),
                            Invoke(WriteChunkFunc)))
            .WillOnce(DoAll(SetArgPointee<2>(response2),
                            Invoke(WriteChunkFunc)))
            .WillOnce(DoAll(SetArgPointee<2>(response3),
                            Invoke(WriteChunkFunc)));
        copysetClient.WriteChunk(logicPoolId, copysetId, chunkId,
                                 buff1, offset, len, reqDone, 0);
        cond.Wait();
        ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_COPYSET_NOTEXIST,
                  reqDone->GetErrorCode());
    }
    /* copyset 不存在，更新 leader 成功 */
    {
        RequestContext *reqCtx = new FakeRequestContext();
        reqCtx->optype_ = OpType::WRITE;
        reqCtx->logicpoolid_ = logicPoolId;
        reqCtx->copysetid_ = copysetId;
        reqCtx->chunkid_ = chunkId;
        reqCtx->data_ = buff1;
        reqCtx->offset_ = 0;
        reqCtx->rawlength_ = len;

        CountDownEvent cond(1);
        RequestClosure *reqDone = new FakeRequestClosure(&cond, reqCtx);
        reqCtx->done_ = reqDone;
        ChunkResponse response1;
        response1.set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_COPYSET_NOTEXIST);
        response1.set_redirect(leaderStr2);
        ChunkResponse response2;
        response2.set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS);
        response2.set_redirect(leaderStr3);
        EXPECT_CALL(mockMetaCache, GetLeader(_, _, _, _, _)).Times(3)
            .WillOnce(DoAll(SetArgPointee<2>(leaderId1),
                            SetArgPointee<3>(leaderAdder1),
                            Return(0)))
            .WillOnce(DoAll(SetArgPointee<2>(leaderId2),
                            SetArgPointee<3>(leaderAdder2),
                            Return(0)))
            .WillOnce(DoAll(SetArgPointee<2>(leaderId2),
                            SetArgPointee<3>(leaderAdder2),
                            Return(0)));
        EXPECT_CALL(mockChunkService, WriteChunk(_, _, _, _)).Times(2)
            .WillOnce(DoAll(SetArgPointee<2>(response1),
                            Invoke(WriteChunkFunc)))
            .WillOnce(DoAll(SetArgPointee<2>(response2),
                            Invoke(WriteChunkFunc)));
        copysetClient.WriteChunk(logicPoolId, copysetId, chunkId,
                                 buff1, offset, len, reqDone, 0);
        cond.Wait();
        ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS,
                  reqDone->GetErrorCode());
    }
}

/**
 * read error testing
 */
TEST_F(CopysetClientTest, read_error_test) {
    MockChunkServiceImpl mockChunkService;
    ASSERT_EQ(server_->AddService(&mockChunkService,
                                  brpc::SERVER_DOESNT_OWN_SERVICE), 0);
    ASSERT_EQ(server_->Start(listenAddr_.c_str(), nullptr), 0);

    CopysetClient copysetClient;
    RequestSenderManager senderManager;
    MockMetaCache mockMetaCache;
    mockMetaCache.DelegateToFake();
    copysetClient.Init(&senderManager, &mockMetaCache);

    LogicPoolID logicPoolId = 1;
    CopysetID copysetId = 100001;
    ChunkID chunkId = 1;
    size_t len = 8;
    char buff1[8 + 1];
    char buff2[8 + 1];
    memset(buff1, 'a', 8);
    memset(buff2, 'a', 8);
    buff1[8] = '\0';
    buff2[8] = '\0';
    off_t offset = 0;

    ChunkServerID leaderId1 = 10000;
    butil::EndPoint leaderAdder1;
    std::string leaderStr1 = "127.0.0.1:8200";
    butil::str2endpoint(leaderStr1.c_str(), &leaderAdder1);
    ChunkServerID leaderId2 = 10001;
    butil::EndPoint leaderAdder2;
    std::string leaderStr2 = "127.0.0.1:8200";
    butil::str2endpoint(leaderStr2.c_str(), &leaderAdder2);
    ChunkServerID leaderId3 = 10002;
    butil::EndPoint leaderAdder3;
    std::string leaderStr3 = "127.0.0.1:8200";
    butil::str2endpoint(leaderStr3.c_str(), &leaderAdder3);
    ChunkServerID leaderId4 = 10003;
    butil::EndPoint leaderAdder4;
    std::string leaderStr4 = "127.0.0.1:8200";
    butil::str2endpoint(leaderStr4.c_str(), &leaderAdder4);

    /* 非法参数 */
    {
        RequestContext *reqCtx = new FakeRequestContext();
        reqCtx->optype_ = OpType::READ;
        reqCtx->logicpoolid_ = logicPoolId;
        reqCtx->copysetid_ = copysetId;
        reqCtx->chunkid_ = chunkId;
        reqCtx->data_ = buff1;
        reqCtx->offset_ = 0;
        reqCtx->rawlength_ = len;

        CountDownEvent cond(1);
        RequestClosure *reqDone = new FakeRequestClosure(&cond, reqCtx);
        reqCtx->done_ = reqDone;
        ChunkResponse response;
        response.set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_INVALID_REQUEST);
        EXPECT_CALL(mockMetaCache, GetLeader(_, _, _, _, _))
            .Times(AtLeast(1)).WillOnce(DoAll(SetArgPointee<2>(leaderId1),
                                              SetArgPointee<3>(leaderAdder1),
                                              Return(0)));
        EXPECT_CALL(mockChunkService, ReadChunk(_, _, _, _)).Times(1)
            .WillOnce(DoAll(SetArgPointee<2>(response),
                            Invoke(ReadChunkFunc)));
        copysetClient.ReadChunk(logicPoolId, copysetId, chunkId,
                                offset, len, 0, reqDone, 0);
        cond.Wait();
        ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_INVALID_REQUEST,
                  reqDone->GetErrorCode());
    }
    /* controller error */
    {
        RequestContext *reqCtx = new FakeRequestContext();
        reqCtx->optype_ = OpType::READ;
        reqCtx->logicpoolid_ = logicPoolId;
        reqCtx->copysetid_ = copysetId;
        reqCtx->chunkid_ = chunkId;
        reqCtx->offset_ = 0;
        reqCtx->rawlength_ = len;

        CountDownEvent cond(1);
        RequestClosure *reqDone = new FakeRequestClosure(&cond, reqCtx);
        reqCtx->done_ = reqDone;
        gReadCntlFailedCode = -1;
        EXPECT_CALL(mockMetaCache, GetLeader(_, _, _, _, _)).Times(3)
            .WillRepeatedly(DoAll(SetArgPointee<2>(leaderId1),
                                  SetArgPointee<3>(leaderAdder1),
                                  Return(0)));
        EXPECT_CALL(mockChunkService, ReadChunk(_, _, _, _)).Times(3)
            .WillRepeatedly(Invoke(ReadChunkFunc));
        copysetClient.ReadChunk(logicPoolId, copysetId, chunkId,
                                offset, len, 0, reqDone, 0);
        cond.Wait();
        ASSERT_NE(0, reqDone->GetErrorCode());
        gReadCntlFailedCode = 0;
    }
    /* 其他错误 */
    {
        RequestContext *reqCtx = new FakeRequestContext();
        reqCtx->optype_ = OpType::READ;
        reqCtx->logicpoolid_ = logicPoolId;
        reqCtx->copysetid_ = copysetId;
        reqCtx->chunkid_ = chunkId;
        reqCtx->data_ = buff1;
        reqCtx->offset_ = 0;
        reqCtx->rawlength_ = len;

        CountDownEvent cond(1);
        RequestClosure *reqDone = new FakeRequestClosure(&cond, reqCtx);
        reqCtx->done_ = reqDone;
        ChunkResponse response;
        response.set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_FAILURE_UNKNOWN);
        EXPECT_CALL(mockMetaCache, GetLeader(_, _, _, _, _))
            .Times(3).WillRepeatedly(DoAll(SetArgPointee<2>(leaderId1),
                                           SetArgPointee<3>(leaderAdder1),
                                           Return(0)));
        EXPECT_CALL(mockChunkService, ReadChunk(_, _, _, _)).Times(3)
            .WillRepeatedly(DoAll(SetArgPointee<2>(response),
                                  Invoke(ReadChunkFunc)));
        copysetClient.ReadChunk(logicPoolId, copysetId, chunkId,
                                offset, len, 0, reqDone, 0);
        cond.Wait();
        ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_FAILURE_UNKNOWN,
                  reqDone->GetErrorCode());
    }
    /* 不是 leader，返回正确的 leader */
    {
        RequestContext *reqCtx = new FakeRequestContext();
        reqCtx->optype_ = OpType::READ;
        reqCtx->logicpoolid_ = logicPoolId;
        reqCtx->copysetid_ = copysetId;
        reqCtx->chunkid_ = chunkId;
        reqCtx->data_ = buff1;
        reqCtx->offset_ = 0;
        reqCtx->rawlength_ = len;

        CountDownEvent cond(1);
        RequestClosure *reqDone = new FakeRequestClosure(&cond, reqCtx);
        reqCtx->done_ = reqDone;
        ChunkResponse response1;
        response1.set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_REDIRECTED);
        response1.set_redirect(leaderStr2);
        ChunkResponse response2;
        response2.set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS);
        EXPECT_CALL(mockMetaCache, GetLeader(_, _, _, _, _)).Times(2)
            .WillOnce(DoAll(SetArgPointee<2>(leaderId1),
                            SetArgPointee<3>(leaderAdder1),
                            Return(0)))
            .WillOnce(DoAll(SetArgPointee<2>(leaderId2),
                            SetArgPointee<3>(leaderAdder2),
                            Return(0)));
        EXPECT_CALL(mockMetaCache, UpdateLeader(_, _, _, _)).Times(1)
            .WillOnce(Return(0));
        EXPECT_CALL(mockChunkService, ReadChunk(_, _, _, _)).Times(2)
            .WillOnce(DoAll(SetArgPointee<2>(response1),
                            Invoke(ReadChunkFunc)))
            .WillOnce(DoAll(SetArgPointee<2>(response2),
                            Invoke(ReadChunkFunc)));
        copysetClient.ReadChunk(logicPoolId, copysetId, chunkId,
                                offset, len, 0, reqDone, 0);
        cond.Wait();
        ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS,
                  reqDone->GetErrorCode());
    }
    /* 不是 leader，但是没有返回 leader，刷新 meta cache 成功 */
    {
        RequestContext *reqCtx = new FakeRequestContext();
        reqCtx->optype_ = OpType::READ;
        reqCtx->logicpoolid_ = logicPoolId;
        reqCtx->copysetid_ = copysetId;
        reqCtx->chunkid_ = chunkId;
        reqCtx->data_ = buff1;
        reqCtx->offset_ = 0;
        reqCtx->rawlength_ = len;

        CountDownEvent cond(1);
        RequestClosure *reqDone = new FakeRequestClosure(&cond, reqCtx);
        reqCtx->done_ = reqDone;
        ChunkResponse response1;
        response1.set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_REDIRECTED);
//        response1.set_redirect(leaderStr2);
        ChunkResponse response2;
        response2.set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS);
        EXPECT_CALL(mockMetaCache, GetLeader(_, _, _, _, _)).Times(3)
            .WillOnce(DoAll(SetArgPointee<2>(leaderId1),
                            SetArgPointee<3>(leaderAdder1),
                            Return(0)))
            .WillOnce(DoAll(SetArgPointee<2>(leaderId2),
                            SetArgPointee<3>(leaderAdder2),
                            Return(0)))
            .WillOnce(DoAll(SetArgPointee<2>(leaderId2),
                            SetArgPointee<3>(leaderAdder2),
                            Return(0)));
        EXPECT_CALL(mockChunkService, ReadChunk(_, _, _, _)).Times(2)
            .WillOnce(DoAll(SetArgPointee<2>(response1),
                            Invoke(ReadChunkFunc)))
            .WillOnce(DoAll(SetArgPointee<2>(response2),
                            Invoke(ReadChunkFunc)));
        copysetClient.ReadChunk(logicPoolId, copysetId, chunkId,
                                offset, len, 0, reqDone, 0);
        cond.Wait();
        ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS,
                  reqDone->GetErrorCode());
    }
    /* 不是 leader，但是没有返回 leader，刷新 meta cache 失败 */
    {
        RequestContext *reqCtx = new FakeRequestContext();
        reqCtx->optype_ = OpType::READ;
        reqCtx->logicpoolid_ = logicPoolId;
        reqCtx->copysetid_ = copysetId;
        reqCtx->chunkid_ = chunkId;
        reqCtx->data_ = buff1;
        reqCtx->offset_ = 0;
        reqCtx->rawlength_ = len;

        CountDownEvent cond(1);
        RequestClosure *reqDone = new FakeRequestClosure(&cond, reqCtx);
        reqCtx->done_ = reqDone;
        ChunkResponse response1;
        response1.set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_REDIRECTED);
//        response1.set_redirect(leaderStr2);
        ChunkResponse response2;
        response2.set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS);
        EXPECT_CALL(mockMetaCache, GetLeader(_, _, _, _, _)).Times(3)
            .WillOnce(DoAll(SetArgPointee<2>(leaderId1),
                            SetArgPointee<3>(leaderAdder1),
                            Return(0)))
            .WillOnce(DoAll(SetArgPointee<2>(leaderId2),
                            SetArgPointee<3>(leaderAdder2),
                            Return(-1)))
            .WillOnce(DoAll(SetArgPointee<2>(leaderId2),
                            SetArgPointee<3>(leaderAdder2),
                            Return(0)));
        EXPECT_CALL(mockChunkService, ReadChunk(_, _, _, _)).Times(2)
            .WillOnce(DoAll(SetArgPointee<2>(response1),
                            Invoke(ReadChunkFunc)))
            .WillOnce(DoAll(SetArgPointee<2>(response2),
                            Invoke(ReadChunkFunc)));
        copysetClient.ReadChunk(logicPoolId, copysetId, chunkId,
                                offset, len, 0, reqDone, 0);
        cond.Wait();
        ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS,
                  reqDone->GetErrorCode());
    }
    /* 不是 leader，但返回的是错误 leader */
    {
        RequestContext *reqCtx = new FakeRequestContext();
        reqCtx->optype_ = OpType::READ;
        reqCtx->logicpoolid_ = logicPoolId;
        reqCtx->copysetid_ = copysetId;
        reqCtx->chunkid_ = chunkId;
        reqCtx->data_ = buff1;
        reqCtx->offset_ = 0;
        reqCtx->rawlength_ = len;

        CountDownEvent cond(1);
        RequestClosure *reqDone = new FakeRequestClosure(&cond, reqCtx);
        reqCtx->done_ = reqDone;
        ChunkResponse response1;
        response1.set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_REDIRECTED);
        response1.set_redirect(leaderStr2);
        ChunkResponse response2;
        response2.set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_REDIRECTED);
        response2.set_redirect(leaderStr3);
        ChunkResponse response3;
        response3.set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_REDIRECTED);
        response3.set_redirect(leaderStr4);
        EXPECT_CALL(mockMetaCache, GetLeader(_, _, _, _, _)).Times(3)
            .WillOnce(DoAll(SetArgPointee<2>(leaderId1),
                            SetArgPointee<3>(leaderAdder1),
                            Return(0)))
            .WillOnce(DoAll(SetArgPointee<2>(leaderId2),
                            SetArgPointee<3>(leaderAdder2),
                            Return(0)))
            .WillOnce(DoAll(SetArgPointee<2>(leaderId3),
                            SetArgPointee<3>(leaderAdder3),
                            Return(0)));
        EXPECT_CALL(mockMetaCache, UpdateLeader(_, _, _, _)).Times(3)
            .WillRepeatedly(Return(0));
        EXPECT_CALL(mockChunkService, ReadChunk(_, _, _, _)).Times(3)
            .WillOnce(DoAll(SetArgPointee<2>(response1),
                            Invoke(ReadChunkFunc)))
            .WillOnce(DoAll(SetArgPointee<2>(response2),
                            Invoke(ReadChunkFunc)))
            .WillOnce(DoAll(SetArgPointee<2>(response3),
                            Invoke(ReadChunkFunc)));
        copysetClient.ReadChunk(logicPoolId, copysetId, chunkId,
                                offset, len, 0, reqDone, 0);
        cond.Wait();
        ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_REDIRECTED,
                  reqDone->GetErrorCode());
    }
    /* copyset 不存在，更新 leader 依然失败 */
    {
        RequestContext *reqCtx = new FakeRequestContext();
        reqCtx->optype_ = OpType::READ;
        reqCtx->logicpoolid_ = logicPoolId;
        reqCtx->copysetid_ = copysetId;
        reqCtx->chunkid_ = chunkId;
        reqCtx->data_ = buff1;
        reqCtx->offset_ = 0;
        reqCtx->rawlength_ = len;

        CountDownEvent cond(1);
        RequestClosure *reqDone = new FakeRequestClosure(&cond, reqCtx);
        reqCtx->done_ = reqDone;
        ChunkResponse response1;
        response1.set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_COPYSET_NOTEXIST);
        response1.set_redirect(leaderStr2);
        ChunkResponse response2;
        response2.set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_COPYSET_NOTEXIST);
        response2.set_redirect(leaderStr3);
        ChunkResponse response3;
        response3.set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_COPYSET_NOTEXIST);
        response3.set_redirect(leaderStr4);
        EXPECT_CALL(mockMetaCache, GetLeader(_, _, _, _, _)).Times(6)
            .WillOnce(DoAll(SetArgPointee<2>(leaderId1),
                            SetArgPointee<3>(leaderAdder1),
                            Return(0)))
            .WillOnce(DoAll(SetArgPointee<2>(leaderId2),
                            SetArgPointee<3>(leaderAdder2),
                            Return(0)))
            .WillOnce(DoAll(SetArgPointee<2>(leaderId2),
                            SetArgPointee<3>(leaderAdder2),
                            Return(0)))
            .WillOnce(DoAll(SetArgPointee<2>(leaderId3),
                            SetArgPointee<3>(leaderAdder3),
                            Return(0)))
            .WillOnce(DoAll(SetArgPointee<2>(leaderId3),
                            SetArgPointee<3>(leaderAdder3),
                            Return(0)))
            .WillOnce(DoAll(SetArgPointee<2>(leaderId4),
                            SetArgPointee<3>(leaderAdder4),
                            Return(0)));
        EXPECT_CALL(mockChunkService, ReadChunk(_, _, _, _)).Times(3)
            .WillOnce(DoAll(SetArgPointee<2>(response1),
                            Invoke(ReadChunkFunc)))
            .WillOnce(DoAll(SetArgPointee<2>(response2),
                            Invoke(ReadChunkFunc)))
            .WillOnce(DoAll(SetArgPointee<2>(response3),
                            Invoke(ReadChunkFunc)));
        copysetClient.ReadChunk(logicPoolId, copysetId, chunkId,
                                offset, len, 0, reqDone, 0);
        cond.Wait();
        ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_COPYSET_NOTEXIST,
                  reqDone->GetErrorCode());
    }
    /* copyset 不存在，更新 leader 成功 */
    {
        RequestContext *reqCtx = new FakeRequestContext();
        reqCtx->optype_ = OpType::READ;
        reqCtx->logicpoolid_ = logicPoolId;
        reqCtx->copysetid_ = copysetId;
        reqCtx->chunkid_ = chunkId;
        reqCtx->data_ = buff1;
        reqCtx->offset_ = 0;
        reqCtx->rawlength_ = len;

        CountDownEvent cond(1);
        RequestClosure *reqDone = new FakeRequestClosure(&cond, reqCtx);
        reqCtx->done_ = reqDone;
        ChunkResponse response1;
        response1.set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_COPYSET_NOTEXIST);
        response1.set_redirect(leaderStr2);
        ChunkResponse response2;
        response2.set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS);
        response2.set_redirect(leaderStr3);
        EXPECT_CALL(mockMetaCache, GetLeader(_, _, _, _, _)).Times(3)
            .WillOnce(DoAll(SetArgPointee<2>(leaderId1),
                            SetArgPointee<3>(leaderAdder1),
                            Return(0)))
            .WillOnce(DoAll(SetArgPointee<2>(leaderId2),
                            SetArgPointee<3>(leaderAdder2),
                            Return(0)))
            .WillOnce(DoAll(SetArgPointee<2>(leaderId2),
                            SetArgPointee<3>(leaderAdder2),
                            Return(0)));
        EXPECT_CALL(mockChunkService, ReadChunk(_, _, _, _)).Times(2)
            .WillOnce(DoAll(SetArgPointee<2>(response1),
                            Invoke(ReadChunkFunc)))
            .WillOnce(DoAll(SetArgPointee<2>(response2),
                            Invoke(ReadChunkFunc)));
        copysetClient.ReadChunk(logicPoolId, copysetId, chunkId,
                                offset, len, 0, reqDone, 0);
        cond.Wait();
        ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS,
                  reqDone->GetErrorCode());
    }
}

}   // namespace client
}   // namespace curve
