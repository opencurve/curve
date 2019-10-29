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
#include <chrono>   // NOLINT

#include "src/client/copyset_client.h"
#include "test/client/mock_meta_cache.h"
#include "src/common/concurrent/count_down_event.h"
#include "test/client/mock_chunkservice.h"
#include "test/client/mock_request_context.h"
#include "src/client/chunk_closure.h"
#include "src/common/timeutility.h"

namespace curve {
namespace client {

using curve::chunkserver::CHUNK_OP_STATUS;

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
using curve::common::TimeUtility;

class CopysetClientTest : public testing::Test {
 protected:
    virtual void SetUp() {
        listenAddr_ = "127.0.0.1:9109";
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
        if (gWriteCntlFailedCode == brpc::ERPCTIMEDOUT) {
            std::this_thread::sleep_for(std::chrono::milliseconds(3500));
        }
        brpc::Controller *cntl = dynamic_cast<brpc::Controller *>(controller);
        cntl->SetFailed(gWriteCntlFailedCode, "write controller error");
    }
}

static void ReadChunkFunc(::google::protobuf::RpcController *controller,
                          const ::curve::chunkserver::ChunkRequest *request,
                          ::curve::chunkserver::ChunkResponse *response,
                          google::protobuf::Closure *done) {
    brpc::ClosureGuard doneGuard(done);
    if (0 != gReadCntlFailedCode) {
        if (gReadCntlFailedCode == brpc::ERPCTIMEDOUT) {
            std::this_thread::sleep_for(std::chrono::milliseconds(4000));
        }
        brpc::Controller *cntl = dynamic_cast<brpc::Controller *>(controller);
        cntl->SetFailed(gReadCntlFailedCode, "read controller error");
    }
}

static void ReadChunkSnapshotFunc(::google::protobuf::RpcController *controller,
                                    const ::curve::chunkserver::ChunkRequest *request,  //NOLINT
                                    ::curve::chunkserver::ChunkResponse *response,      //NOLINT
                                    google::protobuf::Closure *done) {
    brpc::ClosureGuard doneGuard(done);
    if (0 != gReadCntlFailedCode) {
        brpc::Controller *cntl = dynamic_cast<brpc::Controller *>(controller);
        cntl->SetFailed(-1, "delete snapshot controller error");
    }
}

static void DeleteChunkSnapshotFunc(::google::protobuf::RpcController *controller,      //NOLINT
                                  const ::curve::chunkserver::ChunkRequest *request,    //NOLINT
                                  ::curve::chunkserver::ChunkResponse *response,
                                  google::protobuf::Closure *done) {
    brpc::ClosureGuard doneGuard(done);
    if (0 != gReadCntlFailedCode) {
        brpc::Controller *cntl = dynamic_cast<brpc::Controller *>(controller);
        cntl->SetFailed(-1, "delete snapshot controller error");
    }
}

static void CreateCloneChunkFunc(::google::protobuf::RpcController *controller,      //NOLINT
                                  const ::curve::chunkserver::ChunkRequest *request,    //NOLINT
                                  ::curve::chunkserver::ChunkResponse *response,
                                  google::protobuf::Closure *done) {
    brpc::ClosureGuard doneGuard(done);
    if (0 != gReadCntlFailedCode) {
        brpc::Controller *cntl = dynamic_cast<brpc::Controller *>(controller);
        cntl->SetFailed(-1, "create clone chunk controller error");
    }
}

static void RecoverChunkFunc(::google::protobuf::RpcController *controller,      //NOLINT
                                  const ::curve::chunkserver::ChunkRequest *request,    //NOLINT
                                  ::curve::chunkserver::ChunkResponse *response,
                                  google::protobuf::Closure *done) {
    brpc::ClosureGuard doneGuard(done);
    if (0 != gReadCntlFailedCode) {
        brpc::Controller *cntl = dynamic_cast<brpc::Controller *>(controller);
        cntl->SetFailed(-1, "recover chunk controller error");
    }
}

static void GetChunkInfoFunc(::google::protobuf::RpcController *controller,
                             const ::curve::chunkserver::GetChunkInfoRequest *request,  //NOLINT
                             ::curve::chunkserver::GetChunkInfoResponse *response,      //NOLINT
                             google::protobuf::Closure *done) {
    brpc::ClosureGuard doneGuard(done);
    if (0 != gReadCntlFailedCode) {
        brpc::Controller *cntl = dynamic_cast<brpc::Controller *>(controller);
        cntl->SetFailed(-1, "get chunk info controller error");
    }
}

TEST_F(CopysetClientTest, normal_test) {
    MockChunkServiceImpl mockChunkService;
    ASSERT_EQ(server_->AddService(&mockChunkService,
                                  brpc::SERVER_DOESNT_OWN_SERVICE), 0);
    ASSERT_EQ(server_->Start(listenAddr_.c_str(), nullptr), 0);

    IOSenderOption_t ioSenderOpt;
    ioSenderOpt.rpcTimeoutMs = 5000;
    ioSenderOpt.rpcRetryTimes = 3;
    ioSenderOpt.failRequestOpt.opMaxRetry = 3;
    ioSenderOpt.failRequestOpt.opRetryIntervalUs = 500;
    ioSenderOpt.enableAppliedIndexRead = 1;

    CopysetClient copysetClient;
    MockMetaCache mockMetaCache;
    mockMetaCache.DelegateToFake();

    RequestScheduler scheduler;
    copysetClient.Init(&mockMetaCache, ioSenderOpt, &scheduler, nullptr);

    LogicPoolID logicPoolId = 1;
    CopysetID copysetId = 100001;
    ChunkID chunkId = 1;
    uint64_t sn = 1;
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
    std::string leaderStr1 = "127.0.0.1:9109";
    butil::str2endpoint(leaderStr1.c_str(), &leaderAdder1);
    ChunkServerID leaderId2 = 10001;
    butil::EndPoint leaderAdder2;
    std::string leaderStr2 = "127.0.0.1:9109";
    butil::str2endpoint(leaderStr2.c_str(), &leaderAdder2);
    ChunkServerID leaderId3 = 10002;
    butil::EndPoint leaderAdder3;
    std::string leaderStr3 = "127.0.0.1:9109";
    butil::str2endpoint(leaderStr3.c_str(), &leaderAdder3);
    ChunkServerID leaderId4 = 10003;
    butil::EndPoint leaderAdder4;
    std::string leaderStr4 = "127.0.0.1:9109";
    butil::str2endpoint(leaderStr4.c_str(), &leaderAdder4);

    // write success
    for (int i = 0; i < 10; ++i) {
        RequestContext *reqCtx = new FakeRequestContext();
        reqCtx->optype_ = OpType::WRITE;
        reqCtx->idinfo_ = ChunkIDInfo(chunkId, logicPoolId, copysetId);


        reqCtx->writeBuffer_ = buff1;
        reqCtx->offset_ = i * 8;
        reqCtx->rawlength_ = len;

        curve::common::CountDownEvent cond(1);
        RequestClosure *reqDone = new FakeRequestClosure(&cond, reqCtx);;
        reqCtx->done_ = reqDone;

        ChunkResponse response;
        response.set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS);
        EXPECT_CALL(mockMetaCache, GetLeader(_, _, _, _, _, _)).
                                            Times(AtLeast(1))
            .WillOnce(DoAll(SetArgPointee<2>(leaderId1),
                            SetArgPointee<3>(leaderAdder1),
                            Return(0)));
        EXPECT_CALL(mockChunkService, WriteChunk(_, _, _, _)).Times(1)
            .WillOnce(DoAll(SetArgPointee<2>(response),
                            Invoke(WriteChunkFunc)));
        copysetClient.WriteChunk(reqCtx->idinfo_, 0,
                                 buff1, offset, len, reqDone);
        cond.Wait();
        ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS,
                  reqDone->GetErrorCode());
    }
    {
        RequestContext *reqCtx = new FakeRequestContext();
        reqCtx->optype_ = OpType::WRITE;
        reqCtx->idinfo_ = ChunkIDInfo(chunkId, logicPoolId, copysetId);


        reqCtx->writeBuffer_ = buff1;
        reqCtx->offset_ = offset;
        reqCtx->rawlength_ = len;

        curve::common::CountDownEvent cond(1);
        RequestClosure *reqDone = new FakeRequestClosure(&cond, reqCtx);

        reqCtx->done_ = reqDone;
        ChunkResponse response;
        response.set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS);
        EXPECT_CALL(mockMetaCache, GetLeader(_, _, _, _, _, _)).
                                            Times(AtLeast(1))
            .WillOnce(DoAll(SetArgPointee<2>(leaderId1),
                            SetArgPointee<3>(leaderAdder1),
                            Return(-1)))
            .WillOnce(DoAll(SetArgPointee<2>(leaderId1),
                            SetArgPointee<3>(leaderAdder1),
                            Return(0)));
        EXPECT_CALL(mockChunkService, WriteChunk(_, _, _, _)).Times(1)
            .WillOnce(DoAll(SetArgPointee<2>(response),
                            Invoke(WriteChunkFunc)));
        copysetClient.WriteChunk(reqCtx->idinfo_, 0,
                                 buff1, offset, len, reqDone);
        cond.Wait();
        ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS,
                  reqDone->GetErrorCode());
    }
    {
        RequestContext *reqCtx = new FakeRequestContext();
        reqCtx->optype_ = OpType::WRITE;
        reqCtx->idinfo_ = ChunkIDInfo(chunkId, logicPoolId, copysetId);


        reqCtx->writeBuffer_ = buff1;
        reqCtx->offset_ = offset;
        reqCtx->rawlength_ = len;

        curve::common::CountDownEvent cond(1);
        RequestClosure *reqDone = new FakeRequestClosure(&cond, reqCtx);

        reqCtx->done_ = reqDone;
        ChunkResponse response;
        response.set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS);
        EXPECT_CALL(mockMetaCache, GetLeader(_, _, _, _, _, _)).
                                            Times(AtLeast(1))
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
        copysetClient.WriteChunk(reqCtx->idinfo_, 0,
                                 buff1, offset, len, reqDone);
        cond.Wait();
        ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS,
                  reqDone->GetErrorCode());
    }
    // read success
    for (int i = 0; i < 10; ++i) {
        RequestContext *reqCtx = new FakeRequestContext();
        reqCtx->optype_ = OpType::READ;
        reqCtx->idinfo_ = ChunkIDInfo(chunkId, logicPoolId, copysetId);


        reqCtx->seq_ = sn;
        reqCtx->readBuffer_ = buff1;
        reqCtx->offset_ = i * 8;
        reqCtx->rawlength_ = len;

        curve::common::CountDownEvent cond(1);
        RequestClosure *reqDone = new FakeRequestClosure(&cond, reqCtx);

        reqCtx->done_ = reqDone;
        ChunkResponse response;
        response.set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS);
        EXPECT_CALL(mockMetaCache, GetLeader(_, _, _, _, _, _)).
                                            Times(AtLeast(1))
            .WillOnce(DoAll(SetArgPointee<2>(leaderId1),
                            SetArgPointee<3>(leaderAdder1),
                            Return(0)));
        EXPECT_CALL(mockChunkService, ReadChunk(_, _, _, _)).Times(1)
            .WillOnce(DoAll(SetArgPointee<2>(response),
                            Invoke(ReadChunkFunc)));
        copysetClient.ReadChunk(reqCtx->idinfo_, sn,
                                offset, len, 0, reqDone);
        cond.Wait();
        ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS,
                  reqDone->GetErrorCode());
    }
    {
        RequestContext *reqCtx = new FakeRequestContext();
        reqCtx->optype_ = OpType::READ;
        reqCtx->idinfo_ = ChunkIDInfo(chunkId, logicPoolId, copysetId);


        reqCtx->readBuffer_ = buff1;
        reqCtx->offset_ = offset;
        reqCtx->rawlength_ = len;

        curve::common::CountDownEvent cond(1);
        RequestClosure *reqDone = new FakeRequestClosure(&cond, reqCtx);

        reqCtx->done_ = reqDone;
        ChunkResponse response;
        response.set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS);
        EXPECT_CALL(mockMetaCache, GetLeader(_, _, _, _, _, _)).
                                            Times(AtLeast(1))
            .WillOnce(DoAll(SetArgPointee<2>(leaderId1),
                            SetArgPointee<3>(leaderAdder1),
                            Return(-1)))
            .WillOnce(DoAll(SetArgPointee<2>(leaderId1),
                            SetArgPointee<3>(leaderAdder1),
                            Return(0)));
        EXPECT_CALL(mockChunkService, ReadChunk(_, _, _, _)).Times(1)
            .WillOnce(DoAll(SetArgPointee<2>(response),
                            Invoke(ReadChunkFunc)));
        copysetClient.ReadChunk(reqCtx->idinfo_, sn,
                                offset, len, 0, reqDone);
        cond.Wait();
        ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS,
                  reqDone->GetErrorCode());
    }
    {
        RequestContext *reqCtx = new FakeRequestContext();
        reqCtx->optype_ = OpType::READ;
        reqCtx->idinfo_ = ChunkIDInfo(chunkId, logicPoolId, copysetId);


        reqCtx->readBuffer_ = buff1;
        reqCtx->offset_ = offset;
        reqCtx->rawlength_ = len;

        curve::common::CountDownEvent cond(1);
        RequestClosure *reqDone = new FakeRequestClosure(&cond, reqCtx);

        reqCtx->done_ = reqDone;
        ChunkResponse response;
        response.set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS);
        EXPECT_CALL(mockMetaCache, GetLeader(_, _, _, _, _, _)).
                                            Times(AtLeast(1))
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
        copysetClient.ReadChunk(reqCtx->idinfo_, sn,
                                offset, len, 0, reqDone);
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

    IOSenderOption_t ioSenderOpt;
    ioSenderOpt.rpcTimeoutMs = 1000;
    ioSenderOpt.rpcRetryTimes = 3;
    ioSenderOpt.failRequestOpt.opMaxRetry = 3;
    ioSenderOpt.failRequestOpt.opRetryIntervalUs = 5000;
    ioSenderOpt.failRequestOpt.maxTimeoutMS = 3500;
    ioSenderOpt.failRequestOpt.maxRetrySleepIntervalUs = 3500000;
    ioSenderOpt.enableAppliedIndexRead = 1;

    RequestScheduleOption_t reqopt;
    reqopt.ioSenderOpt = ioSenderOpt;

    CopysetClient copysetClient;
    MockMetaCache mockMetaCache;
    mockMetaCache.DelegateToFake();

    RequestScheduler scheduler;
    scheduler.Init(reqopt, &mockMetaCache);
    scheduler.Run();
    copysetClient.Init(&mockMetaCache, ioSenderOpt, &scheduler, nullptr);

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
    std::string leaderStr1 = "127.0.0.1:9109";
    butil::str2endpoint(leaderStr1.c_str(), &leaderAdder1);
    ChunkServerID leaderId2 = 10001;
    butil::EndPoint leaderAdder2;
    std::string leaderStr2 = "127.0.0.1:9109";
    butil::str2endpoint(leaderStr2.c_str(), &leaderAdder2);
    ChunkServerID leaderId3 = 10002;
    butil::EndPoint leaderAdder3;
    std::string leaderStr3 = "127.0.0.1:9109";
    butil::str2endpoint(leaderStr3.c_str(), &leaderAdder3);
    ChunkServerID leaderId4 = 10003;
    butil::EndPoint leaderAdder4;
    std::string leaderStr4 = "127.0.0.1:9109";
    butil::str2endpoint(leaderStr4.c_str(), &leaderAdder4);

    /* 非法参数 */
    {
        RequestContext *reqCtx = new FakeRequestContext();
        reqCtx->optype_ = OpType::WRITE;
        reqCtx->idinfo_ = ChunkIDInfo(chunkId, logicPoolId, copysetId);


        reqCtx->writeBuffer_ = buff1;
        reqCtx->offset_ = 0;
        reqCtx->rawlength_ = len;

        curve::common::CountDownEvent cond(1);
        RequestClosure *reqDone = new FakeRequestClosure(&cond, reqCtx);

        reqCtx->done_ = reqDone;
        ChunkResponse response;
        response.set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_INVALID_REQUEST);
        EXPECT_CALL(mockMetaCache, GetLeader(_, _, _, _, _, _))
            .Times(AtLeast(1)).WillOnce(DoAll(SetArgPointee<2>(leaderId1),
                                              SetArgPointee<3>(leaderAdder1),
                                              Return(0)));
        EXPECT_CALL(mockChunkService, WriteChunk(_, _, _, _)).Times(1)
            .WillOnce(DoAll(SetArgPointee<2>(response),
                            Invoke(WriteChunkFunc)));
        copysetClient.WriteChunk(reqCtx->idinfo_, 0,
                                 buff1, offset, len, reqDone);
        cond.Wait();
        ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_INVALID_REQUEST,
                  reqDone->GetErrorCode());
    }
    /* controller error */
    {
        RequestContext *reqCtx = new FakeRequestContext();
        reqCtx->optype_ = OpType::WRITE;
        reqCtx->idinfo_ = ChunkIDInfo(chunkId, logicPoolId, copysetId);


        reqCtx->writeBuffer_ = buff1;
        reqCtx->offset_ = 0;
        reqCtx->rawlength_ = len;

        // 配置文件设置的重试睡眠时间为5000，因为没有触发底层指数退避，所以重试之间不会睡眠
        uint64_t start = TimeUtility::GetTimeofDayUs();

        curve::common::CountDownEvent cond(1);
        RequestClosure *reqDone = new FakeRequestClosure(&cond, reqCtx);

        reqCtx->done_ = reqDone;
        gWriteCntlFailedCode = -1;
        EXPECT_CALL(mockMetaCache, GetLeader(_, _, _, _, _, _)).Times(6)
            .WillRepeatedly(DoAll(SetArgPointee<2>(leaderId1),
                                  SetArgPointee<3>(leaderAdder1),
                                  Return(0)));
        EXPECT_CALL(mockChunkService, WriteChunk(_, _, _, _)).Times(3)
            .WillRepeatedly(Invoke(WriteChunkFunc));
        copysetClient.WriteChunk(reqCtx->idinfo_, 0,
                                 buff1, offset, len, reqDone);
        cond.Wait();
        ASSERT_NE(0, reqDone->GetErrorCode());

        uint64_t end = TimeUtility::GetTimeofDayUs();
        ASSERT_GT(end - start, 10000);
        gWriteCntlFailedCode = 0;
    }
    /* controller set timeout */
    {
        RequestContext *reqCtx = new FakeRequestContext();
        reqCtx->optype_ = OpType::WRITE;
        reqCtx->idinfo_ = ChunkIDInfo(chunkId, logicPoolId, copysetId);


        reqCtx->writeBuffer_ = buff1;
        reqCtx->offset_ = 0;
        reqCtx->rawlength_ = len;

        curve::common::CountDownEvent cond(1);
        RequestClosure *reqDone = new FakeRequestClosure(&cond, reqCtx);

        // 配置文件设置的重试超时时间为5000，因为chunkserver设置返回timeout
        // 导致触发底层超时时间指数退避，每次重试间隔增大。重试三次正常只需要睡眠3*1000
        // 但是增加指数退避之后，超时时间将增加到1000 + 2000 + 2000 = 5000
        // 加上随机因子，三次重试时间应该大于7000, 且小于8000
        uint64_t start = TimeUtility::GetTimeofDayMs();

        reqCtx->done_ = reqDone;
        gWriteCntlFailedCode = brpc::ERPCTIMEDOUT;
        EXPECT_CALL(mockMetaCache, GetLeader(_, _, _, _, _, _))
            .Times(AtLeast(3))
            .WillRepeatedly(DoAll(SetArgPointee<2>(leaderId1),
                                  SetArgPointee<3>(leaderAdder1),
                                  Return(0)));
        EXPECT_CALL(mockChunkService, WriteChunk(_, _, _, _)).Times(3)
            .WillRepeatedly(Invoke(WriteChunkFunc));
        copysetClient.WriteChunk(reqCtx->idinfo_, 0,
                                 buff1, offset, len, reqDone);
        cond.Wait();
        ASSERT_NE(0, reqDone->GetErrorCode());

        uint64_t end = TimeUtility::GetTimeofDayMs();
        ASSERT_GT(end - start, 3000);
        ASSERT_LT(end - start, 6000);
        std::this_thread::sleep_for(std::chrono::seconds(8));

        gWriteCntlFailedCode = 0;
    }

    /* controller set timeout */
    {
        RequestContext *reqCtx = new FakeRequestContext();
        reqCtx->optype_ = OpType::WRITE;
        reqCtx->idinfo_ = ChunkIDInfo(chunkId, logicPoolId, copysetId);


        reqCtx->writeBuffer_ = buff1;
        reqCtx->offset_ = 0;
        reqCtx->rawlength_ = len;

        curve::common::CountDownEvent cond(1);
        RequestClosure *reqDone = new FakeRequestClosure(&cond, reqCtx);

        // 配置文件设置的重试睡眠时间为5000，因为chunkserver设置返回timeout
        // 导致触发底层指数退避，每次重试间隔增大。重试三次正常只需要睡眠3*5000
        // 但是增加指数退避之后，睡眠间隔将增加到10000 + 20000 = 30000
        // 加上随机因子，三次重试时间应该大于29000, 且小于50000
        uint64_t start = TimeUtility::GetTimeofDayUs();

        reqCtx->done_ = reqDone;
        ChunkResponse response;
        response.set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_OVERLOAD);
        EXPECT_CALL(mockMetaCache, GetLeader(_, _, _, _, _, _))
            .Times(3).WillRepeatedly(DoAll(SetArgPointee<2>(leaderId1),
                                           SetArgPointee<3>(leaderAdder1),
                                           Return(0)));
        EXPECT_CALL(mockChunkService, WriteChunk(_, _, _, _)).Times(3)
            .WillRepeatedly(DoAll(SetArgPointee<2>(response),
                                  Invoke(WriteChunkFunc)));
        copysetClient.WriteChunk(reqCtx->idinfo_, 0,
                                 buff1, offset, len, reqDone);
        cond.Wait();
        ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_OVERLOAD,
                 reqDone->GetErrorCode());

        uint64_t end = TimeUtility::GetTimeofDayUs();
        ASSERT_GT(end - start, 29000);
        ASSERT_LT(end - start, 2 * 50000);
        gWriteCntlFailedCode = 0;
    }

    /* 其他错误 */
    {
        RequestContext *reqCtx = new FakeRequestContext();
        reqCtx->optype_ = OpType::WRITE;
        reqCtx->idinfo_ = ChunkIDInfo(chunkId, logicPoolId, copysetId);


        reqCtx->writeBuffer_ = buff1;
        reqCtx->offset_ = 0;
        reqCtx->rawlength_ = len;

        curve::common::CountDownEvent cond(1);
        RequestClosure *reqDone = new FakeRequestClosure(&cond, reqCtx);

        reqCtx->done_ = reqDone;
        ChunkResponse response;
        response.set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_FAILURE_UNKNOWN);
        EXPECT_CALL(mockMetaCache, GetLeader(_, _, _, _, _, _))
            .Times(3).WillRepeatedly(DoAll(SetArgPointee<2>(leaderId1),
                                           SetArgPointee<3>(leaderAdder1),
                                           Return(0)));
        EXPECT_CALL(mockChunkService, WriteChunk(_, _, _, _)).Times(3)
            .WillRepeatedly(DoAll(SetArgPointee<2>(response),
                                  Invoke(WriteChunkFunc)));
        copysetClient.WriteChunk(reqCtx->idinfo_, 0,
                                 buff1, offset, len, reqDone);
        cond.Wait();
        ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_FAILURE_UNKNOWN,
                 reqDone->GetErrorCode());
    }
    /* 不是 leader，返回正确的 leader */
    {
        RequestContext *reqCtx = new FakeRequestContext();
        reqCtx->optype_ = OpType::WRITE;
        reqCtx->idinfo_ = ChunkIDInfo(chunkId, logicPoolId, copysetId);


        reqCtx->writeBuffer_ = buff1;
        reqCtx->offset_ = 0;
        reqCtx->rawlength_ = len;

        curve::common::CountDownEvent cond(1);
        RequestClosure *reqDone = new FakeRequestClosure(&cond, reqCtx);

        reqCtx->done_ = reqDone;
        ChunkResponse response1;
        response1.set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_REDIRECTED);
        response1.set_redirect(leaderStr2);
        ChunkResponse response2;
        response2.set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS);
        EXPECT_CALL(mockMetaCache, GetLeader(_, _, _, _, _, _)).Times(2)
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
        copysetClient.WriteChunk(reqCtx->idinfo_, 0,
                                 buff1, offset, len, reqDone);
        cond.Wait();
        ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS,
                  reqDone->GetErrorCode());
    }
    /* 不是 leader，没有返回 leader，刷新 meta cache 成功 */
    {
        RequestContext *reqCtx = new FakeRequestContext();
        reqCtx->optype_ = OpType::WRITE;
        reqCtx->idinfo_ = ChunkIDInfo(chunkId, logicPoolId, copysetId);


        reqCtx->writeBuffer_ = buff1;
        reqCtx->offset_ = 0;
        reqCtx->rawlength_ = len;

        curve::common::CountDownEvent cond(1);
        RequestClosure *reqDone = new FakeRequestClosure(&cond, reqCtx);

        reqCtx->done_ = reqDone;
        ChunkResponse response1;
        response1.set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_REDIRECTED);
//        response1.set_redirect(leaderStr2);
        ChunkResponse response2;
        response2.set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS);
        EXPECT_CALL(mockMetaCache, GetLeader(_, _, _, _, _, _)).Times(3)
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
        copysetClient.WriteChunk(reqCtx->idinfo_, 0,
                                 buff1, offset, len, reqDone);
        cond.Wait();
        ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS,
                  reqDone->GetErrorCode());
    }
    /* 不是 leader，没有返回 leader，刷新 meta cache 失败 */
    {
        RequestContext *reqCtx = new FakeRequestContext();
        reqCtx->optype_ = OpType::WRITE;
        reqCtx->idinfo_ = ChunkIDInfo(chunkId, logicPoolId, copysetId);


        reqCtx->writeBuffer_ = buff1;
        reqCtx->offset_ = 0;
        reqCtx->rawlength_ = len;

        curve::common::CountDownEvent cond(1);
        RequestClosure *reqDone = new FakeRequestClosure(&cond, reqCtx);

        reqCtx->done_ = reqDone;
        ChunkResponse response1;
        response1.set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_REDIRECTED);
//        response1.set_redirect(leaderStr2);
        ChunkResponse response2;
        response2.set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS);
        EXPECT_CALL(mockMetaCache, GetLeader(_, _, _, _, _, _)).Times(3)
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
        copysetClient.WriteChunk(reqCtx->idinfo_, 0,
                                 buff1, offset, len, reqDone);
        cond.Wait();
        ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS,
                  reqDone->GetErrorCode());
    }
    /* 不是 leader，但返回的是错误 leader */
    {
        RequestContext *reqCtx = new FakeRequestContext();
        reqCtx->optype_ = OpType::WRITE;
        reqCtx->idinfo_ = ChunkIDInfo(chunkId, logicPoolId, copysetId);


        reqCtx->writeBuffer_ = buff1;
        reqCtx->offset_ = 0;
        reqCtx->rawlength_ = len;

        curve::common::CountDownEvent cond(1);
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
        EXPECT_CALL(mockMetaCache, GetLeader(_, _, _, _, _, _)).Times(3)
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
        copysetClient.WriteChunk(reqCtx->idinfo_, 0,
                                 buff1, offset, len, reqDone);
        cond.Wait();
        ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_REDIRECTED,
                  reqDone->GetErrorCode());
    }
    /* copyset 不存在，更新 leader 依然失败 */
    {
        RequestContext *reqCtx = new FakeRequestContext();
        reqCtx->optype_ = OpType::WRITE;
        reqCtx->idinfo_ = ChunkIDInfo(chunkId, logicPoolId, copysetId);


        reqCtx->writeBuffer_ = buff1;
        reqCtx->offset_ = 0;
        reqCtx->rawlength_ = len;

        curve::common::CountDownEvent cond(1);
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
        EXPECT_CALL(mockMetaCache, GetLeader(_, _, _, _, _, _)).Times(6)
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
        copysetClient.WriteChunk(reqCtx->idinfo_, 0,
                                 buff1, offset, len, reqDone);
        cond.Wait();
        ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_COPYSET_NOTEXIST,
                  reqDone->GetErrorCode());
    }
    /* copyset 不存在，更新 leader 成功 */
    {
        RequestContext *reqCtx = new FakeRequestContext();
        reqCtx->optype_ = OpType::WRITE;
        reqCtx->idinfo_ = ChunkIDInfo(chunkId, logicPoolId, copysetId);


        reqCtx->writeBuffer_ = buff1;
        reqCtx->offset_ = 0;
        reqCtx->rawlength_ = len;

        curve::common::CountDownEvent cond(1);
        RequestClosure *reqDone = new FakeRequestClosure(&cond, reqCtx);

        reqCtx->done_ = reqDone;
        ChunkResponse response1;
        response1.set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_COPYSET_NOTEXIST);
        response1.set_redirect(leaderStr2);
        ChunkResponse response2;
        response2.set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS);
        response2.set_redirect(leaderStr3);
        EXPECT_CALL(mockMetaCache, GetLeader(_, _, _, _, _, _)).Times(3)
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
        copysetClient.WriteChunk(reqCtx->idinfo_, 0,
                                 buff1, offset, len, reqDone);
        cond.Wait();
        ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS,
                  reqDone->GetErrorCode());
    }
    scheduler.Fini();
}

/**
 * write failed testing
 */
TEST_F(CopysetClientTest, write_failed_test) {
    MockChunkServiceImpl mockChunkService;
    ASSERT_EQ(server_->AddService(&mockChunkService,
                                  brpc::SERVER_DOESNT_OWN_SERVICE), 0);
    ASSERT_EQ(server_->Start(listenAddr_.c_str(), nullptr), 0);

    IOSenderOption_t ioSenderOpt;
    ioSenderOpt.rpcTimeoutMs = 500;
    ioSenderOpt.rpcRetryTimes = 3;
    ioSenderOpt.failRequestOpt.opMaxRetry = 50;
    ioSenderOpt.failRequestOpt.opRetryIntervalUs = 5000;
    ioSenderOpt.failRequestOpt.maxTimeoutMS = 1000;
    ioSenderOpt.failRequestOpt.maxRetrySleepIntervalUs = 100000;
    ioSenderOpt.enableAppliedIndexRead = 1;

    RequestScheduleOption_t reqopt;
    reqopt.ioSenderOpt = ioSenderOpt;

    CopysetClient copysetClient;
    MockMetaCache mockMetaCache;
    mockMetaCache.DelegateToFake();

    RequestScheduler scheduler;
    scheduler.Init(reqopt, &mockMetaCache);
    scheduler.Run();
    copysetClient.Init(&mockMetaCache, ioSenderOpt, &scheduler, nullptr);

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
    std::string leaderStr1 = "127.0.0.1:9109";
    butil::str2endpoint(leaderStr1.c_str(), &leaderAdder1);
    ChunkServerID leaderId2 = 10001;
    butil::EndPoint leaderAdder2;
    std::string leaderStr2 = "127.0.0.1:9109";
    butil::str2endpoint(leaderStr2.c_str(), &leaderAdder2);
    ChunkServerID leaderId3 = 10002;
    butil::EndPoint leaderAdder3;
    std::string leaderStr3 = "127.0.0.1:9109";
    butil::str2endpoint(leaderStr3.c_str(), &leaderAdder3);
    ChunkServerID leaderId4 = 10003;
    butil::EndPoint leaderAdder4;
    std::string leaderStr4 = "127.0.0.1:9109";
    butil::str2endpoint(leaderStr4.c_str(), &leaderAdder4);

    /* controller set timeout */
    {
        RequestContext *reqCtx = new FakeRequestContext();
        reqCtx->optype_ = OpType::WRITE;
        reqCtx->idinfo_ = ChunkIDInfo(chunkId, logicPoolId, copysetId);


        reqCtx->writeBuffer_ = buff1;
        reqCtx->offset_ = 0;
        reqCtx->rawlength_ = len;

        curve::common::CountDownEvent cond(1);
        RequestClosure *reqDone = new FakeRequestClosure(&cond, reqCtx);

        // 配置文件设置的重试超时时间为500，因为chunkserver设置返回timeout
        // 导致触发底层超时时间指数退避，每次重试间隔增大。重试50次正常只需要超时49*500
        // 但是增加指数退避之后，超时时间将增加到49*1000 = 49000
        uint64_t start = TimeUtility::GetTimeofDayMs();

        reqCtx->done_ = reqDone;
        gWriteCntlFailedCode = brpc::ERPCTIMEDOUT;
        EXPECT_CALL(mockMetaCache, GetLeader(_, _, _, _, _, _))
            .Times(AtLeast(50))
            .WillRepeatedly(DoAll(SetArgPointee<2>(leaderId1),
                                  SetArgPointee<3>(leaderAdder1),
                                  Return(0)));
        EXPECT_CALL(mockChunkService, WriteChunk(_, _, _, _)).Times(50)
            .WillRepeatedly(Invoke(WriteChunkFunc));
        copysetClient.WriteChunk(reqCtx->idinfo_, 0,
                                 buff1, offset, len, reqDone);
        cond.Wait();
        ASSERT_NE(0, reqDone->GetErrorCode());

        uint64_t end = TimeUtility::GetTimeofDayMs();
        ASSERT_GT(end - start, 25000);
        ASSERT_LT(end - start, 55000);
        std::this_thread::sleep_for(std::chrono::seconds(8));

        gWriteCntlFailedCode = 0;
    }

    {
        RequestContext *reqCtx = new FakeRequestContext();
        reqCtx->optype_ = OpType::WRITE;
        reqCtx->idinfo_ = ChunkIDInfo(chunkId, logicPoolId, copysetId);


        reqCtx->writeBuffer_ = buff1;
        reqCtx->offset_ = 0;
        reqCtx->rawlength_ = len;

        curve::common::CountDownEvent cond(1);
        RequestClosure *reqDone = new FakeRequestClosure(&cond, reqCtx);

        // 配置文件设置的重试睡眠时间为5000us，因为chunkserver设置返回timeout
        // 导致触发底层指数退避，每次重试间隔增大。重试50次正常只需要睡眠49*5000us
        // 但是增加指数退避之后，睡眠间隔将增加到
        // 10000 + 20000 + 40000... ~= 4650000
        uint64_t start = TimeUtility::GetTimeofDayUs();

        reqCtx->done_ = reqDone;
        ChunkResponse response;
        response.set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_OVERLOAD);
        EXPECT_CALL(mockMetaCache, GetLeader(_, _, _, _, _, _))
            .Times(50).WillRepeatedly(DoAll(SetArgPointee<2>(leaderId1),
                                           SetArgPointee<3>(leaderAdder1),
                                           Return(0)));
        EXPECT_CALL(mockChunkService, WriteChunk(_, _, _, _)).Times(50)
            .WillRepeatedly(DoAll(SetArgPointee<2>(response),
                                  Invoke(WriteChunkFunc)));
        copysetClient.WriteChunk(reqCtx->idinfo_, 0,
                                 buff1, offset, len, reqDone);
        cond.Wait();
        ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_OVERLOAD,
                 reqDone->GetErrorCode());

        uint64_t end = TimeUtility::GetTimeofDayUs();
        ASSERT_GT(end - start, 250000);
        ASSERT_LT(end - start, 4650000);
        gWriteCntlFailedCode = 0;
    }
    scheduler.Fini();
}


/**
 * read failed testing
 */
TEST_F(CopysetClientTest, read_failed_test) {
    MockChunkServiceImpl mockChunkService;
    ASSERT_EQ(server_->AddService(&mockChunkService,
                                  brpc::SERVER_DOESNT_OWN_SERVICE), 0);
    ASSERT_EQ(server_->Start(listenAddr_.c_str(), nullptr), 0);

    IOSenderOption_t ioSenderOpt;
    ioSenderOpt.rpcTimeoutMs = 500;
    ioSenderOpt.rpcRetryTimes = 3;
    ioSenderOpt.failRequestOpt.opMaxRetry = 50;
    ioSenderOpt.failRequestOpt.opRetryIntervalUs = 5000;
    ioSenderOpt.failRequestOpt.maxTimeoutMS = 1000;
    ioSenderOpt.failRequestOpt.maxRetrySleepIntervalUs = 100000;
    ioSenderOpt.enableAppliedIndexRead = 1;

    RequestScheduleOption_t reqopt;
    reqopt.ioSenderOpt = ioSenderOpt;

    CopysetClient copysetClient;
    MockMetaCache mockMetaCache;
    mockMetaCache.DelegateToFake();

    RequestScheduler scheduler;
    scheduler.Init(reqopt, &mockMetaCache);
    scheduler.Run();
    copysetClient.Init(&mockMetaCache, ioSenderOpt, &scheduler, nullptr);

    LogicPoolID logicPoolId = 1;
    CopysetID copysetId = 100001;
    ChunkID chunkId = 1;
    uint64_t sn = 1;
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
    std::string leaderStr1 = "127.0.0.1:9109";
    butil::str2endpoint(leaderStr1.c_str(), &leaderAdder1);
    ChunkServerID leaderId2 = 10001;
    butil::EndPoint leaderAdder2;
    std::string leaderStr2 = "127.0.0.1:9109";
    butil::str2endpoint(leaderStr2.c_str(), &leaderAdder2);
    ChunkServerID leaderId3 = 10002;
    butil::EndPoint leaderAdder3;
    std::string leaderStr3 = "127.0.0.1:9109";
    butil::str2endpoint(leaderStr3.c_str(), &leaderAdder3);
    ChunkServerID leaderId4 = 10003;
    butil::EndPoint leaderAdder4;
    std::string leaderStr4 = "127.0.0.1:9109";
    butil::str2endpoint(leaderStr4.c_str(), &leaderAdder4);

    /* controller set timeout */
    {
        RequestContext *reqCtx = new FakeRequestContext();
        reqCtx->optype_ = OpType::READ;
        reqCtx->idinfo_ = ChunkIDInfo(chunkId, logicPoolId, copysetId);


        reqCtx->offset_ = 0;
        reqCtx->rawlength_ = len;

        // 配置文件设置的重试超时时间为500，因为chunkserver设置返回timeout
        // 导致触发底层超时时间指数退避，每次重试间隔增大。重试50次正常只需要50*500
        // 但是增加指数退避之后，超时时间将增加到500 + 1000 + 2000... ~= 60000
        uint64_t start = TimeUtility::GetTimeofDayMs();

        curve::common::CountDownEvent cond(1);
        RequestClosure *reqDone = new FakeRequestClosure(&cond, reqCtx);

        reqCtx->done_ = reqDone;
        gReadCntlFailedCode = brpc::ERPCTIMEDOUT;
        EXPECT_CALL(mockMetaCache, GetLeader(_, _, _, _, _, _))
            .Times(AtLeast(50))
            .WillRepeatedly(DoAll(SetArgPointee<2>(leaderId1),
                                  SetArgPointee<3>(leaderAdder1),
                                  Return(0)));
        EXPECT_CALL(mockChunkService, ReadChunk(_, _, _, _)).Times(50)
            .WillRepeatedly(Invoke(ReadChunkFunc));
        copysetClient.ReadChunk(reqCtx->idinfo_, sn,
                                offset, len, 0, reqDone);
        cond.Wait();
        ASSERT_NE(0, reqDone->GetErrorCode());

        uint64_t end = TimeUtility::GetTimeofDayMs();
        ASSERT_GT(end - start, 25000);
        ASSERT_LT(end - start, 60000);

        std::this_thread::sleep_for(std::chrono::seconds(8));

        gReadCntlFailedCode = 0;
    }

    /* 设置 overload */
    {
        RequestContext *reqCtx = new FakeRequestContext();
        reqCtx->optype_ = OpType::READ;
        reqCtx->idinfo_ = ChunkIDInfo(chunkId, logicPoolId, copysetId);


        reqCtx->readBuffer_ = buff1;
        reqCtx->offset_ = 0;
        reqCtx->rawlength_ = len;

        curve::common::CountDownEvent cond(1);
        RequestClosure *reqDone = new FakeRequestClosure(&cond, reqCtx);

        // 配置文件设置的重试睡眠时间为5000us，因为chunkserver设置返回timeout
        // 导致触发底层指数退避，每次重试间隔增大。重试50次正常只需要睡眠49*5000
        // 但是增加指数退避之后，睡眠间隔将增加到
        // 10000 + 20000 + 40000 ... = 4650000
        // 加上随机因子，三次重试时间应该大于2900, 且小于5000
        uint64_t start = TimeUtility::GetTimeofDayUs();

        reqCtx->done_ = reqDone;
        ChunkResponse response;
        response.set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_OVERLOAD);
        EXPECT_CALL(mockMetaCache, GetLeader(_, _, _, _, _, _))
            .Times(50).WillRepeatedly(DoAll(SetArgPointee<2>(leaderId1),
                                           SetArgPointee<3>(leaderAdder1),
                                           Return(0)));
        EXPECT_CALL(mockChunkService, ReadChunk(_, _, _, _)).Times(50)
            .WillRepeatedly(DoAll(SetArgPointee<2>(response),
                                  Invoke(ReadChunkFunc)));
        copysetClient.ReadChunk(reqCtx->idinfo_, sn,
                                offset, len, 0, reqDone);
        cond.Wait();
        ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_OVERLOAD,
                  reqDone->GetErrorCode());

        uint64_t end = TimeUtility::GetTimeofDayUs();
        ASSERT_GT(end - start, 250000);
        ASSERT_LT(end - start, 4650000);
    }
    scheduler.Fini();
}

/**
 * read error testing
 */
TEST_F(CopysetClientTest, read_error_test) {
    MockChunkServiceImpl mockChunkService;
    ASSERT_EQ(server_->AddService(&mockChunkService,
                                  brpc::SERVER_DOESNT_OWN_SERVICE), 0);
    ASSERT_EQ(server_->Start(listenAddr_.c_str(), nullptr), 0);

    IOSenderOption_t ioSenderOpt;
    ioSenderOpt.rpcTimeoutMs = 1000;
    ioSenderOpt.rpcRetryTimes = 3;
    ioSenderOpt.failRequestOpt.opMaxRetry = 3;
    ioSenderOpt.failRequestOpt.opRetryIntervalUs = 500;
    ioSenderOpt.failRequestOpt.maxTimeoutMS = 3500;
    ioSenderOpt.failRequestOpt.maxRetrySleepIntervalUs = 3500000;
    ioSenderOpt.enableAppliedIndexRead = 1;

    RequestScheduleOption_t reqopt;
    reqopt.ioSenderOpt = ioSenderOpt;

    CopysetClient copysetClient;
    MockMetaCache mockMetaCache;
    mockMetaCache.DelegateToFake();

    RequestScheduler scheduler;
    scheduler.Init(reqopt, &mockMetaCache);
    scheduler.Run();

    copysetClient.Init(&mockMetaCache, ioSenderOpt, &scheduler);

    LogicPoolID logicPoolId = 1;
    CopysetID copysetId = 100001;
    ChunkID chunkId = 1;
    uint64_t sn = 1;
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
    std::string leaderStr1 = "127.0.0.1:9109";
    butil::str2endpoint(leaderStr1.c_str(), &leaderAdder1);
    ChunkServerID leaderId2 = 10001;
    butil::EndPoint leaderAdder2;
    std::string leaderStr2 = "127.0.0.1:9109";
    butil::str2endpoint(leaderStr2.c_str(), &leaderAdder2);
    ChunkServerID leaderId3 = 10002;
    butil::EndPoint leaderAdder3;
    std::string leaderStr3 = "127.0.0.1:9109";
    butil::str2endpoint(leaderStr3.c_str(), &leaderAdder3);
    ChunkServerID leaderId4 = 10003;
    butil::EndPoint leaderAdder4;
    std::string leaderStr4 = "127.0.0.1:9109";
    butil::str2endpoint(leaderStr4.c_str(), &leaderAdder4);

    /* 非法参数 */
    {
        RequestContext *reqCtx = new FakeRequestContext();
        reqCtx->optype_ = OpType::READ;
        reqCtx->idinfo_ = ChunkIDInfo(chunkId, logicPoolId, copysetId);


        reqCtx->readBuffer_ = buff1;
        reqCtx->offset_ = 0;
        reqCtx->rawlength_ = len;

        curve::common::CountDownEvent cond(1);
        RequestClosure *reqDone = new FakeRequestClosure(&cond, reqCtx);

        reqCtx->done_ = reqDone;
        ChunkResponse response;
        response.set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_INVALID_REQUEST);
        EXPECT_CALL(mockMetaCache, GetLeader(_, _, _, _, _, _))
            .Times(AtLeast(1)).WillOnce(DoAll(SetArgPointee<2>(leaderId1),
                                              SetArgPointee<3>(leaderAdder1),
                                              Return(0)));
        EXPECT_CALL(mockChunkService, ReadChunk(_, _, _, _)).Times(1)
            .WillOnce(DoAll(SetArgPointee<2>(response),
                            Invoke(ReadChunkFunc)));
        copysetClient.ReadChunk(reqCtx->idinfo_, sn,
                                offset, len, 0, reqDone);
        cond.Wait();
        ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_INVALID_REQUEST,
                  reqDone->GetErrorCode());
    }
    /* chunk not exist */
    {
        RequestContext *reqCtx = new FakeRequestContext();
        reqCtx->optype_ = OpType::READ;
        reqCtx->idinfo_ = ChunkIDInfo(chunkId, logicPoolId, copysetId);


        reqCtx->readBuffer_ = buff1;
        reqCtx->offset_ = 0;
        reqCtx->rawlength_ = len;

        curve::common::CountDownEvent cond(1);
        RequestClosure *reqDone = new FakeRequestClosure(&cond, reqCtx);

        reqCtx->done_ = reqDone;
        ChunkResponse response;
        response.set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_CHUNK_NOTEXIST);
        EXPECT_CALL(mockMetaCache, GetLeader(_, _, _, _, _, _))
            .Times(AtLeast(1)).WillOnce(DoAll(SetArgPointee<2>(leaderId1),
                                              SetArgPointee<3>(leaderAdder1),
                                              Return(0)));
        EXPECT_CALL(mockChunkService, ReadChunk(_, _, _, _)).Times(1)
            .WillOnce(DoAll(SetArgPointee<2>(response),
                            Invoke(ReadChunkFunc)));
        copysetClient.ReadChunk(reqCtx->idinfo_, sn,
                                offset, len, 0, reqDone);
        cond.Wait();
        ASSERT_EQ(0, reqDone->GetErrorCode());
    }
    /* controller error */
    {
        RequestContext *reqCtx = new FakeRequestContext();
        reqCtx->optype_ = OpType::READ;
        reqCtx->idinfo_ = ChunkIDInfo(chunkId, logicPoolId, copysetId);


        reqCtx->offset_ = 0;
        reqCtx->rawlength_ = len;

        // 配置文件设置的重试睡眠时间为5000，因为没有触发底层指数退避，所以重试之间不会睡眠
        uint64_t start = TimeUtility::GetTimeofDayUs();

        curve::common::CountDownEvent cond(1);
        RequestClosure *reqDone = new FakeRequestClosure(&cond, reqCtx);

        reqCtx->done_ = reqDone;
        gReadCntlFailedCode = -1;
        EXPECT_CALL(mockMetaCache, GetLeader(_, _, _, _, _, _))
            .Times(AtLeast(3))
            .WillRepeatedly(DoAll(SetArgPointee<2>(leaderId1),
                                  SetArgPointee<3>(leaderAdder1),
                                  Return(0)));
        EXPECT_CALL(mockChunkService, ReadChunk(_, _, _, _)).Times(3)
            .WillRepeatedly(Invoke(ReadChunkFunc));
        copysetClient.ReadChunk(reqCtx->idinfo_, sn,
                                offset, len, 0, reqDone);
        cond.Wait();
        ASSERT_NE(0, reqDone->GetErrorCode());

        uint64_t end = TimeUtility::GetTimeofDayUs();
        ASSERT_GT(end - start, 1400);
        gReadCntlFailedCode = 0;
    }

    /* controller set timeout */
    {
        RequestContext *reqCtx = new FakeRequestContext();
        reqCtx->optype_ = OpType::READ;
        reqCtx->idinfo_ = ChunkIDInfo(chunkId, logicPoolId, copysetId);


        reqCtx->offset_ = 0;
        reqCtx->rawlength_ = len;

        // 配置文件设置的超时时间为1000，因为chunkserver设置返回timeout
        // 导致触发底层超时时间指数退避，每次重试间隔增大。重试三次正常只需要睡眠3*1000
        // 但是增加指数退避之后，超时时间将增加到1000 + 2000 + 2000 = 5000
        // 加上随机因子，三次重试时间应该大于7000, 且小于8000
        uint64_t start = TimeUtility::GetTimeofDayMs();

        curve::common::CountDownEvent cond(1);
        RequestClosure *reqDone = new FakeRequestClosure(&cond, reqCtx);

        reqCtx->done_ = reqDone;
        gReadCntlFailedCode = brpc::ERPCTIMEDOUT;
        EXPECT_CALL(mockMetaCache, GetLeader(_, _, _, _, _, _))
            .Times(AtLeast(3))
            .WillRepeatedly(DoAll(SetArgPointee<2>(leaderId1),
                                  SetArgPointee<3>(leaderAdder1),
                                  Return(0)));
        EXPECT_CALL(mockChunkService, ReadChunk(_, _, _, _)).Times(3)
            .WillRepeatedly(Invoke(ReadChunkFunc));
        copysetClient.ReadChunk(reqCtx->idinfo_, sn,
                                offset, len, 0, reqDone);
        cond.Wait();
        ASSERT_NE(0, reqDone->GetErrorCode());

        uint64_t end = TimeUtility::GetTimeofDayMs();
        ASSERT_GT(end - start, 3000);
        ASSERT_LT(end - start, 6000);

        std::this_thread::sleep_for(std::chrono::seconds(8));

        gReadCntlFailedCode = 0;
    }

    /* 设置 overload */
    {
        RequestContext *reqCtx = new FakeRequestContext();
        reqCtx->optype_ = OpType::READ;
        reqCtx->idinfo_ = ChunkIDInfo(chunkId, logicPoolId, copysetId);


        reqCtx->readBuffer_ = buff1;
        reqCtx->offset_ = 0;
        reqCtx->rawlength_ = len;

        curve::common::CountDownEvent cond(1);
        RequestClosure *reqDone = new FakeRequestClosure(&cond, reqCtx);

        // 配置文件设置的重试睡眠时间为500，因为chunkserver设置返回timeout
        // 导致触发底层指数退避，每次重试间隔增大。重试三次正常只需要睡眠3*500
        // 但是增加指数退避之后，睡眠间隔将增加到1000 + 2000 = 3000
        // 加上随机因子，三次重试时间应该大于2900, 且小于5000
        uint64_t start = TimeUtility::GetTimeofDayUs();

        reqCtx->done_ = reqDone;
        ChunkResponse response;
        response.set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_OVERLOAD);
        EXPECT_CALL(mockMetaCache, GetLeader(_, _, _, _, _, _))
            .Times(3).WillRepeatedly(DoAll(SetArgPointee<2>(leaderId1),
                                           SetArgPointee<3>(leaderAdder1),
                                           Return(0)));
        EXPECT_CALL(mockChunkService, ReadChunk(_, _, _, _)).Times(3)
            .WillRepeatedly(DoAll(SetArgPointee<2>(response),
                                  Invoke(ReadChunkFunc)));
        copysetClient.ReadChunk(reqCtx->idinfo_, sn,
                                offset, len, 0, reqDone);
        cond.Wait();
        ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_OVERLOAD,
                  reqDone->GetErrorCode());

        uint64_t end = TimeUtility::GetTimeofDayUs();
        ASSERT_GT(end - start, 2900);
        ASSERT_LT(end - start, 3 * 5000);
    }

    /* 其他错误 */
    {
        RequestContext *reqCtx = new FakeRequestContext();
        reqCtx->optype_ = OpType::READ;
        reqCtx->idinfo_ = ChunkIDInfo(chunkId, logicPoolId, copysetId);


        reqCtx->readBuffer_ = buff1;
        reqCtx->offset_ = 0;
        reqCtx->rawlength_ = len;

        curve::common::CountDownEvent cond(1);
        RequestClosure *reqDone = new FakeRequestClosure(&cond, reqCtx);

        reqCtx->done_ = reqDone;
        ChunkResponse response;
        response.set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_FAILURE_UNKNOWN);
        EXPECT_CALL(mockMetaCache, GetLeader(_, _, _, _, _, _))
            .Times(3).WillRepeatedly(DoAll(SetArgPointee<2>(leaderId1),
                                           SetArgPointee<3>(leaderAdder1),
                                           Return(0)));
        EXPECT_CALL(mockChunkService, ReadChunk(_, _, _, _)).Times(3)
            .WillRepeatedly(DoAll(SetArgPointee<2>(response),
                                  Invoke(ReadChunkFunc)));
        copysetClient.ReadChunk(reqCtx->idinfo_, sn,
                                offset, len, 0, reqDone);
        cond.Wait();
        ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_FAILURE_UNKNOWN,
                  reqDone->GetErrorCode());
    }
    /* 不是 leader，返回正确的 leader */
    {
        RequestContext *reqCtx = new FakeRequestContext();
        reqCtx->optype_ = OpType::READ;
        reqCtx->idinfo_ = ChunkIDInfo(chunkId, logicPoolId, copysetId);


        reqCtx->readBuffer_ = buff1;
        reqCtx->offset_ = 0;
        reqCtx->rawlength_ = len;

        curve::common::CountDownEvent cond(1);
        RequestClosure *reqDone = new FakeRequestClosure(&cond, reqCtx);

        reqCtx->done_ = reqDone;
        ChunkResponse response1;
        response1.set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_REDIRECTED);
        response1.set_redirect(leaderStr2);
        ChunkResponse response2;
        response2.set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS);
        EXPECT_CALL(mockMetaCache, GetLeader(_, _, _, _, _, _)).Times(2)
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
        copysetClient.ReadChunk(reqCtx->idinfo_, sn,
                                offset, len, 0, reqDone);
        cond.Wait();
        ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS,
                  reqDone->GetErrorCode());
    }
    /* 不是 leader，但是没有返回 leader，刷新 meta cache 成功 */
    {
        RequestContext *reqCtx = new FakeRequestContext();
        reqCtx->optype_ = OpType::READ;
        reqCtx->idinfo_ = ChunkIDInfo(chunkId, logicPoolId, copysetId);


        reqCtx->readBuffer_ = buff1;
        reqCtx->offset_ = 0;
        reqCtx->rawlength_ = len;

        curve::common::CountDownEvent cond(1);
        RequestClosure *reqDone = new FakeRequestClosure(&cond, reqCtx);

        reqCtx->done_ = reqDone;
        ChunkResponse response1;
        response1.set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_REDIRECTED);
//        response1.set_redirect(leaderStr2);
        ChunkResponse response2;
        response2.set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS);
        EXPECT_CALL(mockMetaCache, GetLeader(_, _, _, _, _, _)).Times(3)
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
        copysetClient.ReadChunk(reqCtx->idinfo_, sn,
                                offset, len, 0, reqDone);
        cond.Wait();
        ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS,
                  reqDone->GetErrorCode());
    }
    /* 不是 leader，但是没有返回 leader，刷新 meta cache 失败 */
    {
        RequestContext *reqCtx = new FakeRequestContext();
        reqCtx->optype_ = OpType::READ;
        reqCtx->idinfo_ = ChunkIDInfo(chunkId, logicPoolId, copysetId);


        reqCtx->readBuffer_ = buff1;
        reqCtx->offset_ = 0;
        reqCtx->rawlength_ = len;

        curve::common::CountDownEvent cond(1);
        RequestClosure *reqDone = new FakeRequestClosure(&cond, reqCtx);

        reqCtx->done_ = reqDone;
        ChunkResponse response1;
        response1.set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_REDIRECTED);
//        response1.set_redirect(leaderStr2);
        ChunkResponse response2;
        response2.set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS);
        EXPECT_CALL(mockMetaCache, GetLeader(_, _, _, _, _, _)).Times(3)
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
        copysetClient.ReadChunk(reqCtx->idinfo_, sn,
                                offset, len, 0, reqDone);
        cond.Wait();
        ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS,
                  reqDone->GetErrorCode());
    }
    /* 不是 leader，但返回的是错误 leader */
    {
        RequestContext *reqCtx = new FakeRequestContext();
        reqCtx->optype_ = OpType::READ;
        reqCtx->idinfo_ = ChunkIDInfo(chunkId, logicPoolId, copysetId);


        reqCtx->readBuffer_ = buff1;
        reqCtx->offset_ = 0;
        reqCtx->rawlength_ = len;

        curve::common::CountDownEvent cond(1);
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
        EXPECT_CALL(mockMetaCache, GetLeader(_, _, _, _, _, _)).Times(3)
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
        copysetClient.ReadChunk(reqCtx->idinfo_, sn,
                                offset, len, 0, reqDone);
        cond.Wait();
        ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_REDIRECTED,
                  reqDone->GetErrorCode());
    }
    /* copyset 不存在，更新 leader 依然失败 */
    {
        RequestContext *reqCtx = new FakeRequestContext();
        reqCtx->optype_ = OpType::READ;
        reqCtx->idinfo_ = ChunkIDInfo(chunkId, logicPoolId, copysetId);


        reqCtx->readBuffer_ = buff1;
        reqCtx->offset_ = 0;
        reqCtx->rawlength_ = len;

        curve::common::CountDownEvent cond(1);
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
        EXPECT_CALL(mockMetaCache, GetLeader(_, _, _, _, _, _)).Times(6)
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
        copysetClient.ReadChunk(reqCtx->idinfo_, sn,
                                offset, len, 0, reqDone);
        cond.Wait();
        ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_COPYSET_NOTEXIST,
                  reqDone->GetErrorCode());
    }
    /* copyset 不存在，更新 leader 成功 */
    {
        RequestContext *reqCtx = new FakeRequestContext();
        reqCtx->optype_ = OpType::READ;
        reqCtx->idinfo_ = ChunkIDInfo(chunkId, logicPoolId, copysetId);


        reqCtx->readBuffer_ = buff1;
        reqCtx->offset_ = 0;
        reqCtx->rawlength_ = len;

        curve::common::CountDownEvent cond(1);
        RequestClosure *reqDone = new FakeRequestClosure(&cond, reqCtx);

        reqCtx->done_ = reqDone;
        ChunkResponse response1;
        response1.set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_COPYSET_NOTEXIST);
        response1.set_redirect(leaderStr2);
        ChunkResponse response2;
        response2.set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS);
        response2.set_redirect(leaderStr3);
        EXPECT_CALL(mockMetaCache, GetLeader(_, _, _, _, _, _)).Times(3)
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
        copysetClient.ReadChunk(reqCtx->idinfo_, sn,
                                offset, len, 0, reqDone);
        cond.Wait();
        ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS,
                  reqDone->GetErrorCode());
    }
    scheduler.Fini();
}

/**
 * read snapshot error testing
 */
TEST_F(CopysetClientTest, read_snapshot_error_test) {
    MockChunkServiceImpl mockChunkService;
    ASSERT_EQ(server_->AddService(&mockChunkService,
                                  brpc::SERVER_DOESNT_OWN_SERVICE), 0);
    ASSERT_EQ(server_->Start(listenAddr_.c_str(), nullptr), 0);

    IOSenderOption_t ioSenderOpt;
    ioSenderOpt.rpcTimeoutMs = 5000;
    ioSenderOpt.rpcRetryTimes = 3;
    ioSenderOpt.failRequestOpt.opMaxRetry = 3;
    ioSenderOpt.failRequestOpt.opRetryIntervalUs = 500;
    ioSenderOpt.enableAppliedIndexRead = 1;

    CopysetClient copysetClient;
    MockMetaCache mockMetaCache;
    mockMetaCache.DelegateToFake();
    RequestScheduler scheduler;
    copysetClient.Init(&mockMetaCache, ioSenderOpt, &scheduler);

    LogicPoolID logicPoolId = 1;
    CopysetID copysetId = 100001;
    ChunkID chunkId = 1;
    size_t len = 8;
    int sn = 1;
    char buff1[8 + 1];
    char buff2[8 + 1];
    memset(buff1, 'a', 8);
    memset(buff2, 'a', 8);
    buff1[8] = '\0';
    buff2[8] = '\0';
    off_t offset = 0;

    ChunkServerID leaderId1 = 10000;
    butil::EndPoint leaderAdder1;
    std::string leaderStr1 = "127.0.0.1:9109";
    butil::str2endpoint(leaderStr1.c_str(), &leaderAdder1);
    ChunkServerID leaderId2 = 10001;
    butil::EndPoint leaderAdder2;
    std::string leaderStr2 = "127.0.0.1:9109";
    butil::str2endpoint(leaderStr2.c_str(), &leaderAdder2);
    ChunkServerID leaderId3 = 10002;
    butil::EndPoint leaderAdder3;
    std::string leaderStr3 = "127.0.0.1:9109";
    butil::str2endpoint(leaderStr3.c_str(), &leaderAdder3);
    ChunkServerID leaderId4 = 10003;
    butil::EndPoint leaderAdder4;
    std::string leaderStr4 = "127.0.0.1:9109";
    butil::str2endpoint(leaderStr4.c_str(), &leaderAdder4);

    /* 非法参数 */
    {
        RequestContext *reqCtx = new FakeRequestContext();
        reqCtx->optype_ = OpType::READ_SNAP;
        reqCtx->idinfo_ = ChunkIDInfo(chunkId, logicPoolId, copysetId);


        reqCtx->seq_ = sn;
        reqCtx->readBuffer_ = buff1;
        reqCtx->offset_ = 0;
        reqCtx->rawlength_ = len;

        curve::common::CountDownEvent cond(1);
        RequestClosure *reqDone = new FakeRequestClosure(&cond, reqCtx);

        reqCtx->done_ = reqDone;
        ChunkResponse response;
        response.set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_INVALID_REQUEST);
        EXPECT_CALL(mockMetaCache, GetLeader(_, _, _, _, _, _))
            .Times(AtLeast(1)).WillOnce(DoAll(SetArgPointee<2>(leaderId1),
                                              SetArgPointee<3>(leaderAdder1),
                                              Return(0)));
        EXPECT_CALL(mockChunkService, ReadChunkSnapshot(_, _, _, _)).Times(1)
            .WillOnce(DoAll(SetArgPointee<2>(response),
                            Invoke(ReadChunkSnapshotFunc)));
        copysetClient.ReadChunkSnapshot(reqCtx->idinfo_,
                                        sn, offset, len, reqDone, 0);
        cond.Wait();
        ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_INVALID_REQUEST,
                  reqDone->GetErrorCode());
    }
    /* chunk snapshot not exist */
    {
        RequestContext *reqCtx = new FakeRequestContext();
        reqCtx->optype_ = OpType::READ_SNAP;
        reqCtx->idinfo_ = ChunkIDInfo(chunkId, logicPoolId, copysetId);


        reqCtx->seq_ = sn;
        reqCtx->readBuffer_ = buff1;
        reqCtx->offset_ = 0;
        reqCtx->rawlength_ = len;

        curve::common::CountDownEvent cond(1);
        RequestClosure *reqDone = new FakeRequestClosure(&cond, reqCtx);

        reqCtx->done_ = reqDone;
        ChunkResponse response;
        response.set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_CHUNK_NOTEXIST);
        EXPECT_CALL(mockMetaCache, GetLeader(_, _, _, _, _, _))
            .Times(AtLeast(1)).WillOnce(DoAll(SetArgPointee<2>(leaderId1),
                                              SetArgPointee<3>(leaderAdder1),
                                              Return(0)));
        EXPECT_CALL(mockChunkService, ReadChunkSnapshot(_, _, _, _)).Times(1)
            .WillOnce(DoAll(SetArgPointee<2>(response),
                            Invoke(ReadChunkSnapshotFunc)));
        copysetClient.ReadChunkSnapshot(reqCtx->idinfo_,
                                        sn, offset, len, reqDone, 0);
        cond.Wait();
        ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_CHUNK_NOTEXIST,
                  reqDone->GetErrorCode());
    }
    /* controller error */
    {
        RequestContext *reqCtx = new FakeRequestContext();
        reqCtx->optype_ = OpType::READ_SNAP;
        reqCtx->idinfo_ = ChunkIDInfo(chunkId, logicPoolId, copysetId);


        reqCtx->seq_ = sn;
        reqCtx->offset_ = 0;
        reqCtx->rawlength_ = len;

        curve::common::CountDownEvent cond(1);
        RequestClosure *reqDone = new FakeRequestClosure(&cond, reqCtx);

        reqCtx->done_ = reqDone;
        gReadCntlFailedCode = -1;
        EXPECT_CALL(mockMetaCache, GetLeader(_, _, _, _, _, _)).Times(6)
            .WillRepeatedly(DoAll(SetArgPointee<2>(leaderId1),
                                  SetArgPointee<3>(leaderAdder1),
                                  Return(0)));
        EXPECT_CALL(mockChunkService, ReadChunkSnapshot(_, _, _, _)).Times(3)
            .WillRepeatedly(Invoke(ReadChunkSnapshotFunc));
        copysetClient.ReadChunkSnapshot(reqCtx->idinfo_,
                                        sn, offset, len, reqDone, 0);
        cond.Wait();
        ASSERT_NE(0, reqDone->GetErrorCode());
        gReadCntlFailedCode = 0;
    }
    /* 其他错误 */
    {
        RequestContext *reqCtx = new FakeRequestContext();
        reqCtx->optype_ = OpType::READ_SNAP;
        reqCtx->idinfo_ = ChunkIDInfo(chunkId, logicPoolId, copysetId);


        reqCtx->seq_ = sn;
        reqCtx->readBuffer_ = buff1;
        reqCtx->offset_ = 0;
        reqCtx->rawlength_ = len;

        curve::common::CountDownEvent cond(1);
        RequestClosure *reqDone = new FakeRequestClosure(&cond, reqCtx);

        reqCtx->done_ = reqDone;
        ChunkResponse response;
        response.set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_FAILURE_UNKNOWN);
        EXPECT_CALL(mockMetaCache, GetLeader(_, _, _, _, _, _))
            .Times(3).WillRepeatedly(DoAll(SetArgPointee<2>(leaderId1),
                                           SetArgPointee<3>(leaderAdder1),
                                           Return(0)));
        EXPECT_CALL(mockChunkService, ReadChunkSnapshot(_, _, _, _)).Times(3)
            .WillRepeatedly(DoAll(SetArgPointee<2>(response),
                                  Invoke(ReadChunkSnapshotFunc)));
        copysetClient.ReadChunkSnapshot(reqCtx->idinfo_,
                                        sn, offset, len, reqDone, 0);
        cond.Wait();
        ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_FAILURE_UNKNOWN,
                  reqDone->GetErrorCode());
    }
    /* 不是 leader，返回正确的 leader */
    {
        RequestContext *reqCtx = new FakeRequestContext();
        reqCtx->optype_ = OpType::READ_SNAP;
        reqCtx->idinfo_ = ChunkIDInfo(chunkId, logicPoolId, copysetId);


        reqCtx->seq_ = sn;
        reqCtx->readBuffer_ = buff1;
        reqCtx->offset_ = 0;
        reqCtx->rawlength_ = len;

        curve::common::CountDownEvent cond(1);
        RequestClosure *reqDone = new FakeRequestClosure(&cond, reqCtx);

        reqCtx->done_ = reqDone;
        ChunkResponse response1;
        response1.set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_REDIRECTED);
        response1.set_redirect(leaderStr2);
        ChunkResponse response2;
        response2.set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS);
        EXPECT_CALL(mockMetaCache, GetLeader(_, _, _, _, _, _)).Times(2)
            .WillOnce(DoAll(SetArgPointee<2>(leaderId1),
                            SetArgPointee<3>(leaderAdder1),
                            Return(0)))
            .WillOnce(DoAll(SetArgPointee<2>(leaderId2),
                            SetArgPointee<3>(leaderAdder2),
                            Return(0)));
        EXPECT_CALL(mockMetaCache, UpdateLeader(_, _, _, _)).Times(1)
            .WillOnce(Return(0));
        EXPECT_CALL(mockChunkService, ReadChunkSnapshot(_, _, _, _)).Times(2)
            .WillOnce(DoAll(SetArgPointee<2>(response1),
                            Invoke(ReadChunkSnapshotFunc)))
            .WillOnce(DoAll(SetArgPointee<2>(response2),
                            Invoke(ReadChunkSnapshotFunc)));
        copysetClient.ReadChunkSnapshot(reqCtx->idinfo_,
                                        sn, offset, len, reqDone, 0);
        cond.Wait();
        ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS,
                  reqDone->GetErrorCode());
    }
    /* 不是 leader，但是没有返回 leader，刷新 meta cache 成功 */
    {
        RequestContext *reqCtx = new FakeRequestContext();
        reqCtx->optype_ = OpType::READ_SNAP;
        reqCtx->idinfo_ = ChunkIDInfo(chunkId, logicPoolId, copysetId);


        reqCtx->seq_ = sn;
        reqCtx->readBuffer_ = buff1;
        reqCtx->offset_ = 0;
        reqCtx->rawlength_ = len;

        curve::common::CountDownEvent cond(1);
        RequestClosure *reqDone = new FakeRequestClosure(&cond, reqCtx);

        reqCtx->done_ = reqDone;
        ChunkResponse response1;
        response1.set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_REDIRECTED);
        ChunkResponse response2;
        response2.set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS);
        EXPECT_CALL(mockMetaCache, GetLeader(_, _, _, _, _, _)).Times(3)
            .WillOnce(DoAll(SetArgPointee<2>(leaderId1),
                            SetArgPointee<3>(leaderAdder1),
                            Return(0)))
            .WillOnce(DoAll(SetArgPointee<2>(leaderId2),
                            SetArgPointee<3>(leaderAdder2),
                            Return(0)))
            .WillOnce(DoAll(SetArgPointee<2>(leaderId2),
                            SetArgPointee<3>(leaderAdder2),
                            Return(0)));
        EXPECT_CALL(mockChunkService, ReadChunkSnapshot(_, _, _, _)).Times(2)
            .WillOnce(DoAll(SetArgPointee<2>(response1),
                            Invoke(ReadChunkSnapshotFunc)))
            .WillOnce(DoAll(SetArgPointee<2>(response2),
                            Invoke(ReadChunkSnapshotFunc)));
        copysetClient.ReadChunkSnapshot(reqCtx->idinfo_,
                                        sn, offset, len, reqDone, 0);
        cond.Wait();
        ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS,
                  reqDone->GetErrorCode());
    }
    /* 不是 leader，但是没有返回 leader，刷新 meta cache 失败 */
    {
        RequestContext *reqCtx = new FakeRequestContext();
        reqCtx->optype_ = OpType::READ_SNAP;
        reqCtx->idinfo_ = ChunkIDInfo(chunkId, logicPoolId, copysetId);


        reqCtx->seq_ = sn;
        reqCtx->readBuffer_ = buff1;
        reqCtx->offset_ = 0;
        reqCtx->rawlength_ = len;

        curve::common::CountDownEvent cond(1);
        RequestClosure *reqDone = new FakeRequestClosure(&cond, reqCtx);

        reqCtx->done_ = reqDone;
        ChunkResponse response1;
        response1.set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_REDIRECTED);
        ChunkResponse response2;
        response2.set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS);
        EXPECT_CALL(mockMetaCache, GetLeader(_, _, _, _, _, _)).Times(3)
            .WillOnce(DoAll(SetArgPointee<2>(leaderId1),
                            SetArgPointee<3>(leaderAdder1),
                            Return(0)))
            .WillOnce(DoAll(SetArgPointee<2>(leaderId2),
                            SetArgPointee<3>(leaderAdder2),
                            Return(-1)))
            .WillOnce(DoAll(SetArgPointee<2>(leaderId2),
                            SetArgPointee<3>(leaderAdder2),
                            Return(0)));
        EXPECT_CALL(mockChunkService, ReadChunkSnapshot(_, _, _, _)).Times(2)
            .WillOnce(DoAll(SetArgPointee<2>(response1),
                            Invoke(ReadChunkSnapshotFunc)))
            .WillOnce(DoAll(SetArgPointee<2>(response2),
                            Invoke(ReadChunkSnapshotFunc)));
        copysetClient.ReadChunkSnapshot(reqCtx->idinfo_,
                                        sn, offset, len, reqDone, 0);
        cond.Wait();
        ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS,
                  reqDone->GetErrorCode());
    }
    /* 不是 leader，但返回的是错误 leader */
    {
        RequestContext *reqCtx = new FakeRequestContext();
        reqCtx->optype_ = OpType::READ_SNAP;
        reqCtx->idinfo_ = ChunkIDInfo(chunkId, logicPoolId, copysetId);


        reqCtx->seq_ = sn;
        reqCtx->readBuffer_ = buff1;
        reqCtx->offset_ = 0;
        reqCtx->rawlength_ = len;

        curve::common::CountDownEvent cond(1);
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
        EXPECT_CALL(mockMetaCache, GetLeader(_, _, _, _, _, _)).Times(3)
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
        EXPECT_CALL(mockChunkService, ReadChunkSnapshot(_, _, _, _)).Times(3)
            .WillOnce(DoAll(SetArgPointee<2>(response1),
                            Invoke(ReadChunkSnapshotFunc)))
            .WillOnce(DoAll(SetArgPointee<2>(response2),
                            Invoke(ReadChunkSnapshotFunc)))
            .WillOnce(DoAll(SetArgPointee<2>(response3),
                            Invoke(ReadChunkSnapshotFunc)));
        copysetClient.ReadChunkSnapshot(reqCtx->idinfo_,
                                        sn, offset, len, reqDone, 0);
        cond.Wait();
        ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_REDIRECTED,
                  reqDone->GetErrorCode());
    }
    /* copyset 不存在，更新 leader 依然失败 */
    {
        RequestContext *reqCtx = new FakeRequestContext();
        reqCtx->optype_ = OpType::READ_SNAP;
        reqCtx->idinfo_ = ChunkIDInfo(chunkId, logicPoolId, copysetId);


        reqCtx->seq_ = sn;
        reqCtx->readBuffer_ = buff1;
        reqCtx->offset_ = 0;
        reqCtx->rawlength_ = len;

        curve::common::CountDownEvent cond(1);
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
        EXPECT_CALL(mockMetaCache, GetLeader(_, _, _, _, _, _)).Times(6)
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
        EXPECT_CALL(mockChunkService, ReadChunkSnapshot(_, _, _, _)).Times(3)
            .WillOnce(DoAll(SetArgPointee<2>(response1),
                            Invoke(ReadChunkSnapshotFunc)))
            .WillOnce(DoAll(SetArgPointee<2>(response2),
                            Invoke(ReadChunkSnapshotFunc)))
            .WillOnce(DoAll(SetArgPointee<2>(response3),
                            Invoke(ReadChunkSnapshotFunc)));
        copysetClient.ReadChunkSnapshot(reqCtx->idinfo_,
                                        sn, offset, len, reqDone, 0);
        cond.Wait();
        ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_COPYSET_NOTEXIST,
                  reqDone->GetErrorCode());
    }
    /* copyset 不存在，更新 leader 成功 */
    {
        RequestContext *reqCtx = new FakeRequestContext();
        reqCtx->optype_ = OpType::READ_SNAP;
        reqCtx->idinfo_ = ChunkIDInfo(chunkId, logicPoolId, copysetId);


        reqCtx->seq_ = sn;
        reqCtx->readBuffer_ = buff1;
        reqCtx->offset_ = 0;
        reqCtx->rawlength_ = len;

        curve::common::CountDownEvent cond(1);
        RequestClosure *reqDone = new FakeRequestClosure(&cond, reqCtx);

        reqCtx->done_ = reqDone;
        ChunkResponse response1;
        response1.set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_COPYSET_NOTEXIST);
        response1.set_redirect(leaderStr2);
        ChunkResponse response2;
        response2.set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS);
        response2.set_redirect(leaderStr3);
        EXPECT_CALL(mockMetaCache, GetLeader(_, _, _, _, _, _)).Times(3)
            .WillOnce(DoAll(SetArgPointee<2>(leaderId1),
                            SetArgPointee<3>(leaderAdder1),
                            Return(0)))
            .WillOnce(DoAll(SetArgPointee<2>(leaderId2),
                            SetArgPointee<3>(leaderAdder2),
                            Return(0)))
            .WillOnce(DoAll(SetArgPointee<2>(leaderId2),
                            SetArgPointee<3>(leaderAdder2),
                            Return(0)));
        EXPECT_CALL(mockChunkService, ReadChunkSnapshot(_, _, _, _)).Times(2)
            .WillOnce(DoAll(SetArgPointee<2>(response1),
                            Invoke(ReadChunkSnapshotFunc)))
            .WillOnce(DoAll(SetArgPointee<2>(response2),
                            Invoke(ReadChunkSnapshotFunc)));
        copysetClient.ReadChunkSnapshot(reqCtx->idinfo_,
                                        sn, offset, len, reqDone, 0);
        cond.Wait();
        ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS,
                  reqDone->GetErrorCode());
    }
    {
        RequestContext *reqCtx = new FakeRequestContext();
        reqCtx->optype_ = OpType::READ_SNAP;
        reqCtx->idinfo_ = ChunkIDInfo(chunkId, logicPoolId, copysetId);


        reqCtx->seq_ = sn;
        reqCtx->readBuffer_ = buff1;
        reqCtx->offset_ = 0;
        reqCtx->rawlength_ = len;

        curve::common::CountDownEvent cond(1);
        RequestClosure *reqDone = new FakeRequestClosure(&cond, reqCtx);

        reqCtx->done_ = reqDone;
        ChunkResponse response;
        response.set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS);
        EXPECT_CALL(mockMetaCache, GetLeader(_, _, _, _, _, _)).
                                            Times(AtLeast(1))
            .WillOnce(DoAll(SetArgPointee<2>(leaderId1),
                            SetArgPointee<3>(leaderAdder1),
                            Return(-1)))
            .WillOnce(DoAll(SetArgPointee<2>(leaderId1),
                            SetArgPointee<3>(leaderAdder1),
                            Return(-1)))
            .WillOnce(DoAll(SetArgPointee<2>(leaderId1),
                            SetArgPointee<3>(leaderAdder1),
                            Return(0)));
        EXPECT_CALL(mockChunkService, ReadChunkSnapshot(_, _, _, _)).Times(1)
            .WillOnce(DoAll(SetArgPointee<2>(response),
                            Invoke(ReadChunkSnapshotFunc)));
        copysetClient.ReadChunkSnapshot(reqCtx->idinfo_,
                                        sn, offset, len, reqDone, 0);
        cond.Wait();
        ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS,
                  reqDone->GetErrorCode());
    }
}

/**
 * delete snapshot error testing
 */
TEST_F(CopysetClientTest, delete_snapshot_error_test) {
    MockChunkServiceImpl mockChunkService;
    ASSERT_EQ(server_->AddService(&mockChunkService,
                                  brpc::SERVER_DOESNT_OWN_SERVICE), 0);
    ASSERT_EQ(server_->Start(listenAddr_.c_str(), nullptr), 0);

    IOSenderOption_t ioSenderOpt;
    ioSenderOpt.rpcTimeoutMs = 5000;
    ioSenderOpt.rpcRetryTimes = 3;
    ioSenderOpt.failRequestOpt.opMaxRetry = 3;
    ioSenderOpt.failRequestOpt.opRetryIntervalUs = 500;
    ioSenderOpt.enableAppliedIndexRead = 1;

    CopysetClient copysetClient;
    MockMetaCache mockMetaCache;
    mockMetaCache.DelegateToFake();
    RequestScheduler scheduler;
    copysetClient.Init(&mockMetaCache, ioSenderOpt, &scheduler);

    LogicPoolID logicPoolId = 1;
    CopysetID copysetId = 100001;
    ChunkID chunkId = 1;
    uint64_t sn = 1;

    ChunkServerID leaderId1 = 10000;
    butil::EndPoint leaderAdder1;
    std::string leaderStr1 = "127.0.0.1:9109";
    butil::str2endpoint(leaderStr1.c_str(), &leaderAdder1);
    ChunkServerID leaderId2 = 10001;
    butil::EndPoint leaderAdder2;
    std::string leaderStr2 = "127.0.0.1:9109";
    butil::str2endpoint(leaderStr2.c_str(), &leaderAdder2);
    ChunkServerID leaderId3 = 10002;
    butil::EndPoint leaderAdder3;
    std::string leaderStr3 = "127.0.0.1:9109";
    butil::str2endpoint(leaderStr3.c_str(), &leaderAdder3);
    ChunkServerID leaderId4 = 10003;
    butil::EndPoint leaderAdder4;
    std::string leaderStr4 = "127.0.0.1:9109";
    butil::str2endpoint(leaderStr4.c_str(), &leaderAdder4);

    /* 非法参数 */
    {
        RequestContext *reqCtx = new FakeRequestContext();
        reqCtx->optype_ = OpType::DELETE_SNAP;
        reqCtx->idinfo_ = ChunkIDInfo(chunkId, logicPoolId, copysetId);


        reqCtx->correctedSeq_ = sn;

        curve::common::CountDownEvent cond(1);
        RequestClosure *reqDone = new FakeRequestClosure(&cond, reqCtx);

        reqCtx->done_ = reqDone;
        ChunkResponse response;
        response.set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_INVALID_REQUEST);
        EXPECT_CALL(mockMetaCache, GetLeader(_, _, _, _, _, _))
            .Times(AtLeast(1)).WillOnce(DoAll(SetArgPointee<2>(leaderId1),
                                              SetArgPointee<3>(leaderAdder1),
                                              Return(0)));
        EXPECT_CALL(mockChunkService, DeleteChunkSnapshotOrCorrectSn(_, _, _, _))  // NOLINT
            .Times(1)
            .WillOnce(DoAll(SetArgPointee<2>(response),
                            Invoke(DeleteChunkSnapshotFunc)));
        copysetClient.DeleteChunkSnapshotOrCorrectSn(reqCtx->idinfo_,
                  sn, reqDone, 0);
        cond.Wait();
        ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_INVALID_REQUEST,
                  reqDone->GetErrorCode());
    }
    /* controller error */
    {
        RequestContext *reqCtx = new FakeRequestContext();
        reqCtx->optype_ = OpType::DELETE_SNAP;
        reqCtx->idinfo_ = ChunkIDInfo(chunkId, logicPoolId, copysetId);


        reqCtx->correctedSeq_ = sn;

        curve::common::CountDownEvent cond(1);
        RequestClosure *reqDone = new FakeRequestClosure(&cond, reqCtx);

        reqCtx->done_ = reqDone;
        gReadCntlFailedCode = -1;
        EXPECT_CALL(mockMetaCache, GetLeader(_, _, _, _, _, _)).Times(6)
            .WillRepeatedly(DoAll(SetArgPointee<2>(leaderId1),
                                  SetArgPointee<3>(leaderAdder1),
                                  Return(0)));
        EXPECT_CALL(mockChunkService, DeleteChunkSnapshotOrCorrectSn(_, _, _, _))  // NOLINT
            .Times(3)
            .WillRepeatedly(Invoke(DeleteChunkSnapshotFunc));
        copysetClient.DeleteChunkSnapshotOrCorrectSn(reqCtx->idinfo_,
                  sn, reqDone, 0);
        cond.Wait();
        ASSERT_NE(0, reqDone->GetErrorCode());
        gReadCntlFailedCode = 0;
    }
    /* 其他错误 */
    {
        RequestContext *reqCtx = new FakeRequestContext();
        reqCtx->optype_ = OpType::DELETE_SNAP;
        reqCtx->idinfo_ = ChunkIDInfo(chunkId, logicPoolId, copysetId);


        reqCtx->correctedSeq_ = sn;

        curve::common::CountDownEvent cond(1);
        RequestClosure *reqDone = new FakeRequestClosure(&cond, reqCtx);

        reqCtx->done_ = reqDone;
        ChunkResponse response;
        response.set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_FAILURE_UNKNOWN);
        EXPECT_CALL(mockMetaCache, GetLeader(_, _, _, _, _, _))
            .Times(3).WillRepeatedly(DoAll(SetArgPointee<2>(leaderId1),
                                           SetArgPointee<3>(leaderAdder1),
                                           Return(0)));
        EXPECT_CALL(mockChunkService, DeleteChunkSnapshotOrCorrectSn(_, _, _, _))  // NOLINT
            .Times(3)
            .WillRepeatedly(DoAll(SetArgPointee<2>(response),
                                  Invoke(DeleteChunkSnapshotFunc)));
        copysetClient.DeleteChunkSnapshotOrCorrectSn(reqCtx->idinfo_,
                  sn, reqDone, 0);
        cond.Wait();
        ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_FAILURE_UNKNOWN,
                  reqDone->GetErrorCode());
    }
    /* 不是 leader，返回正确的 leader */
    {
        RequestContext *reqCtx = new FakeRequestContext();
        reqCtx->optype_ = OpType::DELETE_SNAP;
        reqCtx->idinfo_ = ChunkIDInfo(chunkId, logicPoolId, copysetId);


        reqCtx->correctedSeq_ = sn;

        curve::common::CountDownEvent cond(1);
        RequestClosure *reqDone = new FakeRequestClosure(&cond, reqCtx);

        reqCtx->done_ = reqDone;
        ChunkResponse response1;
        response1.set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_REDIRECTED);
        response1.set_redirect(leaderStr2);
        ChunkResponse response2;
        response2.set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS);
        EXPECT_CALL(mockMetaCache, GetLeader(_, _, _, _, _, _)).Times(2)
            .WillOnce(DoAll(SetArgPointee<2>(leaderId1),
                            SetArgPointee<3>(leaderAdder1),
                            Return(0)))
            .WillOnce(DoAll(SetArgPointee<2>(leaderId2),
                            SetArgPointee<3>(leaderAdder2),
                            Return(0)));
        EXPECT_CALL(mockMetaCache, UpdateLeader(_, _, _, _)).Times(1)
            .WillOnce(Return(0));
        EXPECT_CALL(mockChunkService, DeleteChunkSnapshotOrCorrectSn(_, _, _, _))  // NOLINT
            .Times(2)
            .WillOnce(DoAll(SetArgPointee<2>(response1),
                            Invoke(ReadChunkSnapshotFunc)))
            .WillOnce(DoAll(SetArgPointee<2>(response2),
                            Invoke(DeleteChunkSnapshotFunc)));
        copysetClient.DeleteChunkSnapshotOrCorrectSn(reqCtx->idinfo_,
                  sn, reqDone, 0);
        cond.Wait();
        ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS,
                  reqDone->GetErrorCode());
    }
    /* 不是 leader，但是没有返回 leader，刷新 meta cache 成功 */
    {
        RequestContext *reqCtx = new FakeRequestContext();
        reqCtx->optype_ = OpType::DELETE_SNAP;
        reqCtx->idinfo_ = ChunkIDInfo(chunkId, logicPoolId, copysetId);


        reqCtx->correctedSeq_ = sn;

        curve::common::CountDownEvent cond(1);
        RequestClosure *reqDone = new FakeRequestClosure(&cond, reqCtx);

        reqCtx->done_ = reqDone;
        ChunkResponse response1;
        response1.set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_REDIRECTED);
        ChunkResponse response2;
        response2.set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS);
        EXPECT_CALL(mockMetaCache, GetLeader(_, _, _, _, _, _)).Times(3)
            .WillOnce(DoAll(SetArgPointee<2>(leaderId1),
                            SetArgPointee<3>(leaderAdder1),
                            Return(0)))
            .WillOnce(DoAll(SetArgPointee<2>(leaderId2),
                            SetArgPointee<3>(leaderAdder2),
                            Return(0)))
            .WillOnce(DoAll(SetArgPointee<2>(leaderId2),
                            SetArgPointee<3>(leaderAdder2),
                            Return(0)));
        EXPECT_CALL(mockChunkService, DeleteChunkSnapshotOrCorrectSn(_, _, _, _))  // NOLINT
            .Times(2)
            .WillOnce(DoAll(SetArgPointee<2>(response1),
                            Invoke(DeleteChunkSnapshotFunc)))
            .WillOnce(DoAll(SetArgPointee<2>(response2),
                            Invoke(DeleteChunkSnapshotFunc)));
        copysetClient.DeleteChunkSnapshotOrCorrectSn(reqCtx->idinfo_,
                  sn, reqDone, 0);
        cond.Wait();
        ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS,
                  reqDone->GetErrorCode());
    }
    /* 不是 leader，但是没有返回 leader，刷新 meta cache 失败 */
    {
        RequestContext *reqCtx = new FakeRequestContext();
        reqCtx->optype_ = OpType::DELETE_SNAP;
        reqCtx->idinfo_ = ChunkIDInfo(chunkId, logicPoolId, copysetId);


        reqCtx->correctedSeq_ = sn;

        curve::common::CountDownEvent cond(1);
        RequestClosure *reqDone = new FakeRequestClosure(&cond, reqCtx);

        reqCtx->done_ = reqDone;
        ChunkResponse response1;
        response1.set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_REDIRECTED);
        ChunkResponse response2;
        response2.set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS);
        EXPECT_CALL(mockMetaCache, GetLeader(_, _, _, _, _, _)).Times(3)
            .WillOnce(DoAll(SetArgPointee<2>(leaderId1),
                            SetArgPointee<3>(leaderAdder1),
                            Return(0)))
            .WillOnce(DoAll(SetArgPointee<2>(leaderId2),
                            SetArgPointee<3>(leaderAdder2),
                            Return(-1)))
            .WillOnce(DoAll(SetArgPointee<2>(leaderId2),
                            SetArgPointee<3>(leaderAdder2),
                            Return(0)));
        EXPECT_CALL(mockChunkService, DeleteChunkSnapshotOrCorrectSn(_, _, _, _))  // NOLINT
            .Times(2)
            .WillOnce(DoAll(SetArgPointee<2>(response1),
                            Invoke(DeleteChunkSnapshotFunc)))
            .WillOnce(DoAll(SetArgPointee<2>(response2),
                            Invoke(DeleteChunkSnapshotFunc)));
        copysetClient.DeleteChunkSnapshotOrCorrectSn(reqCtx->idinfo_,
                  sn, reqDone, 0);
        cond.Wait();
        ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS,
                  reqDone->GetErrorCode());
    }
    /* 不是 leader，但返回的是错误 leader */
    {
        RequestContext *reqCtx = new FakeRequestContext();
        reqCtx->optype_ = OpType::DELETE_SNAP;
        reqCtx->idinfo_ = ChunkIDInfo(chunkId, logicPoolId, copysetId);


        reqCtx->correctedSeq_ = sn;

        curve::common::CountDownEvent cond(1);
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
        EXPECT_CALL(mockMetaCache, GetLeader(_, _, _, _, _, _)).Times(3)
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
        EXPECT_CALL(mockChunkService, DeleteChunkSnapshotOrCorrectSn(_, _, _, _))  // NOLINT
            .Times(3)
            .WillOnce(DoAll(SetArgPointee<2>(response1),
                            Invoke(DeleteChunkSnapshotFunc)))
            .WillOnce(DoAll(SetArgPointee<2>(response2),
                            Invoke(DeleteChunkSnapshotFunc)))
            .WillOnce(DoAll(SetArgPointee<2>(response3),
                            Invoke(DeleteChunkSnapshotFunc)));
        copysetClient.DeleteChunkSnapshotOrCorrectSn(reqCtx->idinfo_,
                  sn, reqDone, 0);
        cond.Wait();
        ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_REDIRECTED,
                  reqDone->GetErrorCode());
    }
    /* copyset 不存在，更新 leader 依然失败 */
    {
        RequestContext *reqCtx = new FakeRequestContext();
        reqCtx->optype_ = OpType::DELETE_SNAP;
        reqCtx->idinfo_ = ChunkIDInfo(chunkId, logicPoolId, copysetId);


        reqCtx->correctedSeq_ = sn;

        curve::common::CountDownEvent cond(1);
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
        EXPECT_CALL(mockMetaCache, GetLeader(_, _, _, _, _, _)).Times(6)
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
        EXPECT_CALL(mockChunkService, DeleteChunkSnapshotOrCorrectSn(_, _, _, _))  // NOLINT
            .Times(3)
            .WillOnce(DoAll(SetArgPointee<2>(response1),
                            Invoke(DeleteChunkSnapshotFunc)))
            .WillOnce(DoAll(SetArgPointee<2>(response2),
                            Invoke(DeleteChunkSnapshotFunc)))
            .WillOnce(DoAll(SetArgPointee<2>(response3),
                            Invoke(DeleteChunkSnapshotFunc)));
        copysetClient.DeleteChunkSnapshotOrCorrectSn(reqCtx->idinfo_,
                  sn, reqDone, 0);
        cond.Wait();
        ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_COPYSET_NOTEXIST,
                  reqDone->GetErrorCode());
    }
    /* copyset 不存在，更新 leader 成功 */
    {
        RequestContext *reqCtx = new FakeRequestContext();
        reqCtx->optype_ = OpType::DELETE_SNAP;
        reqCtx->idinfo_ = ChunkIDInfo(chunkId, logicPoolId, copysetId);


        reqCtx->correctedSeq_ = sn;

        curve::common::CountDownEvent cond(1);
        RequestClosure *reqDone = new FakeRequestClosure(&cond, reqCtx);

        reqCtx->done_ = reqDone;
        ChunkResponse response1;
        response1.set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_COPYSET_NOTEXIST);
        response1.set_redirect(leaderStr2);
        ChunkResponse response2;
        response2.set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS);
        response2.set_redirect(leaderStr3);
        EXPECT_CALL(mockMetaCache, GetLeader(_, _, _, _, _, _)).Times(3)
            .WillOnce(DoAll(SetArgPointee<2>(leaderId1),
                            SetArgPointee<3>(leaderAdder1),
                            Return(0)))
            .WillOnce(DoAll(SetArgPointee<2>(leaderId2),
                            SetArgPointee<3>(leaderAdder2),
                            Return(0)))
            .WillOnce(DoAll(SetArgPointee<2>(leaderId2),
                            SetArgPointee<3>(leaderAdder2),
                            Return(0)));
        EXPECT_CALL(mockChunkService, DeleteChunkSnapshotOrCorrectSn(_, _, _, _))  // NOLINT
            .Times(2)
            .WillOnce(DoAll(SetArgPointee<2>(response1),
                            Invoke(DeleteChunkSnapshotFunc)))
            .WillOnce(DoAll(SetArgPointee<2>(response2),
                            Invoke(DeleteChunkSnapshotFunc)));
        copysetClient.DeleteChunkSnapshotOrCorrectSn(reqCtx->idinfo_,
                  sn, reqDone, 0);
        cond.Wait();
        ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS,
                  reqDone->GetErrorCode());
    }
    {
        RequestContext *reqCtx = new FakeRequestContext();
        reqCtx->optype_ = OpType::DELETE_SNAP;
        reqCtx->idinfo_ = ChunkIDInfo(chunkId, logicPoolId, copysetId);


        reqCtx->correctedSeq_ = sn;

        curve::common::CountDownEvent cond(1);
        RequestClosure *reqDone = new FakeRequestClosure(&cond, reqCtx);

        reqCtx->done_ = reqDone;
        ChunkResponse response;
        response.set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS);
        EXPECT_CALL(mockMetaCache, GetLeader(_, _, _, _, _, _)).
                                            Times(AtLeast(1))
            .WillOnce(DoAll(SetArgPointee<2>(leaderId1),
                            SetArgPointee<3>(leaderAdder1),
                            Return(-1)))
            .WillOnce(DoAll(SetArgPointee<2>(leaderId1),
                            SetArgPointee<3>(leaderAdder1),
                            Return(-1)))
            .WillOnce(DoAll(SetArgPointee<2>(leaderId1),
                            SetArgPointee<3>(leaderAdder1),
                            Return(0)));
        EXPECT_CALL(mockChunkService, DeleteChunkSnapshotOrCorrectSn(_, _, _, _))  // NOLINT
            .Times(1)
            .WillOnce(DoAll(SetArgPointee<2>(response),
                            Invoke(DeleteChunkSnapshotFunc)));
        copysetClient.DeleteChunkSnapshotOrCorrectSn(reqCtx->idinfo_,
                                          sn, reqDone, 0);
        cond.Wait();
        ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS,
                  reqDone->GetErrorCode());
    }
}

/**
 * create clone chunk error testing
 */
TEST_F(CopysetClientTest, create_clone_error_test) {
    MockChunkServiceImpl mockChunkService;
    ASSERT_EQ(server_->AddService(&mockChunkService,
                                  brpc::SERVER_DOESNT_OWN_SERVICE), 0);
    ASSERT_EQ(server_->Start(listenAddr_.c_str(), nullptr), 0);

    IOSenderOption_t ioSenderOpt;
    ioSenderOpt.rpcTimeoutMs = 5000;
    ioSenderOpt.rpcRetryTimes = 3;
    ioSenderOpt.failRequestOpt.opMaxRetry = 3;
    ioSenderOpt.failRequestOpt.opRetryIntervalUs = 500;
    ioSenderOpt.enableAppliedIndexRead = 1;

    CopysetClient copysetClient;
    MockMetaCache mockMetaCache;
    mockMetaCache.DelegateToFake();
    RequestScheduler scheduler;
    copysetClient.Init(&mockMetaCache, ioSenderOpt, &scheduler);

    LogicPoolID logicPoolId = 1;
    CopysetID copysetId = 100001;
    ChunkID chunkId = 1;
    uint64_t sn = 1;

    ChunkServerID leaderId1 = 10000;
    butil::EndPoint leaderAdder1;
    std::string leaderStr1 = "127.0.0.1:9109";
    butil::str2endpoint(leaderStr1.c_str(), &leaderAdder1);
    ChunkServerID leaderId2 = 10001;
    butil::EndPoint leaderAdder2;
    std::string leaderStr2 = "127.0.0.1:9109";
    butil::str2endpoint(leaderStr2.c_str(), &leaderAdder2);
    ChunkServerID leaderId3 = 10002;
    butil::EndPoint leaderAdder3;
    std::string leaderStr3 = "127.0.0.1:9109";
    butil::str2endpoint(leaderStr3.c_str(), &leaderAdder3);
    ChunkServerID leaderId4 = 10003;
    butil::EndPoint leaderAdder4;
    std::string leaderStr4 = "127.0.0.1:9109";
    butil::str2endpoint(leaderStr4.c_str(), &leaderAdder4);

    /* 非法参数 */
    {
        RequestContext *reqCtx = new FakeRequestContext();
        reqCtx->optype_ = OpType::CREATE_CLONE;
        reqCtx->idinfo_ = ChunkIDInfo(chunkId, logicPoolId, copysetId);


        reqCtx->seq_ = sn;

        curve::common::CountDownEvent cond(1);
        RequestClosure *reqDone = new FakeRequestClosure(&cond, reqCtx);

        reqCtx->done_ = reqDone;
        ChunkResponse response;
        response.set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_INVALID_REQUEST);
        EXPECT_CALL(mockMetaCache, GetLeader(_, _, _, _, _, _))
            .Times(AtLeast(1)).WillOnce(DoAll(SetArgPointee<2>(leaderId1),
                                              SetArgPointee<3>(leaderAdder1),
                                              Return(0)));
        EXPECT_CALL(mockChunkService, CreateCloneChunk(_, _, _, _)).Times(1)      // NOLINT
            .WillOnce(DoAll(SetArgPointee<2>(response),
                            Invoke(CreateCloneChunkFunc)));
        copysetClient.CreateCloneChunk(reqCtx->idinfo_,
                                        "destination", sn, 1, 1024, reqDone, 0);
        cond.Wait();
        ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_INVALID_REQUEST,
                  reqDone->GetErrorCode());
    }
    /* controller error */
    {
        RequestContext *reqCtx = new FakeRequestContext();
        reqCtx->optype_ = OpType::DELETE_SNAP;
        reqCtx->idinfo_ = ChunkIDInfo(chunkId, logicPoolId, copysetId);


        reqCtx->correctedSeq_ = sn;

        curve::common::CountDownEvent cond(1);
        RequestClosure *reqDone = new FakeRequestClosure(&cond, reqCtx);

        reqCtx->done_ = reqDone;
        gReadCntlFailedCode = -1;
        EXPECT_CALL(mockMetaCache, GetLeader(_, _, _, _, _, _)).Times(6)
            .WillRepeatedly(DoAll(SetArgPointee<2>(leaderId1),
                                  SetArgPointee<3>(leaderAdder1),
                                  Return(0)));
        EXPECT_CALL(mockChunkService, CreateCloneChunk(_, _, _, _)).Times(3)      // NOLINT
            .WillRepeatedly(Invoke(CreateCloneChunkFunc));
        copysetClient.CreateCloneChunk(reqCtx->idinfo_,
        "destination", sn, 1, 1024, reqDone, 0);
        cond.Wait();
        ASSERT_NE(0, reqDone->GetErrorCode());
        gReadCntlFailedCode = 0;
    }
    /* 其他错误 */
    {
        RequestContext *reqCtx = new FakeRequestContext();
        reqCtx->optype_ = OpType::DELETE_SNAP;
        reqCtx->idinfo_ = ChunkIDInfo(chunkId, logicPoolId, copysetId);


        reqCtx->correctedSeq_ = sn;

        curve::common::CountDownEvent cond(1);
        RequestClosure *reqDone = new FakeRequestClosure(&cond, reqCtx);

        reqCtx->done_ = reqDone;
        ChunkResponse response;
        response.set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_FAILURE_UNKNOWN);
        EXPECT_CALL(mockMetaCache, GetLeader(_, _, _, _, _, _))
            .Times(3).WillRepeatedly(DoAll(SetArgPointee<2>(leaderId1),
                                           SetArgPointee<3>(leaderAdder1),
                                           Return(0)));
        EXPECT_CALL(mockChunkService, CreateCloneChunk(_, _, _, _)).Times(3)      // NOLINT
            .WillRepeatedly(DoAll(SetArgPointee<2>(response),
                                  Invoke(CreateCloneChunkFunc)));
        copysetClient.CreateCloneChunk(reqCtx->idinfo_,
        "destination", sn, 1, 1024, reqDone, 0);
        cond.Wait();
        ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_FAILURE_UNKNOWN,
                  reqDone->GetErrorCode());
    }
    /* op success */
    {
        RequestContext *reqCtx = new FakeRequestContext();
        reqCtx->optype_ = OpType::DELETE_SNAP;
        reqCtx->idinfo_ = ChunkIDInfo(chunkId, logicPoolId, copysetId);


        reqCtx->correctedSeq_ = sn;

        curve::common::CountDownEvent cond(1);
        RequestClosure *reqDone = new FakeRequestClosure(&cond, reqCtx);

        reqCtx->done_ = reqDone;

        ChunkResponse response2;
        response2.set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS);
        EXPECT_CALL(mockMetaCache, GetLeader(_, _, _, _, _, _)).Times(1)
            .WillOnce(DoAll(SetArgPointee<2>(leaderId1),
                            SetArgPointee<3>(leaderAdder1),
                            Return(0)))
            .WillOnce(DoAll(SetArgPointee<2>(leaderId2),
                            SetArgPointee<3>(leaderAdder2),
                            Return(0)));
        EXPECT_CALL(mockChunkService, CreateCloneChunk(_, _, _, _)).Times(1)
            .WillOnce(DoAll(SetArgPointee<2>(response2),
                            Invoke(CreateCloneChunkFunc)));
        copysetClient.CreateCloneChunk(reqCtx->idinfo_,
        "destination", sn, 1, 1024, reqDone, 0);
        cond.Wait();
        ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS,
                  reqDone->GetErrorCode());
    }
    /* 不是 leader，但是没有返回 leader，刷新 meta cache 成功 */
    {
        RequestContext *reqCtx = new FakeRequestContext();
        reqCtx->optype_ = OpType::DELETE_SNAP;
        reqCtx->idinfo_ = ChunkIDInfo(chunkId, logicPoolId, copysetId);


        reqCtx->correctedSeq_ = sn;

        curve::common::CountDownEvent cond(1);
        RequestClosure *reqDone = new FakeRequestClosure(&cond, reqCtx);

        reqCtx->done_ = reqDone;
        ChunkResponse response1;
        response1.set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_REDIRECTED);
        ChunkResponse response2;
        response2.set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS);
        EXPECT_CALL(mockMetaCache, GetLeader(_, _, _, _, _, _)).Times(3)
            .WillOnce(DoAll(SetArgPointee<2>(leaderId1),
                            SetArgPointee<3>(leaderAdder1),
                            Return(0)))
            .WillOnce(DoAll(SetArgPointee<2>(leaderId2),
                            SetArgPointee<3>(leaderAdder2),
                            Return(0)))
            .WillOnce(DoAll(SetArgPointee<2>(leaderId2),
                            SetArgPointee<3>(leaderAdder2),
                            Return(0)));
        EXPECT_CALL(mockChunkService, CreateCloneChunk(_, _, _, _)).Times(2)      // NOLINT
            .WillOnce(DoAll(SetArgPointee<2>(response1),
                            Invoke(CreateCloneChunkFunc)))
            .WillOnce(DoAll(SetArgPointee<2>(response2),
                            Invoke(CreateCloneChunkFunc)));
        copysetClient.CreateCloneChunk(reqCtx->idinfo_,
        "destination", sn, 1, 1024, reqDone, 0);
        cond.Wait();
        ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS,
                  reqDone->GetErrorCode());
    }
    /* 不是 leader，但是没有返回 leader，刷新 meta cache 失败 */
    {
        RequestContext *reqCtx = new FakeRequestContext();
        reqCtx->optype_ = OpType::DELETE_SNAP;
        reqCtx->idinfo_ = ChunkIDInfo(chunkId, logicPoolId, copysetId);


        reqCtx->correctedSeq_ = sn;

        curve::common::CountDownEvent cond(1);
        RequestClosure *reqDone = new FakeRequestClosure(&cond, reqCtx);

        reqCtx->done_ = reqDone;
        ChunkResponse response1;
        response1.set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_REDIRECTED);
        ChunkResponse response2;
        response2.set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS);
        EXPECT_CALL(mockMetaCache, GetLeader(_, _, _, _, _, _)).Times(3)
            .WillOnce(DoAll(SetArgPointee<2>(leaderId1),
                            SetArgPointee<3>(leaderAdder1),
                            Return(0)))
            .WillOnce(DoAll(SetArgPointee<2>(leaderId2),
                            SetArgPointee<3>(leaderAdder2),
                            Return(-1)))
            .WillOnce(DoAll(SetArgPointee<2>(leaderId2),
                            SetArgPointee<3>(leaderAdder2),
                            Return(0)));
        EXPECT_CALL(mockChunkService, CreateCloneChunk(_, _, _, _)).Times(2)      // NOLINT
            .WillOnce(DoAll(SetArgPointee<2>(response1),
                            Invoke(CreateCloneChunkFunc)))
            .WillOnce(DoAll(SetArgPointee<2>(response2),
                            Invoke(CreateCloneChunkFunc)));
        copysetClient.CreateCloneChunk(reqCtx->idinfo_,
        "destination", sn, 1, 1024, reqDone, 0);
        cond.Wait();
        ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS,
                  reqDone->GetErrorCode());
    }
    /* 不是 leader，但返回的是错误 leader */
    {
        RequestContext *reqCtx = new FakeRequestContext();
        reqCtx->optype_ = OpType::DELETE_SNAP;
        reqCtx->idinfo_ = ChunkIDInfo(chunkId, logicPoolId, copysetId);


        reqCtx->correctedSeq_ = sn;

        curve::common::CountDownEvent cond(1);
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
        EXPECT_CALL(mockMetaCache, GetLeader(_, _, _, _, _, _)).Times(3)
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
        EXPECT_CALL(mockChunkService, CreateCloneChunk(_, _, _, _)).Times(3)      // NOLINT
            .WillOnce(DoAll(SetArgPointee<2>(response1),
                            Invoke(CreateCloneChunkFunc)))
            .WillOnce(DoAll(SetArgPointee<2>(response2),
                            Invoke(CreateCloneChunkFunc)))
            .WillOnce(DoAll(SetArgPointee<2>(response3),
                            Invoke(CreateCloneChunkFunc)));
        copysetClient.CreateCloneChunk(reqCtx->idinfo_,
                                "destination", sn, 1, 1024, reqDone, 0);
        cond.Wait();
        ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_REDIRECTED,
                  reqDone->GetErrorCode());
    }
    /* copyset 不存在，更新 leader 依然失败 */
    {
        RequestContext *reqCtx = new FakeRequestContext();
        reqCtx->optype_ = OpType::DELETE_SNAP;
        reqCtx->idinfo_ = ChunkIDInfo(chunkId, logicPoolId, copysetId);


        reqCtx->correctedSeq_ = sn;

        curve::common::CountDownEvent cond(1);
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
        EXPECT_CALL(mockMetaCache, GetLeader(_, _, _, _, _, _)).Times(6)
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
        EXPECT_CALL(mockChunkService, CreateCloneChunk(_, _, _, _)).Times(3)      // NOLINT
            .WillOnce(DoAll(SetArgPointee<2>(response1),
                            Invoke(CreateCloneChunkFunc)))
            .WillOnce(DoAll(SetArgPointee<2>(response2),
                            Invoke(CreateCloneChunkFunc)))
            .WillOnce(DoAll(SetArgPointee<2>(response3),
                            Invoke(CreateCloneChunkFunc)));
        copysetClient.CreateCloneChunk(reqCtx->idinfo_,
                             "destination", sn, 1, 1024, reqDone, 0);
        cond.Wait();
        ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_COPYSET_NOTEXIST,
                  reqDone->GetErrorCode());
    }
    /* copyset 不存在，更新 leader 成功 */
    {
        RequestContext *reqCtx = new FakeRequestContext();
        reqCtx->optype_ = OpType::DELETE_SNAP;
        reqCtx->idinfo_ = ChunkIDInfo(chunkId, logicPoolId, copysetId);


        reqCtx->correctedSeq_ = sn;

        curve::common::CountDownEvent cond(1);
        RequestClosure *reqDone = new FakeRequestClosure(&cond, reqCtx);

        reqCtx->done_ = reqDone;
        ChunkResponse response1;
        response1.set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_COPYSET_NOTEXIST);
        response1.set_redirect(leaderStr2);
        ChunkResponse response2;
        response2.set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS);
        response2.set_redirect(leaderStr3);
        EXPECT_CALL(mockMetaCache, GetLeader(_, _, _, _, _, _)).Times(3)
            .WillOnce(DoAll(SetArgPointee<2>(leaderId1),
                            SetArgPointee<3>(leaderAdder1),
                            Return(0)))
            .WillOnce(DoAll(SetArgPointee<2>(leaderId2),
                            SetArgPointee<3>(leaderAdder2),
                            Return(0)))
            .WillOnce(DoAll(SetArgPointee<2>(leaderId2),
                            SetArgPointee<3>(leaderAdder2),
                            Return(0)));
        EXPECT_CALL(mockChunkService, CreateCloneChunk(_, _, _, _)).Times(2)      // NOLINT
            .WillOnce(DoAll(SetArgPointee<2>(response1),
                            Invoke(CreateCloneChunkFunc)))
            .WillOnce(DoAll(SetArgPointee<2>(response2),
                            Invoke(CreateCloneChunkFunc)));
        copysetClient.CreateCloneChunk(reqCtx->idinfo_,
                             "destination", sn, 1, 1024, reqDone, 0);
        cond.Wait();
        ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS,
                  reqDone->GetErrorCode());
    }
    {
        RequestContext *reqCtx = new FakeRequestContext();
        reqCtx->optype_ = OpType::DELETE_SNAP;
        reqCtx->idinfo_ = ChunkIDInfo(chunkId, logicPoolId, copysetId);


        reqCtx->correctedSeq_ = sn;

        curve::common::CountDownEvent cond(1);
        RequestClosure *reqDone = new FakeRequestClosure(&cond, reqCtx);

        reqCtx->done_ = reqDone;
        ChunkResponse response;
        response.set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS);
        EXPECT_CALL(mockMetaCache, GetLeader(_, _, _, _, _, _)).
                                            Times(AtLeast(1))
            .WillOnce(DoAll(SetArgPointee<2>(leaderId1),
                            SetArgPointee<3>(leaderAdder1),
                            Return(-1)))
            .WillOnce(DoAll(SetArgPointee<2>(leaderId1),
                            SetArgPointee<3>(leaderAdder1),
                            Return(-1)))
            .WillOnce(DoAll(SetArgPointee<2>(leaderId1),
                            SetArgPointee<3>(leaderAdder1),
                            Return(0)));
        EXPECT_CALL(mockChunkService, CreateCloneChunk(_, _, _, _)).Times(1)      // NOLINT
            .WillOnce(DoAll(SetArgPointee<2>(response),
                            Invoke(CreateCloneChunkFunc)));
        copysetClient.CreateCloneChunk(reqCtx->idinfo_,
                                        "destination", sn, 1, 1024, reqDone, 0);
        cond.Wait();
        ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS,
                  reqDone->GetErrorCode());
    }
}


/**
 * recover chunk error testing
 */
TEST_F(CopysetClientTest, recover_chunk_error_test) {
    MockChunkServiceImpl mockChunkService;
    ASSERT_EQ(server_->AddService(&mockChunkService,
                                  brpc::SERVER_DOESNT_OWN_SERVICE), 0);
    ASSERT_EQ(server_->Start(listenAddr_.c_str(), nullptr), 0);

    IOSenderOption_t ioSenderOpt;
    ioSenderOpt.rpcTimeoutMs = 5000;
    ioSenderOpt.rpcRetryTimes = 3;
    ioSenderOpt.failRequestOpt.opMaxRetry = 3;
    ioSenderOpt.failRequestOpt.opRetryIntervalUs = 500;
    ioSenderOpt.enableAppliedIndexRead = 1;

    CopysetClient copysetClient;
    MockMetaCache mockMetaCache;
    mockMetaCache.DelegateToFake();
    RequestScheduler scheduler;
    copysetClient.Init(&mockMetaCache, ioSenderOpt, &scheduler);

    LogicPoolID logicPoolId = 1;
    CopysetID copysetId = 100001;
    ChunkID chunkId = 1;
    uint64_t sn = 1;

    ChunkServerID leaderId1 = 10000;
    butil::EndPoint leaderAdder1;
    std::string leaderStr1 = "127.0.0.1:9109";
    butil::str2endpoint(leaderStr1.c_str(), &leaderAdder1);
    ChunkServerID leaderId2 = 10001;
    butil::EndPoint leaderAdder2;
    std::string leaderStr2 = "127.0.0.1:9109";
    butil::str2endpoint(leaderStr2.c_str(), &leaderAdder2);
    ChunkServerID leaderId3 = 10002;
    butil::EndPoint leaderAdder3;
    std::string leaderStr3 = "127.0.0.1:9109";
    butil::str2endpoint(leaderStr3.c_str(), &leaderAdder3);
    ChunkServerID leaderId4 = 10003;
    butil::EndPoint leaderAdder4;
    std::string leaderStr4 = "127.0.0.1:9109";
    butil::str2endpoint(leaderStr4.c_str(), &leaderAdder4);

    /* 非法参数 */
    {
        RequestContext *reqCtx = new FakeRequestContext();
        reqCtx->optype_ = OpType::DELETE_SNAP;
        reqCtx->idinfo_ = ChunkIDInfo(chunkId, logicPoolId, copysetId);


        reqCtx->correctedSeq_ = sn;

        curve::common::CountDownEvent cond(1);
        RequestClosure *reqDone = new FakeRequestClosure(&cond, reqCtx);

        reqCtx->done_ = reqDone;
        ChunkResponse response;
        response.set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_INVALID_REQUEST);
        EXPECT_CALL(mockMetaCache, GetLeader(_, _, _, _, _, _))
            .Times(AtLeast(1)).WillOnce(DoAll(SetArgPointee<2>(leaderId1),
                                              SetArgPointee<3>(leaderAdder1),
                                              Return(0)));
        EXPECT_CALL(mockChunkService, RecoverChunk(_, _, _, _)).Times(1)
            .WillOnce(DoAll(SetArgPointee<2>(response),
                            Invoke(RecoverChunkFunc)));
        copysetClient.RecoverChunk(reqCtx->idinfo_, 0, 4096, reqDone, 0);
        cond.Wait();
        ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_INVALID_REQUEST,
                  reqDone->GetErrorCode());
    }
    /* controller error */
    {
        RequestContext *reqCtx = new FakeRequestContext();
        reqCtx->optype_ = OpType::DELETE_SNAP;
        reqCtx->idinfo_ = ChunkIDInfo(chunkId, logicPoolId, copysetId);


        reqCtx->correctedSeq_ = sn;

        curve::common::CountDownEvent cond(1);
        RequestClosure *reqDone = new FakeRequestClosure(&cond, reqCtx);

        reqCtx->done_ = reqDone;
        gReadCntlFailedCode = -1;
        EXPECT_CALL(mockMetaCache, GetLeader(_, _, _, _, _, _)).Times(6)
            .WillRepeatedly(DoAll(SetArgPointee<2>(leaderId1),
                                  SetArgPointee<3>(leaderAdder1),
                                  Return(0)));
        EXPECT_CALL(mockChunkService, RecoverChunk(_, _, _, _)).Times(3)
            .WillRepeatedly(Invoke(RecoverChunkFunc));
        copysetClient.RecoverChunk(reqCtx->idinfo_, 0, 4096, reqDone, 0);
        cond.Wait();
        ASSERT_NE(0, reqDone->GetErrorCode());
        gReadCntlFailedCode = 0;
    }
    /* 其他错误 */
    {
        RequestContext *reqCtx = new FakeRequestContext();
        reqCtx->optype_ = OpType::DELETE_SNAP;
        reqCtx->idinfo_ = ChunkIDInfo(chunkId, logicPoolId, copysetId);


        reqCtx->correctedSeq_ = sn;

        curve::common::CountDownEvent cond(1);
        RequestClosure *reqDone = new FakeRequestClosure(&cond, reqCtx);

        reqCtx->done_ = reqDone;
        ChunkResponse response;
        response.set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_FAILURE_UNKNOWN);
        EXPECT_CALL(mockMetaCache, GetLeader(_, _, _, _, _, _))
            .Times(3).WillRepeatedly(DoAll(SetArgPointee<2>(leaderId1),
                                           SetArgPointee<3>(leaderAdder1),
                                           Return(0)));
        EXPECT_CALL(mockChunkService, RecoverChunk(_, _, _, _)).Times(3)
            .WillRepeatedly(DoAll(SetArgPointee<2>(response),
                                  Invoke(RecoverChunkFunc)));
        copysetClient.RecoverChunk(reqCtx->idinfo_, 0, 4096, reqDone, 0);
        cond.Wait();
        ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_FAILURE_UNKNOWN,
                  reqDone->GetErrorCode());
    }
    /* 不是 leader，返回正确的 leader */
    {
        RequestContext *reqCtx = new FakeRequestContext();
        reqCtx->optype_ = OpType::DELETE_SNAP;
        reqCtx->idinfo_ = ChunkIDInfo(chunkId, logicPoolId, copysetId);


        reqCtx->correctedSeq_ = sn;

        curve::common::CountDownEvent cond(1);
        RequestClosure *reqDone = new FakeRequestClosure(&cond, reqCtx);

        reqCtx->done_ = reqDone;
        ChunkResponse response1;
        response1.set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_REDIRECTED);
        response1.set_redirect(leaderStr2);
        ChunkResponse response2;
        response2.set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS);
        EXPECT_CALL(mockMetaCache, GetLeader(_, _, _, _, _, _)).Times(1)
            .WillOnce(DoAll(SetArgPointee<2>(leaderId1),
                            SetArgPointee<3>(leaderAdder1),
                            Return(0)))
            .WillOnce(DoAll(SetArgPointee<2>(leaderId2),
                            SetArgPointee<3>(leaderAdder2),
                            Return(0)));
        EXPECT_CALL(mockChunkService, RecoverChunk(_, _, _, _)).Times(1)
            .WillOnce(DoAll(SetArgPointee<2>(response2),
                            Invoke(RecoverChunkFunc)));
        copysetClient.RecoverChunk(reqCtx->idinfo_, 0, 4096, reqDone, 0);
        cond.Wait();
        ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS,
                  reqDone->GetErrorCode());
    }
    /* 不是 leader，但是没有返回 leader，刷新 meta cache 成功 */
    {
        RequestContext *reqCtx = new FakeRequestContext();
        reqCtx->optype_ = OpType::DELETE_SNAP;
        reqCtx->idinfo_ = ChunkIDInfo(chunkId, logicPoolId, copysetId);


        reqCtx->correctedSeq_ = sn;

        curve::common::CountDownEvent cond(1);
        RequestClosure *reqDone = new FakeRequestClosure(&cond, reqCtx);

        reqCtx->done_ = reqDone;
        ChunkResponse response1;
        response1.set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_REDIRECTED);
        ChunkResponse response2;
        response2.set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS);
        EXPECT_CALL(mockMetaCache, GetLeader(_, _, _, _, _, _)).Times(3)
            .WillOnce(DoAll(SetArgPointee<2>(leaderId1),
                            SetArgPointee<3>(leaderAdder1),
                            Return(0)))
            .WillOnce(DoAll(SetArgPointee<2>(leaderId2),
                            SetArgPointee<3>(leaderAdder2),
                            Return(0)))
            .WillOnce(DoAll(SetArgPointee<2>(leaderId2),
                            SetArgPointee<3>(leaderAdder2),
                            Return(0)));
        EXPECT_CALL(mockChunkService, RecoverChunk(_, _, _, _)).Times(2)
            .WillOnce(DoAll(SetArgPointee<2>(response1),
                            Invoke(RecoverChunkFunc)))
            .WillOnce(DoAll(SetArgPointee<2>(response2),
                            Invoke(RecoverChunkFunc)));
        copysetClient.RecoverChunk(reqCtx->idinfo_, 0, 4096, reqDone, 0);
        cond.Wait();
        ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS,
                  reqDone->GetErrorCode());
    }
    /* 不是 leader，但是没有返回 leader，刷新 meta cache 失败 */
    {
        RequestContext *reqCtx = new FakeRequestContext();
        reqCtx->optype_ = OpType::DELETE_SNAP;
        reqCtx->idinfo_ = ChunkIDInfo(chunkId, logicPoolId, copysetId);


        reqCtx->correctedSeq_ = sn;

        curve::common::CountDownEvent cond(1);
        RequestClosure *reqDone = new FakeRequestClosure(&cond, reqCtx);

        reqCtx->done_ = reqDone;
        ChunkResponse response1;
        response1.set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_REDIRECTED);
        ChunkResponse response2;
        response2.set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS);
        EXPECT_CALL(mockMetaCache, GetLeader(_, _, _, _, _, _)).Times(3)
            .WillOnce(DoAll(SetArgPointee<2>(leaderId1),
                            SetArgPointee<3>(leaderAdder1),
                            Return(0)))
            .WillOnce(DoAll(SetArgPointee<2>(leaderId2),
                            SetArgPointee<3>(leaderAdder2),
                            Return(-1)))
            .WillOnce(DoAll(SetArgPointee<2>(leaderId2),
                            SetArgPointee<3>(leaderAdder2),
                            Return(0)));
        EXPECT_CALL(mockChunkService, RecoverChunk(_, _, _, _)).Times(2)
            .WillOnce(DoAll(SetArgPointee<2>(response1),
                            Invoke(RecoverChunkFunc)))
            .WillOnce(DoAll(SetArgPointee<2>(response2),
                            Invoke(RecoverChunkFunc)));
        copysetClient.RecoverChunk(reqCtx->idinfo_, 0, 4096, reqDone, 0);
        cond.Wait();
        ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS,
                  reqDone->GetErrorCode());
    }
    /* 不是 leader，但返回的是错误 leader */
    {
        RequestContext *reqCtx = new FakeRequestContext();
        reqCtx->optype_ = OpType::DELETE_SNAP;
        reqCtx->idinfo_ = ChunkIDInfo(chunkId, logicPoolId, copysetId);


        reqCtx->correctedSeq_ = sn;

        curve::common::CountDownEvent cond(1);
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
        EXPECT_CALL(mockMetaCache, GetLeader(_, _, _, _, _, _)).Times(3)
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
        EXPECT_CALL(mockChunkService, RecoverChunk(_, _, _, _)).Times(3)
            .WillOnce(DoAll(SetArgPointee<2>(response1),
                            Invoke(RecoverChunkFunc)))
            .WillOnce(DoAll(SetArgPointee<2>(response2),
                            Invoke(RecoverChunkFunc)))
            .WillOnce(DoAll(SetArgPointee<2>(response3),
                            Invoke(RecoverChunkFunc)));
        copysetClient.RecoverChunk(reqCtx->idinfo_, 0, 4096, reqDone, 0);
        cond.Wait();
        ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_REDIRECTED,
                  reqDone->GetErrorCode());
    }
    /* copyset 不存在，更新 leader 依然失败 */
    {
        RequestContext *reqCtx = new FakeRequestContext();
        reqCtx->optype_ = OpType::DELETE_SNAP;
        reqCtx->idinfo_ = ChunkIDInfo(chunkId, logicPoolId, copysetId);


        reqCtx->correctedSeq_ = sn;

        curve::common::CountDownEvent cond(1);
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
        EXPECT_CALL(mockMetaCache, GetLeader(_, _, _, _, _, _)).Times(6)
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
        EXPECT_CALL(mockChunkService, RecoverChunk(_, _, _, _)).Times(3)
            .WillOnce(DoAll(SetArgPointee<2>(response1),
                            Invoke(RecoverChunkFunc)))
            .WillOnce(DoAll(SetArgPointee<2>(response2),
                            Invoke(RecoverChunkFunc)))
            .WillOnce(DoAll(SetArgPointee<2>(response3),
                            Invoke(RecoverChunkFunc)));
        copysetClient.RecoverChunk(reqCtx->idinfo_, 0, 4096, reqDone, 0);
        cond.Wait();
        ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_COPYSET_NOTEXIST,
                  reqDone->GetErrorCode());
    }
    /* copyset 不存在，更新 leader 成功 */
    {
        RequestContext *reqCtx = new FakeRequestContext();
        reqCtx->optype_ = OpType::DELETE_SNAP;
        reqCtx->idinfo_ = ChunkIDInfo(chunkId, logicPoolId, copysetId);


        reqCtx->correctedSeq_ = sn;

        curve::common::CountDownEvent cond(1);
        RequestClosure *reqDone = new FakeRequestClosure(&cond, reqCtx);

        reqCtx->done_ = reqDone;
        ChunkResponse response1;
        response1.set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_COPYSET_NOTEXIST);
        response1.set_redirect(leaderStr2);
        ChunkResponse response2;
        response2.set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS);
        response2.set_redirect(leaderStr3);
        EXPECT_CALL(mockMetaCache, GetLeader(_, _, _, _, _, _)).Times(3)
            .WillOnce(DoAll(SetArgPointee<2>(leaderId1),
                            SetArgPointee<3>(leaderAdder1),
                            Return(0)))
            .WillOnce(DoAll(SetArgPointee<2>(leaderId2),
                            SetArgPointee<3>(leaderAdder2),
                            Return(0)))
            .WillOnce(DoAll(SetArgPointee<2>(leaderId2),
                            SetArgPointee<3>(leaderAdder2),
                            Return(0)));
        EXPECT_CALL(mockChunkService, RecoverChunk(_, _, _, _)).Times(2)
            .WillOnce(DoAll(SetArgPointee<2>(response1),
                            Invoke(RecoverChunkFunc)))
            .WillOnce(DoAll(SetArgPointee<2>(response2),
                            Invoke(RecoverChunkFunc)));
        copysetClient.RecoverChunk(reqCtx->idinfo_, 0, 4096, reqDone, 0);
        cond.Wait();
        ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS,
                  reqDone->GetErrorCode());
    }
    {
        RequestContext *reqCtx = new FakeRequestContext();
        reqCtx->optype_ = OpType::DELETE_SNAP;
        reqCtx->idinfo_ = ChunkIDInfo(chunkId, logicPoolId, copysetId);


        reqCtx->correctedSeq_ = sn;

        curve::common::CountDownEvent cond(1);
        RequestClosure *reqDone = new FakeRequestClosure(&cond, reqCtx);
        reqCtx->done_ = reqDone;
        ChunkResponse response;
        response.set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS);
        EXPECT_CALL(mockMetaCache, GetLeader(_, _, _, _, _, _)).
                                            Times(AtLeast(1))
            .WillOnce(DoAll(SetArgPointee<2>(leaderId1),
                            SetArgPointee<3>(leaderAdder1),
                            Return(-1)))
            .WillOnce(DoAll(SetArgPointee<2>(leaderId1),
                            SetArgPointee<3>(leaderAdder1),
                            Return(-1)))
            .WillOnce(DoAll(SetArgPointee<2>(leaderId1),
                            SetArgPointee<3>(leaderAdder1),
                            Return(0)));
        EXPECT_CALL(mockChunkService, RecoverChunk(_, _, _, _)).Times(1)
            .WillOnce(DoAll(SetArgPointee<2>(response),
                            Invoke(RecoverChunkFunc)));
        copysetClient.RecoverChunk(reqCtx->idinfo_,
                                          0, 4096, reqDone, 0);
        cond.Wait();
        ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS,
                  reqDone->GetErrorCode());
    }
}

/**
 * get chunk info error testing
 */
TEST_F(CopysetClientTest, get_chunk_info_test) {
    MockChunkServiceImpl mockChunkService;
    ASSERT_EQ(server_->AddService(&mockChunkService,
                                  brpc::SERVER_DOESNT_OWN_SERVICE), 0);
    ASSERT_EQ(server_->Start(listenAddr_.c_str(), nullptr), 0);

    IOSenderOption_t ioSenderOpt;
    ioSenderOpt.rpcTimeoutMs = 5000;
    ioSenderOpt.rpcRetryTimes = 3;
    ioSenderOpt.failRequestOpt.opMaxRetry = 3;
    ioSenderOpt.failRequestOpt.opRetryIntervalUs = 500;
    ioSenderOpt.enableAppliedIndexRead = 1;

    CopysetClient copysetClient;
    MockMetaCache mockMetaCache;
    mockMetaCache.DelegateToFake();
    RequestScheduler scheduler;
    copysetClient.Init(&mockMetaCache, ioSenderOpt, &scheduler);

    LogicPoolID logicPoolId = 1;
    CopysetID copysetId = 100001;
    ChunkID chunkId = 1;

    ChunkServerID leaderId1 = 10000;
    butil::EndPoint leaderAdder1;
    std::string leaderStr1 = "127.0.0.1:9109";
    butil::str2endpoint(leaderStr1.c_str(), &leaderAdder1);
    ChunkServerID leaderId2 = 10001;
    butil::EndPoint leaderAdder2;
    std::string leaderStr2 = "127.0.0.1:9109";
    butil::str2endpoint(leaderStr2.c_str(), &leaderAdder2);
    ChunkServerID leaderId3 = 10002;
    butil::EndPoint leaderAdder3;
    std::string leaderStr3 = "127.0.0.1:9109";
    butil::str2endpoint(leaderStr3.c_str(), &leaderAdder3);
    ChunkServerID leaderId4 = 10003;
    butil::EndPoint leaderAdder4;
    std::string leaderStr4 = "127.0.0.1:9109";
    butil::str2endpoint(leaderStr4.c_str(), &leaderAdder4);

    /* 非法参数 */
    {
        RequestContext *reqCtx = new FakeRequestContext();
        reqCtx->optype_ = OpType::GET_CHUNK_INFO;
        reqCtx->idinfo_ = ChunkIDInfo(chunkId, logicPoolId, copysetId);



        curve::common::CountDownEvent cond(1);
        RequestClosure *reqDone = new FakeRequestClosure(&cond, reqCtx);
        reqCtx->done_ = reqDone;
        GetChunkInfoResponse response;
        response.set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_INVALID_REQUEST);
        EXPECT_CALL(mockMetaCache, GetLeader(_, _, _, _, _, _))
            .Times(AtLeast(1)).WillOnce(DoAll(SetArgPointee<2>(leaderId1),
                                              SetArgPointee<3>(leaderAdder1),
                                              Return(0)));
        EXPECT_CALL(mockChunkService, GetChunkInfo(_, _, _, _)).Times(1)
            .WillOnce(DoAll(SetArgPointee<2>(response),
                            Invoke(GetChunkInfoFunc)));
        copysetClient.GetChunkInfo(reqCtx->idinfo_, reqDone, 0);
        cond.Wait();
        ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_INVALID_REQUEST,
                  reqDone->GetErrorCode());
    }
    /* controller error */
    {
        RequestContext *reqCtx = new FakeRequestContext();
        reqCtx->optype_ = OpType::GET_CHUNK_INFO;
        reqCtx->idinfo_ = ChunkIDInfo(chunkId, logicPoolId, copysetId);



        curve::common::CountDownEvent cond(1);
        RequestClosure *reqDone = new FakeRequestClosure(&cond, reqCtx);
        reqCtx->done_ = reqDone;
        gReadCntlFailedCode = -1;
        EXPECT_CALL(mockMetaCache, GetLeader(_, _, _, _, _, _)).Times(6)
            .WillRepeatedly(DoAll(SetArgPointee<2>(leaderId1),
                                  SetArgPointee<3>(leaderAdder1),
                                  Return(0)));
        EXPECT_CALL(mockChunkService, GetChunkInfo(_, _, _, _)).Times(3)
            .WillRepeatedly(Invoke(GetChunkInfoFunc));
        copysetClient.GetChunkInfo(reqCtx->idinfo_, reqDone, 0);
        cond.Wait();
        ASSERT_NE(0, reqDone->GetErrorCode());
        gReadCntlFailedCode = 0;
    }
    /* 其他错误 */
    {
        RequestContext *reqCtx = new FakeRequestContext();
        reqCtx->optype_ = OpType::GET_CHUNK_INFO;
        reqCtx->idinfo_ = ChunkIDInfo(chunkId, logicPoolId, copysetId);



        curve::common::CountDownEvent cond(1);
        RequestClosure *reqDone = new FakeRequestClosure(&cond, reqCtx);
        reqCtx->done_ = reqDone;
        GetChunkInfoResponse response;
        response.set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_FAILURE_UNKNOWN);
        EXPECT_CALL(mockMetaCache, GetLeader(_, _, _, _, _, _))
            .Times(3).WillRepeatedly(DoAll(SetArgPointee<2>(leaderId1),
                                           SetArgPointee<3>(leaderAdder1),
                                           Return(0)));
        EXPECT_CALL(mockChunkService, GetChunkInfo(_, _, _, _)).Times(3)
            .WillRepeatedly(DoAll(SetArgPointee<2>(response),
                                  Invoke(GetChunkInfoFunc)));
        copysetClient.GetChunkInfo(reqCtx->idinfo_, reqDone, 0);
        cond.Wait();
        ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_FAILURE_UNKNOWN,
                  reqDone->GetErrorCode());
    }
    /* 不是 leader，返回正确的 leader */
    {
        RequestContext *reqCtx = new FakeRequestContext();
        reqCtx->optype_ = OpType::GET_CHUNK_INFO;
        reqCtx->idinfo_ = ChunkIDInfo(chunkId, logicPoolId, copysetId);



        curve::common::CountDownEvent cond(1);
        RequestClosure *reqDone = new FakeRequestClosure(&cond, reqCtx);
        reqCtx->done_ = reqDone;
        GetChunkInfoResponse response1;
        response1.set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_REDIRECTED);
        response1.set_redirect(leaderStr2);
        GetChunkInfoResponse response2;
        response2.set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS);
        EXPECT_CALL(mockMetaCache, GetLeader(_, _, _, _, _, _)).Times(2)
            .WillOnce(DoAll(SetArgPointee<2>(leaderId1),
                            SetArgPointee<3>(leaderAdder1),
                            Return(0)))
            .WillOnce(DoAll(SetArgPointee<2>(leaderId2),
                            SetArgPointee<3>(leaderAdder2),
                            Return(0)));
        EXPECT_CALL(mockMetaCache, UpdateLeader(_, _, _, _)).Times(1)
            .WillOnce(Return(0));
        EXPECT_CALL(mockChunkService, GetChunkInfo(_, _, _, _)).Times(2)
            .WillOnce(DoAll(SetArgPointee<2>(response1),
                            Invoke(GetChunkInfoFunc)))
            .WillOnce(DoAll(SetArgPointee<2>(response2),
                            Invoke(GetChunkInfoFunc)));
        copysetClient.GetChunkInfo(reqCtx->idinfo_, reqDone, 0);
        cond.Wait();
        ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS,
                  reqDone->GetErrorCode());
    }
    /* 不是 leader，但是没有返回 leader，刷新 meta cache 成功 */
    {
        RequestContext *reqCtx = new FakeRequestContext();
        reqCtx->optype_ = OpType::GET_CHUNK_INFO;
        reqCtx->idinfo_ = ChunkIDInfo(chunkId, logicPoolId, copysetId);



        curve::common::CountDownEvent cond(1);
        RequestClosure *reqDone = new FakeRequestClosure(&cond, reqCtx);
        reqCtx->done_ = reqDone;
        GetChunkInfoResponse response1;
        response1.set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_REDIRECTED);
        GetChunkInfoResponse response2;
        response2.set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS);
        EXPECT_CALL(mockMetaCache, GetLeader(_, _, _, _, _, _)).Times(3)
            .WillOnce(DoAll(SetArgPointee<2>(leaderId1),
                            SetArgPointee<3>(leaderAdder1),
                            Return(0)))
            .WillOnce(DoAll(SetArgPointee<2>(leaderId2),
                            SetArgPointee<3>(leaderAdder2),
                            Return(0)))
            .WillOnce(DoAll(SetArgPointee<2>(leaderId2),
                            SetArgPointee<3>(leaderAdder2),
                            Return(0)));
        EXPECT_CALL(mockChunkService, GetChunkInfo(_, _, _, _)).Times(2)
            .WillOnce(DoAll(SetArgPointee<2>(response1),
                            Invoke(GetChunkInfoFunc)))
            .WillOnce(DoAll(SetArgPointee<2>(response2),
                            Invoke(GetChunkInfoFunc)));
        copysetClient.GetChunkInfo(reqCtx->idinfo_, reqDone, 0);
        cond.Wait();
        ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS,
                  reqDone->GetErrorCode());
    }
    /* 不是 leader，但是没有返回 leader，刷新 meta cache 失败 */
    {
        RequestContext *reqCtx = new FakeRequestContext();
        reqCtx->optype_ = OpType::GET_CHUNK_INFO;
        reqCtx->idinfo_ = ChunkIDInfo(chunkId, logicPoolId, copysetId);



        curve::common::CountDownEvent cond(1);
        RequestClosure *reqDone = new FakeRequestClosure(&cond, reqCtx);
        reqCtx->done_ = reqDone;
        GetChunkInfoResponse response1;
        response1.set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_REDIRECTED);
        GetChunkInfoResponse response2;
        response2.set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS);
        EXPECT_CALL(mockMetaCache, GetLeader(_, _, _, _, _, _)).Times(3)
            .WillOnce(DoAll(SetArgPointee<2>(leaderId1),
                            SetArgPointee<3>(leaderAdder1),
                            Return(0)))
            .WillOnce(DoAll(SetArgPointee<2>(leaderId2),
                            SetArgPointee<3>(leaderAdder2),
                            Return(-1)))
            .WillOnce(DoAll(SetArgPointee<2>(leaderId2),
                            SetArgPointee<3>(leaderAdder2),
                            Return(0)));
        EXPECT_CALL(mockChunkService, GetChunkInfo(_, _, _, _)).Times(2)
            .WillOnce(DoAll(SetArgPointee<2>(response1),
                            Invoke(GetChunkInfoFunc)))
            .WillOnce(DoAll(SetArgPointee<2>(response2),
                            Invoke(GetChunkInfoFunc)));
        copysetClient.GetChunkInfo(reqCtx->idinfo_, reqDone, 0);
        cond.Wait();
        ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS,
                  reqDone->GetErrorCode());
    }
    /* 不是 leader，但返回的是错误 leader */
    {
        RequestContext *reqCtx = new FakeRequestContext();
        reqCtx->optype_ = OpType::GET_CHUNK_INFO;
        reqCtx->idinfo_ = ChunkIDInfo(chunkId, logicPoolId, copysetId);



        curve::common::CountDownEvent cond(1);
        RequestClosure *reqDone = new FakeRequestClosure(&cond, reqCtx);
        reqCtx->done_ = reqDone;
        GetChunkInfoResponse response1;
        response1.set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_REDIRECTED);
        response1.set_redirect(leaderStr2);
        GetChunkInfoResponse response2;
        response2.set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_REDIRECTED);
        response2.set_redirect(leaderStr3);
        GetChunkInfoResponse response3;
        response3.set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_REDIRECTED);
        response3.set_redirect(leaderStr4);
        EXPECT_CALL(mockMetaCache, GetLeader(_, _, _, _, _, _)).Times(3)
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
        EXPECT_CALL(mockChunkService, GetChunkInfo(_, _, _, _)).Times(3)
            .WillOnce(DoAll(SetArgPointee<2>(response1),
                            Invoke(GetChunkInfoFunc)))
            .WillOnce(DoAll(SetArgPointee<2>(response2),
                            Invoke(GetChunkInfoFunc)))
            .WillOnce(DoAll(SetArgPointee<2>(response3),
                            Invoke(GetChunkInfoFunc)));
        copysetClient.GetChunkInfo(reqCtx->idinfo_, reqDone, 0);
        cond.Wait();
        ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_REDIRECTED,
                  reqDone->GetErrorCode());
    }
    /* copyset 不存在，更新 leader 依然失败 */
    {
        RequestContext *reqCtx = new FakeRequestContext();
        reqCtx->optype_ = OpType::GET_CHUNK_INFO;
        reqCtx->idinfo_ = ChunkIDInfo(chunkId, logicPoolId, copysetId);



        curve::common::CountDownEvent cond(1);
        RequestClosure *reqDone = new FakeRequestClosure(&cond, reqCtx);
        reqCtx->done_ = reqDone;
        GetChunkInfoResponse response1;
        response1.set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_COPYSET_NOTEXIST);
        response1.set_redirect(leaderStr2);
        GetChunkInfoResponse response2;
        response2.set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_COPYSET_NOTEXIST);
        response2.set_redirect(leaderStr3);
        GetChunkInfoResponse response3;
        response3.set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_COPYSET_NOTEXIST);
        response3.set_redirect(leaderStr4);
        EXPECT_CALL(mockMetaCache, GetLeader(_, _, _, _, _, _)).Times(6)
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
        EXPECT_CALL(mockChunkService, GetChunkInfo(_, _, _, _)).Times(3)
            .WillOnce(DoAll(SetArgPointee<2>(response1),
                            Invoke(GetChunkInfoFunc)))
            .WillOnce(DoAll(SetArgPointee<2>(response2),
                            Invoke(GetChunkInfoFunc)))
            .WillOnce(DoAll(SetArgPointee<2>(response3),
                            Invoke(GetChunkInfoFunc)));
        copysetClient.GetChunkInfo(reqCtx->idinfo_, reqDone, 0);
        cond.Wait();
        ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_COPYSET_NOTEXIST,
                  reqDone->GetErrorCode());
    }
    /* copyset 不存在，更新 leader 成功 */
    {
        RequestContext *reqCtx = new FakeRequestContext();
        reqCtx->optype_ = OpType::GET_CHUNK_INFO;
        reqCtx->idinfo_ = ChunkIDInfo(chunkId, logicPoolId, copysetId);



        curve::common::CountDownEvent cond(1);
        RequestClosure *reqDone = new FakeRequestClosure(&cond, reqCtx);
        reqCtx->done_ = reqDone;
        GetChunkInfoResponse response1;
        response1.set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_COPYSET_NOTEXIST);
        response1.set_redirect(leaderStr2);
        GetChunkInfoResponse response2;
        response2.set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS);
        response2.set_redirect(leaderStr3);
        EXPECT_CALL(mockMetaCache, GetLeader(_, _, _, _, _, _)).Times(3)
            .WillOnce(DoAll(SetArgPointee<2>(leaderId1),
                            SetArgPointee<3>(leaderAdder1),
                            Return(0)))
            .WillOnce(DoAll(SetArgPointee<2>(leaderId2),
                            SetArgPointee<3>(leaderAdder2),
                            Return(0)))
            .WillOnce(DoAll(SetArgPointee<2>(leaderId2),
                            SetArgPointee<3>(leaderAdder2),
                            Return(0)));
        EXPECT_CALL(mockChunkService, GetChunkInfo(_, _, _, _)).Times(2)
            .WillOnce(DoAll(SetArgPointee<2>(response1),
                            Invoke(GetChunkInfoFunc)))
            .WillOnce(DoAll(SetArgPointee<2>(response2),
                            Invoke(GetChunkInfoFunc)));
        copysetClient.GetChunkInfo(reqCtx->idinfo_, reqDone, 0);
        cond.Wait();
        ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS,
                  reqDone->GetErrorCode());
    }
    {
        RequestContext *reqCtx = new FakeRequestContext();
        reqCtx->optype_ = OpType::GET_CHUNK_INFO;
        reqCtx->idinfo_ = ChunkIDInfo(chunkId, logicPoolId, copysetId);



        curve::common::CountDownEvent cond(1);
        RequestClosure *reqDone = new FakeRequestClosure(&cond, reqCtx);
        reqCtx->done_ = reqDone;
        GetChunkInfoResponse response;
        response.set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS);
        EXPECT_CALL(mockMetaCache, GetLeader(_, _, _, _, _, _)).
                    Times(AtLeast(1))
            .WillOnce(DoAll(SetArgPointee<2>(leaderId1),
                            SetArgPointee<3>(leaderAdder1),
                            Return(-1)))
            .WillOnce(DoAll(SetArgPointee<2>(leaderId1),
                            SetArgPointee<3>(leaderAdder1),
                            Return(-1)))
            .WillOnce(DoAll(SetArgPointee<2>(leaderId1),
                            SetArgPointee<3>(leaderAdder1),
                            Return(0)));
        EXPECT_CALL(mockChunkService, GetChunkInfo(_, _, _, _)).Times(1)
            .WillOnce(DoAll(SetArgPointee<2>(response),
                            Invoke(GetChunkInfoFunc)));
        copysetClient.GetChunkInfo(reqCtx->idinfo_, reqDone, 0);
        cond.Wait();
        ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS,
                  reqDone->GetErrorCode());
    }
}

}   // namespace client
}   // namespace curve
