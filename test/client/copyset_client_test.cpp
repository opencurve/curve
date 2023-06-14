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
 * Created Date: 18-9-29
 * Author: wudemiao
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
#include "test/client/fake/fakeChunkserver.h"
#include "test/client/mock_request_scheduler.h"
#include "src/client/request_closure.h"
#include "src/client/metacache.h"

namespace curve {
namespace client {

using curve::chunkserver::CHUNK_OP_STATUS;
using curve::chunkserver::ChunkRequest;

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
        cntl->SetFailed(-1, "read snapshot controller error");
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

static void CreateCloneChunkFunc(
    ::google::protobuf::RpcController* controller,
    const ::curve::chunkserver::ChunkRequest* request,
    ::curve::chunkserver::ChunkResponse* response,
    google::protobuf::Closure* done) {
    brpc::ClosureGuard doneGuard(done);
    if (0 != gReadCntlFailedCode) {
        brpc::Controller* cntl = dynamic_cast<brpc::Controller*>(controller);
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

    IOSenderOption ioSenderOpt;
    ioSenderOpt.failRequestOpt.chunkserverRPCTimeoutMS = 5000;
    ioSenderOpt.failRequestOpt.chunkserverOPMaxRetry = 3;
    ioSenderOpt.failRequestOpt.chunkserverOPRetryIntervalUS = 500;
    ioSenderOpt.chunkserverEnableAppliedIndexRead = 1;

    CopysetClient copysetClient;
    MockMetaCache mockMetaCache;
    mockMetaCache.DelegateToFake();

    RequestScheduler scheduler;
    copysetClient.Init(&mockMetaCache, ioSenderOpt, &scheduler, nullptr);

    LogicPoolID logicPoolId = 1;
    CopysetID copysetId = 100001;
    ChunkID chunkId = 1;
    uint64_t fileId = 1;
    uint64_t epoch = 1;
    uint64_t sn = 1;
    size_t len = 8;
    char buff1[8 + 1];
    char buff2[8 + 1];
    memset(buff1, 'a', 8);
    memset(buff2, 'a', 8);
    buff1[8] = '\0';
    buff2[8] = '\0';
    off_t offset = 0;

    butil::IOBuf iobuf;
    iobuf.append(buff1, sizeof(buff1) - 1);

    ChunkServerID leaderId = 10000;
    butil::EndPoint leaderAddr;
    std::string leaderStr = "127.0.0.1:9109";
    butil::str2endpoint(leaderStr.c_str(), &leaderAddr);

    FileMetric fm("test");
    IOTracker iot(nullptr, nullptr, nullptr, &fm);
    iot.PrepareReadIOBuffers(1);

    // write success
    for (int i = 0; i < 10; ++i) {
        RequestContext *reqCtx = new FakeRequestContext();
        reqCtx->optype_ = OpType::WRITE;
        reqCtx->idinfo_ = ChunkIDInfo(chunkId, logicPoolId, copysetId);

        reqCtx->writeData_ = iobuf;

        reqCtx->offset_ = i * 8;
        reqCtx->rawlength_ = len;

        reqCtx->fileId_ = fileId;
        reqCtx->epoch_ = epoch;
        reqCtx->seq_ = 0;

        curve::common::CountDownEvent cond(1);
        RequestClosure *reqDone = new FakeRequestClosure(&cond, reqCtx);
        reqDone->SetFileMetric(&fm);
        reqDone->SetIOTracker(&iot);
        reqCtx->done_ = reqDone;

        ChunkResponse response;
        response.set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS);
        EXPECT_CALL(mockMetaCache, GetLeader(_, _, _, _, _, _)).
                                            Times(AtLeast(1))
            .WillOnce(DoAll(SetArgPointee<2>(leaderId),
                            SetArgPointee<3>(leaderAddr),
                            Return(0)));
        EXPECT_CALL(mockChunkService, WriteChunk(_, _, _, _)).Times(1)
            .WillOnce(DoAll(SetArgPointee<2>(response),
                            Invoke(WriteChunkFunc)));
        copysetClient.WriteChunk(reqCtx, reqDone);
        cond.Wait();
        ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS,
                  reqDone->GetErrorCode());
    }
    {
        RequestContext *reqCtx = new FakeRequestContext();
        reqCtx->optype_ = OpType::WRITE;
        reqCtx->idinfo_ = ChunkIDInfo(chunkId, logicPoolId, copysetId);

        reqCtx->writeData_ = iobuf;
        reqCtx->offset_ = offset;
        reqCtx->rawlength_ = len;
        
        reqCtx->fileId_ = fileId;
        reqCtx->epoch_ = epoch;
        reqCtx->seq_ = 0;

        curve::common::CountDownEvent cond(1);
        RequestClosure *reqDone = new FakeRequestClosure(&cond, reqCtx);
        reqDone->SetFileMetric(&fm);
        reqDone->SetIOTracker(&iot);

        reqCtx->done_ = reqDone;
        ChunkResponse response;
        response.set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS);
        EXPECT_CALL(mockMetaCache, GetLeader(_, _, _, _, _, _)).
                                            Times(AtLeast(1))
            .WillOnce(Return(-1))
            .WillOnce(DoAll(SetArgPointee<2>(leaderId),
                            SetArgPointee<3>(leaderAddr),
                            Return(0)));
        EXPECT_CALL(mockChunkService, WriteChunk(_, _, _, _)).Times(1)
            .WillOnce(DoAll(SetArgPointee<2>(response),
                            Invoke(WriteChunkFunc)));
        copysetClient.WriteChunk(reqCtx, reqDone);
        cond.Wait();
        ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS,
                  reqDone->GetErrorCode());
    }
    {
        RequestContext *reqCtx = new FakeRequestContext();
        reqCtx->optype_ = OpType::WRITE;
        reqCtx->idinfo_ = ChunkIDInfo(chunkId, logicPoolId, copysetId);

        reqCtx->writeData_ = iobuf;
        reqCtx->offset_ = offset;
        reqCtx->rawlength_ = len;

        reqCtx->fileId_ = fileId;
        reqCtx->epoch_ = epoch;
        reqCtx->seq_ = 0;

        curve::common::CountDownEvent cond(1);
        RequestClosure *reqDone = new FakeRequestClosure(&cond, reqCtx);
        reqDone->SetFileMetric(&fm);
        reqDone->SetIOTracker(&iot);

        reqCtx->done_ = reqDone;
        ChunkResponse response;
        response.set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS);
        EXPECT_CALL(mockMetaCache, GetLeader(_, _, _, _, _, _)).
                                            Times(AtLeast(1))
            .WillOnce(Return(-1))
            .WillOnce(Return(-1))
            .WillOnce(DoAll(SetArgPointee<2>(leaderId),
                            SetArgPointee<3>(leaderAddr),
                            Return(0)));
        EXPECT_CALL(mockChunkService, WriteChunk(_, _, _, _)).Times(1)
            .WillOnce(DoAll(SetArgPointee<2>(response),
                            Invoke(WriteChunkFunc)));
        copysetClient.WriteChunk(reqCtx, reqDone);
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
        reqCtx->offset_ = i * 8;
        reqCtx->rawlength_ = len;
        reqCtx->subIoIndex_ = 0;

        curve::common::CountDownEvent cond(1);
        RequestClosure *reqDone = new FakeRequestClosure(&cond, reqCtx);
        reqDone->SetFileMetric(&fm);
        reqDone->SetIOTracker(&iot);

        reqCtx->done_ = reqDone;
        ChunkResponse response;
        response.set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS);
        EXPECT_CALL(mockMetaCache, GetLeader(_, _, _, _, _, _)).
                                            Times(AtLeast(1))
            .WillOnce(DoAll(SetArgPointee<2>(leaderId),
                            SetArgPointee<3>(leaderAddr),
                            Return(0)));
        EXPECT_CALL(mockChunkService, ReadChunk(_, _, _, _)).Times(1)
            .WillOnce(DoAll(SetArgPointee<2>(response),
                            Invoke(ReadChunkFunc)));
        copysetClient.ReadChunk(reqCtx, reqDone);
        cond.Wait();
        ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS,
                  reqDone->GetErrorCode());
    }
    {
        RequestContext *reqCtx = new FakeRequestContext();
        reqCtx->optype_ = OpType::READ;
        reqCtx->idinfo_ = ChunkIDInfo(chunkId, logicPoolId, copysetId);

        reqCtx->subIoIndex_ = 0;
        reqCtx->offset_ = offset;
        reqCtx->rawlength_ = len;

        curve::common::CountDownEvent cond(1);
        RequestClosure *reqDone = new FakeRequestClosure(&cond, reqCtx);
        reqDone->SetFileMetric(&fm);
        reqDone->SetIOTracker(&iot);

        reqCtx->done_ = reqDone;
        ChunkResponse response;
        response.set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS);
        EXPECT_CALL(mockMetaCache, GetLeader(_, _, _, _, _, _)).
                                            Times(AtLeast(1))
            .WillOnce(Return(-1))
            .WillOnce(DoAll(SetArgPointee<2>(leaderId),
                            SetArgPointee<3>(leaderAddr),
                            Return(0)));
        EXPECT_CALL(mockChunkService, ReadChunk(_, _, _, _)).Times(1)
            .WillOnce(DoAll(SetArgPointee<2>(response),
                            Invoke(ReadChunkFunc)));
        copysetClient.ReadChunk(reqCtx, reqDone);
        cond.Wait();
        ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS,
                  reqDone->GetErrorCode());
    }
    {
        RequestContext *reqCtx = new FakeRequestContext();
        reqCtx->optype_ = OpType::READ;
        reqCtx->idinfo_ = ChunkIDInfo(chunkId, logicPoolId, copysetId);

        reqCtx->subIoIndex_ = 0;
        reqCtx->offset_ = offset;
        reqCtx->rawlength_ = len;

        curve::common::CountDownEvent cond(1);
        RequestClosure *reqDone = new FakeRequestClosure(&cond, reqCtx);
        reqDone->SetFileMetric(&fm);
        reqDone->SetIOTracker(&iot);

        reqCtx->done_ = reqDone;
        ChunkResponse response;
        response.set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS);
        EXPECT_CALL(mockMetaCache, GetLeader(_, _, _, _, _, _)).
                                            Times(AtLeast(1))
            .WillOnce(Return(-1))
            .WillOnce(Return(-1))
            .WillOnce(DoAll(SetArgPointee<2>(leaderId),
                            SetArgPointee<3>(leaderAddr),
                            Return(0)));
        EXPECT_CALL(mockChunkService, ReadChunk(_, _, _, _)).Times(1)
            .WillOnce(DoAll(SetArgPointee<2>(response),
                            Invoke(ReadChunkFunc)));
        copysetClient.ReadChunk(reqCtx, reqDone);
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

    IOSenderOption ioSenderOpt;
    ioSenderOpt.failRequestOpt.chunkserverRPCTimeoutMS = 1000;
    ioSenderOpt.failRequestOpt.chunkserverOPMaxRetry = 3;
    ioSenderOpt.failRequestOpt.chunkserverOPRetryIntervalUS = 5000;
    ioSenderOpt.failRequestOpt.chunkserverMaxRPCTimeoutMS = 3500;
    ioSenderOpt.failRequestOpt.chunkserverMaxRetrySleepIntervalUS = 3500000;
    ioSenderOpt.chunkserverEnableAppliedIndexRead = 1;

    RequestScheduleOption reqopt;
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
    uint64_t fileId = 1;
    uint64_t epoch = 1;
    size_t len = 8;
    char buff1[8 + 1];
    char buff2[8 + 1];
    memset(buff1, 'a', 8);
    memset(buff2, 'a', 8);
    buff1[8] = '\0';
    buff2[8] = '\0';
    off_t offset = 0;

    butil::IOBuf iobuf;
    iobuf.append(buff1, sizeof(buff1) - 1);

    ChunkServerID leaderId = 10000;
    butil::EndPoint leaderAddr;
    std::string leaderStr = "127.0.0.1:9109";
    butil::str2endpoint(leaderStr.c_str(), &leaderAddr);

    FileMetric fm("test");
    IOTracker iot(nullptr, nullptr, nullptr, &fm);

    /* 非法参数 */
    {
        RequestContext *reqCtx = new FakeRequestContext();
        reqCtx->optype_ = OpType::WRITE;
        reqCtx->idinfo_ = ChunkIDInfo(chunkId, logicPoolId, copysetId);

        reqCtx->writeData_ = iobuf;
        reqCtx->offset_ = offset;
        reqCtx->rawlength_ = len;

        reqCtx->fileId_ = fileId;
        reqCtx->epoch_ = epoch;
        reqCtx->seq_ = 0;

        curve::common::CountDownEvent cond(1);
        RequestClosure *reqDone = new FakeRequestClosure(&cond, reqCtx);

        reqDone->SetFileMetric(&fm);
        reqDone->SetIOTracker(&iot);

        reqCtx->done_ = reqDone;
        ChunkResponse response;
        response.set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_INVALID_REQUEST);
        EXPECT_CALL(mockMetaCache, GetLeader(_, _, _, _, _, _))
            .Times(AtLeast(1)).WillOnce(DoAll(SetArgPointee<2>(leaderId),
                                              SetArgPointee<3>(leaderAddr),
                                              Return(0)));
        EXPECT_CALL(mockChunkService, WriteChunk(_, _, _, _)).Times(1)
            .WillOnce(DoAll(SetArgPointee<2>(response),
                            Invoke(WriteChunkFunc)));
        copysetClient.WriteChunk(reqCtx, reqDone);
        cond.Wait();
        ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_INVALID_REQUEST,
                  reqDone->GetErrorCode());
    }
    /* controller error */
    {
        RequestContext *reqCtx = new FakeRequestContext();
        reqCtx->optype_ = OpType::WRITE;
        reqCtx->idinfo_ = ChunkIDInfo(chunkId, logicPoolId, copysetId);

        reqCtx->writeData_ = iobuf;
        reqCtx->offset_ = offset;
        reqCtx->rawlength_ = len;

        reqCtx->fileId_ = fileId;
        reqCtx->epoch_ = epoch;
        reqCtx->seq_ = 0;

        // 配置文件设置的重试睡眠时间为5000，因为没有触发底层指数退避，所以重试之间不会睡眠
        uint64_t start = TimeUtility::GetTimeofDayUs();

        curve::common::CountDownEvent cond(1);
        RequestClosure *reqDone = new FakeRequestClosure(&cond, reqCtx);
        reqDone->SetFileMetric(&fm);
        reqDone->SetIOTracker(&iot);

        reqCtx->done_ = reqDone;
        gWriteCntlFailedCode = -1;
        EXPECT_CALL(mockMetaCache, GetLeader(_, _, _, _, _, _)).Times(6)
            .WillRepeatedly(DoAll(SetArgPointee<2>(leaderId),
                                  SetArgPointee<3>(leaderAddr),
                                  Return(0)));
        EXPECT_CALL(mockChunkService, WriteChunk(_, _, _, _)).Times(3)
            .WillRepeatedly(Invoke(WriteChunkFunc));
        copysetClient.WriteChunk(reqCtx, reqDone);
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

        reqCtx->writeData_ = iobuf;
        reqCtx->offset_ = offset;
        reqCtx->rawlength_ = len;

        reqCtx->fileId_ = fileId;
        reqCtx->epoch_ = epoch;
        reqCtx->seq_ = 0;

        curve::common::CountDownEvent cond(1);
        RequestClosure *reqDone = new FakeRequestClosure(&cond, reqCtx);
        reqDone->SetFileMetric(&fm);
        reqDone->SetIOTracker(&iot);

        // 配置文件设置的重试超时时间为5000，因为chunkserver设置返回timeout
        // 导致触发底层超时时间指数退避，每次重试间隔增大。重试三次正常只需要睡眠3*1000
        // 但是增加指数退避之后，超时时间将增加到1000 + 2000 + 2000 = 5000
        // 加上随机因子，三次重试时间应该大于7000, 且小于8000
        uint64_t start = TimeUtility::GetTimeofDayMs();

        reqCtx->done_ = reqDone;
        gWriteCntlFailedCode = brpc::ERPCTIMEDOUT;
        EXPECT_CALL(mockMetaCache, GetLeader(_, _, _, _, _, _))
            .Times(AtLeast(3))
            .WillRepeatedly(DoAll(SetArgPointee<2>(leaderId),
                                  SetArgPointee<3>(leaderAddr),
                                  Return(0)));
        EXPECT_CALL(mockChunkService, WriteChunk(_, _, _, _)).Times(3)
            .WillRepeatedly(Invoke(WriteChunkFunc));
        copysetClient.WriteChunk(reqCtx, reqDone);
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

        reqCtx->writeData_ = iobuf;
        reqCtx->offset_ = offset;
        reqCtx->rawlength_ = len;

        reqCtx->fileId_ = fileId;
        reqCtx->epoch_ = epoch;
        reqCtx->seq_ = 0;

        curve::common::CountDownEvent cond(1);
        RequestClosure *reqDone = new FakeRequestClosure(&cond, reqCtx);
        reqDone->SetFileMetric(&fm);
        reqDone->SetIOTracker(&iot);

        // 配置文件设置的重试睡眠时间为5000，因为chunkserver设置返回timeout
        // 导致触发底层指数退避，每次重试间隔增大。重试三次正常只需要睡眠3*5000
        // 但是增加指数退避之后，睡眠间隔将增加到10000 + 20000 = 30000
        // 加上随机因子，三次重试时间应该大于29000, 且小于50000
        uint64_t start = TimeUtility::GetTimeofDayUs();

        reqCtx->done_ = reqDone;
        ChunkResponse response;
        response.set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_OVERLOAD);
        EXPECT_CALL(mockMetaCache, GetLeader(_, _, _, _, _, _))
            .Times(3).WillRepeatedly(DoAll(SetArgPointee<2>(leaderId),
                                           SetArgPointee<3>(leaderAddr),
                                           Return(0)));
        EXPECT_CALL(mockChunkService, WriteChunk(_, _, _, _)).Times(3)
            .WillRepeatedly(DoAll(SetArgPointee<2>(response),
                                  Invoke(WriteChunkFunc)));
        copysetClient.WriteChunk(reqCtx, reqDone);
        cond.Wait();
        ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_OVERLOAD,
                 reqDone->GetErrorCode());

        uint64_t end = TimeUtility::GetTimeofDayUs();
        ASSERT_GT(end - start, 28000);
        ASSERT_LT(end - start, 2 * 50000);
        gWriteCntlFailedCode = 0;
    }

    /* 其他错误 */
    {
        RequestContext *reqCtx = new FakeRequestContext();
        reqCtx->optype_ = OpType::WRITE;
        reqCtx->idinfo_ = ChunkIDInfo(chunkId, logicPoolId, copysetId);

        reqCtx->writeData_ = iobuf;
        reqCtx->offset_ = offset;
        reqCtx->rawlength_ = len;

        reqCtx->fileId_ = fileId;
        reqCtx->epoch_ = epoch;
        reqCtx->seq_ = 0;

        curve::common::CountDownEvent cond(1);
        RequestClosure *reqDone = new FakeRequestClosure(&cond, reqCtx);
        reqDone->SetFileMetric(&fm);
        reqDone->SetIOTracker(&iot);

        reqCtx->done_ = reqDone;
        ChunkResponse response;
        response.set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_FAILURE_UNKNOWN);
        EXPECT_CALL(mockMetaCache, GetLeader(_, _, _, _, _, _))
            .Times(3).WillRepeatedly(DoAll(SetArgPointee<2>(leaderId),
                                           SetArgPointee<3>(leaderAddr),
                                           Return(0)));
        EXPECT_CALL(mockChunkService, WriteChunk(_, _, _, _)).Times(3)
            .WillRepeatedly(DoAll(SetArgPointee<2>(response),
                                  Invoke(WriteChunkFunc)));
        copysetClient.WriteChunk(reqCtx, reqDone);
        cond.Wait();
        ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_FAILURE_UNKNOWN,
                 reqDone->GetErrorCode());
    }
    /* 不是 leader，返回正确的 leader */
    {
        RequestContext *reqCtx = new FakeRequestContext();
        reqCtx->optype_ = OpType::WRITE;
        reqCtx->idinfo_ = ChunkIDInfo(chunkId, logicPoolId, copysetId);

        reqCtx->writeData_ = iobuf;
        reqCtx->offset_ = offset;
        reqCtx->rawlength_ = len;

        reqCtx->fileId_ = fileId;
        reqCtx->epoch_ = epoch;
        reqCtx->seq_ = 0;

        curve::common::CountDownEvent cond(1);
        RequestClosure *reqDone = new FakeRequestClosure(&cond, reqCtx);
        reqDone->SetFileMetric(&fm);
        reqDone->SetIOTracker(&iot);

        reqCtx->done_ = reqDone;
        ChunkResponse response1;
        response1.set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_REDIRECTED);
        response1.set_redirect(leaderStr);
        ChunkResponse response2;
        response2.set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS);
        EXPECT_CALL(mockMetaCache, GetLeader(_, _, _, _, _, _)).Times(3)
            .WillRepeatedly(DoAll(SetArgPointee<2>(leaderId),
                            SetArgPointee<3>(leaderAddr),
                            Return(0)));
        EXPECT_CALL(mockMetaCache, UpdateLeader(_, _, _)).Times(1)
            .WillOnce(Return(0));
        EXPECT_CALL(mockChunkService, WriteChunk(_, _, _, _)).Times(2)
            .WillOnce(DoAll(SetArgPointee<2>(response1),
                            Invoke(WriteChunkFunc)))
            .WillOnce(DoAll(SetArgPointee<2>(response2),
                            Invoke(WriteChunkFunc)));
        copysetClient.WriteChunk(reqCtx, reqDone);
        cond.Wait();
        ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS,
                  reqDone->GetErrorCode());

        ASSERT_EQ(1, fm.writeRPC.redirectQps.count.get_value());
    }
    /* 不是 leader，没有返回 leader，刷新 meta cache 成功 */
    {
        RequestContext *reqCtx = new FakeRequestContext();
        reqCtx->optype_ = OpType::WRITE;
        reqCtx->idinfo_ = ChunkIDInfo(chunkId, logicPoolId, copysetId);

        reqCtx->writeData_ = iobuf;
        reqCtx->offset_ = offset;
        reqCtx->rawlength_ = len;

        reqCtx->fileId_ = fileId;
        reqCtx->epoch_ = epoch;
        reqCtx->seq_ = 0;

        curve::common::CountDownEvent cond(1);
        RequestClosure *reqDone = new FakeRequestClosure(&cond, reqCtx);
        reqDone->SetFileMetric(&fm);
        reqDone->SetIOTracker(&iot);

        reqCtx->done_ = reqDone;
        ChunkResponse response1;
        response1.set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_REDIRECTED);
//        response1.set_redirect(leaderStr2);
        ChunkResponse response2;
        response2.set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS);
        EXPECT_CALL(mockMetaCache, GetLeader(_, _, _, _, _, _)).Times(3)
            .WillRepeatedly(DoAll(SetArgPointee<2>(leaderId),
                            SetArgPointee<3>(leaderAddr),
                            Return(0)));
        EXPECT_CALL(mockChunkService, WriteChunk(_, _, _, _)).Times(2)
            .WillOnce(DoAll(SetArgPointee<2>(response1),
                            Invoke(WriteChunkFunc)))
            .WillOnce(DoAll(SetArgPointee<2>(response2),
                            Invoke(WriteChunkFunc)));

        copysetClient.WriteChunk(reqCtx, reqDone);
        cond.Wait();
        ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS,
                  reqDone->GetErrorCode());
    }
    /* 不是 leader，没有返回 leader，刷新 meta cache 失败 */
    {
        RequestContext *reqCtx = new FakeRequestContext();
        reqCtx->optype_ = OpType::WRITE;
        reqCtx->idinfo_ = ChunkIDInfo(chunkId, logicPoolId, copysetId);

        reqCtx->writeData_ = iobuf;
        reqCtx->offset_ = offset;
        reqCtx->rawlength_ = len;

        reqCtx->fileId_ = fileId;
        reqCtx->epoch_ = epoch;
        reqCtx->seq_ = 0;

        curve::common::CountDownEvent cond(1);
        RequestClosure *reqDone = new FakeRequestClosure(&cond, reqCtx);
        reqDone->SetFileMetric(&fm);
        reqDone->SetIOTracker(&iot);

        reqCtx->done_ = reqDone;
        ChunkResponse response1;
        response1.set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_REDIRECTED);
//        response1.set_redirect(leaderStr2);
        ChunkResponse response2;
        response2.set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS);
        EXPECT_CALL(mockMetaCache, GetLeader(_, _, _, _, _, _)).Times(3)
            .WillOnce(DoAll(SetArgPointee<2>(leaderId),
                            SetArgPointee<3>(leaderAddr),
                            Return(0)))
            .WillOnce(Return(-1))
            .WillOnce(DoAll(SetArgPointee<2>(leaderId),
                            SetArgPointee<3>(leaderAddr),
                            Return(0)));
        EXPECT_CALL(mockChunkService, WriteChunk(_, _, _, _)).Times(2)
            .WillOnce(DoAll(SetArgPointee<2>(response1),
                            Invoke(WriteChunkFunc)))
            .WillOnce(DoAll(SetArgPointee<2>(response2),
                            Invoke(WriteChunkFunc)));
        copysetClient.WriteChunk(reqCtx, reqDone);
        cond.Wait();
        ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS,
                  reqDone->GetErrorCode());
    }
    /* 不是 leader，但返回的是错误 leader */
    {
        RequestContext *reqCtx = new FakeRequestContext();
        reqCtx->optype_ = OpType::WRITE;
        reqCtx->idinfo_ = ChunkIDInfo(chunkId, logicPoolId, copysetId);

        reqCtx->writeData_ = iobuf;
        reqCtx->offset_ = offset;
        reqCtx->rawlength_ = len;

        reqCtx->fileId_ = fileId;
        reqCtx->epoch_ = epoch;
        reqCtx->seq_ = 0;

        curve::common::CountDownEvent cond(1);
        RequestClosure *reqDone = new FakeRequestClosure(&cond, reqCtx);
        FileMetric fm("test");
        reqDone->SetFileMetric(&fm);
        reqDone->SetIOTracker(&iot);

        reqCtx->done_ = reqDone;
        ChunkResponse response;
        response.set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_REDIRECTED);
        response.set_redirect(leaderStr);
        EXPECT_CALL(mockMetaCache, GetLeader(_, _, _, _, _, _)).Times(6)
            .WillRepeatedly(DoAll(SetArgPointee<2>(leaderId),
                            SetArgPointee<3>(leaderAddr),
                            Return(0)));
        EXPECT_CALL(mockMetaCache, UpdateLeader(_, _, _))
            .Times(3)
            .WillRepeatedly(Return(0));
        EXPECT_CALL(mockChunkService, WriteChunk(_, _, _, _)).Times(3)
            .WillRepeatedly(DoAll(SetArgPointee<2>(response),
                            Invoke(WriteChunkFunc)));
        auto startTimeUs = curve::common::TimeUtility::GetTimeofDayUs();
        copysetClient.WriteChunk(reqCtx, reqDone);
        cond.Wait();
        auto elpased = curve::common::TimeUtility::GetTimeofDayUs()
                     - startTimeUs;
        // chunkserverOPRetryIntervalUS = 5000
        // 每次redirect睡眠500us，共重试2次(chunkserverOPMaxRetry=3，判断时大于等于就返回，所以共只重试了两次)
        // 所以总共耗费时间大于1000us
        ASSERT_GE(elpased, 1000);
        ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_REDIRECTED,
                  reqDone->GetErrorCode());
        ASSERT_EQ(3, fm.writeRPC.redirectQps.count.get_value());
    }
    /* copyset 不存在，更新 leader 依然失败 */
    {
        RequestContext *reqCtx = new FakeRequestContext();
        reqCtx->optype_ = OpType::WRITE;
        reqCtx->idinfo_ = ChunkIDInfo(chunkId, logicPoolId, copysetId);

        reqCtx->writeData_ = iobuf;
        reqCtx->offset_ = offset;
        reqCtx->rawlength_ = len;

        reqCtx->fileId_ = fileId;
        reqCtx->epoch_ = epoch;
        reqCtx->seq_ = 0;

        curve::common::CountDownEvent cond(1);
        RequestClosure *reqDone = new FakeRequestClosure(&cond, reqCtx);
        reqDone->SetFileMetric(&fm);
        reqDone->SetIOTracker(&iot);

        reqCtx->done_ = reqDone;
        ChunkResponse response;
        response.set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_COPYSET_NOTEXIST);
        response.set_redirect(leaderStr);
        EXPECT_CALL(mockMetaCache, GetLeader(_, _, _, _, _, _)).Times(6)
            .WillRepeatedly(DoAll(SetArgPointee<2>(leaderId),
                            SetArgPointee<3>(leaderAddr),
                            Return(0)));
        EXPECT_CALL(mockChunkService, WriteChunk(_, _, _, _)).Times(3)
            .WillRepeatedly(DoAll(SetArgPointee<2>(response),
                            Invoke(WriteChunkFunc)));
        copysetClient.WriteChunk(reqCtx, reqDone);
        cond.Wait();
        ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_COPYSET_NOTEXIST,
                  reqDone->GetErrorCode());
    }
    /* copyset 不存在，更新 leader 成功 */
    {
        RequestContext *reqCtx = new FakeRequestContext();
        reqCtx->optype_ = OpType::WRITE;
        reqCtx->idinfo_ = ChunkIDInfo(chunkId, logicPoolId, copysetId);

        reqCtx->writeData_ = iobuf;
        reqCtx->offset_ = offset;
        reqCtx->rawlength_ = len;

        reqCtx->fileId_ = fileId;
        reqCtx->epoch_ = epoch;
        reqCtx->seq_ = 0;

        curve::common::CountDownEvent cond(1);
        RequestClosure *reqDone = new FakeRequestClosure(&cond, reqCtx);
        reqDone->SetFileMetric(&fm);
        reqDone->SetIOTracker(&iot);

        reqCtx->done_ = reqDone;
        ChunkResponse response1;
        response1.set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_COPYSET_NOTEXIST);
        response1.set_redirect(leaderStr);
        ChunkResponse response2;
        response2.set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS);
        response2.set_redirect(leaderStr);
        EXPECT_CALL(mockMetaCache, GetLeader(_, _, _, _, _, _)).Times(3)
            .WillRepeatedly(DoAll(SetArgPointee<2>(leaderId),
                            SetArgPointee<3>(leaderAddr),
                            Return(0)));
        EXPECT_CALL(mockChunkService, WriteChunk(_, _, _, _)).Times(2)
            .WillOnce(DoAll(SetArgPointee<2>(response1),
                            Invoke(WriteChunkFunc)))
            .WillOnce(DoAll(SetArgPointee<2>(response2),
                            Invoke(WriteChunkFunc)));
        copysetClient.WriteChunk(reqCtx, reqDone);
        cond.Wait();
        ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS,
                  reqDone->GetErrorCode());
    }
    // epoch too old
    {
        RequestContext *reqCtx = new FakeRequestContext();
        reqCtx->optype_ = OpType::WRITE;
        reqCtx->idinfo_ = ChunkIDInfo(chunkId, logicPoolId, copysetId);

        reqCtx->writeData_ = iobuf;
        reqCtx->offset_ = offset;
        reqCtx->rawlength_ = len;

        reqCtx->fileId_ = fileId;
        reqCtx->epoch_ = epoch;
        reqCtx->seq_ = 0;

        curve::common::CountDownEvent cond(1);
        RequestClosure *reqDone = new FakeRequestClosure(&cond, reqCtx);
        reqDone->SetFileMetric(&fm);
        reqDone->SetIOTracker(&iot);

        reqCtx->done_ = reqDone;
        ChunkResponse response;
        response.set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_EPOCH_TOO_OLD);
        EXPECT_CALL(mockMetaCache, GetLeader(_, _, _, _, _, _))
            .Times(AtLeast(1)).WillOnce(DoAll(SetArgPointee<2>(leaderId),
                                              SetArgPointee<3>(leaderAddr),
                                              Return(0)));
        EXPECT_CALL(mockChunkService, WriteChunk(_, _, _, _)).Times(1)
            .WillRepeatedly(DoAll(SetArgPointee<2>(response),
                                  Invoke(WriteChunkFunc)));
        copysetClient.WriteChunk(reqCtx, reqDone);
        cond.Wait();
        ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_EPOCH_TOO_OLD,
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

    IOSenderOption ioSenderOpt;
    ioSenderOpt.failRequestOpt.chunkserverRPCTimeoutMS = 500;
    ioSenderOpt.failRequestOpt.chunkserverOPMaxRetry = 50;
    ioSenderOpt.failRequestOpt.chunkserverOPRetryIntervalUS = 5000;
    ioSenderOpt.failRequestOpt.chunkserverMaxRPCTimeoutMS = 1000;
    ioSenderOpt.failRequestOpt.chunkserverMaxRetrySleepIntervalUS = 100000;
    ioSenderOpt.chunkserverEnableAppliedIndexRead = 1;

    RequestScheduleOption reqopt;
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
    uint64_t fileId = 1;
    uint64_t epoch = 1;
    size_t len = 8;
    char buff1[8 + 1];
    char buff2[8 + 1];
    memset(buff1, 'a', 8);
    memset(buff2, 'a', 8);
    buff1[8] = '\0';
    buff2[8] = '\0';
    off_t offset = 0;
    butil::IOBuf iobuf;
    iobuf.append(buff1, sizeof(buff1) - 1);

    ChunkServerID leaderId = 10000;
    butil::EndPoint leaderAddr;
    std::string leaderStr = "127.0.0.1:9109";
    butil::str2endpoint(leaderStr.c_str(), &leaderAddr);

    FileMetric fm("test");
    IOTracker iot(nullptr, nullptr, nullptr, &fm);

    /* controller set timeout */
    {
        RequestContext *reqCtx = new FakeRequestContext();
        reqCtx->optype_ = OpType::WRITE;
        reqCtx->idinfo_ = ChunkIDInfo(chunkId, logicPoolId, copysetId);

        reqCtx->writeData_ = iobuf;
        reqCtx->offset_ = offset;
        reqCtx->rawlength_ = len;

        reqCtx->fileId_ = fileId;
        reqCtx->epoch_ = epoch;
        reqCtx->seq_ = 0;

        curve::common::CountDownEvent cond(1);
        RequestClosure *reqDone = new FakeRequestClosure(&cond, reqCtx);
        reqDone->SetFileMetric(&fm);
        reqDone->SetIOTracker(&iot);

        // 配置文件设置的重试超时时间为500，因为chunkserver设置返回timeout
        // 导致触发底层超时时间指数退避，每次重试间隔增大。重试50次正常只需要超时49*500
        // 但是增加指数退避之后，超时时间将增加到49*1000 = 49000
        uint64_t start = TimeUtility::GetTimeofDayMs();

        reqCtx->done_ = reqDone;
        gWriteCntlFailedCode = brpc::ERPCTIMEDOUT;
        EXPECT_CALL(mockMetaCache, GetLeader(_, _, _, _, _, _))
            .Times(AtLeast(50))
            .WillRepeatedly(DoAll(SetArgPointee<2>(leaderId),
                                  SetArgPointee<3>(leaderAddr),
                                  Return(0)));
        EXPECT_CALL(mockChunkService, WriteChunk(_, _, _, _)).Times(50)
            .WillRepeatedly(Invoke(WriteChunkFunc));
        copysetClient.WriteChunk(reqCtx, reqDone);
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

        reqCtx->writeData_ = iobuf;
        reqCtx->offset_ = offset;
        reqCtx->rawlength_ = len;

        reqCtx->fileId_ = fileId;
        reqCtx->epoch_ = epoch;
        reqCtx->seq_ = 0;

        curve::common::CountDownEvent cond(1);
        RequestClosure *reqDone = new FakeRequestClosure(&cond, reqCtx);
        reqDone->SetFileMetric(&fm);
        reqDone->SetIOTracker(&iot);

        // 配置文件设置的重试睡眠时间为5000us，因为chunkserver设置返回timeout
        // 导致触发底层指数退避，每次重试间隔增大。重试50次正常只需要睡眠49*5000us
        // 但是增加指数退避之后，睡眠间隔将增加到
        // 10000 + 20000 + 40000... ~= 4650000
        uint64_t start = TimeUtility::GetTimeofDayUs();

        reqCtx->done_ = reqDone;
        ChunkResponse response;
        response.set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_OVERLOAD);
        EXPECT_CALL(mockMetaCache, GetLeader(_, _, _, _, _, _))
            .Times(50).WillRepeatedly(DoAll(SetArgPointee<2>(leaderId),
                                           SetArgPointee<3>(leaderAddr),
                                           Return(0)));
        EXPECT_CALL(mockChunkService, WriteChunk(_, _, _, _)).Times(50)
            .WillRepeatedly(DoAll(SetArgPointee<2>(response),
                                  Invoke(WriteChunkFunc)));
        copysetClient.WriteChunk(reqCtx, reqDone);
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

    IOSenderOption ioSenderOpt;
    ioSenderOpt.failRequestOpt.chunkserverRPCTimeoutMS = 500;
    ioSenderOpt.failRequestOpt.chunkserverOPMaxRetry = 50;
    ioSenderOpt.failRequestOpt.chunkserverOPRetryIntervalUS = 5000;
    ioSenderOpt.failRequestOpt.chunkserverMaxRPCTimeoutMS = 1000;
    ioSenderOpt.failRequestOpt.chunkserverMaxRetrySleepIntervalUS = 100000;
    ioSenderOpt.chunkserverEnableAppliedIndexRead = 1;

    RequestScheduleOption reqopt;
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

    ChunkServerID leaderId = 10000;
    butil::EndPoint leaderAddr;
    std::string leaderStr = "127.0.0.1:9109";
    butil::str2endpoint(leaderStr.c_str(), &leaderAddr);

    FileMetric fm("test");
    IOTracker iot(nullptr, nullptr, nullptr, &fm);
    iot.PrepareReadIOBuffers(1);

    /* controller set timeout */
    {
        RequestContext *reqCtx = new FakeRequestContext();
        reqCtx->optype_ = OpType::READ;
        reqCtx->idinfo_ = ChunkIDInfo(chunkId, logicPoolId, copysetId);

        reqCtx->subIoIndex_ = 0;
        reqCtx->offset_ = 0;
        reqCtx->rawlength_ = len;

        // 配置文件设置的重试超时时间为500，因为chunkserver设置返回timeout
        // 导致触发底层超时时间指数退避，每次重试间隔增大。重试50次正常只需要50*500
        // 但是增加指数退避之后，超时时间将增加到500 + 1000 + 2000... ~= 60000
        uint64_t start = TimeUtility::GetTimeofDayMs();

        curve::common::CountDownEvent cond(1);
        RequestClosure *reqDone = new FakeRequestClosure(&cond, reqCtx);
        reqDone->SetFileMetric(&fm);
        reqDone->SetIOTracker(&iot);

        reqCtx->done_ = reqDone;
        gReadCntlFailedCode = brpc::ERPCTIMEDOUT;
        EXPECT_CALL(mockMetaCache, GetLeader(_, _, _, _, _, _))
            .Times(AtLeast(50))
            .WillRepeatedly(DoAll(SetArgPointee<2>(leaderId),
                                  SetArgPointee<3>(leaderAddr),
                                  Return(0)));
        EXPECT_CALL(mockChunkService, ReadChunk(_, _, _, _)).Times(50)
            .WillRepeatedly(Invoke(ReadChunkFunc));
        copysetClient.ReadChunk(reqCtx, reqDone);
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

        reqCtx->subIoIndex_ = 0;
        reqCtx->offset_ = 0;
        reqCtx->rawlength_ = len;

        curve::common::CountDownEvent cond(1);
        RequestClosure *reqDone = new FakeRequestClosure(&cond, reqCtx);
        reqDone->SetFileMetric(&fm);
        reqDone->SetIOTracker(&iot);

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
            .Times(50).WillRepeatedly(DoAll(SetArgPointee<2>(leaderId),
                                           SetArgPointee<3>(leaderAddr),
                                           Return(0)));
        EXPECT_CALL(mockChunkService, ReadChunk(_, _, _, _)).Times(50)
            .WillRepeatedly(DoAll(SetArgPointee<2>(response),
                                  Invoke(ReadChunkFunc)));
        copysetClient.ReadChunk(reqCtx, reqDone);
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

    IOSenderOption ioSenderOpt;
    ioSenderOpt.failRequestOpt.chunkserverRPCTimeoutMS = 1000;
    ioSenderOpt.failRequestOpt.chunkserverOPMaxRetry = 3;
    ioSenderOpt.failRequestOpt.chunkserverOPRetryIntervalUS = 500;
    ioSenderOpt.failRequestOpt.chunkserverMaxRPCTimeoutMS = 3500;
    ioSenderOpt.failRequestOpt.chunkserverMaxRetrySleepIntervalUS = 3500000;
    ioSenderOpt.chunkserverEnableAppliedIndexRead = 1;

    RequestScheduleOption reqopt;
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

    ChunkServerID leaderId = 10000;
    butil::EndPoint leaderAddr;
    std::string leaderStr = "127.0.0.1:9109";
    butil::str2endpoint(leaderStr.c_str(), &leaderAddr);

    FileMetric fm("test");
    IOTracker iot(nullptr, nullptr, nullptr, &fm);
    iot.PrepareReadIOBuffers(1);

    /* 非法参数 */
    {
        RequestContext *reqCtx = new FakeRequestContext();
        reqCtx->optype_ = OpType::READ;
        reqCtx->idinfo_ = ChunkIDInfo(chunkId, logicPoolId, copysetId);

        reqCtx->subIoIndex_ = 0;
        reqCtx->offset_ = 0;
        reqCtx->rawlength_ = len;

        curve::common::CountDownEvent cond(1);
        RequestClosure *reqDone = new FakeRequestClosure(&cond, reqCtx);
        reqDone->SetFileMetric(&fm);
        reqDone->SetIOTracker(&iot);

        reqCtx->done_ = reqDone;
        ChunkResponse response;
        response.set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_INVALID_REQUEST);
        EXPECT_CALL(mockMetaCache, GetLeader(_, _, _, _, _, _))
            .Times(AtLeast(1)).WillOnce(DoAll(SetArgPointee<2>(leaderId),
                                              SetArgPointee<3>(leaderAddr),
                                              Return(0)));
        EXPECT_CALL(mockChunkService, ReadChunk(_, _, _, _)).Times(1)
            .WillOnce(DoAll(SetArgPointee<2>(response),
                            Invoke(ReadChunkFunc)));
        copysetClient.ReadChunk(reqCtx, reqDone);
        cond.Wait();
        ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_INVALID_REQUEST,
                  reqDone->GetErrorCode());
    }
    /* chunk not exist */
    {
        RequestContext *reqCtx = new FakeRequestContext();
        reqCtx->optype_ = OpType::READ;
        reqCtx->idinfo_ = ChunkIDInfo(chunkId, logicPoolId, copysetId);

        reqCtx->subIoIndex_ = 0;
        reqCtx->offset_ = 0;
        reqCtx->rawlength_ = len;

        curve::common::CountDownEvent cond(1);
        RequestClosure *reqDone = new FakeRequestClosure(&cond, reqCtx);
        reqDone->SetFileMetric(&fm);
        reqDone->SetIOTracker(&iot);

        reqCtx->done_ = reqDone;
        ChunkResponse response;
        response.set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_CHUNK_NOTEXIST);
        EXPECT_CALL(mockMetaCache, GetLeader(_, _, _, _, _, _))
            .Times(AtLeast(1)).WillOnce(DoAll(SetArgPointee<2>(leaderId),
                                              SetArgPointee<3>(leaderAddr),
                                              Return(0)));
        EXPECT_CALL(mockChunkService, ReadChunk(_, _, _, _)).Times(1)
            .WillOnce(DoAll(SetArgPointee<2>(response),
                            Invoke(ReadChunkFunc)));
        copysetClient.ReadChunk(reqCtx, reqDone);
        cond.Wait();
        ASSERT_EQ(0, reqDone->GetErrorCode());
    }
    /* controller error */
    {
        RequestContext *reqCtx = new FakeRequestContext();
        reqCtx->optype_ = OpType::READ;
        reqCtx->idinfo_ = ChunkIDInfo(chunkId, logicPoolId, copysetId);

        reqCtx->subIoIndex_ = 0;
        reqCtx->offset_ = 0;
        reqCtx->rawlength_ = len;

        // 配置文件设置的重试睡眠时间为5000，因为没有触发底层指数退避，所以重试之间不会睡眠
        uint64_t start = TimeUtility::GetTimeofDayUs();

        curve::common::CountDownEvent cond(1);
        RequestClosure *reqDone = new FakeRequestClosure(&cond, reqCtx);
        reqDone->SetFileMetric(&fm);
        reqDone->SetIOTracker(&iot);

        reqCtx->done_ = reqDone;
        gReadCntlFailedCode = -1;
        EXPECT_CALL(mockMetaCache, GetLeader(_, _, _, _, _, _))
            .Times(AtLeast(3))
            .WillRepeatedly(DoAll(SetArgPointee<2>(leaderId),
                                  SetArgPointee<3>(leaderAddr),
                                  Return(0)));
        EXPECT_CALL(mockChunkService, ReadChunk(_, _, _, _)).Times(3)
            .WillRepeatedly(Invoke(ReadChunkFunc));
        copysetClient.ReadChunk(reqCtx, reqDone);
        cond.Wait();
        ASSERT_NE(0, reqDone->GetErrorCode());

        uint64_t end = TimeUtility::GetTimeofDayUs();
        ASSERT_GT(end - start, 1000);
        gReadCntlFailedCode = 0;
    }

    /* controller set timeout */
    {
        RequestContext *reqCtx = new FakeRequestContext();
        reqCtx->optype_ = OpType::READ;
        reqCtx->idinfo_ = ChunkIDInfo(chunkId, logicPoolId, copysetId);

        reqCtx->subIoIndex_ = 0;
        reqCtx->offset_ = 0;
        reqCtx->rawlength_ = len;

        // 配置文件设置的超时时间为1000，因为chunkserver设置返回timeout
        // 导致触发底层超时时间指数退避，每次重试间隔增大。重试三次正常只需要睡眠3*1000
        // 但是增加指数退避之后，超时时间将增加到1000 + 2000 + 2000 = 5000
        // 加上随机因子，三次重试时间应该大于7000, 且小于8000
        uint64_t start = TimeUtility::GetTimeofDayMs();

        curve::common::CountDownEvent cond(1);
        RequestClosure *reqDone = new FakeRequestClosure(&cond, reqCtx);
        reqDone->SetFileMetric(&fm);
        reqDone->SetIOTracker(&iot);

        reqCtx->done_ = reqDone;
        gReadCntlFailedCode = brpc::ERPCTIMEDOUT;
        EXPECT_CALL(mockMetaCache, GetLeader(_, _, _, _, _, _))
            .Times(AtLeast(3))
            .WillRepeatedly(DoAll(SetArgPointee<2>(leaderId),
                                  SetArgPointee<3>(leaderAddr),
                                  Return(0)));
        EXPECT_CALL(mockChunkService, ReadChunk(_, _, _, _)).Times(3)
            .WillRepeatedly(Invoke(ReadChunkFunc));
        copysetClient.ReadChunk(reqCtx, reqDone);
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

        reqCtx->subIoIndex_ = 0;
        reqCtx->offset_ = 0;
        reqCtx->rawlength_ = len;

        curve::common::CountDownEvent cond(1);
        RequestClosure *reqDone = new FakeRequestClosure(&cond, reqCtx);
        reqDone->SetFileMetric(&fm);
        reqDone->SetIOTracker(&iot);

        // 配置文件设置的重试睡眠时间为500，因为chunkserver设置返回timeout
        // 导致触发底层指数退避，每次重试间隔增大。重试三次正常只需要睡眠3*500
        // 但是增加指数退避之后，睡眠间隔将增加到1000 + 2000 = 3000
        // 加上随机因子，三次重试时间应该大于2900, 且小于5000
        uint64_t start = TimeUtility::GetTimeofDayUs();

        reqCtx->done_ = reqDone;
        ChunkResponse response;
        response.set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_OVERLOAD);
        EXPECT_CALL(mockMetaCache, GetLeader(_, _, _, _, _, _))
            .Times(3).WillRepeatedly(DoAll(SetArgPointee<2>(leaderId),
                                           SetArgPointee<3>(leaderAddr),
                                           Return(0)));
        EXPECT_CALL(mockChunkService, ReadChunk(_, _, _, _)).Times(3)
            .WillRepeatedly(DoAll(SetArgPointee<2>(response),
                                  Invoke(ReadChunkFunc)));
        copysetClient.ReadChunk(reqCtx, reqDone);
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

        reqCtx->subIoIndex_ = 0;
        reqCtx->offset_ = 0;
        reqCtx->rawlength_ = len;

        curve::common::CountDownEvent cond(1);
        RequestClosure *reqDone = new FakeRequestClosure(&cond, reqCtx);
        reqDone->SetFileMetric(&fm);
        reqDone->SetIOTracker(&iot);

        reqCtx->done_ = reqDone;
        ChunkResponse response;
        response.set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_FAILURE_UNKNOWN);
        EXPECT_CALL(mockMetaCache, GetLeader(_, _, _, _, _, _))
            .Times(3).WillRepeatedly(DoAll(SetArgPointee<2>(leaderId),
                                           SetArgPointee<3>(leaderAddr),
                                           Return(0)));
        EXPECT_CALL(mockChunkService, ReadChunk(_, _, _, _)).Times(3)
            .WillRepeatedly(DoAll(SetArgPointee<2>(response),
                                  Invoke(ReadChunkFunc)));
        copysetClient.ReadChunk(reqCtx, reqDone);
        cond.Wait();
        ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_FAILURE_UNKNOWN,
                  reqDone->GetErrorCode());
    }
    /* 不是 leader，返回正确的 leader */
    {
        RequestContext *reqCtx = new FakeRequestContext();
        reqCtx->optype_ = OpType::READ;
        reqCtx->idinfo_ = ChunkIDInfo(chunkId, logicPoolId, copysetId);

        reqCtx->subIoIndex_ = 0;
        reqCtx->offset_ = 0;
        reqCtx->rawlength_ = len;

        curve::common::CountDownEvent cond(1);
        RequestClosure *reqDone = new FakeRequestClosure(&cond, reqCtx);
        reqDone->SetFileMetric(&fm);
        reqDone->SetIOTracker(&iot);

        reqCtx->done_ = reqDone;
        ChunkResponse response1;
        response1.set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_REDIRECTED);
        response1.set_redirect(leaderStr);
        ChunkResponse response2;
        response2.set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS);
        EXPECT_CALL(mockMetaCache, GetLeader(_, _, _, _, _, _)).Times(3)
            .WillRepeatedly(DoAll(SetArgPointee<2>(leaderId),
                            SetArgPointee<3>(leaderAddr),
                            Return(0)));
        EXPECT_CALL(mockMetaCache, UpdateLeader(_, _, _)).Times(1)
            .WillOnce(Return(0));
        EXPECT_CALL(mockChunkService, ReadChunk(_, _, _, _)).Times(2)
            .WillOnce(DoAll(SetArgPointee<2>(response1),
                            Invoke(ReadChunkFunc)))
            .WillOnce(DoAll(SetArgPointee<2>(response2),
                            Invoke(ReadChunkFunc)));
        copysetClient.ReadChunk(reqCtx, reqDone);
        cond.Wait();
        ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS,
                  reqDone->GetErrorCode());
    }
    /* 不是 leader，但是没有返回 leader，刷新 meta cache 成功 */
    {
        RequestContext *reqCtx = new FakeRequestContext();
        reqCtx->optype_ = OpType::READ;
        reqCtx->idinfo_ = ChunkIDInfo(chunkId, logicPoolId, copysetId);

        reqCtx->subIoIndex_ = 0;
        reqCtx->offset_ = 0;
        reqCtx->rawlength_ = len;

        curve::common::CountDownEvent cond(1);
        RequestClosure *reqDone = new FakeRequestClosure(&cond, reqCtx);
        reqDone->SetFileMetric(&fm);
        reqDone->SetIOTracker(&iot);

        reqCtx->done_ = reqDone;
        ChunkResponse response1;
        response1.set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_REDIRECTED);
//        response1.set_redirect(leaderStr2);
        ChunkResponse response2;
        response2.set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS);
        EXPECT_CALL(mockMetaCache, GetLeader(_, _, _, _, _, _)).Times(3)
            .WillOnce(DoAll(SetArgPointee<2>(leaderId),
                            SetArgPointee<3>(leaderAddr),
                            Return(0)))
            .WillOnce(DoAll(SetArgPointee<2>(leaderId),
                            SetArgPointee<3>(leaderAddr),
                            Return(0)))
            .WillOnce(DoAll(SetArgPointee<2>(leaderId),
                            SetArgPointee<3>(leaderAddr),
                            Return(0)));
        EXPECT_CALL(mockChunkService, ReadChunk(_, _, _, _)).Times(2)
            .WillOnce(DoAll(SetArgPointee<2>(response1),
                            Invoke(ReadChunkFunc)))
            .WillOnce(DoAll(SetArgPointee<2>(response2),
                            Invoke(ReadChunkFunc)));
        copysetClient.ReadChunk(reqCtx, reqDone);
        cond.Wait();
        ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS,
                  reqDone->GetErrorCode());
    }
    /* 不是 leader，但是没有返回 leader，刷新 meta cache 失败 */
    {
        RequestContext *reqCtx = new FakeRequestContext();
        reqCtx->optype_ = OpType::READ;
        reqCtx->idinfo_ = ChunkIDInfo(chunkId, logicPoolId, copysetId);

        reqCtx->subIoIndex_ = 0;
        reqCtx->offset_ = 0;
        reqCtx->rawlength_ = len;

        curve::common::CountDownEvent cond(1);
        RequestClosure *reqDone = new FakeRequestClosure(&cond, reqCtx);
        reqDone->SetFileMetric(&fm);
        reqDone->SetIOTracker(&iot);

        reqCtx->done_ = reqDone;
        ChunkResponse response1;
        response1.set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_REDIRECTED);
//        response1.set_redirect(leaderStr2);
        ChunkResponse response2;
        response2.set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS);
        EXPECT_CALL(mockMetaCache, GetLeader(_, _, _, _, _, _)).Times(3)
            .WillOnce(DoAll(SetArgPointee<2>(leaderId),
                            SetArgPointee<3>(leaderAddr),
                            Return(0)))
            .WillOnce(DoAll(SetArgPointee<2>(leaderId),
                            SetArgPointee<3>(leaderAddr),
                            Return(-1)))
            .WillOnce(DoAll(SetArgPointee<2>(leaderId),
                            SetArgPointee<3>(leaderAddr),
                            Return(0)));
        EXPECT_CALL(mockChunkService, ReadChunk(_, _, _, _)).Times(2)
            .WillOnce(DoAll(SetArgPointee<2>(response1),
                            Invoke(ReadChunkFunc)))
            .WillOnce(DoAll(SetArgPointee<2>(response2),
                            Invoke(ReadChunkFunc)));
        copysetClient.ReadChunk(reqCtx, reqDone);
        cond.Wait();
        ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS,
                  reqDone->GetErrorCode());
    }
    /* 不是 leader，但返回的是错误 leader */
    {
        RequestContext *reqCtx = new FakeRequestContext();
        reqCtx->optype_ = OpType::READ;
        reqCtx->idinfo_ = ChunkIDInfo(chunkId, logicPoolId, copysetId);

        reqCtx->subIoIndex_ = 0;
        reqCtx->offset_ = 0;
        reqCtx->rawlength_ = len;

        curve::common::CountDownEvent cond(1);
        RequestClosure *reqDone = new FakeRequestClosure(&cond, reqCtx);
        reqDone->SetFileMetric(&fm);
        reqDone->SetIOTracker(&iot);

        reqCtx->done_ = reqDone;
        ChunkResponse response;
        response.set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_REDIRECTED);
        response.set_redirect(leaderStr);
        EXPECT_CALL(mockMetaCache, GetLeader(_, _, _, _, _, _)).Times(6)
            .WillRepeatedly(DoAll(SetArgPointee<2>(leaderId),
                            SetArgPointee<3>(leaderAddr),
                            Return(0)));
        EXPECT_CALL(mockMetaCache, UpdateLeader(_, _, _)).Times(3)
            .WillRepeatedly(Return(0));
        EXPECT_CALL(mockChunkService, ReadChunk(_, _, _, _)).Times(3)
            .WillRepeatedly(DoAll(SetArgPointee<2>(response),
                            Invoke(ReadChunkFunc)));
        copysetClient.ReadChunk(reqCtx, reqDone);
        cond.Wait();
        ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_REDIRECTED,
                  reqDone->GetErrorCode());
    }
    /* copyset 不存在，更新 leader 依然失败 */
    {
        RequestContext *reqCtx = new FakeRequestContext();
        reqCtx->optype_ = OpType::READ;
        reqCtx->idinfo_ = ChunkIDInfo(chunkId, logicPoolId, copysetId);

        reqCtx->subIoIndex_ = 0;
        reqCtx->offset_ = 0;
        reqCtx->rawlength_ = len;

        curve::common::CountDownEvent cond(1);
        RequestClosure *reqDone = new FakeRequestClosure(&cond, reqCtx);
        reqDone->SetFileMetric(&fm);
        reqDone->SetIOTracker(&iot);

        reqCtx->done_ = reqDone;
        ChunkResponse response;
        response.set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_COPYSET_NOTEXIST);
        response.set_redirect(leaderStr);
        EXPECT_CALL(mockMetaCache, GetLeader(_, _, _, _, _, _)).Times(6)
            .WillRepeatedly(DoAll(SetArgPointee<2>(leaderId),
                            SetArgPointee<3>(leaderAddr),
                            Return(0)));
        EXPECT_CALL(mockChunkService, ReadChunk(_, _, _, _)).Times(3)
            .WillRepeatedly(DoAll(SetArgPointee<2>(response),
                            Invoke(ReadChunkFunc)));
        copysetClient.ReadChunk(reqCtx, reqDone);
        cond.Wait();
        ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_COPYSET_NOTEXIST,
                  reqDone->GetErrorCode());
    }
    /* copyset 不存在，更新 leader 成功 */
    {
        RequestContext *reqCtx = new FakeRequestContext();
        reqCtx->optype_ = OpType::READ;
        reqCtx->idinfo_ = ChunkIDInfo(chunkId, logicPoolId, copysetId);

        reqCtx->subIoIndex_ = 0;
        reqCtx->offset_ = 0;
        reqCtx->rawlength_ = len;

        curve::common::CountDownEvent cond(1);
        RequestClosure *reqDone = new FakeRequestClosure(&cond, reqCtx);
        reqDone->SetFileMetric(&fm);
        reqDone->SetIOTracker(&iot);

        reqCtx->done_ = reqDone;
        ChunkResponse response1;
        response1.set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_COPYSET_NOTEXIST);
        response1.set_redirect(leaderStr);
        ChunkResponse response2;
        response2.set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS);
        response2.set_redirect(leaderStr);
        EXPECT_CALL(mockMetaCache, GetLeader(_, _, _, _, _, _)).Times(3)
            .WillOnce(DoAll(SetArgPointee<2>(leaderId),
                            SetArgPointee<3>(leaderAddr),
                            Return(0)))
            .WillOnce(DoAll(SetArgPointee<2>(leaderId),
                            SetArgPointee<3>(leaderAddr),
                            Return(0)))
            .WillOnce(DoAll(SetArgPointee<2>(leaderId),
                            SetArgPointee<3>(leaderAddr),
                            Return(0)));
        EXPECT_CALL(mockChunkService, ReadChunk(_, _, _, _)).Times(2)
            .WillOnce(DoAll(SetArgPointee<2>(response1),
                            Invoke(ReadChunkFunc)))
            .WillOnce(DoAll(SetArgPointee<2>(response2),
                            Invoke(ReadChunkFunc)));
        copysetClient.ReadChunk(reqCtx, reqDone);
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

    IOSenderOption ioSenderOpt;
    ioSenderOpt.failRequestOpt.chunkserverRPCTimeoutMS = 5000;
    ioSenderOpt.failRequestOpt.chunkserverOPMaxRetry = 3;
    ioSenderOpt.failRequestOpt.chunkserverOPRetryIntervalUS = 500;
    ioSenderOpt.chunkserverEnableAppliedIndexRead = 1;

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

    ChunkServerID leaderId = 10000;
    butil::EndPoint leaderAddr;
    std::string leaderStr = "127.0.0.1:9109";
    butil::str2endpoint(leaderStr.c_str(), &leaderAddr);

    FileMetric fm("test");
    IOTracker iot(nullptr, nullptr, nullptr, &fm);

    /* 非法参数 */
    {
        RequestContext *reqCtx = new FakeRequestContext();
        reqCtx->optype_ = OpType::READ_SNAP;
        reqCtx->idinfo_ = ChunkIDInfo(chunkId, logicPoolId, copysetId);


        reqCtx->seq_ = sn;
        reqCtx->offset_ = 0;
        reqCtx->rawlength_ = len;

        curve::common::CountDownEvent cond(1);
        RequestClosure *reqDone = new FakeRequestClosure(&cond, reqCtx);
        reqDone->SetFileMetric(&fm);
        reqDone->SetIOTracker(&iot);

        reqCtx->done_ = reqDone;
        ChunkResponse response;
        response.set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_INVALID_REQUEST);
        EXPECT_CALL(mockMetaCache, GetLeader(_, _, _, _, _, _))
            .Times(AtLeast(1)).WillOnce(DoAll(SetArgPointee<2>(leaderId),
                                              SetArgPointee<3>(leaderAddr),
                                              Return(0)));
        EXPECT_CALL(mockChunkService, ReadChunkSnapshot(_, _, _, _)).Times(1)
            .WillOnce(DoAll(SetArgPointee<2>(response),
                            Invoke(ReadChunkSnapshotFunc)));
        copysetClient.ReadChunkSnapshot(reqCtx->idinfo_,
                                        sn, {sn}, offset, len, reqDone);
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
        reqCtx->offset_ = 0;
        reqCtx->rawlength_ = len;

        curve::common::CountDownEvent cond(1);
        RequestClosure *reqDone = new FakeRequestClosure(&cond, reqCtx);
        reqDone->SetFileMetric(&fm);
        reqDone->SetIOTracker(&iot);

        reqCtx->done_ = reqDone;
        ChunkResponse response;
        response.set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_CHUNK_NOTEXIST);
        EXPECT_CALL(mockMetaCache, GetLeader(_, _, _, _, _, _))
            .Times(AtLeast(1)).WillOnce(DoAll(SetArgPointee<2>(leaderId),
                                              SetArgPointee<3>(leaderAddr),
                                              Return(0)));
        EXPECT_CALL(mockChunkService, ReadChunkSnapshot(_, _, _, _)).Times(1)
            .WillOnce(DoAll(SetArgPointee<2>(response),
                            Invoke(ReadChunkSnapshotFunc)));
        copysetClient.ReadChunkSnapshot(reqCtx->idinfo_,
                                        sn, {sn}, offset, len, reqDone);
        cond.Wait();
        ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS,
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
        reqDone->SetFileMetric(&fm);
        reqDone->SetIOTracker(&iot);

        reqCtx->done_ = reqDone;
        gReadCntlFailedCode = -1;
        EXPECT_CALL(mockMetaCache, GetLeader(_, _, _, _, _, _)).Times(6)
            .WillRepeatedly(DoAll(SetArgPointee<2>(leaderId),
                                  SetArgPointee<3>(leaderAddr),
                                  Return(0)));
        EXPECT_CALL(mockChunkService, ReadChunkSnapshot(_, _, _, _)).Times(3)
            .WillRepeatedly(Invoke(ReadChunkSnapshotFunc));
        copysetClient.ReadChunkSnapshot(reqCtx->idinfo_,
                                        sn, {sn}, offset, len, reqDone);
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
        reqCtx->offset_ = 0;
        reqCtx->rawlength_ = len;

        curve::common::CountDownEvent cond(1);
        RequestClosure *reqDone = new FakeRequestClosure(&cond, reqCtx);
        reqDone->SetFileMetric(&fm);
        reqDone->SetIOTracker(&iot);

        reqCtx->done_ = reqDone;
        ChunkResponse response;
        response.set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_FAILURE_UNKNOWN);
        EXPECT_CALL(mockMetaCache, GetLeader(_, _, _, _, _, _))
            .Times(3).WillRepeatedly(DoAll(SetArgPointee<2>(leaderId),
                                           SetArgPointee<3>(leaderAddr),
                                           Return(0)));
        EXPECT_CALL(mockChunkService, ReadChunkSnapshot(_, _, _, _)).Times(3)
            .WillRepeatedly(DoAll(SetArgPointee<2>(response),
                                  Invoke(ReadChunkSnapshotFunc)));
        copysetClient.ReadChunkSnapshot(reqCtx->idinfo_,
                                        sn, {sn}, offset, len, reqDone);
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
        reqCtx->offset_ = 0;
        reqCtx->rawlength_ = len;

        curve::common::CountDownEvent cond(1);
        RequestClosure *reqDone = new FakeRequestClosure(&cond, reqCtx);
        reqDone->SetFileMetric(&fm);
        reqDone->SetIOTracker(&iot);

        reqCtx->done_ = reqDone;
        ChunkResponse response1;
        response1.set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_REDIRECTED);
        response1.set_redirect(leaderStr);
        ChunkResponse response2;
        response2.set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS);
        EXPECT_CALL(mockMetaCache, GetLeader(_, _, _, _, _, _)).Times(3)
            .WillRepeatedly(DoAll(SetArgPointee<2>(leaderId),
                            SetArgPointee<3>(leaderAddr),
                            Return(0)));
        EXPECT_CALL(mockMetaCache, UpdateLeader(_, _, _)).Times(1)
            .WillOnce(Return(0));
        EXPECT_CALL(mockChunkService, ReadChunkSnapshot(_, _, _, _)).Times(2)
            .WillOnce(DoAll(SetArgPointee<2>(response1),
                            Invoke(ReadChunkSnapshotFunc)))
            .WillOnce(DoAll(SetArgPointee<2>(response2),
                            Invoke(ReadChunkSnapshotFunc)));
        copysetClient.ReadChunkSnapshot(reqCtx->idinfo_,
                                        sn, {sn}, offset, len, reqDone);
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
        reqCtx->offset_ = 0;
        reqCtx->rawlength_ = len;

        curve::common::CountDownEvent cond(1);
        RequestClosure *reqDone = new FakeRequestClosure(&cond, reqCtx);
        reqDone->SetFileMetric(&fm);
        reqDone->SetIOTracker(&iot);

        reqCtx->done_ = reqDone;
        ChunkResponse response1;
        response1.set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_REDIRECTED);
        ChunkResponse response2;
        response2.set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS);
        EXPECT_CALL(mockMetaCache, GetLeader(_, _, _, _, _, _)).Times(3)
            .WillRepeatedly(DoAll(SetArgPointee<2>(leaderId),
                            SetArgPointee<3>(leaderAddr),
                            Return(0)));
        EXPECT_CALL(mockChunkService, ReadChunkSnapshot(_, _, _, _)).Times(2)
            .WillOnce(DoAll(SetArgPointee<2>(response1),
                            Invoke(ReadChunkSnapshotFunc)))
            .WillOnce(DoAll(SetArgPointee<2>(response2),
                            Invoke(ReadChunkSnapshotFunc)));
        copysetClient.ReadChunkSnapshot(reqCtx->idinfo_,
                                        sn, {sn}, offset, len, reqDone);
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
        reqCtx->offset_ = 0;
        reqCtx->rawlength_ = len;

        curve::common::CountDownEvent cond(1);
        RequestClosure *reqDone = new FakeRequestClosure(&cond, reqCtx);
        reqDone->SetFileMetric(&fm);
        reqDone->SetIOTracker(&iot);

        reqCtx->done_ = reqDone;
        ChunkResponse response1;
        response1.set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_REDIRECTED);
        ChunkResponse response2;
        response2.set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS);
        EXPECT_CALL(mockMetaCache, GetLeader(_, _, _, _, _, _)).Times(3)
            .WillOnce(DoAll(SetArgPointee<2>(leaderId),
                            SetArgPointee<3>(leaderAddr),
                            Return(0)))
            .WillOnce(Return(-1))
            .WillOnce(DoAll(SetArgPointee<2>(leaderId),
                            SetArgPointee<3>(leaderAddr),
                            Return(0)));
        EXPECT_CALL(mockChunkService, ReadChunkSnapshot(_, _, _, _)).Times(2)
            .WillOnce(DoAll(SetArgPointee<2>(response1),
                            Invoke(ReadChunkSnapshotFunc)))
            .WillOnce(DoAll(SetArgPointee<2>(response2),
                            Invoke(ReadChunkSnapshotFunc)));
        copysetClient.ReadChunkSnapshot(reqCtx->idinfo_,
                                        sn, {sn}, offset, len, reqDone);
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
        reqCtx->offset_ = 0;
        reqCtx->rawlength_ = len;

        curve::common::CountDownEvent cond(1);
        RequestClosure *reqDone = new FakeRequestClosure(&cond, reqCtx);
        reqDone->SetFileMetric(&fm);
        reqDone->SetIOTracker(&iot);

        reqCtx->done_ = reqDone;
        ChunkResponse response;
        response.set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_REDIRECTED);
        response.set_redirect(leaderStr);
        EXPECT_CALL(mockMetaCache, GetLeader(_, _, _, _, _, _)).Times(6)
            .WillRepeatedly(DoAll(SetArgPointee<2>(leaderId),
                            SetArgPointee<3>(leaderAddr),
                            Return(0)));
        EXPECT_CALL(mockMetaCache, UpdateLeader(_, _, _)).Times(3)
            .WillRepeatedly(Return(0));
        EXPECT_CALL(mockChunkService, ReadChunkSnapshot(_, _, _, _)).Times(3)
            .WillRepeatedly(DoAll(SetArgPointee<2>(response),
                            Invoke(ReadChunkSnapshotFunc)));
        copysetClient.ReadChunkSnapshot(reqCtx->idinfo_,
                                        sn, {sn}, offset, len, reqDone);
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
        reqCtx->offset_ = 0;
        reqCtx->rawlength_ = len;

        curve::common::CountDownEvent cond(1);
        RequestClosure *reqDone = new FakeRequestClosure(&cond, reqCtx);
        reqDone->SetFileMetric(&fm);
        reqDone->SetIOTracker(&iot);

        reqCtx->done_ = reqDone;
        ChunkResponse response;
        response.set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_COPYSET_NOTEXIST);
        response.set_redirect(leaderStr);
        EXPECT_CALL(mockMetaCache, GetLeader(_, _, _, _, _, _)).Times(6)
            .WillRepeatedly(DoAll(SetArgPointee<2>(leaderId),
                            SetArgPointee<3>(leaderAddr),
                            Return(0)));
        EXPECT_CALL(mockChunkService, ReadChunkSnapshot(_, _, _, _)).Times(3)
            .WillRepeatedly(DoAll(SetArgPointee<2>(response),
                            Invoke(ReadChunkSnapshotFunc)));
        copysetClient.ReadChunkSnapshot(reqCtx->idinfo_,
                                        sn, {sn}, offset, len, reqDone);
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
        reqCtx->offset_ = 0;
        reqCtx->rawlength_ = len;

        curve::common::CountDownEvent cond(1);
        RequestClosure *reqDone = new FakeRequestClosure(&cond, reqCtx);
        reqDone->SetFileMetric(&fm);
        reqDone->SetIOTracker(&iot);

        reqCtx->done_ = reqDone;
        ChunkResponse response1;
        response1.set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_COPYSET_NOTEXIST);
        response1.set_redirect(leaderStr);
        ChunkResponse response2;
        response2.set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS);
        response2.set_redirect(leaderStr);
        EXPECT_CALL(mockMetaCache, GetLeader(_, _, _, _, _, _)).Times(3)
            .WillRepeatedly(DoAll(SetArgPointee<2>(leaderId),
                            SetArgPointee<3>(leaderAddr),
                            Return(0)));
        EXPECT_CALL(mockChunkService, ReadChunkSnapshot(_, _, _, _)).Times(2)
            .WillOnce(DoAll(SetArgPointee<2>(response1),
                            Invoke(ReadChunkSnapshotFunc)))
            .WillOnce(DoAll(SetArgPointee<2>(response2),
                            Invoke(ReadChunkSnapshotFunc)));
        copysetClient.ReadChunkSnapshot(reqCtx->idinfo_,
                                        sn, {sn}, offset, len, reqDone);
        cond.Wait();
        ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS,
                  reqDone->GetErrorCode());
    }
    {
        RequestContext *reqCtx = new FakeRequestContext();
        reqCtx->optype_ = OpType::READ_SNAP;
        reqCtx->idinfo_ = ChunkIDInfo(chunkId, logicPoolId, copysetId);


        reqCtx->seq_ = sn;
        reqCtx->offset_ = 0;
        reqCtx->rawlength_ = len;

        curve::common::CountDownEvent cond(1);
        RequestClosure *reqDone = new FakeRequestClosure(&cond, reqCtx);
        reqDone->SetFileMetric(&fm);
        reqDone->SetIOTracker(&iot);

        reqCtx->done_ = reqDone;
        ChunkResponse response;
        response.set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS);
        EXPECT_CALL(mockMetaCache, GetLeader(_, _, _, _, _, _)).
                                            Times(AtLeast(1))
            .WillOnce(Return(-1))
            .WillOnce(Return(-1))
            .WillOnce(DoAll(SetArgPointee<2>(leaderId),
                            SetArgPointee<3>(leaderAddr),
                            Return(0)));
        EXPECT_CALL(mockChunkService, ReadChunkSnapshot(_, _, _, _)).Times(1)
            .WillOnce(DoAll(SetArgPointee<2>(response),
                            Invoke(ReadChunkSnapshotFunc)));
        copysetClient.ReadChunkSnapshot(reqCtx->idinfo_,
                                        sn, {sn}, offset, len, reqDone);
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

    IOSenderOption ioSenderOpt;
    ioSenderOpt.failRequestOpt.chunkserverRPCTimeoutMS = 5000;
    ioSenderOpt.failRequestOpt.chunkserverOPMaxRetry = 3;
    ioSenderOpt.failRequestOpt.chunkserverOPRetryIntervalUS = 500;
    ioSenderOpt.chunkserverEnableAppliedIndexRead = 1;

    CopysetClient copysetClient;
    MockMetaCache mockMetaCache;
    mockMetaCache.DelegateToFake();
    RequestScheduler scheduler;
    copysetClient.Init(&mockMetaCache, ioSenderOpt, &scheduler);

    LogicPoolID logicPoolId = 1;
    CopysetID copysetId = 100001;
    ChunkID chunkId = 1;
    uint64_t sn = 1;

    ChunkServerID leaderId = 10000;
    butil::EndPoint leaderAddr;
    std::string leaderStr = "127.0.0.1:9109";
    butil::str2endpoint(leaderStr.c_str(), &leaderAddr);

    FileMetric fm("test");
    IOTracker iot(nullptr, nullptr, nullptr, &fm);

    /* 非法参数 */
    {
        RequestContext *reqCtx = new FakeRequestContext();
        reqCtx->optype_ = OpType::DELETE_SNAP;
        reqCtx->idinfo_ = ChunkIDInfo(chunkId, logicPoolId, copysetId);


        reqCtx->correctedSeq_ = sn;

        curve::common::CountDownEvent cond(1);
        RequestClosure *reqDone = new FakeRequestClosure(&cond, reqCtx);
        reqDone->SetFileMetric(&fm);
        reqDone->SetIOTracker(&iot);

        reqCtx->done_ = reqDone;
        ChunkResponse response;
        response.set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_INVALID_REQUEST);
        EXPECT_CALL(mockMetaCache, GetLeader(_, _, _, _, _, _))
            .Times(AtLeast(1)).WillOnce(DoAll(SetArgPointee<2>(leaderId),
                                              SetArgPointee<3>(leaderAddr),
                                              Return(0)));
        EXPECT_CALL(mockChunkService, DeleteChunkSnapshotOrCorrectSn(_, _, _, _))  // NOLINT
            .Times(1)
            .WillOnce(DoAll(SetArgPointee<2>(response),
                            Invoke(DeleteChunkSnapshotFunc)));
        copysetClient.DeleteChunkSnapshotOrCorrectSn(reqCtx->idinfo_,
                  sn, reqDone);
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
        reqDone->SetFileMetric(&fm);
        reqDone->SetIOTracker(&iot);

        reqCtx->done_ = reqDone;
        gReadCntlFailedCode = -1;
        EXPECT_CALL(mockMetaCache, GetLeader(_, _, _, _, _, _)).Times(6)
            .WillRepeatedly(DoAll(SetArgPointee<2>(leaderId),
                                  SetArgPointee<3>(leaderAddr),
                                  Return(0)));
        EXPECT_CALL(mockChunkService, DeleteChunkSnapshotOrCorrectSn(_, _, _, _))  // NOLINT
            .Times(3)
            .WillRepeatedly(Invoke(DeleteChunkSnapshotFunc));
        copysetClient.DeleteChunkSnapshotOrCorrectSn(reqCtx->idinfo_,
                  sn, reqDone);
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
        reqDone->SetFileMetric(&fm);
        reqDone->SetIOTracker(&iot);

        reqCtx->done_ = reqDone;
        ChunkResponse response;
        response.set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_FAILURE_UNKNOWN);
        EXPECT_CALL(mockMetaCache, GetLeader(_, _, _, _, _, _))
            .Times(3).WillRepeatedly(DoAll(SetArgPointee<2>(leaderId),
                                           SetArgPointee<3>(leaderAddr),
                                           Return(0)));
        EXPECT_CALL(mockChunkService, DeleteChunkSnapshotOrCorrectSn(_, _, _, _))  // NOLINT
            .Times(3)
            .WillRepeatedly(DoAll(SetArgPointee<2>(response),
                                  Invoke(DeleteChunkSnapshotFunc)));
        copysetClient.DeleteChunkSnapshotOrCorrectSn(reqCtx->idinfo_,
                  sn, reqDone);
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
        reqDone->SetFileMetric(&fm);
        reqDone->SetIOTracker(&iot);

        reqCtx->done_ = reqDone;
        ChunkResponse response1;
        response1.set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_REDIRECTED);
        response1.set_redirect(leaderStr);
        ChunkResponse response2;
        response2.set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS);
        EXPECT_CALL(mockMetaCache, GetLeader(_, _, _, _, _, _)).Times(3)
            .WillOnce(DoAll(SetArgPointee<2>(leaderId),
                            SetArgPointee<3>(leaderAddr),
                            Return(0)))
            .WillOnce(DoAll(SetArgPointee<2>(leaderId),
                            SetArgPointee<3>(leaderAddr),
                            Return(0)));
        EXPECT_CALL(mockMetaCache, UpdateLeader(_, _, _)).Times(1)
            .WillOnce(Return(0));
        EXPECT_CALL(mockChunkService, DeleteChunkSnapshotOrCorrectSn(_, _, _, _))  // NOLINT
            .Times(2)
            .WillOnce(DoAll(SetArgPointee<2>(response1),
                            Invoke(ReadChunkSnapshotFunc)))
            .WillOnce(DoAll(SetArgPointee<2>(response2),
                            Invoke(DeleteChunkSnapshotFunc)));
        copysetClient.DeleteChunkSnapshotOrCorrectSn(reqCtx->idinfo_,
                  sn, reqDone);
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
        reqDone->SetFileMetric(&fm);
        reqDone->SetIOTracker(&iot);

        reqCtx->done_ = reqDone;
        ChunkResponse response1;
        response1.set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_REDIRECTED);
        ChunkResponse response2;
        response2.set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS);
        EXPECT_CALL(mockMetaCache, GetLeader(_, _, _, _, _, _)).Times(3)
            .WillRepeatedly(DoAll(SetArgPointee<2>(leaderId),
                            SetArgPointee<3>(leaderAddr),
                            Return(0)));
        EXPECT_CALL(mockChunkService, DeleteChunkSnapshotOrCorrectSn(_, _, _, _))  // NOLINT
            .Times(2)
            .WillOnce(DoAll(SetArgPointee<2>(response1),
                            Invoke(DeleteChunkSnapshotFunc)))
            .WillOnce(DoAll(SetArgPointee<2>(response2),
                            Invoke(DeleteChunkSnapshotFunc)));
        copysetClient.DeleteChunkSnapshotOrCorrectSn(reqCtx->idinfo_,
                  sn, reqDone);
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
        reqDone->SetFileMetric(&fm);
        reqDone->SetIOTracker(&iot);

        reqCtx->done_ = reqDone;
        ChunkResponse response1;
        response1.set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_REDIRECTED);
        ChunkResponse response2;
        response2.set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS);
        EXPECT_CALL(mockMetaCache, GetLeader(_, _, _, _, _, _)).Times(3)
            .WillOnce(DoAll(SetArgPointee<2>(leaderId),
                            SetArgPointee<3>(leaderAddr),
                            Return(0)))
            .WillOnce(DoAll(SetArgPointee<2>(leaderId),
                            SetArgPointee<3>(leaderAddr),
                            Return(-1)))
            .WillOnce(DoAll(SetArgPointee<2>(leaderId),
                            SetArgPointee<3>(leaderAddr),
                            Return(0)));
        EXPECT_CALL(mockChunkService, DeleteChunkSnapshotOrCorrectSn(_, _, _, _))  // NOLINT
            .Times(2)
            .WillOnce(DoAll(SetArgPointee<2>(response1),
                            Invoke(DeleteChunkSnapshotFunc)))
            .WillOnce(DoAll(SetArgPointee<2>(response2),
                            Invoke(DeleteChunkSnapshotFunc)));
        copysetClient.DeleteChunkSnapshotOrCorrectSn(reqCtx->idinfo_,
                  sn, reqDone);
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
        reqDone->SetFileMetric(&fm);
        reqDone->SetIOTracker(&iot);

        reqCtx->done_ = reqDone;
        ChunkResponse response;
        response.set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_REDIRECTED);
        response.set_redirect(leaderStr);;
        EXPECT_CALL(mockMetaCache, GetLeader(_, _, _, _, _, _)).Times(6)
            .WillRepeatedly(DoAll(SetArgPointee<2>(leaderId),
                            SetArgPointee<3>(leaderAddr),
                            Return(0)));
        EXPECT_CALL(mockMetaCache, UpdateLeader(_, _, _)).Times(3)
            .WillRepeatedly(Return(0));
        EXPECT_CALL(mockChunkService, DeleteChunkSnapshotOrCorrectSn(_, _, _, _))  // NOLINT
            .Times(3)
            .WillRepeatedly(DoAll(SetArgPointee<2>(response),
                            Invoke(DeleteChunkSnapshotFunc)));
        copysetClient.DeleteChunkSnapshotOrCorrectSn(reqCtx->idinfo_,
                  sn, reqDone);
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
        reqDone->SetFileMetric(&fm);
        reqDone->SetIOTracker(&iot);

        reqCtx->done_ = reqDone;
        ChunkResponse response;
        response.set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_COPYSET_NOTEXIST);
        response.set_redirect(leaderStr);
        EXPECT_CALL(mockMetaCache, GetLeader(_, _, _, _, _, _)).Times(6)
            .WillRepeatedly(DoAll(SetArgPointee<2>(leaderId),
                            SetArgPointee<3>(leaderAddr),
                            Return(0)));
        EXPECT_CALL(mockChunkService, DeleteChunkSnapshotOrCorrectSn(_, _, _, _))  // NOLINT
            .Times(3)
            .WillRepeatedly(DoAll(SetArgPointee<2>(response),
                            Invoke(DeleteChunkSnapshotFunc)));
        copysetClient.DeleteChunkSnapshotOrCorrectSn(reqCtx->idinfo_,
                  sn, reqDone);
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
        reqDone->SetFileMetric(&fm);
        reqDone->SetIOTracker(&iot);

        reqCtx->done_ = reqDone;
        ChunkResponse response1;
        response1.set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_COPYSET_NOTEXIST);
        response1.set_redirect(leaderStr);
        ChunkResponse response2;
        response2.set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS);
        response2.set_redirect(leaderStr);
        EXPECT_CALL(mockMetaCache, GetLeader(_, _, _, _, _, _)).Times(3)
            .WillRepeatedly(DoAll(SetArgPointee<2>(leaderId),
                            SetArgPointee<3>(leaderAddr),
                            Return(0)));
        EXPECT_CALL(mockChunkService, DeleteChunkSnapshotOrCorrectSn(_, _, _, _))  // NOLINT
            .Times(2)
            .WillOnce(DoAll(SetArgPointee<2>(response1),
                            Invoke(DeleteChunkSnapshotFunc)))
            .WillOnce(DoAll(SetArgPointee<2>(response2),
                            Invoke(DeleteChunkSnapshotFunc)));
        copysetClient.DeleteChunkSnapshotOrCorrectSn(reqCtx->idinfo_,
                  sn, reqDone);
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
        reqDone->SetFileMetric(&fm);
        reqDone->SetIOTracker(&iot);

        reqCtx->done_ = reqDone;
        ChunkResponse response;
        response.set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS);
        EXPECT_CALL(mockMetaCache, GetLeader(_, _, _, _, _, _)).
                                            Times(AtLeast(1))
            .WillOnce(DoAll(SetArgPointee<2>(leaderId),
                            SetArgPointee<3>(leaderAddr),
                            Return(-1)))
            .WillOnce(DoAll(SetArgPointee<2>(leaderId),
                            SetArgPointee<3>(leaderAddr),
                            Return(-1)))
            .WillOnce(DoAll(SetArgPointee<2>(leaderId),
                            SetArgPointee<3>(leaderAddr),
                            Return(0)));
        EXPECT_CALL(mockChunkService, DeleteChunkSnapshotOrCorrectSn(_, _, _, _))  // NOLINT
            .Times(1)
            .WillOnce(DoAll(SetArgPointee<2>(response),
                            Invoke(DeleteChunkSnapshotFunc)));
        copysetClient.DeleteChunkSnapshotOrCorrectSn(reqCtx->idinfo_,
                                          sn, reqDone);
        cond.Wait();
        ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS,
                  reqDone->GetErrorCode());
    }
}

/**
 * create clone chunk error testing
 */
TEST_F(CopysetClientTest, create_s3_clone_error_test) {
    MockChunkServiceImpl mockChunkService;
    ASSERT_EQ(server_->AddService(&mockChunkService,
                                  brpc::SERVER_DOESNT_OWN_SERVICE), 0);
    ASSERT_EQ(server_->Start(listenAddr_.c_str(), nullptr), 0);

    IOSenderOption ioSenderOpt;
    ioSenderOpt.failRequestOpt.chunkserverRPCTimeoutMS = 5000;
    ioSenderOpt.failRequestOpt.chunkserverOPMaxRetry = 3;
    ioSenderOpt.failRequestOpt.chunkserverOPRetryIntervalUS = 500;
    ioSenderOpt.chunkserverEnableAppliedIndexRead = 1;

    CopysetClient copysetClient;
    MockMetaCache mockMetaCache;
    mockMetaCache.DelegateToFake();
    RequestScheduler scheduler;
    copysetClient.Init(&mockMetaCache, ioSenderOpt, &scheduler);

    LogicPoolID logicPoolId = 1;
    CopysetID copysetId = 100001;
    ChunkID chunkId = 1;
    uint64_t sn = 1;

    ChunkServerID leaderId = 10000;
    butil::EndPoint leaderAddr;
    std::string leaderStr = "127.0.0.1:9109";
    butil::str2endpoint(leaderStr.c_str(), &leaderAddr);

    FileMetric fm("test");
    IOTracker iot(nullptr, nullptr, nullptr, &fm);

    /* 非法参数 */
    {
        RequestContext *reqCtx = new FakeRequestContext();
        reqCtx->optype_ = OpType::CREATE_CLONE;
        reqCtx->idinfo_ = ChunkIDInfo(chunkId, logicPoolId, copysetId);


        reqCtx->seq_ = sn;

        curve::common::CountDownEvent cond(1);
        RequestClosure *reqDone = new FakeRequestClosure(&cond, reqCtx);
        reqDone->SetFileMetric(&fm);
        reqDone->SetIOTracker(&iot);

        reqCtx->done_ = reqDone;
        ChunkResponse response;
        response.set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_INVALID_REQUEST);
        EXPECT_CALL(mockMetaCache, GetLeader(_, _, _, _, _, _))
            .Times(AtLeast(1)).WillOnce(DoAll(SetArgPointee<2>(leaderId),
                                              SetArgPointee<3>(leaderAddr),
                                              Return(0)));
        EXPECT_CALL(mockChunkService, CreateCloneChunk(_, _, _, _)).Times(1)      // NOLINT
            .WillOnce(DoAll(SetArgPointee<2>(response),
                            Invoke(CreateCloneChunkFunc)));
        copysetClient.CreateCloneChunk(reqCtx->idinfo_,
                                        "destination", sn, 1, 1024, reqDone);
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
        reqDone->SetFileMetric(&fm);
        reqDone->SetIOTracker(&iot);

        reqCtx->done_ = reqDone;
        gReadCntlFailedCode = -1;
        EXPECT_CALL(mockMetaCache, GetLeader(_, _, _, _, _, _)).Times(6)
            .WillRepeatedly(DoAll(SetArgPointee<2>(leaderId),
                                  SetArgPointee<3>(leaderAddr),
                                  Return(0)));
        EXPECT_CALL(mockChunkService, CreateCloneChunk(_, _, _, _)).Times(3)      // NOLINT
            .WillRepeatedly(Invoke(CreateCloneChunkFunc));
        copysetClient.CreateCloneChunk(reqCtx->idinfo_,
        "destination", sn, 1, 1024, reqDone);
        cond.Wait();
        ASSERT_NE(0, reqDone->GetErrorCode());
        gReadCntlFailedCode = 0;
    }
    // /* 其他错误 */
    {
        RequestContext *reqCtx = new FakeRequestContext();
        reqCtx->optype_ = OpType::DELETE_SNAP;
        reqCtx->idinfo_ = ChunkIDInfo(chunkId, logicPoolId, copysetId);


        reqCtx->correctedSeq_ = sn;

        curve::common::CountDownEvent cond(1);
        RequestClosure *reqDone = new FakeRequestClosure(&cond, reqCtx);
        reqDone->SetFileMetric(&fm);
        reqDone->SetIOTracker(&iot);

        reqCtx->done_ = reqDone;
        ChunkResponse response;
        response.set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_FAILURE_UNKNOWN);
        EXPECT_CALL(mockMetaCache, GetLeader(_, _, _, _, _, _))
            .Times(3).WillRepeatedly(DoAll(SetArgPointee<2>(leaderId),
                                           SetArgPointee<3>(leaderAddr),
                                           Return(0)));
        EXPECT_CALL(mockChunkService, CreateCloneChunk(_, _, _, _)).Times(3)      // NOLINT
            .WillRepeatedly(DoAll(SetArgPointee<2>(response),
                                  Invoke(CreateCloneChunkFunc)));
        copysetClient.CreateCloneChunk(reqCtx->idinfo_,
        "destination", sn, 1, 1024, reqDone);
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
        reqDone->SetFileMetric(&fm);
        reqDone->SetIOTracker(&iot);

        reqCtx->done_ = reqDone;

        ChunkResponse response2;
        response2.set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS);
        EXPECT_CALL(mockMetaCache, GetLeader(_, _, _, _, _, _)).Times(1)
            .WillOnce(DoAll(SetArgPointee<2>(leaderId),
                            SetArgPointee<3>(leaderAddr),
                            Return(0)))
            .WillOnce(DoAll(SetArgPointee<2>(leaderId),
                            SetArgPointee<3>(leaderAddr),
                            Return(0)));
        EXPECT_CALL(mockChunkService, CreateCloneChunk(_, _, _, _)).Times(1)
            .WillOnce(DoAll(SetArgPointee<2>(response2),
                            Invoke(CreateCloneChunkFunc)));
        copysetClient.CreateCloneChunk(reqCtx->idinfo_,
        "destination", sn, 1, 1024, reqDone);
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
        reqDone->SetFileMetric(&fm);
        reqDone->SetIOTracker(&iot);

        reqCtx->done_ = reqDone;
        ChunkResponse response1;
        response1.set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_REDIRECTED);
        ChunkResponse response2;
        response2.set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS);
        EXPECT_CALL(mockMetaCache, GetLeader(_, _, _, _, _, _)).Times(3)
            .WillRepeatedly(DoAll(SetArgPointee<2>(leaderId),
                            SetArgPointee<3>(leaderAddr),
                            Return(0)));
        EXPECT_CALL(mockChunkService, CreateCloneChunk(_, _, _, _)).Times(2)      // NOLINT
            .WillOnce(DoAll(SetArgPointee<2>(response1),
                            Invoke(CreateCloneChunkFunc)))
            .WillOnce(DoAll(SetArgPointee<2>(response2),
                            Invoke(CreateCloneChunkFunc)));
        copysetClient.CreateCloneChunk(reqCtx->idinfo_,
        "destination", sn, 1, 1024, reqDone);
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
        reqDone->SetFileMetric(&fm);
        reqDone->SetIOTracker(&iot);

        reqCtx->done_ = reqDone;
        ChunkResponse response1;
        response1.set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_REDIRECTED);
        ChunkResponse response2;
        response2.set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS);
        EXPECT_CALL(mockMetaCache, GetLeader(_, _, _, _, _, _)).Times(3)
            .WillOnce(DoAll(SetArgPointee<2>(leaderId),
                            SetArgPointee<3>(leaderAddr),
                            Return(0)))
            .WillOnce(Return(-1))
            .WillOnce(DoAll(SetArgPointee<2>(leaderId),
                            SetArgPointee<3>(leaderAddr),
                            Return(0)));
        EXPECT_CALL(mockChunkService, CreateCloneChunk(_, _, _, _)).Times(2)      // NOLINT
            .WillOnce(DoAll(SetArgPointee<2>(response1),
                            Invoke(CreateCloneChunkFunc)))
            .WillOnce(DoAll(SetArgPointee<2>(response2),
                            Invoke(CreateCloneChunkFunc)));
        copysetClient.CreateCloneChunk(reqCtx->idinfo_,
        "destination", sn, 1, 1024, reqDone);
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
        reqDone->SetFileMetric(&fm);
        reqDone->SetIOTracker(&iot);

        reqCtx->done_ = reqDone;
        ChunkResponse response;
        response.set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_REDIRECTED);
        response.set_redirect(leaderStr);
        EXPECT_CALL(mockMetaCache, GetLeader(_, _, _, _, _, _)).Times(6)
            .WillRepeatedly(DoAll(SetArgPointee<2>(leaderId),
                            SetArgPointee<3>(leaderAddr),
                            Return(0)));
        EXPECT_CALL(mockMetaCache, UpdateLeader(_, _, _)).Times(3)
            .WillRepeatedly(Return(0));
        EXPECT_CALL(mockChunkService, CreateCloneChunk(_, _, _, _)).Times(3)      // NOLINT
            .WillRepeatedly(DoAll(SetArgPointee<2>(response),
                            Invoke(CreateCloneChunkFunc)));
        copysetClient.CreateCloneChunk(reqCtx->idinfo_,
                                "destination", sn, 1, 1024, reqDone);
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
        reqDone->SetFileMetric(&fm);
        reqDone->SetIOTracker(&iot);

        reqCtx->done_ = reqDone;
        ChunkResponse response;
        response.set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_COPYSET_NOTEXIST);
        response.set_redirect(leaderStr);
        EXPECT_CALL(mockMetaCache, GetLeader(_, _, _, _, _, _)).Times(6)
            .WillRepeatedly(DoAll(SetArgPointee<2>(leaderId),
                            SetArgPointee<3>(leaderAddr),
                            Return(0)));
        EXPECT_CALL(mockChunkService, CreateCloneChunk(_, _, _, _)).Times(3)      // NOLINT
            .WillRepeatedly(DoAll(SetArgPointee<2>(response),
                            Invoke(CreateCloneChunkFunc)));
        copysetClient.CreateCloneChunk(reqCtx->idinfo_,
                             "destination", sn, 1, 1024, reqDone);
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
        reqDone->SetFileMetric(&fm);
        reqDone->SetIOTracker(&iot);

        reqCtx->done_ = reqDone;
        ChunkResponse response1;
        response1.set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_COPYSET_NOTEXIST);
        response1.set_redirect(leaderStr);
        ChunkResponse response2;
        response2.set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS);
        response2.set_redirect(leaderStr);
        EXPECT_CALL(mockMetaCache, GetLeader(_, _, _, _, _, _)).Times(3)
            .WillOnce(DoAll(SetArgPointee<2>(leaderId),
                            SetArgPointee<3>(leaderAddr),
                            Return(0)))
            .WillOnce(DoAll(SetArgPointee<2>(leaderId),
                            SetArgPointee<3>(leaderAddr),
                            Return(0)))
            .WillOnce(DoAll(SetArgPointee<2>(leaderId),
                            SetArgPointee<3>(leaderAddr),
                            Return(0)));
        EXPECT_CALL(mockChunkService, CreateCloneChunk(_, _, _, _)).Times(2)      // NOLINT
            .WillOnce(DoAll(SetArgPointee<2>(response1),
                            Invoke(CreateCloneChunkFunc)))
            .WillOnce(DoAll(SetArgPointee<2>(response2),
                            Invoke(CreateCloneChunkFunc)));
        copysetClient.CreateCloneChunk(reqCtx->idinfo_,
                             "destination", sn, 1, 1024, reqDone);
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
        reqDone->SetFileMetric(&fm);
        reqDone->SetIOTracker(&iot);

        reqCtx->done_ = reqDone;
        ChunkResponse response;
        response.set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS);
        EXPECT_CALL(mockMetaCache, GetLeader(_, _, _, _, _, _)).
                                            Times(AtLeast(1))
            .WillOnce(Return(-1))
            .WillOnce(Return(-1))
            .WillOnce(DoAll(SetArgPointee<2>(leaderId),
                            SetArgPointee<3>(leaderAddr),
                            Return(0)));
        EXPECT_CALL(mockChunkService, CreateCloneChunk(_, _, _, _)).Times(1)      // NOLINT
            .WillOnce(DoAll(SetArgPointee<2>(response),
                            Invoke(CreateCloneChunkFunc)));
        copysetClient.CreateCloneChunk(reqCtx->idinfo_,
                                        "destination", sn, 1, 1024, reqDone);
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

    IOSenderOption ioSenderOpt;
    ioSenderOpt.failRequestOpt.chunkserverRPCTimeoutMS = 5000;
    ioSenderOpt.failRequestOpt.chunkserverOPMaxRetry = 3;
    ioSenderOpt.failRequestOpt.chunkserverOPRetryIntervalUS = 500;
    ioSenderOpt.chunkserverEnableAppliedIndexRead = 1;

    CopysetClient copysetClient;
    MockMetaCache mockMetaCache;
    mockMetaCache.DelegateToFake();
    RequestScheduler scheduler;
    copysetClient.Init(&mockMetaCache, ioSenderOpt, &scheduler);

    LogicPoolID logicPoolId = 1;
    CopysetID copysetId = 100001;
    ChunkID chunkId = 1;
    uint64_t sn = 1;

    ChunkServerID leaderId = 10000;
    butil::EndPoint leaderAddr;
    std::string leaderStr = "127.0.0.1:9109";
    butil::str2endpoint(leaderStr.c_str(), &leaderAddr);

    FileMetric fm("test");
    IOTracker iot(nullptr, nullptr, nullptr, &fm);

    /* 非法参数 */
    {
        RequestContext *reqCtx = new FakeRequestContext();
        reqCtx->optype_ = OpType::DELETE_SNAP;
        reqCtx->idinfo_ = ChunkIDInfo(chunkId, logicPoolId, copysetId);


        reqCtx->correctedSeq_ = sn;

        curve::common::CountDownEvent cond(1);
        RequestClosure *reqDone = new FakeRequestClosure(&cond, reqCtx);
        reqDone->SetFileMetric(&fm);
        reqDone->SetIOTracker(&iot);

        reqCtx->done_ = reqDone;
        ChunkResponse response;
        response.set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_INVALID_REQUEST);
        EXPECT_CALL(mockMetaCache, GetLeader(_, _, _, _, _, _))
            .Times(AtLeast(1)).WillOnce(DoAll(SetArgPointee<2>(leaderId),
                                              SetArgPointee<3>(leaderAddr),
                                              Return(0)));
        EXPECT_CALL(mockChunkService, RecoverChunk(_, _, _, _)).Times(1)
            .WillOnce(DoAll(SetArgPointee<2>(response),
                            Invoke(RecoverChunkFunc)));
        copysetClient.RecoverChunk(reqCtx->idinfo_, 0, 4096, reqDone);
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
        reqDone->SetFileMetric(&fm);
        reqDone->SetIOTracker(&iot);

        reqCtx->done_ = reqDone;
        gReadCntlFailedCode = -1;
        EXPECT_CALL(mockMetaCache, GetLeader(_, _, _, _, _, _)).Times(6)
            .WillRepeatedly(DoAll(SetArgPointee<2>(leaderId),
                                  SetArgPointee<3>(leaderAddr),
                                  Return(0)));
        EXPECT_CALL(mockChunkService, RecoverChunk(_, _, _, _)).Times(3)
            .WillRepeatedly(Invoke(RecoverChunkFunc));
        copysetClient.RecoverChunk(reqCtx->idinfo_, 0, 4096, reqDone);
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
        reqDone->SetFileMetric(&fm);
        reqDone->SetIOTracker(&iot);

        reqCtx->done_ = reqDone;
        ChunkResponse response;
        response.set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_FAILURE_UNKNOWN);
        EXPECT_CALL(mockMetaCache, GetLeader(_, _, _, _, _, _))
            .Times(3).WillRepeatedly(DoAll(SetArgPointee<2>(leaderId),
                                           SetArgPointee<3>(leaderAddr),
                                           Return(0)));
        EXPECT_CALL(mockChunkService, RecoverChunk(_, _, _, _)).Times(3)
            .WillRepeatedly(DoAll(SetArgPointee<2>(response),
                                  Invoke(RecoverChunkFunc)));
        copysetClient.RecoverChunk(reqCtx->idinfo_, 0, 4096, reqDone);
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
        reqDone->SetFileMetric(&fm);
        reqDone->SetIOTracker(&iot);

        reqCtx->done_ = reqDone;
        ChunkResponse response1;
        response1.set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_REDIRECTED);
        response1.set_redirect(leaderStr);
        ChunkResponse response2;
        response2.set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS);
        EXPECT_CALL(mockMetaCache, GetLeader(_, _, _, _, _, _)).Times(1)
            .WillOnce(DoAll(SetArgPointee<2>(leaderId),
                            SetArgPointee<3>(leaderAddr),
                            Return(0)));
        EXPECT_CALL(mockChunkService, RecoverChunk(_, _, _, _)).Times(1)
            .WillOnce(DoAll(SetArgPointee<2>(response2),
                            Invoke(RecoverChunkFunc)));
        copysetClient.RecoverChunk(reqCtx->idinfo_, 0, 4096, reqDone);
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
        reqDone->SetFileMetric(&fm);
        reqDone->SetIOTracker(&iot);

        reqCtx->done_ = reqDone;
        ChunkResponse response1;
        response1.set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_REDIRECTED);
        ChunkResponse response2;
        response2.set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS);
        EXPECT_CALL(mockMetaCache, GetLeader(_, _, _, _, _, _)).Times(3)
            .WillOnce(DoAll(SetArgPointee<2>(leaderId),
                            SetArgPointee<3>(leaderAddr),
                            Return(0)))
            .WillOnce(DoAll(SetArgPointee<2>(leaderId),
                            SetArgPointee<3>(leaderAddr),
                            Return(0)))
            .WillOnce(DoAll(SetArgPointee<2>(leaderId),
                            SetArgPointee<3>(leaderAddr),
                            Return(0)));
        EXPECT_CALL(mockChunkService, RecoverChunk(_, _, _, _)).Times(2)
            .WillOnce(DoAll(SetArgPointee<2>(response1),
                            Invoke(RecoverChunkFunc)))
            .WillOnce(DoAll(SetArgPointee<2>(response2),
                            Invoke(RecoverChunkFunc)));
        copysetClient.RecoverChunk(reqCtx->idinfo_, 0, 4096, reqDone);
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
        reqDone->SetFileMetric(&fm);
        reqDone->SetIOTracker(&iot);

        reqCtx->done_ = reqDone;
        ChunkResponse response1;
        response1.set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_REDIRECTED);
        ChunkResponse response2;
        response2.set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS);
        EXPECT_CALL(mockMetaCache, GetLeader(_, _, _, _, _, _)).Times(3)
            .WillOnce(DoAll(SetArgPointee<2>(leaderId),
                            SetArgPointee<3>(leaderAddr),
                            Return(0)))
            .WillOnce(Return(-1))
            .WillOnce(DoAll(SetArgPointee<2>(leaderId),
                            SetArgPointee<3>(leaderAddr),
                            Return(0)));
        EXPECT_CALL(mockChunkService, RecoverChunk(_, _, _, _)).Times(2)
            .WillOnce(DoAll(SetArgPointee<2>(response1),
                            Invoke(RecoverChunkFunc)))
            .WillOnce(DoAll(SetArgPointee<2>(response2),
                            Invoke(RecoverChunkFunc)));
        copysetClient.RecoverChunk(reqCtx->idinfo_, 0, 4096, reqDone);
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
        reqDone->SetFileMetric(&fm);
        reqDone->SetIOTracker(&iot);

        reqCtx->done_ = reqDone;
        ChunkResponse response;
        response.set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_REDIRECTED);
        response.set_redirect(leaderStr);
        EXPECT_CALL(mockMetaCache, GetLeader(_, _, _, _, _, _)).Times(6)
            .WillRepeatedly(DoAll(SetArgPointee<2>(leaderId),
                            SetArgPointee<3>(leaderAddr),
                            Return(0)));
        EXPECT_CALL(mockMetaCache, UpdateLeader(_, _, _)).Times(3)
            .WillRepeatedly(Return(0));
        EXPECT_CALL(mockChunkService, RecoverChunk(_, _, _, _)).Times(3)
            .WillRepeatedly(DoAll(SetArgPointee<2>(response),
                            Invoke(RecoverChunkFunc)));
        copysetClient.RecoverChunk(reqCtx->idinfo_, 0, 4096, reqDone);
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
        reqDone->SetFileMetric(&fm);
        reqDone->SetIOTracker(&iot);

        reqCtx->done_ = reqDone;
        ChunkResponse response;
        response.set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_COPYSET_NOTEXIST);
        response.set_redirect(leaderStr);
        EXPECT_CALL(mockMetaCache, GetLeader(_, _, _, _, _, _)).Times(6)
            .WillRepeatedly(DoAll(SetArgPointee<2>(leaderId),
                            SetArgPointee<3>(leaderAddr),
                            Return(0)));
        EXPECT_CALL(mockChunkService, RecoverChunk(_, _, _, _)).Times(3)
            .WillRepeatedly(DoAll(SetArgPointee<2>(response),
                            Invoke(RecoverChunkFunc)));
        copysetClient.RecoverChunk(reqCtx->idinfo_, 0, 4096, reqDone);
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
        reqDone->SetFileMetric(&fm);
        reqDone->SetIOTracker(&iot);

        reqCtx->done_ = reqDone;
        ChunkResponse response1;
        response1.set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_COPYSET_NOTEXIST);
        response1.set_redirect(leaderStr);
        ChunkResponse response2;
        response2.set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS);
        response2.set_redirect(leaderStr);
        EXPECT_CALL(mockMetaCache, GetLeader(_, _, _, _, _, _)).Times(3)
            .WillRepeatedly(DoAll(SetArgPointee<2>(leaderId),
                            SetArgPointee<3>(leaderAddr),
                            Return(0)));
        EXPECT_CALL(mockChunkService, RecoverChunk(_, _, _, _)).Times(2)
            .WillOnce(DoAll(SetArgPointee<2>(response1),
                            Invoke(RecoverChunkFunc)))
            .WillOnce(DoAll(SetArgPointee<2>(response2),
                            Invoke(RecoverChunkFunc)));
        copysetClient.RecoverChunk(reqCtx->idinfo_, 0, 4096, reqDone);
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
        reqDone->SetFileMetric(&fm);
        reqDone->SetIOTracker(&iot);
        reqCtx->done_ = reqDone;
        ChunkResponse response;
        response.set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS);
        EXPECT_CALL(mockMetaCache, GetLeader(_, _, _, _, _, _)).
                                            Times(AtLeast(1))
            .WillOnce(Return(-1))
            .WillOnce(Return(-1))
            .WillOnce(DoAll(SetArgPointee<2>(leaderId),
                            SetArgPointee<3>(leaderAddr),
                            Return(0)));
        EXPECT_CALL(mockChunkService, RecoverChunk(_, _, _, _)).Times(1)
            .WillOnce(DoAll(SetArgPointee<2>(response),
                            Invoke(RecoverChunkFunc)));
        copysetClient.RecoverChunk(reqCtx->idinfo_,
                                          0, 4096, reqDone);
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

    IOSenderOption ioSenderOpt;
    ioSenderOpt.failRequestOpt.chunkserverRPCTimeoutMS = 5000;
    ioSenderOpt.failRequestOpt.chunkserverOPMaxRetry = 3;
    ioSenderOpt.failRequestOpt.chunkserverOPRetryIntervalUS = 500;
    ioSenderOpt.chunkserverEnableAppliedIndexRead = 1;

    CopysetClient copysetClient;
    MockMetaCache mockMetaCache;
    mockMetaCache.DelegateToFake();
    RequestScheduler scheduler;
    copysetClient.Init(&mockMetaCache, ioSenderOpt, &scheduler);

    LogicPoolID logicPoolId = 1;
    CopysetID copysetId = 100001;
    ChunkID chunkId = 1;

    ChunkServerID leaderId = 10000;
    butil::EndPoint leaderAddr;
    std::string leaderStr = "127.0.0.1:9109";
    butil::str2endpoint(leaderStr.c_str(), &leaderAddr);

    FileMetric fm("test");
    IOTracker iot(nullptr, nullptr, nullptr, &fm);

    /* 非法参数 */
    {
        RequestContext *reqCtx = new FakeRequestContext();
        reqCtx->optype_ = OpType::GET_CHUNK_INFO;
        reqCtx->idinfo_ = ChunkIDInfo(chunkId, logicPoolId, copysetId);



        curve::common::CountDownEvent cond(1);
        RequestClosure *reqDone = new FakeRequestClosure(&cond, reqCtx);
        reqDone->SetFileMetric(&fm);
        reqDone->SetIOTracker(&iot);
        reqCtx->done_ = reqDone;
        GetChunkInfoResponse response;
        response.set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_INVALID_REQUEST);
        EXPECT_CALL(mockMetaCache, GetLeader(_, _, _, _, _, _))
            .Times(AtLeast(1)).WillOnce(DoAll(SetArgPointee<2>(leaderId),
                                              SetArgPointee<3>(leaderAddr),
                                              Return(0)));
        EXPECT_CALL(mockChunkService, GetChunkInfo(_, _, _, _)).Times(1)
            .WillOnce(DoAll(SetArgPointee<2>(response),
                            Invoke(GetChunkInfoFunc)));
        copysetClient.GetChunkInfo(reqCtx->idinfo_, reqDone);
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
        reqDone->SetFileMetric(&fm);
        reqDone->SetIOTracker(&iot);
        reqCtx->done_ = reqDone;
        gReadCntlFailedCode = -1;
        EXPECT_CALL(mockMetaCache, GetLeader(_, _, _, _, _, _)).Times(6)
            .WillRepeatedly(DoAll(SetArgPointee<2>(leaderId),
                                  SetArgPointee<3>(leaderAddr),
                                  Return(0)));
        EXPECT_CALL(mockChunkService, GetChunkInfo(_, _, _, _)).Times(3)
            .WillRepeatedly(Invoke(GetChunkInfoFunc));
        copysetClient.GetChunkInfo(reqCtx->idinfo_, reqDone);
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
        reqDone->SetFileMetric(&fm);
        reqDone->SetIOTracker(&iot);
        reqCtx->done_ = reqDone;
        GetChunkInfoResponse response;
        response.set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_FAILURE_UNKNOWN);
        EXPECT_CALL(mockMetaCache, GetLeader(_, _, _, _, _, _))
            .Times(3).WillRepeatedly(DoAll(SetArgPointee<2>(leaderId),
                                           SetArgPointee<3>(leaderAddr),
                                           Return(0)));
        EXPECT_CALL(mockChunkService, GetChunkInfo(_, _, _, _)).Times(3)
            .WillRepeatedly(DoAll(SetArgPointee<2>(response),
                                  Invoke(GetChunkInfoFunc)));
        copysetClient.GetChunkInfo(reqCtx->idinfo_, reqDone);
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
        reqDone->SetFileMetric(&fm);
        reqDone->SetIOTracker(&iot);
        reqCtx->done_ = reqDone;
        GetChunkInfoResponse response1;
        response1.set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_REDIRECTED);
        response1.set_redirect(leaderStr);
        GetChunkInfoResponse response2;
        response2.set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS);
        EXPECT_CALL(mockMetaCache, GetLeader(_, _, _, _, _, _)).Times(3)
            .WillRepeatedly(DoAll(SetArgPointee<2>(leaderId),
                            SetArgPointee<3>(leaderAddr),
                            Return(0)));
        EXPECT_CALL(mockMetaCache, UpdateLeader(_, _, _)).Times(1)
            .WillOnce(Return(0));
        EXPECT_CALL(mockChunkService, GetChunkInfo(_, _, _, _)).Times(2)
            .WillOnce(DoAll(SetArgPointee<2>(response1),
                            Invoke(GetChunkInfoFunc)))
            .WillOnce(DoAll(SetArgPointee<2>(response2),
                            Invoke(GetChunkInfoFunc)));
        copysetClient.GetChunkInfo(reqCtx->idinfo_, reqDone);
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
        reqDone->SetFileMetric(&fm);
        reqDone->SetIOTracker(&iot);
        reqCtx->done_ = reqDone;
        GetChunkInfoResponse response1;
        response1.set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_REDIRECTED);
        GetChunkInfoResponse response2;
        response2.set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS);
        EXPECT_CALL(mockMetaCache, GetLeader(_, _, _, _, _, _)).Times(3)
            .WillRepeatedly(DoAll(SetArgPointee<2>(leaderId),
                            SetArgPointee<3>(leaderAddr),
                            Return(0)));
        EXPECT_CALL(mockChunkService, GetChunkInfo(_, _, _, _)).Times(2)
            .WillOnce(DoAll(SetArgPointee<2>(response1),
                            Invoke(GetChunkInfoFunc)))
            .WillOnce(DoAll(SetArgPointee<2>(response2),
                            Invoke(GetChunkInfoFunc)));
        copysetClient.GetChunkInfo(reqCtx->idinfo_, reqDone);
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
        reqDone->SetFileMetric(&fm);
        reqDone->SetIOTracker(&iot);
        reqCtx->done_ = reqDone;
        GetChunkInfoResponse response1;
        response1.set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_REDIRECTED);
        GetChunkInfoResponse response2;
        response2.set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS);
        EXPECT_CALL(mockMetaCache, GetLeader(_, _, _, _, _, _)).Times(3)
            .WillOnce(DoAll(SetArgPointee<2>(leaderId),
                            SetArgPointee<3>(leaderAddr),
                            Return(0)))
            .WillOnce(Return(-1))
            .WillOnce(DoAll(SetArgPointee<2>(leaderId),
                            SetArgPointee<3>(leaderAddr),
                            Return(0)));
        EXPECT_CALL(mockChunkService, GetChunkInfo(_, _, _, _)).Times(2)
            .WillOnce(DoAll(SetArgPointee<2>(response1),
                            Invoke(GetChunkInfoFunc)))
            .WillOnce(DoAll(SetArgPointee<2>(response2),
                            Invoke(GetChunkInfoFunc)));
        copysetClient.GetChunkInfo(reqCtx->idinfo_, reqDone);
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
        reqDone->SetFileMetric(&fm);
        reqDone->SetIOTracker(&iot);
        reqCtx->done_ = reqDone;
        GetChunkInfoResponse response;
        response.set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_REDIRECTED);
        response.set_redirect(leaderStr);
        EXPECT_CALL(mockMetaCache, GetLeader(_, _, _, _, _, _)).Times(6)
            .WillRepeatedly(DoAll(SetArgPointee<2>(leaderId),
                            SetArgPointee<3>(leaderAddr),
                            Return(0)));
        EXPECT_CALL(mockMetaCache, UpdateLeader(_, _, _)).Times(3)
            .WillRepeatedly(Return(0));
        EXPECT_CALL(mockChunkService, GetChunkInfo(_, _, _, _)).Times(3)
            .WillRepeatedly(DoAll(SetArgPointee<2>(response),
                            Invoke(GetChunkInfoFunc)));
        copysetClient.GetChunkInfo(reqCtx->idinfo_, reqDone);
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
        reqDone->SetFileMetric(&fm);
        reqDone->SetIOTracker(&iot);
        reqCtx->done_ = reqDone;
        GetChunkInfoResponse response;
        response.set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_COPYSET_NOTEXIST);
        response.set_redirect(leaderStr);
        EXPECT_CALL(mockMetaCache, GetLeader(_, _, _, _, _, _)).Times(6)
            .WillRepeatedly(DoAll(SetArgPointee<2>(leaderId),
                            SetArgPointee<3>(leaderAddr),
                            Return(0)));
        EXPECT_CALL(mockChunkService, GetChunkInfo(_, _, _, _)).Times(3)
            .WillRepeatedly(DoAll(SetArgPointee<2>(response),
                            Invoke(GetChunkInfoFunc)));
        copysetClient.GetChunkInfo(reqCtx->idinfo_, reqDone);
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
        reqDone->SetFileMetric(&fm);
        reqDone->SetIOTracker(&iot);
        reqCtx->done_ = reqDone;
        GetChunkInfoResponse response1;
        response1.set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_COPYSET_NOTEXIST);
        response1.set_redirect(leaderStr);
        GetChunkInfoResponse response2;
        response2.set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS);
        response2.set_redirect(leaderStr);
        EXPECT_CALL(mockMetaCache, GetLeader(_, _, _, _, _, _)).Times(3)
            .WillOnce(DoAll(SetArgPointee<2>(leaderId),
                            SetArgPointee<3>(leaderAddr),
                            Return(0)))
            .WillOnce(DoAll(SetArgPointee<2>(leaderId),
                            SetArgPointee<3>(leaderAddr),
                            Return(0)))
            .WillOnce(DoAll(SetArgPointee<2>(leaderId),
                            SetArgPointee<3>(leaderAddr),
                            Return(0)));
        EXPECT_CALL(mockChunkService, GetChunkInfo(_, _, _, _)).Times(2)
            .WillOnce(DoAll(SetArgPointee<2>(response1),
                            Invoke(GetChunkInfoFunc)))
            .WillOnce(DoAll(SetArgPointee<2>(response2),
                            Invoke(GetChunkInfoFunc)));
        copysetClient.GetChunkInfo(reqCtx->idinfo_, reqDone);
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
        reqDone->SetFileMetric(&fm);
        reqDone->SetIOTracker(&iot);
        reqCtx->done_ = reqDone;
        GetChunkInfoResponse response;
        response.set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS);
        EXPECT_CALL(mockMetaCache, GetLeader(_, _, _, _, _, _)).
                    Times(AtLeast(1))
            .WillOnce(DoAll(SetArgPointee<2>(leaderId),
                            SetArgPointee<3>(leaderAddr),
                            Return(-1)))
            .WillOnce(DoAll(SetArgPointee<2>(leaderId),
                            SetArgPointee<3>(leaderAddr),
                            Return(-1)))
            .WillOnce(DoAll(SetArgPointee<2>(leaderId),
                            SetArgPointee<3>(leaderAddr),
                            Return(0)));
        EXPECT_CALL(mockChunkService, GetChunkInfo(_, _, _, _)).Times(1)
            .WillOnce(DoAll(SetArgPointee<2>(response),
                            Invoke(GetChunkInfoFunc)));
        copysetClient.GetChunkInfo(reqCtx->idinfo_, reqDone);
        cond.Wait();
        ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS,
                  reqDone->GetErrorCode());
    }
}

namespace {

bool gWriteSuccessFlag = false;

void WriteCallBack(CurveAioContext* aioctx) {
    gWriteSuccessFlag = true;
    delete aioctx;
}

void PrepareOpenFile(FakeCurveFSService *service,
                     OpenFileResponse *openresp,
                     FakeReturn *fakeReturn) {
    openresp->set_statuscode(curve::mds::StatusCode::kOK);
    auto *session = openresp->mutable_protosession();
    session->set_sessionid("xxx");
    session->set_leasetime(10000);
    session->set_createtime(10000);
    session->set_sessionstatus(curve::mds::SessionStatus::kSessionOK);
    auto *fileinfo = openresp->mutable_fileinfo();
    fileinfo->set_id(1);
    fileinfo->set_filename("filename");
    fileinfo->set_parentid(0);
    fileinfo->set_length(10ULL * 1024 * 1024 * 1024);
    fileinfo->set_blocksize(4096);

    *fakeReturn = FakeReturn(nullptr, static_cast<void *>(openresp));

    service->SetOpenFile(fakeReturn);
}

}  // namespace

TEST(ChunkServerBackwardTest, ChunkServerBackwardTest) {
    const std::string endpoint = "127.0.0.1:9102";

    ClientConfig cc;
    const std::string& configPath = "./conf/client.conf";
    cc.Init(configPath.c_str());
    FileInstance fileinstance;
    UserInfo_t userinfo;
    userinfo.owner = "userinfo";

    std::shared_ptr<MDSClient> mdsclient = std::make_shared<MDSClient>();

    // set mds addr
    auto mdsopts = cc.GetFileServiceOption().metaServerOpt;
    mdsopts.mdsAddrs.clear();
    mdsopts.mdsAddrs.push_back(endpoint);

    ASSERT_EQ(LIBCURVE_ERROR::OK, mdsclient->Initialize(mdsopts));
    ASSERT_TRUE(fileinstance.Initialize(
        "/test", mdsclient, userinfo, OpenFlags{}, cc.GetFileServiceOption()));

    // create fake chunkserver service
    FakeChunkServerService fakechunkservice;
    // 设置cli服务
    CliServiceFake fakeCliservice;

    FakeCurveFSService curvefsService;
    OpenFileResponse openresp;
    FakeReturn fakeReturn;

    PrepareOpenFile(&curvefsService, &openresp, &fakeReturn);

    brpc::Server server;
    ASSERT_EQ(0, server.AddService(&fakechunkservice,
        brpc::SERVER_DOESNT_OWN_SERVICE)) << "Fail to add fakechunkservice";
    ASSERT_EQ(0, server.AddService(&fakeCliservice,
        brpc::SERVER_DOESNT_OWN_SERVICE)) << "Fail to add fakecliservice";
    ASSERT_EQ(
        0, server.AddService(&curvefsService, brpc::SERVER_DOESNT_OWN_SERVICE))
        << "Fail to add curvefsService";

    ASSERT_EQ(0, server.Start(endpoint.c_str(), nullptr))
        << "Fail to start server at " << endpoint;

    // fill metacache
    curve::client::MetaCache* mc
        = fileinstance.GetIOManager4File()->GetMetaCache();
    curve::client::ChunkIDInfo_t chunkinfo(1, 2, 3);
    mc->UpdateChunkInfoByIndex(0, chunkinfo);
    curve::client::CopysetInfo cpinfo;
    curve::client::EndPoint ep;
    butil::str2endpoint("127.0.0.1", 9102, &ep);

    braft::PeerId pd(ep);
    curve::client::ChunkServerAddr addr = curve::client::ChunkServerAddr(ep);
    curve::client::CopysetPeerInfo peer(1, addr, addr);
    cpinfo.csinfos_.push_back(peer);
    mc->UpdateCopysetInfo(2, 3, cpinfo);

    fakeCliservice.SetPeerID(pd);

    curve::chunkserver::ChunkResponse response;
    response.set_status(
        curve::chunkserver::CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS);
    response.set_appliedindex(0);
    FakeReturn writeFakeRet(nullptr, static_cast<void*>(&response));
    fakechunkservice.SetFakeWriteReturn(&writeFakeRet);

    const int kNewFileSn = 100;
    const int kOldFileSn = 30;

    ASSERT_EQ(LIBCURVE_ERROR::OK, fileinstance.Open());

    // 设置文件版本号
    fileinstance.GetIOManager4File()->SetLatestFileSn(kNewFileSn);

    // 发送写请求，并等待sec秒后检查io是否返回
    auto startWriteAndCheckResult = [&fileinstance](int sec)-> bool {  // NOLINT
        CurveAioContext* aioctx = new CurveAioContext();
        char buffer[4096];

        aioctx->buf = buffer;
        aioctx->offset = 0;
        aioctx->length = sizeof(buffer);
        aioctx->op = LIBCURVE_OP::LIBCURVE_OP_WRITE;
        aioctx->cb = WriteCallBack;

        // 下发写请求
        fileinstance.AioWrite(aioctx, UserDataType::RawBuffer);

        std::this_thread::sleep_for(std::chrono::seconds(sec));
        return gWriteSuccessFlag;
    };

    // 第一次写成功，并更新chunkserver端的文件版本号
    ASSERT_TRUE(startWriteAndCheckResult(3));

    // 设置一个旧的版本号去写
    fileinstance.GetIOManager4File()->SetLatestFileSn(kOldFileSn);
    gWriteSuccessFlag = false;

    // chunkserver返回backward，重新获取版本号后还是旧的版本
    // IO hang
    ASSERT_FALSE(startWriteAndCheckResult(3));

    // 更新版本号为正常状态
    fileinstance.GetIOManager4File()->SetLatestFileSn(kNewFileSn);
    std::this_thread::sleep_for(std::chrono::seconds(1));

    // 上次写请求成功
    ASSERT_EQ(true, gWriteSuccessFlag);

    server.Stop(0);
    server.Join();
}

TEST_F(CopysetClientTest, retry_rpc_sleep_test) {
    MockChunkServiceImpl mockChunkService;
    ASSERT_EQ(
        server_->AddService(&mockChunkService, brpc::SERVER_DOESNT_OWN_SERVICE),
        0);
    ASSERT_EQ(server_->Start(listenAddr_.c_str(), nullptr), 0);

    const uint64_t sleepUsBeforeRetry = 5 * 1000 * 1000;

    IOSenderOption ioSenderOpt;
    ioSenderOpt.failRequestOpt.chunkserverRPCTimeoutMS = 1000;
    ioSenderOpt.failRequestOpt.chunkserverOPMaxRetry = 3;
    ioSenderOpt.failRequestOpt.chunkserverOPRetryIntervalUS =
        sleepUsBeforeRetry;
    ioSenderOpt.failRequestOpt.chunkserverMaxRPCTimeoutMS = 3500;
    ioSenderOpt.failRequestOpt.chunkserverMaxRetrySleepIntervalUS = 3500000;
    ioSenderOpt.chunkserverEnableAppliedIndexRead = 1;

    RequestScheduleOption reqopt;
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
    uint64_t fileId = 1;
    uint64_t epoch = 1;
    size_t len = 8;
    char buff1[8] = {0};
    butil::IOBuf iobuf;
    iobuf.append(buff1, sizeof(len));
    off_t offset = 0;

    ChunkServerID leaderId = 10000;
    ChunkServerID leaderId2 = 10001;
    butil::EndPoint leaderAddr;
    std::string leaderStr = "127.0.0.1:9109";
    butil::str2endpoint(leaderStr.c_str(), &leaderAddr);

    FileMetric fm("test");
    IOTracker iot(nullptr, nullptr, nullptr, &fm);

    {
        // redirect情况下, chunkserver返回新的leader
        // 重试之前不会睡眠
        RequestContext* reqCtx = new FakeRequestContext();
        reqCtx->optype_ = OpType::WRITE;
        reqCtx->idinfo_ = ChunkIDInfo(chunkId, logicPoolId, copysetId);
        // reqCtx->writeBuffer_ = buff1;
        reqCtx->writeData_ = iobuf;
        reqCtx->offset_ = offset;
        reqCtx->rawlength_ = len;

        reqCtx->fileId_ = fileId;
        reqCtx->epoch_ = epoch;
        reqCtx->seq_ = 0;

        curve::common::CountDownEvent cond(1);
        RequestClosure* reqDone = new FakeRequestClosure(&cond, reqCtx);
        reqDone->SetFileMetric(&fm);
        reqDone->SetIOTracker(&iot);
        reqCtx->done_ = reqDone;

        ChunkResponse response1;
        response1.set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_REDIRECTED);
        response1.set_redirect(leaderStr);

        ChunkResponse response2;
        response2.set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS);

        EXPECT_CALL(mockMetaCache, GetLeader(_, _, _, _, _, _))
            .Times(3)
            .WillOnce(DoAll(SetArgPointee<2>(leaderId),
                            SetArgPointee<3>(leaderAddr), Return(0)))
            .WillRepeatedly(DoAll(SetArgPointee<2>(leaderId2),
                            SetArgPointee<3>(leaderAddr), Return(0)));
        EXPECT_CALL(mockMetaCache, UpdateLeader(_, _, _))
            .Times(1)
            .WillOnce(Return(0));
        EXPECT_CALL(mockChunkService, WriteChunk(_, _, _, _))
            .Times(2)
            .WillOnce(
                DoAll(SetArgPointee<2>(response1), Invoke(WriteChunkFunc)))
            .WillOnce(
                DoAll(SetArgPointee<2>(response2), Invoke(WriteChunkFunc)));

        auto startUs = curve::common::TimeUtility::GetTimeofDayUs();
        copysetClient.WriteChunk(reqCtx, reqDone);
        cond.Wait();
        auto endUs = curve::common::TimeUtility::GetTimeofDayUs();

        // 返回新的leader id，所以重试之前不会进行睡眠
        ASSERT_LE(endUs - startUs, sleepUsBeforeRetry / 10);
        ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS,
                  reqDone->GetErrorCode());
    }

    {
        // redirect情况下,chunkserver返回旧leader
        // 重试之前会睡眠
        RequestContext* reqCtx = new FakeRequestContext();
        reqCtx->optype_ = OpType::WRITE;
        reqCtx->idinfo_ = ChunkIDInfo(chunkId, logicPoolId, copysetId);
        // reqCtx->writeBuffer_ = buff1;
        reqCtx->writeData_ = iobuf;
        reqCtx->offset_ = offset;
        reqCtx->rawlength_ = len;

        reqCtx->fileId_ = fileId;
        reqCtx->epoch_ = epoch;
        reqCtx->seq_ = 0;

        curve::common::CountDownEvent cond(1);
        RequestClosure* reqDone = new FakeRequestClosure(&cond, reqCtx);
        reqDone->SetFileMetric(&fm);
        reqDone->SetIOTracker(&iot);
        reqCtx->done_ = reqDone;

        ChunkResponse response1;
        response1.set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_REDIRECTED);
        response1.set_redirect(leaderStr);

        ChunkResponse response2;
        response2.set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS);

        EXPECT_CALL(mockMetaCache, GetLeader(_, _, _, _, _, _))
            .Times(3)
            .WillRepeatedly(DoAll(SetArgPointee<2>(leaderId),
                            SetArgPointee<3>(leaderAddr), Return(0)));
        EXPECT_CALL(mockMetaCache, UpdateLeader(_, _, _))
            .Times(1)
            .WillOnce(Return(0));
        EXPECT_CALL(mockChunkService, WriteChunk(_, _, _, _))
            .Times(2)
            .WillOnce(
                DoAll(SetArgPointee<2>(response1), Invoke(WriteChunkFunc)))
            .WillOnce(
                DoAll(SetArgPointee<2>(response2), Invoke(WriteChunkFunc)));
        auto startUs = curve::common::TimeUtility::GetTimeofDayUs();
        copysetClient.WriteChunk(reqCtx, reqDone);
        cond.Wait();
        auto endUs = curve::common::TimeUtility::GetTimeofDayUs();

        // 返回同样的leader id，重试之前会进行睡眠
        ASSERT_GE(endUs - startUs, sleepUsBeforeRetry / 10);
        ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS,
                  reqDone->GetErrorCode());
    }

    {
        // redirect情况下,chunkserver未返回leader
        // 主动refresh获取到新leader
        RequestContext* reqCtx = new FakeRequestContext();
        reqCtx->optype_ = OpType::WRITE;
        reqCtx->idinfo_ = ChunkIDInfo(chunkId, logicPoolId, copysetId);
        // reqCtx->writeBuffer_ = buff1;
        reqCtx->writeData_ = iobuf;
        reqCtx->offset_ = offset;
        reqCtx->rawlength_ = len;

        reqCtx->fileId_ = fileId;
        reqCtx->epoch_ = epoch;
        reqCtx->seq_ = 0;

        curve::common::CountDownEvent cond(1);
        RequestClosure* reqDone = new FakeRequestClosure(&cond, reqCtx);
        reqDone->SetFileMetric(&fm);
        reqDone->SetIOTracker(&iot);
        reqCtx->done_ = reqDone;

        ChunkResponse response1;
        response1.set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_REDIRECTED);

        ChunkResponse response2;
        response2.set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS);

        EXPECT_CALL(mockMetaCache, GetLeader(_, _, _, _, _, _))
            .Times(3)
            .WillOnce(DoAll(SetArgPointee<2>(leaderId),
                            SetArgPointee<3>(leaderAddr), Return(0)))
            .WillRepeatedly(DoAll(SetArgPointee<2>(leaderId2),
                            SetArgPointee<3>(leaderAddr), Return(0)));
        EXPECT_CALL(mockMetaCache, UpdateLeader(_, _, _)).Times(0);
        EXPECT_CALL(mockChunkService, WriteChunk(_, _, _, _))
            .Times(2)
            .WillOnce(
                DoAll(SetArgPointee<2>(response1), Invoke(WriteChunkFunc)))
            .WillOnce(
                DoAll(SetArgPointee<2>(response2), Invoke(WriteChunkFunc)));
        auto startUs = curve::common::TimeUtility::GetTimeofDayUs();
        copysetClient.WriteChunk(reqCtx, reqDone);
        cond.Wait();
        auto endUs = curve::common::TimeUtility::GetTimeofDayUs();

        // 返回新的leader id，所以重试之前不会进行睡眠
        ASSERT_LE(endUs - startUs, sleepUsBeforeRetry / 10);
        ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS,
                  reqDone->GetErrorCode());
    }

    {
        // redirect情况下,chunkserver未返回leader
        // 主动refresh获取到旧leader
        RequestContext* reqCtx = new FakeRequestContext();
        reqCtx->optype_ = OpType::WRITE;
        reqCtx->idinfo_ = ChunkIDInfo(chunkId, logicPoolId, copysetId);
        // reqCtx->writeBuffer_ = buff1;
        reqCtx->writeData_ = iobuf;
        reqCtx->offset_ = offset;
        reqCtx->rawlength_ = len;

        reqCtx->fileId_ = fileId;
        reqCtx->epoch_ = epoch;
        reqCtx->seq_ = 0;

        curve::common::CountDownEvent cond(1);
        RequestClosure* reqDone = new FakeRequestClosure(&cond, reqCtx);
        reqDone->SetFileMetric(&fm);
        reqDone->SetIOTracker(&iot);
        reqCtx->done_ = reqDone;

        ChunkResponse response1;
        response1.set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_REDIRECTED);

        ChunkResponse response2;
        response2.set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS);

        EXPECT_CALL(mockMetaCache, GetLeader(_, _, _, _, _, _))
            .Times(3)
            .WillRepeatedly(DoAll(SetArgPointee<2>(leaderId),
                            SetArgPointee<3>(leaderAddr), Return(0)));
        EXPECT_CALL(mockMetaCache, UpdateLeader(_, _, _)).Times(0);
        EXPECT_CALL(mockChunkService, WriteChunk(_, _, _, _))
            .Times(2)
            .WillOnce(
                DoAll(SetArgPointee<2>(response1), Invoke(WriteChunkFunc)))
            .WillOnce(
                DoAll(SetArgPointee<2>(response2), Invoke(WriteChunkFunc)));
        auto startUs = curve::common::TimeUtility::GetTimeofDayUs();
        copysetClient.WriteChunk(reqCtx, reqDone);
        cond.Wait();
        auto endUs = curve::common::TimeUtility::GetTimeofDayUs();

        ASSERT_GE(endUs - startUs, sleepUsBeforeRetry / 10);
        ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS,
                  reqDone->GetErrorCode());
    }
    scheduler.Fini();
}

class TestRunnedRequestClosure : public RequestClosure {
 public:
    TestRunnedRequestClosure() : RequestClosure(nullptr) {}

    void Run() override {
        runned_ = true;
    }

    bool IsRunned() const {
        return runned_;
    }

 private:
    bool runned_ = false;
};

// 测试session失效后，重试请求会被重新放入请求队列
TEST(CopysetClientBasicTest, TestReScheduleWhenSessionNotValid) {
    MockRequestScheduler requestScheduler;
    CopysetClient copysetClient;
    IOSenderOption ioSenderOption;
    MetaCache metaCache;

    ASSERT_EQ(0, copysetClient.Init(&metaCache, ioSenderOption,
                                    &requestScheduler, nullptr));

    // 设置session not valid
    copysetClient.StartRecycleRetryRPC();

    {
        EXPECT_CALL(requestScheduler, ReSchedule(_))
            .Times(1);

        TestRunnedRequestClosure closure;
        copysetClient.ReadChunk(new FakeRequestContext(), &closure);
        ASSERT_FALSE(closure.IsRunned());
    }

    {
        EXPECT_CALL(requestScheduler, ReSchedule(_))
            .Times(1);

        TestRunnedRequestClosure closure;
        copysetClient.WriteChunk(new FakeRequestContext(), &closure);
        ASSERT_FALSE(closure.IsRunned());
    }
}

}   // namespace client
}   // namespace curve
