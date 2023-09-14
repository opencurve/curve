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
#include "test/client/mock/mock_meta_cache.h"
#include "src/common/concurrent/count_down_event.h"
#include "test/client/mock/mock_chunkservice.h"
#include "test/client/mock/mock_request_context.h"
#include "src/client/chunk_closure.h"
#include "src/common/timeutility.h"
#include "test/client/fake/fakeChunkserver.h"
#include "test/client/mock/mock_request_scheduler.h"
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

/* TODO(wudemiao) current controller error cannot be returned through mock */
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
        copysetClient.WriteChunk(reqCtx->idinfo_, fileId, epoch, 0,
                                 iobuf, offset, len, {}, reqDone);
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
        copysetClient.WriteChunk(reqCtx->idinfo_, fileId, epoch, 0,
                                 iobuf, offset, len, {}, reqDone);
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
        copysetClient.WriteChunk(reqCtx->idinfo_, fileId, epoch, 0,
                                 iobuf, offset, len, {}, reqDone);
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
        copysetClient.ReadChunk(reqCtx->idinfo_, sn,
                                offset, len, {}, reqDone);
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
        copysetClient.ReadChunk(reqCtx->idinfo_, sn,
                                offset, len, {}, reqDone);
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
        copysetClient.ReadChunk(reqCtx->idinfo_, sn,
                                offset, len, {}, reqDone);
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

    /* Illegal parameter */
    {
        RequestContext *reqCtx = new FakeRequestContext();
        reqCtx->optype_ = OpType::WRITE;
        reqCtx->idinfo_ = ChunkIDInfo(chunkId, logicPoolId, copysetId);

        reqCtx->writeData_ = iobuf;
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
        EXPECT_CALL(mockChunkService, WriteChunk(_, _, _, _)).Times(1)
            .WillOnce(DoAll(SetArgPointee<2>(response),
                            Invoke(WriteChunkFunc)));
        copysetClient.WriteChunk(reqCtx->idinfo_, fileId, epoch, 0,
                                 iobuf, offset, len, {}, reqDone);
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
        reqCtx->offset_ = 0;
        reqCtx->rawlength_ = len;

        // The retry sleep time set in the configuration file is 5000, as there is no triggering of underlying index backoff, so there will be no sleep between retries
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
        copysetClient.WriteChunk(reqCtx->idinfo_, fileId, epoch, 0,
                                 iobuf, offset, len, {}, reqDone);
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
        reqCtx->offset_ = 0;
        reqCtx->rawlength_ = len;

        curve::common::CountDownEvent cond(1);
        RequestClosure *reqDone = new FakeRequestClosure(&cond, reqCtx);
        reqDone->SetFileMetric(&fm);
        reqDone->SetIOTracker(&iot);

        // The retry timeout set by the configuration file is 5000 because the chunkserver setting returns timeout
        // Causing the triggering of an exponential backoff of the underlying timeout time, increasing the interval between each retry. Retrying three times is normal, only 3 * 1000 sleep is required
        // But after increasing the index backoff, the timeout will increase to 1000+2000+2000=5000
        // Adding random factors, the three retry times should be greater than 7000 and less than 8000
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
        copysetClient.WriteChunk(reqCtx->idinfo_, fileId, epoch, 0,
                                 iobuf, offset, len, {}, reqDone);
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
        reqCtx->offset_ = 0;
        reqCtx->rawlength_ = len;

        curve::common::CountDownEvent cond(1);
        RequestClosure *reqDone = new FakeRequestClosure(&cond, reqCtx);
        reqDone->SetFileMetric(&fm);
        reqDone->SetIOTracker(&iot);

        // The retry sleep time set in the configuration file is 5000 because the chunkserver setting returns timeout
        // Causing triggering of low-level exponential backoff, increasing the interval between each retry. Retrying three times is normal, only 3 * 5000 sleep is required
        // But after increasing the index retreat, the sleep interval will increase to 10000+20000=30000
        // Adding random factors, the three retry times should be greater than 29000 and less than 50000
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
        copysetClient.WriteChunk(reqCtx->idinfo_, fileId, epoch, 0,
                                 iobuf, offset, len, {}, reqDone);
        cond.Wait();
        ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_OVERLOAD,
                 reqDone->GetErrorCode());

        uint64_t end = TimeUtility::GetTimeofDayUs();
        ASSERT_GT(end - start, 28000);
        ASSERT_LT(end - start, 2 * 50000);
        gWriteCntlFailedCode = 0;
    }

    /* Other errors */
    {
        RequestContext *reqCtx = new FakeRequestContext();
        reqCtx->optype_ = OpType::WRITE;
        reqCtx->idinfo_ = ChunkIDInfo(chunkId, logicPoolId, copysetId);

        reqCtx->writeData_ = iobuf;
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
        EXPECT_CALL(mockChunkService, WriteChunk(_, _, _, _)).Times(3)
            .WillRepeatedly(DoAll(SetArgPointee<2>(response),
                                  Invoke(WriteChunkFunc)));
        copysetClient.WriteChunk(reqCtx->idinfo_, fileId, epoch, 0,
                                 iobuf, offset, len, {}, reqDone);
        cond.Wait();
        ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_FAILURE_UNKNOWN,
                 reqDone->GetErrorCode());
    }
    /* Not a leader, returning the correct leader */
    {
        RequestContext *reqCtx = new FakeRequestContext();
        reqCtx->optype_ = OpType::WRITE;
        reqCtx->idinfo_ = ChunkIDInfo(chunkId, logicPoolId, copysetId);

        reqCtx->writeData_ = iobuf;
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
        EXPECT_CALL(mockChunkService, WriteChunk(_, _, _, _)).Times(2)
            .WillOnce(DoAll(SetArgPointee<2>(response1),
                            Invoke(WriteChunkFunc)))
            .WillOnce(DoAll(SetArgPointee<2>(response2),
                            Invoke(WriteChunkFunc)));
        copysetClient.WriteChunk(reqCtx->idinfo_, fileId, epoch, 0,
                                 iobuf, offset, len, {}, reqDone);
        cond.Wait();
        ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS,
                  reqDone->GetErrorCode());

        ASSERT_EQ(1, fm.writeRPC.redirectQps.count.get_value());
    }
    /* Not a leader, did not return a leader, refreshing the meta cache succeeded */
    {
        RequestContext *reqCtx = new FakeRequestContext();
        reqCtx->optype_ = OpType::WRITE;
        reqCtx->idinfo_ = ChunkIDInfo(chunkId, logicPoolId, copysetId);

        reqCtx->writeData_ = iobuf;
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
            .WillRepeatedly(DoAll(SetArgPointee<2>(leaderId),
                            SetArgPointee<3>(leaderAddr),
                            Return(0)));
        EXPECT_CALL(mockChunkService, WriteChunk(_, _, _, _)).Times(2)
            .WillOnce(DoAll(SetArgPointee<2>(response1),
                            Invoke(WriteChunkFunc)))
            .WillOnce(DoAll(SetArgPointee<2>(response2),
                            Invoke(WriteChunkFunc)));

        copysetClient.WriteChunk(reqCtx->idinfo_, fileId, epoch, 0,
                                 iobuf, offset, len, {}, reqDone);
        cond.Wait();
        ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS,
                  reqDone->GetErrorCode());
    }
    /* Not a leader, did not return a leader, refreshing the meta cache failed */
    {
        RequestContext *reqCtx = new FakeRequestContext();
        reqCtx->optype_ = OpType::WRITE;
        reqCtx->idinfo_ = ChunkIDInfo(chunkId, logicPoolId, copysetId);

        reqCtx->writeData_ = iobuf;
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
            .WillOnce(Return(-1))
            .WillOnce(DoAll(SetArgPointee<2>(leaderId),
                            SetArgPointee<3>(leaderAddr),
                            Return(0)));
        EXPECT_CALL(mockChunkService, WriteChunk(_, _, _, _)).Times(2)
            .WillOnce(DoAll(SetArgPointee<2>(response1),
                            Invoke(WriteChunkFunc)))
            .WillOnce(DoAll(SetArgPointee<2>(response2),
                            Invoke(WriteChunkFunc)));
        copysetClient.WriteChunk(reqCtx->idinfo_, fileId, epoch, 0,
                                 iobuf, offset, len, {}, reqDone);
        cond.Wait();
        ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS,
                  reqDone->GetErrorCode());
    }
    /* Not a leader, but returned an incorrect leader */
    {
        RequestContext *reqCtx = new FakeRequestContext();
        reqCtx->optype_ = OpType::WRITE;
        reqCtx->idinfo_ = ChunkIDInfo(chunkId, logicPoolId, copysetId);

        reqCtx->writeData_ = iobuf;
        reqCtx->offset_ = 0;
        reqCtx->rawlength_ = len;

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
        copysetClient.WriteChunk(reqCtx->idinfo_, fileId, epoch, 0,
                                 iobuf, offset, len, {}, reqDone);
        cond.Wait();
        auto elpased = curve::common::TimeUtility::GetTimeofDayUs()
                     - startTimeUs;
        // chunkserverOPRetryIntervalUS = 5000
        // redirect sleep for 500us each time and retry a total of 2 times (chunkserverOPMaxRetry=3, returns if it is greater than or equal to, so only two retries were made)
        // So the total time spent is greater than 1000us
        ASSERT_GE(elpased, 1000);
        ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_REDIRECTED,
                  reqDone->GetErrorCode());
        ASSERT_EQ(3, fm.writeRPC.redirectQps.count.get_value());
    }
    /* copyset does not exist, updating leader still failed */
    {
        RequestContext *reqCtx = new FakeRequestContext();
        reqCtx->optype_ = OpType::WRITE;
        reqCtx->idinfo_ = ChunkIDInfo(chunkId, logicPoolId, copysetId);

        reqCtx->writeData_ = iobuf;
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
        EXPECT_CALL(mockChunkService, WriteChunk(_, _, _, _)).Times(3)
            .WillRepeatedly(DoAll(SetArgPointee<2>(response),
                            Invoke(WriteChunkFunc)));
        copysetClient.WriteChunk(reqCtx->idinfo_, fileId, epoch, 0,
                                 iobuf, offset, len, {}, reqDone);
        cond.Wait();
        ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_COPYSET_NOTEXIST,
                  reqDone->GetErrorCode());
    }
    /* copyset does not exist, updating leader succeeded */
    {
        RequestContext *reqCtx = new FakeRequestContext();
        reqCtx->optype_ = OpType::WRITE;
        reqCtx->idinfo_ = ChunkIDInfo(chunkId, logicPoolId, copysetId);

        reqCtx->writeData_ = iobuf;
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
        EXPECT_CALL(mockChunkService, WriteChunk(_, _, _, _)).Times(2)
            .WillOnce(DoAll(SetArgPointee<2>(response1),
                            Invoke(WriteChunkFunc)))
            .WillOnce(DoAll(SetArgPointee<2>(response2),
                            Invoke(WriteChunkFunc)));
        copysetClient.WriteChunk(reqCtx->idinfo_, fileId, epoch, 0,
                                 iobuf, offset, len, {}, reqDone);
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
        reqCtx->offset_ = 0;
        reqCtx->rawlength_ = len;

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
        copysetClient.WriteChunk(reqCtx->idinfo_, fileId, epoch, 0,
                                 iobuf, offset, len, {}, reqDone);
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
        reqCtx->offset_ = 0;
        reqCtx->rawlength_ = len;

        curve::common::CountDownEvent cond(1);
        RequestClosure *reqDone = new FakeRequestClosure(&cond, reqCtx);
        reqDone->SetFileMetric(&fm);
        reqDone->SetIOTracker(&iot);

        // The retry timeout set by the configuration file is 500 because the chunkserver setting returns timeout
        // Causing the triggering of an exponential backoff of the underlying timeout time, increasing the interval between each retry. Retrying 50 times normally only requires a timeout of 49 * 500
        // But after increasing the index backoff, the timeout will increase to 49 * 1000=49000
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
        copysetClient.WriteChunk(reqCtx->idinfo_, fileId, epoch, 0,
                                 iobuf, offset, len, {}, reqDone);
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
        reqCtx->offset_ = 0;
        reqCtx->rawlength_ = len;

        curve::common::CountDownEvent cond(1);
        RequestClosure *reqDone = new FakeRequestClosure(&cond, reqCtx);
        reqDone->SetFileMetric(&fm);
        reqDone->SetIOTracker(&iot);

        // The retry sleep time set in the configuration file is 5000us because the chunkserver setting returns timeout
        // Causing triggering of low-level exponential backoff, increasing the interval between each retry. Retrying 50 times normally only requires 49 * 5000us of sleep
        // But after increasing the index of retreat, the sleep interval will increase to
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
        copysetClient.WriteChunk(reqCtx->idinfo_, fileId, epoch, 0,
                                 iobuf, offset, len, {}, reqDone);
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

        // The retry timeout set by the configuration file is 500 because the chunkserver setting returns timeout
        // Causing the triggering of an exponential backoff of the underlying timeout time, increasing the interval between each retry. Retrying 50 times normally only requires 50 * 500
        // But after increasing the index retreat, the timeout will increase to 500+1000+2000...~=60000
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
        copysetClient.ReadChunk(reqCtx->idinfo_, sn,
                                offset, len, {}, reqDone);
        cond.Wait();
        ASSERT_NE(0, reqDone->GetErrorCode());

        uint64_t end = TimeUtility::GetTimeofDayMs();
        ASSERT_GT(end - start, 25000);
        ASSERT_LT(end - start, 60000);

        std::this_thread::sleep_for(std::chrono::seconds(8));

        gReadCntlFailedCode = 0;
    }

    /* Set overload */
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

        // The retry sleep time set in the configuration file is 5000us because the chunkserver setting returns timeout
        // Causing triggering of low-level exponential backoff, increasing the interval between each retry. Retrying 50 times is normal, only requiring 49 * 5000 sleep
        // But after increasing the index of retreat, the sleep interval will increase to
        // 10000 + 20000 + 40000 ... = 4650000
        // Adding random factors, the three retry times should be greater than 2900 and less than 5000
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
        copysetClient.ReadChunk(reqCtx->idinfo_, sn,
                                offset, len, {}, reqDone);
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

    /* Illegal parameter */
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
        copysetClient.ReadChunk(reqCtx->idinfo_, sn,
                                offset, len, {}, reqDone);
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
        copysetClient.ReadChunk(reqCtx->idinfo_, sn,
                                offset, len, {}, reqDone);
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

        // The retry sleep time set in the configuration file is 5000, as there is no triggering of underlying index backoff, so there will be no sleep between retries
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
        copysetClient.ReadChunk(reqCtx->idinfo_, sn,
                                offset, len, {}, reqDone);
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

        // The timeout configured in the settings file is 1000, but due to chunk server timeout, it triggers exponential backoff, increasing the interval between retries. In normal conditions,
        // three retries would only require a sleep time of 3 * 1000. However, with the added exponential
        // backoff, the timeout intervals will increase to 1000 + 2000 + 2000 = 5000. Considering the random factor,
        // the total time for three retries should be greater than 7000 and less than 8000.
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
        copysetClient.ReadChunk(reqCtx->idinfo_, sn,
                                offset, len, {}, reqDone);
        cond.Wait();
        ASSERT_NE(0, reqDone->GetErrorCode());

        uint64_t end = TimeUtility::GetTimeofDayMs();
        ASSERT_GT(end - start, 3000);
        ASSERT_LT(end - start, 6000);

        std::this_thread::sleep_for(std::chrono::seconds(8));

        gReadCntlFailedCode = 0;
    }

    /* Set overload */
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

        // The retry sleep time set in the configuration file is 500, but due to chunkserver timeouts, it triggers exponential backoff, increasing the interval between retries. In normal conditions,
        // three retries would only require a sleep time of 3 * 500. However, with the added exponential
        // backoff, the sleep intervals will increase to 1000 + 2000 = 3000. Considering the random factor,
        // the total time for three retries should be greater than 2900 and less than 5000.
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
        copysetClient.ReadChunk(reqCtx->idinfo_, sn,
                                offset, len, {}, reqDone);
        cond.Wait();
        ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_OVERLOAD,
                  reqDone->GetErrorCode());

        uint64_t end = TimeUtility::GetTimeofDayUs();
        ASSERT_GT(end - start, 2900);
        ASSERT_LT(end - start, 3 * 5000);
    }

    /* Other errors */
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
        copysetClient.ReadChunk(reqCtx->idinfo_, sn,
                                offset, len, {}, reqDone);
        cond.Wait();
        ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_FAILURE_UNKNOWN,
                  reqDone->GetErrorCode());
    }
    /* Not a leader, returning the correct leader */
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
        copysetClient.ReadChunk(reqCtx->idinfo_, sn,
                                offset, len, {}, reqDone);
        cond.Wait();
        ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS,
                  reqDone->GetErrorCode());
    }
    /* Not a leader, but did not return a leader, refreshing the meta cache succeeded */
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
        copysetClient.ReadChunk(reqCtx->idinfo_, sn,
                                offset, len, {}, reqDone);
        cond.Wait();
        ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS,
                  reqDone->GetErrorCode());
    }
    /* Not a leader, but did not return a leader, refreshing the meta cache failed */
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
        copysetClient.ReadChunk(reqCtx->idinfo_, sn,
                                offset, len, {}, reqDone);
        cond.Wait();
        ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS,
                  reqDone->GetErrorCode());
    }
    /* Not a leader, but returned an incorrect leader */
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
        copysetClient.ReadChunk(reqCtx->idinfo_, sn,
                                offset, len, {}, reqDone);
        cond.Wait();
        ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_REDIRECTED,
                  reqDone->GetErrorCode());
    }
    /* copyset does not exist, updating leader still failed */
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
        copysetClient.ReadChunk(reqCtx->idinfo_, sn,
                                offset, len, {}, reqDone);
        cond.Wait();
        ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_COPYSET_NOTEXIST,
                  reqDone->GetErrorCode());
    }
    /* copyset does not exist, updating leader succeeded */
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
        copysetClient.ReadChunk(reqCtx->idinfo_, sn,
                                offset, len, {}, reqDone);
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

    /* Illegal parameter */
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
                                        sn, offset, len, reqDone);
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
                                        sn, offset, len, reqDone);
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
                                        sn, offset, len, reqDone);
        cond.Wait();
        ASSERT_NE(0, reqDone->GetErrorCode());
        gReadCntlFailedCode = 0;
    }
    /* Other errors */
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
                                        sn, offset, len, reqDone);
        cond.Wait();
        ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_FAILURE_UNKNOWN,
                  reqDone->GetErrorCode());
    }
    /* Not a leader, returning the correct leader */
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
                                        sn, offset, len, reqDone);
        cond.Wait();
        ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS,
                  reqDone->GetErrorCode());
    }
    /* Not a leader, but did not return a leader, refreshing the meta cache succeeded */
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
                                        sn, offset, len, reqDone);
        cond.Wait();
        ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS,
                  reqDone->GetErrorCode());
    }
    /* Not a leader, but did not return a leader, refreshing the meta cache failed */
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
                                        sn, offset, len, reqDone);
        cond.Wait();
        ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS,
                  reqDone->GetErrorCode());
    }
    /* Not a leader, but returned an incorrect leader */
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
                                        sn, offset, len, reqDone);
        cond.Wait();
        ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_REDIRECTED,
                  reqDone->GetErrorCode());
    }
    /* copyset does not exist, updating leader still failed */
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
                                        sn, offset, len, reqDone);
        cond.Wait();
        ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_COPYSET_NOTEXIST,
                  reqDone->GetErrorCode());
    }
    /* copyset does not exist, updating leader succeeded */
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
                                        sn, offset, len, reqDone);
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
                                        sn, offset, len, reqDone);
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

    /* Illegal parameter */
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
    /* Other errors */
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
    /* Not a leader, returning the correct leader */
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
    /* Not a leader, but did not return a leader, refreshing the meta cache succeeded */
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
    /* Not a leader, but did not return a leader, refreshing the meta cache failed */
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
    /* Not a leader, but returned an incorrect leader */
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
    /* copyset does not exist, updating leader still failed */
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
    /* copyset does not exist, updating leader succeeded */
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

    /* Illegal parameter */
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
    // /* Other errors */
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
    /* Not a leader, but did not return a leader, refreshing the meta cache succeeded */
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
    /* Not a leader, but did not return a leader, refreshing the meta cache failed */
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
    /* Not a leader, but returned an incorrect leader */
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
    /* copyset does not exist, updating leader still failed */
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
    /* copyset does not exist, updating leader succeeded */
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

    /* Illegal parameter */
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
    /* Other errors */
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
    /* Not a leader, returning the correct leader */
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
    /* Not a leader, but did not return a leader, refreshing the meta cache succeeded */
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
    /* Not a leader, but did not return a leader, refreshing the meta cache failed */
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
    /* Not a leader, but returned an incorrect leader */
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
    /* copyset does not exist, updating leader still failed */
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
    /* copyset does not exist, updating leader succeeded */
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

    /* Illegal parameter */
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
    /* Other errors */
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
    /* Not a leader, returning the correct leader */
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
    /* Not a leader, but did not return a leader, refreshing the meta cache succeeded */
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
    /* Not a leader, but did not return a leader, refreshing the meta cache failed */
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
    /* Not a leader, but returned an incorrect leader */
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
    /* copyset does not exist, updating leader still failed */
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
    /* copyset does not exist, updating leader succeeded */
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
    UserInfo userinfo;
    userinfo.owner = "userinfo";

    std::shared_ptr<MDSClient> mdsclient = std::make_shared<MDSClient>();

    // set mds addr
    auto mdsopts = cc.GetFileServiceOption().metaServerOpt;
    mdsopts.rpcRetryOpt.addrs.clear();
    mdsopts.rpcRetryOpt.addrs.push_back(endpoint);

    ASSERT_EQ(LIBCURVE_ERROR::OK, mdsclient->Initialize(mdsopts));
    ASSERT_TRUE(fileinstance.Initialize(
        "/test", mdsclient, userinfo, OpenFlags{}, cc.GetFileServiceOption()));

    // create fake chunkserver service
    FakeChunkServerService fakechunkservice;
    // Set up cli service
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
    curve::client::MetaCache* mc =
        fileinstance.GetIOManager4File()->GetMetaCache();
    curve::client::ChunkIDInfo_t chunkinfo(1, 2, 3);
    mc->UpdateChunkInfoByIndex(0, chunkinfo);
    curve::client::CopysetInfo<ChunkServerID> cpinfo;
    curve::client::EndPoint ep;
    butil::str2endpoint("127.0.0.1", 9102, &ep);

    braft::PeerId pd(ep);
    curve::client::PeerAddr addr = curve::client::PeerAddr(ep);
    curve::client::CopysetPeerInfo<ChunkServerID> peer(1, addr, addr);
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

    // Set file version number
    fileinstance.GetIOManager4File()->SetLatestFileSn(kNewFileSn);

    // Send a write request and wait for seconds to check if IO returns
    auto startWriteAndCheckResult = [&fileinstance](int sec)-> bool {  // NOLINT
        CurveAioContext* aioctx = new CurveAioContext();
        char buffer[4096];

        aioctx->buf = buffer;
        aioctx->offset = 0;
        aioctx->length = sizeof(buffer);
        aioctx->op = LIBCURVE_OP::LIBCURVE_OP_WRITE;
        aioctx->cb = WriteCallBack;

        // Send write request
        fileinstance.AioWrite(aioctx, UserDataType::RawBuffer);

        std::this_thread::sleep_for(std::chrono::seconds(sec));
        return gWriteSuccessFlag;
    };

    // Successfully written for the first time and updated the file version number on the chunkserver side
    ASSERT_TRUE(startWriteAndCheckResult(3));

    // Set an old version number to write
    fileinstance.GetIOManager4File()->SetLatestFileSn(kOldFileSn);
    gWriteSuccessFlag = false;

    // chunkserver returns the feedback, and after obtaining the version number again, it is still the old version
    // IO hang
    ASSERT_FALSE(startWriteAndCheckResult(3));

    // Update version number to normal state
    fileinstance.GetIOManager4File()->SetLatestFileSn(kNewFileSn);
    std::this_thread::sleep_for(std::chrono::seconds(1));

    // Last write request successful
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
        // In the redirect case, chunkserver returns a new leader
        // Will not sleep until retry
        RequestContext* reqCtx = new FakeRequestContext();
        reqCtx->optype_ = OpType::WRITE;
        reqCtx->idinfo_ = ChunkIDInfo(chunkId, logicPoolId, copysetId);
        // reqCtx->writeBuffer_ = buff1;
        reqCtx->writeData_ = iobuf;
        reqCtx->offset_ = 0;
        reqCtx->rawlength_ = len;

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
        copysetClient.WriteChunk(reqCtx->idinfo_, fileId, epoch, 0,
                                 iobuf, offset, len, {},
                                 reqDone);
        cond.Wait();
        auto endUs = curve::common::TimeUtility::GetTimeofDayUs();

        // Returns a new leader ID, so there will be no sleep before retrying
        ASSERT_LE(endUs - startUs, sleepUsBeforeRetry / 10);
        ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS,
                  reqDone->GetErrorCode());
    }

    {
        // In the redirect case, chunkserver returns the old leader
        // Sleep before retrying
        RequestContext* reqCtx = new FakeRequestContext();
        reqCtx->optype_ = OpType::WRITE;
        reqCtx->idinfo_ = ChunkIDInfo(chunkId, logicPoolId, copysetId);
        // reqCtx->writeBuffer_ = buff1;
        reqCtx->writeData_ = iobuf;
        reqCtx->offset_ = 0;
        reqCtx->rawlength_ = len;

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
        copysetClient.WriteChunk(reqCtx->idinfo_, fileId, epoch, 0,
                                 iobuf, offset, len, {},
                                 reqDone);
        cond.Wait();
        auto endUs = curve::common::TimeUtility::GetTimeofDayUs();

        // Return the same leader ID and sleep before retrying
        ASSERT_GE(endUs - startUs, sleepUsBeforeRetry / 10);
        ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS,
                  reqDone->GetErrorCode());
    }

    {
        // In the redirect case, chunkserver did not return a leader
        // Actively refresh to obtain a new leader
        RequestContext* reqCtx = new FakeRequestContext();
        reqCtx->optype_ = OpType::WRITE;
        reqCtx->idinfo_ = ChunkIDInfo(chunkId, logicPoolId, copysetId);
        // reqCtx->writeBuffer_ = buff1;
        reqCtx->writeData_ = iobuf;
        reqCtx->offset_ = 0;
        reqCtx->rawlength_ = len;

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
        copysetClient.WriteChunk(reqCtx->idinfo_, fileId, epoch, 0,
                                 iobuf, offset, len, {},
                                 reqDone);
        cond.Wait();
        auto endUs = curve::common::TimeUtility::GetTimeofDayUs();

        // Returns a new leader id, so there will be no sleep before retrying
        ASSERT_LE(endUs - startUs, sleepUsBeforeRetry / 10);
        ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS,
                  reqDone->GetErrorCode());
    }

    {
        // In the redirect case, chunkserver did not return a leader
        // Actively refresh to obtain old leader
        RequestContext* reqCtx = new FakeRequestContext();
        reqCtx->optype_ = OpType::WRITE;
        reqCtx->idinfo_ = ChunkIDInfo(chunkId, logicPoolId, copysetId);
        // reqCtx->writeBuffer_ = buff1;
        reqCtx->writeData_ = iobuf;
        reqCtx->offset_ = 0;
        reqCtx->rawlength_ = len;

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
        copysetClient.WriteChunk(reqCtx->idinfo_, fileId, epoch, 0,
                                 iobuf, offset, len, {},
                                 reqDone);
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

// After the test session fails, the retry request will be placed back in the request queue
TEST(CopysetClientBasicTest, TestReScheduleWhenSessionNotValid) {
    MockRequestScheduler requestScheduler;
    CopysetClient copysetClient;
    IOSenderOption ioSenderOption;
    MetaCache metaCache;

    ASSERT_EQ(0, copysetClient.Init(&metaCache, ioSenderOption,
                                    &requestScheduler, nullptr));

    // Set session not valid
    copysetClient.StartRecycleRetryRPC();

    {
        EXPECT_CALL(requestScheduler, ReSchedule(_))
            .Times(1);

        TestRunnedRequestClosure closure;
        copysetClient.ReadChunk({}, 0, 0, 0, {}, &closure);
        ASSERT_FALSE(closure.IsRunned());
    }

    {
        EXPECT_CALL(requestScheduler, ReSchedule(_))
            .Times(1);

        TestRunnedRequestClosure closure;
        copysetClient.WriteChunk({}, 1, 1, 0, {}, 0, 0, {}, &closure);
        ASSERT_FALSE(closure.IsRunned());
    }
}

}   // namespace client
}   // namespace curve
