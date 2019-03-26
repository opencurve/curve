/*
 * Project: curve
 * Created Date: 18-9-29
 * Author: wudemiao
 * Copyright (c) 2018 netease
 */

#include <brpc/controller.h>
#include <gtest/gtest.h>
#include <gmock/gmock.h>
#include <brpc/channel.h>

#include "src/client/request_scheduler.h"
#include "src/client/client_common.h"
#include "test/client/mock_meta_cache.h"
#include "test/client/mock_chunkservice.h"
#include "test/client/mock_request_context.h"
#include "src/common/concurrent/count_down_event.h"

namespace curve {
namespace client {

using ::testing::AnyNumber;

TEST(RequestSchedulerTest, fake_server_test) {
    RequestScheduleOption_t opt;
    opt.queueCapacity = 4096;
    opt.threadpoolSize = 2;
    opt.ioSenderOpt.rpcTimeoutMs = 200;
    opt.ioSenderOpt.rpcRetryTimes = 3;
    opt.ioSenderOpt.failRequestOpt.opMaxRetry = 5;
    opt.ioSenderOpt.failRequestOpt.opRetryIntervalUs = 5000;
    opt.ioSenderOpt.enableAppliedIndexRead = 1;

    brpc::Server server;
    std::string listenAddr = "127.0.0.1:8200";
    FakeChunkServiceImpl fakeChunkService;
    ASSERT_EQ(server.AddService(&fakeChunkService,
                                brpc::SERVER_DOESNT_OWN_SERVICE), 0);
    brpc::ServerOptions option;
    option.idle_timeout_sec = -1;
    ASSERT_EQ(server.Start(listenAddr.c_str(), &option), 0);

    RequestScheduler requestScheduler;
    MockMetaCache mockMetaCache;
    mockMetaCache.DelegateToFake();

    LOG(INFO) << "start testing";
    /* error init test */
    {
        ASSERT_EQ(-1, requestScheduler.Init(opt, nullptr));
        ASSERT_EQ(-1, requestScheduler.Init(opt, nullptr));
        ASSERT_EQ(-1, requestScheduler.Init(opt, nullptr));
        ASSERT_EQ(-1, requestScheduler.Init(opt, nullptr));
        ASSERT_EQ(0, requestScheduler.Init(opt, &mockMetaCache));
        ASSERT_EQ(-1, requestScheduler.Init(opt, nullptr));
    }

    opt.queueCapacity = 100;
    opt.threadpoolSize = 4;
    ASSERT_EQ(0, requestScheduler.Init(opt, &mockMetaCache));
    LogicPoolID logicPoolId = 1;
    CopysetID copysetId = 100001;
    ChunkID chunkId = 1;
    uint64_t sn = 0;
    size_t len = 8;
    char writebuff[8 + 1];
    char readbuff[8 + 1];
    char cmpbuff[8 + 1];
    memset(writebuff, 'a', 8);
    memset(readbuff, '0', 8);
    memset(cmpbuff, 'a', 8);
    writebuff[8] = '\0';
    readbuff[8] = '\0';
    cmpbuff[8] = '\0';
    off_t offset = 0;

    /* error request schedule test when scheduler not run */
    {
        RequestContext *reqCtx = new FakeRequestContext();
        reqCtx->optype_ = OpType::WRITE;
        reqCtx->idinfo_ = ChunkIDInfo(chunkId, logicPoolId, copysetId);
        reqCtx->data_ = writebuff;
        reqCtx->offset_ = offset;
        reqCtx->rawlength_ = len;

        curve::common::CountDownEvent cond(0);
        RequestClosure *reqDone = new FakeRequestClosure(&cond, reqCtx);
        reqCtx->done_ = reqDone;

        std::list<RequestContext *> reqCtxs;
        reqCtxs.push_back(reqCtx);
        ASSERT_EQ(-1, requestScheduler.ScheduleRequest(reqCtxs));
    }
    {
        RequestContext *reqCtx = new FakeRequestContext();
        reqCtx->optype_ = OpType::WRITE;
        reqCtx->idinfo_ = ChunkIDInfo(chunkId, logicPoolId, copysetId);
        reqCtx->data_ = writebuff;
        reqCtx->offset_ = offset;
        reqCtx->rawlength_ = len;

        curve::common::CountDownEvent cond(0);
        RequestClosure *reqDone = new FakeRequestClosure(&cond, reqCtx);
        reqCtx->done_ = reqDone;
        ASSERT_EQ(-1, requestScheduler.ScheduleRequest(reqCtx));
    }

    /* Scheduler run */
    ASSERT_EQ(0, requestScheduler.Run());
    EXPECT_CALL(mockMetaCache, GetLeader(_, _, _, _, _)).Times(AnyNumber());

    usleep(100 * 1000);

    char writebuff1[16 + 1];
    char readbuff1[16 + 1];
    char cmpbuff1[16 + 1];
    memset(writebuff1, 'a', 16);
    memset(readbuff1, '0', 16);
    memset(cmpbuff1, 'a', 16);
    writebuff1[16] = '\0';
    readbuff1[16] = '\0';
    cmpbuff1[16] = '\0';
    const uint64_t len1 = 16;
    /* write should with attachment size */
    {
        RequestContext *reqCtx = new FakeRequestContext();
        reqCtx->optype_ = OpType::WRITE;
        reqCtx->idinfo_ = ChunkIDInfo(chunkId, logicPoolId, copysetId);


        reqCtx->seq_ = sn;

        reqCtx->data_ = writebuff1;
        reqCtx->offset_ = 0;
        reqCtx->rawlength_ = len1;

        curve::common::CountDownEvent cond(1);
        RequestClosure *reqDone = new FakeRequestClosure(&cond, reqCtx);
        reqCtx->done_ = reqDone;

        std::list<RequestContext *> reqCtxs;
        reqCtxs.push_back(reqCtx);
        ASSERT_EQ(0, requestScheduler.ScheduleRequest(reqCtxs));
        cond.Wait();
    }
    {
        RequestContext *reqCtx = new FakeRequestContext();
        reqCtx->optype_ = OpType::READ;
        reqCtx->idinfo_ = ChunkIDInfo(chunkId, logicPoolId, copysetId);


        memset(readbuff1, '0', 16);
        reqCtx->data_ = readbuff1;
        reqCtx->offset_ = 0;
        reqCtx->rawlength_ = len1;

        curve::common::CountDownEvent cond(1);
        RequestClosure *reqDone = new FakeRequestClosure(&cond, reqCtx);
        reqCtx->done_ = reqDone;

        std::list<RequestContext *> reqCtxs;
        reqCtxs.push_back(reqCtx);
        ASSERT_EQ(0, requestScheduler.ScheduleRequest(reqCtxs));
        cond.Wait();
        ASSERT_STREQ(reqCtx->data_, cmpbuff1);
        ASSERT_EQ(0, reqDone->GetErrorCode());
    }
    {
        RequestContext *reqCtx = new FakeRequestContext();
        reqCtx->optype_ = OpType::WRITE;
        reqCtx->idinfo_ = ChunkIDInfo(chunkId, logicPoolId, copysetId);


        reqCtx->seq_ = sn;
        ::memset(writebuff1, 'a', 8);
        ::memset(writebuff1 + 8, '\0', 8);
        reqCtx->data_ = writebuff1;
        reqCtx->offset_ = 0;
        reqCtx->rawlength_ = len1;

        curve::common::CountDownEvent cond(1);
        RequestClosure *reqDone = new FakeRequestClosure(&cond, reqCtx);
        reqCtx->done_ = reqDone;

        std::list<RequestContext *> reqCtxs;
        reqCtxs.push_back(reqCtx);
        ASSERT_EQ(0, requestScheduler.ScheduleRequest(reqCtxs));
        cond.Wait();
    }
    {
        RequestContext *reqCtx = new FakeRequestContext();
        reqCtx->optype_ = OpType::READ;
        reqCtx->idinfo_ = ChunkIDInfo(chunkId, logicPoolId, copysetId);


        reqCtx->seq_ = sn;
        memset(readbuff1, '0', 16);
        reqCtx->data_ = readbuff1;
        reqCtx->offset_ = 0;
        reqCtx->rawlength_ = len1;

        curve::common::CountDownEvent cond(1);
        RequestClosure *reqDone = new FakeRequestClosure(&cond, reqCtx);
        reqCtx->done_ = reqDone;

        std::list<RequestContext *> reqCtxs;
        reqCtxs.push_back(reqCtx);
        ASSERT_EQ(0, requestScheduler.ScheduleRequest(reqCtxs));
        cond.Wait();
        ASSERT_EQ(reqCtx->data_[0], 'a');
        ASSERT_EQ(reqCtx->data_[1], 'a');
        ASSERT_EQ(reqCtx->data_[2], 'a');
        ASSERT_EQ(reqCtx->data_[3], 'a');
        ASSERT_EQ(reqCtx->data_[4], 'a');
        ASSERT_EQ(reqCtx->data_[5], 'a');
        ASSERT_EQ(reqCtx->data_[6], 'a');
        ASSERT_EQ(reqCtx->data_[7], 'a');
        ASSERT_EQ(reqCtx->data_[8], '\0');
        ASSERT_EQ(reqCtx->data_[9], '\0');
        ASSERT_EQ(reqCtx->data_[10], '\0');
        ASSERT_EQ(reqCtx->data_[11], '\0');
        ASSERT_EQ(reqCtx->data_[12], '\0');
        ASSERT_EQ(reqCtx->data_[13], '\0');
        ASSERT_EQ(reqCtx->data_[14], '\0');
        ASSERT_EQ(reqCtx->data_[15], '\0');
        ASSERT_EQ(0, reqDone->GetErrorCode());
    }

    // read snapshot
    // 1. 先 write snapshot
    {
        RequestContext *reqCtx = new FakeRequestContext();
        reqCtx->optype_ = OpType::WRITE;
        reqCtx->idinfo_ = ChunkIDInfo(chunkId, logicPoolId, copysetId);


        reqCtx->seq_ = sn;

        ::memset(writebuff1, 'a', 16);
        reqCtx->data_ = writebuff1;
        reqCtx->offset_ = 0;
        reqCtx->rawlength_ = len1;

        curve::common::CountDownEvent cond(1);
        RequestClosure *reqDone = new FakeRequestClosure(&cond, reqCtx);
        reqCtx->done_ = reqDone;

        std::list<RequestContext *> reqCtxs;
        reqCtxs.push_back(reqCtx);
        ASSERT_EQ(0, requestScheduler.ScheduleRequest(reqCtxs));
        cond.Wait();
    }
    // 2. 再 read snapshot 验证一遍
    {
        RequestContext *reqCtx = new FakeRequestContext();
        reqCtx->optype_ = OpType::READ_SNAP;
        reqCtx->idinfo_ = ChunkIDInfo(chunkId, logicPoolId, copysetId);


        reqCtx->seq_ = sn;
        memset(readbuff1, '0', 16);
        reqCtx->data_ = readbuff1;
        reqCtx->offset_ = 0;
        reqCtx->rawlength_ = len1;

        curve::common::CountDownEvent cond(1);
        RequestClosure *reqDone = new FakeRequestClosure(&cond, reqCtx);
        reqCtx->done_ = reqDone;

        std::list<RequestContext *> reqCtxs;
        reqCtxs.push_back(reqCtx);
        ASSERT_EQ(0, requestScheduler.ScheduleRequest(reqCtxs));
        cond.Wait();
        ASSERT_STREQ(reqCtx->data_, cmpbuff1);
        ASSERT_EQ(0, reqDone->GetErrorCode());
    }
    // 3. 在 delete snapshot
    {
        RequestContext *reqCtx = new FakeRequestContext();
        reqCtx->optype_ = OpType::DELETE_SNAP;
        reqCtx->idinfo_ = ChunkIDInfo(chunkId, logicPoolId, copysetId);


        reqCtx->seq_ = sn;
        reqCtx->offset_ = 0;
        reqCtx->rawlength_ = len1;

        curve::common::CountDownEvent cond(1);
        RequestClosure *reqDone = new FakeRequestClosure(&cond, reqCtx);
        reqCtx->done_ = reqDone;

        std::list<RequestContext *> reqCtxs;
        reqCtxs.push_back(reqCtx);
        ASSERT_EQ(0, requestScheduler.ScheduleRequest(reqCtxs));
        cond.Wait();
        ASSERT_EQ(0, reqDone->GetErrorCode());
    }
    // 4. 重复 delete snapshot
    {
        RequestContext *reqCtx = new FakeRequestContext();
        reqCtx->optype_ = OpType::DELETE_SNAP;
        reqCtx->idinfo_ = ChunkIDInfo(chunkId, logicPoolId, copysetId);


        reqCtx->seq_ = sn;
        reqCtx->offset_ = 0;
        reqCtx->rawlength_ = len1;

        curve::common::CountDownEvent cond(1);
        RequestClosure *reqDone = new FakeRequestClosure(&cond, reqCtx);
        reqCtx->done_ = reqDone;

        std::list<RequestContext *> reqCtxs;
        reqCtxs.push_back(reqCtx);
        ASSERT_EQ(0, requestScheduler.ScheduleRequest(reqCtxs));
        cond.Wait();
        ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_CHUNK_NOTEXIST,
                  reqDone->GetErrorCode());
    }

    // 测试 get chunk info
    {
        ChunkInfoDetail chunkInfo;
        RequestContext *reqCtx = new FakeRequestContext();
        reqCtx->optype_ = OpType::GET_CHUNK_INFO;
        reqCtx->idinfo_ = ChunkIDInfo(chunkId, logicPoolId, copysetId);


        reqCtx->chunkinfodetail_ = &chunkInfo;

        curve::common::CountDownEvent cond(1);
        RequestClosure *reqDone = new FakeRequestClosure(&cond, reqCtx);
        reqCtx->done_ = reqDone;

        std::list<RequestContext *> reqCtxs;
        reqCtxs.push_back(reqCtx);
        ASSERT_EQ(0, requestScheduler.ScheduleRequest(reqCtxs));
        cond.Wait();
        ASSERT_EQ(2, reqDone->GetReqCtx()->chunkinfodetail_->chunkSn.size());
        ASSERT_EQ(1, reqDone->GetReqCtx()->chunkinfodetail_->chunkSn[0]);
        ASSERT_EQ(2, reqDone->GetReqCtx()->chunkinfodetail_->chunkSn[1]);
        ASSERT_EQ(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS,
                  reqDone->GetErrorCode());
    }

    // 测试createClonechunk
    {
        RequestContext *reqCtx = new FakeRequestContext();
        reqCtx->optype_ = OpType::CREATE_CLONE;
        reqCtx->idinfo_ = ChunkIDInfo(chunkId, logicPoolId, copysetId);


        reqCtx->seq_ = sn;
        reqCtx->offset_ = 0;
        reqCtx->rawlength_ = len1;
        reqCtx->location_ = "destination";

        curve::common::CountDownEvent cond(1);
        RequestClosure *reqDone = new FakeRequestClosure(&cond, reqCtx);
        reqCtx->done_ = reqDone;

        std::list<RequestContext *> reqCtxs;
        reqCtxs.push_back(reqCtx);
        ASSERT_EQ(0, requestScheduler.ScheduleRequest(reqCtxs));
        cond.Wait();
        ASSERT_EQ(0, reqDone->GetErrorCode());
    }

    // 测试recoverChunk
    {
        RequestContext *reqCtx = new FakeRequestContext();
        reqCtx->optype_ = OpType::RECOVER_CHUNK;
        reqCtx->idinfo_ = ChunkIDInfo(chunkId, logicPoolId, copysetId);


        reqCtx->seq_ = sn;
        reqCtx->offset_ = 0;
        reqCtx->rawlength_ = len1;

        curve::common::CountDownEvent cond(1);
        RequestClosure *reqDone = new FakeRequestClosure(&cond, reqCtx);
        reqCtx->done_ = reqDone;

        std::list<RequestContext *> reqCtxs;
        reqCtxs.push_back(reqCtx);
        ASSERT_EQ(0, requestScheduler.ScheduleRequest(reqCtxs));
        cond.Wait();
        ASSERT_EQ(0, reqDone->GetErrorCode());
    }

    /* read/write chunk test */
    const int kMaxLoop = 100;
    for (int i = 0; i < kMaxLoop; ++i) {
        RequestContext *reqCtx = new FakeRequestContext();
        reqCtx->optype_ = OpType::WRITE;
        reqCtx->idinfo_ = ChunkIDInfo(chunkId, logicPoolId, copysetId);


        reqCtx->seq_ = sn;
        reqCtx->data_ = writebuff;
        reqCtx->offset_ = offset + i;
        reqCtx->rawlength_ = len;

        curve::common::CountDownEvent cond(1);
        RequestClosure *reqDone = new FakeRequestClosure(&cond, reqCtx);
        reqCtx->done_ = reqDone;

        std::list<RequestContext *> reqCtxs;
        reqCtxs.push_back(reqCtx);
        ASSERT_EQ(0, requestScheduler.ScheduleRequest(reqCtxs));
        cond.Wait();
    }

    for (int i = 0; i < kMaxLoop; ++i) {
        RequestContext *reqCtx = new FakeRequestContext();
        reqCtx->optype_ = OpType::READ;
        reqCtx->idinfo_ = ChunkIDInfo(chunkId, logicPoolId, copysetId);


        reqCtx->seq_ = sn;
        memset(readbuff, '0', 8);
        reqCtx->data_ = readbuff;
        reqCtx->offset_ = offset + i;
        reqCtx->rawlength_ = len;

        curve::common::CountDownEvent cond(1);
        RequestClosure *reqDone = new FakeRequestClosure(&cond, reqCtx);
        reqCtx->done_ = reqDone;

        std::list<RequestContext *> reqCtxs;
        reqCtxs.push_back(reqCtx);
        ASSERT_EQ(0, requestScheduler.ScheduleRequest(reqCtxs));
        cond.Wait();
        ASSERT_STREQ(reqCtx->data_, cmpbuff);
        ASSERT_EQ(0, reqDone->GetErrorCode());
    }
    {
        RequestContext *reqCtx = new FakeRequestContext();
        reqCtx->optype_ = OpType::UNKNOWN;
        reqCtx->idinfo_ = ChunkIDInfo(chunkId, logicPoolId, copysetId);


        memset(readbuff, '0', 8);
        reqCtx->data_ = readbuff;
        reqCtx->offset_ = offset;
        reqCtx->rawlength_ = len;

        curve::common::CountDownEvent cond(1);
        RequestClosure *reqDone = new FakeRequestClosure(&cond, reqCtx);
        reqCtx->done_ = reqDone;

        std::list<RequestContext *> reqCtxs;
        reqCtxs.push_back(reqCtx);
        ASSERT_EQ(0, requestScheduler.ScheduleRequest(reqCtxs));
        cond.Wait();
        ASSERT_EQ(-1, reqDone->GetErrorCode());
    }

    /* 2. 并发测试 */
    curve::common::CountDownEvent cond(4 * kMaxLoop);
    auto func = [&]() {
        for (int i = 0; i < kMaxLoop; ++i) {
            RequestContext *reqCtx = new FakeRequestContext();
            reqCtx->optype_ = OpType::WRITE;
            reqCtx->idinfo_ = ChunkIDInfo(chunkId, logicPoolId, copysetId);

            reqCtx->seq_ = sn;
            reqCtx->data_ = writebuff;
            reqCtx->offset_ = offset + i;
            reqCtx->rawlength_ = len;

            RequestClosure *reqDone = new FakeRequestClosure(&cond, reqCtx);
            reqCtx->done_ = reqDone;
            ASSERT_EQ(0, requestScheduler.ScheduleRequest(reqCtx));
        }
    };

    std::thread t1(func);
    std::thread t2(func);
    std::thread t3(func);
    std::thread t4(func);
    t1.join();
    t2.join();
    t3.join();
    t4.join();

    cond.Wait();

    for (int i = 0; i < kMaxLoop; i += 1) {
        RequestContext *reqCtx = new FakeRequestContext();
        reqCtx->optype_ = OpType::READ;
        reqCtx->idinfo_ = ChunkIDInfo(chunkId, 1000, copysetId);


        reqCtx->seq_ = sn;
        memset(readbuff, '0', 8);
        reqCtx->data_ = readbuff;
        reqCtx->offset_ = offset + i;
        reqCtx->rawlength_ = len;
        curve::common::CountDownEvent cond(1);
        RequestClosure *reqDone = new FakeRequestClosure(&cond, reqCtx);
        reqCtx->done_ = reqDone;
        ASSERT_EQ(0, requestScheduler.ScheduleRequest(reqCtx));
        cond.Wait();
        ASSERT_STREQ(reqCtx->data_, cmpbuff);
        ASSERT_EQ(0, reqDone->GetErrorCode());
    }

    requestScheduler.Fini();
    ASSERT_EQ(0, server.Stop(0));
    ASSERT_EQ(0, server.Join());
}

}   // namespace client
}   // namespace curve
