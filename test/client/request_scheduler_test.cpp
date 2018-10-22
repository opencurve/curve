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

namespace curve {
namespace client {

using ::testing::AnyNumber;

TEST(RequestSchedulerTest, fake_server_test) {
    brpc::Server server;
    std::string listenAddr = "127.0.0.1:8200";
    FakeChunkServiceImpl fakeChunkService;
    ASSERT_EQ(server.AddService(&fakeChunkService,
                                brpc::SERVER_DOESNT_OWN_SERVICE), 0);
    brpc::ServerOptions option;
    option.idle_timeout_sec = -1;
    ASSERT_EQ(server.Start(listenAddr.c_str(), &option), 0);

    RequestScheduler requestScheduler;
    RequestSenderManager senderManager;
    MockMetaCache mockMetaCache;
    mockMetaCache.DelegateToFake();

    /* error init test */
    {
        ASSERT_EQ(-1, requestScheduler.Init(-1, 8, nullptr, nullptr));
        ASSERT_EQ(-1, requestScheduler.Init(0, 8, nullptr, nullptr));
        ASSERT_EQ(-1, requestScheduler.Init(2, 0, nullptr, nullptr));
        ASSERT_EQ(-1, requestScheduler.Init(2, -1, nullptr, nullptr));
        ASSERT_EQ(-1, requestScheduler.Init(2, 4, nullptr, &mockMetaCache));
        ASSERT_EQ(-1, requestScheduler.Init(2, 4, &senderManager, nullptr));
    }

    ASSERT_EQ(0, requestScheduler.Init(100, 4, &senderManager, &mockMetaCache));
    LogicPoolID logicPoolId = 1;
    CopysetID copysetId = 100001;
    ChunkID chunkId = 1;
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
        reqCtx->logicpoolid_ = logicPoolId;
        reqCtx->copysetid_ = copysetId;
        reqCtx->chunkid_ = chunkId;
        reqCtx->data_ = writebuff;
        reqCtx->offset_ = offset;
        reqCtx->rawlength_ = len;

        RequestClosure *reqDone = new FakeRequestClosure(reqCtx);
        reqCtx->done_ = reqDone;

        std::list<RequestContext *> reqCtxs;
        reqCtxs.push_back(reqCtx);
        ASSERT_EQ(-1, requestScheduler.ScheduleRequest(reqCtxs));
    }
    {
        RequestContext *reqCtx = new FakeRequestContext();
        reqCtx->optype_ = OpType::WRITE;
        reqCtx->logicpoolid_ = logicPoolId;
        reqCtx->copysetid_ = copysetId;
        reqCtx->chunkid_ = chunkId;
        reqCtx->data_ = writebuff;
        reqCtx->offset_ = offset;
        reqCtx->rawlength_ = len;

        RequestClosure *reqDone = new FakeRequestClosure(reqCtx);
        reqCtx->done_ = reqDone;
        ASSERT_EQ(-1, requestScheduler.ScheduleRequest(reqCtx));
    }

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
        reqCtx->logicpoolid_ = logicPoolId;
        reqCtx->copysetid_ = copysetId;
        reqCtx->chunkid_ = chunkId;
        reqCtx->data_ = writebuff1;
        reqCtx->offset_ = 0;
        reqCtx->rawlength_ = len1;

        RequestClosure *reqDone = new FakeRequestClosure(reqCtx);
        reqCtx->done_ = reqDone;

        std::list<RequestContext *> reqCtxs;
        reqCtxs.push_back(reqCtx);
        ASSERT_EQ(0, requestScheduler.ScheduleRequest(reqCtxs));
        ::usleep(50*1000);
    }
    {
        RequestContext *reqCtx = new FakeRequestContext();
        reqCtx->optype_ = OpType::READ;
        reqCtx->logicpoolid_ = logicPoolId;
        reqCtx->copysetid_ = copysetId;
        reqCtx->chunkid_ = chunkId;
        memset(readbuff1, '0', 16);
        reqCtx->data_ = readbuff1;
        reqCtx->offset_ = 0;
        reqCtx->rawlength_ = len1;

        RequestClosure *reqDone = new FakeRequestClosure(reqCtx);
        reqCtx->done_ = reqDone;

        std::list<RequestContext *> reqCtxs;
        reqCtxs.push_back(reqCtx);
        ASSERT_EQ(0, requestScheduler.ScheduleRequest(reqCtxs));
        usleep(50 * 1000);
        ASSERT_STREQ(reqCtx->data_, cmpbuff1);
        ASSERT_EQ(0, reqDone->GetErrorCode());
    }
    {
        RequestContext *reqCtx = new FakeRequestContext();
        reqCtx->optype_ = OpType::WRITE;
        reqCtx->logicpoolid_ = logicPoolId;
        reqCtx->copysetid_ = copysetId;
        reqCtx->chunkid_ = chunkId;
        ::memset(writebuff1, 'a', 8);
        ::memset(writebuff1+8, '\0', 8);
        reqCtx->data_ = writebuff1;
        reqCtx->offset_ = 0;
        reqCtx->rawlength_ = len1;

        RequestClosure *reqDone = new FakeRequestClosure(reqCtx);
        reqCtx->done_ = reqDone;

        std::list<RequestContext *> reqCtxs;
        reqCtxs.push_back(reqCtx);
        ASSERT_EQ(0, requestScheduler.ScheduleRequest(reqCtxs));
        ::usleep(50*1000);
    }
    {
        RequestContext *reqCtx = new FakeRequestContext();
        reqCtx->optype_ = OpType::READ;
        reqCtx->logicpoolid_ = logicPoolId;
        reqCtx->copysetid_ = copysetId;
        reqCtx->chunkid_ = chunkId;
        memset(readbuff1, '0', 16);
        reqCtx->data_ = readbuff1;
        reqCtx->offset_ = 0;
        reqCtx->rawlength_ = len1;

        RequestClosure *reqDone = new FakeRequestClosure(reqCtx);
        reqCtx->done_ = reqDone;

        std::list<RequestContext *> reqCtxs;
        reqCtxs.push_back(reqCtx);
        ASSERT_EQ(0, requestScheduler.ScheduleRequest(reqCtxs));
        usleep(50 * 1000);
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

    /* basic test */
    const int kMaxLoop = 100;
    for (int i = 0; i < kMaxLoop; ++i) {
        RequestContext *reqCtx = new FakeRequestContext();
        reqCtx->optype_ = OpType::WRITE;
        reqCtx->logicpoolid_ = logicPoolId;
        reqCtx->copysetid_ = copysetId;
        reqCtx->chunkid_ = chunkId;
        reqCtx->data_ = writebuff;
        reqCtx->offset_ = offset + i;
        reqCtx->rawlength_ = len;

        RequestClosure *reqDone = new FakeRequestClosure(reqCtx);
        reqCtx->done_ = reqDone;

        std::list<RequestContext *> reqCtxs;
        reqCtxs.push_back(reqCtx);
        ASSERT_EQ(0, requestScheduler.ScheduleRequest(reqCtxs));
    }
    sleep(1);
    for (int i = 0; i < kMaxLoop; ++i) {
        RequestContext *reqCtx = new FakeRequestContext();
        reqCtx->optype_ = OpType::READ;
        reqCtx->logicpoolid_ = logicPoolId;
        reqCtx->copysetid_ = copysetId;
        reqCtx->chunkid_ = chunkId;
        memset(readbuff, '0', 8);
        reqCtx->data_ = readbuff;
        reqCtx->offset_ = offset + i;
        reqCtx->rawlength_ = len;

        RequestClosure *reqDone = new FakeRequestClosure(reqCtx);
        reqCtx->done_ = reqDone;

        std::list<RequestContext *> reqCtxs;
        reqCtxs.push_back(reqCtx);
        ASSERT_EQ(0, requestScheduler.ScheduleRequest(reqCtxs));
        usleep(50 * 1000);
        ASSERT_STREQ(reqCtx->data_, cmpbuff);
        ASSERT_EQ(0, reqDone->GetErrorCode());
    }
    {
        RequestContext *reqCtx = new FakeRequestContext();
        reqCtx->optype_ = OpType::UNKNOWN;
        reqCtx->logicpoolid_ = logicPoolId;
        reqCtx->copysetid_ = copysetId;
        reqCtx->chunkid_ = chunkId;
        memset(readbuff, '0', 8);
        reqCtx->data_ = readbuff;
        reqCtx->offset_ = offset;
        reqCtx->rawlength_ = len;

        RequestClosure *reqDone = new FakeRequestClosure(reqCtx);
        reqCtx->done_ = reqDone;

        std::list<RequestContext *> reqCtxs;
        reqCtxs.push_back(reqCtx);
        ASSERT_EQ(0, requestScheduler.ScheduleRequest(reqCtxs));
        usleep(50 * 1000);
        ASSERT_EQ(-1, reqDone->GetErrorCode());
    }

    /* 2. 并发测试 */
    auto func = [&]() {
        for (int i = 0; i < kMaxLoop; ++i) {
            RequestContext *reqCtx = new FakeRequestContext();
            reqCtx->optype_ = OpType::WRITE;
            reqCtx->logicpoolid_ = logicPoolId;
            reqCtx->copysetid_ = copysetId;
            reqCtx->chunkid_ = chunkId;
            reqCtx->data_ = writebuff;
            reqCtx->offset_ = offset + i;
            reqCtx->rawlength_ = len;

            RequestClosure *reqDone = new FakeRequestClosure(reqCtx);
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

    sleep(2);


    for (int i = 0; i < kMaxLoop; i += 1) {
        RequestContext *reqCtx = new FakeRequestContext();
        reqCtx->optype_ = OpType::READ;
        reqCtx->logicpoolid_ = 1000;
        reqCtx->copysetid_ = copysetId;
        reqCtx->chunkid_ = chunkId;
        memset(readbuff, '0', 8);
        reqCtx->data_ = readbuff;
        reqCtx->offset_ = offset + i;
        reqCtx->rawlength_ = len;
        RequestClosure *reqDone = new FakeRequestClosure(reqCtx);
        reqCtx->done_ = reqDone;
        ASSERT_EQ(0, requestScheduler.ScheduleRequest(reqCtx));
        usleep(50 * 1000);
        ASSERT_STREQ(reqCtx->data_, cmpbuff);
        ASSERT_EQ(0, reqDone->GetErrorCode());
    }

    usleep(500 * 1000);
    requestScheduler.Fini();
    ASSERT_EQ(0, server.Stop(0));
    ASSERT_EQ(0, server.Join());
}

}   // namespace client
}   // namespace curve
