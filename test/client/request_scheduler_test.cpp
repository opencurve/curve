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
#include <gtest/gtest.h>
#include <gmock/gmock.h>
#include <brpc/channel.h>
#include <butil/iobuf.h>

#include "src/client/request_scheduler.h"
#include "src/client/client_common.h"
#include "test/client/mock/mock_meta_cache.h"
#include "test/client/mock/mock_chunkservice.h"
#include "test/client/mock/mock_request_context.h"
#include "src/common/concurrent/count_down_event.h"

namespace curve {
namespace client {

using ::testing::AnyNumber;

TEST(RequestSchedulerTest, fake_server_test) {
    RequestScheduleOption opt;
    opt.scheduleQueueCapacity = 4096;
    opt.scheduleThreadpoolSize = 2;
    opt.ioSenderOpt.failRequestOpt.chunkserverRPCTimeoutMS = 200;
    opt.ioSenderOpt.failRequestOpt.chunkserverOPMaxRetry = 5;
    opt.ioSenderOpt.failRequestOpt.chunkserverOPRetryIntervalUS = 5000;

    brpc::Server server;
    std::string listenAddr = "127.0.0.1:9109";
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

    opt.scheduleQueueCapacity = 100;
    opt.scheduleThreadpoolSize = 4;
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

    FileMetric fm("test");
    IOTracker iot(nullptr, nullptr, nullptr, &fm);

    /* error request schedule test when scheduler not run */
    {
        RequestContext *reqCtx = new FakeRequestContext();
        reqCtx->optype_ = OpType::WRITE;
        reqCtx->idinfo_ = ChunkIDInfo(chunkId, logicPoolId, copysetId);
        reqCtx->writeData_.append(writebuff, len);
        reqCtx->offset_ = offset;
        reqCtx->rawlength_ = len;

        curve::common::CountDownEvent cond(0);
        RequestClosure *reqDone = new FakeRequestClosure(&cond, reqCtx);
        reqDone->SetFileMetric(&fm);
        reqDone->SetIOTracker(&iot);
        reqCtx->done_ = reqDone;

        std::vector<RequestContext *> reqCtxs;
        reqCtxs.push_back(reqCtx);
        ASSERT_EQ(-1, requestScheduler.ScheduleRequest(reqCtxs));
    }
    {
        RequestContext *reqCtx = new FakeRequestContext();
        reqCtx->optype_ = OpType::WRITE;
        reqCtx->idinfo_ = ChunkIDInfo(chunkId, logicPoolId, copysetId);
        reqCtx->writeData_.append(writebuff, len);
        reqCtx->offset_ = offset;
        reqCtx->rawlength_ = len;

        curve::common::CountDownEvent cond(0);
        RequestClosure *reqDone = new FakeRequestClosure(&cond, reqCtx);
        reqDone->SetFileMetric(&fm);
        reqDone->SetIOTracker(&iot);
        reqCtx->done_ = reqDone;
        ASSERT_EQ(-1, requestScheduler.ScheduleRequest(reqCtx));
    }

    /* Scheduler run */
    ASSERT_EQ(0, requestScheduler.Run());
    EXPECT_CALL(mockMetaCache, GetLeader(_, _, _, _, _, _)).Times(AnyNumber());

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
    butil::IOBuf expectReadData;
    expectReadData.append(cmpbuff1, 16);
    const uint64_t len1 = 16;
    /* write should with attachment size */
    {
        RequestContext *reqCtx = new FakeRequestContext();
        reqCtx->optype_ = OpType::WRITE;
        reqCtx->idinfo_ = ChunkIDInfo(chunkId, logicPoolId, copysetId);

        reqCtx->seq_ = sn;
        reqCtx->writeData_.append(writebuff1, len1);
        reqCtx->offset_ = 0;
        reqCtx->rawlength_ = len1;

        curve::common::CountDownEvent cond(1);
        RequestClosure *reqDone = new FakeRequestClosure(&cond, reqCtx);
        reqDone->SetFileMetric(&fm);
        reqDone->SetIOTracker(&iot);
        reqCtx->done_ = reqDone;

        std::vector<RequestContext *> reqCtxs;
        reqCtxs.push_back(reqCtx);
        ASSERT_EQ(0, requestScheduler.ScheduleRequest(reqCtxs));
        cond.Wait();
    }
    {
        RequestContext *reqCtx = new FakeRequestContext();
        reqCtx->optype_ = OpType::READ;
        reqCtx->idinfo_ = ChunkIDInfo(chunkId, logicPoolId, copysetId);

        memset(readbuff1, '0', 16);
        reqCtx->offset_ = 0;
        reqCtx->rawlength_ = len1;

        curve::common::CountDownEvent cond(1);
        RequestClosure *reqDone = new FakeRequestClosure(&cond, reqCtx);
        reqDone->SetFileMetric(&fm);
        reqDone->SetIOTracker(&iot);
        reqCtx->done_ = reqDone;

        std::vector<RequestContext *> reqCtxs;
        reqCtxs.push_back(reqCtx);
        ASSERT_EQ(0, requestScheduler.ScheduleRequest(reqCtxs));
        cond.Wait();
        // ASSERT_STREQ(reqCtx->readBuffer_, cmpbuff1);
        ASSERT_EQ(0, reqDone->GetErrorCode());
    }
    {
        RequestContext *reqCtx = new FakeRequestContext();
        reqCtx->optype_ = OpType::WRITE;
        reqCtx->idinfo_ = ChunkIDInfo(chunkId, logicPoolId, copysetId);


        reqCtx->seq_ = sn;
        ::memset(writebuff1, 'a', 8);
        ::memset(writebuff1 + 8, '\0', 8);
        reqCtx->writeData_.append(writebuff1, len1);
        reqCtx->offset_ = 0;
        reqCtx->rawlength_ = len1;

        curve::common::CountDownEvent cond(1);
        RequestClosure *reqDone = new FakeRequestClosure(&cond, reqCtx);
        reqDone->SetFileMetric(&fm);
        reqDone->SetIOTracker(&iot);
        reqCtx->done_ = reqDone;

        std::vector<RequestContext *> reqCtxs;
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
        reqCtx->offset_ = 0;
        reqCtx->rawlength_ = len1;

        curve::common::CountDownEvent cond(1);
        RequestClosure *reqDone = new FakeRequestClosure(&cond, reqCtx);
        reqDone->SetFileMetric(&fm);
        reqDone->SetIOTracker(&iot);
        reqCtx->done_ = reqDone;

        std::vector<RequestContext *> reqCtxs;
        reqCtxs.push_back(reqCtx);
        ASSERT_EQ(0, requestScheduler.ScheduleRequest(reqCtxs));
        cond.Wait();
        std::unique_ptr<char[]> readData(new char[len1]);
        memcpy(readData.get(), reqCtx->readData_.to_string().c_str(), len1);
        ASSERT_EQ(readData[0], 'a');
        ASSERT_EQ(readData[1], 'a');
        ASSERT_EQ(readData[2], 'a');
        ASSERT_EQ(readData[3], 'a');
        ASSERT_EQ(readData[4], 'a');
        ASSERT_EQ(readData[5], 'a');
        ASSERT_EQ(readData[6], 'a');
        ASSERT_EQ(readData[7], 'a');
        ASSERT_EQ(readData[8], '\0');
        ASSERT_EQ(readData[9], '\0');
        ASSERT_EQ(readData[10], '\0');
        ASSERT_EQ(readData[11], '\0');
        ASSERT_EQ(readData[12], '\0');
        ASSERT_EQ(readData[13], '\0');
        ASSERT_EQ(readData[14], '\0');
        ASSERT_EQ(readData[15], '\0');

        // ASSERT_EQ(reqCtx->readData_, expectReadData);
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
        reqCtx->writeData_.append(writebuff1, len1);
        reqCtx->offset_ = 0;
        reqCtx->rawlength_ = len1;

        curve::common::CountDownEvent cond(1);
        RequestClosure *reqDone = new FakeRequestClosure(&cond, reqCtx);
        reqDone->SetFileMetric(&fm);
        reqDone->SetIOTracker(&iot);
        reqCtx->done_ = reqDone;

        std::vector<RequestContext *> reqCtxs;
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
        reqCtx->offset_ = 0;
        reqCtx->rawlength_ = len1;

        curve::common::CountDownEvent cond(1);
        RequestClosure *reqDone = new FakeRequestClosure(&cond, reqCtx);
        reqDone->SetFileMetric(&fm);
        reqDone->SetIOTracker(&iot);
        reqCtx->done_ = reqDone;

        std::vector<RequestContext *> reqCtxs;
        reqCtxs.push_back(reqCtx);
        ASSERT_EQ(0, requestScheduler.ScheduleRequest(reqCtxs));
        cond.Wait();
        butil::IOBuf expectReadData;
        expectReadData.append(cmpbuff1, len1);
        ASSERT_EQ(reqCtx->readData_, expectReadData);
        ASSERT_EQ(0, reqDone->GetErrorCode());
    }
    // 3. 在 delete snapshot
    {
        RequestContext *reqCtx = new FakeRequestContext();
        reqCtx->optype_ = OpType::DELETE_SNAP;
        reqCtx->idinfo_ = ChunkIDInfo(chunkId, logicPoolId, copysetId);


        reqCtx->correctedSeq_ = sn;
        reqCtx->offset_ = 0;
        reqCtx->rawlength_ = len1;

        curve::common::CountDownEvent cond(1);
        RequestClosure *reqDone = new FakeRequestClosure(&cond, reqCtx);
        reqDone->SetFileMetric(&fm);
        reqDone->SetIOTracker(&iot);
        reqCtx->done_ = reqDone;

        std::vector<RequestContext *> reqCtxs;
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


        reqCtx->correctedSeq_ = sn;
        reqCtx->offset_ = 0;
        reqCtx->rawlength_ = len1;

        curve::common::CountDownEvent cond(1);
        RequestClosure *reqDone = new FakeRequestClosure(&cond, reqCtx);
        reqDone->SetFileMetric(&fm);
        reqDone->SetIOTracker(&iot);
        reqCtx->done_ = reqDone;

        std::vector<RequestContext *> reqCtxs;
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
        reqDone->SetFileMetric(&fm);
        reqDone->SetIOTracker(&iot);
        reqCtx->done_ = reqDone;

        std::vector<RequestContext *> reqCtxs;
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
        reqDone->SetFileMetric(&fm);
        reqDone->SetIOTracker(&iot);
        reqCtx->done_ = reqDone;

        std::vector<RequestContext *> reqCtxs;
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
        reqDone->SetFileMetric(&fm);
        reqDone->SetIOTracker(&iot);
        reqCtx->done_ = reqDone;

        std::vector<RequestContext *> reqCtxs;
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
        reqCtx->writeData_.append(writebuff, len);
        reqCtx->offset_ = offset + i;
        reqCtx->rawlength_ = len;

        curve::common::CountDownEvent cond(1);
        RequestClosure *reqDone = new FakeRequestClosure(&cond, reqCtx);
        reqDone->SetFileMetric(&fm);
        reqDone->SetIOTracker(&iot);
        reqCtx->done_ = reqDone;

        std::vector<RequestContext *> reqCtxs;
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
        reqCtx->offset_ = offset + i;
        reqCtx->rawlength_ = len;

        curve::common::CountDownEvent cond(1);
        RequestClosure *reqDone = new FakeRequestClosure(&cond, reqCtx);
        reqDone->SetFileMetric(&fm);
        reqDone->SetIOTracker(&iot);
        reqCtx->done_ = reqDone;

        std::vector<RequestContext *> reqCtxs;
        reqCtxs.push_back(reqCtx);
        ASSERT_EQ(0, requestScheduler.ScheduleRequest(reqCtxs));
        cond.Wait();
        butil::IOBuf expectReadData;
        expectReadData.append(cmpbuff, len);
        ASSERT_EQ(reqCtx->readData_, expectReadData);
        ASSERT_EQ(0, reqDone->GetErrorCode());
    }
    {
        RequestContext *reqCtx = new FakeRequestContext();
        reqCtx->optype_ = OpType::UNKNOWN;
        reqCtx->idinfo_ = ChunkIDInfo(chunkId, logicPoolId, copysetId);


        memset(readbuff, '0', 8);
        // reqCtx->readBuffer_ = readbuff;
        reqCtx->offset_ = offset;
        reqCtx->rawlength_ = len;

        curve::common::CountDownEvent cond(1);
        RequestClosure *reqDone = new FakeRequestClosure(&cond, reqCtx);
        reqDone->SetFileMetric(&fm);
        reqDone->SetIOTracker(&iot);
        reqCtx->done_ = reqDone;

        std::vector<RequestContext *> reqCtxs;
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
            reqCtx->writeData_.append(writebuff, len);
            reqCtx->offset_ = offset + i;
            reqCtx->rawlength_ = len;

            RequestClosure *reqDone = new FakeRequestClosure(&cond, reqCtx);
            reqDone->SetFileMetric(&fm);
            reqDone->SetIOTracker(&iot);
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
        // reqCtx->readBuffer_ = readbuff;
        reqCtx->offset_ = offset + i;
        reqCtx->rawlength_ = len;
        curve::common::CountDownEvent cond(1);
        RequestClosure *reqDone = new FakeRequestClosure(&cond, reqCtx);
        reqDone->SetFileMetric(&fm);
        reqDone->SetIOTracker(&iot);
        reqCtx->done_ = reqDone;
        ASSERT_EQ(0, requestScheduler.ScheduleRequest(reqCtx));
        cond.Wait();
        butil::IOBuf expectReadData;
        expectReadData.append(cmpbuff, len);
        ASSERT_EQ(reqCtx->readData_, expectReadData);
        ASSERT_EQ(0, reqDone->GetErrorCode());
    }

    requestScheduler.Fini();
    ASSERT_EQ(0, server.Stop(0));
    ASSERT_EQ(0, server.Join());
}

TEST(RequestSchedulerTest, CommonTest) {
    RequestScheduleOption opt;
    opt.scheduleQueueCapacity = 4096;
    opt.scheduleThreadpoolSize = 2;
    opt.ioSenderOpt.failRequestOpt.chunkserverRPCTimeoutMS = 200;
    opt.ioSenderOpt.failRequestOpt.chunkserverOPMaxRetry = 5;
    opt.ioSenderOpt.failRequestOpt.chunkserverOPRetryIntervalUS = 5000;

    RequestScheduler sche;
    MetaCache metaCache;
    FileMetric fm("test");

    // scheduleQueueCapacity 设置为 0
    opt.scheduleQueueCapacity = 0;
    ASSERT_EQ(-1, sche.Init(opt, &metaCache, &fm));

    // threadpoolsize 设置为 0
    opt.scheduleQueueCapacity = 4096;
    opt.scheduleThreadpoolSize = 0;
    ASSERT_EQ(-1, sche.Init(opt, &metaCache, &fm));

    opt.scheduleQueueCapacity = 4096;
    opt.scheduleThreadpoolSize = 2;

    ASSERT_EQ(0, sche.Init(opt, &metaCache, &fm));
    ASSERT_EQ(0, sche.Run());
    ASSERT_EQ(0, sche.Run());
    ASSERT_EQ(0, sche.Fini());
    ASSERT_EQ(0, sche.Fini());
}

}   // namespace client
}   // namespace curve
