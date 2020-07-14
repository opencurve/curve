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

/**
 * Project: nebd
 * Create Date: 2020-01-20
 * Author: wuhanqing
 */

#include <gtest/gtest.h>
#include <gflags/gflags.h>
#include <brpc/server.h>

#include <mutex>  // NOLINT
#include <condition_variable>  // NOLINT
#include <atomic>

#include "nebd/src/part1/nebd_client.h"
#include "nebd/src/part1/libnebd.h"
#include "nebd/src/part1/libnebd_file.h"

#include "nebd/test/part1/fake_file_service.h"
#include "nebd/test/part1/mock_file_service.h"
#include "nebd/test/utils/config_generator.h"

const char* kFileName = "nebd-test-filename";
const char* kFileNameWithSlash = "nebd-test-filenae//filename";
const char* kNebdServerTestAddress = "./nebd-client-test.sock";
const char* kNebdClientConf = "./nebd/test/part1/nebd-client-test.conf";
const int64_t kFileSize = 10LL * 1024 * 1024 * 1024;
const int64_t kBufSize = 1024;

namespace nebd {
namespace client {

std::mutex mtx;
std::condition_variable cond;
std::atomic<bool> aioOpReturn{false};

void AioCallBack(NebdClientAioContext* ctx) {
    ASSERT_EQ(0, ctx->ret);
    ASSERT_EQ(0, ctx->retryCount);
    std::lock_guard<std::mutex> lk(mtx);
    aioOpReturn = true;
    cond.notify_one();
    delete ctx;
}

void AioResponseFailCallBack(NebdClientAioContext* ctx) {
    ASSERT_EQ(-1, ctx->ret);
    ASSERT_EQ(0, ctx->retryCount);
    std::lock_guard<std::mutex> lk(mtx);
    aioOpReturn = true;
    cond.notify_one();
    delete ctx;
}

void AioRpcFailCallBack(NebdClientAioContext* ctx) {
    ASSERT_EQ(0, ctx->ret);

    std::lock_guard<std::mutex> lk(mtx);
    aioOpReturn = true;
    cond.notify_one();
    delete ctx;
}

template <typename Request, typename Response>
void MockClientFunc(google::protobuf::RpcController* cntl_base,
                    const Request* request,
                    Response* response,
                    google::protobuf::Closure* done) {
    brpc::ClosureGuard doneGuard(done);
}

template <typename Request, typename Response, int RpcErrCode>
void MockClientRpcFailedFunc(google::protobuf::RpcController* cntl_base,
                             const Request* request,
                             Response* response,
                             google::protobuf::Closure* done) {
    brpc::ClosureGuard doneGuard(done);
    static int invokeTimes = 0;
    ++invokeTimes;

    if (invokeTimes < 10) {
        brpc::Controller* cntl = static_cast<brpc::Controller*>(cntl_base);
        cntl->SetFailed(RpcErrCode, "failed");
    } else {
        LOG(INFO) << "invoke 10 times, set response";
        response->set_retcode(RetCode::kOK);
    }
}

class NebdFileClientTest : public ::testing::Test {
 public:
    void SetUp() override {}

    void TearDown() override {}

    void AddFakeService() {
        ASSERT_EQ(0, server.AddService(
            &fakeService,
            brpc::SERVER_DOESNT_OWN_SERVICE)) << "Add service failed";
    }

    void AddMockService() {
        ASSERT_EQ(0, server.AddService(
            &mockService,
            brpc::SERVER_DOESNT_OWN_SERVICE)) << "Add service failed";
    }

    void StartServer(const std::string& address = kNebdServerTestAddress) {
        ASSERT_EQ(0, server.StartAtSockFile(
            address.c_str(), nullptr)) << "Start server failed";
    }

    void StopServer() {
        server.Stop(0);
        server.Join();
    }

    brpc::Server server;
    FakeNebdFileService fakeService;
    MockNebdFileService mockService;
};

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

TEST_F(NebdFileClientTest, AioRpcFailTest) {
    AddMockService();
    StartServer();
    ASSERT_EQ(0, Init4Nebd(kNebdClientConf));

    char buffer[kBufSize];

    {
        NebdClientAioContext* ctx = new NebdClientAioContext();
        ctx->buf = buffer;
        ctx->offset = 0;
        ctx->length = kBufSize;
        ctx->ret = 0;
        ctx->op = LIBAIO_OP_WRITE;
        ctx->cb = AioRpcFailCallBack;
        ctx->retryCount = 0;

        EXPECT_CALL(mockService, Write(_, _, _, _))
            .Times(10)
            .WillRepeatedly(
                Invoke(MockClientRpcFailedFunc<WriteRequest, WriteResponse, EINVAL>));  // NOLINT

        aioOpReturn = false;
        auto start = std::chrono::system_clock::now();
        ASSERT_EQ(0, AioWrite4Nebd(1, ctx));

        std::unique_lock<std::mutex> ulk(mtx);
        cond.wait(ulk, []() { return aioOpReturn.load(); });
        ASSERT_TRUE(aioOpReturn.load());
        auto end = std::chrono::system_clock::now();
        auto elpased = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();  // NOLINT

        // 重试睡眠时间: 100ms + 200ms + ... + 900ms = 4500ms
        ASSERT_TRUE(elpased >= 4000 && elpased <= 5000);
    }

    {
        NebdClientAioContext* ctx = new NebdClientAioContext();
        ctx->buf = buffer;
        ctx->offset = 0;
        ctx->length = kBufSize;
        ctx->ret = 0;
        ctx->op = LIBAIO_OP_READ;
        ctx->cb = AioRpcFailCallBack;
        ctx->retryCount = 0;

        EXPECT_CALL(mockService, Read(_, _, _, _))
            .Times(10)
            .WillRepeatedly(
                Invoke(MockClientRpcFailedFunc<ReadRequest, ReadResponse, EINVAL>));  // NOLINT
        aioOpReturn = false;
        ASSERT_EQ(0, AioRead4Nebd(1, ctx));

        std::unique_lock<std::mutex> ulk(mtx);
        cond.wait(ulk, []() { return aioOpReturn.load(); });
        ASSERT_TRUE(aioOpReturn.load());
    }

    {
        NebdClientAioContext* ctx = new NebdClientAioContext();
        ctx->buf = 0;
        ctx->offset = 0;
        ctx->length = kBufSize;
        ctx->ret = 0;
        ctx->op = LIBAIO_OP_DISCARD;
        ctx->cb = AioRpcFailCallBack;
        ctx->retryCount = 0;

        EXPECT_CALL(mockService, Discard(_, _, _, _))
            .Times(10)
            .WillRepeatedly(
                Invoke(MockClientRpcFailedFunc<DiscardRequest, DiscardResponse, EINVAL>));  // NOLINT
        aioOpReturn = false;
        ASSERT_EQ(0, Discard4Nebd(1, ctx));

        std::unique_lock<std::mutex> ulk(mtx);
        cond.wait(ulk, []() { return aioOpReturn.load(); });
        ASSERT_TRUE(aioOpReturn.load());
    }

    {
        NebdClientAioContext* ctx = new NebdClientAioContext();
        ctx->buf = 0;
        ctx->offset = 0;
        ctx->length = kBufSize;
        ctx->ret = 0;
        ctx->op = LIBAIO_OP_FLUSH;
        ctx->cb = AioRpcFailCallBack;
        ctx->retryCount = 0;

        EXPECT_CALL(mockService, Flush(_, _, _, _))
            .Times(10)
            .WillRepeatedly(
                Invoke(MockClientRpcFailedFunc<FlushRequest, FlushResponse, EINVAL>));  // NOLINT
        aioOpReturn = false;
        ASSERT_EQ(0, Flush4Nebd(1, ctx));

        std::unique_lock<std::mutex> ulk(mtx);
        cond.wait(ulk, []() { return aioOpReturn.load(); });
        ASSERT_TRUE(aioOpReturn.load());
    }

    ASSERT_NO_THROW(Uninit4Nebd());
    StopServer();
}

TEST_F(NebdFileClientTest, NoNebdServerTest) {
    ASSERT_EQ(-1, Init4Nebd("/xxx/nebd-client.conf"));
    ASSERT_EQ(0, Init4Nebd(kNebdClientConf));

    {
        auto start = std::chrono::system_clock::now();
        ASSERT_EQ(-1, Open4Nebd(kFileName));
        auto end = std::chrono::system_clock::now();
        auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
            end - start).count();

        // rpc failed的清空下，睡眠100ms后继续重试，共重试10次
        ASSERT_TRUE(elapsed >= 900 && elapsed <= 1100);
    }
    ASSERT_EQ(-1, Extend4Nebd(1, kFileSize));
    ASSERT_EQ(-1, GetFileSize4Nebd(1));
    ASSERT_EQ(-1, GetInfo4Nebd(1));
    ASSERT_EQ(-1, InvalidCache4Nebd(1));
    ASSERT_EQ(-1, Close4Nebd(1));

    ASSERT_NO_THROW(Uninit4Nebd());
}

TEST_F(NebdFileClientTest, CommonTest) {
    AddFakeService();
    StartServer();

    ASSERT_EQ(0, Init4Nebd(kNebdClientConf));

    int fd = Open4Nebd(kFileName);
    ASSERT_GE(fd, 0);

    int fd2 = Open4Nebd(kFileNameWithSlash);
    ASSERT_GE(fd2, 0);
    ASSERT_EQ(0, Close4Nebd(fd2));

    ASSERT_EQ(0, Extend4Nebd(fd, kFileSize));
    ASSERT_EQ(kFileSize, GetFileSize4Nebd(fd));
    ASSERT_EQ(kFileSize, GetInfo4Nebd(fd));
    ASSERT_EQ(0, InvalidCache4Nebd(fd));

    char buffer[kBufSize];

    {
        NebdClientAioContext* ctx = new NebdClientAioContext();
        ctx->buf = buffer;
        ctx->offset = 0;
        ctx->length = kBufSize;
        ctx->ret = 0;
        ctx->op = LIBAIO_OP_WRITE;
        ctx->cb = AioCallBack;
        ctx->retryCount = 0;

        aioOpReturn = false;
        ASSERT_EQ(0, AioWrite4Nebd(fd, ctx));
        std::unique_lock<std::mutex> ulk(mtx);
        cond.wait(ulk, []() { return aioOpReturn.load(); });
        ASSERT_TRUE(aioOpReturn.load());
    }

    {
        NebdClientAioContext* ctx = new NebdClientAioContext();
        ctx->buf = buffer;
        ctx->offset = 0;
        ctx->length = kBufSize;
        ctx->ret = 0;
        ctx->op = LIBAIO_OP_READ;
        ctx->cb = AioCallBack;
        ctx->retryCount = 0;

        aioOpReturn = false;
        ASSERT_EQ(0, AioRead4Nebd(fd, ctx));
        std::unique_lock<std::mutex> ulk2(mtx);
        cond.wait(ulk2, []() { return aioOpReturn.load(); });
        ASSERT_TRUE(aioOpReturn.load());
    }

    {
        NebdClientAioContext* ctx = new NebdClientAioContext();
        ctx->buf = 0;
        ctx->offset = 0;
        ctx->length = kBufSize;
        ctx->ret = 0;
        ctx->op = LIBAIO_OP_DISCARD;
        ctx->cb = AioCallBack;
        ctx->retryCount = 0;

        aioOpReturn = false;
        ASSERT_EQ(0, Discard4Nebd(fd, ctx));
        std::unique_lock<std::mutex> ulk2(mtx);
        cond.wait(ulk2, []() { return aioOpReturn.load(); });
        ASSERT_TRUE(aioOpReturn.load());
    }

    {
        NebdClientAioContext* ctx = new NebdClientAioContext();
        ctx->buf = 0;
        ctx->offset = 0;
        ctx->length = kBufSize;
        ctx->ret = 0;
        ctx->op = LIBAIO_OP_FLUSH;
        ctx->cb = AioCallBack;
        ctx->retryCount = 0;

        aioOpReturn = false;
        ASSERT_EQ(0, Flush4Nebd(fd, ctx));
        std::unique_lock<std::mutex> ulk2(mtx);
        cond.wait(ulk2, []() { return aioOpReturn.load(); });
        ASSERT_TRUE(aioOpReturn.load());
    }

    ASSERT_EQ(0, Close4Nebd(fd));
    ASSERT_NO_THROW(Uninit4Nebd());
    StopServer();
}

TEST_F(NebdFileClientTest, ReOpenTest) {
    AddFakeService();
    StartServer();

    ASSERT_EQ(0, Init4Nebd(kNebdClientConf));

    int fd = Open4Nebd(kFileName);
    ASSERT_GT(fd, 0);

    // 文件已经被打开，并占用文件锁
    // 再次打开时，获取文件锁失败，直接返回
    ASSERT_EQ(-1, Open4Nebd(kFileName));

    ASSERT_EQ(0, Close4Nebd(fd));

    fd = Open4Nebd(kFileName);
    ASSERT_GT(fd, 0);
    ASSERT_EQ(0, Close4Nebd(fd));

    ASSERT_NO_THROW(Uninit4Nebd());

    StopServer();
}

TEST_F(NebdFileClientTest, ResponseFailTest) {
    AddMockService();
    StartServer();

    ASSERT_EQ(0, Init4Nebd(kNebdClientConf));

    {
        OpenFileResponse response;
        response.set_retcode(RetCode::kNoOK);
        EXPECT_CALL(mockService, OpenFile(_, _, _, _))
            .Times(1)
            .WillOnce(DoAll(
                SetArgPointee<2>(response),
                Invoke(MockClientFunc<OpenFileRequest, OpenFileResponse>)));  // NOLINT
        ASSERT_EQ(-1, Open4Nebd(kFileName));
    }

    {
        CloseFileResponse response;
        response.set_retcode(RetCode::kNoOK);
        EXPECT_CALL(mockService, CloseFile(_, _, _, _))
            .Times(1)
            .WillOnce(DoAll(
                SetArgPointee<2>(response),
                Invoke(MockClientFunc<CloseFileRequest, CloseFileResponse>)));  // NOLINT
        ASSERT_EQ(0, Close4Nebd(0));
    }

    {
        ResizeResponse response;
        response.set_retcode(RetCode::kNoOK);
        EXPECT_CALL(mockService, ResizeFile(_, _, _, _))
            .Times(1)
            .WillOnce(DoAll(
                SetArgPointee<2>(response),
                Invoke(MockClientFunc<ResizeRequest, ResizeResponse>)));
        ASSERT_EQ(-1, Extend4Nebd(1, kFileSize));
    }

    {
        GetInfoResponse response;
        response.set_retcode(RetCode::kNoOK);
        EXPECT_CALL(mockService, GetInfo(_, _, _, _))
            .Times(1)
            .WillOnce(DoAll(
                SetArgPointee<2>(response),
                Invoke(MockClientFunc<GetInfoRequest, GetInfoResponse>)));  // NOLINT
        ASSERT_EQ(-1, GetFileSize4Nebd(1));
    }

    {
        GetInfoResponse response;
        response.set_retcode(RetCode::kNoOK);
        EXPECT_CALL(mockService, GetInfo(_, _, _, _))
            .Times(1)
            .WillOnce(DoAll(
                SetArgPointee<2>(response),
                Invoke(MockClientFunc<GetInfoRequest, GetInfoResponse>)));  // NOLINT
        ASSERT_EQ(-1, GetInfo4Nebd(1));
    }

    {
        InvalidateCacheResponse response;
        response.set_retcode(RetCode::kNoOK);
        EXPECT_CALL(mockService, InvalidateCache(_, _, _, _))
            .Times(1)
            .WillOnce(DoAll(
                SetArgPointee<2>(response),
                Invoke(MockClientFunc<InvalidateCacheRequest, InvalidateCacheResponse>)));  // NOLINT
        ASSERT_EQ(-1, InvalidCache4Nebd(1));
    }

    char buffer[kBufSize];

    {
        NebdClientAioContext* ctx = new NebdClientAioContext();
        ctx->buf = buffer;
        ctx->offset = 0;
        ctx->length = kBufSize;
        ctx->ret = 0;
        ctx->op = LIBAIO_OP_WRITE;
        ctx->cb = AioResponseFailCallBack;
        ctx->retryCount = 0;

        WriteResponse response;
        response.set_retcode(RetCode::kNoOK);
        EXPECT_CALL(mockService, Write(_, _, _, _))
            .Times(1)
            .WillOnce(DoAll(
                SetArgPointee<2>(response),
                Invoke(MockClientFunc<WriteRequest, WriteResponse>)));  // NOLINT
        aioOpReturn = false;
        ASSERT_EQ(0, AioWrite4Nebd(1, ctx));
        std::unique_lock<std::mutex> ulk(mtx);
        cond.wait(ulk, []() { return aioOpReturn.load(); });
        ASSERT_TRUE(aioOpReturn.load());
    }

    {
        NebdClientAioContext* ctx = new NebdClientAioContext();
        ctx->buf = buffer;
        ctx->offset = 0;
        ctx->length = kBufSize;
        ctx->ret = 0;
        ctx->op = LIBAIO_OP_READ;
        ctx->cb = AioResponseFailCallBack;
        ctx->retryCount = 0;

        ReadResponse response;
        response.set_retcode(RetCode::kNoOK);
        EXPECT_CALL(mockService, Read(_, _, _, _))
            .Times(1)
            .WillOnce(DoAll(
                SetArgPointee<2>(response),
                Invoke(MockClientFunc<ReadRequest, ReadResponse>)));
        aioOpReturn = false;
        ASSERT_EQ(0, AioRead4Nebd(1, ctx));
        std::unique_lock<std::mutex> ulk(mtx);
        cond.wait(ulk, []() { return aioOpReturn.load(); });
        ASSERT_TRUE(aioOpReturn.load());
    }

    {
        NebdClientAioContext* ctx = new NebdClientAioContext();
        ctx->buf = 0;
        ctx->offset = 0;
        ctx->length = kBufSize;
        ctx->ret = 0;
        ctx->op = LIBAIO_OP_DISCARD;
        ctx->cb = AioResponseFailCallBack;
        ctx->retryCount = 0;

        DiscardResponse response;
        response.set_retcode(RetCode::kNoOK);
        EXPECT_CALL(mockService, Discard(_, _, _, _))
            .Times(1)
            .WillOnce(DoAll(
                SetArgPointee<2>(response),
                Invoke(MockClientFunc<DiscardRequest, DiscardResponse>)));  // NOLINT
        aioOpReturn = false;
        ASSERT_EQ(0, Discard4Nebd(1, ctx));
        std::unique_lock<std::mutex> ulk(mtx);
        cond.wait(ulk, []() { return aioOpReturn.load(); });
        ASSERT_TRUE(aioOpReturn.load());
    }

    {
        NebdClientAioContext* ctx = new NebdClientAioContext();
        ctx->buf = 0;
        ctx->offset = 0;
        ctx->length = kBufSize;
        ctx->ret = 0;
        ctx->op = LIBAIO_OP_FLUSH;
        ctx->cb = AioResponseFailCallBack;
        ctx->retryCount = 0;

        FlushResponse response;
        response.set_retcode(RetCode::kNoOK);
        EXPECT_CALL(mockService, Flush(_, _, _, _))
            .Times(1)
            .WillOnce(DoAll(
                SetArgPointee<2>(response),
                Invoke(MockClientFunc<FlushRequest, FlushResponse>)));
        aioOpReturn = false;
        ASSERT_EQ(0, Flush4Nebd(1, ctx));
        std::unique_lock<std::mutex> ulk(mtx);
        cond.wait(ulk, []() { return aioOpReturn.load(); });
        ASSERT_TRUE(aioOpReturn.load());
    }

    ASSERT_NO_THROW(Uninit4Nebd());
    StopServer();
}

TEST_F(NebdFileClientTest, InitAndUninitTest) {
    ASSERT_NO_FATAL_FAILURE(nebdClient.Uninit());

    AddFakeService();
    StartServer();

    ASSERT_NO_FATAL_FAILURE(nebdClient.Init(kNebdClientConf));
    ASSERT_NO_FATAL_FAILURE(nebdClient.Uninit());
    ASSERT_NO_FATAL_FAILURE(nebdClient.Uninit());

    StopServer();
}

}  // namespace client
}  // namespace nebd


int main(int argc, char* argv[]) {
    std::vector<std::string> nebdConfig {
        std::string("nebdserver.serverAddress=") + kNebdServerTestAddress,
        std::string("metacache.fileLockPath=/tmp"),
        std::string("request.syncRpcMaxRetryTimes=10"),
        std::string("log.path=.")
    };

    nebd::common::NebdClientConfigGenerator generator;
    generator.SetConfigPath(kNebdClientConf);
    generator.SetConfigOptions(nebdConfig);
    generator.Generate();

    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
