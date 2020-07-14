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
 * Create Date: 2020-03-03
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

const char* kFileName = "nebd-lib-test-filename";
const char* kNebdServerTestAddress = "./nebd-lib-unittest.sock";
const char* kNebdClientConf = "./nebd/test/part1/nebd-lib-test.conf";
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

class NebdLibTest : public ::testing::Test {
 public:
    void SetUp() override {
        ::system("mkdir -p /etc/nebd");
        std::string copyCmd = "cp " + std::string(kNebdClientConf)
                            + " /etc/nebd/nebd-client.conf";
        LOG(INFO) << copyCmd;
        int ret = ::system(copyCmd.c_str());
        ASSERT_EQ(0, ret) << "copy config file failed";
    }

    void TearDown() override {}

    void AddFakeService() {
        ASSERT_EQ(0, server.AddService(
            &fakeService,
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

TEST_F(NebdLibTest, CommonTest) {
    AddFakeService();
    StartServer();

    ASSERT_EQ(0, nebd_lib_init());
    ASSERT_EQ(0, nebd_lib_init());

    int fd = nebd_lib_open(kFileName);
    ASSERT_GE(fd, 0);

    ASSERT_EQ(0, nebd_lib_resize(fd, kFileSize));
    ASSERT_EQ(kFileSize, nebd_lib_filesize(fd));
    ASSERT_EQ(kFileSize, nebd_lib_getinfo(fd));
    ASSERT_EQ(0, nebd_lib_invalidcache(fd));

    ASSERT_EQ(-1, nebd_lib_pread(fd, 0, 0, 0));
    ASSERT_EQ(-1, nebd_lib_pwrite(fd, 0, 0, 0));

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

    ASSERT_EQ(0, nebd_lib_close(fd));
    ASSERT_EQ(0, nebd_lib_uninit());
    ASSERT_EQ(0, nebd_lib_uninit());
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
