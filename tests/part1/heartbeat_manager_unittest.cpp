/*
 * Project: nebd
 * Created Date: 2019-08-12
 * Author: hzchenwei7
 * Copyright (c) 2018 netease
 */

#include <gtest/gtest.h>
#include <gflags/gflags.h>
#include <brpc/server.h>

#include <memory>
#include <thread>  // NOLINT

#include "src/part1/heartbeat_manager.h"
#include "src/part1/nebd_metacache.h"
#include "tests/part1/fake_heartbeat_service.h"

namespace nebd {
namespace client {

const char* kHeartBeatSockFile = "./heartbeat.test.sock";

class HeartbeatManagerTest : public testing::Test {
 public:
    void SetUp() override {
        metaCache = std::make_shared<NebdClientMetaCache>();
        manager.reset(new HeartbeatManager(metaCache));

        int ret = server.AddService(&fakeHeartBeatService,
                                    brpc::SERVER_DOESNT_OWN_SERVICE);
        ASSERT_EQ(0, ret) << "AddService heartbeat falied";

        ret = server.StartAtSockFile(kHeartBeatSockFile, nullptr);
        ASSERT_EQ(0, ret) << "Start Server failed";

        option.intervalS = 1;
        option.rpcTimeoutMs = 100;
        option.serverAddress = kHeartBeatSockFile;
    }

    void TearDown() override {
        server.Stop(0);
        server.Join();
    }

    std::unique_ptr<HeartbeatManager> manager;
    std::shared_ptr<NebdClientMetaCache> metaCache;
    brpc::Server server;
    FakeHeartbeatService fakeHeartBeatService;
    HeartbeatOption option;
};

TEST_F(HeartbeatManagerTest, InitTest) {
    ASSERT_EQ(0, manager->Init(
        option));
}

TEST_F(HeartbeatManagerTest, InvokeTimesTest) {
    ASSERT_EQ(0, manager->Init(
        option));

    manager->Run();

    // metaCache中数据为空，不发送心跳消息
    for (int i = 0; i < 10; ++i) {
        ASSERT_EQ(0, fakeHeartBeatService.GetInvokeTimes());
        std::this_thread::sleep_for(std::chrono::seconds(1));
    }

    // 添加数据
    NebdClientFileInfo fileInfo(1, "/test1", FileLock("/test1.lock"));
    metaCache->AddFileInfo(fileInfo);

    std::this_thread::sleep_for(std::chrono::seconds(10));
    int times = fakeHeartBeatService.GetInvokeTimes();
    ASSERT_TRUE(times >= 9 && times <= 11);

    // 清空metaCache数据
    metaCache->RemoveFileInfo(1);
    std::this_thread::sleep_for(std::chrono::seconds(2));

    fakeHeartBeatService.ClearInvokeTimes();
    std::this_thread::sleep_for(std::chrono::seconds(5));
    ASSERT_EQ(0, fakeHeartBeatService.GetInvokeTimes());

    manager->Stop();
}

TEST_F(HeartbeatManagerTest, RequestValidTest) {
    ASSERT_EQ(0, manager->Init(
        option));
    manager->Run();

    std::vector<FileInfo> currentFileInfos;

    // 添加一个文件
    NebdClientFileInfo fileInfo(1, "/test1", FileLock("/test1.lock"));
    metaCache->AddFileInfo(fileInfo);
    FileInfo info;
    info.set_fd(1);
    info.set_name("/test1");
    currentFileInfos.push_back(info);

    std::this_thread::sleep_for(std::chrono::seconds(2));
    auto latestFileInfos = fakeHeartBeatService.GetLatestRequestFileInfos();
    ASSERT_EQ(1, latestFileInfos.size());

    for (int i = 0; i < currentFileInfos.size(); ++i) {
        ASSERT_EQ(currentFileInfos[i].fd(), latestFileInfos[i].fd());
        ASSERT_EQ(currentFileInfos[i].name(), latestFileInfos[i].name());
    }

    // 添加第二个文件
    fileInfo = NebdClientFileInfo(2, "/test2", FileLock("/test2.lock"));
    metaCache->AddFileInfo(fileInfo);
    info.set_fd(2);
    info.set_name("/test2");
    currentFileInfos.push_back(info);

    std::this_thread::sleep_for(std::chrono::seconds(2));
    latestFileInfos = fakeHeartBeatService.GetLatestRequestFileInfos();
    ASSERT_EQ(2, latestFileInfos.size());

    std::sort(latestFileInfos.begin(), latestFileInfos.end(),
              [](const FileInfo& lhs, const FileInfo& rhs) {
                  return lhs.fd() < rhs.fd();
              });

    for (int i = 0; i < currentFileInfos.size(); ++i) {
        ASSERT_EQ(currentFileInfos[i].fd(), latestFileInfos[i].fd());
        ASSERT_EQ(currentFileInfos[i].name(), latestFileInfos[i].name());
    }

    // 删除第一个文件
    metaCache->RemoveFileInfo(1);
    currentFileInfos.erase(currentFileInfos.begin());

    std::this_thread::sleep_for(std::chrono::seconds(2));
    latestFileInfos = fakeHeartBeatService.GetLatestRequestFileInfos();
    ASSERT_EQ(1, latestFileInfos.size());

    for (int i = 0; i < currentFileInfos.size(); ++i) {
        ASSERT_EQ(currentFileInfos[i].fd(), latestFileInfos[i].fd());
        ASSERT_EQ(currentFileInfos[i].name(), latestFileInfos[i].name());
    }

    manager->Stop();
}

}  // namespace client
}  // namespace nebd

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

