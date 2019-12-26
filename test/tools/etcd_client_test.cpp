/*
 * Project: curve
 * File Created: 2019-12-05
 * Author: charisu
 * Copyright (c)￼ 2018 netease
 */

#include <gtest/gtest.h>
#include <thread> //NOLINT
#include <chrono> //NOLINT
#include <cstdlib>
#include "src/tools/etcd_client.h"
#include "src/common/timeutility.h"

class EtcdClientTest : public ::testing::Test {
 protected:
    void SetUp() {
        system("rm -fr test1.etcd");
        system("rm -fr test2.etcd");
        etcdPid = ::fork();
        if (0 > etcdPid) {
            ASSERT_TRUE(false);
        } else if (0 == etcdPid) {
            std::string runEtcd =
                std::string("etcd --listen-client-urls") +
                std::string(" http://127.0.0.1:2366") +
                std::string(" --advertise-client-urls") +
                std::string(" http://127.0.0.1:2366") +
                std::string(" --listen-peer-urls http://127.0.0.1:2367") +
                std::string(" --name test1");
            ASSERT_EQ(0, execl("/bin/sh", "sh", "-c", runEtcd.c_str(), NULL));
            exit(0);
        }
        // 一定时间内尝试check直到etcd完全起来
        curve::tool::EtcdClient client;
        ASSERT_EQ(0, client.Init("127.0.0.1:2366"));
        bool running;
        uint64_t startTime = ::curve::common::TimeUtility::GetTimeofDaySec();
        while (::curve::common::TimeUtility::GetTimeofDaySec() -
                                                        startTime <= 5) {
            std::string leaderAddr;
            std::map<std::string, bool> onlineState;
            ASSERT_EQ(0, client.GetEtcdClusterStatus(&leaderAddr,
                                                     &onlineState));
            if (onlineState["127.0.0.1:2366"]) {
                running = true;
                break;
            }
        }
        ASSERT_TRUE(running);
    }

    void TearDown() {
        system(("kill " + std::to_string(etcdPid)).c_str());
        std::this_thread::sleep_for(std::chrono::seconds(2));
    }

    pid_t etcdPid;
    const std::string etcdAddr = "127.0.0.1:2366,127.0.0.1:2368";
};

TEST_F(EtcdClientTest, GetEtcdClusterStatus) {
    curve::tool::EtcdClient client;
    // Init失败的情况
    ASSERT_EQ(-1, client.Init(""));
    // Init成功
    ASSERT_EQ(0, client.Init(etcdAddr));
    std::string leaderAddr;
    std::map<std::string, bool> onlineState;

    // 正常情况
    ASSERT_EQ(0, client.GetEtcdClusterStatus(&leaderAddr, &onlineState));
    std::map<std::string, bool> expected = {{"127.0.0.1:2366", true},
                                            {"127.0.0.1:2368", false}};
    ASSERT_EQ(expected, onlineState);
    ASSERT_EQ("127.0.0.1:2366", leaderAddr);

    // 空指针错误
    ASSERT_EQ(-1, client.GetEtcdClusterStatus(nullptr, &onlineState));
    ASSERT_EQ(-1, client.GetEtcdClusterStatus(&leaderAddr, nullptr));
}
