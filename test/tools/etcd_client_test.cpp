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
 * File Created: 2019-12-05
 * Author: charisu
 */

#include <gtest/gtest.h>
#include <thread>  //NOLINT
#include <chrono>  //NOLINT
#include <cstdlib>
#include "src/tools/etcd_client.h"
#include "src/common/timeutility.h"

class EtcdClientTest : public ::testing::Test {
 protected:
    void SetUp() {
        system("rm -fr test1.etcd");
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
                std::string(" --initial-advertise-peer-urls "
                                                "http://127.0.0.1:2367") +
                std::string(" --initial-cluster toolEtcdClientTest="
                                                "http://127.0.0.1:2367") +
                std::string(" --name toolEtcdClientTest");
            /**
             *  重要提示！！！！
             *  fork后，子进程尽量不要用LOG()打印，可能死锁！！！
             */
            ASSERT_EQ(0, execl("/bin/sh", "sh", "-c", runEtcd.c_str(), NULL));
            exit(0);
        }
        // 一定时间内尝试check直到etcd完全起来
        curve::tool::EtcdClient client;
        ASSERT_EQ(0, client.Init("127.0.0.1:2366"));
        bool running;
        uint64_t startTime = ::curve::common::TimeUtility::GetTimeofDaySec();
        while (::curve::common::TimeUtility::GetTimeofDaySec() - startTime <=
               5) {
            std::vector<std::string> leaderAddrVec;
            std::map<std::string, bool> onlineState;
            ASSERT_EQ(0,
                    client.GetEtcdClusterStatus(&leaderAddrVec, &onlineState));
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
    std::vector<std::string> leaderAddrVec;
    std::map<std::string, bool> onlineState;

    // 正常情况
    ASSERT_EQ(0, client.GetEtcdClusterStatus(&leaderAddrVec, &onlineState));
    std::map<std::string, bool> expected = { { "127.0.0.1:2366", true },
                                             { "127.0.0.1:2368", false } };
    ASSERT_EQ(expected, onlineState);
    ASSERT_EQ(1, leaderAddrVec.size());
    ASSERT_EQ("127.0.0.1:2366", leaderAddrVec[0]);

    // 空指针错误
    ASSERT_EQ(-1, client.GetEtcdClusterStatus(nullptr, &onlineState));
    ASSERT_EQ(-1, client.GetEtcdClusterStatus(&leaderAddrVec, nullptr));
}

TEST_F(EtcdClientTest, GetAndCheckEtcdVersion) {
    curve::tool::EtcdClient client;
    ASSERT_EQ(0, client.Init("127.0.0.1:2366"));

    // 正常情况
    std::string version;
    std::vector<std::string> failedList;
    ASSERT_EQ(0, client.GetAndCheckEtcdVersion(&version, &failedList));
    ASSERT_TRUE(failedList.empty());

    // 个别etcd获取version失败
    ASSERT_EQ(0, client.Init(etcdAddr));
    ASSERT_EQ(0, client.GetAndCheckEtcdVersion(&version, &failedList));
    ASSERT_EQ(1, failedList.size());
    ASSERT_EQ("127.0.0.1:2368", failedList[0]);
}
