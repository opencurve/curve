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

#include "src/tools/etcd_client.h"

#include <gtest/gtest.h>

#include <chrono>  //NOLINT
#include <cstdlib>
#include <thread>  //NOLINT

#include "src/common/timeutility.h"

class EtcdClientTest : public ::testing::Test {
 protected:
    void SetUp() {
        system("rm -fr test1.etcd");
        etcdPid = ::fork();
        if (0 > etcdPid) {
            ASSERT_TRUE(false);
        } else if (0 == etcdPid) {
            /**
             * Important reminder!!!!
             * After forking, try not to use LOG() printing for child processes,
             * as it may cause deadlock!!!
             */
            ASSERT_EQ(
                0,
                execlp("etcd", "etcd", "--listen-client-urls",
                       "http://127.0.0.1:2366", "--advertise-client-urls",
                       "http://127.0.0.1:2366", "--listen-peer-urls",
                       "http://127.0.0.1:2367", "--initial-advertise-peer-urls",
                       "http://127.0.0.1:2367", "--initial-cluster",
                       "toolEtcdClientTest=http://127.0.0.1:2367", "--name",
                       "toolEtcdClientTest", nullptr));
            exit(0);
        }
        // Try checking for a certain period of time until the ETCD is
        // completely up
        curve::tool::EtcdClient client;
        ASSERT_EQ(0, client.Init("127.0.0.1:2366"));
        bool running;
        uint64_t startTime = ::curve::common::TimeUtility::GetTimeofDaySec();
        while (::curve::common::TimeUtility::GetTimeofDaySec() - startTime <=
               5) {
            std::vector<std::string> leaderAddrVec;
            std::map<std::string, bool> onlineState;
            ASSERT_EQ(
                0, client.GetEtcdClusterStatus(&leaderAddrVec, &onlineState));
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
        system("rm -rf toolEtcdClientTest.etcd");
    }

    pid_t etcdPid;
    const std::string etcdAddr = "127.0.0.1:2366,127.0.0.1:2368";
};

TEST_F(EtcdClientTest, GetEtcdClusterStatus) {
    curve::tool::EtcdClient client;
    // The situation of Init failure
    ASSERT_EQ(-1, client.Init(""));
    // Init succeeded
    ASSERT_EQ(0, client.Init(etcdAddr));
    std::vector<std::string> leaderAddrVec;
    std::map<std::string, bool> onlineState;

    // Normal situation
    ASSERT_EQ(0, client.GetEtcdClusterStatus(&leaderAddrVec, &onlineState));
    std::map<std::string, bool> expected = {{"127.0.0.1:2366", true},
                                            {"127.0.0.1:2368", false}};
    ASSERT_EQ(expected, onlineState);
    ASSERT_EQ(1, leaderAddrVec.size());
    ASSERT_EQ("127.0.0.1:2366", leaderAddrVec[0]);

    // Null pointer error
    ASSERT_EQ(-1, client.GetEtcdClusterStatus(nullptr, &onlineState));
    ASSERT_EQ(-1, client.GetEtcdClusterStatus(&leaderAddrVec, nullptr));
}

TEST_F(EtcdClientTest, GetAndCheckEtcdVersion) {
    curve::tool::EtcdClient client;
    ASSERT_EQ(0, client.Init("127.0.0.1:2366"));

    // Normal situation
    std::string version;
    std::vector<std::string> failedList;
    ASSERT_EQ(0, client.GetAndCheckEtcdVersion(&version, &failedList));
    ASSERT_TRUE(failedList.empty());

    // Individual ETCD failed to obtain version
    ASSERT_EQ(0, client.Init(etcdAddr));
    ASSERT_EQ(0, client.GetAndCheckEtcdVersion(&version, &failedList));
    ASSERT_EQ(1, failedList.size());
    ASSERT_EQ("127.0.0.1:2368", failedList[0]);
}
