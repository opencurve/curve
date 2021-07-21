/*
 *  Copyright (c) 2021 NetEase Inc.
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
 * @Project: curve
 * @Date: 2021-06-10 10:46:50
 * @Author: chenwei
 */

#include "curvefs/src/mds/mds.h"
#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <thread>  // NOLINT
#include "src/kvstorageclient/etcd_client.h"
#include "src/common/timeutility.h"

using ::testing::AtLeast;
using ::testing::StrEq;
using ::testing::_;
using ::testing::Return;
using ::testing::ReturnArg;
using ::testing::DoAll;
using ::testing::SetArgPointee;
using ::testing::SaveArg;

using ::curve::kvstorage::EtcdClientImp;

namespace curvefs {
namespace mds {

const char* kEtcdAddr = "127.0.0.1:20032";
const char* kMdsListenAddr = "127.0.0.1:20035";

class MdsTest : public ::testing::Test {
 protected:
    void SetUp() override {}

    void TearDown() override {}

    static void ClearEnv() {
        system("rm -rf curve_fs_test_mds.etcd");
    }

    static void StartEtcd() {
        etcdPid_ = fork();

        ASSERT_GE(etcdPid_, 0);

        if (etcdPid_ == 0) {
            std::string cmd =
                std::string("etcd --listen-client-urls") +
                std::string(" 'http://localhost:20032'") +
                std::string(" --advertise-client-urls") +
                std::string(" 'http://localhost:20032'") +
                std::string(" --listen-peer-urls 'http://localhost:20033'") +
                std::string(" --name curve_fs_test_mds");

            LOG(INFO) << "start etcd: " << cmd;

            ASSERT_EQ(0, execl("/bin/sh", "sh", "-c", cmd.c_str(), nullptr));
            exit(0);
        }

        auto client = std::make_shared<EtcdClientImp>();
        EtcdConf conf{const_cast<char*>(kEtcdAddr), strlen(kEtcdAddr), 1000};
        uint64_t now = curve::common::TimeUtility::GetTimeofDaySec();
        bool initSucc = false;
        while (curve::common::TimeUtility::GetTimeofDaySec() - now <= 5) {
            if (0 == client->Init(conf, 0, 3)) {
                initSucc = true;
                break;
            }
        }

        ASSERT_TRUE(initSucc);
        ASSERT_EQ(EtcdErrCode::EtcdDeadlineExceeded,
                  client->Put("05", "hello word"));
        ASSERT_EQ(EtcdErrCode::EtcdDeadlineExceeded,
                  client->CompareAndSwap("04", "10", "110"));
        client->CloseClient();
    }

    static void SetUpTestCase() {
        ClearEnv();
        StartEtcd();
    }

    static void TearDownTestCase() {
        system(("kill -9 " + std::to_string(etcdPid_)).c_str());
        std::this_thread::sleep_for(std::chrono::seconds(2));
        ClearEnv();
    }

 protected:
    static pid_t etcdPid_;
};

pid_t MdsTest::etcdPid_ = 0;

TEST_F(MdsTest, test1) {
    curvefs::mds::Mds mds;
    auto conf = std::make_shared<Configuration>();
    conf->SetConfigPath("curvefs/conf/mds.conf");
    ASSERT_TRUE(conf->LoadConfig());
    conf->SetStringValue("mds.listen.addr", kMdsListenAddr);
    conf->SetStringValue("etcd.endpoint", kEtcdAddr);

    // initialize MDS options
    mds.InitOptions(conf);

    // Initialize other modules after winning election
    mds.Init();

    // start mds server and wait CTRL+C to quit
    // mds.Run();
    std::thread mdsThread(&Mds::Run, &mds);

    // sleep 5s
    sleep(3);

    // stop server and background threads
    mds.Stop();
    mdsThread.join();
}

TEST_F(MdsTest, test2) {
    curvefs::mds::Mds mds;
    auto conf = std::make_shared<Configuration>();
    conf->SetConfigPath("curvefs/conf/mds.conf");
    ASSERT_TRUE(conf->LoadConfig());
    conf->SetStringValue("mds.listen.addr", kMdsListenAddr);
    conf->SetStringValue("etcd.endpoint", kEtcdAddr);

    // initialize MDS options
    mds.InitOptions(conf);

    // not init, run
    mds.Run();
    mds.Run();

    // not start, stop
    mds.Stop();
    mds.Stop();

    // Initialize other modules after winning election
    mds.Init();
    mds.Init();

    // start mds server and wait CTRL+C to quit
    // mds.Run();
    std::thread mdsThread(&Mds::Run, &mds);

    // sleep 5s
    sleep(3);

    // stop server and background threads
    mds.Stop();
    mdsThread.join();
}
}  // namespace mds
}  // namespace curvefs
