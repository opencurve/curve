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
 * Created Date: April 2nd 2021
 * Author: wanghai01
 */

#include <gtest/gtest.h>
#include <glog/logging.h>
#include <thread>
#include <chrono>
#include <cstdlib>
#include <memory>
#include <string>
#include <ctime>
#include "src/common/timeutility.h"
#include "src/kvstorageclient/etcd_client.h"
#include "src/common/concurrent/dlock.h"

namespace curve {
namespace common {

using ::curve::kvstorage::EtcdClientImp;

class TestDLock : public ::testing::Test {
 protected:
    TestDLock() {}
    ~TestDLock() {}

    void SetUp() override {
        system("rm -fr testDLock.etcd");
        client_ = std::make_shared<EtcdClientImp>();
        char endpoints[] = "127.0.0.1:2375";
        EtcdConf conf = {endpoints, static_cast<int>(strlen(endpoints)), 1000};
        ASSERT_EQ(EtcdErrCode::EtcdDeadlineExceeded,
                  client_->Init(conf, 200, 3));

        etcdPid = ::fork();
        if (0 > etcdPid) {
            ASSERT_TRUE(false);
        } else if (0 == etcdPid) {
            std::string runEtcd =
                std::string("etcd --listen-client-urls") +
                std::string(" 'http://localhost:2375'") +
                std::string(" --advertise-client-urls") +
                std::string(" 'http://localhost:2375'") +
                std::string(" --listen-peer-urls 'http://localhost:2374'") +
                std::string(" --name testDLock");

            ASSERT_EQ(0, execl("/bin/sh", "sh", "-c", runEtcd.c_str(), NULL));
            exit(0);
        }

        // Try init for a certain period of time until etcd is fully up
        uint64_t now = ::curve::common::TimeUtility::GetTimeofDaySec();
        bool initSuccess = false;
        while (::curve::common::TimeUtility::GetTimeofDaySec() - now <= 5) {
            if (0 == client_->Init(conf, 0, 3)) {
                initSuccess = true;
                break;
            }
        }
        ASSERT_TRUE(initSuccess);
        ASSERT_EQ(EtcdErrCode::EtcdDeadlineExceeded,
                  client_->Put("01", "hello word"));
        client_->SetTimeout(5000);

        uint64_t dlockPfx = 10000;
        opts_ = {std::to_string(dlockPfx), 3, 5000, 3};
    }

    void TearDown() override {
        client_ = nullptr;
        system(("kill " + std::to_string(etcdPid)).c_str());
        std::this_thread::sleep_for(std::chrono::seconds(2));

        system("rm -fr testDLock.etcd");
    }

 protected:
    DLockOpts opts_;
    std::shared_ptr<EtcdClientImp> client_;
    pid_t etcdPid;
};

// 1. lock the object no competition
TEST_F(TestDLock, test_LockNoCmp) {
    // init dlock
    std::shared_ptr<DLock> dlock = std::make_shared<DLock>(opts_);
    ASSERT_GT(dlock->Init(), 0);
    ASSERT_EQ(EtcdErrCode::EtcdOK, dlock->Lock());

    // std::vector<std::string> out;
    // int res = client_->List("0", "", &out);
    // ASSERT_EQ(EtcdErrCode::EtcdOK, res);
    // ASSERT_EQ(1, out.size());

    ASSERT_EQ(EtcdErrCode::EtcdOK, dlock->Unlock());
}

TEST_F(TestDLock, test_LockCmpSuccess) {
    std::shared_ptr<DLock> dlock = std::make_shared<DLock>(opts_);
    ASSERT_GT(dlock->Init(), 0);
    ASSERT_EQ(EtcdErrCode::EtcdOK, dlock->Lock());

    auto anotherLock = [this] {
        std::shared_ptr<DLock> dlock = std::make_shared<DLock>(opts_);
        ASSERT_GT(dlock->Init(), 0);
        // wait until the lock is release or context timeout
        ASSERT_EQ(EtcdErrCode::EtcdOK, dlock->Lock());
        dlock->Unlock();
    };
    std::thread newLock(anotherLock);
    // 2s < 5000ms
    sleep(2);
    dlock->Unlock();
    newLock.join();
}

TEST_F(TestDLock, test_LockCmpSuccessAfterRetry) {
    std::shared_ptr<DLock> dlock = std::make_shared<DLock>(opts_);
    ASSERT_GT(dlock->Init(), 0);
    ASSERT_EQ(EtcdErrCode::EtcdOK, dlock->Lock());

    auto anotherLock = [this] {
        std::shared_ptr<DLock> dlock = std::make_shared<DLock>(opts_);
        ASSERT_GT(dlock->Init(), 0);
        // wait until the lock is release or context timeout
        ASSERT_EQ(EtcdErrCode::EtcdOK, dlock->Lock());
        dlock->Unlock();
    };
    std::thread newLock(anotherLock);
    // 6s > 5000ms
    sleep(6);
    dlock->Unlock();
    newLock.join();
}

TEST_F(TestDLock, test_LockCmpTimeout) {
    std::shared_ptr<DLock> dlock = std::make_shared<DLock>(opts_);
    ASSERT_GT(dlock->Init(), 0);
    ASSERT_EQ(EtcdErrCode::EtcdOK, dlock->Lock());

    auto anotherLock = [this] {
        std::shared_ptr<DLock> dlock = std::make_shared<DLock>(opts_);
        ASSERT_GT(dlock->Init(), 0);
        // wait until the lock is release or context timeout
        ASSERT_EQ(EtcdErrCode::EtcdDeadlineExceeded, dlock->Lock());
        dlock->Unlock();
    };
    std::thread newLock(anotherLock);
    // 21 > (4*5000)ms
    sleep(21);
    dlock->Unlock();
    newLock.join();
}

}  // namespace common
}  // namespace curve
