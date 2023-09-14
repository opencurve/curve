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
 * File Created: Friday, 12th July 2019 11:29:28 am
 * Author: tongguangxun
 */

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include <chrono>              //NOLINT
#include <condition_variable>  //NOLINT
#include <mutex>               // NOLINT
#include <string>
#include <thread>  //NOLINT

#include "src/client/client_common.h"
#include "src/client/client_config.h"
#include "src/client/config_info.h"
#include "src/client/file_instance.h"
#include "src/client/iomanager4file.h"
#include "src/client/lease_executor.h"
#include "src/client/mds_client.h"

namespace curve {
namespace client {

TEST(InflightRPCTest, TestMetric) {
    IOManager4File ioManager;
    IOOption ioOption;
    ioOption.ioSenderOpt.inflightOpt.fileMaxInFlightRPCNum = 1024;
    ioManager.Initialize("/test_metric", ioOption, nullptr);

    auto fileMetric = ioManager.GetMetric();
    RequestClosure requestClosure(nullptr);

    requestClosure.SetIOManager(&ioManager);
    requestClosure.SetFileMetric(fileMetric);

    ASSERT_EQ(0, fileMetric->inflightRPCNum.get_value());

    requestClosure.GetInflightRPCToken();
    ASSERT_EQ(1, fileMetric->inflightRPCNum.get_value());

    requestClosure.GetInflightRPCToken();
    ASSERT_EQ(2, fileMetric->inflightRPCNum.get_value());

    requestClosure.ReleaseInflightRPCToken();
    ASSERT_EQ(1, fileMetric->inflightRPCNum.get_value());

    requestClosure.ReleaseInflightRPCToken();
    ASSERT_EQ(0, fileMetric->inflightRPCNum.get_value());
}

TEST(InflightRPCTest, TestInflightRPC) {
    int maxInflightNum = 8;

    {
        // Number of inflight tests
        InflightControl control;
        control.SetMaxInflightNum(maxInflightNum);
        ASSERT_EQ(0, control.GetCurrentInflightNum());

        for (int i = 1; i <= maxInflightNum; ++i) {
            control.GetInflightToken();
        }
        ASSERT_EQ(maxInflightNum, control.GetCurrentInflightNum());

        for (int i = 1; i <= maxInflightNum; ++i) {
            control.ReleaseInflightToken();
        }
        ASSERT_EQ(0, control.GetCurrentInflightNum());
    }

    {
        // Testing the concurrency of GetInflightTokan and ReleaseInflightToken
        InflightControl control;
        control.SetMaxInflightNum(maxInflightNum);

        for (int i = 1; i <= maxInflightNum; ++i) {
            control.GetInflightToken();
        }
        ASSERT_EQ(maxInflightNum, control.GetCurrentInflightNum());

        volatile bool flag = false;
        auto getTask = [&]() {
            ASSERT_FALSE(flag);
            control.GetInflightToken();
            ASSERT_TRUE(flag);
        };
        auto releaseTask = [&]() {
            std::this_thread::sleep_for(std::chrono::seconds(1));
            flag = true;
            control.ReleaseInflightToken();
        };

        std::thread t1(getTask);
        std::thread t2(releaseTask);

        t1.join();
        t2.join();

        for (int i = 1; i <= maxInflightNum; ++i) {
            control.ReleaseInflightToken();
        }
        ASSERT_EQ(0, control.GetCurrentInflightNum());
    }

    {
        // Testing WaitInflightAllComeBack
        InflightControl control;
        control.SetMaxInflightNum(maxInflightNum);
        for (int i = 1; i <= maxInflightNum; ++i) {
            control.GetInflightToken();
        }
        ASSERT_EQ(maxInflightNum, control.GetCurrentInflightNum());

        auto waitInflightAllComeback = [&]() {
            control.WaitInflightAllComeBack();
            ASSERT_EQ(0, control.GetCurrentInflightNum());
        };

        std::thread t(waitInflightAllComeback);

        for (int i = 1; i <= maxInflightNum; ++i) {
            control.ReleaseInflightToken();
        }

        t.join();
        ASSERT_EQ(0, control.GetCurrentInflightNum());
    }
}

TEST(InflightRPCTest, FileCloseTest) {
// Test that when the lease renewal fails at the time of file closure, it will not invoke the already destructed resources of the IO manager.
// The lease duration is 10 seconds, and only one renewal is allowed during the lease period. 
// If the renewal fails, it will trigger the IO manager's block IO, which actually calls the LeaseTimeoutBlockIO of the scheduler.
    IOOption ioOption;
    ioOption.reqSchdulerOpt.ioSenderOpt.failRequestOpt.chunkserverRPCTimeoutMS =
        10000;
    // Set the maximum number of inflight RPCs to 1
    ioOption.ioSenderOpt.inflightOpt.fileMaxInFlightRPCNum = 1;

    std::condition_variable cv;
    std::mutex mtx;
    bool inited = false;

    std::condition_variable resumecv;
    std::mutex resumemtx;
    bool resume = true;

    IOManager4File* iomanager;

    auto f1 = [&]() {
        for (int i = 0; i < 50; i++) {
            {
                std::unique_lock<std::mutex> lk(resumemtx);
                resumecv.wait(lk, [&]() { return resume; });
                resume = false;
            }
            iomanager = new IOManager4File();
            ASSERT_TRUE(iomanager->Initialize("/", ioOption, nullptr));

            {
                std::unique_lock<std::mutex> lk(mtx);
                inited = true;
                cv.notify_one();
            }
            iomanager->UnInitialize();
        }
    };

    auto f2 = [&]() {
        for (int i = 0; i < 50; i++) {
            {
                std::unique_lock<std::mutex> lk(mtx);
                cv.wait(lk, [&]() { return inited; });
                inited = false;
            }

            LeaseOption lopt;
            lopt.mdsRefreshTimesPerLease = 1;
            UserInfo_t userinfo("test", "");
            LeaseExecutor lease(lopt, userinfo, nullptr, iomanager);

            for (int j = 0; j < 5; j++) {
                // After testing the iomanager exit, please call its scheduler resource again without crashing
                lease.InvalidLease();
            }

            lease.Stop();

            {
                std::unique_lock<std::mutex> lk(resumemtx);
                resume = true;
                resumecv.notify_one();
            }
        }
    };

    // Concurrently run two threads: one thread initializes the IO manager and then deinitializes it,
    // while the other thread initiates lease renewal and then calls the IO manager to make it block IO.
    // Expectation: Concurrent execution of the two threads should not result in concurrent competition
    // for shared resources, even if the lease thread fails to renew while the IO manager thread exits.
    std::thread t1(f1);
    std::thread t2(f2);

    t1.joinable() ? t1.join() : void();
    t2.joinable() ? t2.join() : void();
}

}  // namespace client
}  // namespace curve

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
