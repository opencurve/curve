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
 * File Created: Thursday, 19th December 2019 9:59:32 pm
 * Author: tongguangxun
 */

#include <gtest/gtest.h>
#include <gflags/gflags.h>
#include <glog/logging.h>

#include <string>
#include <thread>   //NOLINT
#include <chrono>   //NOLINT
#include <vector>
#include <algorithm>

#include "src/client/client_common.h"
#include "src/client/file_instance.h"
#include "test/client/fake/mockMDS.h"
#include "src/client/metacache.h"
#include "test/client/fake/mock_schedule.h"
#include "include/client/libcurve.h"
#include "src/client/libcurve_file.h"
#include "src/client/client_config.h"
#include "src/client/service_helper.h"
#include "src/client/mds_client.h"
#include "src/client/config_info.h"
#include "test/client/fake/fakeMDS.h"
#include "src/client/metacache_struct.h"
#include "src/common/net_common.h"
#include "test/integration/cluster_common/cluster.h"
#include "test/util/config_generator.h"

namespace curve {
namespace client {

// Testing mds failover switching state machine
TEST(MDSChangeTest, MDSFailoverTest) {
    RPCExcutorRetryPolicy rpcexcutor;

    MetaServerOption  metaopt;
    metaopt.rpcRetryOpt.addrs.push_back("127.0.0.1:9903");
    metaopt.rpcRetryOpt.addrs.push_back("127.0.0.1:9904");
    metaopt.rpcRetryOpt.addrs.push_back("127.0.0.1:9905");

    metaopt.rpcRetryOpt.rpcTimeoutMs = 1000;
    metaopt.rpcRetryOpt.rpcRetryIntervalUS = 10000;  // 10ms
    metaopt.rpcRetryOpt.maxFailedTimesBeforeChangeAddr = 2;
    metaopt.rpcRetryOpt.rpcTimeoutMs = 1500;

    rpcexcutor.SetOption(metaopt.rpcRetryOpt);

    int mds0RetryTimes = 0;
    int mds1RetryTimes = 0;
    int mds2RetryTimes = 0;

    // Scenario 1: mds0, 1, 2, currentworkindex=0, mds0, mds1, and mds2 are all down,
    //        All RPCs sent to them are returned as EHOSTDOWN, resulting in upper level clients constantly switching to mds and retrying
    //        Continue according to 0-->1-->2
    //        Every time rpc returns -EHOSTDOWN, it will directly trigger RPC switching. The final currentworkindex did not switch
    auto task1 = [&](int mdsindex, uint64_t rpctimeoutMS,
                brpc::Channel* channel, brpc::Controller* cntl)->int {
        if (mdsindex == 0) {
            mds0RetryTimes++;
        }

        if (mdsindex == 1) {
            mds1RetryTimes++;
        }

        if (mdsindex == 2) {
            mds2RetryTimes++;
        }
        return -EHOSTDOWN;
    };

    uint64_t startMS = TimeUtility::GetTimeofDayMs();
    // Control surface interface call, 1000 is the total retry time of this RPC
    rpcexcutor.DoRPCTask(task1, 1000);
    uint64_t endMS = TimeUtility::GetTimeofDayMs();
    ASSERT_GT(endMS - startMS, 1000 - 1);

    // This retry is a polling retry, and the number of retries per mds should be close to and not exceed the total number of mds
    ASSERT_LT(abs(mds0RetryTimes - mds1RetryTimes), 3);
    ASSERT_LT(abs(mds2RetryTimes - mds1RetryTimes), 3);

    startMS = TimeUtility::GetTimeofDayMs();
    rpcexcutor.DoRPCTask(task1, 3000);
    endMS = TimeUtility::GetTimeofDayMs();
    ASSERT_GT(endMS - startMS, 3000 - 1);
    ASSERT_EQ(0, rpcexcutor.GetCurrentWorkIndex());

    // Scenario 2: mds0, 1, 2, currentworkindex = 0, mds0 goes down, and it will be working at this time
    //       Mds index switches to index2, and it is expected that the client will directly switch to index2 after retrying with index = 0
    //       At this point, mds2 directly returns OK and rpc stops trying again.
    //       Expected client to send a total of two RPCs, one to mds0 and the other to mds2, skipping the middle
    //       mds1。
    mds0RetryTimes = 0;
    mds1RetryTimes = 0;
    mds2RetryTimes = 0;
    auto task2 = [&](int mdsindex, uint64_t rpctimeoutMS,
                brpc::Channel* channel, brpc::Controller* cntl)->int {
        if (mdsindex == 0) {
            mds0RetryTimes++;
            rpcexcutor.SetCurrentWorkIndex(2);
            return -ECONNRESET;
        }

        if (mdsindex == 1) {
            mds1RetryTimes++;
            return -ECONNRESET;
        }

        if (mdsindex == 2) {
            mds2RetryTimes++;
            // If OK is returned this time, then RPC should have succeeded and will not try again
            return LIBCURVE_ERROR::OK;
        }

        return 0;
    };
    startMS = TimeUtility::GetTimeofDayMs();
    rpcexcutor.DoRPCTask(task2, 1000);
    endMS = TimeUtility::GetTimeofDayMs();
    ASSERT_LT(endMS - startMS, 1000);
    ASSERT_EQ(2, rpcexcutor.GetCurrentWorkIndex());
    ASSERT_EQ(mds0RetryTimes, 1);
    ASSERT_EQ(mds1RetryTimes, 0);
    ASSERT_EQ(mds2RetryTimes, 1);

    // Scenario 3: mds0, 1, 2, currentworkindex = 1, and mds1 is down,
    //      At this point, it will switch to mds0 and mds2
    //      After switching to 2, mds1 resumed, and then switched to mds1, and the rpc was successfully sent.
    //      At this point, the switching order is 1->2->0, 1->2->0, 1.
    mds0RetryTimes = 0;
    mds1RetryTimes = 0;
    mds2RetryTimes = 0;
    rpcexcutor.SetCurrentWorkIndex(1);
    auto task3 = [&](int mdsindex, uint64_t rpctimeoutMS,
                brpc::Channel* channel, brpc::Controller* cntl)->int {
        if (mdsindex == 0) {
            mds0RetryTimes++;
            return -ECONNRESET;
        }

        if (mdsindex == 1) {
            mds1RetryTimes++;
            // When retrying on mds1 for the third time, success is returned upwards and the retry is stopped
            if (mds1RetryTimes == 3) {
                return LIBCURVE_ERROR::OK;
            }
            return -ECONNREFUSED;
        }

        if (mdsindex == 2) {
            mds2RetryTimes++;
            return -brpc::ELOGOFF;
        }

        return 0;
    };

    startMS = TimeUtility::GetTimeofDayMs();
    rpcexcutor.DoRPCTask(task3, 1000);
    endMS = TimeUtility::GetTimeofDayMs();
    ASSERT_LT(endMS - startMS, 1000);
    ASSERT_EQ(mds0RetryTimes, 2);
    ASSERT_EQ(mds1RetryTimes, 3);
    ASSERT_EQ(mds2RetryTimes, 2);

    ASSERT_EQ(1, rpcexcutor.GetCurrentWorkIndex());

    // Scenario 4: mds0, 1, 2, currentWorkindex = 0, but the rpc request to mds1 consistently times out
    //       The final result returned by rpc is timeout
    //      For timeout mds nodes, they will continuously retry mds.maxFailedTimesBeforeChangeMDS and switch
    //      Current mds.maxFailedTimesBeforeChangeMDS=2.
    //       So the retry logic should be: 0->0->1->2, 0->0->1->2, 0->0->1->2, ...
    LOG(INFO) << "case 4";
    mds0RetryTimes = 0;
    mds1RetryTimes = 0;
    mds2RetryTimes = 0;
    rpcexcutor.SetCurrentWorkIndex(0);
    auto task4 = [&](int mdsindex, uint64_t rpctimeoutMS,
                brpc::Channel* channel, brpc::Controller* cntl)->int {
        if (mdsindex == 0) {
            mds0RetryTimes++;
            return mds0RetryTimes % 2 == 0 ? -brpc::ERPCTIMEDOUT
                                           : -ETIMEDOUT;
        }

        if (mdsindex == 1) {
            mds1RetryTimes++;
            return -ECONNREFUSED;
        }

        if (mdsindex == 2) {
            mds2RetryTimes++;
            return -brpc::ELOGOFF;
        }

        return 0;
    };

    startMS = TimeUtility::GetTimeofDayMs();
    rpcexcutor.DoRPCTask(task4, 3000);
    endMS = TimeUtility::GetTimeofDayMs();
    ASSERT_GT(endMS - startMS, 3000 - 1);
    ASSERT_EQ(0, rpcexcutor.GetCurrentWorkIndex());
    // This retry is a polling retry, and the number of retries per mds should be close to and not exceed the total number of mds
    ASSERT_GT(mds0RetryTimes, mds1RetryTimes + mds2RetryTimes);

    // Scenario 5: mds0, 1, 2, currentWorkIndex = 0
    //      But the first 10 requests from rpc all returned EHOSTDOWN
    //      Mds retries sleep for 10ms, so it takes a total of 100ms
    rpcexcutor.SetCurrentWorkIndex(0);
    int hostDownTimes = 10;
    auto task5 = [&](int mdsindex, uint64_t rpctimeoutMs,
                     brpc::Channel* channel,
                     brpc::Controller* cntl) {
        static int count = 0;
        if (++count <= hostDownTimes) {
            return -EHOSTDOWN;
        }

        return 0;
    };
    startMS = TimeUtility::GetTimeofDayMs();
    rpcexcutor.DoRPCTask(task5, 10000);  // Total retry time 10s
    endMS = TimeUtility::GetTimeofDayMs();
    ASSERT_GE(endMS - startMS, 100);

    // Scenario 6: mds keeps returning EHOSTDOWN during the retry process, with a total of 5 retries
    rpcexcutor.SetCurrentWorkIndex(0);
    int calledTimes = 0;
    auto task6 = [&](int mdsindex, uint64_t rpctimeoutMs,
                     brpc::Channel* channel,
                     brpc::Controller* cntl) {
        ++calledTimes;
        return -EHOSTDOWN;
    };

    startMS = TimeUtility::GetTimeofDayMs();
    rpcexcutor.DoRPCTask(task6, 5 * 1000);  // Total retry time 5s
    endMS = TimeUtility::GetTimeofDayMs();
    ASSERT_GE(endMS - startMS, 5 * 1000 - 1);

    // In each hostdown situation, sleep for 10ms and the total retry time is 5s, so the total number of retries is less than or equal to 500 times
    // In order to minimize false positives, 10 redundant attempts were added
    LOG(INFO) << "called times " << calledTimes;
    ASSERT_LE(calledTimes, 510);
}

}  // namespace client
}  // namespace curve

const std::vector<std::string> registConfOff {
    std::string("mds.listen.addr=127.0.0.1:9903,127.0.0.1:9904,127.0.0.1:9905"),
    std::string("rpcRetryTimes=3"),
    std::string("global.logPath=./runlog/"),
    std::string("chunkserver.rpcTimeoutMS=1000"),
    std::string("chunkserver.opMaxRetry=3"),
    std::string("metacache.getLeaderRetry=3"),
    std::string("metacache.getLeaderTimeOutMS=1000"),
    std::string("global.fileMaxInFlightRPCNum=2048"),
    std::string("metacache.rpcRetryIntervalUS=500"),
    std::string("mds.rpcRetryIntervalUS=500"),
    std::string("schedule.threadpoolSize=2"),
    std::string("mds.registerToMDS=false")
};

const std::vector<std::string> registConfON {
    std::string("mds.listen.addr=127.0.0.1:9903,127.0.0.1:9904,127.0.0.1:9905"),
    std::string("global.logPath=./runlog/"),
    std::string("synchronizeRPCTimeoutMS=500"),
    std::string("synchronizeRPCRetryTime=3"),
    std::string("chunkserver.rpcTimeoutMS=1000"),
    std::string("chunkserver.opMaxRetry=3"),
    std::string("metacache.getLeaderRetry=3"),
    std::string("metacache.getLeaderTimeOutMS=1000"),
    std::string("global.fileMaxInFlightRPCNum=2048"),
    std::string("metacache.rpcRetryIntervalUS=500"),
    std::string("mds.rpcRetryIntervalUS=500"),
    std::string("schedule.threadpoolSize=2"),
    std::string("mds.registerToMDS=true")
};

std::string mdsMetaServerAddr = "127.0.0.1:9903,127.0.0.1:9904,127.0.0.1:9905";     // NOLINT
uint32_t segment_size = 1 * 1024 * 1024 * 1024ul;   // NOLINT
uint32_t chunk_size = 4 * 1024 * 1024;   // NOLINT
std::string configpath = "./test/client/configs/mds_failover.conf";   // NOLINT
int main(int argc, char ** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    ::testing::InitGoogleMock(&argc, argv);
    google::ParseCommandLineFlags(&argc, &argv, false);

    int ret = RUN_ALL_TESTS();
    return ret;
}
