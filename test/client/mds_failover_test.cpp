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

// 测试mds failover切换状态机
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

    int currentWorkMDSIndex = 1;
    int mds0RetryTimes = 0;
    int mds1RetryTimes = 0;
    int mds2RetryTimes = 0;

    // 场景1： mds0、1、2, currentworkindex = 0, mds0, mds1, mds2都宕机，
    //        发到其rpc都以EHOSTDOWN返回，导致上层client会一直切换mds重试
    //        按照0-->1-->2持续进行
    //        每次rpc返回-EHOSTDOWN，会直接触发RPC切换。最终currentworkindex没有切换
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
    // 控制面接口调用, 1000为本次rpc的重试总时间
    rpcexcutor.DoRPCTask(task1, 1000);
    uint64_t endMS = TimeUtility::GetTimeofDayMs();
    ASSERT_GT(endMS - startMS, 1000 - 1);

    // 本次重试为轮询重试，每个mds的重试次数应该接近，不超过总的mds数量
    ASSERT_LT(abs(mds0RetryTimes - mds1RetryTimes), 3);
    ASSERT_LT(abs(mds2RetryTimes - mds1RetryTimes), 3);

    startMS = TimeUtility::GetTimeofDayMs();
    rpcexcutor.DoRPCTask(task1, 3000);
    endMS = TimeUtility::GetTimeofDayMs();
    ASSERT_GT(endMS - startMS, 3000 - 1);
    ASSERT_EQ(0, rpcexcutor.GetCurrentWorkIndex());

    // 场景2：mds0、1、2, currentworkindex = 0, mds0宕机，并且这时候将正在工作的
    //       mds索引切换到index2，预期client在index=0重试之后会直接切换到index 2
    //       mds2这这时候直接返回OK，rpc停止重试。
    //       预期client总共发送两次rpc，一次发送到mds0，另一次发送到mds2，跳过中间的
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
            // 本次返回ok，那么RPC应该成功了，不会再重试
            return LIBCURVE_ERROR::OK;
        }
    };
    startMS = TimeUtility::GetTimeofDayMs();
    rpcexcutor.DoRPCTask(task2, 1000);
    endMS = TimeUtility::GetTimeofDayMs();
    ASSERT_LT(endMS - startMS, 1000);
    ASSERT_EQ(2, rpcexcutor.GetCurrentWorkIndex());
    ASSERT_EQ(mds0RetryTimes, 1);
    ASSERT_EQ(mds1RetryTimes, 0);
    ASSERT_EQ(mds2RetryTimes, 1);

    // 场景3：mds0、1、2，currentworkindex = 1，且mds1宕机了，
    //       这时候会切换到mds0和mds2
    //       在切换到2之后，mds1又恢复了，这时候切换到mds1，然后rpc发送成功。
    //       这时候的切换顺序为1->2->0, 1->2->0, 1。
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
            // 当在mds1上重试到第三次的时候向上返回成功，停止重试
            if (mds1RetryTimes == 3) {
                return LIBCURVE_ERROR::OK;
            }
            return -ECONNREFUSED;
        }

        if (mdsindex == 2) {
            mds2RetryTimes++;
            return -brpc::ELOGOFF;
        }
    };

    startMS = TimeUtility::GetTimeofDayMs();
    rpcexcutor.DoRPCTask(task3, 1000);
    endMS = TimeUtility::GetTimeofDayMs();
    ASSERT_LT(endMS - startMS, 1000);
    ASSERT_EQ(mds0RetryTimes, 2);
    ASSERT_EQ(mds1RetryTimes, 3);
    ASSERT_EQ(mds2RetryTimes, 2);

    ASSERT_EQ(1, rpcexcutor.GetCurrentWorkIndex());

    // 场景4：mds0、1、2, currentWorkindex = 0, 但是发往mds1的rpc请求一直超时
    //       最后rpc返回结果是超时.
    //      对于超时的mds节点会连续重试mds.maxFailedTimesBeforeChangeMDS后切换
    //      当前mds.maxFailedTimesBeforeChangeMDS=2。
    //      所以重试逻辑应该是：0->0->1->2, 0->0->1->2, 0->0->1->2, ...
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
    };

    startMS = TimeUtility::GetTimeofDayMs();
    rpcexcutor.DoRPCTask(task4, 3000);
    endMS = TimeUtility::GetTimeofDayMs();
    ASSERT_GT(endMS - startMS, 3000 - 1);
    ASSERT_EQ(0, rpcexcutor.GetCurrentWorkIndex());
    // 本次重试为轮询重试，每个mds的重试次数应该接近，不超过总的mds数量
    ASSERT_GT(mds0RetryTimes, mds1RetryTimes + mds2RetryTimes);

    // 场景5：mds0、1、2，currentWorkIndex = 0
    //       但是rpc请求前10次全部返回EHOSTDOWN
    //       mds重试睡眠10ms，所以总共耗时100ms时间
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
    rpcexcutor.DoRPCTask(task5, 10000);  // 总重试时间10s
    endMS = TimeUtility::GetTimeofDayMs();
    ASSERT_GE(endMS - startMS, 100);

    // 场景6： mds在重试过程中一直返回EHOSTDOWN，总共重试5s
    rpcexcutor.SetCurrentWorkIndex(0);
    int calledTimes = 0;
    auto task6 = [&](int mdsindex, uint64_t rpctimeoutMs,
                     brpc::Channel* channel,
                     brpc::Controller* cntl) {
        ++calledTimes;
        return -EHOSTDOWN;
    };

    startMS = TimeUtility::GetTimeofDayMs();
    rpcexcutor.DoRPCTask(task6, 5 * 1000);  // 总重试时间5s
    endMS = TimeUtility::GetTimeofDayMs();
    ASSERT_GE(endMS - startMS, 5 * 1000 - 1);

    // 每次hostdown情况下，睡眠10ms，总重试时间5s，所以总共重试次数小于等于500次
    // 为了尽量减少误判，所以加入10次冗余
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
std::string configpath = "./test/client/mds_failover.conf";   // NOLINT
int main(int argc, char ** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    ::testing::InitGoogleMock(&argc, argv);
    google::ParseCommandLineFlags(&argc, &argv, false);

    int ret = RUN_ALL_TESTS();
    return ret;
}
