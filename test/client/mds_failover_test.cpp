/*
 * Project: curve
 * File Created: Thursday, 19th December 2019 9:59:32 pm
 * Author: tongguangxun
 * Copyright (c)￼ 2019 netease
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

using curve::client::MDSClient;

// 测试mds failover切换状态机
TEST(MDSChangeTest, MDSFailoverTest) {
    class MDSClientDerived : public MDSClient {
     public:
        MDSClient::MDSRPCExcutor rpcexcutor;
    };

    MetaServerOption_t  metaopt;
    metaopt.metaaddrvec.push_back("127.0.0.1:9903");
    metaopt.metaaddrvec.push_back("127.0.0.1:9904");
    metaopt.metaaddrvec.push_back("127.0.0.1:9905");

    metaopt.mdsRPCTimeoutMs = 1000;
    metaopt.mdsRPCRetryIntervalUS = 200;
    metaopt.mdsMaxFailedTimesBeforeChangeMDS = 2;
    metaopt.mdsRPCTimeoutMs = 1500;

    MDSClientDerived mdsd;
    mdsd.Initialize(metaopt);
    mdsd.rpcexcutor.SetOption(metaopt);

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
    mdsd.rpcexcutor.DoRPCTask(task1, 1000);
    uint64_t endMS = TimeUtility::GetTimeofDayMs();
    ASSERT_GT(endMS - startMS, 1000 - 1);

    // 本次重试为轮询重试，每个mds的重试次数应该接近，不超过总的mds数量
    ASSERT_LT(abs(mds0RetryTimes - mds1RetryTimes), 3);
    ASSERT_LT(abs(mds2RetryTimes - mds1RetryTimes), 3);

    startMS = TimeUtility::GetTimeofDayMs();
    mdsd.rpcexcutor.DoRPCTask(task1, 3000);
    endMS = TimeUtility::GetTimeofDayMs();
    ASSERT_GT(endMS - startMS, 3000 - 1);
    ASSERT_EQ(0, mdsd.rpcexcutor.GetCurrentWorkIndex());

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
            mdsd.rpcexcutor.SetCurrentWorkIndex(2);
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
    mdsd.rpcexcutor.DoRPCTask(task2, 1000);
    endMS = TimeUtility::GetTimeofDayMs();
    ASSERT_LT(endMS - startMS, 1000);
    ASSERT_EQ(2, mdsd.rpcexcutor.GetCurrentWorkIndex());
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
    mdsd.rpcexcutor.SetCurrentWorkIndex(1);
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
    mdsd.rpcexcutor.DoRPCTask(task3, 1000);
    endMS = TimeUtility::GetTimeofDayMs();
    ASSERT_LT(endMS - startMS, 1000);
    ASSERT_EQ(mds0RetryTimes, 2);
    ASSERT_EQ(mds1RetryTimes, 3);
    ASSERT_EQ(mds2RetryTimes, 2);

    ASSERT_EQ(1, mdsd.rpcexcutor.GetCurrentWorkIndex());

    // 场景4：mds0、1、2, currentWorkindex = 0, 但是发往mds1的rpc请求一直超时
    //       最后rpc返回结果是超时.
    //      对于超时的mds节点会连续重试mds.maxFailedTimesBeforeChangeMDS后切换
    //      当前mds.maxFailedTimesBeforeChangeMDS=2。
    //      所以重试逻辑应该是：0->0->1->2, 0->0->1->2, 0->0->1->2, ...
    LOG(INFO) << "case 4";
    mds0RetryTimes = 0;
    mds1RetryTimes = 0;
    mds2RetryTimes = 0;
    mdsd.rpcexcutor.SetCurrentWorkIndex(0);
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
    mdsd.rpcexcutor.DoRPCTask(task4, 3000);
    endMS = TimeUtility::GetTimeofDayMs();
    ASSERT_GT(endMS - startMS, 3000 - 1);
    ASSERT_EQ(0, mdsd.rpcexcutor.GetCurrentWorkIndex());
    // 本次重试为轮询重试，每个mds的重试次数应该接近，不超过总的mds数量
    ASSERT_GT(mds0RetryTimes, mds1RetryTimes + mds2RetryTimes);
}

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

TEST(MDSClientTestRegitser, Register) {
    std::string configpath = "./test/client/testConfig/mds_register_on.conf";

    curve::CurveCluster* cluster = new curve::CurveCluster();
    cluster->PrepareConfig<curve::ClientConfigGenerator>(
        configpath, registConfON);
    brpc::Server server1, server2, server3;
    MetaServerOption_t metaopt;
    metaopt.metaaddrvec.push_back("127.0.0.1:9903");
    metaopt.metaaddrvec.push_back("127.0.0.1:9904");
    metaopt.metaaddrvec.push_back("127.0.0.1:9905");

    metaopt.mdsMaxRetryMS = 1000;
    metaopt.mdsRPCTimeoutMs = 500;
    metaopt.mdsRPCRetryIntervalUS = 200;
    metaopt.mdsMaxFailedTimesBeforeChangeMDS = 2;

    curve::client::MDSClient mdsclient;
    mdsclient.Initialize(metaopt);

    FakeMDSCurveFSService curvefsservice1;
    FakeMDSCurveFSService curvefsservice2;
    FakeMDSCurveFSService curvefsservice3;

    if (server1.AddService(&curvefsservice1,
                        brpc::SERVER_DOESNT_OWN_SERVICE) != 0) {
        LOG(ERROR) << "Fail to add service";
    }

    if (server2.AddService(&curvefsservice2,
                        brpc::SERVER_DOESNT_OWN_SERVICE) != 0) {
        LOG(ERROR) << "Fail to add service";
    }

    if (server3.AddService(&curvefsservice3,
                        brpc::SERVER_DOESNT_OWN_SERVICE) != 0) {
        LOG(ERROR) << "Fail to add service";
    }

    brpc::ServerOptions options;
    options.idle_timeout_sec = -1;
    ASSERT_EQ(server1.Start("127.0.0.1:9903", &options), 0);
    ASSERT_EQ(server2.Start("127.0.0.1:9904", &options), 0);
    ASSERT_EQ(server3.Start("127.0.0.1:9905", &options), 0);

    RegistClientResponse* registResp1 = new RegistClientResponse();
    registResp1->set_statuscode(::curve::mds::StatusCode::kStorageError);
    FakeReturn* fakeregist1 = new FakeReturn(nullptr, static_cast<void*>(registResp1));      // NOLINT
    curvefsservice1.SetRegistRet(fakeregist1);

    // regist失败，初始化失败
    ASSERT_EQ(-LIBCURVE_ERROR::FAILED, Init(configpath.c_str()));

    // config with regist off
    std::string confpath = "./test/client/testConfig/mds_register_off.conf";
    // regist失败，初始化失败
    ASSERT_EQ(-LIBCURVE_ERROR::FAILED, Init(configpath.c_str()));

    cluster->PrepareConfig<curve::ClientConfigGenerator>(
        confpath, registConfOff);

    ASSERT_EQ(0, Init(confpath.c_str()));
    ASSERT_EQ(0, Init(confpath.c_str()));
    UnInit();

    RegistClientResponse* registResp = new RegistClientResponse();
    registResp->set_statuscode(::curve::mds::StatusCode::kOK);
    FakeReturn* fakeregist = new FakeReturn(nullptr, static_cast<void*>(registResp));      // NOLINT
    curvefsservice1.SetRegistRet(fakeregist);

    LOG(INFO) << "configpath = " << configpath;
    // regist成功，初始化成功
    std::string ip;
    curve::common::NetCommon::GetLocalIP(&ip);
    ASSERT_EQ(0, Init(configpath.c_str()));

    UnInit();

    // 设置mds1 EHOSTDOWN, 触发切换mds，会切换到mds2
    // mds rpc成功但是返回参数错误
    brpc::Controller cntl;
    cntl.SetFailed(EHOSTDOWN, "failed");
    RegistClientResponse* registResp3 = new RegistClientResponse();
    registResp3->set_statuscode(::curve::mds::StatusCode::kOK);
    FakeReturn* fakeregist3 = new FakeReturn(&cntl, static_cast<void*>(registResp3));      // NOLINT
    curvefsservice1.SetRegistRet(fakeregist3);

    RegistClientResponse* registResp2 = new RegistClientResponse();
    registResp2->set_statuscode(::curve::mds::StatusCode::kParaError);
    FakeReturn* fakeregist2 = new FakeReturn(nullptr, static_cast<void*>(registResp2));      // NOLINT
    curvefsservice2.SetRegistRet(fakeregist2);

    RegistClientResponse* registResp4 = new RegistClientResponse();
    registResp4->set_statuscode(::curve::mds::StatusCode::kNotSupported);
    FakeReturn* fakeregist4 = new FakeReturn(nullptr, static_cast<void*>(registResp4));      // NOLINT
    curvefsservice3.SetRegistRet(fakeregist4);
    ASSERT_EQ(-LIBCURVE_ERROR::FAILED, Init(configpath.c_str()));
    LOG(INFO) << "start test!";
    ASSERT_EQ(LIBCURVE_ERROR::INTERNAL_ERROR,
    mdsclient.Register(ip, 999));

    // 设置mds2 ECONNRESET，触发切换mds，切换到mds3，mds3返回ECONNREFUSED
    // 重新切回mds1, mds1 返回OK
    RegistClientResponse* registResp5 = new RegistClientResponse();
    registResp5->set_statuscode(::curve::mds::StatusCode::kOK);
    FakeReturn* fakeregist5 = new FakeReturn(nullptr, static_cast<void*>(registResp5));      // NOLINT
    curvefsservice1.SetRegistRet(fakeregist5);

    brpc::Controller cntl2;
    cntl2.SetFailed(ECONNRESET, "failed");
    RegistClientResponse* registResp6 = new RegistClientResponse();
    registResp6->set_statuscode(::curve::mds::StatusCode::kParaError);
    FakeReturn* fakeregist6 = new FakeReturn(&cntl2, static_cast<void*>(registResp6));      // NOLINT
    curvefsservice2.SetRegistRet(fakeregist6);

    brpc::Controller cntl3;
    cntl3.SetFailed(ECONNREFUSED, "failed");
    RegistClientResponse* registResp7 = new RegistClientResponse();
    registResp7->set_statuscode(::curve::mds::StatusCode::kNotSupported);
    FakeReturn* fakeregist7 = new FakeReturn(&cntl3, static_cast<void*>(registResp7));      // NOLINT
    curvefsservice3.SetRegistRet(fakeregist7);

    auto func = [&]() {
        int i = 0;
        while (i++ < 100) {
            ASSERT_EQ(LIBCURVE_ERROR::OK,
            mdsclient.Register(ip, 999));
        }
    };

    std::thread t1(func);
    std::thread t2(func);

    t1.join();
    t2.join();
    UnInit();

    // 设置mds1 EHOSTDOWN，触发切换mds，切换到mds2，mds2返回ECONNREFUSED
    //  切到mds3, mds3 返回OK
    brpc::Controller cntl4;
    cntl4.SetFailed(EHOSTDOWN, "failed");
    RegistClientResponse* registResp8 = new RegistClientResponse();
    registResp8->set_statuscode(::curve::mds::StatusCode::kOK);
    FakeReturn* fakeregist8 = new FakeReturn(&cntl4, static_cast<void*>(registResp8));      // NOLINT
    curvefsservice1.SetRegistRet(fakeregist8);

    brpc::Controller cntl5;
    cntl5.SetFailed(ECONNREFUSED, "failed");
    RegistClientResponse* registResp9 = new RegistClientResponse();
    registResp9->set_statuscode(::curve::mds::StatusCode::kParaError);
    FakeReturn* fakeregist9 = new FakeReturn(&cntl5, static_cast<void*>(registResp9));      // NOLINT
    curvefsservice2.SetRegistRet(fakeregist9);

    RegistClientResponse* registResp10 = new RegistClientResponse();
    registResp10->set_statuscode(::curve::mds::StatusCode::kOK);
    FakeReturn* fakeregist10 = new FakeReturn(nullptr, static_cast<void*>(registResp10));      // NOLINT
    curvefsservice3.SetRegistRet(fakeregist10);

    ASSERT_EQ(LIBCURVE_ERROR::OK, mdsclient.Register(ip, 999));

    ASSERT_STREQ(ip.c_str(), curvefsservice1.GetIP().c_str());
    ASSERT_EQ(curvefsservice1.GetPort(), 999);

    ASSERT_EQ(0, server1.Stop(0));
    ASSERT_EQ(0, server2.Stop(0));
    ASSERT_EQ(0, server3.Stop(0));
    ASSERT_EQ(0, server1.Join());
    ASSERT_EQ(0, server2.Join());
    ASSERT_EQ(0, server3.Join());
}

std::string mdsMetaServerAddr = "127.0.0.1:9903,127.0.0.1:9904,127.0.0.1:9905";     // NOLINT
uint32_t segment_size = 1 * 1024 * 1024 * 1024ul;   // NOLINT
uint32_t chunk_size = 4 * 1024 * 1024;   // NOLINT
std::string configpath = "./test/client/testConfig/mds_failover.conf";   // NOLINT
int main(int argc, char ** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    ::testing::InitGoogleMock(&argc, argv);
    google::ParseCommandLineFlags(&argc, &argv, false);

    int ret = RUN_ALL_TESTS();
    return ret;
}

