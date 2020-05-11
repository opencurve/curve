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

#include <gtest/gtest.h>
#include <glog/logging.h>
#include <gflags/gflags.h>
#include <fiu-control.h>

#include <string>
#include <thread>   //NOLINT
#include <chrono>   //NOLINT
#include <mutex>    // NOLINT
#include <condition_variable>   //NOLINT

#include "test/client/fake/mock_schedule.h"
#include "src/client/io_tracker.h"
#include "src/client/splitor.h"
#include "test/client/fake/mockMDS.h"
#include "src/client/client_common.h"
#include "src/client/file_instance.h"
#include "src/client/metacache.h"
#include "src/client/iomanager4file.h"
#include "src/client/libcurve_file.h"
#include "src/client/client_config.h"
#include "src/client/mds_client.h"
#include "src/client/metacache_struct.h"
#include "test/client/fake/fakeMDS.h"
#include "src/client/lease_excutor.h"
#include "src/client/config_info.h"
#include "test/integration/cluster_common/cluster.h"
#include "test/util/config_generator.h"

DECLARE_string(chunkserver_list);

extern std::string mdsMetaServerAddr;
extern uint32_t chunk_size;
extern std::string configpath;

extern char* writebuffer;

using curve::client::LeaseExecutor;
using curve::client::EndPoint;
using curve::client::UserInfo_t;
using curve::client::CopysetInfo_t;
using curve::client::SegmentInfo;
using curve::client::FInfo_t;
using curve::client::MDSClient;
using curve::client::ClientConfig;
using curve::client::FileInstance;
using curve::client::IOTracker;
using curve::client::MetaCache;
using curve::client::RequestContext;
using curve::client::IOManager4File;
using curve::client::LogicalPoolCopysetIDInfo_t;
using curve::client::FileMetric;

bool iorflag = false;
std::mutex rmtx;
std::condition_variable rcv;
bool iowflag = false;
std::mutex wmtx;
std::condition_variable wcv;

void readcb(CurveAioContext* context) {
    LOG(INFO) << "aio call back here, errorcode = " << context->ret;
    iorflag = true;
    rcv.notify_one();
}

void writecb(CurveAioContext* context) {
    LOG(INFO) << "aio call back here, errorcode = " << context->ret;
    iowflag = true;
    wcv.notify_one();
}

/**
 * 要测试哪些内容：
 * 1. inflight RPC计数metric统计正确
 * 2. inflight RPC在超过限制的时候会hang后面的RPC请求
 * 3. inflight RPC在关闭文件的时候如果session正常那么就
 *    等待所有inflight RPC返回才能关闭成功，如果session
 *    处于invalid状态，name在关闭文件的时候所有的RPC直接
 *    向上返回错误，flush完毕之后才能关闭文件。
 * 4. inflight RPC在收到session过期通知的停止正在重试的RPC
 *    请求, 并且这些请求会重新入队
 */


class InflightRPCTest : public ::testing::Test {
 public:
    void SetUp() {
        fiu_init(0);
        FLAGS_chunkserver_list =
             "127.0.0.1:9144:0,127.0.0.1:9145:0,127.0.0.1:9146:0";
        fopt.metaServerOpt.mdsMaxRetryMS = 1000;
        fopt.metaServerOpt.metaaddrvec.push_back("127.0.0.1:9143");
        fopt.metaServerOpt.mdsRPCTimeoutMs = 500;
        fopt.metaServerOpt.mdsRPCRetryIntervalUS = 50000;
        fopt.loginfo.logLevel = 0;
        fopt.ioOpt.ioSplitOpt.fileIOSplitMaxSizeKB = 64;
        fopt.ioOpt.ioSenderOpt.chunkserverEnableAppliedIndexRead = 1;
        fopt.ioOpt.ioSenderOpt.failRequestOpt.chunkserverRPCTimeoutMS = 10000;
        fopt.ioOpt.ioSenderOpt.failRequestOpt.chunkserverOPMaxRetry = 3;
        fopt.ioOpt.ioSenderOpt.failRequestOpt.chunkserverOPRetryIntervalUS = 500;  // NOLINT
        fopt.ioOpt.metaCacheOpt.metacacheGetLeaderRetry = 3;
        fopt.ioOpt.metaCacheOpt.metacacheRPCRetryIntervalUS = 500;
        fopt.ioOpt.reqSchdulerOpt.scheduleQueueCapacity = 4096;
        fopt.ioOpt.reqSchdulerOpt.scheduleThreadpoolSize = 1;
        fopt.ioOpt.reqSchdulerOpt.ioSenderOpt.
            failRequestOpt.chunkserverMaxRPCTimeoutMS = 5000;
        fopt.ioOpt.reqSchdulerOpt.ioSenderOpt = fopt.ioOpt.ioSenderOpt;
        fopt.leaseOpt.mdsRefreshTimesPerLease = 4;

        userinfo.owner = "userinfo";
        userinfo.password = "12345";

        mdsclient_.Initialize(fopt.metaServerOpt);

        // 设置leaderid
        EndPoint ep;
        butil::str2endpoint("127.0.0.1", 9144, &ep);
        PeerId pd(ep);
        mds = new FakeMDS("/test");
        mds->Initialize();
        mds->StartCliService(pd);
        mds->StartService();
        mds->CreateCopysetNode(true);
    }

    void TearDown() {
        mdsclient_.UnInitialize();
        delete fileinstance_;
        mds->UnInitialize();
    }

    FakeMDS* mds;
    UserInfo_t userinfo;
    MDSClient mdsclient_;
    FileServiceOption_t fopt;
    curve::client::ClientConfig cc;
    FileInstance*    fileinstance_;
};

TEST_F(InflightRPCTest, metricTest) {
    fileinstance_ = new FileInstance();
    fileinstance_->Initialize("/test", &mdsclient_, userinfo, fopt);
    curve::client::IOManager4File* iomana = fileinstance_->GetIOManager4File();
    auto fm = iomana->GetMetric();

    // 这个IO会被拆分为两个RPC
    CurveAioContext* aioctx = new CurveAioContext;
    aioctx->offset = 0;
    aioctx->length = 128 * 1024;
    aioctx->ret = LIBCURVE_ERROR::OK;
    aioctx->cb = readcb;
    aioctx->buf = new char[aioctx->length];
    aioctx->op = LIBCURVE_OP::LIBCURVE_OP_READ;

    CurveAioContext* aioctx2 = new CurveAioContext;
    aioctx2->offset = 0;
    aioctx2->length = 128 * 1024;
    aioctx2->ret = LIBCURVE_ERROR::OK;
    aioctx2->cb = writecb;
    aioctx2->buf = new char[aioctx2->length];
    aioctx2->op = LIBCURVE_OP::LIBCURVE_OP_WRITE;

    // 设置rpc等待时间，这样确保rpc发出去但是没有回来，发出的
    // RPC都是inflight RPC
    mds->EnableNetUnstable(6000);
    iorflag = false;
    iowflag = false;
    iomana->AioRead(aioctx, &mdsclient_);
    iomana->AioWrite(aioctx2, &mdsclient_);

    std::this_thread::sleep_for(std::chrono::seconds(1));
    ASSERT_EQ(4, fm->inflightRPCNum.get_value());

    {
        std::unique_lock<std::mutex> lk(rmtx);
        rcv.wait(lk, []()->bool{return iorflag;});
    }

    {
        std::unique_lock<std::mutex> lk(wmtx);
        wcv.wait(lk, []()->bool{return iowflag;});
    }

    ASSERT_EQ(0, fm->inflightRPCNum.get_value());
    fileinstance_->UnInitialize();
}


TEST_F(InflightRPCTest, inflightRPCTest) {
    // 测试inflight RPC超过限制的时候copyset client hang住request scheduler
    fileinstance_ = new FileInstance();
    fopt.ioOpt.reqSchdulerOpt.ioSenderOpt.
        failRequestOpt.chunkserverRPCTimeoutMS = 10000;
    // 设置inflight RPC最大数量为1
    fopt.ioOpt.ioSenderOpt.inflightOpt.fileMaxInFlightRPCNum = 1;
    fileinstance_->Initialize("/test", &mdsclient_, userinfo, fopt);
    curve::client::IOManager4File* iomana = fileinstance_->GetIOManager4File();
    auto fm = iomana->GetMetric();
    auto scheduler = iomana->GetScheduler();

    // 这个IO会被拆分为两个RPC
    CurveAioContext* aioctx = new CurveAioContext;
    aioctx->offset = 0;
    aioctx->length = 128 * 1024;
    aioctx->ret = LIBCURVE_ERROR::OK;
    aioctx->cb = readcb;
    aioctx->buf = new char[aioctx->length];
    aioctx->op = LIBCURVE_OP::LIBCURVE_OP_READ;

    CurveAioContext* aioctx2 = new CurveAioContext;
    aioctx2->offset = 0;
    aioctx2->length = 128 * 1024;
    aioctx2->ret = LIBCURVE_ERROR::OK;
    aioctx2->cb = writecb;
    aioctx2->buf = new char[aioctx2->length];
    aioctx2->op = LIBCURVE_OP::LIBCURVE_OP_WRITE;

    // 设置rpc等待时间，这样确保rpc发出去但是没有回来，发出的
    // RPC都是inflight RPC
    mds->EnableNetUnstable(6000);
    iorflag = false;
    iowflag = false;
    iomana->AioRead(aioctx, &mdsclient_);
    iomana->AioWrite(aioctx2, &mdsclient_);

    std::this_thread::sleep_for(std::chrono::seconds(1));
    // 有一个IO已经发出去，另外一个RPC hang在getinflightRPCToken
    // 队列里还剩两个RPC带发送
    ASSERT_EQ(1, fm->inflightRPCNum.get_value());
    ASSERT_EQ(2, scheduler->GetQueue()->Size());
    ASSERT_EQ(1, fm->inflightRPCNum.get_value());

    {
        std::unique_lock<std::mutex> lk(rmtx);
        rcv.wait(lk, []()->bool{return iorflag;});
    }

    std::this_thread::sleep_for(std::chrono::milliseconds(500));
    // 有一个IO已经回来，另外一个IO开始发送第一个RPC
    // RPC hang在getinflightRPCToken队列里还剩RPC已经全部出队
    ASSERT_EQ(1, fm->inflightRPCNum.get_value());
    ASSERT_EQ(0, scheduler->GetQueue()->Size());
    ASSERT_EQ(1, fm->inflightRPCNum.get_value());

    {
        std::unique_lock<std::mutex> lk(wmtx);
        wcv.wait(lk, []()->bool{return iowflag;});
    }

    ASSERT_EQ(0, fm->inflightRPCNum.get_value());

    fileinstance_->UnInitialize();
}

TEST_F(InflightRPCTest, FileCloseTest) {
    // 测试在文件关闭的时候，lese续约失败不会调用iomanager已析构的资源
    // lease时长10s，在lease期间仅续约一次，一次失败就会调用iomanager
    // block IO，这时候其实调用的是scheduler的LeaseTimeoutBlockIO
    fileinstance_ = new FileInstance();
    fopt.ioOpt.reqSchdulerOpt.ioSenderOpt.
        failRequestOpt.chunkserverRPCTimeoutMS = 10000;
    // 设置inflight RPC最大数量为1
    fopt.ioOpt.ioSenderOpt.inflightOpt.fileMaxInFlightRPCNum = 1;

    std::condition_variable cv;
    std::mutex mtx;
    bool inited = false;

    std::condition_variable resumecv;
    std::mutex resumemtx;
    bool resume = true;

    IOManager4File* iomanager;

    auto f1 = [&]() {
        for (int  i = 0; i < 50; i++) {
            {
                std::unique_lock<std::mutex> lk(resumemtx);
                resumecv.wait(lk, [&](){ return resume;});
                resume = false;
            }
            iomanager = new IOManager4File();
            ASSERT_TRUE(iomanager->Initialize("/", fopt.ioOpt, &mdsclient_));

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
                cv.wait(lk, [&](){ return inited;});
                inited = false;
            }

            LeaseOption lopt;
            lopt.mdsRefreshTimesPerLease = 1;
            UserInfo_t userinfo("test", "");
            LeaseExecutor lease(lopt, userinfo, &mdsclient_, iomanager);

            for (int j = 0; j < 5; j ++) {
                // 测试iomanager退出之后，lease再去调用其scheduler资源不会crash
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

    // 并发两个线程，一个线程启动iomanager初始化，然后反初始化
    // 另一个线程启动lease续约，然后调用iomanager使其block IO
    // 预期：并发两个线程，lease线程续约失败即使在iomanager线程
    // 退出的同时去调用其block IO接口也不会出现并发竞争共享资源的
    // 场景。
    std::thread t1(f1);
    std::thread t2(f2);

    t1.joinable() ? t1.join() : void();
    t2.joinable() ? t2.join() : void();
}

TEST_F(InflightRPCTest, sessionValidRPCTest) {
    // 测试session正常，RPC会一直重试到rpcRetryTimes之后然后错误返回
    fileinstance_ = new FileInstance();
    fopt.ioOpt.reqSchdulerOpt.ioSenderOpt.
        failRequestOpt.chunkserverOPMaxRetry = 5;
    fopt.ioOpt.reqSchdulerOpt.ioSenderOpt.
        failRequestOpt.chunkserverRPCTimeoutMS = 5000;
    fopt.ioOpt.reqSchdulerOpt.ioSenderOpt.
        failRequestOpt.chunkserverMaxRPCTimeoutMS = 5000;
    // 设置inflight RPC最大数量为100
    fopt.ioOpt.ioSenderOpt.inflightOpt.fileMaxInFlightRPCNum = 100;
    fileinstance_->Initialize("/test", &mdsclient_, userinfo, fopt);
    curve::client::IOManager4File* iomana = fileinstance_->GetIOManager4File();
    auto fm = iomana->GetMetric();
    auto scheduler = iomana->GetScheduler();
    auto fakemds = mds->GetMDSService();
    std::vector<FakeChunkService *> chunkservice = mds->GetChunkservice();

    for (auto it : chunkservice) {
        it->CleanRetryTimes();
    }

    ASSERT_EQ(LIBCURVE_ERROR::OK, fileinstance_->Open("file",
    userinfo));

    // 这个IO会被拆分为两个RPC
    CurveAioContext* aioctx = new CurveAioContext;
    aioctx->offset = 0;
    aioctx->length = 128 * 1024;
    aioctx->ret = LIBCURVE_ERROR::OK;
    aioctx->cb = readcb;
    aioctx->buf = new char[aioctx->length];
    aioctx->op = LIBCURVE_OP::LIBCURVE_OP_READ;

    CurveAioContext* aioctx2 = new CurveAioContext;
    aioctx2->offset = 0;
    aioctx2->length = 128 * 1024;
    aioctx2->ret = LIBCURVE_ERROR::OK;
    aioctx2->cb = writecb;
    aioctx2->buf = new char[aioctx2->length];
    aioctx2->op = LIBCURVE_OP::LIBCURVE_OP_WRITE;

    // 设置rpc等待时间，这样确保rpc发出去但是没有回来，发出的
    // RPC都是inflight RPC
    mds->EnableNetUnstable(10000);
    iorflag = false;
    iowflag = false;
    ASSERT_EQ(LIBCURVE_ERROR::OK, iomana->AioRead(aioctx, &mdsclient_));
    ASSERT_EQ(LIBCURVE_ERROR::OK, iomana->AioWrite(aioctx2, &mdsclient_));

    // 数据发出去之后立即关闭file instance，这时候instance需要
    // 等到所有的inflight RPC都返回之后才能返回

    std::this_thread::sleep_for(std::chrono::seconds(2));
    ASSERT_EQ(4, fm->inflightRPCNum.get_value());
    fileinstance_->UnInitialize();

    // 两IO会被拆分成4个RPC，4个RPC每个重试5次，总共重试次数为20
    uint64_t retrys = 0;
    for (auto it : chunkservice) {
        retrys += it->GetRetryTimes();
    }

    ASSERT_EQ(retrys, 20);
}


const std::vector<std::string> clientConf {
    std::string("mds.listen.addr=127.0.0.1:9143"),
    std::string("global.logPath=./runlog/"),
    std::string("chunkserver.rpcTimeoutMS=10000"),
    std::string("chunkserver.opMaxRetry=3"),
    std::string("metacache.getLeaderRetry=3"),
};

std::string mdsMetaServerAddr = "127.0.0.1:9143";     // NOLINT
uint32_t segment_size = 1 * 1024 * 1024 * 1024ul;   // NOLINT
uint32_t chunk_size = 4 * 1024 * 1024;   // NOLINT
std::string configpath = "./test/client/testConfig/client_inflightrpc.conf";   // NOLINT

int main(int argc, char ** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    ::testing::InitGoogleMock(&argc, argv);
    google::ParseCommandLineFlags(&argc, &argv, false);

    curve::CurveCluster* cluster = new curve::CurveCluster();

    cluster->PrepareConfig<curve::ClientConfigGenerator>(
        configpath, clientConf);

    int ret = RUN_ALL_TESTS();
    return ret;
}
