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
 * File Created: Monday, 24th June 2019 10:00:53 am
 * Author: tongguangxun
 */

#include <gtest/gtest.h>
#include <gflags/gflags.h>
#include <glog/logging.h>

#include <chrono>   //  NOLINT
#include <thread>   //  NOLINT
#include <mutex>    //  NOLINT
#include <condition_variable>   // NOLINT

#include "include/client/libcurve.h"
#include "src/client/client_metric.h"
#include "src/client/file_instance.h"
#include "test/client/fake/mock_schedule.h"
#include "test/client/fake/fakeMDS.h"
#include "src/client/libcurve_file.h"
#include "src/client/client_common.h"
#include "src/client/client_config.h"
#include "test/integration/cluster_common/cluster.h"
#include "test/util/config_generator.h"

DECLARE_string(chunkserver_list);

uint32_t segment_size = 1 * 1024 * 1024 * 1024ul;                                   // NOLINT
uint32_t chunk_size = 4 * 1024 * 1024;                                              // NOLINT
std::string mdsMetaServerAddr = "127.0.0.1:9150";                                     // NOLINT

namespace curve {
namespace client {

const std::vector<std::string> clientConf {
    std::string("mds.listen.addr=127.0.0.1:9150"),
    std::string("global.logPath=./runlog/"),
    std::string("chunkserver.rpcTimeoutMS=1000"),
    std::string("chunkserver.opMaxRetry=3"),
    std::string("metacache.getLeaderRetry=3"),
    std::string("metacache.getLeaderTimeOutMS=1000"),
    std::string("global.fileMaxInFlightRPCNum=2048"),
    std::string("metacache.rpcRetryIntervalUS=500"),
    std::string("mds.rpcRetryIntervalUS=500"),
    std::string("schedule.threadpoolSize=2"),
};

TEST(MetricTest, ChunkServer_MetricTest) {
    MetaServerOption_t  metaopt;
    metaopt.metaaddrvec.push_back(mdsMetaServerAddr);
    metaopt.mdsRPCTimeoutMs = 500;
    metaopt.mdsRPCRetryIntervalUS = 200;

    MDSClient  mdsclient;
    ASSERT_EQ(0, mdsclient.Initialize(metaopt));

    FLAGS_chunkserver_list = "127.0.0.1:9130:0,127.0.0.1:9131:0,127.0.0.1:9132:0";   // NOLINT

    std::string configpath("./test/client/client_metric.conf");
    curve::CurveCluster* cluster = new curve::CurveCluster();

    cluster->PrepareConfig<curve::ClientConfigGenerator>(
        configpath, clientConf);

    ClientConfig cc;
    ASSERT_EQ(0, cc.Init(configpath.c_str()));

    // filename必须是全路径
    std::string filename = "/1_userinfo_";

    // init mds service
    FakeMDS mds(filename);
    mds.Initialize();
    mds.StartService();
    // 设置leaderid
    EndPoint ep;
    butil::str2endpoint("127.0.0.1", 9130, &ep);
    PeerId pd(ep);
    mds.StartCliService(pd);
    mds.CreateCopysetNode(true);

    UserInfo_t userinfo;
    userinfo.owner = "test";
    auto opt = cc.GetFileServiceOption();

    FileInstance fi;
    ASSERT_TRUE(fi.Initialize(filename.c_str(), &mdsclient, userinfo, opt));

    FileMetric* fm = fi.GetIOManager4File()->GetMetric();

    char* buffer;

    buffer = new char[8 * 1024];
    memset(buffer, 'a', 1024);
    memset(buffer + 1024, 'b', 1024);
    memset(buffer + 2 * 1024, 'c', 1024);
    memset(buffer + 3 * 1024, 'd', 1024);
    memset(buffer + 4 * 1024, 'e', 1024);
    memset(buffer + 5 * 1024, 'f', 1024);
    memset(buffer + 6 * 1024, 'g', 1024);
    memset(buffer + 7 * 1024, 'h', 1024);

    int ret = fi.Write(buffer, 0, 8192);
    ASSERT_EQ(8192, ret);
    ret = fi.Write(buffer, 0, 4096);
    ASSERT_EQ(4096, ret);

    ret = fi.Read(buffer, 0, 8192);
    ASSERT_EQ(8192, ret);
    ret = fi.Read(buffer, 0, 4096);
    ASSERT_EQ(4096, ret);

    // 先睡眠，确保采样
    std::this_thread::sleep_for(std::chrono::seconds(2));

    ASSERT_GT(fm->writeRPC.latency.max_latency(), 0);
    ASSERT_GT(fm->readRPC.latency.max_latency(), 0);

    // read write超时重试
    mds.EnableNetUnstable(8000);
    ret = fi.Write(buffer, 0, 4096);
    ASSERT_EQ(-2, ret);
    ret = fi.Write(buffer, 0, 4096);
    ASSERT_EQ(-2, ret);

    ret = fi.Read(buffer, 0, 4096);
    ASSERT_EQ(-2, ret);
    ret = fi.Read(buffer, 0, 4096);
    ASSERT_EQ(-2, ret);


    // 4次正确读写，4次超时读写,超时会引起重试，重试次数为3，数据量最大是8192
    ASSERT_EQ(fm->inflightRPCNum.get_value(), 0);
    ASSERT_EQ(fm->userRead.qps.count.get_value(), 2);
    ASSERT_EQ(fm->userWrite.qps.count.get_value(), 2);
    ASSERT_EQ(fm->userRead.eps.count.get_value(), 2);
    ASSERT_EQ(fm->userWrite.eps.count.get_value(), 2);
    ASSERT_EQ(fm->userWrite.rps.count.get_value(), 4);
    ASSERT_EQ(fm->userRead.rps.count.get_value(), 4);
    ASSERT_EQ(fm->readRPC.qps.count.get_value(), 2);
    ASSERT_EQ(fm->writeRPC.qps.count.get_value(), 2);
    ASSERT_EQ(fm->readRPC.rps.count.get_value(), 8);
    ASSERT_EQ(fm->writeRPC.rps.count.get_value(), 8);
    ASSERT_EQ(fm->readRPC.eps.count.get_value(), 6);
    ASSERT_EQ(fm->readRPC.eps.count.get_value(), 6);
    ASSERT_EQ(fm->writeRPC.timeoutQps.count.get_value(), 6);
    ASSERT_EQ(fm->readRPC.timeoutQps.count.get_value(), 6);
    ASSERT_EQ(fm->writeRPC.latency.count(), 2);
    ASSERT_EQ(fm->readRPC.latency.count(), 2);

    delete[] buffer;
    fi.UnInitialize();
    mds.UnInitialize();
    mdsclient.UnInitialize();
}

bool flag = false;
std::mutex mtx;
std::condition_variable cv;
void cb(CurveAioContext* ctx) {
    std::unique_lock<std::mutex> lk(mtx);
    flag = true;
    cv.notify_one();
}

TEST(MetricTest, SuspendRPC_MetricTest) {
    MetaServerOption_t  metaopt;
    metaopt.metaaddrvec.push_back(mdsMetaServerAddr);
    metaopt.mdsRPCTimeoutMs = 500;
    metaopt.mdsRPCRetryIntervalUS = 200;

    MDSClient  mdsclient;
    ASSERT_EQ(0, mdsclient.Initialize(metaopt));

    FLAGS_chunkserver_list = "127.0.0.1:9130:0,127.0.0.1:9131:0,127.0.0.1:9132:0";   // NOLINT

    // filename必须是全路径
    std::string filename = "/1_userinfo_";

    // init mds service
    FakeMDS mds(filename);
    mds.Initialize();
    mds.StartService();
    // 设置leaderid
    EndPoint ep;
    butil::str2endpoint("127.0.0.1", 9130, &ep);
    PeerId pd(ep);
    mds.StartCliService(pd);
    mds.CreateCopysetNode(true);

    UserInfo_t userinfo;
    userinfo.owner = "test";

    FileServiceOption_t opt;
    opt.ioOpt.reqSchdulerOpt.
    ioSenderOpt.failRequestOpt.chunkserverOPMaxRetry = 50;
    opt.ioOpt.reqSchdulerOpt.
    ioSenderOpt.failRequestOpt.chunkserverRPCTimeoutMS = 50;
    opt.ioOpt.reqSchdulerOpt.
    ioSenderOpt.failRequestOpt.chunkserverMaxRPCTimeoutMS = 50;

    FileInstance fi;
    ASSERT_TRUE(fi.Initialize(filename.c_str(), &mdsclient, userinfo, opt));

    FileMetric* fm = fi.GetIOManager4File()->GetMetric();

    char* buffer;

    buffer = new char[8 * 1024];
    memset(buffer, 'a', 1024);
    memset(buffer + 1024, 'b', 1024);
    memset(buffer + 2 * 1024, 'c', 1024);
    memset(buffer + 3 * 1024, 'd', 1024);
    memset(buffer + 4 * 1024, 'e', 1024);
    memset(buffer + 5 * 1024, 'f', 1024);
    memset(buffer + 6 * 1024, 'g', 1024);
    memset(buffer + 7 * 1024, 'h', 1024);

    int ret = fi.Write(buffer, 0, 8192);
    ASSERT_EQ(8192, ret);
    ret = fi.Write(buffer, 0, 4096);
    ASSERT_EQ(4096, ret);

    ret = fi.Read(buffer, 0, 8192);
    ASSERT_EQ(8192, ret);
    ret = fi.Read(buffer, 0, 4096);
    ASSERT_EQ(4096, ret);

    // 先睡眠，确保采样
    std::this_thread::sleep_for(std::chrono::seconds(2));

    ASSERT_GT(fm->writeRPC.latency.max_latency(), 0);
    ASSERT_GT(fm->readRPC.latency.max_latency(), 0);

    // read write超时重试
    mds.EnableNetUnstable(100);
    ret = fi.Write(buffer, 0, 4096);
    ASSERT_EQ(-2, ret);
    ret = fi.Write(buffer, 0, 4096);
    ASSERT_EQ(-2, ret);

    ret = fi.Read(buffer, 0, 4096);
    ASSERT_EQ(-2, ret);
    ret = fi.Read(buffer, 0, 4096);
    ASSERT_EQ(-2, ret);

    ASSERT_EQ(fm->suspendRPCMetric.count.get_value(), 0);

    mds.EnableNetUnstable(100);

    char* buf1 = new char[4 * 1024];
    CurveAioContext* aioctx = new CurveAioContext;
    aioctx->buf = buf1;
    aioctx->offset = 0;
    aioctx->op = LIBCURVE_OP_WRITE;
    aioctx->length = 4 * 1024;
    aioctx->cb = cb;
    fi.AioWrite(aioctx);

    std::this_thread::sleep_for(std::chrono::seconds(2));
    ASSERT_EQ(fm->suspendRPCMetric.count.get_value(), 1);

    {
        std::unique_lock<std::mutex> lk(mtx);
        cv.wait(lk, [](){return flag;});
    }

    delete[] buffer;
    delete[] buf1;
    fi.UnInitialize();
    mds.UnInitialize();
    mdsclient.UnInitialize();
}

TEST(MetricTest, MetricHelperTest) {
    FileMetric* fm = nullptr;

    ASSERT_NO_THROW(MetricHelper::IncremUserRPSCount(fm, OpType::WRITE));
    ASSERT_NO_THROW(MetricHelper::IncremUserRPSCount(fm, OpType::READ));

    ASSERT_NO_THROW(MetricHelper::IncremRPCRPSCount(fm, OpType::WRITE));
    ASSERT_NO_THROW(MetricHelper::IncremRPCRPSCount(fm, OpType::READ));

    ASSERT_NO_THROW(MetricHelper::IncremInflightRPC(fm));
    ASSERT_NO_THROW(MetricHelper::DecremInflightRPC(fm));

    ASSERT_NO_THROW(MetricHelper::IncremGetLeaderRetryTime(fm));

    ASSERT_NO_THROW(MetricHelper::IncremUserQPSCount(fm, 0, OpType::READ));
    ASSERT_NO_THROW(MetricHelper::IncremUserEPSCount(fm, OpType::READ));
    ASSERT_NO_THROW(MetricHelper::IncremFailRPCCount(fm, OpType::READ));
    ASSERT_NO_THROW(MetricHelper::IncremTimeOutRPCCount(fm, OpType::READ));
    ASSERT_NO_THROW(MetricHelper::IncremRPCQPSCount(fm, 0, OpType::READ));
    ASSERT_NO_THROW(MetricHelper::UserLatencyRecord(fm, 0, OpType::READ));
    ASSERT_NO_THROW(MetricHelper::IncremInflightRPC(fm));
    ASSERT_NO_THROW(MetricHelper::DecremInflightRPC(fm));
    ASSERT_NO_THROW(MetricHelper::IncremIOSuspendNum(fm));
    ASSERT_NO_THROW(MetricHelper::DecremIOSuspendNum(fm));
    ASSERT_NO_THROW(MetricHelper::LatencyRecord(fm, 0, OpType::READ));
    ASSERT_NO_THROW(MetricHelper::IncremRedirectRPCCount(fm, OpType::READ));
    ASSERT_NO_THROW(MetricHelper::IncremRedirectRPCCount(fm, OpType::WRITE));

    FileMetric fm2("test");

    ASSERT_NO_THROW(MetricHelper::IncremUserRPSCount(&fm2, OpType::WRITE));
    ASSERT_NO_THROW(MetricHelper::IncremUserRPSCount(&fm2, OpType::READ));

    ASSERT_NO_THROW(MetricHelper::IncremRPCRPSCount(&fm2, OpType::WRITE));
    ASSERT_NO_THROW(MetricHelper::IncremRPCRPSCount(&fm2, OpType::READ));

    ASSERT_NO_THROW(MetricHelper::IncremInflightRPC(&fm2));
    ASSERT_NO_THROW(MetricHelper::DecremInflightRPC(&fm2));

    ASSERT_NO_THROW(MetricHelper::IncremGetLeaderRetryTime(&fm2));

    ASSERT_NO_THROW(MetricHelper::IncremUserQPSCount(&fm2, 0, OpType::READ));
    ASSERT_NO_THROW(MetricHelper::IncremUserEPSCount(&fm2, OpType::READ));
    ASSERT_NO_THROW(MetricHelper::IncremFailRPCCount(&fm2, OpType::READ));
    ASSERT_NO_THROW(MetricHelper::IncremTimeOutRPCCount(&fm2, OpType::READ));
    ASSERT_NO_THROW(MetricHelper::IncremRPCQPSCount(&fm2, 0, OpType::READ));
    ASSERT_NO_THROW(MetricHelper::UserLatencyRecord(&fm2, 0, OpType::READ));
    ASSERT_NO_THROW(MetricHelper::IncremInflightRPC(&fm2));
    ASSERT_NO_THROW(MetricHelper::DecremInflightRPC(&fm2));
    ASSERT_NO_THROW(MetricHelper::IncremIOSuspendNum(&fm2));
    ASSERT_NO_THROW(MetricHelper::DecremIOSuspendNum(&fm2));
    ASSERT_NO_THROW(MetricHelper::LatencyRecord(&fm2, 0, OpType::READ));

    ASSERT_NO_THROW(MetricHelper::IncremRedirectRPCCount(&fm2, OpType::READ));
    ASSERT_NO_THROW(MetricHelper::IncremRedirectRPCCount(&fm2, OpType::WRITE));

    ASSERT_NO_THROW(MetricHelper::IncremGetLeaderRetryTime(nullptr));
    ASSERT_NO_THROW(MetricHelper::IncremUserQPSCount(nullptr, 0, OpType::READ));
    ASSERT_NO_THROW(MetricHelper::IncremUserEPSCount(nullptr, OpType::READ));
    ASSERT_NO_THROW(MetricHelper::IncremUserRPSCount(nullptr, OpType::READ));
    ASSERT_NO_THROW(MetricHelper::IncremFailRPCCount(nullptr, OpType::READ));
    ASSERT_NO_THROW(MetricHelper::IncremTimeOutRPCCount(nullptr, OpType::READ));
    ASSERT_NO_THROW(MetricHelper::IncremRPCQPSCount(nullptr, 0, OpType::READ));
    ASSERT_NO_THROW(MetricHelper::IncremRPCRPSCount(nullptr, OpType::READ));
    ASSERT_NO_THROW(MetricHelper::LatencyRecord(nullptr, 0, OpType::READ));
    ASSERT_NO_THROW(MetricHelper::IncremInflightRPC(nullptr));
    ASSERT_NO_THROW(MetricHelper::DecremInflightRPC(nullptr));
    ASSERT_NO_THROW(MetricHelper::IncremIOSuspendNum(nullptr));
    ASSERT_NO_THROW(MetricHelper::DecremIOSuspendNum(nullptr));
}

}   //  namespace client
}   //  namespace curve
