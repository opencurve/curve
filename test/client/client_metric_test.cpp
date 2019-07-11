/*
 * Project: curve
 * File Created: Monday, 24th June 2019 10:00:53 am
 * Author: tongguangxun
 * Copyright (c)￼ 2018 netease
 */

#include <gtest/gtest.h>
#include <gflags/gflags.h>
#include <glog/logging.h>

#include <chrono>   //  NOLINT
#include <thread>   //  NOLINT

#include "include/client/libcurve.h"
#include "src/client/client_metric.h"
#include "src/client/file_instance.h"
#include "test/client/fake/mock_schedule.h"
#include "test/client/fake/fakeMDS.h"
#include "src/client/libcurve_file.h"
#include "src/client/client_common.h"
#include "src/client/client_config.h"

DECLARE_string(chunkserver_list);

uint32_t segment_size = 1 * 1024 * 1024 * 1024ul;                                   // NOLINT
uint32_t chunk_size = 4 * 1024 * 1024;                                              // NOLINT
std::string metaserver_addr = "127.0.0.1:9150";                                     // NOLINT

namespace curve {
namespace client {
TEST(MetricTest, MDS_MetricTest) {
    MetaServerOption_t  metaopt;
    metaopt.metaaddrvec.push_back(metaserver_addr);
    metaopt.rpcTimeoutMs = 500;
    metaopt.rpcRetryTimes = 5;
    metaopt.retryIntervalUs = 200;
    metaopt.synchronizeRPCRetryTime = 3;

    brpc::Server server;
    FakeMDSCurveFSService curvefsservice;
    if (server.AddService(&curvefsservice,
                          brpc::SERVER_DOESNT_OWN_SERVICE) != 0) {
        LOG(FATAL) << "Fail to add service";
    }

    brpc::ServerOptions options;
    options.idle_timeout_sec = -1;
    ASSERT_EQ(server.Start(metaserver_addr.c_str(), &options), 0);

    MDSClient  mdsclient;
    ASSERT_EQ(0, mdsclient.Initialize(metaopt));

    std::string filename = "/1_userinfo_";
    UserInfo_t userinfo;
    userinfo.owner = "userinfo";

    // set response file exist
    ::curve::mds::CreateFileResponse response;
    response.set_statuscode(::curve::mds::StatusCode::kFileExists);
    FakeReturn* fakeret = new FakeReturn(nullptr, static_cast<void*>(&response));   //  NOLINT
    curvefsservice.SetCreateFileFakeReturn(fakeret);

    mdsclient.CreateFile(filename.c_str(), userinfo, 10*1024*1024*1024ul);

    // 设置rpc失败，触发重试
    brpc::Controller cntl;
    cntl.SetFailed(-1, "failed");
    FakeReturn* fakeret2 = new FakeReturn(&cntl, static_cast<void*>(&response));   //  NOLINT
    curvefsservice.SetCreateFileFakeReturn(fakeret2);

    mdsclient.CreateFile(filename.c_str(), userinfo, 10*1024*1024*1024ul);

    MDSClientMetric_t* mdsmetric = mdsclient.GetMetric();

    ASSERT_EQ(mdsmetric->createFile.qps.count.get_value(), 4);
    ASSERT_EQ(mdsmetric->createFile.eps.count.get_value(), 3);

    // file close ok
    ::curve::mds::CloseFileResponse response1;
    response1.set_statuscode(::curve::mds::StatusCode::kOK);
    FakeReturn* fakeret3 = new FakeReturn(nullptr, static_cast<void*>(&response1));     // NOLINT
    curvefsservice.SetCloseFile(fakeret3);
    mdsclient.CloseFile(filename.c_str(), userinfo,  "sessid");

    // 设置rpc失败，触发重试
    brpc::Controller cntl2;
    cntl2.SetFailed(-1, "failed");
    FakeReturn* fakeret4 = new FakeReturn(&cntl2, static_cast<void*>(&response));     // NOLINT
    curvefsservice.SetCloseFile(fakeret4);
    mdsclient.CloseFile(filename.c_str(), userinfo,  "sessid");

    // 共调用6次，1次成功，5次重试
    ASSERT_EQ(mdsmetric->closeFile.qps.count.get_value(), 4);
    ASSERT_EQ(mdsmetric->closeFile.eps.count.get_value(), 3);

    // file open ok
    FInfo_t fi;
    LeaseSession lease;
    ::curve::mds::OpenFileResponse openresponse;
    openresponse.set_statuscode(::curve::mds::StatusCode::kOK);
    FakeReturn* fakeret5 = new FakeReturn(nullptr, static_cast<void*>(&openresponse));     // NOLINT
    curvefsservice.SetOpenFile(fakeret5);
    mdsclient.OpenFile(filename.c_str(), userinfo, &fi, &lease);

    // 设置rpc失败，触发重试
    brpc::Controller cntl3;
    cntl3.SetFailed(-1, "failed");
    FakeReturn* fakeret6 = new FakeReturn(&cntl3, static_cast<void*>(&openresponse));     // NOLINT
    curvefsservice.SetOpenFile(fakeret6);
    mdsclient.OpenFile(filename.c_str(), userinfo, &fi, &lease);

    // 共调用6次，1次成功，5次重试
    ASSERT_EQ(mdsmetric->closeFile.qps.count.get_value(), 4);
    ASSERT_EQ(mdsmetric->closeFile.eps.count.get_value(), 3);

    // set delete file ok
    ::curve::mds::DeleteFileResponse delresponse;
    delresponse.set_statuscode(::curve::mds::StatusCode::kOK);
    FakeReturn* fakeret7 = new FakeReturn(nullptr, static_cast<void*>(&delresponse));     // NOLINT
    curvefsservice.SetDeleteFile(fakeret7);
    mdsclient.DeleteFile(filename.c_str(), userinfo);

    // 设置rpc失败，触发重试
    brpc::Controller cntl4;
    cntl4.SetFailed(-1, "failed");
    FakeReturn* fakeret8 = new FakeReturn(&cntl4, static_cast<void*>(&delresponse));     // NOLINT
    curvefsservice.SetDeleteFile(fakeret8);
    mdsclient.DeleteFile(filename.c_str(), userinfo);

    // 共调用6次，1次成功，5次重试
    ASSERT_EQ(mdsmetric->deleteFile.qps.count.get_value(), 4);
    ASSERT_EQ(mdsmetric->deleteFile.eps.count.get_value(), 3);
    mdsclient.UnInitialize();

    server.Stop(0);
    server.Join();
}

TEST(MetricTest, Config_MetricTest) {
    FLAGS_chunkserver_list = "127.0.0.1:9140:0,127.0.0.1:9141:0,127.0.0.1:9142:0";   // NOLINT
    ASSERT_EQ(0, Init("./test/client/testConfig/client_metric.conf"));

    // filename必须是全路径
    std::string filename = "/1_userinfo_";

    // init mds service
    FakeMDS mds(filename);
    mds.Initialize();
    mds.StartService();
    // 设置leaderid
    EndPoint ep;
    butil::str2endpoint("127.0.0.1", 9140, &ep);
    PeerId pd(ep);
    mds.StartCliService(pd);
    mds.CreateCopysetNode(true);

    // libcurve file operation
    C_UserInfo_t userinfo;
    memcpy(userinfo.owner, "userinfo", 9);
    Create(filename.c_str(), &userinfo, 10*1024*1024*1024ul);

    sleep(1);

    int fd;
    char* buffer;
    char* readbuffer;

    fd = Open(filename.c_str(), &userinfo);
    ASSERT_EQ(fd, 0);

    buffer = new char[8 * 1024];
    memset(buffer, 'a', 1024);
    memset(buffer + 1024, 'b', 1024);
    memset(buffer + 2 * 1024, 'c', 1024);
    memset(buffer + 3 * 1024, 'd', 1024);
    memset(buffer + 4 * 1024, 'e', 1024);
    memset(buffer + 5 * 1024, 'f', 1024);
    memset(buffer + 6 * 1024, 'g', 1024);
    memset(buffer + 7 * 1024, 'h', 1024);

    int ret = Write(fd, buffer, 0, 4096);
    ASSERT_EQ(ret, 4096);
    delete[] buffer;

    ASSERT_EQ(confMetric_.rpcTimeoutMs.get_value(), 1000);
    ASSERT_EQ(confMetric_.rpcRetryTimes.get_value(), 3);
    ASSERT_EQ(confMetric_.getLeaderTimeOutMs.get_value(), 1000);
    ASSERT_EQ(confMetric_.getLeaderRetry.get_value(), 3);
    ASSERT_EQ(confMetric_.getLeaderRetryIntervalUs.get_value(), 500);
    ASSERT_EQ(confMetric_.threadpoolSize.get_value(), 2);
    ASSERT_EQ(confMetric_.queueCapacity.get_value(), 4096);
    ASSERT_EQ(confMetric_.opRetryIntervalUs.get_value(), 50000);
    ASSERT_EQ(confMetric_.opMaxRetry.get_value(), 3);
    ASSERT_EQ(confMetric_.enableAppliedIndexRead.get_value(), 1);
    ASSERT_EQ(confMetric_.ioSplitMaxSizeKB.get_value(), 64);
    ASSERT_EQ(confMetric_.maxInFlightRPCNum.get_value(), 2048);

    Close(fd);
    mds.UnInitialize();
    UnInit();
}

TEST(MetricTest, ChunkServer_MetricTest) {
    MetaServerOption_t  metaopt;
    metaopt.metaaddrvec.push_back(metaserver_addr);
    metaopt.rpcTimeoutMs = 500;
    metaopt.rpcRetryTimes = 5;
    metaopt.retryIntervalUs = 200;

    MDSClient  mdsclient;
    ASSERT_EQ(0, mdsclient.Initialize(metaopt));

    FLAGS_chunkserver_list = "127.0.0.1:9130:0,127.0.0.1:9131:0,127.0.0.1:9132:0";   // NOLINT
    ClientConfig cc;
    ASSERT_EQ(0, cc.Init("./test/client/testConfig/client_metric.conf"));

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

    FileMetric_t* fm = fi.GetIOManager4File()->GetMetric();

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
    mds.EnableNetUnstable(1500);
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
    ASSERT_EQ(fm->getLeaderRetryQPS.count.get_value(), 12);
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
}   //  namespace client
}   //  namespace curve
