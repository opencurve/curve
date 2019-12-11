/*
 * Project: curve
 * File Created: Tuesday, 9th October 2018 5:16:52 pm
 * Author: tongguangxun
 * Copyright (c) 2018 NetEase
 */

#include <gtest/gtest.h>
#include <gflags/gflags.h>
#include <glog/logging.h>
#include <fiu-control.h>
#include <string>
#include <iostream>
#include <thread>   //NOLINT
#include <chrono>      // NOLINT
#include <condition_variable>  // NOLINT
#include <mutex>   // NOLINT

#include "include/client/libcurve.h"
#include "src/client/file_instance.h"
#include "test/client/fake/mock_schedule.h"
#include "test/client/fake/fakeMDS.h"
#include "src/client/libcurve_file.h"
#include "src/client/client_common.h"

using curve::client::MetaCacheErrorType;
using curve::client::ChunkIDInfo_t;
using curve::client::ChunkServerAddr;
using curve::client::MetaCache;
using curve::client::UserInfo_t;
using curve::client::EndPoint;
using curve::client::MDSClient;
using curve::client::ClientConfig;
using curve::client::FileInstance;
using curve::client::CopysetInfo_t;
using curve::client::CopysetIDInfo;
using curve::client::FileClient;

extern std::string configpath;
extern uint32_t chunk_size;
extern uint32_t segment_size;

bool writeflag = false;
bool readflag = false;
std::mutex writeinterfacemtx;
std::condition_variable writeinterfacecv;
std::mutex interfacemtx;
std::condition_variable interfacecv;

DECLARE_string(chunkserver_list);
DECLARE_uint32(logic_pool_id);
DECLARE_uint32(copyset_num);
DECLARE_uint64(test_disk_size);
void writecallbacktest(CurveAioContext* context) {
    writeflag = true;
    writeinterfacecv.notify_one();
    LOG(INFO) << "aio call back here, errorcode = " << context->ret;
}
void readcallbacktest(CurveAioContext* context) {
    readflag = true;
    interfacecv.notify_one();
    LOG(INFO) << "aio call back here, errorcode = " << context->ret;
}

TEST(TestLibcurveInterface, InterfaceTest) {
    FLAGS_chunkserver_list =
         "127.0.0.1:9115:0,127.0.0.1:9116:0,127.0.0.1:9117:0";

    std::string filename = "/1_userinfo_";
    C_UserInfo_t userinfo;
    memcpy(userinfo.owner, "userinfo", 9);
    memcpy(userinfo.password, "", 256);

    // 设置leaderid
    EndPoint ep;
    butil::str2endpoint("127.0.0.1", 9115, &ep);
    PeerId pd(ep);

    // init mds service
    FakeMDS mds(filename);
    mds.Initialize();
    mds.StartCliService(pd);
    mds.StartService();
    mds.CreateCopysetNode(true);

    ASSERT_EQ(0, Init(configpath.c_str()));

    std::this_thread::sleep_for(std::chrono::milliseconds(1000));

    // test get cluster id
    const int CLUSTERIDMAX = 256;
    char clusterId[CLUSTERIDMAX];

    ASSERT_EQ(GetClusterId(nullptr, 0), -LIBCURVE_ERROR::FAILED);

    memset(clusterId, 0, sizeof(clusterId));
    ASSERT_EQ(GetClusterId(clusterId, CLUSTERIDMAX), LIBCURVE_ERROR::OK);
    ASSERT_GT(strlen(clusterId), 0);
    ASSERT_EQ(strlen(clusterId), 36);

    memset(clusterId, 0, sizeof(clusterId));
    ASSERT_EQ(GetClusterId(clusterId, 0), -LIBCURVE_ERROR::FAILED);
    ASSERT_EQ(GetClusterId(clusterId, 1), -LIBCURVE_ERROR::FAILED);

    // libcurve file operation
    int temp = Create(filename.c_str(), &userinfo, FLAGS_test_disk_size);

    int fd = Open(filename.c_str(), &userinfo);

    ASSERT_NE(fd, -1);

    char* buffer = new char[8 * 1024];
    memset(buffer, 'a', 1024);
    memset(buffer + 1024, 'b', 1024);
    memset(buffer + 2 * 1024, 'c', 1024);
    memset(buffer + 3 * 1024, 'd', 1024);
    memset(buffer + 4 * 1024, 'e', 1024);
    memset(buffer + 5 * 1024, 'f', 1024);
    memset(buffer + 6 * 1024, 'g', 1024);
    memset(buffer + 7 * 1024, 'h', 1024);

    CurveAioContext writeaioctx;
    writeaioctx.buf = buffer;
    writeaioctx.offset = 0;
    writeaioctx.length = 8 * 1024;
    writeaioctx.cb = writecallbacktest;

    AioWrite(fd, &writeaioctx);
    {
        std::unique_lock<std::mutex> lk(writeinterfacemtx);
        writeinterfacecv.wait(lk, []()->bool{return writeflag;});
    }
    writeflag = false;
    AioWrite(fd, &writeaioctx);
    {
        std::unique_lock<std::mutex> lk(writeinterfacemtx);
        writeinterfacecv.wait(lk, []()->bool{return writeflag;});
    }
    char* readbuffer = new char[8 * 1024];
    CurveAioContext readaioctx;
    readaioctx.buf = readbuffer;
    readaioctx.offset = 0;
    readaioctx.length = 8 * 1024;
    readaioctx.cb = readcallbacktest;
    AioRead(fd, &readaioctx);
    {
        std::unique_lock<std::mutex> lk(interfacemtx);
        interfacecv.wait(lk, []()->bool{return readflag;});
    }

    for (int i = 0; i < 1024; i++) {
        ASSERT_EQ(readbuffer[i], 'a');
        ASSERT_EQ(readbuffer[i +  1024], 'b');
        ASSERT_EQ(readbuffer[i +  2 * 1024], 'c');
        ASSERT_EQ(readbuffer[i +  3 * 1024], 'd');
        ASSERT_EQ(readbuffer[i +  4 * 1024], 'e');
        ASSERT_EQ(readbuffer[i +  5 * 1024], 'f');
        ASSERT_EQ(readbuffer[i +  6 * 1024], 'g');
        ASSERT_EQ(readbuffer[i +  7 * 1024], 'h');
    }

    mds.EnableNetUnstable(400);
    int count = 0;
    while (count < 20) {
        uint64_t offset = 0;
        uint64_t length = 8 * 1024;

        memset(buffer, 'i', 1024);
        memset(buffer + 1024, 'j', 1024);
        memset(buffer + 2 * 1024, 'k', 1024);
        memset(buffer + 3 * 1024, 'l', 1024);
        memset(buffer + 4 * 1024, 'm', 1024);
        memset(buffer + 5 * 1024, 'n', 1024);
        memset(buffer + 6 * 1024, 'o', 1024);
        memset(buffer + 7 * 1024, 'p', 1024);

        ASSERT_EQ(length, Write(fd, buffer, offset, length));
        ASSERT_EQ(length, Read(fd, readbuffer, offset, length));

        for (int i = 0; i < 1024; i++) {
            ASSERT_EQ(readbuffer[i], 'i');
            ASSERT_EQ(readbuffer[i +  1024], 'j');
            ASSERT_EQ(readbuffer[i +  2 * 1024], 'k');
            ASSERT_EQ(readbuffer[i +  3 * 1024], 'l');
            ASSERT_EQ(readbuffer[i +  4 * 1024], 'm');
            ASSERT_EQ(readbuffer[i +  5 * 1024], 'n');
            ASSERT_EQ(readbuffer[i +  6 * 1024], 'o');
            ASSERT_EQ(readbuffer[i +  7 * 1024], 'p');
        }
        count++;
        std::this_thread::sleep_for(std::chrono::milliseconds(400));
    }

    /**
     * the disk is faked, the size is just = 10 * 1024 * 1024 * 1024.
     * when the offset pass the boundary, it will return failed.
     */
    off_t off = 10 * 1024 * 1024 * 1024ul;
    uint64_t len = 8 * 1024;

    ASSERT_EQ(-1 * LIBCURVE_ERROR::FAILED, Write(fd, buffer, off, len));
    ASSERT_EQ(-1 * LIBCURVE_ERROR::FAILED, Read(fd, readbuffer, off, len));

    off_t off1 = 1 * 1024 * 1024 * 1024ul - 8 * 1024;
    uint64_t len1 = 8 * 1024;

    LOG(ERROR) << "normal read write！";
    ASSERT_EQ(len, Write(fd, buffer, off1, len1));
    ASSERT_EQ(len, Read(fd, readbuffer, off1, len1));
    Close(fd);
    mds.UnInitialize();
    delete[] buffer;
    delete[] readbuffer;
    UnInit();
}

TEST(TestLibcurveInterface, FileClientTest) {
    fiu_init(0);
    FLAGS_chunkserver_list =
         "127.0.0.1:9115:0,127.0.0.1:9116:0,127.0.0.1:9117:0";

    std::string filename = "/1";
    UserInfo_t userinfo;
    userinfo.owner = "userinfo";

    FileClient fc;

    // 设置leaderid
    EndPoint ep;
    butil::str2endpoint("127.0.0.1", 9115, &ep);
    PeerId pd(ep);

    // init mds service
    FakeMDS mds(filename);
    mds.Initialize();
    mds.StartCliService(pd);
    mds.StartService();
    mds.CreateCopysetNode(true);

    ASSERT_EQ(0, fc.Init(configpath));

    // init twice also return 0
    ASSERT_EQ(0, fc.Init(configpath));

    int fd = fc.Open4ReadOnly(filename, userinfo);
    int fd2 = fc.Open(filename, userinfo);
    int fd3 = fc.Open(filename, UserInfo_t{});
    int fd4 = fc.Open4ReadOnly(filename, UserInfo_t{});

    ASSERT_NE(fd, -1);
    ASSERT_NE(fd2, -1);

    // user info invalid
    ASSERT_EQ(-1, fd3);
    ASSERT_EQ(-1, fd4);

    fiu_enable("test/client/fake/fakeMDS.GetOrAllocateSegment", 1, nullptr, 0);

    char* buffer = new char[8 * 1024];
    memset(buffer, 'a', 1024);
    memset(buffer + 1024, 'b', 1024);
    memset(buffer + 2 * 1024, 'c', 1024);
    memset(buffer + 3 * 1024, 'd', 1024);
    memset(buffer + 4 * 1024, 'e', 1024);
    memset(buffer + 5 * 1024, 'f', 1024);
    memset(buffer + 6 * 1024, 'g', 1024);
    memset(buffer + 7 * 1024, 'h', 1024);

    CurveAioContext writeaioctx;
    writeaioctx.buf = buffer;
    writeaioctx.offset = 0;
    writeaioctx.length = 8 * 1024;
    writeaioctx.cb = writecallbacktest;

    ASSERT_EQ(-1, fc.AioWrite(fd, &writeaioctx));

    writeflag = false;
    ASSERT_EQ(0, fc.AioWrite(fd2, &writeaioctx));
    {
        std::unique_lock<std::mutex> lk(writeinterfacemtx);
        writeinterfacecv.wait(lk, []()->bool{return writeflag;});
    }
    char* readbuffer = new char[8 * 1024];
    CurveAioContext readaioctx;
    readaioctx.buf = readbuffer;
    readaioctx.offset = 0;
    readaioctx.length = 8 * 1024;
    readaioctx.cb = readcallbacktest;

    readflag = false;
    fc.AioRead(fd, &readaioctx);
    {
        std::unique_lock<std::mutex> lk(interfacemtx);
        interfacecv.wait(lk, []()->bool{return readflag;});
    }

    for (int i = 0; i < 1024; i++) {
        ASSERT_EQ(readbuffer[i], 'a');
        ASSERT_EQ(readbuffer[i +  1024], 'b');
        ASSERT_EQ(readbuffer[i +  2 * 1024], 'c');
        ASSERT_EQ(readbuffer[i +  3 * 1024], 'd');
        ASSERT_EQ(readbuffer[i +  4 * 1024], 'e');
        ASSERT_EQ(readbuffer[i +  5 * 1024], 'f');
        ASSERT_EQ(readbuffer[i +  6 * 1024], 'g');
        ASSERT_EQ(readbuffer[i +  7 * 1024], 'h');
    }

    fc.Close(fd);
    fc.Close(fd2);
    mds.UnInitialize();
    delete[] buffer;
    delete[] readbuffer;
    fc.UnInit();

    // uninit twice
    fc.UnInit();
}

/*
TEST(TestLibcurveInterface, ChunkserverUnstableTest) {
    std::string filename = "/1_userinfo_";

    UserInfo_t userinfo;
    MDSClient mdsclient_;
    FileServiceOption_t fopt;
    FileInstance    fileinstance_;

    FLAGS_chunkserver_list =
         "127.0.0.1:9151:0,127.0.0.1:9152:0,127.0.0.1:9153:0";

    userinfo.owner = "userinfo";
    userinfo.password = "12345";
    fopt.metaServerOpt.metaaddrvec.push_back("127.0.0.1:9104");
    fopt.metaServerOpt.rpcTimeoutMs = 500;
    fopt.metaServerOpt.rpcRetryTimes = 3;
    fopt.loginfo.loglevel = 0;
    fopt.ioOpt.ioSplitOpt.ioSplitMaxSizeKB = 64;
    fopt.ioOpt.ioSenderOpt.enableAppliedIndexRead = 1;
    fopt.ioOpt.ioSenderOpt.rpcTimeoutMs = 1000;
    fopt.ioOpt.ioSenderOpt.rpcRetryTimes = 3;
    fopt.ioOpt.ioSenderOpt.failRequestOpt.opMaxRetry = 3;
    fopt.ioOpt.ioSenderOpt.failRequestOpt.opRetryIntervalUs = 500;
    fopt.ioOpt.metaCacheOpt.getLeaderRetry = 3;
    fopt.ioOpt.metaCacheOpt.retryIntervalUs = 500;
    fopt.ioOpt.reqSchdulerOpt.queueCapacity = 4096;
    fopt.ioOpt.reqSchdulerOpt.threadpoolSize = 2;
    fopt.ioOpt.reqSchdulerOpt.ioSenderOpt = fopt.ioOpt.ioSenderOpt;
    fopt.leaseOpt.refreshTimesPerLease = 4;

    mdsclient_.Initialize(fopt.metaServerOpt);
    fileinstance_.Initialize("/test", &mdsclient_, userinfo, fopt);

    // 设置leaderid
    EndPoint ep;
    butil::str2endpoint("127.0.0.1", 9151, &ep);
    PeerId pd(ep);

    // init mds service
    FakeMDS mds(filename);
    mds.Initialize();
    mds.StartCliService(pd);
    mds.StartService();
    mds.CreateCopysetNode(true);

    std::this_thread::sleep_for(std::chrono::milliseconds(1000));
    int fd = fileinstance_.Open(filename.c_str(), userinfo);

    MetaCache* mc = fileinstance_.GetIOManager4File()->GetMetaCache();

    ASSERT_NE(fd, -1);

    CliServiceFake* cliservice = mds.GetCliService();
    std::vector<FakeChunkService*> chunkservice = mds.GetFakeChunkService();

    char* buffer = new char[8 * 1024];
    uint64_t offset = 0;
    uint64_t length = 8 * 1024;

    memset(buffer, 'i', 1024);
    memset(buffer + 1024, 'j', 1024);
    memset(buffer + 2 * 1024, 'k', 1024);
    memset(buffer + 3 * 1024, 'l', 1024);
    memset(buffer + 4 * 1024, 'm', 1024);
    memset(buffer + 5 * 1024, 'n', 1024);
    memset(buffer + 6 * 1024, 'o', 1024);
    memset(buffer + 7 * 1024, 'p', 1024);

    ASSERT_EQ(length, fileinstance_.Write(buffer, offset, length));
    ASSERT_EQ(length, fileinstance_.Read(buffer, offset, length));

    // 正常情况下只有第一次会去get leader
    ASSERT_EQ(1, cliservice->GetInvokeTimes());
    // metacache中被写过的copyset leadermaychange都处于正常状态
    ChunkIDInfo_t chunkinfo1;
    MetaCacheErrorType rc = mc->GetChunkInfoByIndex(0, &chunkinfo1);
    ASSERT_EQ(rc, MetaCacheErrorType::OK);
    for (int i = 0; i < FLAGS_copyset_num; i++) {
        CopysetInfo_t ci = mc->GetCopysetinfo(FLAGS_logic_pool_id, i);
        if (i == chunkinfo1.cpid_) {
            ASSERT_NE(-1, ci.GetCurrentLeaderIndex());
            ASSERT_FALSE(ci.LeaderMayChange());
        } else {
            ASSERT_EQ(-1, ci.GetCurrentLeaderIndex());
            ASSERT_FALSE(ci.LeaderMayChange());
        }
    }

    // 设置chunkservice返回失败，那么mds每次重试都会去拉新的leader
    // 127.0.0.1:9151:0,127.0.0.1:9152:0,127.0.0.1:9153:0是当前集群信息
    // 127.0.0.1:9151对应第一个chunkservice
    // 设置rpc失败，会导致client将该chunkserverid上的leader copyset都标记为
    // leadermaychange
    chunkservice[0]->SetRPCFailed();
    // 现在写第二个chunk，第二个chunk与第一个chunk不在同一个copyset里，这次读写失败
    ASSERT_EQ(-2, fileinstance_.Write(buffer, 1 * chunk_size, length));
    ASSERT_EQ(-2, fileinstance_.Read(buffer, 1 * chunk_size, length));
    // 获取第2个chunk的chunkid信息
    ChunkIDInfo_t chunkinfo2;
    rc = mc->GetChunkInfoByIndex(1, &chunkinfo2);
    ASSERT_EQ(rc, MetaCacheErrorType::OK);
    ASSERT_NE(chunkinfo2.cpid_, chunkinfo1.cpid_);
    for (int i = 0; i < FLAGS_copyset_num; i++) {
        CopysetInfo_t ci = mc->GetCopysetinfo(FLAGS_logic_pool_id, i);
        if (i == chunkinfo1.cpid_ || i == chunkinfo2.cpid_) {
            ASSERT_NE(-1, ci.GetCurrentLeaderIndex());
            // 这两个leader为该chunkserver的copyset的LeaderMayChange置位
            ASSERT_TRUE(ci.LeaderMayChange());
        } else {
            // 对于当前copyset没有leader信息的就直接置位LeaderMayChange
            ASSERT_EQ(-1, ci.GetCurrentLeaderIndex());
            ASSERT_TRUE(ci.LeaderMayChange());
        }
    }

    chunkservice[0]->ReSetRPCFailed();
    // 再次写第二个chunk，这时候获取leader成功后，会将LeaderMayChange置位fasle
    // 第一个chunk对应的copyset依然LeaderMayChange为true
    ASSERT_EQ(8192, fileinstance_.Write(buffer, 1 * chunk_size, length));
    ASSERT_EQ(8192, fileinstance_.Read(buffer, 1 * chunk_size, length));
    for (int i = 0; i < FLAGS_copyset_num; i++) {
        CopysetInfo_t ci = mc->GetCopysetinfo(FLAGS_logic_pool_id, i);
        if (i == chunkinfo2.cpid_) {
            ASSERT_NE(-1, ci.GetCurrentLeaderIndex());
            // copyset2的LeaderMayChange置位
            ASSERT_FALSE(ci.LeaderMayChange());
        } else if (i == chunkinfo1.cpid_) {
            ASSERT_NE(-1, ci.GetCurrentLeaderIndex());
            // copyset1的LeaderMayChange保持原有状态
            ASSERT_TRUE(ci.LeaderMayChange());
        } else {
            // 对于当前copyset没有leader信息的就直接置位LeaderMayChange
            ASSERT_EQ(-1, ci.GetCurrentLeaderIndex());
            ASSERT_TRUE(ci.LeaderMayChange());
        }
    }

    cliservice->ReSetInvokeTimes();
    EndPoint ep2;
    butil::str2endpoint("127.0.0.1", 9152, &ep2);
    PeerId pd2(ep2);
    cliservice->SetPeerID(pd2);
    // 设置rpc失败，迫使copyset切换leader，切换leader后读写成功
    chunkservice[0]->SetRPCFailed();
    // 读写第一个和第二个chunk
    ASSERT_EQ(8192, fileinstance_.Write(buffer, 0 * chunk_size, length));
    ASSERT_EQ(8192, fileinstance_.Read(buffer, 0 * chunk_size, length));
    ASSERT_EQ(8192, fileinstance_.Write(buffer, 0 * chunk_size, length));
    ASSERT_EQ(8192, fileinstance_.Read(buffer, 0 * chunk_size, length));
    ASSERT_EQ(1, cliservice->GetInvokeTimes());
    // 这个时候
    for (int i = 0; i < FLAGS_copyset_num; i++) {
        CopysetInfo_t ci = mc->GetCopysetinfo(FLAGS_logic_pool_id, i);
        if (i == chunkinfo2.cpid_) {
            ASSERT_NE(-1, ci.GetCurrentLeaderIndex());
            // copyset2的LeaderMayChange置位
            ASSERT_FALSE(ci.LeaderMayChange());
        } else if (i == chunkinfo1.cpid_) {
            ASSERT_NE(-1, ci.GetCurrentLeaderIndex());
            // copyset1的LeaderMayChange置位
            ASSERT_FALSE(ci.LeaderMayChange());
        } else {
            // 对于当前copyset没有leader信息的就直接置位LeaderMayChange
            ASSERT_EQ(-1, ci.GetCurrentLeaderIndex());
            ASSERT_TRUE(ci.LeaderMayChange());
        }
    }

    // 验证copyset id信息更新
    // copyset id = 888， chunkserver id = 100 101 102
    // copyset id = 999， chunkserver id = 102 103 104
    CopysetInfo_t csinfo1;
    ChunkServerAddr addr;
    csinfo1.cpid_ = 888;
    curve::client::CopysetPeerInfo_t peer1(100, addr);
    csinfo1.csinfos_.push_back(peer1);
    curve::client::CopysetPeerInfo_t peer2(101, addr);
    csinfo1.csinfos_.push_back(peer2);
    curve::client::CopysetPeerInfo_t peer3(102, addr);
    csinfo1.csinfos_.push_back(peer3);

    CopysetInfo_t csinfo2;
    csinfo2.cpid_ = 999;
    curve::client::CopysetPeerInfo_t peer4(102, addr);
    csinfo2.csinfos_.push_back(peer4);
    curve::client::CopysetPeerInfo_t peer5(103, addr);
    csinfo2.csinfos_.push_back(peer5);
    curve::client::CopysetPeerInfo_t peer6(104, addr);
    csinfo2.csinfos_.push_back(peer6);

    mc->UpdateCopysetInfo(FLAGS_logic_pool_id, 888, csinfo1);
    mc->UpdateCopysetInfo(FLAGS_logic_pool_id, 999, csinfo2);

    auto cpinfo1 = mc->GetCopysetinfo(FLAGS_logic_pool_id, 888);
    auto cpinfo2 = mc->GetCopysetinfo(FLAGS_logic_pool_id, 999);

    ASSERT_EQ(888, cpinfo1.cpid_);
    ASSERT_EQ(999, cpinfo2.cpid_);


    mc->AddCopysetIDInfo(100, CopysetIDInfo(FLAGS_logic_pool_id, 888));
    mc->AddCopysetIDInfo(101, CopysetIDInfo(FLAGS_logic_pool_id, 888));
    mc->AddCopysetIDInfo(102, CopysetIDInfo(FLAGS_logic_pool_id, 888));
    mc->AddCopysetIDInfo(102, CopysetIDInfo(FLAGS_logic_pool_id, 999));
    mc->AddCopysetIDInfo(103, CopysetIDInfo(FLAGS_logic_pool_id, 999));
    mc->AddCopysetIDInfo(104, CopysetIDInfo(FLAGS_logic_pool_id, 999));

    ASSERT_TRUE(mc->CopysetIDInfoIn(100, FLAGS_logic_pool_id, 888));
    ASSERT_TRUE(mc->CopysetIDInfoIn(101, FLAGS_logic_pool_id, 888));
    ASSERT_TRUE(mc->CopysetIDInfoIn(102, FLAGS_logic_pool_id, 888));
    ASSERT_TRUE(mc->CopysetIDInfoIn(102, FLAGS_logic_pool_id, 999));
    ASSERT_TRUE(mc->CopysetIDInfoIn(103, FLAGS_logic_pool_id, 999));
    ASSERT_TRUE(mc->CopysetIDInfoIn(104, FLAGS_logic_pool_id, 999));
    ASSERT_FALSE(mc->CopysetIDInfoIn(101, FLAGS_logic_pool_id, 999));


    CopysetInfo_t csinfo3;
    csinfo3.cpid_ = 999;
    curve::client::CopysetPeerInfo_t peer7(100, addr);
    csinfo3.csinfos_.push_back(peer7);
    curve::client::CopysetPeerInfo_t peer8(101, addr);
    csinfo3.csinfos_.push_back(peer8);
    curve::client::CopysetPeerInfo_t peer9(103, addr);
    csinfo3.csinfos_.push_back(peer9);

    // 更新copyset信息，chunkserver 104的信息被清除
    // 100，和 101上添加了新的copyset信息
    mc->UpdateChunkserverCopysetInfo(FLAGS_logic_pool_id, csinfo3);
    ASSERT_TRUE(mc->CopysetIDInfoIn(100, FLAGS_logic_pool_id, 888));
    ASSERT_TRUE(mc->CopysetIDInfoIn(100, FLAGS_logic_pool_id, 999));
    ASSERT_TRUE(mc->CopysetIDInfoIn(101, FLAGS_logic_pool_id, 888));
    ASSERT_TRUE(mc->CopysetIDInfoIn(101, FLAGS_logic_pool_id, 999));
    ASSERT_TRUE(mc->CopysetIDInfoIn(102, FLAGS_logic_pool_id, 888));
    ASSERT_TRUE(mc->CopysetIDInfoIn(103, FLAGS_logic_pool_id, 999));
    ASSERT_FALSE(mc->CopysetIDInfoIn(104, FLAGS_logic_pool_id, 999));
    ASSERT_FALSE(mc->CopysetIDInfoIn(102, FLAGS_logic_pool_id, 999));

    mdsclient_.UnInitialize();
    fileinstance_.UnInitialize();
    mds.UnInitialize();
    delete[] buffer;
}
*/
TEST(TestLibcurveInterface, InterfaceExceptionTest) {
    std::string filename = "/1_userinfo_";

    C_UserInfo_t userinfo;
    memcpy(userinfo.owner, "userinfo", 9);
    memcpy(userinfo.password, "", 256);

    ASSERT_EQ(-2, Init(configpath.c_str()));

    // open not create file
    ASSERT_EQ(-1 * LIBCURVE_ERROR::FAILED, Open(filename.c_str(), &userinfo));

    // 设置leaderid
    EndPoint ep;
    butil::str2endpoint("127.0.0.1", 9106, &ep);
    PeerId pd(ep);

    // init mds service
    FakeMDS mds(filename);
    mds.Initialize();
    mds.StartCliService(pd);
    mds.StartService();
    mds.CreateCopysetNode(true);

    ASSERT_EQ(0, Init(configpath.c_str()));


    char* buffer = new char[8 * 1024];
    memset(buffer, 'a', 8*1024);

    // not aligned test
    CurveAioContext ctx;
    ctx.buf = buffer;
    ctx.offset = 1;
    ctx.length = 7 * 1024;
    ctx.cb = writecallbacktest;
    ASSERT_EQ(-LIBCURVE_ERROR::NOT_ALIGNED, AioWrite(1234, &ctx));
    ASSERT_EQ(-LIBCURVE_ERROR::NOT_ALIGNED, AioRead(1234, &ctx));
    ASSERT_EQ(-LIBCURVE_ERROR::NOT_ALIGNED, Write(1234, buffer, 1, 4096));
    ASSERT_EQ(-LIBCURVE_ERROR::NOT_ALIGNED, Read(1234, buffer, 4096 , 123));

    CurveAioContext writeaioctx;
    writeaioctx.buf = buffer;
    writeaioctx.offset = 0;
    writeaioctx.length = 8 * 1024;
    writeaioctx.cb = writecallbacktest;

    // aiowrite not opened file
    ASSERT_EQ(-LIBCURVE_ERROR::BAD_FD, AioWrite(1234, &writeaioctx));

    // aioread not opened file
    char* readbuffer = new char[8 * 1024];
    CurveAioContext readaioctx;
    readaioctx.buf = readbuffer;
    readaioctx.offset = 0;
    readaioctx.length = 8 * 1024;
    readaioctx.cb = readcallbacktest;
    ASSERT_EQ(-1 * LIBCURVE_ERROR::BAD_FD, AioRead(1234, &readaioctx));

    uint64_t offset = 0;
    uint64_t length = 8 * 1024;

    // write not opened file
    ASSERT_EQ(-1 * LIBCURVE_ERROR::BAD_FD,
                Write(1234, buffer, offset, length));
    // read not opened file
    ASSERT_EQ(-1 * LIBCURVE_ERROR::BAD_FD, Read(1234,
                readbuffer, offset, length));

    delete[] buffer;
    delete[] readbuffer;
    UnInit();
    mds.UnInitialize();
}

TEST(TestLibcurveInterface, UnstableChunkserverTest) {
    std::string filename = "/1_userinfo_";

    UserInfo_t userinfo;
    MDSClient mdsclient_;
    FileServiceOption_t fopt;
    FileInstance    fileinstance_;

    FLAGS_chunkserver_list =
         "127.0.0.1:9151:0,127.0.0.1:9152:0,127.0.0.1:9153:0";

    userinfo.owner = "userinfo";
    userinfo.password = "12345";
    fopt.metaServerOpt.metaaddrvec.push_back("127.0.0.1:9104");
    fopt.metaServerOpt.rpcTimeoutMs = 500;
    fopt.metaServerOpt.rpcRetryTimes = 3;
    fopt.loginfo.loglevel = 0;
    fopt.ioOpt.ioSplitOpt.ioSplitMaxSizeKB = 64;
    fopt.ioOpt.ioSenderOpt.enableAppliedIndexRead = 1;
    fopt.ioOpt.ioSenderOpt.rpcTimeoutMs = 1000;
    fopt.ioOpt.ioSenderOpt.rpcRetryTimes = 3;
    fopt.ioOpt.ioSenderOpt.failRequestOpt.opMaxRetry = 3;
    fopt.ioOpt.ioSenderOpt.failRequestOpt.opRetryIntervalUs = 500;
    fopt.ioOpt.metaCacheOpt.getLeaderRetry = 3;
    fopt.ioOpt.metaCacheOpt.retryIntervalUs = 500;
    fopt.ioOpt.reqSchdulerOpt.queueCapacity = 4096;
    fopt.ioOpt.reqSchdulerOpt.threadpoolSize = 2;
    fopt.ioOpt.reqSchdulerOpt.ioSenderOpt = fopt.ioOpt.ioSenderOpt;
    fopt.leaseOpt.refreshTimesPerLease = 4;
    fopt.ioOpt.ioSenderOpt.failRequestOpt.maxStableChunkServerTimeoutTimes = 10;  // NOLINT

    mdsclient_.Initialize(fopt.metaServerOpt);
    fileinstance_.Initialize("/test", &mdsclient_, userinfo, fopt);

    // 设置leaderid
    EndPoint ep;
    butil::str2endpoint("127.0.0.1", 9151, &ep);
    PeerId pd(ep);

    // init mds service
    FakeMDS mds(filename);
    mds.Initialize();
    mds.StartCliService(pd);
    mds.StartService();
    mds.CreateCopysetNode(true);

    std::this_thread::sleep_for(std::chrono::milliseconds(1000));
    int fd = fileinstance_.Open(filename.c_str(), userinfo);

    MetaCache* mc = fileinstance_.GetIOManager4File()->GetMetaCache();

    ASSERT_NE(fd, -1);

    CliServiceFake* cliservice = mds.GetCliService();
    std::vector<FakeChunkService*> chunkservice = mds.GetFakeChunkService();

    char* buffer = new char[8 * 1024];
    uint64_t offset = 0;
    uint64_t length = 8 * 1024;

    memset(buffer, 'i', 1024);
    memset(buffer + 1024, 'j', 1024);
    memset(buffer + 2 * 1024, 'k', 1024);
    memset(buffer + 3 * 1024, 'l', 1024);
    memset(buffer + 4 * 1024, 'm', 1024);
    memset(buffer + 5 * 1024, 'n', 1024);
    memset(buffer + 6 * 1024, 'o', 1024);
    memset(buffer + 7 * 1024, 'p', 1024);

    ASSERT_EQ(length, fileinstance_.Write(buffer, offset, length));
    ASSERT_EQ(length, fileinstance_.Read(buffer, offset, length));

    // 正常情况下只有第一次会去get leader
    ASSERT_EQ(1, cliservice->GetInvokeTimes());
    // metacache中被写过的copyset leadermaychange都处于正常状态
    ChunkIDInfo_t chunkinfo1;
    MetaCacheErrorType rc = mc->GetChunkInfoByIndex(0, &chunkinfo1);
    ASSERT_EQ(rc, MetaCacheErrorType::OK);
    for (int i = 0; i < FLAGS_copyset_num; i++) {
        CopysetInfo_t ci = mc->GetCopysetinfo(FLAGS_logic_pool_id, i);
        if (i == chunkinfo1.cpid_) {
            ASSERT_NE(-1, ci.GetCurrentLeaderIndex());
            ASSERT_FALSE(ci.LeaderMayChange());
        } else {
            ASSERT_EQ(-1, ci.GetCurrentLeaderIndex());
            ASSERT_FALSE(ci.LeaderMayChange());
        }
    }

    mds.EnableNetUnstable(10000);

    // 写2次，读2次，每次请求重试3次
    // 因为在chunkserver端设置了延迟，导致每次请求都会超时
    // unstable阈值为10，所以第11次请求返回时，对应的chunkserver被标记为unstable
    // leader在对应chunkserver上的copyset会设置leaderMayChange为true
    // 下次发起请求时，会先去刷新leader信息，
    // 由于leader没有发生改变，而且延迟仍然存在
    // 所以第12次请求仍然超时，leaderMayChange仍然为true
    ASSERT_EQ(-2, fileinstance_.Write(buffer, 1 * chunk_size, length));
    ASSERT_EQ(-2, fileinstance_.Write(buffer, 1 * chunk_size, length));
    ASSERT_EQ(-2, fileinstance_.Read(buffer, 1 * chunk_size, length));
    ASSERT_EQ(-2, fileinstance_.Read(buffer, 1 * chunk_size, length));

    // 获取第2个chunk的chunkid信息
    ChunkIDInfo_t chunkinfo2;
    rc = mc->GetChunkInfoByIndex(1, &chunkinfo2);
    ASSERT_EQ(rc, MetaCacheErrorType::OK);
    ASSERT_NE(chunkinfo2.cpid_, chunkinfo1.cpid_);
    for (int i = 0; i < FLAGS_copyset_num; ++i) {
        CopysetInfo_t ci = mc->GetCopysetinfo(FLAGS_logic_pool_id, i);
        if (i == chunkinfo1.cpid_ || i == chunkinfo2.cpid_) {
            ASSERT_NE(-1, ci.GetCurrentLeaderIndex());
            ASSERT_TRUE(ci.LeaderMayChange());
        } else {
            ASSERT_EQ(-1, ci.GetCurrentLeaderIndex());
            ASSERT_TRUE(ci.LeaderMayChange());
        }
    }

    // 当copyset处于unstable状态时
    // 不进入超时时间指数退避逻辑，rpc超时时间设置为默认值
    // 所以每个请求总时间为3s，4个请求需要12s
    auto start = TimeUtility::GetTimeofDayMs();
    ASSERT_EQ(-2, fileinstance_.Write(buffer, 1 * chunk_size, length));
    ASSERT_EQ(-2, fileinstance_.Write(buffer, 1 * chunk_size, length));
    ASSERT_EQ(-2, fileinstance_.Read(buffer, 1 * chunk_size, length));
    ASSERT_EQ(-2, fileinstance_.Read(buffer, 1 * chunk_size, length));
    auto end = TimeUtility::GetTimeofDayMs();
    ASSERT_GT(end - start, 11 * 1000);
    ASSERT_LT(end - start, 13 * 1000);

    mds.DisableNetUnstable();

    // 取消延迟，再次读写第2个chunk
    // 获取leader信息后，会将leaderMayChange置为false
    // 第一个chunk对应的copyset依赖leaderMayChange为true
    ASSERT_EQ(8192, fileinstance_.Write(buffer, 1 * chunk_size, length));
    ASSERT_EQ(8192, fileinstance_.Read(buffer, 1 * chunk_size, length));
    for (int i = 0; i < FLAGS_copyset_num; ++i) {
        CopysetInfo_t ci = mc->GetCopysetinfo(FLAGS_logic_pool_id, i);
        if (i == chunkinfo2.cpid_) {
            ASSERT_NE(-1, ci.GetCurrentLeaderIndex());
            ASSERT_FALSE(ci.LeaderMayChange());
        } else if (i == chunkinfo1.cpid_) {
            ASSERT_NE(-1, ci.GetCurrentLeaderIndex());
            ASSERT_TRUE(ci.LeaderMayChange());
        } else {
            ASSERT_EQ(-1, ci.GetCurrentLeaderIndex());
            ASSERT_TRUE(ci.LeaderMayChange());
        }
    }

    cliservice->ReSetInvokeTimes();
    EndPoint ep2;
    butil::str2endpoint("127.0.0.1", 9153, &ep2);
    PeerId pd2(ep2);
    cliservice->SetPeerID(pd2);

    // 设置rcp返回失败，迫使copyset切换leader, 切换leader后读写成功
    chunkservice[0]->SetRPCFailed();

    ASSERT_EQ(8192, fileinstance_.Write(buffer, 0 * chunk_size, length));
    ASSERT_EQ(8192, fileinstance_.Read(buffer, 0 * chunk_size, length));
    ASSERT_EQ(8192, fileinstance_.Write(buffer, 1 * chunk_size, length));
    ASSERT_EQ(8192, fileinstance_.Read(buffer, 1 * chunk_size, length));

    ASSERT_EQ(2, cliservice->GetInvokeTimes());

    for (int i = 0; i < FLAGS_copyset_num; ++i) {
        CopysetInfo_t ci = mc->GetCopysetinfo(FLAGS_logic_pool_id, i);
        if (i == chunkinfo2.cpid_) {
            ASSERT_NE(-1, ci.GetCurrentLeaderIndex());
            ASSERT_FALSE(ci.LeaderMayChange());
        } else if (i == chunkinfo1.cpid_) {
            ASSERT_NE(-1, ci.GetCurrentLeaderIndex());
            ASSERT_FALSE(ci.LeaderMayChange());
        } else {
            ASSERT_EQ(-1, ci.GetCurrentLeaderIndex());
            ASSERT_TRUE(ci.LeaderMayChange());
        }
    }

    mdsclient_.UnInitialize();
    fileinstance_.UnInitialize();
    mds.UnInitialize();
    delete[] buffer;
}

