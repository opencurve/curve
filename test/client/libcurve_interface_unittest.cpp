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
 * File Created: Tuesday, 9th October 2018 5:16:52 pm
 * Author: tongguangxun
 */

#include <fiu-control.h>
#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gtest/gtest.h>
#include <chrono>              // NOLINT
#include <condition_variable>  // NOLINT
#include <iostream>
#include <mutex>  // NOLINT
#include <string>
#include <thread>  //NOLINT

#include "include/client/libcurve.h"
#include "src/client/chunk_closure.h"
#include "src/client/client_common.h"
#include "src/client/file_instance.h"
#include "src/client/libcurve_file.h"
#include "test/client/fake/fakeMDS.h"
#include "test/client/fake/mock_schedule.h"

extern std::string configpath;
extern uint32_t chunk_size;
extern uint32_t segment_size;

DECLARE_string(chunkserver_list);
DECLARE_uint32(logic_pool_id);
DECLARE_uint32(copyset_num);
DECLARE_uint64(test_disk_size);

namespace curve {
namespace client {

bool writeflag = false;
bool readflag = false;
std::mutex writeinterfacemtx;
std::condition_variable writeinterfacecv;
std::mutex interfacemtx;
std::condition_variable interfacecv;

void writecallbacktest(CurveAioContext *context) {
    std::lock_guard<std::mutex> lk(writeinterfacemtx);
    writeflag = true;
    writeinterfacecv.notify_one();
    LOG(INFO) << "aio call back here, errorcode = " << context->ret;
}

void readcallbacktest(CurveAioContext *context) {
    std::lock_guard<std::mutex> lk(writeinterfacemtx);
    readflag = true;
    interfacecv.notify_one();
    LOG(INFO) << "aio call back here, errorcode = " << context->ret;
}

class TestLibcurveInterface : public ::testing::Test {
 protected:
    void SetUp() override {}

    void TearDown() override {}
};

TEST_F(TestLibcurveInterface, InterfaceTest) {
    FLAGS_chunkserver_list =
        "127.0.0.1:9115:0,127.0.0.1:9116:0,127.0.0.1:9117:0";

    std::string filename = "/1_userinfo_";
    C_UserInfo_t userinfo;
    memcpy(userinfo.owner, "userinfo", 9);
    memcpy(userinfo.password, "", 1);

    // Set leaderid
    EndPoint ep;
    butil::str2endpoint("127.0.0.1", 9115, &ep);
    PeerId pd(ep);

    // init mds service
    FakeMDS mds(filename);
    mds.Initialize();
    mds.StartCliService(pd);
    mds.StartService();
    mds.CreateCopysetNode(true);

    // test get cluster id
    const int CLUSTERIDMAX = 256;
    char clusterId[CLUSTERIDMAX];

    ASSERT_EQ(GetClusterId(clusterId, CLUSTERIDMAX), -LIBCURVE_ERROR::FAILED);

    ASSERT_EQ(0, Init(configpath.c_str()));

    std::this_thread::sleep_for(std::chrono::milliseconds(1000));

    ASSERT_EQ(GetClusterId(nullptr, 0), -LIBCURVE_ERROR::FAILED);

    memset(clusterId, 0, sizeof(clusterId));
    ASSERT_EQ(GetClusterId(clusterId, CLUSTERIDMAX), LIBCURVE_ERROR::OK);
    ASSERT_GT(strlen(clusterId), 0);
    ASSERT_EQ(strlen(clusterId), 36);

    memset(clusterId, 0, sizeof(clusterId));
    ASSERT_EQ(GetClusterId(clusterId, 0), -LIBCURVE_ERROR::FAILED);
    ASSERT_EQ(GetClusterId(clusterId, 1), -LIBCURVE_ERROR::FAILED);

    // libcurve file operation
    (void)Create(filename.c_str(), &userinfo, FLAGS_test_disk_size);

    int fd = Open(filename.c_str(), &userinfo);

    ASSERT_NE(fd, -1);

    char *buffer = new char[8 * 1024];
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
        writeinterfacecv.wait(lk, []() -> bool { return writeflag; });
    }
    writeflag = false;
    AioWrite(fd, &writeaioctx);
    {
        std::unique_lock<std::mutex> lk(writeinterfacemtx);
        writeinterfacecv.wait(lk, []() -> bool { return writeflag; });
    }
    char *readbuffer = new char[8 * 1024];
    CurveAioContext readaioctx;
    readaioctx.buf = readbuffer;
    readaioctx.offset = 0;
    readaioctx.length = 8 * 1024;
    readaioctx.cb = readcallbacktest;
    AioRead(fd, &readaioctx);
    {
        std::unique_lock<std::mutex> lk(interfacemtx);
        interfacecv.wait(lk, []() -> bool { return readflag; });
    }

    for (int i = 0; i < 1024; i++) {
        ASSERT_EQ(readbuffer[i], 'a');
        ASSERT_EQ(readbuffer[i + 1024], 'b');
        ASSERT_EQ(readbuffer[i + 2 * 1024], 'c');
        ASSERT_EQ(readbuffer[i + 3 * 1024], 'd');
        ASSERT_EQ(readbuffer[i + 4 * 1024], 'e');
        ASSERT_EQ(readbuffer[i + 5 * 1024], 'f');
        ASSERT_EQ(readbuffer[i + 6 * 1024], 'g');
        ASSERT_EQ(readbuffer[i + 7 * 1024], 'h');
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
            ASSERT_EQ(readbuffer[i + 1024], 'j');
            ASSERT_EQ(readbuffer[i + 2 * 1024], 'k');
            ASSERT_EQ(readbuffer[i + 3 * 1024], 'l');
            ASSERT_EQ(readbuffer[i + 4 * 1024], 'm');
            ASSERT_EQ(readbuffer[i + 5 * 1024], 'n');
            ASSERT_EQ(readbuffer[i + 6 * 1024], 'o');
            ASSERT_EQ(readbuffer[i + 7 * 1024], 'p');
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

TEST_F(TestLibcurveInterface, FileClientTest) {
    fiu_init(0);
    FLAGS_chunkserver_list =
        "127.0.0.1:9115:0,127.0.0.1:9116:0,127.0.0.1:9117:0";

    std::string filename = "/1";
    UserInfo_t userinfo;
    userinfo.owner = "userinfo";

    FileClient fc;

    // Set leaderid
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

    ASSERT_EQ(0, fc.GetOpenedFileNum());

    int fd = fc.Open4ReadOnly(filename, userinfo);
    int fd2 = fc.Open(filename, userinfo);
    int fd3 = fc.Open(filename, UserInfo_t{});
    int fd4 = fc.Open4ReadOnly(filename, UserInfo_t{});

    ASSERT_NE(fd, -1);
    ASSERT_NE(fd2, -1);

    // user info invalid
    ASSERT_EQ(-1, fd3);
    ASSERT_EQ(-1, fd4);

    ASSERT_EQ(2, fc.GetOpenedFileNum());

    fiu_enable("test/client/fake/fakeMDS.GetOrAllocateSegment", 1, nullptr, 0);

    char *buffer = new char[8 * 1024];
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
        writeinterfacecv.wait(lk, []() -> bool { return writeflag; });
    }
    char *readbuffer = new char[8 * 1024];
    memset(readbuffer, 0xFF, 8 * 1024);
    CurveAioContext readaioctx;
    readaioctx.buf = readbuffer;
    readaioctx.offset = 0;
    readaioctx.length = 8 * 1024;
    readaioctx.cb = readcallbacktest;

    readflag = false;
    fc.AioRead(fd, &readaioctx);
    {
        std::unique_lock<std::mutex> lk(interfacemtx);
        interfacecv.wait(lk, []() -> bool { return readflag; });
    }

    ASSERT_EQ(readaioctx.ret, readaioctx.length);
    for (int i = 0; i < 1024; i++) {
        ASSERT_EQ(readbuffer[i], 'a');
        ASSERT_EQ(readbuffer[i + 1024], 'b');
        ASSERT_EQ(readbuffer[i + 2 * 1024], 'c');
        ASSERT_EQ(readbuffer[i + 3 * 1024], 'd');
        ASSERT_EQ(readbuffer[i + 4 * 1024], 'e');
        ASSERT_EQ(readbuffer[i + 5 * 1024], 'f');
        ASSERT_EQ(readbuffer[i + 6 * 1024], 'g');
        ASSERT_EQ(readbuffer[i + 7 * 1024], 'h');
    }

    fc.Close(fd);
    fc.Close(fd2);

    ASSERT_EQ(0, fc.GetOpenedFileNum());

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
    FileServiceOption fopt;
    FileInstance    fileinstance_;

    FLAGS_chunkserver_list =
         "127.0.0.1:9151:0,127.0.0.1:9152:0,127.0.0.1:9153:0";

    userinfo.owner = "userinfo";
    userinfo.password = "12345";
    fopt.metaServerOpt.mdsAddrs.push_back("127.0.0.1:9104");
    fopt.metaServerOpt.chunkserverRPCTimeoutMS = 500;
    fopt.loginfo.logLevel = 0;
    fopt.ioOpt.ioSplitOpt.fileIOSplitMaxSizeKB = 64;
    fopt.ioOpt.ioSenderOpt.chunkserverRPCTimeoutMS = 1000;
    fopt.ioOpt.ioSenderOpt.failRequestOpt.chunkserverOPMaxRetry = 3;
    fopt.ioOpt.ioSenderOpt.failRequestOpt.chunkserverOPRetryIntervalUS = 500;
    fopt.ioOpt.metaCacheOpt.metacacheGetLeaderRetry = 3;
    fopt.ioOpt.metaCacheOpt.metacacheRPCRetryIntervalUS = 500;
    fopt.ioOpt.reqSchdulerOpt.scheduleQueueCapacity = 4096;
    fopt.ioOpt.reqSchdulerOpt.scheduleThreadpoolSize = 2;
    fopt.ioOpt.reqSchdulerOpt.ioSenderOpt = fopt.ioOpt.ioSenderOpt;
    fopt.leaseOpt.mdsRefreshTimesPerLease = 4;

    mdsclient_.Initialize(fopt.metaServerOpt);
    fileinstance_.Initialize("/test", &mdsclient_, userinfo, fopt);

    // set leaderid
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

    // Normally, getting the leader will only occur the first time.
    ASSERT_EQ(1, cliservice->GetInvokeTimes());
    // LeaderMayChange remains in a normal state for copyset leader that has been written to in metacache.
    ChunkIDInfo_t chunkinfo1;
    MetaCacheErrorType rc = mc->GetChunkInfoByIndex(0, &chunkinfo1);
    ASSERT_EQ(rc, MetaCacheErrorType::OK);
    for (int i = 0; i < FLAGS_copyset_num; i++) {
        CopysetPeerInfo ci = mc->GetCopysetinfo(FLAGS_logic_pool_id, i);
        if (i == chunkinfo1.cpid_) {
            ASSERT_NE(-1, ci.GetCurrentLeaderIndex());
            ASSERT_FALSE(ci.LeaderMayChange());
        } else {
            ASSERT_EQ(-1, ci.GetCurrentLeaderIndex());
            ASSERT_FALSE(ci.LeaderMayChange());
        }
    }

    // If chunkservice returns failure, MDS will retry and fetch new leaders each time.
    // The current cluster information is: 127.0.0.1:9151:0, 127.0.0.1:9152:0, 127.0.0.1:9153:0.
    // 127.0.0.1:9151 corresponds to the first chunkservice.
    // An RPC failure causes the client to mark all leader copysets on that chunkserver id as 
    // leadermaychange
    chunkservice[0]->SetRPCFailed();
    //
Now, write to the second chunk; as it does not belong to the same copyset as the first chunk, this read and write attempt fails.
    ASSERT_EQ(-2, fileinstance_.Write(buffer, 1 * chunk_size, length));
    ASSERT_EQ(-2, fileinstance_.Read(buffer, 1 * chunk_size, length));
    // Obtain chunkid information for the second chunk.
    ChunkIDInfo_t chunkinfo2;
    rc = mc->GetChunkInfoByIndex(1, &chunkinfo2);
    ASSERT_EQ(rc, MetaCacheErrorType::OK);
    ASSERT_NE(chunkinfo2.cpid_, chunkinfo1.cpid_);
    for (int i = 0; i < FLAGS_copyset_num; i++) {
        CopysetPeerInfo ci = mc->GetCopysetinfo(FLAGS_logic_pool_id, i);
        if (i == chunkinfo1.cpid_ || i == chunkinfo2.cpid_) {
            ASSERT_NE(-1, ci.GetCurrentLeaderIndex());
            // Set LeaderMayChange for both of these leaders of the chunkserver's copysets.
            ASSERT_TRUE(ci.LeaderMayChange());
        } else {
            // For copysets without current leader information, set LeaderMayChange directly.
            ASSERT_EQ(-1, ci.GetCurrentLeaderIndex());
            ASSERT_TRUE(ci.LeaderMayChange());
        }
    }

    chunkservice[0]->ReSetRPCFailed();
    // Write to the second chunk again; after successfully obtaining a leader, LeaderMayChange will be set to false.
    // LeaderMayChange for the copyset corresponding to the first chunk remains true.
    ASSERT_EQ(8192, fileinstance_.Write(buffer, 1 * chunk_size, length));
    ASSERT_EQ(8192, fileinstance_.Read(buffer, 1 * chunk_size, length));
    for (int i = 0; i < FLAGS_copyset_num; i++) {
        CopysetPeerInfo ci = mc->GetCopysetinfo(FLAGS_logic_pool_id, i);
        if (i == chunkinfo2.cpid_) {
            ASSERT_NE(-1, ci.GetCurrentLeaderIndex());
            // Set LeaderMayChange for copyset2.
            ASSERT_FALSE(ci.LeaderMayChange());
        } else if (i == chunkinfo1.cpid_) {
            ASSERT_NE(-1, ci.GetCurrentLeaderIndex());
            // LeaderMayChange for copyset1 remains unchanged.
            ASSERT_TRUE(ci.LeaderMayChange());
        } else {
            // For copysets without current leader information, set LeaderMayChange directly.
            ASSERT_EQ(-1, ci.GetCurrentLeaderIndex());
            ASSERT_TRUE(ci.LeaderMayChange());
        }
    }

    cliservice->ReSetInvokeTimes();
    EndPoint ep2;
    butil::str2endpoint("127.0.0.1", 9152, &ep2);
    PeerId pd2(ep2);
    cliservice->SetPeerID(pd2);
    //  Force an RPC failure to trigger copyset leader switch; successful read and write after leader switch.
    chunkservice[0]->SetRPCFailed();
    // Read and write to the first and second chunks.
    ASSERT_EQ(8192, fileinstance_.Write(buffer, 0 * chunk_size, length));
    ASSERT_EQ(8192, fileinstance_.Read(buffer, 0 * chunk_size, length));
    ASSERT_EQ(8192, fileinstance_.Write(buffer, 0 * chunk_size, length));
    ASSERT_EQ(8192, fileinstance_.Read(buffer, 0 * chunk_size, length));
    ASSERT_EQ(1, cliservice->GetInvokeTimes());
    // At this point
    for (int i = 0; i < FLAGS_copyset_num; i++) {
        CopysetPeerInfo ci = mc->GetCopysetinfo(FLAGS_logic_pool_id, i);
        if (i == chunkinfo2.cpid_) {
            ASSERT_NE(-1, ci.GetCurrentLeaderIndex());
            // Set LeaderMayChange for copyset2
            ASSERT_FALSE(ci.LeaderMayChange());
        } else if (i == chunkinfo1.cpid_) {
            ASSERT_NE(-1, ci.GetCurrentLeaderIndex());
            // Set LeaderMayChange for copyset1
            ASSERT_FALSE(ci.LeaderMayChange());
        } else {
            // For the current copyset without leader information, directly set LeaderMayChange
            ASSERT_EQ(-1, ci.GetCurrentLeaderIndex());
            ASSERT_TRUE(ci.LeaderMayChange());
        }
    }

    // Verify the update of copyset ID information.
    // copyset id = 888， chunkserver id = 100 101 102
    // copyset id = 999， chunkserver id = 102 103 104
    CopysetPeerInfo csinfo1;
    PeerAddr addr;
    csinfo1.cpid_ = 888;
    curve::client::CopysetPeerInfo peer1(100, addr);
    csinfo1.csinfos_.push_back(peer1);
    curve::client::CopysetPeerInfo peer2(101, addr);
    csinfo1.csinfos_.push_back(peer2);
    curve::client::CopysetPeerInfo peer3(102, addr);
    csinfo1.csinfos_.push_back(peer3);

    CopysetPeerInfo csinfo2;
    csinfo2.cpid_ = 999;
    curve::client::CopysetPeerInfo peer4(102, addr);
    csinfo2.csinfos_.push_back(peer4);
    curve::client::CopysetPeerInfo peer5(103, addr);
    csinfo2.csinfos_.push_back(peer5);
    curve::client::CopysetPeerInfo peer6(104, addr);
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


    CopysetPeerInfo csinfo3;
    csinfo3.cpid_ = 999;
    curve::client::CopysetPeerInfo peer7(100, addr);
    csinfo3.csinfos_.push_back(peer7);
    curve::client::CopysetPeerInfo peer8(101, addr);
    csinfo3.csinfos_.push_back(peer8);
    curve::client::CopysetPeerInfo peer9(103, addr);
    csinfo3.csinfos_.push_back(peer9);

    // Update copyset information, clearing the information for chunkserver 104.
    // New copyset information has been added on chunk servers 100 and 101.
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
TEST_F(TestLibcurveInterface, InterfaceExceptionTest) {
    std::string filename = "/1_userinfo_";

    C_UserInfo_t userinfo;
    memcpy(userinfo.owner, "userinfo", 9);
    memcpy(userinfo.password, "", 1);

    // open not create file
    ASSERT_EQ(-1 * LIBCURVE_ERROR::FAILED, Open(filename.c_str(), &userinfo));

    // Set leaderid
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

    char *buffer = new char[8 * 1024];
    memset(buffer, 'a', 8 * 1024);

    CurveAioContext writeaioctx;
    writeaioctx.buf = buffer;
    writeaioctx.offset = 0;
    writeaioctx.length = 8 * 1024;
    writeaioctx.cb = writecallbacktest;

    // aiowrite not opened file
    ASSERT_EQ(-LIBCURVE_ERROR::BAD_FD, AioWrite(1234, &writeaioctx));

    // aioread not opened file
    char *readbuffer = new char[8 * 1024];
    CurveAioContext readaioctx;
    readaioctx.buf = readbuffer;
    readaioctx.offset = 0;
    readaioctx.length = 8 * 1024;
    readaioctx.cb = readcallbacktest;
    ASSERT_EQ(-1 * LIBCURVE_ERROR::BAD_FD, AioRead(1234, &readaioctx));

    uint64_t offset = 0;
    uint64_t length = 8 * 1024;

    // write not opened file
    ASSERT_EQ(-1 * LIBCURVE_ERROR::BAD_FD, Write(1234, buffer, offset, length));
    // read not opened file
    ASSERT_EQ(-1 * LIBCURVE_ERROR::BAD_FD,
              Read(1234, readbuffer, offset, length));

    delete[] buffer;
    delete[] readbuffer;
    UnInit();
    mds.UnInitialize();
}

TEST_F(TestLibcurveInterface, UnstableChunkserverTest) {
    std::string filename = "/1_userinfo_";

    UserInfo_t userinfo;
    std::shared_ptr<MDSClient> mdsclient_ = std::make_shared<MDSClient>();
    FileServiceOption fopt;
    FileInstance fileinstance_;

    FLAGS_chunkserver_list =
        "127.0.0.1:9151:0,127.0.0.1:9152:0,127.0.0.1:9153:0";

    userinfo.owner = "userinfo";
    userinfo.password = "UnstableChunkserverTest";
    fopt.metaServerOpt.rpcRetryOpt.addrs.push_back("127.0.0.1:9104");
    fopt.metaServerOpt.rpcRetryOpt.rpcTimeoutMs = 500;
    fopt.loginfo.logLevel = 0;
    fopt.ioOpt.ioSplitOpt.fileIOSplitMaxSizeKB = 64;
    fopt.ioOpt.ioSenderOpt.failRequestOpt.chunkserverRPCTimeoutMS = 1000;
    fopt.ioOpt.ioSenderOpt.failRequestOpt.chunkserverOPMaxRetry = 3;
    fopt.ioOpt.ioSenderOpt.failRequestOpt.chunkserverOPRetryIntervalUS = 500;
    fopt.ioOpt.metaCacheOpt.metacacheGetLeaderRetry = 3;
    fopt.ioOpt.metaCacheOpt.metacacheRPCRetryIntervalUS = 500;
    fopt.ioOpt.reqSchdulerOpt.scheduleQueueCapacity = 4096;
    fopt.ioOpt.reqSchdulerOpt.scheduleThreadpoolSize = 2;
    fopt.ioOpt.reqSchdulerOpt.ioSenderOpt = fopt.ioOpt.ioSenderOpt;
    fopt.leaseOpt.mdsRefreshTimesPerLease = 4;
    fopt.ioOpt.metaCacheOpt.chunkserverUnstableOption
        .maxStableChunkServerTimeoutTimes = 10;  // NOLINT

    LOG(INFO) << "fopt size " << sizeof(fopt);
    // curve::client::ClientClosure::SetFailureRequestOption(
    //     fopt.ioOpt.ioSenderOpt.failRequestOpt);
    LOG(INFO) << "here";

    mdsclient_->Initialize(fopt.metaServerOpt);
    fileinstance_.Initialize(
        "/UnstableChunkserverTest", mdsclient_, userinfo, OpenFlags{}, fopt);

    // Set leaderid
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
    int fd = fileinstance_.Open();

    MetaCache *mc = fileinstance_.GetIOManager4File()->GetMetaCache();

    ASSERT_NE(fd, -1);

    CliServiceFake *cliservice = mds.GetCliService();
    std::vector<FakeChunkService *> chunkservice = mds.GetFakeChunkService();

    char *buffer = new char[8 * 1024];
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

    // The copyset leadermaychanges that have been written in Metacache are all in a normal state
    ChunkIDInfo_t chunkinfo1;
    MetaCacheErrorType rc = mc->GetChunkInfoByIndex(0, &chunkinfo1);
    ASSERT_EQ(rc, MetaCacheErrorType::OK);
    for (int i = 0; i < FLAGS_copyset_num; i++) {
        CopysetInfo<ChunkServerID> ci =
            mc->GetCopysetinfo(FLAGS_logic_pool_id, i);
        if (i == chunkinfo1.cpid_) {
            ASSERT_NE(-1, ci.GetCurrentLeaderIndex());
            ASSERT_FALSE(ci.LeaderMayChange());
        } else {
            ASSERT_EQ(-1, ci.GetCurrentLeaderIndex());
            ASSERT_FALSE(ci.LeaderMayChange());
        }
    }

    mds.EnableNetUnstable(10000);

    // Write twice, read twice, and retry three times per request
    // Due to the delay set on the chunkserver side, each request will time out
    // The unstable threshold is 10, so when the 11th request returns, the corresponding chunkserver is marked as unstable
    // The copyset of the leader on the corresponding chunkserver will set leaderMayChange to true
    // The next time a request is made, the leader information will be refreshed first,
    // Since the leader has not changed and the delay still exists
    // So the 12th request still timed out, and leaderMayChange is still true
    ASSERT_EQ(-2, fileinstance_.Write(buffer, 1 * chunk_size, length));
    ASSERT_EQ(-2, fileinstance_.Write(buffer, 1 * chunk_size, length));
    ASSERT_EQ(-2, fileinstance_.Read(buffer, 1 * chunk_size, length));
    ASSERT_EQ(-2, fileinstance_.Read(buffer, 1 * chunk_size, length));

    // Obtain chunkid information for the second chunk
    ChunkIDInfo_t chunkinfo2;
    rc = mc->GetChunkInfoByIndex(1, &chunkinfo2);
    ASSERT_EQ(rc, MetaCacheErrorType::OK);
    ASSERT_NE(chunkinfo2.cpid_, chunkinfo1.cpid_);
    for (int i = 0; i < FLAGS_copyset_num; ++i) {
        CopysetInfo<ChunkServerID> ci =
            mc->GetCopysetinfo(FLAGS_logic_pool_id, i);
        if (i == chunkinfo1.cpid_ || i == chunkinfo2.cpid_) {
            ASSERT_NE(-1, ci.GetCurrentLeaderIndex());
            ASSERT_TRUE(ci.LeaderMayChange());
        } else {
            ASSERT_EQ(-1, ci.GetCurrentLeaderIndex());
            ASSERT_TRUE(ci.LeaderMayChange());
        }
    }

    // When copyset is in an unstable state
    // Do not enter the timeout index backoff logic, and set the rpc timeout to the default value
    // So the total time for each request is 3 seconds, and 4 requests require 12 seconds
    auto start = TimeUtility::GetTimeofDayMs();
    ASSERT_EQ(-2, fileinstance_.Write(buffer, 1 * chunk_size, length));
    ASSERT_EQ(-2, fileinstance_.Write(buffer, 1 * chunk_size, length));
    ASSERT_EQ(-2, fileinstance_.Read(buffer, 1 * chunk_size, length));
    ASSERT_EQ(-2, fileinstance_.Read(buffer, 1 * chunk_size, length));
    auto end = TimeUtility::GetTimeofDayMs();
    ASSERT_GT(end - start, 11 * 1000);
    ASSERT_LT(end - start, 13 * 1000);

    mds.DisableNetUnstable();

    // Cancel delay and read and write the second chunk again
    // After obtaining the leader information, the leaderMayChange will be set to false
    // The copyset dependency for the first chunk, leaderMayChange, is true
    ASSERT_EQ(8192, fileinstance_.Write(buffer, 1 * chunk_size, length));
    ASSERT_EQ(8192, fileinstance_.Read(buffer, 1 * chunk_size, length));
    for (int i = 0; i < FLAGS_copyset_num; ++i) {
        CopysetInfo<ChunkServerID> ci =
            mc->GetCopysetinfo(FLAGS_logic_pool_id, i);
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

    // Failed to set rcp return, forcing copyset to switch leaders. After switching leaders, read and write succeeded
    chunkservice[0]->SetRPCFailed();

    ASSERT_EQ(8192, fileinstance_.Write(buffer, 0 * chunk_size, length));
    ASSERT_EQ(8192, fileinstance_.Read(buffer, 0 * chunk_size, length));
    ASSERT_EQ(8192, fileinstance_.Write(buffer, 1 * chunk_size, length));
    ASSERT_EQ(8192, fileinstance_.Read(buffer, 1 * chunk_size, length));

    for (int i = 0; i < FLAGS_copyset_num; ++i) {
        CopysetInfo<ChunkServerID> ci =
            mc->GetCopysetinfo(FLAGS_logic_pool_id, i);
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

    fileinstance_.Close();
    fileinstance_.UnInitialize();
    mds.UnInitialize();
    delete[] buffer;
}

TEST_F(TestLibcurveInterface, ResumeTimeoutBackoff) {
    std::string filename = "/1_userinfo_";

    UserInfo_t userinfo;
    std::shared_ptr<MDSClient> mdsclient_ = std::make_shared<MDSClient>();
    FileServiceOption fopt;
    FileInstance fileinstance_;

    FLAGS_chunkserver_list =
        "127.0.0.1:9151:0,127.0.0.1:9152:0,127.0.0.1:9153:0";

    userinfo.owner = "userinfo";
    userinfo.password = "ResumeTimeoutBackoff";
    fopt.metaServerOpt.rpcRetryOpt.addrs.push_back("127.0.0.1:9104");
    fopt.metaServerOpt.rpcRetryOpt.rpcTimeoutMs = 500;
    fopt.loginfo.logLevel = 0;
    fopt.ioOpt.ioSplitOpt.fileIOSplitMaxSizeKB = 64;
    fopt.ioOpt.ioSenderOpt.failRequestOpt.chunkserverRPCTimeoutMS = 1000;
    fopt.ioOpt.ioSenderOpt.failRequestOpt.chunkserverMaxRPCTimeoutMS = 8000;
    fopt.ioOpt.ioSenderOpt.failRequestOpt.chunkserverOPMaxRetry = 11;
    fopt.ioOpt.ioSenderOpt.failRequestOpt.chunkserverOPRetryIntervalUS = 500;
    fopt.ioOpt.metaCacheOpt.metacacheGetLeaderRetry = 3;
    fopt.ioOpt.metaCacheOpt.metacacheRPCRetryIntervalUS = 500;
    fopt.ioOpt.reqSchdulerOpt.scheduleQueueCapacity = 4096;
    fopt.ioOpt.reqSchdulerOpt.scheduleThreadpoolSize = 2;
    fopt.ioOpt.reqSchdulerOpt.ioSenderOpt = fopt.ioOpt.ioSenderOpt;
    fopt.leaseOpt.mdsRefreshTimesPerLease = 4;
    fopt.ioOpt.metaCacheOpt.chunkserverUnstableOption
        .maxStableChunkServerTimeoutTimes = 10;  // NOLINT

    mdsclient_->Initialize(fopt.metaServerOpt);
    fileinstance_.Initialize("/ResumeTimeoutBackoff", mdsclient_, userinfo,
                             OpenFlags{}, fopt);

    // Set leaderid
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
    int fd = fileinstance_.Open();

    MetaCache *mc = fileinstance_.GetIOManager4File()->GetMetaCache();

    ASSERT_NE(fd, -1);

    std::vector<FakeChunkService *> chunkservice = mds.GetFakeChunkService();

    char *buffer = new char[8 * 1024];
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

    // The copyset leadermaychanges that have been written in Metacache are all in a normal state
    ChunkIDInfo_t chunkinfo1;
    MetaCacheErrorType rc = mc->GetChunkInfoByIndex(0, &chunkinfo1);
    ASSERT_EQ(rc, MetaCacheErrorType::OK);
    for (int i = 0; i < FLAGS_copyset_num; i++) {
        CopysetInfo<ChunkServerID> ci =
            mc->GetCopysetinfo(FLAGS_logic_pool_id, i);
        if (i == chunkinfo1.cpid_) {
            ASSERT_NE(-1, ci.GetCurrentLeaderIndex());
            ASSERT_FALSE(ci.LeaderMayChange());
        } else {
            ASSERT_EQ(-1, ci.GetCurrentLeaderIndex());
            ASSERT_FALSE(ci.LeaderMayChange());
        }
    }

    mds.EnableNetUnstable(10000);

    // Write twice, retry 11 times per request
    // Due to the delay set on the chunkserver side, each request will time out
    // The first request will be retried 11 times and the chunkserver will be marked as unstable
    ASSERT_EQ(-2, fileinstance_.Write(buffer, 1 * chunk_size, length));

    // The second write request, due to its corresponding copyset leader may change
    // The first request timeout is 1 second
    // The timeout for the next four retries is also 1 second due to the leader may change
    // 5th to 11th requests due to more than minRetryTimesForceTimeoutBackoff retries
    // So all timeout times enter exponential backoff, which is 8s * 6 = 48s
    // So the second write request took a total of 53 seconds and failed to write
    auto start = TimeUtility::GetTimeofDayMs();
    ASSERT_EQ(-2, fileinstance_.Write(buffer, 1 * chunk_size, length));
    auto elapsedMs = TimeUtility::GetTimeofDayMs() - start;
    ASSERT_GE(elapsedMs, 52 * 1000);
    ASSERT_LE(elapsedMs, 55 * 1000);

    fileinstance_.Close();
    fileinstance_.UnInitialize();
    mds.UnInitialize();
    delete[] buffer;
}

TEST_F(TestLibcurveInterface, InterfaceStripeTest) {
    FLAGS_chunkserver_list =
        "127.0.0.1:9115:0,127.0.0.1:9116:0,127.0.0.1:9117:0";

    std::string filename = "/1";
    std::string filename2 = "/2";
    UserInfo_t userinfo;
    userinfo.owner = "userinfo";
    uint64_t size = 100 * 1024 * 1024 * 1024ul;
    FileClient fc;

    // Set leaderid
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

    FakeMDSCurveFSService *service = NULL;
    service = mds.GetMDSService();
    ::curve::mds::CreateFileResponse response;
    response.set_statuscode(::curve::mds::StatusCode::kOK);
    FakeReturn *fakeret =
        new FakeReturn(nullptr, static_cast<void *>(&response));
    service->SetCreateFileFakeReturn(fakeret);
    CreateFileContext context;
    context.pagefile = true;
    context.name = filename;
    context.user = userinfo;
    context.length = size;
    int ret = fc.Create2(context);
    ASSERT_EQ(LIBCURVE_ERROR::OK, ret);

    response.set_statuscode(::curve::mds::StatusCode::kFileExists);
    fakeret = new FakeReturn(nullptr, static_cast<void *>(&response));
    service->SetCreateFileFakeReturn(fakeret);
    context.pagefile = true;
    context.name = filename2;
    context.user = userinfo;
    context.length = size;
    context.stripeUnit = 1024 * 1024;
    context.stripeCount = 4;
    ret = fc.Create2(context);
    ASSERT_EQ(LIBCURVE_ERROR::EXISTS, -ret);

    FileStatInfo_t fsinfo;
    ::curve::mds::FileInfo *info = new curve::mds::FileInfo;
    ::curve::mds::GetFileInfoResponse getinforesponse;
    info->set_filename(filename2);
    info->set_id(1);
    info->set_parentid(0);
    info->set_filetype(curve::mds::FileType::INODE_PAGEFILE);
    info->set_chunksize(4 * 1024 * 1024);
    info->set_length(4 * 1024 * 1024 * 1024ul);
    info->set_ctime(12345678);
    info->set_segmentsize(1 * 1024 * 1024 * 1024ul);
    info->set_stripeunit(1024 * 1024);
    info->set_stripecount(4);
    getinforesponse.set_allocated_fileinfo(info);
    getinforesponse.set_statuscode(::curve::mds::StatusCode::kOK);
    FakeReturn *fakegetinfo =
        new FakeReturn(nullptr, static_cast<void *>(&getinforesponse));
    service->SetGetFileInfoFakeReturn(fakegetinfo);
    ret = fc.StatFile(filename2, userinfo, &fsinfo);
    ASSERT_EQ(1024 * 1024, fsinfo.stripeUnit);
    ASSERT_EQ(4, fsinfo.stripeCount);
    mds.UnInitialize();
    fc.UnInit();
}

}  // namespace client
}  // namespace curve
