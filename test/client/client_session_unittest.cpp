/*
 * Project: curve
 * File Created: Monday, 24th December 2018 10:41:03 am
 * Author: tongguangxun
 * Copyright (c)￼ 2018 netease
 */

#include <gtest/gtest.h>
#include <gmock/gmock.h>
#include <gflags/gflags.h>
#include <glog/logging.h>
#include <brpc/server.h>
#include <sys/types.h>
#include <unistd.h>
#include <sys/stat.h>
#include <fcntl.h>


#include <mutex>    // NOLINT
#include <chrono>   // NOLINT
#include <atomic>
#include <functional>
#include <condition_variable>    // NOLINT
#include <string>

#include "src/client/client_config.h"
#include "test/client/fake/fakeMDS.h"
#include "src/client/file_instance.h"
#include "src/client/timertask_worker.h"
#include "src/client/iomanager4file.h"
#include "src/client/libcurve_file.h"
#include "test/client/fake/fakeChunkserver.h"
#include "test/integration/cluster_common/cluster.h"
#include "test/util/config_generator.h"

extern std::string mdsMetaServerAddr;
extern std::string configpath;
DECLARE_string(chunkserver_list);

using curve::client::MDSClient;
using curve::client::UserInfo_t;
using curve::client::ClientConfig;
using curve::client::FileClient;
using curve::client::FileInstance;
using curve::client::TimerTask;
using curve::client::TimerTaskWorker;
using curve::client::FileMetric_t;

#define SLEEP_TIME_S 5

uint64_t ioSleepTime = 0;

bool sessionFlag = false;
std::mutex sessionMtx;
std::condition_variable sessionCV;

void sessioncallback(CurveAioContext* aioctx) {
    uint64_t ioEndTime = TimeUtility::GetTimeofDayUs();

    ASSERT_GT(ioEndTime - ioSleepTime, SLEEP_TIME_S * 1000000);

    sessionFlag = true;
}

TEST(TimerTaskWorkerTest, TimerTaskWorkerRunTaskTest) {
    // test timer worker run task follow task lease
    TimerTaskWorker timerworker;
    timerworker.Start();
    timerworker.Start();

    // task run in sequence by lease
    std::mutex mtx;
    std::condition_variable cv;
    std::atomic<uint64_t> testnum1(0);
    auto tt1 = [&testnum1, &cv]() {
        testnum1.fetch_add(1);
        cv.notify_all();
    };
    TimerTask task1(1000000);
    task1.AddCallback(tt1);
    ASSERT_TRUE(timerworker.AddTimerTask(&task1));
    ASSERT_FALSE(timerworker.AddTimerTask(&task1));

    std::atomic<uint64_t> testnum2(0);
    auto tt2 = [&testnum2, &cv]() {
        testnum2.fetch_add(1);
        cv.notify_all();
    };
    TimerTask task2(2000000);
    task2.AddCallback(tt2);
    ASSERT_TRUE(timerworker.AddTimerTask(&task2));
    ASSERT_FALSE(timerworker.AddTimerTask(&task2));

    task1.SetDeleteSelf();

    std::unique_lock<std::mutex> lk(mtx);
    cv.wait(lk, [&testnum1, &testnum2]()->bool{
        return testnum2.load() || testnum1.load();
    });

    ASSERT_EQ(1, testnum1.load());
    ASSERT_NE(1, testnum2.load());

    cv.wait(lk, [&testnum1, &testnum2]()->bool{
        return testnum2.load() >= 2;
    });

    ASSERT_TRUE(timerworker.CancelTimerTask(&task2));
    ASSERT_FALSE(timerworker.CancelTimerTask(&task1));

    TimerTask task3(2000000);
    ASSERT_FALSE(timerworker.CancelTimerTask(&task3));

    timerworker.Stop();
}

TEST(ClientSession, LeaseTaskTest) {
    FLAGS_chunkserver_list =
             "127.0.0.1:9176:0,127.0.0.1:9177:0,127.0.0.1:9178:0";

    std::string filename = "/1";

    /*** init mds service ***/
    FakeMDS mds(filename);
    mds.Initialize();
    mds.StartService();
    // 设置leaderid
    curve::client::EndPoint ep;
    butil::str2endpoint("127.0.0.1", 9176, &ep);
    PeerId pd(ep);
    mds.StartCliService(pd);
    mds.CreateCopysetNode(true);
    ClientConfig cc;
    cc.Init(configpath.c_str());

    FileInstance fileinstance;

    UserInfo_t userinfo;
    userinfo.owner = "userinfo";

    MDSClient mdsclient;
    mdsclient.Initialize(cc.GetFileServiceOption().metaServerOpt);
    ASSERT_TRUE(fileinstance.Initialize(filename, &mdsclient, userinfo,
                                        cc.GetFileServiceOption()));

    brpc::Server server;
    FakeMDSCurveFSService* curvefsservice = mds.GetMDSService();

    // set openfile response
    ::curve::mds::OpenFileResponse openresponse;
    curve::mds::FileInfo * finfo = new curve::mds::FileInfo;
    ::curve::mds::ProtoSession* se = new ::curve::mds::ProtoSession;
    se->set_sessionid("1");
    se->set_createtime(12345);
    se->set_leasetime(10000000);
    se->set_sessionstatus(::curve::mds::SessionStatus::kSessionOK);

    finfo->set_filename(filename);
    openresponse.set_statuscode(::curve::mds::StatusCode::kOK);
    openresponse.set_allocated_protosession(se);
    openresponse.set_allocated_fileinfo(finfo);

    FakeReturn* openfakeret
     = new FakeReturn(nullptr, static_cast<void*>(&openresponse));
    curvefsservice->SetOpenFile(openfakeret);

    // 2. set refresh response
    std::mutex mtx;
    std::condition_variable refreshcv;
    auto refresht = [&mtx, &refreshcv]() {
        LOG(INFO) << "get refresh session request!";
        std::unique_lock<std::mutex> lk(mtx);
        refreshcv.notify_one();
    };
    curve::mds::FileInfo * info = new curve::mds::FileInfo;
    info->set_filename(filename);
    info->set_seqnum(2);
    info->set_id(1);
    info->set_parentid(0);
    info->set_filetype(curve::mds::FileType::INODE_PAGEFILE);
    info->set_chunksize(4 * 1024 * 1024);
    info->set_length(4 * 1024 * 1024 * 1024ul);
    info->set_ctime(12345678);

    ::curve::mds::ReFreshSessionResponse refreshresp;
    refreshresp.set_statuscode(::curve::mds::StatusCode::kOK);
    refreshresp.set_sessionid("1234");
    refreshresp.set_allocated_fileinfo(info);
    FakeReturn* refreshfakeret
    = new FakeReturn(nullptr, static_cast<void*>(&refreshresp));
    curvefsservice->SetRefreshSession(refreshfakeret, refresht);

    // 3. open the file
    int openret = fileinstance.Open(filename, userinfo);
    ASSERT_EQ(openret, LIBCURVE_ERROR::OK);

    // 4. wait for refresh
    for (int i = 0; i < 4; i++) {
        {
            std::unique_lock<std::mutex> lk(mtx);
            refreshcv.wait(lk);
        }
    }

    auto iomanager = fileinstance.GetIOManager4File();

    // 5. set refresh failed
    // 当lease续约失败的时候，会将IO停住，这时候设置LeaseValid为false
    refreshresp.set_statuscode(::curve::mds::StatusCode::KInternalError);
    FakeReturn* refreshfakeretnotexits
     = new FakeReturn(nullptr, static_cast<void*>(&refreshresp));
    curvefsservice->SetRefreshSession(refreshfakeretnotexits, refresht);

    for (int i = 0; i < 5; i++) {
        {
            std::unique_lock<std::mutex> lk(mtx);
            refreshcv.wait(lk);
        }
    }

    curve::client::LeaseExcutor* lease = fileinstance.GetLeaseExcutor();
    ASSERT_FALSE(lease->LeaseValid());

    // 6. set refresh success
    // 如果lease续约失败后又重新续约成功了，这时候Lease是可用的了，leasevalid为true
    // 这时候IO被恢复了。
    refreshresp.set_statuscode(::curve::mds::StatusCode::kOK);
    FakeReturn* refreshfakeretOK
     = new FakeReturn(nullptr, static_cast<void*>(&refreshresp));
    curvefsservice->SetRefreshSession(refreshfakeretOK, refresht);

    for (int i = 0; i < 2; i++) {
        {
            std::unique_lock<std::mutex> lk(mtx);
            refreshcv.wait(lk);
        }
    }
    ASSERT_TRUE(lease->LeaseValid());

    // 7. set refresh failed
    // 续约失败，IO都是直接返回-LIBCURVE_ERROR::DISABLEIO
    refreshresp.set_statuscode(::curve::mds::StatusCode::KInternalError);
    FakeReturn* refreshfakeretfail
     = new FakeReturn(nullptr, static_cast<void*>(&refreshresp));
    curvefsservice->SetRefreshSession(refreshfakeretfail, refresht);

    for (int i = 0; i < 5; i++) {
        {
            std::unique_lock<std::mutex> lk(mtx);
            refreshcv.wait(lk);
        }
    }

    char* buf2 = new char[8 * 1024];
    CurveAioContext aioctx;
    aioctx.offset = 0;
    aioctx.length = 4 * 1024;
    aioctx.ret = LIBCURVE_ERROR::OK;
    aioctx.cb = sessioncallback;
    aioctx.buf = buf2;

    ioSleepTime = TimeUtility::GetTimeofDayUs();

    ASSERT_EQ(0, fileinstance.AioRead(&aioctx));

    std::this_thread::sleep_for(std::chrono::seconds(SLEEP_TIME_S));

    // 8. set refresh success
    // 如果lease续约失败后又重新续约成功了，这时候Lease是可用的了，leasevalid为true
    // 这时候IO被恢复了。
    refreshresp.set_statuscode(::curve::mds::StatusCode::kOK);
    FakeReturn* refreshfakeretOK1
     = new FakeReturn(nullptr, static_cast<void*>(&refreshresp));
    curvefsservice->SetRefreshSession(refreshfakeretOK1, refresht);

    for (int i = 0; i < 3; i++) {
        {
            std::unique_lock<std::mutex> lk(mtx);
            refreshcv.wait(lk);
        }
    }
    ASSERT_TRUE(lease->LeaseValid());

    // 9. set refresh AuthFail
    refreshresp.set_statuscode(::curve::mds::kOwnerAuthFail);
    FakeReturn* refreshFakeRetAuthFail =
        new FakeReturn(nullptr, static_cast<void*>(&refreshresp));
    curvefsservice->SetRefreshSession(refreshFakeRetAuthFail, refresht);

    for (int i = 0; i < 5; i++) {
        {
            std::unique_lock<std::mutex> lk(mtx);
            refreshcv.wait(lk);
        }
    }

    char* buf3 = new char[8 * 1024];
    CurveAioContext aioctx2;
    aioctx2.offset = 0;
    aioctx2.length = 4 * 1024;
    aioctx2.ret = LIBCURVE_ERROR::OK;
    aioctx2.cb = sessioncallback;
    aioctx2.buf = buf3;

    ioSleepTime = TimeUtility::GetTimeofDayUs();

    ASSERT_EQ(0, fileinstance.AioRead(&aioctx2));

    std::this_thread::sleep_for(std::chrono::seconds(SLEEP_TIME_S));

    // 10. set refresh success
    refreshresp.set_statuscode(::curve::mds::StatusCode::kOK);
    FakeReturn* refreshfakeretOK2 =
        new FakeReturn(nullptr, static_cast<void*>(&refreshresp));
    curvefsservice->SetRefreshSession(refreshfakeretOK2, refresht);

    for (int i = 0; i < 2; i++) {
        {
            std::unique_lock<std::mutex> lk(mtx);
            refreshcv.wait(lk);
        }
    }
    ASSERT_TRUE(lease->LeaseValid());

    // 11. set refresh kFileNotExists
    auto timerTask = fileinstance.GetLeaseExcutor()->GetTimerTask();
    refreshresp.set_statuscode(::curve::mds::kFileNotExists);
    FakeReturn* refreshFakeRetFileNotExists =
        new FakeReturn(nullptr, static_cast<void*>(&refreshresp));
    curvefsservice->SetRefreshSession(refreshFakeRetFileNotExists, refresht);

    {
        std::unique_lock<std::mutex> lk(mtx);
        refreshcv.wait(lk);
    }

    std::this_thread::sleep_for(std::chrono::seconds(1));
    lease = fileinstance.GetLeaseExcutor();
    ASSERT_FALSE(lease->LeaseValid());

    // 11. set refresh success
    refreshresp.set_statuscode(::curve::mds::StatusCode::kOK);
    FakeReturn* refreshfakeretOK3 =
        new FakeReturn(nullptr, static_cast<void*>(&refreshresp));
    curvefsservice->SetRefreshSession(refreshfakeretOK3, refresht);
    timerTask->ClearDeleteSelf();
    fileinstance.GetLeaseExcutor()->SetTimerTask(timerTask);

    for (int i = 0; i < 2; i++) {
        {
            std::unique_lock<std::mutex> lk(mtx);
            refreshcv.wait(lk);
        }
    }
    ASSERT_TRUE(lease->LeaseValid());

    // 12. set refresh success
    refreshresp.set_statuscode(::curve::mds::StatusCode::kOK);
    FakeReturn* refreshfakeretOK4 =
        new FakeReturn(nullptr, static_cast<void*>(&refreshresp));
    curvefsservice->SetRefreshSession(refreshfakeretOK4, refresht);
    timerTask->ClearDeleteSelf();
    fileinstance.GetLeaseExcutor()->SetTimerTask(timerTask);

    for (int i = 0; i < 2; i++) {
        {
            std::unique_lock<std::mutex> lk(mtx);
            refreshcv.wait(lk);
        }
    }
    ASSERT_TRUE(lease->LeaseValid());

    std::unique_lock<std::mutex> lk(sessionMtx);
    sessionCV.wait(lk, [&]() { return sessionFlag; });

    // 13. set fake close return
    ::curve::mds::CloseFileResponse closeresp;
    closeresp.set_statuscode(::curve::mds::StatusCode::kOK);
    FakeReturn* closefileret
     = new FakeReturn(nullptr, static_cast<void*>(&closeresp));
    curvefsservice->SetCloseFile(closefileret);

    // 14. set refresh success
    // 如果lease续约失败后又重新续约成功了，这时候Lease是可用的了，leasevalid为true
    // 这时候IO被恢复了。
    sessionFlag = false;
    brpc::Controller* cntl = new brpc::Controller;
    cntl->SetFailed(1, "set failed!");
    refreshresp.set_statuscode(::curve::mds::StatusCode::kOK);
    FakeReturn* refreshfakeretfailed
     = new FakeReturn(cntl, static_cast<void*>(&refreshresp));
    curvefsservice->SetRefreshSession(refreshfakeretfailed, refresht);

    curvefsservice->CleanRetryTimes();


    brpc::Server server2;
    FakeMDSCurveFSService curvefsservice2;
    server2.AddService(&curvefsservice2, brpc::SERVER_DOESNT_OWN_SERVICE);

    brpc::ServerOptions options;
    options.idle_timeout_sec = -1;
    if (server2.Start("127.0.0.1:9102", &options) != 0) {
        LOG(ERROR) << "Fail to start Server";
    }

    brpc::Controller* cntl2 = new brpc::Controller;
    cntl2->SetFailed(1, "set failed!");
    ::curve::mds::ReFreshSessionResponse refreshresp2;
    refreshresp2.set_statuscode(::curve::mds::StatusCode::kOK);
    FakeReturn* refreshfakeretfailed2
     = new FakeReturn(cntl2, static_cast<void*>(&refreshresp2));
    curvefsservice2.SetRefreshSession(refreshfakeretfailed2, refresht);

    curvefsservice2.CleanRetryTimes();

    ASSERT_TRUE(lease->LeaseValid());
    std::this_thread::sleep_for(std::chrono::seconds(25));
    ASSERT_FALSE(lease->LeaseValid());

    fileinstance.UnInitialize();
    server.Stop(0);
    server.Join();
    server2.Stop(0);
    server2.Join();
    mds.UnInitialize();
}

TEST(ClientSession, AppliedIndexTest) {
    ClientConfig cc;
    cc.Init(configpath.c_str());
    FileInstance fileinstance;
    UserInfo_t userinfo;
    userinfo.owner = "userinfo";

    MDSClient mdsclient;
    mdsclient.Initialize(cc.GetFileServiceOption().metaServerOpt);
    ASSERT_TRUE(fileinstance.Initialize("/test", &mdsclient, userinfo,
                                        cc.GetFileServiceOption()));

    // create fake chunkserver service
    FakeChunkServerService fakechunkservice;
    // 设置cli服务
    CliServiceFake fakeCliservice;

    brpc::Server server;
    if (server.AddService(&fakechunkservice,
                          brpc::SERVER_DOESNT_OWN_SERVICE) != 0) {
        LOG(FATAL) << "Fail to add service";
    }
    if (server.AddService(&fakeCliservice,
                          brpc::SERVER_DOESNT_OWN_SERVICE) != 0) {
        LOG(FATAL) << "Fail to add service";
    }

    brpc::ServerOptions options;
    options.idle_timeout_sec = -1;
    ASSERT_EQ(server.Start("127.0.0.1:9102", &options), 0);

    // fill metacache
    curve::client::MetaCache* mc
        = fileinstance.GetIOManager4File()->GetMetaCache();
    curve::client::ChunkIDInfo_t chunkinfo(1, 2, 3);
    mc->UpdateChunkInfoByIndex(0, chunkinfo);
    curve::client::CopysetInfo cpinfo;
    curve::client::EndPoint ep;
    butil::str2endpoint("127.0.0.1", 9102, &ep);

    braft::PeerId pd(ep);
    curve::client::CopysetPeerInfo
        peer(1, curve::client::ChunkServerAddr(ep));
    cpinfo.csinfos_.push_back(peer);
    mc->UpdateCopysetInfo(2, 3, cpinfo);

    fakeCliservice.SetPeerID(pd);

    // 1. first write with applied index = 0 return
    // create fake return
    // first write, and set the applied index = 0
    ::curve::chunkserver::ChunkResponse response;
    response.set_status(::curve::chunkserver::CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS);    // NOLINT
    response.set_appliedindex(0);
    FakeReturn* writeret = new FakeReturn(nullptr, static_cast<void*>(&response));   // NOLINT
    fakechunkservice.SetFakeWriteReturn(writeret);

    // send write request
    // curve::client::IOManager4File* ioctx = fileinstance.GetIOCtxManager();
    char buffer[8192] = {0};
    fileinstance.Write(buffer, 0, 8192);

    // create fake read return
    ::curve::chunkserver::ChunkResponse readresponse;
    readresponse.set_status(::curve::chunkserver::CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS);    // NOLINT
    readresponse.set_appliedindex(0);
    FakeReturn* readret = new FakeReturn(nullptr, static_cast<void*>(&readresponse));   // NOLINT
    fakechunkservice.SetFakeReadReturn(readret);

    // send read request
    fileinstance.Read(buffer, 0, 8192);

    // verify buffer content
    for (int i = 0; i < 4096; i++) {
        ASSERT_EQ(buffer[i], 'c');
        ASSERT_EQ(buffer[i + 4096], 'd');
    }

    // 2. second write with appliedindex = 1 return. then read with appliedindex
    //    and with applied index = 0 return.
    // create fake return
    // first write, and set the applied index = 0
    response.set_status(::curve::chunkserver::CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS);    // NOLINT
    response.set_appliedindex(1);
    FakeReturn* writeret2 = new FakeReturn(nullptr, static_cast<void*>(&response));   // NOLINT
    fakechunkservice.SetFakeWriteReturn(writeret2);

    // send write request
    fileinstance.Write(buffer, 0, 8192);

    // create fake read return
    ::curve::chunkserver::ChunkResponse readresponse2;
    readresponse2.set_status(::curve::chunkserver::CHUNK_OP_STATUS::CHUNK_OP_STATUS_CHUNK_NOTEXIST);    // NOLINT
    readresponse2.set_appliedindex(0);
    FakeReturn* readret2 = new FakeReturn(nullptr, static_cast<void*>(&readresponse2));   // NOLINT
    fakechunkservice.SetFakeReadReturn(readret2);

    // send read request
    memset(buffer, 1, 8192);
    fileinstance.Read(buffer, 0, 8192);

    // verify buffer content
    for (int i = 0; i < 4096; i++) {
        // chunk not exit, so the data will be set to 0.
        ASSERT_EQ(buffer[i], 0);
        ASSERT_EQ(buffer[i + 4096], 0);
    }

    ::curve::chunkserver::ChunkResponse readresponse3;
    readresponse3.set_status(::curve::chunkserver::CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS);    // NOLINT
    readresponse3.set_appliedindex(0);
    FakeReturn* readret3 = new FakeReturn(nullptr, static_cast<void*>(&readresponse3));   // NOLINT
    fakechunkservice.SetFakeReadReturn(readret3);

    // send read request with applied index = 0
    fileinstance.Read(buffer, 0, 8192);

    // verify buffer content
    for (int i = 0; i < 4096; i++) {
        ASSERT_EQ(buffer[i], 'c');
        ASSERT_EQ(buffer[i + 4096], 'd');
    }

    fileinstance.UnInitialize();

    delete writeret;
    delete writeret2;
    delete readret3;
    delete readret2;
    delete readret;
}

std::string mdsMetaServerAddr = "127.0.0.1:9101";     // NOLINT
uint32_t segment_size = 1 * 1024 * 1024 * 1024ul;   // NOLINT
uint32_t chunk_size = 4 * 1024 * 1024;   // NOLINT
std::string configpath = "./test/client/testConfig/client_session.conf";   // NOLINT

const std::vector<std::string> clientConf {
    std::string("mds.listen.addr=127.0.0.1:9101,127.0.0.1:9102"),
    std::string("global.logPath=./runlog/"),
    std::string("chunkserver.rpcTimeoutMS=1000"),
    std::string("chunkserver.opMaxRetry=3"),
    std::string("metacache.getLeaderRetry=3"),
    std::string("metacache.getLeaderTimeOutMS=1000"),
    std::string("global.fileMaxInFlightRPCNum=2048"),
    std::string("metacache.rpcRetryIntervalUS=500"),
    std::string("mds.rpcRetryIntervalUS=500"),
    std::string("schedule.threadpoolSize=2"),
    std::string("mds.maxRetryMS=5000")
};

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
