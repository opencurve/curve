/*
 * Project: curve
 * File Created: Monday, 24th December 2018 10:41:03 am
 * Author: tongguangxun
 * Copyright (c)￼ 2018 netease
 */

#include <gtest/gtest.h>
#include <gflags/gflags.h>
#include <glog/logging.h>
#include <brpc/server.h>

#include <mutex>    // NOLINT
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
#include "include/client/libcurve_qemu.h"
#include "test/client/fake/fakeChunkserver.h"

extern std::string metaserver_addr;
extern std::string configpath;

using curve::client::UserInfo;
using curve::client::ClientConfig;
using curve::client::FileClient;
using curve::client::FileInstance;
using curve::client::TimerTask;
using curve::client::TimerTaskWorker;

void sessioncallback(CurveAioContext* aioctx) {
    ASSERT_EQ(LIBCURVE_ERROR::DISABLEIO, aioctx->err);
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
        return testnum2.load() == 2;
    });

    ASSERT_TRUE(timerworker.CancelTimerTask(&task2));
    ASSERT_FALSE(timerworker.CancelTimerTask(&task1));

    TimerTask task3(2000000);
    ASSERT_FALSE(timerworker.CancelTimerTask(&task3));

    timerworker.Stop();
}

TEST(ClientSession, LeaseTaskTest) {
    std::string filename = "_filename_";
    std::string fullpathfilename = "/1_userinfo_";

    ClientConfig cc;
    cc.Init(configpath.c_str());

    FileInstance fileinstance;
    UserInfo userinfo("userinfo", "");
    ASSERT_TRUE(fileinstance.Initialize(userinfo,
                                        cc.GetFileServiceOption()));

    brpc::Server server;
    FakeMDSCurveFSService curvefsservice;

    // set openfile response
    ::curve::mds::OpenFileResponse openresponse;
    curve::mds::FileInfo * finfo = new curve::mds::FileInfo;
    ::curve::mds::ProtoSession* se = new ::curve::mds::ProtoSession;
    se->set_sessionid("1");
    se->set_token("token");
    se->set_createtime(12345);
    se->set_leasetime(10000000);
    se->set_sessionstatus(::curve::mds::SessionStatus::kSessionOK);

    finfo->set_filename(filename);
    finfo->set_fullpathname(fullpathfilename);
    openresponse.set_statuscode(::curve::mds::StatusCode::kOK);
    openresponse.set_allocated_protosession(se);
    openresponse.set_allocated_fileinfo(finfo);

    FakeReturn* openfakeret
     = new FakeReturn(nullptr, static_cast<void*>(&openresponse));
    curvefsservice.SetOpenFile(openfakeret);

    // 1. start service
    if (server.AddService(&curvefsservice,
                          brpc::SERVER_DOESNT_OWN_SERVICE) != 0) {
        LOG(FATAL) << "Fail to add service";
    }

    brpc::ServerOptions options;
    options.idle_timeout_sec = -1;
    ASSERT_EQ(server.Start(metaserver_addr.c_str(), &options), 0);

    // 2. set refresh response
    std::mutex mtx;
    std::condition_variable refreshcv;
    auto refresht = [&mtx, &refreshcv]() {
        LOG(INFO) << "get refresh session request!";
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
    info->set_fullpathname(fullpathfilename);

    ::curve::mds::ReFreshSessionResponse refreshresp;
    refreshresp.set_statuscode(::curve::mds::StatusCode::kOK);
    refreshresp.set_sessionid("1234");
    refreshresp.set_allocated_fileinfo(info);
    FakeReturn* refreshfakeret
    = new FakeReturn(nullptr, static_cast<void*>(&refreshresp));
    curvefsservice.SetRefreshSession(refreshfakeret, refresht);

    // 3. open the file
    LIBCURVE_ERROR openret = fileinstance.Open(filename, 0, false);
    ASSERT_EQ(openret, LIBCURVE_ERROR::OK);

    // 4. wait for refresh
    for (int i = 0; i < 4; i++) {
        {
            std::unique_lock<std::mutex> lk(mtx);
            refreshcv.wait(lk);
        }
    }

    // 5. set refresh failed
    refreshresp.set_statuscode(::curve::mds::StatusCode::KInternalError);
    FakeReturn* refreshfakeretnotexits
     = new FakeReturn(nullptr, static_cast<void*>(&refreshresp));
    curvefsservice.SetRefreshSession(refreshfakeretnotexits, refresht);

    for (int i = 0; i < 5; i++) {
        {
            std::unique_lock<std::mutex> lk(mtx);
            refreshcv.wait(lk);
        }
    }

    // 6. set fake close return
    ::curve::mds::CloseFileResponse closeresp;
    closeresp.set_statuscode(::curve::mds::StatusCode::kOK);
    FakeReturn* closefileret
     = new FakeReturn(nullptr, static_cast<void*>(&closeresp));
    curvefsservice.SetCloseFile(closefileret);

    // 7. set refresh ret = kSessionNotExist
    refreshresp.set_statuscode(::curve::mds::StatusCode::kSessionNotExist);
    FakeReturn* refreshSessionNotExist
     = new FakeReturn(nullptr, static_cast<void*>(&refreshresp));
    curvefsservice.SetRefreshSession(refreshSessionNotExist, refresht);

    for (int i = 0; i < 1; i++) {
        {
            std::unique_lock<std::mutex> lk(mtx);
            refreshcv.wait(lk);
        }
    }

    CurveAioContext aioctx;
    aioctx.offset = 4 * 1024 * 1024 - 4 * 1024;
    aioctx.length = 4 * 1024 * 1024 + 8 * 1024;
    aioctx.ret = 0;
    aioctx.err = LIBCURVE_ERROR::UNKNOWN;
    aioctx.cb = sessioncallback;
    aioctx.buf = nullptr;

    fileinstance.AioRead(&aioctx);
    fileinstance.AioWrite(&aioctx);

    char buffer[10];
    ASSERT_EQ(LIBCURVE_ERROR::DISABLEIO, fileinstance.Write(buffer, 0, 0));
    ASSERT_EQ(LIBCURVE_ERROR::DISABLEIO, fileinstance.Read(buffer, 0, 0));

    ASSERT_NE(-1, fileinstance.Close());

    fileinstance.UnInitialize();
    server.Stop(0);
    server.Join();
}

TEST(ClientSession, AppliedIndexTest) {
    ClientConfig cc;
    cc.Init(configpath.c_str());
    FileInstance fileinstance;
    UserInfo userinfo("userinfo", "");
    ASSERT_TRUE(fileinstance.Initialize(userinfo,
                                        cc.GetFileServiceOption()));

    // create fake chunkserver service
    FakeChunkServerService fakechunkservice;
    brpc::Server server;
    if (server.AddService(&fakechunkservice,
                          brpc::SERVER_DOESNT_OWN_SERVICE) != 0) {
        LOG(FATAL) << "Fail to add service";
    }

    brpc::ServerOptions options;
    options.idle_timeout_sec = -1;
    ASSERT_EQ(server.Start("127.0.0.1:5555", &options), 0);

    // fill metacache
    curve::client::MetaCache* mc
        = fileinstance.GetIOManager4File()->GetMetaCache();
    curve::client::ChunkIDInfo_t chunkinfo(1, 2, 3);
    mc->UpdateChunkInfoByIndex(0, chunkinfo);
    curve::client::CopysetInfo cpinfo;
    curve::client::EndPoint ep;
    butil::str2endpoint("127.0.0.1", 5555, &ep);
    curve::client::PeerId pd(ep);
    curve::client::CopysetPeerInfo peer(1, pd);
    cpinfo.csinfos_.push_back(peer);
    mc->UpdateCopysetInfo(2, 3, cpinfo);

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
