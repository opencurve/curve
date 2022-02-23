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
 * File Created: Monday, 24th December 2018 10:41:03 am
 * Author: tongguangxun
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
#include "src/client/iomanager4file.h"
#include "src/client/libcurve_file.h"
#include "test/client/fake/fakeChunkserver.h"
#include "test/integration/cluster_common/cluster.h"
#include "test/util/config_generator.h"

extern std::string mdsMetaServerAddr;
extern std::string configpath;
DECLARE_string(chunkserver_list);

namespace curve {
namespace client {

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

    std::shared_ptr<MDSClient> mdsclient = std::make_shared<MDSClient>();
    mdsclient->Initialize(cc.GetFileServiceOption().metaServerOpt);
    ASSERT_TRUE(fileinstance.Initialize(
        filename, mdsclient, userinfo, OpenFlags{}, cc.GetFileServiceOption()));

    brpc::Server server;
    FakeMDSCurveFSService* curvefsservice = mds.GetMDSService();

    // set openfile response
    ::curve::mds::OpenFileResponse openresponse;
    curve::mds::FileInfo * finfo = new curve::mds::FileInfo;
    ::curve::mds::ProtoSession* se = new ::curve::mds::ProtoSession;
    se->set_sessionid("1");
    se->set_createtime(12345);
    se->set_leasetime(100000);
    se->set_sessionstatus(::curve::mds::SessionStatus::kSessionOK);

    finfo->set_filename(filename);
    finfo->set_id(1);
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
    int openret = fileinstance.Open();
    ASSERT_EQ(openret, LIBCURVE_ERROR::OK);

    // 4. wait for refresh
    for (int i = 0; i < 4; i++) {
        {
            std::unique_lock<std::mutex> lk(mtx);
            refreshcv.wait(lk);
        }
    }

    auto iomanager = fileinstance.GetIOManager4File();

    curve::client::LeaseExecutor* lease = fileinstance.GetLeaseExecutor();

    // 5. set refresh AuthFail
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

    ASSERT_EQ(0, fileinstance.AioRead(&aioctx2, UserDataType::RawBuffer));

    std::this_thread::sleep_for(std::chrono::seconds(SLEEP_TIME_S));

    // 6. set refresh success
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

    // 7. set refresh kFileNotExists
    refreshresp.set_statuscode(::curve::mds::kFileNotExists);
    FakeReturn* refreshFakeRetFileNotExists =
        new FakeReturn(nullptr, static_cast<void*>(&refreshresp));
    curvefsservice->SetRefreshSession(refreshFakeRetFileNotExists, refresht);

    {
        std::unique_lock<std::mutex> lk(mtx);
        refreshcv.wait(lk);
    }

    std::this_thread::sleep_for(std::chrono::seconds(1));
    lease = fileinstance.GetLeaseExecutor();
    ASSERT_FALSE(lease->LeaseValid());

    // 8. set refresh success
    refreshresp.set_statuscode(::curve::mds::StatusCode::kOK);
    FakeReturn* refreshfakeretOK3 =
        new FakeReturn(nullptr, static_cast<void*>(&refreshresp));
    curvefsservice->SetRefreshSession(refreshfakeretOK3, refresht);
    fileinstance.GetLeaseExecutor()->ResetRefreshSessionTask();

    for (int i = 0; i < 2; i++) {
        {
            std::unique_lock<std::mutex> lk(mtx);
            refreshcv.wait(lk);
        }
    }
    ASSERT_TRUE(lease->LeaseValid());

    {
        std::unique_lock<std::mutex> lk(sessionMtx);
        sessionCV.wait(lk, [&]() { return sessionFlag; });
    }

    // 9. set inode id changed
    refreshresp.set_allocated_fileinfo(nullptr);  // clear existing file info

    curve::mds::FileInfo* newFileInfo = new curve::mds::FileInfo;
    newFileInfo->set_filename(filename);
    newFileInfo->set_seqnum(2);
    newFileInfo->set_id(100);
    newFileInfo->set_parentid(0);
    newFileInfo->set_filetype(curve::mds::FileType::INODE_PAGEFILE);
    newFileInfo->set_chunksize(4 * 1024 * 1024);
    newFileInfo->set_length(4 * 1024 * 1024 * 1024ul);
    newFileInfo->set_ctime(12345678);

    refreshresp.set_allocated_fileinfo(newFileInfo);
    refreshresp.set_statuscode(::curve::mds::StatusCode::kOK);

    FakeReturn* refreshFakeRetWithNewInodeId = new FakeReturn(
        nullptr, static_cast<void*>(&refreshresp));
    curvefsservice->SetRefreshSession(
        refreshFakeRetWithNewInodeId, refresht);

    {
        std::unique_lock<std::mutex> lk(mtx);
        refreshcv.wait(lk);
    }

    std::this_thread::sleep_for(std::chrono::seconds(1));
    lease = fileinstance.GetLeaseExecutor();
    ASSERT_FALSE(lease->LeaseValid());

    // 10. set refresh success
    refreshresp.set_allocated_fileinfo(nullptr);  // clear existing file info

    newFileInfo = new curve::mds::FileInfo;
    newFileInfo->set_filename(filename);
    newFileInfo->set_seqnum(2);
    newFileInfo->set_id(1);
    newFileInfo->set_parentid(0);
    newFileInfo->set_filetype(curve::mds::FileType::INODE_PAGEFILE);
    newFileInfo->set_chunksize(4 * 1024 * 1024);
    newFileInfo->set_length(4 * 1024 * 1024 * 1024ul);
    newFileInfo->set_ctime(12345678);

    refreshresp.set_allocated_fileinfo(newFileInfo);
    refreshresp.set_statuscode(::curve::mds::StatusCode::kOK);

    FakeReturn* refreshfakeretOK4 =
        new FakeReturn(nullptr, static_cast<void*>(&refreshresp));
    curvefsservice->SetRefreshSession(refreshfakeretOK4, refresht);
    fileinstance.GetLeaseExecutor()->ResetRefreshSessionTask();

    for (int i = 0; i < 2; i++) {
        {
            std::unique_lock<std::mutex> lk(mtx);
            refreshcv.wait(lk);
        }
    }
    ASSERT_TRUE(lease->LeaseValid());

    std::unique_lock<std::mutex> lk(sessionMtx);
    sessionCV.wait(lk, [&]() { return sessionFlag; });

    // 11. set fake close return
    ::curve::mds::CloseFileResponse closeresp;
    closeresp.set_statuscode(::curve::mds::StatusCode::kOK);
    FakeReturn* closefileret
     = new FakeReturn(nullptr, static_cast<void*>(&closeresp));
    curvefsservice->SetCloseFile(closefileret);

    LOG(INFO) << "uninit fileinstance";
    fileinstance.Close();
    fileinstance.UnInitialize();

    LOG(INFO) << "stop server";
    server.Stop(0);
    server.Join();

    LOG(INFO) << "uninit mds";
    mds.UnInitialize();
}

}  // namespace client
}  // namespace curve

std::string mdsMetaServerAddr = "127.0.0.1:9101";     // NOLINT
uint32_t segment_size = 1 * 1024 * 1024 * 1024ul;   // NOLINT
uint32_t chunk_size = 4 * 1024 * 1024;   // NOLINT
std::string configpath = "./test/client/client_session.conf";   // NOLINT

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
