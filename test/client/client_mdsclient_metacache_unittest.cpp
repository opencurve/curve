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

#include <gtest/gtest.h>
#include <gflags/gflags.h>
#include <glog/logging.h>
#include <brpc/controller.h>
#include <brpc/server.h>
#include <fiu-control.h>
#include <brpc/channel.h>
#include <brpc/errno.pb.h>

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
// #include "test/client/mock_curvefs_service.h"
#include "test/client/mock/mock_namespace_service.h"

extern std::string mdsMetaServerAddr;
extern uint32_t chunk_size;
extern std::string configpath;
extern curve::client::FileClient* globalclient;

namespace curve {
namespace client {

using curve::mds::CurveFSService;
using curve::mds::topology::TopologyService;
using curve::mds::topology::GetChunkServerListInCopySetsResponse;
using curve::mds::topology::ChunkServerStatus;
using curve::mds::topology::DiskState;
using curve::mds::topology::OnlineState;

class MDSClientTest : public ::testing::Test {
 public:
    void SetUp() {
        metaopt.mdsAddrs.push_back("127.0.0.1:9104");
        metaopt.mdsAddrs.push_back("127.0.0.1:9104");

        metaopt.mdsMaxRetryMS = 1000;
        metaopt.mdsRPCTimeoutMs = 500;
        metaopt.mdsRPCRetryIntervalUS = 200;
        metaopt.mdsRPCTimeoutMs = 2000;
        metaopt.mdsMaxRetryMsInIOPath = 10000;
        mdsclient_.Initialize(metaopt);
        userinfo.owner = "test";

        if (server.AddService(&topologyservice,
                              brpc::SERVER_DOESNT_OWN_SERVICE) != 0) {
            ASSERT_TRUE(false) << "Fail to add service";
        }

        if (server.AddService(&curvefsservice,
                              brpc::SERVER_DOESNT_OWN_SERVICE) != 0) {
            ASSERT_TRUE(false) << "Fail to add service";
        }

        curve::mds::topology::GetChunkServerInfoResponse* response =
            new curve::mds::topology::GetChunkServerInfoResponse();
        response->set_statuscode(0);
        curve::mds::topology::ChunkServerInfo* serverinfo =
            new curve::mds::topology::ChunkServerInfo();
        serverinfo->set_chunkserverid(888);
        serverinfo->set_disktype("nvme");
        serverinfo->set_hostip("10.182.26.2");
        serverinfo->set_externalip("127.0.0.1");
        serverinfo->set_port(9124);
        serverinfo->set_status(ChunkServerStatus::RETIRED);
        serverinfo->set_diskstatus(DiskState::DISKNORMAL);
        serverinfo->set_onlinestate(OnlineState::ONLINE);
        serverinfo->set_mountpoint("/test");
        serverinfo->set_diskcapacity(11111);
        serverinfo->set_diskused(1111);
        response->set_allocated_chunkserverinfo(serverinfo);
        FakeReturn* getcsret = new FakeReturn(nullptr, static_cast<void*>(response));      // NOLINT
        topologyservice.SetGetChunkserverFakeReturn(getcsret);

        brpc::ServerOptions options;
        options.idle_timeout_sec = -1;
        LOG(INFO) << "meta server addr = " << mdsMetaServerAddr.c_str();
        ASSERT_EQ(server.Start(mdsMetaServerAddr.c_str(), &options), 0);

        ASSERT_EQ(0, Init(configpath.c_str()))
            << "Fail to init config, path = " << configpath;
    }

    void TearDown() {
        mdsclient_.UnInitialize();
        UnInit();
        ASSERT_EQ(0, server.Stop(0));
        ASSERT_EQ(0, server.Join());
    }

    brpc::Server        server;
    FileClient          fileClient_;
    UserInfo_t          userinfo;
    MDSClient           mdsclient_;
    MetaServerOption  metaopt;
    FakeTopologyService topologyservice;
    FakeMDSCurveFSService curvefsservice;
    static int i;
};

TEST_F(MDSClientTest, Createfile) {
    std::string filename = "/1_userinfo_";
    size_t len = 4 * 1024 * 1024;

    // set response file exist
    ::curve::mds::CreateFileResponse response;
    response.set_statuscode(::curve::mds::StatusCode::kFileExists);

    FakeReturn* fakeret
     = new FakeReturn(nullptr, static_cast<void*>(&response));

    curvefsservice.SetCreateFileFakeReturn(fakeret);

    LOG(INFO) << "now create file!";
    int ret = globalclient->Create(filename.c_str(), userinfo, len);
    ASSERT_EQ(ret, -1 * LIBCURVE_ERROR::EXISTS);

    // set response file exist
    ::curve::mds::CreateFileResponse response1;
    response1.set_statuscode(::curve::mds::StatusCode::kOK);

    FakeReturn* fakeret1
     = new FakeReturn(nullptr, static_cast<void*>(&response1));

    curvefsservice.SetCreateFileFakeReturn(fakeret1);
    ASSERT_EQ(LIBCURVE_ERROR::OK, globalclient->Create(filename.c_str(),
                                    userinfo, len));

    // 设置rpc失败，触发重试
    brpc::Controller cntl;
    cntl.SetFailed(-1, "failed");

    FakeReturn* fakeret2
     = new FakeReturn(&cntl, static_cast<void*>(&response));

    curvefsservice.SetCreateFileFakeReturn(fakeret2);
    curvefsservice.CleanRetryTimes();

    uint64_t starttime = curve::common::TimeUtility::GetTimeofDayMs();
    ASSERT_EQ(-LIBCURVE_ERROR::FAILED,
             globalclient->Create(filename.c_str(), userinfo, len));
    uint64_t endtime = curve::common::TimeUtility::GetTimeofDayMs();
    ASSERT_GT(endtime - starttime, metaopt.mdsMaxRetryMS - 1);

    LOG(INFO) << "create file done!";
    delete fakeret;
    delete fakeret2;
}

TEST_F(MDSClientTest, MkDir) {
    std::string dirpath = "/1";
    size_t len = 4 * 1024 * 1024;
    // set response file exist
    ::curve::mds::CreateFileResponse response;
    response.set_statuscode(::curve::mds::StatusCode::kFileExists);

    FakeReturn* fakeret
     = new FakeReturn(nullptr, static_cast<void*>(&response));

    curvefsservice.SetCreateFileFakeReturn(fakeret);

    LOG(INFO) << "now create file!";
    int ret = globalclient->Mkdir(dirpath.c_str(), userinfo);
    ASSERT_EQ(ret, -1 * LIBCURVE_ERROR::EXISTS);

    C_UserInfo_t cuserinfo;
    memcpy(cuserinfo.owner, "test", 5);
    ret = Mkdir(dirpath.c_str(), &cuserinfo);
    ASSERT_EQ(ret, -1 * LIBCURVE_ERROR::EXISTS);

    // set response file exist
    ::curve::mds::CreateFileResponse response1;
    response1.set_statuscode(::curve::mds::StatusCode::kOK);

    FakeReturn* fakeret1
     = new FakeReturn(nullptr, static_cast<void*>(&response1));

    curvefsservice.SetCreateFileFakeReturn(fakeret1);
    ASSERT_EQ(LIBCURVE_ERROR::OK, globalclient->Mkdir(dirpath.c_str(),
                                    userinfo));


    // 设置rpc失败，触发重试
    brpc::Controller cntl;
    cntl.SetFailed(-1, "failed");

    FakeReturn* fakeret2
     = new FakeReturn(&cntl, static_cast<void*>(&response));

    curvefsservice.SetCreateFileFakeReturn(fakeret2);
    curvefsservice.CleanRetryTimes();

    uint64_t starttime = curve::common::TimeUtility::GetTimeofDayMs();
    ASSERT_EQ(-1 * LIBCURVE_ERROR::FAILED,
             globalclient->Mkdir(dirpath.c_str(), userinfo));
    uint64_t endtime = curve::common::TimeUtility::GetTimeofDayMs();
    ASSERT_GT(endtime - starttime, metaopt.mdsMaxRetryMS - 1);

    LOG(INFO) << "create file done!";
    delete fakeret;
    delete fakeret2;
}

TEST_F(MDSClientTest, Closefile) {
    std::string filename = "/1_userinfo_";
    size_t len = 4 * 1024 * 1024;
    // file not exist
    ::curve::mds::CloseFileResponse response;
    response.set_statuscode(::curve::mds::StatusCode::kFileNotExists);

    FakeReturn* fakeret
                    = new FakeReturn(nullptr, static_cast<void*>(&response));
    curvefsservice.SetCloseFile(fakeret);

    LOG(INFO) << "now create file!";
    LIBCURVE_ERROR ret = mdsclient_.CloseFile(filename.c_str(),
                            userinfo, "sessid");
    ASSERT_EQ(ret, LIBCURVE_ERROR::NOTEXIST);


    // file close ok
    ::curve::mds::CloseFileResponse response1;
    response1.set_statuscode(::curve::mds::StatusCode::kOK);

    FakeReturn* fakeret1
                = new FakeReturn(nullptr, static_cast<void*>(&response1));
    curvefsservice.SetCloseFile(fakeret1);

    LOG(INFO) << "now create file!";
    ret = mdsclient_.CloseFile(filename.c_str(),
                                userinfo, "sessid");
    ASSERT_EQ(ret, LIBCURVE_ERROR::OK);

    // 设置rpc失败，触发重试
    brpc::Controller cntl;
    cntl.SetFailed(-1, "failed");

    FakeReturn* fakeret2
                = new FakeReturn(&cntl, static_cast<void*>(&response));
    curvefsservice.SetCloseFile(fakeret2);
    curvefsservice.CleanRetryTimes();

    ASSERT_EQ(LIBCURVE_ERROR::FAILED,
         mdsclient_.CloseFile(filename.c_str(), userinfo,  "sessid"));

    delete fakeret;
    delete fakeret2;
}

TEST_F(MDSClientTest, Openfile) {
    std::string filename = "/1_userinfo_";
    size_t len = 4 * 1024 * 1024;
    /**
     * set openfile response
     */
    ::curve::mds::OpenFileResponse openresponse;

    openresponse.set_statuscode(::curve::mds::StatusCode::kOK);
    FakeReturn* fakeret
     = new FakeReturn(nullptr, static_cast<void*>(&openresponse));
    curvefsservice.SetOpenFile(fakeret);

    FInfo finfo;
    LeaseSession lease;
    ASSERT_EQ(globalclient->Open(filename, userinfo),
                                 -1*LIBCURVE_ERROR::FAILED);

    // has protosession no fileinfo
    ::curve::mds::OpenFileResponse openresponse1;

    ::curve::mds::ProtoSession* se = new ::curve::mds::ProtoSession;
    se->set_sessionid("1");
    se->set_createtime(12345);
    se->set_leasetime(10000000);
    se->set_sessionstatus(::curve::mds::SessionStatus::kSessionOK);

    openresponse1.set_statuscode(::curve::mds::StatusCode::kOK);
    openresponse1.set_allocated_protosession(se);

    FakeReturn* fakeret1
     = new FakeReturn(nullptr, static_cast<void*>(&openresponse1));
    curvefsservice.SetOpenFile(fakeret1);

    ASSERT_EQ(globalclient->Open(filename, userinfo), -LIBCURVE_ERROR::FAILED);

    // has protosession and finfo
    ::curve::mds::OpenFileResponse openresponse2;

    ::curve::mds::ProtoSession* se2 = new ::curve::mds::ProtoSession;
    se2->set_sessionid("1");
    se2->set_createtime(12345);
    se2->set_leasetime(10000000);
    se2->set_sessionstatus(::curve::mds::SessionStatus::kSessionOK);

    ::curve::mds::FileInfo* fin = new ::curve::mds::FileInfo;
    fin->set_filename("_filename_");
    fin->set_id(1);
    fin->set_parentid(0);
    fin->set_filetype(curve::mds::FileType::INODE_PAGEFILE);
    fin->set_chunksize(4 * 1024 * 1024);
    fin->set_length(1 * 1024 * 1024 * 1024ul);
    fin->set_ctime(12345678);
    fin->set_seqnum(0);
    fin->set_segmentsize(1 * 1024 * 1024 * 1024ul);

    openresponse2.set_statuscode(::curve::mds::StatusCode::kOK);
    openresponse2.set_allocated_protosession(se2);
    openresponse2.set_allocated_fileinfo(fin);

    FakeReturn* fakeret2
     = new FakeReturn(nullptr, static_cast<void*>(&openresponse2));
    curvefsservice.SetOpenFile(fakeret2);

    ASSERT_EQ(globalclient->Open(filename, userinfo), LIBCURVE_ERROR::OK);
    ASSERT_EQ(LIBCURVE_ERROR::OK, Write(0, nullptr, 0, 0));
    ASSERT_EQ(LIBCURVE_ERROR::OK, Read(0, nullptr, 0, 0));

    ::curve::mds::ProtoSession* socupied = new ::curve::mds::ProtoSession;
    socupied->set_sessionid("1");
    socupied->set_createtime(12345);
    socupied->set_leasetime(10000000);
    socupied->set_sessionstatus(::curve::mds::SessionStatus::kSessionOK);

    ::curve::mds::FileInfo* focupied = new ::curve::mds::FileInfo;
    focupied->set_filename("_filename_");
    focupied->set_id(1);
    focupied->set_parentid(0);
    focupied->set_filetype(curve::mds::FileType::INODE_PAGEFILE);
    focupied->set_chunksize(4 * 1024 * 1024);
    focupied->set_length(1 * 1024 * 1024 * 1024ul);
    focupied->set_ctime(12345678);
    focupied->set_seqnum(0);
    focupied->set_segmentsize(1 * 1024 * 1024 * 1024ul);

    ::curve::mds::OpenFileResponse responseOccupied;
    responseOccupied.set_statuscode(::curve::mds::StatusCode::kFileOccupied);
    responseOccupied.set_allocated_protosession(socupied);
    responseOccupied.set_allocated_fileinfo(focupied);

    curve::mds::ReFreshSessionResponse refreshresponse;
    refreshresponse.set_statuscode(::curve::mds::StatusCode::kOK);
    refreshresponse.set_sessionid("2");

    FakeReturn* r
     = new FakeReturn(nullptr, static_cast<void*>(&responseOccupied));
    curvefsservice.SetOpenFile(r);
    FakeReturn* refreshret =
    new FakeReturn(nullptr, static_cast<void*>(&refreshresponse));
    curvefsservice.SetRefreshSession(refreshret, [](){});

    curve::mds::FileInfo * info = new curve::mds::FileInfo;
    ::curve::mds::GetFileInfoResponse getinforesponse;
    info->set_filename("_filename_");
    info->set_id(1);
    info->set_parentid(0);
    info->set_filetype(curve::mds::FileType::INODE_PAGEFILE);
    info->set_chunksize(4 * 1024 * 1024);
    info->set_length(4 * 1024 * 1024 * 1024ul);
    info->set_ctime(12345678);
    info->set_segmentsize(1 * 1024 * 1024 * 1024ul);

    getinforesponse.set_allocated_fileinfo(info);
    getinforesponse.set_statuscode(::curve::mds::StatusCode::kOK);

    FakeReturn* fakegetinfo =
        new FakeReturn(nullptr, static_cast<void*>(&getinforesponse));
    curvefsservice.SetGetFileInfoFakeReturn(fakegetinfo);

    int fd = globalclient->Open(filename, userinfo);
    ASSERT_EQ(fd, -LIBCURVE_ERROR::FILE_OCCUPIED);
    ASSERT_EQ(LIBCURVE_ERROR::OK, Write(fd, nullptr, 0, 0));
    ASSERT_EQ(LIBCURVE_ERROR::OK, Read(fd, nullptr, 0, 0));

    // 测试关闭文件
    ::curve::mds::CloseFileResponse closeresp;
    closeresp.set_statuscode(::curve::mds::StatusCode::kOK);

    FakeReturn* fakecloseret
                = new FakeReturn(nullptr, static_cast<void*>(&closeresp));
    curvefsservice.SetCloseFile(fakecloseret);

    globalclient->Close(fd);

    CurveAioContext aioctx;
    aioctx.length = 0;
    ASSERT_EQ(LIBCURVE_ERROR::OK, AioWrite(fd, &aioctx));
    ASSERT_EQ(LIBCURVE_ERROR::OK, AioRead(fd, &aioctx));

    // 设置rpc失败，触发重试
    brpc::Controller cntl;
    cntl.SetFailed(-1, "failed");

    FakeReturn* fakeret3
                = new FakeReturn(&cntl, static_cast<void*>(&openresponse2));
    curvefsservice.SetOpenFile(fakeret3);
    curvefsservice.CleanRetryTimes();

    ASSERT_EQ(-LIBCURVE_ERROR::FAILED, globalclient->Open(filename, userinfo));

    // file close not exist
    ::curve::mds::CloseFileResponse response2;
    response2.set_statuscode(::curve::mds::StatusCode::kSessionNotExist);

    FakeReturn* fakeret4
                = new FakeReturn(nullptr, static_cast<void*>(&response2));
    curvefsservice.SetCloseFile(fakeret4);

    globalclient->Close(0);
    delete fakeret;
    delete fakeret1;
    delete fakeret2;
}

TEST_F(MDSClientTest, Renamefile) {
    std::string filename1 = "/1_userinfo_";
    std::string filename2 = "/1_userinfo_";
    // set response file exist
    ::curve::mds::RenameFileResponse response;
    response.set_statuscode(::curve::mds::StatusCode::kFileExists);

    FakeReturn* fakeret
     = new FakeReturn(nullptr, static_cast<void*>(&response));

    curvefsservice.SetRenameFile(fakeret);

    int ret = globalclient->Rename(userinfo, filename1, filename2);
    ASSERT_EQ(ret, -1 * LIBCURVE_ERROR::EXISTS);

    C_UserInfo_t cuserinfo;
    memcpy(cuserinfo.owner, "test", 5);
    ret = Rename(&cuserinfo, filename1.c_str(), filename2.c_str());
    ASSERT_EQ(ret, -1 * LIBCURVE_ERROR::EXISTS);

    // set rename file ok
    ::curve::mds::RenameFileResponse response1;
    response1.set_statuscode(::curve::mds::StatusCode::kOK);

    FakeReturn* fakeret1
     = new FakeReturn(nullptr, static_cast<void*>(&response1));

    curvefsservice.SetRenameFile(fakeret1);
    ASSERT_EQ(LIBCURVE_ERROR::OK, globalclient->Rename(userinfo,
                                                        filename1,
                                                        filename2));

    // set rename file dir not exists
    ::curve::mds::RenameFileResponse response2;
    response2.set_statuscode(::curve::mds::StatusCode::kDirNotExist);

    FakeReturn* fakeret3
     = new FakeReturn(nullptr, static_cast<void*>(&response2));

    curvefsservice.SetRenameFile(fakeret3);
    ASSERT_EQ(-1 * LIBCURVE_ERROR::NOTEXIST,
              globalclient->Rename(userinfo, filename1, filename2));

    // set rename file auth fail
    ::curve::mds::RenameFileResponse response3;
    response3.set_statuscode(::curve::mds::StatusCode::kOwnerAuthFail);

    FakeReturn* fakeret4
     = new FakeReturn(nullptr, static_cast<void*>(&response3));

    curvefsservice.SetRenameFile(fakeret4);
    ASSERT_EQ(-1 * LIBCURVE_ERROR::AUTHFAIL,
              globalclient->Rename(userinfo, filename1, filename2));

    // set rename file MDS storage error
    ::curve::mds::RenameFileResponse response4;
    response4.set_statuscode(::curve::mds::StatusCode::kStorageError);

    FakeReturn* fakeret5
     = new FakeReturn(nullptr, static_cast<void*>(&response4));

    curvefsservice.SetRenameFile(fakeret5);
    ASSERT_EQ(-1 * LIBCURVE_ERROR::INTERNAL_ERROR,
              globalclient->Rename(userinfo, filename1, filename2));

    // 设置rpc失败，触发重试
    brpc::Controller cntl;
    cntl.SetFailed(-1, "failed");

    FakeReturn* fakeret2
     = new FakeReturn(&cntl, static_cast<void*>(&response));

    curvefsservice.SetRenameFile(fakeret2);
    curvefsservice.CleanRetryTimes();

    ASSERT_EQ(-1 * LIBCURVE_ERROR::FAILED, globalclient->Rename(userinfo,
                                                        filename1,
                                                        filename2));

    delete fakeret;
    delete fakeret2;
    delete fakeret3;
    delete fakeret4;
    delete fakeret5;
}

TEST_F(MDSClientTest, Extendfile) {
    std::string filename1 = "/1_userinfo_";
    uint64_t newsize = 10 * 1024 * 1024 * 1024ul;

    // set response file exist
    ::curve::mds::ExtendFileResponse response;
    response.set_statuscode(::curve::mds::StatusCode::kFileExists);

    FakeReturn* fakeret
     = new FakeReturn(nullptr, static_cast<void*>(&response));

    curvefsservice.SetExtendFile(fakeret);

    int ret = globalclient->Extend(filename1, userinfo, newsize);
    ASSERT_EQ(ret, -1 * LIBCURVE_ERROR::EXISTS);

    C_UserInfo_t cuserinfo;
    memcpy(cuserinfo.owner, "test", 5);
    ret = Extend(filename1.c_str(), &cuserinfo, newsize);
    ASSERT_EQ(ret, -1 * LIBCURVE_ERROR::EXISTS);

    // set extend file ok
    ::curve::mds::ExtendFileResponse response1;
    response1.set_statuscode(::curve::mds::StatusCode::kOK);

    FakeReturn* fakeret1
     = new FakeReturn(nullptr, static_cast<void*>(&response1));

    curvefsservice.SetExtendFile(fakeret1);
    ASSERT_EQ(LIBCURVE_ERROR::OK, globalclient->Extend(filename1,
                                                    userinfo,
                                                    newsize));

    // set extend file dir not exists
    ::curve::mds::ExtendFileResponse response2;
    response2.set_statuscode(::curve::mds::StatusCode::kDirNotExist);

    FakeReturn* fakeret3
     = new FakeReturn(nullptr, static_cast<void*>(&response2));

    curvefsservice.SetExtendFile(fakeret3);
    ASSERT_EQ(-1 * LIBCURVE_ERROR::NOTEXIST,
              globalclient->Extend(filename1, userinfo, newsize));

    // set extend file auth fail
    ::curve::mds::ExtendFileResponse response3;
    response3.set_statuscode(::curve::mds::StatusCode::kOwnerAuthFail);

    FakeReturn* fakeret4
     = new FakeReturn(nullptr, static_cast<void*>(&response3));

    curvefsservice.SetExtendFile(fakeret4);
    ASSERT_EQ(-1 * LIBCURVE_ERROR::AUTHFAIL,
              globalclient->Extend(filename1, userinfo, newsize));

    // set extend file mds storage error
    ::curve::mds::ExtendFileResponse response4;
    response4.set_statuscode(::curve::mds::StatusCode::kStorageError);

    FakeReturn* fakeret5
     = new FakeReturn(nullptr, static_cast<void*>(&response4));

    curvefsservice.SetExtendFile(fakeret5);
    ASSERT_EQ(-1 * LIBCURVE_ERROR::INTERNAL_ERROR,
              globalclient->Extend(filename1, userinfo, newsize));

    // set extend bigger file
    ::curve::mds::ExtendFileResponse response5;
    response5.set_statuscode(::curve::mds::StatusCode::kShrinkBiggerFile);

    FakeReturn* fakeret6
     = new FakeReturn(nullptr, static_cast<void*>(&response5));

    curvefsservice.SetExtendFile(fakeret6);
    ASSERT_EQ(-1 * LIBCURVE_ERROR::NO_SHRINK_BIGGER_FILE,
              globalclient->Extend(filename1, userinfo, newsize));

    // 设置rpc失败，触发重试
    brpc::Controller cntl;
    cntl.SetFailed(-1, "failed");

    FakeReturn* fakeret2
     = new FakeReturn(&cntl, static_cast<void*>(&response));

    curvefsservice.SetExtendFile(fakeret2);
    curvefsservice.CleanRetryTimes();

    ASSERT_EQ(-1 * LIBCURVE_ERROR::FAILED, globalclient->Extend(filename1,
                                                        userinfo,
                                                        newsize));

    delete fakeret;
    delete fakeret2;
    delete fakeret3;
    delete fakeret4;
    delete fakeret5;
    delete fakeret6;
}

TEST_F(MDSClientTest, Deletefile) {
    std::string filename1 = "/1_userinfo_";
    uint64_t newsize = 10 * 1024 * 1024 * 1024ul;

    // set response file exist
    ::curve::mds::DeleteFileResponse response;
    response.set_statuscode(::curve::mds::StatusCode::kFileNotExists);

    FakeReturn* fakeret
     = new FakeReturn(nullptr, static_cast<void*>(&response));

    curvefsservice.SetDeleteFile(fakeret);

    int ret = globalclient->Unlink(filename1, userinfo);
    ASSERT_EQ(ret, -1 * LIBCURVE_ERROR::NOTEXIST);

    // set extend file ok
    ::curve::mds::DeleteFileResponse response1;
    response1.set_statuscode(::curve::mds::StatusCode::kOK);

    FakeReturn* fakeret1
     = new FakeReturn(nullptr, static_cast<void*>(&response1));

    curvefsservice.SetDeleteFile(fakeret1);
    ASSERT_EQ(LIBCURVE_ERROR::OK, globalclient->Unlink(filename1,
                                                    userinfo));

    // set delete file dir not exists
    ::curve::mds::DeleteFileResponse response2;
    response2.set_statuscode(::curve::mds::StatusCode::kDirNotExist);

    FakeReturn* fakeret3
     = new FakeReturn(nullptr, static_cast<void*>(&response2));

    curvefsservice.SetDeleteFile(fakeret3);
    ASSERT_EQ(-1 * LIBCURVE_ERROR::NOTEXIST,
              globalclient->Unlink(filename1, userinfo));

    // set delete file auth fail
    ::curve::mds::DeleteFileResponse response3;
    response3.set_statuscode(::curve::mds::StatusCode::kOwnerAuthFail);

    FakeReturn* fakeret4
     = new FakeReturn(nullptr, static_cast<void*>(&response3));

    curvefsservice.SetDeleteFile(fakeret4);
    ASSERT_EQ(-1 * LIBCURVE_ERROR::AUTHFAIL,
              globalclient->Unlink(filename1, userinfo));

    // set delete file mds storage error
    ::curve::mds::DeleteFileResponse response4;
    response4.set_statuscode(::curve::mds::StatusCode::kStorageError);

    FakeReturn* fakeret5
     = new FakeReturn(nullptr, static_cast<void*>(&response4));

    curvefsservice.SetDeleteFile(fakeret5);
    ASSERT_EQ(-1 * LIBCURVE_ERROR::INTERNAL_ERROR,
              globalclient->Unlink(filename1, userinfo));

    // 设置delete force
    fiu_init(0);
    fiu_enable("test/client/fake/fakeMDS/forceDeleteFile", 1, nullptr, 0);
    ASSERT_EQ(-1 * LIBCURVE_ERROR::NOT_SUPPORT,
              globalclient->Unlink(filename1, userinfo, true));
    fiu_disable("test/client/fake/fakeMDS/forceDeleteFile");

    // 设置rpc失败，触发重试
    brpc::Controller cntl;
    cntl.SetFailed(-1, "failed");

    FakeReturn* fakeret2
     = new FakeReturn(&cntl, static_cast<void*>(&response));

    curvefsservice.SetDeleteFile(fakeret2);
    curvefsservice.CleanRetryTimes();

    ASSERT_EQ(-1 * LIBCURVE_ERROR::FAILED, globalclient->Unlink(filename1,
                                                        userinfo));

    C_UserInfo_t cuserinfo;
    memcpy(cuserinfo.owner, "test", 5);
    ASSERT_EQ(-1 * LIBCURVE_ERROR::FAILED, DeleteForce(filename1.c_str(),
                                                       &cuserinfo));

    delete fakeret;
    delete fakeret2;
}

TEST_F(MDSClientTest, Rmdir) {
    std::string filename1 = "/1/";
    uint64_t newsize = 10 * 1024 * 1024 * 1024ul;

    // set response dir not exist
    ::curve::mds::DeleteFileResponse response;
    response.set_statuscode(::curve::mds::StatusCode::kDirNotExist);

    FakeReturn* fakeret
     = new FakeReturn(nullptr, static_cast<void*>(&response));

    curvefsservice.SetDeleteFile(fakeret);

    int ret = globalclient->Rmdir(filename1, userinfo);
    ASSERT_EQ(ret, -1 * LIBCURVE_ERROR::NOTEXIST);

    C_UserInfo_t cuserinfo;
    memcpy(cuserinfo.owner, "test", 5);
    ret = Rmdir(filename1.c_str(), &cuserinfo);
    ASSERT_EQ(ret, -1 * LIBCURVE_ERROR::NOTEXIST);

    // set extend file ok
    ::curve::mds::DeleteFileResponse response1;
    response1.set_statuscode(::curve::mds::StatusCode::kOK);

    FakeReturn* fakeret1
     = new FakeReturn(nullptr, static_cast<void*>(&response1));

    curvefsservice.SetDeleteFile(fakeret1);
    ASSERT_EQ(LIBCURVE_ERROR::OK, globalclient->Rmdir(filename1,
                                                    userinfo));

    // set delete file dir not exists
    ::curve::mds::DeleteFileResponse response2;
    response2.set_statuscode(::curve::mds::StatusCode::kDirNotExist);

    FakeReturn* fakeret3
     = new FakeReturn(nullptr, static_cast<void*>(&response2));

    curvefsservice.SetDeleteFile(fakeret3);
    ASSERT_EQ(-1 * LIBCURVE_ERROR::NOTEXIST,
              globalclient->Rmdir(filename1, userinfo));

    // set delete file auth fail
    ::curve::mds::DeleteFileResponse response3;
    response3.set_statuscode(::curve::mds::StatusCode::kOwnerAuthFail);

    FakeReturn* fakeret4
     = new FakeReturn(nullptr, static_cast<void*>(&response3));

    curvefsservice.SetDeleteFile(fakeret4);
    ASSERT_EQ(-1 * LIBCURVE_ERROR::AUTHFAIL,
              globalclient->Rmdir(filename1, userinfo));

    // set delete file mds storage error
    ::curve::mds::DeleteFileResponse response4;
    response4.set_statuscode(::curve::mds::StatusCode::kStorageError);

    FakeReturn* fakeret5
     = new FakeReturn(nullptr, static_cast<void*>(&response4));

    curvefsservice.SetDeleteFile(fakeret5);
    ASSERT_EQ(-1 * LIBCURVE_ERROR::INTERNAL_ERROR,
              globalclient->Rmdir(filename1, userinfo));

    // 设置rpc失败，触发重试
    brpc::Controller cntl;
    cntl.SetFailed(-1, "failed");

    FakeReturn* fakeret2
     = new FakeReturn(&cntl, static_cast<void*>(&response));

    curvefsservice.SetDeleteFile(fakeret2);
    curvefsservice.CleanRetryTimes();

    ASSERT_EQ(-1 * LIBCURVE_ERROR::FAILED, globalclient->Rmdir(filename1,
                                                        userinfo));
    delete fakeret;
    delete fakeret2;
}

TEST_F(MDSClientTest, StatFile) {
    std::string filename = "/1_userinfo_";

    curve::mds::FileInfo * info = new curve::mds::FileInfo;
    ::curve::mds::GetFileInfoResponse response;
    info->set_filename("_filename_");
    info->set_id(1);
    info->set_parentid(0);
    info->set_filetype(curve::mds::FileType::INODE_PAGEFILE);
    info->set_chunksize(4 * 1024 * 1024);
    info->set_length(4 * 1024 * 1024 * 1024ul);
    info->set_ctime(12345678);
    info->set_segmentsize(1 * 1024 * 1024 * 1024ul);

    response.set_allocated_fileinfo(info);
    response.set_statuscode(::curve::mds::StatusCode::kOK);

    FakeReturn* fakeret =
        new FakeReturn(nullptr, static_cast<void*>(&response));
    curvefsservice.SetGetFileInfoFakeReturn(fakeret);

    curve::client::FInfo_t* finfo = new curve::client::FInfo_t;
    FileStatInfo fstat;
    globalclient->StatFile(filename, userinfo, &fstat);

    ASSERT_EQ(fstat.id, 1);
    ASSERT_EQ(fstat.parentid, 0);
    ASSERT_EQ(static_cast<curve::mds::FileType>(fstat.filetype),
        curve::mds::FileType::INODE_PAGEFILE);
    ASSERT_EQ(fstat.ctime, 12345678);
    ASSERT_EQ(fstat.length, 4 * 1024 * 1024 * 1024ul);

    // 设置rpc失败，触发重试
    brpc::Controller cntl;
    cntl.SetFailed(-1, "failed");

    FakeReturn* fakeret2
     = new FakeReturn(&cntl, static_cast<void*>(&response));

    curvefsservice.SetGetFileInfoFakeReturn(fakeret2);
    curvefsservice.CleanRetryTimes();

    ASSERT_EQ(-1 * LIBCURVE_ERROR::FAILED,
         globalclient->StatFile(filename, userinfo, &fstat));

    delete fakeret;
    delete fakeret2;
    delete finfo;
}

TEST_F(MDSClientTest, GetFileInfo) {
    std::string filename = "/1_userinfo_";
    curve::mds::FileInfo * info = new curve::mds::FileInfo;
    ::curve::mds::GetFileInfoResponse response;
    info->set_filename("_filename_");
    info->set_id(1);
    info->set_parentid(0);
    info->set_filetype(curve::mds::FileType::INODE_PAGEFILE);
    info->set_chunksize(4 * 1024 * 1024);
    info->set_length(4 * 1024 * 1024 * 1024ul);
    info->set_ctime(12345678);
    info->set_segmentsize(1 * 1024 * 1024 * 1024ul);

    response.set_allocated_fileinfo(info);
    response.set_statuscode(::curve::mds::StatusCode::kOK);

    FakeReturn* fakeret =
        new FakeReturn(nullptr, static_cast<void*>(&response));
    curvefsservice.SetGetFileInfoFakeReturn(fakeret);

    curve::client::FInfo_t* finfo = new curve::client::FInfo_t;
    mdsclient_.GetFileInfo(filename, userinfo, finfo);

    ASSERT_EQ(finfo->filename, "_filename_");
    ASSERT_EQ(finfo->id, 1);
    ASSERT_EQ(finfo->parentid, 0);
    ASSERT_EQ(static_cast<curve::mds::FileType>(finfo->filetype),
        curve::mds::FileType::INODE_PAGEFILE);
    ASSERT_EQ(finfo->chunksize, 4 * 1024 * 1024);
    ASSERT_EQ(finfo->length, 4 * 1024 * 1024 * 1024ul);
    ASSERT_EQ(finfo->ctime, 12345678);
    ASSERT_EQ(finfo->segmentsize, 1 * 1024 * 1024 * 1024ul);

    // 设置rpc失败，触发重试
    brpc::Controller cntl;
    cntl.SetFailed(-1, "failed");

    FakeReturn* fakeret2
     = new FakeReturn(&cntl, static_cast<void*>(&response));

    curvefsservice.SetGetFileInfoFakeReturn(fakeret2);
    curvefsservice.CleanRetryTimes();

    ASSERT_EQ(LIBCURVE_ERROR::FAILED,
         mdsclient_.GetFileInfo(filename.c_str(),
          userinfo, finfo));

    delete fakeret;
    delete fakeret2;
    delete finfo;
}

TEST_F(MDSClientTest, GetOrAllocateSegment) {
    std::string filename = "/1_userinfo_";

    curve::client::FInfo_t fi;
    fi.userinfo = userinfo;
    fi.chunksize   = 4 * 1024 * 1024;
    fi.segmentsize = 1 * 1024 * 1024 * 1024ul;

    std::chrono::system_clock::time_point start, end;
    auto startTimer = [&start]() { start = std::chrono::system_clock::now(); };
    auto endTimer = [&end]() { end = std::chrono::system_clock::now(); };
    auto checkTimer = [&start, &end](uint64_t min, uint64_t max) {
        auto elpased =  std::chrono::duration_cast<std::chrono::milliseconds>(
            end - start).count();
        ASSERT_GE(elpased, min);
        ASSERT_LE(elpased, max);
    };

    // TEST CASE: GetOrAllocateSegment failed, wait 10 seconds
    curve::mds::GetOrAllocateSegmentResponse resp;
    resp.set_statuscode(::curve::mds::StatusCode::kOK);
    FakeReturn* fakeres = new FakeReturn(nullptr,
                static_cast<void*>(&resp));
    curvefsservice.SetGetOrAllocateSegmentFakeReturn(fakeres);

    SegmentInfo seg;
    startTimer();
    ASSERT_EQ(LIBCURVE_ERROR::FAILED,
        mdsclient_.GetOrAllocateSegment(true, 0, &fi, &seg));
    endTimer();
    checkTimer(10000, 11000);

    curve::mds::GetOrAllocateSegmentResponse response;
    curve::mds::PageFileSegment* pfs = new curve::mds::PageFileSegment;
    response.set_statuscode(::curve::mds::StatusCode::kOK);
    response.set_allocated_pagefilesegment(pfs);
    response.mutable_pagefilesegment()->set_logicalpoolid(1234);
    response.mutable_pagefilesegment()->set_segmentsize(1*1024*1024*1024ul);
    response.mutable_pagefilesegment()->set_chunksize(4 * 1024 * 1024);
    response.mutable_pagefilesegment()->set_startoffset(0);
    for (int i = 0; i < 256; i ++) {
        auto chunk = response.mutable_pagefilesegment()->add_chunks();
        chunk->set_copysetid(i);
        chunk->set_chunkid(i);
    }
    FakeReturn* fakeret = new FakeReturn(nullptr,
                static_cast<void*>(&response));
    curvefsservice.SetGetOrAllocateSegmentFakeReturn(fakeret);

    ::curve::mds::topology::GetChunkServerListInCopySetsResponse response_1;
    response_1.set_statuscode(0);
    uint64_t chunkserveridc = 1;
    for (int i = 0; i < 256; i ++) {
        auto csinfo = response_1.add_csinfo();
        csinfo->set_copysetid(i);

        for (int j = 0; j < 3; j++) {
            auto cslocs = csinfo->add_cslocs();
            cslocs->set_chunkserverid(chunkserveridc++);
            cslocs->set_hostip("127.0.0.1");
            cslocs->set_port(5000 + j);
        }
    }
    FakeReturn* faktopologyeret = new FakeReturn(nullptr,
        static_cast<void*>(&response_1));
    topologyservice.SetFakeReturn(faktopologyeret);

    curve::client::MetaCache mc;
    curve::client::ChunkIDInfo_t cinfo;
    ASSERT_EQ(MetaCacheErrorType::CHUNKINFO_NOT_FOUND,
                mc.GetChunkInfoByIndex(0, &cinfo));

    SegmentInfo segInfo;
    LogicalPoolCopysetIDInfo_t lpcsIDInfo;
    mdsclient_.GetOrAllocateSegment(true, 0, &fi, &segInfo);
    int count = 0;
    for (auto iter : segInfo.chunkvec) {
        uint64_t index = (segInfo.startoffset +
                            count*fi.chunksize)/ fi.chunksize;
        mc.UpdateChunkInfoByIndex(index, iter);
        ++count;
    }

    std::vector<CopysetInfo> cpinfoVec;
    mdsclient_.GetServerList(segInfo.lpcpIDInfo.lpid,
                            segInfo.lpcpIDInfo.cpidVec, &cpinfoVec);
    for (auto iter : cpinfoVec) {
        iter.UpdateLeaderIndex(0);
        mc.UpdateCopysetInfo(segInfo.lpcpIDInfo.lpid,
        iter.cpid_, iter);
    }
    for (int i = 0; i < 256; i++) {
        ASSERT_EQ(MetaCacheErrorType::OK, mc.GetChunkInfoByIndex(i, &cinfo));
        ASSERT_EQ(cinfo.lpid_, 1234);
        ASSERT_EQ(cinfo.cpid_, i);
        ASSERT_EQ(cinfo.cid_, i);
    }

    curve::client::EndPoint ep;
    butil::str2endpoint("127.0.0.1", 5000, &ep);
    curve::client::ChunkServerAddr pd(ep);

    for (int i = 0; i < 256; i++) {
        auto serverlist = mc.GetServerList(1234, i);
        ASSERT_TRUE(serverlist.IsValid());
        int chunkserverid = i * 3 + 1;

        uint32_t csid;
        curve::client::EndPoint temp;
        mc.GetLeader(1234, i, &csid, &temp);
        ASSERT_EQ(csid, chunkserverid);
        ASSERT_EQ(temp, ep);
        for (int j = 0; j < 3; j++) {
            auto csInfo = serverlist.csinfos_[j];
            ASSERT_EQ(csInfo.chunkserverID, chunkserverid++);
            curve::client::EndPoint ep1;
            butil::str2endpoint("127.0.0.1", 5000 + j, &ep1);
            curve::client::ChunkServerAddr pd1(ep1);
            curve::client::EndPoint ep2;
            butil::str2endpoint("127.0.0.1", 5000 + j, &ep2);
            curve::client::ChunkServerAddr pd2(ep2);
            ASSERT_EQ(pd1, csInfo.internalAddr);
            ASSERT_EQ(pd2, csInfo.externalAddr);
        }
    }

    GetChunkServerListInCopySetsResponse response_2;
    response_2.set_statuscode(-1);
    FakeReturn* faktopologyeret_2 = new FakeReturn(nullptr,
        static_cast<void*>(&response_2));
    topologyservice.SetFakeReturn(faktopologyeret_2);

    uint32_t csid;
    curve::client::EndPoint temp;
    ASSERT_EQ(-1, mc.GetLeader(2345, 0, &csid, &temp));

    curve::client::EndPoint ep1;
    butil::str2endpoint("127.0.0.1", 5001, &ep1);
    mc.UpdateLeader(1234, 0, ep1);

    curve::client::EndPoint toep;
    ChunkServerID cid;
    mc.GetLeader(1234, 0, &cid, &toep, false);

    ASSERT_EQ(ep1, toep);
    ASSERT_EQ(0, mc.UpdateLeader(1234, 0, ep1));

    ASSERT_EQ(0, mc.GetAppliedIndex(1111, 0));

    // test applied index update
    curve::client::CopysetInfo csinfo;
    mc.UpdateCopysetInfo(111, 123, csinfo);
    ASSERT_EQ(0, mc.GetAppliedIndex(111, 123));
    mc.UpdateAppliedIndex(111, 123, 4);
    ASSERT_EQ(4, mc.GetAppliedIndex(111, 123));
    mc.UpdateAppliedIndex(111, 123, 100000);
    ASSERT_EQ(100000, mc.GetAppliedIndex(111, 123));

    // Boundary test metacache.
    // we fake the disk size = 1G.
    // and the chunksize = 4M.
    // so if visit the chunk index > 255
    // will return failed.
    curve::client::ChunkIDInfo_t chunkinfo;
    ASSERT_EQ(MetaCacheErrorType::CHUNKINFO_NOT_FOUND, mc.GetChunkInfoByIndex(256, &chunkinfo));   // NOLINT
    curve::client::LogicPoolID lpid = 1234;
    curve::client::CopysetID copyid = 0;
    curve::client::ChunkServerAddr pid;
    std::vector<CopysetPeerInfo> conf;
    GetLeaderInfo getLeaderInfo(lpid, copyid, conf, 10);
    ASSERT_EQ(-1, ServiceHelper::GetLeader(getLeaderInfo, &pid));

    curve::client::EndPoint ep11, ep22, ep33;
    butil::str2endpoint("127.0.0.1", 7777, &ep11);
    curve::client::ChunkServerAddr pd11(ep11);
    butil::str2endpoint("127.0.0.1", 7777, &ep22);
    curve::client::ChunkServerAddr pd22(ep22);
    butil::str2endpoint("127.0.0.1", 7777, &ep33);
    curve::client::ChunkServerAddr pd33(ep33);

    conf.push_back(CopysetPeerInfo(1, pd11, pd11));
    conf.push_back(CopysetPeerInfo(2, pd22, pd22));
    conf.push_back(CopysetPeerInfo(3, pd33, pd33));
    GetLeaderInfo getLeaderInfo2(lpid, copyid, conf, 10);
    ASSERT_EQ(-1, ServiceHelper::GetLeader(getLeaderInfo2, &pid));

    delete fakeret;
    delete faktopologyeret;
}

TEST_F(MDSClientTest, GetServerList) {
    brpc::Server server;

    ::curve::mds::topology::GetChunkServerListInCopySetsResponse response_1;
    response_1.set_statuscode(0);
    uint32_t chunkserveridc = 1;

    ::curve::mds::topology::ChunkServerLocation* cslocs;
    ::curve::mds::topology::CopySetServerInfo* csinfo;
    for (int j = 0; j < 256; j++) {
        csinfo = response_1.add_csinfo();
        csinfo->set_copysetid(j);
        for (int i = 0; i < 3; i++) {
            cslocs = csinfo->add_cslocs();
            cslocs->set_chunkserverid(chunkserveridc++);
            cslocs->set_hostip("10.182.26.2");
            cslocs->set_externalip("127.0.0.1");
            cslocs->set_port(5000);
        }
    }

    FakeReturn* faktopologyeret = new FakeReturn(nullptr,
        static_cast<void*>(&response_1));
    topologyservice.SetFakeReturn(faktopologyeret);

    std::vector<curve::client::CopysetID> cpidvec;
    for (int i = 0; i < 256; i++) {
        cpidvec.push_back(i);
    }

    std::vector<CopysetInfo> cpinfoVec;
    curve::client::MetaCache mc;
    ASSERT_NE(LIBCURVE_ERROR::FAILED,
                mdsclient_.GetServerList(1234, cpidvec, &cpinfoVec));
    for (auto iter : cpinfoVec) {
        mc.UpdateCopysetInfo(1234, iter.cpid_, iter);
    }

    for (int i = 0; i < 256; i++) {
        auto serverlist = mc.GetServerList(1234, i);
        ASSERT_TRUE(serverlist.IsValid());
        int chunkserverid = i * 3 + 1;
        for (int j = 0; j < 3; j++) {
            auto csInfo = serverlist.csinfos_[j];
            curve::client::EndPoint ep1;
            butil::str2endpoint("10.182.26.2", 5000, &ep1);
            curve::client::ChunkServerAddr pd1(ep1);
            curve::client::EndPoint ep2;
            butil::str2endpoint("127.0.0.1", 5000, &ep2);
            curve::client::ChunkServerAddr pd2(ep2);
            ASSERT_EQ(csInfo.chunkserverID, chunkserverid++);
            ASSERT_EQ(pd1, csInfo.internalAddr);
            ASSERT_EQ(pd2, csInfo.externalAddr);
        }
    }
}

TEST_F(MDSClientTest, GetLeaderTest) {
    brpc::Server chunkserver1;
    brpc::Server chunkserver2;
    brpc::Server chunkserver3;
    brpc::Server chunkserver4;

    FakeCliService cliservice1;
    FakeCliService cliservice2;
    FakeCliService cliservice3;
    FakeCliService cliservice4;

    if (chunkserver1.AddService(&cliservice1,
                          brpc::SERVER_DOESNT_OWN_SERVICE) != 0) {
        LOG(FATAL) << "Fail to add service";
    }

    if (chunkserver2.AddService(&cliservice2,
                          brpc::SERVER_DOESNT_OWN_SERVICE) != 0) {
        LOG(FATAL) << "Fail to add service";
    }

    if (chunkserver3.AddService(&cliservice3,
                          brpc::SERVER_DOESNT_OWN_SERVICE) != 0) {
        LOG(FATAL) << "Fail to add service";
    }

    if (chunkserver4.AddService(&cliservice4,
                          brpc::SERVER_DOESNT_OWN_SERVICE) != 0) {
        LOG(FATAL) << "Fail to add service";
    }

    brpc::ServerOptions options;
    options.idle_timeout_sec = -1;
    if (chunkserver1.Start("127.0.0.1:9120", &options) != 0) {
        LOG(ERROR) << "Fail to start Server";
    }

    if (chunkserver2.Start("127.0.0.1:9121", &options) != 0) {
        LOG(ERROR) << "Fail to start Server";
    }

    if (chunkserver3.Start("127.0.0.1:9122", &options) != 0) {
        LOG(ERROR) << "Fail to start Server";
    }

    if (chunkserver4.Start("127.0.0.1:9123", &options) != 0) {
        LOG(ERROR) << "Fail to start Server";
    }

    curve::client::MetaCache mc;
    MetaCacheOption mcOpt;
    mc.Init(mcOpt, &mdsclient_);
    curve::client::CopysetInfo cslist;

    curve::client::ChunkServerAddr pd1;
    pd1.Parse("10.182.26.2:9120:0");
    curve::client::ChunkServerAddr pd2;
    pd2.Parse("127.0.0.1:9120:0");
    curve::client::CopysetPeerInfo peerinfo_1(1, pd1, pd2);
    cslist.AddCopysetPeerInfo(peerinfo_1);

    pd1.addr_.port = 9121;
    pd2.addr_.port = 9121;
    curve::client::CopysetPeerInfo peerinfo_2(2, pd1, pd2);
    cslist.AddCopysetPeerInfo(peerinfo_2);

    pd1.addr_.port = 9122;
    pd2.addr_.port = 9122;
    curve::client::CopysetPeerInfo peerinfo_3(3, pd1, pd2);
    cslist.AddCopysetPeerInfo(peerinfo_3);

    mc.UpdateCopysetInfo(1234, 1234, cslist);

    // 测试复制组里第三个addr为leader
    curve::chunkserver::GetLeaderResponse2 response1;
    curve::common::Peer *peer1 = new curve::common::Peer();
    peer1->set_address(peerinfo_3.internalAddr.ToString());
    response1.set_allocated_leader(peer1);
    FakeReturn fakeret1(nullptr, static_cast<void*>(&response1));
    cliservice1.SetFakeReturn(&fakeret1);
    cliservice2.SetFakeReturn(&fakeret1);
    cliservice3.SetFakeReturn(&fakeret1);

    curve::client::ChunkServerID ckid;
    curve::client::EndPoint leaderep;

    cliservice1.CleanInvokeTimes();
    cliservice2.CleanInvokeTimes();
    cliservice3.CleanInvokeTimes();

    ASSERT_EQ(0, mc.GetLeader(1234, 1234, &ckid, &leaderep, true));
    ASSERT_EQ(ckid, 3);
    curve::client::EndPoint expected;
    butil::str2endpoint("127.0.0.1", 9122, &expected);
    ASSERT_EQ(expected, leaderep);

    // 测试拉取新leader失败，需要到mds重新fetch新的serverlist
    // 当前新leader是3，尝试再刷新leader，这个时候会从1， 2获取leader
    // 但是这时候leader找不到了，于是就会触发向mds重新拉取最新的server list
    brpc::Controller controller11;
    controller11.SetFailed(-1, "error");
    FakeReturn fakeret111(&controller11, static_cast<void*>(&response1));
    cliservice1.SetFakeReturn(&fakeret111);
    cliservice2.SetFakeReturn(&fakeret111);
    cliservice3.SetFakeReturn(&fakeret111);

    ::curve::mds::topology::GetChunkServerListInCopySetsResponse response_1;
    response_1.set_statuscode(0);
    uint32_t chunkserveridc = 1;

    ::curve::mds::topology::ChunkServerLocation* cslocs;
    ::curve::mds::topology::CopySetServerInfo* csinfo;
    csinfo = response_1.add_csinfo();
    csinfo->set_copysetid(1234);
    for (int i = 0; i < 4; i++) {
        cslocs = csinfo->add_cslocs();
        cslocs->set_chunkserverid(chunkserveridc++);
        cslocs->set_hostip("10.182.26.2");
        cslocs->set_externalip("127.0.0.1");
        cslocs->set_port(9120 + i);
    }

    FakeReturn* faktopologyeret = new FakeReturn(nullptr,
        static_cast<void*>(&response_1));
    topologyservice.SetFakeReturn(faktopologyeret);

    cliservice1.CleanInvokeTimes();
    cliservice2.CleanInvokeTimes();
    cliservice3.CleanInvokeTimes();

    // 向当前集群中拉取leader，然后会从mds一侧获取新server list
    ASSERT_EQ(0, mc.GetLeader(1234, 1234, &ckid, &leaderep, true));

    // getleader请求会跳过当前leader
    ASSERT_EQ(0, cliservice3.GetInvokeTimes());

    // 因为从mds获取新的copyset信息了，所以其leader信息被重置了，需要重新获取新leader
    // 获取新新的leader，这时候会从1，2，3，4这四个server拉取新leader，并成功获取新leader
    std::string leader = "10.182.26.2:9123:0";
    peer1 = new curve::common::Peer();
    peer1->set_address(leader);
    peer1->set_id(4321);
    response1.set_allocated_leader(peer1);
    fakeret1 = FakeReturn(nullptr, static_cast<void*>(&response1));

    cliservice1.SetFakeReturn(&fakeret1);
    cliservice2.SetFakeReturn(&fakeret1);
    cliservice3.SetFakeReturn(&fakeret1);
    cliservice4.SetFakeReturn(&fakeret1);

    LOG(INFO) << "get leader test for nameing service";
    ASSERT_EQ(0, mc.GetLeader(1234, 1234, &ckid, &leaderep, true));
    butil::str2endpoint("127.0.0.1", 9123, &expected);
    ASSERT_EQ(expected, leaderep);

    cliservice1.CleanInvokeTimes();
    cliservice2.CleanInvokeTimes();
    cliservice3.CleanInvokeTimes();
    cliservice4.CleanInvokeTimes();

    // refresh为false，所以只会从metacache中获取，不会发起rpc请求
    ASSERT_EQ(0, mc.GetLeader(1234, 1234, &ckid, &leaderep, false));
    ASSERT_EQ(expected, leaderep);
    ASSERT_EQ(0, cliservice1.GetInvokeTimes());
    ASSERT_EQ(0, cliservice2.GetInvokeTimes());
    ASSERT_EQ(0, cliservice3.GetInvokeTimes());
    ASSERT_EQ(0, cliservice4.GetInvokeTimes());

    // 测试新增一个leader，该节点不在配置组内, 然后通过向mds
    // 查询其chunkserverInfo之后, 将其成功插入metacache
    curve::common::Peer *peer7 = new curve::common::Peer();
    leader = "10.182.26.2:9124:0";
    peer7->set_address(leader);
    response1.set_allocated_leader(peer7);
    FakeReturn fakeret44(nullptr, static_cast<void*>(&response1));
    cliservice1.SetFakeReturn(&fakeret44);
    cliservice2.SetFakeReturn(&fakeret44);
    cliservice3.SetFakeReturn(&fakeret44);
    cliservice4.SetFakeReturn(&fakeret44);

    cliservice1.CleanInvokeTimes();
    cliservice2.CleanInvokeTimes();
    cliservice3.CleanInvokeTimes();

    mc.GetLeader(1234, 1234, &ckid, &leaderep, true);

    CopysetInfo cpinfo = mc.GetServerList(1234, 1234);
    ASSERT_EQ(cpinfo.csinfos_.size(), 5);
    curve::client::CopysetPeerInfo cpeer;
    cpeer.internalAddr.Parse("10.182.26.2:9124:0");
    cpeer.externalAddr.Parse("127.0.0.1:9124:0");
    auto it = std::find(cpinfo.csinfos_.begin(), cpinfo.csinfos_.end(), cpeer);
    ASSERT_NE(it, cpinfo.csinfos_.end());

    chunkserver1.Stop(0);
    chunkserver1.Join();
    chunkserver2.Stop(0);
    chunkserver2.Join();
    chunkserver3.Stop(0);
    chunkserver3.Join();
    chunkserver4.Stop(0);
    chunkserver4.Join();
}


TEST_F(MDSClientTest, GetFileInfoException) {
    std::string filename = "/1_userinfo_";
    FakeReturn* fakeret = nullptr;
    curve::client::FInfo_t* finfo = nullptr;
    {
        curve::mds::FileInfo* info = new curve::mds::FileInfo;
        ::curve::mds::GetFileInfoResponse response;
        response.set_statuscode(::curve::mds::StatusCode::kOK);
        response.set_allocated_fileinfo(info);

        fakeret = new FakeReturn(nullptr,
                static_cast<void*>(&response));
        curvefsservice.SetGetFileInfoFakeReturn(fakeret);

        finfo = new curve::client::FInfo_t;
        ASSERT_EQ(LIBCURVE_ERROR::OK,
                mdsclient_.GetFileInfo(filename, userinfo, finfo));
    }

    {
        curve::mds::FileInfo * info = new curve::mds::FileInfo;
        ::curve::mds::GetFileInfoResponse response;
        response.set_statuscode(::curve::mds::StatusCode::kOK);
        info->clear_parentid();
        info->clear_id();
        info->clear_filetype();
        info->clear_chunksize();
        info->clear_length();
        info->clear_ctime();
//        info->clear_snapshotid();
        info->clear_segmentsize();
        response.set_allocated_fileinfo(info);

        fakeret = new FakeReturn(nullptr,
                static_cast<void*>(&response));
        curvefsservice.SetGetFileInfoFakeReturn(fakeret);

        finfo = new curve::client::FInfo_t;
        ASSERT_EQ(LIBCURVE_ERROR::OK,
                mdsclient_.GetFileInfo(filename, userinfo, finfo));
    }

    delete fakeret;
    delete finfo;
}

TEST_F(MDSClientTest, CreateCloneFile) {
    std::string filename = "/1_userinfo_";

    FInfo finfo;
    curve::mds::FileInfo * info = new curve::mds::FileInfo;

    // 设置rpc失败，触发重试
    brpc::Controller cntl;
    cntl.SetFailed(-1, "failed");

    curve::mds::CreateCloneFileResponse response;
    response.set_statuscode(::curve::mds::StatusCode::kFileNotExists);

    FakeReturn* fakecreateclone
     = new FakeReturn(&cntl, static_cast<void*>(&response));

    curvefsservice.SetCreateCloneFile(fakecreateclone);
    curvefsservice.CleanRetryTimes();

    ASSERT_EQ(LIBCURVE_ERROR::FAILED,
              mdsclient_.CreateCloneFile("source", "destination", userinfo,
                                         10 * 1024 * 1024, 0, 4 * 1024 * 1024,
                                         0, 0,
                                         &finfo));
    // 认证失败
    curve::mds::CreateCloneFileResponse response1;
    response1.set_statuscode(::curve::mds::StatusCode::kOwnerAuthFail);

    FakeReturn* fakecreateclone1
     = new FakeReturn(nullptr, static_cast<void*>(&response1));

    curvefsservice.SetCreateCloneFile(fakecreateclone1);

    ASSERT_EQ(LIBCURVE_ERROR::AUTHFAIL,
              mdsclient_.CreateCloneFile("source", "destination", userinfo,
                                         10 * 1024 * 1024, 0, 4 * 1024 * 1024,
                                         0, 0,
                                         &finfo));
    // 请求成功
    info->set_id(5);
    curve::mds::CreateCloneFileResponse response2;
    response2.set_statuscode(::curve::mds::StatusCode::kOK);
    response2.set_allocated_fileinfo(info);

    FakeReturn* fakecreateclone2
     = new FakeReturn(nullptr, static_cast<void*>(&response2));

    curvefsservice.SetCreateCloneFile(fakecreateclone2);

    std::string cloneSource = "/clone/source";
    uint64_t cloneLength = 1 * 1024 * 1024 * 1024;
    info->set_clonesource(cloneSource);
    info->set_clonelength(cloneLength);
    ASSERT_EQ(LIBCURVE_ERROR::OK,
              mdsclient_.CreateCloneFile("source", "destination", userinfo,
                                         10 * 1024 * 1024, 0, 4 * 1024 * 1024,
                                         0, 0,
                                         &finfo));
    ASSERT_EQ(5, finfo.id);
    ASSERT_EQ(cloneSource, finfo.sourceInfo.name);
    ASSERT_EQ(cloneLength, finfo.sourceInfo.length);
}

TEST_F(MDSClientTest, CompleteCloneMeta) {
    std::string filename = "/1_userinfo_";
    // 设置rpc失败，触发重试
    brpc::Controller cntl;
    cntl.SetFailed(-1, "failed");

    curve::mds::SetCloneFileStatusResponse response;
    response.set_statuscode(::curve::mds::StatusCode::kFileNotExists);

    FakeReturn* fakecreateclone
     = new FakeReturn(&cntl, static_cast<void*>(&response));

    curvefsservice.SetCloneFileStatus(fakecreateclone);
    curvefsservice.CleanRetryTimes();

    ASSERT_EQ(LIBCURVE_ERROR::FAILED, mdsclient_.CompleteCloneMeta(
                                                            "destination",
                                                            userinfo));

    // 认证失败
    curve::mds::SetCloneFileStatusResponse response1;
    response1.set_statuscode(::curve::mds::StatusCode::kOwnerAuthFail);

    FakeReturn* fakecreateclone1
     = new FakeReturn(nullptr, static_cast<void*>(&response1));

    curvefsservice.SetCloneFileStatus(fakecreateclone1);

    ASSERT_EQ(LIBCURVE_ERROR::AUTHFAIL, mdsclient_.CompleteCloneMeta(
                                                            "destination",
                                                            userinfo));
    // 请求成功
    curve::mds::SetCloneFileStatusResponse response2;
    response2.set_statuscode(::curve::mds::StatusCode::kOK);

    FakeReturn* fakecreateclone2
     = new FakeReturn(nullptr, static_cast<void*>(&response2));

    curvefsservice.SetCloneFileStatus(fakecreateclone2);

    ASSERT_EQ(LIBCURVE_ERROR::OK, mdsclient_.CompleteCloneMeta("destination",
                                                            userinfo));
}

TEST_F(MDSClientTest, CompleteCloneFile) {
    std::string filename = "/1_userinfo_";

    // 设置rpc失败，触发重试
    brpc::Controller cntl;
    cntl.SetFailed(-1, "failed");

    curve::mds::SetCloneFileStatusResponse response;
    response.set_statuscode(::curve::mds::StatusCode::kFileNotExists);

    FakeReturn* fakecreateclone
     = new FakeReturn(&cntl, static_cast<void*>(&response));

    curvefsservice.SetCloneFileStatus(fakecreateclone);
    curvefsservice.CleanRetryTimes();

    ASSERT_EQ(LIBCURVE_ERROR::FAILED, mdsclient_.CompleteCloneFile(
                                                            "destination",
                                                            userinfo));

    // 认证失败
    curve::mds::SetCloneFileStatusResponse response1;
    response1.set_statuscode(::curve::mds::StatusCode::kOwnerAuthFail);

    FakeReturn* fakecreateclone1
     = new FakeReturn(nullptr, static_cast<void*>(&response1));

    curvefsservice.SetCloneFileStatus(fakecreateclone1);

    ASSERT_EQ(LIBCURVE_ERROR::AUTHFAIL, mdsclient_.CompleteCloneFile(
                                                            "destination",
                                                            userinfo));
    // 请求成功
    curve::mds::SetCloneFileStatusResponse response2;
    response2.set_statuscode(::curve::mds::StatusCode::kOK);

    FakeReturn* fakecreateclone2
     = new FakeReturn(nullptr, static_cast<void*>(&response2));

    curvefsservice.SetCloneFileStatus(fakecreateclone2);

    ASSERT_EQ(LIBCURVE_ERROR::OK, mdsclient_.CompleteCloneFile("destination",
                                                            userinfo));
}

TEST_F(MDSClientTest, ChangeOwner) {
    std::string filename1 = "/1_userinfo_";
    UserInfo_t          userinfo;
    userinfo.owner = "root";
    userinfo.password = "rootpwd";

    // set response file not exist
    ::curve::mds::ChangeOwnerResponse response;
    response.set_statuscode(::curve::mds::StatusCode::kFileNotExists);

    FakeReturn* fakeret
     = new FakeReturn(nullptr, static_cast<void*>(&response));

    curvefsservice.SetChangeOwner(fakeret);

    int ret = globalclient->ChangeOwner(filename1, "newowner", userinfo);
    ASSERT_EQ(ret, -1 * LIBCURVE_ERROR::NOTEXIST);

    // set extend file ok
    ::curve::mds::ChangeOwnerResponse response1;
    response1.set_statuscode(::curve::mds::StatusCode::kOK);

    FakeReturn* fakeret1
     = new FakeReturn(nullptr, static_cast<void*>(&response1));

    curvefsservice.SetChangeOwner(fakeret1);
    ASSERT_EQ(LIBCURVE_ERROR::OK, globalclient->ChangeOwner(filename1,
                                                          "newowner",
                                                          userinfo));

    // set file dir not exists
    ::curve::mds::ChangeOwnerResponse response2;
    response2.set_statuscode(::curve::mds::StatusCode::kDirNotExist);

    FakeReturn* fakeret3
     = new FakeReturn(nullptr, static_cast<void*>(&response2));

    curvefsservice.SetChangeOwner(fakeret3);
    ASSERT_EQ(-1 * LIBCURVE_ERROR::NOTEXIST,
              globalclient->ChangeOwner(filename1, "newowner", userinfo));

    // set file auth fail
    ::curve::mds::ChangeOwnerResponse response3;
    response3.set_statuscode(::curve::mds::StatusCode::kOwnerAuthFail);

    FakeReturn* fakeret4
     = new FakeReturn(nullptr, static_cast<void*>(&response3));

    curvefsservice.SetChangeOwner(fakeret4);
    ASSERT_EQ(-1 * LIBCURVE_ERROR::AUTHFAIL,
              globalclient->ChangeOwner(filename1, "newowner", userinfo));

    // set file mds storage error
    ::curve::mds::ChangeOwnerResponse response4;
    response4.set_statuscode(::curve::mds::StatusCode::kStorageError);

    FakeReturn* fakeret5
     = new FakeReturn(nullptr, static_cast<void*>(&response4));

    curvefsservice.SetChangeOwner(fakeret5);
    ASSERT_EQ(-1 * LIBCURVE_ERROR::INTERNAL_ERROR,
              globalclient->ChangeOwner(filename1, "newowner", userinfo));

    // 设置rpc失败，触发重试
    brpc::Controller cntl;
    cntl.SetFailed(-1, "failed");

    FakeReturn* fakeret2
     = new FakeReturn(&cntl, static_cast<void*>(&response));

    curvefsservice.SetChangeOwner(fakeret2);
    curvefsservice.CleanRetryTimes();

    ASSERT_EQ(-1 * LIBCURVE_ERROR::FAILED, globalclient->ChangeOwner(filename1,
                                                                   "newowner",
                                                                    userinfo));

    delete fakeret;
    delete fakeret2;
}

TEST_F(MDSClientTest, ListChunkServerTest_CntlFailed) {
    ListChunkServerResponse response;
    std::vector<ChunkServerID> csIds;
    std::string ip = "127.0.0.1:6666";

    brpc::Controller cntl;
    cntl.SetFailed(-1, "Failed");

    std::unique_ptr<FakeReturn> fakeret(
        new FakeReturn(&cntl, static_cast<void*>(&response)));
    topologyservice.SetFakeReturn(fakeret.get());

    auto startTime = curve::common::TimeUtility::GetTimeofDayMs();
    ASSERT_EQ(LIBCURVE_ERROR::FAILED,
        mdsclient_.ListChunkServerInServer(ip, &csIds));
    auto endTime = curve::common::TimeUtility::GetTimeofDayMs();
    ASSERT_GT(endTime - startTime, metaopt.mdsMaxRetryMS - 1);
}

TEST_F(MDSClientTest, ListChunkServerTest_ResponseError) {
    ListChunkServerResponse response;
    response.set_statuscode(-2);

    std::vector<ChunkServerID> csIds;
    std::string ip = "127.0.0.1:6666";

    std::unique_ptr<FakeReturn> fakeret(
        new FakeReturn(nullptr, static_cast<void*>(&response)));
    topologyservice.SetFakeReturn(fakeret.get());

    ASSERT_EQ(LIBCURVE_ERROR::FAILED,
        mdsclient_.ListChunkServerInServer(ip, &csIds));
}

TEST_F(MDSClientTest, ListChunkServerTest_ResponseOK) {
    ListChunkServerResponse response;
    std::vector<ChunkServerID> csIds{1, 10, 100};

    response.set_statuscode(0);
    for (auto id : csIds) {
        auto csInfo = response.add_chunkserverinfos();
        csInfo->set_chunkserverid(id);
        csInfo->set_disktype("nvme");
        csInfo->set_hostip("127.0.0.1:" + std::to_string(id));
        csInfo->set_port(id);
        csInfo->set_status(curve::mds::topology::ChunkServerStatus::READWRITE);
        csInfo->set_diskstatus(curve::mds::topology::DiskState::DISKNORMAL);
        csInfo->set_onlinestate(curve::mds::topology::OnlineState::ONLINE);
        csInfo->set_mountpoint("/data/" + std::to_string(id));
        csInfo->set_diskcapacity(1234);
        csInfo->set_diskused(100);
    }
    std::string ip = "127.0.0.1:6666";

    std::unique_ptr<FakeReturn> fakeret(
        new FakeReturn(nullptr, static_cast<void*>(&response)));
    topologyservice.SetFakeReturn(fakeret.get());

    std::vector<ChunkServerID> returnIds;
    ASSERT_EQ(LIBCURVE_ERROR::OK,
        mdsclient_.ListChunkServerInServer(ip, &returnIds));

    std::sort(returnIds.begin(), returnIds.end());
    ASSERT_EQ(csIds, returnIds);
}

TEST_F(MDSClientTest, ListDir) {
    std::string filename1 = "/1_userinfo_";
    // set response file not exist
    ::curve::mds::ListDirResponse response;
    response.set_statuscode(::curve::mds::StatusCode::kFileNotExists);

    FakeReturn* fakeret
     = new FakeReturn(nullptr, static_cast<void*>(&response));

    curvefsservice.SetListDir(fakeret);

    int arrsize;
    std::vector<FileStatInfo> filestatVec;
    int ret = globalclient->Listdir(filename1, userinfo, &filestatVec);
    ASSERT_EQ(ret, -1 * LIBCURVE_ERROR::NOTEXIST);

    // set extend file ok
    ::curve::mds::ListDirResponse response1;
    response1.set_statuscode(::curve::mds::StatusCode::kOK);

    for (int i = 0; i < 5; i++) {
        auto fin = response1.add_fileinfo();
        fin->set_filename("_filename_");
        fin->set_id(i);
        fin->set_parentid(i);
        fin->set_filetype(curve::mds::FileType::INODE_PAGEFILE);
        fin->set_chunksize(4 * 1024 * 1024);
        fin->set_length(i * 1024 * 1024 * 1024ul);
        fin->set_ctime(12345678);
        fin->set_seqnum(i);
        fin->set_segmentsize(1 * 1024 * 1024 * 1024ul);
        fin->set_owner("test");
    }

    FakeReturn* fakeret1
     = new FakeReturn(nullptr, static_cast<void*>(&response1));

    curvefsservice.SetListDir(fakeret1);
    ASSERT_EQ(LIBCURVE_ERROR::OK, globalclient->Listdir(filename1,
                                                      userinfo,
                                                      &filestatVec));
    int arraysize = 0;
    C_UserInfo_t cuserinfo;
    memcpy(cuserinfo.owner, "test", 5);
    FileStatInfo* filestat = new FileStatInfo[5];
    DirInfo_t* dir = OpenDir(filename1.c_str(), &cuserinfo);
    ASSERT_NE(dir, nullptr);
    ASSERT_EQ(-LIBCURVE_ERROR::FAILED, Listdir(nullptr));
    ASSERT_EQ(LIBCURVE_ERROR::OK, Listdir(dir));
    for (int i = 0; i < 5; i++) {
        ASSERT_EQ(dir->fileStat[i].id, i);
        ASSERT_EQ(dir->fileStat[i].parentid, i);
        ASSERT_EQ(dir->fileStat[i].ctime, 12345678);
        ASSERT_EQ(dir->fileStat[i].length, i * 1024 * 1024 * 1024ul);
        ASSERT_EQ(dir->fileStat[i].filetype,
                  curve::mds::FileType::INODE_PAGEFILE);
        ASSERT_EQ(0, strcmp(dir->fileStat[i].owner, "test"));
        ASSERT_EQ(0, strcmp(dir->fileStat[i].filename, "_filename_"));
    }

    CloseDir(dir);
    ASSERT_NO_THROW(CloseDir(nullptr));

    for (int i = 0; i < 5; i++) {
        ASSERT_EQ(filestatVec[i].id, i);
        ASSERT_EQ(filestatVec[i].parentid, i);
        ASSERT_EQ(filestatVec[i].ctime, 12345678);
        ASSERT_EQ(filestatVec[i].length, i * 1024 * 1024 * 1024ul);
        ASSERT_EQ(filestatVec[i].filetype,
                  curve::mds::FileType::INODE_PAGEFILE);
        ASSERT_EQ(0, strcmp(filestatVec[i].owner, "test"));
        ASSERT_EQ(0, strcmp(filestatVec[i].filename, "_filename_"));
    }
    delete[] filestat;

    // set file dir not exists
    ::curve::mds::ListDirResponse response2;
    response2.set_statuscode(::curve::mds::StatusCode::kDirNotExist);

    FakeReturn* fakeret3
     = new FakeReturn(nullptr, static_cast<void*>(&response2));

    curvefsservice.SetListDir(fakeret3);
    ASSERT_EQ(-1 * LIBCURVE_ERROR::NOTEXIST,
              globalclient->Listdir(filename1, userinfo, &filestatVec));

    // set file auth fail
    ::curve::mds::ListDirResponse response3;
    response3.set_statuscode(::curve::mds::StatusCode::kOwnerAuthFail);

    FakeReturn* fakeret4
     = new FakeReturn(nullptr, static_cast<void*>(&response3));

    curvefsservice.SetListDir(fakeret4);
    ASSERT_EQ(-1 * LIBCURVE_ERROR::AUTHFAIL,
              globalclient->Listdir(filename1, userinfo, &filestatVec));

    // set file mds storage error
    ::curve::mds::ListDirResponse response4;
    response4.set_statuscode(::curve::mds::StatusCode::kStorageError);

    FakeReturn* fakeret5
     = new FakeReturn(nullptr, static_cast<void*>(&response4));

    curvefsservice.SetListDir(fakeret5);
    ASSERT_EQ(-1 * LIBCURVE_ERROR::INTERNAL_ERROR,
              globalclient->Listdir(filename1, userinfo, &filestatVec));

    // 设置rpc失败，触发重试
    brpc::Controller cntl;
    cntl.SetFailed(-1, "failed");

    FakeReturn* fakeret2
     = new FakeReturn(&cntl, static_cast<void*>(&response));

    curvefsservice.SetListDir(fakeret2);
    curvefsservice.CleanRetryTimes();

    ASSERT_EQ(-1 * LIBCURVE_ERROR::FAILED,
                    globalclient->Listdir(filename1, userinfo, &filestatVec));

    delete fakeret;
    delete fakeret2;
}

TEST(LibcurveInterface, InvokeWithOutInit) {
    CurveAioContext aioctx;
    UserInfo_t      userinfo;
    C_UserInfo_t*    ui;

    FileClient fc;
    ASSERT_EQ(-LIBCURVE_ERROR::FAILED, fc.Create("", userinfo, 0));
    ASSERT_EQ(-LIBCURVE_ERROR::FAILED, fc.Listdir("", userinfo, nullptr));
    ASSERT_EQ(-LIBCURVE_ERROR::FAILED, fc.Mkdir("nullptr", userinfo));
    ASSERT_EQ(-LIBCURVE_ERROR::FAILED, fc.Rmdir("nullptr", userinfo));
    ASSERT_EQ(-LIBCURVE_ERROR::FAILED, fc.Close(0));
    ASSERT_EQ(-LIBCURVE_ERROR::FAILED, fc.StatFile("", userinfo, nullptr));
    ASSERT_EQ(-LIBCURVE_ERROR::FAILED, fc.ChangeOwner("nullptr", "nullptr", userinfo));  //  NOLINT
    ASSERT_EQ(-LIBCURVE_ERROR::FAILED, fc.Create("", userinfo, 0));
    ASSERT_EQ(-LIBCURVE_ERROR::FAILED, fc.Rename(userinfo, "", ""));
    ASSERT_EQ(-LIBCURVE_ERROR::FAILED, fc.Extend("", userinfo,  0));
    ASSERT_EQ(-LIBCURVE_ERROR::FAILED, fc.Unlink("", userinfo, false));

    ASSERT_EQ(-LIBCURVE_ERROR::FAILED, Open4Qemu(""));
    ASSERT_EQ(-LIBCURVE_ERROR::FAILED, Open4Qemu("/test_dd_"));
    ASSERT_EQ(-LIBCURVE_ERROR::FAILED, Extend4Qemu("", 0));
    ASSERT_EQ(-LIBCURVE_ERROR::FAILED, Extend4Qemu("/test_dd_", 0));
    ASSERT_EQ(-LIBCURVE_ERROR::FAILED, Read(0, nullptr, 0, 0));
    ASSERT_EQ(-LIBCURVE_ERROR::FAILED, Write(0, nullptr, 0, 0));
    ASSERT_EQ(-LIBCURVE_ERROR::FAILED, AioRead(0, &aioctx));
    ASSERT_EQ(-LIBCURVE_ERROR::FAILED, AioWrite(0, &aioctx));
    ASSERT_EQ(-LIBCURVE_ERROR::FAILED, Create("", ui, 0));
    ASSERT_EQ(-LIBCURVE_ERROR::FAILED, Rename(ui, "", ""));
    ASSERT_EQ(-LIBCURVE_ERROR::FAILED, Extend("", ui,  0));
    ASSERT_EQ(-LIBCURVE_ERROR::FAILED, Unlink("", ui));
    ASSERT_EQ(-LIBCURVE_ERROR::FAILED, DeleteForce("", ui));
    ASSERT_EQ(nullptr, OpenDir("", ui));
    ASSERT_EQ(-LIBCURVE_ERROR::FAILED, Listdir(nullptr));
    ASSERT_EQ(-LIBCURVE_ERROR::FAILED, Mkdir(nullptr, ui));
    ASSERT_EQ(-LIBCURVE_ERROR::FAILED, Rmdir(nullptr, ui));
    ASSERT_EQ(-LIBCURVE_ERROR::FAILED, Close(0));
    ASSERT_EQ(-LIBCURVE_ERROR::FAILED, StatFile(nullptr, ui, nullptr));
    ASSERT_EQ(-LIBCURVE_ERROR::FAILED, ChangeOwner(nullptr, nullptr, nullptr));
}

class ServiceHelperGetLeaderTest : public MDSClientTest {
 public:
    using GetLeaderResponse2 = curve::chunkserver::GetLeaderResponse2;

    void SetUp() override {
        // 添加service，并启动server
        for (int i = 0; i < kChunkServerNum; ++i) {
            auto& chunkserver = chunkServers[i];
            auto& fakeCliService = fakeCliServices[i];
            ASSERT_EQ(0, chunkserver.AddService(&fakeCliService,
                brpc::SERVER_DOESNT_OWN_SERVICE)) << "Fail to add service";

            brpc::ServerOptions options;
            options.idle_timeout_sec = -1;

            const auto& ipPort =
                "127.0.0.1:" + std::to_string(chunkserverPorts[i]);
            ASSERT_EQ(0, chunkserver.Start(
                ipPort.c_str(), &options)) << "Fail to start server";
        }

        internalAddrs.resize(kChunkServerNum);
        externalAddrs.resize(kChunkServerNum);
        for (int i = 0; i < kChunkServerNum; ++i) {
            curve::client::EndPoint endpoint;
            butil::str2endpoint("10.182.26.2",
                                chunkserverPorts[i],
                                &endpoint);
            internalAddrs[i] = ChunkServerAddr(endpoint);
            butil::str2endpoint("127.0.0.1",
                                chunkserverPorts[i],
                                &endpoint);
            externalAddrs[i] = ChunkServerAddr(endpoint);
        }

        // 设置copyset peer信息
        for (int i = 0; i < kChunkServerNum; ++i) {
            curve::client::CopysetPeerInfo peerinfo;
            peerinfo.chunkserverID = i + 1;
            peerinfo.internalAddr = internalAddrs[i];
            peerinfo.externalAddr = externalAddrs[i];
            copysetInfo.AddCopysetPeerInfo(peerinfo);
            copysetPeerInfos.push_back(peerinfo);
        }

        ResetAllFakeCliService();
    }

    void ResetAllFakeCliService() {
        for (auto& cliService : fakeCliServices) {
            cliService.CleanInvokeTimes();
            cliService.ClearDelay();
            cliService.ClearErrorCode();
        }
    }

    int GetAllInvokeTimes() {
        int total = 0;
        for (auto& cliService : fakeCliServices) {
            total += cliService.GetInvokeTimes();
        }

        return total;
    }

    void TearDown() override {
        for (auto& server : chunkServers) {
            server.Stop(0);
            server.Join();
        }
    }

    GetLeaderResponse2 MakeResponse(
        const curve::client::ChunkServerAddr& addr) {
        GetLeaderResponse2 response;
        curve::common::Peer* peer = new curve::common::Peer();
        peer->set_address(addr.ToString());
        response.set_allocated_leader(peer);

        return response;
    }

    void SetGetLeaderResponse(const curve::client::ChunkServerAddr& addr) {
        static GetLeaderResponse2 response;
        response = MakeResponse(addr);

        static FakeReturn fakeret(nullptr, nullptr);
        fakeret = FakeReturn(nullptr, static_cast<void*>(&response));

        for (auto& cliService : fakeCliServices) {
            cliService.SetFakeReturn(&fakeret);
        }

        GetLeaderInfo getLeaderInfo(kLogicPoolId, kCopysetId,
            copysetPeerInfos, -1);
        ASSERT_EQ(0, ServiceHelper::GetLeader(getLeaderInfo,
            &leaderAddr, &leaderId, nullptr));

        ResetAllFakeCliService();
    }

    static const int kChunkServerNum = 3;
    static const int kLogicPoolId = 1234;
    static const int kCopysetId = 1234;

    std::vector<int> chunkserverPorts{9120, 9121, 9122};
    brpc::Server chunkServers[kChunkServerNum];
    FakeCliService fakeCliServices[kChunkServerNum];

    std::vector<curve::client::ChunkServerAddr> internalAddrs;
    std::vector<curve::client::ChunkServerAddr> externalAddrs;
    std::vector<CopysetPeerInfo> copysetPeerInfos;

    curve::client::CopysetInfo copysetInfo;

    curve::client::ChunkServerID leaderId;
    curve::client::EndPoint leaderEndPoint;
    curve::client::ChunkServerAddr leaderAddr;
};

TEST_F(ServiceHelperGetLeaderTest, NormalTest) {
    // 测试复制组里第一个chunkserver为leader
    GetLeaderResponse2 response = MakeResponse(internalAddrs[0]);

    FakeReturn fakeret0(nullptr, static_cast<void*>(&response));
    fakeCliServices[0].SetFakeReturn(&fakeret0);

    FakeReturn fakeret1(nullptr, static_cast<void*>(&response));
    fakeCliServices[1].SetFakeReturn(&fakeret1);

    FakeReturn fakeret2(nullptr, static_cast<void*>(&response));
    fakeCliServices[2].SetFakeReturn(&fakeret2);

    GetLeaderRpcOption rpcOption;
    rpcOption.rpcTimeoutMs = 1000;
    GetLeaderInfo getLeaderInfo(kLogicPoolId, kCopysetId,
        copysetPeerInfos, -1, rpcOption);
    ASSERT_EQ(0, ServiceHelper::GetLeader(getLeaderInfo, &leaderAddr,
        &leaderId, nullptr));
    ASSERT_EQ(internalAddrs[0], leaderAddr);

    ResetAllFakeCliService();

    // 测试第二次拉取新的leader，直接跳过第一个chunkserver，查找第2，3两个
    int32_t currentLeaderIndex = 0;
    curve::client::ChunkServerAddr currentLeader =
        internalAddrs[currentLeaderIndex];

    response = MakeResponse(currentLeader);
    fakeret1 = FakeReturn(nullptr, static_cast<void*>(&response));
    fakeCliServices[1].SetFakeReturn(&fakeret1);
    fakeret2 = FakeReturn(nullptr, static_cast<void*>(&response));
    fakeCliServices[2].SetFakeReturn(&fakeret2);

    getLeaderInfo = GetLeaderInfo(kLogicPoolId, kCopysetId,
        copysetPeerInfos, currentLeaderIndex, rpcOption);
    ASSERT_EQ(0, ServiceHelper::GetLeader(getLeaderInfo, &leaderAddr,
        &leaderId, nullptr));

    ASSERT_EQ(currentLeader, leaderAddr);

    ResetAllFakeCliService();

    // 测试第三次获取leader，会跳过第二个chunkserver，重试1/3
    currentLeaderIndex = 1;
    currentLeader = internalAddrs[currentLeaderIndex];

    response = MakeResponse(currentLeader);

    fakeret1 = FakeReturn(nullptr, static_cast<void*>(&response));
    fakeCliServices[1].SetFakeReturn(&fakeret1);
    fakeret2 = FakeReturn(nullptr, static_cast<void*>(&response));
    fakeCliServices[2].SetFakeReturn(&fakeret2);

    getLeaderInfo = GetLeaderInfo(kLogicPoolId, kCopysetId,
        copysetPeerInfos, currentLeaderIndex, rpcOption);
    ASSERT_EQ(0, ServiceHelper::GetLeader(getLeaderInfo, &leaderAddr,
        &leaderId, nullptr));

    ASSERT_EQ(currentLeader, leaderAddr);

    ResetAllFakeCliService();
}

TEST_F(ServiceHelperGetLeaderTest, RpcDelayTest) {
    // 设置第三个chunkserver为leader
    const auto currentLeaderIndex = 2;
    const auto& currentLeader = internalAddrs[2];
    SetGetLeaderResponse(currentLeader);

    // 再次GetLeader会向chunkserver 1/2 发送请求
    // 在chunksever GetLeader service 中加入sleep，触发backup request
    fakeCliServices[0].SetDelayMs(200);
    fakeCliServices[1].SetDelayMs(200);

    GetLeaderRpcOption rpcOption;
    rpcOption.rpcTimeoutMs = 1000;
    GetLeaderInfo getLeaderInfo(kLogicPoolId, kCopysetId,
        copysetPeerInfos, currentLeaderIndex, rpcOption);
    ASSERT_EQ(0, ServiceHelper::GetLeader(getLeaderInfo, &leaderAddr,
        &leaderId, nullptr));

    std::this_thread::sleep_for(std::chrono::seconds(1));

    ASSERT_EQ(currentLeader, leaderAddr);

    fakeCliServices[0].ClearDelay();
    fakeCliServices[1].ClearDelay();
}

TEST_F(ServiceHelperGetLeaderTest, RpcDelayAndExceptionTest) {
    std::vector<int> exceptionErrCodes{
        ENOENT, EAGAIN, EHOSTDOWN,
        ECONNREFUSED, ECONNRESET, brpc::ELOGOFF};

    // 设置第三个chunkserver为leader，GetLeader会向chunkserver 1/2发送请求
    const auto currentLeaderIndex = 2;
    const auto& currentLeader = internalAddrs[currentLeaderIndex];
    SetGetLeaderResponse(currentLeader);

    // 设置第一个chunkserver GetLeader service 延迟
    fakeCliServices[0].SetDelayMs(200);

    // 设置第二个chunkserver 返回对应的错误码
    for (auto errCode : exceptionErrCodes) {
        fakeCliServices[1].SetErrorCode(errCode);
        brpc::Controller controller;
        controller.SetFailed(errCode, "Failed");
        curve::common::Peer* peer = new curve::common::Peer();
        peer->set_address(currentLeader.ToString());
        GetLeaderResponse2 response;
        response.set_allocated_leader(peer);

        FakeReturn fakeret(&controller, static_cast<void*>(&response));
        fakeCliServices[1].SetFakeReturn(&fakeret);

        GetLeaderRpcOption rpcOption;
        rpcOption.rpcTimeoutMs = 1000;
        GetLeaderInfo getLeaderInfo(kLogicPoolId, kCopysetId,
            copysetPeerInfos, currentLeaderIndex, rpcOption);

        ASSERT_EQ(0, ServiceHelper::GetLeader(getLeaderInfo, &leaderAddr,
            &leaderId, nullptr));

        std::this_thread::sleep_for(std::chrono::seconds(1));
        ASSERT_EQ(currentLeader, leaderAddr);

        for (auto& cliservice : fakeCliServices) {
            cliservice.CleanInvokeTimes();
        }
    }
}

TEST_F(ServiceHelperGetLeaderTest, AllChunkServerExceptionTest) {
    std::vector<int> exceptionErrCodes{
        ENOENT, EAGAIN, EHOSTDOWN,
        ECONNREFUSED, ECONNRESET, brpc::ELOGOFF};

    // 设置第三个chunkserver为leader
    const auto currentLeaderIndex = 2;
    const auto& currentLeader = internalAddrs[currentLeaderIndex];

    SetGetLeaderResponse(currentLeader);

    // 另外两个chunkserver都返回对应的错误码
    for (auto errCode : exceptionErrCodes) {
        fakeCliServices[0].SetErrorCode(errCode);
        fakeCliServices[1].SetErrorCode(errCode);

        brpc::Controller controller;
        controller.SetFailed(errCode, "Failed");
        curve::common::Peer* peer = new curve::common::Peer();
        peer->set_address(currentLeader.ToString());
        GetLeaderResponse2 response;
        response.set_allocated_leader(peer);

        FakeReturn fakeret(&controller, static_cast<void*>(&response));

        fakeCliServices[0].SetFakeReturn(&fakeret);
        fakeCliServices[1].SetFakeReturn(&fakeret);

        GetLeaderRpcOption rpcOption;
        rpcOption.rpcTimeoutMs = 1000;
        GetLeaderInfo getLeaderInfo(kLogicPoolId, kCopysetId,
            copysetPeerInfos, currentLeaderIndex, rpcOption);
        ASSERT_EQ(-1, ServiceHelper::GetLeader(getLeaderInfo, &leaderAddr,
            &leaderId, nullptr));

        ResetAllFakeCliService();
    }
}

TEST_F(ServiceHelperGetLeaderTest, EmptyCopysetPeerInfoTest) {
    GetLeaderRpcOption rpcOption;
    rpcOption.rpcTimeoutMs = 1000;

    GetLeaderInfo getLeaderInfo(kLogicPoolId, kCopysetId, {}, -1, rpcOption);
    ASSERT_EQ(-1, ServiceHelper::GetLeader(getLeaderInfo, &leaderAddr,
                                           &leaderId, nullptr));
}

TEST_F(MDSClientTest, StatFileStatusTest) {
    std::vector<curve::mds::FileStatus> fileStatus{
        curve::mds::FileStatus::kFileCreated,
        curve::mds::FileStatus::kFileDeleting,
        curve::mds::FileStatus::kFileCloning,
        curve::mds::FileStatus::kFileCloneMetaInstalled,
        curve::mds::FileStatus::kFileCloned,
        curve::mds::FileStatus::kFileBeingCloned,
    };

    std::string filename = "/StatFileStatusTest";

    for (auto status : fileStatus) {
        std::unique_ptr<curve::mds::FileInfo> info(new curve::mds::FileInfo());
        ::curve::mds::GetFileInfoResponse response;
        info->set_filename(filename);
        info->set_owner(userinfo.owner);
        info->set_id(1);
        info->set_parentid(0);
        info->set_filetype(curve::mds::FileType::INODE_PAGEFILE);
        info->set_chunksize(4 * 1024 * 1024);
        info->set_length(4 * 1024 * 1024 * 1024ul);
        info->set_ctime(12345678);
        info->set_segmentsize(1 * 1024 * 1024 * 1024ul);

        info->set_filestatus(status);

        response.set_allocated_fileinfo(info.release());
        response.set_statuscode(::curve::mds::StatusCode::kOK);

        FakeReturn* fakeret =
            new FakeReturn(nullptr, static_cast<void*>(&response));
        curvefsservice.SetGetFileInfoFakeReturn(fakeret);

        std::unique_ptr<curve::client::FInfo_t> finfo(
            new curve::client::FInfo_t());
        FileStatInfo fstat;
        globalclient->StatFile(filename, userinfo, &fstat);

        ASSERT_EQ(fstat.id, 1);
        ASSERT_EQ(fstat.parentid, 0);
        ASSERT_EQ(static_cast<curve::mds::FileType>(fstat.filetype),
                  curve::mds::FileType::INODE_PAGEFILE);
        ASSERT_EQ(fstat.ctime, 12345678);
        ASSERT_EQ(fstat.length, 4 * 1024 * 1024 * 1024ul);
        ASSERT_EQ(fstat.fileStatus, static_cast<int>(status));
        ASSERT_EQ(fstat.filename, filename);
        ASSERT_EQ(fstat.owner, userinfo.owner);
    }
}

TEST_F(MDSClientTest, DeAllocateSegmentTest) {
    FInfo fileInfo;
    fileInfo.fullPathName = "/DeAllocateSegmentTest";

    // rpc failed
    {
        brpc::Controller cntl;
        cntl.SetFailed(-1, "rpc failed");

        FakeReturn* fakeRet = new FakeReturn(&cntl, nullptr);
        curvefsservice.SetDeAllocateSegmentFakeReturn(fakeRet);

        uint64_t startMs = curve::common::TimeUtility::GetTimeofDayMs();
        ASSERT_EQ(LIBCURVE_ERROR::FAILED,
                  mdsclient_.DeAllocateSegment(&fileInfo, 0ull));
        uint64_t endMs = curve::common::TimeUtility::GetTimeofDayMs();
        ASSERT_GE(endMs - startMs, metaopt.mdsMaxRetryMS);
    }

    // rpc return ok
    {
        curve::mds::DeAllocateSegmentResponse response;
        response.set_statuscode(curve::mds::StatusCode::kOK);
        FakeReturn* fakeRet = new FakeReturn(nullptr, &response);
        curvefsservice.SetDeAllocateSegmentFakeReturn(fakeRet);

        ASSERT_EQ(LIBCURVE_ERROR::OK,
                  mdsclient_.DeAllocateSegment(&fileInfo, 0ull));
    }

    // rpc return segment not allocated
    {
        curve::mds::DeAllocateSegmentResponse response;
        response.set_statuscode(curve::mds::StatusCode::kSegmentNotAllocated);
        FakeReturn* fakeRet = new FakeReturn(nullptr, &response);
        curvefsservice.SetDeAllocateSegmentFakeReturn(fakeRet);

        ASSERT_EQ(LIBCURVE_ERROR::OK,
                  mdsclient_.DeAllocateSegment(&fileInfo, 0ull));
    }

    // other error code
    {
        std::vector<curve::mds::StatusCode> errorCodes{
            curve::mds::StatusCode::kOwnerAuthFail,
            curve::mds::StatusCode::kParaError,
            curve::mds::StatusCode::kNotSupported,
            curve::mds::StatusCode::kFileUnderSnapShot};

        for (auto err : errorCodes) {
            curve::mds::DeAllocateSegmentResponse response;
            response.set_statuscode(err);
            FakeReturn* fakeRet = new FakeReturn(nullptr, &response);
            curvefsservice.SetDeAllocateSegmentFakeReturn(fakeRet);

            ASSERT_NE(LIBCURVE_ERROR::OK,
                      mdsclient_.DeAllocateSegment(&fileInfo, 0ull));
        }
    }
}

using ::testing::_;
using ::testing::DoAll;
using ::testing::ElementsAre;
using ::testing::Invoke;
using ::testing::NotNull;
using ::testing::Return;
using ::testing::ReturnArg;
using ::testing::SaveArg;
using ::testing::SaveArgPointee;
using ::testing::SetArgPointee;
using ::testing::SetArrayArgument;

static void MockRefreshSession(::google::protobuf::RpcController* controller,
                               const curve::mds::ReFreshSessionRequest* request,
                               curve::mds::ReFreshSessionResponse* response,
                               ::google::protobuf::Closure* done) {
    brpc::ClosureGuard guard(done);

    response->set_statuscode(curve::mds::StatusCode::kOK);
    response->set_sessionid("");
}

class MDSClientRefreshSessionTest : public ::testing::Test {
 public:
    void SetUp() override {
        ASSERT_EQ(0, server_.AddService(&curveFsService_,
                                        brpc::SERVER_DOESNT_OWN_SERVICE));
        ASSERT_EQ(0, server_.Start(kServerAddress, nullptr));
    }

    void TearDown() override {
        server_.Stop(0);
        server_.Join();
    }

 protected:
    const char* kServerAddress = "127.0.0.1:21000";
    const uint32_t kTestPort = 1234;

    brpc::Server server_;
    curve::mds::MockNameService curveFsService_;
};

TEST_F(MDSClientRefreshSessionTest, StartDummyServerTest) {
    curve::client::ClientDummyServerInfo::GetInstance().SetRegister(true);
    curve::client::ClientDummyServerInfo::GetInstance().SetPort(kTestPort);
    curve::client::ClientDummyServerInfo::GetInstance().SetIP(kServerAddress);

    MDSClient mdsClient;
    MetaServerOption opt;
    opt.mdsAddrs.push_back(kServerAddress);
    ASSERT_EQ(0, mdsClient.Initialize(opt));

    curve::mds::ReFreshSessionRequest request;
    curve::mds::ReFreshSessionResponse response;
    curve::mds::FileInfo* fileInfo = new curve::mds::FileInfo();
    response.set_allocated_fileinfo(fileInfo);
    EXPECT_CALL(curveFsService_, RefreshSession(_, _, _, _))
        .WillOnce(DoAll(SaveArgPointee<1>(&request),
                        SetArgPointee<2>(response),
                        Invoke(MockRefreshSession)));

    UserInfo userInfo;
    userInfo.owner = "test";
    LeaseRefreshResult result;
    ASSERT_EQ(0, mdsClient.RefreshSession("/filename", userInfo, "", &result));

    ASSERT_TRUE(request.has_clientport());
    ASSERT_TRUE(request.has_clientip());
    ASSERT_EQ(request.clientport(), kTestPort);
    ASSERT_EQ(request.clientip(), kServerAddress);
}

TEST_F(MDSClientRefreshSessionTest, NoStartDummyServerTest) {
    curve::client::ClientDummyServerInfo::GetInstance().SetRegister(false);

    MDSClient mdsClient;
    MetaServerOption opt;
    opt.mdsAddrs.push_back(kServerAddress);
    ASSERT_EQ(0, mdsClient.Initialize(opt));

    curve::mds::ReFreshSessionRequest request;
    curve::mds::ReFreshSessionResponse response;
    curve::mds::FileInfo* fileInfo = new curve::mds::FileInfo();
    response.set_allocated_fileinfo(fileInfo);
    EXPECT_CALL(curveFsService_, RefreshSession(_, _, _, _))
        .WillOnce(DoAll(SaveArgPointee<1>(&request),
                        SetArgPointee<2>(response),
                        Invoke(MockRefreshSession)));

    UserInfo userInfo;
    userInfo.owner = "test";
    LeaseRefreshResult result;
    ASSERT_EQ(0, mdsClient.RefreshSession("/filename", userInfo, "", &result));

    ASSERT_FALSE(request.has_clientport());
    ASSERT_FALSE(request.has_clientip());
}

}  // namespace client
}  // namespace curve
