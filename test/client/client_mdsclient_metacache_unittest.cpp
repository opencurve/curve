/*
 * Project: curve
 * File Created: Tuesday, 9th October 2018 5:16:52 pm
 * Author: tongguangxun
 * Copyright (c) 2018 NetEase
 */

#include <gtest/gtest.h>
#include <gflags/gflags.h>
#include <glog/logging.h>
#include <brpc/controller.h>
#include <brpc/server.h>
#include <fiu-control.h>

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

extern std::string metaserver_addr;
extern uint32_t chunk_size;
extern std::string configpath;
extern curve::client::FileClient* globalclient;

using curve::client::UserInfo_t;
using curve::client::CopysetPeerInfo;
using curve::client::CopysetInfo_t;
using curve::client::SegmentInfo;
using curve::client::FInfo;
using curve::client::LeaseSession;
using curve::client::LogicalPoolCopysetIDInfo_t;
using curve::client::MetaCacheErrorType;
using curve::client::MDSClient;
using curve::client::ServiceHelper;
using curve::client::FileClient;
using curve::client::LogicPoolID;
using curve::client::CopysetID;
using curve::client::ChunkServerID;
using curve::client::ChunkServerAddr;
using curve::client::FileInstance;
using curve::mds::CurveFSService;
using curve::mds::topology::TopologyService;
using curve::mds::RegistClientResponse;
using ::curve::mds::topology::GetChunkServerListInCopySetsResponse;

class MDSClientTest : public ::testing::Test {
 public:
    void SetUp() {
        metaopt.metaaddrvec.push_back("127.0.0.1:9104");

        metaopt.metaaddrvec.push_back("127.0.0.1:9104");
        metaopt.rpcTimeoutMs = 500;
        metaopt.rpcRetryTimes = 5;
        metaopt.retryIntervalUs = 200;
        metaopt.synchronizeRPCTimeoutMS = 2000;
        metaopt.synchronizeRPCRetryTime = 3;
        mdsclient_.Initialize(metaopt);
        userinfo.owner = "test";

        if (server.AddService(&topologyservice,
                            brpc::SERVER_DOESNT_OWN_SERVICE) != 0) {
            LOG(FATAL) << "Fail to add service";
        }

        if (server.AddService(&curvefsservice,
                            brpc::SERVER_DOESNT_OWN_SERVICE) != 0) {
            LOG(FATAL) << "Fail to add service";
        }

        RegistClientResponse* registResp = new RegistClientResponse();
        registResp->set_statuscode(::curve::mds::StatusCode::kOK);
        FakeReturn* fakeregist = new FakeReturn(nullptr, static_cast<void*>(registResp));      // NOLINT
        curvefsservice.SetRegistRet(fakeregist);

        brpc::ServerOptions options;
        options.idle_timeout_sec = -1;
        LOG(INFO) << "meta server addr = " << metaserver_addr.c_str();
        ASSERT_EQ(server.Start(metaserver_addr.c_str(), &options), 0);

        LOG(INFO) << configpath.c_str();
        if (Init(configpath.c_str()) != 0) {
            LOG(ERROR) << "Fail to init config";
        }
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
    MetaServerOption_t  metaopt;
    FakeTopologyService topologyservice;
    FakeMDSCurveFSService curvefsservice;
    static int i;
};

TEST(MDSClientTestRegitser, Register) {
    brpc::Server        server;
    MetaServerOption_t  metaopt;
    metaopt.metaaddrvec.push_back("127.0.0.1:9104");

    metaopt.rpcTimeoutMs = 500;
    metaopt.rpcRetryTimes = 5;
    metaopt.retryIntervalUs = 200;

    FakeMDSCurveFSService curvefsservice;

    if (server.AddService(&curvefsservice,
                        brpc::SERVER_DOESNT_OWN_SERVICE) != 0) {
        LOG(FATAL) << "Fail to add service";
    }

    brpc::ServerOptions options;
    options.idle_timeout_sec = -1;
    LOG(INFO) << "meta server addr = " << metaserver_addr.c_str();
    ASSERT_EQ(server.Start(metaserver_addr.c_str(), &options), 0);

    RegistClientResponse* registResp1 = new RegistClientResponse();
    registResp1->set_statuscode(::curve::mds::StatusCode::kStorageError);
    FakeReturn* fakeregist1 = new FakeReturn(nullptr, static_cast<void*>(registResp1));      // NOLINT
    curvefsservice.SetRegistRet(fakeregist1);

    // regist失败，初始化失败
    ASSERT_EQ(-LIBCURVE_ERROR::FAILED, Init(configpath.c_str()));

    // config with regist off
    std::string confpath = "./test/client/testConfig/client_session.conf";
    ASSERT_EQ(0, Init(confpath.c_str()));
    ASSERT_EQ(0, Init(confpath.c_str()));
    UnInit();

    RegistClientResponse* registResp = new RegistClientResponse();
    registResp->set_statuscode(::curve::mds::StatusCode::kOK);
    FakeReturn* fakeregist = new FakeReturn(nullptr, static_cast<void*>(registResp));      // NOLINT
    curvefsservice.SetRegistRet(fakeregist);

    // regist成功，初始化成功
    std::string ip;
    curve::common::NetCommon::GetLocalIP(&ip);
    ASSERT_EQ(0, Init(configpath.c_str()));

    UnInit();
    ASSERT_STREQ(ip.c_str(), curvefsservice.GetIP().c_str());

    ASSERT_GT(curvefsservice.GetPort(), 8999);
    ASSERT_EQ(0, server.Stop(0));
    ASSERT_EQ(0, server.Join());
}

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

    ASSERT_EQ(-LIBCURVE_ERROR::FAILED,
             globalclient->Create(filename.c_str(),
                                userinfo, len));
    ASSERT_EQ(6, curvefsservice.GetRetryTimes());

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

    ASSERT_EQ(-1 * LIBCURVE_ERROR::FAILED,
             globalclient->Mkdir(dirpath.c_str(), userinfo));
    // 一个mds地址重试3次，配置文件中有两个mds地址，所以要重试6次
    ASSERT_EQ(6, curvefsservice.GetRetryTimes());

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
         mdsclient_.CloseFile(filename.c_str(),
                                userinfo,  "sessid"));
    ASSERT_EQ(metaopt.synchronizeRPCRetryTime * metaopt.metaaddrvec.size(),
        curvefsservice.GetRetryTimes());

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

    // 打开一个文件, 如果返回的是occupied，那么会重试到重试次数用完
    uint64_t starttime = TimeUtility::GetTimeofDayUs();
    ::curve::mds::ProtoSession* socupied1 = new ::curve::mds::ProtoSession;
    socupied1->set_sessionid("1");
    socupied1->set_createtime(12345);
    socupied1->set_leasetime(5000000);
    socupied1->set_sessionstatus(::curve::mds::SessionStatus::kSessionOK);

    ::curve::mds::OpenFileResponse responseOccupied1;
    responseOccupied1.set_statuscode(::curve::mds::StatusCode::kFileOccupied);
    responseOccupied1.set_allocated_protosession(socupied1);

    FakeReturn* r1
     = new FakeReturn(nullptr, static_cast<void*>(&responseOccupied1));
    curvefsservice.SetOpenFile(r1);

    int retcode = globalclient->Open(filename + "test", userinfo);
    uint64_t end = TimeUtility::GetTimeofDayUs();
    ASSERT_EQ(retcode, -LIBCURVE_ERROR::FILE_OCCUPIED);
    ASSERT_GT(end - starttime, 200 * 3);

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
    ASSERT_EQ(metaopt.synchronizeRPCRetryTime * metaopt.metaaddrvec.size(),
        curvefsservice.GetRetryTimes());

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
    ASSERT_EQ(6, curvefsservice.GetRetryTimes());

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
    ASSERT_EQ(6, curvefsservice.GetRetryTimes());

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
    ASSERT_EQ(6, curvefsservice.GetRetryTimes());

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
    ASSERT_EQ(6, curvefsservice.GetRetryTimes());
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
    ASSERT_EQ(6, curvefsservice.GetRetryTimes());

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
    ASSERT_EQ(metaopt.synchronizeRPCRetryTime * metaopt.metaaddrvec.size(),
        curvefsservice.GetRetryTimes());

    delete fakeret;
    delete fakeret2;
    delete finfo;
}

TEST_F(MDSClientTest, GetOrAllocateSegment) {
    std::string filename = "/1_userinfo_";

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
            cslocs->set_port(5000);
        }
    }
    FakeReturn* faktopologyeret = new FakeReturn(nullptr,
        static_cast<void*>(&response_1));
    topologyservice.SetFakeReturn(faktopologyeret);

    curve::client::FInfo_t fi;
    fi.chunksize   = 4 * 1024 * 1024;
    fi.segmentsize = 1 * 1024 * 1024 * 1024ul;
    curve::client::MetaCache mc;
    curve::client::ChunkIDInfo_t cinfo;
    ASSERT_EQ(MetaCacheErrorType::CHUNKINFO_NOT_FOUND,
                mc.GetChunkInfoByIndex(0, &cinfo));

    SegmentInfo segInfo;
    LogicalPoolCopysetIDInfo_t lpcsIDInfo;
    mdsclient_.GetOrAllocateSegment(true, userinfo,
                                    0, &fi, &segInfo);
    int count = 0;
    for (auto iter : segInfo.chunkvec) {
        uint64_t index = (segInfo.startoffset +
                            count*fi.chunksize)/ fi.chunksize;
        mc.UpdateChunkInfoByIndex(index, iter);
        ++count;
    }

    std::vector<CopysetInfo_t> cpinfoVec;
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
        for (auto iter : serverlist.csinfos_) {
            ASSERT_EQ(iter.chunkserverid_, chunkserverid++);
            ASSERT_EQ(pd, iter.csaddr_);
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
    butil::str2endpoint("127.0.0.1", 7777, &ep1);
    ChunkServerID cid1 = 4;
    mc.UpdateLeader(1234, 0, &cid1, ep1);

    curve::client::EndPoint toep;
    ChunkServerID cid;
    mc.GetLeader(1234, 0, &cid, &toep, false);

    ASSERT_EQ(ep1, toep);
    ASSERT_EQ(0, mc.UpdateLeader(1234, 0, &cid1, ep1));

    ASSERT_EQ(0, mc.GetAppliedIndex(1111, 0));

    // test applied index update
    curve::client::CopysetInfo_t csinfo;
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
    ASSERT_EQ(-1, ServiceHelper::GetLeader(lpid, copyid, conf, &pid, 10));

    curve::client::EndPoint ep11, ep22, ep33;
    butil::str2endpoint("127.0.0.1", 7777, &ep11);
    curve::client::ChunkServerAddr pd11(ep11);
    butil::str2endpoint("127.0.0.1", 7777, &ep22);
    curve::client::ChunkServerAddr pd22(ep22);
    butil::str2endpoint("127.0.0.1", 7777, &ep33);
    curve::client::ChunkServerAddr pd33(ep33);

    conf.push_back(CopysetPeerInfo(1, pd11));
    conf.push_back(CopysetPeerInfo(2, pd22));
    conf.push_back(CopysetPeerInfo(3, pd33));
    ASSERT_EQ(-1, ServiceHelper::GetLeader(lpid, copyid, conf, &pid, 10));

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
            cslocs->set_hostip("127.0.0.1");
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

    std::vector<CopysetInfo_t> cpinfoVec;
    curve::client::MetaCache mc;
    ASSERT_NE(LIBCURVE_ERROR::FAILED,
                mdsclient_.GetServerList(1234, cpidvec, &cpinfoVec));
    for (auto iter : cpinfoVec) {
        mc.UpdateCopysetInfo(1234, iter.cpid_, iter);
    }

    curve::client::EndPoint ep;
    butil::str2endpoint("127.0.0.1", 5000, &ep);
    curve::client::ChunkServerAddr pd(ep);
    for (int i = 0; i < 256; i++) {
        auto serverlist = mc.GetServerList(1234, i);
        ASSERT_TRUE(serverlist.IsValid());
        int chunkserverid = i * 3 + 1;
        for (auto iter : serverlist.csinfos_) {
            ASSERT_EQ(iter.chunkserverid_, chunkserverid++);
            ASSERT_EQ(pd, iter.csaddr_);
        }
    }

    // 设置rpc失败，触发重试
    brpc::Controller cntl;
    cntl.SetFailed(-1, "failed");

    FakeReturn* fakeret2
     = new FakeReturn(&cntl, static_cast<void*>(&response_1));

    topologyservice.SetFakeReturn(fakeret2);
    topologyservice.CleanRetryTimes();

    ASSERT_EQ(LIBCURVE_ERROR::FAILED,
         mdsclient_.GetServerList(12345, cpidvec, &cpinfoVec));
    ASSERT_EQ(metaopt.rpcRetryTimes * metaopt.metaaddrvec.size(),
        topologyservice.GetRetryTimes());

    // 设置rpc返回hostdown，触发主动切换mds
    // 当rpc内部收到EHOSTDOWN错误的时候，其内部会重试，默认为3次
    // 这样每一次都是4次
    brpc::Controller cntl2;
    cntl2.SetFailed(EHOSTDOWN, "failed");

    FakeReturn* fakeret3
     = new FakeReturn(&cntl2, static_cast<void*>(&response_1));
    topologyservice.SetFakeReturn(fakeret3);
    topologyservice.CleanRetryTimes();
    ASSERT_EQ(LIBCURVE_ERROR::FAILED,
         mdsclient_.GetServerList(12345, cpidvec, &cpinfoVec));
    ASSERT_EQ(4 * metaopt.rpcRetryTimes, topologyservice.GetRetryTimes());

    delete faktopologyeret;
    delete fakeret2;
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

    curve::client::EndPoint ep1, ep2, ep3, ep4;
    butil::str2endpoint("127.0.0.1", 9120, &ep1);
    curve::client::ChunkServerAddr pd1(ep1);
    butil::str2endpoint("127.0.0.1", 9121, &ep2);
    curve::client::ChunkServerAddr pd2(ep2);
    butil::str2endpoint("127.0.0.1", 9122, &ep3);
    curve::client::ChunkServerAddr pd3(ep3);
    butil::str2endpoint("127.0.0.1", 9123, &ep4);
    curve::client::ChunkServerAddr pd4(ep4);

    std::vector<CopysetPeerInfo> cfg;
    cfg.push_back(CopysetPeerInfo(1, pd1));
    cfg.push_back(CopysetPeerInfo(2, pd2));
    cfg.push_back(CopysetPeerInfo(3, pd3));

    curve::client::MetaCache mc;
    MetaCacheOption mcOpt;
    mc.Init(mcOpt, &mdsclient_);
    curve::client::CopysetInfo_t cslist;

    curve::client::CopysetPeerInfo peerinfo_1;
    peerinfo_1.chunkserverid_ = 1;
    peerinfo_1.csaddr_ = pd1;
    cslist.AddCopysetPeerInfo(peerinfo_1);

    curve::client::CopysetPeerInfo peerinfo_2;
    peerinfo_2.chunkserverid_ = 2;
    peerinfo_2.csaddr_ = pd2;
    cslist.AddCopysetPeerInfo(peerinfo_2);

    curve::client::CopysetPeerInfo peerinfo_3;
    peerinfo_3.chunkserverid_ = 3;
    peerinfo_3.csaddr_ = pd3;
    cslist.AddCopysetPeerInfo(peerinfo_3);

    mc.UpdateCopysetInfo(1234, 1234, cslist);

    // 测试复制组里第一个addr为leader
    curve::chunkserver::GetLeaderResponse2 response1;
    curve::common::Peer *peer1 = new curve::common::Peer();
    peer1->set_address(pd1.ToString());
    response1.set_allocated_leader(peer1);
    FakeReturn fakeret1(nullptr, static_cast<void*>(&response1));
    cliservice1.SetFakeReturn(&fakeret1);

    curve::chunkserver::GetLeaderResponse2 response2;
    curve::common::Peer *peer2 = new curve::common::Peer();
    peer2->set_address(pd2.ToString());
    response2.set_allocated_leader(peer2);
    FakeReturn fakeret2(nullptr, static_cast<void*>(&response2));
    cliservice2.SetFakeReturn(&fakeret2);

    curve::chunkserver::GetLeaderResponse2 response3;
    curve::common::Peer *peer3 = new curve::common::Peer();
    peer3->set_address(pd2.ToString());
    response3.set_allocated_leader(peer3);
    FakeReturn fakeret3(nullptr, static_cast<void*>(&response3));
    cliservice3.SetFakeReturn(&fakeret3);

    curve::client::ChunkServerID ckid;
    curve::client::EndPoint leaderep;

    cliservice1.CleanInvokeTimes();
    cliservice2.CleanInvokeTimes();
    cliservice3.CleanInvokeTimes();

    mc.GetLeader(1234, 1234, &ckid, &leaderep, true);

    ASSERT_EQ(1, cliservice1.GetInvokeTimes());
    ASSERT_EQ(0, cliservice2.GetInvokeTimes());
    ASSERT_EQ(0, cliservice3.GetInvokeTimes());

    ASSERT_EQ(ckid, 1);
    ASSERT_EQ(ep1, leaderep);

    // 测试第二次拉取新的leader，直接跳过第一个index，查找第2，3两个
    cliservice1.CleanInvokeTimes();
    cliservice2.CleanInvokeTimes();
    cliservice3.CleanInvokeTimes();

    mc.GetLeader(1234, 1234, &ckid, &leaderep, true);

    ASSERT_EQ(0, cliservice1.GetInvokeTimes());
    ASSERT_EQ(1, cliservice2.GetInvokeTimes());
    ASSERT_EQ(0, cliservice3.GetInvokeTimes());

    ASSERT_EQ(ckid, 2);
    ASSERT_EQ(ep2, leaderep);

    // 测试第三次拉取新的leader，会跳过第二个index，重试1，3
    brpc::Controller controller1;
    controller1.SetFailed(-1, "error");
    curve::common::Peer *peer4 = new curve::common::Peer();
    peer4->set_address(pd3.ToString());
    response1.set_allocated_leader(peer4);
    FakeReturn fakeret11(&controller1, static_cast<void*>(&response1));
    cliservice1.SetFakeReturn(&fakeret11);

    curve::common::Peer *peer5 = new curve::common::Peer();
    peer5->set_address(pd2.ToString());
    response2.set_allocated_leader(peer5);
    FakeReturn fakeret22(nullptr, static_cast<void*>(&response2));
    cliservice2.SetFakeReturn(&fakeret22);

    curve::common::Peer *peer6 = new curve::common::Peer();
    peer6->set_address(pd3.ToString());
    response3.set_allocated_leader(peer6);
    FakeReturn fakeret33(nullptr, static_cast<void*>(&response3));
    cliservice3.SetFakeReturn(&fakeret33);

    cliservice1.CleanInvokeTimes();
    cliservice2.CleanInvokeTimes();
    cliservice3.CleanInvokeTimes();

    mc.GetLeader(1234, 1234, &ckid, &leaderep, true);

    ASSERT_EQ(1, cliservice1.GetInvokeTimes());
    ASSERT_EQ(0, cliservice2.GetInvokeTimes());
    ASSERT_EQ(1, cliservice3.GetInvokeTimes());

    ASSERT_EQ(ckid, 3);
    ASSERT_EQ(ep3, leaderep);

    // 测试拉取新leader失败，需要到mds重新fetch新的serverlist
    // 当前新leader是3，尝试再刷新leader，这个时候会从1， 2获取leader
    // 但是这时候leader找不到了，于是就会触发向mds重新拉取最新的server list
    brpc::Controller controller11;
    controller11.SetFailed(-1, "error");
    curve::common::Peer *peer9 = new curve::common::Peer();
    peer9->set_address(pd3.ToString());
    peer9->set_id(4321);
    response1.set_allocated_leader(peer9);
    FakeReturn fakeret111(&controller11, static_cast<void*>(&response1));
    cliservice1.SetFakeReturn(&fakeret111);

    brpc::Controller controller22;
    controller22.SetFailed(-1, "error");
    curve::common::Peer *peer10 = new curve::common::Peer();
    peer10->set_address(pd2.ToString());
    peer10->set_id(4321);
    response2.set_allocated_leader(peer10);
    FakeReturn fakeret222(&controller22, static_cast<void*>(&response2));
    cliservice2.SetFakeReturn(&fakeret222);

    brpc::Controller controller33;
    controller33.SetFailed(-1, "error");
    curve::common::Peer *peer11 = new curve::common::Peer();
    peer11->set_address(pd3.ToString());
    peer11->set_id(4321);
    response3.set_allocated_leader(peer11);
    FakeReturn fakeret333(&controller33, static_cast<void*>(&response3));
    cliservice3.SetFakeReturn(&fakeret333);

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
        cslocs->set_hostip("127.0.0.1");
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

    ASSERT_EQ(1, cliservice1.GetInvokeTimes());
    ASSERT_EQ(1, cliservice2.GetInvokeTimes());
    ASSERT_EQ(0, cliservice3.GetInvokeTimes());

    // 获取新新的leader，这时候会从1，2，4这三个server拉取新leader，并成功获取新leader
    brpc::Controller controller44;
    curve::chunkserver::GetLeaderResponse2 response4;
    curve::common::Peer *peer12 = new curve::common::Peer();
    peer12->set_address(pd4.ToString());
    peer12->set_id(4321);
    response4.set_allocated_leader(peer12);
    FakeReturn fakeret444(nullptr, static_cast<void*>(&response4));
    cliservice4.SetFakeReturn(&fakeret444);

    // 清空被凋次数
    cliservice1.CleanInvokeTimes();
    cliservice2.CleanInvokeTimes();
    cliservice3.CleanInvokeTimes();
    cliservice4.CleanInvokeTimes();
    ASSERT_EQ(0, mc.GetLeader(1234, 1234, &ckid, &leaderep, true));
    ASSERT_EQ(leaderep, ep4);

    ASSERT_EQ(1, cliservice1.GetInvokeTimes());
    ASSERT_EQ(1, cliservice2.GetInvokeTimes());
    ASSERT_EQ(1, cliservice3.GetInvokeTimes());
    ASSERT_EQ(1, cliservice4.GetInvokeTimes());

    // 直接获取新的leader信息
    cliservice1.CleanInvokeTimes();
    cliservice2.CleanInvokeTimes();
    cliservice3.CleanInvokeTimes();
    cliservice4.CleanInvokeTimes();
    ASSERT_EQ(0, mc.GetLeader(1234, 1234, &ckid, &leaderep, false));
    ASSERT_EQ(leaderep, ep4);

    ASSERT_EQ(0, cliservice1.GetInvokeTimes());
    ASSERT_EQ(0, cliservice2.GetInvokeTimes());
    ASSERT_EQ(0, cliservice3.GetInvokeTimes());
    ASSERT_EQ(0, cliservice4.GetInvokeTimes());

    // 测试新增一个leader，其chunkserverid未知
    curve::client::EndPoint ep5;
    butil::str2endpoint("127.0.0.1", 9124, &ep5);
    curve::client::ChunkServerAddr pd5(ep5);

    curve::common::Peer *peer7 = new curve::common::Peer();
    peer7->set_address(pd5.ToString());
    response1.set_allocated_leader(peer7);
    FakeReturn fakeret44(nullptr, static_cast<void*>(&response1));
    cliservice1.SetFakeReturn(&fakeret44);

    cliservice1.CleanInvokeTimes();
    cliservice2.CleanInvokeTimes();
    cliservice3.CleanInvokeTimes();

    mc.GetLeader(1234, 1234, &ckid, &leaderep, true);

    ASSERT_EQ(1, cliservice1.GetInvokeTimes());
    ASSERT_EQ(0, cliservice2.GetInvokeTimes());
    ASSERT_EQ(0, cliservice3.GetInvokeTimes());

    CopysetInfo_t cpinfo = mc.GetServerList(1234, 1234);
    // 新的leader因为没有id，所以并没有被添加到copyset中
    ASSERT_EQ(cpinfo.csinfos_.size(), 4);
    curve::client::CopysetPeerInfo_t cpeer;
    cpeer.csaddr_.addr_ = pd5.addr_;
    auto it = std::find(cpinfo.csinfos_.begin(), cpinfo.csinfos_.end(), cpeer);
    ASSERT_EQ(it, cpinfo.csinfos_.end());

    // 测试新增一个leader，但是其chunkserverid已知
    // 设置新的leaderid和addr
    curve::common::Peer *peer8 = new curve::common::Peer();
    peer8->set_address(pd5.ToString());
    peer8->set_id(4321);
    response1.set_allocated_leader(peer8);
    FakeReturn fakeret55(nullptr, static_cast<void*>(&response1));
    cliservice1.SetFakeReturn(&fakeret55);

    cliservice1.CleanInvokeTimes();
    cliservice2.CleanInvokeTimes();
    cliservice3.CleanInvokeTimes();

    mc.GetLeader(1234, 1234, &ckid, &leaderep, true);

    ASSERT_EQ(1, cliservice1.GetInvokeTimes());
    ASSERT_EQ(0, cliservice2.GetInvokeTimes());
    ASSERT_EQ(0, cliservice3.GetInvokeTimes());

    cpinfo = mc.GetServerList(1234, 1234);
    ASSERT_EQ(cpinfo.csinfos_.size(), 5);
    auto t = std::find(cpinfo.csinfos_.begin(), cpinfo.csinfos_.end(), cpeer);
    ASSERT_NE(t, cpinfo.csinfos_.end());
    int leaderindex = cpinfo.GetCurrentLeaderIndex();

    ASSERT_EQ(pd5, cpinfo.csinfos_[leaderindex].csaddr_);

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

TEST_F(MDSClientTest, GetOrAllocateSegmentException) {
    std::string filename = "/1_userinfo_";

    curve::mds::GetOrAllocateSegmentResponse response;
    curve::mds::PageFileSegment* pfs = new curve::mds::PageFileSegment;
    response.set_statuscode(::curve::mds::StatusCode::kOK);
    response.set_allocated_pagefilesegment(pfs);
    FakeReturn* fakeret = new FakeReturn(nullptr,
                static_cast<void*>(&response));
    curvefsservice.SetGetOrAllocateSegmentFakeReturn(fakeret);

    ::curve::mds::topology::GetChunkServerListInCopySetsResponse response_1;
    response_1.set_statuscode(0);
    FakeReturn* faktopologyeret = new FakeReturn(nullptr,
        static_cast<void*>(&response_1));
    topologyservice.SetFakeReturn(faktopologyeret);

    curve::client::FInfo_t fi;
    SegmentInfo segInfo;
    LogicalPoolCopysetIDInfo_t lpcsIDInfo;
    curve::client::MetaCache mc;
    ASSERT_EQ(LIBCURVE_ERROR::FAILED,
            mdsclient_.GetOrAllocateSegment(true, userinfo,
                    0, &fi, &segInfo));


    // 设置rpc失败，触发重试
    brpc::Controller cntl;
    cntl.SetFailed(-1, "failed");

    FakeReturn* fakeret2
     = new FakeReturn(&cntl, static_cast<void*>(&response));
    curvefsservice.SetGetOrAllocateSegmentFakeReturn(fakeret2);

    curvefsservice.CleanRetryTimes();

    ASSERT_EQ(LIBCURVE_ERROR::FAILED,
            mdsclient_.GetOrAllocateSegment(true, userinfo,
                        0, &fi, &segInfo));

    ASSERT_EQ(metaopt.rpcRetryTimes * metaopt.metaaddrvec.size(),
            curvefsservice.GetRetryTimes());

    delete fakeret;
    delete faktopologyeret;
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

    ASSERT_EQ(LIBCURVE_ERROR::FAILED, mdsclient_.CreateCloneFile("destination",
                                                            userinfo,
                                                            10 * 1024 * 1024,
                                                            0,
                                                            4*1024*1024,
                                                            &finfo));

    ASSERT_EQ(metaopt.synchronizeRPCRetryTime * metaopt.metaaddrvec.size(),
            curvefsservice.GetRetryTimes());

    // 认证失败
    curve::mds::CreateCloneFileResponse response1;
    response1.set_statuscode(::curve::mds::StatusCode::kOwnerAuthFail);

    FakeReturn* fakecreateclone1
     = new FakeReturn(nullptr, static_cast<void*>(&response1));

    curvefsservice.SetCreateCloneFile(fakecreateclone1);

    ASSERT_EQ(LIBCURVE_ERROR::AUTHFAIL, mdsclient_.CreateCloneFile(
                                                            "destination",
                                                            userinfo,
                                                            10 * 1024 * 1024,
                                                            0,
                                                            4*1024*1024,
                                                            &finfo));
    // 请求成功
    info->set_id(5);
    curve::mds::CreateCloneFileResponse response2;
    response2.set_statuscode(::curve::mds::StatusCode::kOK);
    response2.set_allocated_fileinfo(info);

    FakeReturn* fakecreateclone2
     = new FakeReturn(nullptr, static_cast<void*>(&response2));

    curvefsservice.SetCreateCloneFile(fakecreateclone2);

    ASSERT_EQ(LIBCURVE_ERROR::OK, mdsclient_.CreateCloneFile("destination",
                                                            userinfo,
                                                            10 * 1024 * 1024,
                                                            0,
                                                            4*1024*1024,
                                                            &finfo));
    ASSERT_EQ(5, finfo.id);
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

    ASSERT_EQ(metaopt.synchronizeRPCRetryTime * metaopt.metaaddrvec.size(),
            curvefsservice.GetRetryTimes());

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

    ASSERT_EQ(metaopt.synchronizeRPCRetryTime * metaopt.metaaddrvec.size(),
            curvefsservice.GetRetryTimes());

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
    ASSERT_EQ(6, curvefsservice.GetRetryTimes());

    delete fakeret;
    delete fakeret2;
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
    ASSERT_EQ(6, curvefsservice.GetRetryTimes());

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
