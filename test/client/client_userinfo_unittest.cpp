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
 * File Created: Tuesday, 5th March 2019 11:14:25 am
 * Author: tongguangxun
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

#include "include/client/libcurve.h"
#include "src/client/client_common.h"
#include "test/client/fake/fakeMDS.h"
#include "src/client/libcurve_file.h"
#include "src/client/iomanager4chunk.h"
#include "src/client/libcurve_snapshot.h"

extern std::string mdsMetaServerAddr;
extern std::string configpath;

namespace curve {
namespace client {

class CurveClientUserAuthFail : public ::testing::Test {
 public:
    void SetUp() {
        metaopt.rpcRetryOpt.addrs.push_back("127.0.0.1:9104");

        metaopt.rpcRetryOpt.addrs.push_back("127.0.0.1:9104");
        metaopt.rpcRetryOpt.rpcTimeoutMs = 500;
        metaopt.rpcRetryOpt.rpcRetryIntervalUS = 200;

        if (server.AddService(&curvefsservice,
                            brpc::SERVER_DOESNT_OWN_SERVICE) != 0) {
            LOG(FATAL) << "Fail to add service";
        }

        if (server.AddService(&topologyservice,
                            brpc::SERVER_DOESNT_OWN_SERVICE) != 0) {
            LOG(FATAL) << "Fail to add service";
        }

        brpc::ServerOptions options;
        options.idle_timeout_sec = -1;
        LOG(INFO) << "meta server addr = " << mdsMetaServerAddr.c_str();
        ASSERT_EQ(server.Start(mdsMetaServerAddr.c_str(), &options), 0);
    }

    void TearDown() {
        ASSERT_EQ(0, server.Stop(0));
        ASSERT_EQ(0, server.Join());
    }

    brpc::Server        server;
    MetaServerOption  metaopt;
    FakeMDSCurveFSService curvefsservice;
    FakeMDSTopologyService topologyservice;
};

TEST_F(CurveClientUserAuthFail, CurveClientUserAuthFailTest) {
    std::string filename = "./1_userinfo_.txt";

    Init(configpath.c_str());
    ClientConfig cc;
    cc.Init(configpath.c_str());

    C_UserInfo_t cuserinfo;
    memcpy(cuserinfo.owner, "userinfo", 9);

    UserInfo_t userinfo;
    userinfo.owner = "userinfo";
    UserInfo_t emptyuserinfo;

    std::shared_ptr<MDSClient> mdsclient = std::make_shared<MDSClient>();
    mdsclient->Initialize(cc.GetFileServiceOption().metaServerOpt);

    FileInstance fileinstance;
    ASSERT_FALSE(fileinstance.Initialize(filename, mdsclient, emptyuserinfo,
                                         cc.GetFileServiceOption()));
    ASSERT_TRUE(fileinstance.Initialize(filename, mdsclient, userinfo,
                                        cc.GetFileServiceOption()));

    // set openfile response
    ::curve::mds::OpenFileResponse openresponse;
    curve::mds::FileInfo * finfo = new curve::mds::FileInfo;
    ::curve::mds::ProtoSession* se = new ::curve::mds::ProtoSession;
    se->set_sessionid("1");
    se->set_createtime(12345);
    se->set_leasetime(10000000);
    se->set_sessionstatus(::curve::mds::SessionStatus::kSessionOK);
    openresponse.set_statuscode(::curve::mds::StatusCode::kOwnerAuthFail);
    openresponse.set_allocated_protosession(se);

    openresponse.set_allocated_fileinfo(finfo);
    openresponse.mutable_fileinfo()->set_seqnum(2);
    openresponse.mutable_fileinfo()->set_filename(filename);

    FakeReturn* openfakeret
     = new FakeReturn(nullptr, static_cast<void*>(&openresponse));
    curvefsservice.SetOpenFile(openfakeret);

    // 1. create a File authfailed

    ::curve::mds::CreateFileResponse response;
    response.set_statuscode(::curve::mds::StatusCode::kOwnerAuthFail);
    FakeReturn* fakeret
     = new FakeReturn(nullptr, static_cast<void*>(&response));
    curvefsservice.SetCreateFileFakeReturn(fakeret);

    size_t len = 4 * 1024 * 1024ul;
    int ret = Create(filename.c_str(), &cuserinfo, len);
    ASSERT_EQ(ret, -LIBCURVE_ERROR::AUTHFAIL);

    // 2. set refresh response auth failed
    std::mutex mtx;
    std::condition_variable refreshcv;
    auto refresht = [&mtx, &refreshcv]() {
        LOG(INFO) << "get refresh session request!";
        refreshcv.notify_one();
    };
    curve::mds::FileInfo * info = new curve::mds::FileInfo;
    ::curve::mds::ReFreshSessionResponse refreshresp;
    refreshresp.set_statuscode(::curve::mds::StatusCode::kOwnerAuthFail);
    refreshresp.set_sessionid("1234");
    refreshresp.set_allocated_fileinfo(info);
    refreshresp.mutable_fileinfo()->set_seqnum(2);
    refreshresp.mutable_fileinfo()->set_filename(filename);
    refreshresp.mutable_fileinfo()->set_id(1);
    refreshresp.mutable_fileinfo()->set_parentid(0);
    refreshresp.mutable_fileinfo()->set_filetype(curve::mds::FileType::INODE_PAGEFILE);     // NOLINT
    refreshresp.mutable_fileinfo()->set_chunksize(4 * 1024 * 1024);
    refreshresp.mutable_fileinfo()->set_length(4 * 1024 * 1024 * 1024ul);
    refreshresp.mutable_fileinfo()->set_ctime(12345678);
    FakeReturn* refreshfakeret
     = new FakeReturn(nullptr, static_cast<void*>(&refreshresp));
    curvefsservice.SetRefreshSession(refreshfakeret, refresht);

    // 3. open the file auth failed
    int openret = fileinstance.Open(filename, userinfo);
    ASSERT_EQ(openret, -LIBCURVE_ERROR::AUTHFAIL);

    // 4. open file success
    openresponse.set_statuscode(::curve::mds::StatusCode::kOK);
    FakeReturn* openfakeret2
     = new FakeReturn(nullptr, static_cast<void*>(&openresponse));
    curvefsservice.SetOpenFile(openfakeret2);

    openret = fileinstance.Open(filename, userinfo);
    ASSERT_EQ(openret, LIBCURVE_ERROR::OK);
/*
    // 5. wait for refresh
    for (int i = 0; i < 4; i++) {
        {
            std::unique_lock<std::mutex> lk(mtx);
            refreshcv.wait(lk);
        }
    }

    CurveAioContext aioctx;
    aioctx.offset = 4 * 1024 * 1024 - 4 * 1024;
    aioctx.length = 4 * 1024 * 1024 + 8 * 1024;
    aioctx.ret = LIBCURVE_ERROR::OK;
    aioctx.cb = sessioncallback;
    aioctx.buf = nullptr;

    fileinstance.AioRead(&aioctx);
    fileinstance.AioWrite(&aioctx);

    for (int i = 0; i < 1; i++) {
        {
            std::unique_lock<std::mutex> lk(mtx);
            refreshcv.wait(lk);
        }
    }

    char buffer[10];
    ASSERT_EQ(-LIBCURVE_ERROR::DISABLEIO, fileinstance.Write(buffer, 0, 0));
    ASSERT_EQ(-LIBCURVE_ERROR::DISABLEIO, fileinstance.Read(buffer, 0, 0));
*/
    // 6. set fake close return
    ::curve::mds::CloseFileResponse closeresp;
    closeresp.set_statuscode(::curve::mds::StatusCode::kOwnerAuthFail);
    FakeReturn* closefileret
     = new FakeReturn(nullptr, static_cast<void*>(&closeresp));
    curvefsservice.SetCloseFile(closefileret);
    ASSERT_EQ(-LIBCURVE_ERROR::AUTHFAIL, fileinstance.Close());

    fileinstance.UnInitialize();
    UnInit();
}

TEST_F(CurveClientUserAuthFail, CurveSnapClientUserAuthFailTest) {
    ClientConfigOption opt;
    opt.metaServerOpt.rpcRetryOpt.rpcTimeoutMs = 500;
    opt.metaServerOpt.rpcRetryOpt.addrs.push_back("127.0.0.1:9104");
    opt.ioOpt.reqSchdulerOpt.scheduleQueueCapacity = 4096;
    opt.ioOpt.reqSchdulerOpt.scheduleThreadpoolSize = 2;
    opt.ioOpt.ioSenderOpt.failRequestOpt.chunkserverOPMaxRetry = 3;
    opt.ioOpt.ioSenderOpt.failRequestOpt.chunkserverOPRetryIntervalUS = 500;
    opt.ioOpt.metaCacheOpt.metacacheGetLeaderRetry = 3;
    opt.ioOpt.ioSenderOpt.chunkserverEnableAppliedIndexRead = 1;
    opt.ioOpt.ioSplitOpt.fileIOSplitMaxSizeKB = 64;
    opt.ioOpt.reqSchdulerOpt.ioSenderOpt = opt.ioOpt.ioSenderOpt;
    opt.loginfo.logLevel = 0;

    SnapshotClient cl;
    ASSERT_TRUE(!cl.Init(opt));

    UserInfo_t emptyuserinfo;

    std::string filename = "./1_usertest_.img";
    brpc::Server server;
    uint64_t seq = 1;
    // test create snap
    // normal test
    ::curve::mds::CreateSnapShotResponse response;
    response.set_statuscode(::curve::mds::StatusCode::kOK);
    response.clear_snapshotfileinfo();
    FakeReturn* fakeret
     = new FakeReturn(nullptr, static_cast<void*>(&response));
    curvefsservice.SetCreateSnapShot(fakeret);
    ASSERT_EQ(-LIBCURVE_ERROR::FAILED, cl.CreateSnapShot(filename,
                                                        emptyuserinfo,
                                                        &seq));

    // set response
    response.set_statuscode(::curve::mds::StatusCode::kOwnerAuthFail);
    ::curve::mds::FileInfo* finf = new ::curve::mds::FileInfo;
    finf->set_filename(filename);
    finf->set_id(1);
    finf->set_parentid(0);
    finf->set_filetype(curve::mds::FileType::INODE_PAGEFILE);
    finf->set_chunksize(16 * 1024 * 1024);
    finf->set_length(1 * 1024 * 1024 * 1024);
    finf->set_ctime(12345678);
    finf->set_seqnum(2);
    finf->set_segmentsize(1 * 1024 * 1024 * 1024);
    response.set_allocated_snapshotfileinfo(finf);
    FakeReturn* fakeret1
     = new FakeReturn(nullptr, static_cast<void*>(&response));
    curvefsservice.SetCreateSnapShot(fakeret1);

    ASSERT_EQ(-LIBCURVE_ERROR::AUTHFAIL, cl.CreateSnapShot(filename,
                                                        emptyuserinfo,
                                                        &seq));

    // test delete
    // normal delete test
    ::curve::mds::DeleteSnapShotResponse delresponse;
    delresponse.set_statuscode(::curve::mds::StatusCode::kOwnerAuthFail);
    FakeReturn* delfakeret
     = new FakeReturn(nullptr, static_cast<void*>(&delresponse));

    curvefsservice.SetDeleteSnapShot(delfakeret);
    ASSERT_EQ(-LIBCURVE_ERROR::AUTHFAIL, cl.DeleteSnapShot(filename,
                                                        emptyuserinfo,
                                                        seq));

    // test get SegmentInfo
    // normal getinfo
    curve::mds::GetOrAllocateSegmentResponse* getresponse =
                        new curve::mds::GetOrAllocateSegmentResponse();
    curve::mds::PageFileSegment* pfs = new curve::mds::PageFileSegment;
    pfs->set_logicalpoolid(0);
    pfs->set_segmentsize(1ull*1024*1024*1024);
    pfs->set_chunksize(16*1024*1024);
    pfs->set_startoffset(0);
    getresponse->set_statuscode(::curve::mds::StatusCode::kOwnerAuthFail);
    getresponse->set_allocated_pagefilesegment(pfs);
    FakeReturn* getfakeret = new FakeReturn(nullptr,
                                    static_cast<void*>(getresponse));
    curvefsservice.SetGetSnapshotSegmentInfo(getfakeret);

    ::curve::mds::topology::GetChunkServerListInCopySetsResponse* geresponse_1 =
          new  ::curve::mds::topology::GetChunkServerListInCopySetsResponse();
    geresponse_1->set_statuscode(0);
    FakeReturn* faktopologyeret = new FakeReturn(nullptr,
        static_cast<void*>(geresponse_1));
    topologyservice.SetFakeReturn(faktopologyeret);

    SegmentInfo seginfo;
    LogicalPoolCopysetIDInfo lpcsIDInfo;
    ASSERT_EQ(-LIBCURVE_ERROR::AUTHFAIL,
            cl.GetSnapshotSegmentInfo(filename,
                                      emptyuserinfo,
                                      0, 0, &seginfo));

    // test list snapshot
    // normal delete test
    ::curve::mds::ListSnapShotFileInfoResponse listresponse;
    listresponse.add_fileinfo();
    listresponse.mutable_fileinfo(0)->set_filename(filename);
    listresponse.mutable_fileinfo(0)->set_id(1);
    listresponse.mutable_fileinfo(0)->set_parentid(0);
    listresponse.mutable_fileinfo(0)->set_filetype(curve::mds::FileType::INODE_PAGEFILE);    // NOLINT
    listresponse.mutable_fileinfo(0)->set_chunksize(4 * 1024 * 1024);
    listresponse.mutable_fileinfo(0)->set_length(4 * 1024 * 1024 * 1024ul);
    listresponse.mutable_fileinfo(0)->set_ctime(12345678);
    listresponse.mutable_fileinfo(0)->set_seqnum(0);
    listresponse.mutable_fileinfo(0)->set_segmentsize(1 * 1024 * 1024 * 1024ul);

    listresponse.set_statuscode(::curve::mds::StatusCode::kOwnerAuthFail);
    FakeReturn* listfakeret
     = new FakeReturn(nullptr, static_cast<void*>(&listresponse));
    curve::client::FInfo_t sinfo;
    curvefsservice.SetListSnapShot(listfakeret);
    ASSERT_EQ(-LIBCURVE_ERROR::AUTHFAIL, cl.GetSnapShot(filename,
                                                        emptyuserinfo,
                                                        seq, &sinfo));

    std::vector<uint64_t> seqvec;
    std::map<uint64_t, curve::client::FInfo_t> fivec;
    seqvec.push_back(seq);
    curve::client::FInfo_t ffinfo;
    ASSERT_EQ(-LIBCURVE_ERROR::AUTHFAIL,
                cl.ListSnapShot(filename, emptyuserinfo, &seqvec, &fivec));
    cl.UnInit();

    delete fakeret;
    delete fakeret1;
    delete listfakeret;
    delete delfakeret;
}

// root user测试
TEST_F(CurveClientUserAuthFail, CurveSnapClientRootUserAuthTest) {
    ClientConfigOption opt;
    opt.metaServerOpt.rpcRetryOpt.rpcTimeoutMs = 500;
    opt.metaServerOpt.rpcRetryOpt.addrs.push_back("127.0.0.1:9104");
    opt.ioOpt.reqSchdulerOpt.scheduleQueueCapacity = 4096;
    opt.ioOpt.reqSchdulerOpt.scheduleThreadpoolSize = 2;
    opt.ioOpt.ioSenderOpt.failRequestOpt.chunkserverOPMaxRetry = 3;
    opt.ioOpt.ioSenderOpt.failRequestOpt.chunkserverOPRetryIntervalUS = 500;
    opt.ioOpt.metaCacheOpt.metacacheGetLeaderRetry = 3;
    opt.ioOpt.ioSenderOpt.chunkserverEnableAppliedIndexRead = 1;
    opt.ioOpt.ioSplitOpt.fileIOSplitMaxSizeKB = 64;
    opt.ioOpt.reqSchdulerOpt.ioSenderOpt = opt.ioOpt.ioSenderOpt;
    opt.loginfo.logLevel = 0;

    SnapshotClient cl;
    ASSERT_TRUE(!cl.Init(opt));

    UserInfo_t rootuserinfo;
    rootuserinfo.owner ="root";
    rootuserinfo.password = "123";

    std::string filename = "./1_usertest_.img";
    brpc::Server server;
    uint64_t seq = 1;
    // test create snap
    // normal test
    ::curve::mds::CreateSnapShotResponse response;
    response.set_statuscode(::curve::mds::StatusCode::kOK);
    response.clear_snapshotfileinfo();
    FakeReturn* fakeret
     = new FakeReturn(nullptr, static_cast<void*>(&response));
    curvefsservice.SetCreateSnapShot(fakeret);
    ASSERT_EQ(-LIBCURVE_ERROR::FAILED, cl.CreateSnapShot(filename,
                                                        rootuserinfo,
                                                        &seq));

    // set response
    response.set_statuscode(::curve::mds::StatusCode::kOwnerAuthFail);
    ::curve::mds::FileInfo* finf = new ::curve::mds::FileInfo;
    finf->set_filename(filename);
    finf->set_id(1);
    finf->set_parentid(0);
    finf->set_filetype(curve::mds::FileType::INODE_PAGEFILE);
    finf->set_chunksize(16 * 1024 * 1024);
    finf->set_length(1 * 1024 * 1024 * 1024);
    finf->set_ctime(12345678);
    finf->set_seqnum(2);
    finf->set_segmentsize(1 * 1024 * 1024 * 1024);
    response.set_allocated_snapshotfileinfo(finf);
    FakeReturn* fakeret1
     = new FakeReturn(nullptr, static_cast<void*>(&response));
    curvefsservice.SetCreateSnapShot(fakeret1);

    ASSERT_EQ(-LIBCURVE_ERROR::AUTHFAIL, cl.CreateSnapShot(filename,
                                                        rootuserinfo,
                                                        &seq));

    // test delete
    // normal delete test
    ::curve::mds::DeleteSnapShotResponse delresponse;
    delresponse.set_statuscode(::curve::mds::StatusCode::kOwnerAuthFail);
    FakeReturn* delfakeret
     = new FakeReturn(nullptr, static_cast<void*>(&delresponse));

    curvefsservice.SetDeleteSnapShot(delfakeret);
    ASSERT_EQ(-LIBCURVE_ERROR::AUTHFAIL, cl.DeleteSnapShot(filename,
                                                        rootuserinfo,
                                                        seq));

    // test get SegmentInfo
    // normal getinfo
    curve::mds::GetOrAllocateSegmentResponse* getresponse =
                        new curve::mds::GetOrAllocateSegmentResponse();
    curve::mds::PageFileSegment* pfs = new curve::mds::PageFileSegment;
    pfs->set_logicalpoolid(0);
    pfs->set_segmentsize(1ull*1024*1024*1024);
    pfs->set_chunksize(16ull*1024*1024);
    pfs->set_startoffset(0);
    getresponse->set_statuscode(::curve::mds::StatusCode::kOwnerAuthFail);
    getresponse->set_allocated_pagefilesegment(pfs);
    FakeReturn* getfakeret = new FakeReturn(nullptr,
                                    static_cast<void*>(getresponse));
    curvefsservice.SetGetSnapshotSegmentInfo(getfakeret);

    ::curve::mds::topology::GetChunkServerListInCopySetsResponse* geresponse_1 =
          new  ::curve::mds::topology::GetChunkServerListInCopySetsResponse();
    geresponse_1->set_statuscode(0);
    FakeReturn* faktopologyeret = new FakeReturn(nullptr,
        static_cast<void*>(geresponse_1));
    topologyservice.SetFakeReturn(faktopologyeret);

    SegmentInfo seginfo;
    LogicalPoolCopysetIDInfo lpcsIDInfo;
    ASSERT_EQ(-LIBCURVE_ERROR::AUTHFAIL,
            cl.GetSnapshotSegmentInfo(filename,
                                      rootuserinfo,
                                      0, 0, &seginfo));

    // test list snapshot
    // normal delete test
    ::curve::mds::ListSnapShotFileInfoResponse listresponse;
    listresponse.add_fileinfo();
    listresponse.mutable_fileinfo(0)->set_filename(filename);
    listresponse.mutable_fileinfo(0)->set_id(1);
    listresponse.mutable_fileinfo(0)->set_parentid(0);
    listresponse.mutable_fileinfo(0)->set_filetype(curve::mds::FileType::INODE_PAGEFILE);    // NOLINT
    listresponse.mutable_fileinfo(0)->set_chunksize(4 * 1024 * 1024);
    listresponse.mutable_fileinfo(0)->set_length(4 * 1024 * 1024 * 1024ul);
    listresponse.mutable_fileinfo(0)->set_ctime(12345678);
    listresponse.mutable_fileinfo(0)->set_seqnum(0);
    listresponse.mutable_fileinfo(0)->set_segmentsize(1 * 1024 * 1024 * 1024ul);

    listresponse.set_statuscode(::curve::mds::StatusCode::kOwnerAuthFail);
    FakeReturn* listfakeret
     = new FakeReturn(nullptr, static_cast<void*>(&listresponse));
    curve::client::FInfo_t sinfo;
    curvefsservice.SetListSnapShot(listfakeret);
    ASSERT_EQ(-LIBCURVE_ERROR::AUTHFAIL, cl.GetSnapShot(filename,
                                                        rootuserinfo,
                                                        seq, &sinfo));

    std::vector<uint64_t> seqvec;
    std::map<uint64_t, curve::client::FInfo_t> fivec;
    seqvec.push_back(seq);
    curve::client::FInfo_t ffinfo;
    ASSERT_EQ(-LIBCURVE_ERROR::AUTHFAIL,
                cl.ListSnapShot(filename, rootuserinfo,
                                &seqvec, &fivec));
    cl.UnInit();

    delete fakeret;
    delete fakeret1;
    delete listfakeret;
    delete delfakeret;
}

}  // namespace client
}  // namespace curve
