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
 * File Created: Thursday, 27th December 2018 11:37:33 am
 * Author: tongguangxun
 */
#include <glog/logging.h>
#include <gtest/gtest.h>
#include <brpc/server.h>
#include <brpc/controller.h>
#include <sys/types.h>
#include <unistd.h>
#include <sys/stat.h>
#include <fcntl.h>

#include <chrono>   // NOLINT
#include <thread>   // NOLINT
#include <string>

#include "src/client/client_config.h"
#include "src/client/metacache.h"
#include "test/client/fake/mock_schedule.h"
#include "test/client/fake/fakeMDS.h"
#include "src/client/libcurve_snapshot.h"
#include "src/client/iomanager4chunk.h"
#include "src/client/client_common.h"
#include "include/client/libcurve.h"

extern std::string mdsMetaServerAddr;

namespace curve {
namespace client {

class SnapCloneClosureTest : public curve::client::SnapCloneClosure {
 public:
    void Run() {}
};

TEST(SnapInstance, SnapShotTest) {
    ClientConfigOption opt;
    opt.metaServerOpt.mdsMaxRetryMS = 1000;
    opt.metaServerOpt.rpcRetryOpt.rpcTimeoutMs = 500;
    opt.metaServerOpt.rpcRetryOpt.addrs.push_back("127.0.0.1:9103");
    opt.ioOpt.reqSchdulerOpt.scheduleQueueCapacity = 4096;
    opt.ioOpt.reqSchdulerOpt.scheduleThreadpoolSize = 2;
    opt.ioOpt.ioSenderOpt.failRequestOpt.chunkserverOPMaxRetry = 3;
    opt.ioOpt.ioSenderOpt.failRequestOpt.chunkserverOPRetryIntervalUS = 500;
    opt.ioOpt.metaCacheOpt.metacacheGetLeaderRetry = 3;
    opt.ioOpt.ioSplitOpt.fileIOSplitMaxSizeKB = 64;
    opt.ioOpt.reqSchdulerOpt.ioSenderOpt = opt.ioOpt.ioSenderOpt;
    opt.loginfo.logLevel = 0;

    UserInfo_t userinfo;
    userinfo.owner = "test";
    UserInfo_t emptyuserinfo;

    SnapshotClient cl;
    ASSERT_TRUE(!cl.Init(opt));

    uint64_t seq = 1;
    std::string filename = "./1_test_.img";

    brpc::Server server;
    FakeMDSCurveFSService curvefsservice;
    FakeMDSTopologyService topologyservice;

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
    ASSERT_EQ(server.Start(mdsMetaServerAddr.c_str(), &options), 0);

    // test create snap
    // normal test
    ::curve::mds::CreateSnapShotResponse response;
    response.set_statuscode(::curve::mds::StatusCode::kOK);
    response.clear_snapshotfileinfo();
    FakeReturn* fakeret
     = new FakeReturn(nullptr, static_cast<void*>(&response));
    curvefsservice.SetCreateSnapShot(fakeret);
    ASSERT_EQ(-LIBCURVE_ERROR::FAILED, cl.CreateSnapShot(filename,
                                                        userinfo,
                                                        &seq));

    // set sequence
    response.set_statuscode(::curve::mds::StatusCode::kOK);
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
    ASSERT_EQ(LIBCURVE_ERROR::OK, cl.CreateSnapShot(filename,
                                                    emptyuserinfo,
                                                    &seq));
    ASSERT_EQ(2, seq);

    // rpc failed
    curvefsservice.CleanRetryTimes();
    brpc::Controller cntl;
    cntl.SetFailed(-1, "test fail");
    FakeReturn* fakeret2
     = new FakeReturn(&cntl, static_cast<void*>(&response));
    curvefsservice.SetCreateSnapShot(fakeret2);
    ASSERT_EQ(-LIBCURVE_ERROR::FAILED, cl.CreateSnapShot(filename,
                                                        userinfo,
                                                        &seq));

    // set return kFileUnderSnapShot
    response.set_statuscode(::curve::mds::StatusCode::kFileUnderSnapShot);
    FakeReturn* checkfakeret3
     = new FakeReturn(nullptr, static_cast<void*>(&response));
    curvefsservice.SetCreateSnapShot(checkfakeret3);
    ASSERT_EQ(-LIBCURVE_ERROR::UNDER_SNAPSHOT, cl.CreateSnapShot(filename,
                                                        userinfo,
                                                        &seq));

    // set return kClientVersionNotMatch
    ::curve::mds::CreateSnapShotResponse versionNotMatchResponse;
    versionNotMatchResponse.set_statuscode(
        curve::mds::StatusCode::kClientVersionNotMatch);
    std::unique_ptr<FakeReturn> versionNotMatchFakeRetrun(
        new FakeReturn(nullptr, static_cast<void*>(&versionNotMatchResponse)));

    curvefsservice.SetCreateSnapShot(versionNotMatchFakeRetrun.get());
    ASSERT_EQ(-LIBCURVE_ERROR::CLIENT_NOT_SUPPORT_SNAPSHOT,
              cl.CreateSnapShot(filename, userinfo, &seq));

    // test renamefile
    ::curve::mds::RenameFileResponse renameresp;
    renameresp.set_statuscode(::curve::mds::StatusCode::kOK);
    FakeReturn* renamefakeret
     = new FakeReturn(nullptr, static_cast<void*>(&renameresp));
    curvefsservice.SetRenameFile(renamefakeret);
    ASSERT_EQ(LIBCURVE_ERROR::OK, cl.RenameCloneFile(userinfo,
                                                        1, 2, "1", "2"));

    renameresp.set_statuscode(::curve::mds::StatusCode::kOwnerAuthFail);
    FakeReturn* renamefakeret1
     = new FakeReturn(nullptr, static_cast<void*>(&renameresp));
    curvefsservice.SetRenameFile(renamefakeret1);
    ASSERT_EQ(-LIBCURVE_ERROR::AUTHFAIL, cl.RenameCloneFile(userinfo,
                                                        1, 2, "1", "2"));
    ASSERT_EQ(2, seq);

    curvefsservice.CleanRetryTimes();
    brpc::Controller renamecntl;
    renamecntl.SetFailed(-1, "test fail");
    FakeReturn* renamefake2
     = new FakeReturn(&renamecntl, static_cast<void*>(&renameresp));
    curvefsservice.SetRenameFile(renamefake2);
    ASSERT_EQ(-LIBCURVE_ERROR::FAILED, cl.RenameCloneFile(userinfo,
                                                        1, 2, "1", "2"));

    // test delete
    // normal delete test
    ::curve::mds::DeleteSnapShotResponse delresponse;
    delresponse.set_statuscode(::curve::mds::StatusCode::kOK);
    FakeReturn* delfakeret
     = new FakeReturn(nullptr, static_cast<void*>(&delresponse));

    curvefsservice.SetDeleteSnapShot(delfakeret);
    ASSERT_EQ(LIBCURVE_ERROR::OK, cl.DeleteSnapShot(filename,
                                                    emptyuserinfo,
                                                    seq));

    // snapshot not exist
    ::curve::mds::DeleteSnapShotResponse noexist_resp;
    noexist_resp.set_statuscode(::curve::mds::StatusCode::kSnapshotFileNotExists);      // NOLINT
    FakeReturn* notexist_fakeret
     = new FakeReturn(nullptr, static_cast<void*>(&noexist_resp));

    curvefsservice.SetDeleteSnapShot(notexist_fakeret);
    ASSERT_EQ(-LIBCURVE_ERROR::NOTEXIST, cl.DeleteSnapShot(filename,
                                                    emptyuserinfo,
                                                    seq));

    // rpc fail
    curvefsservice.CleanRetryTimes();
    brpc::Controller delcntl;
    delcntl.SetFailed(-1, "test fail");
    curve::mds::DeleteSnapShotResponse deleteSnapshotResponse;
    FakeReturn* delfakeret2
     = new FakeReturn(&delcntl, static_cast<void*>(&deleteSnapshotResponse));
    curvefsservice.SetDeleteSnapShot(delfakeret2);
    ASSERT_EQ(-LIBCURVE_ERROR::FAILED, cl.DeleteSnapShot(filename,
                                                        userinfo,
                                                        seq));

    // test get SegmentInfo
    // normal getinfo
    curve::mds::GetOrAllocateSegmentResponse* getresponse =
                        new curve::mds::GetOrAllocateSegmentResponse();
    curve::mds::PageFileSegment* pfs = new curve::mds::PageFileSegment;
    getresponse->set_statuscode(::curve::mds::StatusCode::kOK);
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
    ASSERT_EQ(-LIBCURVE_ERROR::FAILED,
            cl.GetSnapshotSegmentInfo(filename,
                                      emptyuserinfo,
                                      0, 0, &seginfo));

    // normal segment info
    curve::mds::GetOrAllocateSegmentResponse* getresponse1 =
                      new  curve::mds::GetOrAllocateSegmentResponse();
    getresponse1->set_statuscode(::curve::mds::StatusCode::kOK);
    getresponse1->set_allocated_pagefilesegment(pfs);
    getresponse1->mutable_pagefilesegment()->set_logicalpoolid(1234);
    getresponse1->mutable_pagefilesegment()->set_segmentsize(1*1024*1024*1024ul);      // NOLINT
    getresponse1->mutable_pagefilesegment()->set_chunksize(4 * 1024 * 1024);
    getresponse1->mutable_pagefilesegment()->set_startoffset(0);
    for (int i = 0; i < 256; i ++) {
        auto chunk = getresponse1->mutable_pagefilesegment()->add_chunks();
        chunk->set_copysetid(i);
        chunk->set_chunkid(i);
    }
    FakeReturn* getfakeret1 = new FakeReturn(nullptr,
                static_cast<void*>(getresponse1));
    curvefsservice.SetGetSnapshotSegmentInfo(getfakeret1);
    ASSERT_EQ(LIBCURVE_ERROR::OK,
            cl.GetSnapshotSegmentInfo(filename, userinfo,
                                      0, 0, &seginfo));

    ASSERT_EQ(seginfo.segmentsize, 1*1024*1024*1024ul);
    ASSERT_EQ(seginfo.chunksize, 4 * 1024 * 1024);
    for (int i = 0; i < 256; i ++) {
        ASSERT_EQ(seginfo.chunkvec[i].cid_, i);
    }

    // rpc return not exist
    curvefsservice.CleanRetryTimes();
    curve::mds::GetOrAllocateSegmentResponse* getresp =
            new curve::mds::GetOrAllocateSegmentResponse();
    getresp->set_statuscode(::curve::mds::StatusCode::kSnapshotFileNotExists);
    FakeReturn* getfake = new FakeReturn(nullptr,
                                        static_cast<void*>(getresp));
    curvefsservice.SetGetSnapshotSegmentInfo(getfake);
    ASSERT_EQ(-LIBCURVE_ERROR::NOTEXIST,
            cl.GetSnapshotSegmentInfo(filename, emptyuserinfo,
                                      0, 0, &seginfo));

    // rpc fail
    curvefsservice.CleanRetryTimes();
    curve::mds::GetOrAllocateSegmentResponse* getresponse2 =
            new curve::mds::GetOrAllocateSegmentResponse();
    getresponse2->set_statuscode(::curve::mds::StatusCode::kOK);
    brpc::Controller getcntl;
    getcntl.SetFailed(-1, "test fail");
    FakeReturn* getfakeret2 = new FakeReturn(&getcntl,
                static_cast<void*>(getresponse2));
    curvefsservice.SetGetSnapshotSegmentInfo(getfakeret2);
    ASSERT_EQ(-LIBCURVE_ERROR::FAILED,
            cl.GetSnapshotSegmentInfo(filename, userinfo,
                                      0, 0, &seginfo));

    // test list snapshot
    // normal delete test
    ::curve::mds::ListSnapShotFileInfoResponse* listresponse
    = new ::curve::mds::ListSnapShotFileInfoResponse;
    listresponse->add_fileinfo();
    listresponse->mutable_fileinfo(0)->set_filename(filename);
    listresponse->mutable_fileinfo(0)->set_id(1);
    listresponse->mutable_fileinfo(0)->set_parentid(0);
    listresponse->mutable_fileinfo(0)->set_filetype(curve::mds::FileType::INODE_PAGEFILE);    // NOLINT
    listresponse->mutable_fileinfo(0)->set_chunksize(4 * 1024 * 1024);
    listresponse->mutable_fileinfo(0)->set_length(4 * 1024 * 1024 * 1024ul);
    listresponse->mutable_fileinfo(0)->set_ctime(12345678);
    listresponse->mutable_fileinfo(0)->set_seqnum(seq);
    listresponse->mutable_fileinfo(0)->set_segmentsize(1 * 1024 * 1024 * 1024ul);   //  NOLINT

    listresponse->set_statuscode(::curve::mds::StatusCode::kOK);
    FakeReturn* listfakeret
     = new FakeReturn(nullptr, static_cast<void*>(listresponse));
    curve::client::FInfo_t sinfo;
    curvefsservice.SetListSnapShot(listfakeret);
    ASSERT_EQ(LIBCURVE_ERROR::OK, cl.GetSnapShot(filename,
                                                emptyuserinfo,
                                                seq, &sinfo));
    ASSERT_EQ(sinfo.id, 1);
    ASSERT_EQ(sinfo.filename, filename.c_str());
    ASSERT_EQ(sinfo.parentid, 0);
    ASSERT_EQ(sinfo.chunksize, 4 * 1024 * 1024);
    ASSERT_EQ(sinfo.ctime, 12345678);
    ASSERT_EQ(sinfo.seqnum, seq);
    ASSERT_EQ(sinfo.segmentsize, 1 * 1024 * 1024 * 1024ul);
    ASSERT_EQ(sinfo.filetype, curve::mds::FileType::INODE_PAGEFILE);

    std::vector<uint64_t> seqvec;
    std::map<uint64_t, curve::client::FInfo_t> fimap;
    seqvec.push_back(1);
    seqvec.push_back(seq);
    ASSERT_EQ(-LIBCURVE_ERROR::NOTEXIST,
                cl.ListSnapShot(filename, emptyuserinfo, &seqvec, &fimap));

    listresponse->add_fileinfo();
    listresponse->mutable_fileinfo(1)->set_filename(filename);
    listresponse->mutable_fileinfo(1)->set_id(2);
    listresponse->mutable_fileinfo(1)->set_parentid(1);
    listresponse->mutable_fileinfo(1)->set_filetype(curve::mds::FileType::INODE_PAGEFILE);    // NOLINT
    listresponse->mutable_fileinfo(1)->set_chunksize(4 * 1024 * 1024);
    listresponse->mutable_fileinfo(1)->set_length(4 * 1024 * 1024 * 1024ul);
    listresponse->mutable_fileinfo(1)->set_ctime(12345678);
    listresponse->mutable_fileinfo(1)->set_seqnum(1);
    listresponse->mutable_fileinfo(1)->set_segmentsize(1 * 1024 * 1024 * 1024ul);   //  NOLINT

    ASSERT_EQ(0, cl.ListSnapShot(filename, emptyuserinfo, &seqvec, &fimap));
    ASSERT_EQ(fimap[seq].id, 1);
    ASSERT_EQ(fimap[seq].filename, filename.c_str());
    ASSERT_EQ(fimap[seq].parentid, 0);
    ASSERT_EQ(fimap[seq].chunksize, 4 * 1024 * 1024);
    ASSERT_EQ(fimap[seq].ctime, 12345678);
    ASSERT_EQ(fimap[seq].seqnum, seq);
    ASSERT_EQ(fimap[seq].segmentsize, 1 * 1024 * 1024 * 1024ul);
    ASSERT_EQ(fimap[seq].filetype, curve::mds::FileType::INODE_PAGEFILE);

    ASSERT_EQ(fimap[1].id, 2);
    ASSERT_EQ(fimap[1].filename, filename.c_str());
    ASSERT_EQ(fimap[1].parentid, 1);
    ASSERT_EQ(fimap[1].chunksize, 4 * 1024 * 1024);
    ASSERT_EQ(fimap[1].ctime, 12345678);
    ASSERT_EQ(fimap[1].seqnum, 1);
    ASSERT_EQ(fimap[1].segmentsize, 1 * 1024 * 1024 * 1024ul);
    ASSERT_EQ(fimap[1].filetype, curve::mds::FileType::INODE_PAGEFILE);

    // GetSnapShot when return not equal seq num
    ::curve::mds::ListSnapShotFileInfoResponse* listresponse_seq
    = new ::curve::mds::ListSnapShotFileInfoResponse;
    listresponse_seq->add_fileinfo();
    listresponse_seq->mutable_fileinfo(0)->set_filename(filename);
    listresponse_seq->mutable_fileinfo(0)->set_id(1);
    listresponse_seq->mutable_fileinfo(0)->set_parentid(0);
    listresponse_seq->mutable_fileinfo(0)->set_filetype(curve::mds::FileType::INODE_PAGEFILE);    // NOLINT
    listresponse_seq->mutable_fileinfo(0)->set_chunksize(4 * 1024 * 1024);
    listresponse_seq->mutable_fileinfo(0)->set_length(4 * 1024 * 1024 * 1024ul);
    listresponse_seq->mutable_fileinfo(0)->set_ctime(12345678);
    listresponse_seq->mutable_fileinfo(0)->set_seqnum(seq+1);
    listresponse_seq->mutable_fileinfo(0)->set_segmentsize(1 * 1024 * 1024 * 1024ul);   //  NOLINT

    listresponse_seq->set_statuscode(::curve::mds::StatusCode::kOK);
    FakeReturn* listfakeret_seq
     = new FakeReturn(nullptr, static_cast<void*>(listresponse_seq));
    curve::client::FInfo_t sinfo_seq;
    curvefsservice.SetListSnapShot(listfakeret_seq);
    ASSERT_EQ(-LIBCURVE_ERROR::NOTEXIST, cl.GetSnapShot(filename,
                                                emptyuserinfo,
                                                seq, &sinfo_seq));

    // rpc fail
    curvefsservice.CleanRetryTimes();
    ::curve::mds::ListSnapShotFileInfoResponse* listresponse1 =
        new ::curve::mds::ListSnapShotFileInfoResponse();
    brpc::Controller listcntl;
    listcntl.SetFailed(-1, "test fail");
    FakeReturn* listfakeret2
     = new FakeReturn(&listcntl, static_cast<void*>(listresponse1));
    curvefsservice.SetListSnapShot(listfakeret2);
    ASSERT_EQ(-LIBCURVE_ERROR::FAILED,
            cl.GetSnapShot(filename, userinfo, seq, &sinfo));

    curvefsservice.CleanRetryTimes();
    ASSERT_EQ(-LIBCURVE_ERROR::FAILED,
            cl.ListSnapShot(filename, userinfo, &seqvec, &fimap));

    // rpc fail
    ::curve::mds::CheckSnapShotStatusResponse* checkresponse1 =
        new ::curve::mds::CheckSnapShotStatusResponse();
    brpc::Controller checkcntl;
    checkcntl.SetFailed(-1, "test fail");
    FakeReturn* checkfakeret
     = new FakeReturn(&checkcntl, static_cast<void*>(checkresponse1));
    curvefsservice.SetCheckSnap(checkfakeret);
    ASSERT_EQ(-LIBCURVE_ERROR::FAILED,
            cl.CheckSnapShotStatus(filename, userinfo, seq, nullptr));

    // set return ok
    ::curve::mds::CheckSnapShotStatusResponse* checkresponse2 =
        new ::curve::mds::CheckSnapShotStatusResponse();
    checkresponse2->set_statuscode(::curve::mds::StatusCode::kOK);
    FakeReturn* checkfakeret1
     = new FakeReturn(nullptr, static_cast<void*>(checkresponse2));
    curvefsservice.SetCheckSnap(checkfakeret1);
    ASSERT_EQ(LIBCURVE_ERROR::OK,
            cl.CheckSnapShotStatus(filename, emptyuserinfo, seq, nullptr));

    // set return kOwnerAuthFail
    ::curve::mds::CheckSnapShotStatusResponse* checkresponse3 =
        new ::curve::mds::CheckSnapShotStatusResponse();
    checkresponse3->set_statuscode(::curve::mds::StatusCode::kOwnerAuthFail);
    FakeReturn* checkfakeret2
     = new FakeReturn(nullptr, static_cast<void*>(checkresponse3));
    curvefsservice.SetCheckSnap(checkfakeret2);
    ASSERT_EQ(-LIBCURVE_ERROR::AUTHFAIL,
            cl.CheckSnapShotStatus(filename, userinfo, seq, nullptr));

    // set return kSnapshotDeleting
    ::curve::mds::CheckSnapShotStatusResponse* checkresp6 =
        new ::curve::mds::CheckSnapShotStatusResponse();
    checkresp6->set_statuscode(::curve::mds::StatusCode::kSnapshotDeleting);
    FakeReturn* checkfakeret5
     = new FakeReturn(nullptr, static_cast<void*>(checkresp6));
    curvefsservice.SetCheckSnap(checkfakeret5);
    ASSERT_EQ(-LIBCURVE_ERROR::DELETING,
            cl.CheckSnapShotStatus(filename, userinfo, seq, nullptr));

    // set return kSnapshotFileNotExists
    ::curve::mds::CheckSnapShotStatusResponse* resp7 =
        new ::curve::mds::CheckSnapShotStatusResponse();
    resp7->set_statuscode(::curve::mds::StatusCode::kSnapshotFileNotExists);
    FakeReturn* checkfakeret6
     = new FakeReturn(nullptr, static_cast<void*>(resp7));
    curvefsservice.SetCheckSnap(checkfakeret6);
    ASSERT_EQ(-LIBCURVE_ERROR::NOTEXIST,
            cl.CheckSnapShotStatus(filename, userinfo, seq, nullptr));

    // set return kSnapshotFileDeleteError
    ::curve::mds::CheckSnapShotStatusResponse* resp8 =
        new ::curve::mds::CheckSnapShotStatusResponse();
    resp8->set_statuscode(::curve::mds::StatusCode::kSnapshotFileDeleteError);
    FakeReturn* checkfakeret7
     = new FakeReturn(nullptr, static_cast<void*>(resp8));
    curvefsservice.SetCheckSnap(checkfakeret7);
    ASSERT_EQ(-LIBCURVE_ERROR::DELETE_ERROR,
            cl.CheckSnapShotStatus(filename, userinfo, seq, nullptr));

    // set response file exist
    ::curve::mds::DeleteFileResponse deleteresponse;
    deleteresponse.set_statuscode(::curve::mds::StatusCode::kFileNotExists);

    FakeReturn* fakeretdelete
     = new FakeReturn(nullptr, static_cast<void*>(&deleteresponse));

    curvefsservice.SetDeleteFile(fakeretdelete);

    int ret = cl.DeleteFile(filename, userinfo, 10);
    ASSERT_EQ(ret, -1 * LIBCURVE_ERROR::NOTEXIST);

    cl.UnInit();

    ASSERT_EQ(0, server.Stop(0));
    ASSERT_EQ(0, server.Join());
    delete fakeret;
    delete fakeret1;
    delete fakeret2;
    delete listfakeret;
    delete listfakeret2;
    delete getfakeret1;
    delete getfakeret2;
    delete delfakeret2;
    delete delfakeret;
}

TEST(SnapInstance, ReadChunkSnapshotTest) {
    ClientConfigOption opt;
    opt.metaServerOpt.mdsMaxRetryMS = 1000;
    opt.metaServerOpt.rpcRetryOpt.addrs.push_back("127.0.0.1:9103");
    opt.ioOpt.reqSchdulerOpt.scheduleQueueCapacity = 4096;
    opt.ioOpt.reqSchdulerOpt.scheduleThreadpoolSize = 2;
    opt.ioOpt.ioSenderOpt.failRequestOpt.chunkserverOPMaxRetry = 3;
    opt.ioOpt.ioSenderOpt.failRequestOpt.chunkserverOPRetryIntervalUS = 500;
    opt.ioOpt.metaCacheOpt.metacacheGetLeaderRetry = 3;
    opt.ioOpt.ioSplitOpt.fileIOSplitMaxSizeKB = 64;
    opt.ioOpt.reqSchdulerOpt.ioSenderOpt = opt.ioOpt.ioSenderOpt;
    opt.loginfo.logLevel = 0;

    SnapCloneClosureTest scc, scc2;

    SnapshotClient cl;
    ASSERT_TRUE(!cl.Init(opt));
    auto max_split_size_kb = 1024 * 64;
    MockRequestScheduler* mocksch = new MockRequestScheduler;
    mocksch->DelegateToFake();

    // fake metacache
    MetaCache* mc = cl.GetIOManager4Chunk()->GetMetaCache();
    ChunkID cid = 1;
    CopysetInfo<ChunkServerID> cpinfo;
    mc->UpdateChunkInfoByID(cid, ChunkIDInfo(cid, 2, 3));
    mc->UpdateCopysetInfo(2, 3, cpinfo);

    IOManager4Chunk* ioctxmana = cl.GetIOManager4Chunk();
    ioctxmana->SetRequestScheduler(mocksch);

    uint64_t len = 4 * 1024 + 2 * max_split_size_kb;
    char* buf = new char[len];
    memset(buf, 0, len);
    LOG(ERROR) << "start read snap chunk";
    ioctxmana->ReadSnapChunk(ChunkIDInfo(cid, 2, 3), 0, 0,
                                         len, buf, &scc);

    std::this_thread::sleep_for(std::chrono::seconds(1));
    ASSERT_EQ(132 * 1024, scc.GetRetCode());

    LOG(ERROR) << "read snap chunk success!";
    ASSERT_EQ(buf[0], 'a');
    ASSERT_EQ(buf[max_split_size_kb - 1], 'a');
    ASSERT_EQ(buf[max_split_size_kb], 'b');
    ASSERT_EQ(buf[2 * max_split_size_kb - 1], 'b');
    ASSERT_EQ(buf[2 * max_split_size_kb], 'c');
    ASSERT_EQ(buf[len - 1], 'c');

    mocksch->EnableScheduleFailed();
    ASSERT_EQ(0,
    ioctxmana->ReadSnapChunk(ChunkIDInfo(cid, 2, 3), 0, 0, len, buf, &scc2));

    cl.UnInit();
}

TEST(SnapInstance, DeleteChunkSnapshotTest) {
    ClientConfigOption opt;
    opt.metaServerOpt.mdsMaxRetryMS = 1000;
    opt.metaServerOpt.rpcRetryOpt.addrs.push_back("127.0.0.1:9103");
    opt.ioOpt.reqSchdulerOpt.scheduleQueueCapacity = 4096;
    opt.ioOpt.reqSchdulerOpt.scheduleThreadpoolSize = 2;
    opt.ioOpt.ioSenderOpt.failRequestOpt.chunkserverOPMaxRetry = 3;
    opt.ioOpt.ioSenderOpt.failRequestOpt.chunkserverOPRetryIntervalUS = 500;
    opt.ioOpt.metaCacheOpt.metacacheGetLeaderRetry = 3;
    opt.ioOpt.ioSplitOpt.fileIOSplitMaxSizeKB = 64;
    opt.ioOpt.reqSchdulerOpt.ioSenderOpt = opt.ioOpt.ioSenderOpt;
    opt.loginfo.logLevel = 0;

    SnapshotClient cl;
    ASSERT_TRUE(!cl.Init(opt));
    MockRequestScheduler* mocksch = new MockRequestScheduler;
    mocksch->DelegateToFake();


    // fake metacache
    MetaCache* mc = cl.GetIOManager4Chunk()->GetMetaCache();
    ChunkID cid = 1;
    CopysetInfo<ChunkServerID> cpinfo;
    mc->UpdateChunkInfoByID(cid, ChunkIDInfo(cid, 2, 3));
    mc->UpdateCopysetInfo(2, 3, cpinfo);

    IOManager4Chunk* ioctxmana = cl.GetIOManager4Chunk();
    ioctxmana->SetRequestScheduler(mocksch);

    ASSERT_EQ(0, ioctxmana->DeleteSnapChunkOrCorrectSn(ChunkIDInfo(cid, 2, 3), 0));  // NOLINT

    mocksch->EnableScheduleFailed();
    ASSERT_EQ(-LIBCURVE_ERROR::FAILED,
    ioctxmana->DeleteSnapChunkOrCorrectSn(ChunkIDInfo(cid, 2, 3), 0));

    cl.UnInit();
}

TEST(SnapInstance, GetChunkInfoTest) {
    ClientConfigOption opt;
    opt.metaServerOpt.mdsMaxRetryMS = 1000;
    opt.metaServerOpt.rpcRetryOpt.addrs.push_back("127.0.0.1:9103");
    opt.ioOpt.reqSchdulerOpt.scheduleQueueCapacity = 4096;
    opt.ioOpt.reqSchdulerOpt.scheduleThreadpoolSize = 2;
    opt.ioOpt.ioSenderOpt.failRequestOpt.chunkserverOPMaxRetry = 3;
    opt.ioOpt.ioSenderOpt.failRequestOpt.chunkserverOPRetryIntervalUS = 500;
    opt.ioOpt.metaCacheOpt.metacacheGetLeaderRetry = 3;
    opt.ioOpt.ioSplitOpt.fileIOSplitMaxSizeKB = 64;
    opt.ioOpt.reqSchdulerOpt.ioSenderOpt = opt.ioOpt.ioSenderOpt;
    opt.loginfo.logLevel = 0;

    SnapshotClient cl;
    ASSERT_TRUE(!cl.Init(opt));
    MockRequestScheduler* mocksch = new MockRequestScheduler;
    mocksch->DelegateToFake();

    // fake metacache
    MetaCache* mc = cl.GetIOManager4Chunk()->GetMetaCache();
    ChunkID cid = 1;
    CopysetInfo<ChunkServerID> cpinfo;
    mc->UpdateChunkInfoByID(cid, ChunkIDInfo(cid, 2, 3));
    mc->UpdateCopysetInfo(2, 3, cpinfo);

    IOManager4Chunk* ioctxmana = cl.GetIOManager4Chunk();
    ioctxmana->SetRequestScheduler(mocksch);
    ChunkInfoDetail cinfode;
    ASSERT_EQ(0, ioctxmana->GetChunkInfo(ChunkIDInfo(cid, 2, 3), &cinfode));

    ASSERT_EQ(2222, cinfode.chunkSn[0]);

    mocksch->EnableScheduleFailed();
    ASSERT_EQ(-LIBCURVE_ERROR::FAILED,
    ioctxmana->GetChunkInfo(ChunkIDInfo(cid, 2, 3), &cinfode));

    cl.UnInit();
}

TEST(SnapInstance, RecoverChunkTest) {
    ClientConfigOption opt;
    opt.metaServerOpt.mdsMaxRetryMS = 1000;
    opt.metaServerOpt.rpcRetryOpt.addrs.push_back("127.0.0.1:9103");
    opt.ioOpt.reqSchdulerOpt.scheduleQueueCapacity = 4096;
    opt.ioOpt.reqSchdulerOpt.scheduleThreadpoolSize = 2;
    opt.ioOpt.ioSenderOpt.failRequestOpt.chunkserverOPMaxRetry = 3;
    opt.ioOpt.ioSenderOpt.failRequestOpt.chunkserverOPRetryIntervalUS = 500;
    opt.ioOpt.metaCacheOpt.metacacheGetLeaderRetry = 3;
    opt.ioOpt.ioSplitOpt.fileIOSplitMaxSizeKB = 64;
    opt.ioOpt.reqSchdulerOpt.ioSenderOpt = opt.ioOpt.ioSenderOpt;
    opt.loginfo.logLevel = 0;

    SnapCloneClosureTest scc, scc2;

    SnapshotClient cl;
    ASSERT_TRUE(!cl.Init(opt));
    MockRequestScheduler* mocksch = new MockRequestScheduler;
    mocksch->DelegateToFake();

    // fake metacache
    MetaCache* mc = cl.GetIOManager4Chunk()->GetMetaCache();
    ChunkID cid = 1;
    CopysetInfo<ChunkServerID> cpinfo;
    mc->UpdateChunkInfoByID(cid, ChunkIDInfo(cid, 2, 3));
    mc->UpdateCopysetInfo(2, 3, cpinfo);

    IOManager4Chunk* ioctxmana = cl.GetIOManager4Chunk();
    ioctxmana->SetRequestScheduler(mocksch);
    ioctxmana->RecoverChunk(ChunkIDInfo(cid, 2, 3), 1, 4*1024*1024, &scc);

    std::this_thread::sleep_for(std::chrono::seconds(1));
    ASSERT_EQ(LIBCURVE_ERROR::OK, scc.GetRetCode());

    mocksch->EnableScheduleFailed();
    ASSERT_EQ(0,
    ioctxmana->RecoverChunk(ChunkIDInfo(cid, 2, 3), 1, 4*1024*1024, &scc2));

    cl.UnInit();
}

TEST(SnapInstance, CreateCloneChunkTest) {
    ClientConfigOption opt;
    opt.metaServerOpt.mdsMaxRetryMS = 1000;
    opt.metaServerOpt.rpcRetryOpt.addrs.push_back("127.0.0.1:9103");
    opt.ioOpt.reqSchdulerOpt.scheduleQueueCapacity = 4096;
    opt.ioOpt.reqSchdulerOpt.scheduleThreadpoolSize = 2;
    opt.ioOpt.ioSenderOpt.failRequestOpt.chunkserverOPMaxRetry = 3;
    opt.ioOpt.ioSenderOpt.failRequestOpt.chunkserverOPRetryIntervalUS = 500;
    opt.ioOpt.metaCacheOpt.metacacheGetLeaderRetry = 3;
    opt.ioOpt.ioSplitOpt.fileIOSplitMaxSizeKB = 64;
    opt.ioOpt.reqSchdulerOpt.ioSenderOpt = opt.ioOpt.ioSenderOpt;
    opt.loginfo.logLevel = 0;

    SnapCloneClosureTest scc, scc2;

    SnapshotClient cl;
    ASSERT_TRUE(!cl.Init(opt));
    MockRequestScheduler* mocksch = new MockRequestScheduler;
    mocksch->DelegateToFake();

    // fake metacache
    MetaCache* mc = cl.GetIOManager4Chunk()->GetMetaCache();
    ChunkID cid = 1;
    CopysetInfo<ChunkServerID> cpinfo;
    mc->UpdateChunkInfoByID(cid, ChunkIDInfo(cid, 2, 3));
    mc->UpdateCopysetInfo(2, 3, cpinfo);

    IOManager4Chunk* ioctxmana = cl.GetIOManager4Chunk();
    ioctxmana->SetRequestScheduler(mocksch);
    ioctxmana->CreateCloneChunk("destination", ChunkIDInfo(cid, 2, 3),
                                1, 2, 1024, &scc);

    std::this_thread::sleep_for(std::chrono::seconds(1));
    ASSERT_EQ(LIBCURVE_ERROR::OK, scc.GetRetCode());

    mocksch->EnableScheduleFailed();
    ASSERT_EQ(0,
    ioctxmana->CreateCloneChunk("destination", ChunkIDInfo(cid, 2, 3),
                                1, 2, 1024, &scc2));

    cl.UnInit();
}

}  // namespace client
}  // namespace curve

std::string mdsMetaServerAddr = "127.0.0.1:9103";     // NOLINT
uint32_t segment_size = 1 * 1024 * 1024 * 1024ul;   // NOLINT
uint32_t chunk_size = 4 * 1024 * 1024;   // NOLINT

int main(int argc, char ** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    ::testing::InitGoogleMock(&argc, argv);
    google::ParseCommandLineFlags(&argc, &argv, false);

    int ret = RUN_ALL_TESTS();
    return ret;
}
