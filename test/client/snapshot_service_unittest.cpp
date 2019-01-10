/*
 * Project: curve
 * File Created: Thursday, 27th December 2018 11:37:33 am
 * Author: tongguangxun
 * Copyright (c)￼ 2018 netease
 */
#include <glog/logging.h>
#include <gtest/gtest.h>
#include <brpc/server.h>
#include <brpc/controller.h>

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
#include "src/client/libcurve_define.h"

extern std::string configpath;
extern std::string metaserver_addr;

using curve::client::SegmentInfo;
using curve::client::ChunkInfoDetail;
using curve::client::SnapshotClient;
using curve::client::ChunkID;
using curve::client::LogicPoolID;
using curve::client::CopysetID;
using curve::client::ChunkIDInfo_t;
using curve::client::CopysetInfo_t;
using curve::client::MetaCache;
using curve::client::IOManager4Chunk;
using curve::client::LogicalPoolCopysetIDInfo;

TEST(SnapInstance, SnapShotTest) {
    ClientConfigOption_t opt;
    opt.metaserveropt.rpc_timeout_ms = 500;
    opt.metaserveropt.rpc_retry_times = 3;
    opt.metaserveropt.metaaddrvec.push_back("127.0.0.1:8000");
    opt.ioopt.reqschopt.request_scheduler_queue_capacity = 4096;
    opt.ioopt.reqschopt.request_scheduler_threadpool_size = 2;
    opt.ioopt.iosenderopt.failreqopt.client_chunk_op_max_retry = 3;
    opt.ioopt.iosenderopt.failreqopt.client_chunk_op_retry_interval_us = 500;
    opt.ioopt.metacacheopt.get_leader_retry = 3;
    opt.ioopt.iosenderopt.enable_applied_index_read = 1;
    opt.ioopt.iosplitopt.io_split_max_size_kb = 64;
    opt.ioopt.reqschopt.iosenderopt = opt.ioopt.iosenderopt;
    opt.loglevel = 0;

    SnapshotClient cl;
    ASSERT_TRUE(!cl.Init(opt));
    std::string filename = "./test.img";
    brpc::Server server;
    uint64_t seq = 1;
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
    ASSERT_EQ(server.Start(metaserver_addr.c_str(), &options), 0);

    // test create snap
    // normal test
    ::curve::mds::CreateSnapShotResponse response;
    response.set_statuscode(::curve::mds::StatusCode::kOK);
    response.clear_snapshotfileinfo();
    FakeReturn* fakeret
     = new FakeReturn(nullptr, static_cast<void*>(&response));
    curvefsservice.SetCreateSnapShot(fakeret);
    ASSERT_EQ(LIBCURVE_ERROR::FAILED, cl.CreateSnapShot(filename, &seq));

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
    ASSERT_EQ(LIBCURVE_ERROR::OK, cl.CreateSnapShot(filename, &seq));
    ASSERT_EQ(2, seq);

    // rpc failed
    brpc::Controller cntl;
    cntl.SetFailed(-1, "test fail");
    FakeReturn* fakeret2
     = new FakeReturn(&cntl, static_cast<void*>(&response));
    curvefsservice.SetCreateSnapShot(fakeret2);
    ASSERT_EQ(LIBCURVE_ERROR::FAILED, cl.CreateSnapShot(filename, &seq));


    // test delete
    // normal delete test
    ::curve::mds::DeleteSnapShotResponse delresponse;
    delresponse.set_statuscode(::curve::mds::StatusCode::kOK);
    FakeReturn* delfakeret
     = new FakeReturn(nullptr, static_cast<void*>(&delresponse));

    curvefsservice.SetDeleteSnapShot(delfakeret);
    ASSERT_EQ(LIBCURVE_ERROR::OK, cl.DeleteSnapShot(filename, seq));

    // rpc fail
    brpc::Controller delcntl;
    delcntl.SetFailed(-1, "test fail");
    FakeReturn* delfakeret2
     = new FakeReturn(&delcntl, static_cast<void*>(&response));
    curvefsservice.SetDeleteSnapShot(delfakeret2);
    ASSERT_EQ(LIBCURVE_ERROR::FAILED, cl.DeleteSnapShot(filename, seq));

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
    ASSERT_EQ(LIBCURVE_ERROR::FAILED,
            cl.GetSnapshotSegmentInfo(filename, &lpcsIDInfo, 0, 0, &seginfo));

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
            cl.GetSnapshotSegmentInfo(filename, &lpcsIDInfo, 0, 0, &seginfo));
    ASSERT_EQ(LIBCURVE_ERROR::OK,
            cl.GetServerList(lpcsIDInfo.lpid, lpcsIDInfo.cpidVec));

    ASSERT_EQ(seginfo.segmentsize, 1*1024*1024*1024ul);
    ASSERT_EQ(seginfo.chunksize, 4 * 1024 * 1024);
    for (int i = 0; i < 256; i ++) {
        ASSERT_EQ(seginfo.chunkvec[i].cid_, i);
    }
    // rpc fail
    curve::mds::GetOrAllocateSegmentResponse* getresponse2 =
            new curve::mds::GetOrAllocateSegmentResponse();
    getresponse2->set_statuscode(::curve::mds::StatusCode::kOK);
    brpc::Controller getcntl;
    getcntl.SetFailed(-1, "test fail");
    FakeReturn* getfakeret2 = new FakeReturn(&getcntl,
                static_cast<void*>(getresponse2));
    curvefsservice.SetGetSnapshotSegmentInfo(getfakeret2);
    ASSERT_EQ(LIBCURVE_ERROR::FAILED,
            cl.GetSnapshotSegmentInfo(filename, &lpcsIDInfo, 0, 0, &seginfo));
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

    listresponse.set_statuscode(::curve::mds::StatusCode::kOK);
    FakeReturn* listfakeret
     = new FakeReturn(nullptr, static_cast<void*>(&listresponse));
    curve::client::FInfo_t sinfo;
    curvefsservice.SetListSnapShot(listfakeret);
    ASSERT_EQ(LIBCURVE_ERROR::OK, cl.GetSnapShot(filename, seq, &sinfo));

    ASSERT_EQ(sinfo.id, 1);
    ASSERT_STREQ(sinfo.filename, filename.c_str());
    ASSERT_EQ(sinfo.parentid, 0);
    ASSERT_EQ(sinfo.chunksize, 4 * 1024 * 1024);
    ASSERT_EQ(sinfo.ctime, 12345678);
    ASSERT_EQ(sinfo.seqnum, 0);
    ASSERT_EQ(sinfo.segmentsize, 1 * 1024 * 1024 * 1024ul);
    ASSERT_EQ(sinfo.filetype, curve::mds::FileType::INODE_PAGEFILE);

    std::vector<uint64_t> seqvec;
    std::vector<curve::client::FInfo_t*> fivec;
    seqvec.push_back(seq);
    curve::client::FInfo_t ffinfo;
    fivec.push_back(&ffinfo);
    ASSERT_EQ(LIBCURVE_ERROR::OK,
                cl.ListSnapShot(filename, &seqvec, &fivec));

    ASSERT_EQ(fivec[0]->id, 1);
    ASSERT_STREQ(fivec[0]->filename, filename.c_str());
    ASSERT_EQ(fivec[0]->parentid, 0);
    ASSERT_EQ(fivec[0]->chunksize, 4 * 1024 * 1024);
    ASSERT_EQ(fivec[0]->ctime, 12345678);
    ASSERT_EQ(fivec[0]->seqnum, 0);
    ASSERT_EQ(fivec[0]->segmentsize, 1 * 1024 * 1024 * 1024ul);
    ASSERT_EQ(fivec[0]->filetype, curve::mds::FileType::INODE_PAGEFILE);

    // rpc fail
    ::curve::mds::ListSnapShotFileInfoResponse* listresponse1 =
        new ::curve::mds::ListSnapShotFileInfoResponse();
    brpc::Controller listcntl;
    cntl.SetFailed(-1, "test fail");
    FakeReturn* listfakeret2
     = new FakeReturn(&listcntl, static_cast<void*>(listresponse1));
    curvefsservice.SetListSnapShot(listfakeret2);
    ASSERT_EQ(LIBCURVE_ERROR::FAILED,
            cl.GetSnapShot(filename, seq, &sinfo));
    ASSERT_EQ(LIBCURVE_ERROR::FAILED,
            cl.ListSnapShot(filename, &seqvec, &fivec));


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
    ClientConfigOption_t opt;
    opt.metaserveropt.metaaddrvec.push_back("127.0.0.1:8000");
    opt.ioopt.reqschopt.request_scheduler_queue_capacity = 4096;
    opt.ioopt.reqschopt.request_scheduler_threadpool_size = 2;
    opt.ioopt.iosenderopt.failreqopt.client_chunk_op_max_retry = 3;
    opt.ioopt.iosenderopt.failreqopt.client_chunk_op_retry_interval_us = 500;
    opt.ioopt.metacacheopt.get_leader_retry = 3;
    opt.ioopt.iosenderopt.enable_applied_index_read = 1;
    opt.ioopt.iosplitopt.io_split_max_size_kb = 64;
    opt.ioopt.reqschopt.iosenderopt = opt.ioopt.iosenderopt;
    opt.loglevel = 0;

    SnapshotClient cl;
    ASSERT_TRUE(!cl.Init(opt));
    auto max_split_size_kb = 1024 * 64;
    MockRequestScheduler* mocksch = new MockRequestScheduler;
    mocksch->DelegateToFake();

    // fake metacache
    MetaCache* mc = cl.GetIOManager4Chunk()->GetMetaCache();
    ChunkID cid = 1;
    CopysetInfo_t cpinfo;
    mc->UpdateChunkInfoByID(2, 3, cid);
    mc->UpdateCopysetInfo(2, 3, cpinfo);

    IOManager4Chunk* ioctxmana = cl.GetIOManager4Chunk();
    ioctxmana->SetRequestScheduler(mocksch);

    uint64_t len = 4 * 1024 + 2 * max_split_size_kb;
    char* buf = new char[len];
    memset(buf, 0, len);
    LOG(ERROR) << "start read snap chunk";
    ASSERT_EQ(132 * 1024, ioctxmana->ReadSnapChunk(2, 3, cid, 0, 0, len, buf));
    LOG(ERROR) << "read snap chunk success!";
    ASSERT_EQ(buf[0], 'a');
    ASSERT_EQ(buf[max_split_size_kb - 1], 'a');
    ASSERT_EQ(buf[max_split_size_kb], 'b');
    ASSERT_EQ(buf[2 * max_split_size_kb - 1], 'b');
    ASSERT_EQ(buf[2 * max_split_size_kb], 'c');
    ASSERT_EQ(buf[len - 1], 'c');

    cl.UnInit();
}

TEST(SnapInstance, DeleteChunkSnapshotTest) {
    ClientConfigOption_t opt;
    opt.metaserveropt.metaaddrvec.push_back("127.0.0.1:8000");
    opt.ioopt.reqschopt.request_scheduler_queue_capacity = 4096;
    opt.ioopt.reqschopt.request_scheduler_threadpool_size = 2;
    opt.ioopt.iosenderopt.failreqopt.client_chunk_op_max_retry = 3;
    opt.ioopt.iosenderopt.failreqopt.client_chunk_op_retry_interval_us = 500;
    opt.ioopt.metacacheopt.get_leader_retry = 3;
    opt.ioopt.iosenderopt.enable_applied_index_read = 1;
    opt.ioopt.iosplitopt.io_split_max_size_kb = 64;
    opt.ioopt.reqschopt.iosenderopt = opt.ioopt.iosenderopt;
    opt.loglevel = 0;

    SnapshotClient cl;
    ASSERT_TRUE(!cl.Init(opt));
    auto max_split_size_kb = 1024 * 64;
    MockRequestScheduler* mocksch = new MockRequestScheduler;
    mocksch->DelegateToFake();


    // fake metacache
    MetaCache* mc = cl.GetIOManager4Chunk()->GetMetaCache();
    ChunkID cid = 1;
    CopysetInfo_t cpinfo;
    mc->UpdateChunkInfoByID(2, 3, cid);
    mc->UpdateCopysetInfo(2, 3, cpinfo);

    IOManager4Chunk* ioctxmana = cl.GetIOManager4Chunk();
    ioctxmana->SetRequestScheduler(mocksch);

    ASSERT_EQ(0, ioctxmana->DeleteSnapChunk(2, 3, cid, 0));

    cl.UnInit();
}

TEST(SnapInstance, GetChunkInfoTest) {
    ClientConfigOption_t opt;
    opt.metaserveropt.metaaddrvec.push_back("127.0.0.1:8000");
    opt.ioopt.reqschopt.request_scheduler_queue_capacity = 4096;
    opt.ioopt.reqschopt.request_scheduler_threadpool_size = 2;
    opt.ioopt.iosenderopt.failreqopt.client_chunk_op_max_retry = 3;
    opt.ioopt.iosenderopt.failreqopt.client_chunk_op_retry_interval_us = 500;
    opt.ioopt.metacacheopt.get_leader_retry = 3;
    opt.ioopt.iosenderopt.enable_applied_index_read = 1;
    opt.ioopt.iosplitopt.io_split_max_size_kb = 64;
    opt.ioopt.reqschopt.iosenderopt = opt.ioopt.iosenderopt;
    opt.loglevel = 0;

    SnapshotClient cl;
    ASSERT_TRUE(!cl.Init(opt));
    MockRequestScheduler* mocksch = new MockRequestScheduler;
    mocksch->DelegateToFake();


    // fake metacache
    MetaCache* mc = cl.GetIOManager4Chunk()->GetMetaCache();
    ChunkID cid = 1;
    CopysetInfo_t cpinfo;
    mc->UpdateChunkInfoByID(2, 3, cid);
    mc->UpdateCopysetInfo(2, 3, cpinfo);

    IOManager4Chunk* ioctxmana = cl.GetIOManager4Chunk();
    ioctxmana->SetRequestScheduler(mocksch);
    ChunkInfoDetail cinfode;
    ASSERT_EQ(0, ioctxmana->GetChunkInfo(2, 3, cid, &cinfode));

    ASSERT_EQ(2222, cinfode.chunkSn[0]);
    cl.UnInit();
}
