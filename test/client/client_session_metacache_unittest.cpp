/*
 * Project: curve
 * File Created: Tuesday, 9th October 2018 5:16:52 pm
 * Author: tongguangxun
 * Copyright (c) 2018 NetEase
 */

#include <gtest/gtest.h>
#include <gflags/gflags.h>
#include <glog/logging.h>
#include <brpc/server.h>

#include <string>
#include <thread>   //NOLINT
#include <chrono>   //NOLINT
#include <vector>

#include "src/client/client_common.h"
#include "src/client/session.h"
#include "test/client/fake/mockMDS.h"
#include "src/client/metacache.h"
#include "test/client/fake/mock_schedule.h"
#include "include/client/libcurve.h"

DECLARE_string(metaserver_addr);
DECLARE_uint32(chunk_size);

using curve::client::LogicPoolID;
using curve::client::CopysetID;
using curve::client::Configuration;
using curve::client::ChunkServerID;
using curve::client::PeerId;
using curve::client::Session;
using curve::mds::CurveFSService;
using curve::mds::topology::TopologyService;
using ::curve::mds::topology::GetChunkServerListInCopySetsResponse;
class SessionTest : public ::testing::Test {
 public:
    void SetUp() {
        session_ = new Session();
        session_->Initialize();
    }

    void TearDown() {
        session_->UnInitialize();
        delete session_;
    }

    Session*    session_;
    static int i;
};

TEST_F(SessionTest, Createfile) {
    std::string filename = "./test.file";
    size_t len = 4 * 1024 * 1024;

    brpc::Server server;

    FakeCurveFSService curvefsservice;

    if (server.AddService(&curvefsservice,
                          brpc::SERVER_DOESNT_OWN_SERVICE) != 0) {
        LOG(FATAL) << "Fail to add service";
    }

    brpc::ServerOptions options;
    options.idle_timeout_sec = -1;

    ASSERT_EQ(server.Start(FLAGS_metaserver_addr.c_str(), &options), 0);

    ::curve::mds::CreateFileResponse response;
    response.set_statuscode(::curve::mds::StatusCode::kFileExists);

    FakeReturn* fakeret
     = new FakeReturn(nullptr, static_cast<void*>(&response));

    curvefsservice.SetFakeReturn(fakeret);

    CreateFileErrorType ret = session_->CreateFile(filename.c_str(), len);

    ASSERT_EQ(ret, CreateFileErrorType::FILE_ALREADY_EXISTS);

    ASSERT_NE(CreateFileErrorType::FILE_CREATE_FAILED,
         CreateFile(filename.c_str(), len));

    ASSERT_EQ(0, server.Stop(0));
    ASSERT_EQ(0, server.Join());
    delete fakeret;
}

TEST_F(SessionTest, GetFileInfo) {
    std::string filename = "./test.file";

    brpc::Server server;

    FakeCurveFSService curvefsservice;

    if (server.AddService(&curvefsservice,
            brpc::SERVER_DOESNT_OWN_SERVICE) != 0) {
        LOG(FATAL) << "Fail to add service";
    }

    // Start the server.
    brpc::ServerOptions options;
    options.idle_timeout_sec = -1;
    if (server.Start(FLAGS_metaserver_addr.c_str(),
        &options) != 0) {
        LOG(ERROR) << "Fail to start Server";
    }

    curve::mds::FileInfo * info = new curve::mds::FileInfo;
    ::curve::mds::GetFileInfoResponse response;
    response.set_allocated_fileinfo(info);
    response.mutable_fileinfo()->
        set_filename(filename);
    response.mutable_fileinfo()->
        set_id(1);
    response.mutable_fileinfo()->
        set_parentid(0);
    response.mutable_fileinfo()->
        set_filetype(curve::mds::FileType::INODE_PAGEFILE);
    response.mutable_fileinfo()->
        set_chunksize(4 * 1024 * 1024);
    response.mutable_fileinfo()->
        set_length(4 * 1024 * 1024 * 1024);
    response.mutable_fileinfo()->
        set_ctime(12345678);
    // response.mutable_fileinfo()->
    //     set_snapshotid(0);
    response.mutable_fileinfo()->
        set_segmentsize(1 * 1024 * 1024 * 1024);
    response.set_statuscode(::curve::mds::StatusCode::kOK);

    FakeReturn* fakeret = new FakeReturn(nullptr,
            static_cast<void*>(&response));
    curvefsservice.SetFakeReturn(fakeret);

    FInfo_t* finfo = new FInfo_t;
    session_->GetFileInfo(filename, finfo);

    ASSERT_EQ(finfo->filename, filename);
    ASSERT_EQ(finfo->id, 1);
    ASSERT_EQ(finfo->parentid, 0);
    ASSERT_EQ(static_cast<curve::mds::FileType>(finfo->filetype),
        curve::mds::FileType::INODE_PAGEFILE);
    ASSERT_EQ(finfo->chunksize, 4 * 1024 * 1024);
    ASSERT_EQ(finfo->length, 4 * 1024 * 1024 * 1024);
    ASSERT_EQ(finfo->ctime, 12345678);
    ASSERT_EQ(finfo->segmentsize, 1 * 1024 * 1024 * 1024);
    // ASSERT_EQ(finfo->snapshotid, 0);

    FInfo f = GetInfo(filename.c_str());
    ASSERT_EQ(f.filename, filename);
    ASSERT_EQ(f.id, 1);
    ASSERT_EQ(f.parentid, 0);
    ASSERT_EQ(static_cast<curve::mds::FileType>(f.filetype),
        curve::mds::FileType::INODE_PAGEFILE);
    ASSERT_EQ(f.chunksize, 4 * 1024 * 1024);
    ASSERT_EQ(f.length, 4 * 1024 * 1024 * 1024);
    ASSERT_EQ(f.ctime, 12345678);
    ASSERT_EQ(f.segmentsize, 1 * 1024 * 1024 * 1024);
    // ASSERT_EQ(f.snapshotid, 0);

    ASSERT_EQ(0, server.Stop(0));
    ASSERT_EQ(0, server.Join());
    delete fakeret;
    delete finfo;
}

TEST_F(SessionTest, GetOrAllocateSegment) {
    std::string filename = "./test.file";

    brpc::Server server;

    FakeCurveFSService curvefsservice;
    FakeTopologyService topologyservice;

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
    if (server.Start(FLAGS_metaserver_addr.c_str(), &options) != 0) {
        LOG(ERROR) << "Fail to start Server";
    }

    curve::mds::GetOrAllocateSegmentResponse response;
    curve::mds::PageFileSegment* pfs = new curve::mds::PageFileSegment;
    response.set_statuscode(::curve::mds::StatusCode::kOK);
    response.set_allocated_pagefilesegment(pfs);
    response.mutable_pagefilesegment()->set_logicalpoolid(1234);
    response.mutable_pagefilesegment()->set_segmentsize(1 * 1024 * 1024 * 1024);
    response.mutable_pagefilesegment()->set_chunksize(4 * 1024 * 1024);
    response.mutable_pagefilesegment()->set_startoffset(0);
    for (int i = 0; i < 256; i ++) {
        auto chunk = response.mutable_pagefilesegment()->add_chunks();
        chunk->set_copysetid(i);
        chunk->set_chunkid(i);
    }
    FakeReturn* fakeret = new FakeReturn(nullptr,
                static_cast<void*>(&response));
    curvefsservice.SetFakeReturn(fakeret);

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

    curve::client::MetaCache* mc = session_->GetMetaCache();
    for (int i = 0; i < 256; i++) {
        curve::client::Chunkinfo_t chunkinfo;
        ASSERT_EQ(0, mc->GetChunkInfo(i, &chunkinfo));
        ASSERT_EQ(chunkinfo.logicpoolid_, 1234);
        ASSERT_EQ(chunkinfo.copysetid_, i);
        ASSERT_EQ(chunkinfo.chunkid_, i);
    }

    curve::client::EndPoint ep;
    butil::str2endpoint("127.0.0.1", 5000, &ep);
    curve::client::PeerId pd(ep);
    for (int i = 0; i < 256; i++) {
        auto serverlist = mc->GetServerList(1234, i);
        ASSERT_TRUE(serverlist.IsValid());
        int chunkserverid = i * 3 + 1;

        uint32_t csid;
        curve::client::EndPoint temp;
        mc->GetLeader(1234, i, &csid, &temp);
        ASSERT_TRUE(csid == chunkserverid);
        ASSERT_TRUE(temp == ep);
        for (auto iter : serverlist.csinfos_) {
            ASSERT_EQ(iter.chunkserverid_, chunkserverid++);
            ASSERT_EQ(pd, iter.peerid_);
        }
    }

    GetChunkServerListInCopySetsResponse response_2;
    response_2.set_statuscode(-1);
    FakeReturn* faktopologyeret_2 = new FakeReturn(nullptr,
        static_cast<void*>(&response_2));
    topologyservice.SetFakeReturn(faktopologyeret_2);

    uint32_t csid;
    curve::client::EndPoint temp;
    ASSERT_EQ(-1, mc->GetLeader(2345, 0, &csid, &temp));

    curve::client::EndPoint ep1;
    butil::str2endpoint("127.0.0.1", 7777, &ep1);
    ChunkServerID cid1 = 4;
    mc->UpdateLeader(1234, 0, &cid1, ep1);

    curve::client::EndPoint toep;
    ChunkServerID cid;
    mc->GetLeader(1234, 0, &cid, &toep, false);

    ASSERT_EQ(ep1, toep);
    ASSERT_EQ(0, mc->UpdateLeader(1234, 0, &cid1, ep1));

    ASSERT_EQ(0, mc->GetAppliedIndex(1111, 0));
    // Boundary test metacache.
    // we fake the disk size = 1G.
    // and the chunksize = 4M.
    // so if visit the chunk index > 255
    // will return failed.
    curve::client::Chunkinfo_t chunkinfo;
    ASSERT_EQ(-1, mc->GetChunkInfo(256, &chunkinfo));

    class DerivedMetacache : public curve::client::MetaCache {
     public:
        DerivedMetacache():curve::client::MetaCache(nullptr) {
        }
        int testPrivateGetLeader(const LogicPoolID &logicPoolId,
                        const CopysetID &copysetId,
                        const Configuration &conf,
                        PeerId *leaderId) {
            return this->GetLeader(logicPoolId, copysetId, conf, leaderId);
        }
    };

    DerivedMetacache* testmc = new DerivedMetacache();
    curve::client::LogicPoolID lpid = 1234;
    curve::client::CopysetID copyid = 0;
    curve::client::PeerId pid;
    curve::client::Configuration conf;
    ASSERT_EQ(-1, testmc->testPrivateGetLeader(lpid, copyid, conf, &pid));

    curve::client::EndPoint ep11, ep22, ep33;
    butil::str2endpoint("127.0.0.1", 7777, &ep11);
    curve::client::PeerId pd11(ep11);
    butil::str2endpoint("127.0.0.1", 7777, &ep22);
    curve::client::PeerId pd22(ep22);
    butil::str2endpoint("127.0.0.1", 7777, &ep33);
    curve::client::PeerId pd33(ep33);

    curve::client::Configuration cfg;
    cfg.add_peer(pd11);
    cfg.add_peer(pd22);
    cfg.add_peer(pd33);
    ASSERT_EQ(-1, testmc->testPrivateGetLeader(lpid, copyid, conf, &pid));


    ASSERT_EQ(0, server.Stop(0));
    ASSERT_EQ(0, server.Join());
    delete fakeret;
    delete faktopologyeret;
}

TEST_F(SessionTest, GetServerList) {
    brpc::Server server;

    FakeTopologyService topologyservice;

    if (server.AddService(&topologyservice,
                          brpc::SERVER_DOESNT_OWN_SERVICE) != 0) {
        LOG(FATAL) << "Fail to add service";
    }
    brpc::ServerOptions options;
    options.idle_timeout_sec = -1;
    if (server.Start(FLAGS_metaserver_addr.c_str(), &options) != 0) {
        LOG(ERROR) << "Fail to start Server";
    }

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
    ASSERT_NE(-1, session_->GetServerList(1234, cpidvec));

    curve::client::MetaCache* mc = session_->GetMetaCache();
    curve::client::EndPoint ep;
    butil::str2endpoint("127.0.0.1", 5000, &ep);
    curve::client::PeerId pd(ep);
    for (int i = 0; i < 256; i++) {
        auto serverlist = mc->GetServerList(1234, i);
        ASSERT_TRUE(serverlist.IsValid());
        int chunkserverid = i * 3 + 1;
        for (auto iter : serverlist.csinfos_) {
            ASSERT_EQ(iter.chunkserverid_, chunkserverid++);
            ASSERT_EQ(pd, iter.peerid_);
        }
    }
    ASSERT_EQ(0, server.Stop(0));
    ASSERT_EQ(0, server.Join());
    delete faktopologyeret;
}

TEST_F(SessionTest, GetLeaderTest) {
    brpc::Server chunkserver1;
    brpc::Server chunkserver2;
    brpc::Server chunkserver3;

    FakeCliService cliservice;

    if (chunkserver1.AddService(&cliservice,
                          brpc::SERVER_DOESNT_OWN_SERVICE) != 0) {
        LOG(FATAL) << "Fail to add service";
    }

    if (chunkserver2.AddService(&cliservice,
                          brpc::SERVER_DOESNT_OWN_SERVICE) != 0) {
        LOG(FATAL) << "Fail to add service";
    }

    if (chunkserver3.AddService(&cliservice,
                          brpc::SERVER_DOESNT_OWN_SERVICE) != 0) {
        LOG(FATAL) << "Fail to add service";
    }

    brpc::ServerOptions options;
    options.idle_timeout_sec = -1;
    if (chunkserver1.Start("127.0.0.1:7000", &options) != 0) {
        LOG(ERROR) << "Fail to start Server";
    }

    if (chunkserver2.Start("127.0.0.1:7001", &options) != 0) {
        LOG(ERROR) << "Fail to start Server";
    }

    if (chunkserver3.Start("127.0.0.1:7002", &options) != 0) {
        LOG(ERROR) << "Fail to start Server";
    }

    curve::client::EndPoint ep1, ep2, ep3;
    butil::str2endpoint("127.0.0.1", 7000, &ep1);
    curve::client::PeerId pd1(ep1);
    butil::str2endpoint("127.0.0.1", 7001, &ep2);
    curve::client::PeerId pd2(ep2);
    butil::str2endpoint("127.0.0.1", 7002, &ep3);
    curve::client::PeerId pd3(ep3);

    curve::client::Configuration cfg;
    cfg.add_peer(pd1);
    cfg.add_peer(pd2);
    cfg.add_peer(pd3);

    curve::client::MetaCache* mc = session_->GetMetaCache();
    curve::client::CopysetInfo_t cslist;

    curve::client::CopysetPeerInfo_t peerinfo_1;
    peerinfo_1.chunkserverid_ = 1;
    peerinfo_1.peerid_ = pd1;
    cslist.AddCopysetPeerInfo(peerinfo_1);

    curve::client::CopysetPeerInfo_t peerinfo_2;
    peerinfo_2.chunkserverid_ = 2;
    peerinfo_2.peerid_ = pd2;
    cslist.AddCopysetPeerInfo(peerinfo_2);

    curve::client::CopysetPeerInfo_t peerinfo_3;
    peerinfo_3.chunkserverid_ = 3;
    peerinfo_3.peerid_ = pd3;
    cslist.AddCopysetPeerInfo(peerinfo_3);

    mc->UpdateCopysetInfo(1234, 1234, cslist);

    curve::chunkserver::GetLeaderResponse response;
    response.set_leader_id(pd1.to_string());

    FakeReturn fakeret(nullptr, static_cast<void*>(&response));
    cliservice.SetFakeReturn(&fakeret);

    curve::client::ChunkServerID ckid;
    curve::client::EndPoint leaderep;
    mc->GetLeader(1234, 1234, &ckid, &leaderep, true);

    ASSERT_EQ(ckid, 1);
    ASSERT_EQ(ep1, leaderep);

    chunkserver1.Stop(0);
    chunkserver1.Join();
    chunkserver2.Stop(0);
    chunkserver2.Join();
    chunkserver3.Stop(0);
    chunkserver3.Join();
}


TEST_F(SessionTest, GetFileInfoException) {
    std::string filename = "./test.file";

    brpc::Server server;

    FakeCurveFSService curvefsservice;

    if (server.AddService(&curvefsservice,
            brpc::SERVER_DOESNT_OWN_SERVICE) != 0) {
        LOG(FATAL) << "Fail to add service";
    }

    // Start the server.
    brpc::ServerOptions options;
    options.idle_timeout_sec = -1;
    if (server.Start(FLAGS_metaserver_addr.c_str(),
        &options) != 0) {
        LOG(ERROR) << "Fail to start Server";
    }
    FakeReturn* fakeret = nullptr;
    FInfo_t* finfo = nullptr;
    {
        // curve::mds::FileInfo info;
        ::curve::mds::GetFileInfoResponse response;
        response.set_statuscode(::curve::mds::StatusCode::kFileExists);
        // response.set_allocated_fileinfo(info);

        fakeret = new FakeReturn(nullptr,
                static_cast<void*>(&response));
        curvefsservice.SetFakeReturn(fakeret);

        finfo = new FInfo_t;
        ASSERT_EQ(::curve::mds::StatusCode::kFileExists,
                session_->GetFileInfo(filename, finfo));
    }

    {
        curve::mds::FileInfo * info = new curve::mds::FileInfo;
        ::curve::mds::GetFileInfoResponse response;
        response.set_statuscode(::curve::mds::StatusCode::kFileExists);
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
        curvefsservice.SetFakeReturn(fakeret);

        finfo = new FInfo_t;
        ASSERT_EQ(::curve::mds::StatusCode::kFileExists,
                session_->GetFileInfo(filename, finfo));
    }

    ASSERT_EQ(0, server.Stop(0));
    ASSERT_EQ(0, server.Join());
    delete fakeret;
    delete finfo;
}


TEST_F(SessionTest, GetOrAllocateSegmentException) {
    std::string filename = "./test.file";

    brpc::Server server;

    FakeCurveFSService curvefsservice;
    FakeTopologyService topologyservice;

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
    if (server.Start(FLAGS_metaserver_addr.c_str(), &options) != 0) {
        LOG(ERROR) << "Fail to start Server";
    }

    curve::mds::GetOrAllocateSegmentResponse response;
    curve::mds::PageFileSegment* pfs = new curve::mds::PageFileSegment;
    response.set_statuscode(::curve::mds::StatusCode::kOK);
    response.set_allocated_pagefilesegment(pfs);
    FakeReturn* fakeret = new FakeReturn(nullptr,
                static_cast<void*>(&response));
    curvefsservice.SetFakeReturn(fakeret);

    ::curve::mds::topology::GetChunkServerListInCopySetsResponse response_1;
    response_1.set_statuscode(0);
    FakeReturn* faktopologyeret = new FakeReturn(nullptr,
        static_cast<void*>(&response_1));
    topologyservice.SetFakeReturn(faktopologyeret);

    ASSERT_EQ(-1, session_->GetOrAllocateSegment(0));
    ASSERT_EQ(0, server.Stop(0));
    ASSERT_EQ(0, server.Join());
    delete fakeret;
    delete faktopologyeret;
}
