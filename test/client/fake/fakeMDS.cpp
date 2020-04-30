/*
 * Project: curve
 * File Created: Saturday, 13th October 2018 10:50:22 am
 * Author: tongguangxun
 * Copyright (c) 2018 NetEase
 */

#include <gflags/gflags.h>
#include <butil/endpoint.h>
#include <brpc/channel.h>

#include <string>

#include "test/client/fake/fakeMDS.h"
#include "src/client/client_common.h"

extern std::string mdsMetaServerAddr;
extern uint32_t chunk_size;
extern uint32_t segment_size;

using curve::client::SegmentInfo;
using curve::client::ChunkInfoDetail;

DEFINE_bool(start_builtin_service, false, "start builtin services");
DEFINE_bool(fake_chunkserver, true, "create fake chunkserver");
DEFINE_uint64(test_disk_size, 10 * 1024 * 1024 * 1024ul, "test size");
DEFINE_uint32(copyset_num, 32, "copyset num in one chunkserver");
DEFINE_uint32(logic_pool_id, 10000, "logic pool id");
DEFINE_uint64(seq_num, 1, "seqnum");
// raft::Configuration format to reuse raft parse functions
DEFINE_string(chunkserver_list,
             "127.0.0.1:9106:0,127.0.0.1:9107:0,127.0.0.1:9108:0",
            "chunkserver address");

using ::curve::mds::RegistClientResponse;
using curve::chunkserver::COPYSET_OP_STATUS;
using ::curve::mds::topology::GetChunkServerListInCopySetsResponse;

FakeMDS::FakeMDS(std::string filename) {
    filename_ = filename;
}

bool FakeMDS::Initialize() {
    server_ = new brpc::Server();

    braft::Configuration conf;
    if (conf.parse_from(FLAGS_chunkserver_list) != 0) {
        LOG(ERROR) << "Fail to parse chunkserver list";
        return -1;
    }
    conf.list_peers(&peers_);
    for (unsigned i = 0; i < peers_.size(); i++) {
        server_addrs_.push_back(peers_[i].addr);
        chunkservers_.push_back(new brpc::Server());
        chunkServices_.push_back(new FakeChunkService());
        copysetServices_.push_back(new FakeCreateCopysetService());
        raftStateServices_.push_back(new FakeRaftStateService());
    }
    return true;
}

void FakeMDS::UnInitialize() {
    server_->Stop(0);
    server_->Join();
    for (unsigned i = 0; i < peers_.size(); i++) {
        chunkservers_[i]->Stop(0);
        chunkservers_[i]->Join();
    }
    for (unsigned i = 0; i < peers_.size(); i++) {
        delete chunkservers_[i];
        delete chunkServices_[i];
        delete copysetServices_[i];
    }
    delete server_;
}

void FakeMDS::ExposeMetric() {
    for (const auto& item : metrics_) {
        item.second->expose(item.first);
    }
}

bool FakeMDS::StartService() {
    if (server_->AddService(&fakecurvefsservice_,
                brpc::SERVER_DOESNT_OWN_SERVICE) != 0) {
        LOG(FATAL) << "Fail to add service";
    }
    if (server_->AddService(&faketopologyservice_,
                brpc::SERVER_DOESNT_OWN_SERVICE) != 0) {
        LOG(FATAL) << "Fail to add service";
    }
    if (server_->AddService(&fakeHeartbeatService_,
                brpc::SERVER_DOESNT_OWN_SERVICE) != 0) {
        LOG(FATAL) << "Fail to add heartbeat service";
    }
    if (server_->AddService(&fakeScheduleService_,
                brpc::SERVER_DOESNT_OWN_SERVICE) != 0) {
        LOG(FATAL) << "Fail to add heartbeat service";
    }
    brpc::ServerOptions options;
    options.idle_timeout_sec = -1;

    if (server_->Start(mdsMetaServerAddr.c_str(), &options) != 0) {
        LOG(ERROR) << "Fail to start Server";
    }

    /**
     * set regist fake return
     */
    ::curve::mds::RegistClientResponse* registResp = new ::curve::mds::RegistClientResponse();      // NOLINT
    registResp->set_statuscode(::curve::mds::StatusCode::kOK);
    FakeReturn* fakeregist = new FakeReturn(nullptr, static_cast<void*>(registResp));      // NOLINT
    fakecurvefsservice_.SetRegistRet(fakeregist);

    /**
     * set CreateFile fake return
     */
    ::curve::mds::CreateFileResponse* createfileresponse = new ::curve::mds::CreateFileResponse();      // NOLINT
    createfileresponse->set_statuscode(::curve::mds::StatusCode::kOK);
    FakeReturn* fakcreatefileeret = new FakeReturn(nullptr, static_cast<void*>(createfileresponse));      // NOLINT
    fakecurvefsservice_.SetCreateFileFakeReturn(fakcreatefileeret);

    /**
     * set GetFileInfo fake return
     */
    curve::mds::FileInfo * info = new curve::mds::FileInfo;
    ::curve::mds::GetFileInfoResponse* getfileinforesponse = new ::curve::mds::GetFileInfoResponse();      // NOLINT
    info->set_filename(filename_.substr(1, filename_.size() -1));
    info->set_id(1);
    info->set_parentid(0);
    info->set_filetype(curve::mds::FileType::INODE_PAGEFILE);
    info->set_chunksize(chunk_size);
    info->set_length(FLAGS_test_disk_size);
    info->set_ctime(12345678);
    info->set_segmentsize(segment_size);
    info->set_owner("userinfo");
    getfileinforesponse->set_allocated_fileinfo(info);

    getfileinforesponse->set_statuscode(::curve::mds::StatusCode::kOK);
    FakeReturn* fakeGetFileInforet = new FakeReturn(nullptr, static_cast<void*>(getfileinforesponse));      // NOLINT
    fakecurvefsservice_.SetGetFileInfoFakeReturn(fakeGetFileInforet);

    /**
     * set GetOrAllocateSegment fake return
     */
    curve::mds::GetOrAllocateSegmentResponse* getallocateresponse = new curve::mds::GetOrAllocateSegmentResponse();      // NOLINT
    curve::mds::PageFileSegment* pfs = new curve::mds::PageFileSegment;

    getallocateresponse->set_statuscode(::curve::mds::StatusCode::kOK);
    getallocateresponse->set_allocated_pagefilesegment(pfs);
    getallocateresponse->mutable_pagefilesegment()->set_logicalpoolid(10000);
    getallocateresponse->mutable_pagefilesegment()->set_segmentsize(segment_size);      // NOLINT
    getallocateresponse->mutable_pagefilesegment()->set_chunksize(chunk_size);
    getallocateresponse->mutable_pagefilesegment()->set_startoffset(0);

    uint32_t chunknum = FLAGS_test_disk_size/chunk_size;
    for (unsigned i = 1; i <= chunknum; i ++) {
        auto chunk = getallocateresponse->mutable_pagefilesegment()->add_chunks();      // NOLINT
        chunk->set_copysetid(i%FLAGS_copyset_num);
        chunk->set_chunkid(i);
    }
    FakeReturn* fakeret = new FakeReturn(nullptr, static_cast<void*>(getallocateresponse));      // NOLINT
    fakecurvefsservice_.SetGetOrAllocateSegmentFakeReturn(fakeret);

    /**
     * set GetServerList fake FakeReturn
     */
    copysetnodeVec_.clear();
    GetChunkServerListInCopySetsResponse* getserverlistresponse = new GetChunkServerListInCopySetsResponse();      // NOLINT
    getserverlistresponse->set_statuscode(0);

    for (unsigned i = 0; i < FLAGS_copyset_num; i ++) {
        CopysetCreatStruct copysetstruct;
        copysetstruct.logicpoolid = FLAGS_logic_pool_id;
        copysetstruct.copysetid = i;
        copysetstruct.leaderid = peers_[i % peers_.size()];
        auto csinfo = getserverlistresponse->add_csinfo();
        csinfo->set_copysetid(i);

        for (unsigned j = 0; j < peers_.size(); j++) {
            auto cslocs = csinfo->add_cslocs();
            cslocs->set_chunkserverid(j);
            cslocs->set_hostip(butil::ip2str(server_addrs_[j].ip).c_str());
            cslocs->set_port(server_addrs_[j].port);
            copysetstruct.conf.push_back(peers_[j]);
        }
        copysetnodeVec_.push_back(copysetstruct);
    }
    FakeReturn* faktopologyeret = new FakeReturn(nullptr,static_cast<void*>(getserverlistresponse));      // NOLINT
    faketopologyservice_.SetFakeReturn(faktopologyeret);

    /**
     * set openfile response
     */

    ::curve::mds::OpenFileResponse* openresponse =
     new ::curve::mds::OpenFileResponse();
    ::curve::mds::ProtoSession* se = new ::curve::mds::ProtoSession;
    se->set_sessionid("1");
    se->set_createtime(12345);
    se->set_leasetime(10000000);
    se->set_sessionstatus(::curve::mds::SessionStatus::kSessionOK);

    ::curve::mds::FileInfo* fin = new ::curve::mds::FileInfo;
    fin->set_filename(filename_.substr(1, filename_.size() -1));
    fin->set_id(1);
    fin->set_parentid(0);
    fin->set_filetype(curve::mds::FileType::INODE_PAGEFILE);
    fin->set_chunksize(chunk_size);
    fin->set_length(segment_size);
    fin->set_ctime(12345678);
    fin->set_seqnum(FLAGS_seq_num);
    fin->set_segmentsize(segment_size);
    fin->set_owner("userinfo");

    openresponse->set_statuscode(::curve::mds::StatusCode::kOK);
    openresponse->set_allocated_protosession(se);
    openresponse->set_allocated_fileinfo(fin);
    FakeReturn* openfakeret = new FakeReturn(nullptr, static_cast<void*>(openresponse));      // NOLINT
    fakecurvefsservice_.SetOpenFile(openfakeret);

    /**
     * set refresh response
     */
    ::curve::mds::ReFreshSessionResponse* refreshresp = new ::curve::mds::ReFreshSessionResponse();      // NOLINT
    refreshresp->set_statuscode(::curve::mds::StatusCode::kOK);
    refreshresp->set_sessionid("1234");
    FakeReturn* refreshfakeret = new FakeReturn(nullptr, static_cast<void*>(refreshresp));      // NOLINT
    fakecurvefsservice_.SetRefreshSession(refreshfakeret, nullptr);

    /**
     * set create snapshot response
     */
    ::curve::mds::CreateSnapShotResponse* response = new ::curve::mds::CreateSnapShotResponse();      // NOLINT
    response->set_statuscode(::curve::mds::StatusCode::kOK);
    ::curve::mds::FileInfo* finf = new ::curve::mds::FileInfo;
    finf->set_filename(filename_);
    finf->set_id(1);
    finf->set_parentid(0);
    finf->set_filetype(curve::mds::FileType::INODE_PAGEFILE);
    finf->set_chunksize(chunk_size);
    finf->set_length(segment_size);
    finf->set_ctime(12345678);
    finf->set_seqnum(FLAGS_seq_num);
    finf->set_segmentsize(segment_size);
    response->set_allocated_snapshotfileinfo(finf);
    FakeReturn* fakeret1 = new FakeReturn(nullptr, static_cast<void*>(response));      // NOLINT
    fakecurvefsservice_.SetCreateSnapShot(fakeret1);

    /**
     * set list snapshot response
     */
    ::curve::mds::ListSnapShotFileInfoResponse* listresponse = new ::curve::mds::ListSnapShotFileInfoResponse;      // NOLINT
    listresponse->add_fileinfo();
    listresponse->mutable_fileinfo(0)->set_filename(filename_);
    listresponse->mutable_fileinfo(0)->set_id(1);
    listresponse->mutable_fileinfo(0)->set_parentid(0);
    listresponse->mutable_fileinfo(0)->set_filetype(curve::mds::FileType::INODE_PAGEFILE);    // NOLINT
    listresponse->mutable_fileinfo(0)->set_chunksize(chunk_size);
    listresponse->mutable_fileinfo(0)->set_length(segment_size);
    listresponse->mutable_fileinfo(0)->set_ctime(12345678);
    listresponse->mutable_fileinfo(0)->set_seqnum(2);
    listresponse->mutable_fileinfo(0)->set_segmentsize(segment_size);

    listresponse->set_statuscode(::curve::mds::StatusCode::kOK);
    FakeReturn* listfakeret = new FakeReturn(nullptr, static_cast<void*>(listresponse));      // NOLINT
    fakecurvefsservice_.SetListSnapShot(listfakeret);

    /**
     * set get snap allocate info
     */
    FakeReturn* snapfakeret = new FakeReturn(nullptr, static_cast<void*>(getallocateresponse));      // NOLINT
    fakecurvefsservice_.SetGetSnapshotSegmentInfo(fakeret);

    /**
     * set delete snapshot response
     */
    ::curve::mds::DeleteSnapShotResponse* delresponse = new ::curve::mds::DeleteSnapShotResponse;      // NOLINT
    delresponse->set_statuscode(::curve::mds::StatusCode::kOK);
    FakeReturn* delfakeret = new FakeReturn(nullptr, static_cast<void*>(delresponse));      // NOLINT
    fakecurvefsservice_.SetDeleteSnapShot(delfakeret);

    /**
     * set fake close return
     */
    ::curve::mds::CloseFileResponse* closeresp = new ::curve::mds::CloseFileResponse;    // NOLINT
    closeresp->set_statuscode(::curve::mds::StatusCode::kOK);
    FakeReturn* closefileret
     = new FakeReturn(nullptr, static_cast<void*>(closeresp));
    fakecurvefsservice_.SetCloseFile(closefileret);

    /**
     * set fake extend file response
     */
    ::curve::mds::ExtendFileResponse* extendfileresponse =
    new ::curve::mds::ExtendFileResponse;
    extendfileresponse->set_statuscode(::curve::mds::StatusCode::kOK);

    FakeReturn* fakeExtendRet
     = new FakeReturn(nullptr, static_cast<void*>(extendfileresponse));

    fakecurvefsservice_.SetExtendFile(fakeExtendRet);

    /**
     * set list physical pool response
     */
    ListPhysicalPoolResponse* listphypoolresp = new ListPhysicalPoolResponse();
    FakeReturn* fakeListPPRet = new FakeReturn(nullptr, response);
    faketopologyservice_.fakelistpoolret_ = fakeListPPRet;

    return true;
}

bool FakeMDS::CreateCopysetNode(bool enablecli) {
    /**
     * set Create Copyset in target chunkserver
     */
    if (FLAGS_fake_chunkserver) {
        CreateFakeChunkservers(enablecli);
    }

    LOG(INFO) << "copyset num: " << copysetnodeVec_.size()
              << "member count: " << peers_.size();

    for (auto iter : copysetnodeVec_) {
        for (unsigned i = 0; i < peers_.size(); i++) {
            brpc::Channel channel;
            if (channel.Init(server_addrs_[i], NULL) != 0) {
                LOG(FATAL) << "Fail to init channel to " << server_addrs_[i];
            }

            curve::chunkserver::CopysetService_Stub stub(&channel);
            brpc::Controller cntl;
            cntl.set_timeout_ms(1000);

            curve::chunkserver::CopysetRequest request;
            curve::chunkserver::CopysetResponse response;
            request.set_logicpoolid(iter.logicpoolid);
            request.set_copysetid(iter.copysetid);
            for (auto it : iter.conf) {
                request.add_peerid(it.to_string());
            }

            stub.CreateCopysetNode(&cntl, &request, &response, nullptr);
            LOG(INFO) << "Created copyset member, "
                << "poolid: " << iter.logicpoolid << ", "
                << "copysetid: " << iter.copysetid << ", "
                << "on chunkserver: " << server_addrs_[i] << ", ";

            if (cntl.Failed()) {
                LOG(ERROR) << cntl.ErrorText() << response.status();
                // return false;
            }
        }
    }

    return true;
}

void FakeMDS::EnableNetUnstable(uint64_t waittime) {
    LOG(INFO) << "enable chunk rpc service net unstable!";
    for (auto iter : chunkServices_) {
        iter->EnableNetUnstable(waittime);
    }
}

void FakeMDS::DisableNetUnstable() {
    LOG(INFO) << "chunk rpc service net is stable now!";
    for (auto c : chunkServices_) {
        c->DisableNetUnstable();
    }
}

void FakeMDS::CreateFakeChunkservers(bool enablecli) {
    for (unsigned i = 0; i < peers_.size(); i++) {
        if (chunkservers_[i]->AddService(chunkServices_[i],
                              brpc::SERVER_DOESNT_OWN_SERVICE) != 0) {
            LOG(FATAL) << "Fail to add service";
        }
        if (chunkservers_[i]->AddService(copysetServices_[i],
                              brpc::SERVER_DOESNT_OWN_SERVICE) != 0) {
            LOG(FATAL) << "Fail to add service";
        }

        if (enablecli && chunkservers_[i]->AddService(&fakeCliService_,
                              brpc::SERVER_DOESNT_OWN_SERVICE) != 0) {
            LOG(FATAL) << "Fail to add service";
        }
        if (chunkservers_[i]->AddService(raftStateServices_[i],
                              brpc::SERVER_DOESNT_OWN_SERVICE) != 0) {
            LOG(FATAL) << "Fail to add service";
        }

        brpc::ServerOptions options;
        options.idle_timeout_sec = -1;

        if (FLAGS_start_builtin_service == false) {
            options.has_builtin_services = false;
        } else {
            options.has_builtin_services = true;
        }

        if (chunkservers_[i]->Start(server_addrs_[i], &options) != 0) {
            LOG(FATAL) << "Fail to start Server";
        }
        LOG(INFO) << "Created chunkserver: " << server_addrs_[i];

        curve::chunkserver::CopysetResponse* cpresp
        = new curve::chunkserver::CopysetResponse();
        cpresp->set_status(COPYSET_OP_STATUS::COPYSET_OP_STATUS_SUCCESS);
        FakeReturn* fakereturn = new FakeReturn(nullptr, cpresp);
        copysetServices_[i]->SetFakeReturn(fakereturn);

        ::curve::chunkserver::ChunkResponse* delresp =
            new ::curve::chunkserver::ChunkResponse();
        delresp->set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS);
        delresp->set_chunksn(1111);
        delresp->set_snapsn(1);
        FakeReturn* delfakeret = new FakeReturn(nullptr, delresp);
        chunkServices_[i]->SetDeleteChunkSnapshot(delfakeret);

        ::curve::chunkserver::ChunkResponse* readresp =
            new ::curve::chunkserver::ChunkResponse();
        readresp->set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS);
        readresp->set_appliedindex(10);
        readresp->set_chunksn(1111);
        readresp->set_snapsn(1);
        FakeReturn* readfakeret = new FakeReturn(nullptr, readresp);
        chunkServices_[i]->SetReadChunkSnapshot(readfakeret);

        ::curve::chunkserver::GetChunkInfoResponse* getchunkinfo =
            new ::curve::chunkserver::GetChunkInfoResponse();
        getchunkinfo->set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS);
        getchunkinfo->add_chunksn(1111);
        FakeReturn* getinfofakeret = new FakeReturn(nullptr, getchunkinfo);
        chunkServices_[i]->SetGetChunkInfo(getinfofakeret);
    }
}

void FakeMDS::StartCliService(PeerId leaderID) {
    fakeCliService_.SetPeerID(leaderID);
}
