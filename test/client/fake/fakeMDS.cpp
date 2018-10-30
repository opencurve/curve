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

DECLARE_string(metaserver_addr);
DECLARE_uint32(chunk_size);
DECLARE_uint64(segment_size);

DEFINE_bool(fake_chunkserver, true, "create fake chunkserver");

DEFINE_uint64(test_disk_size,
                10 * 1024 * 1024 * 1024ul,
                "test size");
DEFINE_uint32(copyset_num,
                32,
                "copyset num in one chunkserver");
// raft::Configuration format to reuse raft parse functions
DEFINE_string(chunkserver_list,
                "127.0.0.1:8200:0,127.0.0.1:8201:0,127.0.0.1:8202:0",
                "chunkserver address");

using curve::chunkserver::COPYSET_OP_STATUS;
using ::curve::mds::topology::GetChunkServerListInCopySetsResponse;

FakeMDS::FakeMDS(std::string filename) {
    filename_ = filename;
    // size_ = size;
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
    }
    return true;
}

void FakeMDS::UnInitialize() {
    server_->Stop(0);
    server_->Join();
    for (unsigned i = 0; i < peers_.size(); i++) {
        chunkservers_[i]->Stop(0);
    }
    for (unsigned i = 0; i < peers_.size(); i++) {
        chunkservers_[i]->Join();
        delete chunkservers_[i];
        delete chunkServices_[i];
        delete copysetServices_[i];
    }
    delete server_;
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
    brpc::ServerOptions options;
    options.idle_timeout_sec = -1;

    if (server_->Start(FLAGS_metaserver_addr.c_str(), &options) != 0) {
        LOG(ERROR) << "Fail to start Server";
    }

    /**************** CreateFile fake return ***********************/
    ::curve::mds::CreateFileResponse* createfileresponse
    = new ::curve::mds::CreateFileResponse();

    createfileresponse->set_statuscode(::curve::mds::StatusCode::kOK);

    FakeReturn* fakcreatefileeret
     = new FakeReturn(nullptr, static_cast<void*>(createfileresponse));
    fakecurvefsservice_.SetCreateFileFakeReturn(fakcreatefileeret);

    /**************  GetFileInfo fake return ***********************/
    curve::mds::FileInfo * info = new curve::mds::FileInfo;
    ::curve::mds::GetFileInfoResponse* getfileinforesponse
    = new ::curve::mds::GetFileInfoResponse();
    getfileinforesponse->set_allocated_fileinfo(info);
    getfileinforesponse->mutable_fileinfo()->
        set_filename(filename_);
    getfileinforesponse->mutable_fileinfo()->
        set_id(1);
    getfileinforesponse->mutable_fileinfo()->
        set_parentid(0);
    getfileinforesponse->mutable_fileinfo()->
        set_filetype(curve::mds::FileType::INODE_PAGEFILE);
    getfileinforesponse->mutable_fileinfo()->
        set_chunksize(FLAGS_chunk_size);
    getfileinforesponse->mutable_fileinfo()->
        set_length(FLAGS_test_disk_size);
    getfileinforesponse->mutable_fileinfo()->
        set_ctime(12345678);
    // getfileinforesponse->mutable_fileinfo()->
    //     set_snapshotid(0);
    getfileinforesponse->mutable_fileinfo()->
        set_segmentsize(FLAGS_segment_size);
    getfileinforesponse->set_statuscode(::curve::mds::StatusCode::kOK);
    FakeReturn* fakeGetFileInforet
    = new FakeReturn(nullptr, static_cast<void*>(getfileinforesponse));
    fakecurvefsservice_.SetGetFileInfoFakeReturn(fakeGetFileInforet);

    /********* GetOrAllocateSegment fake return *******************/
    curve::mds::GetOrAllocateSegmentResponse* getallocateresponse
    = new curve::mds::GetOrAllocateSegmentResponse();
    curve::mds::PageFileSegment* pfs
    = new curve::mds::PageFileSegment;

    getallocateresponse->
        set_statuscode(::curve::mds::StatusCode::kOK);
    getallocateresponse->
        set_allocated_pagefilesegment(pfs);
    getallocateresponse->
        mutable_pagefilesegment()->set_logicalpoolid(10000);
    getallocateresponse->
        mutable_pagefilesegment()->set_segmentsize(FLAGS_segment_size);
    getallocateresponse->
        mutable_pagefilesegment()->set_chunksize(FLAGS_chunk_size);
    getallocateresponse->
        mutable_pagefilesegment()->set_startoffset(0);

    uint32_t chunknum = FLAGS_test_disk_size/FLAGS_chunk_size;
    for (unsigned i = 0; i < chunknum; i ++) {
        auto chunk = getallocateresponse->
            mutable_pagefilesegment()->add_chunks();
        chunk->set_copysetid(i%FLAGS_copyset_num);
        chunk->set_chunkid(i);
    }
    FakeReturn* fakeret
    = new FakeReturn(nullptr, static_cast<void*>(getallocateresponse));
    fakecurvefsservice_.SetGetOrAllocateSegmentFakeReturn(fakeret);

    /********** GetServerList fake return ***********************/
    copysetnodeVec_.clear();
    GetChunkServerListInCopySetsResponse* getserverlistresponse =
        new GetChunkServerListInCopySetsResponse();
    getserverlistresponse->set_statuscode(0);

    for (unsigned i = 0; i < FLAGS_copyset_num; i ++) {
        CopysetCreatStruct copysetstruct;
        copysetstruct.logicpoolid = 10000;
        copysetstruct.copysetid = i;
        // initialize leader randomly
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
    FakeReturn* faktopologyeret = new FakeReturn(nullptr,
                 static_cast<void*>(getserverlistresponse));
    faketopologyservice_.SetFakeReturn(faktopologyeret);

    return true;
}

bool FakeMDS::CreateCopysetNode() {
    /********** Create Copyset in target chunkserver *************/
    if (FLAGS_fake_chunkserver) {
        CreateFakeChunkservers();
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

void FakeMDS::CreateFakeChunkservers() {
    for (unsigned i = 0; i < peers_.size(); i++) {
        if (chunkservers_[i]->AddService(chunkServices_[i],
                              brpc::SERVER_DOESNT_OWN_SERVICE) != 0) {
            LOG(FATAL) << "Fail to add service";
        }
        if (chunkservers_[i]->AddService(copysetServices_[i],
                              brpc::SERVER_DOESNT_OWN_SERVICE) != 0) {
            LOG(FATAL) << "Fail to add service";
        }

        brpc::ServerOptions options;
        options.idle_timeout_sec = -1;
        if (chunkservers_[i]->Start(server_addrs_[i], &options) != 0) {
            LOG(FATAL) << "Fail to start Server";
        }
        LOG(INFO) << "Created chunkserver: " << server_addrs_[i];

        curve::chunkserver::CopysetResponse* cpresp
        = new curve::chunkserver::CopysetResponse();
        cpresp->set_status(COPYSET_OP_STATUS::COPYSET_OP_STATUS_SUCCESS);
        FakeReturn* fakereturn = new FakeReturn(nullptr, cpresp);
        copysetServices_[i]->SetFakeReturn(fakereturn);
    }
}
