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
DECLARE_uint32(segment_size);

DEFINE_uint32(test_disk_size,
                1 * 1024 * 1024 * 1024,
                "test size");
DEFINE_uint32(copyset_num,
                200,
                "copyset num in one chunkserver");
DEFINE_string(chunkserver_ip,
                "127.0.0.1",
                "chunkserver address");
DEFINE_uint32(chunkserver_start_port,
                6000,
                "chunkserver port start, if have 3 chunkserver in one machine");

using curve::chunkserver::COPYSET_OP_STATUS;
using ::curve::mds::topology::GetChunkServerListInCopySetsResponse;

FakeMDS::FakeMDS(std::string filename, uint64_t size) {
    filename_ = filename;
    size_ = size;
}

bool FakeMDS::Initialize() {
    server_ = new brpc::Server();
    chunkserverrpcserver_  = new brpc::Server();
    return true;
}

void FakeMDS::UnInitialize() {
    server_->Stop(0);
    server_->Join();
    chunkserverrpcserver_->Stop(0);
    chunkserverrpcserver_->Join();
    delete server_;
    delete chunkserverrpcserver_;
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
    getfileinforesponse->mutable_fileinfo()->
        set_snapshotid(0);
    getfileinforesponse->mutable_fileinfo()->
        set_segmentsize(FLAGS_segment_size);
    getfileinforesponse->set_statuscode(::curve::mds::StatusCode::kFileExists);
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
        mutable_pagefilesegment()->set_logicalpoolid(1234);
    getallocateresponse->
        mutable_pagefilesegment()->set_segmentsize(FLAGS_segment_size);
    getallocateresponse->
        mutable_pagefilesegment()->set_chunksize(FLAGS_chunk_size);
    getallocateresponse->
        mutable_pagefilesegment()->set_startoffset(0);

    uint32_t chunknum = FLAGS_test_disk_size/FLAGS_chunk_size;
    for (int i = 0; i < chunknum; i ++) {
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

    for (int i = 0; i < FLAGS_copyset_num; i ++) {
        CopysetCreatStruct copysetstruct;
        copysetstruct.logicpoolid = 1234;
        copysetstruct.copysetid = i;
        curve::client::EndPoint ep;
        butil::str2endpoint(FLAGS_chunkserver_ip.c_str(),
                     FLAGS_chunkserver_start_port, &ep);
        curve::client::PeerId leaderpeer(ep, 0);
        copysetstruct.leaderid = leaderpeer;

        auto csinfo = getserverlistresponse->add_csinfo();
        csinfo->set_copysetid(i);

        for (int j = 0; j < 3; j++) {
            auto cslocs = csinfo->add_cslocs();
            cslocs->set_chunkserverid(j);
            cslocs->set_hostip(FLAGS_chunkserver_ip);
            cslocs->set_port(FLAGS_chunkserver_start_port + j);

            curve::client::EndPoint ep;
            butil::str2endpoint(FLAGS_chunkserver_ip.c_str(),
                     FLAGS_chunkserver_start_port + j, &ep);
            curve::client::PeerId temppeer(ep, 0);
            copysetstruct.conf.push_back(temppeer);
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
    for (auto iter : copysetnodeVec_) {
        brpc::Channel channel;
        if (channel.Init(iter.leaderid.addr, NULL) != 0) {
            LOG(FATAL) << "Fail to init channel to " << iter.leaderid.addr;
        }

        curve::chunkserver::CopysetService_Stub stub(&channel);
        brpc::Controller cntl;
        cntl.set_timeout_ms(10);

        curve::chunkserver::CopysetRequest request;
        curve::chunkserver::CopysetResponse response;
        request.set_logicpoolid(iter.logicpoolid);
        request.set_copysetid(iter.copysetid);
        for (auto it : iter.conf) {
            request.add_peerid(it.to_string());
        }
        stub.CreateCopysetNode(&cntl, &request, &response, nullptr);
        if (cntl.Failed()) {
            LOG(ERROR) << cntl.ErrorText();
            return false;
        }
    }
    return true;
}

void FakeMDS::FakeCreateCopysetReturn() {
    if (chunkserverrpcserver_->AddService(&fakechunkserverservice_,
                          brpc::SERVER_DOESNT_OWN_SERVICE) != 0) {
        LOG(FATAL) << "Fail to add service";
    }

    if (chunkserverrpcserver_->AddService(&fakecreatecopysetservice_,
                          brpc::SERVER_DOESNT_OWN_SERVICE) != 0) {
        LOG(FATAL) << "Fail to add service";
    }
    brpc::ServerOptions options;
    options.idle_timeout_sec = -1;
    curve::client::EndPoint ep;
    butil::str2endpoint(FLAGS_chunkserver_ip.c_str(),
            FLAGS_chunkserver_start_port, &ep);
    if (chunkserverrpcserver_->Start(ep, &options) != 0) {
        LOG(ERROR) << "Fail to start Server";
    }

    curve::chunkserver::CopysetResponse* cpresp
    = new curve::chunkserver::CopysetResponse();
    cpresp->set_status(COPYSET_OP_STATUS::COPYSET_OP_STATUS_SUCCESS);
    FakeReturn* fakereturn = new FakeReturn(nullptr, cpresp);
    fakecreatecopysetservice_.SetFakeReturn(fakereturn);
}
