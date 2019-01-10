/*
 * Project: curve
 * File Created: Saturday, 13th October 2018 10:50:15 am
 * Author: tongguangxun
 * Copyright (c) 2018 NetEase
 */

#include <brpc/server.h>

#include <string>
#include <vector>
#include <functional>
#include "src/client/client_common.h"
#include "test/client/fake/mockMDS.h"
#include "test/client/fake/fakeChunkserver.h"

#include "proto/nameserver2.pb.h"
#include "proto/topology.pb.h"
#include "proto/copyset.pb.h"

#ifndef TEST_CLIENT_FAKE_FAKEMDS_H_
#define TEST_CLIENT_FAKE_FAKEMDS_H_

using ::curve::mds::topology::GetChunkServerListInCopySetsResponse;
using ::curve::mds::topology::GetChunkServerListInCopySetsRequest;
using ::curve::mds::topology::ChunkServerRegistRequest;
using ::curve::mds::topology::ChunkServerRegistResponse;

class FakeMDSCurveFSService : public curve::mds::CurveFSService {
 public:
    void CreateFile(::google::protobuf::RpcController* controller,
                       const ::curve::mds::CreateFileRequest* request,
                       ::curve::mds::CreateFileResponse* response,
                       ::google::protobuf::Closure* done) {
        brpc::ClosureGuard done_guard(done);
        if (fakeCreateFileret_->controller_ != nullptr
             && fakeCreateFileret_->controller_->Failed()) {
            controller->SetFailed("failed");
        }

        auto resp = static_cast<::curve::mds::CreateFileResponse*>(
                fakeCreateFileret_->response_);
        response->CopyFrom(*resp);
    }

    void GetFileInfo(::google::protobuf::RpcController* controller,
                       const ::curve::mds::GetFileInfoRequest* request,
                       ::curve::mds::GetFileInfoResponse* response,
                       ::google::protobuf::Closure* done) {
        brpc::ClosureGuard done_guard(done);
        if (fakeGetFileInforet_->controller_ != nullptr &&
             fakeGetFileInforet_->controller_->Failed()) {
            controller->SetFailed("failed");
        }
        auto resp = static_cast<::curve::mds::GetFileInfoResponse*>(
                    fakeGetFileInforet_->response_);
        response->CopyFrom(*resp);
    }

    void GetOrAllocateSegment(::google::protobuf::RpcController* controller,
                       const ::curve::mds::GetOrAllocateSegmentRequest* request,
                       ::curve::mds::GetOrAllocateSegmentResponse* response,
                       ::google::protobuf::Closure* done) {
        brpc::ClosureGuard done_guard(done);
        if (fakeGetOrAllocateSegmentret_->controller_ != nullptr &&
             fakeGetOrAllocateSegmentret_->controller_->Failed()) {
            controller->SetFailed("failed");
        }

        auto resp = static_cast<::curve::mds::GetOrAllocateSegmentResponse*>(
                    fakeGetOrAllocateSegmentret_->response_);
        response->CopyFrom(*resp);
    }

    void OpenFile(::google::protobuf::RpcController* controller,
                const ::curve::mds::OpenFileRequest* request,
                ::curve::mds::OpenFileResponse* response,
                ::google::protobuf::Closure* done) {
        brpc::ClosureGuard done_guard(done);

        auto resp = static_cast<::curve::mds::OpenFileResponse*>(
                    fakeopenfile_->response_);
        response->CopyFrom(*resp);
    }

    void RefreshSession(::google::protobuf::RpcController* controller,
                        const curve::mds::ReFreshSessionRequest* request,
                        curve::mds::ReFreshSessionResponse* response,
                       ::google::protobuf::Closure* done) {
        {
            brpc::ClosureGuard done_guard(done);
            if (fakeRefreshSession_->controller_ != nullptr &&
                fakeRefreshSession_->controller_->Failed()) {
                controller->SetFailed("failed");
            }

            static int seq = 1;

            auto resp = static_cast<::curve::mds::ReFreshSessionResponse*>(
                        fakeRefreshSession_->response_);
            response->CopyFrom(*resp);

            if (response->statuscode() == ::curve::mds::StatusCode::kOK) {
                curve::mds::FileInfo * info = new curve::mds::FileInfo;
                response->set_statuscode(::curve::mds::StatusCode::kOK);
                response->set_sessionid("1234");
                response->set_allocated_fileinfo(info);
                response->mutable_fileinfo()->set_seqnum(seq++);
                response->mutable_fileinfo()->set_filename("filename");
                response->mutable_fileinfo()->set_id(1);
                response->mutable_fileinfo()->set_parentid(0);
                response->mutable_fileinfo()->set_filetype(curve::mds::FileType::INODE_PAGEFILE);     // NOLINT
                response->mutable_fileinfo()->set_chunksize(4 * 1024 * 1024);
                response->mutable_fileinfo()->set_length(4 * 1024 * 1024 * 1024ul);     // NOLINT
                response->mutable_fileinfo()->set_ctime(12345678);
                LOG(INFO) << "refresh session request!";
            }
        }
        if (refreshtask_)
            refreshtask_();
    }

    void CreateSnapShot(::google::protobuf::RpcController* controller,
                       const ::curve::mds::CreateSnapShotRequest* request,
                       ::curve::mds::CreateSnapShotResponse* response,
                       ::google::protobuf::Closure* done) {
        brpc::ClosureGuard done_guard(done);
        if (fakecreatesnapshotret_->controller_ != nullptr &&
             fakecreatesnapshotret_->controller_->Failed()) {
            controller->SetFailed("failed");
        }

        auto resp = static_cast<::curve::mds::CreateSnapShotResponse*>(
                    fakecreatesnapshotret_->response_);
        response->CopyFrom(*resp);
    }

    void ListSnapShot(::google::protobuf::RpcController* controller,
                       const ::curve::mds::ListSnapShotFileInfoRequest* request,
                       ::curve::mds::ListSnapShotFileInfoResponse* response,
                       ::google::protobuf::Closure* done) {
        brpc::ClosureGuard done_guard(done);
        if (fakelistsnapshotret_->controller_ != nullptr &&
             fakelistsnapshotret_->controller_->Failed()) {
            controller->SetFailed("failed");
        }

        auto resp = static_cast<::curve::mds::ListSnapShotFileInfoResponse*>(
                    fakelistsnapshotret_->response_);
        response->CopyFrom(*resp);
    }

    void DeleteSnapShot(::google::protobuf::RpcController* controller,
                       const ::curve::mds::DeleteSnapShotRequest* request,
                       ::curve::mds::DeleteSnapShotResponse* response,
                       ::google::protobuf::Closure* done) {
        brpc::ClosureGuard done_guard(done);
        if (fakedeletesnapshotret_->controller_ != nullptr &&
             fakedeletesnapshotret_->controller_->Failed()) {
            controller->SetFailed("failed");
        }

        auto resp = static_cast<::curve::mds::DeleteSnapShotResponse*>(
                    fakedeletesnapshotret_->response_);
        response->CopyFrom(*resp);
    }

    void GetSnapShotFileSegment(::google::protobuf::RpcController* controller,
                       const ::curve::mds::GetOrAllocateSegmentRequest* request,
                       ::curve::mds::GetOrAllocateSegmentResponse* response,
                       ::google::protobuf::Closure* done) {
        brpc::ClosureGuard done_guard(done);
        if (fakegetsnapsegmentinforet_->controller_ != nullptr &&
             fakegetsnapsegmentinforet_->controller_->Failed()) {
            controller->SetFailed("failed");
        }

        auto resp = static_cast<::curve::mds::GetOrAllocateSegmentResponse*>(
                    fakegetsnapsegmentinforet_->response_);
        response->CopyFrom(*resp);
    }

    void DeleteChunkSnapshot(::google::protobuf::RpcController* controller,
                    const ::curve::chunkserver::ChunkRequest* request,
                    ::curve::chunkserver::ChunkResponse* response,
                    ::google::protobuf::Closure* done) {
        brpc::ClosureGuard done_guard(done);
        if (fakedeletesnapchunkret_->controller_ != nullptr &&
             fakedeletesnapchunkret_->controller_->Failed()) {
            controller->SetFailed("failed");
        }

        auto resp = static_cast<::curve::chunkserver::ChunkResponse*>(
                    fakedeletesnapchunkret_->response_);
        response->CopyFrom(*resp);
    }

    void ReadChunkSnapshot(::google::protobuf::RpcController* controller,
                    const ::curve::chunkserver::ChunkRequest* request,
                    ::curve::chunkserver::ChunkResponse* response,
                    ::google::protobuf::Closure* done) {
        brpc::ClosureGuard done_guard(done);
        if (fakereadchunksnapret_->controller_ != nullptr &&
             fakereadchunksnapret_->controller_->Failed()) {
            controller->SetFailed("failed");
        }

        auto resp = static_cast<::curve::chunkserver::ChunkResponse*>(
                    fakereadchunksnapret_->response_);
        response->CopyFrom(*resp);
    }

    void CloseFile(::google::protobuf::RpcController* controller,
                    const ::curve::mds::CloseFileRequest* request,
                    ::curve::mds::CloseFileResponse* response,
                    ::google::protobuf::Closure* done) {
        brpc::ClosureGuard done_guard(done);
        if (fakeclosefile_->controller_ != nullptr &&
             fakeclosefile_->controller_->Failed()) {
            controller->SetFailed("failed");
        }

        auto resp = static_cast<::curve::mds::CloseFileResponse*>(
                    fakeclosefile_->response_);
        response->CopyFrom(*resp);
    }

    void SetCreateFileFakeReturn(FakeReturn* fakeret) {
        fakeCreateFileret_ = fakeret;
    }

    void SetGetFileInfoFakeReturn(FakeReturn* fakeret) {
        fakeGetFileInforet_ = fakeret;
    }

    void SetGetOrAllocateSegmentFakeReturn(FakeReturn* fakeret) {
        fakeGetOrAllocateSegmentret_ = fakeret;
    }

    void SetOpenFile(FakeReturn* fakeret) {
        fakeopenfile_ = fakeret;
    }

    void SetRefreshSession(FakeReturn* fakeret, std::function<void(void)> t) {
        fakeRefreshSession_ = fakeret;
        refreshtask_ = std::move(t);
    }

    void SetCreateSnapShot(FakeReturn* fakeret) {
        fakecreatesnapshotret_ = fakeret;
    }

    void SetDeleteSnapShot(FakeReturn* fakeret) {
        fakedeletesnapshotret_ = fakeret;
    }

    void SetListSnapShot(FakeReturn* fakeret) {
        fakelistsnapshotret_ = fakeret;
    }

    void SetGetSnapshotSegmentInfo(FakeReturn* fakeret) {
        fakegetsnapsegmentinforet_ = fakeret;
    }

    void SetReadChunkSnapshot(FakeReturn* fakeret) {
        fakereadchunksnapret_ = fakeret;
    }

    void SetDeleteChunkSnapshot(FakeReturn* fakeret) {
        fakedeletesnapchunkret_ = fakeret;
    }

    void SetCloseFile(FakeReturn* fakeret) {
        fakeclosefile_ = fakeret;
    }

    FakeReturn* fakeCreateFileret_;
    FakeReturn* fakeGetFileInforet_;
    FakeReturn* fakeGetOrAllocateSegmentret_;
    FakeReturn* fakeopenfile_;
    FakeReturn* fakeclosefile_;
    FakeReturn* fakeRefreshSession_;

    FakeReturn* fakecreatesnapshotret_;
    FakeReturn* fakelistsnapshotret_;
    FakeReturn* fakedeletesnapshotret_;
    FakeReturn* fakereadchunksnapret_;
    FakeReturn* fakedeletesnapchunkret_;
    FakeReturn* fakegetsnapsegmentinforet_;
    std::function<void(void)> refreshtask_;
};

class FakeMDSTopologyService : public curve::mds::topology::TopologyService {
 public:
    void GetChunkServerListInCopySets(
                       ::google::protobuf::RpcController* controller,
                       const GetChunkServerListInCopySetsRequest* request,
                       GetChunkServerListInCopySetsResponse* response,
                       ::google::protobuf::Closure* done) {
        brpc::ClosureGuard done_guard(done);
        int statcode = 0;
        if (response->has_statuscode()) {
            statcode = response->statuscode();
        }
        if (statcode == -1 ||
            (fakeret_->controller_ != nullptr
             && fakeret_->controller_->Failed())) {
            controller->SetFailed("failed");
        }

        auto resp = static_cast<GetChunkServerListInCopySetsResponse*>(
            fakeret_->response_);
        response->CopyFrom(*resp);
    }

    void RegistChunkServer(
                       ::google::protobuf::RpcController* controller,
                       const ChunkServerRegistRequest* request,
                       ChunkServerRegistResponse* response,
                       ::google::protobuf::Closure* done) {
        brpc::ClosureGuard done_guard(done);

        response->set_statuscode(0);
        response->set_chunkserverid(request->port());
        response->set_token(request->hostip());
    }

    void SetFakeReturn(FakeReturn* fakeret) {
        fakeret_ = fakeret;
    }

    FakeReturn* fakeret_;
};

class FakeCreateCopysetService : public curve::chunkserver::CopysetService {
 public:
    void CreateCopysetNode(
                        ::google::protobuf::RpcController* controller,
                       const ::curve::chunkserver::CopysetRequest* request,
                       ::curve::chunkserver::CopysetResponse* response,
                       ::google::protobuf::Closure* done) {
        brpc::ClosureGuard done_guard(done);
        if (fakeret_->controller_ != nullptr
         && fakeret_->controller_->Failed()) {
            controller->SetFailed("failed");
        }

        auto resp = static_cast<::curve::chunkserver::CopysetResponse*>(
            fakeret_->response_);
        response->CopyFrom(*resp);
    }

    void SetFakeReturn(FakeReturn* fakeret) {
        fakeret_ = fakeret;
    }

    FakeReturn* fakeret_;
};

class FakeMDS {
 public:
    explicit FakeMDS(std::string filename);
    bool Initialize();
    void UnInitialize();

    bool StartService();
    bool CreateCopysetNode();
    void EnableNetUnstable(uint64_t waittime);
    void CreateFakeChunkservers();

    struct CopysetCreatStruct {
        curve::client::LogicPoolID logicpoolid;
        curve::client::CopysetID copysetid;
        curve::client::PeerId leaderid;
        std::vector<curve::client::PeerId> conf;
    };

 private:
    std::vector<CopysetCreatStruct> copysetnodeVec_;
    brpc::Server* server_;
    std::vector<brpc::Server *> chunkservers_;
    std::vector<butil::EndPoint> server_addrs_;
    std::vector<braft::PeerId> peers_;
    std::vector<FakeChunkService *> chunkServices_;
    std::vector<FakeCreateCopysetService *> copysetServices_;
    std::string filename_;

    uint64_t size_;
    FakeMDSCurveFSService fakecurvefsservice_;
    FakeMDSTopologyService faketopologyservice_;
};

#endif   // TEST_CLIENT_FAKE_FAKEMDS_H_
