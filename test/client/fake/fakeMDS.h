/*
 * Project: curve
 * File Created: Saturday, 13th October 2018 10:50:15 am
 * Author: tongguangxun
 * Copyright (c) 2018 NetEase
 */

#include <brpc/server.h>

#include <string>
#include <vector>
#include "src/client/client_common.h"
#include "test/client/fake/mockMDS.h"
#include "test/client/fake/fakeChunkserver.h"

#include "proto/nameserver2.pb.h"
#include "proto/topology.pb.h"
#include "proto/copyset.pb.h"

using ::curve::mds::topology::GetChunkServerListInCopySetsResponse;
using ::curve::mds::topology::GetChunkServerListInCopySetsRequest;


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

    void SetCreateFileFakeReturn(FakeReturn* fakeret) {
        fakeCreateFileret_ = fakeret;
    }

    void SetGetFileInfoFakeReturn(FakeReturn* fakeret) {
        fakeGetFileInforet_ = fakeret;
    }

    void SetGetOrAllocateSegmentFakeReturn(FakeReturn* fakeret) {
        fakeGetOrAllocateSegmentret_ = fakeret;
    }

    FakeReturn* fakeCreateFileret_;
    FakeReturn* fakeGetFileInforet_;
    FakeReturn* fakeGetOrAllocateSegmentret_;
};

class FakeMDSTopologyService : public curve::mds::topology::TopologyService {
 public:
    void GetChunkServerListInCopySets(
                       ::google::protobuf::RpcController* controller,
                       const GetChunkServerListInCopySetsRequest* request,
                       GetChunkServerListInCopySetsResponse* response,
                       ::google::protobuf::Closure* done) {
        brpc::ClosureGuard done_guard(done);
        if (fakeret_->controller_ != nullptr
             && fakeret_->controller_->Failed()) {
            controller->SetFailed("failed");
        }

        auto resp = static_cast<GetChunkServerListInCopySetsResponse*>(
            fakeret_->response_);
        response->CopyFrom(*resp);
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
    FakeMDS(std::string filename, uint64_t size);
    bool Initialize();
    void UnInitialize();

    bool StartService();
    bool CreateCopysetNode();

    void FakeCreateCopysetReturn();

    struct CopysetCreatStruct {
        curve::client::LogicPoolID logicpoolid;
        curve::client::CopysetID copysetid;
        curve::client::PeerId leaderid;
        std::vector<curve::client::PeerId> conf;
    };

 private:
    std::vector<CopysetCreatStruct> copysetnodeVec_;
    brpc::Server* server_;
    brpc::Server* chunkserverrpcserver_;
    std::string filename_;
    uint64_t size_;
    FakeChunkService    fakechunkserverservice_;
    FakeMDSCurveFSService fakecurvefsservice_;
    FakeCreateCopysetService fakecreatecopysetservice_;
    FakeMDSTopologyService faketopologyservice_;
};
