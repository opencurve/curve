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

#ifndef TEST_CLIENT_FAKE_FAKEMDS_H_
#define TEST_CLIENT_FAKE_FAKEMDS_H_

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
