/*
 * Project: curve
 * File Created: Tuesday, 9th October 2018 2:19:51 pm
 * Author: tongguangxun
 * Copyright (c) 2018 NetEase
 */
#ifndef TEST_CURVE_MOCK_MDS_H
#define TEST_CURVE_MOCK_MDS_H

#include <glog/logging.h>

#include "proto/nameserver2.pb.h"
#include "proto/topology.pb.h"
#include "proto/cli.pb.h"

using ::curve::mds::topology::GetChunkServerListInCopySetsResponse;
using ::curve::mds::topology::GetChunkServerListInCopySetsRequest;

class FakeReturn {
 public:
    FakeReturn(::google::protobuf::RpcController* controller,
              void* response) {
        response_ = response;
        controller_ = controller;
    }

    void* response_;
    ::google::protobuf::RpcController* controller_;
};

class FakeCurveFSService : public curve::mds::CurveFSService {
 public:
    void CreateFile(::google::protobuf::RpcController* controller,
                       const ::curve::mds::CreateFileRequest* request,
                       ::curve::mds::CreateFileResponse* response,
                       ::google::protobuf::Closure* done) {
        brpc::ClosureGuard done_guard(done);
        if (fakeret_->controller_ != nullptr
         && fakeret_->controller_->Failed()) {
            controller->SetFailed("failed");
        }

        auto resp = static_cast<::curve::mds::CreateFileResponse*>(
            fakeret_->response_);
        response->CopyFrom(*resp);
    }

    void GetFileInfo(::google::protobuf::RpcController* controller,
                       const ::curve::mds::GetFileInfoRequest* request,
                       ::curve::mds::GetFileInfoResponse* response,
                       ::google::protobuf::Closure* done) {
        brpc::ClosureGuard done_guard(done);
        if (fakeret_->controller_ != nullptr
             && fakeret_->controller_->Failed()) {
            controller->SetFailed("failed");
        }
        auto resp = static_cast<::curve::mds::GetFileInfoResponse*>(
                    fakeret_->response_);
        response->CopyFrom(*resp);
    }

    void GetOrAllocateSegment(
                       ::google::protobuf::RpcController* controller,
                       const ::curve::mds::GetOrAllocateSegmentRequest* request,
                       ::curve::mds::GetOrAllocateSegmentResponse* response,
                       ::google::protobuf::Closure* done) {
        brpc::ClosureGuard done_guard(done);
        if (fakeret_->controller_ != nullptr
             && fakeret_->controller_->Failed()) {
            controller->SetFailed("failed");
        }

        auto resp = static_cast<::curve::mds::GetOrAllocateSegmentResponse*>(
                    fakeret_->response_);
        response->CopyFrom(*resp);
    }

    void SetFakeReturn(FakeReturn* fakeret) {
        fakeret_ = fakeret;
    }
    FakeReturn* fakeret_;
};

class FakeTopologyService : public curve::mds::topology::TopologyService {
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

class FakeCliService : public curve::chunkserver::CliService {
 public:
    void get_leader(
                    ::google::protobuf::RpcController* controller,
                    const curve::chunkserver::GetLeaderRequest* request,
                    curve::chunkserver::GetLeaderResponse* response,
                    ::google::protobuf::Closure* done) {
        brpc::ClosureGuard done_guard(done);
        if (fakeret_->controller_ != nullptr
         && fakeret_->controller_->Failed()) {
            controller->SetFailed("failed");
        }

        auto resp = static_cast<curve::chunkserver::GetLeaderResponse*>(
            fakeret_->response_);
        response->CopyFrom(*resp);
    }

    void SetFakeReturn(FakeReturn* fakeret) {
        fakeret_ = fakeret;
    }

    FakeReturn* fakeret_;
};

#endif
