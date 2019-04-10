/*
 * Project: curve
 * File Created: Tuesday, 9th October 2018 2:19:51 pm
 * Author: tongguangxun
 * Copyright (c) 2018 NetEase
 */
#ifndef TEST_CLIENT_FAKE_MOCKMDS_H_
#define TEST_CLIENT_FAKE_MOCKMDS_H_

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
    FakeCurveFSService() {
        retrytimes_ = 0;
        fakeret_ = nullptr;
        fakeopenfile_ = nullptr;
    }

    void CreateFile(::google::protobuf::RpcController* controller,
                       const ::curve::mds::CreateFileRequest* request,
                       ::curve::mds::CreateFileResponse* response,
                       ::google::protobuf::Closure* done) {
        brpc::ClosureGuard done_guard(done);
        if (fakeret_->controller_ != nullptr
         && fakeret_->controller_->Failed()) {
            controller->SetFailed("failed");
        }

        retrytimes_++;

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

        retrytimes_++;

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

        retrytimes_++;

        auto resp = static_cast<::curve::mds::GetOrAllocateSegmentResponse*>(
                    fakeret_->response_);
        response->CopyFrom(*resp);
    }

    void OpenFile(::google::protobuf::RpcController* controller,
                const ::curve::mds::OpenFileRequest* request,
                ::curve::mds::OpenFileResponse* response,
                ::google::protobuf::Closure* done) {
        brpc::ClosureGuard done_guard(done);
        if (fakeopenfile_->controller_ != nullptr
             && fakeopenfile_->controller_->Failed()) {
            controller->SetFailed("failed");
        }

        retrytimes_++;

        auto resp = static_cast<::curve::mds::OpenFileResponse*>(
                    fakeopenfile_->response_);
        response->CopyFrom(*resp);
    }

    void CreateCloneFile(::google::protobuf::RpcController* controller,
                        const ::curve::mds::CreateCloneFileRequest* request,
                        ::curve::mds::CreateCloneFileResponse* response,
                        ::google::protobuf::Closure* done) {
        brpc::ClosureGuard done_guard(done);
        if (fakeCreateCloneFile_->controller_ != nullptr
             && fakeCreateCloneFile_->controller_->Failed()) {
            controller->SetFailed("failed");
        }

        retrytimes_++;

        auto resp = static_cast<::curve::mds::CreateCloneFileResponse*>(
                    fakeCreateCloneFile_->response_);
        response->CopyFrom(*resp);
    }

    void SetCloneFileStatus(::google::protobuf::RpcController* controller,
                        const ::curve::mds::SetCloneFileStatusRequest* request,
                        ::curve::mds::SetCloneFileStatusResponse* response,
                        ::google::protobuf::Closure* done) {
        brpc::ClosureGuard done_guard(done);
        if (fakeSetCloneFileStatus_->controller_ != nullptr
             && fakeSetCloneFileStatus_->controller_->Failed()) {
            controller->SetFailed("failed");
        }

        retrytimes_++;

        auto resp = static_cast<::curve::mds::SetCloneFileStatusResponse*>(
                    fakeSetCloneFileStatus_->response_);
        response->CopyFrom(*resp);
    }

    void SetFakeReturn(FakeReturn* fakeret) {
        fakeret_ = fakeret;
    }

    void SetOpenFile(FakeReturn* fakeret) {
        fakeopenfile_ = fakeret;
    }

    void SetCreateCloneFile(FakeReturn* fakeret) {
        fakeCreateCloneFile_ = fakeret;
    }

    void SetCloneFileStatus(FakeReturn* fakeret) {
        fakeSetCloneFileStatus_ = fakeret;
    }

    void CleanRetryTimes() {
        retrytimes_ = 0;
    }

    uint64_t GetRetryTimes() {
        return retrytimes_;
    }

    uint64_t retrytimes_;
    FakeReturn* fakeret_;
    FakeReturn* fakeopenfile_;
    FakeReturn* fakeCreateCloneFile_;
    FakeReturn* fakeSetCloneFileStatus_;
};

class FakeTopologyService : public curve::mds::topology::TopologyService {
 public:
    FakeTopologyService() {
        retrytimes_ = 0;
        fakeret_ = nullptr;
    }

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

        retrytimes_++;

        auto resp = static_cast<GetChunkServerListInCopySetsResponse*>(
            fakeret_->response_);
        response->CopyFrom(*resp);
    }

    void SetFakeReturn(FakeReturn* fakeret) {
        fakeret_ = fakeret;
    }

    void CleanRetryTimes() {
        retrytimes_ = 0;
    }

    uint64_t GetRetryTimes() {
        return retrytimes_;
    }

    uint64_t retrytimes_;
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

#endif  // TEST_CLIENT_FAKE_MOCKMDS_H_
