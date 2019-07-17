/*
 * Project: curve
 * File Created: Saturday, 13th October 2018 10:50:15 am
 * Author: tongguangxun
 * Copyright (c) 2018 NetEase
 */

#ifndef TEST_CLIENT_FAKE_FAKEMDS_H_
#define TEST_CLIENT_FAKE_FAKEMDS_H_
#include <gtest/gtest.h>
#include <brpc/server.h>

#include <string>
#include <vector>
#include <functional>
#include <utility>
#include "src/client/client_common.h"
#include "test/client/fake/mockMDS.h"
#include "test/client/fake/fakeChunkserver.h"

#include "proto/nameserver2.pb.h"
#include "proto/topology.pb.h"
#include "proto/copyset.pb.h"
#include "src/common/timeutility.h"
#include "src/common/authenticator.h"
#include "proto/heartbeat.pb.h"

using braft::PeerId;
using curve::common::Authenticator;
using curve::chunkserver::COPYSET_OP_STATUS;
using ::curve::mds::topology::GetChunkServerListInCopySetsResponse;
using ::curve::mds::topology::GetChunkServerListInCopySetsRequest;
using ::curve::mds::topology::ChunkServerRegistRequest;
using ::curve::mds::topology::ChunkServerRegistResponse;

using HeartbeatRequest  = curve::mds::heartbeat::ChunkServerHeartbeatRequest;
using HeartbeatResponse = curve::mds::heartbeat::ChunkServerHeartbeatResponse;

class FakeMDSCurveFSService : public curve::mds::CurveFSService {
 public:
    FakeMDSCurveFSService() {
        retrytimes_ = 0;
    }

    void RegistClient(::google::protobuf::RpcController* controller,
                       const ::curve::mds::RegistClientRequest* request,
                       ::curve::mds::RegistClientResponse* response,
                       ::google::protobuf::Closure* done) {
        brpc::ClosureGuard done_guard(done);
        if (fakeRegisterret_->controller_ != nullptr &&
             fakeRegisterret_->controller_->Failed()) {
            controller->SetFailed("failed");
        }

        retrytimes_++;

        ip_ = request->ip();
        port_ = request->port();

        auto resp = static_cast<::curve::mds::RegistClientResponse*>(
                    fakeRegisterret_->response_);
        response->CopyFrom(*resp);
    }

    void CreateFile(::google::protobuf::RpcController* controller,
                       const ::curve::mds::CreateFileRequest* request,
                       ::curve::mds::CreateFileResponse* response,
                       ::google::protobuf::Closure* done) {
        brpc::ClosureGuard done_guard(done);
        if (fakeCreateFileret_->controller_ != nullptr
             && fakeCreateFileret_->controller_->Failed()) {
            controller->SetFailed("failed");
        }

        retrytimes_++;

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

        retrytimes_++;

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

        retrytimes_++;

        auto resp = static_cast<::curve::mds::GetOrAllocateSegmentResponse*>(
                    fakeGetOrAllocateSegmentret_->response_);
        response->CopyFrom(*resp);
    }

    void OpenFile(::google::protobuf::RpcController* controller,
                const ::curve::mds::OpenFileRequest* request,
                ::curve::mds::OpenFileResponse* response,
                ::google::protobuf::Closure* done) {
        brpc::ClosureGuard done_guard(done);
        if (fakeopenfile_->controller_ != nullptr &&
             fakeopenfile_->controller_->Failed()) {
            controller->SetFailed("failed");
        }

        retrytimes_++;

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

            if (resp->statuscode() == ::curve::mds::StatusCode::kOK) {
                curve::mds::FileInfo * info = new curve::mds::FileInfo;
                info->set_seqnum(seq++);
                info->set_filename("_filename_");
                info->set_id(1);
                info->set_parentid(0);
                info->set_filetype(curve::mds::FileType::INODE_PAGEFILE);
                info->set_chunksize(4 * 1024 * 1024);
                info->set_length(4 * 1024 * 1024 * 1024ul);
                info->set_ctime(12345678);

                response->set_statuscode(::curve::mds::StatusCode::kOK);
                response->set_sessionid("1234");
                response->set_allocated_fileinfo(info);
                LOG(INFO) << "refresh session request!";
            } else {
                response->CopyFrom(*resp);
            }
        }

        retrytimes_++;

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

        if (request->has_signature()) {
            CheckAuth(request->signature(),
                      request->filename(),
                      request->owner(),
                      request->date());
        }

        retrytimes_++;

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

        if (request->has_signature()) {
            CheckAuth(request->signature(),
                      request->filename(),
                      request->owner(),
                      request->date());
        }

        retrytimes_++;

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

        if (request->has_signature()) {
            CheckAuth(request->signature(),
                      request->filename(),
                      request->owner(),
                      request->date());
        }

        retrytimes_++;

        auto resp = static_cast<::curve::mds::DeleteSnapShotResponse*>(
                    fakedeletesnapshotret_->response_);
        response->CopyFrom(*resp);
    }

    void CheckSnapShotStatus(::google::protobuf::RpcController* controller,
                       const ::curve::mds::CheckSnapShotStatusRequest* request,
                       ::curve::mds::CheckSnapShotStatusResponse* response,
                       ::google::protobuf::Closure* done) {
        brpc::ClosureGuard done_guard(done);
        if (fakechecksnapshotret_->controller_ != nullptr &&
             fakechecksnapshotret_->controller_->Failed()) {
            controller->SetFailed("failed");
        }

        if (request->has_signature()) {
            CheckAuth(request->signature(),
                      request->filename(),
                      request->owner(),
                      request->date());
        }

        auto resp = static_cast<::curve::mds::DeleteSnapShotResponse*>(
                    fakechecksnapshotret_->response_);
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

        if (request->has_signature()) {
            CheckAuth(request->signature(),
                      request->filename(),
                      request->owner(),
                      request->date());
        }

        retrytimes_++;

        auto resp = static_cast<::curve::mds::GetOrAllocateSegmentResponse*>(
                    fakegetsnapsegmentinforet_->response_);
        response->CopyFrom(*resp);
    }

    void DeleteChunkSnapshotOrCorrectSn(
                    ::google::protobuf::RpcController* controller,
                    const ::curve::chunkserver::ChunkRequest* request,
                    ::curve::chunkserver::ChunkResponse* response,
                    ::google::protobuf::Closure* done) {
        brpc::ClosureGuard done_guard(done);
        if (fakedeletesnapchunkret_->controller_ != nullptr &&
             fakedeletesnapchunkret_->controller_->Failed()) {
            controller->SetFailed("failed");
        }

        retrytimes_++;

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

        retrytimes_++;

        auto resp = static_cast<::curve::mds::CloseFileResponse*>(
                    fakeclosefile_->response_);
        response->CopyFrom(*resp);
    }

    void RenameFile(::google::protobuf::RpcController* controller,
                    const ::curve::mds::RenameFileRequest* request,
                    ::curve::mds::RenameFileResponse* response,
                    ::google::protobuf::Closure* done) {
        brpc::ClosureGuard done_guard(done);
        if (fakerenamefile_->controller_ != nullptr &&
             fakerenamefile_->controller_->Failed()) {
            controller->SetFailed("failed");
        }

        retrytimes_++;

        auto resp = static_cast<::curve::mds::CloseFileResponse*>(
                    fakerenamefile_->response_);
        response->CopyFrom(*resp);
    }

    void DeleteFile(::google::protobuf::RpcController* controller,
                    const ::curve::mds::DeleteFileRequest* request,
                    ::curve::mds::DeleteFileResponse* response,
                    ::google::protobuf::Closure* done) {
        brpc::ClosureGuard done_guard(done);
        if (fakedeletefile_->controller_ != nullptr &&
             fakedeletefile_->controller_->Failed()) {
            controller->SetFailed("failed");
        }

        if (request->has_fileid()) {
            ASSERT_GT(request->fileid(), 0);
        }

        retrytimes_++;

        auto resp = static_cast<::curve::mds::CloseFileResponse*>(
                    fakedeletefile_->response_);

        if (request->forcedelete()) {
            LOG(INFO) << "force delete file!";
            fiu_do_on("test/client/fake/fakeMDS/forceDeleteFile",
            resp->set_statuscode(curve::mds::StatusCode::kNotSupported));
        }

        response->CopyFrom(*resp);
    }

    void ExtendFile(::google::protobuf::RpcController* controller,
                    const ::curve::mds::ExtendFileRequest* request,
                    ::curve::mds::ExtendFileResponse* response,
                    ::google::protobuf::Closure* done) {
        brpc::ClosureGuard done_guard(done);
        if (fakeextendfile_->controller_ != nullptr &&
             fakeextendfile_->controller_->Failed()) {
            controller->SetFailed("failed");
        }

        retrytimes_++;

        auto resp = static_cast<::curve::mds::ExtendFileResponse*>(
                    fakeextendfile_->response_);
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

    void ChangeOwner(::google::protobuf::RpcController* controller,
                    const ::curve::mds::ChangeOwnerRequest* request,
                    ::curve::mds::ChangeOwnerResponse* response,
                    ::google::protobuf::Closure* done) {
        brpc::ClosureGuard done_guard(done);
        if (fakeChangeOwner_->controller_ != nullptr &&
             fakeChangeOwner_->controller_->Failed()) {
            controller->SetFailed("failed");
        }

        retrytimes_++;

        auto resp = static_cast<::curve::mds::ChangeOwnerResponse*>(
                    fakeChangeOwner_->response_);

        response->CopyFrom(*resp);
    }

    void ListDir(::google::protobuf::RpcController* controller,
                    const ::curve::mds::ListDirRequest* request,
                    ::curve::mds::ListDirResponse* response,
                    ::google::protobuf::Closure* done) {
        brpc::ClosureGuard done_guard(done);
        if (fakeListDir_->controller_ != nullptr &&
             fakeListDir_->controller_->Failed()) {
            controller->SetFailed("failed");
        }

        retrytimes_++;

        auto resp = static_cast<::curve::mds::ListDirResponse*>(
                    fakeListDir_->response_);

        response->CopyFrom(*resp);
    }

    void SetListDir(FakeReturn* fakeret) {
        fakeListDir_ = fakeret;
    }

    void SetCreateCloneFile(FakeReturn* fakeret) {
        fakeCreateCloneFile_ = fakeret;
    }

    void SetExtendFile(FakeReturn* fakeret) {
        fakeextendfile_ = fakeret;
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

    void SetCheckSnap(FakeReturn* fakeret) {
        fakechecksnapshotret_ = fakeret;
    }

    void SetRenameFile(FakeReturn* fakeret) {
        fakerenamefile_ = fakeret;
    }

    void SetDeleteFile(FakeReturn* fakeret) {
        fakedeletefile_ = fakeret;
    }

    void SetRegistRet(FakeReturn* fakeret) {
        fakeRegisterret_ = fakeret;
    }

    void SetCloneFileStatus(FakeReturn* fakeret) {
        fakeSetCloneFileStatus_ = fakeret;
    }

    void SetChangeOwner(FakeReturn* fakeret) {
        fakeChangeOwner_ = fakeret;
    }

    void CleanRetryTimes() {
        retrytimes_ = 0;
    }

    uint64_t GetRetryTimes() {
        return retrytimes_;
    }

    std::string GetIP() {
        return ip_;
    }

    uint16_t GetPort() {
        return port_;
    }

    void CheckAuth(const std::string& signature,
                   const std::string& filename,
                   const std::string& owner,
                   uint64_t date) {
        std::string str2sig = Authenticator::GetString2Signature(date, owner);
        std::string sigtest = Authenticator::CalcString2Signature(str2sig,
                                                                "123");
        ASSERT_STREQ(sigtest.c_str(), signature.c_str());
    }

    uint64_t retrytimes_;

    std::string ip_;
    uint16_t port_;

    FakeReturn* fakeListDir_;
    FakeReturn* fakeSetCloneFileStatus_;
    FakeReturn* fakeCreateCloneFile_;
    FakeReturn* fakeCreateFileret_;
    FakeReturn* fakeGetFileInforet_;
    FakeReturn* fakeGetOrAllocateSegmentret_;
    FakeReturn* fakeopenfile_;
    FakeReturn* fakeclosefile_;
    FakeReturn* fakerenamefile_;
    FakeReturn* fakeRefreshSession_;
    FakeReturn* fakedeletefile_;
    FakeReturn* fakeextendfile_;
    FakeReturn* fakeRegisterret_;
    FakeReturn* fakeChangeOwner_;

    FakeReturn* fakechecksnapshotret_;
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

typedef void (*HeartbeatCallback) (
    ::google::protobuf::RpcController* controller,
    const HeartbeatRequest* request,
    HeartbeatResponse* response,
    ::google::protobuf::Closure* done);

class FakeMDSHeartbeatService : public curve::mds::heartbeat::HeartbeatService {
 public:
    FakeMDSHeartbeatService() : cb_(nullptr) {}

    void ChunkServerHeartbeat(::google::protobuf::RpcController* controller,
                              const HeartbeatRequest* request,
                              HeartbeatResponse* response,
                              ::google::protobuf::Closure* done) {
        brpc::ClosureGuard done_guard(done);
        std::unique_lock<std::mutex> lock(cbMtx_);

        if (cb_) {
            cb_(controller, request, response, done_guard.release());
        }
    }

    void SetCallback(HeartbeatCallback cb) {
        std::unique_lock<std::mutex> lock(cbMtx_);
        cb_ = cb;
    }

 private:
    HeartbeatCallback cb_;

    mutable std::mutex          cbMtx_;
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

    void GetCopysetStatus(::google::protobuf::RpcController *controller,
                   const ::curve::chunkserver::CopysetStatusRequest *request,
                   ::curve::chunkserver::CopysetStatusResponse *response,
                   google::protobuf::Closure *done) {
        brpc::ClosureGuard doneGuard(done);

        response->set_state("state");
        curve::common::Peer *peer = new curve::common::Peer();
        response->set_allocated_peer(peer);
        peer->set_address("127.0.0.1:1111");
        curve::common::Peer *leader = new curve::common::Peer();
        response->set_allocated_leader(leader);
        leader->set_address("127.0.0.1:1111");
        response->set_readonly(1);
        response->set_term(1);
        response->set_committedindex(1);
        response->set_knownappliedindex(applyindex_);
        response->set_pendingindex(1);
        response->set_pendingqueuesize(1);
        response->set_applyingindex(0);
        response->set_firstindex(1);
        response->set_lastindex(1);
        response->set_diskindex(1);
        response->set_epoch(1);
        response->set_hash(std::to_string(hash_));
        response->set_status(COPYSET_OP_STATUS::COPYSET_OP_STATUS_SUCCESS);
    }

    void SetHash(uint64_t hash) {
        hash_ = hash;
    }

    void SetApplyindex(uint64_t index) {
        applyindex_ = index;
    }

    void SetFakeReturn(FakeReturn* fakeret) {
        fakeret_ = fakeret;
    }

 public:
    uint64_t applyindex_;
    uint64_t hash_;
    FakeReturn* fakeret_;
};

class FakeMDS {
 public:
    explicit FakeMDS(std::string filename);
    bool Initialize();
    void UnInitialize();

    bool StartService();
    bool CreateCopysetNode(bool enablecli = false);
    void EnableNetUnstable(uint64_t waittime);
    void CreateFakeChunkservers(bool enablecli);

    void StartCliService(PeerId leaderID);

    void SetChunkServerHeartbeatCallback(HeartbeatCallback cb) {
        fakeHeartbeatService_.SetCallback(cb);
    }

    struct CopysetCreatStruct {
        curve::client::LogicPoolID logicpoolid;
        curve::client::CopysetID copysetid;
        PeerId leaderid;
        std::vector<PeerId> conf;
    };

    FakeMDSCurveFSService* GetMDSService() {
        return &fakecurvefsservice_;
    }

    std::vector<FakeCreateCopysetService *> GetCreateCopysetService() {
        return copysetServices_;
    }

    std::vector<FakeChunkService*> GetFakeChunkService() {
        return chunkServices_;
    }

    CliServiceFake* GetCliService() {
        return &fakeCliService_;
    }

    std::vector<FakeChunkService *> GetChunkservice() {
        return chunkServices_;
    }

 private:
    std::vector<CopysetCreatStruct> copysetnodeVec_;
    brpc::Server* server_;
    std::vector<brpc::Server *> chunkservers_;
    std::vector<butil::EndPoint> server_addrs_;
    std::vector<PeerId> peers_;
    std::vector<FakeChunkService *> chunkServices_;
    std::vector<FakeCreateCopysetService *> copysetServices_;
    std::string filename_;

    uint64_t size_;
    CliServiceFake        fakeCliService_;
    FakeMDSCurveFSService fakecurvefsservice_;
    FakeMDSTopologyService faketopologyservice_;
    FakeMDSHeartbeatService fakeHeartbeatService_;
};

#endif   // TEST_CLIENT_FAKE_FAKEMDS_H_
