/*
 *  Copyright (c) 2020 NetEase Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

/*
 * Project: curve
 * File Created: Wednesday, 17th October 2018 10:51:42 am
 * Author: tongguangxun
 */

#ifndef TEST_CLIENT_FAKE_FAKECHUNKSERVER_H_
#define TEST_CLIENT_FAKE_FAKECHUNKSERVER_H_

#include <braft/configuration.h>
#include <brpc/controller.h>
#include <brpc/server.h>
#include <glog/logging.h>
#include <fiu-control.h>
#include <braft/builtin_service.pb.h>

#include <thread>   // NOLINT
#include <algorithm>

#include "proto/chunk.pb.h"
#include "proto/cli2.pb.h"
#include "proto/copyset.pb.h"
#include "src/client/client_common.h"
#include "test/client/fake/mockMDS.h"

using braft::PeerId;
using curve::chunkserver::ChunkService;
using curve::chunkserver::CHUNK_OP_STATUS;

class FakeChunkService : public ChunkService {
 public:
    FakeChunkService() {
        rpcFailed = false;
        retryTimes.store(0);
        waittimeMS = 10;
        wait4netunstable = false;
    }
    virtual ~FakeChunkService() {}

    void WriteChunk(::google::protobuf::RpcController *controller,
                    const ::curve::chunkserver::ChunkRequest *request,
                    ::curve::chunkserver::ChunkResponse *response,
                    google::protobuf::Closure *done) {
        brpc::ClosureGuard doneGuard(done);
        retryTimes.fetch_add(1);
        brpc::Controller *cntl = dynamic_cast<brpc::Controller *>(controller);

        if (rpcFailed) {
            cntl->SetFailed(-1, "set rpc failed!");
        }

        ::memcpy(chunk_,
                 cntl->request_attachment().to_string().c_str(),
                 request->size());
        response->set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS);
        response->set_appliedindex(2);
        if (wait4netunstable) {
            std::this_thread::sleep_for(std::chrono::milliseconds(waittimeMS));
        }
    }

    void ReadChunk(::google::protobuf::RpcController *controller,
                   const ::curve::chunkserver::ChunkRequest *request,
                   ::curve::chunkserver::ChunkResponse *response,
                   google::protobuf::Closure *done) {
        brpc::ClosureGuard doneGuard(done);
        retryTimes.fetch_add(1);
        brpc::Controller *cntl = dynamic_cast<brpc::Controller *>(controller);
        if (rpcFailed) {
            cntl->SetFailed(EHOSTDOWN, "set rpc failed!");
        }

        char buff[128 * 1024] = {0};
        ::memcpy(buff, chunk_, request->size());
        cntl->response_attachment().append(buff, request->size());
        response->set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS);
        response->set_appliedindex(2);
        if (wait4netunstable) {
            std::this_thread::sleep_for(std::chrono::milliseconds(waittimeMS));
        }
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

        brpc::Controller *cntl = dynamic_cast<brpc::Controller *>(controller);
        char buff[8192] = {1};
        ::memset(buff, 1, 8192);
        cntl->response_attachment().append(buff, request->size());
        auto resp = static_cast<::curve::chunkserver::ChunkResponse*>(
                    fakereadchunksnapret_->response_);
        response->CopyFrom(*resp);
    }

    void GetChunkInfo(::google::protobuf::RpcController *controller,
                        const ::curve::chunkserver::GetChunkInfoRequest *request,      // NOLINT
                        ::curve::chunkserver::GetChunkInfoResponse *response,
                        google::protobuf::Closure *done) {
        brpc::ClosureGuard done_guard(done);
        if (fakeGetChunkInforet_->controller_ != nullptr &&
             fakeGetChunkInforet_->controller_->Failed()) {
            controller->SetFailed("failed");
        }

        auto resp = static_cast<::curve::chunkserver::GetChunkInfoResponse*>(
                    fakeGetChunkInforet_->response_);
        response->CopyFrom(*resp);
    }

    void GetChunkHash(::google::protobuf::RpcController *controller,
                        const ::curve::chunkserver::GetChunkHashRequest *request,      // NOLINT
                        ::curve::chunkserver::GetChunkHashResponse *response,
                        google::protobuf::Closure *done) {
        brpc::ClosureGuard done_guard(done);
        if (fakeGetChunkHashRet_->controller_ != nullptr &&
             fakeGetChunkHashRet_->controller_->Failed()) {
            controller->SetFailed("failed");
        }

        auto resp = static_cast<::curve::chunkserver::GetChunkHashResponse*>(
                    fakeGetChunkHashRet_->response_);
        response->CopyFrom(*resp);
    }

    void SetReadChunkSnapshot(FakeReturn* fakeret) {
        fakereadchunksnapret_ = fakeret;
    }

    void SetDeleteChunkSnapshot(FakeReturn* fakeret) {
        fakedeletesnapchunkret_ = fakeret;
    }

    void SetGetChunkInfo(FakeReturn* fakeret) {
        fakeGetChunkInforet_ = fakeret;
    }

    void SetGetChunkHash(FakeReturn* fakeret) {
        fakeGetChunkHashRet_ = fakeret;
    }

    void SetRPCFailed() {
        rpcFailed = true;
    }

    void ReSetRPCFailed() {
        rpcFailed = false;
    }

    FakeReturn* fakedeletesnapchunkret_;
    FakeReturn* fakereadchunksnapret_;
    FakeReturn* fakeGetChunkInforet_;
    FakeReturn* fakeGetChunkHashRet_;

    void EnableNetUnstable(uint64_t waittime) {
        wait4netunstable = true;
        waittimeMS = waittime;
    }

    void DisableNetUnstable() {
        wait4netunstable = false;
        waittimeMS = 0;
    }

    void CleanRetryTimes() {
        retryTimes.store(0);
    }

    uint64_t GetRetryTimes() {
        return retryTimes.load();
    }

 private:
    // wait4netunstable用来模拟网络延时，当打开之后，每个读写rpc会停留一段时间再返回
    bool wait4netunstable;
    uint64_t waittimeMS;
    bool rpcFailed;
    std::atomic<uint64_t> retryTimes;
    char chunk_[128 * 1024];
};

class CliServiceFake : public curve::chunkserver::CliService2 {
 public:
    CliServiceFake() {
        invokeTimes = 0;
    }

    void GetLeader(::google::protobuf::RpcController* controller,
                    const curve::chunkserver::GetLeaderRequest2* request,
                    curve::chunkserver::GetLeaderResponse2* response,
                    ::google::protobuf::Closure* done) {
        brpc::ClosureGuard done_guard(done);
        curve::common::Peer *peer = new curve::common::Peer();
        peer->set_address(leaderid_.to_string());
        response->set_allocated_leader(peer);
        invokeTimes++;
    }

    void SetPeerID(PeerId peerid) {
        leaderid_ = peerid;
    }

    uint64_t GetInvokeTimes() {
        return invokeTimes;
    }

    void ReSetInvokeTimes() {
        invokeTimes = 0;
    }

 private:
    PeerId leaderid_;
    uint64_t invokeTimes;
};

class FakeChunkServerService : public ChunkService {
 public:
    void WriteChunk(::google::protobuf::RpcController *controller,
                    const ::curve::chunkserver::ChunkRequest *request,
                    ::curve::chunkserver::ChunkResponse *response,
                    google::protobuf::Closure *done) {
        brpc::ClosureGuard doneGuard(done);

        if (fakewriteret_->controller_ != nullptr && fakewriteret_->controller_->Failed()) {    // NOLINT
            controller->SetFailed("failed");
        }

        auto resp = static_cast<::curve::chunkserver::ChunkResponse*>(fakewriteret_->response_);    // NOLINT
        response->CopyFrom(*resp);

        static uint64_t latestSn = 0;
        if (request->sn() < latestSn) {
            response->set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_BACKWARD);
        }

        latestSn = std::max(latestSn, request->sn());
    }

    void ReadChunk(::google::protobuf::RpcController *controller,
                   const ::curve::chunkserver::ChunkRequest *request,
                   ::curve::chunkserver::ChunkResponse *response,
                   google::protobuf::Closure *done) {
        brpc::ClosureGuard doneGuard(done);

        brpc::Controller *cntl = dynamic_cast<brpc::Controller *>(controller);
        char buff[8192] = {0};
        if (request->has_appliedindex()) {
            memset(buff, 'a', 4096);
            memset(buff + 4096, 'b', 4096);
        } else {
            memset(buff, 'c', 4096);
            memset(buff + 4096, 'd', 4096);
        }
        cntl->response_attachment().append(buff, request->size());
        auto resp = static_cast<::curve::chunkserver::ChunkResponse*>(fakereadret_->response_);     // NOLINT
        response->CopyFrom(*resp);
    }

    void SetFakeWriteReturn(FakeReturn* ret) {
        fakewriteret_ = ret;
    }

    void SetFakeReadReturn(FakeReturn* ret) {
        fakereadret_ = ret;
    }

 private:
    FakeReturn* fakewriteret_;
    FakeReturn* fakereadret_;
};

class FakeRaftStateService : public braft::raft_stat {
 public:
    void default_method(::google::protobuf::RpcController* controller,
                        const ::braft::IndexRequest*,
                        ::braft::IndexResponse*,
                        ::google::protobuf::Closure* done) {
        brpc::ClosureGuard doneGuard(done);
        brpc::Controller *cntl = dynamic_cast<brpc::Controller *>(controller);  // NOLINT
        if (failed_) {
            cntl->SetFailed("failed for test");
            return;
        }
        cntl->response_attachment().append(buf_);
    }
    void SetBuf(const butil::IOBuf& iobuf) {
        buf_ = iobuf;
    }
    void SetFailed(bool failed) {
        failed_ = failed;
    }
 private:
    butil::IOBuf buf_;
    bool failed_ = false;
};

#endif  // TEST_CLIENT_FAKE_FAKECHUNKSERVER_H_
