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
 * Created Date: 18-10-10
 * Author: wudemiao
 */

#ifndef TEST_CLIENT_MOCK_MOCK_CHUNKSERVICE_H_
#define TEST_CLIENT_MOCK_MOCK_CHUNKSERVICE_H_

#include <brpc/controller.h>
#include <brpc/server.h>
#include <gtest/gtest.h>
#include <gmock/gmock.h>

#include <set>

#include "proto/chunk.pb.h"
#include "src/client/client_common.h"

namespace curve {
namespace client {

using ::testing::_;
using ::testing::Invoke;

using curve::chunkserver::ChunkService;
using curve::chunkserver::CHUNK_OP_STATUS;

/* 当前仅仅模拟单 chunk read/write */
class FakeChunkServiceImpl : public ChunkService {
 public:
    virtual ~FakeChunkServiceImpl() {}

    void WriteChunk(::google::protobuf::RpcController *controller,
                    const ::curve::chunkserver::ChunkRequest *request,
                    ::curve::chunkserver::ChunkResponse *response,
                    google::protobuf::Closure *done) {
        brpc::ClosureGuard doneGuard(done);

        chunkIds_.insert(request->chunkid());
        brpc::Controller *cntl = dynamic_cast<brpc::Controller *>(controller);
        ::memcpy(chunk_ + request->offset(),
                 cntl->request_attachment().to_string().c_str(),
                 request->size());
        response->set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS);
    }

    void ReadChunk(::google::protobuf::RpcController *controller,
                   const ::curve::chunkserver::ChunkRequest *request,
                   ::curve::chunkserver::ChunkResponse *response,
                   google::protobuf::Closure *done) {
        brpc::ClosureGuard doneGuard(done);

        brpc::Controller *cntl = dynamic_cast<brpc::Controller *>(controller);
        char buff[4096] = {0};
        ::memcpy(buff, chunk_ + request->offset(), request->size());
        cntl->response_attachment().append(buff, request->size());
        response->set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS);
    }

    void ReadChunkSnapshot(::google::protobuf::RpcController *controller,
                           const ::curve::chunkserver::ChunkRequest *request,
                           ::curve::chunkserver::ChunkResponse *response,
                           google::protobuf::Closure *done) {
        brpc::ClosureGuard doneGuard(done);

        brpc::Controller *cntl = dynamic_cast<brpc::Controller *>(controller);
        char buff[4096] = {0};
        ::memcpy(buff, chunk_ + request->offset(), request->size());
        cntl->response_attachment().append(buff, request->size());
        response->set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS);
    }

    void DeleteChunkSnapshotOrCorrectSn(
                             ::google::protobuf::RpcController *controller,
                             const ::curve::chunkserver::ChunkRequest *request,
                             ::curve::chunkserver::ChunkResponse *response,
                             google::protobuf::Closure *done) {
        brpc::ClosureGuard doneGuard(done);
        LOG(INFO) << "delete chunk snapshot: " << request->chunkid();
        if (chunkIds_.find(request->chunkid()) == chunkIds_.end()) {
            response->set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_CHUNK_NOTEXIST);  //NOLINT
            LOG(INFO) << "delete chunk snapshot: "
                      << request->chunkid() << " not exist";
            return;
        }
        chunkIds_.erase(request->chunkid());
        response->set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS);
    }

    void GetChunkInfo(::google::protobuf::RpcController *controller,
                      const ::curve::chunkserver::GetChunkInfoRequest *request,
                      ::curve::chunkserver::GetChunkInfoResponse *response,
                      google::protobuf::Closure *done) {
        brpc::ClosureGuard doneGuard(done);
        response->add_chunksn(1);
        response->add_chunksn(2);
        response->set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS);
    }

    void CreateCloneChunk(::google::protobuf::RpcController *controller,
                      const ::curve::chunkserver::ChunkRequest *request,
                      ::curve::chunkserver::ChunkResponse *response,
                      google::protobuf::Closure *done) {
        brpc::ClosureGuard doneGuard(done);
        response->set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS);
    }

    void RecoverChunk(::google::protobuf::RpcController *controller,
                      const ::curve::chunkserver::ChunkRequest *request,
                      ::curve::chunkserver::ChunkResponse *response,
                      google::protobuf::Closure *done) {
        brpc::ClosureGuard doneGuard(done);
        response->set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS);
    }

 private:
    std::set<ChunkID> chunkIds_;
    /* 由于 bthread 栈空间的限制，这里不会开很大的空间，如果测试需要更大的空间
     * 请在堆上申请 */
    char chunk_[4096] = {0};
};

class MockChunkServiceImpl : public ChunkService {
 public:
    MOCK_METHOD4(WriteChunk, void(::google::protobuf::RpcController
        *controller,
        const ::curve::chunkserver::ChunkRequest *request,
        ::curve::chunkserver::ChunkResponse *response,
        google::protobuf::Closure *done));
    MOCK_METHOD4(ReadChunk, void(::google::protobuf::RpcController
        *controller,
        const ::curve::chunkserver::ChunkRequest *request,
        ::curve::chunkserver::ChunkResponse *response,
        google::protobuf::Closure *done));
    MOCK_METHOD4(ReadChunkSnapshot, void(::google::protobuf::RpcController
        *controller,
        const ::curve::chunkserver::ChunkRequest *request,
        ::curve::chunkserver::ChunkResponse *response,
        google::protobuf::Closure *done));
    MOCK_METHOD4(DeleteChunkSnapshotOrCorrectSn, void(
        ::google::protobuf::RpcController
        *controller,
        const ::curve::chunkserver::ChunkRequest *request,
        ::curve::chunkserver::ChunkResponse *response,
        google::protobuf::Closure *done));
    MOCK_METHOD4(GetChunkInfo, void(::google::protobuf::RpcController
        *controller,
        const ::curve::chunkserver::GetChunkInfoRequest *request,
        ::curve::chunkserver::GetChunkInfoResponse *response,
        google::protobuf::Closure *done));
    MOCK_METHOD4(CreateCloneChunk,
                 void(::google::protobuf::RpcController* controller,
                      const ::curve::chunkserver::ChunkRequest* request,
                      ::curve::chunkserver::ChunkResponse* response,
                      google::protobuf::Closure* done));
    MOCK_METHOD4(RecoverChunk, void(::google::protobuf::RpcController
        *controller,
        const ::curve::chunkserver::ChunkRequest *request,
        ::curve::chunkserver::ChunkResponse *response,
        google::protobuf::Closure *done));

    void DelegateToFake() {
        ON_CALL(*this, WriteChunk(_, _, _, _))
            .WillByDefault(Invoke(&fakeChunkService,
                                  &FakeChunkServiceImpl::WriteChunk));
        ON_CALL(*this, ReadChunk(_, _, _, _))
            .WillByDefault(Invoke(&fakeChunkService,
                                  &FakeChunkServiceImpl::ReadChunk));
    }

 private:
    FakeChunkServiceImpl fakeChunkService;
};

}   // namespace client
}   // namespace curve

#endif  // TEST_CLIENT_MOCK_MOCK_CHUNKSERVICE_H_
