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
 * Created Date: Wednesday April 3rd 2019
 * Author: yangyaokai
 */

#ifndef TEST_CHUNKSERVER_CLONE_CLONE_TEST_UTIL_H_
#define TEST_CHUNKSERVER_CLONE_CLONE_TEST_UTIL_H_

#include <gtest/gtest.h>
#include <gmock/gmock.h>
#include <google/protobuf/stubs/callback.h>
#include <google/protobuf/message.h>
#include <butil/iobuf.h>
#include <brpc/controller.h>
#include <memory>

#include "proto/chunk.pb.h"

namespace curve {
namespace chunkserver {

using ::testing::_;
using ::testing::Ge;
using ::testing::Gt;
using ::testing::StrEq;
using ::testing::Return;
using ::testing::NotNull;
using ::testing::Mock;
using ::testing::Truly;
using ::testing::DoAll;
using ::testing::ReturnArg;
using ::testing::ElementsAre;
using ::testing::SaveArg;
using ::testing::SetArgPointee;
using ::testing::SetArrayArgument;
using ::testing::Invoke;

using curve::chunkserver::CHUNK_OP_STATUS;
using curve::chunkserver::ChunkResponse;
using curve::chunkserver::GetChunkInfoResponse;
using ::google::protobuf::Message;
using ::google::protobuf::Closure;

const uint32_t SLICE_SIZE = 1024 * 1024;
const uint32_t LAST_INDEX = 5;

class FakeChunkClosure : public ::google::protobuf::Closure {
    struct ResponseContent {
        uint64_t appliedindex;
        int status;
        butil::IOBuf attachment;
        ResponseContent() : appliedindex(0), status(-1) {}
    };

 public:
    FakeChunkClosure() : isDone_(false)
                      , cntl_(nullptr)
                      , request_(nullptr)
                      , response_(nullptr) {}
    ~FakeChunkClosure() {
        std::unique_ptr<FakeChunkClosure> selfGuard(this);
    }

    void Run() {
        std::unique_ptr<brpc::Controller> cntlGuard(cntl_);
        std::unique_ptr<ChunkRequest> requestGuard(request_);
        std::unique_ptr<ChunkResponse> responseGuard(response_);
        isDone_ = true;
        resContent_.appliedindex = response_->appliedindex();
        resContent_.status = response_->status();
        resContent_.attachment.append(
            cntl_->response_attachment().to_string());
    }

    void SetCntl(brpc::Controller* cntl) {
        cntl_ = cntl;
    }
    void SetRequest(Message* request) {
        request_ = dynamic_cast<ChunkRequest *>(request);
    }
    void SetResponse(Message* response) {
        response_ = dynamic_cast<ChunkResponse *>(response);
    }

 public:
    bool                isDone_;
    brpc::Controller    *cntl_;
    ChunkRequest        *request_;
    ChunkResponse       *response_;
    ResponseContent     resContent_;
};

class UnitTestClosure : public ::google::protobuf::Closure {
 public:
    UnitTestClosure() : isDone_(false)
                      , cntl_(nullptr)
                      , request_(nullptr)
                      , response_(nullptr) {}
    ~UnitTestClosure() {
    }

    void Run() {
        isDone_ = true;
    }

    void Reset() {
        if (response_ != nullptr)
            response_->Clear();
        if (cntl_ != nullptr)
            cntl_->response_attachment().clear();
        isDone_ = false;
    }

    void Release() {
        std::unique_ptr<UnitTestClosure> selfGuard(this);
        std::unique_ptr<brpc::Controller> cntlGuard(cntl_);
        std::unique_ptr<ChunkRequest> requestGuard(request_);
        std::unique_ptr<ChunkResponse> responseGuard(response_);
    }

    void SetCntl(brpc::Controller* cntl) {
        cntl_ = cntl;
    }
    void SetRequest(Message* request) {
        request_ = dynamic_cast<ChunkRequest *>(request);
    }
    void SetResponse(Message* response) {
        response_ = dynamic_cast<ChunkResponse *>(response);
    }

 public:
    bool                isDone_;
    brpc::Controller    *cntl_;
    ChunkRequest        *request_;
    ChunkResponse       *response_;
};


}  // namespace chunkserver
}  // namespace curve

#endif  // TEST_CHUNKSERVER_CLONE_CLONE_TEST_UTIL_H_
