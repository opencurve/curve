/*
 * Project: curve
 * Created Date: Wednesday April 3rd 2019
 * Author: yangyaokai
 * Copyright (c) 2019 netease
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

using curve::chunkserver::CHUNK_OP_STATUS;
using curve::chunkserver::ChunkResponse;
using curve::chunkserver::GetChunkInfoResponse;
using ::google::protobuf::Message;
using ::google::protobuf::Closure;

const uint32_t SLICE_SIZE = 1024 * 1024;
const uint32_t PAGE_SIZE = 4 * 1024;
const uint32_t CHUNK_SIZE = 16 * 1024 * 1024;
const uint32_t LAST_INDEX = 5;

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
