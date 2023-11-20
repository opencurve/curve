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
 * Created Date: 18-8-23
 * Author: wudemiao
 */

#ifndef SRC_CHUNKSERVER_CHUNK_CLOSURE_H_
#define SRC_CHUNKSERVER_CHUNK_CLOSURE_H_

#include <brpc/closure_guard.h>

#include <memory>

#include "proto/chunk.pb.h"
#include "src/chunkserver/op_request.h"

namespace curve {
namespace chunkserver {

/**
 * Carry all the contextual closures of the op request and pass them to the raft
 * for processing through the braft::Task, There are two places to call:
 * 1. The op request is processed normally by the raft, and will be called and
 * returned when it is finally applied
 * 2. After the op request was packaged for raft processing, an error occurred
 * before it could be processed, such as leader If the step down becomes a non
 * leader, it will explicitly return an error to the client in advance
 */
class ChunkClosure : public braft::Closure {
 public:
    explicit ChunkClosure(std::shared_ptr<ChunkOpRequest> request)
        : request_(request) {}

    ~ChunkClosure() = default;

    void Run() override;

 public:
    // Contains contextual information for op request
    std::shared_ptr<ChunkOpRequest> request_;
};

class ScanChunkClosure : public google::protobuf::Closure {
 public:
    ScanChunkClosure(ChunkRequest* request, ChunkResponse* response)
        : request_(request), response_(response) {}

    ~ScanChunkClosure() = default;

    void Run() override;

 public:
    ChunkRequest* request_;
    ChunkResponse* response_;
};

class SendScanMapClosure : public google::protobuf::Closure {
 public:
    SendScanMapClosure(FollowScanMapRequest* request,
                       FollowScanMapResponse* response, uint64_t timeout,
                       uint32_t retry, uint64_t retryIntervalUs,
                       brpc::Controller* cntl, brpc::Channel* channel)
        : request_(request),
          response_(response),
          rpcTimeoutMs_(timeout),
          retry_(retry),
          retryIntervalUs_(retryIntervalUs),
          cntl_(cntl),
          channel_(channel) {}

    ~SendScanMapClosure() = default;

    void Run() override;

 private:
    void Guard();

 public:
    FollowScanMapRequest* request_;
    FollowScanMapResponse* response_;
    uint64_t rpcTimeoutMs_;
    uint32_t retry_;
    uint64_t retryIntervalUs_;
    brpc::Controller* cntl_;
    brpc::Channel* channel_;
};

}  // namespace chunkserver
}  // namespace curve

#endif  // SRC_CHUNKSERVER_CHUNK_CLOSURE_H_
