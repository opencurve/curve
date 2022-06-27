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
 * File Created: 2022-06-30
 * Author: xuchaojie
 */

#ifndef SRC_CLIENT_CHUNKSERVER_CLIENT_H_
#define SRC_CLIENT_CHUNKSERVER_CLIENT_H_

#include <brpc/channel.h>

#include "include/client/libcurve_define.h"
#include "src/client/client_common.h"
#include "src/client/metacache_struct.h"
#include "proto/chunk.pb.h"

using ::google::protobuf::Closure;
using ::google::protobuf::Message;
using ::curve::chunkserver::UpdateEpochRequest;
using ::curve::chunkserver::UpdateEpochResponse;

namespace curve {
namespace client {

struct ChunkServerClientRetryOptions {
     uint32_t rpcTimeoutMs;
     uint32_t rpcMaxTry;
     uint32_t rpcIntervalUs;

    ChunkServerClientRetryOptions()
      : rpcTimeoutMs(500),
        rpcMaxTry(3),
        rpcIntervalUs(100000) {}
};

class ChunkServerClientClosure : public Closure {
 public:
    ChunkServerClientClosure() {}
    virtual ~ChunkServerClientClosure() {}

    void SetErrCode(int ret) {
        err_ = ret;
    }

    int GetErrorCode() {
        return err_;
    }

 private:
    int err_;
};

class ChunkServerClient;

struct UpdateEpochContext {
    brpc::Channel *channel;
    brpc::Controller *cntl;
    UpdateEpochRequest *request;
    UpdateEpochResponse *response;
    ChunkServerClientClosure *done;
    uint32_t curTry;
    ChunkServerClientRetryOptions retryOps_;
};

class ChunkServerClient {
 public:
    ChunkServerClient() {}
    ~ChunkServerClient() {}

    int UpdateFileEpoch(
        const CopysetPeerInfo &cs, uint64_t fileId, uint64_t epoch,
        ChunkServerClientClosure *done);

    static void OnUpdateFileEpochReturned(UpdateEpochContext* ctx);

 private:
    ChunkServerClientRetryOptions retryOps_;
};

}   // namespace client
}   // namespace curve

#endif  // SRC_CLIENT_CHUNKSERVER_CLIENT_H_
