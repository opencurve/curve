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

#include "src/chunkserver/op_request.h"

namespace curve {
namespace chunkserver {

/**
 * The closure which carries all contexts of op request，is processed by raft through braft::Task
 * Calls occur in two places：
 * 1.op request which is processed by raft normally，will call return when on apply
 * 2.if op request goes wrong without any error handling after it is processed by raft (eg. leader
 *   steps down into non-leader)，it will return error to client in advance
 */
class ChunkClosure : public braft::Closure {
 public:
    explicit ChunkClosure(std::shared_ptr<ChunkOpRequest> request)
        : request_(request) {}

    ~ChunkClosure() = default;

    void Run() override;

 public:
    // contain all contexts of op request
    std::shared_ptr<ChunkOpRequest> request_;
};

}  // namespace chunkserver
}  // namespace curve

#endif  // SRC_CHUNKSERVER_CHUNK_CLOSURE_H_
