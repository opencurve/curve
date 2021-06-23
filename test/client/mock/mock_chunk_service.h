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
 * Created Date: Wed Jun 23 11:20:28 CST 2021
 * Author: wuhanqing
 */

#ifndef TEST_CLIENT_MOCK_MOCK_CHUNK_SERVICE_H_
#define TEST_CLIENT_MOCK_MOCK_CHUNK_SERVICE_H_

#include <gmock/gmock.h>

#include "proto/chunk.pb.h"

namespace curve {
namespace chunkserver {

class MockChunkService : public ChunkService {
 public:
    MOCK_METHOD4(WriteChunk,
                 void(::google::protobuf::RpcController *controller,
                      const ChunkRequest *request, ChunkResponse *response,
                      ::google::protobuf::Closure *done));
    MOCK_METHOD4(ReadChunk,
                 void(::google::protobuf::RpcController *controller,
                      const ChunkRequest *request, ChunkResponse *response,
                      ::google::protobuf::Closure *done));
};

}  // namespace chunkserver
}  // namespace curve

#endif  // TEST_CLIENT_MOCK_MOCK_CHUNK_SERVICE_H_
