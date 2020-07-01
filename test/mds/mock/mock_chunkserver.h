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
 * Created Date: Wed Mar 13 2019
 * Author: xuchaojie
 */

#ifndef TEST_MDS_MOCK_MOCK_CHUNKSERVER_H_
#define TEST_MDS_MOCK_MOCK_CHUNKSERVER_H_

#include "proto/cli2.pb.h"
#include "proto/chunk.pb.h"

namespace curve {
namespace chunkserver {

using ::google::protobuf::RpcController;
using ::google::protobuf::Closure;

class MockChunkService : public ChunkService {
 public:
    MOCK_METHOD4(DeleteChunkSnapshotOrCorrectSn,
        void(RpcController *controller,
        const ChunkRequest *request,
        ChunkResponse *response,
        Closure *done));

    MOCK_METHOD4(DeleteChunk,
        void(RpcController *controller,
        const ChunkRequest *request,
        ChunkResponse *response,
        Closure *done));
};

class MockCliService : public CliService2 {
 public:
    MOCK_METHOD4(GetLeader,
        void(RpcController *controller,
        const GetLeaderRequest2 *request,
        GetLeaderResponse2 *response,
        Closure *done));
};


}  // namespace chunkserver
}  // namespace curve


#endif  // TEST_MDS_MOCK_MOCK_CHUNKSERVER_H_
