/*
 * Project: curve
 * Created Date: Wed Mar 13 2019
 * Author: xuchaojie
 * Copyright (c) 2018 netease
 */

#ifndef TEST_MDS_CHUNKSERVERCLIENT_MOCK_CHUNKSERVER_H_
#define TEST_MDS_CHUNKSERVERCLIENT_MOCK_CHUNKSERVER_H_

#include "proto/cli.pb.h"
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

class MockCliService : public CliService {
 public:
    MOCK_METHOD4(get_leader,
        void(RpcController *controller,
        const GetLeaderRequest *request,
        GetLeaderResponse *response,
        Closure *done));
};


}  // namespace chunkserver
}  // namespace curve


#endif  // TEST_MDS_CHUNKSERVERCLIENT_MOCK_CHUNKSERVER_H_
