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
 * File Created: 2019-11-28
 * Author: charisu
 */

#include <gtest/gtest.h>
#include "src/tools/chunkserver_client.h"
#include "test/client/fake/mockMDS.h"
#include "test/client/fake/fakeMDS.h"

using curve::chunkserver::GetChunkInfoResponse;
using curve::chunkserver::CHUNK_OP_STATUS;


DECLARE_string(chunkserver_list);
namespace brpc {
DECLARE_int32(health_check_interval);
}

namespace curve {
namespace tool {

class ChunkServerClientTest : public ::testing::Test {
 protected:
    ChunkServerClientTest() : fakemds("test") {}
    void SetUp() {
        FLAGS_chunkserver_list = "127.0.0.1:9191:0";
        brpc::FLAGS_health_check_interval = -1;
        fakemds.Initialize();
        fakemds.CreateFakeChunkservers(false);
    }
    void TearDown() {
        fakemds.UnInitialize();
    }
    ChunkServerClient client;
    FakeMDS fakemds;
};

TEST_F(ChunkServerClientTest, Init) {
    ASSERT_EQ(0, client.Init("127.0.0.1:9191"));
    ASSERT_EQ(-1, client.Init("1235"));
}

TEST_F(ChunkServerClientTest, GetRaftStatus) {
    std::vector<FakeRaftStateService *> statServices =
                                    fakemds.GetRaftStateService();
    // 正常情况
    butil::IOBuf iobuf;
    iobuf.append("test");
    statServices[0]->SetBuf(iobuf);
    ASSERT_EQ(0, client.Init("127.0.0.1:9191"));
    ASSERT_EQ(0, client.GetRaftStatus(&iobuf));

    // 传入空指针
    ASSERT_EQ(-1, client.GetRaftStatus(nullptr));

    // RPC失败的情况
    statServices[0]->SetFailed(true);
    ASSERT_EQ(-1, client.GetRaftStatus(&iobuf));
}

TEST_F(ChunkServerClientTest, CheckChunkServerOnline) {
    std::vector<FakeChunkService *> chunkServices = fakemds.GetChunkservice();
    brpc::Controller cntl;
    std::unique_ptr<GetChunkInfoResponse> response(
                    new GetChunkInfoResponse());
    response->set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS);
    std::unique_ptr<FakeReturn> fakeret(
        new FakeReturn(&cntl, static_cast<void*>(response.get())));
    chunkServices[0]->SetGetChunkInfo(fakeret.get());
    // 正常情况
    ASSERT_EQ(0, client.Init("127.0.0.1:9191"));
    ASSERT_EQ(true, client.CheckChunkServerOnline());

    // RPC失败的情况
    cntl.SetFailed("fail for test");
    ASSERT_EQ(false, client.CheckChunkServerOnline());
}

TEST_F(ChunkServerClientTest, GetCopysetStatus2) {
    auto copysetServices = fakemds.GetCreateCopysetService();
    CopysetStatusRequest request;
    CopysetStatusResponse response;
    curve::common::Peer *peer = new curve::common::Peer();
    peer->set_address("127.0.0.1:9191");
    request.set_logicpoolid(1);
    request.set_copysetid(1001);
    request.set_allocated_peer(peer);
    request.set_queryhash(true);

    // 正常情况
    ASSERT_EQ(0, client.Init("127.0.0.1:9191"));
    ASSERT_EQ(0, client.GetCopysetStatus(request, &response));

    // 返回码不ok的情况
    copysetServices[0]->SetStatus(
        COPYSET_OP_STATUS::COPYSET_OP_STATUS_COPYSET_NOTEXIST);
    ASSERT_EQ(-1, client.GetCopysetStatus(request, &response));

    // RPC失败的情况
    brpc::Controller cntl;
    std::unique_ptr<FakeReturn> fakeret(new FakeReturn(&cntl, nullptr));
    copysetServices[0]->SetFakeReturn(fakeret.get());
    ASSERT_EQ(-1, client.GetCopysetStatus(request, &response));
}

TEST_F(ChunkServerClientTest, GetChunkHash) {
    std::vector<FakeChunkService *> chunkServices = fakemds.GetChunkservice();
    brpc::Controller cntl;
    std::unique_ptr<GetChunkHashResponse> response(
                    new GetChunkHashResponse());
    response->set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS);
    response->set_hash("1234");
    std::unique_ptr<FakeReturn> fakeret(
        new FakeReturn(&cntl, static_cast<void*>(response.get())));
    chunkServices[0]->SetGetChunkHash(fakeret.get());
    Chunk chunk(1, 100, 1001);
    // 正常情况
    ASSERT_EQ(0, client.Init("127.0.0.1:9191"));
    std::string hash;
    ASSERT_EQ(0, client.GetChunkHash(chunk, &hash));
    ASSERT_EQ("1234", hash);

    // RPC失败的情况
    cntl.SetFailed("fail for test");
    ASSERT_EQ(-1, client.GetChunkHash(chunk, &hash));

    // 返回码不为ok
    response->set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_FAILURE_UNKNOWN);
    ASSERT_EQ(-1, client.GetChunkHash(chunk, &hash));
}

}  // namespace tool
}  // namespace curve
