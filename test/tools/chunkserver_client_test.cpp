/*
 * Project: curve
 * File Created: 2019-11-28
 * Author: charisu
 * Copyright (c)￼ 2018 netease
 */

#include <gtest/gtest.h>
#include "src/tools/chunkserver_client.h"
#include "test/client/fake/mockMDS.h"
#include "test/client/fake/fakeMDS.h"

using curve::chunkserver::GetChunkInfoResponse;
using curve::chunkserver::CHUNK_OP_STATUS;

std::string metaserver_addr = "127.0.0.1:9170";  // NOLINT
uint32_t chunk_size = 4*1024*1024;  // NOLINT
uint32_t segment_size = 1*1024*1024*1024;  // NOLINT

DECLARE_string(chunkserver_list);
DEFINE_uint64(rpcTimeout, 3000, "millisecond for rpc timeout");
DEFINE_uint64(rpcRetryTimes, 5, "rpc retry times");
namespace brpc {
DECLARE_int32(health_check_interval);
}

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
    curve::tool::ChunkServerClient client;
    FakeMDS fakemds;
};

TEST_F(ChunkServerClientTest, Init) {
    ASSERT_EQ(0, client.Init("127.0.0.1:9191"));
    ASSERT_EQ(-1, client.Init("1235"));
}

TEST_F(ChunkServerClientTest, GetCopysetStatus) {
    std::vector<FakeRaftStateService *> statServices =
                                    fakemds.GetRaftStateService();
    // 正常情况
    butil::IOBuf iobuf;
    iobuf.append("test");
    statServices[0]->SetBuf(iobuf);
    ASSERT_EQ(0, client.Init("127.0.0.1:9191"));
    ASSERT_EQ(0, client.GetCopysetStatus(&iobuf));

    // 传入空指针
    ASSERT_EQ(-1, client.GetCopysetStatus(nullptr));

    // RPC失败的情况
    statServices[0]->SetFailed(true);
    ASSERT_EQ(-1, client.GetCopysetStatus(&iobuf));
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

