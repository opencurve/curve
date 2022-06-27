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

#include <gtest/gtest.h>
#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gmock/gmock.h>

#include "src/client/chunkserver_broadcaster.h"
#include "test/client/mock/mock_chunkservice.h"

using ::testing::_;
using ::testing::DoAll;
using ::testing::Invoke;
using ::testing::Return;
using ::testing::SetArgPointee;
using ::curve::chunkserver::CHUNK_OP_STATUS;

namespace curve {
namespace client {

class ChunkServerBroadCasterTest : public testing::Test {
 protected:
    virtual void SetUp() {
        listenAddr_ = "chunkserver_broadcastertest_cs_listenAddr";
        server_ = new brpc::Server();
    }

    virtual void TearDown() {
        server_->Stop(0);
        server_->Join();
        delete server_;
        server_ = nullptr;
    }

 public:
    std::string listenAddr_;
    brpc::Server *server_;
};

TEST_F(ChunkServerBroadCasterTest, BroadCastFileEpochSuccess) {
    MockChunkServiceImpl mockChunkService;
    ASSERT_EQ(server_->AddService(&mockChunkService,
                                  brpc::SERVER_DOESNT_OWN_SERVICE), 0);
    ASSERT_EQ(server_->StartAtSockFile(listenAddr_.c_str(), nullptr), 0);

    CHUNK_OP_STATUS csRet = CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS;
    EXPECT_CALL(mockChunkService, UpdateEpoch(_, _, _, _))
        .Times(1000)
        .WillRepeatedly(Invoke([=](
            ::google::protobuf::RpcController *controller,
            const ::curve::chunkserver::UpdateEpochRequest *request,
            ::curve::chunkserver::UpdateEpochResponse *response,
            google::protobuf::Closure *done){
                brpc::ClosureGuard doneGuard(done);
                response->set_status(csRet);
                }));


    auto  csClient = std::make_shared<ChunkServerClient>();
    ChunkServerBroadCaster broadCaster(csClient);
    ChunkServerBroadCasterOption ops;
    ops.broadCastMaxNum = 10;
    broadCaster.Init(ops);

    uint64_t fileId = 1;
    uint64_t epoch = 100;

    std::list<CopysetPeerInfo<ChunkServerID>> csLocs;
    for (int i = 0; i < 1000; i++) {
        CopysetPeerInfo<ChunkServerID> csinfo;
        csinfo.peerID = i;
        csinfo.internalAddr = PeerAddr(EndPoint(listenAddr_));
        csLocs.push_back(std::move(csinfo));
    }
    int ret = broadCaster.BroadCastFileEpoch(fileId, epoch, csLocs);
    ASSERT_EQ(0, ret);
}

TEST_F(ChunkServerBroadCasterTest, BroadCastFileEpochFailedByEpochTooOld) {
    MockChunkServiceImpl mockChunkService;
    ASSERT_EQ(server_->AddService(&mockChunkService,
                                  brpc::SERVER_DOESNT_OWN_SERVICE), 0);
    ASSERT_EQ(server_->StartAtSockFile(listenAddr_.c_str(), nullptr), 0);

    CHUNK_OP_STATUS csRet = CHUNK_OP_STATUS::CHUNK_OP_STATUS_EPOCH_TOO_OLD;
    EXPECT_CALL(mockChunkService, UpdateEpoch(_, _, _, _))
        .WillRepeatedly(Invoke([=](
            ::google::protobuf::RpcController *controller,
            const ::curve::chunkserver::UpdateEpochRequest *request,
            ::curve::chunkserver::UpdateEpochResponse *response,
            google::protobuf::Closure *done){
                brpc::ClosureGuard doneGuard(done);
                response->set_status(csRet);
                }));

    auto  csClient = std::make_shared<ChunkServerClient>();
    ChunkServerBroadCaster broadCaster(csClient);
    ChunkServerBroadCasterOption ops;
    ops.broadCastMaxNum = 10;
    broadCaster.Init(ops);

    uint64_t fileId = 1;
    uint64_t epoch = 100;

    std::list<CopysetPeerInfo<ChunkServerID>> csLocs;
    for (int i = 0; i < 20; i++) {
        CopysetPeerInfo<ChunkServerID> csinfo;
        csinfo.peerID = i;
        csinfo.internalAddr = PeerAddr(EndPoint(listenAddr_));
        csLocs.push_back(std::move(csinfo));
    }
    int ret = broadCaster.BroadCastFileEpoch(fileId, epoch, csLocs);
    ASSERT_EQ(-LIBCURVE_ERROR::EPOCH_TOO_OLD, ret);
}


}   // namespace client
}   // namespace curve

