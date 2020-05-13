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
 * Created Date: 18-11-12
 * Author: wudemiao
 */

#include <brpc/server.h>
#include <gtest/gtest.h>

#include "src/client/client_common.h"
#include "src/client/request_sender.h"
#include "test/client/mock_chunkservice.h"

namespace curve {
namespace client {

using ::testing::_;
using ::testing::AnyNumber;
using ::testing::AtLeast;
using ::testing::DoAll;
using ::testing::InSequence;
using ::testing::Invoke;
using ::testing::Return;
using ::testing::SaveArg;
using ::testing::SaveArgPointee;
using ::testing::SetArgPointee;
using ::testing::SetArgReferee;

class FakeChunkClosure : public ClientClosure {
 public:
    FakeChunkClosure()
        : ClientClosure(nullptr, nullptr), reqeustClosure(nullptr) {
        SetClosure(&reqeustClosure);
    }

    void Run() override {}

    void SendRetryRequest() override {}

 private:
    RequestClosure reqeustClosure;
};

void MockChunkRequestService(::google::protobuf::RpcController* controller,
                             const ::curve::chunkserver::ChunkRequest* request,
                             ::curve::chunkserver::ChunkResponse* response,
                             google::protobuf::Closure* done) {
    brpc::ClosureGuard doneGuard(done);
}

class RequestSenderTest : public ::testing::Test {
 public:
    void SetUp() override {
        ASSERT_EQ(0, server_.AddService(&mockChunkService_,
                                        brpc::SERVER_DOESNT_OWN_SERVICE))
            << "Add service failed";

        ASSERT_EQ(0, server_.Start(serverAddr_.c_str(), nullptr))
            << "Start server failed";
    }

    void TearDown() override {
        server_.Stop(0);
        server_.Join();
    }

 protected:
    brpc::Server server_;
    MockChunkServiceImpl mockChunkService_;
    IOSenderOption ioSenderOption_;
    std::string serverAddr_ = "127.0.0.1:19500";
};

TEST_F(RequestSenderTest, BasicTest) {
    // 非法的 port
    std::string leaderStr = "127.0.0.1:65539";
    butil::EndPoint leaderAddr;
    ChunkServerID leaderId = 1;

    butil::str2endpoint(leaderStr.c_str(), &leaderAddr);
    RequestSender requestSender(leaderId, leaderAddr);
    ASSERT_EQ(-1, requestSender.Init(ioSenderOption_));
}

TEST_F(RequestSenderTest, TestReadChunkAppliedIndex) {
    ioSenderOption_.chunkserverEnableAppliedIndexRead = true;

    butil::EndPoint serverEndpoint;
    butil::str2endpoint(serverAddr_.c_str(), &serverEndpoint);

    RequestSender requestSender(0, serverEndpoint);
    ASSERT_EQ(0, requestSender.Init(ioSenderOption_));

    uint64_t appliedIndex = 0;

    {
        curve::chunkserver::ChunkRequest chunkRequest;
        EXPECT_CALL(mockChunkService_, ReadChunk(_, _, _, _))
            .Times(1)
            .WillOnce(DoAll(SaveArgPointee<1>(&chunkRequest),
                            Invoke(MockChunkRequestService)));

        FakeChunkClosure closure;

        appliedIndex = 100;
        requestSender.ReadChunk(ChunkIDInfo(), 0, 0, 0, appliedIndex, {},
                                &closure);

        std::this_thread::sleep_for(std::chrono::milliseconds(5));

        ASSERT_TRUE(chunkRequest.has_appliedindex());
    }

    {
        curve::chunkserver::ChunkRequest chunkRequest;
        EXPECT_CALL(mockChunkService_, ReadChunk(_, _, _, _))
            .Times(1)
            .WillOnce(DoAll(SaveArgPointee<1>(&chunkRequest),
                            Invoke(MockChunkRequestService)));

        FakeChunkClosure closure;

        appliedIndex = 0;
        requestSender.ReadChunk(ChunkIDInfo(), 0, 0, 0, appliedIndex, {},
                                &closure);

        std::this_thread::sleep_for(std::chrono::milliseconds(5));

        ASSERT_FALSE(chunkRequest.has_appliedindex());
    }
}

TEST_F(RequestSenderTest, TestWriteChunkSourceInfo) {
    butil::EndPoint serverEndpoint;
    butil::str2endpoint(serverAddr_.c_str(), &serverEndpoint);

    RequestSender requestSender(0, serverEndpoint);
    ASSERT_EQ(0, requestSender.Init(ioSenderOption_));

    RequestSourceInfo sourceInfo;

    {
        curve::chunkserver::ChunkRequest chunkRequest;
        EXPECT_CALL(mockChunkService_, WriteChunk(_, _, _, _))
            .Times(1)
            .WillOnce(DoAll(SaveArgPointee<1>(&chunkRequest),
                            Invoke(MockChunkRequestService)));

        FakeChunkClosure closure;

        sourceInfo.cloneFileSource.clear();
        requestSender.WriteChunk(ChunkIDInfo(), 0, 0, 0, 0,
                                 sourceInfo, &closure);

        std::this_thread::sleep_for(std::chrono::milliseconds(5));

        ASSERT_FALSE(chunkRequest.has_clonefilesource());
        ASSERT_FALSE(chunkRequest.has_clonefileoffset());
    }

    {
        curve::chunkserver::ChunkRequest chunkRequest;
        EXPECT_CALL(mockChunkService_, WriteChunk(_, _, _, _))
            .Times(1)
            .WillOnce(DoAll(SaveArgPointee<1>(&chunkRequest),
                            Invoke(MockChunkRequestService)));

        FakeChunkClosure closure;

        sourceInfo.cloneFileSource = "/test_WriteChunkSourceInfo";
        sourceInfo.cloneFileOffset = 0;

        requestSender.WriteChunk(ChunkIDInfo(), 0, 0, 0, 0,
                                 sourceInfo, &closure);

        std::this_thread::sleep_for(std::chrono::milliseconds(5));

        ASSERT_TRUE(chunkRequest.has_clonefilesource());
        ASSERT_TRUE(chunkRequest.has_clonefileoffset());
    }
}

TEST_F(RequestSenderTest, TestReadChunkSourceInfo) {
    ioSenderOption_.chunkserverEnableAppliedIndexRead = true;

    butil::EndPoint serverEndpoint;
    butil::str2endpoint(serverAddr_.c_str(), &serverEndpoint);

    RequestSender requestSender(0, serverEndpoint);
    ASSERT_EQ(0, requestSender.Init(ioSenderOption_));

    uint64_t appliedIndex = 100;
    RequestSourceInfo sourceInfo;

    {
        curve::chunkserver::ChunkRequest chunkRequest;
        EXPECT_CALL(mockChunkService_, ReadChunk(_, _, _, _))
            .Times(1)
            .WillOnce(DoAll(SaveArgPointee<1>(&chunkRequest),
                            Invoke(MockChunkRequestService)));

        FakeChunkClosure closure;

        sourceInfo.cloneFileSource.clear();
        requestSender.ReadChunk(ChunkIDInfo(), 0, 0, 0, appliedIndex,
                                sourceInfo, &closure);

        std::this_thread::sleep_for(std::chrono::milliseconds(5));

        ASSERT_FALSE(chunkRequest.has_clonefilesource());
        ASSERT_FALSE(chunkRequest.has_clonefileoffset());
    }

    {
        curve::chunkserver::ChunkRequest chunkRequest;
        EXPECT_CALL(mockChunkService_, ReadChunk(_, _, _, _))
            .Times(1)
            .WillOnce(DoAll(SaveArgPointee<1>(&chunkRequest),
                            Invoke(MockChunkRequestService)));

        FakeChunkClosure closure;

        sourceInfo.cloneFileSource = "/test_ReadChunkSourceInfo";
        sourceInfo.cloneFileOffset = 0;

        requestSender.ReadChunk(ChunkIDInfo(), 0, 0, 0, appliedIndex,
                                sourceInfo, &closure);

        std::this_thread::sleep_for(std::chrono::milliseconds(5));

        ASSERT_TRUE(chunkRequest.has_clonefilesource());
        ASSERT_TRUE(chunkRequest.has_clonefileoffset());
    }
}

}  // namespace client
}  // namespace curve
