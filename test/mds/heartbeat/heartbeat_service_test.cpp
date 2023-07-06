/*
 *  Copyright (c) 2023 NetEase Inc.
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
 * Created Date: 2023-07-27
 * Author: wanghai (SeanHai)
 */

#include <brpc/controller.h>
#include <brpc/channel.h>
#include <brpc/server.h>
#include <gtest/gtest.h>
#include "proto/heartbeat.pb.h"
#include "test/mds/mock/mock_heartbeat_manager.h"
#include "src/mds/heartbeat/heartbeat_service.h"
#include "src/client/auth_client.h"

namespace curve {
namespace mds {
namespace heartbeat {

using ::testing::Return;
using ::testing::_;
using ::testing::SetArgPointee;

class TestHeartbeatService : public ::testing::Test {
 protected:
    virtual void SetUp() {
        server_ = new brpc::Server();
        heartbeatManager_ = std::make_shared<MockHeartbeatManager>();
        HeartbeatServiceImpl *heartbeatService =
            new HeartbeatServiceImpl(heartbeatManager_);
        ASSERT_EQ(0,
            server_->AddService(heartbeatService, brpc::SERVER_OWNS_SERVICE));
        ASSERT_EQ(0, server_->Start("127.0.0.1", {5910, 5919}, nullptr));
        listenAddr_ = server_->listen_address();

        // auth
        curve::common::ServerAuthOption authOption;
        authOption.enable = true;
        authOption.key = "1122334455667788";
        authOption.lastKey = "1122334455667788";
        std::string cId = "client";
        std::string sId = "snapshotclone";
        std::string sk = "123456789abcdefg";
        auto now = curve::common::TimeUtility::GetTimeofDaySec();
        curve::common::Authenticator::GetInstance().Init(
            curve::common::ZEROIV, authOption);
        curve::mds::auth::Ticket ticket;
        ticket.set_cid(cId);
        ticket.set_sessionkey(sk);
        ticket.set_expiration(now + 100);
        ticket.set_caps("*");
        ticket.set_sid(sId);
        std::string ticketStr;
        ASSERT_TRUE(ticket.SerializeToString(&ticketStr));
        std::string encticket;
        ASSERT_EQ(0, curve::common::Encryptor::AESEncrypt(
            authOption.key, curve::common::ZEROIV, ticketStr, &encticket));
        curve::mds::auth::ClientIdentity clientIdentity;
        clientIdentity.set_cid(cId);
        clientIdentity.set_timestamp(now);
        std::string cIdStr;
        ASSERT_TRUE(clientIdentity.SerializeToString(&cIdStr));
        std::string encCId;
        ASSERT_EQ(0, curve::common::Encryptor::AESEncrypt(
            sk, curve::common::ZEROIV, cIdStr, &encCId));
        token_.set_encticket(encticket);
        token_.set_encclientidentity(encCId);
        fakeToken_.set_encticket("fake");
        fakeToken_.set_encclientidentity("fake");
    }

    virtual void TearDown() {
        server_->Stop(0);
        server_->Join();
        delete server_;
        server_ = nullptr;
    }

 protected:
    std::shared_ptr<MockHeartbeatManager> heartbeatManager_;
    butil::EndPoint listenAddr_;
    brpc::Server *server_;
    curve::mds::auth::Token token_;
    curve::mds::auth::Token fakeToken_;
};

TEST_F(TestHeartbeatService, TestChunkServerHeartbeat) {
    brpc::Channel channel;
    ASSERT_EQ(0, channel.Init(listenAddr_, NULL));
    HeartbeatService_Stub stub(&channel);

    ChunkServerHeartbeatRequest request;
    ChunkServerHeartbeatResponse response;
    brpc::Controller cntl;
    request.set_chunkserverid(1);
    request.set_token("token");
    request.set_ip("127.0.0.1");
    request.set_port(9000);
    request.mutable_diskstate()->set_errmsg("");
    request.mutable_diskstate()->set_errtype(0);
    request.set_diskcapacity(100);
    request.set_diskused(50);
    request.set_leadercount(1);
    request.set_copysetcount(100);


    // seccess
    response.set_statuscode(HeartbeatStatusCode::hbOK);
    request.mutable_authtoken()->set_encclientidentity(
        token_.encclientidentity());
    request.mutable_authtoken()->set_encticket(token_.encticket());
    EXPECT_CALL(*heartbeatManager_, ChunkServerHeartbeat(_, _))
        .WillOnce(SetArgPointee<1>(response));

    stub.ChunkServerHeartbeat(&cntl, &request, &response, NULL);
    if (cntl.Failed()) {
        LOG(ERROR) << cntl.ErrorText();
    }
    ASSERT_FALSE(cntl.Failed());
    ASSERT_EQ(HeartbeatStatusCode::hbOK, response.statuscode());

    // fail
    cntl.Reset();
    response.set_statuscode(HeartbeatStatusCode::hbChunkserverUnknown);
    EXPECT_CALL(*heartbeatManager_, ChunkServerHeartbeat(_, _))
        .WillOnce(SetArgPointee<1>(response));

    stub.ChunkServerHeartbeat(&cntl, &request, &response, NULL);
    ASSERT_FALSE(cntl.Failed());
    ASSERT_EQ(HeartbeatStatusCode::hbChunkserverUnknown, response.statuscode());

    // auth fail
    request.mutable_authtoken()->set_encclientidentity(
        fakeToken_.encclientidentity());
    request.mutable_authtoken()->set_encticket(fakeToken_.encticket());
    cntl.Reset();
    stub.ChunkServerHeartbeat(&cntl, &request, &response, NULL);
    ASSERT_FALSE(cntl.Failed());
    ASSERT_EQ(HeartbeatStatusCode::hbAuthFailed, response.statuscode());
}

}  // namespace heartbeat
}  // namespace mds
}  // namespace curve
