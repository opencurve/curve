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
 * Created Date: 2023-06-12
 * Author: wanghai (SeanHai)
*/

#include <gmock/gmock-actions.h>
#include <gtest/gtest.h>
#include <brpc/server.h>
#include <memory>
#include "proto/auth.pb.h"
#include "src/client/auth_client.h"
#include "src/common/authenticator.h"
#include "src/common/timeutility.h"
#include "test/client/mock/mock_auth_service.h"
#include "test/mds/auth/mock_auth_node.h"


using ::testing::_;
using ::testing::DoAll;
using ::testing::Invoke;
using ::testing::Return;
using ::testing::SetArgPointee;
using ::curve::mds::auth::GetTicketRequest;
using ::curve::mds::auth::GetTicketResponse;
using ::curve::mds::auth::TicketAttach;
using ::curve::mds::auth::AuthStatusCode;
using ::curve::mds::auth::ClientIdentity;
using ::curve::mds::auth::MockAuthNode;

namespace curve {
namespace client {

class AuthClientTest : public testing::Test {
 protected:
    void SetUp() override {
        const std::string mdsAddr1 = "127.0.0.1:9900";
        const std::string mdsAddr2 = "127.0.0.1:9901";

        ASSERT_EQ(0, server_.AddService(&mockAuthService_,
                                        brpc::SERVER_DOESNT_OWN_SERVICE));

        // only start mds on mdsAddr1
        ASSERT_EQ(0, server_.Start(mdsAddr1.c_str(), nullptr));

        option_.rpcRetryOpt.addrs = {mdsAddr2, mdsAddr1};
        option_.rpcRetryOpt.rpcTimeoutMs = 500;            // 500ms
        option_.rpcRetryOpt.maxRPCTimeoutMS = 1000;        // 1s
        option_.rpcRetryOpt.rpcRetryIntervalUS = 1000000;  // 100ms
        option_.mdsMaxRetryMS = 2000;                      // 2s
        option_.rpcRetryOpt.maxFailedTimesBeforeChangeAddr = 1;

        authOption_.clientId = "curve_client";
        authOption_.enable = true;
        authOption_.key = "123456789abcdefg";
        authOption_.lastKey = "1122334455667788";
        authOption_.ticketRefreshIntervalSec = 1;
        authOption_.ticketRefreshThresholdSec = 2;

        authClient_.Init(option_, authOption_);
    }

    void TearDown() override {
        server_.Stop(0);
        LOG(INFO) << "server stopped";
        server_.Join();
        LOG(INFO) << "server joined";
    }

 protected:
    brpc::Server server_;
    curve::mds::auth::MockAuthService mockAuthService_;
    MetaServerOption option_;
    curve::common::AuthClientOption authOption_;
    curve::client::AuthClient authClient_;
};

template <typename RpcRequestType, typename RpcResponseType,
          bool RpcFailed = false>
void FakeRpcService(google::protobuf::RpcController* cntl_base,
                    const RpcRequestType* request, RpcResponseType* response,
                    google::protobuf::Closure* done) {
    if (RpcFailed) {
        brpc::Controller* cntl = static_cast<brpc::Controller*>(cntl_base);
        cntl->SetFailed(113, "Not connected to");
    }
    done->Run();
}

TEST_F(AuthClientTest, TestGetToken_failed1) {
    // case1: cache is empty refresh rpc cntl failed and retry success
    std::string serverId = "mds";
    Token token;
    GetTicketResponse response;
    response.set_status(AuthStatusCode::AUTH_OK);
    EXPECT_CALL(mockAuthService_, GetTicket(_, _, _, _))
        .WillOnce(
            Invoke(FakeRpcService<GetTicketRequest, GetTicketResponse, true>))
        .WillOnce(DoAll(
            SetArgPointee<2>(response),
            Invoke(FakeRpcService<GetTicketRequest, GetTicketResponse>)));
    ASSERT_FALSE(authClient_.GetToken(serverId, &token));
}

TEST_F(AuthClientTest, TestGetToken_failed2) {
    // case2: cache is empty, auth key not exit and retry success
    std::string serverId = "mds";
    Token token;
    GetTicketResponse response;
    response.set_status(AuthStatusCode::AUTH_KEY_NOT_EXIST);
    EXPECT_CALL(mockAuthService_, GetTicket(_, _, _, _))
        .WillOnce(DoAll(
            SetArgPointee<2>(response),
            Invoke(FakeRpcService<GetTicketRequest, GetTicketResponse>)));
    ASSERT_FALSE(authClient_.GetToken(serverId, &token));
}

TEST_F(AuthClientTest, TestGetToken_failed3) {
    // case3: cache is empty, refresh success but expiration
    std::string serverId = "mds";
    Token token;
    std::string ticket = "aaaaaaaaaaaa";
    std::string sk = "1122334455667788";
    TicketAttach info;
    info.set_expiration(curve::common::TimeUtility::GetTimeofDaySec() - 1);
    info.set_sessionkey(sk);
    std::string attachStr;
    EXPECT_TRUE(info.SerializeToString(&attachStr));
    std::string encAttach;
    EXPECT_EQ(0, curve::common::Encryptor::AESEncrypt(
        authOption_.key, curve::common::ZEROIV, attachStr, &encAttach));
    GetTicketResponse response;
    response.set_status(AuthStatusCode::AUTH_OK);
    response.set_encticket(ticket);
    response.set_encticketattach(encAttach);
    EXPECT_CALL(mockAuthService_, GetTicket(_, _, _, _))
        .WillOnce(DoAll(
            SetArgPointee<2>(response),
            Invoke(FakeRpcService<GetTicketRequest, GetTicketResponse>)));
    ASSERT_FALSE(authClient_.GetToken(serverId, &token));
    EXPECT_EQ(1, authClient_.MapSize());
}

TEST_F(AuthClientTest, TestGetToken_failed4) {
    // case4: cache is empty, refresh success but decrypt failed
    Token token;
    std::string serverId = "mds";
    std::string ticket = "aaaaaaaaaaaa";
    std::string encAttach = "encattach";
    GetTicketResponse response;
    response.set_status(AuthStatusCode::AUTH_OK);
    response.set_encticket(ticket);
    response.set_encticketattach(encAttach);
    EXPECT_CALL(mockAuthService_, GetTicket(_, _, _, _))
        .WillOnce(DoAll(
            SetArgPointee<2>(response),
            Invoke(FakeRpcService<GetTicketRequest, GetTicketResponse>)));
    ASSERT_FALSE(authClient_.GetToken(serverId, &token));
}

TEST_F(AuthClientTest, TestGetToken_success) {
    std::string serverId = "mds";
    Token token;
    std::string ticket = "aaaaaaaaaaaa";
    std::string sk = "1122334455667788";
    TicketAttach info;
    info.set_expiration(curve::common::TimeUtility::GetTimeofDaySec() + 100);
    info.set_sessionkey(sk);
    std::string attachStr;
    EXPECT_TRUE(info.SerializeToString(&attachStr));
    std::string encAttach;
    EXPECT_EQ(0, curve::common::Encryptor::AESEncrypt(
        authOption_.key, curve::common::ZEROIV, attachStr, &encAttach));
    GetTicketResponse response;
    response.set_status(AuthStatusCode::AUTH_OK);
    response.set_encticket(ticket);
    response.set_encticketattach(encAttach);
    EXPECT_CALL(mockAuthService_, GetTicket(_, _, _, _))
        .WillOnce(DoAll(
            SetArgPointee<2>(response),
            Invoke(FakeRpcService<GetTicketRequest, GetTicketResponse>)));
    // start two thread to GetToken
    std::thread t1([&]() {
        ASSERT_TRUE(authClient_.GetToken(serverId, &token));
    });
    std::thread t2([&]() {
        ASSERT_TRUE(authClient_.GetToken(serverId, &token));
    });
    t1.join();
    t2.join();
    // get from cache
    ASSERT_TRUE(authClient_.GetToken(serverId, &token));
    ASSERT_EQ(1, authClient_.MapSize());
    ASSERT_EQ(ticket, token.encticket());
    std::string decClientStr;
    ASSERT_EQ(0, curve::common::Encryptor::AESDecrypt(sk,
        curve::common::ZEROIV, token.encclientidentity(), &decClientStr));
    ClientIdentity cInfo;
    ASSERT_TRUE(cInfo.ParseFromString(decClientStr));
    ASSERT_EQ(cInfo.cid(), authOption_.clientId);
}

TEST_F(AuthClientTest, TestGetToken_success_with_lastkey) {
    std::string serverId = "mds";
    Token token;
    std::string ticket = "aaaaaaaaaaaa";
    std::string sk = "1122334455667788";
    TicketAttach info;
    info.set_expiration(curve::common::TimeUtility::GetTimeofDaySec() + 3);
    info.set_sessionkey(sk);
    std::string attachStr;
    EXPECT_TRUE(info.SerializeToString(&attachStr));
    std::string encAttach;
    EXPECT_EQ(0, curve::common::Encryptor::AESEncrypt(
        authOption_.lastKey, curve::common::ZEROIV, attachStr, &encAttach));
    GetTicketResponse response;
    response.set_status(AuthStatusCode::AUTH_OK);
    response.set_encticket(ticket);
    response.set_encticketattach(encAttach);
    EXPECT_CALL(mockAuthService_, GetTicket(_, _, _, _))
        .WillOnce(DoAll(
            SetArgPointee<2>(response),
            Invoke(FakeRpcService<GetTicketRequest, GetTicketResponse>)));
    ASSERT_TRUE(authClient_.GetToken(serverId, &token));
    ASSERT_EQ(1, authClient_.MapSize());
    ASSERT_EQ(ticket, token.encticket());
    std::string decClientStr;
    ASSERT_EQ(0, curve::common::Encryptor::AESDecrypt(sk,
        curve::common::ZEROIV, token.encclientidentity(), &decClientStr));
    ClientIdentity cInfo;
    ASSERT_TRUE(cInfo.ParseFromString(decClientStr));
    ASSERT_EQ(cInfo.cid(), authOption_.clientId);
}

}  // namespace client
}  // namespace curve
