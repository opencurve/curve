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
 * Created Date: 2023-06-21
 * Author: wanghai (SeanHai)
 */

#include <brpc/channel.h>
#include <brpc/server.h>
#include <gtest/gtest.h>
#include "proto/auth.pb.h"
#include "src/mds/auth/auth_service.h"
#include "test/mds/auth/mock_auth_manager.h"

namespace curve {
namespace mds {
namespace auth {

using testing::_;
using testing::SetArgPointee;

class TestAuthService : public ::testing::Test {
 protected:
    TestAuthService() = default;
    ~TestAuthService() = default;

    virtual void SetUp() {
        server_ = new brpc::Server();
        mockManager_ = std::make_shared<MockAuthManger>();
        AuthServiceImpl *authService = new AuthServiceImpl(mockManager_);
        ASSERT_EQ(0, server_->AddService(authService,
                                         brpc::SERVER_OWNS_SERVICE));

        ASSERT_EQ(0, server_->Start("127.0.0.1", {18900, 18999}, nullptr));
        listenAddr_ = server_->listen_address();
    }

    virtual void TearDown() {
        server_->Stop(0);
        server_->Join();
        delete server_;
        server_ = nullptr;
        mockManager_ = nullptr;
    }

 protected:
    std::shared_ptr<MockAuthManger> mockManager_;
    brpc::Server *server_;
    butil::EndPoint listenAddr_;
};

TEST_F(TestAuthService, TestAddKey) {
    brpc::Channel channel;
    ASSERT_EQ(0, channel.Init(listenAddr_, NULL));

    AuthService_Stub stub(&channel);
    brpc::Controller cntl;

    AddKeyRequest request;
    AddKeyResponse successResp;
    AddKeyResponse failedResp;
    request.add_enckey("key");
    successResp.set_status(AuthStatusCode::AUTH_OK);
    failedResp.set_status(AuthStatusCode::AUTH_STORE_KEY_FAILED);
    EXPECT_CALL(*mockManager_, AddKey(_, _))
        .Times(2)
        .WillOnce(SetArgPointee<1>(successResp))
        .WillOnce(SetArgPointee<1>(failedResp));

    stub.AddKey(&cntl, &request, &successResp, nullptr);
    ASSERT_FALSE(cntl.Failed());
    EXPECT_EQ(AuthStatusCode::AUTH_OK, successResp.status());

    cntl.Reset();
    stub.AddKey(&cntl, &request, &failedResp, nullptr);
    ASSERT_FALSE(cntl.Failed());
    EXPECT_EQ(AuthStatusCode::AUTH_STORE_KEY_FAILED, failedResp.status());
}

TEST_F(TestAuthService, TestDeleteKey) {
    brpc::Channel channel;
    ASSERT_EQ(0, channel.Init(listenAddr_, NULL));

    AuthService_Stub stub(&channel);
    brpc::Controller cntl;

    DeleteKeyRequest request;
    DeleteKeyResponse successResp;
    DeleteKeyResponse failedResp;
    request.set_enckeyid("keyId");
    successResp.set_status(AuthStatusCode::AUTH_OK);
    failedResp.set_status(AuthStatusCode::AUTH_STORE_KEY_FAILED);
    EXPECT_CALL(*mockManager_, DeleteKey(_, _))
        .Times(2)
        .WillOnce(SetArgPointee<1>(successResp))
        .WillOnce(SetArgPointee<1>(failedResp));

    stub.DeleteKey(&cntl, &request, &successResp, nullptr);
    ASSERT_FALSE(cntl.Failed());
    EXPECT_EQ(AuthStatusCode::AUTH_OK, successResp.status());

    cntl.Reset();
    stub.DeleteKey(&cntl, &request, &failedResp, nullptr);
    ASSERT_FALSE(cntl.Failed());
    EXPECT_EQ(AuthStatusCode::AUTH_STORE_KEY_FAILED, failedResp.status());
}

TEST_F(TestAuthService, TestGetKey) {
    brpc::Channel channel;
    ASSERT_EQ(0, channel.Init(listenAddr_, NULL));

    AuthService_Stub stub(&channel);
    brpc::Controller cntl;

    GetKeyRequest request;
    GetKeyResponse successResp;
    GetKeyResponse failedResp;
    request.set_enckeyid("keyId");
    successResp.set_status(AuthStatusCode::AUTH_OK);
    failedResp.set_status(AuthStatusCode::AUTH_KEY_NOT_EXIST);
    EXPECT_CALL(*mockManager_, GetKey(_, _))
        .Times(2)
        .WillOnce(SetArgPointee<1>(successResp))
        .WillOnce(SetArgPointee<1>(failedResp));

    stub.GetKey(&cntl, &request, &successResp, nullptr);
    ASSERT_FALSE(cntl.Failed());
    EXPECT_EQ(AuthStatusCode::AUTH_OK, successResp.status());

    cntl.Reset();
    stub.GetKey(&cntl, &request, &failedResp, nullptr);
    ASSERT_FALSE(cntl.Failed());
    EXPECT_EQ(AuthStatusCode::AUTH_KEY_NOT_EXIST, failedResp.status());
}

TEST_F(TestAuthService, TestUpdateKey) {
    brpc::Channel channel;
    ASSERT_EQ(0, channel.Init(listenAddr_, NULL));

    AuthService_Stub stub(&channel);
    brpc::Controller cntl;

    UpdateKeyRequest request;
    UpdateKeyResponse successResp;
    UpdateKeyResponse failedResp;
    request.set_enckey("key");
    successResp.set_status(AuthStatusCode::AUTH_OK);
    failedResp.set_status(AuthStatusCode::AUTH_KEY_NOT_EXIST);
    EXPECT_CALL(*mockManager_, UpdateKey(_, _))
        .Times(2)
        .WillOnce(SetArgPointee<1>(successResp))
        .WillOnce(SetArgPointee<1>(failedResp));

    stub.UpdateKey(&cntl, &request, &successResp, nullptr);
    ASSERT_FALSE(cntl.Failed());
    EXPECT_EQ(AuthStatusCode::AUTH_OK, successResp.status());

    cntl.Reset();
    stub.UpdateKey(&cntl, &request, &failedResp, nullptr);
    ASSERT_FALSE(cntl.Failed());
    EXPECT_EQ(AuthStatusCode::AUTH_KEY_NOT_EXIST, failedResp.status());
}

TEST_F(TestAuthService, TestGetTicket) {
    brpc::Channel channel;
    ASSERT_EQ(0, channel.Init(listenAddr_, NULL));

    AuthService_Stub stub(&channel);
    brpc::Controller cntl;

    GetTicketRequest request;
    GetTicketResponse successResp;
    GetTicketResponse failedResp;
    request.set_cid("cid");
    request.set_sid("sid");
    successResp.set_status(AuthStatusCode::AUTH_OK);
    failedResp.set_status(AuthStatusCode::AUTH_KEY_NOT_EXIST);
    EXPECT_CALL(*mockManager_, GetTicket(_, _))
        .Times(2)
        .WillOnce(SetArgPointee<1>(successResp))
        .WillOnce(SetArgPointee<1>(failedResp));

    stub.GetTicket(&cntl, &request, &successResp, nullptr);
    ASSERT_FALSE(cntl.Failed());
    EXPECT_EQ(AuthStatusCode::AUTH_OK, successResp.status());

    cntl.Reset();
    stub.GetTicket(&cntl, &request, &failedResp, nullptr);
    ASSERT_FALSE(cntl.Failed());
    EXPECT_EQ(AuthStatusCode::AUTH_KEY_NOT_EXIST, failedResp.status());
}

}  // namespace auth
}  // namespace mds
}  // namespace curve
