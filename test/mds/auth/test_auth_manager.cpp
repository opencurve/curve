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
 * Created Date: 2023-06-25
 * Author: wanghai (SeanHai)
 */

#include <gmock/gmock-spec-builders.h>
#include <gtest/gtest.h>
#include <memory>
#include "proto/auth.pb.h"
#include "src/mds/auth/auth_service_manager.h"
#include "test/mds/auth/mock_auth_node.h"


namespace curve {
namespace mds {
namespace auth {

using testing::_;
using testing::Return;

class TestAuthManager : public ::testing::Test {
 protected:
    TestAuthManager() = default;
    ~TestAuthManager() = default;

    virtual void SetUp() {
        mockAuthNode_ = std::make_shared<MockAuthNode>();
        authServiceManager_ = std::make_shared<AuthServiceManager>(
            mockAuthNode_);
    }

    virtual void TearDown() {
        authServiceManager_ = nullptr;
        mockAuthNode_ = nullptr;
    }

 protected:
    std::shared_ptr<MockAuthNode> mockAuthNode_;
    std::shared_ptr<AuthServiceManager> authServiceManager_;
};

TEST_F(TestAuthManager, TestAddKey) {
    AddKeyRequest request;
    AddKeyResponse successResp;
    AddKeyResponse failedResp;
    request.add_enckey("encKey1");
    request.add_enckey("encKey2");

    EXPECT_CALL(*mockAuthNode_, AddKey(_))
        .Times(3)
        .WillOnce(Return(AuthStatusCode::AUTH_OK))
        .WillOnce(Return(AuthStatusCode::AUTH_OK))
        .WillOnce(Return(AuthStatusCode::AUTH_STORE_KEY_FAILED));

    authServiceManager_->AddKey(request, &successResp);
    ASSERT_EQ(successResp.status(), AuthStatusCode::AUTH_OK);

    authServiceManager_->AddKey(request, &failedResp);
    ASSERT_EQ(failedResp.status(), AuthStatusCode::AUTH_STORE_KEY_FAILED);
}

TEST_F(TestAuthManager, TestDeleteKey) {
    DeleteKeyRequest request;
    DeleteKeyResponse successResp;
    DeleteKeyResponse failedResp;
    request.set_enckeyid("encKeyId");

    EXPECT_CALL(*mockAuthNode_, DeleteKey(_))
        .Times(2)
        .WillOnce(Return(AuthStatusCode::AUTH_OK))
        .WillOnce(Return(AuthStatusCode::AUTH_DELETE_KEY_FAILED));

    authServiceManager_->DeleteKey(request, &successResp);
    ASSERT_EQ(successResp.status(), AuthStatusCode::AUTH_OK);

    authServiceManager_->DeleteKey(request, &failedResp);
    ASSERT_EQ(failedResp.status(), AuthStatusCode::AUTH_DELETE_KEY_FAILED);
}

TEST_F(TestAuthManager, TestGetKey) {
    GetKeyRequest request;
    GetKeyResponse successResp;
    GetKeyResponse failedResp;
    request.set_enckeyid("encKeyId");

    EXPECT_CALL(*mockAuthNode_, GetKey(_, _))
        .Times(2)
        .WillOnce(Return(AuthStatusCode::AUTH_OK))
        .WillOnce(Return(AuthStatusCode::AUTH_KEY_NOT_EXIST));

    authServiceManager_->GetKey(request, &successResp);
    ASSERT_EQ(successResp.status(), AuthStatusCode::AUTH_OK);

    authServiceManager_->GetKey(request, &failedResp);
    ASSERT_EQ(failedResp.status(), AuthStatusCode::AUTH_KEY_NOT_EXIST);
}

TEST_F(TestAuthManager, TestUpdateKey) {
    UpdateKeyRequest request;
    UpdateKeyResponse successResp;
    UpdateKeyResponse failedResp;
    request.set_enckey("encKey");

    EXPECT_CALL(*mockAuthNode_, UpdateKey(_))
        .Times(2)
        .WillOnce(Return(AuthStatusCode::AUTH_OK))
        .WillOnce(Return(AuthStatusCode::AUTH_KEY_NOT_EXIST));

    authServiceManager_->UpdateKey(request, &successResp);
    ASSERT_EQ(successResp.status(), AuthStatusCode::AUTH_OK);

    authServiceManager_->UpdateKey(request, &failedResp);
    ASSERT_EQ(failedResp.status(), AuthStatusCode::AUTH_KEY_NOT_EXIST);
}

TEST_F(TestAuthManager, TestGetTicket) {
    GetTicketRequest request;
    GetTicketResponse successResp;
    GetTicketResponse failedResp;
    request.set_cid("cid");
    request.set_sid("sid");

    EXPECT_CALL(*mockAuthNode_, GetTicket(_, _, _, _))
        .Times(2)
        .WillOnce(Return(AuthStatusCode::AUTH_OK))
        .WillOnce(Return(AuthStatusCode::AUTH_KEY_NOT_EXIST));

    authServiceManager_->GetTicket(request, &successResp);
    ASSERT_EQ(successResp.status(), AuthStatusCode::AUTH_OK);

    authServiceManager_->GetTicket(request, &failedResp);
    ASSERT_EQ(failedResp.status(), AuthStatusCode::AUTH_KEY_NOT_EXIST);
}

}  // namespace auth
}  // namespace mds
}  // namespace curve
