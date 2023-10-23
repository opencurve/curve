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

#include <gmock/gmock-actions.h>
#include <gmock/gmock-more-actions.h>
#include <gmock/gmock-spec-builders.h>
#include <gtest/gtest.h>
#include <string>
#include "proto/auth.pb.h"
#include "src/common/authenticator.h"
#include "test/mds/auth/mock_auth_storage.h"
#include "src/mds/auth/authnode.h"

namespace curve {
namespace mds {
namespace auth {

using ::testing::_;
using ::testing::Return;
using ::testing::Invoke;
using ::testing::DoAll;

class TestAuthNode : public ::testing::Test {
 protected:
    void SetUp() override {
        authStorage_ = std::make_shared<MockAuthStorage>();
        authNode_ = std::make_shared<AuthNodeImpl>(authStorage_);
        authKeyStr = "1122334455667788";
        authKey_.set_id(curve::common::AUTH_ROLE);
        authKey_.set_key(authKeyStr);
        authKey_.set_role(RoleType::ROLE_SERVICE);
        authKey_.set_caps("*");

        EXPECT_CALL(*authStorage_, LoadKeys(_))
            .WillOnce(Invoke([&](std::unordered_map<std::string, Key> *keyMap){
                (*keyMap)[curve::common::AUTH_ROLE] = authKey_;
                return true;
            }));
        ASSERT_TRUE(authNode_->Init(authOption_));
    }

    void TearDown() override {
        authNode_ = nullptr;
        authStorage_ = nullptr;
    }

    std::shared_ptr<MockAuthStorage> authStorage_;
    std::shared_ptr<AuthNodeImpl> authNode_;
    Key authKey_;
    AuthOption authOption_;
    std::string authKeyStr;
};

TEST_F(TestAuthNode, TestInit) {
    // init failed
    EXPECT_CALL(*authStorage_, LoadKeys(_))
        .WillOnce(Return(false));
    ASSERT_FALSE(authNode_->Init(authOption_));

    // auth key not exist, init failed
    EXPECT_CALL(*authStorage_, StorageKey(_))
        .WillOnce(Return(false));
    EXPECT_CALL(*authStorage_, LoadKeys(_))
        .WillOnce(Invoke([&](std::unordered_map<std::string, Key> *keyMap){
            keyMap->clear();
            return true;
        }));
    ASSERT_FALSE(authNode_->Init(authOption_));

    // auth key not exist, init success
    EXPECT_CALL(*authStorage_, StorageKey(_))
        .WillOnce(Return(true));
    EXPECT_CALL(*authStorage_, LoadKeys(_))
        .WillOnce(Invoke([&](std::unordered_map<std::string, Key> *keyMap){
            keyMap->clear();
            return true;
        }));
    ASSERT_TRUE(authNode_->Init(authOption_));
}

TEST_F(TestAuthNode, TestAddKey) {
    // error key, decrypt failed
    ASSERT_EQ(AuthStatusCode::AUTH_DECRYPT_FAILED,
        authNode_->AddOrUpdateKey("key"));

    // error key, parse failed
    std::string key;
    ASSERT_EQ(0, curve::common::Encryptor::AESEncrypt(authKeyStr,
        curve::common::ZEROIV, "key", &key));
    ASSERT_EQ(AuthStatusCode::AUTH_DECRYPT_FAILED,
        authNode_->AddOrUpdateKey(key));

    // key is correct, store failed
    Key keyInfo;
    keyInfo.set_id("client");
    keyInfo.set_key("1212343456567878");
    keyInfo.set_role(RoleType::ROLE_CLIENT);
    keyInfo.set_caps("*");
    std::string keyStr;
    ASSERT_TRUE(keyInfo.SerializeToString(&keyStr));
    ASSERT_EQ(0, curve::common::Encryptor::AESEncrypt(authKeyStr,
        curve::common::ZEROIV, keyStr, &key));

    EXPECT_CALL(*authStorage_, StorageKey(_))
        .WillOnce(Return(false));
    ASSERT_EQ(AuthStatusCode::AUTH_STORE_KEY_FAILED,
        authNode_->AddOrUpdateKey(key));

    // key is correct, store success
    EXPECT_CALL(*authStorage_, StorageKey(_))
        .WillOnce(Return(true));
    ASSERT_EQ(AuthStatusCode::AUTH_OK,
        authNode_->AddOrUpdateKey(key));
}

TEST_F(TestAuthNode, TestGetKey) {
    std::string encKey;

    // error keyId, not found
    std::string keyId;
    ASSERT_EQ(0, curve::common::Encryptor::AESEncrypt(authKeyStr,
        curve::common::ZEROIV, "key", &keyId));
    ASSERT_EQ(AuthStatusCode::AUTH_KEY_NOT_EXIST,
        authNode_->GetKey(keyId, &encKey));

    // keyId is correct
    ASSERT_EQ(0, curve::common::Encryptor::AESEncrypt(authKeyStr,
        curve::common::ZEROIV, "auth", &keyId));
    ASSERT_EQ(AuthStatusCode::AUTH_OK, authNode_->GetKey(keyId, &encKey));
}

TEST_F(TestAuthNode, TestUpdateKey) {
    // error key, decrypt failed
    ASSERT_EQ(AuthStatusCode::AUTH_DECRYPT_FAILED,
        authNode_->AddOrUpdateKey("key"));

    // error key, parse failed
    std::string key;
    ASSERT_EQ(0, curve::common::Encryptor::AESEncrypt(authKeyStr,
        curve::common::ZEROIV, "key", &key));
    ASSERT_EQ(AuthStatusCode::AUTH_DECRYPT_FAILED,
        authNode_->AddOrUpdateKey(key));

    // key is correct, store failed
    Key keyInfo;
    keyInfo.set_id("auth");
    keyInfo.set_key(authKeyStr);
    keyInfo.set_role(RoleType::ROLE_SERVICE);
    keyInfo.set_caps("rwx");
    std::string keyStr;
    ASSERT_TRUE(keyInfo.SerializeToString(&keyStr));
    ASSERT_EQ(0, curve::common::Encryptor::AESEncrypt(authKeyStr,
        curve::common::ZEROIV, keyStr, &key));

    EXPECT_CALL(*authStorage_, StorageKey(_))
        .WillOnce(Return(false));
    ASSERT_EQ(AuthStatusCode::AUTH_STORE_KEY_FAILED,
        authNode_->AddOrUpdateKey(key));

    // key is correct, store success
    EXPECT_CALL(*authStorage_, StorageKey(_))
        .WillOnce(Return(true));
    ASSERT_EQ(AuthStatusCode::AUTH_OK,
        authNode_->AddOrUpdateKey(key));

    // get and compare info
    std::string keyId;
    std::string encKey;
    ASSERT_EQ(0, curve::common::Encryptor::AESEncrypt(authKeyStr,
        curve::common::ZEROIV, "auth", &keyId));
    ASSERT_EQ(AuthStatusCode::AUTH_OK, authNode_->GetKey(keyId, &encKey));
    std::string decKey;
    ASSERT_EQ(0, curve::common::Encryptor::AESDecrypt(authKeyStr,
        curve::common::ZEROIV, encKey, &decKey));
    Key keyInfo2;
    ASSERT_TRUE(keyInfo2.ParseFromString(decKey));
    ASSERT_EQ(keyInfo2.caps(), keyInfo.caps());

    // key not exist will add it
    keyInfo.set_id("test");
    ASSERT_TRUE(keyInfo.SerializeToString(&keyStr));
    ASSERT_EQ(0, curve::common::Encryptor::AESEncrypt(authKeyStr,
        curve::common::ZEROIV, keyStr, &key));
    EXPECT_CALL(*authStorage_, StorageKey(_))
        .WillOnce(Return(true));
    ASSERT_EQ(AuthStatusCode::AUTH_OK, authNode_->AddOrUpdateKey(key));
    ASSERT_EQ(0, curve::common::Encryptor::AESEncrypt(authKeyStr,
        curve::common::ZEROIV, "test", &keyId));
    ASSERT_EQ(AuthStatusCode::AUTH_OK, authNode_->GetKey(keyId, &encKey));
}

TEST_F(TestAuthNode, TestGetTicket) {
    std::string encTicket;
    std::string encAttach;
    // cid not exist
    ASSERT_EQ(AuthStatusCode::AUTH_KEY_NOT_EXIST,
        authNode_->GetTicket("cId", "sid", &encTicket, &encAttach));

    // cid exist, sid not exist
    ASSERT_EQ(AuthStatusCode::AUTH_KEY_NOT_EXIST,
        authNode_->GetTicket("auth", "sid", &encTicket, &encAttach));

    // success
    ASSERT_EQ(AuthStatusCode::AUTH_OK,
        authNode_->GetTicket("auth", "auth", &encTicket, &encAttach));
}

TEST_F(TestAuthNode, TestDeleteKey) {
    std::string keyId;
    ASSERT_EQ(0, curve::common::Encryptor::AESEncrypt(authKeyStr,
        curve::common::ZEROIV, "key", &keyId));
    EXPECT_CALL(*authStorage_, DeleteKey(_))
        .WillOnce(Return(false))
        .WillOnce(Return(true));
    ASSERT_EQ(AuthStatusCode::AUTH_DELETE_KEY_FAILED,
        authNode_->DeleteKey(keyId));
    ASSERT_EQ(AuthStatusCode::AUTH_OK,
        authNode_->DeleteKey(keyId));
}

}  // namespace auth
}  // namespace mds
}  // namespace curve
