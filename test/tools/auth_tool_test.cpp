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
 * Created Date: 2023-07-29
 * Author: wanghai (SeanHai)
*/

#include <gtest/gtest.h>
#include <brpc/server.h>
#include <memory>
#include <string>
#include "src/tools/auth_tool.h"
#include "test/tools/mock/mock_mds_client.h"

DECLARE_string(user);
DECLARE_string(role);
DECLARE_string(key);
DECLARE_string(authkey);
DECLARE_string(caps);

using ::testing::_;
using ::testing::Return;
using ::testing::SetArgPointee;
using ::testing::DoAll;

namespace curve {
namespace tool {
class AuthToolTest : public ::testing::Test {
 protected:
    AuthToolTest() {}
    void SetUp() {
        mockMdsClient_ = std::make_shared<MockMDSClient>();
        authTool_ = std::make_shared<AuthTool>(mockMdsClient_);
    }

    void TearDown() {}

    std::shared_ptr<MockMDSClient> mockMdsClient_;
    std::shared_ptr<AuthTool> authTool_;
};

TEST_F(AuthToolTest, TestAddKey) {
    std::string command = "auth-key-add";

    // 1. init mds client failed
    EXPECT_CALL(*mockMdsClient_, Init(_)).WillOnce(Return(-1));
    ASSERT_EQ(-1, authTool_->RunCommand(command));

    // 2. miss required parameter
    EXPECT_CALL(*mockMdsClient_, Init(_)).WillOnce(Return(0));
    ASSERT_EQ(-1, authTool_->RunCommand(command));

    // 3. encypt key failed
    FLAGS_user = "test";
    FLAGS_key = "1234567890abcdef";
    FLAGS_caps = "*";
    FLAGS_role = "client";
    FLAGS_authkey = "key";
    EXPECT_CALL(*mockMdsClient_, Init(_)).WillOnce(Return(0));
    ASSERT_EQ(-1, authTool_->RunCommand(command));

    // 4. add key failed
    FLAGS_authkey = "1234567890abcdef";
    EXPECT_CALL(*mockMdsClient_, Init(_)).WillOnce(Return(0));
    EXPECT_CALL(*mockMdsClient_, AddKey(_))
        .WillOnce(Return(-1));
    ASSERT_EQ(-1, authTool_->RunCommand(command));

    // 5. add key success
    EXPECT_CALL(*mockMdsClient_, Init(_)).WillOnce(Return(0));
    EXPECT_CALL(*mockMdsClient_, AddKey(_))
        .WillOnce(Return(0));
    ASSERT_EQ(0, authTool_->RunCommand(command));
}

TEST_F(AuthToolTest, TestDeleteKey) {
    std::string command = "auth-key-delete";

    // 1. miss required parameter
    EXPECT_CALL(*mockMdsClient_, Init(_)).WillRepeatedly(Return(0));
    FLAGS_user = "";
    FLAGS_authkey = "";
    ASSERT_EQ(-1, authTool_->RunCommand(command));

    FLAGS_user = "test";
    ASSERT_EQ(-1, authTool_->RunCommand(command));

    // 2. encypt key failed
    FLAGS_authkey = "key";
    ASSERT_EQ(-1, authTool_->RunCommand(command));

    // 3. delete key failed
    FLAGS_authkey = "1234567890abcdef";
    EXPECT_CALL(*mockMdsClient_, DelKey(_))
        .WillOnce(Return(-1));
    ASSERT_EQ(-1, authTool_->RunCommand(command));

    // 4. delete key success
    EXPECT_CALL(*mockMdsClient_, DelKey(_))
        .WillOnce(Return(0));
    ASSERT_EQ(0, authTool_->RunCommand(command));
}

TEST_F(AuthToolTest, TestGetKey) {
    std::string command = "auth-key-get";

    // 1. miss required parameter
    EXPECT_CALL(*mockMdsClient_, Init(_)).WillRepeatedly(Return(0));
    FLAGS_user = "";
    FLAGS_authkey = "";
    ASSERT_EQ(-1, authTool_->RunCommand(command));

    FLAGS_user = "test";
    ASSERT_EQ(-1, authTool_->RunCommand(command));

    // 2. encypt key failed
    FLAGS_authkey = "key";
    ASSERT_EQ(-1, authTool_->RunCommand(command));

    // get key failed
    FLAGS_authkey = "1234567890abcdef";
    EXPECT_CALL(*mockMdsClient_, GetKey(_, _))
        .WillOnce(Return(-1));
    ASSERT_EQ(-1, authTool_->RunCommand(command));

    // decrypt key failed
    EXPECT_CALL(*mockMdsClient_, GetKey(_, _))
        .WillOnce(DoAll(SetArgPointee<1>("key"), Return(0)));
    ASSERT_EQ(-1, authTool_->RunCommand(command));

    // get key success
    curve::mds::auth::Key key;
    key.set_id("client");
    key.set_role(curve::mds::auth::RoleType::ROLE_CLIENT);
    key.set_key("1234567890abcdef");
    key.set_caps("*");
    std::string keyStr;
    std::string encKey;
    ASSERT_TRUE(key.SerializeToString(&keyStr));
    ASSERT_EQ(0, curve::common::Encryptor::AESEncrypt(
        FLAGS_authkey, curve::common::ZEROIV, keyStr, &encKey));
    EXPECT_CALL(*mockMdsClient_, GetKey(_, _))
        .WillOnce(DoAll(SetArgPointee<1>(encKey), Return(0)));
    ASSERT_EQ(0, authTool_->RunCommand(command));
}

TEST_F(AuthToolTest, TestUpdateKey) {
    std::string command = "auth-key-update";

    // 1. miss required parameter
    EXPECT_CALL(*mockMdsClient_, Init(_)).WillRepeatedly(Return(0));
    FLAGS_user = "";
    FLAGS_authkey = "";
    FLAGS_key = "";
    ASSERT_EQ(-1, authTool_->RunCommand(command));

    FLAGS_user = "test";
    ASSERT_EQ(-1, authTool_->RunCommand(command));

    FLAGS_authkey = "1234567890abcdef";
    ASSERT_EQ(-1, authTool_->RunCommand(command));

    // get key failed
    FLAGS_key = "1234567890abcdeh";
    EXPECT_CALL(*mockMdsClient_, GetKey(_, _))
        .WillOnce(Return(-1));
    ASSERT_EQ(-1, authTool_->RunCommand(command));

    // update key failed
    curve::mds::auth::Key key;
    key.set_id("client");
    key.set_role(curve::mds::auth::RoleType::ROLE_CLIENT);
    key.set_key("1234567890abcdef");
    key.set_caps("*");
    std::string keyStr;
    std::string encKey;
    ASSERT_TRUE(key.SerializeToString(&keyStr));
    ASSERT_EQ(0, curve::common::Encryptor::AESEncrypt(
        FLAGS_authkey, curve::common::ZEROIV, keyStr, &encKey));
    EXPECT_CALL(*mockMdsClient_, GetKey(_, _))
        .WillOnce(DoAll(SetArgPointee<1>(encKey), Return(0)));
    EXPECT_CALL(*mockMdsClient_, UpdateKey(_))
        .WillOnce(Return(-1));
    ASSERT_EQ(-1, authTool_->RunCommand(command));

    // update key success
    EXPECT_CALL(*mockMdsClient_, GetKey(_, _))
        .WillOnce(DoAll(SetArgPointee<1>(encKey), Return(0)));
    EXPECT_CALL(*mockMdsClient_, UpdateKey(_))
        .WillOnce(Return(0));
    ASSERT_EQ(0, authTool_->RunCommand(command));
}

}  // namespace tool
}  // namespace curve
