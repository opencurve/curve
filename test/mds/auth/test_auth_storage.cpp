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
#include "src/mds/auth/auth_storage_etcd.h"
#include "test/mds/mock/mock_kvstorage_client.h"


namespace curve {
namespace mds {
namespace auth {

using ::testing::_;
using ::testing::Return;
using ::testing::An;
using ::testing::DoAll;
using ::testing::SetArgPointee;

class TestAuthStorage : public ::testing::Test {
 protected:
    void SetUp() override {
        kvStorage_ = std::make_shared<curve::kvstorage::MockKVStorageClient>();
        authStorage_ = std::make_shared<AuthStorageEtcd>(kvStorage_);
    }

    void TearDown() override {
        kvStorage_ = nullptr;
        authStorage_ = nullptr;
    }

    std::shared_ptr<curve::kvstorage::MockKVStorageClient> kvStorage_;
    std::shared_ptr<AuthStorageEtcd> authStorage_;
};

TEST_F(TestAuthStorage, TestLoadKeys) {
    Key key;
    key.set_id("test");
    key.set_key("test");
    key.set_role(RoleType::ROLE_CLIENT);
    key.set_caps("*");
    std::vector<std::string> fail_keys;
    fail_keys.push_back("key");
    std::vector<std::string> keys;
    keys.push_back(key.SerializeAsString());
    keys.push_back(key.SerializeAsString());
    std::unordered_map<std::string, Key> keyMap;
    EXPECT_CALL(*kvStorage_, List(_, _, An<std::vector<std::string>*>()))
        .WillOnce(Return(EtcdErrCode::EtcdKeyNotExist))
        .WillOnce(Return(EtcdErrCode::EtcdUnknown))
        .WillOnce(DoAll(SetArgPointee<2>(fail_keys),
                        Return(EtcdErrCode::EtcdOK)))
        .WillOnce(DoAll(SetArgPointee<2>(keys),
                        Return(EtcdErrCode::EtcdOK)));
    ASSERT_TRUE(authStorage_->LoadKeys(&keyMap));
    ASSERT_FALSE(authStorage_->LoadKeys(&keyMap));
    ASSERT_FALSE(authStorage_->LoadKeys(&keyMap));
    ASSERT_FALSE(authStorage_->LoadKeys(&keyMap));
}

TEST_F(TestAuthStorage, TestStorageKey) {
    Key key;
    key.set_id("test");
    key.set_key("test");
    key.set_role(RoleType::ROLE_CLIENT);
    key.set_caps("*");
    EXPECT_CALL(*kvStorage_, Put(_, _))
        .WillOnce(Return(EtcdErrCode::EtcdUnknown))
        .WillOnce(Return(EtcdErrCode::EtcdOK));
    ASSERT_FALSE(authStorage_->StorageKey(key));
    ASSERT_TRUE(authStorage_->StorageKey(key));
}

TEST_F(TestAuthStorage, TestDeleteKey) {
    EXPECT_CALL(*kvStorage_, Delete(_))
        .WillOnce(Return(EtcdErrCode::EtcdUnknown))
        .WillOnce(Return(EtcdErrCode::EtcdOK));
    ASSERT_FALSE(authStorage_->DeleteKey("id"));
    ASSERT_TRUE(authStorage_->DeleteKey("id"));
}

}  // namespace auth
}  // namespace mds
}  // namespace curve
