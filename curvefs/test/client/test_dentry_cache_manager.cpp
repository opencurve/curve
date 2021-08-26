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
 * Created Date: Thur May 27 2021
 * Author: xuchaojie
 */

#include <gtest/gtest.h>
#include <gmock/gmock.h>
#include <google/protobuf/util/message_differencer.h>

#include "curvefs/test/client/mock_metaserver_client.h"
#include "curvefs/src/client/dentry_cache_manager.h"

namespace curvefs {
namespace client {

using ::testing::Return;
using ::testing::_;
using ::testing::Contains;
using ::testing::SetArgPointee;
using ::testing::DoAll;

class TestDentryCacheManager : public ::testing::Test {
 protected:
    TestDentryCacheManager() {}
    ~TestDentryCacheManager() {}

    virtual void SetUp() {
        metaClient_ = std::make_shared<MockMetaServerClient>();
        dCacheManager_ = std::make_shared<DentryCacheManagerImpl>(metaClient_);
        dCacheOption_.maxListDentryCount = 100;
        dCacheManager_->Init(dCacheOption_);
        dCacheManager_->SetFsId(fsId_);
    }

    virtual void TearDown() {
        metaClient_ = nullptr;
        dCacheManager_ = nullptr;
    }

 protected:
    std::shared_ptr<DentryCacheManagerImpl> dCacheManager_;
    std::shared_ptr<MockMetaServerClient> metaClient_;
    uint32_t fsId_ = 888;
    DCacheOption dCacheOption_;
};

TEST_F(TestDentryCacheManager, GetDentry) {
    uint64_t parent = 99;
    uint64_t inodeid = 100;
    const std::string name = "test";
    uint64_t txId = 0;
    Dentry out;

    Dentry dentryExp;
    dentryExp.set_fsid(fsId_);
    dentryExp.set_name(name);
    dentryExp.set_parentinodeid(parent);
    dentryExp.set_inodeid(inodeid);
    dentryExp.set_txid(txId);

    EXPECT_CALL(*metaClient_, GetDentry(fsId_, parent, name, txId, _))
        .WillOnce(Return(CURVEFS_ERROR::NOTEXIST))
        .WillOnce(DoAll(SetArgPointee<4>(dentryExp),
                Return(CURVEFS_ERROR::OK)));

    CURVEFS_ERROR ret = dCacheManager_->GetDentry(parent, name, txId, &out);
    ASSERT_EQ(CURVEFS_ERROR::NOTEXIST, ret);

    ret = dCacheManager_->GetDentry(parent, name, txId, &out);
    ASSERT_EQ(CURVEFS_ERROR::OK, ret);
    ASSERT_TRUE(
        google::protobuf::util::MessageDifferencer::Equals(dentryExp, out));

    ret = dCacheManager_->GetDentry(parent, name, txId, &out);
    ASSERT_EQ(CURVEFS_ERROR::OK, ret);
    ASSERT_TRUE(
        google::protobuf::util::MessageDifferencer::Equals(dentryExp, out));
}

TEST_F(TestDentryCacheManager, CreateAndGetDentry) {
    uint64_t parent = 99;
    uint64_t inodeid = 100;
    const std::string name = "test";
    uint64_t txId = 0;
    Dentry out;

    Dentry dentryExp;
    dentryExp.set_fsid(fsId_);
    dentryExp.set_name(name);
    dentryExp.set_parentinodeid(parent);
    dentryExp.set_inodeid(inodeid);
    dentryExp.set_txid(txId);

    EXPECT_CALL(*metaClient_, CreateDentry(_))
        .WillOnce(Return(CURVEFS_ERROR::FAILED))
        .WillOnce(Return(CURVEFS_ERROR::OK));

    CURVEFS_ERROR ret = dCacheManager_->CreateDentry(dentryExp);
    ASSERT_EQ(CURVEFS_ERROR::FAILED, ret);

    ret = dCacheManager_->CreateDentry(dentryExp);
    ASSERT_EQ(CURVEFS_ERROR::OK, ret);

    ret = dCacheManager_->GetDentry(parent, name, txId, &out);
    ASSERT_EQ(CURVEFS_ERROR::OK, ret);
    ASSERT_TRUE(
        google::protobuf::util::MessageDifferencer::Equals(dentryExp, out));
}

TEST_F(TestDentryCacheManager, DeleteDentry) {
    uint64_t parent = 99;
    const std::string name = "test";
    uint64_t txId = 0;

    EXPECT_CALL(*metaClient_, DeleteDentry(fsId_, parent, name, txId))
        .WillOnce(Return(CURVEFS_ERROR::NOTEXIST))
        .WillOnce(Return(CURVEFS_ERROR::OK));

    CURVEFS_ERROR ret = dCacheManager_->DeleteDentry(parent, name, txId);
    ASSERT_EQ(CURVEFS_ERROR::NOTEXIST, ret);

    ret = dCacheManager_->DeleteDentry(parent, name, txId);
    ASSERT_EQ(CURVEFS_ERROR::OK, ret);
}

TEST_F(TestDentryCacheManager, ListDentryNomal) {
    uint64_t parent = 99;
    uint64_t txId = 0;

    std::list<Dentry> part1, part2;
    part1.resize(dCacheOption_.maxListDentryCount);
    part2.resize(dCacheOption_.maxListDentryCount - 1);

    EXPECT_CALL(*metaClient_, ListDentry(fsId_, parent, txId, _, _, _))
        .WillOnce(DoAll(SetArgPointee<5>(part1),
                Return(CURVEFS_ERROR::OK)))
        .WillOnce(DoAll(SetArgPointee<5>(part2),
                Return(CURVEFS_ERROR::OK)));

    std::list<Dentry> out;
    CURVEFS_ERROR ret = dCacheManager_->ListDentry(parent, txId, &out, 0);
    ASSERT_EQ(CURVEFS_ERROR::OK, ret);
    ASSERT_EQ(2 * dCacheOption_.maxListDentryCount - 1, out.size());
}

TEST_F(TestDentryCacheManager, ListDentryEmpty) {
    uint64_t parent = 99;
    uint64_t txId = 0;

    EXPECT_CALL(*metaClient_, ListDentry(fsId_, parent, txId, _, _, _))
        .WillOnce(Return(CURVEFS_ERROR::NOTEXIST));

    std::list<Dentry> out;
    CURVEFS_ERROR ret = dCacheManager_->ListDentry(parent, txId, &out, 0);
    ASSERT_EQ(CURVEFS_ERROR::OK, ret);
    ASSERT_EQ(0, out.size());
}

TEST_F(TestDentryCacheManager, ListDentryFailed) {
    uint64_t parent = 99;
    uint64_t txId = 0;

    EXPECT_CALL(*metaClient_, ListDentry(fsId_, parent, txId, _, _, _))
        .WillOnce(Return(CURVEFS_ERROR::FAILED));

    std::list<Dentry> out;
    CURVEFS_ERROR ret = dCacheManager_->ListDentry(parent, txId, &out, 0);
    ASSERT_EQ(CURVEFS_ERROR::FAILED, ret);
    ASSERT_EQ(0, out.size());
}

}  // namespace client
}  // namespace curvefs
