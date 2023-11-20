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
#include <unistd.h>

#include "curvefs/test/client/rpcclient/mock_mds_client.h"
#include "curvefs/test/client/mock_metaserver_client.h"
#include "curvefs/src/client/dentry_manager.h"

namespace curvefs {
namespace client {
namespace common {
DECLARE_bool(enableCto);
}  // namespace common
}  // namespace client
}  // namespace curvefs

namespace curvefs {
namespace client {

using ::testing::Return;
using ::testing::_;
using ::testing::Contains;
using ::testing::SetArgPointee;
using ::testing::DoAll;
using ::testing::Invoke;

using rpcclient::MockMetaServerClient;
using rpcclient::MockMdsClient;

class TestDentryCacheManager : public ::testing::Test {
 protected:
    TestDentryCacheManager() {}
    ~TestDentryCacheManager() {}

    virtual void SetUp() {
        mdsClient_ = std::make_shared<MockMdsClient>();
        metaClient_ = std::make_shared<MockMetaServerClient>();
        dCacheManager_ = std::make_shared<DentryCacheManagerImpl>(metaClient_);
        dCacheManager_->Init(mdsClient_);
        dCacheManager_->SetFsId(fsId_);
    }

    virtual void TearDown() {
        metaClient_ = nullptr;
        dCacheManager_ = nullptr;
    }

 protected:
    std::shared_ptr<DentryCacheManagerImpl> dCacheManager_;
    std::shared_ptr<MockMetaServerClient> metaClient_;
    std::shared_ptr<MockMdsClient> mdsClient_;
    uint32_t fsId_ = 888;
    uint32_t timeout_ = 3;
};

TEST_F(TestDentryCacheManager, CheckAndResolveTx) {
    // In
    std::string primaryKey = "3:1:1:A";
    std::string fakePrimaryKey = "ABC";
    uint64_t startTs = 1;
    uint64_t commitTs = 2;
    uint64_t curTimestamp = 100;
    Dentry dentry;
    TxLock txLock;
    txLock.set_startts(startTs);
    // 1. check tx status failed
    // case: check tx status parse primary key failed
    txLock.set_primarykey(fakePrimaryKey);
    ASSERT_EQ(MetaStatusCode::PARSE_FROM_STRING_FAILED,
        dCacheManager_->CheckAndResolveTx(
            dentry, txLock, curTimestamp, commitTs));
    // case: check failed
    txLock.set_primarykey(primaryKey);
    EXPECT_CALL(*metaClient_, CheckTxStatus(_, _, _, _, _))
        .WillOnce(Return(MetaStatusCode::TX_MISMATCH));
    ASSERT_EQ(MetaStatusCode::TX_MISMATCH,
        dCacheManager_->CheckAndResolveTx(
            dentry, txLock, curTimestamp, commitTs));

    // 2. check tx status success and resolve tx failed
    EXPECT_CALL(*metaClient_, CheckTxStatus(_, _, _, _, _))
        .WillOnce(Return(MetaStatusCode::TX_COMMITTED));
    EXPECT_CALL(*metaClient_, ResolveTxLock(_, startTs, commitTs))
        .WillOnce(Return(MetaStatusCode::STORAGE_INTERNAL_ERROR));
    ASSERT_EQ(MetaStatusCode::STORAGE_INTERNAL_ERROR,
        dCacheManager_->CheckAndResolveTx(
            dentry, txLock, curTimestamp, commitTs));

    EXPECT_CALL(*metaClient_, CheckTxStatus(_, _, _, _, _))
        .WillOnce(Return(MetaStatusCode::TX_ROLLBACKED));
    EXPECT_CALL(*metaClient_, ResolveTxLock(_, startTs, 0))
        .WillOnce(Return(MetaStatusCode::STORAGE_INTERNAL_ERROR));
    ASSERT_EQ(MetaStatusCode::STORAGE_INTERNAL_ERROR,
        dCacheManager_->CheckAndResolveTx(
            dentry, txLock, curTimestamp, commitTs));

    EXPECT_CALL(*metaClient_, CheckTxStatus(_, _, _, _, _))
        .WillOnce(Return(MetaStatusCode::TX_TIMEOUT));
    EXPECT_CALL(*metaClient_, ResolveTxLock(_, startTs, 0))
        .WillOnce(Return(MetaStatusCode::STORAGE_INTERNAL_ERROR));
    ASSERT_EQ(MetaStatusCode::STORAGE_INTERNAL_ERROR,
        dCacheManager_->CheckAndResolveTx(
            dentry, txLock, curTimestamp, commitTs));

    EXPECT_CALL(*metaClient_, CheckTxStatus(_, _, _, _, _))
        .WillOnce(Return(MetaStatusCode::TX_INPROGRESS));
    ASSERT_EQ(MetaStatusCode::TX_INPROGRESS,
        dCacheManager_->CheckAndResolveTx(
            dentry, txLock, curTimestamp, commitTs));

    EXPECT_CALL(*metaClient_, CheckTxStatus(_, _, _, _, _))
        .WillOnce(Return(MetaStatusCode::TX_FAILED));
    ASSERT_EQ(MetaStatusCode::TX_FAILED,
        dCacheManager_->CheckAndResolveTx(
            dentry, txLock, curTimestamp, commitTs));

    // 3. check tx status success and resolve tx success
    EXPECT_CALL(*metaClient_, CheckTxStatus(_, _, _, _, _))
        .WillOnce(Return(MetaStatusCode::TX_COMMITTED));
    EXPECT_CALL(*metaClient_, ResolveTxLock(_, startTs, commitTs))
        .WillOnce(Return(MetaStatusCode::OK));
    ASSERT_EQ(MetaStatusCode::OK,
        dCacheManager_->CheckAndResolveTx(
            dentry, txLock, curTimestamp, commitTs));
}

TEST_F(TestDentryCacheManager, GetDentry) {
    curvefs::client::common::FLAGS_enableCto = false;
    uint64_t parent = 99;
    uint64_t inodeid = 100;
    const std::string name = "test";
    Dentry out;

    Dentry dentryExp;
    dentryExp.set_fsid(fsId_);
    dentryExp.set_name(name);
    dentryExp.set_parentinodeid(parent);
    dentryExp.set_inodeid(inodeid);

    EXPECT_CALL(*metaClient_, GetDentry(fsId_, parent, name, _, _))
        .WillOnce(Return(MetaStatusCode::NOT_FOUND))
        .WillOnce(DoAll(SetArgPointee<3>(dentryExp),
                Return(MetaStatusCode::OK)))
        .WillOnce(DoAll(SetArgPointee<3>(dentryExp),
                Return(MetaStatusCode::OK)));

    CURVEFS_ERROR ret = dCacheManager_->GetDentry(parent, name, &out);
    ASSERT_EQ(CURVEFS_ERROR::NOT_EXIST, ret);

    ret = dCacheManager_->GetDentry(parent, name, &out);
    ASSERT_EQ(CURVEFS_ERROR::OK, ret);
    ASSERT_TRUE(
        google::protobuf::util::MessageDifferencer::Equals(dentryExp, out));

    ret = dCacheManager_->GetDentry(parent, name, &out);
    ASSERT_EQ(CURVEFS_ERROR::OK, ret);
    ASSERT_TRUE(
        google::protobuf::util::MessageDifferencer::Equals(dentryExp, out));

    curvefs::client::common::FLAGS_enableCto = true;
    EXPECT_CALL(*metaClient_, DeleteDentry(
        fsId_, parent, name, FsFileType::TYPE_FILE, _))
        .WillOnce(Return(MetaStatusCode::OK));
    dCacheManager_->DeleteDentry(parent, name, FsFileType::TYPE_FILE);
    EXPECT_CALL(*metaClient_, GetDentry(fsId_, parent, name, _, _))
        .WillOnce(
            DoAll(SetArgPointee<3>(dentryExp), Return(MetaStatusCode::OK)));
    ret = dCacheManager_->GetDentry(parent, name, &out);
    ASSERT_EQ(CURVEFS_ERROR::OK, ret);
    ASSERT_TRUE(
        google::protobuf::util::MessageDifferencer::Equals(dentryExp, out));

    // get dentry but dentry tx key is locked
    // 1. Tso failed
    EXPECT_CALL(*metaClient_, GetDentry(fsId_, parent, name, _, _))
        .WillOnce(Return(MetaStatusCode::TX_KEY_LOCKED));
    EXPECT_CALL(*mdsClient_, Tso(_, _))
        .WillOnce(Return(FSStatusCode::UNKNOWN_ERROR));
    ret = dCacheManager_->GetDentry(parent, name, &out);
    ASSERT_EQ(CURVEFS_ERROR::INTERNAL, ret);
    // 2. CheckAndResolveTx failed
    EXPECT_CALL(*metaClient_, GetDentry(fsId_, parent, name, _, _))
        .WillOnce(Return(MetaStatusCode::TX_KEY_LOCKED));
    EXPECT_CALL(*mdsClient_, Tso(_, _))
        .WillOnce(Return(FSStatusCode::OK));
    ret = dCacheManager_->GetDentry(parent, name, &out);
    ASSERT_EQ(CURVEFS_ERROR::INTERNAL, ret);
    // 3. success
    TxLock txLock;
    txLock.set_primarykey("3:1:1:A");
    txLock.set_startts(1);
    EXPECT_CALL(*metaClient_, GetDentry(fsId_, parent, name, _, _))
        .WillOnce(DoAll(SetArgPointee<4>(txLock),
            Return(MetaStatusCode::TX_KEY_LOCKED)))
        .WillOnce(DoAll(SetArgPointee<3>(dentryExp),
            Return(MetaStatusCode::OK)));
    EXPECT_CALL(*mdsClient_, Tso(_, _))
        .WillOnce(Return(FSStatusCode::OK));
    EXPECT_CALL(*metaClient_, CheckTxStatus(_, _, _, _, _))
        .WillOnce(Return(MetaStatusCode::TX_COMMITTED));
    EXPECT_CALL(*metaClient_, ResolveTxLock(_, _, _))
        .WillOnce(Return(MetaStatusCode::OK));
    ret = dCacheManager_->GetDentry(parent, name, &out);
    ASSERT_EQ(CURVEFS_ERROR::OK, ret);
    ASSERT_TRUE(
        google::protobuf::util::MessageDifferencer::Equals(dentryExp, out));
}

TEST_F(TestDentryCacheManager, CreateAndGetDentry) {
    curvefs::client::common::FLAGS_enableCto = false;
    uint64_t parent = 99;
    uint64_t inodeid = 100;
    const std::string name = "test";
    Dentry out;

    Dentry dentryExp;
    dentryExp.set_fsid(fsId_);
    dentryExp.set_name(name);
    dentryExp.set_parentinodeid(parent);
    dentryExp.set_inodeid(inodeid);

    EXPECT_CALL(*metaClient_, CreateDentry(_, _))
        .WillOnce(Return(MetaStatusCode::UNKNOWN_ERROR))
        .WillOnce(Return(MetaStatusCode::OK));

    EXPECT_CALL(*metaClient_, GetDentry(fsId_, parent, name, _, _))
        .WillOnce(DoAll(SetArgPointee<3>(dentryExp),
                        Return(MetaStatusCode::OK)));

    CURVEFS_ERROR ret = dCacheManager_->CreateDentry(dentryExp);
    ASSERT_EQ(CURVEFS_ERROR::UNKNOWN, ret);

    ret = dCacheManager_->CreateDentry(dentryExp);
    ASSERT_EQ(CURVEFS_ERROR::OK, ret);

    ret = dCacheManager_->GetDentry(parent, name, &out);
    ASSERT_EQ(CURVEFS_ERROR::OK, ret);
    ASSERT_TRUE(
        google::protobuf::util::MessageDifferencer::Equals(dentryExp, out));

    curvefs::client::common::FLAGS_enableCto = true;
    EXPECT_CALL(*metaClient_, DeleteDentry(
        fsId_, parent, name, FsFileType::TYPE_FILE, _))
        .WillOnce(Return(MetaStatusCode::OK));
    dCacheManager_->DeleteDentry(parent, name, FsFileType::TYPE_FILE);
    EXPECT_CALL(*metaClient_, CreateDentry(_, _))
        .WillOnce(Return(MetaStatusCode::OK));
    EXPECT_CALL(*metaClient_, GetDentry(fsId_, parent, name, _, _))
        .WillOnce(
            DoAll(SetArgPointee<3>(dentryExp), Return(MetaStatusCode::OK)));

    ret = dCacheManager_->CreateDentry(dentryExp);
    ASSERT_EQ(CURVEFS_ERROR::OK, ret);
    ret = dCacheManager_->GetDentry(parent, name, &out);
    ASSERT_EQ(CURVEFS_ERROR::OK, ret);
    ASSERT_TRUE(
        google::protobuf::util::MessageDifferencer::Equals(dentryExp, out));

    // create dentry but dentry tx key is locked
    // 1. Tso failed
    EXPECT_CALL(*metaClient_, CreateDentry(_, _))
        .WillOnce(Return(MetaStatusCode::TX_KEY_LOCKED));
    EXPECT_CALL(*mdsClient_, Tso(_, _))
        .WillOnce(Return(FSStatusCode::UNKNOWN_ERROR));
    ret = dCacheManager_->CreateDentry(dentryExp);
    ASSERT_EQ(CURVEFS_ERROR::INTERNAL, ret);
    // 2. CheckAndResolveTx failed
    EXPECT_CALL(*metaClient_, CreateDentry(_, _))
        .WillOnce(Return(MetaStatusCode::TX_KEY_LOCKED));
    EXPECT_CALL(*mdsClient_, Tso(_, _))
        .WillOnce(Return(FSStatusCode::OK));
    ret = dCacheManager_->CreateDentry(dentryExp);
    ASSERT_EQ(CURVEFS_ERROR::INTERNAL, ret);
    // 3. success
    TxLock txLock;
    txLock.set_primarykey("3:1:1:A");
    txLock.set_startts(1);
    EXPECT_CALL(*metaClient_, CreateDentry(_, _))
        .WillOnce(DoAll(SetArgPointee<1>(txLock),
            Return(MetaStatusCode::TX_KEY_LOCKED)))
        .WillOnce(Return(MetaStatusCode::OK));
    EXPECT_CALL(*mdsClient_, Tso(_, _))
        .WillOnce(Return(FSStatusCode::OK));
    EXPECT_CALL(*metaClient_, CheckTxStatus(_, _, _, _, _))
        .WillOnce(Return(MetaStatusCode::TX_COMMITTED));
    EXPECT_CALL(*metaClient_, ResolveTxLock(_, _, _))
        .WillOnce(Return(MetaStatusCode::OK));
    ret = dCacheManager_->CreateDentry(dentryExp);
    ASSERT_EQ(CURVEFS_ERROR::OK, ret);
}

TEST_F(TestDentryCacheManager, DeleteDentry) {
    uint64_t parent = 99;
    const std::string name = "test";

    EXPECT_CALL(*metaClient_, DeleteDentry(
        fsId_, parent, name, FsFileType::TYPE_FILE, _))
        .WillOnce(Return(MetaStatusCode::NOT_FOUND))
        .WillOnce(Return(MetaStatusCode::OK));

    CURVEFS_ERROR ret = dCacheManager_->DeleteDentry(
        parent, name, FsFileType::TYPE_FILE);
    ASSERT_EQ(CURVEFS_ERROR::OK, ret);

    ret = dCacheManager_->DeleteDentry(parent, name, FsFileType::TYPE_FILE);
    ASSERT_EQ(CURVEFS_ERROR::OK, ret);

    // create dentry but dentry tx key is locked
    // 1. Tso failed
    EXPECT_CALL(*metaClient_, DeleteDentry(
        fsId_, parent, name, FsFileType::TYPE_FILE, _))
        .WillOnce(Return(MetaStatusCode::TX_KEY_LOCKED));
    EXPECT_CALL(*mdsClient_, Tso(_, _))
        .WillOnce(Return(FSStatusCode::UNKNOWN_ERROR));
    ret = dCacheManager_->DeleteDentry(parent, name, FsFileType::TYPE_FILE);
    ASSERT_EQ(CURVEFS_ERROR::INTERNAL, ret);
    // 2. CheckAndResolveTx failed
    EXPECT_CALL(*metaClient_, DeleteDentry(
        fsId_, parent, name, FsFileType::TYPE_FILE, _))
        .WillOnce(Return(MetaStatusCode::TX_KEY_LOCKED));
    EXPECT_CALL(*mdsClient_, Tso(_, _))
        .WillOnce(Return(FSStatusCode::OK));
    ret = dCacheManager_->DeleteDentry(parent, name, FsFileType::TYPE_FILE);
    ASSERT_EQ(CURVEFS_ERROR::INTERNAL, ret);
    // 3. success
    TxLock txLock;
    txLock.set_primarykey("3:1:1:A");
    txLock.set_startts(1);
    EXPECT_CALL(*metaClient_, DeleteDentry(
        fsId_, parent, name, FsFileType::TYPE_FILE, _))
        .WillOnce(DoAll(SetArgPointee<4>(txLock),
            Return(MetaStatusCode::TX_KEY_LOCKED)))
        .WillOnce(Return(MetaStatusCode::OK));
    EXPECT_CALL(*mdsClient_, Tso(_, _))
        .WillOnce(Return(FSStatusCode::OK));
    EXPECT_CALL(*metaClient_, CheckTxStatus(_, _, _, _, _))
        .WillOnce(Return(MetaStatusCode::TX_COMMITTED));
    EXPECT_CALL(*metaClient_, ResolveTxLock(_, _, _))
        .WillOnce(Return(MetaStatusCode::OK));
    ret = dCacheManager_->DeleteDentry(parent, name, FsFileType::TYPE_FILE);
    ASSERT_EQ(CURVEFS_ERROR::OK, ret);
}

TEST_F(TestDentryCacheManager, ListDentryNomal) {
    uint64_t parent = 99;

    std::list<Dentry> part1, part2;
    uint32_t limit = 100;
    part1.resize(limit);
    part2.resize(limit - 1);

    EXPECT_CALL(*metaClient_, ListDentry(fsId_, parent, _, _, _, _, _))
        .WillOnce(DoAll(SetArgPointee<5>(part1),
                Return(MetaStatusCode::OK)))
        .WillOnce(DoAll(SetArgPointee<5>(part2),
                Return(MetaStatusCode::OK)));

    std::list<Dentry> out;
    CURVEFS_ERROR ret = dCacheManager_->ListDentry(parent, &out, limit);
    ASSERT_EQ(CURVEFS_ERROR::OK, ret);
    ASSERT_EQ(2 * limit - 1, out.size());
}

TEST_F(TestDentryCacheManager, ListDentryEmpty) {
    uint64_t parent = 99;

    EXPECT_CALL(*metaClient_, ListDentry(fsId_, parent, _, _, _, _, _))
        .WillOnce(Return(MetaStatusCode::OK));

    std::list<Dentry> out;
    CURVEFS_ERROR ret = dCacheManager_->ListDentry(parent, &out, 1);
    ASSERT_EQ(CURVEFS_ERROR::OK, ret);
    ASSERT_EQ(0, out.size());
}

TEST_F(TestDentryCacheManager, ListDentryOnlyDir) {
    uint64_t parent = 99;
    std::list<Dentry> out;
    CURVEFS_ERROR ret = dCacheManager_->ListDentry(parent, &out, 0, 1, 2);
    ASSERT_EQ(CURVEFS_ERROR::OK, ret);
    ASSERT_EQ(0, out.size());
}

TEST_F(TestDentryCacheManager, ListDentryFailed) {
    uint64_t parent = 99;

    EXPECT_CALL(*metaClient_, ListDentry(fsId_, parent, _, _, _, _, _))
        .WillOnce(Return(MetaStatusCode::UNKNOWN_ERROR));

    std::list<Dentry> out;
    CURVEFS_ERROR ret = dCacheManager_->ListDentry(parent, &out, 0);
    ASSERT_EQ(CURVEFS_ERROR::UNKNOWN, ret);
    ASSERT_EQ(0, out.size());
}

TEST_F(TestDentryCacheManager, ListDentry_txLocked) {
    uint64_t parent = 99;
    std::list<Dentry> out;
    std::list<Dentry> part;
    // 1. Tso failed
    EXPECT_CALL(*metaClient_, ListDentry(fsId_, parent, _, _, _, _, _))
        .WillOnce(Return(MetaStatusCode::TX_KEY_LOCKED));
    EXPECT_CALL(*mdsClient_, Tso(_, _))
        .WillOnce(Return(FSStatusCode::UNKNOWN_ERROR));
    CURVEFS_ERROR ret = dCacheManager_->ListDentry(parent, &out, 100);
    ASSERT_EQ(CURVEFS_ERROR::INTERNAL, ret);
    // 2. tx key locked but part empty
    EXPECT_CALL(*metaClient_, ListDentry(fsId_, parent, _, _, _, _, _))
        .WillOnce(Return(MetaStatusCode::TX_KEY_LOCKED));
    EXPECT_CALL(*mdsClient_, Tso(_, _))
        .WillOnce(Return(FSStatusCode::OK));
    ret = dCacheManager_->ListDentry(parent, &out, 100);
    ASSERT_EQ(CURVEFS_ERROR::INTERNAL, ret);
    // 3. CheckAndResolveTx failed
    EXPECT_CALL(*metaClient_, ListDentry(fsId_, parent, _, _, _, _, _))
        .WillOnce(DoAll(SetArgPointee<5>(part),
            Return(MetaStatusCode::TX_KEY_LOCKED)));
    EXPECT_CALL(*mdsClient_, Tso(_, _))
        .WillOnce(Return(FSStatusCode::OK));
    Dentry dentry;
    dentry.set_fsid(fsId_);
    dentry.set_name("test");
    dentry.set_parentinodeid(parent);
    dentry.set_inodeid(100);
    part.emplace_back(dentry);
    ret = dCacheManager_->ListDentry(parent, &out, 100);
    ASSERT_EQ(CURVEFS_ERROR::INTERNAL, ret);
    // 4. success
    TxLock txLock;
    txLock.set_primarykey("3:1:1:A");
    txLock.set_startts(1);
    EXPECT_CALL(*metaClient_, ListDentry(fsId_, parent, _, _, _, _, _))
        .WillOnce(DoAll(SetArgPointee<5>(part), SetArgPointee<6>(txLock),
            Return(MetaStatusCode::TX_KEY_LOCKED)))
        .WillOnce(DoAll(SetArgPointee<5>(part),
            Return(MetaStatusCode::OK)));
    EXPECT_CALL(*mdsClient_, Tso(_, _))
        .WillOnce(Return(FSStatusCode::OK));
    EXPECT_CALL(*metaClient_, CheckTxStatus(_, _, _, _, _))
        .WillOnce(Return(MetaStatusCode::TX_COMMITTED));
    EXPECT_CALL(*metaClient_, ResolveTxLock(_, _, _))
        .WillOnce(Return(MetaStatusCode::OK));
    ret = dCacheManager_->ListDentry(parent, &out, 100);
    ASSERT_EQ(CURVEFS_ERROR::OK, ret);
}

TEST_F(TestDentryCacheManager, GetTimeOutDentry) {
    curvefs::client::common::FLAGS_enableCto = false;
    uint64_t parent = 99;
    uint64_t inodeid = 100;
    const std::string name = "test";
    Dentry out;

    Dentry dentryExp;
    dentryExp.set_fsid(fsId_);
    dentryExp.set_name(name);
    dentryExp.set_parentinodeid(parent);
    dentryExp.set_inodeid(inodeid);

    EXPECT_CALL(*metaClient_, CreateDentry(_, _))
        .WillOnce(Return(MetaStatusCode::OK));

    EXPECT_CALL(*metaClient_, GetDentry(fsId_, parent, name, _, _))
        .WillOnce(DoAll(SetArgPointee<3>(dentryExp),
                        Return(MetaStatusCode::OK)));

    auto ret = dCacheManager_->CreateDentry(dentryExp);
    ASSERT_EQ(CURVEFS_ERROR::OK, ret);

    // get form dcache directly
    ret = dCacheManager_->GetDentry(parent, name, &out);
    ASSERT_EQ(CURVEFS_ERROR::OK, ret);
    ASSERT_TRUE(
        google::protobuf::util::MessageDifferencer::Equals(dentryExp, out));

    // get from metaserver when timeout
    sleep(timeout_);
    EXPECT_CALL(*metaClient_, GetDentry(fsId_, parent, name, _, _))
        .WillOnce(Return(MetaStatusCode::OK));
    ret = dCacheManager_->GetDentry(parent, name, &out);
    ASSERT_EQ(CURVEFS_ERROR::OK, ret);
}

}  // namespace client
}  // namespace curvefs
