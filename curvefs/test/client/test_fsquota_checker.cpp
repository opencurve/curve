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

#include <gtest/gtest.h>

#include "curvefs/src/client/fsquota_checker.h"
#include "curvefs/src/client/fsused_updater.h"
#include "curvefs/test/client/mock_metaserver_client.h"
#include "rpcclient/mock_mds_client.h"

namespace curvefs {
namespace client {

using curvefs::client::rpcclient::MockMdsClient;
using curvefs::client::rpcclient::MockMetaServerClient;
using testing::Matcher;

class FsQuotaCheckerTest : public testing::Test {
 protected:
    virtual void SetUp() {
        FsQuotaChecker::GetInstance().Init(1, mockMdsClient_,
                                           mockMetaServerClient_);
        FsUsedUpdater::GetInstance().Init(1, mockMetaServerClient_);
        mockMdsClient_ = std::make_shared<MockMdsClient>();
        mockMetaServerClient_ = std::make_shared<MockMetaServerClient>();
    }
    virtual void TearDown() {}

    std::shared_ptr<MockMdsClient> mockMdsClient_;
    std::shared_ptr<MockMetaServerClient> mockMetaServerClient_;
};

TEST_F(FsQuotaCheckerTest, QuotaBytesCheck) {
    // case 0: fsused bytes correct
    FsUsedUpdater::GetInstance().UpdateDeltaBytes(100);
    ASSERT_EQ(FsUsedUpdater::GetInstance().GetDeltaBytes(), 100);
    FsUsedUpdater::GetInstance().UpdateDeltaBytes(-100);
    ASSERT_EQ(FsUsedUpdater::GetInstance().GetDeltaBytes(), 0);

    // case 1: quota disabled
    EXPECT_CALL(*mockMdsClient_, GetFsInfo(Matcher<uint32_t>(_), _))
        .WillOnce(testing::Invoke([](uint32_t fsId, FsInfo* fsInfo) {
            fsInfo->set_capacity(0);
            return FSStatusCode::OK;
        }));
    EXPECT_CALL(*mockMetaServerClient_, GetInode(_, _, _, _))
        .WillOnce(testing::Invoke(
            [](uint32_t fsId, uint64_t inodeId, Inode* inode, bool* streaming) {
                auto xattrs = inode->mutable_xattr();
                (*xattrs)[curvefs::XATTR_FS_BYTES] = "0";
                return MetaStatusCode::OK;
            }));
    FsQuotaChecker::GetInstance().UpdateQuotaCache();
    ASSERT_TRUE(FsQuotaChecker::GetInstance().QuotaBytesCheck(100));

    // case 2: incBytes > capacity
    EXPECT_CALL(*mockMdsClient_, GetFsInfo(Matcher<uint32_t>(_), _))
        .WillOnce(testing::Invoke([](uint32_t fsId, FsInfo* fsInfo) {
            fsInfo->set_capacity(1);
            return FSStatusCode::OK;
        }));
    EXPECT_CALL(*mockMetaServerClient_, GetInode(_, _, _, _))
        .WillOnce(testing::Invoke(
            [](uint32_t fsId, uint64_t inodeId, Inode* inode, bool* streaming) {
                auto xattrs = inode->mutable_xattr();
                (*xattrs)[curvefs::XATTR_FS_BYTES] = "0";
                return MetaStatusCode::OK;
            }));
    FsQuotaChecker::GetInstance().UpdateQuotaCache();
    ASSERT_FALSE(FsQuotaChecker::GetInstance().QuotaBytesCheck(2));

    // case 3: capacity - usedBytes < incBytes
    EXPECT_CALL(*mockMdsClient_, GetFsInfo(Matcher<uint32_t>(_), _))
        .WillOnce(testing::Invoke([](uint32_t fsId, FsInfo* fsInfo) {
            fsInfo->set_capacity(3);
            return FSStatusCode::OK;
        }));
    EXPECT_CALL(*mockMetaServerClient_, GetInode(_, _, _, _))
        .WillOnce(testing::Invoke(
            [](uint32_t fsId, uint64_t inodeId, Inode* inode, bool* streaming) {
                auto xattrs = inode->mutable_xattr();
                (*xattrs)[curvefs::XATTR_FS_BYTES] = "1";
                return MetaStatusCode::OK;
            }));
    FsQuotaChecker::GetInstance().UpdateQuotaCache();
    ASSERT_FALSE(FsQuotaChecker::GetInstance().QuotaBytesCheck(3));

    // case 4: capacity - usedBytes < localDelta + incBytes
    EXPECT_CALL(*mockMdsClient_, GetFsInfo(Matcher<uint32_t>(_), _))
        .WillOnce(testing::Invoke([](uint32_t fsId, FsInfo* fsInfo) {
            fsInfo->set_capacity(4);
            return FSStatusCode::OK;
        }));
    EXPECT_CALL(*mockMetaServerClient_, GetInode(_, _, _, _))
        .WillOnce(testing::Invoke(
            [](uint32_t fsId, uint64_t inodeId, Inode* inode, bool* streaming) {
                auto xattrs = inode->mutable_xattr();
                (*xattrs)[curvefs::XATTR_FS_BYTES] = "0";
                return MetaStatusCode::OK;
            }));
    FsQuotaChecker::GetInstance().UpdateQuotaCache();
    FsUsedUpdater::GetInstance().UpdateDeltaBytes(1);
    ASSERT_FALSE(FsQuotaChecker::GetInstance().QuotaBytesCheck(4));
    FsUsedUpdater::GetInstance().UpdateDeltaBytes(-1);

    // case 5: capacity - usedBytes >= localDelta + incBytes
    EXPECT_CALL(*mockMdsClient_, GetFsInfo(Matcher<uint32_t>(_), _))
        .WillOnce(testing::Invoke([](uint32_t fsId, FsInfo* fsInfo) {
            fsInfo->set_capacity(5);
            return FSStatusCode::OK;
        }));
    EXPECT_CALL(*mockMetaServerClient_, GetInode(_, _, _, _))
        .WillOnce(testing::Invoke(
            [](uint32_t fsId, uint64_t inodeId, Inode* inode, bool* streaming) {
                auto xattrs = inode->mutable_xattr();
                (*xattrs)[curvefs::XATTR_FS_BYTES] = "1";
                return MetaStatusCode::OK;
            }));
    FsQuotaChecker::GetInstance().UpdateQuotaCache();
    FsUsedUpdater::GetInstance().UpdateDeltaBytes(1);
    ASSERT_TRUE(FsQuotaChecker::GetInstance().QuotaBytesCheck(3));
    FsUsedUpdater::GetInstance().UpdateDeltaBytes(-1);
}

}  // namespace client
}  // namespace curvefs
