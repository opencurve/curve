/*
 *  Copyright (c) 2022 NetEase Inc.
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
 * Date: Tuesday Mar 01 18:44:29 CST 2022
 * Author: wuhanqing
 */

#include "curvefs/src/mds/space/block_group_storage.h"

#include <google/protobuf/util/message_differencer.h>

#include "absl/memory/memory.h"
#include "curvefs/test/mds/mock/mock_etcd_client.h"
#include "curvefs/test/utils/protobuf_message_utils.h"

namespace curvefs {
namespace mds {
namespace space {

using ::testing::_;
using ::testing::Invoke;
using ::testing::Matcher;
using ::testing::Return;

static constexpr uint32_t kFsId = 1;
static constexpr uint64_t kOffset = 10ULL * 1024 * 1024 * 1024;

class BlockGroupStorageTest : public ::testing::Test {
 protected:
    void SetUp() override {
        etcdclient_ = std::make_shared<MockEtcdClientImpl>();
        storage_ = absl::make_unique<BlockGroupStorageImpl>(etcdclient_);

        auto message = test::GenerateAnDefaultInitializedMessage(
            "curvefs.mds.space.BlockGroup");

        ASSERT_TRUE(message);

        auto* blockgroup = dynamic_cast<BlockGroup*>(message.get());
        ASSERT_NE(nullptr, blockgroup);

        blockGroup_.reset(blockgroup);
        message.release();
    }

 protected:
    std::shared_ptr<MockEtcdClientImpl> etcdclient_;
    std::unique_ptr<BlockGroup> blockGroup_;
    std::unique_ptr<BlockGroupStorage> storage_;
};

TEST_F(BlockGroupStorageTest, TestPutBlockGroup_EncodeError) {
    // missing required fields
    blockGroup_->clear_size();

    EXPECT_CALL(*etcdclient_, Put(_, _)).Times(0);

    ASSERT_EQ(SpaceErrEncode,
              storage_->PutBlockGroup(kFsId, kOffset, *blockGroup_));
}

TEST_F(BlockGroupStorageTest, TestPutBlockGroup_PutError) {
    EXPECT_CALL(*etcdclient_, Put(_, _))
        .WillOnce(Return(EtcdErrCode::EtcdInternal));

    ASSERT_EQ(SpaceErrStorage,
              storage_->PutBlockGroup(kFsId, kOffset, *blockGroup_));
}

TEST_F(BlockGroupStorageTest, TestPutBlockGroup_Success) {
    EXPECT_CALL(*etcdclient_, Put(_, _)).WillOnce(Return(EtcdErrCode::EtcdOK));

    ASSERT_EQ(SpaceOk, storage_->PutBlockGroup(kFsId, kOffset, *blockGroup_));
}

TEST_F(BlockGroupStorageTest, TestRemoveBlockGroup_RemoveSuccess) {
    EXPECT_CALL(*etcdclient_, Delete(_)).WillOnce(Return(EtcdErrCode::EtcdOK));

    ASSERT_EQ(SpaceOk, storage_->RemoveBlockGroup(kFsId, kOffset));
}

TEST_F(BlockGroupStorageTest, TestRemoveBlockGroup_KeyNotExist) {
    EXPECT_CALL(*etcdclient_, Delete(_))
        .WillOnce(Return(EtcdErrCode::EtcdKeyNotExist));

    ASSERT_EQ(SpaceErrNotFound, storage_->RemoveBlockGroup(kFsId, kOffset));
}

TEST_F(BlockGroupStorageTest, TestListBlockGroup_ListError) {
    using type = std::vector<std::string>*;
    EXPECT_CALL(*etcdclient_, List(_, _, Matcher<type>(_)))
        .WillOnce(Return(EtcdErrCode::EtcdInternal));

    std::vector<BlockGroup> groups;
    ASSERT_EQ(SpaceErrStorage, storage_->ListBlockGroups(kFsId, &groups));
}

TEST_F(BlockGroupStorageTest, TestListBlockGroup_DecodeError) {
    using type = std::vector<std::string>*;
    EXPECT_CALL(*etcdclient_, List(_, _, Matcher<type>(_)))
        .WillOnce(Invoke([](const std::string&, const std::string&,
                            std::vector<std::string>* values) {
            values->push_back("hello, world");
            return 0;
        }));

    std::vector<BlockGroup> groups;
    ASSERT_EQ(SpaceErrDecode, storage_->ListBlockGroups(kFsId, &groups));
}

TEST_F(BlockGroupStorageTest, TestListBlockGroup_Success) {
    using type = std::vector<std::string>*;
    EXPECT_CALL(*etcdclient_, List(_, _, Matcher<type>(_)))
        .WillOnce(Invoke([this](const std::string&, const std::string&,
                                std::vector<std::string>* values) {
            std::string value;
            blockGroup_->SerializeToString(&value);
            values->push_back(std::move(value));
            return 0;
        }));

    std::vector<BlockGroup> groups;
    ASSERT_EQ(SpaceOk, storage_->ListBlockGroups(kFsId, &groups));
    ASSERT_EQ(1, groups.size());
    ASSERT_TRUE(google::protobuf::util::MessageDifferencer::Equals(
        groups[0], *blockGroup_));
}

}  // namespace space
}  // namespace mds
}  // namespace curvefs
