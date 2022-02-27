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
 * Date: Tuesday Mar 01 19:30:56 CST 2022
 * Author: wuhanqing
 */

#include "curvefs/src/mds/space/volume_space.h"

#include <gtest/gtest.h>

#include "absl/memory/memory.h"
#include "curvefs/src/mds/space/block_group_storage.h"
#include "curvefs/test/mds/mock/mock_block_group_storage.h"

namespace curvefs {
namespace mds {
namespace space {

using ::testing::_;
using ::testing::Invoke;
using ::testing::Return;

static constexpr uint32_t kFsId = 1;
static constexpr uint64_t kVolumeSize = 10ULL * 1024 * 1024 * 1024;
static constexpr uint32_t kBlockSize = 4096;
static constexpr uint32_t kBlockGroupSize = 128ULL * 1024 * 1024;
static constexpr curvefs::common::BitmapLocation kBitmapLocation =
    curvefs::common::BitmapLocation::AtStart;
static constexpr uint32_t kAllocateOnce = 4;
static const char* kOwner = "test";

static unsigned int seed = time(nullptr);

class VolumeSpaceTest : public ::testing::Test {
 protected:
    void SetUp() override {
        storage_ = absl::make_unique<MockBlockGroupStorage>();
    }

    std::unique_ptr<VolumeSpace> CreateOneEmptyVolumeSpace() {
        EXPECT_CALL(*storage_, ListBlockGroups(_, _))
            .WillOnce(Invoke([](uint32_t, std::vector<BlockGroup>* groups) {
                groups->clear();
                return SpaceOk;
            }));

        auto space =
            VolumeSpace::Create(kFsId, kVolumeSize, kBlockSize, kBlockGroupSize,
                                kBitmapLocation, storage_.get());
        EXPECT_NE(nullptr, space);
        return space;
    }

    std::unique_ptr<VolumeSpace> CreateVolumeSpaceWithTwoGroups(
        bool allocated = true, std::vector<BlockGroup>* out = nullptr) {
        BlockGroup group1;
        group1.set_offset(0);
        group1.set_size(kBlockGroupSize);
        group1.set_available(kBlockGroupSize / 2);
        group1.set_bitmaplocation(kBitmapLocation);
        if (allocated) {
            group1.set_owner(kOwner);
        }

        BlockGroup group2;
        group2.set_offset(kBlockGroupSize);
        group2.set_size(kBlockGroupSize);
        group2.set_available(kBlockGroupSize / 2);
        group2.set_bitmaplocation(kBitmapLocation);
        group2.set_owner(kOwner);
        if (allocated) {
            group2.set_owner(kOwner);
        }

        std::vector<BlockGroup> exist{group1, group2};

        if (out) {
            *out = exist;
        }

        EXPECT_CALL(*storage_, ListBlockGroups(_, _))
            .WillOnce(
                Invoke([&exist](uint32_t, std::vector<BlockGroup>* groups) {
                    *groups = exist;
                    return SpaceOk;
                }));

        auto space =
            VolumeSpace::Create(kFsId, kVolumeSize, kBlockSize, kBlockGroupSize,
                                kBitmapLocation, storage_.get());
        EXPECT_NE(nullptr, space);
        return space;
    }

 protected:
    std::unique_ptr<MockBlockGroupStorage> storage_;
};

TEST_F(VolumeSpaceTest, TestCreate_ListError) {
    EXPECT_CALL(*storage_, ListBlockGroups(_, _))
        .WillOnce(Return(SpaceErrStorage));

    auto space =
        VolumeSpace::Create(kFsId, kVolumeSize, kBlockSize, kBlockGroupSize,
                            kBitmapLocation, storage_.get());
    EXPECT_EQ(nullptr, space);
}

TEST_F(VolumeSpaceTest, TestCreate_Success) {
    EXPECT_CALL(*storage_, ListBlockGroups(_, _))
        .WillOnce(Invoke([](uint32_t, std::vector<BlockGroup>* groups) {
            groups->clear();
            return SpaceOk;
        }));

    auto space =
        VolumeSpace::Create(kFsId, kVolumeSize, kBlockSize, kBlockGroupSize,
                            kBitmapLocation, storage_.get());
    EXPECT_NE(nullptr, space);
}

TEST_F(VolumeSpaceTest, TestAllocateBlockGroups_PersistBlockGroupError) {
    auto space = CreateOneEmptyVolumeSpace();

    EXPECT_CALL(*storage_, PutBlockGroup(_, _, _))
        .WillOnce(Return(SpaceErrStorage));

    std::vector<BlockGroup> groups;
    EXPECT_EQ(SpaceErrStorage,
              space->AllocateBlockGroups(kAllocateOnce, kOwner, &groups));
}

TEST_F(VolumeSpaceTest, TestAllocateBlockGroups) {
    auto space = CreateOneEmptyVolumeSpace();
    constexpr auto totalGroups = kVolumeSize / kBlockGroupSize;

    EXPECT_CALL(*storage_, PutBlockGroup(_, _, _))
        .Times(totalGroups)
        .WillRepeatedly(Return(SpaceOk));

    std::vector<BlockGroup> groups;

    uint64_t i = 0;
    while (i < totalGroups) {
        uint64_t count = rand_r(&seed) % 10;
        count = std::min(totalGroups - i, count);

        std::vector<BlockGroup> newGroups;
        ASSERT_EQ(SpaceOk,
                  space->AllocateBlockGroups(count, kOwner, &newGroups));
        groups.insert(groups.end(), std::make_move_iterator(newGroups.begin()),
                      std::make_move_iterator(newGroups.end()));

        i += count;
    }

    std::set<uint64_t> offsets;
    for (auto& group : groups) {
        ASSERT_EQ(kOwner, group.owner());
        offsets.insert(group.offset());
    }
    ASSERT_EQ(totalGroups, offsets.size());
    ASSERT_EQ(0, *offsets.begin());
    ASSERT_EQ(kVolumeSize - kBlockGroupSize, *offsets.rbegin());

    ASSERT_EQ(SpaceErrNoSpace,
              space->AllocateBlockGroups(kAllocateOnce, kOwner, &groups));
}

TEST_F(VolumeSpaceTest, TestAcquireBlockGroups) {
    auto space = CreateOneEmptyVolumeSpace();

    EXPECT_CALL(*storage_, PutBlockGroup(_, _, _)).WillOnce(Return(SpaceOk));

    uint64_t offset = 0;

    BlockGroup group;
    ASSERT_EQ(SpaceOk, space->AcquireBlockGroup(offset, kOwner, &group));
    ASSERT_EQ(kOwner, group.owner());
    ASSERT_EQ(offset, group.offset());
}

TEST_F(VolumeSpaceTest, TestAcquireBlockGroups_Retry) {
    auto space = CreateOneEmptyVolumeSpace();

    EXPECT_CALL(*storage_, PutBlockGroup(_, _, _))
        .Times(2)
        .WillRepeatedly(Return(SpaceOk));

    uint64_t offset = 0;

    BlockGroup group;
    ASSERT_EQ(SpaceOk, space->AcquireBlockGroup(offset, kOwner, &group));
    ASSERT_EQ(kOwner, group.owner());
    ASSERT_EQ(offset, group.offset());

    BlockGroup group2;
    ASSERT_EQ(SpaceOk, space->AcquireBlockGroup(offset, kOwner, &group2));
    ASSERT_EQ(kOwner, group2.owner());
    ASSERT_EQ(offset, group2.offset());
}

TEST_F(VolumeSpaceTest, TestAcquireBlockGroups_Conflict) {
    auto space = CreateOneEmptyVolumeSpace();

    EXPECT_CALL(*storage_, PutBlockGroup(_, _, _))
        .Times(1)
        .WillOnce(Return(SpaceOk));

    uint64_t offset = 0;

    BlockGroup group;
    ASSERT_EQ(SpaceOk, space->AcquireBlockGroup(offset, kOwner, &group));
    ASSERT_EQ(kOwner, group.owner());
    ASSERT_EQ(offset, group.offset());

    BlockGroup group2;
    ASSERT_EQ(SpaceErrConflict,
              space->AcquireBlockGroup(offset, "another owner", &group2));
}

TEST_F(VolumeSpaceTest, TestRemoveAllBlockGroups_EmptyVolumeSpace) {
    auto space = CreateOneEmptyVolumeSpace();
    ASSERT_EQ(SpaceOk, space->RemoveAllBlockGroups());
}

TEST_F(VolumeSpaceTest, TestRemoveAllBlockGroups_ClearBlockGroupError) {
    for (auto allocated : {true, false}) {
        auto space = CreateVolumeSpaceWithTwoGroups(allocated);

        EXPECT_CALL(*storage_, RemoveBlockGroup(_, _))
            .WillOnce(Return(SpaceErrStorage));

        ASSERT_NE(SpaceOk, space->RemoveAllBlockGroups());
    }
}

TEST_F(VolumeSpaceTest, TestRemoveAllBlockGroups_Success) {
    for (auto allocated : {true, false}) {
        auto space = CreateVolumeSpaceWithTwoGroups(allocated);

        EXPECT_CALL(*storage_, RemoveBlockGroup(_, _))
            .Times(2)
            .WillRepeatedly(Return(SpaceOk));

        ASSERT_EQ(SpaceOk, space->RemoveAllBlockGroups());
    }
}

TEST_F(VolumeSpaceTest, TestReleaseBlockGroup_OwnerNotIdentical) {
    std::vector<BlockGroup> exists;
    auto space = CreateVolumeSpaceWithTwoGroups(true, &exists);

    exists[0].set_owner("hello");

    ASSERT_EQ(SpaceErrConflict, space->ReleaseBlockGroups(exists));
}

TEST_F(VolumeSpaceTest, TestReleaseBlockGroup_SpaceIsNotUsed) {
    std::vector<BlockGroup> exists;
    auto space = CreateVolumeSpaceWithTwoGroups(true, &exists);

    exists[0].set_available(exists[0].size());
    exists[1].set_available(exists[1].size());

    EXPECT_CALL(*storage_, RemoveBlockGroup(_, _))
        .Times(2)
        .WillRepeatedly(Return(SpaceOk));

    ASSERT_EQ(SpaceOk, space->ReleaseBlockGroups(exists));
}

TEST_F(VolumeSpaceTest, TestReleaseBlockGroup_SpaceIsNotUsed_ButClearError) {
    std::vector<BlockGroup> exists;
    auto space = CreateVolumeSpaceWithTwoGroups(true, &exists);

    exists[0].set_available(exists[0].size());
    exists[1].set_available(exists[1].size());

    EXPECT_CALL(*storage_, RemoveBlockGroup(_, _))
        .WillOnce(Return(SpaceErrStorage));

    ASSERT_EQ(SpaceErrStorage, space->ReleaseBlockGroups(exists));
}

MATCHER(NoOwner, "") {
    return !arg.has_owner();
}

TEST_F(VolumeSpaceTest, TestReleaseBlockGroup_SpaceIsPartialUsed) {
    std::vector<BlockGroup> exists;
    auto space = CreateVolumeSpaceWithTwoGroups(true, &exists);

    exists[0].set_available(exists[0].size() / 2);
    exists[1].set_available(exists[1].size() / 2);

    EXPECT_CALL(*storage_, PutBlockGroup(_, _, NoOwner()))
        .Times(2)
        .WillRepeatedly(Return(SpaceOk));

    ASSERT_EQ(SpaceOk, space->ReleaseBlockGroups(exists));
}

TEST_F(VolumeSpaceTest, TestReleaseBlockGroup_SpaceIsPartialUsed_PutError) {
    std::vector<BlockGroup> exists;
    auto space = CreateVolumeSpaceWithTwoGroups(true, &exists);

    exists[0].set_available(exists[0].size() / 2);
    exists[1].set_available(exists[1].size() / 2);

    EXPECT_CALL(*storage_, PutBlockGroup(_, _, NoOwner()))
        .WillOnce(Return(SpaceErrStorage));

    ASSERT_EQ(SpaceErrStorage, space->ReleaseBlockGroups(exists));
}

}  // namespace space
}  // namespace mds
}  // namespace curvefs
