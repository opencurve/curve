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

#include <brpc/closure_guard.h>
#include <brpc/server.h>
#include <gtest/gtest.h>
#include <cassert>
#include <utility>

#include "absl/memory/memory.h"
#include "curvefs/proto/common.pb.h"
#include "curvefs/proto/mds.pb.h"
#include "curvefs/proto/space.pb.h"
#include "curvefs/src/mds/space/block_group_storage.h"
#include "curvefs/test/mds/mock/mock_block_group_storage.h"
#include "curvefs/test/mds/mock/mock_fs_stroage.h"
#include "proto/nameserver2.pb.h"

namespace curvefs {
namespace mds {
namespace space {

using ::testing::_;
using ::testing::Invoke;
using ::testing::Matcher;
using ::testing::Return;

static constexpr uint32_t kFsId = 1;
static constexpr uint64_t kVolumeSize = 10ULL * 1024 * 1024 * 1024;
static constexpr uint32_t kBlockSize = 4096;
static constexpr uint32_t kBlockGroupSize = 128ULL * 1024 * 1024;
static constexpr curvefs::common::BitmapLocation kBitmapLocation =
    curvefs::common::BitmapLocation::AtStart;
static constexpr uint32_t kAllocateOnce = 4;
static constexpr double kExtendFactor = 1.5;
static const char *kOwner = "test";

static unsigned int seed = time(nullptr);

class VolumeSpaceTest : public ::testing::Test {
 protected:
    void SetUp() override {
        storage_ = absl::make_unique<MockBlockGroupStorage>();
        fsStorage_ = absl::make_unique<MockFsStorage>();
        volume_.set_volumesize(kVolumeSize);
        volume_.set_blocksize(kBlockSize);
        volume_.set_blockgroupsize(kBlockGroupSize);
        volume_.set_bitmaplocation(kBitmapLocation);
        volume_.set_autoextend(false);
        volume_.set_volumename("test");
    }

    std::unique_ptr<VolumeSpace> CreateOneEmptyVolumeSpace() {
        EXPECT_CALL(*storage_, ListBlockGroups(_, _))
            .WillOnce(Invoke([](uint32_t, std::vector<BlockGroup> *groups) {
                groups->clear();
                return SpaceOk;
            }));

        // disable background threads during testing.
        auto space = VolumeSpace::Create(kFsId, volume_, storage_.get(),
                                         fsStorage_.get(), 60000);
        EXPECT_NE(nullptr, space);
        return space;
    }

    std::unique_ptr<VolumeSpace>
    CreateVolumeSpaceWithTwoGroups(bool allocated = true,
                                   std::vector<BlockGroup> *out = nullptr) {
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
        if (allocated) {
            group2.set_owner(kOwner);
        }

        std::vector<BlockGroup> exist{group1, group2};

        if (out) {
            *out = exist;
        }

        EXPECT_CALL(*storage_, ListBlockGroups(_, _))
            .WillOnce(
                Invoke([&exist](uint32_t, std::vector<BlockGroup> *groups) {
                    *groups = exist;
                    return SpaceOk;
                }));

        auto space = VolumeSpace::Create(kFsId, volume_, storage_.get(),
                                         fsStorage_.get(), 1);
        EXPECT_NE(nullptr, space);
        return space;
    }

    void
    UpdateBlockGroupSpaceInfo(uint32_t metaserverId,
                              std::unique_ptr<VolumeSpace> &space,  // NOLINT
                              bool needIssued = false,
                              uint64_t expectIssued = 0) {
        BlockGroupDeallcateStatusMap stats;
        DeallocatableBlockGroupVec groups;
        {
            DeallocatableBlockGroup group1;
            group1.set_blockgroupoffset(0);
            group1.set_deallocatablesize(kBlockGroupSize / 2);
            groups.Add()->CopyFrom(group1);


            DeallocatableBlockGroup group2;
            group2.set_blockgroupoffset(kBlockGroupSize);
            group2.set_deallocatablesize(kBlockGroupSize / 4);
            groups.Add()->CopyFrom(group2);
        }

        uint64_t issued = 0;
        if (needIssued) {
            ASSERT_TRUE(space->UpdateDeallocatableBlockGroup(
                metaserverId, groups, stats, &issued));
            ASSERT_EQ(issued, expectIssued);

        } else {
            ASSERT_FALSE(space->UpdateDeallocatableBlockGroup(
                metaserverId, groups, stats, &issued));
        }
    }

    void
    UpdateBlockGroupIssueStat(uint32_t metaserverId,
                              std::unique_ptr<VolumeSpace> &space,  // NOLINT
                              uint64_t blockoffset,
                              BlockGroupDeallcateStatusCode status,
                              bool needIssue = true) {
        DeallocatableBlockGroupVec groups;
        BlockGroupDeallcateStatusMap stats;
        stats[blockoffset] = status;
        if (status == BlockGroupDeallcateStatusCode::BGDP_DONE) {
            DeallocatableBlockGroup group;
            group.set_blockgroupoffset(blockoffset);
            group.set_deallocatablesize(0);
            groups.Add()->CopyFrom(group);
        }

        uint64_t issued = 0;
        if (needIssue) {
            ASSERT_TRUE(space->UpdateDeallocatableBlockGroup(
                metaserverId, groups, stats, &issued));
        } else {
            ASSERT_FALSE(space->UpdateDeallocatableBlockGroup(
                metaserverId, groups, stats, &issued));
        }
    }

 protected:
    std::unique_ptr<MockBlockGroupStorage> storage_;
    std::unique_ptr<MockFsStorage> fsStorage_;
    common::Volume volume_;
};

TEST_F(VolumeSpaceTest, TestCreate_ListError) {
    EXPECT_CALL(*storage_, ListBlockGroups(_, _))
        .WillOnce(Return(SpaceErrStorage));

    auto space = VolumeSpace::Create(kFsId, volume_, storage_.get(),
                                     fsStorage_.get(), 1);
    EXPECT_EQ(nullptr, space);
}

TEST_F(VolumeSpaceTest, TestCreate_Success) {
    EXPECT_CALL(*storage_, ListBlockGroups(_, _))
        .WillOnce(Invoke([](uint32_t, std::vector<BlockGroup> *groups) {
            groups->clear();
            return SpaceOk;
        }));

    auto space = VolumeSpace::Create(kFsId, volume_, storage_.get(),
                                     fsStorage_.get(), 1);
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
        uint64_t count = rand_r(&seed) % 10 + 1;
        count = std::min(totalGroups - i, count);

        std::vector<BlockGroup> newGroups;
        ASSERT_EQ(SpaceOk,
                  space->AllocateBlockGroups(count, kOwner, &newGroups));
        groups.insert(groups.end(), std::make_move_iterator(newGroups.begin()),
                      std::make_move_iterator(newGroups.end()));

        i += count;
    }

    std::set<uint64_t> offsets;
    for (auto &group : groups) {
        ASSERT_EQ(kOwner, group.owner());
        offsets.insert(group.offset());
    }
    ASSERT_EQ(totalGroups, offsets.size());
    ASSERT_EQ(0, *offsets.begin());
    ASSERT_EQ(kVolumeSize - kBlockGroupSize, *offsets.rbegin());

    groups.clear();
    ASSERT_EQ(SpaceErrNoSpace,
              space->AllocateBlockGroups(kAllocateOnce, kOwner, &groups));
}

TEST_F(VolumeSpaceTest, TestAutoExtendVolume_ExtendError) {
    volume_.set_autoextend(true);
    volume_.set_extendfactor(kExtendFactor);
    volume_.add_cluster("127.0.0.1:34000");
    volume_.add_cluster("127.0.0.1:34001");
    volume_.add_cluster("127.0.0.1:34002");

    constexpr auto totalGroups = kVolumeSize / kBlockGroupSize;
    auto space = CreateOneEmptyVolumeSpace();

    EXPECT_CALL(*storage_, PutBlockGroup(_, _, _))
        .Times(totalGroups)
        .WillRepeatedly(Return(SpaceOk));

    std::vector<BlockGroup> groups;
    ASSERT_EQ(SpaceOk,
              space->AllocateBlockGroups(totalGroups, kOwner, &groups));
    ASSERT_EQ(totalGroups, groups.size());

    std::vector<BlockGroup> newGroups;
    ASSERT_EQ(SpaceErrNoSpace,
              space->AllocateBlockGroups(1, kOwner, &newGroups));
}

namespace {
class FakeCurveFSService : public curve::mds::CurveFSService {
 public:
    void ExtendFile(::google::protobuf::RpcController *controller,
                    const ::curve::mds::ExtendFileRequest *request,
                    ::curve::mds::ExtendFileResponse *response,
                    ::google::protobuf::Closure *done) override {
        brpc::ClosureGuard guard(done);
        if (request->newsize() % kBlockGroupSize != 0) {
            response->set_statuscode(curve::mds::kParaError);
        } else {
            response->set_statuscode(curve::mds::kOK);
        }
    }
};
}  // namespace

TEST_F(VolumeSpaceTest, TestAutoExtendVolume_UpdateFsInfoError) {
    volume_.set_autoextend(true);
    volume_.set_extendfactor(kExtendFactor);
    volume_.set_extendalignment(kBlockGroupSize);
    volume_.add_cluster("127.0.0.1:34000");
    volume_.add_cluster("127.0.0.1:34001");
    volume_.add_cluster("127.0.0.1:34002");

    brpc::Server server;
    ASSERT_EQ(0, server.AddService(new FakeCurveFSService(),
                                   brpc::SERVER_OWNS_SERVICE));
    ASSERT_EQ(0, server.Start("127.0.0.1:34000", nullptr));

    constexpr auto totalGroups = kVolumeSize / kBlockGroupSize;
    auto space = CreateOneEmptyVolumeSpace();

    EXPECT_CALL(*storage_, PutBlockGroup(_, _, _))
        .Times(totalGroups)
        .WillRepeatedly(Return(SpaceOk));

    std::vector<BlockGroup> groups;
    ASSERT_EQ(SpaceOk,
              space->AllocateBlockGroups(totalGroups, kOwner, &groups));
    ASSERT_EQ(totalGroups, groups.size());

    EXPECT_CALL(*fsStorage_, Get(Matcher<uint64_t>(_), _))
        .WillOnce(Return(FSStatusCode::OK));
    EXPECT_CALL(*fsStorage_, Update(_))
        .WillOnce(Return(FSStatusCode::STORAGE_ERROR));

    std::vector<BlockGroup> newGroups;
    ASSERT_EQ(SpaceErrNoSpace,
              space->AllocateBlockGroups(1, kOwner, &newGroups));

    server.Stop(0);
    server.Join();
}

TEST_F(VolumeSpaceTest, TestAutoExtendVolumeSuccess) {
    volume_.set_autoextend(true);
    volume_.set_extendfactor(kExtendFactor);
    volume_.set_extendalignment(kBlockGroupSize);
    volume_.add_cluster("127.0.0.1:34000");
    volume_.add_cluster("127.0.0.1:34001");
    volume_.add_cluster("127.0.0.1:34002");

    brpc::Server server;
    ASSERT_EQ(0, server.AddService(new FakeCurveFSService(),
                                   brpc::SERVER_OWNS_SERVICE));
    ASSERT_EQ(0, server.Start("127.0.0.1:34000", nullptr));

    constexpr auto totalGroups = kVolumeSize / kBlockGroupSize;
    auto space = CreateOneEmptyVolumeSpace();

    EXPECT_CALL(*storage_, PutBlockGroup(_, _, _))
        .Times(totalGroups)
        .WillRepeatedly(Return(SpaceOk));

    std::vector<BlockGroup> groups;
    ASSERT_EQ(SpaceOk,
              space->AllocateBlockGroups(totalGroups, kOwner, &groups));
    ASSERT_EQ(totalGroups, groups.size());

    EXPECT_CALL(*fsStorage_, Get(Matcher<uint64_t>(_), _))
        .WillOnce(Return(FSStatusCode::OK));
    EXPECT_CALL(*fsStorage_, Update(_)).WillOnce(Return(FSStatusCode::OK));

    EXPECT_CALL(*storage_, PutBlockGroup(_, _, _))
        .Times(1)
        .WillRepeatedly(Return(SpaceOk));

    std::vector<BlockGroup> newGroups;
    ASSERT_EQ(SpaceOk, space->AllocateBlockGroups(1, kOwner, &newGroups));

    ASSERT_EQ(totalGroups + 1, space->allocatedGroups_.size());
    ASSERT_TRUE(space->availableGroups_.empty());
    ASSERT_FALSE(space->cleanGroups_.empty());

    server.Stop(0);
    server.Join();
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

MATCHER(NoOwner, "") { return !arg.has_owner(); }

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

TEST_F(VolumeSpaceTest, Test_CalBlockGroupAvailableForDeAllocate) {
    // 1. test cal none
    {
        auto space = CreateOneEmptyVolumeSpace();
        sleep(1);
        ASSERT_EQ(0, space->waitDeallocateGroups_.size());

        space = CreateVolumeSpaceWithTwoGroups(false);
        sleep(1);
        ASSERT_EQ(0, space->waitDeallocateGroups_.size());
        ASSERT_EQ(2, space->availableGroups_.size());
    }

    // 2. test cal one and issue
    {
        // cal one
        uint32_t metaserverId1 = 1;
        std::vector<BlockGroup> exists;
        auto space = CreateVolumeSpaceWithTwoGroups(false, &exists);
        UpdateBlockGroupSpaceInfo(metaserverId1, space);
        space->CalBlockGroupAvailableForDeAllocate();
        ASSERT_EQ(1, space->waitDeallocateGroups_.size());
        ASSERT_EQ(exists[0].offset(), space->waitDeallocateGroups_[0].offset());
        ASSERT_EQ(1, space->availableGroups_.size());
        ASSERT_EQ(exists[0].offset(), space->availableGroups_[0].offset());
        ASSERT_EQ(kBlockGroupSize / 2, space->summary_[exists[0].offset()]);
        ASSERT_EQ(kBlockGroupSize / 4, space->summary_[exists[1].offset()]);

        // issue to metaserver1
        EXPECT_CALL(*storage_, PutBlockGroup(_, _, _))
            .Times(1)
            .WillOnce(Return(SpaceOk));
        UpdateBlockGroupSpaceInfo(metaserverId1, space, true,
                                  exists[0].offset());
        ASSERT_TRUE(space->waitDeallocateGroups_.empty());
        ASSERT_EQ(1, space->deallocatingGroups_.size());
        ASSERT_EQ(
            metaserverId1,
            space->deallocatingGroups_[exists[0].offset()].deallocating()[0]);

        // issue to metaserver2
        uint32_t metaserverId2 = 2;
        EXPECT_CALL(*storage_, PutBlockGroup(_, _, _))
            .Times(1)
            .WillOnce(Return(SpaceOk));
        UpdateBlockGroupSpaceInfo(metaserverId2, space, true,
                                  exists[0].offset());
        ASSERT_EQ(
            metaserverId1,
            space->deallocatingGroups_[exists[0].offset()].deallocating()[0]);

        ASSERT_EQ(
            metaserverId2,
            space->deallocatingGroups_[exists[0].offset()].deallocating()[1]);
        ASSERT_EQ(kBlockGroupSize, space->summary_[exists[0].offset()]);
        ASSERT_EQ(kBlockGroupSize / 2, space->summary_[exists[1].offset()]);

        // metaserver2 report, update summary and be issued again
        UpdateBlockGroupSpaceInfo(metaserverId2, space, true,
                                  exists[0].offset());
        ASSERT_EQ(kBlockGroupSize, space->summary_[exists[0].offset()]);
        ASSERT_EQ(kBlockGroupSize / 2, space->summary_[exists[1].offset()]);

        // metaserver1 report issued stat: BGDP_PROCESSING
        UpdateBlockGroupIssueStat(
            metaserverId1, space, exists[0].offset(),
            BlockGroupDeallcateStatusCode::BGDP_PROCESSING);
        ASSERT_EQ(2, space->deallocatingGroups_[exists[0].offset()]
                         .deallocating()
                         .size());
        ASSERT_EQ(2, space->summary_.size());

        // metaserver1 report issued stat: BGDP_DONE
        EXPECT_CALL(*storage_, PutBlockGroup(_, _, _))
            .Times(1)
            .WillOnce(Return(SpaceOk));
        UpdateBlockGroupIssueStat(metaserverId1, space, exists[0].offset(),
                                  BlockGroupDeallcateStatusCode::BGDP_DONE,
                                  false);
        ASSERT_EQ(
            metaserverId2,
            space->deallocatingGroups_[exists[0].offset()].deallocating()[0]);
        ASSERT_EQ(
            metaserverId1,
            space->deallocatingGroups_[exists[0].offset()].deallocated()[0]);
        ASSERT_EQ(2, space->summary_.size());

        // metaserver2 report issued stat: BGDP_DONE
        EXPECT_CALL(*storage_, PutBlockGroup(_, _, _))
            .Times(1)
            .WillOnce(Return(SpaceOk));
        UpdateBlockGroupIssueStat(metaserverId2, space, exists[0].offset(),
                                  BlockGroupDeallcateStatusCode::BGDP_DONE,
                                  false);
        ASSERT_EQ(
            metaserverId1,
            space->deallocatingGroups_[exists[0].offset()].deallocated()[0]);
        ASSERT_EQ(
            metaserverId2,
            space->deallocatingGroups_[exists[0].offset()].deallocated()[1]);
        ASSERT_EQ(1, space->summary_.size());
        ASSERT_EQ(kBlockGroupSize / 2, space->summary_[exists[1].offset()]);

        // metaserver1 and metaserver2 process done
        EXPECT_CALL(*storage_, PutBlockGroup(_, exists[0].offset(), _))
            .Times(1)
            .WillOnce(Return(SpaceOk));
        space->CalBlockGroupAvailableForDeAllocate();
        ASSERT_EQ(1, space->waitDeallocateGroups_.count(exists[1].offset()));
        ASSERT_EQ(1, space->waitDeallocateGroups_.size());
        ASSERT_EQ(1, space->availableGroups_.count(exists[0].offset()));
        ASSERT_EQ(1, space->availableGroups_.size());
    }
}

}  // namespace space
}  // namespace mds
}  // namespace curvefs
