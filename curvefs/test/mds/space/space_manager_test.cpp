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
 * Date: Tuesday Mar 01 16:43:16 CST 2022
 * Author: wuhanqing
 */

#include <gtest/gtest.h>
#include <memory>

#include "absl/memory/memory.h"
#include "curvefs/proto/mds.pb.h"
#include "curvefs/src/mds/space/manager.h"
#include "curvefs/test/mds/mock/mock_etcd_client.h"
#include "curvefs/test/mds/mock/mock_fs_stroage.h"

namespace curvefs {
namespace mds {
namespace space {

//using ::curve::kvstorage::StorageClient;
using ::curvefs::common::Volume;
using ::curvefs::mds::FsInfo;
using ::testing::_;
using ::testing::Invoke;
using ::testing::Matcher;
using ::testing::Return;

namespace {

constexpr uint64_t kVolumeSize = 10ULL * 1024 * 1024 * 1024;  // 10 GiB
constexpr uint32_t kBlockSize = 4096;
constexpr uint32_t kBlockGroupSize = 128ULL * 1024 * 1024;  // 128 MiB
constexpr BitmapLocation kBitmapLocation = BitmapLocation::AtStart;
constexpr uint32_t kFsId = 1;

FsInfo GenerateVolumeFsInfo() {
    FsInfo fsInfo;
    fsInfo.set_fsid(kFsId);
    Volume volume;
    volume.set_volumesize(kVolumeSize);
    volume.set_blocksize(kBlockSize);
    volume.set_blockgroupsize(kBlockGroupSize);
    volume.set_bitmaplocation(kBitmapLocation);
    auto* detail = fsInfo.mutable_detail();
    detail->mutable_volume()->Swap(&volume);

    return fsInfo;
}

}  // namespace

class SpaceManagerTest : public ::testing::Test {
 protected:
    void SetUp() override {
        etcdclient_ = std::make_shared<MockEtcdClientImpl>();
        fsStorage_ = std::make_shared<MockFsStorage>();
        spaceManager_ =
            absl::make_unique<SpaceManagerImpl>(etcdclient_, fsStorage_);
    }

    void TearDown() override {}

    void AddOneNewVolume() {
        FsInfo fsInfo = GenerateVolumeFsInfo();

        using type = std::vector<std::string>*;

        EXPECT_CALL(*etcdclient_, List(_, _, Matcher<type>(_)))
            .WillOnce(Invoke([](const std::string&, const std::string&,
                                std::vector<std::string>* values) {
                values->clear();
                return 0;
            }));

        ASSERT_EQ(SpaceOk, spaceManager_->AddVolume(fsInfo));
    }

 protected:
    std::shared_ptr<MockEtcdClientImpl> etcdclient_;
    std::unique_ptr<SpaceManager> spaceManager_;
    std::shared_ptr<MockFsStorage> fsStorage_;
};

TEST_F(SpaceManagerTest, TestGetVolumeSpace) {
    ASSERT_EQ(nullptr, spaceManager_->GetVolumeSpace(0));

    // add one volume space
    {
        AddOneNewVolume();
        ASSERT_NE(nullptr, spaceManager_->GetVolumeSpace(kFsId));
    }
}

TEST_F(SpaceManagerTest, TestAddVolume_AlreadyExists) {
    AddOneNewVolume();
    ASSERT_NE(nullptr, spaceManager_->GetVolumeSpace(kFsId));

    FsInfo fsInfo = GenerateVolumeFsInfo();

    using type = std::vector<std::string>*;
    EXPECT_CALL(*etcdclient_, List(_, _, Matcher<type>(_))).Times(0);

    ASSERT_EQ(SpaceErrExist, spaceManager_->AddVolume(fsInfo));
}

TEST_F(SpaceManagerTest, TestAddVolume_LoadBlockGroupsError) {
    using type = std::vector<std::string>*;
    EXPECT_CALL(*etcdclient_, List(_, _, Matcher<type>(_)))
        .WillOnce(Return(EtcdErrCode::EtcdInternal));

    FsInfo fsInfo = GenerateVolumeFsInfo();
    ASSERT_EQ(SpaceErrCreate, spaceManager_->AddVolume(fsInfo));
    ASSERT_EQ(nullptr, spaceManager_->GetVolumeSpace(fsInfo.fsid()));
}

TEST_F(SpaceManagerTest, TestRemoveVolume_SpaceNotExist) {
    ASSERT_EQ(SpaceErrNotFound, spaceManager_->RemoveVolume(kFsId + 1));
}

TEST_F(SpaceManagerTest, TestRemoveVolume_Success) {
    AddOneNewVolume();
    ASSERT_EQ(SpaceOk, spaceManager_->RemoveVolume(kFsId));
    ASSERT_EQ(nullptr, spaceManager_->GetVolumeSpace(kFsId));
}

TEST_F(SpaceManagerTest, TestDeleteVolume_SpaceNotExist) {
    ASSERT_EQ(SpaceErrNotFound, spaceManager_->DeleteVolume(kFsId));
}

TEST_F(SpaceManagerTest, TestDeleteVolume_Success) {
    AddOneNewVolume();
    ASSERT_EQ(SpaceOk, spaceManager_->DeleteVolume(kFsId));
    ASSERT_EQ(nullptr, spaceManager_->GetVolumeSpace(kFsId));
}

}  // namespace space
}  // namespace mds
}  // namespace curvefs
