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
 * Date: Saturday Mar 05 23:36:02 CST 2022
 * Author: wuhanqing
 */

#include "curvefs/src/volume/block_group_updater.h"

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "absl/memory/memory.h"
#include "curvefs/test/volume/mock/mock_block_device_client.h"

namespace curvefs {
namespace volume {

using ::testing::_;
using ::testing::Invoke;
using ::testing::Return;

static constexpr uint32_t kBlockSize = 4 * kKiB;
static constexpr uint64_t kBlockGroupSize = 128 * kMiB;   // 128 MiB
static constexpr uint64_t kBlockGroupOffset = 10 * kGiB;  // 10 GiB

class BlockGroupBitmapUpdaterTest : public ::testing::Test {
 protected:
    void SetUp() override {
        mockBlockDev_ = absl::make_unique<MockBlockDeviceClient>();

        Bitmap bitmap(kBlockGroupSize / kBlockSize);
        bitmap.Clear();

        BitmapRange range{
            kBlockGroupOffset,
            kBlockGroupSize / kBlockSize / curve::common::BITMAP_UNIT_SIZE};

        updater_ = absl::make_unique<BlockGroupBitmapUpdater>(
            std::move(bitmap), kBlockSize, kBlockGroupSize, kBlockGroupOffset,
            range, mockBlockDev_.get());
    }

 protected:
    std::unique_ptr<MockBlockDeviceClient> mockBlockDev_;
    std::unique_ptr<BlockGroupBitmapUpdater> updater_;
};

TEST_F(BlockGroupBitmapUpdaterTest, SyncTest_NoDirty) {
    EXPECT_CALL(*mockBlockDev_, Write(_, _, _))
        .Times(0);

    ASSERT_TRUE(updater_->Sync());
}

TEST_F(BlockGroupBitmapUpdaterTest, SyncTest_Dirty) {
    auto start = kBlockGroupOffset;
    auto end = kBlockGroupOffset + kBlockGroupSize;
    updater_->Update({start, end}, BlockGroupBitmapUpdater::Set);

    EXPECT_CALL(*mockBlockDev_, Write(_, _, _))
        .WillOnce(
            Invoke([](const char*, off_t, size_t length) { return length; }));

    ASSERT_TRUE(updater_->Sync());
}

TEST_F(BlockGroupBitmapUpdaterTest, SyncTest_OnlySyncOnce) {
    auto start1 = kBlockGroupOffset;
    auto end1 = kBlockGroupOffset + kBlockGroupSize;
    updater_->Update({start1, end1}, BlockGroupBitmapUpdater::Set);

    auto start2 = kBlockGroupOffset;
    auto end2 = kBlockGroupOffset + kBlockGroupSize;
    updater_->Update({start2, end2}, BlockGroupBitmapUpdater::Clear);

    EXPECT_CALL(*mockBlockDev_, Write(_, _, _))
        .WillOnce(
            Invoke([](const char*, off_t, size_t length) { return length; }));

    ASSERT_TRUE(updater_->Sync());
}

TEST_F(BlockGroupBitmapUpdaterTest, SyncTest_WriteFailed) {
    auto start = kBlockGroupOffset;
    auto end = kBlockGroupOffset + kBlockGroupSize;
    updater_->Update({start, end}, BlockGroupBitmapUpdater::Set);

    EXPECT_CALL(*mockBlockDev_, Write(_, _, _))
        .WillOnce(Return(-1));

    ASSERT_FALSE(updater_->Sync());
}

}  // namespace volume
}  // namespace curvefs
