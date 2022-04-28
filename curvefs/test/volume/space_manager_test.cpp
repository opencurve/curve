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
 * Date: Wednesday Apr 27 09:49:40 CST 2022
 * Author: wuhanqing
 */

#include "curvefs/src/volume/space_manager.h"

#include <gmock/gmock-cardinalities.h>
#include <gmock/gmock-more-actions.h>
#include <gmock/gmock-spec-builders.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <atomic>
#include <iterator>
#include <memory>
#include <thread>

#include "curvefs/proto/space.pb.h"
#include "curvefs/src/volume/common.h"
#include "curvefs/test/client/rpcclient/mock_mds_client.h"
#include "curvefs/test/volume/mock/mock_block_device_client.h"

namespace curvefs {
namespace volume {

using ::testing::_;
using ::testing::Invoke;
using ::testing::AtLeast;

namespace {

constexpr uint32_t kBlockSize = 4096;
constexpr uint32_t kBlockGroupSize = 128ULL * 1024 * 1024;

ssize_t MockRead(char *data, off_t, size_t length) {
    memset(data, 0, length);
    return length;
}

ssize_t MockWrite(const char*, off_t, size_t length) {
    return length;
}

struct MockAllocateBlockGroup {
    explicit MockAllocateBlockGroup(const mds::space::BlockGroup &group)
        : group(group) {}

    mds::space::SpaceErrCode operator()(
        uint32_t,
        uint32_t,
        const std::string &,
        std::vector<curvefs::mds::space::BlockGroup> *groups) const {
        groups->push_back(group);
        return mds::space::SpaceOk;
    }

    const mds::space::BlockGroup &group;
};

struct TaskStruct {
    std::vector<Extent> extent;
    std::thread th;
};

}  // namespace

class SpaceManagerImplTest : public ::testing::Test {
 protected:
    void SetUp() override {
        opt_.blockGroupManagerOption.blockSize = kBlockSize;
        opt_.blockGroupManagerOption.blockGroupSize = kBlockGroupSize;
        opt_.blockGroupManagerOption.owner = "hello";
        opt_.blockGroupManagerOption.blockGroupAllocateOnce = 1;

        opt_.allocatorOption.type = "bitmap";
        opt_.allocatorOption.bitmapAllocatorOption.sizePerBit = 0;
        opt_.allocatorOption.bitmapAllocatorOption.smallAllocProportion = 0;

        mdsClient_ = std::make_shared<client::rpcclient::MockMdsClient>();
        devClient_ = std::make_shared<MockBlockDeviceClient>();
        spaceManager_.reset(new SpaceManagerImpl(opt_, mdsClient_, devClient_));
    }

 protected:
    SpaceManagerOption opt_;
    std::shared_ptr<client::rpcclient::MockMdsClient> mdsClient_;
    std::shared_ptr<MockBlockDeviceClient> devClient_;
    std::unique_ptr<SpaceManagerImpl> spaceManager_;
};

TEST_F(SpaceManagerImplTest, TestAlloc) {
    mds::space::BlockGroup group;
    group.set_offset(0);
    group.set_size(kBlockGroupSize);
    group.set_available(kBlockGroupSize);
    group.set_bitmaplocation(curvefs::common::BitmapLocation::AtStart);

    EXPECT_CALL(*mdsClient_, AllocateVolumeBlockGroup(_, _, _, _))
        .WillOnce(Invoke(MockAllocateBlockGroup{group}));

    EXPECT_CALL(*devClient_, Read(_, _, _))
        .Times(AtLeast(1))
        .WillRepeatedly(Invoke(MockRead));

    EXPECT_CALL(*devClient_, Write(_, _, _))
        .Times(AtLeast(1))
        .WillRepeatedly(Invoke(MockWrite));

    int64_t left = opt_.blockGroupManagerOption.blockGroupSize -
                   opt_.blockGroupManagerOption.blockSize;
    std::vector<Extent> extents;

    while (left > 0) {
        std::vector<Extent> ext;
        ASSERT_TRUE(spaceManager_->Alloc(kBlockSize, {}, &ext));
        extents.insert(extents.end(), std::make_move_iterator(ext.begin()),
                       std::make_move_iterator(ext.end()));

        left -= kBlockSize;
    }

    std::sort(extents.begin(), extents.end(),
              [](const Extent &e1, const Extent &e2) {
                  return e1.offset < e2.offset;
              });

    ASSERT_EQ(opt_.blockGroupManagerOption.blockGroupSize / kBlockSize - 1,
              extents.size());
}

TEST_F(SpaceManagerImplTest, TestMultiThreadAlloc) {
    mds::space::BlockGroup group;
    group.set_offset(0);
    group.set_size(kBlockGroupSize);
    group.set_available(kBlockGroupSize);
    group.set_bitmaplocation(curvefs::common::BitmapLocation::AtStart);

    mds::space::BlockGroup group2(group);
    group2.set_offset(kBlockGroupSize);

    EXPECT_CALL(*mdsClient_, AllocateVolumeBlockGroup(_, _, _, _))
        .WillOnce(Invoke(MockAllocateBlockGroup{group}))
        .WillOnce(Invoke(MockAllocateBlockGroup{group2}));

    EXPECT_CALL(*devClient_, Read(_, _, _))
        .Times(AtLeast(1))
        .WillRepeatedly(Invoke(MockRead));

    EXPECT_CALL(*devClient_, Write(_, _, _))
        .Times(AtLeast(1))
        .WillRepeatedly(Invoke(MockWrite));

    std::atomic<int64_t> left(kBlockGroupSize);

    auto task = [&](std::vector<Extent> *extents) {
        while (left.fetch_sub(kBlockSize, std::memory_order_relaxed) > 0) {
            std::vector<Extent> ext;
            spaceManager_->Alloc(kBlockSize, {}, &ext);
            extents->insert(extents->end(),
                            std::make_move_iterator(ext.begin()),
                            std::make_move_iterator(ext.end()));
        }
    };

    std::vector<TaskStruct> tasks;
    const int total = rand() % 10 + 1;  // NOLINT[runtime/threadsafe_fn]
    tasks.resize(total);
    for (int i = 0; i < total; ++i) {
        tasks[i].th = std::thread{task, &tasks[i].extent};
    }

    for (int i = total - 1; i >= 0; --i) {
        tasks[i].th.join();
    }

    int64_t totalAllocated = 0;
    for (const auto& t : tasks) {
        for (const auto& ext : t.extent) {
            totalAllocated += ext.len;
        }
    }

    ASSERT_EQ(kBlockGroupSize, totalAllocated);
}

}  // namespace volume
}  // namespace curvefs
