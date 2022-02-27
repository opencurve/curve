/*
 *  Copyright (c) 2021 NetEase Inc.
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

#include <algorithm>
#include <ctime>
#include <numeric>
#include <tuple>

#include "curvefs/src/volume/bitmap_allocator.h"
#include "curvefs/test/volume/common.h"

namespace curvefs {
namespace volume {

static const std::vector<uint64_t> kAllocSizes = {
    4 * kKiB, 16 * kKiB, 32 * kKiB, 128 * kKiB, 512 * kKiB,
    1 * kMiB, 4 * kMiB,  32 * kMiB, 128 * kMiB, 512 * kMiB,
    1 * kGiB, 4 * kGiB,  32 * kGiB, 128 * kGiB, 512 * kGiB};

static const std::vector<AllocateType> kAllocTypes = {
    AllocateType::None, AllocateType::Small, AllocateType::Big};

class BitmapAllocatorBruteTests
    : public ::testing::TestWithParam<BitmapAllocatorOption> {
 protected:
    void SetUp() override {
        opt_ = GetParam();
        allocator_.reset(new BitmapAllocator(opt_));
    }

 protected:
    BitmapAllocatorOption opt_;
    std::unique_ptr<BitmapAllocator> allocator_;
};

TEST_P(BitmapAllocatorBruteTests, TestAllocAllSpace) {
    for (int i = 0; i < 3; ++i) {
        LOG(INFO) << "loop " << i << ", " << *allocator_;
        Extents exts;

        // alloc all available space
        while (allocator_->AvailableSize() > 0) {
            AllocateHint hint;
            auto allocSize = kAllocSizes[rand() % kAllocSizes.size()];
            hint.allocType = kAllocTypes[rand() % kAllocTypes.size()];

            allocSize = std::min(allocSize, allocator_->AvailableSize());
            ASSERT_EQ(allocSize, allocator_->Alloc(allocSize, hint, &exts));
        }

        ASSERT_EQ(0, allocator_->AvailableSize());
        ASSERT_EQ(opt_.length, TotalLength(exts));

        // test no overlap extents
        ASSERT_TRUE(ExtentsContinuous(exts));

        // return all allocated extents
        allocator_->DeAlloc(exts);
        ASSERT_EQ(opt_.length, allocator_->AvailableSize());
    }
}

INSTANTIATE_TEST_CASE_P(
    BitmapAllocatorTests, BitmapAllocatorBruteTests,
    ::testing::Values(BitmapAllocatorOption{.startOffset = 0,
                                            .length = 10 * kGiB,
                                            .sizePerBit = 4 * kMiB,
                                            .smallAllocProportion = 0},
                      BitmapAllocatorOption{.startOffset = 0,
                                            .length = 10 * kGiB,
                                            .sizePerBit = 4 * kMiB,
                                            .smallAllocProportion = 0.2},
                      BitmapAllocatorOption{.startOffset = 0,
                                            .length = 10 * kGiB,
                                            .sizePerBit = 4 * kMiB,
                                            .smallAllocProportion = 0.5},
                      BitmapAllocatorOption{.startOffset = 0,
                                            .length = 10 * kGiB,
                                            .sizePerBit = 4 * kMiB,
                                            .smallAllocProportion = 1},

                      BitmapAllocatorOption{.startOffset = 0,
                                            .length = 10 * kGiB,
                                            .sizePerBit = 16 * kMiB,
                                            .smallAllocProportion = 0},
                      BitmapAllocatorOption{.startOffset = 0,
                                            .length = 10 * kGiB,
                                            .sizePerBit = 16 * kMiB,
                                            .smallAllocProportion = 0.2},
                      BitmapAllocatorOption{.startOffset = 0,
                                            .length = 10 * kGiB,
                                            .sizePerBit = 16 * kMiB,
                                            .smallAllocProportion = 0.5},
                      BitmapAllocatorOption{.startOffset = 0,
                                            .length = 10 * kGiB,
                                            .sizePerBit = 16 * kMiB,
                                            .smallAllocProportion = 1},

                      BitmapAllocatorOption{.startOffset = 100 * kGiB,
                                            .length = 100 * kGiB,
                                            .sizePerBit = 4 * kMiB,
                                            .smallAllocProportion = 0},
                      BitmapAllocatorOption{.startOffset = 100 * kGiB,
                                            .length = 100 * kGiB,
                                            .sizePerBit = 4 * kMiB,
                                            .smallAllocProportion = 0.2},
                      BitmapAllocatorOption{.startOffset = 100 * kGiB,
                                            .length = 100 * kGiB,
                                            .sizePerBit = 4 * kMiB,
                                            .smallAllocProportion = 0.5},
                      BitmapAllocatorOption{.startOffset = 100 * kGiB,
                                            .length = 100 * kGiB,
                                            .sizePerBit = 4 * kMiB,
                                            .smallAllocProportion = 1},

                      BitmapAllocatorOption{.startOffset = 1 * kTiB,
                                            .length = 1 * kTiB,
                                            .sizePerBit = 4 * kMiB,
                                            .smallAllocProportion = 0},
                      BitmapAllocatorOption{.startOffset = 1 * kTiB,
                                            .length = 1 * kTiB,
                                            .sizePerBit = 4 * kMiB,
                                            .smallAllocProportion = 0.2},
                      BitmapAllocatorOption{.startOffset = 1 * kTiB,
                                            .length = 1 * kTiB,
                                            .sizePerBit = 4 * kMiB,
                                            .smallAllocProportion = 0.5},
                      BitmapAllocatorOption{.startOffset = 1 * kTiB,
                                            .length = 1 * kTiB,
                                            .sizePerBit = 4 * kMiB,
                                            .smallAllocProportion = 1}));

}  // namespace volume
}  // namespace curvefs
