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
#include <thread>
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

class BitmapAllocatorMultiThreadTests
    : public ::testing::TestWithParam<std::pair<int, BitmapAllocatorOption>> {
 protected:
    void SetUp() override {
        threadNum_ = GetParam().first;
        opt_ = GetParam().second;
        allocator_.reset(new BitmapAllocator(opt_));
    }

    void StartTestThreads() {
        exts.clear();
        ths.clear();

        exts.resize(threadNum_);

        for (int i = 0; i < threadNum_; ++i) {
            ths.emplace_back(&BitmapAllocatorMultiThreadTests::ThreadTask, this,
                             i);
        }
    }

    void ThreadTask(int idx) {
        while (allocator_->AvailableSize() > 0) {
            AllocateHint hint;
            hint.allocType = kAllocTypes[rand() % kAllocTypes.size()];
            auto allocSize = kAllocSizes[rand() % kAllocSizes.size()];
            auto allocated = allocator_->Alloc(allocSize, hint, &exts[idx]);

            if (allocated == 0) {
                break;
            }
        }

        LOG(INFO) << "thread: " << idx
                  << " exited, available: " << allocator_->AvailableSize();
    }

 protected:
    BitmapAllocatorOption opt_;
    std::unique_ptr<BitmapAllocator> allocator_;

    int threadNum_;
    std::vector<std::thread> ths;
    std::vector<Extents> exts;
};

TEST_P(BitmapAllocatorMultiThreadTests, TestMultiThreadAllocAllSpace) {
    for (int i = 0; i < 3; ++i) {
        LOG(INFO) << "loop " << i << ", " << *allocator_;
        ASSERT_EQ(opt_.length, allocator_->AvailableSize());

        StartTestThreads();

        for (auto& t : ths) {
            t.join();
        }

        ASSERT_EQ(0, allocator_->AvailableSize());

        Extents allExts;
        for (auto& e : exts) {
            allExts.insert(allExts.end(), e.begin(), e.end());
        }

        ASSERT_TRUE(ExtentsContinuous(allExts));
        ASSERT_EQ(opt_.length, TotalLength(allExts));

        allocator_->DeAlloc(allExts);
        ASSERT_EQ(opt_.length, allocator_->AvailableSize());
    }
}

INSTANTIATE_TEST_CASE_P(
    BitmapAllocatorMultiThreadTests,
    BitmapAllocatorMultiThreadTests,
    ::testing::Values(
        std::make_pair(4, BitmapAllocatorOption{.startOffset = 0,
                                                .length = 10 * kGiB,
                                                .sizePerBit = 4 * kMiB,
                                                .smallAllocProportion = 0}),
        std::make_pair(4, BitmapAllocatorOption{.startOffset = 0,
                                                .length = 10 * kGiB,
                                                .sizePerBit = 4 * kMiB,
                                                .smallAllocProportion = 0.2}),
        std::make_pair(4, BitmapAllocatorOption{.startOffset = 0,
                                                .length = 10 * kGiB,
                                                .sizePerBit = 4 * kMiB,
                                                .smallAllocProportion = 0.5}),
        std::make_pair(4, BitmapAllocatorOption{.startOffset = 0,
                                                .length = 10 * kGiB,
                                                .sizePerBit = 4 * kMiB,
                                                .smallAllocProportion = 1}),

        std::make_pair(8, BitmapAllocatorOption{.startOffset = 100 * kGiB,
                                                .length = 100 * kGiB,
                                                .sizePerBit = 4 * kMiB,
                                                .smallAllocProportion = 0}),
        std::make_pair(8, BitmapAllocatorOption{.startOffset = 100 * kGiB,
                                                .length = 100 * kGiB,
                                                .sizePerBit = 4 * kMiB,
                                                .smallAllocProportion = 0.2}),
        std::make_pair(8, BitmapAllocatorOption{.startOffset = 100 * kGiB,
                                                .length = 100 * kGiB,
                                                .sizePerBit = 4 * kMiB,
                                                .smallAllocProportion = 0.5}),
        std::make_pair(8, BitmapAllocatorOption{.startOffset = 100 * kGiB,
                                                .length = 100 * kGiB,
                                                .sizePerBit = 4 * kMiB,
                                                .smallAllocProportion = 1}),

        std::make_pair(16, BitmapAllocatorOption{.startOffset = 1 * kTiB,
                                                 .length = 1 * kTiB,
                                                 .sizePerBit = 4 * kMiB,
                                                 .smallAllocProportion = 0}),
        std::make_pair(16, BitmapAllocatorOption{.startOffset = 1 * kTiB,
                                                 .length = 1 * kTiB,
                                                 .sizePerBit = 4 * kMiB,
                                                 .smallAllocProportion = 0.2}),
        std::make_pair(16, BitmapAllocatorOption{.startOffset = 1 * kTiB,
                                                 .length = 1 * kTiB,
                                                 .sizePerBit = 4 * kMiB,
                                                 .smallAllocProportion = 0.5}),
        std::make_pair(16, BitmapAllocatorOption{.startOffset = 1 * kTiB,
                                                 .length = 1 * kTiB,
                                                 .sizePerBit = 4 * kMiB,
                                                 .smallAllocProportion = 1}),

        std::make_pair(32, BitmapAllocatorOption{.startOffset = 50 * kGiB,
                                                 .length = 100 * kTiB,
                                                 .sizePerBit = 16 * kMiB,
                                                 .smallAllocProportion = 0}),
        std::make_pair(32, BitmapAllocatorOption{.startOffset = 50 * kGiB,
                                                 .length = 100 * kTiB,
                                                 .sizePerBit = 16 * kMiB,
                                                 .smallAllocProportion = 0.2}),
        std::make_pair(32, BitmapAllocatorOption{.startOffset = 50 * kGiB,
                                                 .length = 100 * kTiB,
                                                 .sizePerBit = 16 * kMiB,
                                                 .smallAllocProportion = 0.5}),
        std::make_pair(32, BitmapAllocatorOption{.startOffset = 50 * kGiB,
                                                 .length = 100 * kTiB,
                                                 .sizePerBit = 16 * kMiB,
                                                 .smallAllocProportion = 1})));

}  // namespace volume
}  // namespace curvefs

