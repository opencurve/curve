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

#include "curvefs/src/space_allocator/bitmap_allocator.h"

#include <gtest/gtest.h>

#include "curvefs/test/space_allocator/common.h"

namespace curvefs {
namespace space {

class BitmapAllocatorTest : public ::testing::Test {
 protected:
    void SetUp() override {
        opt_.startOffset = 0;
        opt_.length = 10 * kGiB;
        opt_.sizePerBit = 4 * kMiB;
        opt_.smallAllocProportion = 0.2;

        allocator_.reset(new BitmapAllocator(opt_));
    }

    void TearDown() override {}

 protected:
    BitmapAllocatorOption opt_;
    std::unique_ptr<BitmapAllocator> allocator_;
};

TEST_F(BitmapAllocatorTest, CommonTest) {
    ASSERT_EQ(opt_.startOffset, allocator_->StartOffset());
    ASSERT_EQ(opt_.length, allocator_->Total());
    ASSERT_EQ(opt_.length, allocator_->AvailableSize());
}

TEST_F(BitmapAllocatorTest, AllocFromSmallExtentTest) {
    uint64_t allocSize = opt_.sizePerBit / 2;

    {
        std::vector<PExtent> exts;
        SpaceAllocateHint hint;
        hint.allocType = AllocateType::SMALL;

        ASSERT_EQ(allocSize, allocator_->Alloc(allocSize, hint, &exts));
        ASSERT_EQ(1, exts.size());

        std::vector<PExtent> expected = {{opt_.startOffset, allocSize}};
        ASSERT_EQ(expected, exts);

        ASSERT_EQ(opt_.length - allocSize, allocator_->AvailableSize());
        allocator_->DeAlloc(exts);
        ASSERT_EQ(opt_.length, allocator_->AvailableSize());
    }

    {
        std::vector<PExtent> exts;
        SpaceAllocateHint hint;
        hint.allocType = AllocateType::NONE;

        ASSERT_EQ(allocSize, allocator_->Alloc(allocSize, hint, &exts));
        ASSERT_EQ(1, exts.size());

        std::vector<PExtent> expected = {{opt_.startOffset, allocSize}};
        ASSERT_EQ(expected, exts);

        ASSERT_EQ(opt_.length - allocSize, allocator_->AvailableSize());
        allocator_->DeAlloc(exts);
        ASSERT_EQ(opt_.length, allocator_->AvailableSize());
    }
}

TEST_F(BitmapAllocatorTest, AllocFromBitmap) {
    // alloc size >= opt_.sizePerBit
    {
        std::vector<PExtent> exts;
        SpaceAllocateHint hint;
        hint.allocType = AllocateType::SMALL;
        uint64_t allocSize = opt_.sizePerBit;

        ASSERT_EQ(allocSize, allocator_->Alloc(allocSize, hint, &exts));
        ASSERT_EQ(allocSize / opt_.sizePerBit, exts.size());

        std::vector<PExtent> expected = {
            PExtent(opt_.startOffset + opt_.length * opt_.smallAllocProportion,
                   allocSize)};

        ASSERT_EQ(expected, exts);

        ASSERT_EQ(opt_.length - allocSize, allocator_->AvailableSize());
        allocator_->DeAlloc(exts);
        ASSERT_EQ(opt_.length, allocator_->AvailableSize());
    }

    {
        allocator_.reset(new BitmapAllocator(opt_));
        std::vector<PExtent> exts;
        SpaceAllocateHint hint;
        hint.allocType = AllocateType::BIG;

        uint64_t allocSize = opt_.length * (1 - opt_.smallAllocProportion) / 2;
        ASSERT_EQ(allocSize, allocator_->Alloc(allocSize, hint, &exts));
        ASSERT_EQ(allocSize / opt_.sizePerBit, exts.size());

        uint64_t curOff =
            opt_.startOffset + opt_.length * opt_.smallAllocProportion;
        for (auto& e : exts) {
            ASSERT_EQ(e.offset, curOff);
            ASSERT_EQ(e.len, opt_.sizePerBit);
            curOff += opt_.sizePerBit;
        }

        ASSERT_EQ(opt_.length - allocSize, allocator_->AvailableSize());
        allocator_->DeAlloc(exts);
        ASSERT_EQ(opt_.length, allocator_->AvailableSize());
    }
}

TEST_F(BitmapAllocatorTest, AllocSmallTest3) {
    opt_.length = 512 * kMiB;
    opt_.startOffset = 0;
    opt_.sizePerBit = 16 * kMiB;
    opt_.smallAllocProportion = 0;

    allocator_.reset(new BitmapAllocator(opt_));

    std::vector<PExtent> exts;
    SpaceAllocateHint hint;
    hint.allocType = AllocateType::BIG;

    auto allocSize = allocator_->Alloc(128 * kKiB, hint, &exts);
    ASSERT_EQ(allocSize, 128 *kKiB);
    hint.allocType = AllocateType::SMALL;
    hint.leftOffset = exts.back().len + exts.back().offset;
    std::vector<PExtent> ext2;
    allocSize = allocator_->Alloc(128*kKiB, hint, &ext2);
    ASSERT_EQ(allocSize, 128 *kKiB);
    ASSERT_EQ(exts[0].offset + exts[0].len, ext2[0].offset);
}

TEST_F(BitmapAllocatorTest, Alloc4) {
    opt_.length = 8 * kMiB;
    opt_.startOffset = 0;
    opt_.sizePerBit = 4 * kMiB;
    opt_.smallAllocProportion = 0;

    allocator_.reset(new BitmapAllocator(opt_));

    std::vector<PExtent> exts;
    auto alloc = allocator_->Alloc(opt_.length, {}, &exts);
    (void)alloc;

    std::vector<PExtent> deallocExts = {
        {2097152, 256 * kKiB}, {2359296, 256 * kKiB},
        {2621440, 256 * kKiB}, {2883584, 256 * kKiB},

        {3145728, 256 * kKiB}, {3407872, 256 * kKiB},
        {3670016, 256 * kKiB}, {3932160, 256 * kKiB},

        {4456448, 256 * kKiB}, {4718592, 256 * kKiB},
        {4980736, 256 * kKiB}, {5242880, 256 * kKiB},

        {5505024, 256 * kKiB}, {5767168, 256 * kKiB},
        {6029312, 256 * kKiB}, {6291456, 256 * kKiB},

        {6553600, 256 * kKiB}, {6815744, 256 * kKiB},
        {7077888, 256 * kKiB}, {7340032, 256 * kKiB},

        {7602176, 256 * kKiB}, {7864320, 256 * kKiB},
        {8126464, 256 * kKiB}, {0, 256 * kKiB},

        {262144, 256 * kKiB}, {4194304, 256 * kKiB},
        {524288, 256 * kKiB},  {786432, 256 * kKiB},

        {1048576, 256 * kKiB}, {1310720, 256 * kKiB},
        {1572864, 256 * kKiB}, {1835008, 256 * kKiB},
    };

    allocator_->DeAlloc(deallocExts);
    auto len = allocator_->AvailableSize();
    (void)len;
}

TEST_F(BitmapAllocatorTest, TestMarkUsed) {
    // [0, 2kGiB] for small allocate
    // [2kGiB, 8, kGiB] for bit allocate
    std::vector<PExtent> used = {{1 * kGiB, 2 * kGiB}};
    allocator_->MarkUsed(used);
    EXPECT_EQ(8 * kGiB, allocator_->AvailableSize());

    std::vector<PExtent> exts;
    EXPECT_EQ(8 * kGiB, allocator_->Alloc(8 * kGiB, {}, &exts));

    auto sorted = SortAndMerge(exts);
    EXPECT_EQ(2, sorted.size());
    EXPECT_EQ(PExtent(0, 1 * kGiB), sorted[0]);
    EXPECT_EQ(PExtent(3 * kGiB, 7 * kGiB), sorted[1]);
}

}  // namespace space
}  // namespace curvefs

int main(int argc, char* argv[]) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
