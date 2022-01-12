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

#include "curvefs/src/space/common.h"
#include "curvefs/src/space/free_extents.h"

namespace curvefs {
namespace space {

TEST(ExtentTest, TestAsSmallAllocator) {
    FreeExtents freeExt(0, 16 * kMiB);
    SpaceAllocateHint hint;

    ASSERT_EQ(16 * kMiB, freeExt.AvailableSize());
    ASSERT_EQ(1, freeExt.AvailableExtents().size());

    Extents exts;
    ASSERT_EQ(4 * kMiB, freeExt.Alloc(4 * kMiB, hint, &exts));
    ASSERT_EQ(4 * kMiB, freeExt.Alloc(4 * kMiB, hint, &exts));
    ASSERT_EQ(4 * kMiB, freeExt.Alloc(4 * kMiB, hint, &exts));
    ASSERT_EQ(4 * kMiB, freeExt.Alloc(4 * kMiB, hint, &exts));
    ASSERT_EQ(0, freeExt.Alloc(4 * kMiB, hint, &exts));

    ASSERT_EQ(4, exts.size());

    freeExt.DeAlloc(0, 4 * kMiB);
    freeExt.DeAlloc(8 * kMiB, 4 * kMiB);

    auto m = freeExt.AvailableExtents();
    ASSERT_EQ(2, m.size());

    freeExt.DeAlloc(4 * kMiB, 4 * kMiB);
    freeExt.DeAlloc(12 * kMiB, 4 * kMiB);

    m = freeExt.AvailableExtents();
    ASSERT_EQ(1, m.size());
    ASSERT_EQ(0, m.begin()->first);
    ASSERT_EQ(16 * kMiB, m.begin()->second);
    ASSERT_EQ(16 * kMiB, freeExt.AvailableSize());
}

TEST(ExtentTest, TestAsBitAllocator) {
    FreeExtents freeExt(4 * kMiB);

    ASSERT_EQ(0, freeExt.AvailableSize());
    ASSERT_TRUE(freeExt.AvailableExtents().empty());

    freeExt.DeAlloc(2 * kMiB, 2 * kMiB);
    freeExt.DeAlloc(6 * kMiB, 2 * kMiB);
    freeExt.DeAlloc(10 * kMiB, 2 * kMiB);
    freeExt.DeAlloc(14 * kMiB, 2 * kMiB);

    ASSERT_EQ(8 * kMiB, freeExt.AvailableSize());
    ASSERT_EQ(4, freeExt.AvailableExtents().size());

    freeExt.DeAlloc(0 * kMiB, 2 * kMiB);
    freeExt.DeAlloc(4 * kMiB, 2 * kMiB);
    freeExt.DeAlloc(8 * kMiB, 2 * kMiB);
    freeExt.DeAlloc(12 * kMiB, 2 * kMiB);
    ASSERT_EQ(16 * kMiB, freeExt.AvailableSize());
    ASSERT_TRUE(freeExt.AvailableExtents().empty());

    ExtentMap blocks;
    ASSERT_EQ(16 * kMiB, freeExt.AvailableBlocks(&blocks));

    // after get available blocks, this extents should be empty
    ASSERT_EQ(0, freeExt.AvailableSize());
    ASSERT_EQ(0, freeExt.AvailableBlocks(&blocks));
    ASSERT_TRUE(blocks.empty());
}

TEST(ExtentTest, TestAsBitAllocator2) {
    FreeExtents freeExt(4 * kMiB);

    ASSERT_EQ(0, freeExt.AvailableSize());
    ASSERT_TRUE(freeExt.AvailableExtents().empty());

    freeExt.DeAlloc(2 * kMiB, 1 * kMiB);
    freeExt.DeAlloc(5 * kMiB, 1 * kMiB);

    // after this, [off: 2MiB ~ len: 4MiB] is continuous
    // but its offset is not aligned to maxExtentSize
    freeExt.DeAlloc(3 * kMiB, 2 * kMiB);

    ASSERT_EQ(4 * kMiB, freeExt.AvailableSize());
    ASSERT_EQ(1, freeExt.AvailableExtents().size());

    ExtentMap blocks;
    ASSERT_EQ(0, freeExt.AvailableBlocks(&blocks));
    ASSERT_TRUE(blocks.empty());

    freeExt.DeAlloc(9 * kMiB, 2 * kMiB);
    freeExt.DeAlloc(13 * kMiB, 2 * kMiB);

    ASSERT_EQ(8 * kMiB, freeExt.AvailableSize());
    ASSERT_EQ(3, freeExt.AvailableExtents().size());

    freeExt.DeAlloc(0 * kMiB, 2 * kMiB);
    freeExt.DeAlloc(6 * kMiB, 3 * kMiB);
    freeExt.DeAlloc(11 * kMiB, 2 * kMiB);
    freeExt.DeAlloc(15 * kMiB, 1 * kMiB);
    ASSERT_EQ(16 * kMiB, freeExt.AvailableSize());
    ASSERT_TRUE(freeExt.AvailableExtents().empty());

    blocks.clear();
    ASSERT_EQ(16 * kMiB, freeExt.AvailableBlocks(&blocks));

    // after get available blocks, this extents should be empty
    ASSERT_EQ(0, freeExt.AvailableSize());
    ASSERT_EQ(0, freeExt.AvailableBlocks(&blocks));
    ASSERT_TRUE(blocks.empty());
}

TEST(ExtentTest, TestAsSmallAllocatorWithHint) {
    // left offset hint
    {
        FreeExtents freeExt(16 * kMiB, 16 * kMiB);
        SpaceAllocateHint hint;

        Extents ext1;
        ASSERT_EQ(4 * kMiB, freeExt.Alloc(4 * kMiB, hint, &ext1));
        ASSERT_EQ(4 * kMiB, freeExt.Alloc(4 * kMiB, hint, &ext1));
        ASSERT_EQ(4 * kMiB, freeExt.Alloc(4 * kMiB, hint, &ext1));

        freeExt.DeAlloc(20 * kMiB, 4 * kMiB);

        ASSERT_EQ(2, freeExt.AvailableExtents().size());
        ASSERT_EQ(8 * kMiB, freeExt.AvailableSize());

        ext1.clear();
        hint.leftOffset = 20 * kMiB;
        ASSERT_EQ(4 * kMiB, freeExt.Alloc(4 * kMiB, hint, &ext1));
        ASSERT_EQ(ext1[0], PExtent(hint.leftOffset, 4 * kMiB));

        ASSERT_EQ(4 * kMiB, freeExt.AvailableSize());
    }

    // right offset hint
    {
        FreeExtents freeExt(16 * kMiB, 16 * kMiB);
        SpaceAllocateHint hint;

        Extents ext1;
        ASSERT_EQ(4 * kMiB, freeExt.Alloc(4 * kMiB, hint, &ext1));
        ASSERT_EQ(4 * kMiB, freeExt.Alloc(4 * kMiB, hint, &ext1));
        ASSERT_EQ(4 * kMiB, freeExt.Alloc(4 * kMiB, hint, &ext1));

        freeExt.DeAlloc(20 * kMiB, 4 * kMiB);

        ext1.clear();
        hint.rightOffset = 24 * kMiB;
        ASSERT_EQ(4 * kMiB, freeExt.Alloc(4 * kMiB, hint, &ext1));
        ASSERT_EQ(ext1[0], PExtent(hint.rightOffset - 4 * kMiB, 4 * kMiB));

        ASSERT_EQ(4 * kMiB, freeExt.AvailableSize());
    }

    // right offset hint 2
    {
        FreeExtents freeExt(16 * kMiB, 16 * kMiB);
        SpaceAllocateHint hint;

        Extents ext1;
        ASSERT_EQ(16 * kMiB, freeExt.Alloc(16 * kMiB, hint, &ext1));

        // freeExt.DeAlloc(18 * kMiB, 12 * kMiB);
        freeExt.DeAlloc(16 * kMiB, 2 * kMiB);
        freeExt.DeAlloc(30 * kMiB, 2 * kMiB);

        ext1.clear();
        hint.rightOffset = 18 * kMiB;
        ASSERT_EQ(4 * kMiB, freeExt.Alloc(4 * kMiB, hint, &ext1));

        Extents expected = {{16 * kMiB, 2 * kMiB}, {30 * kMiB, 2 * kMiB}};
        ASSERT_EQ(ext1, expected);

        ASSERT_EQ(0, freeExt.AvailableSize());
        ASSERT_TRUE(freeExt.AvailableExtents().empty());
    }

    // right offset hint 3
    {
        FreeExtents freeExt(0, 16 * kMiB);
        SpaceAllocateHint hint;

        Extents ext1;
        ASSERT_EQ(16 * kMiB, freeExt.Alloc(16 * kMiB, hint, &ext1));

        // freeExt.DeAlloc(18 * kMiB, 12 * kMiB);
        freeExt.DeAlloc(0, 2 * kMiB);
        freeExt.DeAlloc(14 * kMiB, 2 * kMiB);

        ext1.clear();
        hint.rightOffset = 2 * kMiB;
        ASSERT_EQ(4 * kMiB, freeExt.Alloc(4 * kMiB, hint, &ext1));

        Extents expected = {{0 * kMiB, 2 * kMiB}, {14 * kMiB, 2 * kMiB}};
        ASSERT_EQ(ext1, expected);

        ASSERT_EQ(0, freeExt.AvailableSize());
        ASSERT_TRUE(freeExt.AvailableExtents().empty());
    }

    // found a extent that satisfy left offset, but its smaller
    {
        FreeExtents freeExt(16 * kMiB, 16 * kMiB);
        SpaceAllocateHint hint;

        Extents ext1;
        ASSERT_EQ(4 * kMiB, freeExt.Alloc(4 * kMiB, hint, &ext1));
        ASSERT_EQ(4 * kMiB, freeExt.Alloc(4 * kMiB, hint, &ext1));
        ASSERT_EQ(4 * kMiB, freeExt.Alloc(4 * kMiB, hint, &ext1));
        ASSERT_EQ(4 * kMiB, freeExt.Alloc(4 * kMiB, hint, &ext1));

        freeExt.DeAlloc(20 * kMiB, 4 * kMiB);
        freeExt.DeAlloc(28 * kMiB, 4 * kMiB);

        ASSERT_EQ(2, freeExt.AvailableExtents().size());
        ASSERT_EQ(8 * kMiB, freeExt.AvailableSize());

        ext1.clear();
        hint.leftOffset = 20 * kMiB;
        ASSERT_EQ(8 * kMiB, freeExt.Alloc(8 * kMiB, hint, &ext1));
        ASSERT_EQ(ext1[0], PExtent(hint.leftOffset, 4 * kMiB));
        ASSERT_EQ(ext1[1], PExtent(28 * kMiB, 4 * kMiB));

        ASSERT_EQ(0, freeExt.AvailableSize());
    }

    // found a extent that satisfy right offset, but its smaller
    {
        FreeExtents freeExt(16 * kMiB, 16 * kMiB);
        SpaceAllocateHint hint;

        Extents ext1;
        ASSERT_EQ(4 * kMiB, freeExt.Alloc(4 * kMiB, hint, &ext1));
        ASSERT_EQ(4 * kMiB, freeExt.Alloc(4 * kMiB, hint, &ext1));
        ASSERT_EQ(4 * kMiB, freeExt.Alloc(4 * kMiB, hint, &ext1));
        ASSERT_EQ(4 * kMiB, freeExt.Alloc(4 * kMiB, hint, &ext1));

        freeExt.DeAlloc(16 * kMiB, 4 * kMiB);
        freeExt.DeAlloc(24 * kMiB, 4 * kMiB);

        ASSERT_EQ(2, freeExt.AvailableExtents().size());
        ASSERT_EQ(8 * kMiB, freeExt.AvailableSize());

        ext1.clear();
        hint.rightOffset = 20 * kMiB;
        ASSERT_EQ(8 * kMiB, freeExt.Alloc(8 * kMiB, hint, &ext1));
        ASSERT_EQ(ext1[0], PExtent(16 * kMiB, 4 * kMiB));
        ASSERT_EQ(ext1[1], PExtent(24 * kMiB, 4 * kMiB));

        ASSERT_EQ(0, freeExt.AvailableSize());
    }

    // extents are fragment
    {
        FreeExtents freeExt(16 * kMiB, 16 * kMiB);
        SpaceAllocateHint hint;

        Extents ext1;
        ASSERT_EQ(16 * kMiB, freeExt.Alloc(16 * kMiB, hint, &ext1));

        freeExt.DeAlloc(16 * kMiB, 1 * kMiB);
        freeExt.DeAlloc(18 * kMiB, 1 * kMiB);
        freeExt.DeAlloc(20 * kMiB, 1 * kMiB);
        freeExt.DeAlloc(22 * kMiB, 1 * kMiB);
        freeExt.DeAlloc(24 * kMiB, 1 * kMiB);
        freeExt.DeAlloc(26 * kMiB, 1 * kMiB);
        freeExt.DeAlloc(28 * kMiB, 1 * kMiB);
        freeExt.DeAlloc(30 * kMiB, 1 * kMiB);

        ASSERT_EQ(8, freeExt.AvailableExtents().size());
        ASSERT_EQ(8 * kMiB, freeExt.AvailableSize());

        ext1.clear();
        hint.leftOffset = 16 * kMiB;
        ASSERT_EQ(8 * kMiB, freeExt.Alloc(8 * kMiB, hint, &ext1));

        Extents expected = {{16 * kMiB, 1 * kMiB}, {18 * kMiB, 1 * kMiB},
                            {20 * kMiB, 1 * kMiB}, {22 * kMiB, 1 * kMiB},
                            {24 * kMiB, 1 * kMiB}, {26 * kMiB, 1 * kMiB},
                            {28 * kMiB, 1 * kMiB}, {30 * kMiB, 1 * kMiB}};

        ASSERT_EQ(ext1, expected);
        ASSERT_EQ(0, freeExt.AvailableSize());
    }
}

TEST(ExtentTest, TestMarkUsed) {
    FreeExtents freeExt(16 * kMiB, 16 * kMiB);
    EXPECT_EQ(16 * kMiB, freeExt.AvailableSize());
    EXPECT_EQ(1, freeExt.AvailableExtents().size());

    freeExt.MarkUsed(20 * kMiB, 4 * kMiB);
    EXPECT_EQ(12 * kMiB, freeExt.AvailableSize());
    EXPECT_EQ(2, freeExt.AvailableExtents().size());

    freeExt.MarkUsed(18 * kMiB, 2 * kMiB);
    EXPECT_EQ(10 * kMiB, freeExt.AvailableSize());
    EXPECT_EQ(2, freeExt.AvailableExtents().size());

    freeExt.MarkUsed(28 * kMiB, 4 * kMiB);
    EXPECT_EQ(6 * kMiB, freeExt.AvailableSize());
    EXPECT_EQ(2, freeExt.AvailableExtents().size());

    freeExt.MarkUsed(24 * kMiB, 2 * kMiB);
    EXPECT_EQ(4 * kMiB, freeExt.AvailableSize());
    EXPECT_EQ(2, freeExt.AvailableExtents().size());

    freeExt.MarkUsed(26 * kMiB, 2 * kMiB);
    EXPECT_EQ(2 * kMiB, freeExt.AvailableSize());
    EXPECT_EQ(1, freeExt.AvailableExtents().size());

    freeExt.MarkUsed(16 * kMiB, 2 * kMiB);
    EXPECT_EQ(0, freeExt.AvailableSize());
    EXPECT_EQ(0, freeExt.AvailableExtents().size());
}

}  // namespace space
}  // namespace curvefs

int main(int argc, char* argv[]) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
