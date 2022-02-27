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
 * Date: Tuesday Mar 15 20:07:11 CST 2022
 * Author: wuhanqing
 */

#include <gtest/gtest.h>

#include "curvefs/src/client/volume/extent_cache.h"
#include "curvefs/test/client/volume/common.h"

namespace curvefs {
namespace client {

// no extents
TEST(ExtentCacheMergeTest, MergeCase1) {
    ExtentCache cache;

    PExtent pext;
    pext.pOffset = 16 * kMiB;
    pext.len = 4 * kMiB;
    pext.UnWritten = false;

    cache.Merge(32 * kMiB, pext);

    auto extents = cache.GetExtentsForTesting();
    ASSERT_TRUE(extents.count(0));

    auto& range = extents[0];
    ASSERT_EQ(1, range.size());
    ASSERT_TRUE(range.count(32 * kMiB));

    ASSERT_EQ(pext, range[32 * kMiB]);
}

// adding extent              |----|
// existing extents                  |----|
TEST(ExtentCacheMergeTest, MergeCase2) {
    ExtentCache cache;

    PExtent existing;
    existing.pOffset = 16 * kMiB;
    existing.len = 4 * kMiB;
    existing.UnWritten = false;

    cache.Merge(32 * kMiB, existing);

    PExtent adding;
    adding.pOffset = 100 * kMiB;
    adding.len = 4 * kMiB;
    adding.UnWritten = false;

    cache.Merge(16 * kMiB, adding);

    auto extents = cache.GetExtentsForTesting();
    ASSERT_TRUE(extents.count(0));

    auto& range = extents[0];
    ASSERT_EQ(2, range.size());
    ASSERT_TRUE(range.count(16 * kMiB));
    ASSERT_TRUE(range.count(32 * kMiB));

    ASSERT_EQ(existing, range[32 * kMiB]);
    ASSERT_EQ(adding, range[16 * kMiB]);
}

// adding extent              |----|
// existing extents                |----|
TEST(ExtentCacheMergeTest, MergeCase3_PhysicalOffsetNotContinuous) {
    ExtentCache cache;

    PExtent existing;
    existing.pOffset = 16 * kMiB;
    existing.len = 4 * kMiB;
    existing.UnWritten = false;

    cache.Merge(20 * kMiB, existing);

    PExtent adding;
    adding.pOffset = 100 * kMiB;
    adding.len = 4 * kMiB;
    adding.UnWritten = false;

    cache.Merge(16 * kMiB, adding);

    auto extents = cache.GetExtentsForTesting();
    ASSERT_TRUE(extents.count(0));

    auto& range = extents[0];
    ASSERT_EQ(2, range.size());
    ASSERT_TRUE(range.count(16 * kMiB));
    ASSERT_TRUE(range.count(20 * kMiB));

    ASSERT_EQ(existing, range[20 * kMiB]);
    ASSERT_EQ(adding, range[16 * kMiB]);
}

// adding extent              |----|
// existing extents                |----|
TEST(ExtentCacheMergeTest, MergeCase3_PhysicalOffsetContinuous) {
    ExtentCache cache;

    PExtent existing;
    existing.pOffset = 104 * kMiB;
    existing.len = 4 * kMiB;
    existing.UnWritten = false;

    cache.Merge(20 * kMiB, existing);

    PExtent adding;
    adding.pOffset = 100 * kMiB;
    adding.len = 4 * kMiB;
    adding.UnWritten = false;

    cache.Merge(16 * kMiB, adding);

    auto extents = cache.GetExtentsForTesting();
    ASSERT_TRUE(extents.count(0));

    auto& range = extents[0];
    ASSERT_EQ(1, range.size());
    ASSERT_TRUE(range.count(16 * kMiB));
    ASSERT_FALSE(range.count(20 * kMiB));

    ASSERT_EQ(100 * kMiB, range[16 * kMiB].pOffset);
    ASSERT_EQ(8 * kMiB, range[16 * kMiB].len);
    ASSERT_FALSE(range[16 * kMiB].UnWritten);
}

// adding extent              |----|
// existing extents                |----|
TEST(ExtentCacheMergeTest, MergeCase3_PhysicalOffsetContinuousButNoWritten) {
    ExtentCache cache;

    PExtent existing;
    existing.pOffset = 104 * kMiB;
    existing.len = 4 * kMiB;
    existing.UnWritten = true;

    cache.Merge(20 * kMiB, existing);

    PExtent adding;
    adding.pOffset = 100 * kMiB;
    adding.len = 4 * kMiB;
    adding.UnWritten = false;

    cache.Merge(16 * kMiB, adding);

    auto extents = cache.GetExtentsForTesting();
    ASSERT_TRUE(extents.count(0));

    auto& range = extents[0];
    ASSERT_EQ(2, range.size());
    ASSERT_TRUE(range.count(16 * kMiB));
    ASSERT_TRUE(range.count(20 * kMiB));

    ASSERT_EQ(100 * kMiB, range[16 * kMiB].pOffset);
    ASSERT_EQ(4 * kMiB, range[16 * kMiB].len);
    ASSERT_FALSE(range[16 * kMiB].UnWritten);

    ASSERT_EQ(104 * kMiB, range[20 * kMiB].pOffset);
    ASSERT_EQ(4 * kMiB, range[20 * kMiB].len);
    ASSERT_TRUE(range[20 * kMiB].UnWritten);
}

// adding extent             |----|
// existing extents     |----|
TEST(ExtentCacheMergeTest, MergeCase4_PhysicalOffsetContinuous) {
    ExtentCache cache;

    PExtent existing;
    existing.pOffset = 96 * kMiB;
    existing.len = 4 * kMiB;
    existing.UnWritten = false;

    cache.Merge(16 * kMiB, existing);

    PExtent adding;
    adding.pOffset = 100 * kMiB;
    adding.len = 4 * kMiB;
    adding.UnWritten = false;

    cache.Merge(20 * kMiB, adding);

    auto extents = cache.GetExtentsForTesting();
    ASSERT_TRUE(extents.count(0));

    auto& range = extents[0];
    ASSERT_EQ(1, range.size());
    ASSERT_TRUE(range.count(16 * kMiB));

    ASSERT_EQ(96 * kMiB, range[16 * kMiB].pOffset);
    ASSERT_EQ(8 * kMiB, range[16 * kMiB].len);
    ASSERT_FALSE(range[16 * kMiB].UnWritten);

    // ASSERT_EQ(104 * kMiB, range[20 * kMiB].pOffset);
    // ASSERT_EQ(4 * kMiB, range[20 * kMiB].len);
    // ASSERT_TRUE(range[20 * kMiB].UnWritten);
}

// adding extent             |----|
// existing extents     |----|
TEST(ExtentCacheMergeTest, MergeCase4_PhysicalNotNotContinuous) {
    ExtentCache cache;

    PExtent existing;
    existing.pOffset = 96 * kMiB;
    existing.len = 4 * kMiB;
    existing.UnWritten = false;

    cache.Merge(16 * kMiB, existing);

    PExtent adding;
    adding.pOffset = 200 * kMiB;
    adding.len = 4 * kMiB;
    adding.UnWritten = false;

    cache.Merge(20 * kMiB, adding);

    auto extents = cache.GetExtentsForTesting();
    ASSERT_EQ(1, extents.count(0));

    auto& range = extents[0];
    ASSERT_EQ(2, range.size());
    ASSERT_EQ(1, range.count(16 * kMiB));
    ASSERT_EQ(1, range.count(20 * kMiB));

    ASSERT_EQ(96 * kMiB, range[16 * kMiB].pOffset);
    ASSERT_EQ(4 * kMiB, range[16 * kMiB].len);
    ASSERT_FALSE(range[16 * kMiB].UnWritten);

    ASSERT_EQ(200 * kMiB, range[20 * kMiB].pOffset);
    ASSERT_EQ(4 * kMiB, range[20 * kMiB].len);
    ASSERT_FALSE(range[20 * kMiB].UnWritten);
}

// adding extent             |----|
// existing extents     |----|    |----|
TEST(ExtentCacheMergeTest, MergeCase5_PhysicalOffsetContinuous) {
    ExtentCache cache;

    PExtent existing;
    existing.pOffset = 96 * kMiB;
    existing.len = 4 * kMiB;
    existing.UnWritten = false;
    cache.Merge(16 * kMiB, existing);
    existing.pOffset = 104 * kMiB;
    existing.len = 4 * kMiB;
    existing.UnWritten = false;
    cache.Merge(24 * kMiB, existing);

    PExtent adding;
    adding.pOffset = 100 * kMiB;
    adding.len = 4 * kMiB;
    adding.UnWritten = false;

    cache.Merge(20 * kMiB, adding);

    auto extents = cache.GetExtentsForTesting();
    ASSERT_TRUE(extents.count(0));

    auto& range = extents[0];
    ASSERT_EQ(1, range.size());
    ASSERT_EQ(1, range.count(16 * kMiB));
    ASSERT_EQ(0, range.count(20 * kMiB));
    ASSERT_EQ(0, range.count(24 * kMiB));

    ASSERT_EQ(96 * kMiB, range[16 * kMiB].pOffset);
    ASSERT_EQ(12 * kMiB, range[16 * kMiB].len);
    ASSERT_FALSE(range[16 * kMiB].UnWritten);
}

// adding extent             |----|
// existing extents     |----|    |----|
TEST(ExtentCacheMergeTest, MergeCase5_RightExtentPhysicalOffsetNotContinuous) {
    ExtentCache cache;

    PExtent existing;
    existing.pOffset = 96 * kMiB;
    existing.len = 4 * kMiB;
    existing.UnWritten = false;
    cache.Merge(16 * kMiB, existing);
    existing.pOffset = 105 * kMiB;
    existing.len = 4 * kMiB;
    existing.UnWritten = false;
    cache.Merge(24 * kMiB, existing);

    PExtent adding;
    adding.pOffset = 100 * kMiB;
    adding.len = 4 * kMiB;
    adding.UnWritten = false;

    cache.Merge(20 * kMiB, adding);

    auto extents = cache.GetExtentsForTesting();
    ASSERT_EQ(1, extents.count(0));

    auto& range = extents[0];
    ASSERT_EQ(2, range.size());
    ASSERT_EQ(1, range.count(16 * kMiB));
    ASSERT_EQ(0, range.count(20 * kMiB));
    ASSERT_EQ(1, range.count(24 * kMiB));

    ASSERT_EQ(96 * kMiB, range[16 * kMiB].pOffset);
    ASSERT_EQ(8 * kMiB, range[16 * kMiB].len);
    ASSERT_FALSE(range[16 * kMiB].UnWritten);

    ASSERT_EQ(105 * kMiB, range[24 * kMiB].pOffset);
    ASSERT_EQ(4 * kMiB, range[24 * kMiB].len);
    ASSERT_FALSE(range[24 * kMiB].UnWritten);
}

// adding extent             |----|
// existing extents     |----|    |----|
TEST(ExtentCacheMergeTest, MergeCase5_LeftExtentPhysicalOffsetNotContinuous) {
    ExtentCache cache;

    PExtent existing;
    existing.pOffset = 95 * kMiB;
    existing.len = 4 * kMiB;
    existing.UnWritten = false;
    cache.Merge(16 * kMiB, existing);
    existing.pOffset = 104 * kMiB;
    existing.len = 4 * kMiB;
    existing.UnWritten = false;
    cache.Merge(24 * kMiB, existing);

    PExtent adding;
    adding.pOffset = 100 * kMiB;
    adding.len = 4 * kMiB;
    adding.UnWritten = false;

    cache.Merge(20 * kMiB, adding);

    auto extents = cache.GetExtentsForTesting();
    ASSERT_EQ(1, extents.count(0));

    auto& range = extents[0];
    ASSERT_EQ(2, range.size());
    ASSERT_EQ(1, range.count(16 * kMiB));
    ASSERT_EQ(1, range.count(20 * kMiB));
    ASSERT_EQ(0, range.count(24 * kMiB));

    ASSERT_EQ(95 * kMiB, range[16 * kMiB].pOffset);
    ASSERT_EQ(4 * kMiB, range[16 * kMiB].len);
    ASSERT_FALSE(range[16 * kMiB].UnWritten);

    ASSERT_EQ(100 * kMiB, range[20 * kMiB].pOffset);
    ASSERT_EQ(8 * kMiB, range[20 * kMiB].len);
    ASSERT_FALSE(range[20 * kMiB].UnWritten);
}

// adding extent             |----|
// existing extents     |----|    |----|
TEST(ExtentCacheMergeTest, MergeCase5_PhysicalOffsetContinuousButNotWritten) {
    ExtentCache cache;

    PExtent existing;
    existing.pOffset = 96 * kMiB;
    existing.len = 4 * kMiB;
    existing.UnWritten = true;
    cache.Merge(16 * kMiB, existing);
    existing.pOffset = 104 * kMiB;
    existing.len = 4 * kMiB;
    existing.UnWritten = true;
    cache.Merge(24 * kMiB, existing);

    PExtent adding;
    adding.pOffset = 100 * kMiB;
    adding.len = 4 * kMiB;
    adding.UnWritten = false;

    cache.Merge(20 * kMiB, adding);

    auto extents = cache.GetExtentsForTesting();
    ASSERT_EQ(1, extents.count(0));

    auto& range = extents[0];
    ASSERT_EQ(3, range.size());
    ASSERT_EQ(1, range.count(16 * kMiB));
    ASSERT_EQ(1, range.count(20 * kMiB));
    ASSERT_EQ(1, range.count(24 * kMiB));

    ASSERT_EQ(96 * kMiB, range[16 * kMiB].pOffset);
    ASSERT_EQ(4 * kMiB, range[16 * kMiB].len);
    ASSERT_TRUE(range[16 * kMiB].UnWritten);

    ASSERT_EQ(100 * kMiB, range[20 * kMiB].pOffset);
    ASSERT_EQ(4 * kMiB, range[20 * kMiB].len);
    ASSERT_FALSE(range[20 * kMiB].UnWritten);

    ASSERT_EQ(104 * kMiB, range[24 * kMiB].pOffset);
    ASSERT_EQ(4 * kMiB, range[24 * kMiB].len);
    ASSERT_TRUE(range[24 * kMiB].UnWritten);
}

}  // namespace client
}  // namespace curvefs
