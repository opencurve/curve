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
 * Date: Wednesday Mar 16 10:32:58 CST 2022
 * Author: wuhanqing
 */

#include <gtest/gtest.h>

#include "curvefs/src/client/volume/extent_cache.h"
#include "curvefs/test/client/volume/common.h"

namespace curvefs {
namespace client {

// write     |----|       |----|                 |----|          |----|
// extent         |----|         |----|     |----|        |----|
TEST(ExtentCacheMarkWrittenTest, Case1_NonOverlap) {
    using MarkWrittenRange = std::pair<uint64_t, uint64_t>;
    using ExtentRange = std::pair<uint64_t, uint64_t>;

    bool unwritten = true;

    std::vector<std::pair<MarkWrittenRange, ExtentRange>> tests{
        {
            {0, 8 * kMiB},
            {8 * kMiB, 4 * kMiB},
        },
        {
            {0, 8 * kMiB},
            {12 * kMiB, 4 * kMiB},
        },
        {
            {4 * kMiB, 8 * kMiB},
            {0, 4 * kMiB},
        },
        {
            {5 * kMiB, 8 * kMiB},
            {0, 4 * kMiB},
        },
    };

    for (auto& test : tests) {
        ExtentCache cache;

        PExtent pext;
        pext.pOffset = 100 * kMiB;
        pext.len = test.second.second;
        pext.UnWritten = unwritten;

        cache.Merge(test.second.first, pext);

        cache.MarkWritten(test.first.first, test.first.second);

        auto extents = cache.GetExtentsForTesting();
        ASSERT_EQ(1, extents.size());
        ASSERT_EQ(1, extents.count(0));

        auto& range = extents[0];
        ASSERT_EQ(1, range.count(test.second.first));

        auto& ext = range[test.second.first];
        ASSERT_EQ(test.second.second, ext.len);
        ASSERT_EQ(pext.pOffset, ext.pOffset);
        ASSERT_TRUE(ext.UnWritten);
    }
}

// write     |-------|    |--------|   |--------|   |--------|
// extent       |----|      |----|     |----|       |--------|
TEST(ExtentCacheMarkWrittenTest, Case2_Overlap) {
    using MarkWrittenRange = std::pair<uint64_t, uint64_t>;
    using ExtentRange = std::pair<uint64_t, uint64_t>;

    bool unwritten = true;

    std::vector<std::pair<MarkWrittenRange, ExtentRange>> tests{
        {
            {0, 8 * kMiB},
            {4 * kMiB, 4 * kMiB},
        },
        {
            {0, 8 * kMiB},
            {2 * kMiB, 4 * kMiB},
        },
        {
            {4 * kMiB, 8 * kMiB},
            {4 * kMiB, 2 * kMiB},
        },
        {
            {0, 4 * kMiB},
            {0, 4 * kMiB},
        },
    };

    for (auto& test : tests) {
        ExtentCache cache;

        PExtent pext;
        pext.pOffset = 100 * kMiB;
        pext.len = test.second.second;
        pext.UnWritten = unwritten;

        cache.Merge(test.second.first, pext);

        cache.MarkWritten(test.first.first, test.first.second);

        auto extents = cache.GetExtentsForTesting();
        ASSERT_EQ(1, extents.size());
        ASSERT_EQ(1, extents.count(0));

        auto& range = extents[0];
        ASSERT_EQ(1, range.count(test.second.first));

        auto& ext = range[test.second.first];
        ASSERT_EQ(test.second.second, ext.len) << ext;
        ASSERT_EQ(pext.pOffset, ext.pOffset) << ext;
        ASSERT_FALSE(ext.UnWritten) << ext;
    }
}

// write     |-------|       |-----|     |--------|
// extent    |--------|  |---------|   |------------|
TEST(ExtentCacheMarkWrittenTest, Case3_Overlap) {
    {
        ExtentCache cache;

        PExtent pext;
        pext.pOffset = 100 * kMiB;
        pext.len = 4 * kMiB;
        pext.UnWritten = true;

        cache.Merge(0, pext);

        cache.MarkWritten(0, 3 * kMiB);

        auto extents = cache.GetExtentsForTesting();
        ASSERT_EQ(1, extents.size());
        ASSERT_EQ(1, extents.count(0));

        auto& range = extents[0];
        ASSERT_EQ(2, range.size());

        ASSERT_EQ(1, range.count(0 * kMiB));
        ASSERT_EQ(1, range.count(3 * kMiB));

        auto& first = range[0 * kMiB];
        auto& second = range[3 * kMiB];

        ASSERT_EQ(3 * kMiB, first.len);
        ASSERT_EQ(100 * kMiB, first.pOffset);
        ASSERT_FALSE(first.UnWritten);

        ASSERT_EQ(1 * kMiB, second.len);
        ASSERT_EQ(103 * kMiB, second.pOffset);
        ASSERT_TRUE(second.UnWritten);
    }

    {
        ExtentCache cache;

        PExtent pext;
        pext.pOffset = 100 * kMiB, pext.len = 4 * kMiB;
        pext.UnWritten = true;

        cache.Merge(0, pext);

        cache.MarkWritten(1 * kMiB, 3 * kMiB);

        auto extents = cache.GetExtentsForTesting();
        ASSERT_EQ(1, extents.size());
        ASSERT_EQ(1, extents.count(0));

        auto& range = extents[0];
        ASSERT_EQ(2, range.size());

        ASSERT_EQ(1, range.count(0 * kMiB));
        ASSERT_EQ(1, range.count(1 * kMiB));

        auto& first = range[0 * kMiB];
        auto& second = range[1 * kMiB];

        ASSERT_EQ(1 * kMiB, first.len);
        ASSERT_EQ(100 * kMiB, first.pOffset);
        ASSERT_TRUE(first.UnWritten);

        ASSERT_EQ(3 * kMiB, second.len);
        ASSERT_EQ(101 * kMiB, second.pOffset);
        ASSERT_FALSE(second.UnWritten);
    }

    {
        ExtentCache cache;

        PExtent pext;
        pext.pOffset = 100 * kMiB, pext.len = 4 * kMiB;
        pext.UnWritten = true;

        cache.Merge(0, pext);

        cache.MarkWritten(1 * kMiB, 2 * kMiB);

        auto extents = cache.GetExtentsForTesting();
        ASSERT_EQ(1, extents.size());
        ASSERT_EQ(1, extents.count(0));

        auto& range = extents[0];
        ASSERT_EQ(3, range.size());

        ASSERT_EQ(1, range.count(0 * kMiB));
        ASSERT_EQ(1, range.count(1 * kMiB));
        ASSERT_EQ(1, range.count(3 * kMiB));

        auto& first = range[0 * kMiB];
        auto& second = range[1 * kMiB];
        auto& third = range[3 * kMiB];

        ASSERT_EQ(1 * kMiB, first.len);
        ASSERT_EQ(100 * kMiB, first.pOffset);
        ASSERT_TRUE(first.UnWritten);

        ASSERT_EQ(2 * kMiB, second.len);
        ASSERT_EQ(101 * kMiB, second.pOffset);
        ASSERT_FALSE(second.UnWritten);

        ASSERT_EQ(1 * kMiB, third.len);
        ASSERT_EQ(103 * kMiB, third.pOffset);
        ASSERT_TRUE(third.UnWritten);
    }
}

// write     |----|             |-----|
// extent       |----|      |----|
TEST(ExtentCacheMarkWrittenTest, Case4_Overlap) {
    {
        ExtentCache cache;

        PExtent pext;
        pext.len = 4 * kMiB;
        pext.pOffset = 100 * kMiB;
        pext.UnWritten = true;

        cache.Merge(8 * kMiB, pext);

        cache.MarkWritten(0, 10 * kMiB);

        auto extents = cache.GetExtentsForTesting();
        ASSERT_EQ(1, extents.size());
        ASSERT_EQ(1, extents.count(0));

        auto& range = extents[0];

        ASSERT_EQ(2, range.size());
        ASSERT_EQ(1, range.count(8 * kMiB));
        ASSERT_EQ(1, range.count(10 * kMiB));

        auto& first = range[8 * kMiB];
        auto& second = range[10 * kMiB];

        ASSERT_EQ(2 * kMiB, first.len);
        ASSERT_EQ(100 * kMiB, first.pOffset);
        ASSERT_FALSE(first.UnWritten);

        ASSERT_EQ(2 * kMiB, second.len);
        ASSERT_EQ(102 * kMiB, second.pOffset);
        ASSERT_TRUE(second.UnWritten);
    }

    {
        ExtentCache cache;

        PExtent pext;
        pext.len = 8 * kMiB;
        pext.pOffset = 100 * kMiB;
        pext.UnWritten = true;

        cache.Merge(0 * kMiB, pext);

        cache.MarkWritten(4 * kMiB, 10 * kMiB);

        auto extents = cache.GetExtentsForTesting();
        ASSERT_EQ(1, extents.size());
        ASSERT_EQ(1, extents.count(0));

        auto& range = extents[0];

        ASSERT_EQ(2, range.size());
        ASSERT_EQ(1, range.count(0 * kMiB));
        ASSERT_EQ(1, range.count(4 * kMiB));

        auto& first = range[0 * kMiB];
        auto& second = range[4 * kMiB];

        ASSERT_EQ(4 * kMiB, first.len);
        ASSERT_EQ(100 * kMiB, first.pOffset);
        ASSERT_TRUE(first.UnWritten);

        ASSERT_EQ(4 * kMiB, second.len);
        ASSERT_EQ(104 * kMiB, second.pOffset);
        ASSERT_FALSE(second.UnWritten);
    }
}

// write     |----|         |---------|
// extent  |----|----|      |----|----|
TEST(ExtentCacheMarkWrittenTest, Case5_Mergeable) {
    {
        ExtentCache cache;

        // 8MiB ~ 12MiB
        PExtent pext;
        pext.len = 4 * kMiB;
        pext.pOffset = 100 * kMiB;
        pext.UnWritten = true;

        cache.Merge(8 * kMiB, pext);

        // 12MiB ~ 16MiB
        pext.len = 4 * kMiB;
        pext.pOffset = 104 * kMiB;
        pext.UnWritten = true;

        cache.Merge(12 * kMiB, pext);

        // mark written 10MiB ~ 14MiB
        cache.MarkWritten(10 * kMiB, 4 * kMiB);

        auto extents = cache.GetExtentsForTesting();
        ASSERT_EQ(1, extents.size());
        ASSERT_EQ(1, extents.count(0));

        auto& range = extents[0];

        ASSERT_EQ(3, range.size());
        ASSERT_EQ(1, range.count(8 * kMiB));
        ASSERT_EQ(1, range.count(10 * kMiB));
        ASSERT_EQ(1, range.count(14 * kMiB));

        auto& first = range[8 * kMiB];
        auto& second = range[10 * kMiB];
        auto& third = range[14 * kMiB];

        ASSERT_EQ(2 * kMiB, first.len);
        ASSERT_EQ(100 * kMiB, first.pOffset);
        ASSERT_TRUE(first.UnWritten);

        ASSERT_EQ(4 * kMiB, second.len);
        ASSERT_EQ(102 * kMiB, second.pOffset);
        ASSERT_FALSE(second.UnWritten);

        ASSERT_EQ(2 * kMiB, third.len);
        ASSERT_EQ(106 * kMiB, third.pOffset);
        ASSERT_TRUE(third.UnWritten);
    }

    {
        ExtentCache cache;

        // 8MiB ~ 12MiB
        PExtent pext;
        pext.len = 4 * kMiB;
        pext.pOffset = 100 * kMiB;
        pext.UnWritten = true;

        cache.Merge(8 * kMiB, pext);

        // 12MiB ~ 16MiB
        pext.len = 4 * kMiB;
        pext.pOffset = 104 * kMiB;
        pext.UnWritten = true;

        cache.Merge(12 * kMiB, pext);

        // mark written 8MiB ~ 16MiB
        cache.MarkWritten(8 * kMiB, 8 * kMiB);

        auto extents = cache.GetExtentsForTesting();
        ASSERT_EQ(1, extents.size());
        ASSERT_EQ(1, extents.count(0));

        auto& range = extents[0];

        ASSERT_EQ(1, range.size());

        auto& first = range[8 * kMiB];

        ASSERT_EQ(8 * kMiB, first.len);
        ASSERT_EQ(100 * kMiB, first.pOffset);
        ASSERT_FALSE(first.UnWritten);
    }
}

// write     |----|         |---------|
// extent  |----|----|      |----|----|
// physical offset is not continuous
TEST(ExtentCacheMarkWrittenTest, Case5_NotMerge) {
    {
        ExtentCache cache;

        // 8MiB ~ 12MiB
        PExtent pext;
        pext.len = 4 * kMiB;
        pext.pOffset = 99 * kMiB;
        pext.UnWritten = true;

        cache.Merge(8 * kMiB, pext);

        // 12MiB ~ 16MiB
        pext.len = 4 * kMiB;
        pext.pOffset = 104 * kMiB;
        pext.UnWritten = true;

        cache.Merge(12 * kMiB, pext);

        // mark written 10MiB ~ 14MiB
        cache.MarkWritten(10 * kMiB, 4 * kMiB);

        auto extents = cache.GetExtentsForTesting();
        ASSERT_EQ(1, extents.size());
        ASSERT_EQ(1, extents.count(0));

        auto& range = extents[0];

        ASSERT_EQ(4, range.size());
        ASSERT_EQ(1, range.count(8 * kMiB));
        ASSERT_EQ(1, range.count(10 * kMiB));
        ASSERT_EQ(1, range.count(12 * kMiB));
        ASSERT_EQ(1, range.count(14 * kMiB));

        auto& first = range[8 * kMiB];
        auto& second = range[10 * kMiB];
        auto& third = range[12 * kMiB];
        auto& fourth = range[14 * kMiB];

        ASSERT_EQ(2 * kMiB, first.len);
        ASSERT_EQ(99 * kMiB, first.pOffset);
        ASSERT_TRUE(first.UnWritten);

        ASSERT_EQ(2 * kMiB, second.len);
        ASSERT_EQ(101 * kMiB, second.pOffset);
        ASSERT_FALSE(second.UnWritten);

        ASSERT_EQ(2 * kMiB, third.len);
        ASSERT_EQ(104 * kMiB, third.pOffset);
        ASSERT_FALSE(third.UnWritten);

        ASSERT_EQ(2 * kMiB, fourth.len);
        ASSERT_EQ(106 * kMiB, fourth.pOffset);
        ASSERT_TRUE(fourth.UnWritten);
    }

    {
        ExtentCache cache;

        // 8MiB ~ 12MiB
        PExtent pext;
        pext.len = 4 * kMiB;
        pext.pOffset = 99 * kMiB;
        pext.UnWritten = true;

        cache.Merge(8 * kMiB, pext);

        // 12MiB ~ 16MiB
        pext.len = 4 * kMiB;
        pext.pOffset = 104 * kMiB;
        pext.UnWritten = true;

        cache.Merge(12 * kMiB, pext);

        // mark written 8MiB ~ 16MiB
        cache.MarkWritten(8 * kMiB, 8 * kMiB);

        auto extents = cache.GetExtentsForTesting();
        ASSERT_EQ(1, extents.size());
        ASSERT_EQ(1, extents.count(0));

        auto& range = extents[0];

        ASSERT_EQ(2, range.size());

        auto& first = range[8 * kMiB];
        auto& second = range[12 * kMiB];

        ASSERT_EQ(4 * kMiB, first.len);
        ASSERT_EQ(99 * kMiB, first.pOffset);
        ASSERT_FALSE(first.UnWritten);

        ASSERT_EQ(4 * kMiB, second.len);
        ASSERT_EQ(104 * kMiB, second.pOffset);
        ASSERT_FALSE(second.UnWritten);
    }
}

// write     |-------------------|
// extent    |----|----|----|----|
//            ^^^^           ^^^^
//           written        written
TEST(ExtentCacheMarkWrittenTest, Case6_Mergeable) {
    ExtentCache cache;

    // 8MiB ~ 12MiB
    PExtent pext;
    pext.len = 4 * kMiB;
    pext.pOffset = 100 * kMiB;
    pext.UnWritten = true;

    cache.Merge(8 * kMiB, pext);

    // 12MiB ~ 16MiB
    pext.len = 4 * kMiB;
    pext.pOffset = 104 * kMiB;
    pext.UnWritten = true;

    cache.Merge(12 * kMiB, pext);

    // 16MiB ~ 20MiB
    pext.len = 4 * kMiB;
    pext.pOffset = 108 * kMiB;
    pext.UnWritten = true;

    cache.Merge(16 * kMiB, pext);

        // 20MiB ~ 24MiB
    pext.len = 4 * kMiB;
    pext.pOffset = 112 * kMiB;
    pext.UnWritten = true;

    cache.Merge(20 * kMiB, pext);

    // mark written 8MiB ~ 12MiB
    cache.MarkWritten(8 * kMiB, 4 * kMiB);

    // mark written 20MiB ~ 24MiB
    cache.MarkWritten(20 * kMiB, 4 * kMiB);

    // mark written 8MiB ~ 24MiB
    cache.MarkWritten(8 * kMiB, 16 * kMiB);

    auto extents = cache.GetExtentsForTesting();
    ASSERT_EQ(1, extents.size());
    ASSERT_EQ(1, extents.count(0));

    auto& range = extents[0];

    ASSERT_EQ(1, range.size());

    auto& first = range[8 * kMiB];

    ASSERT_EQ(16 * kMiB, first.len);
    ASSERT_EQ(100 * kMiB, first.pOffset);
    ASSERT_FALSE(first.UnWritten);
}

// write     |--------------|
// extent    |----|----|----|----|
//            ^^^^           ^^^^
//           written        written
// in this case, the rightmost extent is not merged
TEST(ExtentCacheMarkWrittenTest, Case7) {
    ExtentCache cache;

    // 8MiB ~ 12MiB
    PExtent pext;
    pext.len = 4 * kMiB;
    pext.pOffset = 100 * kMiB;
    pext.UnWritten = true;

    cache.Merge(8 * kMiB, pext);

    // 12MiB ~ 16MiB
    pext.len = 4 * kMiB;
    pext.pOffset = 104 * kMiB;
    pext.UnWritten = true;

    cache.Merge(12 * kMiB, pext);

    // 16MiB ~ 20MiB
    pext.len = 4 * kMiB;
    pext.pOffset = 108 * kMiB;
    pext.UnWritten = true;

    cache.Merge(16 * kMiB, pext);

    // 20MiB ~ 24MiB
    pext.len = 4 * kMiB;
    pext.pOffset = 112 * kMiB;
    pext.UnWritten = true;

    cache.Merge(20 * kMiB, pext);

    // mark written 8MiB ~ 12MiB
    cache.MarkWritten(8 * kMiB, 4 * kMiB);

    // mark written 20MiB ~ 24MiB
    cache.MarkWritten(20 * kMiB, 4 * kMiB);

    // mark written 8MiB ~ 20MiB
    cache.MarkWritten(8 * kMiB, 12 * kMiB);

    auto extents = cache.GetExtentsForTesting();
    ASSERT_EQ(1, extents.size());
    ASSERT_EQ(1, extents.count(0));

    auto& range = extents[0];

    ASSERT_EQ(2, range.size());

    auto& first = range[8 * kMiB];
    auto& second = range[20 * kMiB];

    ASSERT_EQ(12 * kMiB, first.len);
    ASSERT_EQ(100 * kMiB, first.pOffset);
    ASSERT_FALSE(first.UnWritten);

    ASSERT_EQ(4 * kMiB, second.len);
    ASSERT_EQ(112 * kMiB, second.pOffset);
    ASSERT_FALSE(second.UnWritten);
}

// write     |-------|       |-----|     |--------|
// extent    |--------|  |---------|   |------------|
TEST(ExtentCacheMarkWrittenTest, Case3_Overlap_Unaligned1) {
    ExtentCache cache;

    PExtent pext;
    pext.pOffset = 100 * kMiB;
    pext.len = 4 * kKiB;
    pext.UnWritten = true;

    cache.Merge(0, pext);

    // only first 6 bytes are written
    cache.MarkWritten(0, 6);

    auto extents = cache.GetExtentsForTesting();
    ASSERT_EQ(1, extents.size());
    ASSERT_EQ(1, extents.count(0));

    auto& range = extents[0];
    ASSERT_EQ(1, range.size());

    ASSERT_EQ(1, range.count(0));

    auto& first = range[0];

    ASSERT_EQ(4 * kKiB, first.len);
    ASSERT_EQ(100 * kMiB, first.pOffset);
    ASSERT_FALSE(first.UnWritten);
}

TEST(ExtentCacheMarkWrittenTest, Case3_Overlap_Unaligned2) {
    ExtentCache cache;

    PExtent pext;
    pext.pOffset = 100 * kMiB;
    pext.len = 4 * kKiB;
    pext.UnWritten = true;

    cache.Merge(0, pext);

    // only first 6 bytes are written
    cache.MarkWritten(6, 12);

    auto extents = cache.GetExtentsForTesting();
    ASSERT_EQ(1, extents.size());
    ASSERT_EQ(1, extents.count(0));

    auto& range = extents[0];
    ASSERT_EQ(1, range.size());

    ASSERT_EQ(1, range.count(0));

    auto& first = range[0];

    ASSERT_EQ(4 * kKiB, first.len);
    ASSERT_EQ(100 * kMiB, first.pOffset);
    ASSERT_FALSE(first.UnWritten);
}

}  // namespace client
}  // namespace curvefs
