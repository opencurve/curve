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
 * Date: Friday Mar 25 17:24:18 CST 2022
 * Author: wuhanqing
 */

#include <gtest/gtest.h>

#include "curvefs/src/client/volume/extent_cache.h"
#include "curvefs/test/client/volume/common.h"

namespace curvefs {
namespace client {

TEST(ExtentCacheToInodePbTest, EmptyTest) {
    ExtentCache cache;
    auto pb = cache.ToInodePb();
    ASSERT_TRUE(pb.empty());
}

TEST(ExtentCacheToInodePbTest, Case1) {
    for (auto written : {true, false}) {
        ExtentCache cache;

        PExtent pext;
        pext.len = 1 * kMiB;
        pext.pOffset = 2 * kMiB;
        pext.UnWritten = !written;

        cache.Merge(4 * kMiB, pext);

        auto pb = cache.ToInodePb();
        ASSERT_EQ(1, pb.size());

        ASSERT_TRUE(pb.count(0));

        const auto& range = pb[0];
        ASSERT_EQ(1, range.volumeextents_size());

        auto& first = range.volumeextents(0);
        ASSERT_EQ(1 * kMiB, first.length());
        ASSERT_EQ(2 * kMiB, first.volumeoffset());
        ASSERT_EQ(4 * kMiB, first.fsoffset());
        ASSERT_EQ(written, first.isused());
    }
}

// |----|----|----|----|
//  ^^^^      ^^^^
// written   written
TEST(ExtentCacheToInodePbTest, Case2) {
    ExtentCache cache;

    PExtent pext;
    pext.len = 4 * kMiB;
    pext.pOffset = 0 * kMiB;
    pext.UnWritten = true;
    cache.Merge(0 * kMiB, pext);

    pext.len = 4 * kMiB;
    pext.pOffset = 4 * kMiB;
    pext.UnWritten = true;
    cache.Merge(4 * kMiB, pext);

    pext.len = 4 * kMiB;
    pext.pOffset = 8 * kMiB;
    pext.UnWritten = true;
    cache.Merge(8 * kMiB, pext);

    pext.len = 4 * kMiB;
    pext.pOffset = 12 * kMiB;
    pext.UnWritten = true;
    cache.Merge(12 * kMiB, pext);

    cache.MarkWritten(0 * kMiB, 4 * kMiB);
    cache.MarkWritten(8 * kMiB, 4 * kMiB);

    auto pb = cache.ToInodePb();
    ASSERT_EQ(1, pb.size());

    ASSERT_TRUE(pb.count(0));

    const auto& range = pb[0];
    ASSERT_EQ(4, range.volumeextents_size());

    auto& first = range.volumeextents(0);
    auto& second = range.volumeextents(1);
    auto& third = range.volumeextents(2);
    auto& fourth = range.volumeextents(3);

    ASSERT_EQ(4 * kMiB, first.length());
    ASSERT_EQ(0 * kMiB, first.volumeoffset());
    ASSERT_EQ(0 * kMiB, first.fsoffset());
    ASSERT_EQ(true, first.isused());

    ASSERT_EQ(4 * kMiB, second.length());
    ASSERT_EQ(4 * kMiB, second.volumeoffset());
    ASSERT_EQ(4 * kMiB, second.fsoffset());
    ASSERT_EQ(false, second.isused());

    ASSERT_EQ(4 * kMiB, third.length());
    ASSERT_EQ(8 * kMiB, third.volumeoffset());
    ASSERT_EQ(8 * kMiB, third.fsoffset());
    ASSERT_EQ(true, third.isused());

    ASSERT_EQ(4 * kMiB, fourth.length());
    ASSERT_EQ(12 * kMiB, fourth.volumeoffset());
    ASSERT_EQ(12 * kMiB, fourth.fsoffset());
    ASSERT_EQ(false, fourth.isused());
}

// |----|----|----|----|
//       ^^^^ ^^^^
//        written
TEST(ExtentCacheToInodePbTest, Case3) {
    ExtentCache cache;

    PExtent pext;
    pext.len = 4 * kMiB;
    pext.pOffset = 0 * kMiB;
    pext.UnWritten = true;
    cache.Merge(0 * kMiB, pext);

    pext.len = 4 * kMiB;
    pext.pOffset = 4 * kMiB;
    pext.UnWritten = true;
    cache.Merge(4 * kMiB, pext);

    pext.len = 4 * kMiB;
    pext.pOffset = 8 * kMiB;
    pext.UnWritten = true;
    cache.Merge(8 * kMiB, pext);

    pext.len = 4 * kMiB;
    pext.pOffset = 12 * kMiB;
    pext.UnWritten = true;
    cache.Merge(12 * kMiB, pext);

    cache.MarkWritten(4 * kMiB, 8 * kMiB);

    auto pb = cache.ToInodePb();
    ASSERT_EQ(1, pb.size());

    ASSERT_TRUE(pb.count(0));

    const auto& range = pb[0];
    ASSERT_EQ(3, range.volumeextents_size());

    auto& first = range.volumeextents(0);
    auto& second = range.volumeextents(1);
    auto& third = range.volumeextents(2);

    ASSERT_EQ(4 * kMiB, first.length());
    ASSERT_EQ(0 * kMiB, first.volumeoffset());
    ASSERT_EQ(0 * kMiB, first.fsoffset());
    ASSERT_EQ(false, first.isused());

    ASSERT_EQ(8 * kMiB, second.length());
    ASSERT_EQ(4 * kMiB, second.volumeoffset());
    ASSERT_EQ(4 * kMiB, second.fsoffset());
    ASSERT_EQ(true, second.isused());

    ASSERT_EQ(4 * kMiB, third.length());
    ASSERT_EQ(12 * kMiB, third.volumeoffset());
    ASSERT_EQ(12 * kMiB, third.fsoffset());
    ASSERT_EQ(false, third.isused());
}

// |----|----|----|----|
//  ^^^^           ^^^^
// written        written
TEST(ExtentCacheToInodePbTest, Case4) {
    ExtentCache cache;

    PExtent pext;
    pext.len = 4 * kMiB;
    pext.pOffset = 0 * kMiB;
    pext.UnWritten = true;
    cache.Merge(0 * kMiB, pext);

    pext.len = 4 * kMiB;
    pext.pOffset = 4 * kMiB;
    pext.UnWritten = true;
    cache.Merge(4 * kMiB, pext);

    pext.len = 4 * kMiB;
    pext.pOffset = 8 * kMiB;
    pext.UnWritten = true;
    cache.Merge(8 * kMiB, pext);

    pext.len = 4 * kMiB;
    pext.pOffset = 12 * kMiB;
    pext.UnWritten = true;
    cache.Merge(12 * kMiB, pext);

    cache.MarkWritten(0 * kMiB, 4 * kMiB);
    cache.MarkWritten(12 * kMiB, 4 * kMiB);

    auto pb = cache.ToInodePb();
    ASSERT_EQ(1, pb.size());

    ASSERT_TRUE(pb.count(0));

    const auto& range = pb[0];
    ASSERT_EQ(3, range.volumeextents_size());

    auto& first = range.volumeextents(0);
    auto& second = range.volumeextents(1);
    auto& third = range.volumeextents(2);

    ASSERT_EQ(4 * kMiB, first.length());
    ASSERT_EQ(0 * kMiB, first.volumeoffset());
    ASSERT_EQ(0 * kMiB, first.fsoffset());
    ASSERT_EQ(true, first.isused());

    ASSERT_EQ(8 * kMiB, second.length());
    ASSERT_EQ(4 * kMiB, second.volumeoffset());
    ASSERT_EQ(4 * kMiB, second.fsoffset());
    ASSERT_EQ(false, second.isused());

    ASSERT_EQ(4 * kMiB, third.length());
    ASSERT_EQ(12 * kMiB, third.volumeoffset());
    ASSERT_EQ(12 * kMiB, third.fsoffset());
    ASSERT_EQ(true, third.isused());
}

}  // namespace client
}  // namespace curvefs
