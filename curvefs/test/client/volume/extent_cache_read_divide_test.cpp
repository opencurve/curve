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
 * Date: Wednesday Mar 18 16:32:59 CST 2022
 * Author: wuhanqing
 */

#include <gtest/gtest.h>

#include <memory>

#include "curvefs/src/client/volume/extent_cache.h"
#include "curvefs/test/client/volume/common.h"

namespace curvefs {
namespace client {

TEST(ExtentCacheReadDivideTest, DivideWhenNoExtents) {
    ExtentCache cache;
    std::vector<ReadPart> reads;
    std::vector<ReadPart> holes;

    off_t offset = 4 * kKiB;
    size_t length = 4 * kMiB;

    std::unique_ptr<char[]> data(new char[length]);

    cache.DivideForRead(offset, length, data.get(), &reads, &holes);

    ASSERT_TRUE(reads.empty());
    ASSERT_EQ(1, holes.size());

    ASSERT_EQ(offset, holes[0].offset);
    ASSERT_EQ(length, holes[0].length);
    ASSERT_EQ(data.get(), holes[0].data);
}

// read    |----|           |----|
// extent       |----|               |----|
TEST(ExtentCacheReadDivideTest, DivideCase1) {
    for (auto& ext : {std::make_pair(4 * kKiB, 4 * kKiB),
                      std::make_pair(8 * kKiB, 4 * kKiB)}) {
        ExtentCache cache;

        PExtent pext;
        pext.pOffset = 10 * kMiB;
        pext.len = ext.second;

        cache.Merge(ext.first, pext);

        off_t offset = 0 * kKiB;
        size_t length = 4 * kKiB;

        std::unique_ptr<char[]> data(new char[length]);

        std::vector<ReadPart> reads;
        std::vector<ReadPart> holes;
        cache.DivideForRead(offset, length, data.get(), &reads, &holes);

        ASSERT_TRUE(reads.empty());
        ASSERT_EQ(1, holes.size());

        ASSERT_EQ(offset, holes[0].offset);
        ASSERT_EQ(length, holes[0].length);
        ASSERT_EQ(data.get(), holes[0].data);
    }
}

// read             |----|        |----|
// extent    |----|          |----|
TEST(ExtentCacheReadDivideTest, DivideCase2) {
    for (auto& ext : {std::make_pair(4 * kKiB, 4 * kKiB),
                      std::make_pair(8 * kKiB, 4 * kKiB)}) {
        ExtentCache cache;

        PExtent pext;
        pext.pOffset = 10 * kMiB;
        pext.len = 4 * kKiB;

        cache.Merge(0 * kKiB, pext);

        off_t offset = ext.first;
        size_t length = ext.second;

        std::unique_ptr<char[]> data(new char[length]);

        std::vector<ReadPart> reads;
        std::vector<ReadPart> holes;
        cache.DivideForRead(offset, length, data.get(), &reads, &holes);

        ASSERT_TRUE(reads.empty());
        ASSERT_EQ(1, holes.size());

        ASSERT_EQ(offset, holes[0].offset);
        ASSERT_EQ(length, holes[0].length);
        ASSERT_EQ(data.get(), holes[0].data);
    }
}

// read    |----|
// extent     |----|
TEST(ExtentCacheReadDivideTest, DivideCase3) {
    ExtentCache cache;

    PExtent pext;
    pext.len = 4 * kKiB;
    pext.UnWritten = false;
    pext.pOffset = 4 * kMiB;

    cache.Merge(4 * kKiB, pext);

    off_t offset = 0 * kKiB;
    size_t length = 8 * kKiB;
    std::unique_ptr<char[]> data(new char[length]);

    std::vector<ReadPart> reads;
    std::vector<ReadPart> holes;
    cache.DivideForRead(offset, length, data.get(), &reads, &holes);

    ASSERT_EQ(1, reads.size());
    ASSERT_EQ(1, holes.size());

    ASSERT_EQ(4 * kMiB, reads[0].offset);
    ASSERT_EQ(4 * kKiB, reads[0].length);
    ASSERT_EQ(data.get() + 4 * kKiB, reads[0].data);

    ASSERT_EQ(offset, holes[0].offset);
    ASSERT_EQ(4 * kKiB, holes[0].length);
    ASSERT_EQ(data.get(), holes[0].data);
}

// read    |----|
// extent     |----|
TEST(ExtentCacheReadDivideTest, DivideCase3_UnWritten) {
    ExtentCache cache;

    PExtent pext;
    pext.len = 4 * kKiB;
    pext.UnWritten = true;  // existing extent is unwritten
    pext.pOffset = 4 * kMiB;

    cache.Merge(4 * kKiB, pext);

    off_t offset = 0 * kKiB;
    size_t length = 8 * kKiB;
    std::unique_ptr<char[]> data(new char[length]);

    std::vector<ReadPart> reads;
    std::vector<ReadPart> holes;
    cache.DivideForRead(offset, length, data.get(), &reads, &holes);

    ASSERT_EQ(0, reads.size());
    ASSERT_EQ(2, holes.size());

    ASSERT_EQ(0 * kKiB, holes[0].offset);
    ASSERT_EQ(4 * kKiB, holes[0].length);
    ASSERT_EQ(data.get(), holes[0].data);

    ASSERT_EQ(4 * kKiB, holes[1].offset);
    ASSERT_EQ(4 * kKiB, holes[1].length);
    ASSERT_EQ(data.get() + 4 * kKiB, holes[1].data);
}

// read      |-------|
// extent       |----|
TEST(ExtentCacheReadDivideTest, DivideCase4) {
    ExtentCache cache;

    PExtent pext;
    pext.len = 8 * kKiB;
    pext.UnWritten = false;
    pext.pOffset = 4 * kMiB;

    cache.Merge(4 * kKiB, pext);

    off_t offset = 0 * kKiB;
    size_t length = 12 * kKiB;
    std::unique_ptr<char[]> data(new char[length]);

    std::vector<ReadPart> reads;
    std::vector<ReadPart> holes;
    cache.DivideForRead(offset, length, data.get(), &reads, &holes);

    ASSERT_EQ(1, reads.size());
    ASSERT_EQ(1, holes.size());

    ASSERT_EQ(4 * kMiB, reads[0].offset);
    ASSERT_EQ(8 * kKiB, reads[0].length);
    ASSERT_EQ(data.get() + 4 * kKiB, reads[0].data);

    ASSERT_EQ(0 * kKiB, holes[0].offset);
    ASSERT_EQ(4 * kKiB, holes[0].length);
    ASSERT_EQ(data.get(), holes[0].data);
}

// read      |-------|
// extent       |----|
TEST(ExtentCacheReadDivideTest, DivideCase4_UnWritten) {
    ExtentCache cache;

    PExtent pext;
    pext.len = 8 * kKiB;
    pext.UnWritten = true;
    pext.pOffset = 4 * kMiB;

    cache.Merge(4 * kKiB, pext);

    off_t offset = 0 * kKiB;
    size_t length = 12 * kKiB;
    std::unique_ptr<char[]> data(new char[length]);

    std::vector<ReadPart> reads;
    std::vector<ReadPart> holes;
    cache.DivideForRead(offset, length, data.get(), &reads, &holes);

    ASSERT_EQ(0, reads.size());
    ASSERT_EQ(2, holes.size());

    ASSERT_EQ(0 * kKiB, holes[0].offset);
    ASSERT_EQ(4 * kKiB, holes[0].length);
    ASSERT_EQ(data.get(), holes[0].data);

    ASSERT_EQ(4 * kKiB, holes[1].offset);
    ASSERT_EQ(8 * kKiB, holes[1].length);
    ASSERT_EQ(data.get() + 4 * kKiB, holes[1].data);
}

// read      |---------|
// extent       |----|
TEST(ExtentCacheReadDivideTest, DivideCase5) {
    ExtentCache cache;

    PExtent pext;
    pext.len = 8 * kKiB;
    pext.UnWritten = false;
    pext.pOffset = 4 * kMiB;

    cache.Merge(4 * kKiB, pext);

    off_t offset = 0 * kKiB;
    size_t length = 16 * kKiB;
    std::unique_ptr<char[]> data(new char[length]);

    std::vector<ReadPart> reads;
    std::vector<ReadPart> holes;
    cache.DivideForRead(offset, length, data.get(), &reads, &holes);

    ASSERT_EQ(1, reads.size());
    ASSERT_EQ(2, holes.size());

    ASSERT_EQ(0 * kKiB, holes[0].offset);
    ASSERT_EQ(4 * kKiB, holes[0].length);
    ASSERT_EQ(data.get(), holes[0].data);

    ASSERT_EQ(4 * kMiB, reads[0].offset);
    ASSERT_EQ(8 * kKiB, reads[0].length);
    ASSERT_EQ(data.get() + 4 * kKiB, reads[0].data);

    ASSERT_EQ(12 * kKiB, holes[1].offset);
    ASSERT_EQ(4 * kKiB, holes[1].length);
    ASSERT_EQ(data.get() + 12 * kKiB, holes[1].data);
}

// read      |---------|
// extent       |----|
TEST(ExtentCacheReadDivideTest, DivideCase5_UnWritten) {
    ExtentCache cache;

    PExtent pext;
    pext.len = 8 * kKiB;
    pext.UnWritten = true;
    pext.pOffset = 4 * kMiB;

    cache.Merge(4 * kKiB, pext);

    off_t offset = 0 * kKiB;
    size_t length = 16 * kKiB;
    std::unique_ptr<char[]> data(new char[length]);

    std::vector<ReadPart> reads;
    std::vector<ReadPart> holes;
    cache.DivideForRead(offset, length, data.get(), &reads, &holes);

    ASSERT_EQ(0, reads.size());
    ASSERT_EQ(3, holes.size());

    ASSERT_EQ(0 * kKiB, holes[0].offset);
    ASSERT_EQ(4 * kKiB, holes[0].length);
    ASSERT_EQ(data.get(), holes[0].data);

    ASSERT_EQ(4 * kKiB, holes[1].offset);
    ASSERT_EQ(8 * kKiB, holes[1].length);
    ASSERT_EQ(data.get() + 4 * kKiB, holes[1].data);

    ASSERT_EQ(12 * kKiB, holes[2].offset);
    ASSERT_EQ(4 * kKiB, holes[2].length);
    ASSERT_EQ(data.get() + 12 * kKiB, holes[2].data);
}

// read        |----|
// extents   |------|
TEST(ExtentCacheReadDivideTest, DivideCase6) {
    for (auto written : {true, false}) {
        ExtentCache cache;

        PExtent pext;
        pext.len = 8 * kKiB;
        pext.UnWritten = !written;
        pext.pOffset = 4 * kMiB;

        cache.Merge(4 * kKiB, pext);

        off_t offset = 8 * kKiB;
        size_t length = 4 * kKiB;
        std::unique_ptr<char[]> data(new char[length]);

        std::vector<ReadPart> reads;
        std::vector<ReadPart> holes;
        cache.DivideForRead(offset, length, data.get(), &reads, &holes);

        if (written) {
            ASSERT_EQ(1, reads.size());
            ASSERT_TRUE(holes.empty());

            ASSERT_EQ(4 * kMiB + 4 * kKiB, reads[0].offset);
            ASSERT_EQ(4 * kKiB, reads[0].length);
            ASSERT_EQ(data.get(), reads[0].data);
        } else {
            ASSERT_TRUE(reads.empty());
            ASSERT_EQ(1, holes.size());

            ASSERT_EQ(offset, holes[0].offset);
            ASSERT_EQ(length, holes[0].length);
            ASSERT_EQ(data.get(), holes[0].data);
        }
    }
}

// read        |----|
// extents   |--------|
TEST(ExtentCacheReadDivideTest, DivideCase7) {
    for (auto written : {true, false}) {
        ExtentCache cache;

        PExtent pext;
        pext.len = 12 * kKiB;
        pext.UnWritten = !written;
        pext.pOffset = 4 * kMiB;

        cache.Merge(4 * kKiB, pext);

        off_t offset = 8 * kKiB;
        size_t length = 4 * kKiB;
        std::unique_ptr<char[]> data(new char[length]);

        std::vector<ReadPart> reads;
        std::vector<ReadPart> holes;
        cache.DivideForRead(offset, length, data.get(), &reads, &holes);

        if (written) {
            ASSERT_EQ(1, reads.size());
            ASSERT_TRUE(holes.empty());

            ASSERT_EQ(4 * kMiB + 4 * kKiB, reads[0].offset);
            ASSERT_EQ(4 * kKiB, reads[0].length);
            ASSERT_EQ(data.get(), reads[0].data);
        } else {
            ASSERT_TRUE(reads.empty());
            ASSERT_EQ(1, holes.size());

            ASSERT_EQ(offset, holes[0].offset);
            ASSERT_EQ(length, holes[0].length);
            ASSERT_EQ(data.get(), holes[0].data);
        }
    }
}

// read         |----|
// extents    |----|
TEST(ExtentCacheReadDivideTest, DivideCase8) {
    ExtentCache cache;

    PExtent pext;
    pext.len = 8 * kKiB;
    pext.UnWritten = false;
    pext.pOffset = 4 * kMiB;

    cache.Merge(0 * kKiB, pext);

    off_t offset = 4 * kKiB;
    size_t length = 8 * kKiB;
    std::unique_ptr<char[]> data(new char[length]);

    std::vector<ReadPart> reads;
    std::vector<ReadPart> holes;
    cache.DivideForRead(offset, length, data.get(), &reads, &holes);

    ASSERT_EQ(1, reads.size());
    ASSERT_EQ(1, holes.size());

    ASSERT_EQ(4 * kMiB + 4 * kKiB, reads[0].offset);
    ASSERT_EQ(4 * kKiB, reads[0].length);
    ASSERT_EQ(data.get(), reads[0].data);

    ASSERT_EQ(8 * kKiB, holes[0].offset);
    ASSERT_EQ(4 * kKiB, holes[0].length);
    ASSERT_EQ(data.get() + 4 * kKiB, holes[0].data);
}

// read         |----|
// extents    |----|
TEST(ExtentCacheReadDivideTest, DivideCase8_UnWritten) {
    ExtentCache cache;

    PExtent pext;
    pext.len = 8 * kKiB;
    pext.UnWritten = true;
    pext.pOffset = 4 * kMiB;

    cache.Merge(0 * kKiB, pext);

    off_t offset = 4 * kKiB;
    size_t length = 8 * kKiB;
    std::unique_ptr<char[]> data(new char[length]);

    std::vector<ReadPart> reads;
    std::vector<ReadPart> holes;
    cache.DivideForRead(offset, length, data.get(), &reads, &holes);

    ASSERT_EQ(0, reads.size());
    ASSERT_EQ(2, holes.size());

    ASSERT_EQ(4 * kKiB, holes[0].offset);
    ASSERT_EQ(4 * kKiB, holes[0].length);
    ASSERT_EQ(data.get(), holes[0].data);

    ASSERT_EQ(8 * kKiB, holes[1].offset);
    ASSERT_EQ(4 * kKiB, holes[1].length);
    ASSERT_EQ(data.get() + 4 * kKiB, holes[1].data);
}

}  // namespace client
}  // namespace curvefs
