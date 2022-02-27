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
 * Date: Tuesday Mar 15 17:00:34 CST 2022
 * Author: wuhanqing
 */

#include <gtest/gtest.h>

#include <memory>

#include "curvefs/src/client/volume/extent_cache.h"
#include "curvefs/test/client/volume/common.h"
#include "src/common/fast_align.h"

namespace curvefs {
namespace client {

using ::curve::common::align_down;
using ::curve::common::align_up;

class ExtentCacheWriteDivideTest : public ::testing::Test {
 protected:
    void SetUp() override {
        option_.preallocSize = 32 * kKiB;
        option_.rangeSize = 1 * kGiB;
        option_.blocksize = 4 * kKiB;

        ExtentCache::SetOption(option_);
    }

 protected:
    ExtentCacheOption option_;
};

TEST_F(ExtentCacheWriteDivideTest, DivideWhenHasNoExtents) {
    ExtentCache cache;
    std::vector<WritePart> allocated;
    std::vector<AllocPart> needAlloc;

    off_t offset = 4 * kMiB;
    size_t length = 4 * kMiB;

    std::unique_ptr<char[]> data(new char[length]);

    cache.DivideForWrite(offset, length, data.get(), &allocated, &needAlloc);

    ASSERT_EQ(0, allocated.size());
    ASSERT_EQ(1, needAlloc.size());
    ASSERT_EQ(data.get(), needAlloc[0].data);
    ASSERT_EQ(offset, needAlloc[0].allocInfo.lOffset);
    ASSERT_EQ(length, needAlloc[0].allocInfo.len);
    ASSERT_FALSE(needAlloc[0].allocInfo.leftHintAvailable);
    ASSERT_FALSE(needAlloc[0].allocInfo.rightHintAvailable);
}

TEST_F(ExtentCacheWriteDivideTest, DivideWhenHasNoExtents1) {
    ExtentCache cache;
    std::vector<WritePart> allocated;
    std::vector<AllocPart> needAlloc;

    off_t offset = 0;
    size_t length = 128 * kKiB;

    std::unique_ptr<char[]> data(new char[length]);

    cache.DivideForWrite(offset, length, data.get(), &allocated, &needAlloc);

    ASSERT_EQ(0, allocated.size());
    ASSERT_EQ(1, needAlloc.size());
    ASSERT_EQ(data.get(), needAlloc[0].data);
    ASSERT_EQ(offset, needAlloc[0].allocInfo.lOffset);
    ASSERT_EQ(length, needAlloc[0].allocInfo.len);
    ASSERT_EQ(length, needAlloc[0].writelength);
    ASSERT_FALSE(needAlloc[0].allocInfo.leftHintAvailable);
    ASSERT_FALSE(needAlloc[0].allocInfo.rightHintAvailable);
}

// write          |-----|
// extents                 |-----|
TEST_F(ExtentCacheWriteDivideTest, DivideCase1) {
    ExtentCache cache;
    std::vector<WritePart> allocated;
    std::vector<AllocPart> needAlloc;

    PExtent pext;
    pext.pOffset = 12 * kMiB;
    pext.len = 4 * kMiB;
    cache.Merge(12 * kMiB, pext);

    off_t offset = 4 * kMiB;
    size_t length = 4 * kMiB;

    std::unique_ptr<char[]> data(new char[length]);

    cache.DivideForWrite(offset, length, data.get(), &allocated, &needAlloc);

    ASSERT_EQ(0, allocated.size());
    ASSERT_EQ(1, needAlloc.size());
    ASSERT_EQ(data.get(), needAlloc[0].data);
    ASSERT_EQ(offset, needAlloc[0].allocInfo.lOffset);
    ASSERT_EQ(length, needAlloc[0].allocInfo.len);
    ASSERT_FALSE(needAlloc[0].allocInfo.leftHintAvailable);
    ASSERT_FALSE(needAlloc[0].allocInfo.rightHintAvailable);
}

// write          |-----|
// extents              |-----|
TEST_F(ExtentCacheWriteDivideTest, DivideCase2) {
    ExtentCache cache;
    std::vector<WritePart> allocated;
    std::vector<AllocPart> needAlloc;

    PExtent pext;
    pext.pOffset = 12 * kMiB;
    pext.len = 4 * kMiB;
    cache.Merge(8 * kMiB, pext);

    off_t offset = 4 * kMiB;
    size_t length = 4 * kMiB;

    std::unique_ptr<char[]> data(new char[length]);

    cache.DivideForWrite(offset, length, data.get(), &allocated, &needAlloc);

    ASSERT_EQ(0, allocated.size());
    ASSERT_EQ(1, needAlloc.size());
    ASSERT_EQ(data.get(), needAlloc[0].data);
    ASSERT_EQ(offset, needAlloc[0].allocInfo.lOffset);
    ASSERT_EQ(length, needAlloc[0].allocInfo.len);
    ASSERT_TRUE(needAlloc[0].allocInfo.rightHintAvailable);
    ASSERT_EQ(12 * kMiB, needAlloc[0].allocInfo.pOffsetRight);
}

// write          |-----|
// extents            |-----|
TEST_F(ExtentCacheWriteDivideTest, DivideCase3) {
    ExtentCache cache;
    std::vector<WritePart> allocated;
    std::vector<AllocPart> needAlloc;

    PExtent pext;

    // 6MiB ~ 10MiB is allocated
    pext.pOffset = 12 * kMiB;
    pext.len = 4 * kMiB;
    cache.Merge(6 * kMiB, pext);

    // write 4MiB ~ 8MiB
    off_t offset = 4 * kMiB;
    size_t length = 4 * kMiB;

    std::unique_ptr<char[]> data(new char[length]);

    cache.DivideForWrite(offset, length, data.get(), &allocated, &needAlloc);

    ASSERT_EQ(1, allocated.size());
    ASSERT_EQ(1, needAlloc.size());
    ASSERT_EQ(data.get(), needAlloc[0].data);
    ASSERT_EQ(offset, needAlloc[0].allocInfo.lOffset);
    ASSERT_EQ(length / 2, needAlloc[0].allocInfo.len);
    ASSERT_TRUE(needAlloc[0].allocInfo.rightHintAvailable);
    ASSERT_EQ(12 * kMiB, needAlloc[0].allocInfo.pOffsetRight);

    ASSERT_EQ(pext.pOffset, allocated[0].offset);
    ASSERT_EQ(length / 2, allocated[0].length);
    ASSERT_EQ(data.get() + length / 2, allocated[0].data);
}

// write          |---------|
// extents            |-----|
TEST_F(ExtentCacheWriteDivideTest, DivideCase4) {
    ExtentCache cache;
    std::vector<WritePart> allocated;
    std::vector<AllocPart> needAlloc;

    PExtent pext;

    // 6MiB ~ 10MiB is allocated
    pext.pOffset = 12 * kMiB;
    pext.len = 4 * kMiB;
    cache.Merge(6 * kMiB, pext);

    // write 4MiB ~ 10MiB
    off_t offset = 4 * kMiB;
    size_t length = 6 * kMiB;

    std::unique_ptr<char[]> data(new char[length]);

    cache.DivideForWrite(offset, length, data.get(), &allocated, &needAlloc);

    ASSERT_EQ(1, allocated.size());
    ASSERT_EQ(1, needAlloc.size());

    ASSERT_EQ(data.get(), needAlloc[0].data);
    ASSERT_EQ(4 * kMiB, needAlloc[0].allocInfo.lOffset);
    ASSERT_EQ(2 * kMiB, needAlloc[0].allocInfo.len);
    ASSERT_TRUE(needAlloc[0].allocInfo.rightHintAvailable);
    ASSERT_EQ(12 * kMiB, needAlloc[0].allocInfo.pOffsetRight);

    ASSERT_EQ(12 * kMiB, allocated[0].offset);
    ASSERT_EQ(4 * kMiB, allocated[0].length);
    ASSERT_EQ(data.get() + 2 * kMiB, allocated[0].data);
}

// write          |-----------|
// extents            |-----|
TEST_F(ExtentCacheWriteDivideTest, DivideCase5) {
    ExtentCache cache;
    std::vector<WritePart> allocated;
    std::vector<AllocPart> needAlloc;

    PExtent pext;

    // 6MiB ~ 10MiB is allocated
    pext.pOffset = 12 * kMiB;
    pext.len = 4 * kMiB;
    cache.Merge(6 * kMiB, pext);

    // write 4MiB ~ 12MiB
    off_t offset = 4 * kMiB;
    size_t length = 8 * kMiB;

    std::unique_ptr<char[]> data(new char[length]);

    cache.DivideForWrite(offset, length, data.get(), &allocated, &needAlloc);

    ASSERT_EQ(1, allocated.size());
    ASSERT_EQ(2, needAlloc.size());

    ASSERT_EQ(data.get(), needAlloc[0].data);
    ASSERT_EQ(4 * kMiB, needAlloc[0].allocInfo.lOffset);
    ASSERT_EQ(2 * kMiB, needAlloc[0].allocInfo.len);
    ASSERT_TRUE(needAlloc[0].allocInfo.rightHintAvailable);
    ASSERT_EQ(12 * kMiB, needAlloc[0].allocInfo.pOffsetRight);

    ASSERT_EQ(data.get() + 6 * kMiB, needAlloc[1].data);
    ASSERT_EQ(10 * kMiB, needAlloc[1].allocInfo.lOffset);
    ASSERT_EQ(2 * kMiB, needAlloc[1].allocInfo.len);
    ASSERT_TRUE(needAlloc[1].allocInfo.leftHintAvailable);
    ASSERT_EQ(16 * kMiB, needAlloc[1].allocInfo.pOffsetLeft);

    ASSERT_EQ(data.get() + 2 * kMiB, allocated[0].data);
    ASSERT_EQ(12 * kMiB, allocated[0].offset);
    ASSERT_EQ(4 * kMiB, allocated[0].length);
}

// write          |---|
// extents        |-----|
TEST_F(ExtentCacheWriteDivideTest, DivideCase6) {
    ExtentCache cache;
    std::vector<WritePart> allocated;
    std::vector<AllocPart> needAlloc;

    PExtent pext;

    // 6MiB ~ 10MiB is allocated
    pext.pOffset = 12 * kMiB;
    pext.len = 4 * kMiB;
    cache.Merge(6 * kMiB, pext);

    // write 6MiB ~ 8MiB
    off_t offset = 6 * kMiB;
    size_t length = 2 * kMiB;

    std::unique_ptr<char[]> data(new char[length]);

    cache.DivideForWrite(offset, length, data.get(), &allocated, &needAlloc);

    ASSERT_EQ(1, allocated.size());
    ASSERT_EQ(0, needAlloc.size());

    ASSERT_EQ(data.get(), allocated[0].data);
    ASSERT_EQ(12 * kMiB, allocated[0].offset);
    ASSERT_EQ(2 * kMiB, allocated[0].length);
}

// write          |-----|
// extents        |-----|
TEST_F(ExtentCacheWriteDivideTest, DivideCase7) {
    ExtentCache cache;
    std::vector<WritePart> allocated;
    std::vector<AllocPart> needAlloc;

    PExtent pext;

    // 6MiB ~ 10MiB is allocated
    pext.pOffset = 12 * kMiB;
    pext.len = 4 * kMiB;
    cache.Merge(6 * kMiB, pext);

    // write 6MiB ~ 10MiB
    off_t offset = 6 * kMiB;
    size_t length = 4 * kMiB;

    std::unique_ptr<char[]> data(new char[length]);

    cache.DivideForWrite(offset, length, data.get(), &allocated, &needAlloc);

    ASSERT_EQ(1, allocated.size());
    ASSERT_EQ(0, needAlloc.size());

    ASSERT_EQ(data.get(), allocated[0].data);
    ASSERT_EQ(12 * kMiB, allocated[0].offset);
    ASSERT_EQ(4 * kMiB, allocated[0].length);
}

// write          |-------|
// extents        |-----|
TEST_F(ExtentCacheWriteDivideTest, DivideCase8) {
    ExtentCache cache;
    std::vector<WritePart> allocated;
    std::vector<AllocPart> needAlloc;

    PExtent pext;

    // 6MiB ~ 10MiB is allocated
    pext.pOffset = 12 * kMiB;
    pext.len = 4 * kMiB;
    cache.Merge(6 * kMiB, pext);

    // write 6MiB ~ 12MiB
    off_t offset = 6 * kMiB;
    size_t length = 6 * kMiB;

    std::unique_ptr<char[]> data(new char[length]);

    cache.DivideForWrite(offset, length, data.get(), &allocated, &needAlloc);

    ASSERT_EQ(1, allocated.size());
    ASSERT_EQ(1, needAlloc.size());

    ASSERT_EQ(data.get(), allocated[0].data);
    ASSERT_EQ(12 * kMiB, allocated[0].offset);
    ASSERT_EQ(4 * kMiB, allocated[0].length);

    ASSERT_EQ(data.get() + 4 * kMiB, needAlloc[0].data);
    ASSERT_EQ(10 * kMiB, needAlloc[0].allocInfo.lOffset);
    ASSERT_EQ(2 * kMiB, needAlloc[0].allocInfo.len);
    ASSERT_TRUE(needAlloc[0].allocInfo.leftHintAvailable);
    ASSERT_EQ(16 * kMiB, needAlloc[0].allocInfo.pOffsetLeft);
}

// write                   |-----|
// extents        |-----|
TEST_F(ExtentCacheWriteDivideTest, DivideCase9) {
    ExtentCache cache;
    std::vector<WritePart> allocated;
    std::vector<AllocPart> needAlloc;

    PExtent pext;

    // 6MiB ~ 10MiB is allocated
    pext.pOffset = 12 * kMiB;
    pext.len = 4 * kMiB;
    cache.Merge(6 * kMiB, pext);

    // write 12MiB ~ 14MiB
    off_t offset = 12 * kMiB;
    size_t length = 2 * kMiB;

    std::unique_ptr<char[]> data(new char[length]);

    cache.DivideForWrite(offset, length, data.get(), &allocated, &needAlloc);

    ASSERT_EQ(0, allocated.size());
    ASSERT_EQ(1, needAlloc.size());

    ASSERT_EQ(data.get(), needAlloc[0].data);
    ASSERT_EQ(12 * kMiB, needAlloc[0].allocInfo.lOffset);
    ASSERT_EQ(2 * kMiB, needAlloc[0].allocInfo.len);
    ASSERT_FALSE(needAlloc[0].allocInfo.leftHintAvailable);
    ASSERT_FALSE(needAlloc[0].allocInfo.rightHintAvailable);
}

// write                |-----|
// extents        |-----|
TEST_F(ExtentCacheWriteDivideTest, DivideCase10) {
    ExtentCache cache;
    std::vector<WritePart> allocated;
    std::vector<AllocPart> needAlloc;

    PExtent pext;

    // 6MiB ~ 10MiB is allocated
    pext.pOffset = 12 * kMiB;
    pext.len = 4 * kMiB;
    cache.Merge(6 * kMiB, pext);

    // write 10MiB ~ 14MiB
    off_t offset = 10 * kMiB;
    size_t length = 4 * kMiB;

    std::unique_ptr<char[]> data(new char[length]);

    cache.DivideForWrite(offset, length, data.get(), &allocated, &needAlloc);

    ASSERT_EQ(0, allocated.size());
    ASSERT_EQ(1, needAlloc.size());

    ASSERT_EQ(data.get(), needAlloc[0].data);
    ASSERT_EQ(10 * kMiB, needAlloc[0].allocInfo.lOffset);
    ASSERT_EQ(4 * kMiB, needAlloc[0].allocInfo.len);
    ASSERT_TRUE(needAlloc[0].allocInfo.leftHintAvailable);
    ASSERT_EQ(16 * kMiB, needAlloc[0].allocInfo.pOffsetLeft);
}

// write            |-----|
// extents     |------|
TEST_F(ExtentCacheWriteDivideTest, DivideCase11) {
    ExtentCache cache;
    std::vector<WritePart> allocated;
    std::vector<AllocPart> needAlloc;

    PExtent pext;

    // 0 ~ 5MiB is allocated
    pext.pOffset = 100 * kMiB;
    pext.len = 5 * kMiB;
    cache.Merge(0, pext);

    // write 4MiB ~ 8MiB
    off_t offset = 4 * kMiB;
    size_t length = 4 * kMiB;

    std::unique_ptr<char[]> data(new char[length]);

    cache.DivideForWrite(offset, length, data.get(), &allocated, &needAlloc);

    ASSERT_EQ(1, allocated.size());
    ASSERT_EQ(1, needAlloc.size());

    ASSERT_EQ(5 * kMiB, needAlloc[0].allocInfo.lOffset);
    ASSERT_EQ(3 * kMiB, needAlloc[0].allocInfo.len);
    ASSERT_EQ(data.get() + 1 * kMiB, needAlloc[0].data);
    ASSERT_TRUE(needAlloc[0].allocInfo.leftHintAvailable);
    ASSERT_EQ(105 * kMiB, needAlloc[0].allocInfo.pOffsetLeft);

    ASSERT_EQ(104 * kMiB, allocated[0].offset);
    ASSERT_EQ(1 * kMiB, allocated[0].length);
    ASSERT_EQ(data.get(), allocated[0].data);
}

// write            |-----|
// extents     |----------|
TEST_F(ExtentCacheWriteDivideTest, DivideCase12) {
    ExtentCache cache;
    std::vector<WritePart> allocated;
    std::vector<AllocPart> needAlloc;

    PExtent pext;

    // 0 ~ 5MiB is allocated
    pext.pOffset = 100 * kMiB;
    pext.len = 5 * kMiB;
    cache.Merge(0, pext);

    // write 4MiB ~ 5MiB
    off_t offset = 4 * kMiB;
    size_t length = 1 * kMiB;

    std::unique_ptr<char[]> data(new char[length]);

    cache.DivideForWrite(offset, length, data.get(), &allocated, &needAlloc);

    ASSERT_EQ(1, allocated.size());
    ASSERT_EQ(0, needAlloc.size());

    ASSERT_EQ(data.get(), allocated[0].data);
    ASSERT_EQ(104 * kMiB, allocated[0].offset);
    ASSERT_EQ(1 * kMiB, allocated[0].length);
}

// write            |-----|
// extents     |--------------|
TEST_F(ExtentCacheWriteDivideTest, DivideCase13) {
    ExtentCache cache;
    std::vector<WritePart> allocated;
    std::vector<AllocPart> needAlloc;

    PExtent pext;

    // 0 ~ 5MiB is allocated
    pext.pOffset = 100 * kMiB;
    pext.len = 5 * kMiB;
    cache.Merge(0, pext);

    // write 2MiB ~ 3MiB
    off_t offset = 2 * kMiB;
    size_t length = 1 * kMiB;

    std::unique_ptr<char[]> data(new char[length]);

    cache.DivideForWrite(offset, length, data.get(), &allocated, &needAlloc);

    ASSERT_EQ(1, allocated.size());
    ASSERT_EQ(0, needAlloc.size());

    ASSERT_EQ(data.get(), allocated[0].data);
    ASSERT_EQ(102 * kMiB, allocated[0].offset);
    ASSERT_EQ(1 * kMiB, allocated[0].length);
}

// write            |-----|
// extents     |-----|   |-----|
TEST_F(ExtentCacheWriteDivideTest, DivideCase14) {
    ExtentCache cache;
    std::vector<WritePart> allocated;
    std::vector<AllocPart> needAlloc;

    PExtent pext;
    // 6MiB ~ 10MiB is allocated
    pext.pOffset = 12 * kMiB;
    pext.len = 4 * kMiB;
    cache.Merge(6 * kMiB, pext);

    // 0 ~ 5MiB is allocated
    pext.pOffset = 100 * kMiB;
    pext.len = 5 * kMiB;
    cache.Merge(0, pext);

    // write 4MiB ~ 8MiB
    off_t offset = 4 * kMiB;
    size_t length = 4 * kMiB;

    std::unique_ptr<char[]> data(new char[length]);

    cache.DivideForWrite(offset, length, data.get(), &allocated, &needAlloc);

    ASSERT_EQ(2, allocated.size());
    ASSERT_EQ(1, needAlloc.size());

    ASSERT_EQ(5 * kMiB, needAlloc[0].allocInfo.lOffset);
    ASSERT_EQ(1 * kMiB, needAlloc[0].allocInfo.len);
    ASSERT_EQ(data.get() + 1 * kMiB, needAlloc[0].data);
    ASSERT_TRUE(needAlloc[0].allocInfo.leftHintAvailable);
    ASSERT_EQ(105 * kMiB, needAlloc[0].allocInfo.pOffsetLeft);
    ASSERT_FALSE(needAlloc[0].allocInfo.rightHintAvailable);

    ASSERT_EQ(104 * kMiB, allocated[0].offset);
    ASSERT_EQ(1 * kMiB, allocated[0].length);
    ASSERT_EQ(data.get(), allocated[0].data);

    ASSERT_EQ(12 * kMiB, allocated[1].offset);
    ASSERT_EQ(2 * kMiB, allocated[1].length);
    ASSERT_EQ(data.get() + 2 * kMiB, allocated[1].data);
}

// write       |----------------------|
// extents     |-----|   |-----|   |-----|
TEST_F(ExtentCacheWriteDivideTest, DivideCase15) {
    ExtentCache cache;
    std::vector<WritePart> allocated;
    std::vector<AllocPart> needAlloc;

    PExtent pext;
    // 6MiB ~ 10MiB is allocated
    pext.pOffset = 12 * kMiB;
    pext.len = 4 * kMiB;
    cache.Merge(6 * kMiB, pext);

    // 12 ~ 14MiB is allocated
    pext.pOffset = 100 * kMiB;
    pext.len = 2 * kMiB;
    cache.Merge(12 * kMiB, pext);

    // 15 ~ 18MiB is allocated
    pext.pOffset = 1 * kGiB;
    pext.len = 3 * kMiB;
    cache.Merge(15 * kMiB, pext);

    // write 6MiB ~ 16MiB
    off_t offset = 6 * kMiB;
    size_t length = 10 * kMiB;

    std::unique_ptr<char[]> data(new char[length]);

    cache.DivideForWrite(offset, length, data.get(), &allocated, &needAlloc);

    ASSERT_EQ(3, allocated.size());
    ASSERT_EQ(2, needAlloc.size());

    ASSERT_EQ(data.get(), allocated[0].data);
    ASSERT_EQ(12 * kMiB, allocated[0].offset);
    ASSERT_EQ(4 * kMiB, allocated[0].length);

    ASSERT_EQ(data.get() + 4 * kMiB, needAlloc[0].data);
    ASSERT_EQ(10 * kMiB, needAlloc[0].allocInfo.lOffset);
    ASSERT_EQ(2 * kMiB, needAlloc[0].allocInfo.len);
    ASSERT_TRUE(needAlloc[0].allocInfo.leftHintAvailable);
    ASSERT_EQ(16 * kMiB, needAlloc[0].allocInfo.pOffsetLeft);

    ASSERT_EQ(data.get() + 6 * kMiB, allocated[1].data);
    ASSERT_EQ(100 * kMiB, allocated[1].offset);
    ASSERT_EQ(2 * kMiB, allocated[1].length);

    ASSERT_EQ(data.get() + 8 * kMiB, needAlloc[1].data);
    ASSERT_EQ(14 * kMiB, needAlloc[1].allocInfo.lOffset);
    ASSERT_EQ(1 * kMiB, needAlloc[1].allocInfo.len);
    ASSERT_TRUE(needAlloc[1].allocInfo.leftHintAvailable);
    ASSERT_EQ(102 * kMiB, needAlloc[1].allocInfo.pOffsetLeft);

    ASSERT_EQ(data.get() + 9 * kMiB, allocated[2].data);
    ASSERT_EQ(1 * kGiB, allocated[2].offset);
    ASSERT_EQ(1 * kMiB, allocated[2].length);
}

// write       |----------|
// extents     |----|                       |-----|
// We can prealloc more space, but it can't overlap with next allocated extent
TEST_F(ExtentCacheWriteDivideTest, DivideCase16_BoundaryTest) {
    option_.preallocSize = 64 * kKiB;
    ExtentCache::SetOption(option_);
    ExtentCache cache;

    std::vector<WritePart> allocated;
    std::vector<AllocPart> needAlloc;

    PExtent pext;
    // 32KiB ~ 64KiB is allocated
    pext.pOffset = 12 * kMiB;
    pext.len = 32 * kKiB;
    cache.Merge(32 * kKiB, pext);

    // 0KiB ~ 4KiB is allocated
    pext.pOffset = 15 * kMiB;
    pext.len = 4 * kKiB;
    cache.Merge(0 * kKiB, pext);

    // write 0 ~ 8KiB
    off_t offset = 0 * kKiB;
    size_t length = 8 * kKiB;

    std::unique_ptr<char[]> data(new char[length]);

    cache.DivideForWrite(offset, length, data.get(), &allocated, &needAlloc);

    ASSERT_EQ(1, allocated.size());
    ASSERT_EQ(1, needAlloc.size());

    ASSERT_EQ(data.get(), allocated[0].data);
    ASSERT_EQ(15 * kMiB, allocated[0].offset);
    ASSERT_EQ(4 * kKiB, allocated[0].length);

    ASSERT_EQ(data.get() + 4 * kKiB, needAlloc[0].data);
    ASSERT_EQ(4 * kKiB, needAlloc[0].allocInfo.lOffset);
    ASSERT_EQ(28 * kKiB, needAlloc[0].allocInfo.len);
    ASSERT_TRUE(needAlloc[0].allocInfo.leftHintAvailable);
    ASSERT_EQ(15 * kMiB + 4 * kKiB, needAlloc[0].allocInfo.pOffsetLeft);
}

//                            |
// write                |-----|
// extents                    | range boundary
//                            |
// We can prealloc more space, but it can't overlap with next range
TEST_F(ExtentCacheWriteDivideTest, DivideCase17_BoundaryTest) {
    option_.preallocSize = 64 * kKiB;
    ExtentCache::SetOption(option_);
    ExtentCache cache;

    std::vector<WritePart> allocated;
    std::vector<AllocPart> needAlloc;

    // write 0 ~ 8KiB
    off_t offset = option_.rangeSize - 4 * kKiB;
    size_t length = 4 * kKiB;

    std::unique_ptr<char[]> data(new char[length]);

    cache.DivideForWrite(offset, length, data.get(), &allocated, &needAlloc);

    ASSERT_EQ(0, allocated.size());
    ASSERT_EQ(1, needAlloc.size());

    ASSERT_EQ(data.get(), needAlloc[0].data);
    ASSERT_EQ(option_.rangeSize - 4 * kKiB, needAlloc[0].allocInfo.lOffset);
    ASSERT_EQ(4 * kKiB, needAlloc[0].allocInfo.len);
    ASSERT_FALSE(needAlloc[0].allocInfo.leftHintAvailable);
}

TEST_F(ExtentCacheWriteDivideTest, DivideCase18_EmptyRangeAndUnalignedWrite) {
    ExtentCache cache;

    ExtentCacheOption opt;
    opt.blocksize = 4 * kKiB;
    opt.preallocSize = 4 * kKiB;

    cache.SetOption(opt);

    std::vector<WritePart> allocated;
    std::vector<AllocPart> needAlloc;

    // write 64 ~ 616
    off_t offset = 64;
    size_t length = 616;

    std::unique_ptr<char[]> data(new char[length]);

    cache.DivideForWrite(offset, length, data.get(), &allocated, &needAlloc);

    ASSERT_TRUE(allocated.empty());
    ASSERT_EQ(1, needAlloc.size());

    auto& alloc = needAlloc[0];

    ASSERT_EQ(alloc.allocInfo.lOffset, 0);
    ASSERT_EQ(alloc.allocInfo.len, 4 * kKiB);

    ASSERT_EQ(alloc.writelength, length);
    ASSERT_EQ(alloc.padding, offset);
    ASSERT_EQ(alloc.data, data.get());
}

TEST_F(ExtentCacheWriteDivideTest, DivideCase18_EmptyRangeAndUnalignedWrite2) {
    ExtentCache cache;

    ExtentCacheOption opt;
    opt.blocksize = 4 * kKiB;
    opt.preallocSize = 4 * kKiB;

    cache.SetOption(opt);

    std::vector<WritePart> allocated;
    std::vector<AllocPart> needAlloc;

    // write 64 ~ 4096
    off_t offset = 64;
    size_t length = 4096;

    std::unique_ptr<char[]> data(new char[length]);

    cache.DivideForWrite(offset, length, data.get(), &allocated, &needAlloc);

    ASSERT_TRUE(allocated.empty());
    ASSERT_EQ(1, needAlloc.size());

    auto& alloc = needAlloc[0];

    ASSERT_EQ(alloc.allocInfo.lOffset, 0);
    ASSERT_EQ(alloc.allocInfo.len, 8 * kKiB);
    ASSERT_EQ(alloc.writelength, length);
    ASSERT_EQ(alloc.padding, offset);
    ASSERT_EQ(alloc.data, data.get());
}

TEST_F(ExtentCacheWriteDivideTest,
       DivideCase18_EmptyRangeAndUnalignedWrite3_CrossBoundary) {
    ExtentCache cache;

    ExtentCacheOption opt;
    opt.blocksize = 4 * kKiB;
    opt.preallocSize = 4 * kKiB;

    cache.SetOption(opt);

    std::vector<WritePart> allocated;
    std::vector<AllocPart> needAlloc;

    off_t offset = opt.rangeSize - 4;
    size_t length = 4096;

    std::unique_ptr<char[]> data(new char[length]);

    cache.DivideForWrite(offset, length, data.get(), &allocated, &needAlloc);

    ASSERT_TRUE(allocated.empty());
    ASSERT_EQ(2, needAlloc.size());

    auto& alloc1 = needAlloc[0];
    auto& alloc2 = needAlloc[1];

    ASSERT_EQ(alloc1.allocInfo.lOffset, align_down(offset, opt.blocksize));
    ASSERT_EQ(alloc1.allocInfo.len, opt.rangeSize - alloc1.allocInfo.lOffset);
    ASSERT_EQ(alloc1.writelength, 4);
    ASSERT_EQ(alloc1.padding, 4092);
    ASSERT_EQ(alloc1.data, data.get());

    ASSERT_EQ(alloc2.allocInfo.lOffset, opt.rangeSize);
    ASSERT_EQ(alloc2.allocInfo.len, opt.preallocSize);
    ASSERT_EQ(alloc2.writelength, 4092);
    ASSERT_EQ(alloc2.padding, 0);
    ASSERT_EQ(alloc2.data, data.get() + 4);
}

// write                   |---|
// extents        |-----|
TEST_F(ExtentCacheWriteDivideTest, DivideCase19_UnalignedWrite1) {
    ExtentCache cache;
    ExtentCacheOption opt;
    opt.preallocSize = 4 * kKiB;
    opt.blocksize = 4 * kKiB;
    cache.SetOption(opt);

    std::vector<WritePart> allocated;
    std::vector<AllocPart> needAlloc;

    PExtent pext;

    // 6MiB ~ 10MiB is allocated
    pext.pOffset = 12 * kMiB;
    pext.len = 4 * kMiB;
    cache.Merge(6 * kMiB, pext);

    off_t offset = 10 * kMiB + 6;
    size_t length = 1234;

    std::unique_ptr<char[]> data(new char[length]);

    cache.DivideForWrite(offset, length, data.get(), &allocated, &needAlloc);

    ASSERT_EQ(0, allocated.size());
    ASSERT_EQ(1, needAlloc.size());

    auto& alloc = needAlloc[0];

    ASSERT_EQ(data.get(), alloc.data);
    ASSERT_EQ(10 * kMiB, alloc.allocInfo.lOffset);
    ASSERT_EQ(opt.preallocSize, alloc.allocInfo.len);
    ASSERT_TRUE(alloc.allocInfo.leftHintAvailable);
    ASSERT_EQ(16 * kMiB, alloc.allocInfo.pOffsetLeft);
    ASSERT_FALSE(alloc.allocInfo.rightHintAvailable);

    ASSERT_EQ(6, alloc.padding);
    ASSERT_EQ(1234, alloc.writelength);
}

// write              |---|
// extents        |-----|
TEST_F(ExtentCacheWriteDivideTest, DivideCase19_UnalignedWrite2) {
    ExtentCache cache;
    ExtentCacheOption opt;
    opt.preallocSize = 4 * kKiB;
    opt.blocksize = 4 * kKiB;
    cache.SetOption(opt);

    std::vector<WritePart> allocated;
    std::vector<AllocPart> needAlloc;

    PExtent pext;

    // 6MiB ~ 10MiB is allocated
    pext.pOffset = 12 * kMiB;
    pext.len = 4 * kMiB;
    cache.Merge(6 * kMiB, pext);

    off_t offset = 10 * kMiB - 6;
    size_t length = 1234;

    std::unique_ptr<char[]> data(new char[length]);

    cache.DivideForWrite(offset, length, data.get(), &allocated, &needAlloc);

    ASSERT_EQ(1, allocated.size());
    ASSERT_EQ(1, needAlloc.size());

    auto& part = allocated[0];
    auto& alloc = needAlloc[0];

    ASSERT_EQ(data.get(), part.data);
    ASSERT_EQ(pext.pOffset + pext.len - 6, part.offset);
    ASSERT_EQ(6, part.length);

    ASSERT_EQ(data.get() + 6, alloc.data);
    ASSERT_EQ(10 * kMiB, alloc.allocInfo.lOffset);
    ASSERT_EQ(opt.preallocSize, alloc.allocInfo.len);
    ASSERT_TRUE(alloc.allocInfo.leftHintAvailable);
    ASSERT_EQ(16 * kMiB, alloc.allocInfo.pOffsetLeft);
    ASSERT_FALSE(alloc.allocInfo.rightHintAvailable);

    ASSERT_EQ(0, alloc.padding);
    ASSERT_EQ(1228, alloc.writelength);
}

// write       |---|
// extents        |-----|
TEST_F(ExtentCacheWriteDivideTest, DivideCase19_UnalignedWrite3) {
    ExtentCache cache;
    ExtentCacheOption opt;
    opt.preallocSize = 4 * kKiB;
    opt.blocksize = 4 * kKiB;
    cache.SetOption(opt);

    std::vector<WritePart> allocated;
    std::vector<AllocPart> needAlloc;

    PExtent pext;

    // 6MiB ~ 10MiB is allocated
    pext.pOffset = 12 * kMiB;
    pext.len = 4 * kMiB;
    cache.Merge(6 * kMiB, pext);

    off_t offset = 6 * kMiB - 6;
    size_t length = 1234;

    std::unique_ptr<char[]> data(new char[length]);

    cache.DivideForWrite(offset, length, data.get(), &allocated, &needAlloc);

    ASSERT_EQ(1, allocated.size());
    ASSERT_EQ(1, needAlloc.size());

    auto& part = allocated[0];
    auto& alloc = needAlloc[0];

    ASSERT_EQ(data.get(), alloc.data);
    ASSERT_EQ(6 * kMiB - 4 * kKiB, alloc.allocInfo.lOffset);
    ASSERT_EQ(4 * kKiB, alloc.allocInfo.len);
    ASSERT_FALSE(alloc.allocInfo.leftHintAvailable);
    ASSERT_TRUE(alloc.allocInfo.rightHintAvailable);
    ASSERT_EQ(12 * kMiB, alloc.allocInfo.pOffsetRight);

    ASSERT_EQ(4090, alloc.padding);
    ASSERT_EQ(6, alloc.writelength);

    ASSERT_EQ(data.get() + 6, part.data);
    ASSERT_EQ(pext.pOffset, part.offset);
    ASSERT_EQ(1228, part.length);
}

}  // namespace client
}  // namespace curvefs
