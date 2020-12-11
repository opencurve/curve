/*
 *  Copyright (c) 2020 NetEase Inc.
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
 * Date: Sun Dec 20 10:25:08 CST 2020
 */

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include <chrono>  // NOLINT
#include <string>
#include <thread>  // NOLINT

#include "test/client/mock/mock_mdsclient.h"
#include "test/client/mock/mock_meta_cache.h"

namespace curve {
namespace client {

using ::testing::AllOf;
using ::testing::Ge;
using ::testing::Le;
using ::testing::Matcher;
using ::testing::Return;

class IOTrackerTest : public ::testing::Test {
 public:
    void SetUp() override {
        opt_.taskDelayMs = 10;
        IOTracker::InitDiscardOption(opt_);

        metric.reset(new DiscardMetric("IOTrackerTest"));
        discardTaskManager_.reset(new DiscardTaskManager(metric.get()));

        mockMDSClient_.reset(new MockMDSClient());
        mockMetaCache_.reset(new MockMetaCache());

        MetaCacheOption metaCacheOpt;
        metaCacheOpt.discardGranularity = discardGranularity_;
        mockMetaCache_->Init(metaCacheOpt, mockMDSClient_.get());

        fileInfo_.fullPathName = "/IOTrackerTest";
        fileInfo_.length = 100 * GiB;
        fileInfo_.segmentsize = segmentSize_;
        fileInfo_.chunksize = 16 * MiB;

        mockMetaCache_->UpdateFileInfo(fileInfo_);
    }

    void TearDown() override {
        discardTaskManager_->Stop();
    }

 protected:
    DiscardOption opt_;
    FInfo fileInfo_;
    std::unique_ptr<DiscardMetric> metric;
    std::unique_ptr<MockMetaCache> mockMetaCache_;
    std::unique_ptr<MockMDSClient> mockMDSClient_;
    std::unique_ptr<DiscardTaskManager> discardTaskManager_;
    uint64_t segmentSize_ = 1 * GiB;
    uint32_t discardGranularity_ = 4 * KiB;
};

TEST_F(IOTrackerTest, TestDiscardNotSatisfyOneSegment) {
    IOTracker iotracker(nullptr, mockMetaCache_.get(), nullptr);
    uint64_t offset = 50 * GiB;
    uint32_t length = 512 * MiB;

    EXPECT_CALL(*mockMDSClient_, DeAllocateSegment(_, _)).Times(0);
    EXPECT_CALL(*mockMetaCache_, CleanChunksInSegment(_)).Times(0);

    iotracker.StartDiscard(offset, length, mockMDSClient_.get(), &fileInfo_,
                           discardTaskManager_.get());
    ASSERT_EQ(0, iotracker.Wait());

    // check bitmap
    Bitmap& bitmap = mockMetaCache_->GetFileSegment(50)->GetBitmap();
    std::vector<curve::common::BitRange> clearRanges;
    std::vector<curve::common::BitRange> setRanges;
    bitmap.Divide(0, bitmap.Size(), &clearRanges, &setRanges);
    ASSERT_EQ(1, clearRanges.size());
    ASSERT_EQ(1, setRanges.size());

    ASSERT_EQ(0, setRanges[0].beginIndex);
    ASSERT_EQ(segmentSize_ / discardGranularity_ / 2 - 1,
              setRanges[0].endIndex);  // NOLINT
    ASSERT_EQ(segmentSize_ / discardGranularity_ / 2,
              clearRanges[0].beginIndex);  // NOLINT
    ASSERT_EQ(segmentSize_ / discardGranularity_ - 1, clearRanges[0].endIndex);
}

TEST_F(IOTrackerTest, TestDiscardOneSegment) {
    IOTracker iotracker(nullptr, mockMetaCache_.get(), nullptr);
    uint64_t offset = 50 * GiB;
    uint32_t length = 1 * GiB;

    EXPECT_CALL(*mockMDSClient_, DeAllocateSegment(_, 50 * GiB))
        .WillOnce(Return(LIBCURVE_ERROR::OK));
    EXPECT_CALL(*mockMetaCache_, CleanChunksInSegment(50)).Times(1);

    iotracker.StartDiscard(offset, length, mockMDSClient_.get(), &fileInfo_,
                           discardTaskManager_.get());
    ASSERT_EQ(0, iotracker.Wait());

    // check bitmap
    Bitmap& bitmap = mockMetaCache_->GetFileSegment(50)->GetBitmap();
    ASSERT_EQ(Bitmap::NO_POS, bitmap.NextClearBit(0));

    std::this_thread::sleep_for(
        std::chrono::milliseconds(5 * opt_.taskDelayMs));

    // check bitmap after discard
    bitmap = mockMetaCache_->GetFileSegment(50)->GetBitmap();
    ASSERT_EQ(Bitmap::NO_POS, bitmap.NextSetBit(0));
}

TEST_F(IOTrackerTest, TestDiscardMultiSegment) {
    // discard three times
    IOTracker iotracker1(nullptr, mockMetaCache_.get(), nullptr);
    uint64_t offset1 = 50 * GiB;
    uint32_t length1 = 512 * MiB;

    IOTracker iotracker2(nullptr, mockMetaCache_.get(), nullptr);
    uint64_t offset2 = 52 * GiB + 512 * MiB;
    uint32_t length2 = 512 * MiB;

    IOTracker iotracker3(nullptr, mockMetaCache_.get(), nullptr);
    uint64_t offset3 = 50 * GiB + 512 * MiB;
    uint32_t length3 = 2 * GiB;

    Matcher<uint64_t> offsetRange = AllOf(Ge(50 * GiB), Le(52 * GiB));
    EXPECT_CALL(*mockMDSClient_, DeAllocateSegment(_, offsetRange))
        .Times(3)
        .WillRepeatedly(Return(LIBCURVE_ERROR::OK));

    Matcher<SegmentIndex> segmentIndexRange = AllOf(Ge(50), Le(52));
    EXPECT_CALL(*mockMetaCache_, CleanChunksInSegment(segmentIndexRange))
        .Times(3);

    iotracker1.StartDiscard(offset1, length1, mockMDSClient_.get(), &fileInfo_,
                            discardTaskManager_.get());
    ASSERT_EQ(0, iotracker1.Wait());

    iotracker2.StartDiscard(offset2, length2, mockMDSClient_.get(), &fileInfo_,
                            discardTaskManager_.get());
    ASSERT_EQ(0, iotracker2.Wait());

    iotracker3.StartDiscard(offset3, length3, mockMDSClient_.get(), &fileInfo_,
                            discardTaskManager_.get());
    ASSERT_EQ(0, iotracker3.Wait());

    // check bitmap
    Bitmap& bitmap1 = mockMetaCache_->GetFileSegment(50)->GetBitmap();
    ASSERT_EQ(Bitmap::NO_POS, bitmap1.NextClearBit(0));
    Bitmap& bitmap2 = mockMetaCache_->GetFileSegment(51)->GetBitmap();
    ASSERT_EQ(Bitmap::NO_POS, bitmap2.NextClearBit(0));
    Bitmap& bitmap3 = mockMetaCache_->GetFileSegment(52)->GetBitmap();
    ASSERT_EQ(Bitmap::NO_POS, bitmap3.NextClearBit(0));

    std::this_thread::sleep_for(
        std::chrono::milliseconds(5 * opt_.taskDelayMs));

    // check bitmap after discard
    bitmap1 = mockMetaCache_->GetFileSegment(50)->GetBitmap();
    ASSERT_EQ(Bitmap::NO_POS, bitmap1.NextSetBit(0));
    bitmap2 = mockMetaCache_->GetFileSegment(51)->GetBitmap();
    ASSERT_EQ(Bitmap::NO_POS, bitmap2.NextSetBit(0));
    bitmap3 = mockMetaCache_->GetFileSegment(52)->GetBitmap();
    ASSERT_EQ(Bitmap::NO_POS, bitmap3.NextSetBit(0));
}

}  // namespace client
}  // namespace curve
