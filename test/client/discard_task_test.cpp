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
 * Date: Sat Dec 19 22:49:56 CST 2020
 */


#include "src/client/discard_task.h"

#include <gflags/gflags.h>
#include <gtest/gtest.h>

#include <memory>

#include "src/client/auth_client.h"
#include "src/client/config_info.h"
#include "src/common/authenticator.h"
#include "test/client/mock/mock_mdsclient.h"
#include "test/client/mock/mock_meta_cache.h"

namespace curve {
namespace client {

using ::testing::Return;

class DiscardTaskTest : public ::testing::Test {
 public:
    void SetUp() override {
        metric.reset(new DiscardMetric("DiscardTaskTest"));
        discardTaskManager_.reset(new DiscardTaskManager(metric.get()));

        mockMetaCache_.reset(new MockMetaCache());
        mockMDSClient_.reset(new MockMDSClient());

        fileInfo_.fullPathName = "/TestDiscardTask";
        fileInfo_.chunksize = 16ull * 1024 * 1024;
        fileInfo_.segmentsize = 1ull * 1024 * 1024 * 1024;

        mockMetaCache_->UpdateFileInfo(fileInfo_);
    }

    void TearDown() override {
        discardTaskManager_->Stop();
    }

 protected:
    FInfo fileInfo_;
    std::unique_ptr<DiscardTaskManager> discardTaskManager_;
    std::unique_ptr<MockMetaCache> mockMetaCache_;
    std::unique_ptr<MockMDSClient> mockMDSClient_;
    std::unique_ptr<DiscardMetric> metric;
};

TEST_F(DiscardTaskTest, TestDiscardBitmapCleared) {
    SegmentIndex segmentIndex = 100;
    uint64_t offset = segmentIndex * 1024ull * 1024 * 1024;
    DiscardTask task(discardTaskManager_.get(), segmentIndex,
                     mockMetaCache_.get(), mockMDSClient_.get(), metric.get());

    EXPECT_CALL(*mockMDSClient_, DeAllocateSegment(_, offset)).Times(0);
    EXPECT_CALL(*mockMetaCache_, CleanChunksInSegment(segmentIndex)).Times(0);

    ASSERT_NO_FATAL_FAILURE(task.Run());
}

TEST_F(DiscardTaskTest, TestAllDiscard) {
    SegmentIndex segmentIndex = 100;
    uint64_t offset = segmentIndex * 1024ull * 1024 * 1024;
    DiscardTask task(discardTaskManager_.get(), segmentIndex,
                     mockMetaCache_.get(), mockMDSClient_.get(), metric.get());

    // mdsclient return OK
    {
        // set all bit
        FileSegment* segment = mockMetaCache_->GetFileSegment(segmentIndex);
        segment->GetBitmap().Set();

        EXPECT_CALL(*mockMDSClient_, DeAllocateSegment(_, offset))
            .WillOnce(Return(LIBCURVE_ERROR::OK));
        EXPECT_CALL(*mockMetaCache_, CleanChunksInSegment(segmentIndex))
            .Times(1);

        ASSERT_NO_FATAL_FAILURE(task.Run());
        ASSERT_FALSE(segment->IsAllBitSet());
    }

    // mdsclient return under snapshot
    {
        // set all bit
        FileSegment* segment = mockMetaCache_->GetFileSegment(segmentIndex);
        segment->GetBitmap().Set();

        EXPECT_CALL(*mockMDSClient_, DeAllocateSegment(_, offset))
            .WillOnce(Return(LIBCURVE_ERROR::UNDER_SNAPSHOT));
        EXPECT_CALL(*mockMetaCache_, CleanChunksInSegment(segmentIndex))
            .Times(0);

        ASSERT_NO_FATAL_FAILURE(task.Run());
        ASSERT_TRUE(segment->IsAllBitSet());
    }

    // mdsclient return failed
    {
        // set all bit
        FileSegment* segment = mockMetaCache_->GetFileSegment(segmentIndex);
        segment->GetBitmap().Set();

        EXPECT_CALL(*mockMDSClient_, DeAllocateSegment(_, offset))
            .WillOnce(Return(LIBCURVE_ERROR::FAILED));
        EXPECT_CALL(*mockMetaCache_, CleanChunksInSegment(segmentIndex))
            .Times(0);

        ASSERT_NO_FATAL_FAILURE(task.Run());
        ASSERT_TRUE(segment->IsAllBitSet());
    }
}

}  // namespace client
}  // namespace curve
