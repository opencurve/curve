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
 * Created Date: 20191217
 * Author: lixiaocui
 */

#include <gtest/gtest.h>
#include "src/mds/nameserver2/metric.h"

namespace curve {
namespace mds {

TEST(SegmentDiscardMetricTest, TestCommon) {
    const uint64_t segmentSize = 128ull * 1024 * 1024;

    SegmentDiscardMetric metric;

    ASSERT_EQ(0, metric.pendingSegments_.get_value());
    ASSERT_EQ(0, metric.pendingSize_.get_value());
    ASSERT_EQ(0, metric.totalCleanedSegments_.get_value());
    ASSERT_EQ(0, metric.totalCleanedSize_.get_value());

    metric.OnReceiveDiscardRequest(segmentSize);
    metric.OnReceiveDiscardRequest(segmentSize);

    ASSERT_EQ(2, metric.pendingSegments_.get_value());
    ASSERT_EQ(2 * segmentSize, metric.pendingSize_.get_value());
    ASSERT_EQ(0, metric.totalCleanedSegments_.get_value());
    ASSERT_EQ(0, metric.totalCleanedSize_.get_value());

    metric.OnDiscardFinish(segmentSize);
    ASSERT_EQ(1, metric.pendingSegments_.get_value());
    ASSERT_EQ(1 * segmentSize, metric.pendingSize_.get_value());
    ASSERT_EQ(1, metric.totalCleanedSegments_.get_value());
    ASSERT_EQ(1 * segmentSize, metric.totalCleanedSize_.get_value());

    metric.OnDiscardFinish(segmentSize);
    ASSERT_EQ(0, metric.pendingSegments_.get_value());
    ASSERT_EQ(0, metric.pendingSize_.get_value());
    ASSERT_EQ(2, metric.totalCleanedSegments_.get_value());
    ASSERT_EQ(2 * segmentSize, metric.totalCleanedSize_.get_value());
}

}  // namespace mds
}  // namespace curve
