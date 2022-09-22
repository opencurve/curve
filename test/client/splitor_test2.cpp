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

/*
 * Project: curve
 * Date: Thursday Dec 09 19:59:06 CST 2021
 * Author: wuhanqing
 */

#include <gtest/gtest.h>

#include "src/client/client_common.h"
#include "src/client/splitor.h"

namespace curve {
namespace client {

TEST(SplitorTest, NeedGetOrAllocateSegmentTest_ChunkNotAllocated) {
    MetaCache metaCache;

    EXPECT_TRUE(Splitor::NeedGetOrAllocateSegment(
        MetaCacheErrorType::CHUNKINFO_NOT_FOUND, OpType::READ, {}, &metaCache));
    EXPECT_TRUE(Splitor::NeedGetOrAllocateSegment(
        MetaCacheErrorType::CHUNKINFO_NOT_FOUND, OpType::WRITE, {},
        &metaCache));
}

TEST(SplitorTest, NeedGetOrAllocateSegmentTest_ChunkAllocatedButInvalid) {
    MetaCache metaCache;

    ChunkIDInfo chunkInfo;
    chunkInfo.chunkExist = false;

    // write request always return true
    EXPECT_TRUE(Splitor::NeedGetOrAllocateSegment(
        MetaCacheErrorType::OK, OpType::WRITE, chunkInfo, &metaCache));

    // read request with exclusive open return false
    FInfo finfo;
    finfo.context.openflags = CURVE_FORCE_WRITE;
    metaCache.UpdateFileInfo(finfo);
    EXPECT_FALSE(Splitor::NeedGetOrAllocateSegment(
        MetaCacheErrorType::OK, OpType::READ, chunkInfo, &metaCache));

    // read request with non-exclusive open return false
    finfo.context.openflags &= ~(CURVE_EXCLUSIVE);
    metaCache.UpdateFileInfo(finfo);
    EXPECT_TRUE(Splitor::NeedGetOrAllocateSegment(
        MetaCacheErrorType::OK, OpType::READ, chunkInfo, &metaCache));
}

}  // namespace client
}  // namespace curve
