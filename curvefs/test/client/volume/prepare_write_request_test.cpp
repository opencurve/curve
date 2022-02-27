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
 * Date: Friday Mar 18 16:31:01 CST 2022
 * Author: wuhanqing
 */

#include <gtest/gtest.h>

#include "curvefs/src/client/volume/utils.h"
#include "curvefs/test/client/volume/common.h"
#include "curvefs/test/volume/mock/mock_space_manager.h"

namespace curvefs {
namespace client {

using ::curvefs::volume::AllocateHint;
using ::curvefs::volume::MockSpaceManager;

using ::testing::_;
using ::testing::Invoke;
using ::testing::Return;

class PrepareWriteRequestTest : public ::testing::Test {
 protected:
    ExtentCache extentCache_;
    MockSpaceManager mockSpaceManager_;
};

TEST_F(PrepareWriteRequestTest, Case1) {
    off_t offset = 0 * kKiB;
    size_t length = 128 * kKiB;

    std::unique_ptr<char[]> data(new char[length]);

    EXPECT_CALL(mockSpaceManager_, Alloc(_, _, _))
        .WillOnce(Invoke(
            [](uint64_t size, const AllocateHint&, std::vector<Extent>* exts) {
                exts->emplace_back(1 * kMiB, size);
                return true;
            }));

    std::vector<WritePart> writes;
    ASSERT_TRUE(PrepareWriteRequest(offset, length, data.get(), &extentCache_,
                                    &mockSpaceManager_, &writes));

    ASSERT_EQ(1, writes.size());

    ASSERT_EQ(1 * kMiB, writes[0].offset);
    ASSERT_EQ(128 * kKiB, writes[0].length);
    ASSERT_EQ(data.get(), writes[0].data);
}

TEST_F(PrepareWriteRequestTest, Case2) {
    off_t offset = 0 * kKiB;
    size_t length = 128 * kKiB;

    std::unique_ptr<char[]> data(new char[length]);

    PExtent pext;
    pext.pOffset = 1 * kMiB;
    pext.len = 128 * kKiB;
    pext.UnWritten = false;
    extentCache_.Merge(0 * kKiB, pext);

    EXPECT_CALL(mockSpaceManager_, Alloc(_, _, _))
        .Times(0);

    std::vector<WritePart> writes;
    ASSERT_TRUE(PrepareWriteRequest(offset, length, data.get(), &extentCache_,
                                    &mockSpaceManager_, &writes));

    ASSERT_EQ(1, writes.size());

    ASSERT_EQ(1 * kMiB, writes[0].offset);
    ASSERT_EQ(128 * kKiB, writes[0].length);
    ASSERT_EQ(data.get(), writes[0].data);
}

// unaligned write
// 64 ~ 616
TEST_F(PrepareWriteRequestTest, Case3_UnalignedWrite) {
    off_t offset = 64;
    size_t length = 616;

    std::unique_ptr<char[]> data(new char[length]);

    EXPECT_CALL(mockSpaceManager_, Alloc(_, _, _))
        .WillOnce(Invoke(
            [](uint64_t size, const AllocateHint&, std::vector<Extent>* exts) {
                exts->emplace_back(100 * kMiB, 32 * kKiB);

                return size;
            }));

    std::vector<WritePart> writes;
    ASSERT_TRUE(PrepareWriteRequest(offset, length, data.get(), &extentCache_,
                                    &mockSpaceManager_, &writes));

    ASSERT_EQ(1, writes.size());

    ASSERT_EQ(100 * kMiB + 64, writes[0].offset);
    ASSERT_EQ(616, writes[0].length);
    ASSERT_EQ(data.get(), writes[0].data);
}

// unaligned write
// 64 ~ 4096
TEST_F(PrepareWriteRequestTest, Case3_UnalignedWrite2) {
    ExtentCacheOption opt;
    opt.preallocSize = 4 * kKiB;
    ExtentCache::SetOption(opt);

    off_t offset = 64;
    size_t length = 4096;

    std::unique_ptr<char[]> data(new char[length]);

    EXPECT_CALL(mockSpaceManager_, Alloc(_, _, _))
        .WillOnce(Invoke(
            [](uint64_t size, const AllocateHint&, std::vector<Extent>* exts) {
                exts->emplace_back(100 * kMiB, 4 * kKiB);
                exts->emplace_back(200 * kMiB, 4 * kKiB);

                return size;
            }));

    std::vector<WritePart> writes;
    ASSERT_TRUE(PrepareWriteRequest(offset, length, data.get(), &extentCache_,
                                    &mockSpaceManager_, &writes));

    ASSERT_EQ(2, writes.size());

    ASSERT_EQ(100 * kMiB + 64, writes[0].offset);
    ASSERT_EQ(4032, writes[0].length);
    ASSERT_EQ(data.get(), writes[0].data);

    ASSERT_EQ(200 * kMiB, writes[1].offset);
    ASSERT_EQ(64, writes[1].length);
    ASSERT_EQ(data.get() + writes[0].length, writes[1].data);
}

}  // namespace client
}  // namespace curvefs
