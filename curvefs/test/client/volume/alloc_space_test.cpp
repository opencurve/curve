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
 * Date: Friday Mar 18 12:30:42 CST 2022
 * Author: wuhanqing
 */

#include <gmock/gmock.h>
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

class AllocSpaceTest : public ::testing::Test {
 protected:
    MockSpaceManager mockSpaceManager_;
};

TEST_F(AllocSpaceTest, TestAllocError) {
    AllocPart part;
    part.allocInfo.len = 4 * kKiB;
    part.allocInfo.lOffset = 4 * kKiB;

    EXPECT_CALL(mockSpaceManager_, Alloc(_, _, _))
        .WillOnce(Return(false));

    std::map<uint64_t, WritePart> writes;
    std::map<uint64_t, Extent> allocates;

    ASSERT_FALSE(AllocSpace(&mockSpaceManager_, part, &writes, &allocates));
}

TEST_F(AllocSpaceTest, TestAllocOneExtent) {
    AllocPart part;
    part.writelength = 4 * kKiB;
    part.allocInfo.len = 4 * kKiB;
    part.allocInfo.lOffset = 4 * kKiB;

    std::unique_ptr<char[]> data(new char[part.writelength]);

    part.data = data.get();

    EXPECT_CALL(mockSpaceManager_, Alloc(_, _, _))
        .WillOnce(Invoke(
            [](uint64_t, const AllocateHint&, std::vector<Extent>* exts) {
                exts->emplace_back(32 * kKiB, 4 * kKiB);
                return exts;
            }));

    std::map<uint64_t, WritePart> writes;
    std::map<uint64_t, Extent> allocates;

    ASSERT_TRUE(AllocSpace(&mockSpaceManager_, part, &writes, &allocates));

    ASSERT_EQ(1, writes.size());
    ASSERT_EQ(1, allocates.size());

    ASSERT_EQ(1, writes.count(4 * kKiB));
    ASSERT_EQ(1, allocates.count(4 * kKiB));

    ASSERT_EQ(32 * kKiB, writes[4 * kKiB].offset);
    ASSERT_EQ(4 * kKiB, writes[4 * kKiB].length);
    ASSERT_EQ(data.get(), writes[4 * kKiB].data);

    ASSERT_EQ(32 * kKiB, allocates[4 * kKiB].offset);
    ASSERT_EQ(4 * kKiB, allocates[4 * kKiB].len);
}

TEST_F(AllocSpaceTest, TestPreAllocOneExtent) {
    AllocPart part;
    part.writelength = 4 * kKiB;
    part.allocInfo.len = 64 * kKiB;
    part.allocInfo.lOffset = 4 * kKiB;

    std::unique_ptr<char[]> data(new char[part.writelength]);

    part.data = data.get();

    EXPECT_CALL(mockSpaceManager_, Alloc(_, _, _))
        .WillOnce(Invoke(
            [](uint64_t size, const AllocateHint&, std::vector<Extent>* exts) {
                exts->emplace_back(32 * kKiB, size);
                return exts;
            }));

    std::map<uint64_t, WritePart> writes;
    std::map<uint64_t, Extent> allocates;

    ASSERT_TRUE(AllocSpace(&mockSpaceManager_, part, &writes, &allocates));

    ASSERT_EQ(1, writes.size());
    ASSERT_EQ(1, allocates.size());

    ASSERT_EQ(1, writes.count(4 * kKiB));
    ASSERT_EQ(1, allocates.count(4 * kKiB));

    ASSERT_EQ(32 * kKiB, writes[4 * kKiB].offset);
    ASSERT_EQ(4 * kKiB, writes[4 * kKiB].length);
    ASSERT_EQ(data.get(), writes[4 * kKiB].data);

    ASSERT_EQ(32 * kKiB, allocates[4 * kKiB].offset);
    ASSERT_EQ(64 * kKiB, allocates[4 * kKiB].len);
}

TEST_F(AllocSpaceTest, TestAllocOneExtentAndWriteIsNotAligned1) {
    AllocPart part;
    part.writelength = 6;  // 6 bytes
    part.padding = 0;
    part.allocInfo.len = 64 * kKiB;
    part.allocInfo.lOffset = 0 * kKiB;

    std::unique_ptr<char[]> data(new char[part.writelength]);

    part.data = data.get();

    EXPECT_CALL(mockSpaceManager_, Alloc(_, _, _))
        .WillOnce(Invoke(
            [](uint64_t size, const AllocateHint&, std::vector<Extent>* exts) {
                exts->emplace_back(32 * kKiB, size);
                return exts;
            }));

    std::map<uint64_t, WritePart> writes;
    std::map<uint64_t, Extent> allocates;

    ASSERT_TRUE(AllocSpace(&mockSpaceManager_, part, &writes, &allocates));

    ASSERT_EQ(1, writes.size());
    ASSERT_EQ(1, allocates.size());

    ASSERT_EQ(1, writes.count(0 * kKiB));
    ASSERT_EQ(1, allocates.count(0 * kKiB));

    ASSERT_EQ(32 * kKiB, writes[0 * kKiB].offset);
    ASSERT_EQ(6, writes[0 * kKiB].length);
    ASSERT_EQ(data.get(), writes[0 * kKiB].data);

    ASSERT_EQ(32 * kKiB, allocates[0 * kKiB].offset);
    ASSERT_EQ(64 * kKiB, allocates[0 * kKiB].len);
}

TEST_F(AllocSpaceTest, TestAllocOneExtentAndWriteIsNotAligned2) {
    AllocPart part;
    part.writelength = 6;  // 6 bytes
    part.padding = 6;
    part.allocInfo.len = 64 * kKiB;
    part.allocInfo.lOffset = 0 * kKiB;

    std::unique_ptr<char[]> data(new char[part.writelength]);

    part.data = data.get();

    EXPECT_CALL(mockSpaceManager_, Alloc(_, _, _))
        .WillOnce(Invoke(
            [](uint64_t size, const AllocateHint&, std::vector<Extent>* exts) {
                exts->emplace_back(32 * kKiB, size);
                return exts;
            }));

    std::map<uint64_t, WritePart> writes;
    std::map<uint64_t, Extent> allocates;

    ASSERT_TRUE(AllocSpace(&mockSpaceManager_, part, &writes, &allocates));

    ASSERT_EQ(1, writes.size());
    ASSERT_EQ(1, allocates.size());

    ASSERT_EQ(1, writes.count(0 * kKiB));
    ASSERT_EQ(1, allocates.count(0 * kKiB));

    ASSERT_EQ(32 * kKiB + 6, writes[0 * kKiB].offset);
    ASSERT_EQ(6, writes[0 * kKiB].length);
    ASSERT_EQ(data.get(), writes[0 * kKiB].data);

    ASSERT_EQ(32 * kKiB, allocates[0 * kKiB].offset);
    ASSERT_EQ(64 * kKiB, allocates[0 * kKiB].len);
}

TEST_F(AllocSpaceTest, TestAllocOneExtentAndWriteIsNotAligned3) {
    AllocPart part;
    part.writelength = 4096;
    part.padding = 6;
    part.allocInfo.len = 8 * kKiB;
    part.allocInfo.lOffset = 0 * kKiB;

    std::unique_ptr<char[]> data(new char[part.writelength]);

    part.data = data.get();

    EXPECT_CALL(mockSpaceManager_, Alloc(_, _, _))
        .WillOnce(Invoke(
            [](uint64_t size, const AllocateHint&, std::vector<Extent>* exts) {
                exts->emplace_back(32 * kKiB, size / 2);
                exts->emplace_back(64 * kKiB, size / 2);

                return size;
            }));

    std::map<uint64_t, WritePart> writes;
    std::map<uint64_t, Extent> allocates;

    ASSERT_TRUE(AllocSpace(&mockSpaceManager_, part, &writes, &allocates));

    ASSERT_EQ(2, writes.size());
    ASSERT_EQ(2, allocates.size());

    ASSERT_EQ(1, writes.count(0 * kKiB));
    ASSERT_EQ(1, writes.count(4 * kKiB));

    ASSERT_EQ(1, allocates.count(0 * kKiB));
    ASSERT_EQ(1, allocates.count(4 * kKiB));

    auto& write1 = writes[0 * kKiB];
    auto& write2 = writes[4 * kKiB];

    ASSERT_EQ(32 * kKiB + 6, write1.offset);
    ASSERT_EQ(4090, write1.length);
    ASSERT_EQ(data.get(), write1.data);

    ASSERT_EQ(64 * kKiB, write2.offset);
    ASSERT_EQ(6, write2.length);
    ASSERT_EQ(data.get() + 4090, write2.data);

    auto& alloc1 = allocates[0 * kKiB];
    auto& alloc2 = allocates[4 * kKiB];

    ASSERT_EQ(32 * kKiB, alloc1.offset);
    ASSERT_EQ(4 * kKiB, alloc1.len);

    ASSERT_EQ(64 * kKiB, alloc2.offset);
    ASSERT_EQ(4 * kKiB, alloc2.len);
}

TEST_F(AllocSpaceTest, TestAllocOneExtentAndWriteIsNotAligned4) {
    AllocPart part;
    part.writelength = 4096 + 6;
    part.padding = 0;
    part.allocInfo.len = 8 * kKiB;
    part.allocInfo.lOffset = 0 * kKiB;

    std::unique_ptr<char[]> data(new char[part.writelength]);

    part.data = data.get();

    EXPECT_CALL(mockSpaceManager_, Alloc(_, _, _))
        .WillOnce(Invoke(
            [](uint64_t size, const AllocateHint&, std::vector<Extent>* exts) {
                exts->emplace_back(32 * kKiB, size / 2);
                exts->emplace_back(64 * kKiB, size / 2);

                return size;
            }));

    std::map<uint64_t, WritePart> writes;
    std::map<uint64_t, Extent> allocates;

    ASSERT_TRUE(AllocSpace(&mockSpaceManager_, part, &writes, &allocates));

    ASSERT_EQ(2, writes.size());
    ASSERT_EQ(2, allocates.size());

    ASSERT_EQ(1, writes.count(0 * kKiB));
    ASSERT_EQ(1, writes.count(4 * kKiB));

    ASSERT_EQ(1, allocates.count(0 * kKiB));
    ASSERT_EQ(1, allocates.count(4 * kKiB));

    auto& write1 = writes[0 * kKiB];
    auto& write2 = writes[4 * kKiB];

    ASSERT_EQ(32 * kKiB, write1.offset);
    ASSERT_EQ(4096, write1.length);
    ASSERT_EQ(data.get(), write1.data);

    ASSERT_EQ(64 * kKiB, write2.offset);
    ASSERT_EQ(6, write2.length);
    ASSERT_EQ(data.get() + 4096, write2.data);

    auto& alloc1 = allocates[0 * kKiB];
    auto& alloc2 = allocates[4 * kKiB];

    ASSERT_EQ(32 * kKiB, alloc1.offset);
    ASSERT_EQ(4 * kKiB, alloc1.len);

    ASSERT_EQ(64 * kKiB, alloc2.offset);
    ASSERT_EQ(4 * kKiB, alloc2.len);
}

TEST_F(AllocSpaceTest, TestAllocMultiExtents) {
    AllocPart part;
    part.writelength = 16 * kKiB;
    part.allocInfo.len = 64 * kKiB;
    part.allocInfo.lOffset = 4 * kKiB;

    std::unique_ptr<char[]> data(new char[part.writelength]);

    part.data = data.get();

    EXPECT_CALL(mockSpaceManager_, Alloc(_, _, _))
        .WillOnce(Invoke(
            [](uint64_t, const AllocateHint&, std::vector<Extent>* exts) {
                // first 16KiB is discontinuous
                exts->emplace_back(32 * kKiB, 4 * kKiB);
                exts->emplace_back(64 * kKiB, 4 * kKiB);
                exts->emplace_back(128 * kKiB, 4 * kKiB);
                exts->emplace_back(256 * kKiB, 4 * kKiB);

                // last 48KiB is continuous
                exts->emplace_back(512 * kKiB, 48 * kKiB);

                return exts;
            }));

    std::map<uint64_t, WritePart> writes;
    std::map<uint64_t, Extent> allocates;

    ASSERT_TRUE(AllocSpace(&mockSpaceManager_, part, &writes, &allocates));

    ASSERT_EQ(4, writes.size());
    ASSERT_EQ(5, allocates.size());

    ASSERT_EQ(1, writes.count(4 * kKiB));
    ASSERT_EQ(1, writes.count(8 * kKiB));
    ASSERT_EQ(1, writes.count(12 * kKiB));
    ASSERT_EQ(1, writes.count(16 * kKiB));

    ASSERT_EQ(1, allocates.count(4 * kKiB));
    ASSERT_EQ(1, allocates.count(8 * kKiB));
    ASSERT_EQ(1, allocates.count(12 * kKiB));
    ASSERT_EQ(1, allocates.count(16 * kKiB));
    ASSERT_EQ(1, allocates.count(20 * kKiB));

    ASSERT_EQ(32 * kKiB, writes[4 * kKiB].offset);
    ASSERT_EQ(4 * kKiB, writes[4 * kKiB].length);
    ASSERT_EQ(data.get(), writes[4 * kKiB].data);

    ASSERT_EQ(64 * kKiB, writes[8 * kKiB].offset);
    ASSERT_EQ(4 * kKiB, writes[8 * kKiB].length);
    ASSERT_EQ(data.get() + 4 * kKiB, writes[8 * kKiB].data);

    ASSERT_EQ(128 * kKiB, writes[12 * kKiB].offset);
    ASSERT_EQ(4 * kKiB, writes[12 * kKiB].length);
    ASSERT_EQ(data.get() + 8 * kKiB, writes[12 * kKiB].data);

    ASSERT_EQ(256 * kKiB, writes[16 * kKiB].offset);
    ASSERT_EQ(4 * kKiB, writes[16 * kKiB].length);
    ASSERT_EQ(data.get() + 12 * kKiB, writes[16 * kKiB].data);

    ASSERT_EQ(32 * kKiB, allocates[4 * kKiB].offset);
    ASSERT_EQ(4 * kKiB, allocates[4 * kKiB].len);

    ASSERT_EQ(64 * kKiB, allocates[8 * kKiB].offset);
    ASSERT_EQ(4 * kKiB, allocates[8 * kKiB].len);

    ASSERT_EQ(128 * kKiB, allocates[12 * kKiB].offset);
    ASSERT_EQ(4 * kKiB, allocates[12 * kKiB].len);

    ASSERT_EQ(256 * kKiB, allocates[16 * kKiB].offset);
    ASSERT_EQ(4 * kKiB, allocates[16 * kKiB].len);

    ASSERT_EQ(512 * kKiB, allocates[20 * kKiB].offset);
    ASSERT_EQ(48 * kKiB, allocates[20 * kKiB].len);
}

}  // namespace client
}  // namespace curvefs
