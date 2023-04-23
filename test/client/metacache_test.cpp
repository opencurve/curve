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

/**
 * Project: curve
 * Date: Wed Dec 23 16:21:20 CST 2020
 * Author: wuhanqing
 */

#include "src/client/metacache.h"

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <tuple>
#include <vector>

namespace curve {
namespace client {

class MetaCacheTest : public ::testing::Test {
 public:
    void SetUp() override {}

    void TearDown() override {}

 protected:
    void InsertMetaCache(uint64_t fileLength, uint64_t segmentSize,
                         uint64_t chunkSize) {
        fileInfo_.fullPathName = "/MetaCacheTest";
        fileInfo_.length = fileLength;
        fileInfo_.segmentsize = segmentSize;
        fileInfo_.chunksize = chunkSize;

        metaCache_.UpdateFileInfo(fileInfo_);

        uint64_t chunks = fileLength / chunkSize;
        for (uint64_t i = 0; i < chunks; ++i) {
            ChunkIDInfo info(i, i, i);
            metaCache_.UpdateChunkInfoByIndex(i, info);
        }
    }

    uint64_t CountAvaiableChunks(uint64_t fileLength, uint64_t segmentSize,
                                 uint64_t chunksize) {
        uint64_t count = 0;
        uint64_t chunks = fileLength / chunksize;
        ChunkIDInfo info;

        for (uint64_t i = 0; i < chunks; ++i) {
            if (MetaCacheErrorType::OK ==
                metaCache_.GetChunkInfoByIndex(i, &info)) {
                ++count;
            }
        }

        return count;
    }

 protected:
    FInfo fileInfo_;
    MetaCache metaCache_;
};

TEST_F(MetaCacheTest, TestCleanChunksInSegment) {
    std::vector<std::tuple<uint64_t, uint64_t, uint64_t>> testValues{
        {20 * GiB, 1 * GiB, 16 * MiB},
        {20 * GiB, 512 * MiB, 16 * MiB},
        {20 * GiB, 16 * MiB, 16 * MiB},
        {20 * GiB, 16 * MiB, 4 * MiB},
    };

    for (auto& values : testValues) {
        uint64_t fileLength = std::get<0>(values);
        uint64_t segmentSize = std::get<1>(values);
        uint64_t chunkSize = std::get<2>(values);

        InsertMetaCache(fileLength, segmentSize, chunkSize);

        uint64_t totalChunks = fileLength / chunkSize;
        uint64_t chunksInSegment = segmentSize / chunkSize;

        ASSERT_EQ(totalChunks,
                  CountAvaiableChunks(fileLength, segmentSize, chunkSize));

        SegmentIndex segmentIndex = 0;
        uint64_t count = 0;
        while (segmentIndex < fileLength / segmentSize) {
            metaCache_.CleanChunksInSegment(segmentIndex++);
            ++count;
            ASSERT_EQ(totalChunks - count * chunksInSegment,
                      CountAvaiableChunks(fileLength, segmentSize, chunkSize));
        }

        ASSERT_EQ(0, CountAvaiableChunks(fileLength, segmentIndex, chunkSize));
    }
}

}  // namespace client
}  // namespace curve
