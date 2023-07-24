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

    uint64_t CountAvailableChunks(uint64_t fileLength, uint64_t segmentSize,
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
                  CountAvailableChunks(fileLength, segmentSize, chunkSize));

        SegmentIndex segmentIndex = 0;
        uint64_t count = 0;
        while (segmentIndex < fileLength / segmentSize) {
            metaCache_.CleanChunksInSegment(segmentIndex++);
            ++count;
            ASSERT_EQ(totalChunks - count * chunksInSegment,
                      CountAvailableChunks(fileLength, segmentSize, chunkSize));
        }

        ASSERT_EQ(0, CountAvailableChunks(fileLength, segmentIndex, chunkSize));
    }
}

TEST(MetaCacheCommonTest, TestAddCopysetsInfo) {
    MetaCache metaCache;

    std::vector<CopysetPeerInfo<ChunkServerID>> peers;
    PeerAddr addr;
    ASSERT_EQ(0, addr.Parse("127.0.0.1:8200:0"));
    peers.emplace_back(1, addr, addr);
    ASSERT_EQ(0, addr.Parse("127.0.0.1:8201:0"));
    peers.emplace_back(2, addr, addr);
    ASSERT_EQ(0, addr.Parse("127.0.0.1:8202:0"));
    peers.emplace_back(3, addr, addr);

    std::vector<CopysetInfo<ChunkServerID>> copysets;
    for (int i = 1; i <= 5; ++i) {
        CopysetInfo<ChunkServerID> info;
        info.lpid_ = 0;
        info.cpid_ = i;
        info.leaderindex_ = 0;
        info.csinfos_ = peers;
        copysets.emplace_back(info);
    }

    metaCache.AddCopysetsInfo(0, std::move(copysets));

    for (int i = 1; i <= 5; ++i) {
        auto copyset = metaCache.GetCopysetinfo(0, i);
        ASSERT_EQ(0, copyset.leaderindex_);
    }

    copysets.clear();
    for (int i = 1; i <= 10; ++i) {
        CopysetInfo<ChunkServerID> info;
        info.lpid_ = 0;
        info.cpid_ = i;
        info.leaderindex_ = -1;  // unknown leader
        info.csinfos_ = peers;
        copysets.emplace_back(info);
    }

    metaCache.AddCopysetsInfo(0, std::move(copysets));

    // will not overwrite exist copyset info, so leaderindex is still 0
    for (int i = 1; i <= 5; ++i) {
        auto copyset = metaCache.GetCopysetinfo(0, i);
        ASSERT_EQ(0, copyset.leaderindex_);
    }
    for (int i = 6; i <= 10; ++i) {
        auto copyset = metaCache.GetCopysetinfo(0, i);
        ASSERT_EQ(-1, copyset.leaderindex_);
    }
}

}  // namespace client
}  // namespace curve
