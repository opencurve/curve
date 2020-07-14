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
 * Created Date: Saturday December 29th 2018
 * Author: hzsunjianliang
 */

#include <gtest/gtest.h>
#include <gmock/gmock.h>
#include <glog/logging.h>
#include "src/mds/nameserver2/clean_core.h"
#include "test/mds/nameserver2/mock/mock_namespace_storage.h"
#include "test/mds/mock/mock_topology.h"
#include "src/mds/chunkserverclient/copyset_client.h"
#include "test/mds/mock/mock_alloc_statistic.h"

using ::testing::_;
using ::testing::Return;
using curve::mds::topology::MockTopology;
using ::curve::mds::chunkserverclient::ChunkServerClientOption;

namespace curve {
namespace mds {

TEST(CleanCore, testcleansnapshotfile) {
    auto storage = std::make_shared<MockNameServerStorage>();
    auto topology = std::make_shared<MockTopology>();
    ChunkServerClientOption option;
    auto channelPool = std::make_shared<ChannelPool>();
    auto client = std::make_shared<CopysetClient>(topology,
                                            option, channelPool);
    auto allocStatistic = std::make_shared<MockAllocStatistic>();
    auto cleanCore = std::make_shared<CleanCore>(storage,
                                                    client, allocStatistic);

    {
        // segment size = 0
        FileInfo cleanFile;
        cleanFile.set_length(kMiniFileLength);
        cleanFile.set_segmentsize(0);
        TaskProgress progress;
        ASSERT_EQ(cleanCore->CleanSnapShotFile(cleanFile, &progress),
            StatusCode::KInternalError);
    }

    {
        // delete ok (no, segment)
        uint32_t segmentNum = kMiniFileLength / DefaultSegmentSize;
        for (uint32_t i = 0; i < segmentNum; i++) {
            EXPECT_CALL(*storage, GetSegment(_, i * DefaultSegmentSize, _))
            .WillOnce(Return(StoreStatus::KeyNotExist));
        }

        EXPECT_CALL(*storage, DeleteSnapshotFile(_, _))
        .Times(1)
        .WillOnce(Return(StoreStatus::OK));

        FileInfo cleanFile;
        cleanFile.set_length(kMiniFileLength);
        cleanFile.set_segmentsize(DefaultSegmentSize);
        TaskProgress progress;
        ASSERT_EQ(cleanCore->CleanSnapShotFile(cleanFile, &progress),
            StatusCode::kOK);

        ASSERT_EQ(progress.GetStatus(), TaskStatus::SUCCESS);
        ASSERT_EQ(progress.GetProgress(), 100);
    }
    {
        // all ok , but do DeleteFile namespace meta error
        uint32_t segmentNum = kMiniFileLength / DefaultSegmentSize;
        for (uint32_t i = 0; i < segmentNum; i++) {
            EXPECT_CALL(*storage, GetSegment(_, i * DefaultSegmentSize, _))
            .WillOnce(Return(StoreStatus::KeyNotExist));
        }

        EXPECT_CALL(*storage, DeleteSnapshotFile(_, _))
        .WillOnce(Return(StoreStatus::InternalError));

        FileInfo cleanFile;
        cleanFile.set_length(kMiniFileLength);
        cleanFile.set_segmentsize(DefaultSegmentSize);
        TaskProgress progress;
        ASSERT_EQ(cleanCore->CleanSnapShotFile(cleanFile, &progress),
            StatusCode::kSnapshotFileDeleteError);
    }

    {
        // get segment error
        EXPECT_CALL(*storage, GetSegment(_, 0, _))
        .Times(1)
        .WillOnce(Return(StoreStatus::InternalError));

        FileInfo cleanFile;
        cleanFile.set_length(kMiniFileLength);
        cleanFile.set_segmentsize(DefaultSegmentSize);
        TaskProgress progress;
        ASSERT_EQ(cleanCore->CleanSnapShotFile(cleanFile, &progress),
            StatusCode::kSnapshotFileDeleteError);
    }
    {
        // 联调Bug修复：快照文件共享源文件的segment，所以在查询segment的时候需要使用
        // ParentID 进行查找
        uint32_t segmentNum = kMiniFileLength / DefaultSegmentSize;
        uint64_t expectParentID = 101;
        for (uint32_t i = 0; i < segmentNum; i++) {
            EXPECT_CALL(*storage,
                        GetSegment(expectParentID, i * DefaultSegmentSize, _))
            .WillOnce(Return(StoreStatus::KeyNotExist));
        }

        EXPECT_CALL(*storage, DeleteSnapshotFile(_, _))
        .Times(1)
        .WillOnce(Return(StoreStatus::OK));

        FileInfo cleanFile;
        cleanFile.set_length(kMiniFileLength);
        cleanFile.set_segmentsize(DefaultSegmentSize);
        cleanFile.set_parentid(expectParentID);
        TaskProgress progress;
        ASSERT_EQ(cleanCore->CleanSnapShotFile(cleanFile, &progress),
            StatusCode::kOK);

        ASSERT_EQ(progress.GetStatus(), TaskStatus::SUCCESS);
        ASSERT_EQ(progress.GetProgress(), 100);
    }
    {
        // get segment ok, DeleteSnapShotChunk Error
    }

    {
        // get segment ok, DeleteSnapShotChunk OK
        uint32_t segmentNum = kMiniFileLength / DefaultSegmentSize;
        for (uint32_t i = 0; i < segmentNum; i++) {
            EXPECT_CALL(*storage, GetSegment(_, i * DefaultSegmentSize, _))
            .WillOnce(Return(StoreStatus::OK));
        }

        EXPECT_CALL(*storage, DeleteSnapshotFile(_, _))
        .Times(1)
        .WillOnce(Return(StoreStatus::OK));

        FileInfo cleanFile;
        cleanFile.set_length(kMiniFileLength);
        cleanFile.set_segmentsize(DefaultSegmentSize);
        TaskProgress progress;
        ASSERT_EQ(cleanCore->CleanSnapShotFile(cleanFile, &progress),
            StatusCode::kOK);

        ASSERT_EQ(progress.GetStatus(), TaskStatus::SUCCESS);
        ASSERT_EQ(progress.GetProgress(), 100);
    }
}

TEST(CleanCore, testcleanfile) {
    auto storage = std::make_shared<MockNameServerStorage>();
    auto topology = std::make_shared<MockTopology>();
    ChunkServerClientOption option;
    auto channelPool = std::make_shared<ChannelPool>();
    auto client = std::make_shared<CopysetClient>(topology,
                                                    option, channelPool);
    auto allocStatistic = std::make_shared<MockAllocStatistic>();
    auto cleanCore = std::make_shared<CleanCore>(storage,
                                                    client, allocStatistic);

    {
        // segmentsize = 0
        FileInfo cleanFile;
        cleanFile.set_length(kMiniFileLength);
        cleanFile.set_segmentsize(0);
        TaskProgress progress;
        ASSERT_EQ(cleanCore->CleanFile(cleanFile, &progress),
            StatusCode::KInternalError);
    }

    {
        // delete ok (no, segment)
        uint32_t segmentNum = kMiniFileLength / DefaultSegmentSize;
        for (uint32_t i = 0; i < segmentNum; i++) {
            EXPECT_CALL(*storage, GetSegment(_, i * DefaultSegmentSize, _))
            .WillOnce(Return(StoreStatus::KeyNotExist));
        }

        EXPECT_CALL(*storage, DeleteFile(_, _))
        .Times(1)
        .WillOnce(Return(StoreStatus::OK));

        FileInfo cleanFile;
        cleanFile.set_length(kMiniFileLength);
        cleanFile.set_segmentsize(DefaultSegmentSize);
        TaskProgress progress;
        ASSERT_EQ(cleanCore->CleanFile(cleanFile, &progress),
            StatusCode::kOK);

        ASSERT_EQ(progress.GetStatus(), TaskStatus::SUCCESS);
        ASSERT_EQ(progress.GetProgress(), 100);
    }

    {
        // all ok , but do DeleteFile namespace meta error
        uint32_t segmentNum = kMiniFileLength / DefaultSegmentSize;
        for (uint32_t i = 0; i < segmentNum; i++) {
            EXPECT_CALL(*storage, GetSegment(_, i * DefaultSegmentSize, _))
            .WillOnce(Return(StoreStatus::KeyNotExist));
        }

        EXPECT_CALL(*storage, DeleteFile(_, _))
        .WillOnce(Return(StoreStatus::InternalError));

        FileInfo cleanFile;
        cleanFile.set_length(kMiniFileLength);
        cleanFile.set_segmentsize(DefaultSegmentSize);
        TaskProgress progress;
        ASSERT_EQ(cleanCore->CleanFile(cleanFile, &progress),
            StatusCode::kCommonFileDeleteError);
        ASSERT_EQ(progress.GetStatus(), TaskStatus::FAILED);
    }

    {
        // get segment error
        EXPECT_CALL(*storage, GetSegment(_, 0, _))
        .Times(1)
        .WillOnce(Return(StoreStatus::InternalError));

        FileInfo cleanFile;
        cleanFile.set_length(kMiniFileLength);
        cleanFile.set_segmentsize(DefaultSegmentSize);
        TaskProgress progress;
        ASSERT_EQ(cleanCore->CleanFile(cleanFile, &progress),
            StatusCode::kCommonFileDeleteError);
        ASSERT_EQ(progress.GetStatus(), TaskStatus::FAILED);
    }
    {
        // get segment ok, DeleteSnapShotChunk Error
    }
    {
        // get segment ok, DeleteSnapShotChunk ok, DeleteSegment error
        EXPECT_CALL(*storage, GetSegment(_, 0, _))
                .WillOnce(Return(StoreStatus::OK));

        EXPECT_CALL(*storage, DeleteSegment(_, _, _))
        .WillOnce(Return(StoreStatus::InternalError));

        FileInfo cleanFile;
        cleanFile.set_length(kMiniFileLength);
        cleanFile.set_segmentsize(DefaultSegmentSize);
        TaskProgress progress;
        ASSERT_EQ(cleanCore->CleanFile(cleanFile, &progress),
            StatusCode::kCommonFileDeleteError);
        ASSERT_EQ(progress.GetStatus(), TaskStatus::FAILED);
    }
}
}  // namespace mds
}  // namespace curve
