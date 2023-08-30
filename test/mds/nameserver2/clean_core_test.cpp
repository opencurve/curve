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
#include "test/mds/mock/mock_chunkserverclient.h"

using ::testing::_;
using ::testing::Return;
using ::testing::SetArgPointee;
using ::testing::DoAll;
using curve::mds::topology::MockTopology;
using ::curve::mds::chunkserverclient::ChunkServerClientOption;
using ::curve::mds::chunkserverclient::MockChunkServerClient;

namespace curve {
namespace mds {

const uint64_t DefaultSegmentSize = kGB * 1;
const uint64_t kMiniFileLength = 10 * kGB;

class CleanCoreTest : public testing::Test {
 public:
    void SetUp() override {
        storage_ = std::make_shared<MockNameServerStorage>();
        topology_ = std::make_shared<MockTopology>();
        channelPool_ = std::make_shared<ChannelPool>();
        client_ =
            std::make_shared<CopysetClient>(topology_, option_, channelPool_);
        allocStatistic_ = std::make_shared<MockAllocStatistic>();
        cleanCore_ =
            std::make_shared<CleanCore>(storage_, client_, allocStatistic_);

        csClient_ = std::make_shared<MockChunkServerClient>(
            topology_, option_, channelPool_);
    }

    void TearDown() override {}

 protected:
    std::shared_ptr<MockNameServerStorage> storage_;
    std::shared_ptr<MockTopology> topology_;
    ChunkServerClientOption option_;
    std::shared_ptr<ChannelPool> channelPool_;
    std::shared_ptr<CopysetClient> client_;
    std::shared_ptr<MockAllocStatistic> allocStatistic_;
    std::shared_ptr<CleanCore> cleanCore_;
    std::shared_ptr<MockChunkServerClient> csClient_;
};

TEST_F(CleanCoreTest, testcleansnapshotfile) {
    {
        // segment size = 0
        FileInfo cleanFile;
        cleanFile.set_length(kMiniFileLength);
        cleanFile.set_segmentsize(0);
        TaskProgress progress;
        ASSERT_EQ(cleanCore_->CleanSnapShotFile(cleanFile, &progress),
            StatusCode::KInternalError);
    }

    {
        // delete ok (no, segment)
        uint32_t segmentNum = kMiniFileLength / DefaultSegmentSize;
        for (uint32_t i = 0; i < segmentNum; i++) {
            EXPECT_CALL(*storage_, GetSegment(_, i * DefaultSegmentSize, _))
            .WillOnce(Return(StoreStatus::KeyNotExist));
        }

        EXPECT_CALL(*storage_, DeleteSnapshotFile(_, _))
        .Times(1)
        .WillOnce(Return(StoreStatus::OK));

        FileInfo cleanFile;
        cleanFile.set_length(kMiniFileLength);
        cleanFile.set_segmentsize(DefaultSegmentSize);
        TaskProgress progress;
        ASSERT_EQ(cleanCore_->CleanSnapShotFile(cleanFile, &progress),
            StatusCode::kOK);

        ASSERT_EQ(progress.GetStatus(), TaskStatus::SUCCESS);
        ASSERT_EQ(progress.GetProgress(), 100);
    }
    {
        // all ok , but do DeleteFile namespace meta error
        uint32_t segmentNum = kMiniFileLength / DefaultSegmentSize;
        for (uint32_t i = 0; i < segmentNum; i++) {
            EXPECT_CALL(*storage_, GetSegment(_, i * DefaultSegmentSize, _))
            .WillOnce(Return(StoreStatus::KeyNotExist));
        }

        EXPECT_CALL(*storage_, DeleteSnapshotFile(_, _))
        .WillOnce(Return(StoreStatus::InternalError));

        FileInfo cleanFile;
        cleanFile.set_length(kMiniFileLength);
        cleanFile.set_segmentsize(DefaultSegmentSize);
        TaskProgress progress;
        ASSERT_EQ(cleanCore_->CleanSnapShotFile(cleanFile, &progress),
            StatusCode::kSnapshotFileDeleteError);
    }

    {
        // get segment error
        EXPECT_CALL(*storage_, GetSegment(_, 0, _))
        .Times(1)
        .WillOnce(Return(StoreStatus::InternalError));

        FileInfo cleanFile;
        cleanFile.set_length(kMiniFileLength);
        cleanFile.set_segmentsize(DefaultSegmentSize);
        TaskProgress progress;
        ASSERT_EQ(cleanCore_->CleanSnapShotFile(cleanFile, &progress),
            StatusCode::kSnapshotFileDeleteError);
    }
    {
        //Joint debugging bug fix: The snapshot file shares a segment of the source file, so it needs to be used when querying segments
        //ParentID for lookup
        uint32_t segmentNum = kMiniFileLength / DefaultSegmentSize;
        uint64_t expectParentID = 101;
        for (uint32_t i = 0; i < segmentNum; i++) {
            EXPECT_CALL(*storage_,
                        GetSegment(expectParentID, i * DefaultSegmentSize, _))
            .WillOnce(Return(StoreStatus::KeyNotExist));
        }

        EXPECT_CALL(*storage_, DeleteSnapshotFile(_, _))
        .Times(1)
        .WillOnce(Return(StoreStatus::OK));

        FileInfo cleanFile;
        cleanFile.set_length(kMiniFileLength);
        cleanFile.set_segmentsize(DefaultSegmentSize);
        cleanFile.set_parentid(expectParentID);
        TaskProgress progress;
        ASSERT_EQ(cleanCore_->CleanSnapShotFile(cleanFile, &progress),
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
            EXPECT_CALL(*storage_, GetSegment(_, i * DefaultSegmentSize, _))
            .WillOnce(Return(StoreStatus::OK));
        }

        EXPECT_CALL(*storage_, DeleteSnapshotFile(_, _))
        .Times(1)
        .WillOnce(Return(StoreStatus::OK));

        FileInfo cleanFile;
        cleanFile.set_length(kMiniFileLength);
        cleanFile.set_segmentsize(DefaultSegmentSize);
        TaskProgress progress;
        ASSERT_EQ(cleanCore_->CleanSnapShotFile(cleanFile, &progress),
            StatusCode::kOK);

        ASSERT_EQ(progress.GetStatus(), TaskStatus::SUCCESS);
        ASSERT_EQ(progress.GetProgress(), 100);
    }
}

TEST_F(CleanCoreTest, testcleanfile) {
    {
        // segmentsize = 0
        FileInfo cleanFile;
        cleanFile.set_length(kMiniFileLength);
        cleanFile.set_segmentsize(0);
        TaskProgress progress;
        ASSERT_EQ(cleanCore_->CleanFile(cleanFile, &progress),
            StatusCode::KInternalError);
    }

    {
        // delete ok (no, segment)
        uint32_t segmentNum = kMiniFileLength / DefaultSegmentSize;
        for (uint32_t i = 0; i < segmentNum; i++) {
            EXPECT_CALL(*storage_, GetSegment(_, i * DefaultSegmentSize, _))
            .WillOnce(Return(StoreStatus::KeyNotExist));
        }

        EXPECT_CALL(*storage_, DeleteFile(_, _))
        .Times(1)
        .WillOnce(Return(StoreStatus::OK));

        FileInfo cleanFile;
        cleanFile.set_length(kMiniFileLength);
        cleanFile.set_segmentsize(DefaultSegmentSize);
        TaskProgress progress;
        ASSERT_EQ(cleanCore_->CleanFile(cleanFile, &progress),
            StatusCode::kOK);

        ASSERT_EQ(progress.GetStatus(), TaskStatus::SUCCESS);
        ASSERT_EQ(progress.GetProgress(), 100);
    }

    {
        // all ok , but do DeleteFile namespace meta error
        uint32_t segmentNum = kMiniFileLength / DefaultSegmentSize;
        for (uint32_t i = 0; i < segmentNum; i++) {
            EXPECT_CALL(*storage_, GetSegment(_, i * DefaultSegmentSize, _))
            .WillOnce(Return(StoreStatus::KeyNotExist));
        }

        EXPECT_CALL(*storage_, DeleteFile(_, _))
        .WillOnce(Return(StoreStatus::InternalError));

        FileInfo cleanFile;
        cleanFile.set_length(kMiniFileLength);
        cleanFile.set_segmentsize(DefaultSegmentSize);
        TaskProgress progress;
        ASSERT_EQ(cleanCore_->CleanFile(cleanFile, &progress),
            StatusCode::kCommonFileDeleteError);
        ASSERT_EQ(progress.GetStatus(), TaskStatus::FAILED);
    }

    {
        // get segment error
        EXPECT_CALL(*storage_, GetSegment(_, 0, _))
        .Times(1)
        .WillOnce(Return(StoreStatus::InternalError));

        FileInfo cleanFile;
        cleanFile.set_length(kMiniFileLength);
        cleanFile.set_segmentsize(DefaultSegmentSize);
        TaskProgress progress;
        ASSERT_EQ(cleanCore_->CleanFile(cleanFile, &progress),
            StatusCode::kCommonFileDeleteError);
        ASSERT_EQ(progress.GetStatus(), TaskStatus::FAILED);
    }
    {
        // get segment ok, DeleteSnapShotChunk Error
    }
    {
        // get segment ok, DeleteSnapShotChunk ok, DeleteSegment error
        EXPECT_CALL(*storage_, GetSegment(_, 0, _))
                .WillOnce(Return(StoreStatus::OK));

        EXPECT_CALL(*storage_, DeleteSegment(_, _, _))
        .WillOnce(Return(StoreStatus::InternalError));

        FileInfo cleanFile;
        cleanFile.set_length(kMiniFileLength);
        cleanFile.set_segmentsize(DefaultSegmentSize);
        TaskProgress progress;
        ASSERT_EQ(cleanCore_->CleanFile(cleanFile, &progress),
            StatusCode::kCommonFileDeleteError);
        ASSERT_EQ(progress.GetStatus(), TaskStatus::FAILED);
    }
}

TEST_F(CleanCoreTest, TestCleanDiscardSegment) {
    const std::string fakeKey = "fakekey";
    const int kDefaultChunkSize = 16 * 1024 * 1024;

    FileInfo fileInfo;
    fileInfo.set_filename("/test_file");
    fileInfo.set_id(1234);
    fileInfo.set_segmentsize(DefaultSegmentSize);
    fileInfo.set_length(kMiniFileLength);

    PageFileSegment segment;
    segment.set_logicalpoolid(1);
    segment.set_segmentsize(DefaultSegmentSize);
    segment.set_chunksize(kDefaultChunkSize);
    segment.set_startoffset(0);

    for (int i = 0; i < DefaultSegmentSize / kDefaultChunkSize; ++i) {
        auto* chunk = segment.add_chunks();
        chunk->set_copysetid(i);
        chunk->set_chunkid(i);
    }

    DiscardSegmentInfo discardSegmentInfo;
    discardSegmentInfo.set_allocated_fileinfo(new FileInfo(fileInfo));
    discardSegmentInfo.set_allocated_pagefilesegment(
        new PageFileSegment(segment));

    // CopysetClient DeleteChunk failed
    {
        EXPECT_CALL(*topology_, GetCopySet(_, _))
            .WillOnce(Return(false));
        EXPECT_CALL(*storage_, CleanDiscardSegment(_, _, _))
            .Times(0);
        EXPECT_CALL(*allocStatistic_, DeAllocSpace(_, _, _))
            .Times(0);
        TaskProgress progress;
        ASSERT_EQ(StatusCode::KInternalError,
                  cleanCore_->CleanDiscardSegment(fakeKey, discardSegmentInfo,
                                                  &progress));
        ASSERT_EQ(TaskStatus::FAILED, progress.GetStatus());
    }

    // NameServerStorage CleanDiscardSegment failed
    {
        client_->SetChunkServerClient(csClient_);

        CopySetInfo copyset;

        copyset.SetLeader(1);

        EXPECT_CALL(*topology_, GetCopySet(_, _))
            .Times(segment.chunks_size())
            .WillRepeatedly(
                DoAll(SetArgPointee<1>(copyset), Return(true)));
        EXPECT_CALL(*csClient_, DeleteChunk(_, _, _, _, _))
            .Times(segment.chunks_size())
            .WillRepeatedly(Return(kMdsSuccess));

        EXPECT_CALL(*storage_, CleanDiscardSegment(_, _, _))
            .WillOnce(Return(StoreStatus::InternalError));
        EXPECT_CALL(*allocStatistic_, DeAllocSpace(_, _, _))
            .Times(0);

        TaskProgress progress;
        ASSERT_EQ(StatusCode::KInternalError,
                  cleanCore_->CleanDiscardSegment(fakeKey, discardSegmentInfo,
                                                  &progress));
        ASSERT_EQ(TaskStatus::FAILED, progress.GetStatus());
    }

    // ok
    {
        client_->SetChunkServerClient(csClient_);

        CopySetInfo copyset;

        copyset.SetLeader(1);

        EXPECT_CALL(*topology_, GetCopySet(_, _))
            .Times(segment.chunks_size())
            .WillRepeatedly(
                DoAll(SetArgPointee<1>(copyset), Return(true)));
        EXPECT_CALL(*csClient_, DeleteChunk(_, _, _, _, _))
            .Times(segment.chunks_size())
            .WillRepeatedly(Return(kMdsSuccess));

        EXPECT_CALL(*storage_, CleanDiscardSegment(_, _, _))
            .WillOnce(Return(StoreStatus::OK));
        EXPECT_CALL(*allocStatistic_, DeAllocSpace(_, _, _))
            .Times(1);

        TaskProgress progress;
        ASSERT_EQ(StatusCode::kOK, cleanCore_->CleanDiscardSegment(
                                       fakeKey, discardSegmentInfo, &progress));
        ASSERT_EQ(100, progress.GetProgress());
        ASSERT_EQ(TaskStatus::SUCCESS, progress.GetStatus());
    }
}

}  // namespace mds
}  // namespace curve
