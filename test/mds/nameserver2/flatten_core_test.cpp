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
 * Created Date: 2023-09-07
 * Author: xuchaojie
 */

#include <gtest/gtest.h>
#include <gmock/gmock.h>
#include <glog/logging.h>

#include "src/mds/nameserver2/flatten_core.h"
#include "test/mds/nameserver2/mock/mock_namespace_storage.h"
#include "test/mds/nameserver2/mock/mock_copyset_client.h"
#include "test/mds/mock/mock_alloc_statistic.h"
#include "test/mds/nameserver2/mock/mock_chunk_allocate.h"

using ::testing::AtLeast;
using ::testing::StrEq;
using ::testing::_;
using ::testing::Return;
using ::testing::ReturnArg;
using ::testing::DoAll;
using ::testing::SetArgPointee;
using ::testing::SaveArg;
using ::testing::Invoke;
using ::testing::Matcher;

using curve::mds::chunkserverclient::MockCopysetClient;
using curve::mds::chunkserverclient::CopysetClientClosure;

namespace curve {
namespace mds {

const uint64_t kSegmentSize = kGB * 1;
const uint64_t kChunkSize = kMB * 16;

class FlattenCoreTest : public ::testing::Test {
 public:
    void SetUp() override {
        storage_ = std::make_shared<MockNameServerStorage>();
        csClient_ = std::make_shared<MockCopysetClient>();
        mockChunkAllocator_ = std::make_shared<MockChunkAllocator>();
        allocStatistic_ = std::make_shared<MockAllocStatistic>();
        fileLockManager_ = new FileLockManager(8);
        core_ = std::make_shared<FlattenCore>(
            flattenOption_, storage_,
            mockChunkAllocator_, allocStatistic_,
            csClient_,
            fileLockManager_);
    }

    void TearDown() override {
        csClient_ = nullptr;
        storage_ = nullptr;
        core_ = nullptr;
        delete fileLockManager_;
    }

 protected:
    std::shared_ptr<MockNameServerStorage> storage_;
    std::shared_ptr<MockChunkAllocator> mockChunkAllocator_;
    std::shared_ptr<MockAllocStatistic> allocStatistic_;
    std::shared_ptr<MockCopysetClient> csClient_;
    std::shared_ptr<FlattenCore> core_;
    FileLockManager* fileLockManager_;
    FlattenOption flattenOption_;
};

TEST_F(FlattenCoreTest, DoFlatten) {
    // get segment fail
    {
        std::string fileName = "/clone1";
        FileInfo fileInfo;
        fileInfo.set_segmentsize(kSegmentSize);
        fileInfo.set_chunksize(kChunkSize);
        fileInfo.set_clonelength(kSegmentSize);
        FileInfo snapFileInfo;
        TaskProgress progress;

        EXPECT_CALL(*storage_, GetSegment(_, _, _))
            .WillOnce(Return(StoreStatus::InternalError));

        core_->DoFlatten(fileName, fileInfo, snapFileInfo, &progress);
        ASSERT_EQ(TaskStatus::FAILED, progress.GetStatus());
    }
    // not have segment, flatten success
    {
        std::string fileName = "/clone1";
        FileInfo fileInfo;
        fileInfo.set_segmentsize(kSegmentSize);
        fileInfo.set_chunksize(kChunkSize);
        fileInfo.set_clonelength(0);
        FileInfo snapFileInfo;
        TaskProgress progress;

        core_->DoFlatten(fileName, fileInfo, snapFileInfo, &progress);
        ASSERT_EQ(TaskStatus::SUCCESS, progress.GetStatus());
        ASSERT_EQ(100, progress.GetProgress());
    }
    // segment not exist, not have clone chain, flatten falied
    {
        std::string fileName = "/clone1";
        FileInfo fileInfo;
        fileInfo.set_segmentsize(kSegmentSize);
        fileInfo.set_chunksize(kChunkSize);
        fileInfo.set_clonelength(kSegmentSize);
        FileInfo snapFileInfo;
        TaskProgress progress;

        EXPECT_CALL(*storage_, GetSegment(_, _, _))
            .WillOnce(Return(StoreStatus::KeyNotExist));

        core_->DoFlatten(fileName, fileInfo, snapFileInfo, &progress);
        ASSERT_EQ(TaskStatus::FAILED, progress.GetStatus());
        ASSERT_EQ(0, progress.GetProgress());
    }
    // segment not exist, parent segment also not exist, flatten success
    {
        std::string fileName = "/clone1";
        FileInfo fileInfo;
        fileInfo.set_segmentsize(kSegmentSize);
        fileInfo.set_chunksize(kChunkSize);
        fileInfo.set_clonelength(kSegmentSize);
        auto cloneInfo = fileInfo.add_clones();
        cloneInfo->set_fileid(100);
        cloneInfo->set_clonesn(1);

        FileInfo snapFileInfo;
        TaskProgress progress;

        EXPECT_CALL(*storage_, GetSegment(_, _, _))
            .WillOnce(Return(StoreStatus::KeyNotExist))
            .WillOnce(Return(StoreStatus::KeyNotExist));

        core_->DoFlatten(fileName, fileInfo, snapFileInfo, &progress);
        ASSERT_EQ(TaskStatus::SUCCESS, progress.GetStatus());
        ASSERT_EQ(100, progress.GetProgress());
    }
    // segment not exist, parent segment get failed, flatten failed
    {
        std::string fileName = "/clone1";
        FileInfo fileInfo;
        fileInfo.set_segmentsize(kSegmentSize);
        fileInfo.set_chunksize(kChunkSize);
        fileInfo.set_clonelength(kSegmentSize);
        auto cloneInfo = fileInfo.add_clones();
        cloneInfo->set_fileid(100);
        cloneInfo->set_clonesn(1);

        FileInfo snapFileInfo;
        TaskProgress progress;

        EXPECT_CALL(*storage_, GetSegment(_, _, _))
            .WillOnce(Return(StoreStatus::KeyNotExist))
            .WillOnce(Return(StoreStatus::InternalError));

        core_->DoFlatten(fileName, fileInfo, snapFileInfo, &progress);
        ASSERT_EQ(TaskStatus::FAILED, progress.GetStatus());
        ASSERT_EQ(0, progress.GetProgress());
    }
    // segment not exist, parent segment sn is new, flatten success
    {
        std::string fileName = "/clone1";
        FileInfo fileInfo;
        fileInfo.set_segmentsize(kSegmentSize);
        fileInfo.set_chunksize(kChunkSize);
        fileInfo.set_clonelength(kSegmentSize);
        auto cloneInfo = fileInfo.add_clones();
        cloneInfo->set_fileid(100);
        cloneInfo->set_clonesn(1);

        FileInfo snapFileInfo;
        TaskProgress progress;

        PageFileSegment parentSegment;
        parentSegment.set_seqnum(2);
        EXPECT_CALL(*storage_, GetSegment(_, _, _))
            .WillOnce(Return(StoreStatus::KeyNotExist))
            .WillOnce(DoAll(SetArgPointee<2>(parentSegment),
                            Return(StoreStatus::OK)));

        core_->DoFlatten(fileName, fileInfo, snapFileInfo, &progress);
        ASSERT_EQ(TaskStatus::SUCCESS, progress.GetStatus());
        ASSERT_EQ(100, progress.GetProgress());
    }
    // segment not exist, parent segment exist, CloneChunkSegment failed
    {
        std::string fileName = "/clone1";
        FileInfo fileInfo;
        fileInfo.set_segmentsize(kSegmentSize);
        fileInfo.set_chunksize(kChunkSize);
        fileInfo.set_clonelength(kSegmentSize);
        auto cloneInfo = fileInfo.add_clones();
        cloneInfo->set_fileid(100);
        cloneInfo->set_clonesn(1);

        FileInfo snapFileInfo;
        TaskProgress progress;

        PageFileSegment parentSegment;
        parentSegment.set_seqnum(1);
        EXPECT_CALL(*storage_, GetSegment(_, _, _))
            .WillOnce(Return(StoreStatus::KeyNotExist))
            .WillOnce(DoAll(SetArgPointee<2>(parentSegment),
                            Return(StoreStatus::OK)));

        EXPECT_CALL(*mockChunkAllocator_, CloneChunkSegment(_, _, _))
            .WillOnce(Return(false));

        core_->DoFlatten(fileName, fileInfo, snapFileInfo, &progress);
        ASSERT_EQ(TaskStatus::FAILED, progress.GetStatus());
        ASSERT_EQ(0, progress.GetProgress());
    }
    // segment not exist, parent segment exist, put segment failed
    {
        std::string fileName = "/clone1";
        FileInfo fileInfo;
        fileInfo.set_segmentsize(kSegmentSize);
        fileInfo.set_chunksize(kChunkSize);
        fileInfo.set_clonelength(kSegmentSize);
        auto cloneInfo = fileInfo.add_clones();
        cloneInfo->set_fileid(100);
        cloneInfo->set_clonesn(1);

        FileInfo snapFileInfo;
        TaskProgress progress;

        PageFileSegment parentSegment;
        parentSegment.set_seqnum(1);
        EXPECT_CALL(*storage_, GetSegment(_, _, _))
            .WillOnce(Return(StoreStatus::KeyNotExist))
            .WillOnce(DoAll(SetArgPointee<2>(parentSegment),
                            Return(StoreStatus::OK)));

        EXPECT_CALL(*mockChunkAllocator_, CloneChunkSegment(_, _, _))
            .WillOnce(Return(true));

        EXPECT_CALL(*storage_, PutSegment(_, _, _, _))
            .WillOnce(Return(StoreStatus::InternalError));

        core_->DoFlatten(fileName, fileInfo, snapFileInfo, &progress);
        ASSERT_EQ(TaskStatus::FAILED, progress.GetStatus());
        ASSERT_EQ(0, progress.GetProgress());
    }
    // segment not exist, parent segment exist, clone segment success
    // flatten success
    {
        std::string fileName = "/clone1";
        FileInfo fileInfo;
        fileInfo.set_segmentsize(kSegmentSize);
        fileInfo.set_chunksize(kChunkSize);
        fileInfo.set_clonelength(kSegmentSize);
        auto cloneInfo = fileInfo.add_clones();
        cloneInfo->set_fileid(100);
        cloneInfo->set_clonesn(1);

        FileInfo snapFileInfo;
        TaskProgress progress;

        PageFileSegment parentSegment;
        parentSegment.set_seqnum(1);

        EXPECT_CALL(*storage_, GetSegment(_, _, _))
            .WillOnce(Return(StoreStatus::KeyNotExist))
            .WillOnce(DoAll(SetArgPointee<2>(parentSegment),
                            Return(StoreStatus::OK)));

        PageFileSegment segment;
        segment.set_originfileid(100);
        for (int i = 0; i < kSegmentSize; i += kChunkSize) {
            auto chunk = segment.add_chunks();
            chunk->set_copysetid(i);
            chunk->set_chunkid(i);
        }

        EXPECT_CALL(*mockChunkAllocator_, CloneChunkSegment(_, _, _))
            .WillOnce(DoAll(SetArgPointee<2>(segment),
                            Return(true)));

        EXPECT_CALL(*storage_, PutSegment(_, _, _, _))
            .WillOnce(Return(StoreStatus::OK));

        int repeatTimes = (kSegmentSize +
            flattenOption_.flattenChunkPartSize - 1) /
            flattenOption_.flattenChunkPartSize;
        EXPECT_CALL(*csClient_, FlattenChunk(_, _))
            .Times(repeatTimes)
            .WillRepeatedly(Invoke(
                [](const std::shared_ptr<FlattenChunkContext> &ctx,
        CopysetClientClosure* done) {
            done->SetErrCode(kMdsSuccess);
            brpc::ClosureGuard doneGuard(done);
            return kMdsSuccess;
                }));

        core_->DoFlatten(fileName, fileInfo, snapFileInfo, &progress);
        ASSERT_EQ(TaskStatus::SUCCESS, progress.GetStatus());
        ASSERT_EQ(100, progress.GetProgress());
    }
    // segment is not clone segment, flatten success
    {
        std::string fileName = "/clone1";
        FileInfo fileInfo;
        fileInfo.set_segmentsize(kSegmentSize);
        fileInfo.set_chunksize(kChunkSize);
        fileInfo.set_clonelength(kSegmentSize);
        FileInfo snapFileInfo;
        TaskProgress progress;

        PageFileSegment segment;
        EXPECT_CALL(*storage_, GetSegment(_, _, _))
            .WillOnce(DoAll(SetArgPointee<2>(segment),
                            Return(StoreStatus::OK)));

        core_->DoFlatten(fileName, fileInfo, snapFileInfo, &progress);
        ASSERT_EQ(TaskStatus::SUCCESS, progress.GetStatus());
        ASSERT_EQ(100, progress.GetProgress());
    }
    // segment is clone segment, flatten success
    {
        std::string fileName = "/clone1";
        uint64_t cloneLength = kSegmentSize * 10;
        FileInfo fileInfo;
        fileInfo.set_segmentsize(kSegmentSize);
        fileInfo.set_chunksize(kChunkSize);
        fileInfo.set_clonelength(cloneLength);
        FileInfo snapFileInfo;
        TaskProgress progress;

        PageFileSegment segment;
        segment.set_originfileid(100);
        for (int i = 0; i < kSegmentSize; i += kChunkSize) {
            auto chunk = segment.add_chunks();
            chunk->set_copysetid(i);
            chunk->set_chunkid(i);
        }
        uint64_t segmentNum = cloneLength / kSegmentSize;
        EXPECT_CALL(*storage_, GetSegment(_, _, _))
            .Times(segmentNum)
            .WillRepeatedly(DoAll(SetArgPointee<2>(segment),
                            Return(StoreStatus::OK)));

        int repeatTimes = (cloneLength +
            flattenOption_.flattenChunkPartSize - 1) /
            flattenOption_.flattenChunkPartSize;
        EXPECT_CALL(*csClient_, FlattenChunk(_, _))
            .Times(repeatTimes)
            .WillRepeatedly(Invoke(
                [](const std::shared_ptr<FlattenChunkContext> &ctx,
        CopysetClientClosure* done) {
            done->SetErrCode(kMdsSuccess);
            brpc::ClosureGuard doneGuard(done);
            return kMdsSuccess;
                }));

        core_->DoFlatten(fileName, fileInfo, snapFileInfo, &progress);
        ASSERT_EQ(TaskStatus::SUCCESS, progress.GetStatus());
        ASSERT_EQ(100, progress.GetProgress());
    }
    // FlattenChunk failed
    {
        std::string fileName = "/clone1";
        uint64_t cloneLength = kSegmentSize * 10;
        FileInfo fileInfo;
        fileInfo.set_segmentsize(kSegmentSize);
        fileInfo.set_chunksize(kChunkSize);
        fileInfo.set_clonelength(cloneLength);
        FileInfo snapFileInfo;
        TaskProgress progress;

        PageFileSegment segment;
        segment.set_originfileid(100);
        for (int i = 0; i < kSegmentSize; i += kChunkSize) {
            auto chunk = segment.add_chunks();
            chunk->set_copysetid(i);
            chunk->set_chunkid(i);
        }
        EXPECT_CALL(*storage_, GetSegment(_, _, _))
            .Times(AtLeast(1))
            .WillRepeatedly(DoAll(SetArgPointee<2>(segment),
                            Return(StoreStatus::OK)));

        EXPECT_CALL(*csClient_, FlattenChunk(_, _))
            .Times(AtLeast(1))
            .WillRepeatedly(Invoke(
                [](const std::shared_ptr<FlattenChunkContext> &ctx,
        CopysetClientClosure* done) {
            done->SetErrCode(kMdsFail);
            brpc::ClosureGuard doneGuard(done);
            return kMdsSuccess;
                }));

        core_->DoFlatten(fileName, fileInfo, snapFileInfo, &progress);
        ASSERT_EQ(TaskStatus::FAILED, progress.GetStatus());
        ASSERT_GT(100, progress.GetProgress());
    }
    // Get FileInfoNew fail
    {
        std::string fileName = "/clone1";
        FileInfo fileInfo;
        fileInfo.set_segmentsize(kSegmentSize);
        fileInfo.set_chunksize(kChunkSize);
        fileInfo.set_clonelength(kSegmentSize);
        FileInfo snapFileInfo;
        TaskProgress progress;

        PageFileSegment segment;
        EXPECT_CALL(*storage_, GetSegment(_, _, _))
            .WillOnce(DoAll(SetArgPointee<2>(segment),
                            Return(StoreStatus::OK)));

        EXPECT_CALL(*storage_, GetFile(_, _, _))
            .WillOnce(Return(StoreStatus::InternalError));

        core_->DoFlatten(fileName, fileInfo, snapFileInfo, &progress);
        ASSERT_EQ(TaskStatus::FAILED, progress.GetStatus());
    }
    // GetSnapFile fail, PutFile fail
    {
        std::string fileName = "/clone1";
        FileInfo fileInfo;
        fileInfo.set_segmentsize(kSegmentSize);
        fileInfo.set_chunksize(kChunkSize);
        fileInfo.set_clonelength(kSegmentSize);
        FileInfo snapFileInfo;
        TaskProgress progress;

        PageFileSegment segment;
        EXPECT_CALL(*storage_, GetSegment(_, _, _))
            .WillOnce(DoAll(SetArgPointee<2>(segment),
                            Return(StoreStatus::OK)));

        FileInfo fileInfoNew;
        EXPECT_CALL(*storage_, GetFile(_, _, _))
            .WillOnce(DoAll(SetArgPointee<2>(fileInfoNew),
                Return(StoreStatus::OK)));

        EXPECT_CALL(*storage_, GetSnapFile(_, _, _))
            .WillOnce(Return(StoreStatus::InternalError));

        EXPECT_CALL(*storage_, PutFile(_))
        .Times(1)
        .WillOnce(Return(StoreStatus::InternalError));

        core_->DoFlatten(fileName, fileInfo, snapFileInfo, &progress);
        ASSERT_EQ(TaskStatus::FAILED, progress.GetStatus());
    }
    // GetSnapFile fail, PutFile success
    {
        std::string fileName = "/clone1";
        FileInfo fileInfo;
        fileInfo.set_segmentsize(kSegmentSize);
        fileInfo.set_chunksize(kChunkSize);
        fileInfo.set_clonelength(kSegmentSize);
        FileInfo snapFileInfo;
        TaskProgress progress;

        PageFileSegment segment;
        EXPECT_CALL(*storage_, GetSegment(_, _, _))
            .WillOnce(DoAll(SetArgPointee<2>(segment),
                            Return(StoreStatus::OK)));

        FileInfo fileInfoNew;
        EXPECT_CALL(*storage_, GetFile(_, _, _))
            .WillOnce(DoAll(SetArgPointee<2>(fileInfoNew),
                Return(StoreStatus::OK)));

        EXPECT_CALL(*storage_, GetSnapFile(_, _, _))
            .WillOnce(Return(StoreStatus::InternalError));

        EXPECT_CALL(*storage_, PutFile(_))
        .Times(1)
        .WillOnce(Return(StoreStatus::OK));

        core_->DoFlatten(fileName, fileInfo, snapFileInfo, &progress);
        ASSERT_EQ(TaskStatus::SUCCESS, progress.GetStatus());
        ASSERT_EQ(100, progress.GetProgress());
    }
    // GetSnapFile success, Put2File fail
    {
        std::string fileName = "/clone1";
        FileInfo fileInfo;
        fileInfo.set_segmentsize(kSegmentSize);
        fileInfo.set_chunksize(kChunkSize);
        fileInfo.set_clonelength(kSegmentSize);
        FileInfo snapFileInfo;
        TaskProgress progress;

        PageFileSegment segment;
        EXPECT_CALL(*storage_, GetSegment(_, _, _))
            .WillOnce(DoAll(SetArgPointee<2>(segment),
                            Return(StoreStatus::OK)));

        FileInfo fileInfoNew;
        EXPECT_CALL(*storage_, GetFile(_, _, _))
            .WillOnce(DoAll(SetArgPointee<2>(fileInfoNew),
                Return(StoreStatus::OK)));

        FileInfo snapFileNew;
        EXPECT_CALL(*storage_, GetSnapFile(_, _, _))
            .WillOnce(DoAll(SetArgPointee<2>(snapFileNew),
                Return(StoreStatus::OK)));

        EXPECT_CALL(*storage_, Put2File(_, _))
        .Times(1)
        .WillOnce(Return(StoreStatus::InternalError));

        core_->DoFlatten(fileName, fileInfo, snapFileInfo, &progress);
        ASSERT_EQ(TaskStatus::FAILED, progress.GetStatus());
    }
    // GetSnapFile success, Put2File success
    {
        std::string fileName = "/clone1";
        FileInfo fileInfo;
        fileInfo.set_segmentsize(kSegmentSize);
        fileInfo.set_chunksize(kChunkSize);
        fileInfo.set_clonelength(kSegmentSize);
        FileInfo snapFileInfo;
        TaskProgress progress;

        PageFileSegment segment;
        EXPECT_CALL(*storage_, GetSegment(_, _, _))
            .WillOnce(DoAll(SetArgPointee<2>(segment),
                            Return(StoreStatus::OK)));

        FileInfo fileInfoNew;
        EXPECT_CALL(*storage_, GetFile(_, _, _))
            .WillOnce(DoAll(SetArgPointee<2>(fileInfoNew),
                Return(StoreStatus::OK)));

        FileInfo snapFileNew;
        EXPECT_CALL(*storage_, GetSnapFile(_, _, _))
            .WillOnce(DoAll(SetArgPointee<2>(snapFileNew),
                Return(StoreStatus::OK)));

        EXPECT_CALL(*storage_, Put2File(_, _))
        .Times(1)
        .WillOnce(Return(StoreStatus::OK));

        core_->DoFlatten(fileName, fileInfo, snapFileInfo, &progress);
        ASSERT_EQ(TaskStatus::SUCCESS, progress.GetStatus());
        ASSERT_EQ(100, progress.GetProgress());
    }
}

}  // namespace mds
}  // namespace curve


