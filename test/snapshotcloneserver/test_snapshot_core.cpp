/*
 * Project: curve
 * Created Date: Wed Jan 02 2019
 * Author: xuchaojie
 * Copyright (c) 2018 netease
 */

#include <gtest/gtest.h>
#include <gmock/gmock.h>

#include "src/snapshotcloneserver/snapshot/snapshot_core.h"
#include "src/snapshotcloneserver/common/define.h"
#include "src/snapshotcloneserver/snapshot/snapshot_task.h"

#include "test/snapshotcloneserver/mock_snapshot_server.h"


namespace curve {
namespace snapshotcloneserver {

using ::testing::Return;
using ::testing::_;
using ::testing::AnyOf;
using ::testing::AllOf;
using ::testing::SetArgPointee;
using ::testing::Invoke;
using ::testing::DoAll;

class TestSnapshotCoreImpl : public ::testing::Test {
 public:
    TestSnapshotCoreImpl() {}
    virtual ~TestSnapshotCoreImpl() {}

    virtual void SetUp() {
        client_ = std::make_shared<MockCurveFsClient>();
        metaStore_ = std::make_shared<MockSnapshotCloneMetaStore>();
        dataStore_ = std::make_shared<MockSnapshotDataStore>();
        core_ = std::make_shared<SnapshotCoreImpl>(client_,
                metaStore_,
                dataStore_);
    }

    virtual void TearDown() {
        client_ = nullptr;
        metaStore_ = nullptr;
        dataStore_ = nullptr;
        core_ = nullptr;
    }

 protected:
    std::shared_ptr<SnapshotCoreImpl> core_;
    std::shared_ptr<MockCurveFsClient> client_;
    std::shared_ptr<MockSnapshotCloneMetaStore> metaStore_;
    std::shared_ptr<MockSnapshotDataStore> dataStore_;
};


TEST_F(TestSnapshotCoreImpl, TestCreateSnapshotPreSuccess) {
    const std::string file = "file";
    const std::string user = "user";
    const std::string desc = "snap1";
    SnapshotInfo info;
    EXPECT_CALL(*metaStore_, GetSnapshotList(_, _))
        .WillOnce(Return(kErrCodeSnapshotServerSuccess));
    FInfo fInfo;
    fInfo.filestatus = FileStatus::Created;
    EXPECT_CALL(*client_, GetFileInfo(_, _, _))
        .WillOnce(DoAll(
                    SetArgPointee<2>(fInfo),
                    Return(kErrCodeSnapshotServerSuccess)));
    EXPECT_CALL(*metaStore_, AddSnapshot(_))
        .WillOnce(Return(kErrCodeSnapshotServerSuccess));
    int ret = core_->CreateSnapshotPre(file, user, desc, &info);
    ASSERT_EQ(kErrCodeSnapshotServerSuccess, ret);
}

TEST_F(TestSnapshotCoreImpl, TestCreateSnapshotPreFail) {
    const std::string file = "file";
    const std::string user = "user";
    const std::string desc = "snap1";
    SnapshotInfo info;
    EXPECT_CALL(*metaStore_, GetSnapshotList(_, _))
        .WillOnce(Return(kErrCodeSnapshotServerSuccess));
    FInfo fInfo;
    fInfo.filestatus = FileStatus::Created;
    EXPECT_CALL(*client_, GetFileInfo(_, _, _))
        .WillOnce(DoAll(
                    SetArgPointee<2>(fInfo),
                    Return(kErrCodeSnapshotServerSuccess)));
    EXPECT_CALL(*metaStore_, AddSnapshot(_))
        .WillOnce(Return(kErrCodeSnapshotInternalError));
    int ret = core_->CreateSnapshotPre(file, user, desc, &info);
    ASSERT_EQ(kErrCodeSnapshotInternalError, ret);
}

TEST_F(TestSnapshotCoreImpl, TestDeleteSnapshotPreSuccess) {
    UUID uuid = "uuid1";
    std::string user = "user1";
    std::string fileName = "file1";
    std::string desc = "desc1";

    SnapshotInfo info(uuid, user, fileName, desc);
    info.SetStatus(Status::done);
    EXPECT_CALL(*metaStore_, GetSnapshotInfo(uuid, _))
        .WillOnce(DoAll(SetArgPointee<1>(info),
                Return(kErrCodeSnapshotServerSuccess)));

    EXPECT_CALL(*metaStore_, UpdateSnapshot(_))
        .WillOnce(Return(kErrCodeSnapshotServerSuccess));

    SnapshotInfo infoOut;
    int ret = core_->DeleteSnapshotPre(uuid, user, fileName, &infoOut);
    ASSERT_EQ(kErrCodeSnapshotServerSuccess, ret);
}

TEST_F(TestSnapshotCoreImpl, TestDeleteSnapshotPre_GetSnapshotInfoNotExist) {
    UUID uuid = "uuid1";
    std::string user = "user1";
    std::string fileName = "file1";
    std::string desc = "desc1";

    SnapshotInfo info(uuid, user, fileName, desc);
    info.SetStatus(Status::done);
    EXPECT_CALL(*metaStore_, GetSnapshotInfo(_, _))
        .WillOnce(DoAll(SetArgPointee<1>(info),
                Return(kErrCodeSnapshotInternalError)));

    SnapshotInfo infoOut;
    int ret = core_->DeleteSnapshotPre(uuid, user, fileName, &infoOut);
    ASSERT_EQ(kErrCodeSnapshotServerSuccess, ret);
}

TEST_F(TestSnapshotCoreImpl, TestDeleteSnapshotPre_UpdateSnapshotFail) {
    UUID uuid = "uuid1";
    std::string user = "user1";
    std::string fileName = "file1";
    std::string desc = "desc1";

    SnapshotInfo info(uuid, user, fileName, desc);
    info.SetStatus(Status::done);
    EXPECT_CALL(*metaStore_, GetSnapshotInfo(uuid, _))
        .WillOnce(DoAll(SetArgPointee<1>(info),
                Return(kErrCodeSnapshotServerSuccess)));

    EXPECT_CALL(*metaStore_, UpdateSnapshot(_))
        .WillOnce(Return(kErrCodeSnapshotInternalError));

    SnapshotInfo infoOut;
    int ret = core_->DeleteSnapshotPre(uuid, user, fileName, &infoOut);
    ASSERT_EQ(kErrCodeSnapshotInternalError, ret);
}

TEST_F(TestSnapshotCoreImpl, TestDeleteSnapshotPre_InvalidUser) {
    UUID uuid = "uuid1";
    std::string user = "user1";
    std::string fileName = "file1";
    std::string desc = "desc1";

    std::string user2 = "user2";
    SnapshotInfo info(uuid, user2, fileName, desc);
    info.SetStatus(Status::done);
    EXPECT_CALL(*metaStore_, GetSnapshotInfo(uuid, _))
        .WillOnce(DoAll(SetArgPointee<1>(info),
                Return(kErrCodeSnapshotServerSuccess)));

    SnapshotInfo infoOut;
    int ret = core_->DeleteSnapshotPre(uuid, user, fileName, &infoOut);
    ASSERT_EQ(kErrCodeSnapshotUserNotMatch, ret);
}

TEST_F(TestSnapshotCoreImpl, TestDeleteSnapshotPre_DeleteSnapshotUnfinished) {
    UUID uuid = "uuid1";
    std::string user = "user1";
    std::string fileName = "file1";
    std::string desc = "desc1";

    SnapshotInfo info(uuid, user, fileName, desc);
    info.SetStatus(Status::pending);
    EXPECT_CALL(*metaStore_, GetSnapshotInfo(uuid, _))
        .WillOnce(DoAll(SetArgPointee<1>(info),
                Return(kErrCodeSnapshotServerSuccess)));

    SnapshotInfo infoOut;
    int ret = core_->DeleteSnapshotPre(uuid, user, fileName, &infoOut);
    ASSERT_EQ(kErrCodeSnapshotCannotDeleteUnfinished, ret);
}

TEST_F(TestSnapshotCoreImpl,
    TestGetFileSnapshotInfoSuccess) {
    std::string file = "file1";
    std::vector<SnapshotInfo> info;

    EXPECT_CALL(*metaStore_, GetSnapshotList(file, _))
        .WillOnce(Return(kErrCodeSnapshotServerSuccess));

    int ret = core_->GetFileSnapshotInfo(file, &info);
    ASSERT_EQ(kErrCodeSnapshotServerSuccess, ret);
}

TEST_F(TestSnapshotCoreImpl,
    TestHandleCreateSnapshotTaskSuccess) {
    UUID uuid = "uuid1";
    std::string user = "user1";
    std::string fileName = "file1";
    std::string desc = "snap1";
    uint64_t seqNum = 100;

    SnapshotInfo info(uuid, user, fileName, desc);
    info.SetStatus(Status::pending);
    std::shared_ptr<SnapshotTaskInfo> task =
        std::make_shared<SnapshotTaskInfo>(info);

    EXPECT_CALL(*client_, CreateSnapshot(fileName, user, _))
        .WillOnce(DoAll(
                    SetArgPointee<2>(seqNum),
                    Return(kErrCodeSnapshotServerSuccess)));

    FInfo snapInfo;
    snapInfo.seqnum = 100;
    snapInfo.chunksize = 2 * kChunkSplitSize;
    snapInfo.segmentsize = 2 * snapInfo.chunksize;
    snapInfo.length = 2 * snapInfo.segmentsize;
    snapInfo.ctime = 10;
    EXPECT_CALL(*client_, GetSnapshot(fileName, user, seqNum, _))
        .WillOnce(DoAll(
                    SetArgPointee<3>(snapInfo),
                    Return(kErrCodeSnapshotServerSuccess)));


    EXPECT_CALL(*metaStore_, UpdateSnapshot(_))
        .Times(2)
        .WillRepeatedly(Return(kErrCodeSnapshotServerSuccess));

    LogicPoolID lpid1 = 1;
    CopysetID cpid1 = 1;
    ChunkID chunkId1 = 1;
    LogicPoolID lpid2 = 2;
    CopysetID cpid2 = 2;
    ChunkID chunkId2 = 2;

    SegmentInfo segInfo1;
    segInfo1.chunkvec.push_back(
        ChunkIDInfo(chunkId1, lpid1, cpid1));
    segInfo1.chunkvec.push_back(
        ChunkIDInfo(chunkId2, lpid2, cpid2));

    LogicPoolID lpid3 = 3;
    CopysetID cpid3 = 3;
    ChunkID chunkId3 = 3;
    LogicPoolID lpid4 = 4;
    CopysetID cpid4 = 4;
    ChunkID chunkId4 = 4;

    SegmentInfo segInfo2;
    segInfo2.chunkvec.push_back(
        ChunkIDInfo(chunkId3, lpid3, cpid3));
    segInfo2.chunkvec.push_back(
        ChunkIDInfo(chunkId4, lpid4, cpid4));

    EXPECT_CALL(*client_, GetSnapshotSegmentInfo(fileName,
          user,
          seqNum,
            _,
            _))
        .Times(2)
        .WillOnce(DoAll(SetArgPointee<4>(segInfo1),
                    Return(kErrCodeSnapshotServerSuccess)))
        .WillOnce(DoAll(SetArgPointee<4>(segInfo2),
                    Return(kErrCodeSnapshotServerSuccess)));

    uint64_t chunkSn = 100;
    ChunkInfoDetail chunkInfo;
    chunkInfo.chunkSn.push_back(chunkSn);
    EXPECT_CALL(*client_, GetChunkInfo(_, _))
        .Times(4)
        .WillRepeatedly(DoAll(SetArgPointee<1>(chunkInfo),
                    Return(kErrCodeSnapshotServerSuccess)));

    EXPECT_CALL(*dataStore_, PutChunkIndexData(_, _))
        .WillOnce(Return(kErrCodeSnapshotServerSuccess));

    UUID uuid2 = "uuid2";
    std::string desc2 = "desc2";

    std::vector<SnapshotInfo> snapInfos;
    SnapshotInfo info2(uuid2, user, fileName, desc2);
    info.SetSeqNum(seqNum);
    info2.SetSeqNum(seqNum - 1);
    info2.SetStatus(Status::done);
    snapInfos.push_back(info);
    snapInfos.push_back(info2);

    EXPECT_CALL(*metaStore_, GetSnapshotList(fileName, _))
        .WillOnce(DoAll(
                    SetArgPointee<1>(snapInfos),
                    Return(kErrCodeSnapshotServerSuccess)));

    ChunkIndexData indexData;
    indexData.PutChunkDataName(ChunkDataName(fileName, 1, 0));
    EXPECT_CALL(*dataStore_, GetChunkIndexData(_, _))
        .WillOnce(DoAll(
                    SetArgPointee<1>(indexData),
                    Return(kErrCodeSnapshotServerSuccess)));

    EXPECT_CALL(*dataStore_, DataChunkTranferInit(_, _))
        .Times(4)
        .WillRepeatedly(Return(kErrCodeSnapshotServerSuccess));


    EXPECT_CALL(*client_, ReadChunkSnapshot(_, _, _, _, _))
        .Times(8)
        .WillRepeatedly(Return(kErrCodeSnapshotServerSuccess));

    EXPECT_CALL(*dataStore_, DataChunkTranferAddPart(_, _, _, _, _))
        .Times(8)
        .WillRepeatedly(Return(kErrCodeSnapshotServerSuccess));


    EXPECT_CALL(*dataStore_, DataChunkTranferComplete(_, _))
        .Times(4)
        .WillRepeatedly(Return(kErrCodeSnapshotServerSuccess));


    EXPECT_CALL(*client_, DeleteSnapshot(fileName, user, seqNum))
        .WillOnce(Return(kErrCodeSnapshotServerSuccess));

    EXPECT_CALL(*client_, CheckSnapShotStatus(_, _, _))
        .WillOnce(Return(LIBCURVE_ERROR::OK));

    core_->HandleCreateSnapshotTask(task);

    ASSERT_TRUE(task->IsFinish());
}

TEST_F(TestSnapshotCoreImpl,
    TestHandleCreateSnapshotTask_CreateSnapshotFail) {
    UUID uuid = "uuid1";
    std::string user = "user1";
    std::string fileName = "file1";
    std::string desc = "snap1";
    uint64_t seqNum = 100;

    SnapshotInfo info(uuid, user, fileName, desc);
    info.SetStatus(Status::pending);
    std::shared_ptr<SnapshotTaskInfo> task =
        std::make_shared<SnapshotTaskInfo>(info);

    EXPECT_CALL(*client_, CreateSnapshot(fileName, user, _))
        .WillOnce(DoAll(
                    SetArgPointee<2>(seqNum),
                    Return(kErrCodeSnapshotInternalError)));


    EXPECT_CALL(*metaStore_, UpdateSnapshot(_))
        .WillOnce(Return(kErrCodeSnapshotServerSuccess));

    core_->HandleCreateSnapshotTask(task);

    ASSERT_TRUE(task->IsFinish());
}

TEST_F(TestSnapshotCoreImpl,
    TestHandleCreateSnapshotTask_GetSnapshotFail) {
    UUID uuid = "uuid1";
    std::string user = "user1";
    std::string fileName = "file1";
    std::string desc = "snap1";
    uint64_t seqNum = 100;

    SnapshotInfo info(uuid, user, fileName, desc);
    info.SetStatus(Status::pending);
    std::shared_ptr<SnapshotTaskInfo> task =
        std::make_shared<SnapshotTaskInfo>(info);

    EXPECT_CALL(*client_, CreateSnapshot(fileName, user, _))
        .WillOnce(DoAll(
                    SetArgPointee<2>(seqNum),
                    Return(kErrCodeSnapshotServerSuccess)));


    FInfo snapInfo;
    snapInfo.seqnum = 100;
    snapInfo.chunksize = 2 * kChunkSplitSize;
    snapInfo.segmentsize = 2 * snapInfo.chunksize;
    snapInfo.length = 2 * snapInfo.segmentsize;
    snapInfo.ctime = 10;
    EXPECT_CALL(*client_, GetSnapshot(fileName, user, seqNum, _))
        .WillOnce(DoAll(
                    SetArgPointee<3>(snapInfo),
                    Return(kErrCodeSnapshotInternalError)));

    EXPECT_CALL(*metaStore_, UpdateSnapshot(_))
        .WillOnce(Return(kErrCodeSnapshotServerSuccess));

    core_->HandleCreateSnapshotTask(task);

    ASSERT_TRUE(task->IsFinish());
}

TEST_F(TestSnapshotCoreImpl,
    TestHandleCreateSnapshotTask_UpdateSnapshotFail) {
    UUID uuid = "uuid1";
    std::string user = "user1";
    std::string fileName = "file1";
    std::string desc = "snap1";
    uint64_t seqNum = 100;

    SnapshotInfo info(uuid, user, fileName, desc);
    info.SetStatus(Status::pending);
    std::shared_ptr<SnapshotTaskInfo> task =
        std::make_shared<SnapshotTaskInfo>(info);

    EXPECT_CALL(*client_, CreateSnapshot(fileName, user, _))
        .WillOnce(DoAll(
                    SetArgPointee<2>(seqNum),
                    Return(kErrCodeSnapshotServerSuccess)));


    FInfo snapInfo;
    snapInfo.seqnum = 100;
    snapInfo.chunksize = 2 * kChunkSplitSize;
    snapInfo.segmentsize = 2 * snapInfo.chunksize;
    snapInfo.length = 2 * snapInfo.segmentsize;
    snapInfo.ctime = 10;
    EXPECT_CALL(*client_, GetSnapshot(fileName, user, seqNum, _))
        .WillOnce(DoAll(
                    SetArgPointee<3>(snapInfo),
                    Return(kErrCodeSnapshotServerSuccess)));


    EXPECT_CALL(*metaStore_, UpdateSnapshot(_))
        .Times(2)
        .WillOnce(Return(kErrCodeSnapshotInternalError))
        .WillOnce(Return(kErrCodeSnapshotServerSuccess));

    core_->HandleCreateSnapshotTask(task);

    ASSERT_TRUE(task->IsFinish());
}

TEST_F(TestSnapshotCoreImpl,
    TestHandleCreateSnapshotTask_SecondTimeUpdateSnapshotFail) {
    UUID uuid = "uuid1";
    std::string user = "user1";
    std::string fileName = "file1";
    std::string desc = "snap1";
    uint64_t seqNum = 100;

    SnapshotInfo info(uuid, user, fileName, desc);
    info.SetStatus(Status::pending);
    std::shared_ptr<SnapshotTaskInfo> task =
        std::make_shared<SnapshotTaskInfo>(info);

    EXPECT_CALL(*client_, CreateSnapshot(fileName, user, _))
        .WillOnce(DoAll(
                    SetArgPointee<2>(seqNum),
                    Return(kErrCodeSnapshotServerSuccess)));


    FInfo snapInfo;
    snapInfo.seqnum = 100;
    snapInfo.chunksize = 2 * kChunkSplitSize;
    snapInfo.segmentsize = 2 * snapInfo.chunksize;
    snapInfo.length = 2 * snapInfo.segmentsize;
    snapInfo.ctime = 10;
    EXPECT_CALL(*client_, GetSnapshot(fileName, user, seqNum, _))
        .WillOnce(DoAll(
                    SetArgPointee<3>(snapInfo),
                    Return(kErrCodeSnapshotServerSuccess)));


    EXPECT_CALL(*metaStore_, UpdateSnapshot(_))
        .Times(2)
        .WillRepeatedly(Return(kErrCodeSnapshotInternalError));

    core_->HandleCreateSnapshotTask(task);

    ASSERT_TRUE(task->IsFinish());
}

TEST_F(TestSnapshotCoreImpl,
    TestHandleCreateSnapshotTask_GetSnapshotSegmentInfoFail) {
    UUID uuid = "uuid1";
    std::string user = "user1";
    std::string fileName = "file1";
    std::string desc = "snap1";
    uint64_t seqNum = 100;

    SnapshotInfo info(uuid, user, fileName, desc);
    info.SetStatus(Status::pending);
    std::shared_ptr<SnapshotTaskInfo> task =
        std::make_shared<SnapshotTaskInfo>(info);


    EXPECT_CALL(*client_, CreateSnapshot(fileName, user, _))
        .WillOnce(DoAll(
                    SetArgPointee<2>(seqNum),
                    Return(kErrCodeSnapshotServerSuccess)));


    FInfo snapInfo;
    snapInfo.seqnum = 100;
    snapInfo.chunksize = 2 * kChunkSplitSize;
    snapInfo.segmentsize = 2 * snapInfo.chunksize;
    snapInfo.length = 2 * snapInfo.segmentsize;
    snapInfo.ctime = 10;
    EXPECT_CALL(*client_, GetSnapshot(fileName, user, seqNum, _))
        .WillOnce(DoAll(
                    SetArgPointee<3>(snapInfo),
                    Return(kErrCodeSnapshotServerSuccess)));


    EXPECT_CALL(*metaStore_, UpdateSnapshot(_))
        .Times(2)
        .WillRepeatedly(Return(kErrCodeSnapshotServerSuccess));

    EXPECT_CALL(*client_, GetSnapshotSegmentInfo(fileName,
          user,
          seqNum,
            _,
            _))
        .WillRepeatedly(Return(kErrCodeSnapshotInternalError));

    core_->HandleCreateSnapshotTask(task);

    ASSERT_TRUE(task->IsFinish());
}

TEST_F(TestSnapshotCoreImpl,
    TestHandleCreateSnapshotTask_GetChunkInfoFail) {
    UUID uuid = "uuid1";
    std::string user = "user1";
    std::string fileName = "file1";
    std::string desc = "snap1";
    uint64_t seqNum = 100;

    SnapshotInfo info(uuid, user, fileName, desc);
    info.SetStatus(Status::pending);
    std::shared_ptr<SnapshotTaskInfo> task =
        std::make_shared<SnapshotTaskInfo>(info);


    EXPECT_CALL(*client_, CreateSnapshot(fileName, user, _))
        .WillOnce(DoAll(
                    SetArgPointee<2>(seqNum),
                    Return(kErrCodeSnapshotServerSuccess)));


    FInfo snapInfo;
    snapInfo.seqnum = 100;
    snapInfo.chunksize = 2 * kChunkSplitSize;
    snapInfo.segmentsize = 2 * snapInfo.chunksize;
    snapInfo.length = 2 * snapInfo.segmentsize;
    snapInfo.ctime = 10;
    EXPECT_CALL(*client_, GetSnapshot(fileName, user, seqNum, _))
        .WillOnce(DoAll(
                    SetArgPointee<3>(snapInfo),
                    Return(kErrCodeSnapshotServerSuccess)));


    EXPECT_CALL(*metaStore_, UpdateSnapshot(_))
        .Times(2)
        .WillRepeatedly(Return(kErrCodeSnapshotServerSuccess));

    LogicPoolID lpid1 = 1;
    CopysetID cpid1 = 1;
    ChunkID chunkId1 = 1;
    LogicPoolID lpid2 = 2;
    CopysetID cpid2 = 2;
    ChunkID chunkId2 = 2;

    SegmentInfo segInfo1;
    segInfo1.chunkvec.push_back(
        ChunkIDInfo(chunkId1, lpid1, cpid1));
    segInfo1.chunkvec.push_back(
        ChunkIDInfo(chunkId2, lpid2, cpid2));

    LogicPoolID lpid3 = 3;
    CopysetID cpid3 = 3;
    ChunkID chunkId3 = 3;
    LogicPoolID lpid4 = 4;
    CopysetID cpid4 = 4;
    ChunkID chunkId4 = 4;

    SegmentInfo segInfo2;
    segInfo2.chunkvec.push_back(
        ChunkIDInfo(chunkId3, lpid3, cpid3));
    segInfo2.chunkvec.push_back(
        ChunkIDInfo(chunkId4, lpid4, cpid4));

    EXPECT_CALL(*client_, GetSnapshotSegmentInfo(fileName,
          user,
          seqNum,
            _,
            _))
        .WillOnce(DoAll(SetArgPointee<4>(segInfo1),
                    Return(kErrCodeSnapshotServerSuccess)));

    uint64_t chunkSn = 100;
    ChunkInfoDetail chunkInfo;
    chunkInfo.chunkSn.push_back(chunkSn);
    EXPECT_CALL(*client_, GetChunkInfo(_, _))
        .WillOnce(DoAll(SetArgPointee<1>(chunkInfo),
                    Return(kErrCodeSnapshotInternalError)));

    core_->HandleCreateSnapshotTask(task);

    ASSERT_TRUE(task->IsFinish());
}

TEST_F(TestSnapshotCoreImpl,
    TestHandleCreateSnapshotTask_PutChunkIndexDataFail) {
    UUID uuid = "uuid1";
    std::string user = "user1";
    std::string fileName = "file1";
    std::string desc = "snap1";
    uint64_t seqNum = 100;

    SnapshotInfo info(uuid, user, fileName, desc);
    info.SetStatus(Status::pending);
    std::shared_ptr<SnapshotTaskInfo> task =
        std::make_shared<SnapshotTaskInfo>(info);


    EXPECT_CALL(*client_, CreateSnapshot(fileName, user, _))
        .WillOnce(DoAll(
                    SetArgPointee<2>(seqNum),
                    Return(kErrCodeSnapshotServerSuccess)));


    FInfo snapInfo;
    snapInfo.seqnum = 100;
    snapInfo.chunksize = 2 * kChunkSplitSize;
    snapInfo.segmentsize = 2 * snapInfo.chunksize;
    snapInfo.length = 2 * snapInfo.segmentsize;
    snapInfo.ctime = 10;
    EXPECT_CALL(*client_, GetSnapshot(fileName, user, seqNum, _))
        .WillOnce(DoAll(
                    SetArgPointee<3>(snapInfo),
                    Return(kErrCodeSnapshotServerSuccess)));


    EXPECT_CALL(*metaStore_, UpdateSnapshot(_))
        .Times(2)
        .WillRepeatedly(Return(kErrCodeSnapshotServerSuccess));

    LogicPoolID lpid1 = 1;
    CopysetID cpid1 = 1;
    ChunkID chunkId1 = 1;
    LogicPoolID lpid2 = 2;
    CopysetID cpid2 = 2;
    ChunkID chunkId2 = 2;

    SegmentInfo segInfo1;
    segInfo1.chunkvec.push_back(
        ChunkIDInfo(chunkId1, lpid1, cpid1));
    segInfo1.chunkvec.push_back(
        ChunkIDInfo(chunkId2, lpid2, cpid2));

    LogicPoolID lpid3 = 3;
    CopysetID cpid3 = 3;
    ChunkID chunkId3 = 3;
    LogicPoolID lpid4 = 4;
    CopysetID cpid4 = 4;
    ChunkID chunkId4 = 4;

    SegmentInfo segInfo2;
    segInfo2.chunkvec.push_back(
        ChunkIDInfo(chunkId3, lpid3, cpid3));
    segInfo2.chunkvec.push_back(
        ChunkIDInfo(chunkId4, lpid4, cpid4));

    EXPECT_CALL(*client_, GetSnapshotSegmentInfo(fileName,
          user,
          seqNum,
            _,
            _))
        .Times(2)
        .WillOnce(DoAll(SetArgPointee<4>(segInfo1),
                    Return(kErrCodeSnapshotServerSuccess)))
        .WillOnce(DoAll(SetArgPointee<4>(segInfo2),
                    Return(kErrCodeSnapshotServerSuccess)));

    uint64_t chunkSn = 100;
    ChunkInfoDetail chunkInfo;
    chunkInfo.chunkSn.push_back(chunkSn);
    EXPECT_CALL(*client_, GetChunkInfo(_, _))
        .Times(4)
        .WillRepeatedly(DoAll(SetArgPointee<1>(chunkInfo),
                    Return(kErrCodeSnapshotServerSuccess)));

    EXPECT_CALL(*dataStore_, PutChunkIndexData(_, _))
        .WillOnce(Return(kErrCodeSnapshotInternalError));

    core_->HandleCreateSnapshotTask(task);

    ASSERT_TRUE(task->IsFinish());
}

TEST_F(TestSnapshotCoreImpl,
    TestHandleCreateSnapshotTask_GetChunkIndexDataFail) {
    UUID uuid = "uuid1";
    std::string user = "user1";
    std::string fileName = "file1";
    std::string desc = "snap1";
    uint64_t seqNum = 100;

    SnapshotInfo info(uuid, user, fileName, desc);
    info.SetStatus(Status::pending);
    std::shared_ptr<SnapshotTaskInfo> task =
        std::make_shared<SnapshotTaskInfo>(info);


    EXPECT_CALL(*client_, CreateSnapshot(fileName, user, _))
        .WillOnce(DoAll(
                    SetArgPointee<2>(seqNum),
                    Return(kErrCodeSnapshotServerSuccess)));

    FInfo snapInfo;
    snapInfo.seqnum = 100;
    snapInfo.chunksize = 2 * kChunkSplitSize;
    snapInfo.segmentsize = 2 * snapInfo.chunksize;
    snapInfo.length = 2 * snapInfo.segmentsize;
    snapInfo.ctime = 10;
    EXPECT_CALL(*client_, GetSnapshot(fileName, user, seqNum, _))
        .WillOnce(DoAll(
                    SetArgPointee<3>(snapInfo),
                    Return(kErrCodeSnapshotServerSuccess)));


    EXPECT_CALL(*metaStore_, UpdateSnapshot(_))
        .Times(2)
        .WillRepeatedly(Return(kErrCodeSnapshotServerSuccess));

    LogicPoolID lpid1 = 1;
    CopysetID cpid1 = 1;
    ChunkID chunkId1 = 1;
    LogicPoolID lpid2 = 2;
    CopysetID cpid2 = 2;
    ChunkID chunkId2 = 2;

    SegmentInfo segInfo1;
    segInfo1.chunkvec.push_back(
        ChunkIDInfo(chunkId1, lpid1, cpid1));
    segInfo1.chunkvec.push_back(
        ChunkIDInfo(chunkId2, lpid2, cpid2));

    LogicPoolID lpid3 = 3;
    CopysetID cpid3 = 3;
    ChunkID chunkId3 = 3;
    LogicPoolID lpid4 = 4;
    CopysetID cpid4 = 4;
    ChunkID chunkId4 = 4;

    SegmentInfo segInfo2;
    segInfo2.chunkvec.push_back(
        ChunkIDInfo(chunkId3, lpid3, cpid3));
    segInfo2.chunkvec.push_back(
        ChunkIDInfo(chunkId4, lpid4, cpid4));

    EXPECT_CALL(*client_, GetSnapshotSegmentInfo(fileName,
          user,
          seqNum,
            _,
            _))
        .Times(2)
        .WillOnce(DoAll(SetArgPointee<4>(segInfo1),
                    Return(kErrCodeSnapshotServerSuccess)))
        .WillOnce(DoAll(SetArgPointee<4>(segInfo2),
                    Return(kErrCodeSnapshotServerSuccess)));

    uint64_t chunkSn = 100;
    ChunkInfoDetail chunkInfo;
    chunkInfo.chunkSn.push_back(chunkSn);
    EXPECT_CALL(*client_, GetChunkInfo(_, _))
        .Times(4)
        .WillRepeatedly(DoAll(SetArgPointee<1>(chunkInfo),
                    Return(kErrCodeSnapshotServerSuccess)));

    EXPECT_CALL(*dataStore_, PutChunkIndexData(_, _))
        .WillOnce(Return(kErrCodeSnapshotServerSuccess));

    UUID uuid2 = "uuid2";
    std::string desc2 = "desc2";

    std::vector<SnapshotInfo> snapInfos;
    SnapshotInfo info2(uuid2, user, fileName, desc2);
    info2.SetStatus(Status::done);
    snapInfos.push_back(info);
    snapInfos.push_back(info2);

    EXPECT_CALL(*metaStore_, GetSnapshotList(fileName, _))
        .WillOnce(DoAll(
                    SetArgPointee<1>(snapInfos),
                    Return(kErrCodeSnapshotServerSuccess)));

    ChunkIndexData indexData;
    indexData.PutChunkDataName(ChunkDataName(fileName, 1, 0));
    EXPECT_CALL(*dataStore_, GetChunkIndexData(_, _))
        .WillOnce(DoAll(
                    SetArgPointee<1>(indexData),
                    Return(kErrCodeSnapshotInternalError)));

    core_->HandleCreateSnapshotTask(task);

    ASSERT_TRUE(task->IsFinish());
}

TEST_F(TestSnapshotCoreImpl,
    TestHandleCreateSnapshotTask_DataChunkTranferInitFail) {
    UUID uuid = "uuid1";
    std::string user = "user1";
    std::string fileName = "file1";
    std::string desc = "snap1";
    uint64_t seqNum = 100;

    SnapshotInfo info(uuid, user, fileName, desc);
    info.SetStatus(Status::pending);
    std::shared_ptr<SnapshotTaskInfo> task =
        std::make_shared<SnapshotTaskInfo>(info);


    EXPECT_CALL(*client_, CreateSnapshot(fileName, user, _))
        .WillOnce(DoAll(
                    SetArgPointee<2>(seqNum),
                    Return(kErrCodeSnapshotServerSuccess)));

    FInfo snapInfo;
    snapInfo.seqnum = 100;
    snapInfo.chunksize = 2 * kChunkSplitSize;
    snapInfo.segmentsize = 2 * snapInfo.chunksize;
    snapInfo.length = 2 * snapInfo.segmentsize;
    snapInfo.ctime = 10;
    EXPECT_CALL(*client_, GetSnapshot(fileName, user, seqNum, _))
        .WillOnce(DoAll(
                    SetArgPointee<3>(snapInfo),
                    Return(kErrCodeSnapshotServerSuccess)));


    EXPECT_CALL(*metaStore_, UpdateSnapshot(_))
        .Times(2)
        .WillRepeatedly(Return(kErrCodeSnapshotServerSuccess));

    LogicPoolID lpid1 = 1;
    CopysetID cpid1 = 1;
    ChunkID chunkId1 = 1;
    LogicPoolID lpid2 = 2;
    CopysetID cpid2 = 2;
    ChunkID chunkId2 = 2;

    SegmentInfo segInfo1;
    segInfo1.chunkvec.push_back(
        ChunkIDInfo(chunkId1, lpid1, cpid1));
    segInfo1.chunkvec.push_back(
        ChunkIDInfo(chunkId2, lpid2, cpid2));

    LogicPoolID lpid3 = 3;
    CopysetID cpid3 = 3;
    ChunkID chunkId3 = 3;
    LogicPoolID lpid4 = 4;
    CopysetID cpid4 = 4;
    ChunkID chunkId4 = 4;

    SegmentInfo segInfo2;
    segInfo2.chunkvec.push_back(
        ChunkIDInfo(chunkId3, lpid3, cpid3));
    segInfo2.chunkvec.push_back(
        ChunkIDInfo(chunkId4, lpid4, cpid4));

    EXPECT_CALL(*client_, GetSnapshotSegmentInfo(fileName,
          user,
          seqNum,
            _,
            _))
        .Times(2)
        .WillOnce(DoAll(SetArgPointee<4>(segInfo1),
                    Return(kErrCodeSnapshotServerSuccess)))
        .WillOnce(DoAll(SetArgPointee<4>(segInfo2),
                    Return(kErrCodeSnapshotServerSuccess)));

    uint64_t chunkSn = 100;
    ChunkInfoDetail chunkInfo;
    chunkInfo.chunkSn.push_back(chunkSn);
    EXPECT_CALL(*client_, GetChunkInfo(_, _))
        .Times(4)
        .WillRepeatedly(DoAll(SetArgPointee<1>(chunkInfo),
                    Return(kErrCodeSnapshotServerSuccess)));

    EXPECT_CALL(*dataStore_, PutChunkIndexData(_, _))
        .WillOnce(Return(kErrCodeSnapshotServerSuccess));

    UUID uuid2 = "uuid2";
    std::string desc2 = "desc2";

    std::vector<SnapshotInfo> snapInfos;
    SnapshotInfo info2(uuid2, user, fileName, desc2);
    info.SetSeqNum(seqNum);
    info2.SetSeqNum(seqNum - 1);  //上一个快照
    info2.SetStatus(Status::done);
    snapInfos.push_back(info);
    snapInfos.push_back(info2);

    EXPECT_CALL(*metaStore_, GetSnapshotList(fileName, _))
        .WillOnce(DoAll(
                    SetArgPointee<1>(snapInfos),
                    Return(kErrCodeSnapshotServerSuccess)));

    ChunkIndexData indexData;
    indexData.PutChunkDataName(ChunkDataName(fileName, 1, 0));
    EXPECT_CALL(*dataStore_, GetChunkIndexData(_, _))
        .WillOnce(DoAll(
                    SetArgPointee<1>(indexData),
                    Return(kErrCodeSnapshotServerSuccess)));

    EXPECT_CALL(*dataStore_, DataChunkTranferInit(_, _))
        .WillOnce(Return(kErrCodeSnapshotInternalError));

    core_->HandleCreateSnapshotTask(task);

    ASSERT_TRUE(task->IsFinish());
}

TEST_F(TestSnapshotCoreImpl,
    TestHandleCreateSnapshotTask_ReadChunkSnapshotFail) {
    UUID uuid = "uuid1";
    std::string user = "user1";
    std::string fileName = "file1";
    std::string desc = "snap1";
    uint64_t seqNum = 100;

    SnapshotInfo info(uuid, user, fileName, desc);
    info.SetStatus(Status::pending);
    std::shared_ptr<SnapshotTaskInfo> task =
        std::make_shared<SnapshotTaskInfo>(info);

    EXPECT_CALL(*client_, CreateSnapshot(fileName, user, _))
        .WillOnce(DoAll(
                    SetArgPointee<2>(seqNum),
                    Return(kErrCodeSnapshotServerSuccess)));

    FInfo snapInfo;
    snapInfo.seqnum = 100;
    snapInfo.chunksize = 2 * kChunkSplitSize;
    snapInfo.segmentsize = 2 * snapInfo.chunksize;
    snapInfo.length = 2 * snapInfo.segmentsize;
    snapInfo.ctime = 10;
    EXPECT_CALL(*client_, GetSnapshot(fileName, user, seqNum, _))
        .WillOnce(DoAll(
                    SetArgPointee<3>(snapInfo),
                    Return(kErrCodeSnapshotServerSuccess)));


    EXPECT_CALL(*metaStore_, UpdateSnapshot(_))
        .Times(2)
        .WillRepeatedly(Return(kErrCodeSnapshotServerSuccess));

    LogicPoolID lpid1 = 1;
    CopysetID cpid1 = 1;
    ChunkID chunkId1 = 1;
    LogicPoolID lpid2 = 2;
    CopysetID cpid2 = 2;
    ChunkID chunkId2 = 2;

    SegmentInfo segInfo1;
    segInfo1.chunkvec.push_back(
        ChunkIDInfo(chunkId1, lpid1, cpid1));
    segInfo1.chunkvec.push_back(
        ChunkIDInfo(chunkId2, lpid2, cpid2));

    LogicPoolID lpid3 = 3;
    CopysetID cpid3 = 3;
    ChunkID chunkId3 = 3;
    LogicPoolID lpid4 = 4;
    CopysetID cpid4 = 4;
    ChunkID chunkId4 = 4;

    SegmentInfo segInfo2;
    segInfo2.chunkvec.push_back(
        ChunkIDInfo(chunkId3, lpid3, cpid3));
    segInfo2.chunkvec.push_back(
        ChunkIDInfo(chunkId4, lpid4, cpid4));

    EXPECT_CALL(*client_, GetSnapshotSegmentInfo(fileName,
          user,
          seqNum,
            _,
            _))
        .Times(2)
        .WillOnce(DoAll(SetArgPointee<4>(segInfo1),
                    Return(kErrCodeSnapshotServerSuccess)))
        .WillOnce(DoAll(SetArgPointee<4>(segInfo2),
                    Return(kErrCodeSnapshotServerSuccess)));

    uint64_t chunkSn = 100;
    ChunkInfoDetail chunkInfo;
    chunkInfo.chunkSn.push_back(chunkSn);
    EXPECT_CALL(*client_, GetChunkInfo(_, _))
        .Times(4)
        .WillRepeatedly(DoAll(SetArgPointee<1>(chunkInfo),
                    Return(kErrCodeSnapshotServerSuccess)));

    EXPECT_CALL(*dataStore_, PutChunkIndexData(_, _))
        .WillOnce(Return(kErrCodeSnapshotServerSuccess));

    UUID uuid2 = "uuid2";
    std::string desc2 = "desc2";

    std::vector<SnapshotInfo> snapInfos;
    SnapshotInfo info2(uuid2, user, fileName, desc2);
    info.SetSeqNum(seqNum);
    info2.SetSeqNum(seqNum - 1);
    info2.SetStatus(Status::done);
    snapInfos.push_back(info);
    snapInfos.push_back(info2);

    EXPECT_CALL(*metaStore_, GetSnapshotList(fileName,  _))
        .WillOnce(DoAll(
                    SetArgPointee<1>(snapInfos),
                    Return(kErrCodeSnapshotServerSuccess)));

    ChunkIndexData indexData;
    indexData.PutChunkDataName(ChunkDataName(fileName, 1, 0));
    EXPECT_CALL(*dataStore_, GetChunkIndexData(_, _))
        .WillOnce(DoAll(
                    SetArgPointee<1>(indexData),
                    Return(kErrCodeSnapshotServerSuccess)));

    EXPECT_CALL(*dataStore_, DataChunkTranferInit(_, _))
        .Times(1)
        .WillRepeatedly(Return(kErrCodeSnapshotServerSuccess));

    EXPECT_CALL(*client_, ReadChunkSnapshot(_, _, _, _, _))
        .WillOnce(Return(kErrCodeSnapshotInternalError));

    core_->HandleCreateSnapshotTask(task);

    ASSERT_TRUE(task->IsFinish());
}

TEST_F(TestSnapshotCoreImpl,
    TestHandleCreateSnapshotTask_DataChunkTranferAddPartFail) {
    UUID uuid = "uuid1";
    std::string user = "user1";
    std::string fileName = "file1";
    std::string desc = "snap1";
    uint64_t seqNum = 100;

    SnapshotInfo info(uuid, user, fileName, desc);
    info.SetStatus(Status::pending);
    std::shared_ptr<SnapshotTaskInfo> task =
        std::make_shared<SnapshotTaskInfo>(info);


    EXPECT_CALL(*client_, CreateSnapshot(fileName, user, _))
        .WillOnce(DoAll(
                    SetArgPointee<2>(seqNum),
                    Return(kErrCodeSnapshotServerSuccess)));

    FInfo snapInfo;
    snapInfo.seqnum = 100;
    snapInfo.chunksize = 2 * kChunkSplitSize;
    snapInfo.segmentsize = 2 * snapInfo.chunksize;
    snapInfo.length = 2 * snapInfo.segmentsize;
    snapInfo.ctime = 10;
    EXPECT_CALL(*client_, GetSnapshot(fileName, user, seqNum, _))
        .WillOnce(DoAll(
                    SetArgPointee<3>(snapInfo),
                    Return(kErrCodeSnapshotServerSuccess)));


    EXPECT_CALL(*metaStore_, UpdateSnapshot(_))
        .Times(2)
        .WillRepeatedly(Return(kErrCodeSnapshotServerSuccess));

    LogicPoolID lpid1 = 1;
    CopysetID cpid1 = 1;
    ChunkID chunkId1 = 1;
    LogicPoolID lpid2 = 2;
    CopysetID cpid2 = 2;
    ChunkID chunkId2 = 2;

    SegmentInfo segInfo1;
    segInfo1.chunkvec.push_back(
        ChunkIDInfo(chunkId1, lpid1, cpid1));
    segInfo1.chunkvec.push_back(
        ChunkIDInfo(chunkId2, lpid2, cpid2));

    LogicPoolID lpid3 = 3;
    CopysetID cpid3 = 3;
    ChunkID chunkId3 = 3;
    LogicPoolID lpid4 = 4;
    CopysetID cpid4 = 4;
    ChunkID chunkId4 = 4;

    SegmentInfo segInfo2;
    segInfo2.chunkvec.push_back(
        ChunkIDInfo(chunkId3, lpid3, cpid3));
    segInfo2.chunkvec.push_back(
        ChunkIDInfo(chunkId4, lpid4, cpid4));

    EXPECT_CALL(*client_, GetSnapshotSegmentInfo(fileName,
          user,
          seqNum,
            _,
            _))
        .Times(2)
        .WillOnce(DoAll(SetArgPointee<4>(segInfo1),
                    Return(kErrCodeSnapshotServerSuccess)))
        .WillOnce(DoAll(SetArgPointee<4>(segInfo2),
                    Return(kErrCodeSnapshotServerSuccess)));

    uint64_t chunkSn = 100;
    ChunkInfoDetail chunkInfo;
    chunkInfo.chunkSn.push_back(chunkSn);
    EXPECT_CALL(*client_, GetChunkInfo(_, _))
        .Times(4)
        .WillRepeatedly(DoAll(SetArgPointee<1>(chunkInfo),
                    Return(kErrCodeSnapshotServerSuccess)));

    EXPECT_CALL(*dataStore_, PutChunkIndexData(_, _))
        .WillOnce(Return(kErrCodeSnapshotServerSuccess));

    UUID uuid2 = "uuid2";
    std::string desc2 = "desc2";

    std::vector<SnapshotInfo> snapInfos;
    SnapshotInfo info2(uuid2, user, fileName, desc2);
    info.SetSeqNum(seqNum);
    info2.SetSeqNum(seqNum - 1);
    info2.SetStatus(Status::done);
    snapInfos.push_back(info);
    snapInfos.push_back(info2);

    EXPECT_CALL(*metaStore_, GetSnapshotList(fileName, _))
        .WillOnce(DoAll(
                    SetArgPointee<1>(snapInfos),
                    Return(kErrCodeSnapshotServerSuccess)));

    ChunkIndexData indexData;
    indexData.PutChunkDataName(ChunkDataName(fileName, 1, 0));
    EXPECT_CALL(*dataStore_, GetChunkIndexData(_, _))
        .WillOnce(DoAll(
                    SetArgPointee<1>(indexData),
                    Return(kErrCodeSnapshotServerSuccess)));

    EXPECT_CALL(*dataStore_, DataChunkTranferInit(_, _))
        .Times(1)
        .WillRepeatedly(Return(kErrCodeSnapshotServerSuccess));


    EXPECT_CALL(*client_, ReadChunkSnapshot(_, _, _, _, _))
        .Times(1)
        .WillRepeatedly(Return(kErrCodeSnapshotServerSuccess));

    EXPECT_CALL(*dataStore_, DataChunkTranferAddPart(_, _, _, _, _))
        .Times(1)
        .WillRepeatedly(Return(kErrCodeSnapshotInternalError));

    EXPECT_CALL(*dataStore_, DataChunkTranferAbort(_, _))
        .WillOnce(Return(kErrCodeSnapshotServerSuccess));

    core_->HandleCreateSnapshotTask(task);

    ASSERT_TRUE(task->IsFinish());
}

TEST_F(TestSnapshotCoreImpl,
    TestHandleCreateSnapshotTask_DataChunkTranferCompleteFail) {
    UUID uuid = "uuid1";
    std::string user = "user1";
    std::string fileName = "file1";
    std::string desc = "snap1";
    uint64_t seqNum = 100;

    SnapshotInfo info(uuid, user, fileName, desc);
    info.SetStatus(Status::pending);
    std::shared_ptr<SnapshotTaskInfo> task =
        std::make_shared<SnapshotTaskInfo>(info);


    EXPECT_CALL(*client_, CreateSnapshot(fileName, user, _))
        .WillOnce(DoAll(
                    SetArgPointee<2>(seqNum),
                    Return(kErrCodeSnapshotServerSuccess)));

    FInfo snapInfo;
    snapInfo.seqnum = 100;
    snapInfo.chunksize = 2 * kChunkSplitSize;
    snapInfo.segmentsize = 2 * snapInfo.chunksize;
    snapInfo.length = 2 * snapInfo.segmentsize;
    snapInfo.ctime = 10;
    EXPECT_CALL(*client_, GetSnapshot(fileName, user, seqNum, _))
        .WillOnce(DoAll(
                    SetArgPointee<3>(snapInfo),
                    Return(kErrCodeSnapshotServerSuccess)));


    EXPECT_CALL(*metaStore_, UpdateSnapshot(_))
        .Times(2)
        .WillRepeatedly(Return(kErrCodeSnapshotServerSuccess));

    LogicPoolID lpid1 = 1;
    CopysetID cpid1 = 1;
    ChunkID chunkId1 = 1;
    LogicPoolID lpid2 = 2;
    CopysetID cpid2 = 2;
    ChunkID chunkId2 = 2;

    SegmentInfo segInfo1;
    segInfo1.chunkvec.push_back(
        ChunkIDInfo(chunkId1, lpid1, cpid1));
    segInfo1.chunkvec.push_back(
        ChunkIDInfo(chunkId2, lpid2, cpid2));

    LogicPoolID lpid3 = 3;
    CopysetID cpid3 = 3;
    ChunkID chunkId3 = 3;
    LogicPoolID lpid4 = 4;
    CopysetID cpid4 = 4;
    ChunkID chunkId4 = 4;

    SegmentInfo segInfo2;
    segInfo2.chunkvec.push_back(
        ChunkIDInfo(chunkId3, lpid3, cpid3));
    segInfo2.chunkvec.push_back(
        ChunkIDInfo(chunkId4, lpid4, cpid4));

    EXPECT_CALL(*client_, GetSnapshotSegmentInfo(fileName,
            user,
            seqNum,
            _,
            _))
        .Times(2)
        .WillOnce(DoAll(SetArgPointee<4>(segInfo1),
                    Return(kErrCodeSnapshotServerSuccess)))
        .WillOnce(DoAll(SetArgPointee<4>(segInfo2),
                    Return(kErrCodeSnapshotServerSuccess)));

    uint64_t chunkSn = 100;
    ChunkInfoDetail chunkInfo;
    chunkInfo.chunkSn.push_back(chunkSn);
    EXPECT_CALL(*client_, GetChunkInfo(_, _))
        .Times(4)
        .WillRepeatedly(DoAll(SetArgPointee<1>(chunkInfo),
                    Return(kErrCodeSnapshotServerSuccess)));

    EXPECT_CALL(*dataStore_, PutChunkIndexData(_, _))
        .WillOnce(Return(kErrCodeSnapshotServerSuccess));

    UUID uuid2 = "uuid2";
    std::string desc2 = "desc2";

    std::vector<SnapshotInfo> snapInfos;
    SnapshotInfo info2(uuid2, user, fileName, desc2);
    info.SetSeqNum(seqNum);
    info2.SetSeqNum(seqNum - 1);
    info2.SetStatus(Status::done);
    snapInfos.push_back(info);
    snapInfos.push_back(info2);

    EXPECT_CALL(*metaStore_, GetSnapshotList(fileName, _))
        .WillOnce(DoAll(
                    SetArgPointee<1>(snapInfos),
                    Return(kErrCodeSnapshotServerSuccess)));

    ChunkIndexData indexData;
    indexData.PutChunkDataName(ChunkDataName(fileName, 1, 0));
    EXPECT_CALL(*dataStore_, GetChunkIndexData(_, _))
        .WillOnce(DoAll(
                    SetArgPointee<1>(indexData),
                    Return(kErrCodeSnapshotServerSuccess)));

    EXPECT_CALL(*dataStore_, DataChunkTranferInit(_, _))
        .Times(1)
        .WillRepeatedly(Return(kErrCodeSnapshotServerSuccess));


    EXPECT_CALL(*client_, ReadChunkSnapshot(_, _, _, _, _))
        .Times(2)
        .WillRepeatedly(Return(kErrCodeSnapshotServerSuccess));

    EXPECT_CALL(*dataStore_, DataChunkTranferAddPart(_, _, _, _, _))
        .Times(2)
        .WillRepeatedly(Return(kErrCodeSnapshotServerSuccess));

    EXPECT_CALL(*dataStore_, DataChunkTranferComplete(_, _))
        .WillOnce(Return(kErrCodeSnapshotInternalError));

    EXPECT_CALL(*dataStore_, DataChunkTranferAbort(_, _))
        .WillOnce(Return(kErrCodeSnapshotInternalError));

    core_->HandleCreateSnapshotTask(task);

    ASSERT_TRUE(task->IsFinish());
}

TEST_F(TestSnapshotCoreImpl,
    TestHandleCreateSnapshotTask_DeleteSnapshotFail) {
    UUID uuid = "uuid1";
    std::string user = "user1";
    std::string fileName = "file1";
    std::string desc = "snap1";
    uint64_t seqNum = 100;

    SnapshotInfo info(uuid, user, fileName, desc);
    info.SetStatus(Status::pending);
    std::shared_ptr<SnapshotTaskInfo> task =
        std::make_shared<SnapshotTaskInfo>(info);


    EXPECT_CALL(*client_, CreateSnapshot(fileName, user, _))
        .WillOnce(DoAll(
                    SetArgPointee<2>(seqNum),
                    Return(kErrCodeSnapshotServerSuccess)));

    FInfo snapInfo;
    snapInfo.seqnum = 100;
    snapInfo.chunksize = 2 * kChunkSplitSize;
    snapInfo.segmentsize = 2 * snapInfo.chunksize;
    snapInfo.length = 2 * snapInfo.segmentsize;
    snapInfo.ctime = 10;
    EXPECT_CALL(*client_, GetSnapshot(fileName, user, seqNum, _))
        .WillOnce(DoAll(
                    SetArgPointee<3>(snapInfo),
                    Return(kErrCodeSnapshotServerSuccess)));


    EXPECT_CALL(*metaStore_, UpdateSnapshot(_))
        .Times(2)
        .WillRepeatedly(Return(kErrCodeSnapshotServerSuccess));

    LogicPoolID lpid1 = 1;
    CopysetID cpid1 = 1;
    ChunkID chunkId1 = 1;
    LogicPoolID lpid2 = 2;
    CopysetID cpid2 = 2;
    ChunkID chunkId2 = 2;

    SegmentInfo segInfo1;
    segInfo1.chunkvec.push_back(
        ChunkIDInfo(chunkId1, lpid1, cpid1));
    segInfo1.chunkvec.push_back(
        ChunkIDInfo(chunkId2, lpid2, cpid2));

    LogicPoolID lpid3 = 3;
    CopysetID cpid3 = 3;
    ChunkID chunkId3 = 3;
    LogicPoolID lpid4 = 4;
    CopysetID cpid4 = 4;
    ChunkID chunkId4 = 4;

    SegmentInfo segInfo2;
    segInfo2.chunkvec.push_back(
        ChunkIDInfo(chunkId3, lpid3, cpid3));
    segInfo2.chunkvec.push_back(
        ChunkIDInfo(chunkId4, lpid4, cpid4));

    EXPECT_CALL(*client_, GetSnapshotSegmentInfo(fileName,
            user,
            seqNum,
            _,
            _))
        .Times(2)
        .WillOnce(DoAll(SetArgPointee<4>(segInfo1),
                    Return(kErrCodeSnapshotServerSuccess)))
        .WillOnce(DoAll(SetArgPointee<4>(segInfo2),
                    Return(kErrCodeSnapshotServerSuccess)));

    uint64_t chunkSn = 100;
    ChunkInfoDetail chunkInfo;
    chunkInfo.chunkSn.push_back(chunkSn);
    EXPECT_CALL(*client_, GetChunkInfo(_, _))
        .Times(4)
        .WillRepeatedly(DoAll(SetArgPointee<1>(chunkInfo),
                    Return(kErrCodeSnapshotServerSuccess)));

    EXPECT_CALL(*dataStore_, PutChunkIndexData(_, _))
        .WillOnce(Return(kErrCodeSnapshotServerSuccess));

    UUID uuid2 = "uuid2";
    std::string desc2 = "desc2";

    std::vector<SnapshotInfo> snapInfos;
    SnapshotInfo info2(uuid2, user, fileName, desc2);
    info.SetSeqNum(seqNum);
    info2.SetSeqNum(seqNum - 1);
    info2.SetStatus(Status::done);
    snapInfos.push_back(info);
    snapInfos.push_back(info2);

    EXPECT_CALL(*metaStore_, GetSnapshotList(fileName, _))
        .WillOnce(DoAll(
                    SetArgPointee<1>(snapInfos),
                    Return(kErrCodeSnapshotServerSuccess)));

    ChunkIndexData indexData;
    indexData.PutChunkDataName(ChunkDataName(fileName, 1, 0));
    EXPECT_CALL(*dataStore_, GetChunkIndexData(_, _))
        .WillOnce(DoAll(
                    SetArgPointee<1>(indexData),
                    Return(kErrCodeSnapshotServerSuccess)));

    EXPECT_CALL(*dataStore_, DataChunkTranferInit(_, _))
        .Times(4)
        .WillRepeatedly(Return(kErrCodeSnapshotServerSuccess));


    EXPECT_CALL(*client_, ReadChunkSnapshot(_, _, _, _, _))
        .Times(8)
        .WillRepeatedly(Return(kErrCodeSnapshotServerSuccess));

    EXPECT_CALL(*dataStore_, DataChunkTranferAddPart(_, _, _, _, _))
        .Times(8)
        .WillRepeatedly(Return(kErrCodeSnapshotServerSuccess));


    EXPECT_CALL(*dataStore_, DataChunkTranferComplete(_, _))
        .Times(4)
        .WillRepeatedly(Return(kErrCodeSnapshotServerSuccess));

    EXPECT_CALL(*client_, DeleteSnapshot(fileName, user, seqNum))
        .WillOnce(Return(kErrCodeSnapshotInternalError));

    core_->HandleCreateSnapshotTask(task);

    ASSERT_TRUE(task->IsFinish());
}

TEST_F(TestSnapshotCoreImpl,
    TestHandleCreateSnapshotTaskExistIndexDataSuccess) {
    UUID uuid = "uuid1";
    std::string user = "user1";
    std::string fileName = "file1";
    std::string desc = "snap1";
    uint64_t seqNum = 100;

    SnapshotInfo info(uuid, user, fileName, desc);
    info.SetSeqNum(seqNum);
    info.SetChunkSize(2 * kChunkSplitSize);
    info.SetSegmentSize(4 * kChunkSplitSize);
    info.SetFileLength(8 * kChunkSplitSize);
    info.SetStatus(Status::pending);

    std::shared_ptr<SnapshotTaskInfo> task =
        std::make_shared<SnapshotTaskInfo>(info);

    EXPECT_CALL(*dataStore_, ChunkIndexDataExist(_))
        .WillOnce(Return(true));

    ChunkIndexData indexData;
    indexData.PutChunkDataName(ChunkDataName(fileName, seqNum, 0));
    indexData.PutChunkDataName(ChunkDataName(fileName, seqNum, 1));
    indexData.PutChunkDataName(ChunkDataName(fileName, seqNum, 2));
    indexData.PutChunkDataName(ChunkDataName(fileName, seqNum, 3));

    ChunkIndexData indexData2;
    indexData2.PutChunkDataName(ChunkDataName(fileName, 1, 1));

    EXPECT_CALL(*dataStore_, GetChunkIndexData(_, _))
        .Times(2)
        .WillOnce(DoAll(
                    SetArgPointee<1>(indexData),
                    Return(kErrCodeSnapshotServerSuccess)))
        .WillOnce(DoAll(
                    SetArgPointee<1>(indexData2),
                    Return(kErrCodeSnapshotServerSuccess)));

    LogicPoolID lpid1 = 1;
    CopysetID cpid1 = 1;
    ChunkID chunkId1 = 1;
    LogicPoolID lpid2 = 2;
    CopysetID cpid2 = 2;
    ChunkID chunkId2 = 2;

    SegmentInfo segInfo1;
    segInfo1.chunkvec.push_back(
        ChunkIDInfo(chunkId1, lpid1, cpid1));
    segInfo1.chunkvec.push_back(
        ChunkIDInfo(chunkId2, lpid2, cpid2));

    LogicPoolID lpid3 = 3;
    CopysetID cpid3 = 3;
    ChunkID chunkId3 = 3;
    LogicPoolID lpid4 = 4;
    CopysetID cpid4 = 4;
    ChunkID chunkId4 = 4;

    SegmentInfo segInfo2;
    segInfo2.chunkvec.push_back(
        ChunkIDInfo(chunkId3, lpid3, cpid3));
    segInfo2.chunkvec.push_back(
        ChunkIDInfo(chunkId4, lpid4, cpid4));

    EXPECT_CALL(*client_, GetSnapshotSegmentInfo(fileName,
            user,
            seqNum,
            _,
            _))
        .Times(2)
        .WillOnce(DoAll(SetArgPointee<4>(segInfo1),
                    Return(kErrCodeSnapshotServerSuccess)))
        .WillOnce(DoAll(SetArgPointee<4>(segInfo2),
                    Return(kErrCodeSnapshotServerSuccess)));

    UUID uuid2 = "uuid2";
    std::string desc2 = "desc2";

    std::vector<SnapshotInfo> snapInfos;
    SnapshotInfo info2(uuid2, user, fileName, desc2);
    info.SetSeqNum(seqNum);
    info2.SetSeqNum(seqNum - 1);
    info2.SetStatus(Status::done);
    snapInfos.push_back(info);
    snapInfos.push_back(info2);

    EXPECT_CALL(*metaStore_, GetSnapshotList(fileName, _))
        .WillOnce(DoAll(
                    SetArgPointee<1>(snapInfos),
                    Return(kErrCodeSnapshotServerSuccess)));


    EXPECT_CALL(*dataStore_, DataChunkTranferInit(_, _))
        .Times(4)
        .WillRepeatedly(Return(kErrCodeSnapshotServerSuccess));


    EXPECT_CALL(*client_, ReadChunkSnapshot(_, _, _, _, _))
        .Times(8)
        .WillRepeatedly(Return(kErrCodeSnapshotServerSuccess));

    EXPECT_CALL(*dataStore_, DataChunkTranferAddPart(_, _, _, _, _))
        .Times(8)
        .WillRepeatedly(Return(kErrCodeSnapshotServerSuccess));

    EXPECT_CALL(*dataStore_, DataChunkTranferComplete(_, _))
        .Times(4)
        .WillRepeatedly(Return(kErrCodeSnapshotServerSuccess));

    EXPECT_CALL(*client_, DeleteSnapshot(fileName, user, seqNum))
        .WillOnce(Return(kErrCodeSnapshotServerSuccess));

    EXPECT_CALL(*client_, CheckSnapShotStatus(_, _, _))
        .WillOnce(Return(LIBCURVE_ERROR::OK));

    EXPECT_CALL(*metaStore_, UpdateSnapshot(_))
        .Times(1)
        .WillRepeatedly(Return(kErrCodeSnapshotServerSuccess));

    core_->HandleCreateSnapshotTask(task);

    ASSERT_TRUE(task->IsFinish());
}

TEST_F(TestSnapshotCoreImpl,
    TestHandleCreateSnapshotTaskChunkSizeNotAlignTokChunkSplitSize) {
    UUID uuid = "uuid1";
    std::string user = "user1";
    std::string fileName = "file1";
    std::string desc = "snap1";
    uint64_t seqNum = 100;

    SnapshotInfo info(uuid, user, fileName, desc);
    info.SetSeqNum(seqNum);
    info.SetChunkSize(2 * kChunkSplitSize + 1);
    info.SetSegmentSize(4 * kChunkSplitSize);
    info.SetFileLength(8 * kChunkSplitSize);
    info.SetStatus(Status::pending);
    std::shared_ptr<SnapshotTaskInfo> task =
        std::make_shared<SnapshotTaskInfo>(info);

    EXPECT_CALL(*dataStore_, ChunkIndexDataExist(_))
        .WillOnce(Return(true));

    ChunkIndexData indexData;
    indexData.PutChunkDataName(ChunkDataName(fileName, seqNum, 0));
    indexData.PutChunkDataName(ChunkDataName(fileName, seqNum, 1));
    indexData.PutChunkDataName(ChunkDataName(fileName, seqNum, 2));
    indexData.PutChunkDataName(ChunkDataName(fileName, seqNum, 3));

    ChunkIndexData indexData2;
    indexData2.PutChunkDataName(ChunkDataName(fileName, 1, 1));

    EXPECT_CALL(*dataStore_, GetChunkIndexData(_, _))
        .Times(2)
        .WillOnce(DoAll(
                    SetArgPointee<1>(indexData),
                    Return(kErrCodeSnapshotServerSuccess)))
        .WillOnce(DoAll(
                    SetArgPointee<1>(indexData2),
                    Return(kErrCodeSnapshotServerSuccess)));


    LogicPoolID lpid1 = 1;
    CopysetID cpid1 = 1;
    ChunkID chunkId1 = 1;
    LogicPoolID lpid2 = 2;
    CopysetID cpid2 = 2;
    ChunkID chunkId2 = 2;

    SegmentInfo segInfo1;
    segInfo1.chunkvec.push_back(
        ChunkIDInfo(chunkId1, lpid1, cpid1));
    segInfo1.chunkvec.push_back(
        ChunkIDInfo(chunkId2, lpid2, cpid2));

    LogicPoolID lpid3 = 3;
    CopysetID cpid3 = 3;
    ChunkID chunkId3 = 3;
    LogicPoolID lpid4 = 4;
    CopysetID cpid4 = 4;
    ChunkID chunkId4 = 4;

    SegmentInfo segInfo2;
    segInfo2.chunkvec.push_back(
        ChunkIDInfo(chunkId3, lpid3, cpid3));
    segInfo2.chunkvec.push_back(
        ChunkIDInfo(chunkId4, lpid4, cpid4));

    EXPECT_CALL(*client_, GetSnapshotSegmentInfo(fileName,
            user,
            seqNum,
            _,
            _))
        .Times(2)
        .WillOnce(DoAll(SetArgPointee<4>(segInfo1),
                    Return(kErrCodeSnapshotServerSuccess)))
        .WillOnce(DoAll(SetArgPointee<4>(segInfo2),
                    Return(kErrCodeSnapshotServerSuccess)));

    UUID uuid2 = "uuid2";
    std::string desc2 = "desc2";

    std::vector<SnapshotInfo> snapInfos;
    SnapshotInfo info2(uuid2, user, fileName, desc2);
    info.SetSeqNum(seqNum);
    info2.SetSeqNum(seqNum - 1);
    info2.SetStatus(Status::done);
    snapInfos.push_back(info);
    snapInfos.push_back(info2);

    EXPECT_CALL(*metaStore_, GetSnapshotList(fileName, _))
        .WillOnce(DoAll(
                    SetArgPointee<1>(snapInfos),
                    Return(kErrCodeSnapshotServerSuccess)));

    EXPECT_CALL(*metaStore_, UpdateSnapshot(_))
        .Times(1)
        .WillRepeatedly(Return(kErrCodeSnapshotServerSuccess));

    core_->HandleCreateSnapshotTask(task);

    ASSERT_TRUE(task->IsFinish());
}

TEST_F(TestSnapshotCoreImpl,
    TestHandleCreateSnapshotTaskChunkVecInfoMiss) {
    UUID uuid = "uuid1";
    std::string user = "user1";
    std::string fileName = "file1";
    std::string desc = "snap1";
    uint64_t seqNum = 100;

    SnapshotInfo info(uuid, user, fileName, desc);
    info.SetSeqNum(seqNum);
    info.SetChunkSize(2 * kChunkSplitSize);
    info.SetSegmentSize(4 * kChunkSplitSize);
    info.SetFileLength(8 * kChunkSplitSize);
    info.SetStatus(Status::pending);
    std::shared_ptr<SnapshotTaskInfo> task =
        std::make_shared<SnapshotTaskInfo>(info);

    EXPECT_CALL(*dataStore_, ChunkIndexDataExist(_))
        .WillOnce(Return(true));

    ChunkIndexData indexData;
    indexData.PutChunkDataName(ChunkDataName(fileName, seqNum, 0));
    indexData.PutChunkDataName(ChunkDataName(fileName, seqNum, 1));
    indexData.PutChunkDataName(ChunkDataName(fileName, seqNum, 2));
    indexData.PutChunkDataName(ChunkDataName(fileName, seqNum, 3));

    ChunkIndexData indexData2;
    indexData2.PutChunkDataName(ChunkDataName(fileName, 1, 1));

    EXPECT_CALL(*dataStore_, GetChunkIndexData(_, _))
        .Times(2)
        .WillOnce(DoAll(
                    SetArgPointee<1>(indexData),
                    Return(kErrCodeSnapshotServerSuccess)))
        .WillOnce(DoAll(
                    SetArgPointee<1>(indexData2),
                    Return(kErrCodeSnapshotServerSuccess)));


    SegmentInfo segInfo1;

    SegmentInfo segInfo2;

    EXPECT_CALL(*client_, GetSnapshotSegmentInfo(fileName,
            user,
            seqNum,
            _,
            _))
        .Times(2)
        .WillOnce(DoAll(SetArgPointee<4>(segInfo1),
                    Return(kErrCodeSnapshotServerSuccess)))
        .WillOnce(DoAll(SetArgPointee<4>(segInfo2),
                    Return(kErrCodeSnapshotServerSuccess)));

    UUID uuid2 = "uuid2";
    std::string desc2 = "desc2";

    std::vector<SnapshotInfo> snapInfos;
    SnapshotInfo info2(uuid2, user, fileName, desc2);
    info.SetSeqNum(seqNum);
    info2.SetSeqNum(seqNum - 1);
    info2.SetStatus(Status::done);
    snapInfos.push_back(info);
    snapInfos.push_back(info2);

    EXPECT_CALL(*metaStore_, GetSnapshotList(fileName, _))
        .WillOnce(DoAll(
                    SetArgPointee<1>(snapInfos),
                    Return(kErrCodeSnapshotServerSuccess)));

    EXPECT_CALL(*metaStore_, UpdateSnapshot(_))
        .Times(1)
        .WillRepeatedly(Return(kErrCodeSnapshotServerSuccess));

    core_->HandleCreateSnapshotTask(task);

    ASSERT_TRUE(task->IsFinish());
}

TEST_F(TestSnapshotCoreImpl,
    TestHandleDeleteSnapshotTaskSuccess) {
    UUID uuid = "uuid1";
    std::string user = "user1";
    std::string fileName = "file1";
    std::string desc = "snap1";
    uint64_t seqNum = 100;

    SnapshotInfo info(uuid, user, fileName, desc);
    info.SetSeqNum(seqNum);
    info.SetStatus(Status::deleting);
    std::shared_ptr<SnapshotTaskInfo> task =
        std::make_shared<SnapshotTaskInfo>(info);

    UUID uuid2 = "uuid2";
    std::string desc2 = "desc2";

    std::vector<SnapshotInfo> snapInfos;
    SnapshotInfo info2(uuid2, user, fileName, desc2);
    info2.SetStatus(Status::done);
    snapInfos.push_back(info);
    snapInfos.push_back(info2);

    EXPECT_CALL(*metaStore_, GetSnapshotList(fileName, _))
        .WillOnce(DoAll(
                    SetArgPointee<1>(snapInfos),
                    Return(kErrCodeSnapshotServerSuccess)));

    ChunkIndexData indexData1;
    indexData1.PutChunkDataName(ChunkDataName(fileName, seqNum, 0));
    ChunkIndexData indexData2;
    indexData2.PutChunkDataName(ChunkDataName(fileName, 1, 1));
    EXPECT_CALL(*dataStore_, GetChunkIndexData(_, _))
        .Times(2)
        .WillOnce(DoAll(
                    SetArgPointee<1>(indexData1),
                    Return(kErrCodeSnapshotServerSuccess)))
        .WillOnce(DoAll(
                    SetArgPointee<1>(indexData2),
                    Return(kErrCodeSnapshotServerSuccess)));

    EXPECT_CALL(*dataStore_, ChunkDataExist(_))
        .WillRepeatedly(Return(true));

    EXPECT_CALL(*dataStore_, DeleteChunkData(_))
        .WillRepeatedly(Return(kErrCodeSnapshotServerSuccess));

    EXPECT_CALL(*dataStore_, ChunkIndexDataExist(_))
        .WillRepeatedly(Return(true));

    EXPECT_CALL(*dataStore_, DeleteChunkIndexData(_))
        .WillOnce(Return(kErrCodeSnapshotServerSuccess));

    EXPECT_CALL(*metaStore_, DeleteSnapshot(uuid))
        .WillOnce(Return(kErrCodeSnapshotServerSuccess));

    core_->HandleDeleteSnapshotTask(task);
    ASSERT_TRUE(task->IsFinish());
}

TEST_F(TestSnapshotCoreImpl,
    TestHandleDeleteSnapshotTask_GetChunkIndexDataFirstTimeFail) {
    UUID uuid = "uuid1";
    std::string user = "user1";
    std::string fileName = "file1";
    std::string desc = "snap1";
    uint64_t seqNum = 100;

    SnapshotInfo info(uuid, user, fileName, desc);
    info.SetSeqNum(seqNum);
    info.SetStatus(Status::deleting);
    std::shared_ptr<SnapshotTaskInfo> task =
        std::make_shared<SnapshotTaskInfo>(info);

    UUID uuid2 = "uuid2";
    std::string desc2 = "desc2";

    std::vector<SnapshotInfo> snapInfos;
    SnapshotInfo info2(uuid2, user, fileName, desc2);
    info2.SetStatus(Status::done);
    snapInfos.push_back(info);
    snapInfos.push_back(info2);

    EXPECT_CALL(*metaStore_, GetSnapshotList(fileName, _))
        .WillOnce(DoAll(
                    SetArgPointee<1>(snapInfos),
                    Return(kErrCodeSnapshotServerSuccess)));

    ChunkIndexData indexData1;
    indexData1.PutChunkDataName(ChunkDataName(fileName, seqNum, 0));
    ChunkIndexData indexData2;
    indexData2.PutChunkDataName(ChunkDataName(fileName, 1, 1));
    EXPECT_CALL(*dataStore_, GetChunkIndexData(_, _))
        .Times(1)
        .WillOnce(DoAll(
                    SetArgPointee<1>(indexData1),
                    Return(kErrCodeSnapshotInternalError)));

    EXPECT_CALL(*metaStore_, UpdateSnapshot(_))
        .WillOnce(Return(kErrCodeSnapshotServerSuccess));
    core_->HandleDeleteSnapshotTask(task);
    ASSERT_TRUE(task->IsFinish());
}

TEST_F(TestSnapshotCoreImpl,
    TestHandleDeleteSnapshotTask_GetChunkIndexDataSecondTimeFail) {
    UUID uuid = "uuid1";
    std::string user = "user1";
    std::string fileName = "file1";
    std::string desc = "snap1";
    uint64_t seqNum = 100;

    SnapshotInfo info(uuid, user, fileName, desc);
    info.SetSeqNum(seqNum);
    info.SetStatus(Status::deleting);
    std::shared_ptr<SnapshotTaskInfo> task =
        std::make_shared<SnapshotTaskInfo>(info);

    UUID uuid2 = "uuid2";
    std::string desc2 = "desc2";

    std::vector<SnapshotInfo> snapInfos;
    SnapshotInfo info2(uuid2, user, fileName, desc2);
    info2.SetStatus(Status::done);
    snapInfos.push_back(info);
    snapInfos.push_back(info2);

    EXPECT_CALL(*metaStore_, GetSnapshotList(fileName, _))
        .WillOnce(DoAll(
                    SetArgPointee<1>(snapInfos),
                    Return(kErrCodeSnapshotServerSuccess)));

    ChunkIndexData indexData1;
    indexData1.PutChunkDataName(ChunkDataName(fileName, seqNum, 0));
    ChunkIndexData indexData2;
    indexData2.PutChunkDataName(ChunkDataName(fileName, 1, 1));
    EXPECT_CALL(*dataStore_, GetChunkIndexData(_, _))
        .Times(2)
        .WillOnce(DoAll(
                    SetArgPointee<1>(indexData1),
                    Return(kErrCodeSnapshotServerSuccess)))
        .WillOnce(DoAll(
                    SetArgPointee<1>(indexData2),
                    Return(kErrCodeSnapshotInternalError)));

    EXPECT_CALL(*dataStore_, ChunkIndexDataExist(_))
        .WillRepeatedly(Return(true));

    EXPECT_CALL(*metaStore_, UpdateSnapshot(_))
        .WillOnce(Return(kErrCodeSnapshotServerSuccess));

    core_->HandleDeleteSnapshotTask(task);
    ASSERT_TRUE(task->IsFinish());
}

TEST_F(TestSnapshotCoreImpl,
    TestHandleDeleteSnapshotTask_DeleteChunkIndexDataFail) {
    UUID uuid = "uuid1";
    std::string user = "user1";
    std::string fileName = "file1";
    std::string desc = "snap1";
    uint64_t seqNum = 100;

    SnapshotInfo info(uuid, user, fileName, desc);
    info.SetSeqNum(seqNum);
    info.SetStatus(Status::deleting);
    std::shared_ptr<SnapshotTaskInfo> task =
        std::make_shared<SnapshotTaskInfo>(info);

    UUID uuid2 = "uuid2";
    std::string desc2 = "desc2";

    std::vector<SnapshotInfo> snapInfos;
    SnapshotInfo info2(uuid2, user, fileName, desc2);
    info2.SetStatus(Status::done);
    snapInfos.push_back(info);
    snapInfos.push_back(info2);

    EXPECT_CALL(*metaStore_, GetSnapshotList(fileName, _))
        .WillOnce(DoAll(
                    SetArgPointee<1>(snapInfos),
                    Return(kErrCodeSnapshotServerSuccess)));

    ChunkIndexData indexData1;
    indexData1.PutChunkDataName(ChunkDataName(fileName, seqNum, 0));
    ChunkIndexData indexData2;
    indexData2.PutChunkDataName(ChunkDataName(fileName, 1, 1));
    EXPECT_CALL(*dataStore_, GetChunkIndexData(_, _))
        .Times(2)
        .WillOnce(DoAll(
                    SetArgPointee<1>(indexData1),
                    Return(kErrCodeSnapshotServerSuccess)))
        .WillOnce(DoAll(
                    SetArgPointee<1>(indexData2),
                    Return(kErrCodeSnapshotServerSuccess)));

    EXPECT_CALL(*dataStore_, ChunkDataExist(_))
        .WillRepeatedly(Return(true));

    EXPECT_CALL(*dataStore_, DeleteChunkData(_))
        .WillRepeatedly(Return(kErrCodeSnapshotServerSuccess));

    EXPECT_CALL(*dataStore_, ChunkIndexDataExist(_))
        .WillRepeatedly(Return(true));

    EXPECT_CALL(*dataStore_, DeleteChunkIndexData(_))
        .WillOnce(Return(kErrCodeSnapshotInternalError));

    EXPECT_CALL(*metaStore_, UpdateSnapshot(_))
        .WillOnce(Return(kErrCodeSnapshotServerSuccess));

    core_->HandleDeleteSnapshotTask(task);
    ASSERT_TRUE(task->IsFinish());
}

TEST_F(TestSnapshotCoreImpl,
    TestHandleDeleteSnapshotTaskDeleteSnapshotFail) {
    UUID uuid = "uuid1";
    std::string user = "user1";
    std::string fileName = "file1";
    std::string desc = "snap1";
    uint64_t seqNum = 100;

    SnapshotInfo info(uuid, user, fileName, desc);
    info.SetSeqNum(seqNum);
    info.SetStatus(Status::deleting);
    std::shared_ptr<SnapshotTaskInfo> task =
        std::make_shared<SnapshotTaskInfo>(info);

    UUID uuid2 = "uuid2";
    std::string desc2 = "desc2";

    std::vector<SnapshotInfo> snapInfos;
    SnapshotInfo info2(uuid2, user, fileName, desc2);
    info2.SetStatus(Status::done);
    snapInfos.push_back(info);
    snapInfos.push_back(info2);

    EXPECT_CALL(*metaStore_, GetSnapshotList(fileName, _))
        .WillOnce(DoAll(
                    SetArgPointee<1>(snapInfos),
                    Return(kErrCodeSnapshotServerSuccess)));

    ChunkIndexData indexData1;
    indexData1.PutChunkDataName(ChunkDataName(fileName, seqNum, 0));
    ChunkIndexData indexData2;
    indexData2.PutChunkDataName(ChunkDataName(fileName, 1, 1));
    EXPECT_CALL(*dataStore_, GetChunkIndexData(_, _))
        .Times(2)
        .WillOnce(DoAll(
                    SetArgPointee<1>(indexData1),
                    Return(kErrCodeSnapshotServerSuccess)))
        .WillOnce(DoAll(
                    SetArgPointee<1>(indexData2),
                    Return(kErrCodeSnapshotServerSuccess)));

    EXPECT_CALL(*dataStore_, ChunkDataExist(_))
        .WillRepeatedly(Return(true));

    EXPECT_CALL(*dataStore_, DeleteChunkData(_))
        .WillRepeatedly(Return(kErrCodeSnapshotServerSuccess));

    EXPECT_CALL(*dataStore_, ChunkIndexDataExist(_))
        .WillRepeatedly(Return(true));


    EXPECT_CALL(*dataStore_, DeleteChunkIndexData(_))
        .WillOnce(Return(kErrCodeSnapshotServerSuccess));

    EXPECT_CALL(*metaStore_, DeleteSnapshot(uuid))
        .WillOnce(Return(kErrCodeSnapshotInternalError));

    EXPECT_CALL(*metaStore_, UpdateSnapshot(_))
        .WillOnce(Return(kErrCodeSnapshotServerSuccess));

    core_->HandleDeleteSnapshotTask(task);
    ASSERT_TRUE(task->IsFinish());
}

TEST_F(TestSnapshotCoreImpl, TestHandleCreateSnapshotTaskCancelSuccess) {
    UUID uuid = "uuid1";
    std::string user = "user1";
    std::string fileName = "file1";
    std::string desc = "snap1";
    uint64_t seqNum = 100;

    SnapshotInfo info(uuid, user, fileName, desc);
    info.SetStatus(Status::pending);
    std::shared_ptr<SnapshotTaskInfo> task =
        std::make_shared<SnapshotTaskInfo>(info);

    EXPECT_CALL(*client_, CreateSnapshot(fileName, user, _))
        .WillOnce(DoAll(
                    SetArgPointee<2>(seqNum),
                    Return(kErrCodeSnapshotServerSuccess)));

    FInfo snapInfo;
    snapInfo.seqnum = 100;
    snapInfo.chunksize = 2 * kChunkSplitSize;
    snapInfo.segmentsize = 2 * snapInfo.chunksize;
    snapInfo.length = 2 * snapInfo.segmentsize;
    snapInfo.ctime = 10;
    EXPECT_CALL(*client_, GetSnapshot(fileName, user, seqNum, _))
        .WillOnce(DoAll(
                    SetArgPointee<3>(snapInfo),
                    Return(kErrCodeSnapshotServerSuccess)));


    EXPECT_CALL(*metaStore_, UpdateSnapshot(_))
        .WillOnce(Return(kErrCodeSnapshotServerSuccess));

    LogicPoolID lpid1 = 1;
    CopysetID cpid1 = 1;
    ChunkID chunkId1 = 1;
    LogicPoolID lpid2 = 2;
    CopysetID cpid2 = 2;
    ChunkID chunkId2 = 2;

    SegmentInfo segInfo1;
    segInfo1.chunkvec.push_back(
        ChunkIDInfo(chunkId1, lpid1, cpid1));
    segInfo1.chunkvec.push_back(
        ChunkIDInfo(chunkId2, lpid2, cpid2));

    LogicPoolID lpid3 = 3;
    CopysetID cpid3 = 3;
    ChunkID chunkId3 = 3;
    LogicPoolID lpid4 = 4;
    CopysetID cpid4 = 4;
    ChunkID chunkId4 = 4;

    SegmentInfo segInfo2;
    segInfo2.chunkvec.push_back(
        ChunkIDInfo(chunkId3, lpid3, cpid3));
    segInfo2.chunkvec.push_back(
        ChunkIDInfo(chunkId4, lpid4, cpid4));

    EXPECT_CALL(*client_, GetSnapshotSegmentInfo(fileName,
            user,
            seqNum,
            _,
            _))
        .Times(2)
        .WillOnce(DoAll(SetArgPointee<4>(segInfo1),
                    Return(kErrCodeSnapshotServerSuccess)))
        .WillOnce(DoAll(SetArgPointee<4>(segInfo2),
                    Return(kErrCodeSnapshotServerSuccess)));

    uint64_t chunkSn = 100;
    ChunkInfoDetail chunkInfo;
    chunkInfo.chunkSn.push_back(chunkSn);
    EXPECT_CALL(*client_, GetChunkInfo(_, _))
        .Times(4)
        .WillRepeatedly(DoAll(SetArgPointee<1>(chunkInfo),
                    Return(kErrCodeSnapshotServerSuccess)));

    EXPECT_CALL(*dataStore_, PutChunkIndexData(_, _))
        .WillOnce(Return(kErrCodeSnapshotServerSuccess));

    UUID uuid2 = "uuid2";
    std::string desc2 = "desc2";

    std::vector<SnapshotInfo> snapInfos;
    SnapshotInfo info2(uuid2, user, fileName, desc2);
    info.SetSeqNum(seqNum);
    info2.SetSeqNum(seqNum - 1);
    info2.SetStatus(Status::done);
    snapInfos.push_back(info);
    snapInfos.push_back(info2);

    EXPECT_CALL(*metaStore_, GetSnapshotList(fileName, _))
        .WillOnce(DoAll(
                    SetArgPointee<1>(snapInfos),
                    Return(kErrCodeSnapshotServerSuccess)));

    ChunkIndexData indexData;
    indexData.PutChunkDataName(ChunkDataName(fileName, 1, 0));
    EXPECT_CALL(*dataStore_, GetChunkIndexData(_, _))
        .WillOnce(DoAll(
                    SetArgPointee<1>(indexData),
                    Return(kErrCodeSnapshotServerSuccess)));

    EXPECT_CALL(*dataStore_, DataChunkTranferInit(_, _))
        .Times(4)
        .WillRepeatedly(Return(kErrCodeSnapshotServerSuccess));


    EXPECT_CALL(*client_, ReadChunkSnapshot(_, _, _, _, _))
        .Times(8)
        .WillRepeatedly(Return(kErrCodeSnapshotServerSuccess));

    EXPECT_CALL(*dataStore_, DataChunkTranferAddPart(_, _, _, _, _))
        .Times(8)
        .WillRepeatedly(Return(kErrCodeSnapshotServerSuccess));


    EXPECT_CALL(*dataStore_, DataChunkTranferComplete(_, _))
        .Times(4)
        .WillRepeatedly(Return(kErrCodeSnapshotServerSuccess));

    // 此处捕获task，设置cancel
    EXPECT_CALL(*client_, DeleteSnapshot(fileName, user, seqNum))
        .Times(2)
        .WillOnce(Invoke([task](const std::string &filename,
            const std::string &user,
            uint64_t seq) -> int {
                    task->Cancel();
                    return kErrCodeSnapshotServerSuccess;
                        }))
        .WillOnce(Return(kErrCodeSnapshotServerSuccess));

    EXPECT_CALL(*client_, CheckSnapShotStatus(_, _, _))
        .WillRepeatedly(Return(LIBCURVE_ERROR::OK));

    // 进入cancel
    EXPECT_CALL(*dataStore_, ChunkDataExist(_))
        .Times(4)
        .WillRepeatedly(Return(true));

    EXPECT_CALL(*dataStore_, DeleteChunkData(_))
        .Times(4)
        .WillRepeatedly(Return(kErrCodeSnapshotServerSuccess));

    EXPECT_CALL(*dataStore_, DeleteChunkIndexData(_))
        .WillOnce(Return(kErrCodeSnapshotServerSuccess));

    EXPECT_CALL(*metaStore_, DeleteSnapshot(uuid))
        .WillOnce(Return(kErrCodeSnapshotServerSuccess));

    core_->HandleCreateSnapshotTask(task);

    ASSERT_TRUE(task->IsFinish());
}

}  // namespace snapshotcloneserver
}  // namespace curve

