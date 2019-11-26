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
        snapshotRef_ =
            std::make_shared<SnapshotReference>();
        client_ = std::make_shared<MockCurveFsClient>();
        metaStore_ = std::make_shared<MockSnapshotCloneMetaStore>();
        dataStore_ = std::make_shared<MockSnapshotDataStore>();

        option.chunkSplitSize = 1024u * 1024u;
        option.checkSnapshotStatusIntervalMs = 1000u;
        option.maxSnapshotLimit = 64;
        core_ = std::make_shared<SnapshotCoreImpl>(client_,
                metaStore_,
                dataStore_,
                snapshotRef_,
                option);
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
    std::shared_ptr<SnapshotReference> snapshotRef_;
    SnapshotCloneServerOptions option;
};


TEST_F(TestSnapshotCoreImpl, TestCreateSnapshotPreSuccess) {
    const std::string file = "file";
    const std::string user = "user";
    const std::string desc = "snap1";
    SnapshotInfo info;

    std::vector<SnapshotInfo> list;
    SnapshotInfo sinfo;
    sinfo.SetStatus(Status::done);
    list.push_back(sinfo);
    EXPECT_CALL(*metaStore_, GetSnapshotList(_, _))
        .WillOnce(DoAll(
                SetArgPointee<1>(list),
                Return(kErrCodeSuccess)));
    FInfo fInfo;
    fInfo.filestatus = FileStatus::Created;
    fInfo.owner = user;
    EXPECT_CALL(*client_, GetFileInfo(_, _, _))
        .WillOnce(DoAll(
                    SetArgPointee<2>(fInfo),
                    Return(LIBCURVE_ERROR::OK)));
    EXPECT_CALL(*metaStore_, AddSnapshot(_))
        .WillOnce(Return(kErrCodeSuccess));
    int ret = core_->CreateSnapshotPre(file, user, desc, &info);
    ASSERT_EQ(kErrCodeSuccess, ret);
}

TEST_F(TestSnapshotCoreImpl, TestCreateSnapshotPreAddSnapshotFail) {
    const std::string file = "file";
    const std::string user = "user";
    const std::string desc = "snap1";
    SnapshotInfo info;
    EXPECT_CALL(*metaStore_, GetSnapshotList(_, _))
        .WillOnce(Return(kErrCodeSuccess));
    FInfo fInfo;
    fInfo.filestatus = FileStatus::Created;
    fInfo.owner = user;
    EXPECT_CALL(*client_, GetFileInfo(_, _, _))
        .WillOnce(DoAll(
                    SetArgPointee<2>(fInfo),
                    Return(LIBCURVE_ERROR::OK)));
    EXPECT_CALL(*metaStore_, AddSnapshot(_))
        .WillOnce(Return(kErrCodeInternalError));
    int ret = core_->CreateSnapshotPre(file, user, desc, &info);
    ASSERT_EQ(kErrCodeInternalError, ret);
}

TEST_F(TestSnapshotCoreImpl, TestCreateSnapshotPreFailHasError) {
    const std::string file = "file";
    const std::string user = "user";
    const std::string desc = "snap1";
    SnapshotInfo info;

    std::vector<SnapshotInfo> list;
    SnapshotInfo sinfo;
    sinfo.SetStatus(Status::error);
    list.push_back(sinfo);
    EXPECT_CALL(*metaStore_, GetSnapshotList(_, _))
        .WillOnce(DoAll(
                SetArgPointee<1>(list),
                Return(kErrCodeSuccess)));

    int ret = core_->CreateSnapshotPre(file, user, desc, &info);
    ASSERT_EQ(kErrCodeSnapshotCannotCreateWhenError, ret);
}

TEST_F(TestSnapshotCoreImpl, TestCreateSnapshotPreFileNotExist) {
    const std::string file = "file";
    const std::string user = "user";
    const std::string desc = "snap1";
    SnapshotInfo info;
    EXPECT_CALL(*metaStore_, GetSnapshotList(_, _))
        .WillOnce(Return(kErrCodeSuccess));
    FInfo fInfo;
    fInfo.filestatus = FileStatus::Created;
    EXPECT_CALL(*client_, GetFileInfo(_, _, _))
        .WillOnce(DoAll(
                    SetArgPointee<2>(fInfo),
                    Return(-LIBCURVE_ERROR::NOTEXIST)));
    int ret = core_->CreateSnapshotPre(file, user, desc, &info);
    ASSERT_EQ(kErrCodeFileNotExist, ret);
}

TEST_F(TestSnapshotCoreImpl, TestCreateSnapshotPreInvalidUser) {
    const std::string file = "file";
    const std::string user = "user";
    const std::string desc = "snap1";
    SnapshotInfo info;
    EXPECT_CALL(*metaStore_, GetSnapshotList(_, _))
        .WillOnce(Return(kErrCodeSuccess));
    FInfo fInfo;
    fInfo.filestatus = FileStatus::Created;
    fInfo.owner = "user2";
    EXPECT_CALL(*client_, GetFileInfo(_, _, _))
        .WillOnce(DoAll(
                    SetArgPointee<2>(fInfo),
                    Return(LIBCURVE_ERROR::OK)));
    int ret = core_->CreateSnapshotPre(file, user, desc, &info);
    ASSERT_EQ(kErrCodeInvalidUser, ret);
}

TEST_F(TestSnapshotCoreImpl, TestCreateSnapshotPreInternalError) {
    const std::string file = "file";
    const std::string user = "user";
    const std::string desc = "snap1";
    SnapshotInfo info;
    EXPECT_CALL(*metaStore_, GetSnapshotList(_, _))
        .WillOnce(Return(kErrCodeSuccess));
    FInfo fInfo;
    fInfo.filestatus = FileStatus::Created;
    EXPECT_CALL(*client_, GetFileInfo(_, _, _))
        .WillOnce(DoAll(
                    SetArgPointee<2>(fInfo),
                    Return(-LIBCURVE_ERROR::FAILED)));
    int ret = core_->CreateSnapshotPre(file, user, desc, &info);
    ASSERT_EQ(kErrCodeInternalError, ret);
}

TEST_F(TestSnapshotCoreImpl, TestCreateSnapshotPreFailStatusInvalid) {
    const std::string file = "file";
    const std::string user = "user";
    const std::string desc = "snap1";
    SnapshotInfo info;
    EXPECT_CALL(*metaStore_, GetSnapshotList(_, _))
        .WillOnce(Return(kErrCodeSuccess));
    FInfo fInfo;
    fInfo.filestatus = FileStatus::Cloning;
    fInfo.owner = user;
    EXPECT_CALL(*client_, GetFileInfo(_, _, _))
        .WillOnce(DoAll(
                    SetArgPointee<2>(fInfo),
                    Return(LIBCURVE_ERROR::OK)));
    int ret = core_->CreateSnapshotPre(file, user, desc, &info);
    ASSERT_EQ(kErrCodeFileStatusInvalid, ret);
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
                Return(kErrCodeSuccess)));

    EXPECT_CALL(*metaStore_, UpdateSnapshot(_))
        .WillOnce(Return(kErrCodeSuccess));

    SnapshotInfo infoOut;
    int ret = core_->DeleteSnapshotPre(uuid, user, fileName, &infoOut);
    ASSERT_EQ(kErrCodeSuccess, ret);
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
                 Return(kErrCodeInternalError)));

    SnapshotInfo infoOut;
    int ret = core_->DeleteSnapshotPre(uuid, user, fileName, &infoOut);
    ASSERT_EQ(kErrCodeSuccess, ret);
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
                Return(kErrCodeSuccess)));

    EXPECT_CALL(*metaStore_, UpdateSnapshot(_))
        .WillOnce(Return(kErrCodeInternalError));

    SnapshotInfo infoOut;
    int ret = core_->DeleteSnapshotPre(uuid, user, fileName, &infoOut);
    ASSERT_EQ(kErrCodeInternalError, ret);
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
                Return(kErrCodeSuccess)));

    SnapshotInfo infoOut;
    int ret = core_->DeleteSnapshotPre(uuid, user, fileName, &infoOut);
    ASSERT_EQ(kErrCodeInvalidUser, ret);
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
                Return(kErrCodeSuccess)));

    SnapshotInfo infoOut;
    int ret = core_->DeleteSnapshotPre(uuid, user, fileName, &infoOut);
    ASSERT_EQ(kErrCodeSnapshotCannotDeleteUnfinished, ret);
}

TEST_F(TestSnapshotCoreImpl, TestDeleteSnapshotPre_FileNameNotMatch) {
    UUID uuid = "uuid1";
    std::string user = "user1";
    std::string fileName = "file1";
    std::string desc = "desc1";

    std::string fileName2 = "file2";
    SnapshotInfo info(uuid, user, fileName2, desc);
    info.SetStatus(Status::done);
    EXPECT_CALL(*metaStore_, GetSnapshotInfo(uuid, _))
        .WillOnce(DoAll(SetArgPointee<1>(info),
                Return(kErrCodeSuccess)));

    SnapshotInfo infoOut;
    int ret = core_->DeleteSnapshotPre(uuid, user, fileName, &infoOut);
    ASSERT_EQ(kErrCodeFileNameNotMatch, ret);
}

TEST_F(TestSnapshotCoreImpl, TestDeleteSnapshotPre_TaskExit) {
    UUID uuid = "uuid1";
    std::string user = "user1";
    std::string fileName = "file1";
    std::string desc = "desc1";

    SnapshotInfo info(uuid, user, fileName, desc);
    info.SetStatus(Status::deleting);
    EXPECT_CALL(*metaStore_, GetSnapshotInfo(uuid, _))
        .WillOnce(DoAll(SetArgPointee<1>(info),
                Return(kErrCodeSuccess)));

    SnapshotInfo infoOut;
    int ret = core_->DeleteSnapshotPre(uuid, user, fileName, &infoOut);
    ASSERT_EQ(kErrCodeTaskExist, ret);
}

TEST_F(TestSnapshotCoreImpl,
    TestGetFileSnapshotInfoSuccess) {
    std::string file = "file1";
    std::vector<SnapshotInfo> info;

    EXPECT_CALL(*metaStore_, GetSnapshotList(file, _))
        .WillOnce(Return(kErrCodeSuccess));

    int ret = core_->GetFileSnapshotInfo(file, &info);
    ASSERT_EQ(kErrCodeSuccess, ret);
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
                    Return(LIBCURVE_ERROR::OK)));

    FInfo snapInfo;
    snapInfo.seqnum = 100;
    snapInfo.chunksize = 2 * option.chunkSplitSize;
    snapInfo.segmentsize = 2 * snapInfo.chunksize;
    snapInfo.length = 2 * snapInfo.segmentsize;
    snapInfo.ctime = 10;
    EXPECT_CALL(*client_, GetSnapshot(fileName, user, seqNum, _))
        .WillOnce(DoAll(
                    SetArgPointee<3>(snapInfo),
                    Return(LIBCURVE_ERROR::OK)));


    EXPECT_CALL(*metaStore_, UpdateSnapshot(_))
        .Times(2)
        .WillRepeatedly(Return(kErrCodeSuccess));

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
                    Return(LIBCURVE_ERROR::OK)))
        .WillOnce(DoAll(SetArgPointee<4>(segInfo2),
                    Return(kErrCodeSuccess)));

    uint64_t chunkSn = 100;
    ChunkInfoDetail chunkInfo;
    chunkInfo.chunkSn.push_back(chunkSn);
    EXPECT_CALL(*client_, GetChunkInfo(_, _))
        .Times(4)
        .WillRepeatedly(DoAll(SetArgPointee<1>(chunkInfo),
                    Return(LIBCURVE_ERROR::OK)));

    EXPECT_CALL(*dataStore_, PutChunkIndexData(_, _))
        .WillOnce(Return(kErrCodeSuccess));

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
                    Return(kErrCodeSuccess)));

    ChunkIndexData indexData;
    indexData.PutChunkDataName(ChunkDataName(fileName, 1, 0));
    EXPECT_CALL(*dataStore_, GetChunkIndexData(_, _))
        .WillOnce(DoAll(
                    SetArgPointee<1>(indexData),
                    Return(kErrCodeSuccess)));

    EXPECT_CALL(*dataStore_, DataChunkTranferInit(_, _))
        .Times(4)
        .WillRepeatedly(Return(kErrCodeSuccess));


    EXPECT_CALL(*client_, ReadChunkSnapshot(_, _, _, _, _))
        .Times(8)
        .WillRepeatedly(Return(LIBCURVE_ERROR::OK));

    EXPECT_CALL(*dataStore_, DataChunkTranferAddPart(_, _, _, _, _))
        .Times(8)
        .WillRepeatedly(Return(kErrCodeSuccess));


    EXPECT_CALL(*dataStore_, DataChunkTranferComplete(_, _))
        .Times(4)
        .WillRepeatedly(Return(kErrCodeSuccess));


    EXPECT_CALL(*client_, DeleteSnapshot(fileName, user, seqNum))
        .WillOnce(Return(LIBCURVE_ERROR::OK));

    EXPECT_CALL(*client_, CheckSnapShotStatus(_, _, _, _))
        .WillOnce(Return(-LIBCURVE_ERROR::NOTEXIST));

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
                    Return(-LIBCURVE_ERROR::FAILED)));


    EXPECT_CALL(*metaStore_, UpdateSnapshot(_))
        .WillOnce(Return(kErrCodeSuccess));

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
                    Return(LIBCURVE_ERROR::OK)));


    FInfo snapInfo;
    snapInfo.seqnum = 100;
    snapInfo.chunksize = 2 * option.chunkSplitSize;
    snapInfo.segmentsize = 2 * snapInfo.chunksize;
    snapInfo.length = 2 * snapInfo.segmentsize;
    snapInfo.ctime = 10;
    EXPECT_CALL(*client_, GetSnapshot(fileName, user, seqNum, _))
        .WillOnce(DoAll(
                    SetArgPointee<3>(snapInfo),
                    Return(-LIBCURVE_ERROR::FAILED)));

    EXPECT_CALL(*metaStore_, UpdateSnapshot(_))
        .WillOnce(Return(kErrCodeSuccess));

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
                    Return(LIBCURVE_ERROR::OK)));


    FInfo snapInfo;
    snapInfo.seqnum = 100;
    snapInfo.chunksize = 2 * option.chunkSplitSize;
    snapInfo.segmentsize = 2 * snapInfo.chunksize;
    snapInfo.length = 2 * snapInfo.segmentsize;
    snapInfo.ctime = 10;
    EXPECT_CALL(*client_, GetSnapshot(fileName, user, seqNum, _))
        .WillOnce(DoAll(
                    SetArgPointee<3>(snapInfo),
                    Return(LIBCURVE_ERROR::OK)));


    EXPECT_CALL(*metaStore_, UpdateSnapshot(_))
        .Times(2)
        .WillOnce(Return(kErrCodeInternalError))
        .WillOnce(Return(kErrCodeSuccess));

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
                    Return(LIBCURVE_ERROR::OK)));


    FInfo snapInfo;
    snapInfo.seqnum = 100;
    snapInfo.chunksize = 2 * option.chunkSplitSize;
    snapInfo.segmentsize = 2 * snapInfo.chunksize;
    snapInfo.length = 2 * snapInfo.segmentsize;
    snapInfo.ctime = 10;
    EXPECT_CALL(*client_, GetSnapshot(fileName, user, seqNum, _))
        .WillOnce(DoAll(
                    SetArgPointee<3>(snapInfo),
                    Return(LIBCURVE_ERROR::OK)));


    EXPECT_CALL(*metaStore_, UpdateSnapshot(_))
        .Times(2)
        .WillRepeatedly(Return(kErrCodeInternalError));

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
                    Return(LIBCURVE_ERROR::OK)));


    FInfo snapInfo;
    snapInfo.seqnum = 100;
    snapInfo.chunksize = 2 * option.chunkSplitSize;
    snapInfo.segmentsize = 2 * snapInfo.chunksize;
    snapInfo.length = 2 * snapInfo.segmentsize;
    snapInfo.ctime = 10;
    EXPECT_CALL(*client_, GetSnapshot(fileName, user, seqNum, _))
        .WillOnce(DoAll(
                    SetArgPointee<3>(snapInfo),
                    Return(LIBCURVE_ERROR::OK)));


    EXPECT_CALL(*metaStore_, UpdateSnapshot(_))
        .Times(2)
        .WillRepeatedly(Return(kErrCodeSuccess));

    EXPECT_CALL(*client_, GetSnapshotSegmentInfo(fileName,
          user,
          seqNum,
            _,
            _))
        .WillRepeatedly(Return(-LIBCURVE_ERROR::FAILED));

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
                    Return(LIBCURVE_ERROR::OK)));


    FInfo snapInfo;
    snapInfo.seqnum = 100;
    snapInfo.chunksize = 2 * option.chunkSplitSize;
    snapInfo.segmentsize = 2 * snapInfo.chunksize;
    snapInfo.length = 2 * snapInfo.segmentsize;
    snapInfo.ctime = 10;
    EXPECT_CALL(*client_, GetSnapshot(fileName, user, seqNum, _))
        .WillOnce(DoAll(
                    SetArgPointee<3>(snapInfo),
                    Return(LIBCURVE_ERROR::OK)));


    EXPECT_CALL(*metaStore_, UpdateSnapshot(_))
        .Times(2)
        .WillRepeatedly(Return(kErrCodeSuccess));

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
                    Return(LIBCURVE_ERROR::OK)));

    uint64_t chunkSn = 100;
    ChunkInfoDetail chunkInfo;
    chunkInfo.chunkSn.push_back(chunkSn);
    EXPECT_CALL(*client_, GetChunkInfo(_, _))
        .WillOnce(DoAll(SetArgPointee<1>(chunkInfo),
                    Return(-LIBCURVE_ERROR::FAILED)));

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
                    Return(LIBCURVE_ERROR::OK)));


    FInfo snapInfo;
    snapInfo.seqnum = 100;
    snapInfo.chunksize = 2 * option.chunkSplitSize;
    snapInfo.segmentsize = 2 * snapInfo.chunksize;
    snapInfo.length = 2 * snapInfo.segmentsize;
    snapInfo.ctime = 10;
    EXPECT_CALL(*client_, GetSnapshot(fileName, user, seqNum, _))
        .WillOnce(DoAll(
                    SetArgPointee<3>(snapInfo),
                    Return(LIBCURVE_ERROR::OK)));


    EXPECT_CALL(*metaStore_, UpdateSnapshot(_))
        .Times(2)
        .WillRepeatedly(Return(kErrCodeSuccess));

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
                    Return(LIBCURVE_ERROR::OK)))
        .WillOnce(DoAll(SetArgPointee<4>(segInfo2),
                    Return(LIBCURVE_ERROR::OK)));

    uint64_t chunkSn = 100;
    ChunkInfoDetail chunkInfo;
    chunkInfo.chunkSn.push_back(chunkSn);
    EXPECT_CALL(*client_, GetChunkInfo(_, _))
        .Times(4)
        .WillRepeatedly(DoAll(SetArgPointee<1>(chunkInfo),
                    Return(LIBCURVE_ERROR::OK)));

    EXPECT_CALL(*dataStore_, PutChunkIndexData(_, _))
        .WillOnce(Return(kErrCodeInternalError));

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
                    Return(LIBCURVE_ERROR::OK)));

    FInfo snapInfo;
    snapInfo.seqnum = 100;
    snapInfo.chunksize = 2 * option.chunkSplitSize;
    snapInfo.segmentsize = 2 * snapInfo.chunksize;
    snapInfo.length = 2 * snapInfo.segmentsize;
    snapInfo.ctime = 10;
    EXPECT_CALL(*client_, GetSnapshot(fileName, user, seqNum, _))
        .WillOnce(DoAll(
                    SetArgPointee<3>(snapInfo),
                    Return(LIBCURVE_ERROR::OK)));


    EXPECT_CALL(*metaStore_, UpdateSnapshot(_))
        .Times(2)
        .WillRepeatedly(Return(kErrCodeSuccess));

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
                    Return(LIBCURVE_ERROR::OK)))
        .WillOnce(DoAll(SetArgPointee<4>(segInfo2),
                    Return(LIBCURVE_ERROR::OK)));

    uint64_t chunkSn = 100;
    ChunkInfoDetail chunkInfo;
    chunkInfo.chunkSn.push_back(chunkSn);
    EXPECT_CALL(*client_, GetChunkInfo(_, _))
        .Times(4)
        .WillRepeatedly(DoAll(SetArgPointee<1>(chunkInfo),
                    Return(LIBCURVE_ERROR::OK)));

    EXPECT_CALL(*dataStore_, PutChunkIndexData(_, _))
        .WillOnce(Return(kErrCodeSuccess));

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
                    Return(kErrCodeSuccess)));

    ChunkIndexData indexData;
    indexData.PutChunkDataName(ChunkDataName(fileName, 1, 0));
    EXPECT_CALL(*dataStore_, GetChunkIndexData(_, _))
        .WillOnce(DoAll(
                    SetArgPointee<1>(indexData),
                    Return(kErrCodeSuccess)));

    EXPECT_CALL(*dataStore_, DataChunkTranferInit(_, _))
        .WillOnce(Return(kErrCodeInternalError));

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
                    Return(LIBCURVE_ERROR::OK)));

    FInfo snapInfo;
    snapInfo.seqnum = 100;
    snapInfo.chunksize = 2 * option.chunkSplitSize;
    snapInfo.segmentsize = 2 * snapInfo.chunksize;
    snapInfo.length = 2 * snapInfo.segmentsize;
    snapInfo.ctime = 10;
    EXPECT_CALL(*client_, GetSnapshot(fileName, user, seqNum, _))
        .WillOnce(DoAll(
                    SetArgPointee<3>(snapInfo),
                    Return(LIBCURVE_ERROR::OK)));


    EXPECT_CALL(*metaStore_, UpdateSnapshot(_))
        .Times(2)
        .WillRepeatedly(Return(kErrCodeSuccess));

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
                    Return(LIBCURVE_ERROR::OK)))
        .WillOnce(DoAll(SetArgPointee<4>(segInfo2),
                    Return(LIBCURVE_ERROR::OK)));

    uint64_t chunkSn = 100;
    ChunkInfoDetail chunkInfo;
    chunkInfo.chunkSn.push_back(chunkSn);
    EXPECT_CALL(*client_, GetChunkInfo(_, _))
        .Times(4)
        .WillRepeatedly(DoAll(SetArgPointee<1>(chunkInfo),
                    Return(LIBCURVE_ERROR::OK)));

    EXPECT_CALL(*dataStore_, PutChunkIndexData(_, _))
        .WillOnce(Return(kErrCodeSuccess));

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
                    Return(kErrCodeSuccess)));

    ChunkIndexData indexData;
    indexData.PutChunkDataName(ChunkDataName(fileName, 1, 0));
    EXPECT_CALL(*dataStore_, GetChunkIndexData(_, _))
        .WillOnce(DoAll(
                    SetArgPointee<1>(indexData),
                    Return(kErrCodeSuccess)));

    EXPECT_CALL(*dataStore_, DataChunkTranferInit(_, _))
        .Times(1)
        .WillRepeatedly(Return(kErrCodeSuccess));

    EXPECT_CALL(*client_, ReadChunkSnapshot(_, _, _, _, _))
        .WillOnce(Return(-LIBCURVE_ERROR::FAILED));

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
                    Return(LIBCURVE_ERROR::OK)));

    FInfo snapInfo;
    snapInfo.seqnum = 100;
    snapInfo.chunksize = 2 * option.chunkSplitSize;
    snapInfo.segmentsize = 2 * snapInfo.chunksize;
    snapInfo.length = 2 * snapInfo.segmentsize;
    snapInfo.ctime = 10;
    EXPECT_CALL(*client_, GetSnapshot(fileName, user, seqNum, _))
        .WillOnce(DoAll(
                    SetArgPointee<3>(snapInfo),
                    Return(LIBCURVE_ERROR::OK)));


    EXPECT_CALL(*metaStore_, UpdateSnapshot(_))
        .Times(2)
        .WillRepeatedly(Return(kErrCodeSuccess));

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
                    Return(LIBCURVE_ERROR::OK)))
        .WillOnce(DoAll(SetArgPointee<4>(segInfo2),
                    Return(LIBCURVE_ERROR::OK)));

    uint64_t chunkSn = 100;
    ChunkInfoDetail chunkInfo;
    chunkInfo.chunkSn.push_back(chunkSn);
    EXPECT_CALL(*client_, GetChunkInfo(_, _))
        .Times(4)
        .WillRepeatedly(DoAll(SetArgPointee<1>(chunkInfo),
                    Return(LIBCURVE_ERROR::OK)));

    EXPECT_CALL(*dataStore_, PutChunkIndexData(_, _))
        .WillOnce(Return(kErrCodeSuccess));

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
                    Return(kErrCodeSuccess)));

    ChunkIndexData indexData;
    indexData.PutChunkDataName(ChunkDataName(fileName, 1, 0));
    EXPECT_CALL(*dataStore_, GetChunkIndexData(_, _))
        .WillOnce(DoAll(
                    SetArgPointee<1>(indexData),
                    Return(kErrCodeSuccess)));

    EXPECT_CALL(*dataStore_, DataChunkTranferInit(_, _))
        .Times(1)
        .WillRepeatedly(Return(kErrCodeSuccess));


    EXPECT_CALL(*client_, ReadChunkSnapshot(_, _, _, _, _))
        .Times(1)
        .WillRepeatedly(Return(LIBCURVE_ERROR::OK));

    EXPECT_CALL(*dataStore_, DataChunkTranferAddPart(_, _, _, _, _))
        .Times(1)
        .WillRepeatedly(Return(kErrCodeInternalError));

    EXPECT_CALL(*dataStore_, DataChunkTranferAbort(_, _))
        .WillOnce(Return(kErrCodeSuccess));

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
                    Return(LIBCURVE_ERROR::OK)));

    FInfo snapInfo;
    snapInfo.seqnum = 100;
    snapInfo.chunksize = 2 * option.chunkSplitSize;
    snapInfo.segmentsize = 2 * snapInfo.chunksize;
    snapInfo.length = 2 * snapInfo.segmentsize;
    snapInfo.ctime = 10;
    EXPECT_CALL(*client_, GetSnapshot(fileName, user, seqNum, _))
        .WillOnce(DoAll(
                    SetArgPointee<3>(snapInfo),
                    Return(LIBCURVE_ERROR::OK)));


    EXPECT_CALL(*metaStore_, UpdateSnapshot(_))
        .Times(2)
        .WillRepeatedly(Return(kErrCodeSuccess));

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
                    Return(LIBCURVE_ERROR::OK)))
        .WillOnce(DoAll(SetArgPointee<4>(segInfo2),
                    Return(LIBCURVE_ERROR::OK)));

    uint64_t chunkSn = 100;
    ChunkInfoDetail chunkInfo;
    chunkInfo.chunkSn.push_back(chunkSn);
    EXPECT_CALL(*client_, GetChunkInfo(_, _))
        .Times(4)
        .WillRepeatedly(DoAll(SetArgPointee<1>(chunkInfo),
                    Return(LIBCURVE_ERROR::OK)));

    EXPECT_CALL(*dataStore_, PutChunkIndexData(_, _))
        .WillOnce(Return(kErrCodeSuccess));

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
                    Return(kErrCodeSuccess)));

    ChunkIndexData indexData;
    indexData.PutChunkDataName(ChunkDataName(fileName, 1, 0));
    EXPECT_CALL(*dataStore_, GetChunkIndexData(_, _))
        .WillOnce(DoAll(
                    SetArgPointee<1>(indexData),
                    Return(kErrCodeSuccess)));

    EXPECT_CALL(*dataStore_, DataChunkTranferInit(_, _))
        .Times(1)
        .WillRepeatedly(Return(kErrCodeSuccess));


    EXPECT_CALL(*client_, ReadChunkSnapshot(_, _, _, _, _))
        .Times(2)
        .WillRepeatedly(Return(LIBCURVE_ERROR::OK));

    EXPECT_CALL(*dataStore_, DataChunkTranferAddPart(_, _, _, _, _))
        .Times(2)
        .WillRepeatedly(Return(kErrCodeSuccess));

    EXPECT_CALL(*dataStore_, DataChunkTranferComplete(_, _))
        .WillOnce(Return(kErrCodeInternalError));

    EXPECT_CALL(*dataStore_, DataChunkTranferAbort(_, _))
        .WillOnce(Return(kErrCodeInternalError));

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
                    Return(LIBCURVE_ERROR::OK)));

    FInfo snapInfo;
    snapInfo.seqnum = 100;
    snapInfo.chunksize = 2 * option.chunkSplitSize;
    snapInfo.segmentsize = 2 * snapInfo.chunksize;
    snapInfo.length = 2 * snapInfo.segmentsize;
    snapInfo.ctime = 10;
    EXPECT_CALL(*client_, GetSnapshot(fileName, user, seqNum, _))
        .WillOnce(DoAll(
                    SetArgPointee<3>(snapInfo),
                    Return(LIBCURVE_ERROR::OK)));


    EXPECT_CALL(*metaStore_, UpdateSnapshot(_))
        .Times(2)
        .WillRepeatedly(Return(kErrCodeSuccess));

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
                    Return(LIBCURVE_ERROR::OK)))
        .WillOnce(DoAll(SetArgPointee<4>(segInfo2),
                    Return(LIBCURVE_ERROR::OK)));

    uint64_t chunkSn = 100;
    ChunkInfoDetail chunkInfo;
    chunkInfo.chunkSn.push_back(chunkSn);
    EXPECT_CALL(*client_, GetChunkInfo(_, _))
        .Times(4)
        .WillRepeatedly(DoAll(SetArgPointee<1>(chunkInfo),
                    Return(LIBCURVE_ERROR::OK)));

    EXPECT_CALL(*dataStore_, PutChunkIndexData(_, _))
        .WillOnce(Return(kErrCodeSuccess));

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
                    Return(kErrCodeSuccess)));

    ChunkIndexData indexData;
    indexData.PutChunkDataName(ChunkDataName(fileName, 1, 0));
    EXPECT_CALL(*dataStore_, GetChunkIndexData(_, _))
        .WillOnce(DoAll(
                    SetArgPointee<1>(indexData),
                    Return(kErrCodeSuccess)));

    EXPECT_CALL(*dataStore_, DataChunkTranferInit(_, _))
        .Times(4)
        .WillRepeatedly(Return(kErrCodeSuccess));


    EXPECT_CALL(*client_, ReadChunkSnapshot(_, _, _, _, _))
        .Times(8)
        .WillRepeatedly(Return(LIBCURVE_ERROR::OK));

    EXPECT_CALL(*dataStore_, DataChunkTranferAddPart(_, _, _, _, _))
        .Times(8)
        .WillRepeatedly(Return(kErrCodeSuccess));


    EXPECT_CALL(*dataStore_, DataChunkTranferComplete(_, _))
        .Times(4)
        .WillRepeatedly(Return(kErrCodeSuccess));

    EXPECT_CALL(*client_, DeleteSnapshot(fileName, user, seqNum))
        .WillOnce(Return(-LIBCURVE_ERROR::FAILED));

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
    info.SetChunkSize(2 * option.chunkSplitSize);
    info.SetSegmentSize(4 * option.chunkSplitSize);
    info.SetFileLength(8 * option.chunkSplitSize);
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
                    Return(kErrCodeSuccess)))
        .WillOnce(DoAll(
                    SetArgPointee<1>(indexData2),
                    Return(kErrCodeSuccess)));

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
                    Return(LIBCURVE_ERROR::OK)))
        .WillOnce(DoAll(SetArgPointee<4>(segInfo2),
                    Return(LIBCURVE_ERROR::OK)));

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
                    Return(kErrCodeSuccess)));


    EXPECT_CALL(*dataStore_, DataChunkTranferInit(_, _))
        .Times(4)
        .WillRepeatedly(Return(kErrCodeSuccess));


    EXPECT_CALL(*client_, ReadChunkSnapshot(_, _, _, _, _))
        .Times(8)
        .WillRepeatedly(Return(LIBCURVE_ERROR::OK));

    EXPECT_CALL(*dataStore_, DataChunkTranferAddPart(_, _, _, _, _))
        .Times(8)
        .WillRepeatedly(Return(kErrCodeSuccess));

    EXPECT_CALL(*dataStore_, DataChunkTranferComplete(_, _))
        .Times(4)
        .WillRepeatedly(Return(kErrCodeSuccess));

    EXPECT_CALL(*client_, DeleteSnapshot(fileName, user, seqNum))
        .WillOnce(Return(LIBCURVE_ERROR::OK));

    EXPECT_CALL(*client_, CheckSnapShotStatus(_, _, _, _))
        .WillOnce(Return(-LIBCURVE_ERROR::NOTEXIST));

    EXPECT_CALL(*metaStore_, UpdateSnapshot(_))
        .Times(1)
        .WillRepeatedly(Return(kErrCodeSuccess));

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
    info.SetChunkSize(2 * option.chunkSplitSize + 1);
    info.SetSegmentSize(4 * option.chunkSplitSize);
    info.SetFileLength(8 * option.chunkSplitSize);
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
                    Return(kErrCodeSuccess)))
        .WillOnce(DoAll(
                    SetArgPointee<1>(indexData2),
                    Return(kErrCodeSuccess)));


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
                    Return(LIBCURVE_ERROR::OK)))
        .WillOnce(DoAll(SetArgPointee<4>(segInfo2),
                    Return(LIBCURVE_ERROR::OK)));

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
                    Return(kErrCodeSuccess)));

    EXPECT_CALL(*metaStore_, UpdateSnapshot(_))
        .Times(1)
        .WillRepeatedly(Return(kErrCodeSuccess));

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
    info.SetChunkSize(2 * option.chunkSplitSize);
    info.SetSegmentSize(4 * option.chunkSplitSize);
    info.SetFileLength(8 * option.chunkSplitSize);
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
                    Return(kErrCodeSuccess)))
        .WillOnce(DoAll(
                    SetArgPointee<1>(indexData2),
                    Return(kErrCodeSuccess)));


    SegmentInfo segInfo1;

    SegmentInfo segInfo2;

    EXPECT_CALL(*client_, GetSnapshotSegmentInfo(fileName,
            user,
            seqNum,
            _,
            _))
        .Times(2)
        .WillOnce(DoAll(SetArgPointee<4>(segInfo1),
                    Return(LIBCURVE_ERROR::OK)))
        .WillOnce(DoAll(SetArgPointee<4>(segInfo2),
                    Return(LIBCURVE_ERROR::OK)));

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
                    Return(kErrCodeSuccess)));

    EXPECT_CALL(*metaStore_, UpdateSnapshot(_))
        .Times(1)
        .WillRepeatedly(Return(kErrCodeSuccess));

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
                    Return(kErrCodeSuccess)));

    ChunkIndexData indexData1;
    indexData1.PutChunkDataName(ChunkDataName(fileName, seqNum, 0));
    ChunkIndexData indexData2;
    indexData2.PutChunkDataName(ChunkDataName(fileName, 1, 1));
    EXPECT_CALL(*dataStore_, GetChunkIndexData(_, _))
        .Times(2)
        .WillOnce(DoAll(
                    SetArgPointee<1>(indexData1),
                    Return(kErrCodeSuccess)))
        .WillOnce(DoAll(
                    SetArgPointee<1>(indexData2),
                    Return(kErrCodeSuccess)));

    EXPECT_CALL(*dataStore_, ChunkDataExist(_))
        .WillRepeatedly(Return(true));

    EXPECT_CALL(*dataStore_, DeleteChunkData(_))
        .WillRepeatedly(Return(kErrCodeSuccess));

    EXPECT_CALL(*dataStore_, ChunkIndexDataExist(_))
        .WillRepeatedly(Return(true));

    EXPECT_CALL(*dataStore_, DeleteChunkIndexData(_))
        .WillOnce(Return(kErrCodeSuccess));

    EXPECT_CALL(*metaStore_, DeleteSnapshot(uuid))
        .WillOnce(Return(kErrCodeSuccess));

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
                    Return(kErrCodeSuccess)));

    ChunkIndexData indexData1;
    indexData1.PutChunkDataName(ChunkDataName(fileName, seqNum, 0));
    ChunkIndexData indexData2;
    indexData2.PutChunkDataName(ChunkDataName(fileName, 1, 1));
    EXPECT_CALL(*dataStore_, GetChunkIndexData(_, _))
        .Times(2)
        .WillOnce(DoAll(
                    SetArgPointee<1>(indexData1),
                    Return(kErrCodeSuccess)))
        .WillOnce(DoAll(
                    SetArgPointee<1>(indexData2),
                    Return(kErrCodeInternalError)));

    EXPECT_CALL(*dataStore_, ChunkIndexDataExist(_))
        .WillRepeatedly(Return(true));

    EXPECT_CALL(*metaStore_, UpdateSnapshot(_))
        .WillOnce(Return(kErrCodeSuccess));

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
                    Return(kErrCodeSuccess)));

    ChunkIndexData indexData1;
    indexData1.PutChunkDataName(ChunkDataName(fileName, seqNum, 0));
    ChunkIndexData indexData2;
    indexData2.PutChunkDataName(ChunkDataName(fileName, 1, 1));
    EXPECT_CALL(*dataStore_, GetChunkIndexData(_, _))
        .Times(2)
        .WillOnce(DoAll(
                    SetArgPointee<1>(indexData1),
                    Return(kErrCodeSuccess)))
        .WillOnce(DoAll(
                    SetArgPointee<1>(indexData2),
                    Return(kErrCodeSuccess)));

    EXPECT_CALL(*dataStore_, ChunkDataExist(_))
        .WillRepeatedly(Return(true));

    EXPECT_CALL(*dataStore_, DeleteChunkData(_))
        .WillRepeatedly(Return(kErrCodeSuccess));

    EXPECT_CALL(*dataStore_, ChunkIndexDataExist(_))
        .WillRepeatedly(Return(true));

    EXPECT_CALL(*dataStore_, DeleteChunkIndexData(_))
        .WillOnce(Return(kErrCodeInternalError));

    EXPECT_CALL(*metaStore_, UpdateSnapshot(_))
        .WillOnce(Return(kErrCodeSuccess));

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
                    Return(kErrCodeSuccess)));

    ChunkIndexData indexData1;
    indexData1.PutChunkDataName(ChunkDataName(fileName, seqNum, 0));
    ChunkIndexData indexData2;
    indexData2.PutChunkDataName(ChunkDataName(fileName, 1, 1));
    EXPECT_CALL(*dataStore_, GetChunkIndexData(_, _))
        .Times(2)
        .WillOnce(DoAll(
                    SetArgPointee<1>(indexData1),
                    Return(kErrCodeSuccess)))
        .WillOnce(DoAll(
                    SetArgPointee<1>(indexData2),
                    Return(kErrCodeSuccess)));

    EXPECT_CALL(*dataStore_, ChunkDataExist(_))
        .WillRepeatedly(Return(true));

    EXPECT_CALL(*dataStore_, DeleteChunkData(_))
        .WillRepeatedly(Return(kErrCodeSuccess));

    EXPECT_CALL(*dataStore_, ChunkIndexDataExist(_))
        .WillRepeatedly(Return(true));


    EXPECT_CALL(*dataStore_, DeleteChunkIndexData(_))
        .WillOnce(Return(kErrCodeSuccess));

    EXPECT_CALL(*metaStore_, DeleteSnapshot(uuid))
        .WillOnce(Return(kErrCodeInternalError));

    EXPECT_CALL(*metaStore_, UpdateSnapshot(_))
        .WillOnce(Return(kErrCodeSuccess));

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
                    Return(LIBCURVE_ERROR::OK)));

    FInfo snapInfo;
    snapInfo.seqnum = 100;
    snapInfo.chunksize = 2 * option.chunkSplitSize;
    snapInfo.segmentsize = 2 * snapInfo.chunksize;
    snapInfo.length = 2 * snapInfo.segmentsize;
    snapInfo.ctime = 10;
    EXPECT_CALL(*client_, GetSnapshot(fileName, user, seqNum, _))
        .WillOnce(DoAll(
                    SetArgPointee<3>(snapInfo),
                    Return(LIBCURVE_ERROR::OK)));


    EXPECT_CALL(*metaStore_, UpdateSnapshot(_))
        .WillOnce(Return(kErrCodeSuccess));

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
                    Return(LIBCURVE_ERROR::OK)))
        .WillOnce(DoAll(SetArgPointee<4>(segInfo2),
                    Return(LIBCURVE_ERROR::OK)));

    uint64_t chunkSn = 100;
    ChunkInfoDetail chunkInfo;
    chunkInfo.chunkSn.push_back(chunkSn);
    EXPECT_CALL(*client_, GetChunkInfo(_, _))
        .Times(4)
        .WillRepeatedly(DoAll(SetArgPointee<1>(chunkInfo),
                    Return(LIBCURVE_ERROR::OK)));

    EXPECT_CALL(*dataStore_, PutChunkIndexData(_, _))
        .WillOnce(Return(kErrCodeSuccess));

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
                    Return(kErrCodeSuccess)));

    ChunkIndexData indexData;
    indexData.PutChunkDataName(ChunkDataName(fileName, 1, 0));
    EXPECT_CALL(*dataStore_, GetChunkIndexData(_, _))
        .WillOnce(DoAll(
                    SetArgPointee<1>(indexData),
                    Return(kErrCodeSuccess)));

    EXPECT_CALL(*dataStore_, DataChunkTranferInit(_, _))
        .Times(4)
        .WillRepeatedly(Return(kErrCodeSuccess));


    EXPECT_CALL(*client_, ReadChunkSnapshot(_, _, _, _, _))
        .Times(8)
        .WillRepeatedly(Return(LIBCURVE_ERROR::OK));

    EXPECT_CALL(*dataStore_, DataChunkTranferAddPart(_, _, _, _, _))
        .Times(8)
        .WillRepeatedly(Return(kErrCodeSuccess));


    EXPECT_CALL(*dataStore_, DataChunkTranferComplete(_, _))
        .Times(4)
        .WillRepeatedly(Return(kErrCodeSuccess));

    // 此处捕获task，设置cancel
    EXPECT_CALL(*client_, DeleteSnapshot(fileName, user, seqNum))
        .Times(2)
        .WillOnce(Invoke([task](const std::string &filename,
            const std::string &user,
            uint64_t seq) -> int {
                    task->Cancel();
                    return kErrCodeSuccess;
                        }))
        .WillOnce(Return(LIBCURVE_ERROR::OK));

    EXPECT_CALL(*client_, CheckSnapShotStatus(_, _, _, _))
        .WillRepeatedly(Return(-LIBCURVE_ERROR::NOTEXIST));

    // 进入cancel
    EXPECT_CALL(*dataStore_, ChunkDataExist(_))
        .Times(4)
        .WillRepeatedly(Return(true));

    EXPECT_CALL(*dataStore_, DeleteChunkData(_))
        .Times(4)
        .WillRepeatedly(Return(kErrCodeSuccess));

    EXPECT_CALL(*dataStore_, DeleteChunkIndexData(_))
        .WillOnce(Return(kErrCodeSuccess));

    EXPECT_CALL(*metaStore_, DeleteSnapshot(uuid))
        .WillOnce(Return(kErrCodeSuccess));

    core_->HandleCreateSnapshotTask(task);

    ASSERT_TRUE(task->IsFinish());
}

TEST_F(TestSnapshotCoreImpl,
    TestHandleCreateSnapshotTaskCancelAfterCreateSnapshotOnCurvefs) {
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
                    Return(LIBCURVE_ERROR::OK)));

    FInfo snapInfo;
    snapInfo.seqnum = 100;
    snapInfo.chunksize = 2 * option.chunkSplitSize;
    snapInfo.segmentsize = 2 * snapInfo.chunksize;
    snapInfo.length = 2 * snapInfo.segmentsize;
    snapInfo.ctime = 10;
    EXPECT_CALL(*client_, GetSnapshot(fileName, user, seqNum, _))
        .WillOnce(DoAll(
                    SetArgPointee<3>(snapInfo),
                    Return(LIBCURVE_ERROR::OK)));


    // 此处捕获task，设置cancel
    EXPECT_CALL(*metaStore_, UpdateSnapshot(_))
        .WillOnce(
               Invoke([task](const SnapshotInfo &snapinfo){
                    task->Cancel();
                    return kErrCodeSuccess;
                   }));

    // 进入cancel
    EXPECT_CALL(*client_, DeleteSnapshot(fileName, user, seqNum))
        .WillOnce(Return(LIBCURVE_ERROR::OK));

    EXPECT_CALL(*client_, CheckSnapShotStatus(_, _, _, _))
        .WillRepeatedly(Return(-LIBCURVE_ERROR::NOTEXIST));

    EXPECT_CALL(*metaStore_, DeleteSnapshot(uuid))
        .WillOnce(Return(kErrCodeSuccess));

    core_->HandleCreateSnapshotTask(task);

    ASSERT_TRUE(task->IsFinish());
}

TEST_F(TestSnapshotCoreImpl,
    TestHandleCreateSnapshotTaskCancelAfterCreateChunkIndexData) {
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
                    Return(LIBCURVE_ERROR::OK)));

    FInfo snapInfo;
    snapInfo.seqnum = 100;
    snapInfo.chunksize = 2 * option.chunkSplitSize;
    snapInfo.segmentsize = 2 * snapInfo.chunksize;
    snapInfo.length = 2 * snapInfo.segmentsize;
    snapInfo.ctime = 10;
    EXPECT_CALL(*client_, GetSnapshot(fileName, user, seqNum, _))
        .WillOnce(DoAll(
                    SetArgPointee<3>(snapInfo),
                    Return(LIBCURVE_ERROR::OK)));


    EXPECT_CALL(*metaStore_, UpdateSnapshot(_))
        .WillOnce(Return(kErrCodeSuccess));

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
                    Return(LIBCURVE_ERROR::OK)))
        .WillOnce(DoAll(SetArgPointee<4>(segInfo2),
                    Return(LIBCURVE_ERROR::OK)));

    uint64_t chunkSn = 100;
    ChunkInfoDetail chunkInfo;
    chunkInfo.chunkSn.push_back(chunkSn);
    EXPECT_CALL(*client_, GetChunkInfo(_, _))
        .Times(4)
        .WillRepeatedly(DoAll(SetArgPointee<1>(chunkInfo),
                    Return(LIBCURVE_ERROR::OK)));

    // 此处捕获task，设置cancel
    EXPECT_CALL(*dataStore_, PutChunkIndexData(_, _))
        .WillOnce(Invoke([task](const ChunkIndexDataName &name,
                              const ChunkIndexData &meta) {
                    task->Cancel();
                    return kErrCodeSuccess;
                    }));


    // 进入cancel
    EXPECT_CALL(*client_, DeleteSnapshot(fileName, user, seqNum))
        .WillOnce(Return(LIBCURVE_ERROR::OK));

    EXPECT_CALL(*client_, CheckSnapShotStatus(_, _, _, _))
        .WillRepeatedly(Return(-LIBCURVE_ERROR::NOTEXIST));

    EXPECT_CALL(*dataStore_, DeleteChunkIndexData(_))
        .WillOnce(Return(kErrCodeSuccess));

    EXPECT_CALL(*metaStore_, DeleteSnapshot(uuid))
        .WillOnce(Return(kErrCodeSuccess));

    core_->HandleCreateSnapshotTask(task);

    ASSERT_TRUE(task->IsFinish());
}

TEST_F(TestSnapshotCoreImpl,
    TestHandleCreateSnapshotTaskCancelFailOnDeleteChunkData) {
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
                    Return(LIBCURVE_ERROR::OK)));

    FInfo snapInfo;
    snapInfo.seqnum = 100;
    snapInfo.chunksize = 2 * option.chunkSplitSize;
    snapInfo.segmentsize = 2 * snapInfo.chunksize;
    snapInfo.length = 2 * snapInfo.segmentsize;
    snapInfo.ctime = 10;
    EXPECT_CALL(*client_, GetSnapshot(fileName, user, seqNum, _))
        .WillOnce(DoAll(
                    SetArgPointee<3>(snapInfo),
                    Return(LIBCURVE_ERROR::OK)));


    EXPECT_CALL(*metaStore_, UpdateSnapshot(_))
        .Times(2)
        .WillRepeatedly(Return(kErrCodeSuccess));

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
                    Return(LIBCURVE_ERROR::OK)))
        .WillOnce(DoAll(SetArgPointee<4>(segInfo2),
                    Return(LIBCURVE_ERROR::OK)));

    uint64_t chunkSn = 100;
    ChunkInfoDetail chunkInfo;
    chunkInfo.chunkSn.push_back(chunkSn);
    EXPECT_CALL(*client_, GetChunkInfo(_, _))
        .Times(4)
        .WillRepeatedly(DoAll(SetArgPointee<1>(chunkInfo),
                    Return(LIBCURVE_ERROR::OK)));

    EXPECT_CALL(*dataStore_, PutChunkIndexData(_, _))
        .WillOnce(Return(kErrCodeSuccess));

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
                    Return(kErrCodeSuccess)));

    ChunkIndexData indexData;
    indexData.PutChunkDataName(ChunkDataName(fileName, 1, 0));
    EXPECT_CALL(*dataStore_, GetChunkIndexData(_, _))
        .WillOnce(DoAll(
                    SetArgPointee<1>(indexData),
                    Return(kErrCodeSuccess)));

    EXPECT_CALL(*dataStore_, DataChunkTranferInit(_, _))
        .Times(4)
        .WillRepeatedly(Return(kErrCodeSuccess));


    EXPECT_CALL(*client_, ReadChunkSnapshot(_, _, _, _, _))
        .Times(8)
        .WillRepeatedly(Return(LIBCURVE_ERROR::OK));

    EXPECT_CALL(*dataStore_, DataChunkTranferAddPart(_, _, _, _, _))
        .Times(8)
        .WillRepeatedly(Return(kErrCodeSuccess));


    EXPECT_CALL(*dataStore_, DataChunkTranferComplete(_, _))
        .Times(4)
        .WillRepeatedly(Return(kErrCodeSuccess));

    // 此处捕获task，设置cancel
    EXPECT_CALL(*client_, DeleteSnapshot(fileName, user, seqNum))
        .WillOnce(Invoke([task](const std::string &filename,
            const std::string &user,
            uint64_t seq) -> int {
                    task->Cancel();
                    return kErrCodeSuccess;
                        }));

    EXPECT_CALL(*client_, CheckSnapShotStatus(_, _, _, _))
        .WillRepeatedly(Return(-LIBCURVE_ERROR::NOTEXIST));

    // 进入cancel
    EXPECT_CALL(*dataStore_, ChunkDataExist(_))
        .WillRepeatedly(Return(true));

    EXPECT_CALL(*dataStore_, DeleteChunkData(_))
        .WillRepeatedly(Return(kErrCodeInternalError));

    core_->HandleCreateSnapshotTask(task);

    ASSERT_TRUE(task->IsFinish());
}

TEST_F(TestSnapshotCoreImpl,
    TestHandleCreateSnapshotTaskCancelFailOnDeleteChunkIndexData) {
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
                    Return(LIBCURVE_ERROR::OK)));

    FInfo snapInfo;
    snapInfo.seqnum = 100;
    snapInfo.chunksize = 2 * option.chunkSplitSize;
    snapInfo.segmentsize = 2 * snapInfo.chunksize;
    snapInfo.length = 2 * snapInfo.segmentsize;
    snapInfo.ctime = 10;
    EXPECT_CALL(*client_, GetSnapshot(fileName, user, seqNum, _))
        .WillOnce(DoAll(
                    SetArgPointee<3>(snapInfo),
                    Return(LIBCURVE_ERROR::OK)));


    EXPECT_CALL(*metaStore_, UpdateSnapshot(_))
        .WillRepeatedly(Return(kErrCodeSuccess));

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
                    Return(LIBCURVE_ERROR::OK)))
        .WillOnce(DoAll(SetArgPointee<4>(segInfo2),
                    Return(LIBCURVE_ERROR::OK)));

    uint64_t chunkSn = 100;
    ChunkInfoDetail chunkInfo;
    chunkInfo.chunkSn.push_back(chunkSn);
    EXPECT_CALL(*client_, GetChunkInfo(_, _))
        .Times(4)
        .WillRepeatedly(DoAll(SetArgPointee<1>(chunkInfo),
                    Return(LIBCURVE_ERROR::OK)));

    EXPECT_CALL(*dataStore_, PutChunkIndexData(_, _))
        .WillOnce(Return(kErrCodeSuccess));

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
                    Return(kErrCodeSuccess)));

    ChunkIndexData indexData;
    indexData.PutChunkDataName(ChunkDataName(fileName, 1, 0));
    EXPECT_CALL(*dataStore_, GetChunkIndexData(_, _))
        .WillOnce(DoAll(
                    SetArgPointee<1>(indexData),
                    Return(kErrCodeSuccess)));

    EXPECT_CALL(*dataStore_, DataChunkTranferInit(_, _))
        .Times(4)
        .WillRepeatedly(Return(kErrCodeSuccess));


    EXPECT_CALL(*client_, ReadChunkSnapshot(_, _, _, _, _))
        .Times(8)
        .WillRepeatedly(Return(LIBCURVE_ERROR::OK));

    EXPECT_CALL(*dataStore_, DataChunkTranferAddPart(_, _, _, _, _))
        .Times(8)
        .WillRepeatedly(Return(kErrCodeSuccess));


    EXPECT_CALL(*dataStore_, DataChunkTranferComplete(_, _))
        .Times(4)
        .WillRepeatedly(Return(kErrCodeSuccess));

    // 此处捕获task，设置cancel
    EXPECT_CALL(*client_, DeleteSnapshot(fileName, user, seqNum))
        .WillOnce(Invoke([task](const std::string &filename,
            const std::string &user,
            uint64_t seq) -> int {
                    task->Cancel();
                    return kErrCodeSuccess;
                        }));

    EXPECT_CALL(*client_, CheckSnapShotStatus(_, _, _, _))
        .WillRepeatedly(Return(-LIBCURVE_ERROR::NOTEXIST));

    // 进入cancel
    EXPECT_CALL(*dataStore_, ChunkDataExist(_))
        .Times(4)
        .WillRepeatedly(Return(true));

    EXPECT_CALL(*dataStore_, DeleteChunkData(_))
        .Times(4)
        .WillRepeatedly(Return(kErrCodeSuccess));

    EXPECT_CALL(*dataStore_, DeleteChunkIndexData(_))
        .WillOnce(Return(kErrCodeInternalError));

    core_->HandleCreateSnapshotTask(task);

    ASSERT_TRUE(task->IsFinish());
}

TEST_F(TestSnapshotCoreImpl,
    TestHandleCreateSnapshotTaskCancelFailOnDeleteSnapshot) {
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
                    Return(LIBCURVE_ERROR::OK)));

    FInfo snapInfo;
    snapInfo.seqnum = 100;
    snapInfo.chunksize = 2 * option.chunkSplitSize;
    snapInfo.segmentsize = 2 * snapInfo.chunksize;
    snapInfo.length = 2 * snapInfo.segmentsize;
    snapInfo.ctime = 10;
    EXPECT_CALL(*client_, GetSnapshot(fileName, user, seqNum, _))
        .WillOnce(DoAll(
                    SetArgPointee<3>(snapInfo),
                    Return(LIBCURVE_ERROR::OK)));


    EXPECT_CALL(*metaStore_, UpdateSnapshot(_))
        .WillRepeatedly(Return(kErrCodeSuccess));

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
                    Return(LIBCURVE_ERROR::OK)))
        .WillOnce(DoAll(SetArgPointee<4>(segInfo2),
                    Return(LIBCURVE_ERROR::OK)));

    uint64_t chunkSn = 100;
    ChunkInfoDetail chunkInfo;
    chunkInfo.chunkSn.push_back(chunkSn);
    EXPECT_CALL(*client_, GetChunkInfo(_, _))
        .Times(4)
        .WillRepeatedly(DoAll(SetArgPointee<1>(chunkInfo),
                    Return(LIBCURVE_ERROR::OK)));

    EXPECT_CALL(*dataStore_, PutChunkIndexData(_, _))
        .WillOnce(Return(kErrCodeSuccess));

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
                    Return(kErrCodeSuccess)));

    ChunkIndexData indexData;
    indexData.PutChunkDataName(ChunkDataName(fileName, 1, 0));
    EXPECT_CALL(*dataStore_, GetChunkIndexData(_, _))
        .WillOnce(DoAll(
                    SetArgPointee<1>(indexData),
                    Return(kErrCodeSuccess)));

    EXPECT_CALL(*dataStore_, DataChunkTranferInit(_, _))
        .Times(4)
        .WillRepeatedly(Return(kErrCodeSuccess));


    EXPECT_CALL(*client_, ReadChunkSnapshot(_, _, _, _, _))
        .Times(8)
        .WillRepeatedly(Return(LIBCURVE_ERROR::OK));

    EXPECT_CALL(*dataStore_, DataChunkTranferAddPart(_, _, _, _, _))
        .Times(8)
        .WillRepeatedly(Return(kErrCodeSuccess));


    EXPECT_CALL(*dataStore_, DataChunkTranferComplete(_, _))
        .Times(4)
        .WillRepeatedly(Return(kErrCodeSuccess));

    // 此处捕获task，设置cancel
    EXPECT_CALL(*client_, DeleteSnapshot(fileName, user, seqNum))
        .Times(2)
        .WillOnce(Invoke([task](const std::string &filename,
            const std::string &user,
            uint64_t seq) -> int {
                    task->Cancel();
                    return kErrCodeSuccess;
                        }))
        .WillOnce(Return(LIBCURVE_ERROR::OK));

    EXPECT_CALL(*client_, CheckSnapShotStatus(_, _, _, _))
        .WillRepeatedly(Return(-LIBCURVE_ERROR::NOTEXIST));

    // 进入cancel
    EXPECT_CALL(*dataStore_, ChunkDataExist(_))
        .Times(4)
        .WillRepeatedly(Return(true));

    EXPECT_CALL(*dataStore_, DeleteChunkData(_))
        .Times(4)
        .WillRepeatedly(Return(kErrCodeSuccess));

    EXPECT_CALL(*dataStore_, DeleteChunkIndexData(_))
        .WillOnce(Return(kErrCodeSuccess));

    EXPECT_CALL(*metaStore_, DeleteSnapshot(uuid))
        .WillOnce(Return(kErrCodeInternalError));

    core_->HandleCreateSnapshotTask(task);

    ASSERT_TRUE(task->IsFinish());
}

}  // namespace snapshotcloneserver
}  // namespace curve

