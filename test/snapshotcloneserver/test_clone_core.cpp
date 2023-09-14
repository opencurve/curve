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
 * Created Date: Tue Apr 02 2019
 * Author: xuchaojie
 */

#include <gtest/gtest.h>
#include <gmock/gmock.h>

#include "src/snapshotcloneserver/clone/clone_core.h"
#include "src/snapshotcloneserver/clone/clone_task.h"
#include "src/common/snapshotclone/snapshotclone_define.h"
#include "src/common/location_operator.h"

#include "test/snapshotcloneserver/mock_snapshot_server.h"

using ::curve::common::LocationOperator;

using ::testing::Return;
using ::testing::_;
using ::testing::AnyOf;
using ::testing::AllOf;
using ::testing::SetArgPointee;
using ::testing::Invoke;
using ::testing::DoAll;

namespace curve {
namespace snapshotcloneserver {

static const char* kDefaultPoolset = "poolset";

class TestCloneCoreImpl : public ::testing::Test {
 public:
    TestCloneCoreImpl() {}
    virtual ~TestCloneCoreImpl() {}

    virtual void SetUp() {
        snapshotRef_ =
            std::make_shared<SnapshotReference>();
        cloneRef_ =
            std::make_shared<CloneReference>();
        client_ = std::make_shared<MockCurveFsClient>();
        metaStore_ = std::make_shared<MockSnapshotCloneMetaStore>();
        dataStore_ = std::make_shared<MockSnapshotDataStore>();
        option.cloneTempDir = "/clone";
        option.cloneChunkSplitSize = 1024 * 1024;
        option.mdsRootUser = "root";
        option.createCloneChunkConcurrency = 2;
        option.recoverChunkConcurrency = 2;
        option.clientAsyncMethodRetryTimeSec = 1;
        option.clientAsyncMethodRetryIntervalMs = 500;
        core_ = std::make_shared<CloneCoreImpl>(client_,
            metaStore_,
            dataStore_,
            snapshotRef_,
            cloneRef_,
            option);
        EXPECT_CALL(*client_, Mkdir(_, _))
            .WillOnce(Return(LIBCURVE_ERROR::OK));
        ASSERT_EQ(core_->Init(), 0);
    }

    virtual void TearDown() {
        client_ = nullptr;
        metaStore_ = nullptr;
        dataStore_ = nullptr;
        core_ = nullptr;
        snapshotRef_ = nullptr;
        cloneRef_ = nullptr;
    }

 protected:
    // Auxiliary mock function
    void MockBuildFileInfoFromSnapshotSuccess(
        std::shared_ptr<CloneTaskInfo> task);

    void MockBuildFileInfoFromFileSuccess(
        std::shared_ptr<CloneTaskInfo> task);

    void MockCreateCloneFileSuccess(
        std::shared_ptr<CloneTaskInfo> task);

    void MockCloneMetaSuccess(
        std::shared_ptr<CloneTaskInfo> task);

    void MockCreateCloneChunkSuccess(
        std::shared_ptr<CloneTaskInfo> task);

    void MockCompleteCloneMetaSuccess(
        std::shared_ptr<CloneTaskInfo> task);

    void MockRecoverChunkSuccess(
        std::shared_ptr<CloneTaskInfo> task);

    void MockChangeOwnerSuccess(
        std::shared_ptr<CloneTaskInfo> task);

    void MockRenameCloneFileSuccess(
        std::shared_ptr<CloneTaskInfo> task);

    void MockCompleteCloneFileSuccess(
        std::shared_ptr<CloneTaskInfo> task);

    void MockBuildFileInfoFromSnapshotFail(
        std::shared_ptr<CloneTaskInfo> task);

    void MockBuildFileInfoFromFileFail(
        std::shared_ptr<CloneTaskInfo> task);

    void MockCreateCloneFileFail(
        std::shared_ptr<CloneTaskInfo> task);

    void MockCloneMetaFail(
        std::shared_ptr<CloneTaskInfo> task);

    void MockCreateCloneChunkFail(
        std::shared_ptr<CloneTaskInfo> task);

    void MockCompleteCloneMetaFail(
        std::shared_ptr<CloneTaskInfo> task);

    void MockRecoverChunkFail(
        std::shared_ptr<CloneTaskInfo> task);

    void MockChangeOwnerFail(
        std::shared_ptr<CloneTaskInfo> task);

    void MockRenameCloneFileFail(
        std::shared_ptr<CloneTaskInfo> task);

    void MockCompleteCloneFileFail(
        std::shared_ptr<CloneTaskInfo> task);

 protected:
    std::shared_ptr<CloneCoreImpl> core_;
    std::shared_ptr<MockCurveFsClient> client_;
    std::shared_ptr<MockSnapshotCloneMetaStore> metaStore_;
    std::shared_ptr<MockSnapshotDataStore> dataStore_;
    std::shared_ptr<SnapshotReference> snapshotRef_;
    std::shared_ptr<CloneReference> cloneRef_;
    SnapshotCloneServerOptions option;
};


TEST_F(TestCloneCoreImpl, TestClonePreForSnapSuccess) {
    const UUID &source = "id1";
    const std::string user = "user1";
    const std::string destination = "destination1";
    bool lazyFlag = true;
    CloneInfo cloneInfoOut;

    EXPECT_CALL(*client_, GetFileInfo(destination, option.mdsRootUser, _))
        .WillOnce(Return(-LIBCURVE_ERROR::NOTEXIST));

    SnapshotInfo snap("id1", "user1", "file1", "snap1");
    snap.SetStatus(Status::done);
    EXPECT_CALL(*metaStore_, GetSnapshotInfo(source, _))
        .WillOnce(DoAll(
                SetArgPointee<1>(snap),
                Return(kErrCodeSuccess)));

    EXPECT_CALL(*metaStore_, AddCloneInfo(_))
        .WillOnce(Return(kErrCodeSuccess));

    int ret = core_->CloneOrRecoverPre(
        source, user, destination, lazyFlag,
        CloneTaskType::kClone, kDefaultPoolset, &cloneInfoOut);
    ASSERT_EQ(kErrCodeSuccess, ret);

    ASSERT_EQ(1, core_->GetSnapshotRef()->GetSnapshotRef(source));
    ASSERT_EQ(0, core_->GetCloneRef()->GetRef(source));
}

TEST_F(TestCloneCoreImpl, TestClonePreForSnapTaskExist) {
    const UUID &source = "id1";
    const std::string user = "user1";
    const std::string destination = "destination1";
    bool lazyFlag = true;
    CloneInfo cloneInfoOut;

    std::vector<CloneInfo> cloneInfoList;
    CloneInfo info1("taskid1",
        user,
        CloneTaskType::kClone,
        source,
        destination,
        kDefaultPoolset,
        100,
        101,
        0,
        CloneFileType::kSnapshot,
        lazyFlag,
        CloneStep::kCreateCloneFile,
        CloneStatus::cloning);
    cloneInfoList.push_back(info1);
    EXPECT_CALL(*metaStore_, GetCloneInfoByFileName(destination, _))
        .WillOnce(DoAll(
                SetArgPointee<1>(cloneInfoList),
                Return(kErrCodeSuccess)));

    int ret = core_->CloneOrRecoverPre(
        source, user, destination, lazyFlag,
        CloneTaskType::kClone, kDefaultPoolset, &cloneInfoOut);
    ASSERT_EQ(kErrCodeTaskExist, ret);

    ASSERT_EQ(0, core_->GetSnapshotRef()->GetSnapshotRef(source));
    ASSERT_EQ(0, core_->GetCloneRef()->GetRef(source));
}

TEST_F(TestCloneCoreImpl, TestClonePreForSnapFailOnFileExist) {
    const UUID &source = "id1";
    const std::string user = "user1";
    const std::string destination = "destination1";
    bool lazyFlag = true;
    CloneInfo cloneInfoOut;

    std::vector<CloneInfo> cloneInfoList;
    CloneInfo info1("taskid1",
        user,
        CloneTaskType::kRecover,
        source,
        destination,
        "",
        100,
        101,
        0,
        CloneFileType::kSnapshot,
        lazyFlag,
        CloneStep::kCreateCloneFile,
        CloneStatus::cloning);
    cloneInfoList.push_back(info1);
    EXPECT_CALL(*metaStore_, GetCloneInfoByFileName(destination, _))
        .WillOnce(DoAll(
                SetArgPointee<1>(cloneInfoList),
                Return(kErrCodeSuccess)));

    int ret = core_->CloneOrRecoverPre(
        source, user, destination, lazyFlag,
        CloneTaskType::kClone, kDefaultPoolset, &cloneInfoOut);
    ASSERT_EQ(kErrCodeFileExist, ret);

    ASSERT_EQ(0, core_->GetSnapshotRef()->GetSnapshotRef(source));
    ASSERT_EQ(0, core_->GetCloneRef()->GetRef(source));
}

TEST_F(TestCloneCoreImpl, TestClonePreForFileSuccess) {
    const UUID &source = "fi1e1";
    const std::string user = "user1";
    const std::string destination = "destination1";
    bool lazyFlag = true;
    CloneInfo cloneInfoOut;

    EXPECT_CALL(*client_, GetFileInfo(destination, option.mdsRootUser, _))
        .WillOnce(Return(-LIBCURVE_ERROR::NOTEXIST));

    EXPECT_CALL(*metaStore_, GetSnapshotInfo(source, _))
        .WillOnce(Return(kErrCodeInternalError));

    FInfo fInfo;
    fInfo.id = 100;
    fInfo.chunksize = 1024 * 1024;
    fInfo.segmentsize = 0;
    fInfo.length = 2 * fInfo.segmentsize;
    fInfo.seqnum = 100;
    fInfo.owner = "user1";
    fInfo.filename = "file1";
    fInfo.filestatus = FileStatus::Created;
    fInfo.poolset = kDefaultPoolset;
    EXPECT_CALL(*client_, GetFileInfo(source, option.mdsRootUser, _))
        .WillOnce(DoAll(SetArgPointee<2>(fInfo),
                    Return(LIBCURVE_ERROR::OK)));

    EXPECT_CALL(*metaStore_, AddCloneInfo(_))
        .WillOnce(Return(kErrCodeSuccess));

    int ret = core_->CloneOrRecoverPre(
        source, user, destination, lazyFlag,
        CloneTaskType::kClone, kDefaultPoolset, &cloneInfoOut);
    ASSERT_EQ(kErrCodeSuccess, ret);

    ASSERT_EQ(0, core_->GetSnapshotRef()->GetSnapshotRef(source));
    ASSERT_EQ(1, core_->GetCloneRef()->GetRef(source));
}

TEST_F(TestCloneCoreImpl, TestClonePreForSnapInvalidSnapshot) {
    const UUID &source = "fi1e1";
    const std::string user = "user1";
    const std::string destination = "destination1";
    bool lazyFlag = true;
    CloneInfo cloneInfoOut;

    EXPECT_CALL(*client_, GetFileInfo(destination, option.mdsRootUser, _))
        .WillOnce(Return(-LIBCURVE_ERROR::NOTEXIST));

    SnapshotInfo snap("id1", "user2", "file1", "snap1");
    snap.SetStatus(Status::pending);
    EXPECT_CALL(*metaStore_, GetSnapshotInfo(source, _))
        .WillOnce(DoAll(
                SetArgPointee<1>(snap),
                Return(kErrCodeSuccess)));

    int ret = core_->CloneOrRecoverPre(
        source, user, destination, lazyFlag,
        CloneTaskType::kClone, kDefaultPoolset, &cloneInfoOut);
    ASSERT_EQ(kErrCodeInvalidSnapshot, ret);

    ASSERT_EQ(0, core_->GetSnapshotRef()->GetSnapshotRef(source));
    ASSERT_EQ(0, core_->GetCloneRef()->GetRef(source));
}

TEST_F(TestCloneCoreImpl, TestClonePreForSnapInvalidUser) {
    const UUID &source = "fi1e1";
    const std::string user = "user1";
    const std::string destination = "destination1";
    bool lazyFlag = true;
    CloneInfo cloneInfoOut;
    EXPECT_CALL(*client_, GetFileInfo(destination, option.mdsRootUser, _))
        .WillOnce(Return(-LIBCURVE_ERROR::NOTEXIST));

    SnapshotInfo snap("id1", "user2", "file1", "snap1");
    snap.SetStatus(Status::done);
    EXPECT_CALL(*metaStore_, GetSnapshotInfo(source, _))
        .WillOnce(DoAll(
                SetArgPointee<1>(snap),
                Return(kErrCodeSuccess)));

    int ret = core_->CloneOrRecoverPre(
        source, user, destination, lazyFlag,
        CloneTaskType::kClone, kDefaultPoolset, &cloneInfoOut);
    ASSERT_EQ(kErrCodeInvalidUser, ret);
    ASSERT_EQ(0, core_->GetSnapshotRef()->GetSnapshotRef(source));
    ASSERT_EQ(0, core_->GetCloneRef()->GetRef(source));
}

TEST_F(TestCloneCoreImpl, TestClonePreAddCloneInfoFail) {
    const UUID &source = "id1";
    const std::string user = "user1";
    const std::string destination = "destination1";
    bool lazyFlag = true;
    CloneInfo cloneInfoOut;
    EXPECT_CALL(*client_, GetFileInfo(destination, option.mdsRootUser, _))
        .WillOnce(Return(-LIBCURVE_ERROR::NOTEXIST));

    SnapshotInfo snap("id1", "user1", "file1", "snap1");
    snap.SetStatus(Status::done);
    EXPECT_CALL(*metaStore_, GetSnapshotInfo(source, _))
        .WillOnce(DoAll(
                SetArgPointee<1>(snap),
                Return(kErrCodeSuccess)));

    EXPECT_CALL(*metaStore_, AddCloneInfo(_))
        .WillOnce(Return(kErrCodeInternalError));

    int ret = core_->CloneOrRecoverPre(
        source, user, destination, lazyFlag,
        CloneTaskType::kClone, kDefaultPoolset, &cloneInfoOut);
    ASSERT_EQ(kErrCodeInternalError, ret);
    ASSERT_EQ(0, core_->GetSnapshotRef()->GetSnapshotRef(source));
    ASSERT_EQ(0, core_->GetCloneRef()->GetRef(source));
}

TEST_F(TestCloneCoreImpl, TestClonePreForFileNotExist) {
    const UUID &source = "fi1e1";
    const std::string user = "user1";
    const std::string destination = "destination1";
    bool lazyFlag = true;
    CloneInfo cloneInfoOut;
    EXPECT_CALL(*client_, GetFileInfo(destination, option.mdsRootUser, _))
        .WillOnce(Return(-LIBCURVE_ERROR::NOTEXIST));

    EXPECT_CALL(*metaStore_, GetSnapshotInfo(source, _))
        .WillOnce(Return(kErrCodeInternalError));

    EXPECT_CALL(*client_, GetFileInfo(source, option.mdsRootUser, _))
        .WillOnce(Return(-LIBCURVE_ERROR::NOTEXIST));

    int ret = core_->CloneOrRecoverPre(
        source, user, destination, lazyFlag,
        CloneTaskType::kClone, kDefaultPoolset, &cloneInfoOut);
    ASSERT_EQ(kErrCodeFileNotExist, ret);
    ASSERT_EQ(0, core_->GetSnapshotRef()->GetSnapshotRef(source));
    ASSERT_EQ(0, core_->GetCloneRef()->GetRef(source));
}

TEST_F(TestCloneCoreImpl, TestClonePreForFileFail) {
    const UUID &source = "fi1e1";
    const std::string user = "user1";
    const std::string destination = "destination1";
    bool lazyFlag = true;
    CloneInfo cloneInfoOut;
    EXPECT_CALL(*client_, GetFileInfo(destination, option.mdsRootUser, _))
        .WillOnce(Return(-LIBCURVE_ERROR::NOTEXIST));

    EXPECT_CALL(*metaStore_, GetSnapshotInfo(source, _))
        .WillOnce(Return(kErrCodeInternalError));

    EXPECT_CALL(*client_, GetFileInfo(source, option.mdsRootUser, _))
        .WillOnce(Return(-LIBCURVE_ERROR::FAILED));

    int ret = core_->CloneOrRecoverPre(
        source, user, destination, lazyFlag,
        CloneTaskType::kClone, kDefaultPoolset, &cloneInfoOut);
    ASSERT_EQ(kErrCodeInternalError, ret);
    ASSERT_EQ(0, core_->GetSnapshotRef()->GetSnapshotRef(source));
    ASSERT_EQ(0, core_->GetCloneRef()->GetRef(source));
}

TEST_F(TestCloneCoreImpl, TestClonePreDestinationExist) {
    const UUID &source = "fi1e1";
    const std::string user = "user1";
    const std::string destination = "destination1";
    bool lazyFlag = true;
    CloneInfo cloneInfoOut;

    EXPECT_CALL(*client_, GetFileInfo(destination, option.mdsRootUser, _))
        .WillOnce(Return(LIBCURVE_ERROR::OK));

    int ret = core_->CloneOrRecoverPre(
        source, user, destination, lazyFlag,
        CloneTaskType::kClone, kDefaultPoolset, &cloneInfoOut);
    ASSERT_EQ(kErrCodeFileExist, ret);
    ASSERT_EQ(0, core_->GetSnapshotRef()->GetSnapshotRef(source));
    ASSERT_EQ(0, core_->GetCloneRef()->GetRef(source));
}

TEST_F(TestCloneCoreImpl, TestClonePreDestinationAndTaskExist) {
    const UUID &source = "fi1e1";
    const std::string user = "user1";
    const std::string destination = "destination1";
    bool lazyFlag = true;
    CloneInfo cloneInfoOut;
    uint64_t destId = 10086;

    std::vector<CloneInfo> cloneInfoList;
    CloneInfo info1("taskid1",
        user,
        CloneTaskType::kClone,
        source,
        destination,
        kDefaultPoolset,
        100,
        destId,
        0,
        CloneFileType::kFile,
        lazyFlag,
        CloneStep::kRecoverChunk,
        CloneStatus::metaInstalled);
    cloneInfoList.push_back(info1);
    EXPECT_CALL(*metaStore_, GetCloneInfoByFileName(destination, _))
        .WillOnce(DoAll(
                SetArgPointee<1>(cloneInfoList),
                Return(kErrCodeSuccess)));

    FInfo fInfo;
    fInfo.id = destId;
    EXPECT_CALL(*client_, GetFileInfo(destination, option.mdsRootUser, _))
        .WillOnce(DoAll(
                SetArgPointee<2>(fInfo),
                Return(LIBCURVE_ERROR::OK)));

    int ret = core_->CloneOrRecoverPre(
        source, user, destination, lazyFlag,
        CloneTaskType::kClone, kDefaultPoolset, &cloneInfoOut);
    ASSERT_EQ(kErrCodeTaskExist, ret);
    ASSERT_EQ(0, core_->GetSnapshotRef()->GetSnapshotRef(source));
    ASSERT_EQ(0, core_->GetCloneRef()->GetRef(source));
}

TEST_F(TestCloneCoreImpl, TestClonePreDestinationExistButInodeidNotEqual) {
    const UUID &source = "fi1e1";
    const std::string user = "user1";
    const std::string destination = "destination1";
    bool lazyFlag = true;
    CloneInfo cloneInfoOut;
    uint64_t destId = 10086;

    std::vector<CloneInfo> cloneInfoList;
    CloneInfo info1("taskid1",
        user,
        CloneTaskType::kClone,
        source,
        destination,
        kDefaultPoolset,
        100,
        destId,
        0,
        CloneFileType::kFile,
        lazyFlag,
        CloneStep::kRecoverChunk,
        CloneStatus::metaInstalled);
    cloneInfoList.push_back(info1);
    EXPECT_CALL(*metaStore_, GetCloneInfoByFileName(destination, _))
        .WillOnce(DoAll(
                SetArgPointee<1>(cloneInfoList),
                Return(kErrCodeSuccess)));

    FInfo fInfo;
    fInfo.id = destId + 1;
    EXPECT_CALL(*client_, GetFileInfo(destination, option.mdsRootUser, _))
        .WillOnce(DoAll(
                SetArgPointee<2>(fInfo),
                Return(LIBCURVE_ERROR::OK)));

    int ret = core_->CloneOrRecoverPre(
        source, user, destination, lazyFlag,
        CloneTaskType::kClone, kDefaultPoolset, &cloneInfoOut);
    ASSERT_EQ(kErrCodeFileExist, ret);
    ASSERT_EQ(0, core_->GetSnapshotRef()->GetSnapshotRef(source));
    ASSERT_EQ(0, core_->GetCloneRef()->GetRef(source));
}

TEST_F(TestCloneCoreImpl, TestRecoverPreDestinationNotExist) {
    const UUID &source = "fi1e1";
    const std::string user = "user1";
    const std::string destination = "destination1";
    bool lazyFlag = true;
    CloneInfo cloneInfoOut;

    EXPECT_CALL(*client_, GetFileInfo(destination, option.mdsRootUser, _))
        .WillOnce(Return(-LIBCURVE_ERROR::NOTEXIST));

    int ret = core_->CloneOrRecoverPre(
        source, user, destination, lazyFlag,
        CloneTaskType::kRecover, "", &cloneInfoOut);
    ASSERT_EQ(kErrCodeFileNotExist, ret);
    ASSERT_EQ(0, core_->GetSnapshotRef()->GetSnapshotRef(source));
    ASSERT_EQ(0, core_->GetCloneRef()->GetRef(source));
}

TEST_F(TestCloneCoreImpl, TestRecoverPreForSnapSuccess) {
    const UUID &source = "id1";
    const std::string user = "user1";
    const std::string destination = "destination1";
    bool lazyFlag = true;
    CloneInfo cloneInfoOut;

    EXPECT_CALL(*client_, GetFileInfo(destination, option.mdsRootUser, _))
        .WillOnce(Return(LIBCURVE_ERROR::OK));

    SnapshotInfo snap("id1", "user1", destination, "snap1");
    snap.SetStatus(Status::done);
    EXPECT_CALL(*metaStore_, GetSnapshotInfo(source, _))
        .WillOnce(DoAll(
                SetArgPointee<1>(snap),
                Return(kErrCodeSuccess)));

    EXPECT_CALL(*metaStore_, AddCloneInfo(_))
        .WillOnce(Return(kErrCodeSuccess));

    int ret = core_->CloneOrRecoverPre(
        source, user, destination, lazyFlag,
        CloneTaskType::kRecover, "", &cloneInfoOut);
    ASSERT_EQ(kErrCodeSuccess, ret);
    ASSERT_EQ(1, core_->GetSnapshotRef()->GetSnapshotRef(source));
    ASSERT_EQ(0, core_->GetCloneRef()->GetRef(source));
}

TEST_F(TestCloneCoreImpl, TestRecoverPreForSnapDestNotMatch) {
    const UUID &source = "id1";
    const std::string user = "user1";
    const std::string destination = "destination1";
    bool lazyFlag = true;
    CloneInfo cloneInfoOut;

    EXPECT_CALL(*client_, GetFileInfo(destination, option.mdsRootUser, _))
        .WillOnce(Return(LIBCURVE_ERROR::OK));

    SnapshotInfo snap("id1", "user1", "file1", "snap1");
    snap.SetStatus(Status::done);
    EXPECT_CALL(*metaStore_, GetSnapshotInfo(source, _))
        .WillOnce(DoAll(
                SetArgPointee<1>(snap),
                Return(kErrCodeSuccess)));

    int ret = core_->CloneOrRecoverPre(
        source, user, destination, lazyFlag,
        CloneTaskType::kRecover, "", &cloneInfoOut);
    ASSERT_EQ(kErrCodeInvalidSnapshot, ret);
    ASSERT_EQ(0, core_->GetSnapshotRef()->GetSnapshotRef(source));
    ASSERT_EQ(0, core_->GetCloneRef()->GetRef(source));
}

TEST_F(TestCloneCoreImpl, TestClonePreDestinationFileInternalError) {
    const UUID &source = "fi1e1";
    const std::string user = "user1";
    const std::string destination = "destination1";
    bool lazyFlag = true;
    CloneInfo cloneInfoOut;

    EXPECT_CALL(*client_, GetFileInfo(destination, option.mdsRootUser, _))
        .WillOnce(Return(-LIBCURVE_ERROR::UNKNOWN));

    int ret = core_->CloneOrRecoverPre(
        source, user, destination, lazyFlag,
        CloneTaskType::kClone, kDefaultPoolset, &cloneInfoOut);
    ASSERT_EQ(kErrCodeInternalError, ret);
    ASSERT_EQ(0, core_->GetSnapshotRef()->GetSnapshotRef(source));
    ASSERT_EQ(0, core_->GetCloneRef()->GetRef(source));
}

TEST_F(TestCloneCoreImpl, TestClonePreForFileSourceFileStatusInvalid) {
    const UUID &source = "fi1e1";
    const std::string user = "user1";
    const std::string destination = "destination1";
    bool lazyFlag = true;
    CloneInfo cloneInfoOut;
    EXPECT_CALL(*client_, GetFileInfo(destination, option.mdsRootUser, _))
        .WillOnce(Return(-LIBCURVE_ERROR::NOTEXIST));

    EXPECT_CALL(*metaStore_, GetSnapshotInfo(source, _))
        .WillOnce(Return(kErrCodeInternalError));

    FInfo fInfo;
    fInfo.filestatus = FileStatus::Deleting;
    EXPECT_CALL(*client_, GetFileInfo(source, option.mdsRootUser, _))
        .WillOnce(DoAll(SetArgPointee<2>(fInfo), Return(LIBCURVE_ERROR::OK)));

    int ret = core_->CloneOrRecoverPre(
        source, user, destination, lazyFlag,
        CloneTaskType::kClone, kDefaultPoolset, &cloneInfoOut);
    ASSERT_EQ(kErrCodeFileStatusInvalid, ret);
    ASSERT_EQ(0, core_->GetSnapshotRef()->GetSnapshotRef(source));
    ASSERT_EQ(0, core_->GetCloneRef()->GetRef(source));
}

TEST_F(TestCloneCoreImpl, TestClonePreForFileSetCloneFileStatusReturnNotExist) {
    const UUID &source = "fi1e1";
    const std::string user = "user1";
    const std::string destination = "destination1";
    bool lazyFlag = true;
    CloneInfo cloneInfoOut;

    EXPECT_CALL(*client_, GetFileInfo(destination, option.mdsRootUser, _))
        .WillOnce(Return(-LIBCURVE_ERROR::NOTEXIST));

    EXPECT_CALL(*metaStore_, GetSnapshotInfo(source, _))
        .WillOnce(Return(kErrCodeInternalError));

    FInfo fInfo;
    fInfo.id = 100;
    fInfo.chunksize = 1024 * 1024;
    fInfo.segmentsize = 0;
    fInfo.length = 2 * fInfo.segmentsize;
    fInfo.seqnum = 100;
    fInfo.owner = "user1";
    fInfo.filename = "file1";
    fInfo.filestatus = FileStatus::Created;
    EXPECT_CALL(*client_, GetFileInfo(source, option.mdsRootUser, _))
        .WillOnce(DoAll(SetArgPointee<2>(fInfo),
                    Return(LIBCURVE_ERROR::OK)));

    EXPECT_CALL(*metaStore_, AddCloneInfo(_))
        .WillOnce(Return(kErrCodeSuccess));

    EXPECT_CALL(*client_, SetCloneFileStatus(source,
        FileStatus::BeingCloned,
        option.mdsRootUser))
        .WillOnce(Return(-LIBCURVE_ERROR::NOTEXIST));

    int ret = core_->CloneOrRecoverPre(
        source, user, destination, lazyFlag,
        CloneTaskType::kClone, kDefaultPoolset, &cloneInfoOut);
    ASSERT_EQ(kErrCodeSuccess, ret);
    ASSERT_EQ(0, core_->GetSnapshotRef()->GetSnapshotRef(source));
    ASSERT_EQ(1, core_->GetCloneRef()->GetRef(source));
}

TEST_F(TestCloneCoreImpl,
    HandleCloneOrRecoverTaskStage1SuccessForCloneBySnapshot) {
    CloneInfo info("id1", "user1", CloneTaskType::kClone,
    "snapid1", "file1", kDefaultPoolset, CloneFileType::kSnapshot, true);
    info.SetStatus(CloneStatus::cloning);
    auto cloneMetric = std::make_shared<CloneInfoMetric>("id1");
    auto cloneClosure = std::make_shared<CloneClosure>();
    std::shared_ptr<CloneTaskInfo> task =
        std::make_shared<CloneTaskInfo>(info, cloneMetric, cloneClosure);

    EXPECT_CALL(*metaStore_, UpdateCloneInfo(_))
        .WillRepeatedly(Return(kErrCodeSuccess));

    MockBuildFileInfoFromSnapshotSuccess(task);
    MockCreateCloneFileSuccess(task);
    MockCloneMetaSuccess(task);
    MockCreateCloneChunkSuccess(task);
    MockCompleteCloneMetaSuccess(task);
    MockChangeOwnerSuccess(task);
    MockRenameCloneFileSuccess(task);
    core_->HandleCloneOrRecoverTask(task);
}

TEST_F(TestCloneCoreImpl,
    HandleCloneOrRecoverTaskStage2SuccessForCloneBySnapshot) {
    CloneInfo info("id1", "user1", CloneTaskType::kClone, "snapid1", "file1",
                   kDefaultPoolset, 1, 2, 100, CloneFileType::kSnapshot, true,
                   CloneStep::kRecoverChunk, CloneStatus::cloning);
    info.SetStatus(CloneStatus::cloning);
    auto cloneMetric = std::make_shared<CloneInfoMetric>("id1");
    auto cloneClosure = std::make_shared<CloneClosure>();
    std::shared_ptr<CloneTaskInfo> task =
        std::make_shared<CloneTaskInfo>(info, cloneMetric, cloneClosure);

    EXPECT_CALL(*metaStore_, UpdateCloneInfo(_))
        .WillRepeatedly(Return(kErrCodeSuccess));

    MockBuildFileInfoFromSnapshotSuccess(task);
    MockCloneMetaSuccess(task);
    MockRecoverChunkSuccess(task);
    MockCompleteCloneFileSuccess(task);
    core_->HandleCloneOrRecoverTask(task);
}

TEST_F(TestCloneCoreImpl,
    HandleCloneOrRecoverTaskSuccessForCloneBySnapshotNotLazy) {
    CloneInfo info("id1", "user1", CloneTaskType::kClone, "snapid1", "file1",
                   kDefaultPoolset, CloneFileType::kSnapshot, false);
    info.SetStatus(CloneStatus::cloning);
    auto cloneMetric = std::make_shared<CloneInfoMetric>("id1");
    auto cloneClosure = std::make_shared<CloneClosure>();
    std::shared_ptr<CloneTaskInfo> task =
        std::make_shared<CloneTaskInfo>(info, cloneMetric, cloneClosure);

    EXPECT_CALL(*metaStore_, UpdateCloneInfo(_))
        .WillRepeatedly(Return(kErrCodeSuccess));

    MockBuildFileInfoFromSnapshotSuccess(task);
    MockCreateCloneFileSuccess(task);
    MockCloneMetaSuccess(task);
    MockCreateCloneChunkSuccess(task);
    MockCompleteCloneMetaSuccess(task);
    MockRecoverChunkSuccess(task);
    MockCompleteCloneFileSuccess(task);
    MockChangeOwnerSuccess(task);
    MockRenameCloneFileSuccess(task);

    core_->HandleCloneOrRecoverTask(task);
}

TEST_F(TestCloneCoreImpl,
    HandleCloneOrRecoverTaskFailOnBuildFileInfoFromSnapshot) {
    CloneInfo info("id1", "user1", CloneTaskType::kClone,
    "snapid1", "file1", kDefaultPoolset, CloneFileType::kSnapshot, true);
    info.SetStatus(CloneStatus::cloning);
    auto cloneMetric = std::make_shared<CloneInfoMetric>("id1");
    auto cloneClosure = std::make_shared<CloneClosure>();
    std::shared_ptr<CloneTaskInfo> task =
        std::make_shared<CloneTaskInfo>(info, cloneMetric, cloneClosure);

    EXPECT_CALL(*metaStore_, UpdateCloneInfo(_))
        .WillRepeatedly(Return(kErrCodeSuccess));

    MockBuildFileInfoFromSnapshotFail(task);

    core_->HandleCloneOrRecoverTask(task);
}

TEST_F(TestCloneCoreImpl,
    HandleCloneOrRecoverTaskFailOnGetSnapshotInfo) {
    CloneInfo cinfo("id1", "user1", CloneTaskType::kClone,
    "snapid1", "file1", kDefaultPoolset, CloneFileType::kSnapshot, true);
    cinfo.SetStatus(CloneStatus::cloning);
    auto cloneMetric = std::make_shared<CloneInfoMetric>("id1");
    auto cloneClosure = std::make_shared<CloneClosure>();
    std::shared_ptr<CloneTaskInfo> task =
        std::make_shared<CloneTaskInfo>(cinfo, cloneMetric, cloneClosure);

    EXPECT_CALL(*metaStore_, UpdateCloneInfo(_))
        .WillRepeatedly(Return(kErrCodeSuccess));

    UUID uuid = "uuid1";
    const std::string user = "user1";
    const std::string fileName = "file1";
    const std::string desc = "snap1";
    uint64_t seqnum = 100;
    uint32_t chunksize = 1024 * 1024;
    uint64_t segmentsize = 2 * chunksize;
    uint64_t filelength = 2 * segmentsize;
    uint64_t time = 100;
    Status status = Status::done;
    SnapshotInfo info(uuid, user, fileName, desc,
        seqnum, chunksize, segmentsize, filelength, 0, 0, "default",
        time, status);

    EXPECT_CALL(*metaStore_, GetSnapshotInfo(_, _))
        .WillRepeatedly(DoAll(
                SetArgPointee<1>(info),
                Return(kErrCodeInternalError)));

    core_->HandleCloneOrRecoverTask(task);
}

TEST_F(TestCloneCoreImpl,
    HandleCloneOrRecoverTaskStage1SuccessForRecoverBySnapshot) {
    CloneInfo info("id1", "user1", CloneTaskType::kRecover,
    "snapid1", "file1", kDefaultPoolset, CloneFileType::kSnapshot, true);
    info.SetStatus(CloneStatus::recovering);
    auto cloneMetric = std::make_shared<CloneInfoMetric>("id1");
    auto cloneClosure = std::make_shared<CloneClosure>();
    std::shared_ptr<CloneTaskInfo> task =
        std::make_shared<CloneTaskInfo>(info, cloneMetric, cloneClosure);

    EXPECT_CALL(*metaStore_, UpdateCloneInfo(_))
        .WillRepeatedly(Return(kErrCodeSuccess));

    MockBuildFileInfoFromSnapshotSuccess(task);
    MockCreateCloneFileSuccess(task);
    MockCloneMetaSuccess(task);
    MockCreateCloneChunkSuccess(task);
    MockCompleteCloneMetaSuccess(task);
    MockRenameCloneFileSuccess(task);
    core_->HandleCloneOrRecoverTask(task);
}

TEST_F(TestCloneCoreImpl,
    HandleCloneOrRecoverTaskStage2SuccessForRecoverBySnapshot) {
    CloneInfo info("id1", "user1", CloneTaskType::kRecover, "snapid1", "file1",
                   kDefaultPoolset, 1, 2, 100, CloneFileType::kSnapshot, true,
                   CloneStep::kRecoverChunk, CloneStatus::recovering);
    info.SetStatus(CloneStatus::recovering);
    auto cloneMetric = std::make_shared<CloneInfoMetric>("id1");
    auto cloneClosure = std::make_shared<CloneClosure>();
    std::shared_ptr<CloneTaskInfo> task =
        std::make_shared<CloneTaskInfo>(info, cloneMetric, cloneClosure);

    EXPECT_CALL(*metaStore_, UpdateCloneInfo(_))
        .WillRepeatedly(Return(kErrCodeSuccess));

    MockBuildFileInfoFromSnapshotSuccess(task);
    MockCloneMetaSuccess(task);
    MockRecoverChunkSuccess(task);
    MockCompleteCloneFileSuccess(task);
    core_->HandleCloneOrRecoverTask(task);
}

TEST_F(TestCloneCoreImpl,
    HandleCloneOrRecoverTaskFailOnCreateCloneFile) {
    CloneInfo info("id1", "user1", CloneTaskType::kClone,
    "snapid1", "file1", kDefaultPoolset, CloneFileType::kSnapshot, true);
    info.SetStatus(CloneStatus::cloning);
    auto cloneMetric = std::make_shared<CloneInfoMetric>("id1");
    auto cloneClosure = std::make_shared<CloneClosure>();
    std::shared_ptr<CloneTaskInfo> task =
        std::make_shared<CloneTaskInfo>(info, cloneMetric, cloneClosure);

    EXPECT_CALL(*metaStore_, UpdateCloneInfo(_))
        .WillRepeatedly(Return(kErrCodeSuccess));

    MockBuildFileInfoFromSnapshotSuccess(task);
    MockCreateCloneFileFail(task);

    core_->HandleCloneOrRecoverTask(task);
}

TEST_F(TestCloneCoreImpl, HandleCloneOrRecoverTaskFailOnCloneMeta) {
    CloneInfo info("id1", "user1", CloneTaskType::kClone,
    "snapid1", "file1", kDefaultPoolset, CloneFileType::kSnapshot, true);
    info.SetStatus(CloneStatus::cloning);
    auto cloneMetric = std::make_shared<CloneInfoMetric>("id1");
    auto cloneClosure = std::make_shared<CloneClosure>();
    std::shared_ptr<CloneTaskInfo> task =
        std::make_shared<CloneTaskInfo>(info, cloneMetric, cloneClosure);

    EXPECT_CALL(*metaStore_, UpdateCloneInfo(_))
        .WillRepeatedly(Return(kErrCodeSuccess));

    MockBuildFileInfoFromSnapshotSuccess(task);
    MockCreateCloneFileSuccess(task);
    MockCloneMetaFail(task);

    core_->HandleCloneOrRecoverTask(task);
}

TEST_F(TestCloneCoreImpl, HandleCloneOrRecoverTaskFailOnCreateCloneChunk) {
    CloneInfo info("id1", "user1", CloneTaskType::kClone,
    "snapid1", "file1", kDefaultPoolset, CloneFileType::kSnapshot, true);
    info.SetStatus(CloneStatus::cloning);
    auto cloneMetric = std::make_shared<CloneInfoMetric>("id1");
    auto cloneClosure = std::make_shared<CloneClosure>();
    std::shared_ptr<CloneTaskInfo> task =
        std::make_shared<CloneTaskInfo>(info, cloneMetric, cloneClosure);

    EXPECT_CALL(*metaStore_, UpdateCloneInfo(_))
        .WillRepeatedly(Return(kErrCodeSuccess));

    MockBuildFileInfoFromSnapshotSuccess(task);
    MockCreateCloneFileSuccess(task);
    MockCloneMetaSuccess(task);
    MockCreateCloneChunkFail(task);

    core_->HandleCloneOrRecoverTask(task);
}

TEST_F(TestCloneCoreImpl, HandleCloneOrRecoverTaskFailOnCompleteCloneMeta) {
    CloneInfo info("id1", "user1", CloneTaskType::kClone,
    "snapid1", "file1", kDefaultPoolset, CloneFileType::kSnapshot, true);
    info.SetStatus(CloneStatus::cloning);
    auto cloneMetric = std::make_shared<CloneInfoMetric>("id1");
    auto cloneClosure = std::make_shared<CloneClosure>();
    std::shared_ptr<CloneTaskInfo> task =
        std::make_shared<CloneTaskInfo>(info, cloneMetric, cloneClosure);

    EXPECT_CALL(*metaStore_, UpdateCloneInfo(_))
        .WillRepeatedly(Return(kErrCodeSuccess));

    MockBuildFileInfoFromSnapshotSuccess(task);
    MockCreateCloneFileSuccess(task);
    MockCloneMetaSuccess(task);
    MockCreateCloneChunkSuccess(task);
    MockCompleteCloneMetaFail(task);

    core_->HandleCloneOrRecoverTask(task);
}

TEST_F(TestCloneCoreImpl, HandleCloneOrRecoverTaskFailOnChangeOwner) {
    CloneInfo info("id1", "user1", CloneTaskType::kClone,
    "snapid1", "file1", kDefaultPoolset, CloneFileType::kSnapshot, true);
    info.SetStatus(CloneStatus::cloning);
    auto cloneMetric = std::make_shared<CloneInfoMetric>("id1");
    auto cloneClosure = std::make_shared<CloneClosure>();
    std::shared_ptr<CloneTaskInfo> task =
        std::make_shared<CloneTaskInfo>(info, cloneMetric, cloneClosure);

    EXPECT_CALL(*metaStore_, UpdateCloneInfo(_))
        .WillRepeatedly(Return(kErrCodeSuccess));

    MockBuildFileInfoFromSnapshotSuccess(task);
    MockCreateCloneFileSuccess(task);
    MockCloneMetaSuccess(task);
    MockCreateCloneChunkSuccess(task);
    MockCompleteCloneMetaSuccess(task);
    MockChangeOwnerFail(task);

    core_->HandleCloneOrRecoverTask(task);
}

TEST_F(TestCloneCoreImpl, HandleCloneOrRecoverTaskFileOnRenameCloneFile) {
    CloneInfo info("id1", "user1", CloneTaskType::kClone,
    "snapid1", "file1", kDefaultPoolset, CloneFileType::kSnapshot, true);
    info.SetStatus(CloneStatus::cloning);
    auto cloneMetric = std::make_shared<CloneInfoMetric>("id1");
    auto cloneClosure = std::make_shared<CloneClosure>();
    std::shared_ptr<CloneTaskInfo> task =
        std::make_shared<CloneTaskInfo>(info, cloneMetric, cloneClosure);

    EXPECT_CALL(*metaStore_, UpdateCloneInfo(_))
        .WillRepeatedly(Return(kErrCodeSuccess));

    MockBuildFileInfoFromSnapshotSuccess(task);
    MockCreateCloneFileSuccess(task);
    MockCloneMetaSuccess(task);
    MockCreateCloneChunkSuccess(task);
    MockCompleteCloneMetaSuccess(task);
    MockRenameCloneFileFail(task);

    core_->HandleCloneOrRecoverTask(task);
}

TEST_F(TestCloneCoreImpl, HandleCloneOrRecoverTaskFileOnRecoverChunk) {
    CloneInfo info("id1", "user1", CloneTaskType::kClone, "snapid1", "file1",
                   kDefaultPoolset, 1, 2, 100, CloneFileType::kSnapshot, true,
                   CloneStep::kRecoverChunk, CloneStatus::cloning);
    info.SetStatus(CloneStatus::cloning);
    auto cloneMetric = std::make_shared<CloneInfoMetric>("id1");
    auto cloneClosure = std::make_shared<CloneClosure>();
    std::shared_ptr<CloneTaskInfo> task =
        std::make_shared<CloneTaskInfo>(info, cloneMetric, cloneClosure);

    EXPECT_CALL(*metaStore_, UpdateCloneInfo(_))
        .WillRepeatedly(Return(kErrCodeSuccess));

    MockBuildFileInfoFromSnapshotSuccess(task);
    MockCloneMetaSuccess(task);
    MockRecoverChunkFail(task);

    core_->HandleCloneOrRecoverTask(task);
}

TEST_F(TestCloneCoreImpl, HandleCloneOrRecoverTaskFailOnCompleteCloneFail) {
    CloneInfo info("id1", "user1", CloneTaskType::kClone, "snapid1", "file1",
                   kDefaultPoolset, 1, 2, 100, CloneFileType::kSnapshot, true,
                   CloneStep::kRecoverChunk, CloneStatus::cloning);
    info.SetStatus(CloneStatus::cloning);
    auto cloneMetric = std::make_shared<CloneInfoMetric>("id1");
    auto cloneClosure = std::make_shared<CloneClosure>();
    std::shared_ptr<CloneTaskInfo> task =
        std::make_shared<CloneTaskInfo>(info, cloneMetric, cloneClosure);

    EXPECT_CALL(*metaStore_, UpdateCloneInfo(_))
        .WillRepeatedly(Return(kErrCodeSuccess));

    MockBuildFileInfoFromSnapshotSuccess(task);
    MockCloneMetaSuccess(task);
    MockRecoverChunkSuccess(task);
    MockCompleteCloneFileFail(task);

    core_->HandleCloneOrRecoverTask(task);
}

TEST_F(TestCloneCoreImpl,
    HandleCloneOrRecoverTaskStage1SuccessForCloneByFile) {
    CloneInfo info("id1", "user1", CloneTaskType::kClone, "snapid1", "file1",
                   kDefaultPoolset, CloneFileType::kFile, true);
    info.SetStatus(CloneStatus::cloning);
    auto cloneMetric = std::make_shared<CloneInfoMetric>("id1");
    auto cloneClosure = std::make_shared<CloneClosure>();
    std::shared_ptr<CloneTaskInfo> task =
        std::make_shared<CloneTaskInfo>(info, cloneMetric, cloneClosure);

    EXPECT_CALL(*metaStore_, UpdateCloneInfo(_))
        .WillRepeatedly(Return(kErrCodeSuccess));

    MockBuildFileInfoFromFileSuccess(task);
    MockCreateCloneFileSuccess(task);
    MockCloneMetaSuccess(task);
    MockCreateCloneChunkSuccess(task);
    MockCompleteCloneMetaSuccess(task);
    MockRenameCloneFileSuccess(task);

    core_->HandleCloneOrRecoverTask(task);
}

TEST_F(TestCloneCoreImpl,
    HandleCloneOrRecoverTaskStage2SuccessForCloneByFile) {
    CloneInfo info("id1", "user1", CloneTaskType::kClone,
    "snapid1", "file1", kDefaultPoolset, 1, 2, 100, CloneFileType::kFile, true,
    CloneStep::kRecoverChunk, CloneStatus::cloning);
    info.SetStatus(CloneStatus::cloning);
    auto cloneMetric = std::make_shared<CloneInfoMetric>("id1");
    auto cloneClosure = std::make_shared<CloneClosure>();
    std::shared_ptr<CloneTaskInfo> task =
        std::make_shared<CloneTaskInfo>(info, cloneMetric, cloneClosure);

    EXPECT_CALL(*metaStore_, UpdateCloneInfo(_))
        .WillRepeatedly(Return(kErrCodeSuccess));

    MockBuildFileInfoFromFileSuccess(task);
    MockRecoverChunkSuccess(task);
    MockCompleteCloneFileSuccess(task);

    core_->HandleCloneOrRecoverTask(task);
}

TEST_F(TestCloneCoreImpl,
    HandleCloneOrRecoverTaskForCloneByFileFailOnBuildFileInfoFromFile) {
    CloneInfo info("id1", "user1", CloneTaskType::kClone,
    "snapid1", "file1", kDefaultPoolset, CloneFileType::kFile, true);
    info.SetStatus(CloneStatus::cloning);
    auto cloneMetric = std::make_shared<CloneInfoMetric>("id1");
    auto cloneClosure = std::make_shared<CloneClosure>();
    std::shared_ptr<CloneTaskInfo> task =
        std::make_shared<CloneTaskInfo>(info, cloneMetric, cloneClosure);

    EXPECT_CALL(*metaStore_, UpdateCloneInfo(_))
        .WillRepeatedly(Return(kErrCodeSuccess));

    MockBuildFileInfoFromFileFail(task);

    core_->HandleCloneOrRecoverTask(task);
}

TEST_F(TestCloneCoreImpl,
    HandleCloneOrRecoverTaskForCloneByFileFailOnInvalidSegmentSize) {
    CloneInfo info("id1", "user1", CloneTaskType::kClone,
    "snapid1", "file1", kDefaultPoolset, CloneFileType::kFile, true);
    info.SetStatus(CloneStatus::cloning);
    auto cloneMetric = std::make_shared<CloneInfoMetric>("id1");
    auto cloneClosure = std::make_shared<CloneClosure>();
    std::shared_ptr<CloneTaskInfo> task =
        std::make_shared<CloneTaskInfo>(info, cloneMetric, cloneClosure);

    EXPECT_CALL(*metaStore_, UpdateCloneInfo(_))
        .WillRepeatedly(Return(kErrCodeSuccess));

    FInfo fInfo;
    fInfo.id = 100;
    fInfo.chunksize = 1024 * 1024;
    fInfo.segmentsize = 0;
    fInfo.length = 2 * fInfo.segmentsize;
    fInfo.seqnum = 100;
    fInfo.owner = "user1";
    fInfo.filename = "file1";
    EXPECT_CALL(*client_, GetFileInfo(_, _, _))
        .WillRepeatedly(DoAll(
                SetArgPointee<2>(fInfo),
                Return(LIBCURVE_ERROR::OK)));

    core_->HandleCloneOrRecoverTask(task);
}

TEST_F(TestCloneCoreImpl,
    HandleCloneOrRecoverTaskForCloneByFileFailOnInvalidFileLen) {
    CloneInfo info("id1", "user1", CloneTaskType::kClone,
    "snapid1", "file1", kDefaultPoolset, CloneFileType::kFile, true);
    info.SetStatus(CloneStatus::cloning);
    auto cloneMetric = std::make_shared<CloneInfoMetric>("id1");
    auto cloneClosure = std::make_shared<CloneClosure>();
    std::shared_ptr<CloneTaskInfo> task =
        std::make_shared<CloneTaskInfo>(info, cloneMetric, cloneClosure);

    EXPECT_CALL(*metaStore_, UpdateCloneInfo(_))
        .WillRepeatedly(Return(kErrCodeSuccess));

    FInfo fInfo;
    fInfo.id = 100;
    fInfo.chunksize = 1024 * 1024;
    fInfo.segmentsize = 2 * fInfo.chunksize;
    fInfo.length = 1;
    fInfo.seqnum = 100;
    fInfo.owner = "user1";
    fInfo.filename = "file1";
    EXPECT_CALL(*client_, GetFileInfo(_, _, _))
        .WillRepeatedly(DoAll(
                SetArgPointee<2>(fInfo),
                Return(LIBCURVE_ERROR::OK)));

    core_->HandleCloneOrRecoverTask(task);
}

TEST_F(TestCloneCoreImpl,
    HandleCloneOrRecoverTaskStepUnknown) {
    CloneInfo info("id1", "user1", CloneTaskType::kClone,
    "snapid1", "file1", kDefaultPoolset, CloneFileType::kSnapshot, true);
    info.SetStatus(CloneStatus::cloning);
    info.SetNextStep(static_cast<CloneStep>(8));
    auto cloneMetric = std::make_shared<CloneInfoMetric>("id1");
    auto cloneClosure = std::make_shared<CloneClosure>();
    std::shared_ptr<CloneTaskInfo> task =
        std::make_shared<CloneTaskInfo>(info, cloneMetric, cloneClosure);

    EXPECT_CALL(*metaStore_, UpdateCloneInfo(_))
        .WillRepeatedly(Return(kErrCodeSuccess));

    MockBuildFileInfoFromSnapshotSuccess(task);
    MockCloneMetaSuccess(task);

    core_->HandleCloneOrRecoverTask(task);
}

void TestCloneCoreImpl::MockBuildFileInfoFromSnapshotSuccess(
    std::shared_ptr<CloneTaskInfo> task) {
    UUID uuid = "uuid1";
    const std::string user = "user1";
    const std::string fileName = "file1";
    const std::string desc = "snap1";
    uint64_t seqnum = 100;
    uint32_t chunksize = 1024 * 1024;
    uint64_t segmentsize = 2 * chunksize;
    uint64_t filelength = 1 * segmentsize;
    uint64_t time = 100;
    Status status = Status::done;
    SnapshotInfo info(uuid, user, fileName, desc,
        seqnum, chunksize, segmentsize, filelength, 0, 0, kDefaultPoolset,
        time, status);

    EXPECT_CALL(*metaStore_, GetSnapshotInfo(_, _))
        .WillRepeatedly(DoAll(
                SetArgPointee<1>(info),
                Return(kErrCodeSuccess)));

    if (CloneTaskType::kRecover == task->GetCloneInfo().GetTaskType()) {
        FInfo fInfo;
        fInfo.id = 100;
        fInfo.seqnum = 100;
        fInfo.poolset = kDefaultPoolset;
        EXPECT_CALL(*client_, GetFileInfo(_, _, _))
            .WillRepeatedly(DoAll(
                    SetArgPointee<2>(fInfo),
                    Return(LIBCURVE_ERROR::OK)));
    }

    ChunkIndexData snapMeta;
    snapMeta.SetFileName("file1");
    ChunkDataName chunk1("file1", 1, 0);
    ChunkDataName chunk2("file1", 1, 1);
    snapMeta.PutChunkDataName(chunk1);
    snapMeta.PutChunkDataName(chunk2);

    EXPECT_CALL(*dataStore_, GetChunkIndexData(_, _))
        .WillOnce(DoAll(
                    SetArgPointee<1>(snapMeta),
                    Return(kErrCodeSuccess)));

    FInfo fInfo;
    fInfo.id = 100;
    fInfo.seqnum = 100;
    fInfo.poolset = kDefaultPoolset;
    EXPECT_CALL(*client_, GetFileInfo(_, _, _))
        .WillRepeatedly(DoAll(
                SetArgPointee<2>(fInfo),
                Return(LIBCURVE_ERROR::OK)));
}

void TestCloneCoreImpl::MockBuildFileInfoFromFileSuccess(
    std::shared_ptr<CloneTaskInfo> task) {
    FInfo fInfo;
    fInfo.id = 100;
    fInfo.chunksize = 1024 * 1024;
    fInfo.segmentsize = 2 * fInfo.chunksize;
    fInfo.length = 1 * fInfo.segmentsize;
    fInfo.seqnum = 100;
    fInfo.owner = "user1";
    fInfo.filename = "file1";
    EXPECT_CALL(*client_, GetFileInfo(_, _, _))
        .WillRepeatedly(DoAll(
                SetArgPointee<2>(fInfo),
                Return(LIBCURVE_ERROR::OK)));
}

void TestCloneCoreImpl::MockCreateCloneFileSuccess(
    std::shared_ptr<CloneTaskInfo> task) {
    FInfo fInfoOut;
    fInfoOut.id = 100;
    EXPECT_CALL(*client_, CreateCloneFile(_, _, _, _, _, _, _, _, _, _))
        .WillOnce(DoAll(SetArgPointee<9>(fInfoOut),
                Return(LIBCURVE_ERROR::OK)));
}

void TestCloneCoreImpl::MockCloneMetaSuccess(
    std::shared_ptr<CloneTaskInfo> task) {
    uint32_t chunksize = 1024 * 1024;
    uint64_t segmentsize = 2 * chunksize;
    SegmentInfo segInfoOut;
    segInfoOut.segmentsize = segmentsize;
    segInfoOut.chunksize = chunksize;
    segInfoOut.startoffset = 0;
    segInfoOut.chunkvec = {{1, 1, 1},
                           {2, 2, 1}};
    segInfoOut.lpcpIDInfo.lpid = 1;
    segInfoOut.lpcpIDInfo.cpidVec = {1, 2};
    EXPECT_CALL(*client_, GetOrAllocateSegmentInfo(_, 0, _, _, _))
        .WillRepeatedly(
            DoAll(SetArgPointee<4>(segInfoOut),
                Return(LIBCURVE_ERROR::OK)));
}

void TestCloneCoreImpl::MockCreateCloneChunkSuccess(
    std::shared_ptr<CloneTaskInfo> task) {
    std::string location1, location2;
    if (CloneFileType::kSnapshot == task->GetCloneInfo().GetFileType()) {
        location1 = LocationOperator::GenerateS3Location(
            "file1-0-1");
        location2 = LocationOperator::GenerateS3Location(
            "file1-1-1");
    } else {
        location1 =
            LocationOperator::GenerateCurveLocation(
                        task->GetCloneInfo().GetSrc(),
                        std::stoull("0"));
        location2 =
            LocationOperator::GenerateCurveLocation(
                        task->GetCloneInfo().GetSrc(),
                        std::stoull("1048576"));
    }

    uint32_t correctSn = 0;
    if (CloneTaskType::kClone == task->GetCloneInfo().GetTaskType()) {
        correctSn = 0;
    } else {
        correctSn = 100;
    }
    EXPECT_CALL(*client_, CreateCloneChunk(
         AnyOf(location1, location2), _, _, correctSn, _, _))
        .WillRepeatedly(DoAll(
            Invoke([](const std::string &location,
                      const ChunkIDInfo &chunkidinfo,
                      uint64_t sn,
                      uint64_t csn,
                      uint64_t chunkSize,
                      SnapCloneClosure* scc){
                    scc->SetRetCode(LIBCURVE_ERROR::OK);
                    scc->Run();
                }),
            Return(LIBCURVE_ERROR::OK)));
}

void TestCloneCoreImpl::MockCompleteCloneMetaSuccess(
    std::shared_ptr<CloneTaskInfo> task) {
    EXPECT_CALL(*client_, CompleteCloneMeta(_, _))
        .WillOnce(Return(LIBCURVE_ERROR::OK));
}

void TestCloneCoreImpl::MockRecoverChunkSuccess(
    std::shared_ptr<CloneTaskInfo> task) {
    EXPECT_CALL(*client_, RecoverChunk(_, _, _, _))
        .WillRepeatedly(DoAll(
                    Invoke([](const ChunkIDInfo &chunkidinfo,
                              uint64_t offset,
                              uint64_t len,
                              SnapCloneClosure* scc){
                        scc->SetRetCode(LIBCURVE_ERROR::OK),
                        scc->Run();
                        }),
                    Return(LIBCURVE_ERROR::OK)));
}

void TestCloneCoreImpl::MockChangeOwnerSuccess(
    std::shared_ptr<CloneTaskInfo> task) {
    EXPECT_CALL(*client_, ChangeOwner(_, _))
        .WillOnce(Return(LIBCURVE_ERROR::OK));
}

void TestCloneCoreImpl::MockRenameCloneFileSuccess(
    std::shared_ptr<CloneTaskInfo> task) {
    EXPECT_CALL(*client_, RenameCloneFile(_, _, _, _, _))
        .WillOnce(Return(LIBCURVE_ERROR::OK));
}

void TestCloneCoreImpl::MockCompleteCloneFileSuccess(
    std::shared_ptr<CloneTaskInfo> task) {
    EXPECT_CALL(*client_, CompleteCloneFile(_, _))
        .WillOnce(Return(LIBCURVE_ERROR::OK));
}

void TestCloneCoreImpl::MockBuildFileInfoFromSnapshotFail(
    std::shared_ptr<CloneTaskInfo> task) {
    UUID uuid = "uuid1";
    const std::string user = "user1";
    const std::string fileName = "file1";
    const std::string desc = "snap1";
    uint64_t seqnum = 100;
    uint32_t chunksize = 1024 * 1024;
    uint64_t segmentsize = 2 * chunksize;
    uint64_t filelength = 2 * segmentsize;
    uint64_t time = 100;
    Status status = Status::done;
    SnapshotInfo info(uuid, user, fileName, desc,
        seqnum, chunksize, segmentsize, filelength, 0, 0, "default",
        time, status);

    EXPECT_CALL(*metaStore_, GetSnapshotInfo(_, _))
        .WillRepeatedly(DoAll(
                SetArgPointee<1>(info),
                Return(kErrCodeSuccess)));

    if (CloneTaskType::kRecover == task->GetCloneInfo().GetTaskType()) {
        FInfo fInfo;
        fInfo.id = 100;
        EXPECT_CALL(*client_, GetFileInfo(_, _, _))
            .WillRepeatedly(DoAll(
                    SetArgPointee<2>(fInfo),
                    Return(LIBCURVE_ERROR::OK)));
    }

    EXPECT_CALL(*dataStore_, GetChunkIndexData(_, _))
        .WillOnce(Return(kErrCodeInternalError));

    FInfo fInfo;
    fInfo.id = 100;
    EXPECT_CALL(*client_, GetFileInfo(_, _, _))
        .WillRepeatedly(DoAll(
                SetArgPointee<2>(fInfo),
                Return(-LIBCURVE_ERROR::FAILED)));
}

void TestCloneCoreImpl::MockBuildFileInfoFromFileFail(
    std::shared_ptr<CloneTaskInfo> task) {
    FInfo fInfo;
    fInfo.id = 100;
    fInfo.chunksize = 1024 * 1024;
    fInfo.segmentsize = 2 * fInfo.chunksize;
    fInfo.length = 2 * fInfo.segmentsize;
    fInfo.seqnum = 100;
    fInfo.owner = "user1";
    fInfo.filename = "file1";
    EXPECT_CALL(*client_, GetFileInfo(_, _, _))
        .WillRepeatedly(DoAll(
                SetArgPointee<2>(fInfo),
                Return(-LIBCURVE_ERROR::FAILED)));
}

void TestCloneCoreImpl::MockCreateCloneFileFail(
    std::shared_ptr<CloneTaskInfo> task) {
    FInfo fInfoOut;
    fInfoOut.id = 100;
    EXPECT_CALL(*client_, CreateCloneFile(_, _, _, _, _, _, _, _, _, _))
        .WillOnce(DoAll(SetArgPointee<9>(fInfoOut),
            Return(-LIBCURVE_ERROR::FAILED)));
}

void TestCloneCoreImpl::MockCloneMetaFail(
    std::shared_ptr<CloneTaskInfo> task) {
    EXPECT_CALL(*client_, GetOrAllocateSegmentInfo(_, _, _, _, _))
        .WillRepeatedly(Return(-LIBCURVE_ERROR::FAILED));
}

void TestCloneCoreImpl::MockCreateCloneChunkFail(
    std::shared_ptr<CloneTaskInfo> task) {
    EXPECT_CALL(*client_, CreateCloneChunk(_, _, _, _, _, _))
        .WillRepeatedly(DoAll(
            Invoke([](const std::string &location,
                      const ChunkIDInfo &chunkidinfo,
                      uint64_t sn,
                      uint64_t csn,
                      uint64_t chunkSize,
                      SnapCloneClosure* scc){
                    scc->SetRetCode(LIBCURVE_ERROR::OK);
                    scc->Run();
                }),
            Return(-LIBCURVE_ERROR::FAILED)));
}

void TestCloneCoreImpl::MockCompleteCloneMetaFail(
    std::shared_ptr<CloneTaskInfo> task) {
    EXPECT_CALL(*client_, CompleteCloneMeta(_, _))
        .WillOnce(Return(-LIBCURVE_ERROR::FAILED));
}

void TestCloneCoreImpl::MockRecoverChunkFail(
    std::shared_ptr<CloneTaskInfo> task) {
    EXPECT_CALL(*client_, RecoverChunk(_, _, _, _))
        .WillRepeatedly(DoAll(
                    Invoke([](const ChunkIDInfo &chunkidinfo,
                              uint64_t offset,
                              uint64_t len,
                              SnapCloneClosure* scc){
                        scc->Run();
                        }),
                    Return(-LIBCURVE_ERROR::FAILED)));
}

void TestCloneCoreImpl::MockChangeOwnerFail(
    std::shared_ptr<CloneTaskInfo> task) {
    EXPECT_CALL(*client_, ChangeOwner(_, _))
        .WillOnce(Return(-LIBCURVE_ERROR::FAILED));
}

void TestCloneCoreImpl::MockRenameCloneFileFail(
    std::shared_ptr<CloneTaskInfo> task) {
    EXPECT_CALL(*client_, RenameCloneFile(_, _, _, _, _))
        .WillOnce(Return(-LIBCURVE_ERROR::FAILED));
}

void TestCloneCoreImpl::MockCompleteCloneFileFail(
    std::shared_ptr<CloneTaskInfo> task) {
    EXPECT_CALL(*client_, CompleteCloneFile(_, _))
        .WillOnce(Return(-LIBCURVE_ERROR::FAILED));
}

TEST_F(TestCloneCoreImpl, TestCleanOrRecoverTaskPreSuccess) {
    const TaskIdType &taskId = "id1";
    const std::string user = "user1";
    CloneInfo cloneInfoOut;

    CloneInfo cinfo;
    cinfo.SetTaskId(taskId);
    cinfo.SetUser(user);
    cinfo.SetStatus(CloneStatus::error);

    EXPECT_CALL(*metaStore_, GetCloneInfo(_, _))
        .WillOnce(DoAll(
                  SetArgPointee<1>(cinfo),
                  Return(0)));

    EXPECT_CALL(*metaStore_, UpdateCloneInfo(_))
        .WillOnce(Return(kErrCodeSuccess));

    int ret = core_->CleanCloneOrRecoverTaskPre(
        user, taskId, &cloneInfoOut);
    ASSERT_EQ(kErrCodeSuccess, ret);
}

TEST_F(TestCloneCoreImpl, TestCleanOrRecoverTaskPreGetCloneInfoFail) {
    const TaskIdType &taskId = "id1";
    const std::string user = "user1";
    CloneInfo cloneInfoOut;

    CloneInfo cinfo;
    cinfo.SetTaskId(taskId);
    cinfo.SetUser(user);
    cinfo.SetStatus(CloneStatus::error);

    EXPECT_CALL(*metaStore_, GetCloneInfo(_, _))
        .WillOnce(DoAll(
                  SetArgPointee<1>(cinfo),
                  Return(-1)));

    int ret = core_->CleanCloneOrRecoverTaskPre(
        user, taskId, &cloneInfoOut);
    ASSERT_EQ(0, ret);
}

TEST_F(TestCloneCoreImpl, TestCleanOrRecoverTaskPreInvalidUser) {
    const TaskIdType &taskId = "id1";
    const std::string user = "user1";
    CloneInfo cloneInfoOut;

    CloneInfo cinfo;
    cinfo.SetTaskId(taskId);
    cinfo.SetUser("user2");
    cinfo.SetStatus(CloneStatus::error);

    EXPECT_CALL(*metaStore_, GetCloneInfo(_, _))
        .WillOnce(DoAll(
                  SetArgPointee<1>(cinfo),
                  Return(0)));

    int ret = core_->CleanCloneOrRecoverTaskPre(
        user, taskId, &cloneInfoOut);
    ASSERT_EQ(kErrCodeInvalidUser, ret);
}

TEST_F(TestCloneCoreImpl, TestCleanOrRecoverTaskPreCannotCleanUnFinished) {
    const TaskIdType &taskId = "id1";
    const std::string user = "user1";
    CloneInfo cloneInfoOut;

    CloneInfo cinfo;
    cinfo.SetTaskId(taskId);
    cinfo.SetUser(user);
    cinfo.SetStatus(CloneStatus::cloning);

    EXPECT_CALL(*metaStore_, GetCloneInfo(_, _))
        .WillOnce(DoAll(
                  SetArgPointee<1>(cinfo),
                  Return(0)));

    int ret = core_->CleanCloneOrRecoverTaskPre(
        user, taskId, &cloneInfoOut);
    ASSERT_EQ(kErrCodeCannotCleanCloneUnfinished, ret);
}

TEST_F(TestCloneCoreImpl, TestCleanOrRecoverTaskPreTaskExist) {
    const TaskIdType &taskId = "id1";
    const std::string user = "user1";
    CloneInfo cloneInfoOut;

    CloneInfo cinfo;
    cinfo.SetTaskId(taskId);
    cinfo.SetUser(user);
    cinfo.SetStatus(CloneStatus::errorCleaning);

    EXPECT_CALL(*metaStore_, GetCloneInfo(_, _))
        .WillOnce(DoAll(
                  SetArgPointee<1>(cinfo),
                  Return(0)));

    int ret = core_->CleanCloneOrRecoverTaskPre(
        user, taskId, &cloneInfoOut);
    ASSERT_EQ(kErrCodeTaskExist, ret);
}

TEST_F(TestCloneCoreImpl, TestCleanOrRecoverTaskPreUpdateCloneInfoFail) {
    const TaskIdType &taskId = "id1";
    const std::string user = "user1";
    CloneInfo cloneInfoOut;

    CloneInfo cinfo;
    cinfo.SetTaskId(taskId);
    cinfo.SetUser(user);
    cinfo.SetStatus(CloneStatus::error);

    EXPECT_CALL(*metaStore_, GetCloneInfo(_, _))
        .WillOnce(DoAll(
                  SetArgPointee<1>(cinfo),
                  Return(0)));

    EXPECT_CALL(*metaStore_, UpdateCloneInfo(_))
        .WillOnce(Return(kErrCodeInternalError));

    int ret = core_->CleanCloneOrRecoverTaskPre(
        user, taskId, &cloneInfoOut);
    ASSERT_EQ(kErrCodeInternalError, ret);
}

TEST_F(TestCloneCoreImpl, TestHandleCleanCloneOrRecoverTaskSuccess) {
    CloneInfo info("id1", "user1", CloneTaskType::kClone,
    "snapid1", "file1", kDefaultPoolset, CloneFileType::kSnapshot, false);
    info.SetStatus(CloneStatus::errorCleaning);
    auto cloneMetric = std::make_shared<CloneInfoMetric>("id1");
    auto cloneClosure = std::make_shared<CloneClosure>();
    std::shared_ptr<CloneTaskInfo> task =
        std::make_shared<CloneTaskInfo>(info, cloneMetric, cloneClosure);

    EXPECT_CALL(*client_, DeleteFile(_, _, _))
        .WillOnce(Return(LIBCURVE_ERROR::OK));

    EXPECT_CALL(*metaStore_, DeleteCloneInfo(_))
        .WillOnce(Return(0));
    core_->HandleCleanCloneOrRecoverTask(task);
}

TEST_F(TestCloneCoreImpl, TestHandleCleanCloneOrRecoverTaskSuccess2) {
    CloneInfo info("id1", "user1", CloneTaskType::kClone,
    "snapid1", "file1", kDefaultPoolset, CloneFileType::kSnapshot, false);
    info.SetStatus(CloneStatus::errorCleaning);
    auto cloneMetric = std::make_shared<CloneInfoMetric>("id1");
    auto cloneClosure = std::make_shared<CloneClosure>();
    std::shared_ptr<CloneTaskInfo> task =
        std::make_shared<CloneTaskInfo>(info, cloneMetric, cloneClosure);

    EXPECT_CALL(*client_, DeleteFile(_, _, _))
        .WillOnce(Return(-LIBCURVE_ERROR::NOTEXIST));

    EXPECT_CALL(*metaStore_, DeleteCloneInfo(_))
        .WillOnce(Return(0));
    core_->HandleCleanCloneOrRecoverTask(task);
}

TEST_F(TestCloneCoreImpl, TestHandleCleanCloneOrRecoverTaskLazySuccess) {
    CloneInfo info("id1", "user1", CloneTaskType::kClone,
    "snapid1", "file1", kDefaultPoolset, CloneFileType::kSnapshot, true);
    info.SetStatus(CloneStatus::errorCleaning);
    auto cloneMetric = std::make_shared<CloneInfoMetric>("id1");
    auto cloneClosure = std::make_shared<CloneClosure>();
    std::shared_ptr<CloneTaskInfo> task =
        std::make_shared<CloneTaskInfo>(info, cloneMetric, cloneClosure);

    EXPECT_CALL(*client_, DeleteFile(_, _, _))
        .Times(1)
        .WillOnce(Return(LIBCURVE_ERROR::OK));

    EXPECT_CALL(*metaStore_, DeleteCloneInfo(_))
        .WillOnce(Return(0));
    core_->HandleCleanCloneOrRecoverTask(task);
}

TEST_F(TestCloneCoreImpl, TestHandleCleanCloneOrRecoverTaskFail1) {
    CloneInfo info("id1", "user1", CloneTaskType::kClone,
    "snapid1", "file1", kDefaultPoolset, CloneFileType::kSnapshot, false);
    info.SetStatus(CloneStatus::errorCleaning);
    auto cloneMetric = std::make_shared<CloneInfoMetric>("id1");
    auto cloneClosure = std::make_shared<CloneClosure>();
    std::shared_ptr<CloneTaskInfo> task =
        std::make_shared<CloneTaskInfo>(info, cloneMetric, cloneClosure);

    EXPECT_CALL(*client_, DeleteFile(_, _, _))
        .Times(1)
        .WillOnce(Return(-LIBCURVE_ERROR::FAILED));

    EXPECT_CALL(*metaStore_, UpdateCloneInfo(_))
        .WillOnce(Return(0));

    core_->HandleCleanCloneOrRecoverTask(task);
}

TEST_F(TestCloneCoreImpl,
    TestHandleCleanCloneOrRecoverTaskCleanNotErrorSuccess) {
    CloneInfo info("id1", "user1", CloneTaskType::kClone,
    "snapid1", "file1", kDefaultPoolset, CloneFileType::kSnapshot, false);
    info.SetStatus(CloneStatus::cleaning);
    auto cloneMetric = std::make_shared<CloneInfoMetric>("id1");
    auto cloneClosure = std::make_shared<CloneClosure>();
    std::shared_ptr<CloneTaskInfo> task =
        std::make_shared<CloneTaskInfo>(info, cloneMetric, cloneClosure);

    EXPECT_CALL(*metaStore_, DeleteCloneInfo(_))
        .WillOnce(Return(0));

    core_->HandleCleanCloneOrRecoverTask(task);
}

TEST_F(TestCloneCoreImpl,
    TestCheckFileExists) {
    FInfo fInfo;
    fInfo.id = 100;
    EXPECT_CALL(*client_, GetFileInfo(_, _, _))
        .WillOnce(DoAll(
            SetArgPointee<2>(fInfo),
            Return(LIBCURVE_ERROR::OK)));

    ASSERT_EQ(core_->CheckFileExists("filename", 100), kErrCodeFileExist);

    EXPECT_CALL(*client_, GetFileInfo(_, _, _))
        .WillOnce(DoAll(
            SetArgPointee<2>(fInfo),
            Return(LIBCURVE_ERROR::OK)));

    ASSERT_EQ(core_->CheckFileExists("filename", 10), kErrCodeFileNotExist);

    EXPECT_CALL(*client_, GetFileInfo(_, _, _))
        .WillOnce(Return(-LIBCURVE_ERROR::NOTEXIST));

    ASSERT_EQ(core_->CheckFileExists("filename", 100), kErrCodeFileNotExist);

    EXPECT_CALL(*client_, GetFileInfo(_, _, _))
        .WillOnce(Return(LIBCURVE_ERROR::INTERNAL_ERROR));

    ASSERT_EQ(core_->CheckFileExists("filename", 100), kErrCodeInternalError);
}

TEST_F(TestCloneCoreImpl,
    TestHandleDeleteCloneInfoSnapDeleteCloneInfoFail) {
    CloneInfo info("id1", "user1", CloneTaskType::kClone,
    "snapid1", "file1", kDefaultPoolset, CloneFileType::kSnapshot, false);
    EXPECT_CALL(*metaStore_, DeleteCloneInfo(_))
        .WillOnce(Return(-1));
    snapshotRef_->IncrementSnapshotRef("snapid1");
    ASSERT_EQ(core_->HandleDeleteCloneInfo(info), kErrCodeInternalError);
    ASSERT_EQ(snapshotRef_->GetSnapshotRef("snapid1"), 1);
}

TEST_F(TestCloneCoreImpl,
    TestHandleDeleteCloneInfoSnapSuccess) {
    CloneInfo info("id1", "user1", CloneTaskType::kClone,
    "snapid1", "file1", kDefaultPoolset, CloneFileType::kSnapshot, false);
    info.SetStatus(CloneStatus::metaInstalled);
    EXPECT_CALL(*metaStore_, DeleteCloneInfo(_))
        .WillOnce(Return(0));
    snapshotRef_->IncrementSnapshotRef("snapid1");
    ASSERT_EQ(core_->HandleDeleteCloneInfo(info), kErrCodeSuccess);
    ASSERT_EQ(snapshotRef_->GetSnapshotRef("snapid1"), 0);
}

TEST_F(TestCloneCoreImpl,
    TestHandleDeleteCloneInfoFileRefReturnMetainstalledNotTo0) {
    CloneInfo info("id1", "user1", CloneTaskType::kClone,
    "source1", "file1", kDefaultPoolset, CloneFileType::kFile, false);
    info.SetStatus(CloneStatus::metaInstalled);
    EXPECT_CALL(*metaStore_, DeleteCloneInfo(_))
        .WillOnce(Return(0));
    cloneRef_->IncrementRef("source1");
    cloneRef_->IncrementRef("source1");
    ASSERT_EQ(cloneRef_->GetRef("source1"), 2);
    ASSERT_EQ(core_->HandleDeleteCloneInfo(info), kErrCodeSuccess);
    ASSERT_EQ(cloneRef_->GetRef("source1"), 1);
}

TEST_F(TestCloneCoreImpl,
    TestHandleDeleteCloneInfoFileSetStatusFail) {
    CloneInfo info("id1", "user1", CloneTaskType::kClone,
    "source1", "file1", kDefaultPoolset, CloneFileType::kFile, false);
    info.SetStatus(CloneStatus::metaInstalled);
    cloneRef_->IncrementRef("source1");
    ASSERT_EQ(cloneRef_->GetRef("source1"), 1);
    EXPECT_CALL(*client_, SetCloneFileStatus(_, _, _))
        .WillOnce(Return(-1));
    ASSERT_EQ(core_->HandleDeleteCloneInfo(info), kErrCodeInternalError);
    ASSERT_EQ(cloneRef_->GetRef("source1"), 1);
}

TEST_F(TestCloneCoreImpl,
    TestHandleDeleteCloneInfoFileDeleteCloneInfoFail) {
    CloneInfo info("id1", "user1", CloneTaskType::kClone,
    "source1", "file1", kDefaultPoolset, CloneFileType::kFile, false);
    info.SetStatus(CloneStatus::metaInstalled);
    EXPECT_CALL(*metaStore_, DeleteCloneInfo(_))
        .WillOnce(Return(-1));
    cloneRef_->IncrementRef("source1");
    ASSERT_EQ(cloneRef_->GetRef("source1"), 1);
    EXPECT_CALL(*client_, SetCloneFileStatus(_, _, _))
        .WillOnce(Return(LIBCURVE_ERROR::OK));
    ASSERT_EQ(core_->HandleDeleteCloneInfo(info), kErrCodeInternalError);
    ASSERT_EQ(cloneRef_->GetRef("source1"), 1);
}

TEST_F(TestCloneCoreImpl,
    TestHandleDeleteCloneInfoFileSuccess) {
    CloneInfo info("id1", "user1", CloneTaskType::kClone,
    "source1", "file1", kDefaultPoolset, CloneFileType::kFile, false);
    info.SetStatus(CloneStatus::metaInstalled);
    EXPECT_CALL(*metaStore_, DeleteCloneInfo(_))
        .WillOnce(Return(0));
    cloneRef_->IncrementRef("source1");
    ASSERT_EQ(cloneRef_->GetRef("source1"), 1);
    EXPECT_CALL(*client_, SetCloneFileStatus(_, _, _))
        .WillOnce(Return(LIBCURVE_ERROR::OK));
    ASSERT_EQ(core_->HandleDeleteCloneInfo(info), kErrCodeSuccess);
    ASSERT_EQ(cloneRef_->GetRef("source1"), 0);
}

}  // namespace snapshotcloneserver
}  // namespace curve
