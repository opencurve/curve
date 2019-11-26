/*
 * Project: curve
 * Created Date: Fri 12 Apr 2019 09:27:15 PM CST
 * Author: xuchaojie
 * Copyright (c) 2019 netease
 */

#include <gtest/gtest.h>
#include <gmock/gmock.h>

#include "src/snapshotcloneserver/clone/clone_service_manager.h"
#include "src/snapshotcloneserver/common/define.h"

#include "test/snapshotcloneserver/mock_snapshot_server.h"
#include "src/common/concurrent/count_down_event.h"

using curve::common::CountDownEvent;
using ::testing::Return;
using ::testing::_;
using ::testing::AnyOf;
using ::testing::AllOf;
using ::testing::SetArgPointee;
using ::testing::Invoke;
using ::testing::DoAll;
using ::testing::Property;

namespace curve {
namespace snapshotcloneserver {

class TestCloneServiceManager : public ::testing::Test {
 public:
    TestCloneServiceManager() {}
    virtual ~TestCloneServiceManager() {}

    virtual void SetUp() {
        cloneCore_ = std::make_shared<MockCloneCore>();
        std::shared_ptr<CloneTaskManager> cloneTaskMgr_ =
            std::make_shared<CloneTaskManager>();

        SnapshotCloneServerOptions option;
        option.clonePoolThreadNum = 8;
        option.cloneTaskManagerScanIntervalMs = 1000;

        manager_ = std::make_shared<CloneServiceManager>(
            cloneTaskMgr_, cloneCore_);

        ASSERT_EQ(0, manager_->Init(option))
            << "manager init fail.";
        ASSERT_EQ(0, manager_->Start())
            << "manager start fail.";
    }

    virtual void TearDown() {
        cloneCore_ = nullptr;
        manager_->Stop();
        manager_ = nullptr;
    }

 protected:
    std::shared_ptr<MockCloneCore> cloneCore_;
    std::shared_ptr<CloneServiceManager> manager_;
};


TEST_F(TestCloneServiceManager,
    TestCloneFileSuccess) {
    const UUID source = "uuid1";
    const std::string user = "user1";
    const std::string destination = "file1";
    bool lazyFlag = true;

    CloneInfo cloneInfo("uuid1", user, CloneTaskType::kClone,
        source, destination, CloneFileType::kSnapshot, lazyFlag);

    EXPECT_CALL(*cloneCore_, CloneOrRecoverPre(
            source, user, destination, lazyFlag, CloneTaskType::kClone, _))
        .WillOnce(DoAll(
            SetArgPointee<5>(cloneInfo),
            Return(kErrCodeSuccess)));

    CountDownEvent cond1(1);

    EXPECT_CALL(*cloneCore_, HandleCloneOrRecoverTask(_))
        .WillOnce(Invoke([&cond1] (std::shared_ptr<CloneTaskInfo> task) {
                                task->Finish();
                                cond1.Signal();
                            }));

    TaskIdType taskId;
    int ret = manager_->CloneFile(
        source,
        user,
        destination,
        lazyFlag,
        &taskId);
    ASSERT_EQ(kErrCodeSuccess, ret);

    cond1.Wait();
}

TEST_F(TestCloneServiceManager,
    TestCloneFilePreFail) {
    const UUID source = "uuid1";
    const std::string user = "user1";
    const std::string destination = "file1";
    bool lazyFlag = true;

    CloneInfo cloneInfo;

    EXPECT_CALL(*cloneCore_, CloneOrRecoverPre(
            source, user, destination, lazyFlag, CloneTaskType::kClone, _))
        .WillOnce(DoAll(
            SetArgPointee<5>(cloneInfo),
            Return(kErrCodeInternalError)));

    TaskIdType taskId;
    int ret = manager_->CloneFile(
        source,
        user,
        destination,
        lazyFlag,
        &taskId);
    ASSERT_EQ(kErrCodeInternalError, ret);
}

TEST_F(TestCloneServiceManager,
    TestCloneFilePushTaskFail) {
    const UUID source = "uuid1";
    const std::string user = "user1";
    const std::string destination = "file1";
    bool lazyFlag = true;

    CloneInfo cloneInfo("uuid1", user, CloneTaskType::kClone,
        source, destination, CloneFileType::kSnapshot, lazyFlag);

    EXPECT_CALL(*cloneCore_, CloneOrRecoverPre(
            source, user, destination, lazyFlag, CloneTaskType::kClone, _))
        .WillRepeatedly(DoAll(
            SetArgPointee<5>(cloneInfo),
            Return(kErrCodeSuccess)));

    CountDownEvent cond1(1);

    EXPECT_CALL(*cloneCore_, HandleCloneOrRecoverTask(_))
        .WillOnce(Invoke([&cond1] (std::shared_ptr<CloneTaskInfo> task) {
                            }));

    TaskIdType taskId;
    int ret = manager_->CloneFile(
        source,
        user,
        destination,
        lazyFlag,
        &taskId);
    ASSERT_EQ(kErrCodeSuccess, ret);

    ret = manager_->CloneFile(
        source,
        user,
        destination,
        lazyFlag,
        &taskId);
    ASSERT_EQ(kErrCodeTaskExist, ret);
}

TEST_F(TestCloneServiceManager,
    TestRecoverFileSuccess) {
    const UUID source = "uuid1";
    const std::string user = "user1";
    const std::string destination = "file1";
    bool lazyFlag = true;

    CloneInfo cloneInfo("uuid1", user, CloneTaskType::kRecover,
        source, destination, CloneFileType::kSnapshot, lazyFlag);

    EXPECT_CALL(*cloneCore_, CloneOrRecoverPre(
            source, user, destination, lazyFlag, CloneTaskType::kRecover, _))
        .WillOnce(DoAll(
            SetArgPointee<5>(cloneInfo),
            Return(kErrCodeSuccess)));

    CountDownEvent cond1(1);

    EXPECT_CALL(*cloneCore_, HandleCloneOrRecoverTask(_))
        .WillOnce(Invoke([&cond1] (std::shared_ptr<CloneTaskInfo> task) {
                                task->Finish();
                                cond1.Signal();
                            }));

    TaskIdType taskId;
    int ret = manager_->RecoverFile(
        source,
        user,
        destination,
        lazyFlag,
        &taskId);
    ASSERT_EQ(kErrCodeSuccess, ret);

    cond1.Wait();
}

TEST_F(TestCloneServiceManager,
    TestRecoverFilePreFail) {
    const UUID source = "uuid1";
    const std::string user = "user1";
    const std::string destination = "file1";
    bool lazyFlag = true;

    CloneInfo cloneInfo;

    EXPECT_CALL(*cloneCore_, CloneOrRecoverPre(
            source, user, destination, lazyFlag, CloneTaskType::kRecover, _))
        .WillOnce(DoAll(
            SetArgPointee<5>(cloneInfo),
            Return(kErrCodeInternalError)));

    TaskIdType taskId;
    int ret = manager_->RecoverFile(
        source,
        user,
        destination,
        lazyFlag,
        &taskId);
    ASSERT_EQ(kErrCodeInternalError, ret);
}

TEST_F(TestCloneServiceManager,
    TestRecoverFilePushTaskFail) {
    const UUID source = "uuid1";
    const std::string user = "user1";
    const std::string destination = "file1";
    bool lazyFlag = true;

    CloneInfo cloneInfo("uuid1", user, CloneTaskType::kRecover,
        source, destination, CloneFileType::kSnapshot, lazyFlag);

    EXPECT_CALL(*cloneCore_, CloneOrRecoverPre(
            source, user, destination, lazyFlag, CloneTaskType::kRecover, _))
        .WillRepeatedly(DoAll(
            SetArgPointee<5>(cloneInfo),
            Return(kErrCodeSuccess)));

    CountDownEvent cond1(1);

    EXPECT_CALL(*cloneCore_, HandleCloneOrRecoverTask(_))
        .WillOnce(Invoke([&cond1] (std::shared_ptr<CloneTaskInfo> task) {
                            }));

    TaskIdType taskId;
    int ret = manager_->RecoverFile(
        source,
        user,
        destination,
        lazyFlag,
        &taskId);
    ASSERT_EQ(kErrCodeSuccess, ret);

    ret = manager_->RecoverFile(
        source,
        user,
        destination,
        lazyFlag,
        &taskId);
    ASSERT_EQ(kErrCodeTaskExist, ret);
}

TEST_F(TestCloneServiceManager, TestGetCloneTaskInfoSuccess) {
    const UUID source = "uuid1";
    const std::string user = "user1";
    const std::string destination = "file1";
    bool lazyFlag = true;

    CloneInfo cloneInfo("uuid1", user, CloneTaskType::kClone,
        source, destination, CloneFileType::kSnapshot, lazyFlag);

    EXPECT_CALL(*cloneCore_, CloneOrRecoverPre(
            source, user, destination, lazyFlag, CloneTaskType::kClone, _))
        .WillOnce(DoAll(
            SetArgPointee<5>(cloneInfo),
            Return(kErrCodeSuccess)));

    CountDownEvent cond1(1);
    CountDownEvent cond2(1);

    EXPECT_CALL(*cloneCore_, HandleCloneOrRecoverTask(_))
        .WillOnce(Invoke([&cond1, &cond2] (
            std::shared_ptr<CloneTaskInfo> task) {
                                cond2.Wait();
                                task->Finish();
                                cond1.Signal();
                            }));

    TaskIdType taskId;
    int ret = manager_->CloneFile(
        source,
        user,
        destination,
        lazyFlag,
        &taskId);
    ASSERT_EQ(kErrCodeSuccess, ret);

    std::vector<CloneInfo> cloneInfos;
    cloneInfos.push_back(cloneInfo);
    EXPECT_CALL(*cloneCore_, GetCloneInfoList(_))
        .WillOnce(DoAll(SetArgPointee<0>(cloneInfos),
            Return(kErrCodeSuccess)));

    std::vector<TaskCloneInfo> infos;
    ret = manager_->GetCloneTaskInfo(user, nullptr, &infos);
    cond2.Signal();

    ASSERT_EQ(kErrCodeSuccess, ret);
    ASSERT_EQ(1, infos.size());
    CloneInfo cInfo = infos[0].GetCloneInfo();
    ASSERT_EQ("uuid1", cInfo.GetTaskId());
    ASSERT_EQ(user, cInfo.GetUser());
    ASSERT_EQ(CloneTaskType::kClone, cInfo.GetTaskType());
    ASSERT_EQ(source, cInfo.GetSrc());
    ASSERT_EQ(destination, cInfo.GetDest());
    ASSERT_EQ(CloneFileType::kSnapshot, cInfo.GetFileType());
    ASSERT_EQ(lazyFlag, cInfo.GetIsLazy());
    ASSERT_EQ(CloneStatus::cloning, cInfo.GetStatus());

    cond1.Wait();
}

TEST_F(TestCloneServiceManager, TestGetCloneTaskInfoFail) {
    std::vector<CloneInfo> cloneInfos;
    EXPECT_CALL(*cloneCore_, GetCloneInfoList(_))
        .WillOnce(DoAll(SetArgPointee<0>(cloneInfos),
            Return(kErrCodeInternalError)));

    std::vector<TaskCloneInfo> infos;
    int ret = manager_->GetCloneTaskInfo("user1", nullptr, &infos);
    ASSERT_EQ(kErrCodeInternalError, ret);
}

TEST_F(TestCloneServiceManager, TestRecoverCloneTaskSuccess) {
    const UUID source = "uuid1";
    const std::string user = "user1";
    const std::string destination = "file1";
    bool lazyFlag = true;
    CloneInfo cloneInfo("uuid1", user, CloneTaskType::kClone,
        source, destination, CloneFileType::kSnapshot, lazyFlag);

    std::vector<CloneInfo> cloneInfos;
    cloneInfos.push_back(cloneInfo);
    EXPECT_CALL(*cloneCore_, GetCloneInfoList(_))
        .WillOnce(DoAll(SetArgPointee<0>(cloneInfos),
            Return(kErrCodeSuccess)));

    std::shared_ptr<SnapshotReference> snapRef =
        std::make_shared<SnapshotReference>();
    EXPECT_CALL(*cloneCore_, GetSnapshotRef())
        .WillOnce(Return(snapRef));

    CountDownEvent cond1(1);
    EXPECT_CALL(*cloneCore_, HandleCloneOrRecoverTask(_))
        .WillOnce(Invoke([&cond1] (
            std::shared_ptr<CloneTaskInfo> task) {
                                task->Finish();
                                cond1.Signal();
                            }));

    int ret = manager_->RecoverCloneTask();
    ASSERT_EQ(kErrCodeSuccess, ret);
    cond1.Wait();
}

TEST_F(TestCloneServiceManager, TestCloneServiceNotStart) {
    manager_->Stop();
    const UUID source = "uuid1";
    const std::string user = "user1";
    const std::string destination = "file1";
    bool lazyFlag = true;

    CloneInfo cloneInfo("uuid1", user, CloneTaskType::kClone,
        source, destination, CloneFileType::kSnapshot, lazyFlag);

    EXPECT_CALL(*cloneCore_, CloneOrRecoverPre(
            source, user, destination, lazyFlag, CloneTaskType::kClone, _))
        .WillOnce(DoAll(
            SetArgPointee<5>(cloneInfo),
            Return(kErrCodeSuccess)));

    TaskIdType taskId;
    int ret = manager_->CloneFile(
        source,
        user,
        destination,
        lazyFlag,
        &taskId);
    ASSERT_EQ(kErrCodeServiceIsStop, ret);
}

TEST_F(TestCloneServiceManager, TestGetCloneTaskInfoSuccessCloneTaskDone) {
    const UUID source = "uuid1";
    const std::string user = "user1";
    const std::string destination = "file1";
    bool lazyFlag = true;

    CloneInfo cloneInfo("uuid1", user, CloneTaskType::kClone,
        source, destination, CloneFileType::kSnapshot, lazyFlag);
    cloneInfo.SetStatus(CloneStatus::done);

    std::vector<CloneInfo> cloneInfos;
    cloneInfos.push_back(cloneInfo);
    EXPECT_CALL(*cloneCore_, GetCloneInfoList(_))
        .WillOnce(DoAll(SetArgPointee<0>(cloneInfos),
            Return(kErrCodeSuccess)));

    std::vector<TaskCloneInfo> infos;
    int ret = manager_->GetCloneTaskInfo(user, nullptr, &infos);

    ASSERT_EQ(kErrCodeSuccess, ret);
    ASSERT_EQ(1, infos.size());
    CloneInfo cInfo = infos[0].GetCloneInfo();
    ASSERT_EQ("uuid1", cInfo.GetTaskId());
    ASSERT_EQ(user, cInfo.GetUser());
    ASSERT_EQ(CloneTaskType::kClone, cInfo.GetTaskType());
    ASSERT_EQ(source, cInfo.GetSrc());
    ASSERT_EQ(destination, cInfo.GetDest());
    ASSERT_EQ(CloneFileType::kSnapshot, cInfo.GetFileType());
    ASSERT_EQ(lazyFlag, cInfo.GetIsLazy());
    ASSERT_EQ(CloneStatus::done, cInfo.GetStatus());
}

TEST_F(TestCloneServiceManager, TestGetCloneTaskInfoSuccessCloneTaskError) {
    const UUID source = "uuid1";
    const std::string user = "user1";
    const std::string destination = "file1";
    bool lazyFlag = true;

    CloneInfo cloneInfo("uuid1", user, CloneTaskType::kClone,
        source, destination, CloneFileType::kSnapshot, lazyFlag);
    cloneInfo.SetStatus(CloneStatus::error);

    std::vector<CloneInfo> cloneInfos;
    cloneInfos.push_back(cloneInfo);
    EXPECT_CALL(*cloneCore_, GetCloneInfoList(_))
        .WillOnce(DoAll(SetArgPointee<0>(cloneInfos),
            Return(kErrCodeSuccess)));

    std::vector<TaskCloneInfo> infos;
    int ret = manager_->GetCloneTaskInfo(user, nullptr, &infos);

    ASSERT_EQ(kErrCodeSuccess, ret);
    ASSERT_EQ(1, infos.size());
    CloneInfo cInfo = infos[0].GetCloneInfo();
    ASSERT_EQ("uuid1", cInfo.GetTaskId());
    ASSERT_EQ(user, cInfo.GetUser());
    ASSERT_EQ(CloneTaskType::kClone, cInfo.GetTaskType());
    ASSERT_EQ(source, cInfo.GetSrc());
    ASSERT_EQ(destination, cInfo.GetDest());
    ASSERT_EQ(CloneFileType::kSnapshot, cInfo.GetFileType());
    ASSERT_EQ(lazyFlag, cInfo.GetIsLazy());
    ASSERT_EQ(CloneStatus::error, cInfo.GetStatus());
}

TEST_F(TestCloneServiceManager, TestGetCloneTaskInfoSuccessCloneTaskDone2) {
    const UUID source = "uuid1";
    const std::string user = "user1";
    const std::string destination = "file1";
    bool lazyFlag = true;

    CloneInfo cloneInfo("uuid1", user, CloneTaskType::kClone,
        source, destination, CloneFileType::kSnapshot, lazyFlag);
    cloneInfo.SetStatus(CloneStatus::cloning);

    std::vector<CloneInfo> cloneInfos;
    cloneInfos.push_back(cloneInfo);
    EXPECT_CALL(*cloneCore_, GetCloneInfoList(_))
        .WillOnce(DoAll(SetArgPointee<0>(cloneInfos),
            Return(kErrCodeSuccess)));

    cloneInfo.SetStatus(CloneStatus::done);
    EXPECT_CALL(*cloneCore_, GetCloneInfo(_, _))
        .WillOnce(DoAll(SetArgPointee<1>(cloneInfo),
            Return(kErrCodeSuccess)));

    std::vector<TaskCloneInfo> infos;
    int ret = manager_->GetCloneTaskInfo(user, nullptr, &infos);

    ASSERT_EQ(kErrCodeSuccess, ret);
    ASSERT_EQ(1, infos.size());
    CloneInfo cInfo = infos[0].GetCloneInfo();
    ASSERT_EQ("uuid1", cInfo.GetTaskId());
    ASSERT_EQ(user, cInfo.GetUser());
    ASSERT_EQ(CloneTaskType::kClone, cInfo.GetTaskType());
    ASSERT_EQ(source, cInfo.GetSrc());
    ASSERT_EQ(destination, cInfo.GetDest());
    ASSERT_EQ(CloneFileType::kSnapshot, cInfo.GetFileType());
    ASSERT_EQ(lazyFlag, cInfo.GetIsLazy());
    ASSERT_EQ(CloneStatus::done, cInfo.GetStatus());
}

TEST_F(TestCloneServiceManager, TestGetCloneTaskInfoSuccessCloneTaskError2) {
    const UUID source = "uuid1";
    const std::string user = "user1";
    const std::string destination = "file1";
    bool lazyFlag = true;

    CloneInfo cloneInfo("uuid1", user, CloneTaskType::kClone,
        source, destination, CloneFileType::kSnapshot, lazyFlag);
    cloneInfo.SetStatus(CloneStatus::cloning);

    std::vector<CloneInfo> cloneInfos;
    cloneInfos.push_back(cloneInfo);
    EXPECT_CALL(*cloneCore_, GetCloneInfoList(_))
        .WillOnce(DoAll(SetArgPointee<0>(cloneInfos),
            Return(kErrCodeSuccess)));

    cloneInfo.SetStatus(CloneStatus::error);
    EXPECT_CALL(*cloneCore_, GetCloneInfo(_, _))
        .WillOnce(DoAll(SetArgPointee<1>(cloneInfo),
            Return(kErrCodeSuccess)));

    std::vector<TaskCloneInfo> infos;
    int ret = manager_->GetCloneTaskInfo(user, nullptr, &infos);

    ASSERT_EQ(kErrCodeSuccess, ret);
    ASSERT_EQ(1, infos.size());
    CloneInfo cInfo = infos[0].GetCloneInfo();
    ASSERT_EQ("uuid1", cInfo.GetTaskId());
    ASSERT_EQ(user, cInfo.GetUser());
    ASSERT_EQ(CloneTaskType::kClone, cInfo.GetTaskType());
    ASSERT_EQ(source, cInfo.GetSrc());
    ASSERT_EQ(destination, cInfo.GetDest());
    ASSERT_EQ(CloneFileType::kSnapshot, cInfo.GetFileType());
    ASSERT_EQ(lazyFlag, cInfo.GetIsLazy());
    ASSERT_EQ(CloneStatus::error, cInfo.GetStatus());
}

TEST_F(TestCloneServiceManager, TestGetCloneTaskInfoFailCanNotReach) {
    const UUID source = "uuid1";
    const std::string user = "user1";
    const std::string destination = "file1";
    bool lazyFlag = true;

    CloneInfo cloneInfo("uuid1", user, CloneTaskType::kClone,
        source, destination, CloneFileType::kSnapshot, lazyFlag);
    cloneInfo.SetStatus(CloneStatus::cloning);

    std::vector<CloneInfo> cloneInfos;
    cloneInfos.push_back(cloneInfo);
    EXPECT_CALL(*cloneCore_, GetCloneInfoList(_))
        .WillOnce(DoAll(SetArgPointee<0>(cloneInfos),
            Return(kErrCodeSuccess)));

    EXPECT_CALL(*cloneCore_, GetCloneInfo(_, _))
        .WillOnce(DoAll(SetArgPointee<1>(cloneInfo),
            Return(kErrCodeSuccess)));

    std::vector<TaskCloneInfo> infos;
    int ret = manager_->GetCloneTaskInfo(user, nullptr, &infos);

    ASSERT_EQ(kErrCodeSuccess, ret);
}

TEST_F(TestCloneServiceManager, TestGetCloneTaskInfoFailOnGetCloneInfo) {
    const UUID source = "uuid1";
    const std::string user = "user1";
    const std::string destination = "file1";
    bool lazyFlag = true;

    CloneInfo cloneInfo("uuid1", user, CloneTaskType::kClone,
        source, destination, CloneFileType::kSnapshot, lazyFlag);
    cloneInfo.SetStatus(CloneStatus::cloning);

    std::vector<CloneInfo> cloneInfos;
    cloneInfos.push_back(cloneInfo);
    EXPECT_CALL(*cloneCore_, GetCloneInfoList(_))
        .WillOnce(DoAll(SetArgPointee<0>(cloneInfos),
            Return(kErrCodeSuccess)));

    EXPECT_CALL(*cloneCore_, GetCloneInfo(_, _))
        .WillOnce(DoAll(SetArgPointee<1>(cloneInfo),
             Return(kErrCodeInternalError)));

    std::vector<TaskCloneInfo> infos;
    int ret = manager_->GetCloneTaskInfo(user, nullptr, &infos);

    ASSERT_EQ(kErrCodeInternalError, ret);
}

TEST_F(TestCloneServiceManager, TestRecoverCloneTaskGetCloneInfoListFail) {
    const UUID source = "uuid1";
    const std::string user = "user1";
    const std::string destination = "file1";
    bool lazyFlag = true;
    CloneInfo cloneInfo("uuid1", user, CloneTaskType::kClone,
        source, destination, CloneFileType::kSnapshot, lazyFlag);

    std::vector<CloneInfo> cloneInfos;
    cloneInfos.push_back(cloneInfo);
    EXPECT_CALL(*cloneCore_, GetCloneInfoList(_))
        .WillOnce(DoAll(SetArgPointee<0>(cloneInfos),
            Return(kErrCodeInternalError)));

    int ret = manager_->RecoverCloneTask();
    ASSERT_EQ(kErrCodeInternalError, ret);
}

TEST_F(TestCloneServiceManager, TestRecoverCloneTaskPushTaskFail) {
    const UUID source = "uuid1";
    const std::string user = "user1";
    const std::string destination = "file1";
    bool lazyFlag = true;
    CloneInfo cloneInfo("uuid1", user, CloneTaskType::kClone,
        source, destination, CloneFileType::kSnapshot, lazyFlag);

    std::vector<CloneInfo> cloneInfos;
    cloneInfos.push_back(cloneInfo);
    EXPECT_CALL(*cloneCore_, GetCloneInfoList(_))
        .WillOnce(DoAll(SetArgPointee<0>(cloneInfos),
            Return(kErrCodeSuccess)));

    std::shared_ptr<SnapshotReference> snapRef =
        std::make_shared<SnapshotReference>();
    EXPECT_CALL(*cloneCore_, GetSnapshotRef())
        .WillOnce(Return(snapRef));

    manager_->Stop();

    int ret = manager_->RecoverCloneTask();
    ASSERT_EQ(kErrCodeServiceIsStop, ret);
}

TEST_F(TestCloneServiceManager, TestRecoverCloneTaskDefaultSuccess) {
    const UUID source = "uuid1";
    const std::string user = "user1";
    const std::string destination = "file1";
    bool lazyFlag = true;
    CloneInfo cloneInfo("uuid1", user, CloneTaskType::kClone,
        source, destination, CloneFileType::kSnapshot, lazyFlag);
    cloneInfo.SetStatus(CloneStatus::done);

    std::vector<CloneInfo> cloneInfos;
    cloneInfos.push_back(cloneInfo);
    EXPECT_CALL(*cloneCore_, GetCloneInfoList(_))
        .WillOnce(DoAll(SetArgPointee<0>(cloneInfos),
            Return(kErrCodeSuccess)));

    int ret = manager_->RecoverCloneTask();
    ASSERT_EQ(kErrCodeSuccess, ret);
}

}  // namespace snapshotcloneserver
}  // namespace curve

