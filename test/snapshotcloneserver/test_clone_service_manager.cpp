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

        manager_ = std::make_shared<CloneServiceManager>(
            cloneTaskMgr_, cloneCore_);

        ASSERT_EQ(0, manager_->Init())
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
            Return(kErrCodeSnapshotServerSuccess)));

    CountDownEvent cond1(1);

    EXPECT_CALL(*cloneCore_, HandleCloneOrRecoverTask(_))
        .WillOnce(Invoke([&cond1] (std::shared_ptr<CloneTaskInfo> task) {
                                task->Finish();
                                cond1.Signal();
                            }));

    int ret = manager_->CloneFile(
        source,
        user,
        destination,
        lazyFlag);
    ASSERT_EQ(kErrCodeSnapshotServerSuccess, ret);

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
            Return(kErrCodeSnapshotInternalError)));

    int ret = manager_->CloneFile(
        source,
        user,
        destination,
        lazyFlag);
    ASSERT_EQ(kErrCodeSnapshotInternalError, ret);
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
            Return(kErrCodeSnapshotServerSuccess)));

    CountDownEvent cond1(1);

    EXPECT_CALL(*cloneCore_, HandleCloneOrRecoverTask(_))
        .WillOnce(Invoke([&cond1] (std::shared_ptr<CloneTaskInfo> task) {
                            }));

    int ret = manager_->CloneFile(
        source,
        user,
        destination,
        lazyFlag);
    ASSERT_EQ(kErrCodeSnapshotServerSuccess, ret);

    ret = manager_->CloneFile(
        source,
        user,
        destination,
        lazyFlag);
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
            Return(kErrCodeSnapshotServerSuccess)));

    CountDownEvent cond1(1);

    EXPECT_CALL(*cloneCore_, HandleCloneOrRecoverTask(_))
        .WillOnce(Invoke([&cond1] (std::shared_ptr<CloneTaskInfo> task) {
                                task->Finish();
                                cond1.Signal();
                            }));

    int ret = manager_->RecoverFile(
        source,
        user,
        destination,
        lazyFlag);
    ASSERT_EQ(kErrCodeSnapshotServerSuccess, ret);

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
            Return(kErrCodeSnapshotInternalError)));

    int ret = manager_->RecoverFile(
        source,
        user,
        destination,
        lazyFlag);
    ASSERT_EQ(kErrCodeSnapshotInternalError, ret);
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
            Return(kErrCodeSnapshotServerSuccess)));

    CountDownEvent cond1(1);

    EXPECT_CALL(*cloneCore_, HandleCloneOrRecoverTask(_))
        .WillOnce(Invoke([&cond1] (std::shared_ptr<CloneTaskInfo> task) {
                            }));

    int ret = manager_->RecoverFile(
        source,
        user,
        destination,
        lazyFlag);
    ASSERT_EQ(kErrCodeSnapshotServerSuccess, ret);

    ret = manager_->RecoverFile(
        source,
        user,
        destination,
        lazyFlag);
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
            Return(kErrCodeSnapshotServerSuccess)));

    CountDownEvent cond1(1);
    CountDownEvent cond2(1);

    EXPECT_CALL(*cloneCore_, HandleCloneOrRecoverTask(_))
        .WillOnce(Invoke([&cond1, &cond2] (
            std::shared_ptr<CloneTaskInfo> task) {
                                cond2.Wait();
                                task->Finish();
                                cond1.Signal();
                            }));

    int ret = manager_->CloneFile(
        source,
        user,
        destination,
        lazyFlag);
    ASSERT_EQ(kErrCodeSnapshotServerSuccess, ret);

    std::vector<CloneInfo> cloneInfos;
    cloneInfos.push_back(cloneInfo);
    EXPECT_CALL(*cloneCore_, GetCloneInfoList(_))
        .WillOnce(DoAll(SetArgPointee<0>(cloneInfos),
            Return(kErrCodeSnapshotServerSuccess)));

    std::vector<TaskCloneInfo> infos;
    ret = manager_->GetCloneTaskInfo(user, &infos);
    cond2.Signal();

    ASSERT_EQ(kErrCodeSnapshotServerSuccess, ret);
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
            Return(kErrCodeSnapshotInternalError)));

    std::vector<TaskCloneInfo> infos;
    int ret = manager_->GetCloneTaskInfo("user1", &infos);
    ASSERT_EQ(kErrCodeSnapshotInternalError, ret);
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
            Return(kErrCodeSnapshotServerSuccess)));

    CountDownEvent cond1(1);
    EXPECT_CALL(*cloneCore_, HandleCloneOrRecoverTask(_))
        .WillOnce(Invoke([&cond1] (
            std::shared_ptr<CloneTaskInfo> task) {
                                task->Finish();
                                cond1.Signal();
                            }));

    int ret = manager_->RecoverCloneTask();
    ASSERT_EQ(kErrCodeSnapshotServerSuccess, ret);
    cond1.Wait();
}


}  // namespace snapshotcloneserver
}  // namespace curve

