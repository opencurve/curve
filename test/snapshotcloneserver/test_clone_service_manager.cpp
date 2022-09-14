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
 * Created Date: Fri 12 Apr 2019 09:27:15 PM CST
 * Author: xuchaojie
 */

#include <gtest/gtest.h>
#include <gmock/gmock.h>

#include "src/snapshotcloneserver/clone/clone_service_manager.h"
#include "src/common/snapshotclone/snapshotclone_define.h"

#include "test/snapshotcloneserver/mock_snapshot_server.h"
#include "src/common/concurrent/count_down_event.h"
#include "src/snapshotcloneserver/common/snapshotclone_metric.h"

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

static const char* kDefaultPoolset = "poolset";

class TestCloneServiceManager : public ::testing::Test {
 public:
    TestCloneServiceManager() {}
    virtual ~TestCloneServiceManager() {}

    virtual void SetUp() {
        cloneCore_ = std::make_shared<MockCloneCore>();
        cloneMetric_ = std::make_shared<CloneMetric>();
        cloneServiceManagerBackend_ =
            std::make_shared<MockCloneServiceManagerBackend>();
        std::shared_ptr<CloneTaskManager> cloneTaskMgr_ =
            std::make_shared<CloneTaskManager>(cloneCore_, cloneMetric_);

        option_.stage1PoolThreadNum = 3;
        option_.stage2PoolThreadNum = 3;
        option_.commonPoolThreadNum = 3;
        option_.cloneTaskManagerScanIntervalMs = 100;
        option_.backEndReferenceRecordScanIntervalMs = 100;
        option_.backEndReferenceFuncScanIntervalMs = 1000;

        manager_ = std::make_shared<CloneServiceManager>(
            cloneTaskMgr_, cloneCore_, cloneServiceManagerBackend_);

        EXPECT_CALL(*cloneServiceManagerBackend_, Init(_, _))
            .Times(1);

        ASSERT_EQ(0, manager_->Init(option_))
            << "manager init fail.";

        EXPECT_CALL(*cloneServiceManagerBackend_, Start())
            .Times(1);
        ASSERT_EQ(0, manager_->Start())
            << "manager start fail.";
    }

    virtual void TearDown() {
        EXPECT_CALL(*cloneServiceManagerBackend_, Stop())
            .Times(1);
        manager_->Stop();
        cloneServiceManagerBackend_ = nullptr;
        cloneCore_ = nullptr;
        cloneMetric_ = nullptr;
        manager_ = nullptr;
    }

 protected:
    std::shared_ptr<MockCloneCore> cloneCore_;
    std::shared_ptr<CloneServiceManager> manager_;
    std::shared_ptr<CloneMetric> cloneMetric_;
    std::shared_ptr<MockCloneServiceManagerBackend> cloneServiceManagerBackend_;
    SnapshotCloneServerOptions option_;
};


TEST_F(TestCloneServiceManager,
    TestCloneFileSuccess) {
    const UUID source = "uuid1";
    const std::string user = "user1";
    const std::string destination = "file1";
    bool lazyFlag = true;

    CloneInfo cloneInfo("uuid1", user, CloneTaskType::kClone,
        source, destination, "", CloneFileType::kSnapshot, lazyFlag);

    EXPECT_CALL(*cloneCore_, CloneOrRecoverPre(
            source, user, destination, lazyFlag, CloneTaskType::kClone,
            kDefaultPoolset, _))
        .WillOnce(DoAll(
            SetArgPointee<6>(cloneInfo),
            Return(kErrCodeSuccess)));

    CountDownEvent cond1(2);

    EXPECT_CALL(*cloneCore_, HandleCloneOrRecoverTask(_))
        .Times(2)
        .WillRepeatedly(Invoke([&cond1] (std::shared_ptr<CloneTaskInfo> task) {
                task->GetCloneInfo().SetStatus(CloneStatus::done);
                                task->Finish();
                                cond1.Signal();
                            }));

    TaskIdType taskId;
    auto closure = std::make_shared<CloneClosure>();
    int ret = manager_->CloneFile(
        source,
        user,
        destination,
        kDefaultPoolset,
        lazyFlag,
        closure,
        &taskId);
    ASSERT_EQ(kErrCodeSuccess, ret);

    EXPECT_CALL(*cloneCore_, FlattenPre(user, taskId, _))
        .WillOnce(Return(kErrCodeSuccess));

    // Flatten
    ret = manager_->Flatten(user, taskId);
    ASSERT_EQ(kErrCodeSuccess, ret);

    cond1.Wait();

    std::this_thread::sleep_for(
        std::chrono::milliseconds(option_.cloneTaskManagerScanIntervalMs * 2));
    ASSERT_EQ(0, cloneMetric_->cloneDoing.get_value());
    ASSERT_EQ(1, cloneMetric_->cloneSucceed.get_value());
    ASSERT_EQ(0, cloneMetric_->cloneFailed.get_value());
    ASSERT_EQ(0, cloneMetric_->flattenDoing.get_value());
    ASSERT_EQ(1, cloneMetric_->flattenSucceed.get_value());
    ASSERT_EQ(0, cloneMetric_->flattenFailed.get_value());
}

TEST_F(TestCloneServiceManager,
    TestCloneFilePreFail) {
    const UUID source = "uuid1";
    const std::string user = "user1";
    const std::string destination = "file1";
    bool lazyFlag = true;

    CloneInfo cloneInfo;

    EXPECT_CALL(*cloneCore_, CloneOrRecoverPre(
            source, user, destination, lazyFlag, CloneTaskType::kClone,
            kDefaultPoolset, _))
        .WillOnce(DoAll(
            SetArgPointee<6>(cloneInfo),
            Return(kErrCodeInternalError)));

    TaskIdType taskId;
    auto closure = std::make_shared<CloneClosure>();
    int ret = manager_->CloneFile(
        source,
        user,
        destination,
        kDefaultPoolset,
        lazyFlag,
        closure,
        &taskId);
    ASSERT_EQ(kErrCodeInternalError, ret);
}

struct FakeCloneClosure : public CloneClosure {
    FakeCloneClosure()
     : taskId_(""),
       errorCode_(-1),
       callNum_(0) {}
    void Run() {
        callNum_++;
        taskId_ = GetTaskId();
        errorCode_ = GetErrCode();
    }
    TaskIdType taskId_;
    int errorCode_;
    int callNum_;
};

TEST_F(TestCloneServiceManager, TestCloneFileSuccessByTaskExist) {
    const UUID source = "uuid1";
    const std::string user = "user1";
    const std::string destination = "file1";
    bool lazyFlag = true;

    TaskIdType expectUuid = "abc321";
    CloneInfo cloneInfo;
    cloneInfo.SetTaskId(expectUuid);

    EXPECT_CALL(*cloneCore_, CloneOrRecoverPre(
            source, user, destination, lazyFlag, CloneTaskType::kClone,
            kDefaultPoolset, _))
        .WillOnce(DoAll(
            SetArgPointee<6>(cloneInfo),
            Return(kErrCodeTaskExist)));

    TaskIdType taskId;
    auto closure = std::make_shared<FakeCloneClosure>();
    int ret = manager_->CloneFile(
        source,
        user,
        destination,
        kDefaultPoolset,
        lazyFlag,
        closure,
        &taskId);
    ASSERT_EQ(kErrCodeSuccess, ret);
    ASSERT_EQ(expectUuid, taskId);
    ASSERT_EQ(1, closure->callNum_);
    ASSERT_EQ(expectUuid, closure->taskId_);
    ASSERT_EQ(kErrCodeSuccess, closure->errorCode_);
}

TEST_F(TestCloneServiceManager,
    TestCloneFilePushTaskFail) {
    const UUID source = "uuid1";
    const std::string user = "user1";
    const std::string destination = "file1";
    bool lazyFlag = true;

    CloneInfo cloneInfo("uuid1", user, CloneTaskType::kClone,
        source, destination, kDefaultPoolset,
        CloneFileType::kSnapshot, lazyFlag);

    EXPECT_CALL(*cloneCore_, CloneOrRecoverPre(
            source, user, destination, lazyFlag, CloneTaskType::kClone,
            kDefaultPoolset, _))
        .WillRepeatedly(DoAll(
            SetArgPointee<6>(cloneInfo),
            Return(kErrCodeSuccess)));

    CountDownEvent cond1(1);

    EXPECT_CALL(*cloneCore_, HandleCloneOrRecoverTask(_))
        .WillOnce(Invoke([&cond1] (std::shared_ptr<CloneTaskInfo> task) {
                task->GetCloneInfo().SetStatus(CloneStatus::done);
                task->GetClosure()->Run();
                                cond1.Signal();
                            }));

    TaskIdType taskId;
    auto closure = std::make_shared<CloneClosure>();
    int ret = manager_->CloneFile(
        source,
        user,
        destination,
        kDefaultPoolset,
        lazyFlag,
        closure,
        &taskId);
    ASSERT_EQ(kErrCodeSuccess, ret);

    ret = manager_->CloneFile(
        source,
        user,
        destination,
        kDefaultPoolset,
        lazyFlag,
        closure,
        &taskId);
    ASSERT_EQ(kErrCodeTaskExist, ret);

    cond1.Wait();

    std::this_thread::sleep_for(
        std::chrono::milliseconds(option_.cloneTaskManagerScanIntervalMs * 2));
    ASSERT_EQ(1, cloneMetric_->cloneDoing.get_value());
    ASSERT_EQ(0, cloneMetric_->cloneSucceed.get_value());
    ASSERT_EQ(0, cloneMetric_->cloneFailed.get_value());
}

TEST_F(TestCloneServiceManager,
    TestRecoverFileSuccess) {
    const UUID source = "uuid1";
    const std::string user = "user1";
    const std::string destination = "file1";
    bool lazyFlag = true;

    CloneInfo cloneInfo("uuid1", user, CloneTaskType::kRecover,
        source, destination, "", CloneFileType::kSnapshot, lazyFlag);

    EXPECT_CALL(*cloneCore_, CloneOrRecoverPre(
            source, user, destination, lazyFlag, CloneTaskType::kRecover,
            "", _))
        .WillOnce(DoAll(
            SetArgPointee<6>(cloneInfo),
            Return(kErrCodeSuccess)));

    CountDownEvent cond1(2);

    EXPECT_CALL(*cloneCore_, HandleCloneOrRecoverTask(_))
        .Times(2)
        .WillRepeatedly(Invoke([&cond1] (std::shared_ptr<CloneTaskInfo> task) {
                task->GetCloneInfo().SetStatus(CloneStatus::done);
                                task->Finish();
                                cond1.Signal();
                            }));

    TaskIdType taskId;
    auto closure = std::make_shared<CloneClosure>();
    int ret = manager_->RecoverFile(
        source,
        user,
        destination,
        lazyFlag,
        closure,
        &taskId);
    ASSERT_EQ(kErrCodeSuccess, ret);

    EXPECT_CALL(*cloneCore_, FlattenPre(user, taskId, _))
        .WillOnce(Return(kErrCodeSuccess));

    // Flatten
    ret = manager_->Flatten(user, taskId);
    ASSERT_EQ(kErrCodeSuccess, ret);

    cond1.Wait();

    std::this_thread::sleep_for(
        std::chrono::milliseconds(option_.cloneTaskManagerScanIntervalMs * 2));
    ASSERT_EQ(0, cloneMetric_->recoverDoing.get_value());
    ASSERT_EQ(1, cloneMetric_->recoverSucceed.get_value());
    ASSERT_EQ(0, cloneMetric_->recoverFailed.get_value());
}

TEST_F(TestCloneServiceManager,
    TestRecoverFilePreFail) {
    const UUID source = "uuid1";
    const std::string user = "user1";
    const std::string destination = "file1";
    bool lazyFlag = true;

    CloneInfo cloneInfo;

    EXPECT_CALL(*cloneCore_, CloneOrRecoverPre(
            source, user, destination, lazyFlag, CloneTaskType::kRecover,
            "", _))
        .WillOnce(DoAll(
            SetArgPointee<6>(cloneInfo),
            Return(kErrCodeInternalError)));

    TaskIdType taskId;
    auto closure = std::make_shared<CloneClosure>();
    int ret = manager_->RecoverFile(
        source,
        user,
        destination,
        lazyFlag,
        closure,
        &taskId);
    ASSERT_EQ(kErrCodeInternalError, ret);
}

TEST_F(TestCloneServiceManager,
    TestRecoverFileSuccessByTaskExist) {
    const UUID source = "uuid1";
    const std::string user = "user1";
    const std::string destination = "file1";
    bool lazyFlag = true;

    TaskIdType expectUuid = "abc321";
    CloneInfo cloneInfo;
    cloneInfo.SetTaskId(expectUuid);

    EXPECT_CALL(*cloneCore_, CloneOrRecoverPre(
            source, user, destination, lazyFlag, CloneTaskType::kRecover,
            "", _))
        .WillOnce(DoAll(
            SetArgPointee<6>(cloneInfo),
            Return(kErrCodeTaskExist)));

    TaskIdType taskId;
    auto closure = std::make_shared<FakeCloneClosure>();
    int ret = manager_->RecoverFile(
        source,
        user,
        destination,
        lazyFlag,
        closure,
        &taskId);
    ASSERT_EQ(kErrCodeSuccess, ret);
    ASSERT_EQ(expectUuid, taskId);
    ASSERT_EQ(1, closure->callNum_);
    ASSERT_EQ(expectUuid, closure->taskId_);
    ASSERT_EQ(kErrCodeSuccess, closure->errorCode_);
}

TEST_F(TestCloneServiceManager,
    TestRecoverFilePushTaskFail) {
    const UUID source = "uuid1";
    const std::string user = "user1";
    const std::string destination = "file1";
    bool lazyFlag = true;

    CloneInfo cloneInfo("uuid1", user, CloneTaskType::kRecover,
        source, destination, "",
        CloneFileType::kSnapshot, lazyFlag);

    EXPECT_CALL(*cloneCore_, CloneOrRecoverPre(
            source, user, destination, lazyFlag, CloneTaskType::kRecover,
            "", _))
        .WillRepeatedly(DoAll(
            SetArgPointee<6>(cloneInfo),
            Return(kErrCodeSuccess)));

    CountDownEvent cond1(1);

    EXPECT_CALL(*cloneCore_, HandleCloneOrRecoverTask(_))
        .WillOnce(Invoke([&cond1] (std::shared_ptr<CloneTaskInfo> task) {
                task->GetCloneInfo().SetStatus(CloneStatus::done);
                task->GetClosure()->Run();
                                cond1.Signal();
                            }));

    TaskIdType taskId;
    auto closure = std::make_shared<CloneClosure>();
    int ret = manager_->RecoverFile(
        source,
        user,
        destination,
        lazyFlag,
        closure,
        &taskId);
    ASSERT_EQ(kErrCodeSuccess, ret);

    ret = manager_->RecoverFile(
        source,
        user,
        destination,
        lazyFlag,
        closure,
        &taskId);
    ASSERT_EQ(kErrCodeTaskExist, ret);

    cond1.Wait();

    std::this_thread::sleep_for(
        std::chrono::milliseconds(option_.cloneTaskManagerScanIntervalMs * 2));
    ASSERT_EQ(1, cloneMetric_->recoverDoing.get_value());
    ASSERT_EQ(0, cloneMetric_->recoverSucceed.get_value());
    ASSERT_EQ(0, cloneMetric_->recoverFailed.get_value());
}

TEST_F(TestCloneServiceManager, TestGetCloneTaskInfoSuccess) {
    const UUID source = "uuid1";
    const std::string user = "user1";
    const std::string destination = "file1";
    bool lazyFlag = true;

    CloneInfo cloneInfo("uuid1", user, CloneTaskType::kClone,
        source, destination, kDefaultPoolset,
        CloneFileType::kSnapshot, lazyFlag);

    EXPECT_CALL(*cloneCore_, CloneOrRecoverPre(
            source, user, destination, lazyFlag, CloneTaskType::kClone,
            kDefaultPoolset, _))
        .WillOnce(DoAll(
            SetArgPointee<6>(cloneInfo),
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
    auto closure = std::make_shared<CloneClosure>();
    int ret = manager_->CloneFile(
        source,
        user,
        destination,
        kDefaultPoolset,
        lazyFlag,
        closure,
        &taskId);
    ASSERT_EQ(kErrCodeSuccess, ret);

    std::vector<CloneInfo> cloneInfos;
    cloneInfos.push_back(cloneInfo);
    EXPECT_CALL(*cloneCore_, GetCloneInfoList(_))
        .WillOnce(DoAll(SetArgPointee<0>(cloneInfos),
            Return(kErrCodeSuccess)));

    std::vector<TaskCloneInfo> infos;
    ret = manager_->GetCloneTaskInfo(user, &infos);
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

TEST_F(TestCloneServiceManager, GetCloneTaskInfoByFilterSuccess) {
    const UUID source = "uuid1";
    const std::string user = "user1";
    const std::string destination = "file1";
    bool lazyFlag = true;

    CloneInfo cloneInfo("uuid1", user, CloneTaskType::kClone,
        source, destination, kDefaultPoolset,
        CloneFileType::kSnapshot, lazyFlag);
    cloneInfo.SetStatus(CloneStatus::done);

    std::vector<CloneInfo> cloneInfos;
    cloneInfos.push_back(cloneInfo);
    EXPECT_CALL(*cloneCore_, GetCloneInfoList(_))
        .WillOnce(DoAll(SetArgPointee<0>(cloneInfos),
            Return(kErrCodeSuccess)));

    CloneFilterCondition filter;
    std::vector<TaskCloneInfo> infos;
    auto ret = manager_->GetCloneTaskInfoByFilter(filter, &infos);

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
    ASSERT_EQ(kProgressCloneComplete, infos[0].GetCloneProgress());
}

TEST_F(TestCloneServiceManager, GetCloneTaskInfoByFilterFail) {
    const UUID source = "uuid1";
    const std::string user = "user1";
    const std::string destination = "file1";
    bool lazyFlag = true;

    CloneInfo cloneInfo("uuid1", user, CloneTaskType::kClone,
        source, destination, kDefaultPoolset,
        CloneFileType::kSnapshot, lazyFlag);
    cloneInfo.SetStatus(CloneStatus::done);

    std::vector<CloneInfo> cloneInfos;
    cloneInfos.push_back(cloneInfo);
    EXPECT_CALL(*cloneCore_, GetCloneInfoList(_))
        .WillRepeatedly(DoAll(SetArgPointee<0>(cloneInfos),
            Return(kErrCodeSuccess)));

    std::string type = "wrongType";
    CloneFilterCondition filter;
    filter.SetType(&type);
    std::vector<TaskCloneInfo> infos;
    auto ret = manager_->GetCloneTaskInfoByFilter(filter, &infos);

    ASSERT_EQ(kErrCodeSuccess, ret);
    ASSERT_EQ(0, infos.size());

    std::string type1 = "1";
    filter.SetType(&type1);
    ret = manager_->GetCloneTaskInfoByFilter(filter, &infos);

    ASSERT_EQ(kErrCodeSuccess, ret);
    ASSERT_EQ(0, infos.size());

    std::string type2 = "0";
    filter.SetType(&type2);
    ret = manager_->GetCloneTaskInfoByFilter(filter, &infos);

    ASSERT_EQ(kErrCodeSuccess, ret);
    ASSERT_EQ(1, infos.size());
}

TEST_F(TestCloneServiceManager, TestGetCloneTaskInfoByUUIDSuccess) {
    const UUID uuid = "uuid1";
    const UUID source = "src";
    const std::string user = "user1";
    const std::string destination = "file1";
    bool lazyFlag = true;

    CloneInfo cloneInfo(uuid, user, CloneTaskType::kClone,
        source, destination, kDefaultPoolset,
        CloneFileType::kSnapshot, lazyFlag);

    EXPECT_CALL(*cloneCore_, CloneOrRecoverPre(
            source, user, destination, lazyFlag, CloneTaskType::kClone,
            kDefaultPoolset, _))
        .WillOnce(DoAll(
            SetArgPointee<6>(cloneInfo),
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
    auto closure = std::make_shared<CloneClosure>();
    int ret = manager_->CloneFile(
        source,
        user,
        destination,
        kDefaultPoolset,
        lazyFlag,
        closure,
        &taskId);
    ASSERT_EQ(kErrCodeSuccess, ret);

    EXPECT_CALL(*cloneCore_, GetCloneInfo(_, _))
        .WillOnce(DoAll(SetArgPointee<1>(cloneInfo),
            Return(kErrCodeSuccess)));

    std::vector<TaskCloneInfo> infos;
    ret = manager_->GetCloneTaskInfoById(user, uuid, &infos);
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

TEST_F(TestCloneServiceManager, TestGetCloneTaskInfoByFileNameSuccess) {
    const UUID source = "uuid1";
    const std::string user = "user1";
    const std::string destination = "file1";
    bool lazyFlag = true;

    CloneInfo cloneInfo("uuid1", user, CloneTaskType::kClone,
        source, destination, kDefaultPoolset,
        CloneFileType::kSnapshot, lazyFlag);

    EXPECT_CALL(*cloneCore_, CloneOrRecoverPre(
            source, user, destination, lazyFlag, CloneTaskType::kClone,
            kDefaultPoolset, _))
        .WillOnce(DoAll(
            SetArgPointee<6>(cloneInfo),
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
    auto closure = std::make_shared<CloneClosure>();
    int ret = manager_->CloneFile(
        source,
        user,
        destination,
        kDefaultPoolset,
        lazyFlag,
        closure,
        &taskId);
    ASSERT_EQ(kErrCodeSuccess, ret);

    std::vector<CloneInfo> list;
    list.push_back(cloneInfo);
    EXPECT_CALL(*cloneCore_, GetCloneInfoByFileName(_, _))
        .WillOnce(DoAll(SetArgPointee<1>(list),
            Return(kErrCodeSuccess)));

    std::vector<TaskCloneInfo> infos;
    ret = manager_->GetCloneTaskInfoByName(user, destination, &infos);
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

TEST_F(TestCloneServiceManager, TestGetCloneTaskInfoFailNotExist) {
    std::vector<CloneInfo> cloneInfos;
    EXPECT_CALL(*cloneCore_, GetCloneInfoList(_))
        .WillOnce(DoAll(SetArgPointee<0>(cloneInfos),
            Return(-1)));

    std::vector<TaskCloneInfo> infos;
    int ret = manager_->GetCloneTaskInfo("user1", &infos);
    ASSERT_EQ(kErrCodeFileNotExist, ret);
}

TEST_F(TestCloneServiceManager, TestGetCloneTaskInfoByUUIDFailNotExist) {
    UUID uuid = "uuid1";
    CloneInfo cloneInfo;
    EXPECT_CALL(*cloneCore_, GetCloneInfo(_, _))
        .WillOnce(DoAll(SetArgPointee<1>(cloneInfo),
            Return(-1)));

    std::vector<TaskCloneInfo> infos;
    int ret = manager_->GetCloneTaskInfoById("user1", uuid, &infos);
    ASSERT_EQ(kErrCodeFileNotExist, ret);
}

TEST_F(TestCloneServiceManager, TestGetCloneTaskInfoByFileNameFailNotExist) {
    const std::string destination = "file1";
    CloneInfo cloneInfo;
    std::vector<CloneInfo> list;
    EXPECT_CALL(*cloneCore_, GetCloneInfoByFileName(_, _))
        .WillOnce(DoAll(SetArgPointee<1>(list),
            Return(-1)));

    std::vector<TaskCloneInfo> infos;
    int ret = manager_->GetCloneTaskInfoByName(
        "user1", destination, &infos);
    ASSERT_EQ(kErrCodeFileNotExist, ret);
}

TEST_F(TestCloneServiceManager, TestRecoverCloneTaskSuccess) {
    const UUID source = "uuid1";
    const std::string user = "user1";
    const std::string destination = "file1";
    bool lazyFlag = true;
    CloneInfo cloneInfo("uuid1", user, CloneTaskType::kClone,
        source, destination, kDefaultPoolset,
        CloneFileType::kSnapshot, lazyFlag);

    // for Flatten
    CloneInfo cloneInfo2("uuid2", user, CloneTaskType::kClone,
        source, destination, kDefaultPoolset,
        1, 1, 1, CloneFileType::kSnapshot, lazyFlag,
        CloneStep::kRecoverChunk, CloneStatus::cloning);

    std::vector<CloneInfo> cloneInfos;
    cloneInfos.push_back(cloneInfo);
    cloneInfos.push_back(cloneInfo2);
    EXPECT_CALL(*cloneCore_, GetCloneInfoList(_))
        .WillOnce(DoAll(SetArgPointee<0>(cloneInfos),
            Return(kErrCodeSuccess)));

    std::shared_ptr<SnapshotReference> snapRef =
        std::make_shared<SnapshotReference>();
    EXPECT_CALL(*cloneCore_, GetSnapshotRef())
        .Times(2)
        .WillRepeatedly(Return(snapRef));

    CountDownEvent cond1(2);
    EXPECT_CALL(*cloneCore_, HandleCloneOrRecoverTask(_))
        .Times(2)
        .WillRepeatedly(Invoke([&cond1] (
            std::shared_ptr<CloneTaskInfo> task) {
                task->GetCloneInfo().SetStatus(CloneStatus::done);
                                task->Finish();
                                cond1.Signal();
                            }));

    int ret = manager_->RecoverCloneTask();
    ASSERT_EQ(kErrCodeSuccess, ret);

    cond1.Wait();

    std::this_thread::sleep_for(
        std::chrono::milliseconds(option_.cloneTaskManagerScanIntervalMs * 2));
    ASSERT_EQ(0, cloneMetric_->cloneDoing.get_value());
    ASSERT_EQ(1, cloneMetric_->cloneSucceed.get_value());
    ASSERT_EQ(0, cloneMetric_->cloneFailed.get_value());
}

TEST_F(TestCloneServiceManager, TestCloneServiceNotStart) {
    manager_->Stop();
    const UUID source = "uuid1";
    const std::string user = "user1";
    const std::string destination = "file1";
    bool lazyFlag = true;

    CloneInfo cloneInfo("uuid1", user, CloneTaskType::kClone,
        source, destination, kDefaultPoolset,
        CloneFileType::kSnapshot, lazyFlag);

    EXPECT_CALL(*cloneCore_, CloneOrRecoverPre(
            source, user, destination, lazyFlag, CloneTaskType::kClone,
            kDefaultPoolset, _))
        .WillOnce(DoAll(
            SetArgPointee<6>(cloneInfo),
            Return(kErrCodeSuccess)));

    TaskIdType taskId;
    auto closure = std::make_shared<CloneClosure>();
    int ret = manager_->CloneFile(
        source,
        user,
        destination,
        kDefaultPoolset,
        lazyFlag,
        closure,
        &taskId);
    ASSERT_EQ(kErrCodeServiceIsStop, ret);
}

TEST_F(TestCloneServiceManager, TestGetCloneTaskInfoSuccessCloneTaskDone) {
    const UUID source = "uuid1";
    const std::string user = "user1";
    const std::string destination = "file1";
    bool lazyFlag = true;

    CloneInfo cloneInfo("uuid1", user, CloneTaskType::kClone,
        source, destination, kDefaultPoolset,
        CloneFileType::kSnapshot, lazyFlag);
    cloneInfo.SetStatus(CloneStatus::done);

    std::vector<CloneInfo> cloneInfos;
    cloneInfos.push_back(cloneInfo);
    EXPECT_CALL(*cloneCore_, GetCloneInfoList(_))
        .WillOnce(DoAll(SetArgPointee<0>(cloneInfos),
            Return(kErrCodeSuccess)));

    std::vector<TaskCloneInfo> infos;
    int ret = manager_->GetCloneTaskInfo(user, &infos);

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
        source, destination, kDefaultPoolset,
        CloneFileType::kSnapshot, lazyFlag);
    cloneInfo.SetStatus(CloneStatus::error);

    std::vector<CloneInfo> cloneInfos;
    cloneInfos.push_back(cloneInfo);
    EXPECT_CALL(*cloneCore_, GetCloneInfoList(_))
        .WillOnce(DoAll(SetArgPointee<0>(cloneInfos),
            Return(kErrCodeSuccess)));

    std::vector<TaskCloneInfo> infos;
    int ret = manager_->GetCloneTaskInfo(user, &infos);

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
        source, destination, kDefaultPoolset,
        CloneFileType::kSnapshot, lazyFlag);
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
    int ret = manager_->GetCloneTaskInfo(user, &infos);

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
        source, destination, kDefaultPoolset,
        CloneFileType::kSnapshot, lazyFlag);
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
    int ret = manager_->GetCloneTaskInfo(user, &infos);

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
        source, destination, kDefaultPoolset,
        CloneFileType::kSnapshot, lazyFlag);
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
    int ret = manager_->GetCloneTaskInfo(user, &infos);

    ASSERT_EQ(kErrCodeInternalError, ret);
}

TEST_F(TestCloneServiceManager, TestGetCloneTaskInfoFailOnGetCloneInfo) {
    const UUID source = "uuid1";
    const std::string user = "user1";
    const std::string destination = "file1";
    bool lazyFlag = true;

    CloneInfo cloneInfo("uuid1", user, CloneTaskType::kClone,
        source, destination, kDefaultPoolset,
        CloneFileType::kSnapshot, lazyFlag);
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
    int ret = manager_->GetCloneTaskInfo(user, &infos);

    ASSERT_EQ(kErrCodeInternalError, ret);
}

TEST_F(TestCloneServiceManager, TestRecoverCloneTaskGetCloneInfoListFail) {
    const UUID source = "uuid1";
    const std::string user = "user1";
    const std::string destination = "file1";
    bool lazyFlag = true;
    CloneInfo cloneInfo("uuid1", user, CloneTaskType::kClone,
        source, destination, kDefaultPoolset,
        CloneFileType::kSnapshot, lazyFlag);

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
        source, destination, kDefaultPoolset,
        CloneFileType::kSnapshot, lazyFlag);

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

    std::this_thread::sleep_for(
        std::chrono::milliseconds(option_.cloneTaskManagerScanIntervalMs * 2));
    ASSERT_EQ(0, cloneMetric_->cloneDoing.get_value());
    ASSERT_EQ(0, cloneMetric_->cloneSucceed.get_value());
    ASSERT_EQ(0, cloneMetric_->cloneFailed.get_value());
}

TEST_F(TestCloneServiceManager, TestRecoverCloneTaskDefaultSuccess) {
    const UUID source = "uuid1";
    const std::string user = "user1";
    const std::string destination = "file1";
    bool lazyFlag = true;
    CloneInfo cloneInfo("uuid1", user, CloneTaskType::kClone,
        source, destination, kDefaultPoolset,
        CloneFileType::kSnapshot, lazyFlag);
    cloneInfo.SetStatus(CloneStatus::done);

    std::vector<CloneInfo> cloneInfos;
    cloneInfos.push_back(cloneInfo);
    EXPECT_CALL(*cloneCore_, GetCloneInfoList(_))
        .WillOnce(DoAll(SetArgPointee<0>(cloneInfos),
            Return(kErrCodeSuccess)));

    int ret = manager_->RecoverCloneTask();
    ASSERT_EQ(kErrCodeSuccess, ret);
}

TEST_F(TestCloneServiceManager, TestGetCloneRefStatusSuccessNoRef) {
    const UUID source = "/file";
    const UUID source2 = "/file2";
    const std::string user = "user1";
    const std::string destination = "file1";
    bool lazyFlag = true;

    // only done record
    CloneInfo cloneInfo1("uuid1", user, CloneTaskType::kClone,
        source, destination, kDefaultPoolset,
        CloneFileType::kFile, lazyFlag);
    cloneInfo1.SetStatus(CloneStatus::done);
    CloneInfo cloneInfo2("uuid2", user, CloneTaskType::kClone,
        source2, destination, kDefaultPoolset,
        CloneFileType::kFile, lazyFlag);
    cloneInfo2.SetStatus(CloneStatus::metaInstalled);

    std::vector<CloneInfo> list;
    list.push_back(cloneInfo1);
    list.push_back(cloneInfo2);
    EXPECT_CALL(*cloneCore_, GetCloneInfoList(_))
        .WillOnce(DoAll(SetArgPointee<0>(list),
            Return(kErrCodeSuccess)));

    std::vector<CloneInfo> infos;
    CloneRefStatus refStatus;
    auto ret = manager_->GetCloneRefStatus(source, &refStatus, &infos);

    ASSERT_EQ(kErrCodeSuccess, ret);
    ASSERT_EQ(refStatus, CloneRefStatus::kNoRef);
    ASSERT_EQ(0, infos.size());

    // no record found
    EXPECT_CALL(*cloneCore_, GetCloneInfoList(_))
        .WillOnce(Return(-1));
    ret = manager_->GetCloneRefStatus(source, &refStatus, &infos);

    ASSERT_EQ(kErrCodeSuccess, ret);
    ASSERT_EQ(refStatus, CloneRefStatus::kNoRef);
    ASSERT_EQ(0, infos.size());
}

TEST_F(TestCloneServiceManager, TestGetCloneRefStatusSuccessHasRef) {
    const UUID source = "/file";
    const std::string user = "user1";
    const std::string destination = "file1";
    bool lazyFlag = true;

    CloneInfo cloneInfo1("uuid1", user, CloneTaskType::kClone,
        source, destination, kDefaultPoolset,
        CloneFileType::kFile, lazyFlag);
    cloneInfo1.SetStatus(CloneStatus::done);
    CloneInfo cloneInfo2("uuid2", user, CloneTaskType::kClone,
        source, destination, kDefaultPoolset,
        CloneFileType::kFile, lazyFlag);
    cloneInfo2.SetStatus(CloneStatus::cloning);
    CloneInfo cloneInfo3("uuid3", user, CloneTaskType::kClone,
        source, destination, kDefaultPoolset,
        CloneFileType::kFile, lazyFlag);
    cloneInfo3.SetStatus(CloneStatus::metaInstalled);

    std::vector<CloneInfo> list;
    list.push_back(cloneInfo1);
    list.push_back(cloneInfo2);
    list.push_back(cloneInfo3);
    EXPECT_CALL(*cloneCore_, GetCloneInfoList(_))
        .WillOnce(DoAll(SetArgPointee<0>(list),
            Return(kErrCodeSuccess)));

    std::vector<CloneInfo> infos;
    CloneRefStatus refStatus;
    auto ret = manager_->GetCloneRefStatus(source, &refStatus, &infos);

    ASSERT_EQ(kErrCodeSuccess, ret);
    ASSERT_EQ(refStatus, CloneRefStatus::kHasRef);
    ASSERT_EQ(0, infos.size());
}

TEST_F(TestCloneServiceManager, TestGetCloneRefStatusSuccessNeedCheck) {
    const UUID source = "/file1";
    const UUID source2 = "/file2";
    const std::string user = "user1";
    const std::string destination = "file1";
    bool lazyFlag = true;

    CloneInfo cloneInfo1("uuid1", user, CloneTaskType::kClone,
        source , destination, kDefaultPoolset,
        CloneFileType::kFile, lazyFlag);
    cloneInfo1.SetStatus(CloneStatus::done);
    CloneInfo cloneInfo2("uuid2", user, CloneTaskType::kClone,
        source2, destination, kDefaultPoolset,
        CloneFileType::kFile, lazyFlag);
    cloneInfo2.SetStatus(CloneStatus::cloning);
    CloneInfo cloneInfo3("uuid3", user, CloneTaskType::kClone,
        source, destination, kDefaultPoolset,
        CloneFileType::kFile, lazyFlag);
    cloneInfo3.SetStatus(CloneStatus::metaInstalled);

    std::vector<CloneInfo> list;
    list.push_back(cloneInfo1);
    list.push_back(cloneInfo2);
    list.push_back(cloneInfo3);
    EXPECT_CALL(*cloneCore_, GetCloneInfoList(_))
        .WillOnce(DoAll(SetArgPointee<0>(list),
            Return(kErrCodeSuccess)));

    std::vector<CloneInfo> infos;
    CloneRefStatus refStatus;
    auto ret = manager_->GetCloneRefStatus(source, &refStatus, &infos);

    ASSERT_EQ(kErrCodeSuccess, ret);
    ASSERT_EQ(refStatus, CloneRefStatus::kNeedCheck);
     ASSERT_EQ(1, infos.size());
    CloneInfo cInfo = infos[0];
    ASSERT_EQ("uuid3", cInfo.GetTaskId());
    ASSERT_EQ(user, cInfo.GetUser());
    ASSERT_EQ(CloneTaskType::kClone, cInfo.GetTaskType());
    ASSERT_EQ(source, cInfo.GetSrc());
    ASSERT_EQ(destination, cInfo.GetDest());
    ASSERT_EQ(CloneFileType::kFile, cInfo.GetFileType());
    ASSERT_EQ(lazyFlag, cInfo.GetIsLazy());
    ASSERT_EQ(CloneStatus::metaInstalled, cInfo.GetStatus());
}

class TestCloneServiceManagerBackend : public ::testing::Test {
 public:
    TestCloneServiceManagerBackend() {}
    virtual ~TestCloneServiceManagerBackend() {}

    virtual void SetUp() {
        cloneCore_ = std::make_shared<MockCloneCore>();
        cloneServiceManagerBackend_ =
            std::make_shared<CloneServiceManagerBackendImpl>(cloneCore_);
     }

    virtual void TearDown() {
        cloneServiceManagerBackend_ = nullptr;
        cloneCore_ = nullptr;
    }

 protected:
    std::shared_ptr<MockCloneCore> cloneCore_;
    std::shared_ptr<CloneServiceManagerBackend> cloneServiceManagerBackend_;
};

TEST_F(TestCloneServiceManagerBackend,
    TestInitStop) {
    uint32_t backEndReferenceRecordScanIntervalMs = 100;
    uint32_t backEndReferenceFuncScanIntervalMs = 1000;
    cloneServiceManagerBackend_->Init(backEndReferenceRecordScanIntervalMs,
                                        backEndReferenceFuncScanIntervalMs);
    cloneServiceManagerBackend_->Init(backEndReferenceRecordScanIntervalMs,
                                        backEndReferenceFuncScanIntervalMs);
    cloneServiceManagerBackend_->Stop();
    cloneServiceManagerBackend_->Init(backEndReferenceRecordScanIntervalMs,
                                        backEndReferenceFuncScanIntervalMs);
    cloneServiceManagerBackend_->Stop();
    cloneServiceManagerBackend_->Stop();
}

TEST_F(TestCloneServiceManagerBackend,
    TestEmptyCloneInfoList) {
    std::vector<CloneInfo> cloneInfos;

    uint32_t backEndReferenceRecordScanIntervalMs = 100;
    uint32_t backEndReferenceFuncScanIntervalMs = 1000;
    uint32_t testCount = 5;
    cloneServiceManagerBackend_->Init(backEndReferenceRecordScanIntervalMs,
                                    backEndReferenceFuncScanIntervalMs);

    EXPECT_CALL(*cloneCore_, GetCloneInfoList(_))
        .WillRepeatedly(DoAll(SetArgPointee<0>(cloneInfos),
            Return(kErrCodeSuccess)));
    cloneServiceManagerBackend_->Start();
    usleep(testCount * backEndReferenceRecordScanIntervalMs * 1000);
    cloneServiceManagerBackend_->Stop();
}

TEST_F(TestCloneServiceManagerBackend,
    TestNoMetaInstalledClone) {
    CloneInfo cloneInfo1("taskId1", "user1", CloneTaskType::kClone,
        "source1", "destination1", kDefaultPoolset,
        CloneFileType::kSnapshot, true);
    CloneInfo cloneInfo2("taskId2", "user2", CloneTaskType::kRecover,
        "source2", "destination2", kDefaultPoolset,
        CloneFileType::kFile, false);
    std::vector<CloneInfo> cloneInfos;
    cloneInfos.push_back(cloneInfo1);
    cloneInfos.push_back(cloneInfo2);

    uint32_t backEndReferenceRecordScanIntervalMs = 100;
    uint32_t backEndReferenceFuncScanIntervalMs = 1000;
    uint32_t testCount = 5;
    cloneServiceManagerBackend_->Init(backEndReferenceRecordScanIntervalMs,
                                        backEndReferenceFuncScanIntervalMs);

    EXPECT_CALL(*cloneCore_, GetCloneInfoList(_))
        .WillRepeatedly(DoAll(SetArgPointee<0>(cloneInfos),
            Return(kErrCodeSuccess)));
    cloneServiceManagerBackend_->Start();
    usleep(testCount * backEndReferenceRecordScanIntervalMs * 1000);
    cloneServiceManagerBackend_->Stop();
}

TEST_F(TestCloneServiceManagerBackend,
    TestMetaInstalledCloneNotExist) {
    CloneInfo cloneInfo1("taskId1", "user1", CloneTaskType::kClone,
        "source1", "destination1", kDefaultPoolset,
        CloneFileType::kSnapshot, true);
    CloneInfo cloneInfo2("taskId2", "user2", CloneTaskType::kRecover,
        "source2", "destination2", "",
        0, 0, 0, CloneFileType::kFile, false,
        CloneStep::kRecoverChunk, CloneStatus::metaInstalled);
    std::vector<CloneInfo> cloneInfos;
    cloneInfos.push_back(cloneInfo1);
    cloneInfos.push_back(cloneInfo2);

    uint32_t backEndReferenceRecordScanIntervalMs = 100;
    uint32_t backEndReferenceFuncScanIntervalMs = 1000;
    uint32_t testCount = 5;
    cloneServiceManagerBackend_->Init(backEndReferenceRecordScanIntervalMs,
                                        backEndReferenceFuncScanIntervalMs);

    EXPECT_CALL(*cloneCore_, GetCloneInfoList(_))
        .WillRepeatedly(DoAll(SetArgPointee<0>(cloneInfos),
            Return(kErrCodeSuccess)));
    EXPECT_CALL(*cloneCore_, GetCloneInfo(_, _))
        .WillRepeatedly(Return(-1));
    cloneServiceManagerBackend_->Start();
    usleep(testCount * backEndReferenceRecordScanIntervalMs * 1000);
    cloneServiceManagerBackend_->Stop();
}

TEST_F(TestCloneServiceManagerBackend,
    TestMetaInstalledCloneNotMetaInstalled) {
    CloneInfo cloneInfo1("taskId1", "user1", CloneTaskType::kClone,
        "source1", "destination1", kDefaultPoolset,
        CloneFileType::kSnapshot, true);
    CloneInfo cloneInfo2("taskId2", "user2", CloneTaskType::kRecover,
        "source2", "destination2", "",
        0, 0, 0, CloneFileType::kFile, false,
        CloneStep::kRecoverChunk, CloneStatus::metaInstalled);
    std::vector<CloneInfo> cloneInfos;
    cloneInfos.push_back(cloneInfo1);
    cloneInfos.push_back(cloneInfo2);

    uint32_t backEndReferenceRecordScanIntervalMs = 100;
    uint32_t backEndReferenceFuncScanIntervalMs = 1000;
    uint32_t testCount = 5;
    cloneServiceManagerBackend_->Init(backEndReferenceRecordScanIntervalMs,
                                        backEndReferenceFuncScanIntervalMs);

    EXPECT_CALL(*cloneCore_, GetCloneInfoList(_))
        .WillRepeatedly(DoAll(SetArgPointee<0>(cloneInfos),
            Return(kErrCodeSuccess)));
    EXPECT_CALL(*cloneCore_, GetCloneInfo(_, _))
        .WillRepeatedly(DoAll(SetArgPointee<1>(cloneInfo1),
            Return(kErrCodeSuccess)));
    cloneServiceManagerBackend_->Start();
    usleep(testCount * backEndReferenceRecordScanIntervalMs * 1000);
    cloneServiceManagerBackend_->Stop();
}

TEST_F(TestCloneServiceManagerBackend,
    TestMetaInstalledCloneFileExist) {
    CloneInfo cloneInfo1("taskId1", "user1", CloneTaskType::kClone,
        "source1", "destination1", kDefaultPoolset,
        CloneFileType::kSnapshot, true);
    CloneInfo cloneInfo2("taskId2", "user2", CloneTaskType::kRecover,
        "source2", "destination2", "",
        0, 0, 0, CloneFileType::kFile, false,
        CloneStep::kRecoverChunk, CloneStatus::metaInstalled);
    std::vector<CloneInfo> cloneInfos;
    cloneInfos.push_back(cloneInfo1);
    cloneInfos.push_back(cloneInfo2);

    uint32_t backEndReferenceRecordScanIntervalMs = 100;
    uint32_t backEndReferenceFuncScanIntervalMs = 1000;
    uint32_t testCount = 5;
    cloneServiceManagerBackend_->Init(backEndReferenceRecordScanIntervalMs,
                                        backEndReferenceFuncScanIntervalMs);

    EXPECT_CALL(*cloneCore_, GetCloneInfoList(_))
        .WillRepeatedly(DoAll(SetArgPointee<0>(cloneInfos),
            Return(kErrCodeSuccess)));
    EXPECT_CALL(*cloneCore_, GetCloneInfo(_, _))
        .WillRepeatedly(DoAll(SetArgPointee<1>(cloneInfo2),
            Return(kErrCodeSuccess)));
    EXPECT_CALL(*cloneCore_, CheckFileExists(_, _))
        .WillRepeatedly(Return(kErrCodeFileExist));
    cloneServiceManagerBackend_->Start();
    usleep(testCount * backEndReferenceRecordScanIntervalMs * 1000);
    cloneServiceManagerBackend_->Stop();
}

TEST_F(TestCloneServiceManagerBackend,
    TestMetaInstalledCloneDeleteFail) {
    CloneInfo cloneInfo1("taskId1", "user1", CloneTaskType::kClone,
        "source1", "destination1", kDefaultPoolset,
        CloneFileType::kSnapshot, true);
    CloneInfo cloneInfo2("taskId2", "user2", CloneTaskType::kRecover,
        "source2", "destination2", "",
        0, 0, 0, CloneFileType::kFile, false,
        CloneStep::kRecoverChunk, CloneStatus::metaInstalled);
    std::vector<CloneInfo> cloneInfos;
    cloneInfos.push_back(cloneInfo1);
    cloneInfos.push_back(cloneInfo2);

    uint32_t backEndReferenceRecordScanIntervalMs = 100;
    uint32_t backEndReferenceFuncScanIntervalMs = 1000;
    uint32_t testCount = 5;
    cloneServiceManagerBackend_->Init(backEndReferenceRecordScanIntervalMs,
                                        backEndReferenceFuncScanIntervalMs);

    EXPECT_CALL(*cloneCore_, GetCloneInfoList(_))
        .WillRepeatedly(DoAll(SetArgPointee<0>(cloneInfos),
            Return(kErrCodeSuccess)));
    EXPECT_CALL(*cloneCore_, GetCloneInfo(_, _))
        .WillRepeatedly(DoAll(SetArgPointee<1>(cloneInfo2),
            Return(kErrCodeSuccess)));
    EXPECT_CALL(*cloneCore_, CheckFileExists(_, _))
        .WillRepeatedly(Return(kErrCodeFileNotExist));
    EXPECT_CALL(*cloneCore_, HandleDeleteCloneInfo(_))
        .WillRepeatedly(Return(kErrCodeInternalError));
    cloneServiceManagerBackend_->Start();
    usleep(testCount * backEndReferenceRecordScanIntervalMs * 1000);
    cloneServiceManagerBackend_->Stop();
}

TEST_F(TestCloneServiceManagerBackend,
    TestMetaInstalledCloneDeleteSuccess) {
    CloneInfo cloneInfo1("taskId1", "user1", CloneTaskType::kClone,
        "source1", "destination1", kDefaultPoolset,
        CloneFileType::kSnapshot, true);
    CloneInfo cloneInfo2("taskId2", "user2", CloneTaskType::kRecover,
        "source2", "destination2", "",
        0, 0, 0, CloneFileType::kFile, false,
        CloneStep::kRecoverChunk, CloneStatus::metaInstalled);
    std::vector<CloneInfo> cloneInfos;
    cloneInfos.push_back(cloneInfo1);
    cloneInfos.push_back(cloneInfo2);

    uint32_t backEndReferenceRecordScanIntervalMs = 100;
    uint32_t backEndReferenceFuncScanIntervalMs = 1000;
    uint32_t testCount = 5;
    cloneServiceManagerBackend_->Init(backEndReferenceRecordScanIntervalMs,
                                        backEndReferenceFuncScanIntervalMs);

    EXPECT_CALL(*cloneCore_, GetCloneInfoList(_))
        .WillRepeatedly(DoAll(SetArgPointee<0>(cloneInfos),
            Return(kErrCodeSuccess)));
    EXPECT_CALL(*cloneCore_, GetCloneInfo(_, _))
        .WillRepeatedly(DoAll(SetArgPointee<1>(cloneInfo2),
            Return(kErrCodeSuccess)));
    EXPECT_CALL(*cloneCore_, CheckFileExists(_, _))
        .WillRepeatedly(Return(kErrCodeFileNotExist));
    EXPECT_CALL(*cloneCore_, HandleDeleteCloneInfo(_))
        .WillRepeatedly(Return(kErrCodeSuccess));
    cloneServiceManagerBackend_->Start();
    usleep(testCount * backEndReferenceRecordScanIntervalMs * 1000);
    cloneServiceManagerBackend_->Stop();
}

}  // namespace snapshotcloneserver
}  // namespace curve
