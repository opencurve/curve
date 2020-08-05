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
 * Created Date: Wed Dec 26 2018
 * Author: xuchaojie
 */

#include <gtest/gtest.h>
#include <gmock/gmock.h>

#include "src/snapshotcloneserver/snapshot/snapshot_service_manager.h"
#include "src/snapshotcloneserver/common/define.h"

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

class TestSnapshotServiceManager : public ::testing::Test {
 public:
    TestSnapshotServiceManager() {}
    virtual ~TestSnapshotServiceManager() {}

    virtual void SetUp() {
        serverOption_.snapshotPoolThreadNum = 8;
        serverOption_.snapshotTaskManagerScanIntervalMs = 100;
        core_ =
            std::make_shared<MockSnapshotCore>();
        auto metaStore_ =
            std::shared_ptr<MockSnapshotCloneMetaStore>();
        snapshotMetric_ = std::make_shared<SnapshotMetric>(metaStore_);
        std::shared_ptr<SnapshotTaskManager>
            taskMgr_ =
            std::make_shared<SnapshotTaskManager>(core_, snapshotMetric_);

        manager_ = std::make_shared<SnapshotServiceManager>(taskMgr_, core_);

        ASSERT_EQ(0, manager_->Init(serverOption_))
            << "manager init fail.";
        ASSERT_EQ(0, manager_->Start())
            << "manager start fail.";
    }

    virtual void TearDown() {
        core_ = nullptr;
        manager_->Stop();
        manager_ = nullptr;
        snapshotMetric_ = nullptr;
    }

    void PrepareCreateSnapshot(
    const std::string &file,
    const std::string &user,
    const std::string &desc,
    UUID uuid) {
        SnapshotInfo info(uuid, user, file, desc);
        EXPECT_CALL(*core_, CreateSnapshotPre(file, user, desc, _))
            .WillOnce(DoAll(
                SetArgPointee<3>(info),
                Return(kErrCodeSuccess)));

        CountDownEvent cond1(1);

        EXPECT_CALL(*core_, HandleCreateSnapshotTask(_))
            .WillOnce(Invoke([&cond1] (std::shared_ptr<SnapshotTaskInfo> task) {
            task->GetSnapshotInfo().SetStatus(Status::done);
                                    task->Finish();
                                    cond1.Signal();
                                }));

        int ret = manager_->CreateSnapshot(
            file,
            user,
            desc,
            &uuid);
        ASSERT_EQ(kErrCodeSuccess, ret);

        cond1.Wait();
    }

 protected:
    std::shared_ptr<MockSnapshotCore> core_;
    std::shared_ptr<SnapshotServiceManager> manager_;
    std::shared_ptr<SnapshotMetric> snapshotMetric_;
    SnapshotCloneServerOptions serverOption_;
};

TEST_F(TestSnapshotServiceManager,
    TestCreateSnapshotSuccess) {
    const std::string file = "file1";
    const std::string user = "user1";
    const std::string desc = "snap1";
    UUID uuid;
    UUID uuidOut = "abc";

    SnapshotInfo info(uuidOut, user, file, desc);
    EXPECT_CALL(*core_, CreateSnapshotPre(file, user, desc, _))
        .WillOnce(DoAll(
            SetArgPointee<3>(info),
            Return(kErrCodeSuccess)));

    CountDownEvent cond1(1);

    EXPECT_CALL(*core_, HandleCreateSnapshotTask(_))
        .WillOnce(Invoke([&cond1] (std::shared_ptr<SnapshotTaskInfo> task) {
            task->GetSnapshotInfo().SetStatus(Status::done);
                                task->Finish();
                                cond1.Signal();
                            }));

    int ret = manager_->CreateSnapshot(
        file,
        user,
        desc,
        &uuid);
    ASSERT_EQ(kErrCodeSuccess, ret);
    ASSERT_EQ(uuid, uuidOut);

    cond1.Wait();

    std::this_thread::sleep_for(
        std::chrono::milliseconds(
            serverOption_.snapshotTaskManagerScanIntervalMs * 2));

    ASSERT_EQ(0, snapshotMetric_->snapshotWaiting.get_value());
    ASSERT_EQ(0, snapshotMetric_->snapshotDoing.get_value());
    ASSERT_EQ(1, snapshotMetric_->snapshotSucceed.get_value());
    ASSERT_EQ(0, snapshotMetric_->snapshotFailed.get_value());
}

TEST_F(TestSnapshotServiceManager,
    TestCreateSnapshotPreFail) {
    const std::string file = "file1";
    const std::string user = "user1";
    const std::string desc = "snap1";
    UUID uuid;
    UUID uuidOut = "abc";

    SnapshotInfo info(uuidOut, user, file, desc);
    EXPECT_CALL(*core_, CreateSnapshotPre(file, user, desc, _))
        .WillOnce(DoAll(
            SetArgPointee<3>(info),
            Return(kErrCodeInternalError)));


    int ret = manager_->CreateSnapshot(
        file,
        user,
        desc,
        &uuid);
    ASSERT_EQ(kErrCodeInternalError, ret);
}

TEST_F(TestSnapshotServiceManager,
    TestCreateSnapshotSuccessByTaskExist) {
    const std::string file = "file1";
    const std::string user = "user1";
    const std::string desc = "snap1";
    UUID uuid;
    UUID uuidOut = "abc";

    SnapshotInfo info(uuidOut, user, file, desc);
    EXPECT_CALL(*core_, CreateSnapshotPre(file, user, desc, _))
        .WillOnce(DoAll(
            SetArgPointee<3>(info),
            Return(kErrCodeTaskExist)));

    int ret = manager_->CreateSnapshot(
        file,
        user,
        desc,
        &uuid);
    ASSERT_EQ(kErrCodeSuccess, ret);
}

TEST_F(TestSnapshotServiceManager,
    TestCreateSnapshotPushTaskFail) {
    const std::string file1 = "file1";
    const std::string user1 = "user1";
    const std::string desc1 = "snap1";
    UUID uuid1 = "uuid1";

    SnapshotInfo info(uuid1, user1, file1, desc1);
    EXPECT_CALL(*core_, CreateSnapshotPre(file1, user1, desc1, _))
        .WillRepeatedly(DoAll(
            SetArgPointee<3>(info),
            Return(kErrCodeSuccess)));

    EXPECT_CALL(*core_, HandleCreateSnapshotTask(_))
            .WillOnce(Invoke([] (std::shared_ptr<SnapshotTaskInfo> task) {
                                }));
    UUID uuid;
    int ret = manager_->CreateSnapshot(
        file1,
        user1,
        desc1,
        &uuid);
    ASSERT_EQ(kErrCodeSuccess, ret);

    UUID uuid2;
    ret = manager_->CreateSnapshot(
        file1,
        user1,
        desc1,
        &uuid2);

    ASSERT_EQ(kErrCodeInternalError, ret);

    std::this_thread::sleep_for(
        std::chrono::milliseconds(
            serverOption_.snapshotTaskManagerScanIntervalMs * 2));

    ASSERT_EQ(0, snapshotMetric_->snapshotWaiting.get_value());
    ASSERT_EQ(1, snapshotMetric_->snapshotDoing.get_value());
    ASSERT_EQ(0, snapshotMetric_->snapshotSucceed.get_value());
    ASSERT_EQ(0, snapshotMetric_->snapshotFailed.get_value());
}

TEST_F(TestSnapshotServiceManager,
    TestCreateSnapshotMultiThreadSuccess) {
    const std::string file1 = "file1";
    const std::string file2 = "file2";
    const std::string file3 = "file3";
    const std::string user = "user1";
    const std::string desc1 = "snap1";
    const std::string desc2 = "snap2";
    const std::string desc3 = "snap3";

    UUID uuid;
    UUID uuid1 = "uuid1";
    UUID uuid2 = "uuid2";
    UUID uuid3 = "uuid3";

    SnapshotInfo info1(uuid1, user, file1, desc1);
    SnapshotInfo info2(uuid2, user, file2, desc2);
    SnapshotInfo info3(uuid3, user, file3, desc3);

    EXPECT_CALL(*core_, CreateSnapshotPre(_, _, _, _))
        .Times(3)
        .WillOnce(DoAll(
            SetArgPointee<3>(info1),
            Return(kErrCodeSuccess)))
        .WillOnce(DoAll(
            SetArgPointee<3>(info2),
            Return(kErrCodeSuccess)))
        .WillOnce(DoAll(
            SetArgPointee<3>(info3),
            Return(kErrCodeSuccess)));

    std::condition_variable cv;
    std::mutex m;
    int count = 0;
    std::unique_lock<std::mutex> lk(m);

    EXPECT_CALL(*core_, HandleCreateSnapshotTask(_))
        .Times(3)
        .WillRepeatedly(Invoke([&cv, &m, &count] (
                        std::shared_ptr<SnapshotTaskInfo> task) {
            task->GetSnapshotInfo().SetStatus(Status::done);
                                std::unique_lock<std::mutex> lk(m);
                                count++;
                                task->Finish();
                                cv.notify_all();
                            }));


    int ret = manager_->CreateSnapshot(
        file1,
        user,
        desc1,
        &uuid);
    ASSERT_EQ(kErrCodeSuccess, ret);

    ret = manager_->CreateSnapshot(
        file2,
        user,
        desc2,
        &uuid);
    ASSERT_EQ(kErrCodeSuccess, ret);

    ret = manager_->CreateSnapshot(
        file3,
        user,
        desc3,
        &uuid);
    ASSERT_EQ(kErrCodeSuccess, ret);

    cv.wait(lk, [&count](){return count == 3;});


    std::this_thread::sleep_for(
        std::chrono::milliseconds(
            serverOption_.snapshotTaskManagerScanIntervalMs * 2));
    ASSERT_EQ(0, snapshotMetric_->snapshotWaiting.get_value());
    ASSERT_EQ(0, snapshotMetric_->snapshotDoing.get_value());
    ASSERT_EQ(3, snapshotMetric_->snapshotSucceed.get_value());
    ASSERT_EQ(0, snapshotMetric_->snapshotFailed.get_value());
}

TEST_F(TestSnapshotServiceManager,
    TestCreateSnapshotMultiThreadSameFileSuccess) {
    const std::string file1 = "file1";
    const std::string user = "user1";
    const std::string desc1 = "snap1";
    const std::string desc2 = "snap2";
    const std::string desc3 = "snap3";

    UUID uuid;
    UUID uuid1 = "uuid1";
    UUID uuid2 = "uuid2";
    UUID uuid3 = "uuid3";

    SnapshotInfo info1(uuid1, user, file1, desc1);
    SnapshotInfo info2(uuid2, user, file1, desc2);
    SnapshotInfo info3(uuid3, user, file1, desc3);

    EXPECT_CALL(*core_, CreateSnapshotPre(_, _, _, _))
        .Times(3)
        .WillOnce(DoAll(
            SetArgPointee<3>(info1),
            Return(kErrCodeSuccess)))
        .WillOnce(DoAll(
            SetArgPointee<3>(info2),
            Return(kErrCodeSuccess)))
        .WillOnce(DoAll(
            SetArgPointee<3>(info3),
            Return(kErrCodeSuccess)));

    CountDownEvent cond1(3);
    EXPECT_CALL(*core_, HandleCreateSnapshotTask(_))
        .Times(3)
        .WillRepeatedly(Invoke([&cond1] (
                        std::shared_ptr<SnapshotTaskInfo> task) {
            task->GetSnapshotInfo().SetStatus(Status::done);
                                task->Finish();
                                cond1.Signal();
                            }));


    int ret = manager_->CreateSnapshot(
        file1,
        user,
        desc1,
        &uuid);
    ASSERT_EQ(kErrCodeSuccess, ret);

    ret = manager_->CreateSnapshot(
        file1,
        user,
        desc2,
        &uuid);
    ASSERT_EQ(kErrCodeSuccess, ret);

    ret = manager_->CreateSnapshot(
        file1,
        user,
        desc3,
        &uuid);
    ASSERT_EQ(kErrCodeSuccess, ret);
    cond1.Wait();

    std::this_thread::sleep_for(
        std::chrono::milliseconds(
            serverOption_.snapshotTaskManagerScanIntervalMs * 2));

    ASSERT_EQ(0, snapshotMetric_->snapshotWaiting.get_value());
    ASSERT_EQ(0, snapshotMetric_->snapshotDoing.get_value());
    ASSERT_EQ(3, snapshotMetric_->snapshotSucceed.get_value());
    ASSERT_EQ(0, snapshotMetric_->snapshotFailed.get_value());
}

TEST_F(TestSnapshotServiceManager, TestDeleteSnapshotSuccess) {
    const std::string file = "file1";
    const std::string user = "user1";
    const std::string desc = "snap1";
    UUID uuid = "uuid1";

    CountDownEvent cond1(1);

    EXPECT_CALL(*core_, DeleteSnapshotPre(uuid, user, _, _))
        .WillOnce(Return(kErrCodeSuccess));

    EXPECT_CALL(*core_, HandleDeleteSnapshotTask(_))
        .WillOnce(Invoke([&cond1] (std::shared_ptr<SnapshotTaskInfo> task) {
            task->GetSnapshotInfo().SetStatus(Status::done);
                            task->Finish();
                            cond1.Signal();
                }));

    int ret = manager_->DeleteSnapshot(uuid, user, file);
    ASSERT_EQ(kErrCodeSuccess, ret);

    cond1.Wait();
    std::this_thread::sleep_for(
        std::chrono::milliseconds(
            serverOption_.snapshotTaskManagerScanIntervalMs * 2));

    ASSERT_EQ(0, snapshotMetric_->snapshotWaiting.get_value());
    ASSERT_EQ(0, snapshotMetric_->snapshotDoing.get_value());
    ASSERT_EQ(1, snapshotMetric_->snapshotSucceed.get_value());
    ASSERT_EQ(0, snapshotMetric_->snapshotFailed.get_value());
}

// 删除转cancel用例
TEST_F(TestSnapshotServiceManager, TestDeleteSnapshotByCancelSuccess) {
    const std::string file = "file1";
    const std::string user = "user1";
    const std::string desc = "snap1";
    UUID uuid;
    UUID uuidOut = "abc";

    SnapshotInfo info(uuidOut, user, file, desc);
    EXPECT_CALL(*core_, CreateSnapshotPre(file, user, desc, _))
        .WillOnce(DoAll(
            SetArgPointee<3>(info),
            Return(kErrCodeSuccess)));

    CountDownEvent cond1(1);

    EXPECT_CALL(*core_, HandleCreateSnapshotTask(_))
        .WillOnce(Invoke([&cond1] (
                             std::shared_ptr<SnapshotTaskInfo> task) {
                                while (1) {
                                    if (task->IsCanceled()) {
                                        break;
                                    }
                                }
                                task->Finish();
                                cond1.Signal();
                            }));

    int ret = manager_->CreateSnapshot(
        file,
        user,
        desc,
        &uuid);
    ASSERT_EQ(kErrCodeSuccess, ret);
    ASSERT_EQ(uuid, uuidOut);

    EXPECT_CALL(*core_, DeleteSnapshotPre(uuid, user, _, _))
        .WillOnce(Return(kErrCodeSnapshotCannotDeleteUnfinished));

    ret = manager_->DeleteSnapshot(uuid, user, file);
    ASSERT_EQ(kErrCodeSuccess, ret);

    cond1.Wait();
}

TEST_F(TestSnapshotServiceManager, TestDeleteSnapshotByCancelByDeleteSuccess) {
    const std::string file = "file1";
    const std::string user = "user1";
    const std::string desc = "snap1";
    UUID uuid = "uuid1";

    CountDownEvent cond1(1);

    EXPECT_CALL(*core_, DeleteSnapshotPre(uuid, user, _, _))
        .WillOnce(Return(kErrCodeSnapshotCannotDeleteUnfinished))
        .WillOnce(Return(kErrCodeSuccess));

    EXPECT_CALL(*core_, HandleDeleteSnapshotTask(_))
        .WillOnce(Invoke([&cond1] (std::shared_ptr<SnapshotTaskInfo> task) {
            task->GetSnapshotInfo().SetStatus(Status::done);
                            task->Finish();
                            cond1.Signal();
                }));

    int ret = manager_->DeleteSnapshot(uuid, user, file);
    ASSERT_EQ(kErrCodeSuccess, ret);

    cond1.Wait();
    std::this_thread::sleep_for(
        std::chrono::milliseconds(
            serverOption_.snapshotTaskManagerScanIntervalMs * 2));

    ASSERT_EQ(0, snapshotMetric_->snapshotWaiting.get_value());
    ASSERT_EQ(0, snapshotMetric_->snapshotDoing.get_value());
    ASSERT_EQ(1, snapshotMetric_->snapshotSucceed.get_value());
    ASSERT_EQ(0, snapshotMetric_->snapshotFailed.get_value());
}



TEST_F(TestSnapshotServiceManager, TestDeleteSnapshotPreFail) {
    const std::string file = "file1";
    const std::string user = "user1";
    const std::string desc = "snap1";
    UUID uuid = "uuid1";

    EXPECT_CALL(*core_, DeleteSnapshotPre(uuid, user, _, _))
        .WillOnce(Return(kErrCodeInternalError));

    int ret = manager_->DeleteSnapshot(uuid, user, file);
    ASSERT_EQ(kErrCodeInternalError, ret);
}

TEST_F(TestSnapshotServiceManager, TestDeleteSnapshotPushTaskFail) {
    const std::string file = "file1";
    const std::string user = "user1";
    const std::string desc = "snap1";
    UUID uuid = "uuid1";

    CountDownEvent cond1(1);

    EXPECT_CALL(*core_, DeleteSnapshotPre(uuid, user, _, _))
        .WillRepeatedly(Return(kErrCodeSuccess));

    EXPECT_CALL(*core_, HandleDeleteSnapshotTask(_))
        .WillOnce(Invoke([&cond1] (std::shared_ptr<SnapshotTaskInfo> task) {
            task->GetSnapshotInfo().SetStatus(Status::done);
                            cond1.Signal();
                }));

    int ret = manager_->DeleteSnapshot(uuid, user, file);
    ASSERT_EQ(kErrCodeSuccess, ret);

    cond1.Wait();

    ret = manager_->DeleteSnapshot(uuid, user, file);
    ASSERT_EQ(kErrCodeInternalError, ret);
    std::this_thread::sleep_for(
        std::chrono::milliseconds(
            serverOption_.snapshotTaskManagerScanIntervalMs * 2));

    ASSERT_EQ(0, snapshotMetric_->snapshotWaiting.get_value());
    ASSERT_EQ(1, snapshotMetric_->snapshotDoing.get_value());
    ASSERT_EQ(0, snapshotMetric_->snapshotSucceed.get_value());
    ASSERT_EQ(0, snapshotMetric_->snapshotFailed.get_value());
}

TEST_F(TestSnapshotServiceManager, TestCreateAndDeleteSnapshotSuccess) {
    const std::string file = "file1";
    const std::string user = "user1";
    const std::string desc = "snap1";
    UUID uuid = "uuid1";

    PrepareCreateSnapshot(file, user, desc, uuid);

    CountDownEvent cond1(1);

    EXPECT_CALL(*core_, DeleteSnapshotPre(uuid, user, _, _))
        .WillOnce(Return(kErrCodeSuccess));

    EXPECT_CALL(*core_, HandleDeleteSnapshotTask(_))
        .WillOnce(Invoke([&cond1] (std::shared_ptr<SnapshotTaskInfo> task) {
            task->GetSnapshotInfo().SetStatus(Status::done);
                            task->Finish();
                            cond1.Signal();
                }));

    int ret = manager_->DeleteSnapshot(uuid, user, file);
    ASSERT_EQ(kErrCodeSuccess, ret);

    cond1.Wait();
    std::this_thread::sleep_for(
        std::chrono::milliseconds(
            serverOption_.snapshotTaskManagerScanIntervalMs * 2));

    ASSERT_EQ(0, snapshotMetric_->snapshotWaiting.get_value());
    ASSERT_EQ(0, snapshotMetric_->snapshotDoing.get_value());
    ASSERT_EQ(2, snapshotMetric_->snapshotSucceed.get_value());
    ASSERT_EQ(0, snapshotMetric_->snapshotFailed.get_value());
}


TEST_F(TestSnapshotServiceManager, TestGetFileSnapshotInfoSuccess) {
    const std::string file = "file1";
    const std::string user = "user1";
    const std::string desc = "snap1";
    UUID uuid;
    UUID uuidOut = "uuid1";
    uint32_t progress = 50;

    SnapshotInfo info(uuidOut, user, file, desc);
    EXPECT_CALL(*core_, CreateSnapshotPre(file, user, desc, _))
        .WillOnce(DoAll(
            SetArgPointee<3>(info),
            Return(kErrCodeSuccess)));

    CountDownEvent cond1(1);

    EXPECT_CALL(*core_, HandleCreateSnapshotTask(_))
        .WillOnce(Invoke(
                [&cond1, progress] (std::shared_ptr<SnapshotTaskInfo> task) {
                                task->SetProgress(progress);
                                cond1.Signal();
                            }));

    int ret = manager_->CreateSnapshot(
        file,
        user,
        desc,
        &uuid);
    ASSERT_EQ(kErrCodeSuccess, ret);

    cond1.Wait();


    const std::string file2 = "file2";
    const std::string desc2 = "snap2";
    UUID uuid2 = "uuid2";

    std::vector<SnapshotInfo> snapInfo;
    SnapshotInfo snap1(uuidOut, user, file, desc);
    snap1.SetStatus(Status::pending);
    snapInfo.push_back(snap1);

    SnapshotInfo snap2(uuid2, user, file2, desc2);
    snap2.SetStatus(Status::done);
    snapInfo.push_back(snap2);

    std::string user2 = "user2";
    UUID uuid3 = "uuid3";
    UUID uuid4 = "uuid4";

    SnapshotInfo snap3(uuid3, user2, file, desc);
    snap3.SetStatus(Status::done);
    snapInfo.push_back(snap3);

    SnapshotInfo snap4(uuid4, user, file, desc);
    snap4.SetStatus(Status::error);
    snapInfo.push_back(snap4);

    EXPECT_CALL(*core_, GetFileSnapshotInfo(file, _))
        .WillOnce(DoAll(SetArgPointee<1>(snapInfo),
                Return(kErrCodeSuccess)));

    std::vector<FileSnapshotInfo> fileSnapInfo;
    ret = manager_->GetFileSnapshotInfo(file, user, &fileSnapInfo);
    ASSERT_EQ(kErrCodeSuccess, ret);
    ASSERT_EQ(3, fileSnapInfo.size());

    for (auto v : fileSnapInfo) {
        SnapshotInfo s = v.GetSnapshotInfo();
        if (s.GetUuid() == uuidOut) {
            ASSERT_EQ(file, s.GetFileName());
            ASSERT_EQ(user, s.GetUser());
            ASSERT_EQ(desc, s.GetSnapshotName());
            ASSERT_EQ(Status::pending, s.GetStatus());
            ASSERT_EQ(progress, v.GetSnapProgress());
        } else if (s.GetUuid() == uuid2) {
            ASSERT_EQ(file2, s.GetFileName());
            ASSERT_EQ(user, s.GetUser());
            ASSERT_EQ(desc2, s.GetSnapshotName());
            ASSERT_EQ(Status::done, s.GetStatus());
            ASSERT_EQ(100, v.GetSnapProgress());
        } else if (s.GetUuid() == uuid4) {
            ASSERT_EQ(file, s.GetFileName());
            ASSERT_EQ(user, s.GetUser());
            ASSERT_EQ(desc, s.GetSnapshotName());
            ASSERT_EQ(Status::error, s.GetStatus());
            ASSERT_EQ(0, v.GetSnapProgress());
        } else {
            FAIL() << "should not exist this uuid = "
                   << s.GetUuid();
        }
    }
}

TEST_F(TestSnapshotServiceManager, TestGetFileSnapshotInfoFail) {
    const std::string file = "file1";
    const std::string user = "user1";
    const std::string desc = "snap1";
    UUID uuid = "uuid1";

    std::vector<SnapshotInfo> snapInfo;
    EXPECT_CALL(*core_, GetFileSnapshotInfo(file, _))
        .WillOnce(DoAll(SetArgPointee<1>(snapInfo),
                Return(kErrCodeInternalError)));

    std::vector<FileSnapshotInfo> fileSnapInfo;
    int ret = manager_->GetFileSnapshotInfo(file, user, &fileSnapInfo);
    ASSERT_EQ(kErrCodeInternalError, ret);
}

TEST_F(TestSnapshotServiceManager, TestGetFileSnapshotInfoFail2) {
    const std::string file = "file1";
    const std::string user = "user1";
    const std::string desc = "snap1";
    UUID uuid = "uuid1";

    std::vector<SnapshotInfo> snapInfo;
    SnapshotInfo snap1(uuid, user, file, desc);
    snap1.SetStatus(Status::pending);
    snapInfo.push_back(snap1);

    EXPECT_CALL(*core_, GetFileSnapshotInfo(file, _))
        .WillRepeatedly(DoAll(SetArgPointee<1>(snapInfo),
                Return(kErrCodeSuccess)));

    std::vector<FileSnapshotInfo> fileSnapInfo;
    int ret = manager_->GetFileSnapshotInfo(file, user, &fileSnapInfo);
    ASSERT_EQ(kErrCodeInternalError, ret);
}

TEST_F(TestSnapshotServiceManager, TestGetSnapshotListByFilterSuccess) {
    const std::string file = "file1";
    const std::string user = "user1";
    const std::string desc = "snap1";
    UUID uuid;
    UUID uuidOut = "uuid1";
    uint32_t progress = 50;

    SnapshotInfo info(uuidOut, user, file, desc);
    EXPECT_CALL(*core_, CreateSnapshotPre(file, user, desc, _))
        .WillOnce(DoAll(
            SetArgPointee<3>(info),
            Return(kErrCodeSuccess)));

    CountDownEvent cond1(1);

    EXPECT_CALL(*core_, HandleCreateSnapshotTask(_))
        .WillOnce(Invoke(
                [&cond1, progress] (std::shared_ptr<SnapshotTaskInfo> task) {
                                task->SetProgress(progress);
                                cond1.Signal();
                            }));

    int ret = manager_->CreateSnapshot(
        file,
        user,
        desc,
        &uuid);
    ASSERT_EQ(kErrCodeSuccess, ret);

    cond1.Wait();


    const std::string file2 = "file2";
    const std::string desc2 = "snap2";
    UUID uuid2 = "uuid2";

    std::vector<SnapshotInfo> snapInfo;
    SnapshotInfo snap1(uuidOut, user, file, desc);
    snap1.SetStatus(Status::pending);
    snapInfo.push_back(snap1);

    SnapshotInfo snap2(uuid2, user, file2, desc2);
    snap2.SetStatus(Status::done);
    snapInfo.push_back(snap2);

    std::string user2 = "user2";
    UUID uuid3 = "uuid3";
    UUID uuid4 = "uuid4";

    SnapshotInfo snap3(uuid3, user2, file, desc);
    snap3.SetStatus(Status::done);
    snapInfo.push_back(snap3);

    SnapshotInfo snap4(uuid4, user, file, desc);
    snap4.SetStatus(Status::error);
    snapInfo.push_back(snap4);

    EXPECT_CALL(*core_, GetSnapshotList(_))
        .WillOnce(DoAll(SetArgPointee<0>(snapInfo),
                Return(kErrCodeSuccess)));

    // empty filter
    SnapshotFilterCondition filter;
    std::vector<FileSnapshotInfo> fileSnapInfo;
    ret = manager_->GetSnapshotListByFilter(filter, &fileSnapInfo);
    ASSERT_EQ(kErrCodeSuccess, ret);
    ASSERT_EQ(4, fileSnapInfo.size());

    for (auto v : fileSnapInfo) {
        SnapshotInfo s = v.GetSnapshotInfo();
        if (s.GetUuid() == uuidOut) {
            ASSERT_EQ(file, s.GetFileName());
            ASSERT_EQ(user, s.GetUser());
            ASSERT_EQ(desc, s.GetSnapshotName());
            ASSERT_EQ(Status::pending, s.GetStatus());
            ASSERT_EQ(progress, v.GetSnapProgress());
        } else if (s.GetUuid() == uuid2) {
            ASSERT_EQ(file2, s.GetFileName());
            ASSERT_EQ(user, s.GetUser());
            ASSERT_EQ(desc2, s.GetSnapshotName());
            ASSERT_EQ(Status::done, s.GetStatus());
            ASSERT_EQ(100, v.GetSnapProgress());
        } else if (s.GetUuid() == uuid3) {
            ASSERT_EQ(file, s.GetFileName());
            ASSERT_EQ(user2, s.GetUser());
            ASSERT_EQ(desc, s.GetSnapshotName());
            ASSERT_EQ(Status::done, s.GetStatus());
            ASSERT_EQ(100, v.GetSnapProgress());
        } else if (s.GetUuid() == uuid4) {
            ASSERT_EQ(file, s.GetFileName());
            ASSERT_EQ(user, s.GetUser());
            ASSERT_EQ(desc, s.GetSnapshotName());
            ASSERT_EQ(Status::error, s.GetStatus());
            ASSERT_EQ(0, v.GetSnapProgress());
        } else {
            FAIL() << "should not exist this uuid = "
                   << s.GetUuid();
        }
    }

    EXPECT_CALL(*core_, GetSnapshotList(_))
        .WillOnce(DoAll(SetArgPointee<0>(snapInfo),
                Return(kErrCodeSuccess)));

    // filter uuid
    SnapshotFilterCondition filter2;
    filter2.SetUuid(&uuidOut);
    std::vector<FileSnapshotInfo> fileSnapInfo2;
    ret = manager_->GetSnapshotListByFilter(filter2, &fileSnapInfo2);
    ASSERT_EQ(kErrCodeSuccess, ret);
    ASSERT_EQ(1, fileSnapInfo2.size());

    for (auto v : fileSnapInfo2) {
        SnapshotInfo s = v.GetSnapshotInfo();
        if (s.GetUuid() == uuidOut) {
            ASSERT_EQ(file, s.GetFileName());
            ASSERT_EQ(user, s.GetUser());
            ASSERT_EQ(desc, s.GetSnapshotName());
            ASSERT_EQ(Status::pending, s.GetStatus());
            ASSERT_EQ(progress, v.GetSnapProgress());
        } else {
            FAIL() << "should not exist this uuid = "
                   << s.GetUuid();
        }
    }

    EXPECT_CALL(*core_, GetSnapshotList(_))
        .WillOnce(DoAll(SetArgPointee<0>(snapInfo),
                Return(kErrCodeSuccess)));

    // filter by filename
    SnapshotFilterCondition filter3;
    filter3.SetFile(&file);
    std::vector<FileSnapshotInfo> fileSnapInfo3;
    ret = manager_->GetSnapshotListByFilter(filter3, &fileSnapInfo3);
    ASSERT_EQ(kErrCodeSuccess, ret);
    ASSERT_EQ(3, fileSnapInfo3.size());

    for (auto v : fileSnapInfo3) {
        SnapshotInfo s = v.GetSnapshotInfo();
        if (s.GetUuid() == uuidOut) {
            ASSERT_EQ(file, s.GetFileName());
            ASSERT_EQ(user, s.GetUser());
            ASSERT_EQ(desc, s.GetSnapshotName());
            ASSERT_EQ(Status::pending, s.GetStatus());
            ASSERT_EQ(progress, v.GetSnapProgress());
        } else if (s.GetUuid() == uuid3) {
            ASSERT_EQ(file, s.GetFileName());
            ASSERT_EQ(user2, s.GetUser());
            ASSERT_EQ(desc, s.GetSnapshotName());
            ASSERT_EQ(Status::done, s.GetStatus());
            ASSERT_EQ(100, v.GetSnapProgress());
        } else if (s.GetUuid() == uuid4) {
            ASSERT_EQ(file, s.GetFileName());
            ASSERT_EQ(user, s.GetUser());
            ASSERT_EQ(desc, s.GetSnapshotName());
            ASSERT_EQ(Status::error, s.GetStatus());
            ASSERT_EQ(0, v.GetSnapProgress());
        } else {
            FAIL() << "should not exist this uuid = "
                   << s.GetUuid();
        }
    }

    EXPECT_CALL(*core_, GetSnapshotList(_))
        .WillOnce(DoAll(SetArgPointee<0>(snapInfo),
                Return(kErrCodeSuccess)));

    // filter by status
    SnapshotFilterCondition filter4;
    std::string status = "0";
    filter4.SetStatus(&status);
    std::vector<FileSnapshotInfo> fileSnapInfo4;
    ret = manager_->GetSnapshotListByFilter(filter4, &fileSnapInfo4);
    ASSERT_EQ(kErrCodeSuccess, ret);
    ASSERT_EQ(2, fileSnapInfo4.size());

    for (auto v : fileSnapInfo4) {
        SnapshotInfo s = v.GetSnapshotInfo();
        if (s.GetUuid() == uuid2) {
            ASSERT_EQ(file2, s.GetFileName());
            ASSERT_EQ(user, s.GetUser());
            ASSERT_EQ(desc2, s.GetSnapshotName());
            ASSERT_EQ(Status::done, s.GetStatus());
            ASSERT_EQ(100, v.GetSnapProgress());
        } else if (s.GetUuid() == uuid3) {
            ASSERT_EQ(file, s.GetFileName());
            ASSERT_EQ(user2, s.GetUser());
            ASSERT_EQ(desc, s.GetSnapshotName());
            ASSERT_EQ(Status::done, s.GetStatus());
            ASSERT_EQ(100, v.GetSnapProgress());
        } else {
            FAIL() << "should not exist this uuid = "
                   << s.GetUuid();
        }
    }

    EXPECT_CALL(*core_, GetSnapshotList(_))
        .WillOnce(DoAll(SetArgPointee<0>(snapInfo),
                Return(kErrCodeSuccess)));

    // filter by user
    SnapshotFilterCondition filter5;
    filter5.SetUser(&user2);
    std::vector<FileSnapshotInfo> fileSnapInfo5;
    ret = manager_->GetSnapshotListByFilter(filter5, &fileSnapInfo5);
    ASSERT_EQ(kErrCodeSuccess, ret);
    ASSERT_EQ(1, fileSnapInfo5.size());

    for (auto v : fileSnapInfo5) {
        SnapshotInfo s = v.GetSnapshotInfo();
        if (s.GetUuid() == uuid3) {
            ASSERT_EQ(file, s.GetFileName());
            ASSERT_EQ(user2, s.GetUser());
            ASSERT_EQ(desc, s.GetSnapshotName());
            ASSERT_EQ(Status::done, s.GetStatus());
            ASSERT_EQ(100, v.GetSnapProgress());
        } else {
            FAIL() << "should not exist this uuid = "
                   << s.GetUuid();
        }
    }
}

TEST_F(TestSnapshotServiceManager, TestGetSnapshotListByFilterFail) {
    const std::string file = "file1";
    const std::string user = "user1";
    const std::string desc = "snap1";
    UUID uuid = "uuid1";

    std::vector<SnapshotInfo> snapInfo;

    EXPECT_CALL(*core_, GetSnapshotList(_))
        .WillOnce(DoAll(SetArgPointee<0>(snapInfo),
                Return(kErrCodeInternalError)));

    SnapshotFilterCondition filter;
    std::vector<FileSnapshotInfo> fileSnapInfo;
    int ret = manager_->GetSnapshotListByFilter(filter, &fileSnapInfo);
    ASSERT_EQ(kErrCodeInternalError, ret);
}

TEST_F(TestSnapshotServiceManager, TestRecoverSnapshotTaskSuccess) {
    const std::string file1 = "file1";
    const std::string user1 = "user1";
    const std::string desc1 = "snap1";
    UUID uuid1 = "uuid1";
    const std::string desc2 = "snap2";
    UUID uuid2 = "uuid2";
    const std::string desc3 = "snap3";
    UUID uuid3 = "uuid3";
    std::vector<SnapshotInfo> list;
    SnapshotInfo snap1(uuid1, user1, file1, desc1);
    snap1.SetStatus(Status::pending);
    SnapshotInfo snap2(uuid2, user1, file1, desc2);
    snap2.SetStatus(Status::deleting);
    SnapshotInfo snap3(uuid3, user1, file1, desc3);
    snap3.SetStatus(Status::done);
    list.push_back(snap1);
    list.push_back(snap2);
    list.push_back(snap3);
    EXPECT_CALL(*core_, GetSnapshotList(_))
        .WillOnce(DoAll(SetArgPointee<0>(list),
                    Return(kErrCodeSuccess)));

    CountDownEvent cond1(2);

    EXPECT_CALL(*core_, HandleCreateSnapshotTask(_))
        .WillOnce(Invoke([&cond1] (std::shared_ptr<SnapshotTaskInfo> task) {
            task->GetSnapshotInfo().SetStatus(Status::done);
                            task->Finish();
                            cond1.Signal();
                }));

    EXPECT_CALL(*core_, HandleDeleteSnapshotTask(_))
        .WillOnce(Invoke([&cond1] (std::shared_ptr<SnapshotTaskInfo> task) {
            task->GetSnapshotInfo().SetStatus(Status::done);
                            task->Finish();
                            cond1.Signal();
                }));

    int ret = manager_->RecoverSnapshotTask();
    ASSERT_EQ(kErrCodeSuccess, ret);
    cond1.Wait();

    std::this_thread::sleep_for(
        std::chrono::milliseconds(
            serverOption_.snapshotTaskManagerScanIntervalMs * 2));

    ASSERT_EQ(0, snapshotMetric_->snapshotWaiting.get_value());
    ASSERT_EQ(0, snapshotMetric_->snapshotDoing.get_value());
    ASSERT_EQ(2, snapshotMetric_->snapshotSucceed.get_value());
    ASSERT_EQ(0, snapshotMetric_->snapshotFailed.get_value());
}

TEST_F(TestSnapshotServiceManager, TestRecoverSnapshotTaskFail) {
    const std::string file1 = "file1";
    const std::string user1 = "user1";
    const std::string desc1 = "snap1";
    UUID uuid1 = "uuid1";
    const std::string desc2 = "snap2";
    UUID uuid2 = "uuid2";
    std::vector<SnapshotInfo> list;
    SnapshotInfo snap1(uuid1, user1, file1, desc1);
    snap1.SetStatus(Status::pending);
    SnapshotInfo snap2(uuid2, user1, file1, desc2);
    snap2.SetStatus(Status::deleting);
    list.push_back(snap1);
    list.push_back(snap2);
    EXPECT_CALL(*core_, GetSnapshotList(_))
        .WillOnce(DoAll(SetArgPointee<0>(list),
                    Return(kErrCodeInternalError)));

    int ret = manager_->RecoverSnapshotTask();
    ASSERT_EQ(kErrCodeInternalError, ret);
}

TEST_F(TestSnapshotServiceManager,
    TestCancelSnapshotSuccess) {
    const std::string file = "file1";
    const std::string user = "user1";
    const std::string desc = "snap1";
    UUID uuid;
    UUID uuid2;
    UUID uuidOut = "abc";
    UUID uuidOut2 = "def";

    SnapshotInfo info(uuidOut, user, file, desc);
    SnapshotInfo info2(uuidOut2, user, file, desc);
    EXPECT_CALL(*core_, CreateSnapshotPre(file, user, desc, _))
        .Times(2)
        .WillOnce(DoAll(
            SetArgPointee<3>(info),
            Return(kErrCodeSuccess)))
        .WillOnce(DoAll(
            SetArgPointee<3>(info2),
            Return(kErrCodeSuccess)));

    CountDownEvent cond1(1);
    CountDownEvent cond2(1);

    EXPECT_CALL(*core_, HandleCreateSnapshotTask(_))
        .WillOnce(Invoke([&cond1, &cond2] (
                             std::shared_ptr<SnapshotTaskInfo> task) {
                                while (1) {
                                    if (task->IsCanceled()) {
                                        cond1.Signal();
                                        break;
                                    }
                                }
                                task->Finish();
                                cond2.Signal();
                            }));

    // 取消排队的快照会调一次
    EXPECT_CALL(*core_, HandleCancelUnSchduledSnapshotTask(_))
        .WillOnce(Return(kErrCodeSuccess));

    int ret = manager_->CreateSnapshot(
        file,
        user,
        desc,
        &uuid);
    ASSERT_EQ(kErrCodeSuccess, ret);
    ASSERT_EQ(uuid, uuidOut);

    // 再打一个快照，覆盖排队的情况
    ret = manager_->CreateSnapshot(
        file,
        user,
        desc,
        &uuid2);
    ASSERT_EQ(kErrCodeSuccess, ret);
    ASSERT_EQ(uuid2, uuidOut2);

    // 先取消在排队的快照
    ret = manager_->CancelSnapshot(uuidOut2,
        user,
        file);
    ASSERT_EQ(kErrCodeSuccess, ret);

    ret = manager_->CancelSnapshot(uuidOut,
        user,
        file);

    ASSERT_EQ(kErrCodeSuccess, ret);

    cond1.Wait();
    cond2.Wait();
}

TEST_F(TestSnapshotServiceManager,
    TestCancelSnapshotFailDiffUser) {
    const std::string file = "file1";
    const std::string user = "user1";
    const std::string desc = "snap1";
    UUID uuid;
    UUID uuidOut = "abc";

    SnapshotInfo info(uuidOut, user, file, desc);
    EXPECT_CALL(*core_, CreateSnapshotPre(file, user, desc, _))
        .WillOnce(DoAll(
            SetArgPointee<3>(info),
            Return(kErrCodeSuccess)));

    CountDownEvent cond1(1);
    CountDownEvent cond2(1);

    EXPECT_CALL(*core_, HandleCreateSnapshotTask(_))
        .WillOnce(Invoke([&cond1, &cond2] (
                        std::shared_ptr<SnapshotTaskInfo> task) {
                                cond2.Wait();
                                task->Finish();
                                cond1.Signal();
                            }));

    int ret = manager_->CreateSnapshot(
        file,
        user,
        desc,
        &uuid);
    ASSERT_EQ(kErrCodeSuccess, ret);
    ASSERT_EQ(uuid, uuidOut);

    std::string user2 = "user2";
    ret = manager_->CancelSnapshot(uuidOut,
        user2,
        file);
    cond2.Signal();

    ASSERT_EQ(kErrCodeInvalidUser, ret);
    cond1.Wait();
}

TEST_F(TestSnapshotServiceManager,
    TestCancelSnapshotFailDiffFile) {
    const std::string file = "file1";
    const std::string user = "user1";
    const std::string desc = "snap1";
    UUID uuid;
    UUID uuidOut = "abc";

    SnapshotInfo info(uuidOut, user, file, desc);
    EXPECT_CALL(*core_, CreateSnapshotPre(file, user, desc, _))
        .WillOnce(DoAll(
            SetArgPointee<3>(info),
            Return(kErrCodeSuccess)));

    CountDownEvent cond1(1);
    CountDownEvent cond2(1);

    EXPECT_CALL(*core_, HandleCreateSnapshotTask(_))
        .WillOnce(Invoke([&cond1, &cond2] (
                        std::shared_ptr<SnapshotTaskInfo> task) {
                                cond2.Wait();
                                task->Finish();
                                cond1.Signal();
                            }));

    int ret = manager_->CreateSnapshot(
        file,
        user,
        desc,
        &uuid);
    ASSERT_EQ(kErrCodeSuccess, ret);
    ASSERT_EQ(uuid, uuidOut);

    std::string file2 = "file2";
    ret = manager_->CancelSnapshot(uuidOut,
        user,
        file2);
    cond2.Signal();

    ASSERT_EQ(kErrCodeFileNameNotMatch, ret);
    cond1.Wait();
}


}  // namespace snapshotcloneserver
}  // namespace curve

