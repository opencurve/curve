/*
 * Project: curve
 * Created Date: Wed Dec 26 2018
 * Author: xuchaojie
 * Copyright (c) 2018 netease
 */

#include <gtest/gtest.h>
#include <gmock/gmock.h>

#include "src/snapshot/snapshot_service_manager.h"
#include "src/snapshot/snapshot_define.h"

#include "test/snapshot/mock_snapshot_server.h"
#include "test/utils/count_down_event.h"

using ::curve::test::CountDownEvent;

namespace curve {
namespace snapshotserver {

using ::testing::Return;
using ::testing::_;
using ::testing::AnyOf;
using ::testing::AllOf;
using ::testing::SetArgPointee;
using ::testing::Invoke;
using ::testing::DoAll;
using ::testing::Property;

class TestSnapshotServiceManager : public ::testing::Test {
 public:
    TestSnapshotServiceManager() {}
    virtual ~TestSnapshotServiceManager() {}

    virtual void SetUp() {
        core_ =
            std::make_shared<MockSnapshotCore>();
        std::shared_ptr<SnapshotTaskManager> taskMgr_ =
            std::make_shared<SnapshotTaskManager>();
        manager_ = std::make_shared<SnapshotServiceManager>(taskMgr_, core_);

        ASSERT_EQ(0, manager_->Init())
            << "manager init fail.";
        ASSERT_EQ(0, manager_->Start())
            << "manager start fail.";
    }

    virtual void TearDown() {
        core_ = nullptr;
        manager_->Stop();
        manager_ = nullptr;
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
                Return(kErrCodeSnapshotServerSuccess)));

        CountDownEvent cond1(1);

        EXPECT_CALL(*core_, HandleCreateSnapshotTask(_))
            .WillOnce(Invoke([&cond1] (std::shared_ptr<SnapshotTaskInfo> task) {
                                    task->Finish();
                                    cond1.Signal();
                                }));

        int ret = manager_->CreateSnapshot(
            file,
            user,
            desc,
            &uuid);
        ASSERT_EQ(kErrCodeSnapshotServerSuccess, ret);

        cond1.Wait();
    }

 protected:
    std::shared_ptr<MockSnapshotCore> core_;
    std::shared_ptr<SnapshotServiceManager> manager_;
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
            Return(kErrCodeSnapshotServerSuccess)));

    CountDownEvent cond1(1);

    EXPECT_CALL(*core_, HandleCreateSnapshotTask(_))
        .WillOnce(Invoke([&cond1] (std::shared_ptr<SnapshotTaskInfo> task) {
                                task->Finish();
                                cond1.Signal();
                            }));

    int ret = manager_->CreateSnapshot(
        file,
        user,
        desc,
        &uuid);
    ASSERT_EQ(kErrCodeSnapshotServerSuccess, ret);
    ASSERT_EQ(uuid, uuidOut);

    cond1.Wait();
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
            Return(kErrCodeSnapshotInternalError)));


    int ret = manager_->CreateSnapshot(
        file,
        user,
        desc,
        &uuid);
    ASSERT_EQ(kErrCodeSnapshotInternalError, ret);
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
            Return(kErrCodeSnapshotServerSuccess)));

    EXPECT_CALL(*core_, HandleCreateSnapshotTask(_))
            .WillOnce(Invoke([] (std::shared_ptr<SnapshotTaskInfo> task) {
                                }));
    UUID uuid;
    int ret = manager_->CreateSnapshot(
        file1,
        user1,
        desc1,
        &uuid);
    ASSERT_EQ(kErrCodeSnapshotServerSuccess, ret);

    UUID uuid2;
    ret = manager_->CreateSnapshot(
        file1,
        user1,
        desc1,
        &uuid2);

    ASSERT_EQ(kErrCodeSnapshotInternalError, ret);
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
            Return(kErrCodeSnapshotServerSuccess)))
        .WillOnce(DoAll(
            SetArgPointee<3>(info2),
            Return(kErrCodeSnapshotServerSuccess)))
        .WillOnce(DoAll(
            SetArgPointee<3>(info3),
            Return(kErrCodeSnapshotServerSuccess)));

    std::condition_variable cv;
    std::mutex m;
    int count = 0;
    std::unique_lock<std::mutex> lk(m);

    EXPECT_CALL(*core_, HandleCreateSnapshotTask(_))
        .Times(3)
        .WillRepeatedly(Invoke([&cv, &m, &count] (
                        std::shared_ptr<SnapshotTaskInfo> task) {
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
    ASSERT_EQ(kErrCodeSnapshotServerSuccess, ret);

    ret = manager_->CreateSnapshot(
        file2,
        user,
        desc2,
        &uuid);
    ASSERT_EQ(kErrCodeSnapshotServerSuccess, ret);

    ret = manager_->CreateSnapshot(
        file3,
        user,
        desc3,
        &uuid);
    ASSERT_EQ(kErrCodeSnapshotServerSuccess, ret);

    cv.wait(lk, [&count](){return count == 3;});
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
            Return(kErrCodeSnapshotServerSuccess)))
        .WillOnce(DoAll(
            SetArgPointee<3>(info2),
            Return(kErrCodeSnapshotServerSuccess)))
        .WillOnce(DoAll(
            SetArgPointee<3>(info3),
            Return(kErrCodeSnapshotServerSuccess)));

    std::condition_variable cv;
    std::mutex m;
    int count = 0;
    std::unique_lock<std::mutex> lk(m);

    EXPECT_CALL(*core_, HandleCreateSnapshotTask(_))
        .Times(3)
        .WillRepeatedly(Invoke([&cv, &m, &count] (
                        std::shared_ptr<SnapshotTaskInfo> task) {
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
    ASSERT_EQ(kErrCodeSnapshotServerSuccess, ret);

    ret = manager_->CreateSnapshot(
        file1,
        user,
        desc2,
        &uuid);
    ASSERT_EQ(kErrCodeSnapshotServerSuccess, ret);

    ret = manager_->CreateSnapshot(
        file1,
        user,
        desc3,
        &uuid);
    ASSERT_EQ(kErrCodeSnapshotServerSuccess, ret);

    cv.wait(lk, [&count](){return count == 3;});
}

TEST_F(TestSnapshotServiceManager, TestDeleteSnapshotSuccess) {
    const std::string file = "file1";
    const std::string user = "user1";
    const std::string desc = "snap1";
    UUID uuid = "uuid1";

    CountDownEvent cond1(1);

    EXPECT_CALL(*core_, DeleteSnapshotPre(uuid, user, _, _))
        .WillOnce(Return(kErrCodeSnapshotServerSuccess));

    EXPECT_CALL(*core_, HandleDeleteSnapshotTask(_))
        .WillOnce(Invoke([&cond1] (std::shared_ptr<SnapshotTaskInfo> task) {
                            task->Finish();
                            cond1.Signal();
                }));

    int ret = manager_->DeleteSnapshot(uuid, user, file);
    ASSERT_EQ(kErrCodeSnapshotServerSuccess, ret);

    cond1.Wait();
}

TEST_F(TestSnapshotServiceManager, TestDeleteSnapshotPreFail) {
    const std::string file = "file1";
    const std::string user = "user1";
    const std::string desc = "snap1";
    UUID uuid = "uuid1";

    EXPECT_CALL(*core_, DeleteSnapshotPre(uuid, user, _, _))
        .WillOnce(Return(kErrCodeSnapshotInternalError));

    int ret = manager_->DeleteSnapshot(uuid, user, file);
    ASSERT_EQ(kErrCodeSnapshotInternalError, ret);
}

TEST_F(TestSnapshotServiceManager, TestDeleteSnapshotPushTaskFail) {
    const std::string file = "file1";
    const std::string user = "user1";
    const std::string desc = "snap1";
    UUID uuid = "uuid1";

    CountDownEvent cond1(1);

    EXPECT_CALL(*core_, DeleteSnapshotPre(uuid, user, _, _))
        .WillRepeatedly(Return(kErrCodeSnapshotServerSuccess));

    EXPECT_CALL(*core_, HandleDeleteSnapshotTask(_))
        .WillOnce(Invoke([&cond1] (std::shared_ptr<SnapshotTaskInfo> task) {
                            cond1.Signal();
                }));

    int ret = manager_->DeleteSnapshot(uuid, user, file);
    ASSERT_EQ(kErrCodeSnapshotServerSuccess, ret);

    cond1.Wait();

    ret = manager_->DeleteSnapshot(uuid, user, file);
    ASSERT_EQ(kErrCodeSnapshotInternalError, ret);
}

TEST_F(TestSnapshotServiceManager, TestCreateAndDeleteSnapshotSuccess) {
    const std::string file = "file1";
    const std::string user = "user1";
    const std::string desc = "snap1";
    UUID uuid = "uuid1";

    PrepareCreateSnapshot(file, user, desc, uuid);

    CountDownEvent cond1(1);

    EXPECT_CALL(*core_, DeleteSnapshotPre(uuid, user, _, _))
        .WillOnce(Return(kErrCodeSnapshotServerSuccess));

    EXPECT_CALL(*core_, HandleDeleteSnapshotTask(_))
        .WillOnce(Invoke([&cond1] (std::shared_ptr<SnapshotTaskInfo> task) {
                            task->Finish();
                            cond1.Signal();
                }));

    int ret = manager_->DeleteSnapshot(uuid, user, file);
    ASSERT_EQ(kErrCodeSnapshotServerSuccess, ret);

    cond1.Wait();
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
            Return(kErrCodeSnapshotServerSuccess)));

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
    ASSERT_EQ(kErrCodeSnapshotServerSuccess, ret);

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

    SnapshotInfo snap3(uuid3, user2, file, desc);
    snap3.SetStatus(Status::done);
    snapInfo.push_back(snap3);

    EXPECT_CALL(*core_, GetFileSnapshotInfo(file, _))
        .WillOnce(DoAll(SetArgPointee<1>(snapInfo),
                Return(kErrCodeSnapshotServerSuccess)));

    std::vector<FileSnapshotInfo> fileSnapInfo;
    ret = manager_->GetFileSnapshotInfo(file, user, &fileSnapInfo);
    ASSERT_EQ(kErrCodeSnapshotServerSuccess, ret);
    ASSERT_EQ(2, fileSnapInfo.size());

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
                Return(kErrCodeSnapshotInternalError)));

    std::vector<FileSnapshotInfo> fileSnapInfo;
    int ret = manager_->GetFileSnapshotInfo(file, user, &fileSnapInfo);
    ASSERT_EQ(kErrCodeSnapshotInternalError, ret);
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
        .WillOnce(DoAll(SetArgPointee<1>(snapInfo),
                Return(kErrCodeSnapshotServerSuccess)));

    std::vector<FileSnapshotInfo> fileSnapInfo;
    int ret = manager_->GetFileSnapshotInfo(file, user, &fileSnapInfo);
    ASSERT_EQ(kErrCodeSnapshotInternalError, ret);
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
                    Return(kErrCodeSnapshotServerSuccess)));

    CountDownEvent cond1(2);

    EXPECT_CALL(*core_, HandleCreateSnapshotTask(_))
        .WillOnce(Invoke([&cond1] (std::shared_ptr<SnapshotTaskInfo> task) {
                            task->Finish();
                            cond1.Signal();
                }));

    EXPECT_CALL(*core_, HandleDeleteSnapshotTask(_))
        .WillOnce(Invoke([&cond1] (std::shared_ptr<SnapshotTaskInfo> task) {
                            task->Finish();
                            cond1.Signal();
                }));

    int ret = manager_->RecoverSnapshotTask();
    ASSERT_EQ(kErrCodeSnapshotServerSuccess, ret);
    cond1.Wait();
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
                    Return(kErrCodeSnapshotInternalError)));

    int ret = manager_->RecoverSnapshotTask();
    ASSERT_EQ(kErrCodeSnapshotInternalError, ret);
}

TEST_F(TestSnapshotServiceManager,
    TestCancelSnapshotSuccess) {
    const std::string file = "file1";
    const std::string user = "user1";
    const std::string desc = "snap1";
    UUID uuid;
    UUID uuidOut = "abc";

    SnapshotInfo info(uuidOut, user, file, desc);
    EXPECT_CALL(*core_, CreateSnapshotPre(file, user, desc, _))
        .WillOnce(DoAll(
            SetArgPointee<3>(info),
            Return(kErrCodeSnapshotServerSuccess)));

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

    int ret = manager_->CreateSnapshot(
        file,
        user,
        desc,
        &uuid);
    ASSERT_EQ(kErrCodeSnapshotServerSuccess, ret);
    ASSERT_EQ(uuid, uuidOut);

    ret = manager_->CancelSnapshot(uuidOut,
        user,
        file);

    ASSERT_EQ(kErrCodeSnapshotServerSuccess, ret);

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
            Return(kErrCodeSnapshotServerSuccess)));

    CountDownEvent cond1(1);

    EXPECT_CALL(*core_, HandleCreateSnapshotTask(_))
        .WillOnce(Invoke([&cond1] (std::shared_ptr<SnapshotTaskInfo> task) {
                                task->Finish();
                                cond1.Signal();
                            }));

    int ret = manager_->CreateSnapshot(
        file,
        user,
        desc,
        &uuid);
    ASSERT_EQ(kErrCodeSnapshotServerSuccess, ret);
    ASSERT_EQ(uuid, uuidOut);

    std::string user2 = "user2";
    ret = manager_->CancelSnapshot(uuidOut,
        user2,
        file);

    ASSERT_EQ(kErrCodeSnapshotUserNotMatch, ret);

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
            Return(kErrCodeSnapshotServerSuccess)));

    CountDownEvent cond1(1);

    EXPECT_CALL(*core_, HandleCreateSnapshotTask(_))
        .WillOnce(Invoke([&cond1] (std::shared_ptr<SnapshotTaskInfo> task) {
                                task->Finish();
                                cond1.Signal();
                            }));

    int ret = manager_->CreateSnapshot(
        file,
        user,
        desc,
        &uuid);
    ASSERT_EQ(kErrCodeSnapshotServerSuccess, ret);
    ASSERT_EQ(uuid, uuidOut);

    std::string file2 = "file2";
    ret = manager_->CancelSnapshot(uuidOut,
        user,
        file2);

    ASSERT_EQ(kErrCodeSnapshotFileNameNotMatch, ret);

    cond1.Wait();
}

}  // namespace snapshotserver
}  // namespace curve
