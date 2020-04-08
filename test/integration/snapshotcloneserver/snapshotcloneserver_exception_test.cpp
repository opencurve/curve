/*
 * Project: curve
 * Created Date: Wed Dec 04 2019
 * Author: xuchaojie
 * Copyright (c) 2019 netease
 */

#include <gtest/gtest.h>
#include <glog/logging.h>
#include <gflags/gflags.h>
#include <fiu-control.h>
#include <fiu.h>

#include "test/integration/snapshotcloneserver/snapshotcloneserver_module.h"
#include "test/integration/snapshotcloneserver/test_snapshotcloneserver_helpler.h"

const char* kSnapshotCloneServerIpPort = "127.0.0.1:10030";

namespace curve {
namespace snapshotcloneserver {

class SnapshotCloneServerTest : public ::testing::Test {
 public:
    static void SetUpTestCase() {
        options_ = new SnapshotCloneServerOptions();
        options_->addr = kSnapshotCloneServerIpPort;
        options_->snapshotPoolThreadNum = 8;
        options_->snapshotTaskManagerScanIntervalMs = 1000;
        options_->chunkSplitSize = 1048576;
        options_->checkSnapshotStatusIntervalMs = 1000;
        options_->maxSnapshotLimit = 64;
        options_->snapshotCoreThreadNum = 8;
        options_->mdsSessionTimeUs = 1000000;
        options_->stage1PoolThreadNum = 8;
        options_->stage2PoolThreadNum = 8;
        options_->commonPoolThreadNum = 8;
        options_->cloneTaskManagerScanIntervalMs = 1000;
        options_->cloneChunkSplitSize = 65536;
        options_->cloneTempDir = "/clone";
        options_->mdsRootUser = "root";
        options_->createCloneChunkConcurrency = 8;
        options_->recoverChunkConcurrency = 8;
        options_->clientAsyncMethodRetryTimeSec = 1;

        server_ = new SnapshotCloneServerModule();
        server_->Start(*options_);
    }

    static void TearDownTestCase() {
        server_->Stop();
        delete server_;
        delete options_;
    }

    void SetUp() override {
        fiu_init(0);
    }
    void TearDown() override {
        // noop
    }

    bool JudgeSnapTaskFailCleanTaskAndCheck(
        const std::string &user,
        const std::string &file,
        const std::string &uuid,
        SnapshotInfo *snapInfo) {
        // 验证任务失败
        FileSnapshotInfo info1;
        int ret = GetSnapshotInfo(
            user, file, uuid, &info1);
        if (ret < 0) {
            LOG(INFO) << "GetSnapshotInfo Fail"
                      << ", ret = " << ret;
            return false;
        }
        *snapInfo = info1.GetSnapshotInfo();
        if (Status::error != snapInfo->GetStatus()) {
            LOG(INFO) << "Check Task Status Fail.";
            return false;
        }

        ret = DeleteAndCheckSnapshotSuccess(user, file, uuid);
        if (ret < 0) {
            LOG(INFO) << "DeleteSnapshot Fail"
                      << ", ret = " << ret;
            return false;
        }

        // 验证任务不存在
        SnapshotInfo sinfo;
        ret = server_->GetMetaStore()->GetSnapshotInfo(uuid, &sinfo);
        if (ret != -1) {
            LOG(INFO) << "AsserTaskNotExist Fail"
                      << ", ret = " << ret;
            return false;
        }
        return true;
    }

    bool JudgeSnapTaskFailCleanEnvAndCheck(
        const std::string &user,
        const std::string &file,
        const std::string &uuid) {
        SnapshotInfo snapInfo;
        bool success = JudgeSnapTaskFailCleanTaskAndCheck(
            user, file, uuid, &snapInfo);
        if (!success) {
            return false;
        }
        int seqNum = snapInfo.GetSeqNum();
        // 验证curve上无快照
        FInfo fInfo;
        int ret = server_->GetCurveFsClient()->GetSnapshot(
            file, user, seqNum, &fInfo);
        if (ret != -LIBCURVE_ERROR::NOTEXIST) {
            LOG(INFO) << "AssertEnvClean Fail, snapshot exist on curve"
                      << ", ret = " << ret;
            return false;
        }

        // 验证nos上无快照
        ChunkIndexDataName indexData(file, seqNum);
        if (server_->GetDataStore()->ChunkIndexDataExist(indexData)) {
            LOG(INFO) << "AssertEnvClean Fail, snapshot exist on nos.";
            return false;
        }
        return true;
    }

    bool JudgeCloneTaskFailCleanEnvAndCheck(
        const std::string &user,
        const std::string &uuid) {
        // 验证任务状态为error
        TaskCloneInfo info1;
        int ret = GetCloneTaskInfo(
            user, uuid, &info1);
        if (ret < 0) {
            LOG(INFO) << "GetCloneTask fail"
                      << ", ret = " << ret;
            return false;
        }

        auto cloneInfo = info1.GetCloneInfo();
        if (CloneStatus::error != cloneInfo.GetStatus()) {
            LOG(INFO) << "Assert CloneStatus Fail";
            return false;
        }
        return CleanCloneTaskAndCheckEnvClean(user, uuid);
    }

    bool JudgeCloneTaskNotExistCleanEnvAndCheck(
        const std::string &user,
        const std::string &uuid) {
        // 验证任务不存在
        TaskCloneInfo info1;
        int ret = GetCloneTaskInfo(
            user, uuid, &info1);
        if (ret != kErrCodeFileNotExist) {
            LOG(INFO) << "AsserTaskNotExist fail"
                      << ", ret = " << ret;
            return false;
        }

        // 验证curvefs上无临时文件
        if (server_->GetCurveFsClient()->JudgeCloneDirHasFile()) {
            LOG(INFO) << "AssertEnvClean fail"
                       << ", ret = " << ret;
            return false;
        }
        return true;
    }

    bool CleanCloneTaskAndCheckEnvClean(
        const std::string &user,
        const std::string &uuid) {
        int ret = CleanCloneTask(user, uuid);
        if (ret < 0) {
            LOG(INFO) << "CleanCloneTask fail"
                      << ", ret = " << ret;
            return false;
        }

        std::this_thread::sleep_for(std::chrono::milliseconds(3000));

        // 验证任务不存在
        TaskCloneInfo info;
        ret = GetCloneTaskInfo(user, uuid, &info);
        if (kErrCodeFileNotExist != ret) {
            LOG(INFO) << "AsserTaskNotExist fail"
                      << ", ret = " << ret;
            return false;
        }

        // 验证curvefs上无临时文件
        if (server_->GetCurveFsClient()->JudgeCloneDirHasFile()) {
            LOG(INFO) << "AssertEnvClean fail"
                       << ", ret = " << ret;
            return false;
        }
        return true;
    }

    bool PrepreTestSnapshot(
        const std::string &user,
        const std::string &file,
        const std::string &snapName,
        std::string *uuid) {
        int ret = MakeSnapshot(user,
            file , snapName, uuid);
        if (ret < 0) {
            return false;
        }
        bool success1 = CheckSnapshotSuccess(user, file,
            *uuid);
        return success1;
    }

    bool PrepreTestSnapshotIfNotExist() {
        if (testSnapId_.empty()) {
            bool ret = PrepreTestSnapshot(testUser1,
                testFile1, "testSnap", &testSnapId_);
            return ret;
        }
        return true;
    }

    std::string testSnapId_;

    static SnapshotCloneServerModule *server_;
    static SnapshotCloneServerOptions *options_;
};

SnapshotCloneServerModule * SnapshotCloneServerTest::server_ = nullptr;
SnapshotCloneServerOptions * SnapshotCloneServerTest::options_ = nullptr;

TEST_F(SnapshotCloneServerTest, TestCreateSnapshotFailOnCurvefs) {
    std::string uuid;
    std::string user = testUser1;
    std::string file = testFile1;

    fiu_enable("test/integration/snapshotcloneserver/FakeCurveFsClient.CreateSnapshot", // NOLINT
        1, NULL, 0);

    int ret = MakeSnapshot(user, file , "snap1", &uuid);
    ASSERT_EQ(0, ret);

    std::this_thread::sleep_for(std::chrono::milliseconds(3000));

    fiu_disable("test/integration/snapshotcloneserver/FakeCurveFsClient.CreateSnapshot");  // NOLINT

    SnapshotInfo snapInfo;
    ASSERT_TRUE(JudgeSnapTaskFailCleanTaskAndCheck(
                user, file, uuid, &snapInfo));
}


TEST_F(SnapshotCloneServerTest, TestCreateSnapshotFailOnGetSnapshot) {
    std::string uuid;
    std::string user = testUser1;
    std::string file = testFile1;

    fiu_enable("test/integration/snapshotcloneserver/FakeCurveFsClient.GetSnapshot", // NOLINT
        1, NULL, 0);

    int ret = MakeSnapshot(user, file , "snap2", &uuid);
    ASSERT_EQ(0, ret);

    std::this_thread::sleep_for(std::chrono::milliseconds(3000));

    fiu_disable("test/integration/snapshotcloneserver/FakeCurveFsClient.GetSnapshot");  // NOLINT

    ASSERT_TRUE(JudgeSnapTaskFailCleanEnvAndCheck(
                user, file, uuid));
}

TEST_F(SnapshotCloneServerTest, TestCreateSnapshotFailOnDeleteSnapshot) {
    std::string uuid;
    std::string user = testUser1;
    std::string file = testFile1;

    fiu_enable("test/integration/snapshotcloneserver/FakeCurveFsClient.DeleteSnapshot", // NOLINT
        1, NULL, 0);

    int ret = MakeSnapshot(user, file , "snap3", &uuid);
    ASSERT_EQ(0, ret);

    std::this_thread::sleep_for(std::chrono::milliseconds(3000));

    fiu_disable("test/integration/snapshotcloneserver/FakeCurveFsClient.DeleteSnapshot");  // NOLINT

    ASSERT_TRUE(JudgeSnapTaskFailCleanEnvAndCheck(
                user, file, uuid));
}

TEST_F(SnapshotCloneServerTest, TestCreateSnapshotFailOnCheckSnapShotStatus) {
    std::string uuid;
    std::string user = testUser1;
    std::string file = testFile1;

    fiu_enable("test/integration/snapshotcloneserver/FakeCurveFsClient.CheckSnapShotStatus", // NOLINT
        1, NULL, 0);

    int ret = MakeSnapshot(user, file , "snap4", &uuid);
    ASSERT_EQ(0, ret);

    std::this_thread::sleep_for(std::chrono::milliseconds(3000));

    fiu_disable("test/integration/snapshotcloneserver/FakeCurveFsClient.CheckSnapShotStatus");  // NOLINT

    ASSERT_TRUE(JudgeSnapTaskFailCleanEnvAndCheck(
                user, file, uuid));
}

TEST_F(SnapshotCloneServerTest,
    TestCreateSnapshotFailOnGetSnapshotSegmentInfo) {
    std::string uuid;
    std::string user = testUser1;
    std::string file = testFile1;

    fiu_enable("test/integration/snapshotcloneserver/FakeCurveFsClient.GetSnapshotSegmentInfo", // NOLINT
        1, NULL, 0);

    int ret = MakeSnapshot(user, file , "snap5", &uuid);
    ASSERT_EQ(0, ret);

    std::this_thread::sleep_for(std::chrono::milliseconds(3000));

    fiu_disable("test/integration/snapshotcloneserver/FakeCurveFsClient.GetSnapshotSegmentInfo");  // NOLINT

    ASSERT_TRUE(JudgeSnapTaskFailCleanEnvAndCheck(
                user, file, uuid));
}

TEST_F(SnapshotCloneServerTest, TestCreateSnapshotFailOnReadChunkSnapshot) {
    std::string uuid;
    std::string user = testUser1;
    std::string file = testFile1;

    fiu_enable("test/integration/snapshotcloneserver/FakeCurveFsClient.ReadChunkSnapshot", // NOLINT
        1, NULL, 0);

    int ret = MakeSnapshot(user, file , "snap6", &uuid);
    ASSERT_EQ(0, ret);

    std::this_thread::sleep_for(std::chrono::milliseconds(3000));

    fiu_disable("test/integration/snapshotcloneserver/FakeCurveFsClient.ReadChunkSnapshot");  // NOLINT

    ASSERT_TRUE(JudgeSnapTaskFailCleanEnvAndCheck(
                user, file, uuid));
}

TEST_F(SnapshotCloneServerTest, TestCreateSnapshotFailOnGetChunkInfo) {
    std::string uuid;
    std::string user = testUser1;
    std::string file = testFile1;

    fiu_enable("test/integration/snapshotcloneserver/FakeCurveFsClient.GetChunkInfo", // NOLINT
        1, NULL, 0);

    int ret = MakeSnapshot(user, file , "snap7", &uuid);
    ASSERT_EQ(0, ret);

    std::this_thread::sleep_for(std::chrono::milliseconds(3000));

    fiu_disable("test/integration/snapshotcloneserver/FakeCurveFsClient.GetChunkInfo");  // NOLINT

    ASSERT_TRUE(JudgeSnapTaskFailCleanEnvAndCheck(
                user, file, uuid));
}

TEST_F(SnapshotCloneServerTest, TestCreateSnapshotFailOnAddSnapshot) {
    std::string uuid;
    std::string user = testUser1;
    std::string file = testFile1;

    fiu_enable("test/integration/snapshotcloneserver/FakeSnapshotCloneMetaStore.AddSnapshot", // NOLINT
        1, NULL, 0);

    // 验证任务失败
    int ret = MakeSnapshot(user, file , "snap8", &uuid);
    ASSERT_EQ(-1, ret);

    fiu_disable("test/integration/snapshotcloneserver/FakeSnapshotCloneMetaStore.AddSnapshot");  // NOLINT

    // 验证任务不存在
    SnapshotInfo sinfo;
    ret = server_->GetMetaStore()->GetSnapshotInfo(uuid, &sinfo);
    ASSERT_EQ(-1, ret);
}

TEST_F(SnapshotCloneServerTest, TestCreateSnapshotFailOnUpdateSnapshot) {
    std::string uuid;
    std::string user = testUser1;
    std::string file = testFile1;

    fiu_enable("test/integration/snapshotcloneserver/FakeSnapshotCloneMetaStore.UpdateSnapshot", // NOLINT
        1, NULL, 0);

    int ret = MakeSnapshot(user, file , "snap9", &uuid);
    ASSERT_EQ(0, ret);

    std::this_thread::sleep_for(std::chrono::milliseconds(3000));

    fiu_disable("test/integration/snapshotcloneserver/FakeSnapshotCloneMetaStore.UpdateSnapshot");  // NOLINT

    // 验证任务失败
    FileSnapshotInfo info1;
    ret = GetSnapshotInfo(
        user, file, uuid, &info1);

    ASSERT_EQ(kErrCodeInternalError, ret);

    auto snapInfo = info1.GetSnapshotInfo();
    ASSERT_EQ(Status::pending, snapInfo.GetStatus());
}

TEST_F(SnapshotCloneServerTest, TestCreateSnapshotFailOnPutChunkIndexData) {
    std::string uuid;
    std::string user = testUser1;
    std::string file = testFile1;

    fiu_enable("test/integration/snapshotcloneserver/FakeSnapshotDataStore.PutChunkIndexData", // NOLINT
        1, NULL, 0);

    int ret = MakeSnapshot(user, file , "snap10", &uuid);
    ASSERT_EQ(0, ret);

    std::this_thread::sleep_for(std::chrono::milliseconds(3000));

    fiu_disable("test/integration/snapshotcloneserver/FakeSnapshotDataStore.PutChunkIndexData");  // NOLINT

    ASSERT_TRUE(JudgeSnapTaskFailCleanEnvAndCheck(
                user, file, uuid));
}

TEST_F(SnapshotCloneServerTest,
    TestCreateSnapshotFailOnDataChunkTranferComplete) {
    std::string uuid;
    std::string user = testUser1;
    std::string file = testFile1;

    fiu_enable("test/integration/snapshotcloneserver/FakeSnapshotDataStore.DataChunkTranferComplete", // NOLINT
        1, NULL, 0);

    int ret = MakeSnapshot(user, file , "snap11", &uuid);
    ASSERT_EQ(0, ret);

    std::this_thread::sleep_for(std::chrono::milliseconds(3000));

    fiu_disable("test/integration/snapshotcloneserver/FakeSnapshotDataStore.DataChunkTranferComplete");  // NOLINT

    ASSERT_TRUE(JudgeSnapTaskFailCleanEnvAndCheck(
                user, file, uuid));
}

TEST_F(SnapshotCloneServerTest, TestDeleteSnapshotFailOnGetChunkIndexData) {
    std::string uuid;
    std::string user = testUser1;
    std::string file = testFile1;

    ASSERT_TRUE(PrepreTestSnapshot(user, file, "snap12", &uuid));

    fiu_enable("test/integration/snapshotcloneserver/FakeSnapshotDataStore.GetChunkIndexData", // NOLINT
        1, NULL, 0);

    // 验证删除失败
    int ret = DeleteAndCheckSnapshotSuccess(user, file, uuid);
    ASSERT_EQ(-1, ret);

    fiu_disable("test/integration/snapshotcloneserver/FakeSnapshotDataStore.GetChunkIndexData");  // NOLINT

    // 验证任务失败
    SnapshotInfo sinfo;
    ret = server_->GetMetaStore()->GetSnapshotInfo(uuid, &sinfo);
    ASSERT_EQ(0, ret);
    ASSERT_EQ(Status::error, sinfo.GetStatus());
}

TEST_F(SnapshotCloneServerTest, TestDeleteSnapshotFailOnDeleteChunkData) {
    std::string uuid;
    std::string user = testUser1;
    std::string file = testFile1;

    ASSERT_TRUE(PrepreTestSnapshot(user, file, "snap13", &uuid));

    fiu_enable("test/integration/snapshotcloneserver/FakeSnapshotDataStore.DeleteChunkData", // NOLINT
        1, NULL, 0);

    // 验证删除失败
    int ret = DeleteAndCheckSnapshotSuccess(user, file, uuid);
    ASSERT_EQ(-1, ret);

    fiu_disable("test/integration/snapshotcloneserver/FakeSnapshotDataStore.DeleteChunkData");  // NOLINT

    // 验证任务失败
    SnapshotInfo sinfo;
    ret = server_->GetMetaStore()->GetSnapshotInfo(uuid, &sinfo);
    ASSERT_EQ(0, ret);
    ASSERT_EQ(Status::error, sinfo.GetStatus());
}

TEST_F(SnapshotCloneServerTest, TestDeleteSnapshotFailOnDeleteChunkIndexData) {
    std::string uuid;
    std::string user = testUser1;
    std::string file = testFile1;

    ASSERT_TRUE(PrepreTestSnapshot(user, file, "snap14", &uuid));

    fiu_enable("test/integration/snapshotcloneserver/FakeSnapshotDataStore.DeleteChunkIndexData", // NOLINT
        1, NULL, 0);

    // 验证删除失败
    int ret = DeleteAndCheckSnapshotSuccess(user, file, uuid);
    ASSERT_EQ(-1, ret);

    fiu_disable("test/integration/snapshotcloneserver/FakeSnapshotDataStore.DeleteChunkIndexData");  // NOLINT

    // 验证任务失败
    SnapshotInfo sinfo;
    ret = server_->GetMetaStore()->GetSnapshotInfo(uuid, &sinfo);
    ASSERT_EQ(0, ret);
    ASSERT_EQ(Status::error, sinfo.GetStatus());
}

TEST_F(SnapshotCloneServerTest, TestDeleteSnapshotFailOnDeleteSnapshot) {
    std::string uuid;
    std::string user = testUser1;
    std::string file = testFile1;

    ASSERT_TRUE(PrepreTestSnapshot(user, file, "snap15", &uuid));

    fiu_enable("test/integration/snapshotcloneserver/FakeSnapshotCloneMetaStore.DeleteSnapshot", // NOLINT
        1, NULL, 0);

    // 验证删除失败
    int ret = DeleteAndCheckSnapshotSuccess(user, file, uuid);
    ASSERT_EQ(-1, ret);

    fiu_disable("test/integration/snapshotcloneserver/FakeSnapshotCloneMetaStore.DeleteSnapshot");  // NOLINT

    // 验证任务失败
    SnapshotInfo sinfo;
    ret = server_->GetMetaStore()->GetSnapshotInfo(uuid, &sinfo);
    ASSERT_EQ(0, ret);
    ASSERT_EQ(Status::error, sinfo.GetStatus());
}

TEST_F(SnapshotCloneServerTest, TestLazyCloneSnapFailOnCreateCloneFile) {
    ASSERT_TRUE(PrepreTestSnapshotIfNotExist());
    std::string uuid1;

    fiu_enable("test/integration/snapshotcloneserver/FakeCurveFsClient.CreateCloneFile", // NOLINT
        1, NULL, 0);

    int ret = CloneOrRecover("Clone", testUser1, testSnapId_,
        "/user1/clone1", true,
        &uuid1);
    ASSERT_EQ(kErrCodeInternalError, ret);

    std::this_thread::sleep_for(std::chrono::milliseconds(3000));

    fiu_disable("test/integration/snapshotcloneserver/FakeCurveFsClient.CreateCloneFile");  // NOLINT

    ASSERT_TRUE(JudgeCloneTaskFailCleanEnvAndCheck(
            testUser1, uuid1));
}

TEST_F(SnapshotCloneServerTest, TestLazyCloneSnapFailOnCompleteCloneMeta) {
    ASSERT_TRUE(PrepreTestSnapshotIfNotExist());
    std::string uuid1;

    fiu_enable("test/integration/snapshotcloneserver/FakeCurveFsClient.CompleteCloneMeta", // NOLINT
        1, NULL, 0);

    int ret = CloneOrRecover("Clone", testUser1, testSnapId_,
        "/user1/clone1", true,
        &uuid1);
    ASSERT_EQ(kErrCodeInternalError, ret);

    std::this_thread::sleep_for(std::chrono::milliseconds(3000));

    fiu_disable("test/integration/snapshotcloneserver/FakeCurveFsClient.CompleteCloneMeta");  // NOLINT

    ASSERT_TRUE(JudgeCloneTaskFailCleanEnvAndCheck(
            testUser1, uuid1));
}

TEST_F(SnapshotCloneServerTest, TestLazyCloneSnapFailOnGetFileInfo) {
    ASSERT_TRUE(PrepreTestSnapshotIfNotExist());
    std::string uuid1;

    fiu_enable("test/integration/snapshotcloneserver/FakeCurveFsClient.GetFileInfo", // NOLINT
        1, NULL, 0);

    int ret = CloneOrRecover("Clone", testUser1, testSnapId_,
        "/user1/clone1", true,
        &uuid1);
    ASSERT_EQ(kErrCodeInternalError, ret);

    std::this_thread::sleep_for(std::chrono::milliseconds(3000));

    fiu_disable("test/integration/snapshotcloneserver/FakeCurveFsClient.GetFileInfo");  // NOLINT
    ASSERT_TRUE(JudgeCloneTaskNotExistCleanEnvAndCheck(
                testUser1, uuid1));
}

TEST_F(SnapshotCloneServerTest,
    TestLazyCloneSnapFailOnGetOrAllocateSegmentInfo) {
    ASSERT_TRUE(PrepreTestSnapshotIfNotExist());
    std::string uuid1;

    fiu_enable("test/integration/snapshotcloneserver/FakeCurveFsClient.GetOrAllocateSegmentInfo", // NOLINT
        1, NULL, 0);

    int ret = CloneOrRecover("Clone", testUser1, testSnapId_,
        "/user1/clone1", true,
        &uuid1);
    ASSERT_EQ(kErrCodeInternalError, ret);

    std::this_thread::sleep_for(std::chrono::milliseconds(3000));

    fiu_disable("test/integration/snapshotcloneserver/FakeCurveFsClient.GetOrAllocateSegmentInfo");  // NOLINT

    ASSERT_TRUE(JudgeCloneTaskFailCleanEnvAndCheck(
                testUser1, uuid1));
}

TEST_F(SnapshotCloneServerTest, TestLazyCloneSnapFailOnRenameCloneFile) {
    ASSERT_TRUE(PrepreTestSnapshotIfNotExist());
    std::string uuid1;

    fiu_enable("test/integration/snapshotcloneserver/FakeCurveFsClient.RenameCloneFile", // NOLINT
        1, NULL, 0);

    int ret = CloneOrRecover("Clone", testUser1, testSnapId_,
        "/user1/clone1", true,
        &uuid1);
    ASSERT_EQ(kErrCodeInternalError, ret);

    std::this_thread::sleep_for(std::chrono::milliseconds(3000));

    fiu_disable("test/integration/snapshotcloneserver/FakeCurveFsClient.RenameCloneFile");  // NOLINT

    ASSERT_TRUE(JudgeCloneTaskFailCleanEnvAndCheck(
                testUser1, uuid1));
}

TEST_F(SnapshotCloneServerTest, TestLazyCloneSnapFailOnCreateCloneChunk) {
    ASSERT_TRUE(PrepreTestSnapshotIfNotExist());
    std::string uuid1;

    fiu_enable("test/integration/snapshotcloneserver/FakeCurveFsClient.CreateCloneChunk", // NOLINT
        1, NULL, 0);

    int ret = CloneOrRecover("Clone", testUser1, testSnapId_,
        "/user1/clone1", true,
        &uuid1);
    ASSERT_EQ(kErrCodeInternalError, ret);

    std::this_thread::sleep_for(std::chrono::milliseconds(3000));

    fiu_disable("test/integration/snapshotcloneserver/FakeCurveFsClient.CreateCloneChunk");  // NOLINT

    ASSERT_TRUE(JudgeCloneTaskFailCleanEnvAndCheck(
                testUser1, uuid1));
}

TEST_F(SnapshotCloneServerTest, TestLazyCloneSnapFailOnChangeOwner) {
    ASSERT_TRUE(PrepreTestSnapshotIfNotExist());
    std::string uuid1;

    fiu_enable("test/integration/snapshotcloneserver/FakeCurveFsClient.ChangeOwner", // NOLINT
        1, NULL, 0);

    int ret = CloneOrRecover("Clone", testUser1, testSnapId_,
        "/user1/clone1", true,
        &uuid1);
    ASSERT_EQ(kErrCodeInternalError, ret);

    std::this_thread::sleep_for(std::chrono::milliseconds(3000));

    fiu_disable("test/integration/snapshotcloneserver/FakeCurveFsClient.ChangeOwner");  // NOLINT

    ASSERT_TRUE(JudgeCloneTaskFailCleanEnvAndCheck(
                testUser1, uuid1));
}

TEST_F(SnapshotCloneServerTest, TestLazyCloneSnapFailOnGetChunkIndexData) {
    ASSERT_TRUE(PrepreTestSnapshotIfNotExist());
    std::string uuid1;

    fiu_enable("test/integration/snapshotcloneserver/FakeSnapshotDataStore.GetChunkIndexData", // NOLINT
        1, NULL, 0);

    int ret = CloneOrRecover("Clone", testUser1, testSnapId_,
        "/user1/clone1", true,
        &uuid1);
    ASSERT_EQ(kErrCodeInternalError, ret);

    std::this_thread::sleep_for(std::chrono::milliseconds(3000));

    fiu_disable("test/integration/snapshotcloneserver/FakeSnapshotDataStore.GetChunkIndexData");  // NOLINT

    ASSERT_TRUE(JudgeCloneTaskFailCleanEnvAndCheck(
                testUser1, uuid1));
}

TEST_F(SnapshotCloneServerTest, TestLazyCloneSnapFailOnAddCloneInfo) {
    ASSERT_TRUE(PrepreTestSnapshotIfNotExist());
    std::string uuid1;

    fiu_enable("test/integration/snapshotcloneserver/FakeSnapshotCloneMetaStore.AddCloneInfo", // NOLINT
        1, NULL, 0);

    int ret = CloneOrRecover("Clone", testUser1, testSnapId_,
        "/user1/clone1", true,
        &uuid1);
    ASSERT_EQ(kErrCodeInternalError, ret);

    fiu_disable("test/integration/snapshotcloneserver/FakeSnapshotCloneMetaStore.AddCloneInfo");  // NOLINT

    ASSERT_TRUE(JudgeCloneTaskNotExistCleanEnvAndCheck(
                testUser1, uuid1));
}

TEST_F(SnapshotCloneServerTest,
    TestLazyCloneSnapFailOnFileNotExistWhenRecoverChunk) {
    ASSERT_TRUE(PrepreTestSnapshotIfNotExist());
    std::string uuid1;

    fiu_enable("test/integration/snapshotcloneserver/FakeCurveFsClient.RecoverChunk", // NOLINT
        1, NULL, 0);

    int ret = CloneOrRecover("Clone", testUser1, testSnapId_,
        "/user1/clone1", true,
        &uuid1);
    ASSERT_EQ(0, ret);

    // Flatten
    ret = Flatten(testUser1, uuid1);
    ASSERT_EQ(0, ret);

    // 克隆未完成前删除目标文件
    ASSERT_EQ(LIBCURVE_ERROR::OK,
        server_->GetCurveFsClient()->DeleteFile("/user1/clone1", "", 0));

    std::this_thread::sleep_for(std::chrono::milliseconds(3000));

    ASSERT_TRUE(JudgeCloneTaskFailCleanEnvAndCheck(
                testUser1, uuid1));

    fiu_disable("test/integration/snapshotcloneserver/FakeCurveFsClient.RecoverChunk");  // NOLINT
}

TEST_F(SnapshotCloneServerTest,
    TestLazyCloneSnapSuccessWhenRecoverChunkFailOneTime) {
    ASSERT_TRUE(PrepreTestSnapshotIfNotExist());
    std::string uuid1;

    fiu_enable("test/integration/snapshotcloneserver/FakeCurveFsClient.RecoverChunk", // NOLINT
        1, NULL, FIU_ONETIME);

    int ret = CloneOrRecover("Clone", testUser1, testSnapId_,
        "/user1/cloneSuccess1", true,
        &uuid1);
    ASSERT_EQ(0, ret);

    // Flatten
    ret = Flatten(testUser1, uuid1);
    ASSERT_EQ(0, ret);

    bool success1 = CheckCloneOrRecoverSuccess(testUser1, uuid1, true);
    ASSERT_TRUE(success1);

    ASSERT_EQ(LIBCURVE_ERROR::OK,
        server_->GetCurveFsClient()->DeleteFile("/user1/cloneSuccess1", "", 0));

    fiu_disable("test/integration/snapshotcloneserver/FakeCurveFsClient.RecoverChunk");  // NOLINT

    ASSERT_TRUE(CleanCloneTaskAndCheckEnvClean(
                testUser1, uuid1));
}

TEST_F(SnapshotCloneServerTest, TestNotLazyCloneSnapFailOnRecoverChunk) {
    ASSERT_TRUE(PrepreTestSnapshotIfNotExist());
    std::string uuid1;

    fiu_enable("test/integration/snapshotcloneserver/FakeCurveFsClient.RecoverChunk", // NOLINT
        1, NULL, 0);

    int ret = CloneOrRecover("Clone", testUser1, testSnapId_,
        "/user1/clone1", false,
        &uuid1);
    ASSERT_EQ(0, ret);

    std::this_thread::sleep_for(std::chrono::milliseconds(3000));

    fiu_disable("test/integration/snapshotcloneserver/FakeCurveFsClient.RecoverChunk");  // NOLINT

    ASSERT_TRUE(JudgeCloneTaskFailCleanEnvAndCheck(
                testUser1, uuid1));
}

TEST_F(SnapshotCloneServerTest, TestNotLazyCloneSnapFailOnRenameCloneFile) {
    ASSERT_TRUE(PrepreTestSnapshotIfNotExist());
    std::string uuid1;

    fiu_enable("test/integration/snapshotcloneserver/FakeCurveFsClient.RenameCloneFile", // NOLINT
        1, NULL, 0);

    int ret = CloneOrRecover("Clone", testUser1, testSnapId_,
        "/user1/clone1", false,
        &uuid1);
    ASSERT_EQ(0, ret);

    std::this_thread::sleep_for(std::chrono::milliseconds(3000));

    fiu_disable("test/integration/snapshotcloneserver/FakeCurveFsClient.RenameCloneFile");  // NOLINT

    ASSERT_TRUE(JudgeCloneTaskFailCleanEnvAndCheck(
                testUser1, uuid1));
}

TEST_F(SnapshotCloneServerTest, TestNotLazyCloneSnapFailOnChangeOwner) {
    ASSERT_TRUE(PrepreTestSnapshotIfNotExist());
    std::string uuid1;

    fiu_enable("test/integration/snapshotcloneserver/FakeCurveFsClient.ChangeOwner", // NOLINT
        1, NULL, 0);

    int ret = CloneOrRecover("Clone", testUser1, testSnapId_,
        "/user1/clone1", false,
        &uuid1);
    ASSERT_EQ(0, ret);

    std::this_thread::sleep_for(std::chrono::milliseconds(3000));

    fiu_disable("test/integration/snapshotcloneserver/FakeCurveFsClient.ChangeOwner");  // NOLINT

    ASSERT_TRUE(JudgeCloneTaskFailCleanEnvAndCheck(
                testUser1, uuid1));
}

TEST_F(SnapshotCloneServerTest, TestNotLazyCloneSnapFailOnCompleteCloneFile) {
    ASSERT_TRUE(PrepreTestSnapshotIfNotExist());
    std::string uuid1;

    fiu_enable("test/integration/snapshotcloneserver/FakeCurveFsClient.CompleteCloneFile", // NOLINT
        1, NULL, 0);

    int ret = CloneOrRecover("Clone", testUser1, testSnapId_,
        "/user1/clone1", false,
        &uuid1);
    ASSERT_EQ(0, ret);

    std::this_thread::sleep_for(std::chrono::milliseconds(3000));

    fiu_disable("test/integration/snapshotcloneserver/FakeCurveFsClient.CompleteCloneFile");  // NOLINT

    ASSERT_TRUE(JudgeCloneTaskFailCleanEnvAndCheck(
                testUser1, uuid1));
}

TEST_F(SnapshotCloneServerTest, TestLazyCloneImageFailOnCreateCloneFile) {
    std::string uuid1;

    fiu_enable("test/integration/snapshotcloneserver/FakeCurveFsClient.CreateCloneFile", // NOLINT
        1, NULL, 0);

    int ret = CloneOrRecover("Clone", testUser1, testFile1,
        "/user1/clone1", true,
        &uuid1);
    ASSERT_EQ(kErrCodeInternalError, ret);

    std::this_thread::sleep_for(std::chrono::milliseconds(3000));

    fiu_disable("test/integration/snapshotcloneserver/FakeCurveFsClient.CreateCloneFile");  // NOLINT

    ASSERT_TRUE(JudgeCloneTaskFailCleanEnvAndCheck(
                testUser1, uuid1));
}

TEST_F(SnapshotCloneServerTest, TestLazyCloneImageFailOnCompleteCloneMeta) {
    std::string uuid1;

    fiu_enable("test/integration/snapshotcloneserver/FakeCurveFsClient.CompleteCloneMeta", // NOLINT
        1, NULL, 0);

    int ret = CloneOrRecover("Clone", testUser1, testFile1,
        "/user1/clone1", true,
        &uuid1);
    ASSERT_EQ(kErrCodeInternalError, ret);

    std::this_thread::sleep_for(std::chrono::milliseconds(3000));

    fiu_disable("test/integration/snapshotcloneserver/FakeCurveFsClient.CompleteCloneMeta");  // NOLINT

    ASSERT_TRUE(JudgeCloneTaskFailCleanEnvAndCheck(
                testUser1, uuid1));
}

TEST_F(SnapshotCloneServerTest, TestLazyCloneImageFailOnGetFileInfo) {
    std::string uuid1;

    fiu_enable("test/integration/snapshotcloneserver/FakeCurveFsClient.GetFileInfo", // NOLINT
        1, NULL, 0);

    int ret = CloneOrRecover("Clone", testUser1, testFile1,
        "/user1/clone1", true,
        &uuid1);
    ASSERT_EQ(kErrCodeInternalError, ret);

    std::this_thread::sleep_for(std::chrono::milliseconds(3000));

    fiu_disable("test/integration/snapshotcloneserver/FakeCurveFsClient.GetFileInfo");  // NOLINT

    ASSERT_TRUE(JudgeCloneTaskNotExistCleanEnvAndCheck(
                testUser1, uuid1));
}

TEST_F(SnapshotCloneServerTest,
    TestLazyCloneImageFailOnGetOrAllocateSegmentInfo) {
    std::string uuid1;

    fiu_enable("test/integration/snapshotcloneserver/FakeCurveFsClient.GetOrAllocateSegmentInfo", // NOLINT
        1, NULL, 0);

    int ret = CloneOrRecover("Clone", testUser1, testFile1,
        "/user1/clone1", true,
        &uuid1);
    ASSERT_EQ(kErrCodeInternalError, ret);

    std::this_thread::sleep_for(std::chrono::milliseconds(3000));

    fiu_disable("test/integration/snapshotcloneserver/FakeCurveFsClient.GetOrAllocateSegmentInfo");  // NOLINT

    ASSERT_TRUE(JudgeCloneTaskFailCleanEnvAndCheck(
                testUser1, uuid1));
}

TEST_F(SnapshotCloneServerTest,
    TestLazyCloneImageFailOnRenameCloneFile) {
    std::string uuid1;

    fiu_enable("test/integration/snapshotcloneserver/FakeCurveFsClient.RenameCloneFile", // NOLINT
        1, NULL, 0);

    int ret = CloneOrRecover("Clone", testUser1, testFile1,
        "/user1/clone1", true,
        &uuid1);
    ASSERT_EQ(kErrCodeInternalError, ret);

    std::this_thread::sleep_for(std::chrono::milliseconds(3000));

    fiu_disable("test/integration/snapshotcloneserver/FakeCurveFsClient.RenameCloneFile");  // NOLINT

    ASSERT_TRUE(JudgeCloneTaskFailCleanEnvAndCheck(
                testUser1, uuid1));
}

TEST_F(SnapshotCloneServerTest,
    TestLazyCloneImageFailOnCreateCloneChunk) {
    std::string uuid1;

    fiu_enable("test/integration/snapshotcloneserver/FakeCurveFsClient.CreateCloneChunk", // NOLINT
        1, NULL, 0);

    int ret = CloneOrRecover("Clone", testUser1, testFile1,
        "/user1/clone1", true,
        &uuid1);
    ASSERT_EQ(kErrCodeInternalError, ret);

    std::this_thread::sleep_for(std::chrono::milliseconds(3000));

    fiu_disable("test/integration/snapshotcloneserver/FakeCurveFsClient.CreateCloneChunk");  // NOLINT

    ASSERT_TRUE(JudgeCloneTaskFailCleanEnvAndCheck(
                testUser1, uuid1));
}

TEST_F(SnapshotCloneServerTest, TestLazyCloneImageFailOnAddCloneInfo) {
    std::string uuid1;

    fiu_enable("test/integration/snapshotcloneserver/FakeSnapshotCloneMetaStore.AddCloneInfo", // NOLINT
        1, NULL, 0);

    int ret = CloneOrRecover("Clone", testUser1, testFile1,
        "/user1/clone1", true,
        &uuid1);
    ASSERT_EQ(kErrCodeInternalError, ret);

    fiu_disable("test/integration/snapshotcloneserver/FakeSnapshotCloneMetaStore.AddCloneInfo");  // NOLINT

    ASSERT_TRUE(JudgeCloneTaskNotExistCleanEnvAndCheck(
                testUser1, uuid1));
}

TEST_F(SnapshotCloneServerTest, TestLazyCloneSnapImageOnChangeOwner) {
    std::string uuid1;

    fiu_enable("test/integration/snapshotcloneserver/FakeCurveFsClient.ChangeOwner", // NOLINT
        1, NULL, 0);

    int ret = CloneOrRecover("Clone", testUser1, testFile1,
        "/user1/clone1", true,
        &uuid1);
    ASSERT_EQ(kErrCodeInternalError, ret);

    std::this_thread::sleep_for(std::chrono::milliseconds(3000));

    fiu_disable("test/integration/snapshotcloneserver/FakeCurveFsClient.ChangeOwner");  // NOLINT

    ASSERT_TRUE(JudgeCloneTaskFailCleanEnvAndCheck(
                testUser1, uuid1));
}

TEST_F(SnapshotCloneServerTest,
    TestLazyCloneImageFailOnFileNotExistWhenRecoverChunk) {
    std::string uuid1;

    fiu_enable("test/integration/snapshotcloneserver/FakeCurveFsClient.RecoverChunk", // NOLINT
        1, NULL, 0);

    int ret = CloneOrRecover("Clone", testUser1, testFile1,
        "/user1/clone1", true,
        &uuid1);
    ASSERT_EQ(0, ret);

    // Flatten
    ret = Flatten(testUser1, uuid1);
    ASSERT_EQ(0, ret);

    // 克隆未完成前删除目标文件
    ASSERT_EQ(LIBCURVE_ERROR::OK,
        server_->GetCurveFsClient()->DeleteFile("/user1/clone1", "", 0));

    std::this_thread::sleep_for(std::chrono::milliseconds(3000));

    ASSERT_TRUE(JudgeCloneTaskFailCleanEnvAndCheck(
                testUser1, uuid1));

    fiu_disable("test/integration/snapshotcloneserver/FakeCurveFsClient.RecoverChunk");  // NOLINT
}

TEST_F(SnapshotCloneServerTest,
    TestLazyCloneImageSuccessWhenRecoverChunkFailOneTime) {
    std::string uuid1;

    fiu_enable("test/integration/snapshotcloneserver/FakeCurveFsClient.RecoverChunk", // NOLINT
        1, NULL, FIU_ONETIME);

    int ret = CloneOrRecover("Clone", testUser1, testFile1,
        "/user1/cloneSuccess2", true,
        &uuid1);
    ASSERT_EQ(0, ret);

    // Flatten
    ret = Flatten(testUser1, uuid1);
    ASSERT_EQ(0, ret);

    bool success1 = CheckCloneOrRecoverSuccess(testUser1, uuid1, true);
    ASSERT_TRUE(success1);

    ASSERT_EQ(LIBCURVE_ERROR::OK,
        server_->GetCurveFsClient()->DeleteFile("/user1/cloneSuccess2", "", 0));

    fiu_disable("test/integration/snapshotcloneserver/FakeCurveFsClient.RecoverChunk");  // NOLINT
    ASSERT_TRUE(CleanCloneTaskAndCheckEnvClean(
                testUser1, uuid1));
}

TEST_F(SnapshotCloneServerTest,
    TestNotLazyCloneImageFailOnRecoverChunk) {
    std::string uuid1;

    fiu_enable("test/integration/snapshotcloneserver/FakeCurveFsClient.RecoverChunk", // NOLINT
        1, NULL, 0);

    int ret = CloneOrRecover("Clone", testUser1, testFile1,
        "/user1/clone1", false,
        &uuid1);
    ASSERT_EQ(0, ret);

    std::this_thread::sleep_for(std::chrono::milliseconds(3000));

    fiu_disable("test/integration/snapshotcloneserver/FakeCurveFsClient.RecoverChunk");  // NOLINT

    ASSERT_TRUE(JudgeCloneTaskFailCleanEnvAndCheck(
                testUser1, uuid1));
}

TEST_F(SnapshotCloneServerTest,
    TestNotLazyCloneImageFailOnRenameCloneFile) {
    std::string uuid1;

    fiu_enable("test/integration/snapshotcloneserver/FakeCurveFsClient.RenameCloneFile", // NOLINT
        1, NULL, 0);

    int ret = CloneOrRecover("Clone", testUser1, testFile1,
        "/user1/clone1", false,
        &uuid1);
    ASSERT_EQ(0, ret);

    std::this_thread::sleep_for(std::chrono::milliseconds(3000));

    fiu_disable("test/integration/snapshotcloneserver/FakeCurveFsClient.RenameCloneFile");  // NOLINT

    ASSERT_TRUE(JudgeCloneTaskFailCleanEnvAndCheck(
                testUser1, uuid1));
}

TEST_F(SnapshotCloneServerTest, TestNotLazyCloneSnapImageOnChangeOwner) {
    std::string uuid1;

    fiu_enable("test/integration/snapshotcloneserver/FakeCurveFsClient.ChangeOwner", // NOLINT
        1, NULL, 0);

    int ret = CloneOrRecover("Clone", testUser1, testFile1,
        "/user1/clone1", false,
        &uuid1);
    ASSERT_EQ(0, ret);

    std::this_thread::sleep_for(std::chrono::milliseconds(3000));

    fiu_disable("test/integration/snapshotcloneserver/FakeCurveFsClient.ChangeOwner");  // NOLINT

    ASSERT_TRUE(JudgeCloneTaskFailCleanEnvAndCheck(
                testUser1, uuid1));
}

TEST_F(SnapshotCloneServerTest,
    TestNotLazyCloneImageFailOnCompleteCloneFile) {
    ASSERT_TRUE(PrepreTestSnapshotIfNotExist());
    std::string uuid1;

    fiu_enable("test/integration/snapshotcloneserver/FakeCurveFsClient.CompleteCloneFile", // NOLINT
        1, NULL, 0);

    int ret = CloneOrRecover("Clone", testUser1, testFile1,
        "/user1/clone1", false,
        &uuid1);
    ASSERT_EQ(0, ret);

    std::this_thread::sleep_for(std::chrono::milliseconds(3000));

    fiu_disable("test/integration/snapshotcloneserver/FakeCurveFsClient.CompleteCloneFile");  // NOLINT

    ASSERT_TRUE(JudgeCloneTaskFailCleanEnvAndCheck(
                testUser1, uuid1));
}

TEST_F(SnapshotCloneServerTest, TestLazyRecoverSnapFailOnCreateCloneFile) {
    ASSERT_TRUE(PrepreTestSnapshotIfNotExist());
    std::string uuid1;

    fiu_enable("test/integration/snapshotcloneserver/FakeCurveFsClient.CreateCloneFile", // NOLINT
        1, NULL, 0);

    int ret = CloneOrRecover("Recover", testUser1, testSnapId_, testFile1, true,
        &uuid1);
    ASSERT_EQ(kErrCodeInternalError, ret);

    std::this_thread::sleep_for(std::chrono::milliseconds(3000));

    fiu_disable("test/integration/snapshotcloneserver/FakeCurveFsClient.CreateCloneFile");  // NOLINT

    ASSERT_TRUE(JudgeCloneTaskFailCleanEnvAndCheck(
                testUser1, uuid1));
}

TEST_F(SnapshotCloneServerTest, TestLazyRecoverSnapFailOnCompleteCloneMeta) {
    ASSERT_TRUE(PrepreTestSnapshotIfNotExist());
    std::string uuid1;

    fiu_enable("test/integration/snapshotcloneserver/FakeCurveFsClient.CompleteCloneMeta", // NOLINT
        1, NULL, 0);

    int ret = CloneOrRecover("Recover", testUser1, testSnapId_, testFile1, true,
        &uuid1);
    ASSERT_EQ(kErrCodeInternalError, ret);

    std::this_thread::sleep_for(std::chrono::milliseconds(3000));

    fiu_disable("test/integration/snapshotcloneserver/FakeCurveFsClient.CompleteCloneMeta");  // NOLINT

    ASSERT_TRUE(JudgeCloneTaskFailCleanEnvAndCheck(
                testUser1, uuid1));
}

TEST_F(SnapshotCloneServerTest, TestLazyRecoverSnapFailOnGetFileInfo) {
    ASSERT_TRUE(PrepreTestSnapshotIfNotExist());
    std::string uuid1;

    fiu_enable("test/integration/snapshotcloneserver/FakeCurveFsClient.GetFileInfo", // NOLINT
        1, NULL, 0);

    int ret = CloneOrRecover("Recover", testUser1, testSnapId_, testFile1, true,
        &uuid1);
    ASSERT_EQ(kErrCodeInternalError, ret);

    std::this_thread::sleep_for(std::chrono::milliseconds(3000));

    fiu_disable("test/integration/snapshotcloneserver/FakeCurveFsClient.GetFileInfo");  // NOLINT

    ASSERT_TRUE(JudgeCloneTaskNotExistCleanEnvAndCheck(
                testUser1, uuid1));
}

TEST_F(SnapshotCloneServerTest,
    TestLazyRecoverSnapFailOnGetOrAllocateSegmentInfo) {
    ASSERT_TRUE(PrepreTestSnapshotIfNotExist());
    std::string uuid1;

    fiu_enable("test/integration/snapshotcloneserver/FakeCurveFsClient.GetOrAllocateSegmentInfo", // NOLINT
        1, NULL, 0);

    int ret = CloneOrRecover("Recover", testUser1, testSnapId_, testFile1, true,
        &uuid1);
    ASSERT_EQ(kErrCodeInternalError, ret);

    std::this_thread::sleep_for(std::chrono::milliseconds(3000));

    fiu_disable("test/integration/snapshotcloneserver/FakeCurveFsClient.GetOrAllocateSegmentInfo");  // NOLINT

    ASSERT_TRUE(JudgeCloneTaskFailCleanEnvAndCheck(
                testUser1, uuid1));
}

TEST_F(SnapshotCloneServerTest, TestLazyRecoverSnapFailOnRenameCloneFile) {
    ASSERT_TRUE(PrepreTestSnapshotIfNotExist());
    std::string uuid1;

    fiu_enable("test/integration/snapshotcloneserver/FakeCurveFsClient.RenameCloneFile", // NOLINT
        1, NULL, 0);

    int ret = CloneOrRecover("Recover", testUser1, testSnapId_, testFile1, true,
        &uuid1);
    ASSERT_EQ(kErrCodeInternalError, ret);

    std::this_thread::sleep_for(std::chrono::milliseconds(3000));

    fiu_disable("test/integration/snapshotcloneserver/FakeCurveFsClient.RenameCloneFile");  // NOLINT

    ASSERT_TRUE(JudgeCloneTaskFailCleanEnvAndCheck(
                testUser1, uuid1));
}

TEST_F(SnapshotCloneServerTest, TestLazyRecoverSnapFailOnCreateCloneChunk) {
    ASSERT_TRUE(PrepreTestSnapshotIfNotExist());
    std::string uuid1;

    fiu_enable("test/integration/snapshotcloneserver/FakeCurveFsClient.CreateCloneChunk", // NOLINT
        1, NULL, 0);

    int ret = CloneOrRecover("Recover", testUser1, testSnapId_, testFile1, true,
        &uuid1);
    ASSERT_EQ(kErrCodeInternalError, ret);

    std::this_thread::sleep_for(std::chrono::milliseconds(3000));

    fiu_disable("test/integration/snapshotcloneserver/FakeCurveFsClient.CreateCloneChunk");  // NOLINT

    ASSERT_TRUE(JudgeCloneTaskFailCleanEnvAndCheck(
                testUser1, uuid1));
}

TEST_F(SnapshotCloneServerTest, TestLazyRecoverSnapFailOnGetChunkIndexData) {
    ASSERT_TRUE(PrepreTestSnapshotIfNotExist());
    std::string uuid1;

    fiu_enable("test/integration/snapshotcloneserver/FakeSnapshotDataStore.GetChunkIndexData", // NOLINT
        1, NULL, 0);

    int ret = CloneOrRecover("Recover", testUser1, testSnapId_, testFile1, true,
        &uuid1);
    ASSERT_EQ(kErrCodeInternalError, ret);

    std::this_thread::sleep_for(std::chrono::milliseconds(3000));

    fiu_disable("test/integration/snapshotcloneserver/FakeSnapshotDataStore.GetChunkIndexData");  // NOLINT

    ASSERT_TRUE(JudgeCloneTaskFailCleanEnvAndCheck(
                testUser1, uuid1));
}

TEST_F(SnapshotCloneServerTest, TestLazyRecoverSnapFailOnAddCloneInfo) {
    ASSERT_TRUE(PrepreTestSnapshotIfNotExist());
    std::string uuid1;

    fiu_enable("test/integration/snapshotcloneserver/FakeSnapshotCloneMetaStore.AddCloneInfo", // NOLINT
        1, NULL, 0);

    int ret = CloneOrRecover("Recover", testUser1, testSnapId_, testFile1, true,
        &uuid1);
    ASSERT_EQ(kErrCodeInternalError, ret);

    fiu_disable("test/integration/snapshotcloneserver/FakeSnapshotCloneMetaStore.AddCloneInfo");  // NOLINT

    ASSERT_TRUE(JudgeCloneTaskNotExistCleanEnvAndCheck(
                testUser1, uuid1));
}

TEST_F(SnapshotCloneServerTest, TestLazyRecoverSnapFailOnChangeOwner) {
    ASSERT_TRUE(PrepreTestSnapshotIfNotExist());
    std::string uuid1;

    fiu_enable("test/integration/snapshotcloneserver/FakeCurveFsClient.ChangeOwner", // NOLINT
        1, NULL, 0);

    int ret = CloneOrRecover("Recover", testUser1, testSnapId_, testFile1, true,
        &uuid1);
    ASSERT_EQ(kErrCodeInternalError, ret);

    std::this_thread::sleep_for(std::chrono::milliseconds(3000));

    fiu_disable("test/integration/snapshotcloneserver/FakeCurveFsClient.ChangeOwner");  // NOLINT

    ASSERT_TRUE(JudgeCloneTaskFailCleanEnvAndCheck(
                testUser1, uuid1));
}


TEST_F(SnapshotCloneServerTest,
    TestLazyRecoverSnapSuccessWhenRecoverChunkFailOneTime) {
    ASSERT_TRUE(PrepreTestSnapshotIfNotExist());
    std::string uuid1;

    fiu_enable("test/integration/snapshotcloneserver/FakeCurveFsClient.RecoverChunk", // NOLINT
        1, NULL, FIU_ONETIME);

    int ret = CloneOrRecover("Recover", testUser1, testSnapId_,
        testFile1, true,
        &uuid1);
    ASSERT_EQ(0, ret);

    // Flatten
    ret = Flatten(testUser1, uuid1);
    ASSERT_EQ(0, ret);

    bool success1 = CheckCloneOrRecoverSuccess(testUser1, uuid1, false);
    ASSERT_TRUE(success1);

    fiu_disable("test/integration/snapshotcloneserver/FakeCurveFsClient.RecoverChunk");  // NOLINT
}

TEST_F(SnapshotCloneServerTest, TestNotLazyRecoverSnapFailOnRecoverChunk) {
    ASSERT_TRUE(PrepreTestSnapshotIfNotExist());
    std::string uuid1;

    fiu_enable("test/integration/snapshotcloneserver/FakeCurveFsClient.RecoverChunk", // NOLINT
        1, NULL, 0);

    int ret = CloneOrRecover("Recover", testUser1, testSnapId_,
        testFile1, false,
        &uuid1);
    ASSERT_EQ(0, ret);

    std::this_thread::sleep_for(std::chrono::milliseconds(3000));

    fiu_disable("test/integration/snapshotcloneserver/FakeCurveFsClient.RecoverChunk");  // NOLINT

    ASSERT_TRUE(JudgeCloneTaskFailCleanEnvAndCheck(
                testUser1, uuid1));
}

TEST_F(SnapshotCloneServerTest, TestNotLazyRecoverSnapFailOnRenameCloneFile) {
    ASSERT_TRUE(PrepreTestSnapshotIfNotExist());
    std::string uuid1;

    fiu_enable("test/integration/snapshotcloneserver/FakeCurveFsClient.RenameCloneFile", // NOLINT
        1, NULL, 0);

    int ret = CloneOrRecover("Recover", testUser1, testSnapId_,
        testFile1, false,
        &uuid1);
    ASSERT_EQ(0, ret);

    std::this_thread::sleep_for(std::chrono::milliseconds(3000));

    fiu_disable("test/integration/snapshotcloneserver/FakeCurveFsClient.RenameCloneFile");  // NOLINT

    ASSERT_TRUE(JudgeCloneTaskFailCleanEnvAndCheck(
                testUser1, uuid1));
}

TEST_F(SnapshotCloneServerTest, TestNotLazyRecoverSnapFailOnChangeOwner) {
    ASSERT_TRUE(PrepreTestSnapshotIfNotExist());
    std::string uuid1;

    fiu_enable("test/integration/snapshotcloneserver/FakeCurveFsClient.ChangeOwner", // NOLINT
        1, NULL, 0);

    int ret = CloneOrRecover("Recover", testUser1, testSnapId_,
        testFile1, false,
        &uuid1);
    ASSERT_EQ(0, ret);

    std::this_thread::sleep_for(std::chrono::milliseconds(3000));

    fiu_disable("test/integration/snapshotcloneserver/FakeCurveFsClient.ChangeOwner");  // NOLINT

    ASSERT_TRUE(JudgeCloneTaskFailCleanEnvAndCheck(
                testUser1, uuid1));
}

TEST_F(SnapshotCloneServerTest, TestNotLazyRecoverSnapFailOnCompleteCloneFile) {
    ASSERT_TRUE(PrepreTestSnapshotIfNotExist());
    std::string uuid1;

    fiu_enable("test/integration/snapshotcloneserver/FakeCurveFsClient.CompleteCloneFile", // NOLINT
        1, NULL, 0);

    int ret = CloneOrRecover("Recover", testUser1, testSnapId_,
        testFile1, false,
        &uuid1);
    ASSERT_EQ(0, ret);

    std::this_thread::sleep_for(std::chrono::milliseconds(3000));

    fiu_disable("test/integration/snapshotcloneserver/FakeCurveFsClient.CompleteCloneFile");  // NOLINT

    ASSERT_TRUE(JudgeCloneTaskFailCleanEnvAndCheck(
                testUser1, uuid1));
}

TEST_F(SnapshotCloneServerTest,
    TestLazyRecoverSnapFailOnFileNotExistWhenRecoverChunk) {
    ASSERT_TRUE(PrepreTestSnapshotIfNotExist());
    std::string uuid1;

    fiu_enable("test/integration/snapshotcloneserver/FakeCurveFsClient.RecoverChunk", // NOLINT
        1, NULL, 0);

    int ret = CloneOrRecover("Recover", testUser1, testSnapId_,
        testFile1, true,
        &uuid1);
    ASSERT_EQ(0, ret);

    // Flatten
    ret = Flatten(testUser1, uuid1);
    ASSERT_EQ(0, ret);

    // 恢复未完成前删除目标文件
    ASSERT_EQ(LIBCURVE_ERROR::OK,
        server_->GetCurveFsClient()->DeleteFile(testFile1, "", 0));

    std::this_thread::sleep_for(std::chrono::milliseconds(3000));

    ASSERT_TRUE(JudgeCloneTaskFailCleanEnvAndCheck(
                testUser1, uuid1));

    fiu_disable("test/integration/snapshotcloneserver/FakeCurveFsClient.RecoverChunk");  // NOLINT
}

}  // namespace snapshotcloneserver
}  // namespace curve
