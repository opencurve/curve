/*************************************************************************
> File Name: test_snapshot_meta_store.cpp
> Author:
> Created Time: Thu 27 Dec 2018 04:33:53 PM CST
> Copyright (c) 2018 netease
 ************************************************************************/

#include<iostream>
#include <gmock/gmock.h>  //NOLINT
#include <gtest/gtest.h>  //NOLINT
#include "src/snapshotcloneserver/common/snapshotclone_meta_store.h"
#include "test/snapshotcloneserver/mock_repo.h"
namespace curve {
namespace snapshotcloneserver {

class TestDBSnapshotCloneMetaStore : public ::testing::Test {
 public:
    TestDBSnapshotCloneMetaStore() {}
    virtual ~TestDBSnapshotCloneMetaStore() {}

    void SetUp() {
        repo = std::make_shared<MockRepo>();
        metastore_ = std::make_shared<DBSnapshotCloneMetaStore>(repo);
    }
    void TearDown() {}

    std::shared_ptr<MockRepo> repo;
    std::shared_ptr<DBSnapshotCloneMetaStore> metastore_;
    SnapshotCloneMetaStoreOptions options {
        .dbName = "curve_test",
        .dbUser = "root",
        .dbPassword = "qwer",
        .dbAddr = "localhost"
    };
};

TEST_F(TestDBSnapshotCloneMetaStore, testMetaStoreInit_ConnectDB) {
    EXPECT_CALL(*repo, connectDB(_, _, _, _))
        .Times(2)
        .WillOnce(Return(0))
        .WillOnce(Return(-1));
    EXPECT_CALL(*repo, createDatabase())
        .Times(1)
        .WillOnce(Return(0));
    EXPECT_CALL(*repo, useDataBase())
        .Times(1)
        .WillOnce(Return(0));
    EXPECT_CALL(*repo, createAllTables())
        .Times(1)
        .WillOnce(Return(0));
    EXPECT_CALL(*repo, LoadSnapshotRepoItems(_))
        .Times(1)
        .WillOnce(Return(0));
    EXPECT_CALL(*repo, LoadCloneRepoItems(_))
        .Times(1)
        .WillOnce(Return(0));
    ASSERT_EQ(0, metastore_->Init(options));
    ASSERT_EQ(-1, metastore_->Init(options));
}
TEST_F(TestDBSnapshotCloneMetaStore, testMetaStoreInit_CreateDB) {
    EXPECT_CALL(*repo, connectDB(_, _, _, _))
        .Times(2)
        .WillOnce(Return(0))
        .WillOnce(Return(0));
    EXPECT_CALL(*repo, createDatabase())
        .Times(2)
        .WillOnce(Return(0))
        .WillOnce(Return(-1));
    EXPECT_CALL(*repo, useDataBase())
        .Times(1)
        .WillOnce(Return(0));
    EXPECT_CALL(*repo, createAllTables())
        .Times(1)
        .WillOnce(Return(0));
    EXPECT_CALL(*repo, LoadSnapshotRepoItems(_))
        .Times(1)
        .WillOnce(Return(0));
    EXPECT_CALL(*repo, LoadCloneRepoItems(_))
        .Times(1)
        .WillOnce(Return(0));
    ASSERT_EQ(0, metastore_->Init(options));
    ASSERT_EQ(-1, metastore_->Init(options));
}
TEST_F(TestDBSnapshotCloneMetaStore, testMetaStoreInit_UseDB) {
    EXPECT_CALL(*repo, connectDB(_, _, _, _))
        .Times(2)
        .WillOnce(Return(0))
        .WillOnce(Return(0));
    EXPECT_CALL(*repo, createDatabase())
        .Times(2)
        .WillOnce(Return(0))
        .WillOnce(Return(0));
    EXPECT_CALL(*repo, useDataBase())
        .Times(2)
        .WillOnce(Return(0))
        .WillOnce(Return(-1));
    EXPECT_CALL(*repo, createAllTables())
        .Times(1)
        .WillOnce(Return(0));
    EXPECT_CALL(*repo, LoadSnapshotRepoItems(_))
        .Times(1)
        .WillOnce(Return(0));
    EXPECT_CALL(*repo, LoadCloneRepoItems(_))
        .Times(1)
        .WillOnce(Return(0));
    ASSERT_EQ(0, metastore_->Init(options));
    ASSERT_EQ(-1, metastore_->Init(options));
}
TEST_F(TestDBSnapshotCloneMetaStore, testMetaStoreInit_CreateTable) {
    EXPECT_CALL(*repo, connectDB(_, _, _, _))
        .Times(2)
        .WillOnce(Return(0))
        .WillOnce(Return(0));
    EXPECT_CALL(*repo, createDatabase())
        .Times(2)
        .WillOnce(Return(0))
        .WillOnce(Return(0));
    EXPECT_CALL(*repo, useDataBase())
        .Times(2)
        .WillOnce(Return(0))
        .WillOnce(Return(0));
    EXPECT_CALL(*repo, createAllTables())
        .Times(2)
        .WillOnce(Return(0))
        .WillOnce(Return(-1));
    EXPECT_CALL(*repo, LoadSnapshotRepoItems(_))
        .Times(1)
        .WillOnce(Return(0));
    EXPECT_CALL(*repo, LoadCloneRepoItems(_))
        .Times(1)
        .WillOnce(Return(0));
    ASSERT_EQ(0, metastore_->Init(options));
    ASSERT_EQ(-1, metastore_->Init(options));
}
TEST_F(TestDBSnapshotCloneMetaStore, testMetaStoreInit_LoadSnapshotInfo) {
    EXPECT_CALL(*repo, connectDB(_, _, _, _))
        .Times(2)
        .WillOnce(Return(0))
        .WillOnce(Return(0));
    EXPECT_CALL(*repo, createDatabase())
        .Times(2)
        .WillOnce(Return(0))
        .WillOnce(Return(0));
    EXPECT_CALL(*repo, useDataBase())
        .Times(2)
        .WillOnce(Return(0))
        .WillOnce(Return(0));
    EXPECT_CALL(*repo, createAllTables())
        .Times(2)
        .WillOnce(Return(0))
        .WillOnce(Return(0));
    EXPECT_CALL(*repo, LoadSnapshotRepoItems(_))
        .Times(2)
        .WillOnce(Return(0))
        .WillOnce(Return(-1));
    EXPECT_CALL(*repo, LoadCloneRepoItems(_))
        .Times(1)
        .WillOnce(Return(0));
    ASSERT_EQ(0, metastore_->Init(options));
    ASSERT_EQ(-1, metastore_->Init(options));
}
TEST_F(TestDBSnapshotCloneMetaStore, testMetaStoreInit_LoadCloneInfo) {
    EXPECT_CALL(*repo, connectDB(_, _, _, _))
        .Times(2)
        .WillOnce(Return(0))
        .WillOnce(Return(0));
    EXPECT_CALL(*repo, createDatabase())
        .Times(2)
        .WillOnce(Return(0))
        .WillOnce(Return(0));
    EXPECT_CALL(*repo, useDataBase())
        .Times(2)
        .WillOnce(Return(0))
        .WillOnce(Return(0));
    EXPECT_CALL(*repo, createAllTables())
        .Times(2)
        .WillOnce(Return(0))
        .WillOnce(Return(0));
    EXPECT_CALL(*repo, LoadSnapshotRepoItems(_))
        .Times(2)
        .WillOnce(Return(0))
        .WillOnce(Return(0));
    EXPECT_CALL(*repo, LoadCloneRepoItems(_))
        .Times(2)
        .WillOnce(Return(0))
        .WillOnce(Return(-1));
    ASSERT_EQ(0, metastore_->Init(options));
    ASSERT_EQ(-1, metastore_->Init(options));
}
TEST_F(TestDBSnapshotCloneMetaStore, testMetaStoreAddSnapshot) {
      EXPECT_CALL(*repo, InsertSnapshotRepoItem(_))
        .Times(2)
        .WillOnce(Return(0))
        .WillOnce(Return(-1));
    std::string uuid = "this-is-test-uuid";
    SnapshotInfo info(uuid, "curve1", "test", "mysnap");
    ASSERT_EQ(0, metastore_->AddSnapshot(info));
    ASSERT_EQ(-1, metastore_->AddSnapshot(info));
}
TEST_F(TestDBSnapshotCloneMetaStore, testMetaStoreDeleteSnapshot) {
  EXPECT_CALL(*repo, DeleteSnapshotRepoItem(_))
        .Times(2)
        .WillOnce(Return(0))
        .WillOnce(Return(-1));
    std::string uuid = "this-is-test-uuid";
    ASSERT_EQ(0, metastore_->DeleteSnapshot(uuid));
    ASSERT_EQ(-1, metastore_->DeleteSnapshot(uuid));
}
TEST_F(TestDBSnapshotCloneMetaStore, testMetaStoreUpdateSnapshot) {
  EXPECT_CALL(*repo, UpdateSnapshotRepoItem(_))
        .Times(2)
        .WillOnce(Return(0))
        .WillOnce(Return(-1));
    std::string uuid = "this-is-test-uuid";
    std::string filename = "test";
    const uint64_t fileLength = 10*1024*1024*1024;
    const uint64_t segmentSize = 1024*1024*1024;
    const uint32_t chunkSize = 16*1024*1024;
    const uint64_t time = 999999;
    SnapshotInfo info(uuid,
                            "curve1",
                            filename,
                            "mysnap",
                            1,
                            chunkSize,
                            segmentSize,
                            fileLength,
                            time,
                             Status::done);

    ASSERT_EQ(0, metastore_->UpdateSnapshot(info));
    ASSERT_EQ(-1, metastore_->UpdateSnapshot(info));
}
TEST_F(TestDBSnapshotCloneMetaStore, testMetaStoreGetSnapshot) {
     EXPECT_CALL(*repo, InsertSnapshotRepoItem(_))
        .Times(1)
        .WillOnce(Return(0));
    std::string uuid = "this-is-test-uuid";
    SnapshotInfo info(uuid, "curve1", "test", "mysnap");
    SnapshotInfo tmpinfo;
    metastore_->AddSnapshot(info);
    ASSERT_EQ(0, metastore_->GetSnapshotInfo(uuid, &tmpinfo));
    ASSERT_EQ(-1, metastore_->GetSnapshotInfo("test", &tmpinfo));
}
TEST_F(TestDBSnapshotCloneMetaStore, testMetaStoreGetSnapshotList1) {
     EXPECT_CALL(*repo, InsertSnapshotRepoItem(_))
        .Times(3)
        .WillOnce(Return(0))
        .WillOnce(Return(0))
        .WillOnce(Return(0));
    SnapshotInfo info1("uuid1", "curve1", "test", "mysnap1");
    SnapshotInfo info2("uuid2", "curve1", "test", "mysnap2");
    SnapshotInfo info3("uuid3", "curve1", "test", "mysnap3");
    metastore_->AddSnapshot(info1);
    metastore_->AddSnapshot(info2);
    metastore_->AddSnapshot(info3);
    std::vector<SnapshotInfo> v;
    ASSERT_EQ(0, metastore_->GetSnapshotList("test", &v));
    v.clear();
    ASSERT_EQ(-1, metastore_->GetSnapshotList("null", &v));
}
TEST_F(TestDBSnapshotCloneMetaStore, testMetaStoreGetSnapshotList2) {
     EXPECT_CALL(*repo, InsertSnapshotRepoItem(_))
        .Times(3)
        .WillOnce(Return(0))
        .WillOnce(Return(0))
        .WillOnce(Return(0));
    SnapshotInfo info1("uuid1", "curve1", "test", "mysnap1");
    SnapshotInfo info2("uuid2", "curve1", "test", "mysnap2");
    SnapshotInfo info3("uuid3", "curve1", "test", "mysnap3");
    metastore_->AddSnapshot(info1);
    metastore_->AddSnapshot(info2);
    metastore_->AddSnapshot(info3);
    std::vector<SnapshotInfo> v;
    ASSERT_EQ(0, metastore_->GetSnapshotList(&v));
}
TEST_F(TestDBSnapshotCloneMetaStore, testMetaStoreAddCloneInfo) {
      EXPECT_CALL(*repo, InsertCloneRepoItem(_))
        .Times(2)
        .WillOnce(Return(0))
        .WillOnce(Return(-1));
    std::string taskId = "this-is-test-taskID";
    CloneInfo info(taskId,
                    "user1",
                    CloneTaskType::kClone,
                    "src",
                    "dest",
                    CloneFileType::kFile,
                    true);
    ASSERT_EQ(0, metastore_->AddCloneInfo(info));
    ASSERT_EQ(-1, metastore_->AddCloneInfo(info));
}
TEST_F(TestDBSnapshotCloneMetaStore, testMetaStoreDeleteCloneInfo) {
  EXPECT_CALL(*repo, DeleteCloneRepoItem(_))
        .Times(2)
        .WillOnce(Return(0))
        .WillOnce(Return(-1));
    std::string taskId = "this-is-test-taskID";
    ASSERT_EQ(0, metastore_->DeleteCloneInfo(taskId));
    ASSERT_EQ(-1, metastore_->DeleteCloneInfo(taskId));
}
TEST_F(TestDBSnapshotCloneMetaStore, testMetaStoreUpdateCloneInfo) {
  EXPECT_CALL(*repo, UpdateCloneRepoItem(_))
        .Times(2)
        .WillOnce(Return(0))
        .WillOnce(Return(-1));
    std::string taskId = "this-is-test-taskID";
    CloneInfo info(taskId,
                  "user1",
                  CloneTaskType::kClone,
                  "src",
                  "dest",
                  CloneFileType::kFile,
                  true);
    ASSERT_EQ(0, metastore_->UpdateCloneInfo(info));
    ASSERT_EQ(-1, metastore_->UpdateCloneInfo(info));
}
TEST_F(TestDBSnapshotCloneMetaStore, testMetaStoreGetCloneInfo) {
     EXPECT_CALL(*repo, InsertCloneRepoItem(_))
        .Times(1)
        .WillOnce(Return(0));
    std::string taskId = "this-is-test-taskID";
    CloneInfo info(taskId,
                  "user1",
                  CloneTaskType::kClone,
                  "src",
                  "dest",
                  CloneFileType::kFile,
                  true);
    CloneInfo tmpinfo;
    std::string tmpId = "test";
    metastore_->AddCloneInfo(info);
    ASSERT_EQ(0, metastore_->GetCloneInfo(taskId, &tmpinfo));
    ASSERT_EQ(-1, metastore_->GetCloneInfo(tmpId, &tmpinfo));
}
TEST_F(TestDBSnapshotCloneMetaStore, testMetaStoreGetCloneInfoList) {
     EXPECT_CALL(*repo, InsertCloneRepoItem(_))
        .Times(3)
        .WillOnce(Return(0))
        .WillOnce(Return(0))
        .WillOnce(Return(0));
    std::string taskId1 = "this-is-test-taskId1";
    std::string taskId2 = "this-is-test-taskId2";
    std::string taskId3 = "this-is-test-taskId3";
    CloneInfo info1(taskId1,
                    "user1",
                    CloneTaskType::kClone,
                    "src1",
                    "dest1",
                    CloneFileType::kFile,
                    true);
    CloneInfo info2(taskId2,
                    "user1",
                    CloneTaskType::kClone,
                    "src2",
                    "dest2",
                    CloneFileType::kFile,
                    true);
    CloneInfo info3(taskId3,
                    "user1",
                    CloneTaskType::kClone,
                    "src3",
                    "dest3",
                    CloneFileType::kFile,
                    true);
    metastore_->AddCloneInfo(info1);
    metastore_->AddCloneInfo(info2);
    metastore_->AddCloneInfo(info3);
    std::vector<CloneInfo> v;
    ASSERT_EQ(0, metastore_->GetCloneInfoList(&v));
    v.clear();
}
}  // namespace snapshotcloneserver
}  // namespace curve

