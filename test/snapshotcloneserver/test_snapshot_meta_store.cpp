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
    ASSERT_EQ(0, metastore_->Init());
    ASSERT_EQ(-1, metastore_->Init());
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
    ASSERT_EQ(0, metastore_->Init());
    ASSERT_EQ(-1, metastore_->Init());
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
    ASSERT_EQ(0, metastore_->Init());
    ASSERT_EQ(-1, metastore_->Init());
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
    ASSERT_EQ(0, metastore_->Init());
    ASSERT_EQ(-1, metastore_->Init());
}
TEST_F(TestDBSnapshotCloneMetaStore, testMetaStoreInit_LoadInfo) {
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
    ASSERT_EQ(0, metastore_->Init());
    ASSERT_EQ(-1, metastore_->Init());
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
}  // namespace snapshotcloneserver
}  // namespace curve

