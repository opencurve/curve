/*************************************************************************
> File Name: test_snapshot_data_store.cpp
> Author:
> Created Time: Thu 27 Dec 2018 04:33:20 PM CST
> Copyright (c) 2018 netease
 ************************************************************************/

#include<iostream>
#include <gmock/gmock.h>  //NOLINT
#include <gtest/gtest.h>  //NOLINT
#include "src/snapshotcloneserver/snapshot/snapshot_data_store_s3.h"
#include "src/snapshotcloneserver/snapshot/snapshot_data_store.h"
#include "test/snapshotcloneserver/mock_s3_adapter.h"
using ::testing::_;
namespace curve {
namespace snapshotcloneserver {

class TestS3SnapshotDataStore : public ::testing::Test {
 public:
    TestS3SnapshotDataStore() {}
    virtual ~TestS3SnapshotDataStore() {}

    void SetUp() {
        store_ = new S3SnapshotDataStore();
        adapter = std::make_shared<MockS3Adapter>();
        store_->SetAdapter(adapter);
    }
    void TearDown() {
        delete store_;
    }

    S3SnapshotDataStore *store_;
    std::shared_ptr<MockS3Adapter> adapter;
};


TEST_F(TestS3SnapshotDataStore, testInit) {
    EXPECT_CALL(*adapter, Init()).Times(3);
    EXPECT_CALL(*adapter, BucketExist())
        .Times(3)
        .WillOnce(Return(true))
        .WillOnce(Return(false))
        .WillOnce(Return(false));
    EXPECT_CALL(*adapter, CreateBucket())
        .Times(2)
        .WillOnce(Return(0))
        .WillOnce(Return(-1));
    ASSERT_EQ(0, store_->Init());
    ASSERT_EQ(0, store_->Init());
    ASSERT_EQ(-1, store_->Init());
}
TEST_F(TestS3SnapshotDataStore, testChunkIndexDataExist) {
    ChunkIndexDataName indexDataName("test", 1);
    Aws::String obj = "test-1";
    EXPECT_CALL(*adapter, ObjectExist(obj))
        .Times(2)
        .WillOnce(Return(true))
        .WillOnce(Return(false));
    ASSERT_EQ(true, store_->ChunkIndexDataExist(indexDataName));
    ASSERT_EQ(false, store_->ChunkIndexDataExist(indexDataName));
}

TEST_F(TestS3SnapshotDataStore, testPutIndexChunkData) {
    ChunkIndexData indexData;
    ChunkIndexDataName indexDataName("test", 1);
    Aws::String obj = "test-1";
    ChunkDataName cdName("test", 1, 1);
    indexData.PutChunkDataName(cdName);
    std::string cxt;
    indexData.Serialize(&cxt);
    EXPECT_CALL(*adapter, PutObject(obj, cxt))
        .Times(2)
        .WillOnce(Return(0))
        .WillOnce(Return(-1));
    ASSERT_EQ(0, store_->PutChunkIndexData(indexDataName, indexData));
    ASSERT_EQ(-1, store_->PutChunkIndexData(indexDataName, indexData));
}
TEST_F(TestS3SnapshotDataStore, testGetIndexChunk) {
    ChunkIndexData indexData;
    Aws::String obj = "test-1";
    ChunkIndexDataName indexDataName("test", 1);
    EXPECT_CALL(*adapter, GetObject(_, _))
        .Times(2)
        .WillOnce(Return(-1))
        .WillOnce(Return(0));
    ASSERT_EQ(-1, store_->GetChunkIndexData(indexDataName, &indexData));
    ASSERT_EQ(0, store_->GetChunkIndexData(indexDataName, &indexData));
}
TEST_F(TestS3SnapshotDataStore, testDeleteIndexChunk) {
    ChunkIndexDataName indexDataName("test", 1);
    Aws::String obj = "test-1";
    EXPECT_CALL(*adapter, DeleteObject(obj))
        .Times(2)
        .WillOnce(Return(0))
        .WillOnce(Return(-1));
    ASSERT_EQ(0, store_->DeleteChunkIndexData(indexDataName));
    ASSERT_EQ(-1, store_->DeleteChunkIndexData(indexDataName));
}

TEST_F(TestS3SnapshotDataStore, testDataChunkOp) {
    ChunkDataName cdName("test", 1, 1);
    std::string cdKey = cdName.ToDataChunkKey();
    ChunkDataName tmp = cdName.ToChunkDataName(cdKey);
    Aws::String obj = "test-1-1";
    EXPECT_CALL(*adapter, ObjectExist(obj))
        .Times(2)
        .WillOnce(Return(true))
        .WillOnce(Return(false));
    ASSERT_EQ(true, store_->ChunkDataExist(cdName));
    ASSERT_EQ(false, store_->ChunkDataExist(cdName));
}

TEST_F(TestS3SnapshotDataStore, testDataChunkTransferInit) {
    ChunkDataName cdName("test", 1, 1);
    Aws::String uploadID = "test-uploadID";
    Aws::String null_uploadID = "";
    std::shared_ptr<TransferTask> task = std::make_shared<TransferTask>();
    EXPECT_CALL(*adapter, MultiUploadInit(_))
        .Times(2)
        .WillOnce(Return(uploadID))
        .WillOnce(Return(null_uploadID));
    ASSERT_EQ(0, store_->DataChunkTranferInit(cdName, task));
    ASSERT_EQ(-1, store_->DataChunkTranferInit(cdName, task));
}
TEST_F(TestS3SnapshotDataStore, testDataChunkTransferAddPart) {
    ChunkDataName cdName("test", 1, 1);
    Aws::String dataobj = "test-1-1";
    Aws::String uploadID = "test-uploadID";
    Aws::String null_uploadID = "";
    std::shared_ptr<TransferTask> task = std::make_shared<TransferTask>();
    char* buf = new char[1024*1024];
    memset(buf, 0, 1024*1024);
    Aws::S3::Model::CompletedPart cp =
        Aws::S3::Model::CompletedPart().WithETag("mytest").WithPartNumber(1);
    Aws::S3::Model::CompletedPart cp_err =
        Aws::S3::Model::CompletedPart().WithETag("errorTag").WithPartNumber(-1);
    EXPECT_CALL(*adapter, UploadOnePart(_, _, _, _, _))
        .Times(2)
        .WillOnce(Return(cp))
        .WillOnce(Return(cp_err));
    ASSERT_EQ(0, store_->
              DataChunkTranferAddPart(cdName, task, 1, 1024*1024, buf));
    ASSERT_EQ(-1, store_->
              DataChunkTranferAddPart(cdName, task, 2, 1024*1024, buf));
    delete [] buf;
}
TEST_F(TestS3SnapshotDataStore, testDataChunkTransferComplete) {
    ChunkDataName cdName("test", 1, 1);
    std::shared_ptr<TransferTask> task = std::make_shared<TransferTask>();
    EXPECT_CALL(*adapter, CompleteMultiUpload(_, _, _))
        .Times(2)
        .WillOnce(Return(0))
        .WillOnce(Return(-1));
    ASSERT_EQ(0, store_->DataChunkTranferComplete(cdName, task));
    ASSERT_EQ(-1, store_->DataChunkTranferComplete(cdName, task));
}
TEST_F(TestS3SnapshotDataStore, testDataChunkTransferAbort) {
    ChunkDataName cdName("test", 1, 1);
    std::shared_ptr<TransferTask> task = std::make_shared<TransferTask>();
    EXPECT_CALL(*adapter, AbortMultiUpload(_, _))
        .Times(2)
        .WillOnce(Return(0))
        .WillOnce(Return(-1));
    ASSERT_EQ(0, store_->DataChunkTranferAbort(cdName, task));
    ASSERT_EQ(-1, store_->DataChunkTranferAbort(cdName, task));
}

TEST_F(TestS3SnapshotDataStore, testDeleteDataChunk) {
    ChunkDataName cdName("test", 1, 1);
    EXPECT_CALL(*adapter, DeleteObject(_))
        .Times(2)
        .WillOnce(Return(0))
        .WillOnce(Return(-1));
    ASSERT_EQ(0, store_->DeleteChunkData(cdName));
    ASSERT_EQ(-1, store_->DeleteChunkData(cdName));
}

}  // namespace snapshotcloneserver
}  // namespace curve

