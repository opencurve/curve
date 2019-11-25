/*
 * Project: curve
 * File Created: 2019-10-21
 * Author: charisu
 * Copyright (c)￼ 2018 netease
 */

#include <gtest/gtest.h>

#include "src/tools/snapshot_read.h"
#include "test/common/mock_s3_adapter.h"
#include "test/snapshotcloneserver/mock_repo.h"

using curve::common::MockS3Adapter;
using curve::snapshotcloneserver::MockRepo;
using ::testing::_;
using ::testing::Return;
using ::testing::SetArrayArgument;
using ::testing::SetArgPointee;
using ::testing::DoAll;

DECLARE_uint64(offset);
DECLARE_uint64(len);

class SnapshotReadTest : public ::testing::Test {
 protected:
    SnapshotReadTest() {}
    void SetUp() {
        // 初始化mock对象
        s3Adapter_ = std::make_shared<MockS3Adapter>();
        repo_ = std::make_shared<MockRepo>();
        buf_ = new char[chunkSize_];
        memset(buf_, '1', chunkSize_);
        snapBuf_ = new char[len_];
        expectBuf_ = new char[len_];
        memset(expectBuf_, '1', len_);
        memset(expectBuf_ + chunkSize_, 0, chunkSize_);
        confPath_ = "./conf/snapshot_clone_server.conf";
    }

    void TearDown() {
        s3Adapter_ = nullptr;
        delete[] buf_;
        delete[] snapBuf_;
        delete[] expectBuf_;
    }

    void GetSnapsotItemForTest(SnapshotRepoItem* snapshotItem) {
        snapshotItem->fileName = "/test";
        snapshotItem->seqNum = 1;
        snapshotItem->fileLength = 100*chunkSize_;
        snapshotItem->chunkSize = chunkSize_;
    }

    void GetChunkMapForTest(std::string* chunkMap) {
        ChunkMap map;
        for (int i = 0; i < 10; ++i) {
            if (i == 1) {
                continue;
            }
            map.mutable_indexmap()->insert({i, "/test-0-1"});
            map.SerializeToString(chunkMap);
        }
    }

    std::shared_ptr<MockS3Adapter> s3Adapter_;
    std::shared_ptr<MockRepo> repo_;
    char* buf_;
    char* snapBuf_;
    char* expectBuf_;
    uint64_t chunkSize_ = 16777216;
    uint64_t offset_ = 0;
    uint64_t len_ = 10*chunkSize_;
    std::string confPath_;
};

TEST_F(SnapshotReadTest, common) {
    curve::tool::SnapshotRead snapshotRead(repo_, s3Adapter_);
    SnapshotRepoItem snapshotItem;
    std::string chunkMap;
    GetSnapsotItemForTest(&snapshotItem);
    GetChunkMapForTest(&chunkMap);

    // 初始化
    EXPECT_CALL(*repo_, connectDB(_, _, _, _, _))
        .Times(1)
        .WillOnce(Return(OperationOK));
    EXPECT_CALL(*repo_, useDataBase())
        .Times(1)
        .WillOnce(Return(OperationOK));
    EXPECT_CALL(*repo_, QuerySnapshotRepoItem(_, _))
        .Times(1)
        .WillOnce(DoAll(SetArgPointee<1>(snapshotItem),
                Return(OperationOK)));
    ASSERT_EQ(0, snapshotRead.Init("123", confPath_));

    // 读快照，中间index为1的chunk不在chunkMap中
    EXPECT_CALL(*s3Adapter_, GetObject(_, _))
        .Times(1)
        .WillOnce(DoAll(SetArgPointee<1>(chunkMap),
                Return(0)));
    EXPECT_CALL(*s3Adapter_, GetObject(_, _, _, _))
        .Times(9)
        .WillRepeatedly(DoAll(SetArrayArgument<1>(buf_, buf_ + chunkSize_),
                Return(0)));
    ASSERT_EQ(0, snapshotRead.Read(snapBuf_, offset_, len_));
    ASSERT_EQ(0, strcmp(expectBuf_, snapBuf_));
    snapshotRead.UnInit();
}

TEST_F(SnapshotReadTest, initError) {
    curve::tool::SnapshotRead snapshotRead1(nullptr, s3Adapter_);
    ASSERT_EQ(-1, snapshotRead1.Init("123", confPath_));
    curve::tool::SnapshotRead snapshotRead2(repo_, nullptr);
    ASSERT_EQ(-1, snapshotRead2.Init("123", confPath_));
    curve::tool::SnapshotRead snapshotRead(repo_, s3Adapter_);
    // connectDB fail
    EXPECT_CALL(*repo_, connectDB(_, _, _, _, _))
        .Times(3)
        .WillOnce(Return(InternalError))
        .WillRepeatedly(Return(OperationOK));
    ASSERT_EQ(-1, snapshotRead.Init("123", confPath_));

    // useDataBase fail
    EXPECT_CALL(*repo_, useDataBase())
        .Times(2)
        .WillOnce(Return(InternalError))
        .WillRepeatedly(Return(OperationOK));
    ASSERT_EQ(-1, snapshotRead.Init("123", confPath_));

    // QuerySnapshotRepoItem fail
    EXPECT_CALL(*repo_, QuerySnapshotRepoItem(_, _))
        .Times(1)
        .WillOnce(Return(InternalError))
        .WillRepeatedly(Return(OperationOK));
    ASSERT_EQ(-1, snapshotRead.Init("123", confPath_));
}

TEST_F(SnapshotReadTest, readError) {
    curve::tool::SnapshotRead snapshotRead(repo_, s3Adapter_);
    SnapshotRepoItem snapshotItem;
    std::string chunkMap;
    GetSnapsotItemForTest(&snapshotItem);
    GetChunkMapForTest(&chunkMap);

    // 初始化
    EXPECT_CALL(*repo_, connectDB(_, _, _, _, _))
        .Times(1)
        .WillOnce(Return(OperationOK));
    EXPECT_CALL(*repo_, useDataBase())
        .Times(1)
        .WillOnce(Return(OperationOK));
    EXPECT_CALL(*repo_, QuerySnapshotRepoItem(_, _))
        .Times(1)
        .WillOnce(DoAll(SetArgPointee<1>(snapshotItem),
                Return(OperationOK)));
    ASSERT_EQ(0, snapshotRead.Init("123", confPath_));

    // InitChunkMap fail
    EXPECT_CALL(*s3Adapter_, GetObject(_, _))
        .Times(3)
        .WillOnce(Return(-1))
        .WillOnce(DoAll(SetArgPointee<1>("1234"),
                Return(0)))
        .WillRepeatedly(DoAll(SetArgPointee<1>(chunkMap),
                Return(0)));
    ASSERT_EQ(-1, snapshotRead.Read(snapBuf_, offset_, len_));
    ASSERT_EQ(-1, snapshotRead.Read(snapBuf_, offset_, len_));

    // 读快照，S3获取对象失败
    EXPECT_CALL(*s3Adapter_, GetObject(_, _, _, _))
        .Times(1)
        .WillOnce(Return(-1));
    ASSERT_EQ(-1, snapshotRead.Read(snapBuf_, offset_, len_));

    snapshotRead.UnInit();
}
