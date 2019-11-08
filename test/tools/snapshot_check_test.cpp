/*
 * Project: curve
 * File Created: 2019-10-21
 * Author: charisu
 * Copyright (c)￼ 2018 netease
 */

#include <gtest/gtest.h>

#include "src/tools/snapshot_check.h"
#include "test/client/mock_file_client.h"
#include "test/tools/mock_snapshot_read.h"

using curve::client::MockFileClient;
using curve::tool::MockSnapshotRead;
using ::testing::_;
using ::testing::Return;
using ::testing::SetArrayArgument;
using ::testing::SetArgPointee;
using ::testing::DoAll;

DECLARE_string(file_name);

class SnapshotCheckTest : public ::testing::Test {
 protected:
    SnapshotCheckTest() {}
    void SetUp() {
        FLAGS_file_name = "test";
        // 初始化mock对象
        client_ = std::make_shared<MockFileClient>();
        snapshot_ = std::make_shared<MockSnapshotRead>();
        buf_ = new char[chunkSize_];
        memset(buf_, '1', chunkSize_);
        buf2_ = new char[chunkSize_];
        memset(buf2_, '2', chunkSize_);
    }

    void TearDown() {
        client_ = nullptr;
        snapshot_ = nullptr;
        delete[] buf_;
        delete[] buf2_;
    }

    void GetFileInfoForTest(FileStatInfo* fileInfo) {
        fileInfo->length = 160 * 1024 * 1024;
    }

    void GetSnapshotInfoForTest(SnapshotRepoItem* item) {
        item->fileName = "test";
        item->chunkSize = chunkSize_;
        item->fileLength = 10*chunkSize_;
    }

    std::shared_ptr<MockSnapshotRead> snapshot_;
    std::shared_ptr<MockFileClient> client_;
    char* buf_;
    char* buf2_;
    uint64_t chunkSize_ = 16777216;
};


TEST_F(SnapshotCheckTest, common) {
    FileStatInfo fileInfo;
    GetFileInfoForTest(&fileInfo);
    SnapshotRepoItem snapshotInfo;
    GetSnapshotInfoForTest(&snapshotInfo);
    curve::tool::SnapshotCheck snapshotCheck(client_, snapshot_);

    EXPECT_CALL(*client_, Init(_))
        .Times(1)
        .WillOnce(Return(0));
    EXPECT_CALL(*snapshot_, Init(_, _))
        .Times(1)
        .WillOnce(Return(0));
    ASSERT_EQ(0, snapshotCheck.Init());

    // 快照一致性检查
    EXPECT_CALL(*snapshot_, GetSnapshotInfo(_))
        .Times(1)
        .WillOnce(SetArgPointee<0>(snapshotInfo));
    EXPECT_CALL(*snapshot_, Read(_, _, _))
        .Times(10)
        .WillRepeatedly(DoAll(SetArrayArgument<0>(buf_, buf_ + chunkSize_),
                            Return(0)));
    EXPECT_CALL(*client_, Open4ReadOnly(_, _))
        .Times(1)
        .WillOnce(Return(1));
    EXPECT_CALL(*client_, StatFile(_, _, _))
        .Times(1)
        .WillOnce(DoAll(SetArgPointee<2>(fileInfo),
                            Return(0)));
    EXPECT_CALL(*client_, Read(_, _, _, _))
        .Times(10)
        .WillRepeatedly(DoAll(SetArrayArgument<1>(buf_, buf_ + chunkSize_),
                            Return(chunkSize_)));

    ASSERT_EQ(0, snapshotCheck.Check());
    snapshotCheck.UnInit();
}

TEST_F(SnapshotCheckTest, initError) {
    curve::tool::SnapshotCheck snapshotCheck(client_, snapshot_);

    // init client fail
    EXPECT_CALL(*client_, Init(_))
        .Times(1)
        .WillOnce(Return(-1));
    ASSERT_EQ(-1, snapshotCheck.Init());

    // init snapshot fail
    EXPECT_CALL(*snapshot_, Init(_, _))
        .Times(1)
        .WillOnce(Return(-1));
    ASSERT_EQ(-1, snapshotCheck.Init());
}

TEST_F(SnapshotCheckTest, checkError) {
    curve::tool::SnapshotCheck snapshotCheck(client_, snapshot_);
    FileStatInfo fileInfo;
    GetFileInfoForTest(&fileInfo);
    SnapshotRepoItem snapshotInfo;

    // 打开文件出错
    EXPECT_CALL(*client_, Open4ReadOnly(_, _))
        .Times(7)
        .WillOnce(Return(-1))
        .WillRepeatedly(Return(1));
    ASSERT_EQ(-1, snapshotCheck.Check());

    // 获取文件信息
    EXPECT_CALL(*client_, StatFile(_, _, _))
        .Times(6)
        .WillOnce(Return(-1))
        .WillRepeatedly(DoAll(SetArgPointee<2>(fileInfo),
                            Return(0)));
    ASSERT_EQ(-1, snapshotCheck.Check());

    // 文件长度不匹配
    snapshotInfo.fileLength = 1234;
    EXPECT_CALL(*snapshot_, GetSnapshotInfo(_))
        .Times(1)
        .WillOnce(SetArgPointee<0>(snapshotInfo));
    ASSERT_EQ(-1, snapshotCheck.Check());

    GetSnapshotInfoForTest(&snapshotInfo);
    EXPECT_CALL(*snapshot_, GetSnapshotInfo(_))
        .Times(4)
        .WillRepeatedly(SetArgPointee<0>(snapshotInfo));

    // 读文件出错
    EXPECT_CALL(*client_, Read(_, _, _, _))
        .Times(2)
        .WillOnce(Return(-1))
        .WillOnce(DoAll(SetArrayArgument<1>(buf_, buf_ + chunkSize_),
                            Return(1024)));
    ASSERT_EQ(-1, snapshotCheck.Check());
    ASSERT_EQ(-1, snapshotCheck.Check());

    // 读快照出错
    EXPECT_CALL(*client_, Read(_, _, _, _))
        .Times(2)
        .WillRepeatedly(DoAll(SetArrayArgument<1>(buf_, buf_ + chunkSize_),
                            Return(chunkSize_)));
    EXPECT_CALL(*snapshot_, Read(_, _, _))
        .Times(1)
        .WillOnce(Return(-1));
    ASSERT_EQ(-1, snapshotCheck.Check());

    // 读出来的数据不一致
    EXPECT_CALL(*snapshot_, Read(_, _, _))
        .Times(1)
        .WillOnce(DoAll(SetArrayArgument<0>(buf2_, buf2_ + chunkSize_),
                            Return(0)));
    ASSERT_EQ(-1, snapshotCheck.Check());
}
