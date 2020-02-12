/*
 * Project: nebd
 * Created Date: Monday January 20th 2020
 * Author: yangyaokai
 * Copyright (c) 2020 netease
 */

#include <gtest/gtest.h>
#include <gmock/gmock.h>
#include <string>
#include <memory>

#include "src/part2/filerecord_manager.h"
#include "tests/part2/mock_metafile_manager.h"

namespace nebd {
namespace server {

using ::testing::_;
using ::testing::Return;
using ::testing::NotNull;
using ::testing::DoAll;
using ::testing::ReturnArg;
using ::testing::ElementsAre;
using ::testing::SetArgPointee;
using ::testing::SetArrayArgument;

class FileRecordManagerTest : public ::testing::Test {
 public:
    void SetUp() {
        metaFileManager_ = std::make_shared<MockMetaFileManager>();
        recordManager_ = std::make_shared<FileRecordManager>(metaFileManager_);
    }
    void TearDown() {}

 protected:
    std::shared_ptr<FileRecordManager> recordManager_;
    std::shared_ptr<MockMetaFileManager> metaFileManager_;
};

TEST_F(FileRecordManagerTest, BasicTest) {
    int fd1 = 1;
    std::string fileName1 = "file1";
    NebdFileRecord fileRecord1;

    // 初始测试
    ASSERT_FALSE(recordManager_->Exist(fd1));
    ASSERT_FALSE(recordManager_->GetRecord(fd1, &fileRecord1));
    ASSERT_FALSE(recordManager_->GetRecord(fileName1, &fileRecord1));

    uint64_t time1 = 1000;
    uint64_t time2 = 2000;
    // UpdateRecord
    fileRecord1.fileName = fileName1;
    fileRecord1.fd = fd1;
    fileRecord1.status = NebdFileStatus::OPENED;
    fileRecord1.timeStamp = time1;
    EXPECT_CALL(*metaFileManager_, UpdateMetaFile(_))
    .WillOnce(Return(0));
    ASSERT_TRUE(recordManager_->UpdateRecord(fileRecord1));
    ASSERT_TRUE(recordManager_->Exist(fd1));
    // 通过fd获取record
    NebdFileRecord tempRecord;
    ASSERT_TRUE(recordManager_->GetRecord(fd1, &tempRecord));
    ASSERT_EQ(tempRecord.fd, fd1);
    ASSERT_EQ(tempRecord.fileName, fileName1);
    // 通过filename获取record
    ASSERT_TRUE(recordManager_->GetRecord(fileName1, &tempRecord));
    ASSERT_EQ(tempRecord.fd, fd1);
    ASSERT_EQ(tempRecord.fileName, fileName1);

    // 插入一条filename相同，fd不同的记录
    fileRecord1.fd = ++fd1;
    EXPECT_CALL(*metaFileManager_, UpdateMetaFile(_))
    .WillOnce(Return(0));
    ASSERT_TRUE(recordManager_->UpdateRecord(fileRecord1));
    ASSERT_FALSE(recordManager_->Exist(fd1-1));
    ASSERT_TRUE(recordManager_->Exist(fd1));

    // get/update timestamp 测试
    uint64_t record1TimeStamp;
    ASSERT_TRUE(recordManager_->GetFileTimestamp(fd1, &record1TimeStamp));
    ASSERT_EQ(record1TimeStamp, time1);
    ASSERT_TRUE(recordManager_->UpdateFileTimestamp(fd1, time2));
    ASSERT_TRUE(recordManager_->GetFileTimestamp(fd1, &record1TimeStamp));
    ASSERT_EQ(record1TimeStamp, time2);
    // 指定fd记录不存在
    ASSERT_FALSE(recordManager_->GetFileTimestamp(10, &record1TimeStamp));
    ASSERT_FALSE(recordManager_->UpdateFileTimestamp(10, time2));

    // get/update status 测试
    NebdFileStatus record1Status;
    ASSERT_TRUE(recordManager_->GetFileStatus(fd1, &record1Status));
    ASSERT_EQ(record1Status, NebdFileStatus::OPENED);
    ASSERT_TRUE(recordManager_->UpdateFileStatus(fd1, NebdFileStatus::CLOSED));
    ASSERT_TRUE(recordManager_->GetFileStatus(fd1, &record1Status));
    ASSERT_EQ(record1Status, NebdFileStatus::CLOSED);
    // 指定fd记录不存在
    ASSERT_FALSE(recordManager_->GetFileStatus(10, &record1Status));
    ASSERT_FALSE(recordManager_->UpdateFileStatus(10, NebdFileStatus::CLOSED));


    // 插入不同记录
    NebdFileRecord fileRecord2;
    std::string fileName2 = "file2";
    int fd2 = 3;
    fileRecord2.fileName = fileName2;
    fileRecord2.fd = fd2;
    EXPECT_CALL(*metaFileManager_, UpdateMetaFile(_))
    .WillOnce(Return(0));
    ASSERT_TRUE(recordManager_->UpdateRecord(fileRecord2));
    ASSERT_TRUE(recordManager_->Exist(fd1));
    ASSERT_TRUE(recordManager_->Exist(fd2));

    // 插入fd相同，名字不同的记录,返回失败
    NebdFileRecord fileRecord3;
    fileRecord3.fileName = "file3";
    fileRecord3.fd = fd2;
    EXPECT_CALL(*metaFileManager_, UpdateMetaFile(_))
    .Times(0);
    ASSERT_FALSE(recordManager_->UpdateRecord(fileRecord3));

    // update metafile 失败，返回失败
    fileRecord3.fileName = "file3";
    fileRecord3.fd = 4;
    EXPECT_CALL(*metaFileManager_, UpdateMetaFile(_))
    .WillOnce(Return(-1));
    ASSERT_FALSE(recordManager_->UpdateRecord(fileRecord3));

    // remove record
    // 请求删除的fd不存在，直接返回成功
    ASSERT_TRUE(recordManager_->RemoveRecord(10));
    // 请求删除的fd存在，更新metafile，并删除记录
    EXPECT_CALL(*metaFileManager_, UpdateMetaFile(_))
    .WillOnce(Return(0));
    ASSERT_TRUE(recordManager_->RemoveRecord(fd2));
    // 如果更新metafile失败，则不删除记录
    EXPECT_CALL(*metaFileManager_, UpdateMetaFile(_))
    .WillOnce(Return(-1));
    ASSERT_FALSE(recordManager_->RemoveRecord(fd1));

    // 校验最终结果
    FileRecordMap list = recordManager_->ListRecords();
    ASSERT_EQ(list.size(), 1);
    // 通过fd获取record并检查结果
    auto recordIter = list.begin();
    ASSERT_EQ(recordIter->first, fd1);
    ASSERT_EQ(recordIter->second.fd, fd1);
    ASSERT_EQ(recordIter->second.fileName, fileName1);

    // clear
    recordManager_->Clear();
    list = recordManager_->ListRecords();
    ASSERT_EQ(list.size(), 0);
}

TEST_F(FileRecordManagerTest, LoadTest) {
    FileRecordMap list = recordManager_->ListRecords();
    ASSERT_EQ(list.size(), 0);

    // list 失败
    EXPECT_CALL(*metaFileManager_, ListFileRecord(_))
    .WillOnce(Return(-1));
    ASSERT_EQ(-1, recordManager_->Load());

    // list 成功
    FileRecordMap recordsFromFile;
    NebdFileRecord fileRecord1;
    fileRecord1.fileName = "111";
    fileRecord1.fd = 1;
    recordsFromFile.emplace(1, fileRecord1);
    EXPECT_CALL(*metaFileManager_, ListFileRecord(_))
    .WillOnce(DoAll(SetArgPointee<0>(recordsFromFile),
                    Return(0)));
    ASSERT_EQ(0, recordManager_->Load());

    // 校验结果
    list = recordManager_->ListRecords();
    ASSERT_EQ(list.size(), 1);
    auto recordIter = list.begin();
    ASSERT_EQ(recordIter->first, 1);
    ASSERT_EQ(recordIter->second.fd, 1);
    ASSERT_EQ(recordIter->second.fileName, "111");
}

}  // namespace server
}  // namespace nebd

int main(int argc, char ** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    ::testing::InitGoogleMock(&argc, argv);
    return RUN_ALL_TESTS();
}
