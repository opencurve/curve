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

#include "src/part2/file_manager.h"
#include "tests/part2/mock_metafile_manager.h"
#include "tests/part2/mock_request_executor.h"

namespace nebd {
namespace server {

const char testFile1[] = "test:/cinder/111";
const char testFile2[] = "test:/cinder/222";
const char unknownFile[] = "un:/cinder/666";

using ::testing::_;
using ::testing::Return;
using ::testing::NotNull;
using ::testing::DoAll;
using ::testing::ReturnArg;
using ::testing::ElementsAre;
using ::testing::SetArgPointee;
using ::testing::SetArrayArgument;

class FileManagerTest : public ::testing::Test {
 public:
    void SetUp() {
        aioContext_ = new NebdServerAioContext();
        mockInstance_ = std::make_shared<NebdFileInstance>();
        metaFileManager_ = std::make_shared<MockMetaFileManager>();
        executor_ = std::make_shared<MockRequestExecutor>();
        g_test_executor = executor_.get();
        option_.heartbeatTimeoutS = 5;
        option_.checkTimeoutIntervalMs = 1000;
        option_.metaFileManager = metaFileManager_;
        ASSERT_EQ(fileManager_.Init(option_), 0);
    }
    void TearDown() {
        delete aioContext_;
    }

    // 构造初始环境，open两个文件
    void InitEnv() {
        EXPECT_CALL(*executor_, Open(testFile1))
        .WillOnce(Return(mockInstance_));
        EXPECT_CALL(*metaFileManager_, UpdateFileRecord(_))
        .WillOnce(Return(0));
        int fd = fileManager_.Open(testFile1);
        ASSERT_EQ(fd, 1);

        EXPECT_CALL(*executor_, Open(testFile2))
        .WillOnce(Return(mockInstance_));
        EXPECT_CALL(*metaFileManager_, UpdateFileRecord(_))
        .WillOnce(Return(0));
        fd = fileManager_.Open(testFile2);
        ASSERT_EQ(fd, 2);
    }

 protected:
    NebdFileManager fileManager_;
    std::shared_ptr<MockMetaFileManager> metaFileManager_;
    std::shared_ptr<MockRequestExecutor> executor_;
    std::shared_ptr<NebdFileInstance> mockInstance_;
    NebdServerAioContext* aioContext_;
    NebdFileManagerOption option_;
};

TEST_F(FileManagerTest, OpenTest) {
    // open一个不存在的文件
    EXPECT_CALL(*executor_, Open(testFile1))
    .WillOnce(Return(mockInstance_));
    EXPECT_CALL(*metaFileManager_, UpdateFileRecord(_))
    .WillOnce(Return(0));
    int fd = fileManager_.Open(testFile1);
    ASSERT_EQ(fd, 1);

    // 重复open
    EXPECT_CALL(*executor_, Open(_))
    .Times(0);
    EXPECT_CALL(*metaFileManager_, UpdateFileRecord(_))
    .Times(0);
    fd = fileManager_.Open(testFile1);
    ASSERT_EQ(fd, 1);

    // 再次open一个新的文件
    EXPECT_CALL(*executor_, Open(testFile2))
    .WillOnce(Return(mockInstance_));
    EXPECT_CALL(*metaFileManager_, UpdateFileRecord(_))
    .WillOnce(Return(0));
    fd = fileManager_.Open(testFile2);
    ASSERT_EQ(fd, 2);

    // open 已经close的文件
    std::unordered_map<int, NebdFileRecordPtr> recordMap
        = fileManager_.GetRecordMap();
    ASSERT_NE(recordMap[1], nullptr);
    recordMap[1]->status = NebdFileStatus::CLOSED;
    // 重新open该文件
    EXPECT_CALL(*executor_, Open(testFile1))
    .WillOnce(Return(mockInstance_));
    EXPECT_CALL(*metaFileManager_, UpdateFileRecord(_))
    .WillOnce(Return(0));
    fd = fileManager_.Open(testFile1);
    // 校验结果，生成了新的记录并删除了旧的记录
    ASSERT_EQ(fd, 3);
    recordMap = fileManager_.GetRecordMap();
    ASSERT_NE(recordMap[3], nullptr);
    ASSERT_EQ(recordMap[1], nullptr);
    ASSERT_EQ(3, recordMap[3]->fd);
    ASSERT_EQ(testFile1, recordMap[3]->fileName);
}

TEST_F(FileManagerTest, OpenFailTest) {
    // 调用后端open接口时出错
    EXPECT_CALL(*executor_, Open(testFile1))
    .WillOnce(Return(nullptr));
    EXPECT_CALL(*metaFileManager_, UpdateFileRecord(_))
    .Times(0);
    int fd = fileManager_.Open(testFile1);
    ASSERT_EQ(fd, -1);

    // 持久化元数据信息失败
    EXPECT_CALL(*executor_, Open(testFile1))
    .WillOnce(Return(mockInstance_));
    EXPECT_CALL(*metaFileManager_, UpdateFileRecord(_))
    .WillOnce(Return(-1));
    fd = fileManager_.Open(testFile1);
    ASSERT_EQ(fd, -1);

    // Open一个非法的filename
    EXPECT_CALL(*executor_, Open(_))
    .Times(0);
    EXPECT_CALL(*metaFileManager_, UpdateFileRecord(_))
    .Times(0);
    fd = fileManager_.Open(unknownFile);
    ASSERT_EQ(fd, -1);
}

TEST_F(FileManagerTest, LoadTest) {
    // 初始化从metafile返回的元数据
    std::vector<NebdFileRecordPtr> fileRecords;
    NebdFileRecordPtr record1 = std::make_shared<NebdFileRecord>();
    NebdFileRecordPtr record2 = std::make_shared<NebdFileRecord>();
    NebdFileRecordPtr record3 = std::make_shared<NebdFileRecord>();
    record1->fileName = testFile1;
    record1->fd = 1;
    record1->fileInstance = mockInstance_;
    record2->fileName = testFile2;
    record2->fd = 2;
    record2->fileInstance = mockInstance_;
    record3->fileName = unknownFile;
    record3->fd = 3;
    record3->fileInstance = mockInstance_;
    fileRecords.push_back(record1);
    fileRecords.push_back(record2);
    fileRecords.push_back(record3);

    EXPECT_CALL(*metaFileManager_, ListFileRecord(_))
    .WillOnce(DoAll(SetArgPointee<0>(fileRecords),
                    Return(0)));
    EXPECT_CALL(*executor_, Reopen(testFile1, _))
    .WillOnce(Return(mockInstance_));
    // reopen失败，不会导致load失败
    EXPECT_CALL(*executor_, Reopen(testFile2, _))
    .WillOnce(Return(nullptr));
    // 无法识别的文件不会被打开，但不会导致load失败
    EXPECT_CALL(*executor_, Reopen(unknownFile, _))
    .Times(0);
    ASSERT_EQ(0, fileManager_.Load());
}

TEST_F(FileManagerTest, LoadFailTest) {
    // 初始化
    std::vector<NebdFileRecordPtr> fileRecords;
    NebdFileRecordPtr record1 = std::make_shared<NebdFileRecord>();
    NebdFileRecordPtr record2 = std::make_shared<NebdFileRecord>();
    record1->fileName = testFile1;
    record1->fd = 1;
    record1->fileInstance = mockInstance_;
    // fd相同，文件名不同会出现冲突（这种情况理论也不会发生）
    record2->fileName = testFile2;
    record2->fd = 1;
    record2->fileInstance = mockInstance_;
    fileRecords.push_back(record1);
    fileRecords.push_back(record2);

    EXPECT_CALL(*metaFileManager_, ListFileRecord(_))
    .WillOnce(DoAll(SetArgPointee<0>(fileRecords),
                    Return(0)));
    EXPECT_CALL(*executor_, Reopen(testFile1, _))
    .WillOnce(Return(mockInstance_));
    // reopen失败，不会导致load失败
    EXPECT_CALL(*executor_, Reopen(testFile2, _))
    .WillOnce(Return(mockInstance_));
    ASSERT_EQ(-1, fileManager_.Load());
}

TEST_F(FileManagerTest, CloseTest) {
    InitEnv();
    // 指定的fd不存在,直接返回成功
    ASSERT_EQ(0, fileManager_.Close(3));

    // fd存在
    EXPECT_CALL(*executor_, Close(NotNull()))
    .WillOnce(Return(0));
    EXPECT_CALL(*metaFileManager_, RemoveFileRecord(testFile1))
    .WillOnce(Return(0));
    ASSERT_EQ(0, fileManager_.Close(1));

    EXPECT_CALL(*executor_, Close(NotNull()))
    .WillOnce(Return(0));
    EXPECT_CALL(*metaFileManager_, RemoveFileRecord(testFile2))
    .WillOnce(Return(0));
    ASSERT_EQ(0, fileManager_.Close(2));
}

TEST_F(FileManagerTest, CloseFailTest) {
    InitEnv();
    // 指定的fd不存在,直接返回成功
    ASSERT_EQ(0, fileManager_.Close(3));

    // 调用后端存储close时失败
    EXPECT_CALL(*executor_, Close(NotNull()))
    .WillOnce(Return(-1));
    EXPECT_CALL(*metaFileManager_, RemoveFileRecord(testFile1))
    .Times(0);
    ASSERT_EQ(-1, fileManager_.Close(1));

    // 从元数据文件中移除记录时失败
    EXPECT_CALL(*executor_, Close(NotNull()))
    .WillOnce(Return(0));
    EXPECT_CALL(*metaFileManager_, RemoveFileRecord(testFile1))
    .WillOnce(Return(-1));
    ASSERT_EQ(-1, fileManager_.Close(1));
}

TEST_F(FileManagerTest, ExtendTest) {
    InitEnv();

    // 文件是opened状态
    EXPECT_CALL(*executor_, Extend(NotNull(), 4096))
    .WillOnce(Return(0));
    ASSERT_EQ(0, fileManager_.Extend(1, 4096));

    // 文件状态为closed，会重新open文件
    std::unordered_map<int, NebdFileRecordPtr> recordMap
        = fileManager_.GetRecordMap();
    recordMap[1]->status = NebdFileStatus::CLOSED;
    // 重新open该文件，fd不变
    EXPECT_CALL(*executor_, Open(testFile1))
    .WillOnce(Return(mockInstance_));
    EXPECT_CALL(*metaFileManager_, UpdateFileRecord(_))
    .WillOnce(Return(0));
    EXPECT_CALL(*executor_, Extend(NotNull(), 4096))
    .WillOnce(Return(0));
    ASSERT_EQ(0, fileManager_.Extend(1, 4096));
    // 校验结果，fd不变
    recordMap = fileManager_.GetRecordMap();
    ASSERT_NE(recordMap[1], nullptr);
    ASSERT_EQ(testFile1, recordMap[1]->fileName);
}

TEST_F(FileManagerTest, ExtendFaileTest) {
    InitEnv();

    // 文件是opened状态
    EXPECT_CALL(*executor_, Extend(NotNull(), 4096))
    .WillOnce(Return(-1));
    ASSERT_EQ(-1, fileManager_.Extend(1, 4096));

    // 文件状态为closed，会重新open文件
    std::unordered_map<int, NebdFileRecordPtr> recordMap
        = fileManager_.GetRecordMap();
    recordMap[1]->status = NebdFileStatus::CLOSED;
    // 重新open文件时，open失败
    EXPECT_CALL(*executor_, Open(testFile1))
    .WillOnce(Return(nullptr));
    EXPECT_CALL(*metaFileManager_, UpdateFileRecord(_))
    .Times(0);
    EXPECT_CALL(*executor_, Extend(NotNull(), 4096))
    .Times(0);
    ASSERT_EQ(-1, fileManager_.Extend(1, 4096));
    // 重新open文件时，更新meta file失败
    EXPECT_CALL(*executor_, Open(testFile1))
    .WillOnce(Return(mockInstance_));
    EXPECT_CALL(*metaFileManager_, UpdateFileRecord(_))
    .WillOnce(Return(-1));
    EXPECT_CALL(*executor_, Extend(NotNull(), 4096))
    .Times(0);
    ASSERT_EQ(-1, fileManager_.Extend(1, 4096));
}

TEST_F(FileManagerTest, GetInfoTest) {
    InitEnv();

    NebdFileInfo fileInfo;
    // 文件是opened状态
    EXPECT_CALL(*executor_, GetInfo(NotNull(), &fileInfo))
    .WillOnce(Return(0));
    ASSERT_EQ(0, fileManager_.GetInfo(1, &fileInfo));

    // 文件状态为closed，会重新open文件
    std::unordered_map<int, NebdFileRecordPtr> recordMap
        = fileManager_.GetRecordMap();
    recordMap[1]->status = NebdFileStatus::CLOSED;
    // 重新open该文件，fd不变
    EXPECT_CALL(*executor_, Open(testFile1))
    .WillOnce(Return(mockInstance_));
    EXPECT_CALL(*metaFileManager_, UpdateFileRecord(_))
    .WillOnce(Return(0));
    EXPECT_CALL(*executor_, GetInfo(NotNull(), &fileInfo))
    .WillOnce(Return(0));
    ASSERT_EQ(0, fileManager_.GetInfo(1, &fileInfo));
    // 校验结果，fd不变
    recordMap = fileManager_.GetRecordMap();
    ASSERT_NE(recordMap[1], nullptr);
    ASSERT_EQ(testFile1, recordMap[1]->fileName);
}

TEST_F(FileManagerTest, GetInfoFaileTest) {
    InitEnv();

    NebdFileInfo fileInfo;
    // 文件是opened状态
    EXPECT_CALL(*executor_, GetInfo(NotNull(), &fileInfo))
    .WillOnce(Return(-1));
    ASSERT_EQ(-1, fileManager_.GetInfo(1, &fileInfo));

    // 文件状态为closed，会重新open文件
    std::unordered_map<int, NebdFileRecordPtr> recordMap
        = fileManager_.GetRecordMap();
    recordMap[1]->status = NebdFileStatus::CLOSED;
    // 重新open文件时，open失败
    EXPECT_CALL(*executor_, Open(testFile1))
    .WillOnce(Return(nullptr));
    EXPECT_CALL(*metaFileManager_, UpdateFileRecord(_))
    .Times(0);
    EXPECT_CALL(*executor_, GetInfo(NotNull(), _))
    .Times(0);
    ASSERT_EQ(-1, fileManager_.GetInfo(1, &fileInfo));
    // 重新open文件时，更新meta file失败
    EXPECT_CALL(*executor_, Open(testFile1))
    .WillOnce(Return(mockInstance_));
    EXPECT_CALL(*metaFileManager_, UpdateFileRecord(_))
    .WillOnce(Return(-1));
    EXPECT_CALL(*executor_, GetInfo(NotNull(), _))
    .Times(0);
    ASSERT_EQ(-1, fileManager_.GetInfo(1, &fileInfo));
}

TEST_F(FileManagerTest, InvalidCacheTest) {
    InitEnv();

    // 文件是opened状态
    EXPECT_CALL(*executor_, InvalidCache(NotNull()))
    .WillOnce(Return(0));
    ASSERT_EQ(0, fileManager_.InvalidCache(1));

    // 文件状态为closed，会重新open文件
    std::unordered_map<int, NebdFileRecordPtr> recordMap
        = fileManager_.GetRecordMap();
    recordMap[1]->status = NebdFileStatus::CLOSED;
    // 重新open该文件，fd不变
    EXPECT_CALL(*executor_, Open(testFile1))
    .WillOnce(Return(mockInstance_));
    EXPECT_CALL(*metaFileManager_, UpdateFileRecord(_))
    .WillOnce(Return(0));
    EXPECT_CALL(*executor_, InvalidCache(NotNull()))
    .WillOnce(Return(0));
    ASSERT_EQ(0, fileManager_.InvalidCache(1));
    // 校验结果，fd不变
    recordMap = fileManager_.GetRecordMap();
    ASSERT_NE(recordMap[1], nullptr);
    ASSERT_EQ(testFile1, recordMap[1]->fileName);
}

TEST_F(FileManagerTest, InvalidCacheFaileTest) {
    InitEnv();

    // 文件是opened状态
    EXPECT_CALL(*executor_, InvalidCache(NotNull()))
    .WillOnce(Return(-1));
    ASSERT_EQ(-1, fileManager_.InvalidCache(1));

    // 文件状态为closed，会重新open文件
    std::unordered_map<int, NebdFileRecordPtr> recordMap
        = fileManager_.GetRecordMap();
    recordMap[1]->status = NebdFileStatus::CLOSED;
    // 重新open文件时，open失败
    EXPECT_CALL(*executor_, Open(testFile1))
    .WillOnce(Return(nullptr));
    EXPECT_CALL(*metaFileManager_, UpdateFileRecord(_))
    .Times(0);
    EXPECT_CALL(*executor_, InvalidCache(NotNull()))
    .Times(0);
    ASSERT_EQ(-1, fileManager_.InvalidCache(1));
    // 重新open文件时，更新meta file失败
    EXPECT_CALL(*executor_, Open(testFile1))
    .WillOnce(Return(mockInstance_));
    EXPECT_CALL(*metaFileManager_, UpdateFileRecord(_))
    .WillOnce(Return(-1));
    EXPECT_CALL(*executor_, InvalidCache(NotNull()))
    .Times(0);
    ASSERT_EQ(-1, fileManager_.InvalidCache(1));
}

TEST_F(FileManagerTest, AioReadTest) {
    InitEnv();

    // 文件是opened状态
    EXPECT_CALL(*executor_, AioRead(NotNull(), aioContext_))
    .WillOnce(Return(0));
    ASSERT_EQ(0, fileManager_.AioRead(1, aioContext_));

    // 文件状态为closed，会重新open文件
    std::unordered_map<int, NebdFileRecordPtr> recordMap
        = fileManager_.GetRecordMap();
    recordMap[1]->status = NebdFileStatus::CLOSED;
    // 重新open该文件，fd不变
    EXPECT_CALL(*executor_, Open(testFile1))
    .WillOnce(Return(mockInstance_));
    EXPECT_CALL(*metaFileManager_, UpdateFileRecord(_))
    .WillOnce(Return(0));
    EXPECT_CALL(*executor_, AioRead(NotNull(), aioContext_))
    .WillOnce(Return(0));
    ASSERT_EQ(0, fileManager_.AioRead(1, aioContext_));
    // 校验结果，fd不变
    recordMap = fileManager_.GetRecordMap();
    ASSERT_NE(recordMap[1], nullptr);
    ASSERT_EQ(testFile1, recordMap[1]->fileName);
}

TEST_F(FileManagerTest, AioReadFaileTest) {
    InitEnv();

    // 文件是opened状态
    EXPECT_CALL(*executor_, AioRead(NotNull(), aioContext_))
    .WillOnce(Return(-1));
    ASSERT_EQ(-1, fileManager_.AioRead(1, aioContext_));

    // 文件状态为closed，会重新open文件
    std::unordered_map<int, NebdFileRecordPtr> recordMap
        = fileManager_.GetRecordMap();
    recordMap[1]->status = NebdFileStatus::CLOSED;
    // 重新open文件时，open失败
    EXPECT_CALL(*executor_, Open(testFile1))
    .WillOnce(Return(nullptr));
    EXPECT_CALL(*metaFileManager_, UpdateFileRecord(_))
    .Times(0);
    EXPECT_CALL(*executor_, AioRead(NotNull(), aioContext_))
    .Times(0);
    ASSERT_EQ(-1, fileManager_.AioRead(1, aioContext_));
    // 重新open文件时，更新meta file失败
    EXPECT_CALL(*executor_, Open(testFile1))
    .WillOnce(Return(mockInstance_));
    EXPECT_CALL(*metaFileManager_, UpdateFileRecord(_))
    .WillOnce(Return(-1));
    EXPECT_CALL(*executor_, AioRead(NotNull(), aioContext_))
    .Times(0);
    ASSERT_EQ(-1, fileManager_.AioRead(1, aioContext_));
}

TEST_F(FileManagerTest, AioWriteTest) {
    InitEnv();

    // 文件是opened状态
    EXPECT_CALL(*executor_, AioWrite(NotNull(), aioContext_))
    .WillOnce(Return(0));
    ASSERT_EQ(0, fileManager_.AioWrite(1, aioContext_));

    // 文件状态为closed，会重新open文件
    std::unordered_map<int, NebdFileRecordPtr> recordMap
        = fileManager_.GetRecordMap();
    recordMap[1]->status = NebdFileStatus::CLOSED;
    // 重新open该文件，fd不变
    EXPECT_CALL(*executor_, Open(testFile1))
    .WillOnce(Return(mockInstance_));
    EXPECT_CALL(*metaFileManager_, UpdateFileRecord(_))
    .WillOnce(Return(0));
    EXPECT_CALL(*executor_, AioWrite(NotNull(), aioContext_))
    .WillOnce(Return(0));
    ASSERT_EQ(0, fileManager_.AioWrite(1, aioContext_));
    // 校验结果，fd不变
    recordMap = fileManager_.GetRecordMap();
    ASSERT_NE(recordMap[1], nullptr);
    ASSERT_EQ(testFile1, recordMap[1]->fileName);
}

TEST_F(FileManagerTest, AioWriteFaileTest) {
    InitEnv();

    // 文件是opened状态
    EXPECT_CALL(*executor_, AioWrite(NotNull(), aioContext_))
    .WillOnce(Return(-1));
    ASSERT_EQ(-1, fileManager_.AioWrite(1, aioContext_));

    // 文件状态为closed，会重新open文件
    std::unordered_map<int, NebdFileRecordPtr> recordMap
        = fileManager_.GetRecordMap();
    recordMap[1]->status = NebdFileStatus::CLOSED;
    // 重新open文件时，open失败
    EXPECT_CALL(*executor_, Open(testFile1))
    .WillOnce(Return(nullptr));
    EXPECT_CALL(*metaFileManager_, UpdateFileRecord(_))
    .Times(0);
    EXPECT_CALL(*executor_, AioWrite(NotNull(), aioContext_))
    .Times(0);
    ASSERT_EQ(-1, fileManager_.AioWrite(1, aioContext_));
    // 重新open文件时，更新meta file失败
    EXPECT_CALL(*executor_, Open(testFile1))
    .WillOnce(Return(mockInstance_));
    EXPECT_CALL(*metaFileManager_, UpdateFileRecord(_))
    .WillOnce(Return(-1));
    EXPECT_CALL(*executor_, AioWrite(NotNull(), aioContext_))
    .Times(0);
    ASSERT_EQ(-1, fileManager_.AioWrite(1, aioContext_));
}

TEST_F(FileManagerTest, DiscardTest) {
    InitEnv();

    // 文件是opened状态
    EXPECT_CALL(*executor_, Discard(NotNull(), aioContext_))
    .WillOnce(Return(0));
    ASSERT_EQ(0, fileManager_.Discard(1, aioContext_));

    // 文件状态为closed，会重新open文件
    std::unordered_map<int, NebdFileRecordPtr> recordMap
        = fileManager_.GetRecordMap();
    recordMap[1]->status = NebdFileStatus::CLOSED;
    // 重新open该文件，fd不变
    EXPECT_CALL(*executor_, Open(testFile1))
    .WillOnce(Return(mockInstance_));
    EXPECT_CALL(*metaFileManager_, UpdateFileRecord(_))
    .WillOnce(Return(0));
    EXPECT_CALL(*executor_, Discard(NotNull(), aioContext_))
    .WillOnce(Return(0));
    ASSERT_EQ(0, fileManager_.Discard(1, aioContext_));
    // 校验结果，fd不变
    recordMap = fileManager_.GetRecordMap();
    ASSERT_NE(recordMap[1], nullptr);
    ASSERT_EQ(testFile1, recordMap[1]->fileName);
}

TEST_F(FileManagerTest, DiscardFaileTest) {
    InitEnv();

    // 文件是opened状态
    EXPECT_CALL(*executor_, Discard(NotNull(), aioContext_))
    .WillOnce(Return(-1));
    ASSERT_EQ(-1, fileManager_.Discard(1, aioContext_));

    // 文件状态为closed，会重新open文件
    std::unordered_map<int, NebdFileRecordPtr> recordMap
        = fileManager_.GetRecordMap();
    recordMap[1]->status = NebdFileStatus::CLOSED;
    // 重新open文件时，open失败
    EXPECT_CALL(*executor_, Open(testFile1))
    .WillOnce(Return(nullptr));
    EXPECT_CALL(*metaFileManager_, UpdateFileRecord(_))
    .Times(0);
    EXPECT_CALL(*executor_, Discard(NotNull(), aioContext_))
    .Times(0);
    ASSERT_EQ(-1, fileManager_.Discard(1, aioContext_));
    // 重新open文件时，更新meta file失败
    EXPECT_CALL(*executor_, Open(testFile1))
    .WillOnce(Return(mockInstance_));
    EXPECT_CALL(*metaFileManager_, UpdateFileRecord(_))
    .WillOnce(Return(-1));
    EXPECT_CALL(*executor_, Discard(NotNull(), aioContext_))
    .Times(0);
    ASSERT_EQ(-1, fileManager_.Discard(1, aioContext_));
}

TEST_F(FileManagerTest, FlushTest) {
    InitEnv();

    // 文件是opened状态
    EXPECT_CALL(*executor_, Flush(NotNull(), aioContext_))
    .WillOnce(Return(0));
    ASSERT_EQ(0, fileManager_.Flush(1, aioContext_));

    // 文件状态为closed，会重新open文件
    std::unordered_map<int, NebdFileRecordPtr> recordMap
        = fileManager_.GetRecordMap();
    recordMap[1]->status = NebdFileStatus::CLOSED;
    // 重新open该文件，fd不变
    EXPECT_CALL(*executor_, Open(testFile1))
    .WillOnce(Return(mockInstance_));
    EXPECT_CALL(*metaFileManager_, UpdateFileRecord(_))
    .WillOnce(Return(0));
    EXPECT_CALL(*executor_, Flush(NotNull(), aioContext_))
    .WillOnce(Return(0));
    ASSERT_EQ(0, fileManager_.Flush(1, aioContext_));
    // 校验结果，fd不变
    recordMap = fileManager_.GetRecordMap();
    ASSERT_NE(recordMap[1], nullptr);
    ASSERT_EQ(testFile1, recordMap[1]->fileName);
}

TEST_F(FileManagerTest, FlushFaileTest) {
    InitEnv();

    // 文件是opened状态
    EXPECT_CALL(*executor_, Flush(NotNull(), aioContext_))
    .WillOnce(Return(-1));
    ASSERT_EQ(-1, fileManager_.Flush(1, aioContext_));

    // 文件状态为closed，会重新open文件
    std::unordered_map<int, NebdFileRecordPtr> recordMap
        = fileManager_.GetRecordMap();
    recordMap[1]->status = NebdFileStatus::CLOSED;
    // 重新open文件时，open失败
    EXPECT_CALL(*executor_, Open(testFile1))
    .WillOnce(Return(nullptr));
    EXPECT_CALL(*metaFileManager_, UpdateFileRecord(_))
    .Times(0);
    EXPECT_CALL(*executor_, Flush(NotNull(), aioContext_))
    .Times(0);
    ASSERT_EQ(-1, fileManager_.Flush(1, aioContext_));
    // 重新open文件时，更新meta file失败
    EXPECT_CALL(*executor_, Open(testFile1))
    .WillOnce(Return(mockInstance_));
    EXPECT_CALL(*metaFileManager_, UpdateFileRecord(_))
    .WillOnce(Return(-1));
    EXPECT_CALL(*executor_, Flush(NotNull(), aioContext_))
    .Times(0);
    ASSERT_EQ(-1, fileManager_.Flush(1, aioContext_));
}

TEST_F(FileManagerTest, CheckTimeoutTest) {
    ASSERT_EQ(fileManager_.Run(), 0);
    // 已经在run了不允许重复Run
    ASSERT_EQ(fileManager_.Run(), -1);

    // 校验是否在检查超时
    InitEnv();
    std::unordered_map<int, NebdFileRecordPtr> recordMap
        = fileManager_.GetRecordMap();
    ASSERT_EQ(recordMap[1]->status, NebdFileStatus::OPENED);
    ASSERT_EQ(recordMap[2]->status, NebdFileStatus::OPENED);
    // 等待一段时间（未超过超时时间），模拟一个文件收到心跳，另一个没收到
    ::sleep(option_.heartbeatTimeoutS - 2);
    ASSERT_EQ(0, fileManager_.UpdateFileTimestamp(1));
    // 在等待一段时间，其中一个文件超时，另一个文件未超时
    EXPECT_CALL(*executor_, Close(NotNull()))
    .WillOnce(Return(0));
    ::sleep(option_.checkTimeoutIntervalMs / 1000 + 2);
    recordMap = fileManager_.GetRecordMap();
    ASSERT_EQ(recordMap[1]->status, NebdFileStatus::OPENED);
    ASSERT_EQ(recordMap[2]->status, NebdFileStatus::CLOSED);
    // 继续等待一段时间，两个文件都超时
    EXPECT_CALL(*executor_, Close(NotNull()))
    .WillOnce(Return(0));
    ::sleep(option_.heartbeatTimeoutS - 2);
    ASSERT_EQ(recordMap[1]->status, NebdFileStatus::CLOSED);
    ASSERT_EQ(recordMap[2]->status, NebdFileStatus::CLOSED);

    ASSERT_EQ(fileManager_.Fini(), 0);
}

}  // namespace server
}  // namespace nebd

int main(int argc, char ** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    ::testing::InitGoogleMock(&argc, argv);
    return RUN_ALL_TESTS();
}
