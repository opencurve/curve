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
#include "tests/part2/mock_filerecord_manager.h"
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
        executor_ = std::make_shared<MockRequestExecutor>();
        g_test_executor = executor_.get();
        filerecordManager_ = std::make_shared<MockFileRecordManager>();
        fileManager_ = std::make_shared<NebdFileManager>(filerecordManager_);
        ON_CALL(*filerecordManager_, Exist(_))
        .WillByDefault(Return(false));
    }
    void TearDown() {
        delete aioContext_;
    }

 protected:
    std::shared_ptr<NebdFileManager> fileManager_;
    std::shared_ptr<MockFileRecordManager> filerecordManager_;
    std::shared_ptr<MockRequestExecutor> executor_;
    std::shared_ptr<NebdFileInstance> mockInstance_;
    NebdServerAioContext* aioContext_;
};

TEST_F(FileManagerTest, OpenTest) {
    // open一个不存在的文件
    EXPECT_CALL(*filerecordManager_, GetRecord(testFile1, _))
    .WillOnce(Return(false));
    EXPECT_CALL(*executor_, Open(testFile1))
    .WillOnce(Return(mockInstance_));
    EXPECT_CALL(*filerecordManager_, UpdateRecord(_))
    .WillOnce(Return(true));
    int fd = fileManager_->Open(testFile1);
    ASSERT_EQ(fd, 1);

    // 重复open
    NebdFileRecord fileRecord;
    fileRecord.status = NebdFileStatus::OPENED;
    fileRecord.fd = 1;
    EXPECT_CALL(*filerecordManager_, GetRecord(testFile1, _))
    .WillOnce(DoAll(SetArgPointee<1>(fileRecord),
                    Return(true)));
    EXPECT_CALL(*executor_, Open(_))
    .Times(0);
    fd = fileManager_->Open(testFile1);
    ASSERT_EQ(fd, 1);

    // open 已经close的文件
    fileRecord.status = NebdFileStatus::CLOSED;
    fileRecord.fd = 1;
    EXPECT_CALL(*filerecordManager_, GetRecord(testFile1, _))
    .WillOnce(DoAll(SetArgPointee<1>(fileRecord),
                    Return(true)));
    EXPECT_CALL(*executor_, Open(testFile1))
    .WillOnce(Return(mockInstance_));
    EXPECT_CALL(*filerecordManager_, UpdateRecord(_))
    .WillOnce(Return(true));
    fd = fileManager_->Open(testFile1);
    ASSERT_EQ(fd, 2);
}

TEST_F(FileManagerTest, OpenFailTest) {
    // 调用后端open接口时出错
    EXPECT_CALL(*filerecordManager_, GetRecord(testFile1, _))
    .WillOnce(Return(false));
    EXPECT_CALL(*executor_, Open(testFile1))
    .WillOnce(Return(nullptr));
    EXPECT_CALL(*filerecordManager_, UpdateRecord(_))
    .Times(0);
    int fd = fileManager_->Open(testFile1);
    ASSERT_EQ(fd, -1);

    // 持久化元数据信息失败
    EXPECT_CALL(*filerecordManager_, GetRecord(testFile1, _))
    .WillOnce(Return(false));
    EXPECT_CALL(*executor_, Open(testFile1))
    .WillOnce(Return(mockInstance_));
    EXPECT_CALL(*filerecordManager_, UpdateRecord(_))
    .WillOnce(Return(false));
    EXPECT_CALL(*executor_, Close(mockInstance_.get()))
    .WillOnce(Return(0));
    fd = fileManager_->Open(testFile1);
    ASSERT_EQ(fd, -1);

    // Open一个非法的filename
    EXPECT_CALL(*filerecordManager_, GetRecord(unknownFile, _))
    .WillOnce(Return(false));
    EXPECT_CALL(*executor_, Open(_))
    .Times(0);
    EXPECT_CALL(*filerecordManager_, UpdateRecord(_))
    .Times(0);
    fd = fileManager_->Open(unknownFile);
    ASSERT_EQ(fd, -1);
}

TEST_F(FileManagerTest, LoadTest) {
    // 初始化从metafile返回的元数据
    FileRecordMap fileRecords;
    NebdFileRecord record1;
    record1.fileName = testFile1;
    record1.fd = 1;
    record1.fileInstance = mockInstance_;
    fileRecords.emplace(1, record1);

    EXPECT_CALL(*filerecordManager_, Load())
    .WillOnce(Return(0));
    EXPECT_CALL(*filerecordManager_, ListRecords())
    .WillOnce(Return(fileRecords));
    EXPECT_CALL(*executor_, Reopen(testFile1, _))
    .WillOnce(Return(mockInstance_));
    EXPECT_CALL(*filerecordManager_, UpdateRecord(_))
    .WillOnce(Return(true));
    ASSERT_EQ(0, fileManager_->Load());
}

TEST_F(FileManagerTest, LoadFailTest) {
    // 初始化从metafile返回的元数据
    FileRecordMap fileRecords;
    NebdFileRecord record1;
    NebdFileRecord record2;
    NebdFileRecord record3;
    record1.fileName = testFile1;
    record1.fd = 1;
    record1.fileInstance = mockInstance_;
    record2.fileName = testFile2;
    record2.fd = 2;
    record2.fileInstance = mockInstance_;
    record3.fileName = unknownFile;
    record3.fd = 3;
    record3.fileInstance = mockInstance_;
    fileRecords.emplace(1, record1);
    fileRecords.emplace(2, record2);
    fileRecords.emplace(3, record3);

    // load 失败
    EXPECT_CALL(*filerecordManager_, Load())
    .WillOnce(Return(-1));
    ASSERT_EQ(-1, fileManager_->Load());

    // 其他失败都会被忽略，不会导致load失败
    EXPECT_CALL(*filerecordManager_, Load())
    .WillOnce(Return(0));
    EXPECT_CALL(*filerecordManager_, ListRecords())
    .WillOnce(Return(fileRecords));
    // uodate record 失败，不会导致load失败
    EXPECT_CALL(*executor_, Reopen(testFile1, _))
    .WillOnce(Return(mockInstance_));
    EXPECT_CALL(*filerecordManager_, UpdateRecord(_))
    .WillOnce(Return(false));
    // reopen失败，不会导致load失败
    EXPECT_CALL(*executor_, Reopen(testFile2, _))
    .WillOnce(Return(nullptr));
    // 无法识别的文件不会被打开，但不会导致load失败
    EXPECT_CALL(*executor_, Reopen(unknownFile, _))
    .Times(0);
    ASSERT_EQ(0, fileManager_->Load());
}

TEST_F(FileManagerTest, CloseTest) {
    // 指定的fd不存在,直接返回成功
    EXPECT_CALL(*filerecordManager_, GetRecord(3, _))
    .WillOnce(Return(false));
    ASSERT_EQ(0, fileManager_->Close(3, true));

    // fd存在,文件状态为opened,removerecord为true
    NebdFileRecord fileRecord;
    fileRecord.fd = 1;
    fileRecord.status = NebdFileStatus::OPENED;
    fileRecord.executor = executor_.get();
    fileRecord.fileInstance = mockInstance_;
    EXPECT_CALL(*filerecordManager_, GetRecord(1, _))
    .Times(2)
    .WillRepeatedly(DoAll(SetArgPointee<1>(fileRecord),
                          Return(true)));
    EXPECT_CALL(*executor_, Close(NotNull()))
    .WillOnce(Return(0));
    EXPECT_CALL(*filerecordManager_, UpdateFileStatus(1, _))
    .WillOnce(Return(true));
    EXPECT_CALL(*filerecordManager_, RemoveRecord(1))
    .WillOnce(Return(true));
    ASSERT_EQ(0, fileManager_->Close(1, true));

    // fd存在,文件状态为opened,removerecord为false
    fileRecord.fd = 1;
    fileRecord.status = NebdFileStatus::OPENED;
    EXPECT_CALL(*filerecordManager_, GetRecord(1, _))
    .Times(2)
    .WillRepeatedly(DoAll(SetArgPointee<1>(fileRecord),
                          Return(true)));
    EXPECT_CALL(*executor_, Close(NotNull()))
    .WillOnce(Return(0));
    EXPECT_CALL(*filerecordManager_, UpdateFileStatus(1, _))
    .WillOnce(Return(true));
    EXPECT_CALL(*filerecordManager_, RemoveRecord(1))
    .Times(0);
    ASSERT_EQ(0, fileManager_->Close(1, false));

    // 文件状态为closed
    fileRecord.fd = 1;
    fileRecord.status = NebdFileStatus::CLOSED;
    EXPECT_CALL(*filerecordManager_, GetRecord(1, _))
    .Times(2)
    .WillRepeatedly(DoAll(SetArgPointee<1>(fileRecord),
                          Return(true)));
    EXPECT_CALL(*executor_, Close(NotNull()))
    .Times(0);
    EXPECT_CALL(*filerecordManager_, UpdateFileStatus(_, _))
    .Times(0);
    EXPECT_CALL(*filerecordManager_, RemoveRecord(1))
    .WillOnce(Return(true));
    ASSERT_EQ(0, fileManager_->Close(1, true));
}

TEST_F(FileManagerTest, CloseFailTest) {
    NebdFileRecord fileRecord;
    fileRecord.fd = 1;
    fileRecord.status = NebdFileStatus::OPENED;
    fileRecord.executor = executor_.get();
    fileRecord.fileInstance = mockInstance_;
    EXPECT_CALL(*filerecordManager_, GetRecord(1, _))
    .WillRepeatedly(DoAll(SetArgPointee<1>(fileRecord),
                          Return(true)));

    // executor close 失败
    EXPECT_CALL(*executor_, Close(NotNull()))
    .WillOnce(Return(-1));
    ASSERT_EQ(-1, fileManager_->Close(1, true));

    // remove record 失败
    EXPECT_CALL(*executor_, Close(NotNull()))
    .WillOnce(Return(0));
    EXPECT_CALL(*filerecordManager_, UpdateFileStatus(1, _))
    .WillOnce(Return(true));
    EXPECT_CALL(*filerecordManager_, RemoveRecord(1))
    .WillOnce(Return(false));
    ASSERT_EQ(-1, fileManager_->Close(1, true));
}

TEST_F(FileManagerTest, ExtendTest) {
    // 文件是opened状态
    NebdFileRecord fileRecord;
    fileRecord.fd = 1;
    fileRecord.fileName = testFile1;
    fileRecord.status = NebdFileStatus::OPENED;
    fileRecord.executor = executor_.get();
    fileRecord.fileInstance = mockInstance_;
    EXPECT_CALL(*filerecordManager_, GetRecord(1, _))
    .WillOnce(DoAll(SetArgPointee<1>(fileRecord),
                    Return(true)));
    EXPECT_CALL(*executor_, Extend(NotNull(), 4096))
    .WillOnce(Return(0));
    ASSERT_EQ(0, fileManager_->Extend(1, 4096));

    // 文件状态为closed，会重新open文件
    fileRecord.fd = 1;
    fileRecord.fileName = testFile1;
    fileRecord.status = NebdFileStatus::CLOSED;
    EXPECT_CALL(*filerecordManager_, GetRecord(1, _))
    .WillRepeatedly(DoAll(SetArgPointee<1>(fileRecord),
                          Return(true)));
    EXPECT_CALL(*filerecordManager_, GetRecord(testFile1, _))
    .WillRepeatedly(DoAll(SetArgPointee<1>(fileRecord),
                          Return(true)));
    // 重新open该文件，fd不变
    EXPECT_CALL(*executor_, Open(testFile1))
    .WillOnce(Return(mockInstance_));
    EXPECT_CALL(*filerecordManager_, UpdateRecord(_))
    .WillOnce(Return(true));
    EXPECT_CALL(*executor_, Extend(NotNull(), 4096))
    .WillOnce(Return(0));
    ASSERT_EQ(0, fileManager_->Extend(1, 4096));
}

TEST_F(FileManagerTest, ExtendFaileTest) {
    // 文件是opened状态
    NebdFileRecord fileRecord;
    fileRecord.fd = 1;
    fileRecord.fileName = testFile1;
    fileRecord.status = NebdFileStatus::OPENED;
    fileRecord.executor = executor_.get();
    fileRecord.fileInstance = mockInstance_;
    EXPECT_CALL(*filerecordManager_, GetRecord(1, _))
    .WillOnce(DoAll(SetArgPointee<1>(fileRecord),
                    Return(true)));
    EXPECT_CALL(*executor_, Extend(NotNull(), 4096))
    .WillOnce(Return(-1));
    ASSERT_EQ(-1, fileManager_->Extend(1, 4096));

    // 文件状态为closed，会重新open文件
    fileRecord.fd = 1;
    fileRecord.fileName = testFile1;
    fileRecord.status = NebdFileStatus::CLOSED;
    EXPECT_CALL(*filerecordManager_, GetRecord(1, _))
    .WillRepeatedly(DoAll(SetArgPointee<1>(fileRecord),
                          Return(true)));
    EXPECT_CALL(*filerecordManager_, GetRecord(testFile1, _))
    .WillRepeatedly(DoAll(SetArgPointee<1>(fileRecord),
                          Return(true)));
    // 重新open文件时，open失败
    EXPECT_CALL(*executor_, Open(testFile1))
    .WillOnce(Return(nullptr));
    EXPECT_CALL(*filerecordManager_, UpdateRecord(_))
    .Times(0);
    EXPECT_CALL(*executor_, Extend(NotNull(), 4096))
    .Times(0);
    ASSERT_EQ(-1, fileManager_->Extend(1, 4096));
    // 重新open文件时，更新meta file失败
    EXPECT_CALL(*executor_, Open(testFile1))
    .WillOnce(Return(mockInstance_));
    EXPECT_CALL(*filerecordManager_, UpdateRecord(_))
    .WillOnce(Return(false));
    EXPECT_CALL(*executor_, Extend(NotNull(), 4096))
    .Times(0);
    ASSERT_EQ(-1, fileManager_->Extend(1, 4096));
}

TEST_F(FileManagerTest, GetInfoTest) {
    NebdFileInfo fileInfo;
    // 文件是opened状态
    NebdFileRecord fileRecord;
    fileRecord.fd = 1;
    fileRecord.fileName = testFile1;
    fileRecord.status = NebdFileStatus::OPENED;
    fileRecord.executor = executor_.get();
    fileRecord.fileInstance = mockInstance_;
    EXPECT_CALL(*filerecordManager_, GetRecord(1, _))
    .WillOnce(DoAll(SetArgPointee<1>(fileRecord),
                    Return(true)));
    EXPECT_CALL(*executor_, GetInfo(NotNull(), &fileInfo))
    .WillOnce(Return(0));
    ASSERT_EQ(0, fileManager_->GetInfo(1, &fileInfo));

    // 文件状态为closed，会重新open文件
    fileRecord.fd = 1;
    fileRecord.fileName = testFile1;
    fileRecord.status = NebdFileStatus::CLOSED;
    EXPECT_CALL(*filerecordManager_, GetRecord(1, _))
    .WillRepeatedly(DoAll(SetArgPointee<1>(fileRecord),
                          Return(true)));
    EXPECT_CALL(*filerecordManager_, GetRecord(testFile1, _))
    .WillRepeatedly(DoAll(SetArgPointee<1>(fileRecord),
                          Return(true)));
    // 重新open该文件，fd不变
    EXPECT_CALL(*executor_, Open(testFile1))
    .WillOnce(Return(mockInstance_));
    EXPECT_CALL(*filerecordManager_, UpdateRecord(_))
    .WillOnce(Return(true));
    EXPECT_CALL(*executor_, GetInfo(NotNull(), &fileInfo))
    .WillOnce(Return(0));
    ASSERT_EQ(0, fileManager_->GetInfo(1, &fileInfo));
}

TEST_F(FileManagerTest, GetInfoFaileTest) {
    NebdFileInfo fileInfo;
    // 文件是opened状态
    NebdFileRecord fileRecord;
    fileRecord.fd = 1;
    fileRecord.fileName = testFile1;
    fileRecord.status = NebdFileStatus::OPENED;
    fileRecord.executor = executor_.get();
    fileRecord.fileInstance = mockInstance_;
    EXPECT_CALL(*filerecordManager_, GetRecord(1, _))
    .WillOnce(DoAll(SetArgPointee<1>(fileRecord),
                    Return(true)));
    EXPECT_CALL(*executor_, GetInfo(NotNull(), &fileInfo))
    .WillOnce(Return(-1));
    ASSERT_EQ(-1, fileManager_->GetInfo(1, &fileInfo));

    // 文件状态为closed，会重新open文件
    fileRecord.fd = 1;
    fileRecord.fileName = testFile1;
    fileRecord.status = NebdFileStatus::CLOSED;
    EXPECT_CALL(*filerecordManager_, GetRecord(1, _))
    .WillRepeatedly(DoAll(SetArgPointee<1>(fileRecord),
                          Return(true)));
    EXPECT_CALL(*filerecordManager_, GetRecord(testFile1, _))
    .WillRepeatedly(DoAll(SetArgPointee<1>(fileRecord),
                          Return(true)));
    // 重新open文件时，open失败
    EXPECT_CALL(*executor_, Open(testFile1))
    .WillOnce(Return(nullptr));
    EXPECT_CALL(*filerecordManager_, UpdateRecord(_))
    .Times(0);
    EXPECT_CALL(*executor_, GetInfo(NotNull(), _))
    .Times(0);
    ASSERT_EQ(-1, fileManager_->GetInfo(1, &fileInfo));
    // 重新open文件时，更新meta file失败
    EXPECT_CALL(*executor_, Open(testFile1))
    .WillOnce(Return(mockInstance_));
    EXPECT_CALL(*filerecordManager_, UpdateRecord(_))
    .WillOnce(Return(false));
    EXPECT_CALL(*executor_, GetInfo(NotNull(), _))
    .Times(0);
    ASSERT_EQ(-1, fileManager_->GetInfo(1, &fileInfo));
}

TEST_F(FileManagerTest, InvalidCacheTest) {
    // 文件是opened状态
    NebdFileRecord fileRecord;
    fileRecord.fd = 1;
    fileRecord.fileName = testFile1;
    fileRecord.status = NebdFileStatus::OPENED;
    fileRecord.executor = executor_.get();
    fileRecord.fileInstance = mockInstance_;
    EXPECT_CALL(*filerecordManager_, GetRecord(1, _))
    .WillOnce(DoAll(SetArgPointee<1>(fileRecord),
                    Return(true)));
    EXPECT_CALL(*executor_, InvalidCache(NotNull()))
    .WillOnce(Return(0));
    ASSERT_EQ(0, fileManager_->InvalidCache(1));

    // 文件状态为closed，会重新open文件
    fileRecord.fd = 1;
    fileRecord.fileName = testFile1;
    fileRecord.status = NebdFileStatus::CLOSED;
    EXPECT_CALL(*filerecordManager_, GetRecord(1, _))
    .WillRepeatedly(DoAll(SetArgPointee<1>(fileRecord),
                          Return(true)));
    EXPECT_CALL(*filerecordManager_, GetRecord(testFile1, _))
    .WillRepeatedly(DoAll(SetArgPointee<1>(fileRecord),
                          Return(true)));
    // 重新open该文件，fd不变
    EXPECT_CALL(*executor_, Open(testFile1))
    .WillOnce(Return(mockInstance_));
    EXPECT_CALL(*filerecordManager_, UpdateRecord(_))
    .WillOnce(Return(true));
    EXPECT_CALL(*executor_, InvalidCache(NotNull()))
    .WillOnce(Return(0));
    ASSERT_EQ(0, fileManager_->InvalidCache(1));
}

TEST_F(FileManagerTest, InvalidCacheFaileTest) {
    // 文件是opened状态
    NebdFileRecord fileRecord;
    fileRecord.fd = 1;
    fileRecord.fileName = testFile1;
    fileRecord.status = NebdFileStatus::OPENED;
    fileRecord.executor = executor_.get();
    fileRecord.fileInstance = mockInstance_;
    EXPECT_CALL(*filerecordManager_, GetRecord(1, _))
    .WillOnce(DoAll(SetArgPointee<1>(fileRecord),
                    Return(true)));
    EXPECT_CALL(*executor_, InvalidCache(NotNull()))
    .WillOnce(Return(-1));
    ASSERT_EQ(-1, fileManager_->InvalidCache(1));

    // 文件状态为closed，会重新open文件
    fileRecord.fd = 1;
    fileRecord.fileName = testFile1;
    fileRecord.status = NebdFileStatus::CLOSED;
    EXPECT_CALL(*filerecordManager_, GetRecord(1, _))
    .WillRepeatedly(DoAll(SetArgPointee<1>(fileRecord),
                          Return(true)));
    EXPECT_CALL(*filerecordManager_, GetRecord(testFile1, _))
    .WillRepeatedly(DoAll(SetArgPointee<1>(fileRecord),
                          Return(true)));
    // 重新open文件时，open失败
    EXPECT_CALL(*executor_, Open(testFile1))
    .WillOnce(Return(nullptr));
    EXPECT_CALL(*filerecordManager_, UpdateRecord(_))
    .Times(0);
    EXPECT_CALL(*executor_, InvalidCache(NotNull()))
    .Times(0);
    ASSERT_EQ(-1, fileManager_->InvalidCache(1));
    // 重新open文件时，更新meta file失败
    EXPECT_CALL(*executor_, Open(testFile1))
    .WillOnce(Return(mockInstance_));
    EXPECT_CALL(*filerecordManager_, UpdateRecord(_))
    .WillOnce(Return(false));
    EXPECT_CALL(*executor_, InvalidCache(NotNull()))
    .Times(0);
    ASSERT_EQ(-1, fileManager_->InvalidCache(1));
}

TEST_F(FileManagerTest, AioReadTest) {
    // 文件是opened状态
    NebdFileRecord fileRecord;
    fileRecord.fd = 1;
    fileRecord.fileName = testFile1;
    fileRecord.status = NebdFileStatus::OPENED;
    fileRecord.executor = executor_.get();
    fileRecord.fileInstance = mockInstance_;
    EXPECT_CALL(*filerecordManager_, GetRecord(1, _))
    .WillOnce(DoAll(SetArgPointee<1>(fileRecord),
                    Return(true)));
    EXPECT_CALL(*executor_, AioRead(NotNull(), aioContext_))
    .WillOnce(Return(0));
    ASSERT_EQ(0, fileManager_->AioRead(1, aioContext_));

    // 文件状态为closed，会重新open文件
    fileRecord.fd = 1;
    fileRecord.fileName = testFile1;
    fileRecord.status = NebdFileStatus::CLOSED;
    EXPECT_CALL(*filerecordManager_, GetRecord(1, _))
    .WillRepeatedly(DoAll(SetArgPointee<1>(fileRecord),
                          Return(true)));
    EXPECT_CALL(*filerecordManager_, GetRecord(testFile1, _))
    .WillRepeatedly(DoAll(SetArgPointee<1>(fileRecord),
                          Return(true)));
    // 重新open该文件，fd不变
    EXPECT_CALL(*executor_, Open(testFile1))
    .WillOnce(Return(mockInstance_));
    EXPECT_CALL(*filerecordManager_, UpdateRecord(_))
    .WillOnce(Return(true));
    EXPECT_CALL(*executor_, AioRead(NotNull(), aioContext_))
    .WillOnce(Return(0));
    ASSERT_EQ(0, fileManager_->AioRead(1, aioContext_));
}

TEST_F(FileManagerTest, AioReadFaileTest) {
    // 文件不存在
    EXPECT_CALL(*filerecordManager_, GetRecord(10, _))
    .WillOnce(Return(false));
    EXPECT_CALL(*executor_, AioRead(_, _))
    .Times(0);
    ASSERT_EQ(-1, fileManager_->AioRead(10, aioContext_));

    // 文件是opened状态
    NebdFileRecord fileRecord;
    fileRecord.fd = 1;
    fileRecord.fileName = testFile1;
    fileRecord.status = NebdFileStatus::OPENED;
    fileRecord.executor = executor_.get();
    fileRecord.fileInstance = mockInstance_;
    EXPECT_CALL(*filerecordManager_, GetRecord(1, _))
    .WillOnce(DoAll(SetArgPointee<1>(fileRecord),
                    Return(true)));
    EXPECT_CALL(*executor_, AioRead(NotNull(), aioContext_))
    .WillOnce(Return(-1));
    ASSERT_EQ(-1, fileManager_->AioRead(1, aioContext_));

    // 文件状态为closed，会重新open文件
    fileRecord.fd = 1;
    fileRecord.fileName = testFile1;
    fileRecord.status = NebdFileStatus::CLOSED;
    EXPECT_CALL(*filerecordManager_, GetRecord(1, _))
    .WillRepeatedly(DoAll(SetArgPointee<1>(fileRecord),
                          Return(true)));
    EXPECT_CALL(*filerecordManager_, GetRecord(testFile1, _))
    .WillRepeatedly(DoAll(SetArgPointee<1>(fileRecord),
                          Return(true)));
    // 重新open文件时，open失败
    EXPECT_CALL(*executor_, Open(testFile1))
    .WillOnce(Return(nullptr));
    EXPECT_CALL(*filerecordManager_, UpdateRecord(_))
    .Times(0);
    EXPECT_CALL(*executor_, AioRead(NotNull(), aioContext_))
    .Times(0);
    ASSERT_EQ(-1, fileManager_->AioRead(1, aioContext_));
    // 重新open文件时，更新meta file失败
    EXPECT_CALL(*executor_, Open(testFile1))
    .WillOnce(Return(mockInstance_));
    EXPECT_CALL(*filerecordManager_, UpdateRecord(_))
    .WillOnce(Return(false));
    EXPECT_CALL(*executor_, AioRead(NotNull(), aioContext_))
    .Times(0);
    ASSERT_EQ(-1, fileManager_->AioRead(1, aioContext_));
}

TEST_F(FileManagerTest, AioWriteTest) {
    // 文件是opened状态
    NebdFileRecord fileRecord;
    fileRecord.fd = 1;
    fileRecord.fileName = testFile1;
    fileRecord.status = NebdFileStatus::OPENED;
    fileRecord.executor = executor_.get();
    fileRecord.fileInstance = mockInstance_;
    EXPECT_CALL(*filerecordManager_, GetRecord(1, _))
    .WillOnce(DoAll(SetArgPointee<1>(fileRecord),
                    Return(true)));
    EXPECT_CALL(*executor_, AioWrite(NotNull(), aioContext_))
    .WillOnce(Return(0));
    ASSERT_EQ(0, fileManager_->AioWrite(1, aioContext_));

    // 文件状态为closed，会重新open文件
    fileRecord.fd = 1;
    fileRecord.fileName = testFile1;
    fileRecord.status = NebdFileStatus::CLOSED;
    EXPECT_CALL(*filerecordManager_, GetRecord(1, _))
    .WillRepeatedly(DoAll(SetArgPointee<1>(fileRecord),
                          Return(true)));
    EXPECT_CALL(*filerecordManager_, GetRecord(testFile1, _))
    .WillRepeatedly(DoAll(SetArgPointee<1>(fileRecord),
                          Return(true)));
    // 重新open该文件，fd不变
    EXPECT_CALL(*executor_, Open(testFile1))
    .WillOnce(Return(mockInstance_));
    EXPECT_CALL(*filerecordManager_, UpdateRecord(_))
    .WillOnce(Return(true));
    EXPECT_CALL(*executor_, AioWrite(NotNull(), aioContext_))
    .WillOnce(Return(0));
    ASSERT_EQ(0, fileManager_->AioWrite(1, aioContext_));
}

TEST_F(FileManagerTest, AioWriteFaileTest) {
    // 文件不存在
    EXPECT_CALL(*filerecordManager_, GetRecord(10, _))
    .WillOnce(Return(false));
    EXPECT_CALL(*executor_, AioWrite(_, _))
    .Times(0);
    ASSERT_EQ(-1, fileManager_->AioWrite(10, aioContext_));

    // 文件是opened状态
    NebdFileRecord fileRecord;
    fileRecord.fd = 1;
    fileRecord.fileName = testFile1;
    fileRecord.status = NebdFileStatus::OPENED;
    fileRecord.executor = executor_.get();
    fileRecord.fileInstance = mockInstance_;
    EXPECT_CALL(*filerecordManager_, GetRecord(1, _))
    .WillOnce(DoAll(SetArgPointee<1>(fileRecord),
                    Return(true)));
    EXPECT_CALL(*executor_, AioWrite(NotNull(), aioContext_))
    .WillOnce(Return(-1));
    ASSERT_EQ(-1, fileManager_->AioWrite(1, aioContext_));

    // 文件状态为closed，会重新open文件
    fileRecord.fd = 1;
    fileRecord.fileName = testFile1;
    fileRecord.status = NebdFileStatus::CLOSED;
    EXPECT_CALL(*filerecordManager_, GetRecord(1, _))
    .WillRepeatedly(DoAll(SetArgPointee<1>(fileRecord),
                          Return(true)));
    EXPECT_CALL(*filerecordManager_, GetRecord(testFile1, _))
    .WillRepeatedly(DoAll(SetArgPointee<1>(fileRecord),
                          Return(true)));
    // 重新open文件时，open失败
    EXPECT_CALL(*executor_, Open(testFile1))
    .WillOnce(Return(nullptr));
    EXPECT_CALL(*filerecordManager_, UpdateRecord(_))
    .Times(0);
    EXPECT_CALL(*executor_, AioWrite(NotNull(), aioContext_))
    .Times(0);
    ASSERT_EQ(-1, fileManager_->AioWrite(1, aioContext_));
    // 重新open文件时，更新meta file失败
    EXPECT_CALL(*executor_, Open(testFile1))
    .WillOnce(Return(mockInstance_));
    EXPECT_CALL(*filerecordManager_, UpdateRecord(_))
    .WillOnce(Return(false));
    EXPECT_CALL(*executor_, AioWrite(NotNull(), aioContext_))
    .Times(0);
    ASSERT_EQ(-1, fileManager_->AioWrite(1, aioContext_));
}

TEST_F(FileManagerTest, DiscardTest) {
    // 文件是opened状态
    NebdFileRecord fileRecord;
    fileRecord.fd = 1;
    fileRecord.fileName = testFile1;
    fileRecord.status = NebdFileStatus::OPENED;
    fileRecord.executor = executor_.get();
    fileRecord.fileInstance = mockInstance_;
    EXPECT_CALL(*filerecordManager_, GetRecord(1, _))
    .WillOnce(DoAll(SetArgPointee<1>(fileRecord),
                    Return(true)));
    EXPECT_CALL(*executor_, Discard(NotNull(), aioContext_))
    .WillOnce(Return(0));
    ASSERT_EQ(0, fileManager_->Discard(1, aioContext_));

    // 文件状态为closed，会重新open文件
    fileRecord.fd = 1;
    fileRecord.fileName = testFile1;
    fileRecord.status = NebdFileStatus::CLOSED;
    EXPECT_CALL(*filerecordManager_, GetRecord(1, _))
    .WillRepeatedly(DoAll(SetArgPointee<1>(fileRecord),
                          Return(true)));
    EXPECT_CALL(*filerecordManager_, GetRecord(testFile1, _))
    .WillRepeatedly(DoAll(SetArgPointee<1>(fileRecord),
                          Return(true)));
    // 重新open该文件，fd不变
    EXPECT_CALL(*executor_, Open(testFile1))
    .WillOnce(Return(mockInstance_));
    EXPECT_CALL(*filerecordManager_, UpdateRecord(_))
    .WillOnce(Return(true));
    EXPECT_CALL(*executor_, Discard(NotNull(), aioContext_))
    .WillOnce(Return(0));
    ASSERT_EQ(0, fileManager_->Discard(1, aioContext_));
}

TEST_F(FileManagerTest, DiscardFaileTest) {
    // 文件不存在
    EXPECT_CALL(*filerecordManager_, GetRecord(10, _))
    .WillOnce(Return(false));
    EXPECT_CALL(*executor_, Discard(_, _))
    .Times(0);
    ASSERT_EQ(-1, fileManager_->Discard(10, aioContext_));

    // 文件是opened状态
    NebdFileRecord fileRecord;
    fileRecord.fd = 1;
    fileRecord.fileName = testFile1;
    fileRecord.status = NebdFileStatus::OPENED;
    fileRecord.executor = executor_.get();
    fileRecord.fileInstance = mockInstance_;
    EXPECT_CALL(*filerecordManager_, GetRecord(1, _))
    .WillOnce(DoAll(SetArgPointee<1>(fileRecord),
                    Return(true)));
    EXPECT_CALL(*executor_, Discard(NotNull(), aioContext_))
    .WillOnce(Return(-1));
    ASSERT_EQ(-1, fileManager_->Discard(1, aioContext_));

    // 文件状态为closed，会重新open文件
    fileRecord.fd = 1;
    fileRecord.fileName = testFile1;
    fileRecord.status = NebdFileStatus::CLOSED;
    EXPECT_CALL(*filerecordManager_, GetRecord(1, _))
    .WillRepeatedly(DoAll(SetArgPointee<1>(fileRecord),
                          Return(true)));
    EXPECT_CALL(*filerecordManager_, GetRecord(testFile1, _))
    .WillRepeatedly(DoAll(SetArgPointee<1>(fileRecord),
                          Return(true)));
    // 重新open文件时，open失败
    EXPECT_CALL(*executor_, Open(testFile1))
    .WillOnce(Return(nullptr));
    EXPECT_CALL(*filerecordManager_, UpdateRecord(_))
    .Times(0);
    EXPECT_CALL(*executor_, Discard(NotNull(), aioContext_))
    .Times(0);
    ASSERT_EQ(-1, fileManager_->Discard(1, aioContext_));
    // 重新open文件时，更新meta file失败
    EXPECT_CALL(*executor_, Open(testFile1))
    .WillOnce(Return(mockInstance_));
    EXPECT_CALL(*filerecordManager_, UpdateRecord(_))
    .WillOnce(Return(false));
    EXPECT_CALL(*executor_, Discard(NotNull(), aioContext_))
    .Times(0);
    ASSERT_EQ(-1, fileManager_->Discard(1, aioContext_));
}

TEST_F(FileManagerTest, FlushTest) {
    // 文件是opened状态
    NebdFileRecord fileRecord;
    fileRecord.fd = 1;
    fileRecord.fileName = testFile1;
    fileRecord.status = NebdFileStatus::OPENED;
    fileRecord.executor = executor_.get();
    fileRecord.fileInstance = mockInstance_;
    EXPECT_CALL(*filerecordManager_, GetRecord(1, _))
    .WillOnce(DoAll(SetArgPointee<1>(fileRecord),
                    Return(true)));
    EXPECT_CALL(*executor_, Flush(NotNull(), aioContext_))
    .WillOnce(Return(0));
    ASSERT_EQ(0, fileManager_->Flush(1, aioContext_));

    // 文件状态为closed，会重新open文件
    fileRecord.fd = 1;
    fileRecord.fileName = testFile1;
    fileRecord.status = NebdFileStatus::CLOSED;
    EXPECT_CALL(*filerecordManager_, GetRecord(1, _))
    .WillRepeatedly(DoAll(SetArgPointee<1>(fileRecord),
                          Return(true)));
    EXPECT_CALL(*filerecordManager_, GetRecord(testFile1, _))
    .WillRepeatedly(DoAll(SetArgPointee<1>(fileRecord),
                          Return(true)));
    // 重新open该文件，fd不变
    EXPECT_CALL(*executor_, Open(testFile1))
    .WillOnce(Return(mockInstance_));
    EXPECT_CALL(*filerecordManager_, UpdateRecord(_))
    .WillOnce(Return(true));
    EXPECT_CALL(*executor_, Flush(NotNull(), aioContext_))
    .WillOnce(Return(0));
    ASSERT_EQ(0, fileManager_->Flush(1, aioContext_));
}

TEST_F(FileManagerTest, FlushFaileTest) {
    // 文件不存在
    EXPECT_CALL(*filerecordManager_, GetRecord(10, _))
    .WillOnce(Return(false));
    EXPECT_CALL(*executor_, Flush(_, _))
    .Times(0);
    ASSERT_EQ(-1, fileManager_->Flush(10, aioContext_));

    // 文件是opened状态
    NebdFileRecord fileRecord;
    fileRecord.fd = 1;
    fileRecord.fileName = testFile1;
    fileRecord.status = NebdFileStatus::OPENED;
    fileRecord.executor = executor_.get();
    fileRecord.fileInstance = mockInstance_;
    EXPECT_CALL(*filerecordManager_, GetRecord(1, _))
    .WillOnce(DoAll(SetArgPointee<1>(fileRecord),
                    Return(true)));
    EXPECT_CALL(*executor_, Flush(NotNull(), aioContext_))
    .WillOnce(Return(-1));
    ASSERT_EQ(-1, fileManager_->Flush(1, aioContext_));

    // 文件状态为closed，会重新open文件
    fileRecord.fd = 1;
    fileRecord.fileName = testFile1;
    fileRecord.status = NebdFileStatus::CLOSED;
    EXPECT_CALL(*filerecordManager_, GetRecord(1, _))
    .WillRepeatedly(DoAll(SetArgPointee<1>(fileRecord),
                          Return(true)));
    EXPECT_CALL(*filerecordManager_, GetRecord(testFile1, _))
    .WillRepeatedly(DoAll(SetArgPointee<1>(fileRecord),
                          Return(true)));
    // 重新open文件时，open失败
    EXPECT_CALL(*executor_, Open(testFile1))
    .WillOnce(Return(nullptr));
    EXPECT_CALL(*filerecordManager_, UpdateRecord(_))
    .Times(0);
    EXPECT_CALL(*executor_, Flush(NotNull(), aioContext_))
    .Times(0);
    ASSERT_EQ(-1, fileManager_->Flush(1, aioContext_));
    // 重新open文件时，更新meta file失败
    EXPECT_CALL(*executor_, Open(testFile1))
    .WillOnce(Return(mockInstance_));
    EXPECT_CALL(*filerecordManager_, UpdateRecord(_))
    .WillOnce(Return(false));
    EXPECT_CALL(*executor_, Flush(NotNull(), aioContext_))
    .Times(0);
    ASSERT_EQ(-1, fileManager_->Flush(1, aioContext_));
}

}  // namespace server
}  // namespace nebd

int main(int argc, char ** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    ::testing::InitGoogleMock(&argc, argv);
    return RUN_ALL_TESTS();
}
