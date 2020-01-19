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

#include "src/part2/file_record_map.h"

namespace nebd {
namespace server {

class FileRecordMapTest : public ::testing::Test {
 public:
    void SetUp() {}
    void TearDown() {}

 protected:
    FileRecordMap recordMap_;
};

TEST_F(FileRecordMapTest, BasicTest) {
    int fd1 = 1;
    std::string fileName1 = "file1";

    // 初始测试
    ASSERT_FALSE(recordMap_.Exist(fd1));
    ASSERT_EQ(recordMap_.GetRecord(fd1), nullptr);
    ASSERT_EQ(recordMap_.GetRecord(fileName1), nullptr);

    // UpdateRecord
    NebdFileRecordPtr fileRecord1 = std::make_shared<NebdFileRecord>();
    fileRecord1->fileName = fileName1;
    fileRecord1->fd = fd1;
    ASSERT_TRUE(recordMap_.UpdateRecord(fileRecord1));
    ASSERT_TRUE(recordMap_.Exist(fd1));
    // 通过fd获取record
    NebdFileRecordPtr tempRecord = recordMap_.GetRecord(fd1);
    ASSERT_NE(tempRecord, nullptr);
    ASSERT_EQ(tempRecord->fd, fd1);
    ASSERT_EQ(tempRecord->fileName, fileName1);
    // 通过filename获取record
    tempRecord = recordMap_.GetRecord(fileName1);
    ASSERT_NE(tempRecord, nullptr);
    ASSERT_EQ(tempRecord->fd, fd1);
    ASSERT_EQ(tempRecord->fileName, fileName1);

    // 插入一条filename相同，fd不同的记录
    fileRecord1->fd = ++fd1;
    ASSERT_TRUE(recordMap_.UpdateRecord(fileRecord1));
    ASSERT_FALSE(recordMap_.Exist(fd1-1));
    ASSERT_TRUE(recordMap_.Exist(fd1));

    // 插入不同记录
    NebdFileRecordPtr fileRecord2 = std::make_shared<NebdFileRecord>();
    std::string fileName2 = "file2";
    int fd2 = 3;
    fileRecord2->fileName = fileName2;
    fileRecord2->fd = fd2;
    ASSERT_TRUE(recordMap_.UpdateRecord(fileRecord2));
    ASSERT_TRUE(recordMap_.Exist(fd1));
    ASSERT_TRUE(recordMap_.Exist(fd2));

    // 插入fd相同，名字不同的记录
    NebdFileRecordPtr fileRecord3 = std::make_shared<NebdFileRecord>();
    fileRecord3->fileName = fileName1;
    fileRecord3->fd = fd2;
    ASSERT_FALSE(recordMap_.UpdateRecord(fileRecord3));

    // 校验最终结果
    std::unordered_map<int, NebdFileRecordPtr> list = recordMap_.ListRecords();
    ASSERT_EQ(list.size(), 2);
    // 通过fd获取record并检查结果
    NebdFileRecordPtr tempRecord1 = recordMap_.GetRecord(fd1);
    ASSERT_NE(tempRecord1, nullptr);
    ASSERT_EQ(tempRecord1->fd, fd1);
    ASSERT_EQ(tempRecord1->fileName, fileName1);
    NebdFileRecordPtr tempRecord2 = recordMap_.GetRecord(fd2);
    ASSERT_NE(tempRecord2, nullptr);
    ASSERT_EQ(tempRecord2->fd, fd2);
    ASSERT_EQ(tempRecord2->fileName, fileName2);

    // clear
    recordMap_.Clear();
    list = recordMap_.ListRecords();
    ASSERT_EQ(list.size(), 0);
}

}  // namespace server
}  // namespace nebd

int main(int argc, char ** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    ::testing::InitGoogleMock(&argc, argv);
    return RUN_ALL_TESTS();
}
