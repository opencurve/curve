/*
 * Project: nebd
 * Created Date: 2020-01-19
 * Author: charisu
 * Copyright (c) 2020 netease
 */

#include <gtest/gtest.h>
#include <json/json.h>
#include <fstream>

#include "src/part2/metafile_manager.h"
#include "src/part2/request_executor_curve.h"
#include "tests/part2/mock_posix_wrapper.h"

using ::testing::_;
using ::testing::Return;

namespace nebd {
namespace server {

const char metaPath[] = "/tmp/nebd-test-metafilemanager.meta";

void FillCrc(Json::Value* root) {
    std::string jsonString = root->toStyledString();
    uint32_t crc = nebd::common::CRC32(jsonString.c_str(),
                                       jsonString.size());
    (*root)[kCRC] = crc;
}

class MetaFileManagerTest : public ::testing::Test {
 protected:
    void SetUp() override {
        wrapper_ = std::make_shared<common::MockPosixWrapper>();
    }
    void TearDown() override {
        unlink(metaPath);
        std::string tmpPath = std::string(metaPath) + ".tmp";
        unlink(tmpPath.c_str());
    }
    std::shared_ptr<common::MockPosixWrapper> wrapper_;
};

TEST_F(MetaFileManagerTest, nomaltest) {
    NebdMetaFileManager metaFileManager(metaPath);
    FileRecordMap records;
    // 文件不存在
    ASSERT_EQ(0, metaFileManager.ListFileRecord(&records));
    ASSERT_TRUE(records.empty());

    // 添加两条记录，ceph和curve各一
    NebdFileRecord fileRecord1;
    fileRecord1.fileName = "rbd:volume1";
    fileRecord1.fd = 1;
    records.emplace(1, fileRecord1);
    ASSERT_EQ(0, metaFileManager.UpdateMetaFile(records));
    NebdFileRecord fileRecord2;
    fileRecord2.fileName = "cbd:volume2";
    fileRecord2.fd = 2;
    fileRecord2.fileInstance = std::make_shared<CurveFileInstance>();
    fileRecord2.fileInstance->addition["session"] = "test-session";
    records.emplace(2, fileRecord2);
    ASSERT_EQ(0, metaFileManager.UpdateMetaFile(records));

    // listFileRecord
    records.clear();
    ASSERT_EQ(0, metaFileManager.ListFileRecord(&records));
    ASSERT_EQ(2, records.size());
    ASSERT_EQ(fileRecord1.fileName, records[1].fileName);
    ASSERT_EQ(fileRecord1.fd, records[1].fd);
    ASSERT_EQ(fileRecord2.fileName, records[2].fileName);
    ASSERT_EQ(fileRecord2.fd, records[2].fd);
    ASSERT_EQ(fileRecord2.fileInstance->addition,
                    records[2].fileInstance->addition);
}

TEST_F(MetaFileManagerTest, error) {
    NebdMetaFileManager metaFileManager(metaPath, wrapper_);
    FileRecordMap records;
    NebdFileRecord fileRecord;
    fileRecord.fileName = "rbd:volume1";
    fileRecord.fd = 111;
    records.emplace(111, fileRecord);

    // open临时文件失败
    EXPECT_CALL(*wrapper_, open(_, _, _))
        .WillOnce(Return(-1));
    ASSERT_EQ(-1, metaFileManager.UpdateMetaFile(records));

    // 写入临时文件失败
    EXPECT_CALL(*wrapper_, open(_, _, _))
        .WillOnce(Return(1));
    EXPECT_CALL(*wrapper_, pwrite(_, _, _, _))
        .WillOnce(Return(0));
    EXPECT_CALL(*wrapper_, close(_))
    .Times(1);
    ASSERT_EQ(-1, metaFileManager.UpdateMetaFile(records));

    // rename失败
    NebdMetaFileParser parser;
    Json::Value root = parser.ConvertFileRecordsToJson(records);
    EXPECT_CALL(*wrapper_, open(_, _, _))
        .WillOnce(Return(1));
    EXPECT_CALL(*wrapper_, pwrite(_, _, _, _))
        .WillOnce(Return(root.toStyledString().size()));
    EXPECT_CALL(*wrapper_, close(_))
    .Times(1);
    EXPECT_CALL(*wrapper_, rename(_, _))
        .WillOnce(Return(-1));
    ASSERT_EQ(-1, metaFileManager.UpdateMetaFile(records));
}

TEST(MetaFileParserTest, Parse) {
    NebdMetaFileParser parser;
    Json::Value root;
    Json::Value volume;
    Json::Value volumes;
    FileRecordMap records;

    // 正常情况
    volume[kFileName] = "rbd:volume1";
    volume[kFd] = 1;
    volumes.append(volume);
    volume[kFileName] = "cbd:volume2";
    volume[kFd] = 2;
    root[kVolumes] = volumes;
    FillCrc(&root);
    ASSERT_EQ(0, parser.Parse(root, &records));

    // 空指针
    ASSERT_EQ(-1, parser.Parse(root, nullptr));

    // crc校验不正确
    root[kCRC] = root[kCRC].asUInt() + 1;
    ASSERT_EQ(-1, parser.Parse(root, &records));

     // 没有crc字段
    root.removeMember(kCRC);
    ASSERT_EQ(-1, parser.Parse(root, &records));

    // 没有volumes字段或volumes字段是null,不应该报错
    root.clear();
    root["key"] = "value";
    FillCrc(&root);
    ASSERT_EQ(0, parser.Parse(root, &records));
    ASSERT_TRUE(records.empty());
    root.clear();
    Json::Value value;
    root[kVolumes] = value;
    FillCrc(&root);
    ASSERT_EQ(0, parser.Parse(root, &records));
    ASSERT_TRUE(records.empty());

    // 记录中没有filename
    volume.clear();
    volumes.clear();
    root.clear();
    volume[kFd] = 1234;
    volumes.append(volume);
    root[kVolumes] = volumes;
    FillCrc(&root);
    ASSERT_EQ(-1, parser.Parse(root, &records));

    // 记录中没有fd
    volume.clear();
    volumes.clear();
    root.clear();
    volume[kFileName] = "cbd:volume2";
    volumes.append(volume);
    root[kVolumes] = volumes;
    FillCrc(&root);
    ASSERT_EQ(-1, parser.Parse(root, &records));

    // 文件名格式不对
    volume.clear();
    volumes.clear();
    root.clear();
    volume[kFileName] = "volume2";
    volume[kFd] = 1234;
    volumes.append(volume);
    root[kVolumes] = volumes;
    FillCrc(&root);
    ASSERT_EQ(-1, parser.Parse(root, &records));
}

}  // namespace server
}  // namespace nebd

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    RUN_ALL_TESTS();
    return 0;
}
