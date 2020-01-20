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

TEST_F(MetaFileManagerTest, common) {
    NebdMetaFileManager metaFileManager(metaPath);
    std::vector<NebdFileRecordPtr> records;
    // 文件不存在
    ASSERT_EQ(0, metaFileManager.ListFileRecord(&records));
    ASSERT_TRUE(records.empty());

    // 添加两条记录，ceph和curve各一个
    NebdFileRecordPtr fileRecord1 = std::make_shared<NebdFileRecord>();
    fileRecord1->fileName = "rbd:volume1";
    fileRecord1->fd = 111;
    ASSERT_EQ(0, metaFileManager.UpdateFileRecord(fileRecord1));
    NebdFileRecordPtr fileRecord2 = std::make_shared<NebdFileRecord>();
    fileRecord2->fileName = "cbd:volume2";
    fileRecord2->fd = 222;
    fileRecord2->fileInstance = std::make_shared<CurveFileInstance>();
    fileRecord2->fileInstance->addition["session"] = "test-session";
    ASSERT_EQ(0, metaFileManager.UpdateFileRecord(fileRecord2));
    fileRecord2->fd = 3333;
    ASSERT_EQ(0, metaFileManager.UpdateFileRecord(fileRecord2));
    fileRecord2->fileInstance->addition["session"] = "test-session-2";
    ASSERT_EQ(0, metaFileManager.UpdateFileRecord(fileRecord2));
    // 重复更新
    ASSERT_EQ(0, metaFileManager.UpdateFileRecord(fileRecord2));

    // listFileRecord
    ASSERT_EQ(0, metaFileManager.ListFileRecord(&records));
    ASSERT_EQ(2, records.size());
    ASSERT_EQ(fileRecord1->fileName, records[0]->fileName);
    ASSERT_EQ(fileRecord1->fd, records[0]->fd);
    ASSERT_EQ(fileRecord2->fileName, records[1]->fileName);
    ASSERT_EQ(fileRecord2->fd, records[1]->fd);
    ASSERT_EQ(fileRecord2->fileInstance->addition,
                    records[1]->fileInstance->addition);

    // RemoveFileRecord
    ASSERT_EQ(0, metaFileManager.RemoveFileRecord("cbd:volume2"));
    ASSERT_EQ(0, metaFileManager.ListFileRecord(&records));
    ASSERT_EQ(1, records.size());
    ASSERT_EQ(fileRecord1->fileName, records[0]->fileName);
    ASSERT_EQ(fileRecord1->fd, records[0]->fd);
    // volume not exist
    ASSERT_EQ(0, metaFileManager.RemoveFileRecord("cbd:volume123"));
    ASSERT_EQ(0, metaFileManager.ListFileRecord(&records));
    ASSERT_EQ(1, records.size());
}

TEST_F(MetaFileManagerTest, error) {
    NebdMetaFileManager metaFileManager(metaPath, wrapper_);
    std::vector<NebdFileRecordPtr> records;
    NebdFileRecordPtr fileRecord = std::make_shared<NebdFileRecord>();
    fileRecord->fileName = "rbd:volume1";
    fileRecord->fd = 111;

    // open临时文件失败
    EXPECT_CALL(*wrapper_, open(_, _, _))
        .Times(1)
        .WillOnce(Return(-1));
    ASSERT_EQ(-1, metaFileManager.UpdateFileRecord(fileRecord));

    // 写入临时文件失败
    EXPECT_CALL(*wrapper_, open(_, _, _))
        .Times(1)
        .WillOnce(Return(1));
    EXPECT_CALL(*wrapper_, pwrite(_, _, _, _))
        .Times(1)
        .WillOnce(Return(0));
    ASSERT_EQ(-1, metaFileManager.UpdateFileRecord(fileRecord));

    // rename失败
    EXPECT_CALL(*wrapper_, open(_, _, _))
        .Times(1)
        .WillOnce(Return(1));
    EXPECT_CALL(*wrapper_, pwrite(_, _, _, _))
        .Times(1)
        .WillOnce(Return(77));
    EXPECT_CALL(*wrapper_, rename(_, _))
        .Times(1)
        .WillOnce(Return(-1));
    ASSERT_EQ(-1, metaFileManager.UpdateFileRecord(fileRecord));

    // 解析失败
    std::ofstream out(metaPath);
    out.close();
    ASSERT_EQ(-1, metaFileManager.ListFileRecord(&records));
    ASSERT_EQ(-1, metaFileManager.RemoveFileRecord("cbd:volume2"));
    ASSERT_EQ(-1, metaFileManager.UpdateFileRecord(fileRecord));

    // UpdataFileRecord的参数为空
    ASSERT_EQ(-1, metaFileManager.UpdateFileRecord(nullptr));
}

TEST(MetaFileParserTest, Parse) {
    NebdMetaFileParser parser;
    Json::Value root;
    Json::Value volume;
    Json::Value volumes;
    std::vector<NebdFileRecordPtr> records;

    // 正常情况
    volume[kFileName] = "rbd:volume1";
    volume[kFd] = 1;
    volumes.append(volume);
    volume[kFileName] = "cbd:volume2";
    volume[kFd] = 2;
    root[kVolumes] = volumes;
    ASSERT_EQ(0, parser.Parse(root, &records));

    // 文件格式不正确
    root.clear();
    ASSERT_EQ(-1, parser.Parse(root, &records));

    // 没有volume字段
    root["key"] = "value";
    ASSERT_EQ(-1, parser.Parse(root, &records));

    // 记录中没有filename
    volume.clear();
    volumes.clear();
    root.clear();
    volume[kFd] = 1234;
    volumes.append(volume);
    root[kVolumes] = volumes;
    ASSERT_EQ(-1, parser.Parse(root, &records));

    // 记录中没有fd
    volume.clear();
    volumes.clear();
    root.clear();
    volume[kFileName] = "cbd:volume2";
    volumes.append(volume);
    root[kVolumes] = volumes;
    ASSERT_EQ(-1, parser.Parse(root, &records));

    // 文件名格式不对
    volume.clear();
    volumes.clear();
    root.clear();
    volume[kFileName] = "volume2";
    volume[kFd] = 1234;
    volumes.append(volume);
    root[kVolumes] = volumes;
    ASSERT_EQ(-1, parser.Parse(root, &records));
}

}  // namespace server
}  // namespace nebd

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    RUN_ALL_TESTS();
    return 0;
}
