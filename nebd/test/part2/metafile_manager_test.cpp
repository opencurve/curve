/*
 * Project: nebd
 * Created Date: 2020-01-19
 * Author: charisu
 * Copyright (c) 2020 netease
 */

#include <gtest/gtest.h>
#include <json/json.h>
#include <fstream>

#include "nebd/src/part2/metafile_manager.h"
#include "nebd/test/part2/mock_posix_wrapper.h"

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
    NebdMetaFileManagerOption option;
    option.metaFilePath = metaPath;
    NebdMetaFileManager metaFileManager;
    ASSERT_EQ(metaFileManager.Init(option), 0);
    std::vector<NebdFileMeta> fileMetas;
    // 文件不存在
    ASSERT_EQ(0, metaFileManager.ListFileMeta(&fileMetas));
    ASSERT_TRUE(fileMetas.empty());

    // 添加两条记录，ceph和curve各一
    NebdFileMeta fileMeta1;
    fileMeta1.fileName = "rbd:volume1";
    fileMeta1.fd = 1;
    ASSERT_EQ(0, metaFileManager.UpdateFileMeta(fileMeta1.fileName, fileMeta1));
    // 使用相同的内容Update
    ASSERT_EQ(0, metaFileManager.UpdateFileMeta(fileMeta1.fileName, fileMeta1));

    // 插入不同的meta
    NebdFileMeta fileMeta2;
    fileMeta2.fileName = "cbd:volume2";
    fileMeta2.fd = 2;
    fileMeta2.xattr["session"] = "test-session";
    ASSERT_EQ(0, metaFileManager.UpdateFileMeta(fileMeta2.fileName, fileMeta2));

    // listFileRecord
    fileMetas.clear();
    ASSERT_EQ(0, metaFileManager.ListFileMeta(&fileMetas));
    ASSERT_EQ(2, fileMetas.size());
    ASSERT_EQ(fileMeta1, fileMetas[1]);
    ASSERT_EQ(fileMeta2, fileMetas[0]);

    // remove meta
    ASSERT_EQ(0, metaFileManager.RemoveFileMeta(fileMeta2.fileName));
    // remove 不存在的meta
    ASSERT_EQ(0, metaFileManager.RemoveFileMeta("unknown"));
    // 校验结果
    fileMetas.clear();
    ASSERT_EQ(0, metaFileManager.ListFileMeta(&fileMetas));
    ASSERT_EQ(1, fileMetas.size());
    ASSERT_EQ(fileMeta1, fileMetas[0]);
}

TEST_F(MetaFileManagerTest, UpdateMetaFailTest) {
    NebdMetaFileManagerOption option;
    option.metaFilePath = metaPath;
    option.wrapper = wrapper_;
    NebdMetaFileManager metaFileManager;
    ASSERT_EQ(metaFileManager.Init(option), 0);
    NebdFileMeta fileMeta;
    fileMeta.fileName = "rbd:volume1";
    fileMeta.fd = 111;
    FileMetaMap fileMetaMap;
    fileMetaMap.emplace(fileMeta.fileName, fileMeta);
    std::vector<NebdFileMeta> fileMetas;

    // open临时文件失败
    EXPECT_CALL(*wrapper_, open(_, _, _))
        .WillOnce(Return(-1));
    ASSERT_EQ(-1, metaFileManager.UpdateFileMeta(fileMeta.fileName, fileMeta));
    ASSERT_EQ(0, metaFileManager.ListFileMeta(&fileMetas));
    ASSERT_EQ(0, fileMetas.size());

    // 写入临时文件失败
    EXPECT_CALL(*wrapper_, open(_, _, _))
        .WillOnce(Return(1));
    EXPECT_CALL(*wrapper_, pwrite(_, _, _, _))
        .WillOnce(Return(0));
    EXPECT_CALL(*wrapper_, close(_))
    .Times(1);
    ASSERT_EQ(-1, metaFileManager.UpdateFileMeta(fileMeta.fileName, fileMeta));
    ASSERT_EQ(0, metaFileManager.ListFileMeta(&fileMetas));
    ASSERT_EQ(0, fileMetas.size());

    // rename失败
    NebdMetaFileParser parser;
    Json::Value root = parser.ConvertFileMetasToJson(fileMetaMap);
    EXPECT_CALL(*wrapper_, open(_, _, _))
        .WillOnce(Return(1));
    EXPECT_CALL(*wrapper_, pwrite(_, _, _, _))
        .WillOnce(Return(root.toStyledString().size()));
    EXPECT_CALL(*wrapper_, close(_))
    .Times(1);
    EXPECT_CALL(*wrapper_, rename(_, _))
        .WillOnce(Return(-1));
    ASSERT_EQ(-1, metaFileManager.UpdateFileMeta(fileMeta.fileName, fileMeta));
    ASSERT_EQ(0, metaFileManager.ListFileMeta(&fileMetas));
    ASSERT_EQ(0, fileMetas.size());
}

TEST_F(MetaFileManagerTest, RemoveMetaFailTest) {
    NebdMetaFileManagerOption option;
    option.metaFilePath = metaPath;
    option.wrapper = wrapper_;
    NebdMetaFileManager metaFileManager;
    ASSERT_EQ(metaFileManager.Init(option), 0);
    NebdFileMeta fileMeta;
    fileMeta.fileName = "rbd:volume1";
    fileMeta.fd = 111;
    FileMetaMap fileMetaMap;
    fileMetaMap.emplace(fileMeta.fileName, fileMeta);
    std::vector<NebdFileMeta> fileMetas;
    NebdMetaFileParser parser;
    Json::Value root = parser.ConvertFileMetasToJson(fileMetaMap);

    // 先插入一条数据
    EXPECT_CALL(*wrapper_, open(_, _, _))
        .WillOnce(Return(1));
    EXPECT_CALL(*wrapper_, pwrite(_, _, _, _))
        .WillOnce(Return(root.toStyledString().size()));
    EXPECT_CALL(*wrapper_, close(_))
    .Times(1);
    EXPECT_CALL(*wrapper_, rename(_, _))
        .WillOnce(Return(0));
    ASSERT_EQ(0, metaFileManager.UpdateFileMeta(fileMeta.fileName, fileMeta));
    ASSERT_EQ(0, metaFileManager.ListFileMeta(&fileMetas));
    ASSERT_EQ(1, fileMetas.size());

    fileMetaMap.erase(fileMeta.fileName);
    root = parser.ConvertFileMetasToJson(fileMetaMap);

    // open临时文件失败
    EXPECT_CALL(*wrapper_, open(_, _, _))
        .WillOnce(Return(-1));
    ASSERT_EQ(-1, metaFileManager.RemoveFileMeta(fileMeta.fileName));
    ASSERT_EQ(0, metaFileManager.ListFileMeta(&fileMetas));
    ASSERT_EQ(1, fileMetas.size());

    // 写入临时文件失败
    EXPECT_CALL(*wrapper_, open(_, _, _))
        .WillOnce(Return(1));
    EXPECT_CALL(*wrapper_, pwrite(_, _, _, _))
        .WillOnce(Return(0));
    EXPECT_CALL(*wrapper_, close(_))
    .Times(1);
    ASSERT_EQ(-1, metaFileManager.RemoveFileMeta(fileMeta.fileName));
    ASSERT_EQ(0, metaFileManager.ListFileMeta(&fileMetas));
    ASSERT_EQ(1, fileMetas.size());

    // rename失败
    EXPECT_CALL(*wrapper_, open(_, _, _))
        .WillOnce(Return(1));
    EXPECT_CALL(*wrapper_, pwrite(_, _, _, _))
        .WillOnce(Return(root.toStyledString().size()));
    EXPECT_CALL(*wrapper_, close(_))
    .Times(1);
    EXPECT_CALL(*wrapper_, rename(_, _))
        .WillOnce(Return(-1));
    ASSERT_EQ(-1, metaFileManager.RemoveFileMeta(fileMeta.fileName));
    ASSERT_EQ(0, metaFileManager.ListFileMeta(&fileMetas));
    ASSERT_EQ(1, fileMetas.size());
}

TEST(MetaFileParserTest, Parse) {
    NebdMetaFileParser parser;
    Json::Value root;
    Json::Value volume;
    Json::Value volumes;
    FileMetaMap fileMetas;

    // 正常情况
    volume[kFileName] = "rbd:volume1";
    volume[kFd] = 1;
    volumes.append(volume);
    volume[kFileName] = "cbd:volume2";
    volume[kFd] = 2;
    root[kVolumes] = volumes;
    FillCrc(&root);
    ASSERT_EQ(0, parser.Parse(root, &fileMetas));

    // 空指针
    ASSERT_EQ(-1, parser.Parse(root, nullptr));

    // crc校验不正确
    root[kCRC] = root[kCRC].asUInt() + 1;
    ASSERT_EQ(-1, parser.Parse(root, &fileMetas));

     // 没有crc字段
    root.removeMember(kCRC);
    ASSERT_EQ(-1, parser.Parse(root, &fileMetas));

    // 没有volumes字段或volumes字段是null,不应该报错
    root.clear();
    root["key"] = "value";
    FillCrc(&root);
    ASSERT_EQ(0, parser.Parse(root, &fileMetas));
    ASSERT_TRUE(fileMetas.empty());
    root.clear();
    Json::Value value;
    root[kVolumes] = value;
    FillCrc(&root);
    ASSERT_EQ(0, parser.Parse(root, &fileMetas));
    ASSERT_TRUE(fileMetas.empty());

    // 记录中没有filename
    volume.clear();
    volumes.clear();
    root.clear();
    volume[kFd] = 1234;
    volumes.append(volume);
    root[kVolumes] = volumes;
    FillCrc(&root);
    ASSERT_EQ(-1, parser.Parse(root, &fileMetas));

    // 记录中没有fd
    volume.clear();
    volumes.clear();
    root.clear();
    volume[kFileName] = "cbd:volume2";
    volumes.append(volume);
    root[kVolumes] = volumes;
    FillCrc(&root);
    ASSERT_EQ(-1, parser.Parse(root, &fileMetas));
}

}  // namespace server
}  // namespace nebd

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    RUN_ALL_TESTS();
    return 0;
}
