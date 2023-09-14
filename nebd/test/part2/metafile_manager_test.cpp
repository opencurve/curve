/*
 *  Copyright (c) 2020 NetEase Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

/*
 * Project: nebd
 * Created Date: 2020-01-19
 * Author: charisu
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
    // File does not exist
    ASSERT_EQ(0, metaFileManager.ListFileMeta(&fileMetas));
    ASSERT_TRUE(fileMetas.empty());

    // Add two records, one for curve and one for test
    NebdFileMeta fileMeta1;
    fileMeta1.fileName = "test:volume1";
    fileMeta1.fd = 1;
    ASSERT_EQ(0, metaFileManager.UpdateFileMeta(fileMeta1.fileName, fileMeta1));
    // Update using the same content
    ASSERT_EQ(0, metaFileManager.UpdateFileMeta(fileMeta1.fileName, fileMeta1));

    // Insert different meta
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
    // remove non-existent meta
    ASSERT_EQ(0, metaFileManager.RemoveFileMeta("unknown"));
    // Verification results
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
    fileMeta.fileName = "cbd:volume1";
    fileMeta.fd = 111;
    FileMetaMap fileMetaMap;
    fileMetaMap.emplace(fileMeta.fileName, fileMeta);
    std::vector<NebdFileMeta> fileMetas;

    // Open temporary file failed
    EXPECT_CALL(*wrapper_, open(_, _, _))
        .WillOnce(Return(-1));
    ASSERT_EQ(-1, metaFileManager.UpdateFileMeta(fileMeta.fileName, fileMeta));
    ASSERT_EQ(0, metaFileManager.ListFileMeta(&fileMetas));
    ASSERT_EQ(0, fileMetas.size());

    // Failed to write temporary file
    EXPECT_CALL(*wrapper_, open(_, _, _))
        .WillOnce(Return(1));
    EXPECT_CALL(*wrapper_, pwrite(_, _, _, _))
        .WillOnce(Return(0));
    EXPECT_CALL(*wrapper_, close(_))
    .Times(1);
    ASSERT_EQ(-1, metaFileManager.UpdateFileMeta(fileMeta.fileName, fileMeta));
    ASSERT_EQ(0, metaFileManager.ListFileMeta(&fileMetas));
    ASSERT_EQ(0, fileMetas.size());

    // Rename failed
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
    fileMeta.fileName = "cbd:volume1";
    fileMeta.fd = 111;
    FileMetaMap fileMetaMap;
    fileMetaMap.emplace(fileMeta.fileName, fileMeta);
    std::vector<NebdFileMeta> fileMetas;
    NebdMetaFileParser parser;
    Json::Value root = parser.ConvertFileMetasToJson(fileMetaMap);

    // Insert a piece of data first
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

    // Open temporary file failed
    EXPECT_CALL(*wrapper_, open(_, _, _))
        .WillOnce(Return(-1));
    ASSERT_EQ(-1, metaFileManager.RemoveFileMeta(fileMeta.fileName));
    ASSERT_EQ(0, metaFileManager.ListFileMeta(&fileMetas));
    ASSERT_EQ(1, fileMetas.size());

    // Failed to write temporary file
    EXPECT_CALL(*wrapper_, open(_, _, _))
        .WillOnce(Return(1));
    EXPECT_CALL(*wrapper_, pwrite(_, _, _, _))
        .WillOnce(Return(0));
    EXPECT_CALL(*wrapper_, close(_))
    .Times(1);
    ASSERT_EQ(-1, metaFileManager.RemoveFileMeta(fileMeta.fileName));
    ASSERT_EQ(0, metaFileManager.ListFileMeta(&fileMetas));
    ASSERT_EQ(1, fileMetas.size());

    // Rename failed
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

    // Normal situation
    volume[kFileName] = "cbd:volume1";
    volume[kFd] = 1;
    volumes.append(volume);
    volume[kFileName] = "cbd:volume2";
    volume[kFd] = 2;
    root[kVolumes] = volumes;
    FillCrc(&root);
    ASSERT_EQ(0, parser.Parse(root, &fileMetas));

    // Null pointer
    ASSERT_EQ(-1, parser.Parse(root, nullptr));

    // Incorrect crc verification
    root[kCRC] = root[kCRC].asUInt() + 1;
    ASSERT_EQ(-1, parser.Parse(root, &fileMetas));

     // No crc field
    root.removeMember(kCRC);
    ASSERT_EQ(-1, parser.Parse(root, &fileMetas));

    // There is no volumes field or the volumes field is null, and an error should not be reported
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

    // There is no filename in the record 
    volume.clear();
    volumes.clear();
    root.clear();
    volume[kFd] = 1234;
    volumes.append(volume);
    root[kVolumes] = volumes;
    FillCrc(&root);
    ASSERT_EQ(-1, parser.Parse(root, &fileMetas));

    // The record does not contain an 'fd'.
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
    return RUN_ALL_TESTS();
}
