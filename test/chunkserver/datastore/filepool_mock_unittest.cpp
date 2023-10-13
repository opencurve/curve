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
 * Project: curve
 * Created Date: Tuesday July 9th 2019
 * Author: yangyaokai
 */

#include <fcntl.h>
#include <glog/logging.h>
#include <gtest/gtest.h>
#include <json/json.h>

#include <climits>
#include <memory>

#include "include/chunkserver/chunkserver_common.h"
#include "src/chunkserver/datastore/file_pool.h"
#include "src/common/crc32.h"
#include "src/common/curve_define.h"
#include "test/fs/mock_local_filesystem.h"

using ::testing::_;
using ::testing::DoAll;
using ::testing::ElementsAre;
using ::testing::Ge;
using ::testing::Gt;
using ::testing::Invoke;
using ::testing::Matcher;
using ::testing::Mock;
using ::testing::NotNull;
using ::testing::Return;
using ::testing::ReturnArg;
using ::testing::SetArgPointee;
using ::testing::SetArrayArgument;
using ::testing::Truly;

using curve::common::kFilePoolMagic;
using curve::fs::MockLocalFileSystem;

namespace curve {
namespace chunkserver {

const ChunkSizeType CHUNK_SIZE = 16 * 1024 * 1024;
const PageSizeType PAGE_SIZE = 4096;
const uint32_t metaFileSize = 4096;
const uint32_t blockSize = 4096;
const uint32_t fileSize = CHUNK_SIZE + PAGE_SIZE;
const std::string poolDir = "./chunkfilepool_dat";            // NOLINT
const std::string poolMetaPath = "./chunkfilepool_dat.meta";  // NOLINT
const std::string filePath1 = poolDir + "/1";                 // NOLINT
const std::string targetPath = "./data/chunk_1";              // NOLINT
const char* kChunkSize = "chunkSize";
const char* kMetaPageSize = "metaPageSize";
const char* kChunkFilePoolPath = "chunkfilepool_path";
const char* kCRC = "crc";
const char* kBlockSize = "blockSize";

class CSChunkfilePoolMockTest : public testing::Test {
 public:
    void SetUp() { lfs_ = std::make_shared<MockLocalFileSystem>(); }

    void TearDown() {}

    static Json::Value GenerateMetaJson(bool hasBlockSize = false) {
        // JSON format for normal meta files
        FilePoolMeta meta;
        meta.chunkSize = CHUNK_SIZE;
        meta.metaPageSize = PAGE_SIZE;
        meta.hasBlockSize = hasBlockSize;
        if (hasBlockSize) {
            meta.blockSize = blockSize;
        }
        meta.filePoolPath = poolDir;

        Json::Value jsonContent;
        jsonContent[kChunkSize] = CHUNK_SIZE;
        jsonContent[kMetaPageSize] = PAGE_SIZE;

        if (hasBlockSize) {
            jsonContent[kBlockSize] = blockSize;
        }

        jsonContent[kChunkFilePoolPath] = poolDir;
        jsonContent[kCRC] = meta.Crc32();
        return jsonContent;
    }

    void FakeMetaFile() {
        EXPECT_CALL(*lfs_, Open(poolMetaPath, _)).WillOnce(Return(100));
        EXPECT_CALL(*lfs_, Read(100, NotNull(), 0, metaFileSize))
            .WillOnce(Invoke(
                [this](int /*fd*/, char* buf, uint64_t offset, int length) {
                    EXPECT_EQ(offset, 0);
                    EXPECT_EQ(length, metaFileSize);

                    Json::Value root = GenerateMetaJson();
                    auto json = root.toStyledString();
                    strncpy(buf, json.c_str(), json.size() + 1);
                    return metaFileSize;
                }));

        EXPECT_CALL(*lfs_, Close(100)).Times(1);
    }

    void FakePool(FilePool* pool, const FilePoolOptions& options,
                  uint32_t fileNum) {
        if (options.getFileFromPool) {
            FakeMetaFile();
            std::vector<std::string> fileNames;
            struct stat fileInfo;
            fileInfo.st_size = CHUNK_SIZE + PAGE_SIZE;
            for (int i = 1; i <= fileNum; ++i) {
                std::string name = std::to_string(i);
                std::string filePath = poolDir + "/" + name;
                fileNames.push_back(name);
                EXPECT_CALL(*lfs_, FileExists(filePath)).WillOnce(Return(true));
                EXPECT_CALL(*lfs_, Open(filePath, _)).WillOnce(Return(i));
                EXPECT_CALL(*lfs_, Fstat(i, NotNull()))
                    .WillOnce(DoAll(SetArgPointee<1>(fileInfo), Return(0)));
                EXPECT_CALL(*lfs_, Close(i)).Times(1);
            }
            EXPECT_CALL(*lfs_, DirExists(_)).WillOnce(Return(true));
            EXPECT_CALL(*lfs_, List(_, _))
                .WillOnce(DoAll(SetArgPointee<1>(fileNames), Return(0)));

            ASSERT_EQ(true, pool->Initialize(options));
            ASSERT_EQ(fileNum, pool->Size());
        } else {
            EXPECT_CALL(*lfs_, DirExists(_)).WillOnce(Return(true));
            ASSERT_EQ(true, pool->Initialize(options));
        }
    }

 protected:
    std::shared_ptr<MockLocalFileSystem> lfs_;
};

// Exception testing for PersistEnCodeMetaInfo interface
TEST_F(CSChunkfilePoolMockTest, PersistEnCodeMetaInfoTest) {
    FilePoolMeta meta;
    meta.chunkSize = CHUNK_SIZE;
    meta.metaPageSize = PAGE_SIZE;
    meta.hasBlockSize = false;
    meta.filePoolPath = poolDir;

    // open failed
    {
        EXPECT_CALL(*lfs_, Open(poolMetaPath, _)).WillOnce(Return(-1));
        EXPECT_CALL(*lfs_, Write(_, Matcher<const char*>(_), _, _)).Times(0);
        EXPECT_CALL(*lfs_, Close(_)).Times(0);
        ASSERT_EQ(-1, FilePoolHelper::PersistEnCodeMetaInfo(lfs_, meta,
                                                            poolMetaPath));
    }
    // open successful, write failed
    {
        EXPECT_CALL(*lfs_, Open(poolMetaPath, _)).WillOnce(Return(1));
        EXPECT_CALL(*lfs_, Write(1, Matcher<const char*>(NotNull()), 0, 4096))
            .WillOnce(Return(-1));
        EXPECT_CALL(*lfs_, Close(1)).Times(1);
        ASSERT_EQ(-1, FilePoolHelper::PersistEnCodeMetaInfo(lfs_, meta,
                                                            poolMetaPath));
    }
    // open successful, write successful
    {
        EXPECT_CALL(*lfs_, Open(poolMetaPath, _)).WillOnce(Return(1));
        EXPECT_CALL(*lfs_, Write(1, Matcher<const char*>(NotNull()), 0, 4096))
            .WillOnce(Return(4096));
        EXPECT_CALL(*lfs_, Close(1)).Times(1);
        ASSERT_EQ(
            0, FilePoolHelper::PersistEnCodeMetaInfo(lfs_, meta, poolMetaPath));
    }
}

// Exception testing for DecodeMetaInfoFromMetaFile interface
TEST_F(CSChunkfilePoolMockTest, DecodeMetaInfoFromMetaFileTest) {
    FilePoolMeta meta;

    // open failed
    {
        EXPECT_CALL(*lfs_, Open(poolMetaPath, _)).WillOnce(Return(-1));
        EXPECT_CALL(*lfs_, Read(_, _, _, _)).Times(0);
        EXPECT_CALL(*lfs_, Close(_)).Times(0);
        ASSERT_EQ(-1, FilePoolHelper::DecodeMetaInfoFromMetaFile(
                          lfs_, poolMetaPath, metaFileSize, &meta));
    }
    // read failed
    {
        EXPECT_CALL(*lfs_, Open(poolMetaPath, _)).WillOnce(Return(1));
        EXPECT_CALL(*lfs_, Read(1, NotNull(), 0, metaFileSize))
            .WillOnce(Return(-1));
        EXPECT_CALL(*lfs_, Close(1)).Times(1);
        ASSERT_EQ(-1, FilePoolHelper::DecodeMetaInfoFromMetaFile(
                          lfs_, poolMetaPath, metaFileSize, &meta));
    }
    // read successful, parsing Json format failed
    {
        char buf[metaFileSize] = {0};
        EXPECT_CALL(*lfs_, Open(poolMetaPath, _)).WillOnce(Return(1));
        EXPECT_CALL(*lfs_, Read(1, NotNull(), 0, metaFileSize))
            .WillOnce(DoAll(SetArrayArgument<1>(buf, buf + metaFileSize),
                            Return(metaFileSize)));
        EXPECT_CALL(*lfs_, Close(1)).Times(1);
        ASSERT_EQ(-1, FilePoolHelper::DecodeMetaInfoFromMetaFile(
                          lfs_, poolMetaPath, metaFileSize, &meta));
    }
    // parsing Json format succeeded, chunksize is empty
    {
        char buf[metaFileSize] = {0};
        Json::Value root = GenerateMetaJson();
        root.removeMember(kChunkSize);
        memcpy(buf, root.toStyledString().c_str(),
               root.toStyledString().size());

        EXPECT_CALL(*lfs_, Open(poolMetaPath, _)).WillOnce(Return(1));
        EXPECT_CALL(*lfs_, Read(1, NotNull(), 0, metaFileSize))
            .WillOnce(DoAll(SetArrayArgument<1>(buf, buf + metaFileSize),
                            Return(metaFileSize)));
        EXPECT_CALL(*lfs_, Close(1)).Times(1);
        ASSERT_EQ(-1, FilePoolHelper::DecodeMetaInfoFromMetaFile(
                          lfs_, poolMetaPath, metaFileSize, &meta));
    }
    // parsing Json format succeeded, metapagesize is empty
    {
        char buf[metaFileSize] = {0};
        Json::Value root = GenerateMetaJson();
        root.removeMember(kMetaPageSize);
        memcpy(buf, root.toStyledString().c_str(),
               root.toStyledString().size());

        EXPECT_CALL(*lfs_, Open(poolMetaPath, _)).WillOnce(Return(1));
        EXPECT_CALL(*lfs_, Read(1, NotNull(), 0, metaFileSize))
            .WillOnce(DoAll(SetArrayArgument<1>(buf, buf + metaFileSize),
                            Return(metaFileSize)));
        EXPECT_CALL(*lfs_, Close(1)).Times(1);
        ASSERT_EQ(-1, FilePoolHelper::DecodeMetaInfoFromMetaFile(
                          lfs_, poolMetaPath, metaFileSize, &meta));
    }
    // parsing Json format succeeded, kFilePoolPath is empty
    {
        char buf[metaFileSize] = {0};
        Json::Value root = GenerateMetaJson();
        root.removeMember(kChunkFilePoolPath);
        memcpy(buf, root.toStyledString().c_str(),
               root.toStyledString().size());

        EXPECT_CALL(*lfs_, Open(poolMetaPath, _)).WillOnce(Return(1));
        EXPECT_CALL(*lfs_, Read(1, NotNull(), 0, metaFileSize))
            .WillOnce(DoAll(SetArrayArgument<1>(buf, buf + metaFileSize),
                            Return(metaFileSize)));
        EXPECT_CALL(*lfs_, Close(1)).Times(1);
        ASSERT_EQ(-1, FilePoolHelper::DecodeMetaInfoFromMetaFile(
                          lfs_, poolMetaPath, metaFileSize, &meta));
    }
    // Successfully parsed Json format, kCRC is empty
    {
        char buf[metaFileSize] = {0};
        Json::Value root = GenerateMetaJson();
        root.removeMember(kCRC);
        memcpy(buf, root.toStyledString().c_str(),
               root.toStyledString().size());

        EXPECT_CALL(*lfs_, Open(poolMetaPath, _)).WillOnce(Return(1));
        EXPECT_CALL(*lfs_, Read(1, NotNull(), 0, metaFileSize))
            .WillOnce(DoAll(SetArrayArgument<1>(buf, buf + metaFileSize),
                            Return(metaFileSize)));
        EXPECT_CALL(*lfs_, Close(1)).Times(1);
        ASSERT_EQ(-1, FilePoolHelper::DecodeMetaInfoFromMetaFile(
                          lfs_, poolMetaPath, metaFileSize, &meta));
    }
    // Successfully parsed Json format, crc mismatch
    {
        char buf[metaFileSize] = {0};
        Json::Value root = GenerateMetaJson();
        root[kCRC] = 0;
        memcpy(buf, root.toStyledString().c_str(),
               root.toStyledString().size());

        EXPECT_CALL(*lfs_, Open(poolMetaPath, _)).WillOnce(Return(1));
        EXPECT_CALL(*lfs_, Read(1, NotNull(), 0, metaFileSize))
            .WillOnce(DoAll(SetArrayArgument<1>(buf, buf + metaFileSize),
                            Return(metaFileSize)));
        EXPECT_CALL(*lfs_, Close(1)).Times(1);
        ASSERT_EQ(-1, FilePoolHelper::DecodeMetaInfoFromMetaFile(
                          lfs_, poolMetaPath, metaFileSize, &meta));
    }
    // Normal process
    {
        char buf[metaFileSize] = {0};
        Json::Value root = GenerateMetaJson();
        memcpy(buf, root.toStyledString().c_str(),
               root.toStyledString().size());

        EXPECT_CALL(*lfs_, Open(poolMetaPath, _)).WillOnce(Return(1));
        EXPECT_CALL(*lfs_, Read(1, NotNull(), 0, metaFileSize))
            .WillOnce(DoAll(SetArrayArgument<1>(buf, buf + metaFileSize),
                            Return(metaFileSize)));
        EXPECT_CALL(*lfs_, Close(1)).Times(1);
        ASSERT_EQ(0, FilePoolHelper::DecodeMetaInfoFromMetaFile(
                         lfs_, poolMetaPath, metaFileSize, &meta));
    }

    // Normal process
    {
        char buf[metaFileSize] = {0};
        Json::Value root = GenerateMetaJson(true);
        memcpy(buf, root.toStyledString().c_str(),
               root.toStyledString().size());

        EXPECT_CALL(*lfs_, Open(poolMetaPath, _)).WillOnce(Return(1));
        EXPECT_CALL(*lfs_, Read(1, NotNull(), 0, metaFileSize))
            .WillOnce(DoAll(SetArrayArgument<1>(buf, buf + metaFileSize),
                            Return(metaFileSize)));
        EXPECT_CALL(*lfs_, Close(1)).Times(1);
        ASSERT_EQ(0, FilePoolHelper::DecodeMetaInfoFromMetaFile(
                         lfs_, poolMetaPath, metaFileSize, &meta));
    }
}

TEST_F(CSChunkfilePoolMockTest, InitializeTest) {
    // Initialize options
    FilePoolOptions options;
    options.getFileFromPool = true;
    memcpy(options.filePoolDir, poolDir.c_str(), poolDir.size());
    options.fileSize = CHUNK_SIZE;
    options.metaPageSize = PAGE_SIZE;
    memcpy(options.metaPath, poolMetaPath.c_str(), poolMetaPath.size());
    options.metaFileSize = metaFileSize;
    options.retryTimes = 3;

    /****************getFileFromPool is true**************/
    // Failed while checking valid
    {
        // DecodeMetaInfoFromMetaFile has been tested separately on it
        // Here, select a set of uncommon examples from the above to test
        // parsing JSON format failed
        FilePool pool(lfs_);
        char buf[metaFileSize] = {0};
        EXPECT_CALL(*lfs_, Open(poolMetaPath, _)).WillOnce(Return(1));
        EXPECT_CALL(*lfs_, Read(1, NotNull(), 0, metaFileSize))
            .WillOnce(DoAll(SetArrayArgument<1>(buf, buf + metaFileSize),
                            Return(metaFileSize)));
        EXPECT_CALL(*lfs_, Close(1)).Times(1);
        ASSERT_EQ(false, pool.Initialize(options));
    }
    // getFileFromPool is true, checkvalid succeeded, current directory does not
    // exist
    {
        FilePool pool(lfs_);
        FakeMetaFile();
        EXPECT_CALL(*lfs_, DirExists(_)).WillOnce(Return(false));
        ASSERT_EQ(false, pool.Initialize(options));
    }
    // The current directory exists, list directory failed
    {
        FilePool pool(lfs_);
        FakeMetaFile();
        EXPECT_CALL(*lfs_, DirExists(_)).WillOnce(Return(true));
        EXPECT_CALL(*lfs_, List(_, _)).WillOnce(Return(-1));
        ASSERT_EQ(false, pool.Initialize(options));
    }
    // list directory successful, file name contains non numeric characters
    {
        FilePool pool(lfs_);
        FakeMetaFile();
        EXPECT_CALL(*lfs_, DirExists(_)).WillOnce(Return(true));
        std::vector<std::string> fileNames;
        fileNames.push_back("aaa");
        EXPECT_CALL(*lfs_, List(_, _))
            .WillOnce(DoAll(SetArgPointee<1>(fileNames), Return(0)));
        ASSERT_EQ(false, pool.Initialize(options));
    }
    // list directory succeeded, it contains objects of non ordinary file types
    {
        FilePool pool(lfs_);
        FakeMetaFile();
        EXPECT_CALL(*lfs_, DirExists(_)).WillOnce(Return(true));
        std::vector<std::string> fileNames;
        fileNames.push_back("1");
        EXPECT_CALL(*lfs_, List(_, _))
            .WillOnce(DoAll(SetArgPointee<1>(fileNames), Return(0)));
        EXPECT_CALL(*lfs_, FileExists(filePath1)).WillOnce(Return(false));
        ASSERT_EQ(false, pool.Initialize(options));
    }
    // list directory successful, open file failed
    {
        FilePool pool(lfs_);
        FakeMetaFile();
        EXPECT_CALL(*lfs_, DirExists(_)).WillOnce(Return(true));
        std::vector<std::string> fileNames;
        fileNames.push_back("1");
        EXPECT_CALL(*lfs_, List(_, _))
            .WillOnce(DoAll(SetArgPointee<1>(fileNames), Return(0)));
        EXPECT_CALL(*lfs_, FileExists(filePath1)).WillOnce(Return(true));
        EXPECT_CALL(*lfs_, Open(filePath1, _)).WillOnce(Return(-1));
        ASSERT_EQ(false, pool.Initialize(options));
    }
    // Failed to retrieve stat file information
    {
        FilePool pool(lfs_);
        FakeMetaFile();
        EXPECT_CALL(*lfs_, DirExists(_)).WillOnce(Return(true));
        std::vector<std::string> fileNames;
        fileNames.push_back("1");
        EXPECT_CALL(*lfs_, List(_, _))
            .WillOnce(DoAll(SetArgPointee<1>(fileNames), Return(0)));
        EXPECT_CALL(*lfs_, FileExists(filePath1)).WillOnce(Return(true));
        EXPECT_CALL(*lfs_, Open(filePath1, _)).WillOnce(Return(2));
        EXPECT_CALL(*lfs_, Fstat(2, NotNull())).WillOnce(Return(-1));
        EXPECT_CALL(*lfs_, Close(2)).Times(1);
        ASSERT_EQ(false, pool.Initialize(options));
    }
    // stat file information successful, file size mismatch
    {
        FilePool pool(lfs_);
        FakeMetaFile();
        EXPECT_CALL(*lfs_, DirExists(_)).WillOnce(Return(true));
        std::vector<std::string> fileNames;
        fileNames.push_back("1");
        EXPECT_CALL(*lfs_, List(_, _))
            .WillOnce(DoAll(SetArgPointee<1>(fileNames), Return(0)));
        EXPECT_CALL(*lfs_, FileExists(filePath1)).WillOnce(Return(true));
        EXPECT_CALL(*lfs_, Open(filePath1, _)).WillOnce(Return(2));

        struct stat fileInfo;
        fileInfo.st_size = CHUNK_SIZE;
        EXPECT_CALL(*lfs_, Fstat(2, NotNull()))
            .WillOnce(DoAll(SetArgPointee<1>(fileInfo), Return(0)));
        EXPECT_CALL(*lfs_, Close(2)).Times(1);
        ASSERT_EQ(false, pool.Initialize(options));
    }
    // File information matching
    {
        FilePool pool(lfs_);
        FakeMetaFile();
        EXPECT_CALL(*lfs_, DirExists(_)).WillOnce(Return(true));
        std::vector<std::string> fileNames;
        fileNames.push_back("1");
        EXPECT_CALL(*lfs_, List(_, _))
            .WillOnce(DoAll(SetArgPointee<1>(fileNames), Return(0)));
        EXPECT_CALL(*lfs_, FileExists(filePath1)).WillOnce(Return(true));
        EXPECT_CALL(*lfs_, Open(filePath1, _)).WillOnce(Return(2));

        struct stat fileInfo;
        fileInfo.st_size = CHUNK_SIZE + PAGE_SIZE;
        EXPECT_CALL(*lfs_, Fstat(2, NotNull()))
            .WillOnce(DoAll(SetArgPointee<1>(fileInfo), Return(0)));
        EXPECT_CALL(*lfs_, Close(2)).Times(1);
        ASSERT_EQ(true, pool.Initialize(options));
        ASSERT_EQ(1, pool.Size());
    }

    /****************getFileFromPool is false**************/
    options.getFileFromPool = false;
    // The current directory does not exist, creating directory failed
    {
        FilePool pool(lfs_);
        EXPECT_CALL(*lfs_, DirExists(_)).WillOnce(Return(false));
        EXPECT_CALL(*lfs_, Mkdir(_)).WillOnce(Return(-1));
        ASSERT_EQ(false, pool.Initialize(options));
    }
    // The current directory does not exist, creating the directory succeeded
    {
        FilePool pool(lfs_);
        EXPECT_CALL(*lfs_, DirExists(_)).WillOnce(Return(false));
        EXPECT_CALL(*lfs_, Mkdir(_)).WillOnce(Return(0));
        ASSERT_EQ(true, pool.Initialize(options));
    }
    // The current directory exists
    {
        FilePool pool(lfs_);
        EXPECT_CALL(*lfs_, DirExists(_)).WillOnce(Return(true));
        ASSERT_EQ(true, pool.Initialize(options));
    }
}

TEST_F(CSChunkfilePoolMockTest, GetFileTest) {
    // Initialize options
    FilePoolOptions options;
    options.getFileFromPool = true;
    memcpy(options.filePoolDir, poolDir.c_str(), poolDir.size());
    options.fileSize = CHUNK_SIZE;
    options.metaPageSize = PAGE_SIZE;
    memcpy(options.metaPath, poolMetaPath.c_str(), poolMetaPath.size());
    options.metaFileSize = metaFileSize;
    int retryTimes = 3;
    options.retryTimes = retryTimes;

    char metapage[PAGE_SIZE] = {0};

    /****************getFileFromPool is true**************/
    // There is no remaining chunk situation
    {
        FilePool pool(lfs_);
        FakePool(&pool, options, 0);
        ASSERT_EQ(-1, pool.GetFile(targetPath, metapage));
    }
    // Chunk present, open failed
    {
        FilePool pool(lfs_);
        FakePool(&pool, options, 10);
        EXPECT_CALL(*lfs_, Open(_, _))
            .Times(retryTimes)
            .WillRepeatedly(Return(-1));
        EXPECT_CALL(*lfs_, Close(_)).Times(0);
        ASSERT_EQ(-1, pool.GetFile(targetPath, metapage));
        ASSERT_EQ(10 - retryTimes, pool.Size());
    }
    // Chunk exists, write failed
    {
        FilePool pool(lfs_);
        FakePool(&pool, options, 10);
        EXPECT_CALL(*lfs_, Open(_, _))
            .Times(retryTimes)
            .WillRepeatedly(Return(1));
        EXPECT_CALL(*lfs_, Write(1, metapage, 0, PAGE_SIZE))
            .Times(retryTimes)
            .WillRepeatedly(Return(-1));
        EXPECT_CALL(*lfs_, Close(1)).Times(retryTimes);
        ASSERT_EQ(-1, pool.GetFile(targetPath, metapage));
        ASSERT_EQ(10 - retryTimes, pool.Size());
    }
    // Chunk present, fsync failed
    {
        FilePool pool(lfs_);
        FakePool(&pool, options, 10);
        EXPECT_CALL(*lfs_, Open(_, _))
            .Times(retryTimes)
            .WillRepeatedly(Return(1));
        EXPECT_CALL(*lfs_, Write(1, metapage, 0, PAGE_SIZE))
            .Times(retryTimes)
            .WillRepeatedly(Return(PAGE_SIZE));
        EXPECT_CALL(*lfs_, Fsync(1))
            .Times(retryTimes)
            .WillRepeatedly(Return(-1));
        EXPECT_CALL(*lfs_, Close(1)).Times(retryTimes);
        ASSERT_EQ(-1, pool.GetFile(targetPath, metapage));
        ASSERT_EQ(10 - retryTimes, pool.Size());
    }
    // Chunk exists, closing failed
    {
        FilePool pool(lfs_);
        FakePool(&pool, options, 10);
        EXPECT_CALL(*lfs_, Open(_, _))
            .Times(retryTimes)
            .WillRepeatedly(Return(1));
        EXPECT_CALL(*lfs_, Write(1, metapage, 0, PAGE_SIZE))
            .Times(retryTimes)
            .WillRepeatedly(Return(PAGE_SIZE));
        EXPECT_CALL(*lfs_, Fsync(1))
            .Times(retryTimes)
            .WillRepeatedly(Return(0));
        EXPECT_CALL(*lfs_, Close(1))
            .Times(retryTimes)
            .WillRepeatedly(Return(-1));
        ASSERT_EQ(-1, pool.GetFile(targetPath, metapage));
        ASSERT_EQ(10 - retryTimes, pool.Size());
    }
    // Chunk exists, EEXIST error returned when renaming
    {
        FilePool pool(lfs_);
        FakePool(&pool, options, 10);
        EXPECT_CALL(*lfs_, Open(_, _)).WillOnce(Return(1));
        EXPECT_CALL(*lfs_, Write(1, metapage, 0, PAGE_SIZE))
            .WillOnce(Return(PAGE_SIZE));
        EXPECT_CALL(*lfs_, Fsync(1)).WillOnce(Return(0));
        EXPECT_CALL(*lfs_, Close(1)).WillOnce(Return(0));
        EXPECT_CALL(*lfs_, Rename(_, _, _)).WillOnce(Return(-EEXIST));
        ASSERT_EQ(-EEXIST, pool.GetFile(targetPath, metapage));
        ASSERT_EQ(9, pool.Size());
    }
    // Chunk exists, non EEXIST error returned when renaming
    {
        FilePool pool(lfs_);
        FakePool(&pool, options, 10);
        EXPECT_CALL(*lfs_, Open(_, _))
            .Times(retryTimes)
            .WillRepeatedly(Return(1));
        EXPECT_CALL(*lfs_, Write(1, metapage, 0, PAGE_SIZE))
            .Times(retryTimes)
            .WillRepeatedly(Return(PAGE_SIZE));
        EXPECT_CALL(*lfs_, Fsync(1))
            .Times(retryTimes)
            .WillRepeatedly(Return(0));
        EXPECT_CALL(*lfs_, Close(1))
            .Times(retryTimes)
            .WillRepeatedly(Return(0));
        EXPECT_CALL(*lfs_, Rename(_, _, _))
            .Times(retryTimes)
            .WillRepeatedly(Return(-1));
        ASSERT_EQ(-1, pool.GetFile(targetPath, metapage));
        ASSERT_EQ(10 - retryTimes, pool.Size());
    }
    // Chunk exists, rename successful
    {
        FilePool pool(lfs_);
        FakePool(&pool, options, 10);
        EXPECT_CALL(*lfs_, Open(_, _)).WillOnce(Return(1));
        EXPECT_CALL(*lfs_, Write(1, metapage, 0, PAGE_SIZE))
            .WillOnce(Return(PAGE_SIZE));
        EXPECT_CALL(*lfs_, Fsync(1)).WillOnce(Return(0));
        EXPECT_CALL(*lfs_, Close(1)).WillOnce(Return(0));
        EXPECT_CALL(*lfs_, Rename(_, _, _)).WillOnce(Return(0));
        ASSERT_EQ(0, pool.GetFile(targetPath, metapage));
        ASSERT_EQ(9, pool.Size());
    }

    options.getFileFromPool = false;
    /****************getFileFromPool is false**************/
    // Failed on open
    {
        FilePool pool(lfs_);
        FakePool(&pool, options, 0);
        EXPECT_CALL(*lfs_, Open(_, _))
            .Times(retryTimes)
            .WillRepeatedly(Return(-1));
        EXPECT_CALL(*lfs_, Close(1)).Times(0);
        ASSERT_EQ(-1, pool.GetFile(targetPath, metapage));
    }
    // Failed while failing
    {
        FilePool pool(lfs_);
        FakePool(&pool, options, 0);
        EXPECT_CALL(*lfs_, Open(_, _))
            .Times(retryTimes)
            .WillRepeatedly(Return(1));
        EXPECT_CALL(*lfs_, Fallocate(1, 0, 0, fileSize))
            .Times(retryTimes)
            .WillRepeatedly(Return(-1));
        EXPECT_CALL(*lfs_, Close(1)).Times(retryTimes);
        ASSERT_EQ(-1, pool.GetFile(targetPath, metapage));
    }
    // Failed while writing
    {
        FilePool pool(lfs_);
        FakePool(&pool, options, 0);
        EXPECT_CALL(*lfs_, Open(_, _))
            .Times(retryTimes)
            .WillRepeatedly(Return(1));
        EXPECT_CALL(*lfs_, Fallocate(1, 0, 0, fileSize))
            .Times(retryTimes)
            .WillRepeatedly(Return(0));
        EXPECT_CALL(*lfs_,
                    Write(1, Matcher<const char*>(NotNull()), 0, fileSize))
            .Times(retryTimes)
            .WillRepeatedly(Return(-1));
        EXPECT_CALL(*lfs_, Close(1)).Times(retryTimes);
        ASSERT_EQ(-1, pool.GetFile(targetPath, metapage));
    }
    // Fsync failed
    {
        FilePool pool(lfs_);
        FakePool(&pool, options, 0);
        EXPECT_CALL(*lfs_, Open(_, _))
            .Times(retryTimes)
            .WillRepeatedly(Return(1));
        EXPECT_CALL(*lfs_, Fallocate(1, 0, 0, fileSize))
            .Times(retryTimes)
            .WillRepeatedly(Return(0));
        EXPECT_CALL(*lfs_,
                    Write(1, Matcher<const char*>(NotNull()), 0, fileSize))
            .Times(retryTimes)
            .WillRepeatedly(Return(fileSize));
        EXPECT_CALL(*lfs_, Fsync(1))
            .Times(retryTimes)
            .WillRepeatedly(Return(-1));
        EXPECT_CALL(*lfs_, Close(1)).Times(retryTimes);
        ASSERT_EQ(-1, pool.GetFile(targetPath, metapage));
    }
    // Failed to close
    {
        FilePool pool(lfs_);
        FakePool(&pool, options, 0);
        EXPECT_CALL(*lfs_, Open(_, _))
            .Times(retryTimes)
            .WillRepeatedly(Return(1));
        EXPECT_CALL(*lfs_, Fallocate(1, 0, 0, fileSize))
            .Times(retryTimes)
            .WillRepeatedly(Return(0));
        EXPECT_CALL(*lfs_,
                    Write(1, Matcher<const char*>(NotNull()), 0, fileSize))
            .Times(retryTimes)
            .WillRepeatedly(Return(fileSize));
        EXPECT_CALL(*lfs_, Fsync(1))
            .Times(retryTimes)
            .WillRepeatedly(Return(0));
        EXPECT_CALL(*lfs_, Close(1))
            .Times(retryTimes)
            .WillRepeatedly(Return(-1));
        ASSERT_EQ(-1, pool.GetFile(targetPath, metapage));
    }
}

TEST_F(CSChunkfilePoolMockTest, RecycleFileTest) {
    // Initialize options
    FilePoolOptions options;
    options.getFileFromPool = true;
    memcpy(options.filePoolDir, poolDir.c_str(), poolDir.size());
    options.fileSize = CHUNK_SIZE;
    options.metaPageSize = PAGE_SIZE;
    memcpy(options.metaPath, poolMetaPath.c_str(), poolMetaPath.size());
    options.metaFileSize = metaFileSize;
    int retryTimes = 3;
    options.retryTimes = retryTimes;

    /****************getFileFromPool is false**************/
    options.getFileFromPool = false;
    // Failed to delete file
    {
        FilePool pool(lfs_);
        FakePool(&pool, options, 0);
        EXPECT_CALL(*lfs_, Delete(filePath1)).WillOnce(Return(-1));
        ASSERT_EQ(-1, pool.RecycleFile(filePath1));
    }
    // Successfully deleted file
    {
        FilePool pool(lfs_);
        FakePool(&pool, options, 0);
        EXPECT_CALL(*lfs_, Delete(filePath1)).WillOnce(Return(0));
        ASSERT_EQ(0, pool.RecycleFile(filePath1));
    }

    /****************getFileFromPool is true**************/
    options.getFileFromPool = true;
    // open failed
    {
        FilePool pool(lfs_);
        FakePool(&pool, options, 0);

        EXPECT_CALL(*lfs_, Open(targetPath, _)).WillOnce(Return(-1));
        // Failed to delete directly
        EXPECT_CALL(*lfs_, Delete(targetPath)).WillOnce(Return(0));
        // If Delete is successful, return 0
        ASSERT_EQ(0, pool.RecycleFile(targetPath));

        EXPECT_CALL(*lfs_, Open(targetPath, _)).WillOnce(Return(-1));
        // Failed to delete directly
        EXPECT_CALL(*lfs_, Delete(targetPath)).WillOnce(Return(-1));
        // If Delete fails, an error code will be returned
        ASSERT_EQ(-1, pool.RecycleFile(targetPath));
    }

    // Fstat failed
    {
        FilePool pool(lfs_);
        FakePool(&pool, options, 0);

        EXPECT_CALL(*lfs_, Open(targetPath, _)).WillOnce(Return(1));
        EXPECT_CALL(*lfs_, Fstat(1, _)).WillOnce(Return(-1));
        EXPECT_CALL(*lfs_, Close(1)).Times(1);
        // Failed to delete directly
        EXPECT_CALL(*lfs_, Delete(targetPath)).WillOnce(Return(0));
        // If Delete is successful, return 0
        ASSERT_EQ(0, pool.RecycleFile(targetPath));

        EXPECT_CALL(*lfs_, Open(targetPath, _)).WillOnce(Return(1));
        EXPECT_CALL(*lfs_, Fstat(1, _)).WillOnce(Return(-1));
        EXPECT_CALL(*lfs_, Close(1)).Times(1);
        // Failed to delete directly
        EXPECT_CALL(*lfs_, Delete(targetPath)).WillOnce(Return(-1));
        // If Delete fails, an error code will be returned
        ASSERT_EQ(-1, pool.RecycleFile(targetPath));
    }

    // Fstat successful, size mismatch
    {
        FilePool pool(lfs_);
        FakePool(&pool, options, 0);
        struct stat fileInfo;
        fileInfo.st_size = CHUNK_SIZE;

        EXPECT_CALL(*lfs_, Open(targetPath, _)).WillOnce(Return(1));
        EXPECT_CALL(*lfs_, Fstat(1, _))
            .WillOnce(DoAll(SetArgPointee<1>(fileInfo), Return(0)));
        EXPECT_CALL(*lfs_, Close(1)).Times(1);
        // Failed to delete directly
        EXPECT_CALL(*lfs_, Delete(targetPath)).WillOnce(Return(0));
        // If Delete is successful, return 0
        ASSERT_EQ(0, pool.RecycleFile(targetPath));

        EXPECT_CALL(*lfs_, Open(targetPath, _)).WillOnce(Return(1));
        EXPECT_CALL(*lfs_, Fstat(1, _))
            .WillOnce(DoAll(SetArgPointee<1>(fileInfo), Return(0)));
        EXPECT_CALL(*lfs_, Close(1)).Times(1);
        // Failed to delete directly
        EXPECT_CALL(*lfs_, Delete(targetPath)).WillOnce(Return(-1));
        // If Delete fails, an error code will be returned
        ASSERT_EQ(-1, pool.RecycleFile(targetPath));
    }

    // Fstat information matching, rename failed
    {
        FilePool pool(lfs_);
        FakePool(&pool, options, 0);
        struct stat fileInfo;
        fileInfo.st_size = CHUNK_SIZE + PAGE_SIZE;

        EXPECT_CALL(*lfs_, Open(targetPath, _)).WillOnce(Return(1));
        EXPECT_CALL(*lfs_, Fstat(1, _))
            .WillOnce(DoAll(SetArgPointee<1>(fileInfo), Return(0)));
        EXPECT_CALL(*lfs_, Close(1)).Times(1);
        EXPECT_CALL(*lfs_, Rename(_, _, _)).WillOnce(Return(-1));
        ASSERT_EQ(-1, pool.RecycleFile(targetPath));
        ASSERT_EQ(0, pool.Size());
    }

    // Fstat information matching, rename successful
    {
        FilePool pool(lfs_);
        FakePool(&pool, options, 0);
        struct stat fileInfo;
        fileInfo.st_size = CHUNK_SIZE + PAGE_SIZE;

        EXPECT_CALL(*lfs_, Open(targetPath, _)).WillOnce(Return(1));
        EXPECT_CALL(*lfs_, Fstat(1, _))
            .WillOnce(DoAll(SetArgPointee<1>(fileInfo), Return(0)));
        EXPECT_CALL(*lfs_, Close(1)).Times(1);
        EXPECT_CALL(*lfs_, Rename(_, _, _)).WillOnce(Return(0));
        ASSERT_EQ(0, pool.RecycleFile(targetPath));
        ASSERT_EQ(1, pool.Size());
    }
}

}  // namespace chunkserver
}  // namespace curve
