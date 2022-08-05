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

#include <glog/logging.h>
#include <gtest/gtest.h>
#include <json/json.h>
#include <fcntl.h>
#include <climits>
#include <memory>

#include "include/chunkserver/chunkserver_common.h"
#include "src/common/crc32.h"
#include "src/common/curve_define.h"
#include "src/chunkserver/datastore/file_pool.h"
#include "test/fs/mock_local_filesystem.h"

using ::testing::_;
using ::testing::Ge;
using ::testing::Gt;
using ::testing::Return;
using ::testing::NotNull;
using ::testing::Matcher;
using ::testing::Mock;
using ::testing::Truly;
using ::testing::DoAll;
using ::testing::ReturnArg;
using ::testing::ElementsAre;
using ::testing::SetArgPointee;
using ::testing::SetArrayArgument;

using curve::fs::MockLocalFileSystem;
using curve::common::kFilePoolMaigic;

namespace curve {
namespace chunkserver {

const ChunkSizeType CHUNK_SIZE = 16 * 1024 * 1024;
const PageSizeType PAGE_SIZE = 4096;
const uint32_t metaFileSize = 4096;
const uint32_t fileSize = CHUNK_SIZE + PAGE_SIZE;
const std::string poolDir = "./chunkfilepool_dat";  // NOLINT
const std::string poolMetaPath = "./chunkfilepool_dat.meta";  // NOLINT
const std::string filePath1 = poolDir + "/1";  // NOLINT
const std::string targetPath = "./data/chunk_1"; // NOLINT
const char* kChunkSize = "chunkSize";
const char* kMetaPageSize = "metaPageSize";
const char* kChunkFilePoolPath = "chunkfilepool_path";
const char* kCRC = "crc";

class CSChunkfilePoolMockTest : public testing::Test {
 public:
    void SetUp() {
        lfs_ = std::make_shared<MockLocalFileSystem>();
    }

    void TearDown() {}

    Json::Value GenerateMetaJson() {
        // 正常的meta文件的json格式
        uint32_t crcsize = sizeof(kFilePoolMaigic) +
                           sizeof(CHUNK_SIZE) +
                           sizeof(PAGE_SIZE) +
                           poolDir.size();
        char* crcbuf = new char[crcsize];
        ::memcpy(crcbuf, kFilePoolMaigic,
                sizeof(kFilePoolMaigic));
        ::memcpy(crcbuf + sizeof(kFilePoolMaigic),
                &CHUNK_SIZE, sizeof(uint32_t));
        ::memcpy(crcbuf + sizeof(uint32_t) + sizeof(kFilePoolMaigic),
                &PAGE_SIZE, sizeof(uint32_t));
        ::memcpy(crcbuf + 2 * sizeof(uint32_t) + sizeof(kFilePoolMaigic),
                poolDir.c_str(), poolDir.size());
        uint32_t crc = ::curve::common::CRC32(crcbuf, crcsize);
        delete[] crcbuf;

        Json::Value jsonContent;
        jsonContent[kChunkSize] = CHUNK_SIZE;
        jsonContent[kMetaPageSize] = PAGE_SIZE;
        jsonContent[kChunkFilePoolPath] = poolDir;
        jsonContent[kCRC] = crc;
        return jsonContent;
    }

    void FakeMetaFile() {
        char buf[metaFileSize] = {0};
        Json::Value root = GenerateMetaJson();
        memcpy(buf, root.toStyledString().c_str(),
               root.toStyledString().size());

        EXPECT_CALL(*lfs_, Open(poolMetaPath, _))
            .WillOnce(Return(100));
        EXPECT_CALL(*lfs_, Read(100, Matcher<char*>(NotNull()),
            0, metaFileSize))
            .WillOnce(DoAll(SetArrayArgument<1>(buf, buf + metaFileSize),
                            Return(metaFileSize)));
        EXPECT_CALL(*lfs_, Close(100))
            .Times(1);
    }

    void FakePool(FilePool* pool,
                  const FilePoolOptions& options,
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
                EXPECT_CALL(*lfs_, FileExists(filePath))
                    .WillOnce(Return(true));
                EXPECT_CALL(*lfs_, Open(filePath, _))
                    .WillOnce(Return(i));
                EXPECT_CALL(*lfs_, Fstat(i, NotNull()))
                    .WillOnce(DoAll(SetArgPointee<1>(fileInfo),
                                    Return(0)));
                EXPECT_CALL(*lfs_, Close(i))
                    .Times(1);
            }
            EXPECT_CALL(*lfs_, DirExists(_))
                .WillOnce(Return(true));
            EXPECT_CALL(*lfs_, List(_, _))
                .WillOnce(DoAll(SetArgPointee<1>(fileNames),
                                Return(0)));

            ASSERT_EQ(true, pool->Initialize(options));
            ASSERT_EQ(fileNum, pool->Size());
        } else {
            EXPECT_CALL(*lfs_, DirExists(_))
                .WillOnce(Return(true));
            ASSERT_EQ(true, pool->Initialize(options));
        }
    }

 protected:
    std::shared_ptr<MockLocalFileSystem> lfs_;
};

// PersistEnCodeMetaInfo接口的异常测试
TEST_F(CSChunkfilePoolMockTest, PersistEnCodeMetaInfoTest) {
    // open失败
    {
        EXPECT_CALL(*lfs_, Open(poolMetaPath, _))
            .WillOnce(Return(-1));
        EXPECT_CALL(*lfs_, Write(_, Matcher<const char*>(_), _, _))
            .Times(0);
        EXPECT_CALL(*lfs_, Close(_))
            .Times(0);
        ASSERT_EQ(-1,
            FilePoolHelper::PersistEnCodeMetaInfo(lfs_,
                                                       CHUNK_SIZE,
                                                       PAGE_SIZE,
                                                       poolDir,
                                                       poolMetaPath));
    }
    // open成功，write失败
    {
        EXPECT_CALL(*lfs_, Open(poolMetaPath, _))
            .WillOnce(Return(1));
        EXPECT_CALL(*lfs_, Write(1, Matcher<const char*>(NotNull()), 0, 4096))
            .WillOnce(Return(-1));
        EXPECT_CALL(*lfs_, Close(1))
            .Times(1);
        ASSERT_EQ(-1,
            FilePoolHelper::PersistEnCodeMetaInfo(lfs_,
                                                       CHUNK_SIZE,
                                                       PAGE_SIZE,
                                                       poolDir,
                                                       poolMetaPath));
    }
    // open成功，write成功
    {
        EXPECT_CALL(*lfs_, Open(poolMetaPath, _))
            .WillOnce(Return(1));
        EXPECT_CALL(*lfs_, Write(1, Matcher<const char*>(NotNull()), 0, 4096))
            .WillOnce(Return(4096));
        EXPECT_CALL(*lfs_, Close(1))
            .Times(1);
        ASSERT_EQ(0,
            FilePoolHelper::PersistEnCodeMetaInfo(lfs_,
                                                       CHUNK_SIZE,
                                                       PAGE_SIZE,
                                                       poolDir,
                                                       poolMetaPath));
    }
}

// DecodeMetaInfoFromMetaFile接口的异常测试
TEST_F(CSChunkfilePoolMockTest, DecodeMetaInfoFromMetaFileTest) {
    uint32_t chunksize;
    uint32_t metapagesize;
    std::string chunkfilePath;
    // open失败
    {
        EXPECT_CALL(*lfs_, Open(poolMetaPath, _))
            .WillOnce(Return(-1));
        EXPECT_CALL(*lfs_, Read(_, Matcher<char*>(_), _, _))
            .Times(0);
        EXPECT_CALL(*lfs_, Close(_))
            .Times(0);
        ASSERT_EQ(-1,
            FilePoolHelper::DecodeMetaInfoFromMetaFile(lfs_,
                                                            poolMetaPath,
                                                            metaFileSize,
                                                            &chunksize,
                                                            &metapagesize,
                                                            &chunkfilePath));
    }
    // read失败
    {
        EXPECT_CALL(*lfs_, Open(poolMetaPath, _))
            .WillOnce(Return(1));
        EXPECT_CALL(*lfs_, Read(1, Matcher<char*>(NotNull()), 0, metaFileSize))
            .WillOnce(Return(-1));
        EXPECT_CALL(*lfs_, Close(1))
            .Times(1);
        ASSERT_EQ(-1,
            FilePoolHelper::DecodeMetaInfoFromMetaFile(lfs_,
                                                            poolMetaPath,
                                                            metaFileSize,
                                                            &chunksize,
                                                            &metapagesize,
                                                            &chunkfilePath));
    }
    // read成功，解析Json格式失败
    {
        char buf[metaFileSize] = {0};
        EXPECT_CALL(*lfs_, Open(poolMetaPath, _))
            .WillOnce(Return(1));
        EXPECT_CALL(*lfs_, Read(1, Matcher<char*>(NotNull()), 0, metaFileSize))
            .WillOnce(DoAll(SetArrayArgument<1>(buf, buf + metaFileSize),
                            Return(metaFileSize)));
        EXPECT_CALL(*lfs_, Close(1))
            .Times(1);
        ASSERT_EQ(-1,
            FilePoolHelper::DecodeMetaInfoFromMetaFile(lfs_,
                                                            poolMetaPath,
                                                            metaFileSize,
                                                            &chunksize,
                                                            &metapagesize,
                                                            &chunkfilePath));
    }
    // 解析Json格式成功，chunksize为空
    {
        char buf[metaFileSize] = {0};
        Json::Value root = GenerateMetaJson();
        root.removeMember(kChunkSize);
        memcpy(buf, root.toStyledString().c_str(),
               root.toStyledString().size());

        EXPECT_CALL(*lfs_, Open(poolMetaPath, _))
            .WillOnce(Return(1));
        EXPECT_CALL(*lfs_, Read(1, Matcher<char*>(NotNull()), 0, metaFileSize))
            .WillOnce(DoAll(SetArrayArgument<1>(buf, buf + metaFileSize),
                            Return(metaFileSize)));
        EXPECT_CALL(*lfs_, Close(1))
            .Times(1);
        ASSERT_EQ(-1,
            FilePoolHelper::DecodeMetaInfoFromMetaFile(lfs_,
                                                            poolMetaPath,
                                                            metaFileSize,
                                                            &chunksize,
                                                            &metapagesize,
                                                            &chunkfilePath));
    }
    // 解析Json格式成功，metapagesize为空
    {
        char buf[metaFileSize] = {0};
        Json::Value root = GenerateMetaJson();
        root.removeMember(kMetaPageSize);
        memcpy(buf, root.toStyledString().c_str(),
               root.toStyledString().size());

        EXPECT_CALL(*lfs_, Open(poolMetaPath, _))
            .WillOnce(Return(1));
        EXPECT_CALL(*lfs_, Read(1, Matcher<char*>(NotNull()), 0, metaFileSize))
            .WillOnce(DoAll(SetArrayArgument<1>(buf, buf + metaFileSize),
                            Return(metaFileSize)));
        EXPECT_CALL(*lfs_, Close(1))
            .Times(1);
        ASSERT_EQ(-1,
            FilePoolHelper::DecodeMetaInfoFromMetaFile(lfs_,
                                                            poolMetaPath,
                                                            metaFileSize,
                                                            &chunksize,
                                                            &metapagesize,
                                                            &chunkfilePath));
    }
    // 解析Json格式成功，kFilePoolPath为空
    {
        char buf[metaFileSize] = {0};
        Json::Value root = GenerateMetaJson();
        root.removeMember(kChunkFilePoolPath);
        memcpy(buf, root.toStyledString().c_str(),
               root.toStyledString().size());

        EXPECT_CALL(*lfs_, Open(poolMetaPath, _))
            .WillOnce(Return(1));
        EXPECT_CALL(*lfs_, Read(1, Matcher<char*>(NotNull()), 0, metaFileSize))
            .WillOnce(DoAll(SetArrayArgument<1>(buf, buf + metaFileSize),
                            Return(metaFileSize)));
        EXPECT_CALL(*lfs_, Close(1))
            .Times(1);
        ASSERT_EQ(-1,
            FilePoolHelper::DecodeMetaInfoFromMetaFile(lfs_,
                                                            poolMetaPath,
                                                            metaFileSize,
                                                            &chunksize,
                                                            &metapagesize,
                                                            &chunkfilePath));
    }
    // 解析Json格式成功，kCRC为空
    {
        char buf[metaFileSize] = {0};
        Json::Value root = GenerateMetaJson();
        root.removeMember(kCRC);
        memcpy(buf, root.toStyledString().c_str(),
               root.toStyledString().size());

        EXPECT_CALL(*lfs_, Open(poolMetaPath, _))
            .WillOnce(Return(1));
        EXPECT_CALL(*lfs_, Read(1, Matcher<char*>(NotNull()), 0, metaFileSize))
            .WillOnce(DoAll(SetArrayArgument<1>(buf, buf + metaFileSize),
                            Return(metaFileSize)));
        EXPECT_CALL(*lfs_, Close(1))
            .Times(1);
        ASSERT_EQ(-1,
            FilePoolHelper::DecodeMetaInfoFromMetaFile(lfs_,
                                                            poolMetaPath,
                                                            metaFileSize,
                                                            &chunksize,
                                                            &metapagesize,
                                                            &chunkfilePath));
    }
    // 解析Json格式成功，crc不匹配
    {
        char buf[metaFileSize] = {0};
        Json::Value root = GenerateMetaJson();
        root[kCRC] = 0;
        memcpy(buf, root.toStyledString().c_str(),
               root.toStyledString().size());

        EXPECT_CALL(*lfs_, Open(poolMetaPath, _))
            .WillOnce(Return(1));
        EXPECT_CALL(*lfs_, Read(1, Matcher<char*>(NotNull()), 0, metaFileSize))
            .WillOnce(DoAll(SetArrayArgument<1>(buf, buf + metaFileSize),
                            Return(metaFileSize)));
        EXPECT_CALL(*lfs_, Close(1))
            .Times(1);
        ASSERT_EQ(-1,
            FilePoolHelper::DecodeMetaInfoFromMetaFile(lfs_,
                                                            poolMetaPath,
                                                            metaFileSize,
                                                            &chunksize,
                                                            &metapagesize,
                                                            &chunkfilePath));
    }
    // 正常流程
    {
        char buf[metaFileSize] = {0};
        Json::Value root = GenerateMetaJson();
        memcpy(buf, root.toStyledString().c_str(),
               root.toStyledString().size());

        EXPECT_CALL(*lfs_, Open(poolMetaPath, _))
            .WillOnce(Return(1));
        EXPECT_CALL(*lfs_, Read(1, Matcher<char*>(NotNull()), 0, metaFileSize))
            .WillOnce(DoAll(SetArrayArgument<1>(buf, buf + metaFileSize),
                            Return(metaFileSize)));
        EXPECT_CALL(*lfs_, Close(1))
            .Times(1);
        ASSERT_EQ(0,
            FilePoolHelper::DecodeMetaInfoFromMetaFile(lfs_,
                                                            poolMetaPath,
                                                            metaFileSize,
                                                            &chunksize,
                                                            &metapagesize,
                                                            &chunkfilePath));
    }
}

TEST_F(CSChunkfilePoolMockTest, InitializeTest) {
    // 初始化options
    FilePoolOptions options;
    options.getFileFromPool = true;
    memcpy(options.filePoolDir, poolDir.c_str(), poolDir.size());
    options.fileSize = CHUNK_SIZE;
    options.metaPageSize = PAGE_SIZE;
    memcpy(options.metaPath, poolMetaPath.c_str(), poolMetaPath.size());
    options.metaFileSize = metaFileSize;
    options.retryTimes = 3;

    /****************getFileFromPool为true**************/
    // checkvalid时失败
    {
        // DecodeMetaInfoFromMetaFile在上面已经单独测试过了
        // 这里选上面中的一组异常用例来检验即可
        // 解析json格式失败
        FilePool pool(lfs_);
        char buf[metaFileSize] = {0};
        EXPECT_CALL(*lfs_, Open(poolMetaPath, _))
            .WillOnce(Return(1));
        EXPECT_CALL(*lfs_, Read(1, Matcher<char*>(NotNull()), 0, metaFileSize))
            .WillOnce(DoAll(SetArrayArgument<1>(buf, buf + metaFileSize),
                            Return(metaFileSize)));
        EXPECT_CALL(*lfs_, Close(1))
            .Times(1);
        ASSERT_EQ(false, pool.Initialize(options));
    }
    // getFileFromPool为true,checkvalid成功，当前目录不存在
    {
        FilePool pool(lfs_);
        FakeMetaFile();
        EXPECT_CALL(*lfs_, DirExists(_))
            .WillOnce(Return(false));
        ASSERT_EQ(false, pool.Initialize(options));
    }
    // 当前目录存在，list目录失败
    {
        FilePool pool(lfs_);
        FakeMetaFile();
        EXPECT_CALL(*lfs_, DirExists(_))
            .WillOnce(Return(true));
        EXPECT_CALL(*lfs_, List(_, _))
            .WillOnce(Return(-1));
        ASSERT_EQ(false, pool.Initialize(options));
    }
    // list目录成功，文件名中包含非数字字符
    {
        FilePool pool(lfs_);
        FakeMetaFile();
        EXPECT_CALL(*lfs_, DirExists(_))
            .WillOnce(Return(true));
        std::vector<std::string> fileNames;
        fileNames.push_back("aaa");
        EXPECT_CALL(*lfs_, List(_, _))
            .WillOnce(DoAll(SetArgPointee<1>(fileNames),
                            Return(0)));
        ASSERT_EQ(false, pool.Initialize(options));
    }
    // list目录成功，目录中包含非普通文件类型的对象
    {
        FilePool pool(lfs_);
        FakeMetaFile();
        EXPECT_CALL(*lfs_, DirExists(_))
            .WillOnce(Return(true));
        std::vector<std::string> fileNames;
        fileNames.push_back("1");
        EXPECT_CALL(*lfs_, List(_, _))
            .WillOnce(DoAll(SetArgPointee<1>(fileNames),
                            Return(0)));
        EXPECT_CALL(*lfs_, FileExists(filePath1))
            .WillOnce(Return(false));
        ASSERT_EQ(false, pool.Initialize(options));
    }
    // list目录成功，open文件时失败
    {
        FilePool pool(lfs_);
        FakeMetaFile();
        EXPECT_CALL(*lfs_, DirExists(_))
            .WillOnce(Return(true));
        std::vector<std::string> fileNames;
        fileNames.push_back("1");
        EXPECT_CALL(*lfs_, List(_, _))
            .WillOnce(DoAll(SetArgPointee<1>(fileNames),
                            Return(0)));
        EXPECT_CALL(*lfs_, FileExists(filePath1))
            .WillOnce(Return(true));
        EXPECT_CALL(*lfs_, Open(filePath1, _))
            .WillOnce(Return(-1));
        ASSERT_EQ(false, pool.Initialize(options));
    }
    // stat文件信息时失败
    {
        FilePool pool(lfs_);
        FakeMetaFile();
        EXPECT_CALL(*lfs_, DirExists(_))
            .WillOnce(Return(true));
        std::vector<std::string> fileNames;
        fileNames.push_back("1");
        EXPECT_CALL(*lfs_, List(_, _))
            .WillOnce(DoAll(SetArgPointee<1>(fileNames),
                            Return(0)));
        EXPECT_CALL(*lfs_, FileExists(filePath1))
            .WillOnce(Return(true));
        EXPECT_CALL(*lfs_, Open(filePath1, _))
            .WillOnce(Return(2));
        EXPECT_CALL(*lfs_, Fstat(2, NotNull()))
            .WillOnce(Return(-1));
        EXPECT_CALL(*lfs_, Close(2))
            .Times(1);
        ASSERT_EQ(false, pool.Initialize(options));
    }
    // stat文件信息成功，文件大小不匹配
    {
        FilePool pool(lfs_);
        FakeMetaFile();
        EXPECT_CALL(*lfs_, DirExists(_))
            .WillOnce(Return(true));
        std::vector<std::string> fileNames;
        fileNames.push_back("1");
        EXPECT_CALL(*lfs_, List(_, _))
            .WillOnce(DoAll(SetArgPointee<1>(fileNames),
                            Return(0)));
        EXPECT_CALL(*lfs_, FileExists(filePath1))
            .WillOnce(Return(true));
        EXPECT_CALL(*lfs_, Open(filePath1, _))
            .WillOnce(Return(2));

        struct stat fileInfo;
        fileInfo.st_size = CHUNK_SIZE;
        EXPECT_CALL(*lfs_, Fstat(2, NotNull()))
            .WillOnce(DoAll(SetArgPointee<1>(fileInfo),
                            Return(0)));
        EXPECT_CALL(*lfs_, Close(2))
            .Times(1);
        ASSERT_EQ(false, pool.Initialize(options));
    }
    // 文件信息匹配
    {
        FilePool pool(lfs_);
        FakeMetaFile();
        EXPECT_CALL(*lfs_, DirExists(_))
            .WillOnce(Return(true));
        std::vector<std::string> fileNames;
        fileNames.push_back("1");
        EXPECT_CALL(*lfs_, List(_, _))
            .WillOnce(DoAll(SetArgPointee<1>(fileNames),
                            Return(0)));
        EXPECT_CALL(*lfs_, FileExists(filePath1))
            .WillOnce(Return(true));
        EXPECT_CALL(*lfs_, Open(filePath1, _))
            .WillOnce(Return(2));

        struct stat fileInfo;
        fileInfo.st_size = CHUNK_SIZE + PAGE_SIZE;
        EXPECT_CALL(*lfs_, Fstat(2, NotNull()))
            .WillOnce(DoAll(SetArgPointee<1>(fileInfo),
                            Return(0)));
        EXPECT_CALL(*lfs_, Close(2))
            .Times(1);
        ASSERT_EQ(true, pool.Initialize(options));
        ASSERT_EQ(1, pool.Size());
    }

    /****************getFileFromPool为false**************/
    options.getFileFromPool = false;
    // 当前目录不存在，创建目录失败
    {
        FilePool pool(lfs_);
        EXPECT_CALL(*lfs_, DirExists(_))
            .WillOnce(Return(false));
        EXPECT_CALL(*lfs_, Mkdir(_))
            .WillOnce(Return(-1));
        ASSERT_EQ(false, pool.Initialize(options));
    }
    // 当前目录不存在，创建目录成功
    {
        FilePool pool(lfs_);
        EXPECT_CALL(*lfs_, DirExists(_))
            .WillOnce(Return(false));
        EXPECT_CALL(*lfs_, Mkdir(_))
            .WillOnce(Return(0));
        ASSERT_EQ(true, pool.Initialize(options));
    }
    // 当前目录存在
    {
        FilePool pool(lfs_);
        EXPECT_CALL(*lfs_, DirExists(_))
            .WillOnce(Return(true));
        ASSERT_EQ(true, pool.Initialize(options));
    }
}

TEST_F(CSChunkfilePoolMockTest, GetFileTest) {
    // 初始化options
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

    /****************getFileFromPool为true**************/
    // 没有剩余chunk的情况
    {
        FilePool pool(lfs_);
        FakePool(&pool, options, 0);
        ASSERT_EQ(-1, pool.GetFile(targetPath, metapage));
    }
    // 存在chunk，open时失败
    {
        FilePool pool(lfs_);
        FakePool(&pool, options, 10);
        EXPECT_CALL(*lfs_, Open(_, _))
            .Times(retryTimes)
            .WillRepeatedly(Return(-1));
        EXPECT_CALL(*lfs_, Close(_))
            .Times(0);
        ASSERT_EQ(-1, pool.GetFile(targetPath, metapage));
        ASSERT_EQ(10 - retryTimes, pool.Size());
    }
    // 存在chunk，write时失败
    {
        FilePool pool(lfs_);
        FakePool(&pool, options, 10);
        EXPECT_CALL(*lfs_, Open(_, _))
            .Times(retryTimes)
            .WillRepeatedly(Return(1));
        EXPECT_CALL(*lfs_, Write(1, metapage, 0, PAGE_SIZE))
            .Times(retryTimes)
            .WillRepeatedly(Return(-1));
        EXPECT_CALL(*lfs_, Close(1))
            .Times(retryTimes);
        ASSERT_EQ(-1, pool.GetFile(targetPath, metapage));
        ASSERT_EQ(10 - retryTimes, pool.Size());
    }
    // 存在chunk，fsync时失败
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
        EXPECT_CALL(*lfs_, Close(1))
            .Times(retryTimes);
        ASSERT_EQ(-1, pool.GetFile(targetPath, metapage));
        ASSERT_EQ(10 - retryTimes, pool.Size());
    }
    // 存在chunk，close时失败
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
    // 存在chunk，rename时返回EEXIST错误
    {
        FilePool pool(lfs_);
        FakePool(&pool, options, 10);
        EXPECT_CALL(*lfs_, Open(_, _))
            .WillOnce(Return(1));
        EXPECT_CALL(*lfs_, Write(1, metapage, 0, PAGE_SIZE))
            .WillOnce(Return(PAGE_SIZE));
        EXPECT_CALL(*lfs_, Fsync(1))
            .WillOnce(Return(0));
        EXPECT_CALL(*lfs_, Close(1))
            .WillOnce(Return(0));
        EXPECT_CALL(*lfs_, Rename(_, _, _))
            .WillOnce(Return(-EEXIST));
        ASSERT_EQ(-EEXIST, pool.GetFile(targetPath, metapage));
        ASSERT_EQ(9, pool.Size());
    }
    // 存在chunk，rename时返回非EEXIST错误
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
    // 存在chunk，rename成功
    {
        FilePool pool(lfs_);
        FakePool(&pool, options, 10);
        EXPECT_CALL(*lfs_, Open(_, _))
            .WillOnce(Return(1));
        EXPECT_CALL(*lfs_, Write(1, metapage, 0, PAGE_SIZE))
            .WillOnce(Return(PAGE_SIZE));
        EXPECT_CALL(*lfs_, Fsync(1))
            .WillOnce(Return(0));
        EXPECT_CALL(*lfs_, Close(1))
            .WillOnce(Return(0));
        EXPECT_CALL(*lfs_, Rename(_, _, _))
            .WillOnce(Return(0));
        ASSERT_EQ(0, pool.GetFile(targetPath, metapage));
        ASSERT_EQ(9, pool.Size());
    }

    options.getFileFromPool = false;
    /****************getFileFromPool为false**************/
    // open 时失败
    {
        FilePool pool(lfs_);
        FakePool(&pool, options, 0);
        EXPECT_CALL(*lfs_, Open(_, _))
            .Times(retryTimes)
            .WillRepeatedly(Return(-1));
        EXPECT_CALL(*lfs_, Close(1))
            .Times(0);
        ASSERT_EQ(-1, pool.GetFile(targetPath, metapage));
    }
    // fallocate 时失败
    {
        FilePool pool(lfs_);
        FakePool(&pool, options, 0);
        EXPECT_CALL(*lfs_, Open(_, _))
            .Times(retryTimes)
            .WillRepeatedly(Return(1));
        EXPECT_CALL(*lfs_, Fallocate(1, 0, 0, fileSize))
            .Times(retryTimes)
            .WillRepeatedly(Return(-1));
        EXPECT_CALL(*lfs_, Close(1))
            .Times(retryTimes);
        ASSERT_EQ(-1, pool.GetFile(targetPath, metapage));
    }
    // write 时失败
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
        EXPECT_CALL(*lfs_, Close(1))
            .Times(retryTimes);
        ASSERT_EQ(-1, pool.GetFile(targetPath, metapage));
    }
    // fsync 时失败
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
        EXPECT_CALL(*lfs_, Close(1))
            .Times(retryTimes);
        ASSERT_EQ(-1, pool.GetFile(targetPath, metapage));
    }
    // close 时失败
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
    // 初始化options
    FilePoolOptions options;
    options.getFileFromPool = true;
    memcpy(options.filePoolDir, poolDir.c_str(), poolDir.size());
    options.fileSize = CHUNK_SIZE;
    options.metaPageSize = PAGE_SIZE;
    memcpy(options.metaPath, poolMetaPath.c_str(), poolMetaPath.size());
    options.metaFileSize = metaFileSize;
    int retryTimes = 3;
    options.retryTimes = retryTimes;

    /****************getFileFromPool为false**************/
    options.getFileFromPool = false;
    // delete文件时失败
    {
        FilePool pool(lfs_);
        FakePool(&pool, options, 0);
        EXPECT_CALL(*lfs_, Delete(filePath1))
            .WillOnce(Return(-1));
        ASSERT_EQ(-1, pool.RecycleFile(filePath1));
    }
    // delete文件成功
    {
        FilePool pool(lfs_);
        FakePool(&pool, options, 0);
        EXPECT_CALL(*lfs_, Delete(filePath1))
            .WillOnce(Return(0));
        ASSERT_EQ(0, pool.RecycleFile(filePath1));
    }

    /****************getFileFromPool为true**************/
    options.getFileFromPool = true;
    // open失败
    {
        FilePool pool(lfs_);
        FakePool(&pool, options, 0);

        EXPECT_CALL(*lfs_, Open(targetPath, _))
            .WillOnce(Return(-1));
        // 失败直接Delete
        EXPECT_CALL(*lfs_, Delete(targetPath))
            .WillOnce(Return(0));
        // Delete 成功就返回0
        ASSERT_EQ(0, pool.RecycleFile(targetPath));

        EXPECT_CALL(*lfs_, Open(targetPath, _))
            .WillOnce(Return(-1));
        // 失败直接Delete
        EXPECT_CALL(*lfs_, Delete(targetPath))
            .WillOnce(Return(-1));
        // Delete 失败就返回错误码
        ASSERT_EQ(-1, pool.RecycleFile(targetPath));
    }

    // Fstat失败
    {
        FilePool pool(lfs_);
        FakePool(&pool, options, 0);

        EXPECT_CALL(*lfs_, Open(targetPath, _))
            .WillOnce(Return(1));
        EXPECT_CALL(*lfs_, Fstat(1, _))
            .WillOnce(Return(-1));
        EXPECT_CALL(*lfs_, Close(1))
            .Times(1);
        // 失败直接Delete
        EXPECT_CALL(*lfs_, Delete(targetPath))
            .WillOnce(Return(0));
        // Delete 成功就返回0
        ASSERT_EQ(0, pool.RecycleFile(targetPath));

        EXPECT_CALL(*lfs_, Open(targetPath, _))
            .WillOnce(Return(1));
        EXPECT_CALL(*lfs_, Fstat(1, _))
            .WillOnce(Return(-1));
        EXPECT_CALL(*lfs_, Close(1))
            .Times(1);
        // 失败直接Delete
        EXPECT_CALL(*lfs_, Delete(targetPath))
            .WillOnce(Return(-1));
        // Delete 失败就返回错误码
        ASSERT_EQ(-1, pool.RecycleFile(targetPath));
    }

    // Fstat成功，大小不匹配
    {
        FilePool pool(lfs_);
        FakePool(&pool, options, 0);
        struct stat fileInfo;
        fileInfo.st_size = CHUNK_SIZE;

        EXPECT_CALL(*lfs_, Open(targetPath, _))
            .WillOnce(Return(1));
        EXPECT_CALL(*lfs_, Fstat(1, _))
            .WillOnce(DoAll(SetArgPointee<1>(fileInfo),
                            Return(0)));
        EXPECT_CALL(*lfs_, Close(1))
            .Times(1);
        // 失败直接Delete
        EXPECT_CALL(*lfs_, Delete(targetPath))
            .WillOnce(Return(0));
        // Delete 成功就返回0
        ASSERT_EQ(0, pool.RecycleFile(targetPath));

        EXPECT_CALL(*lfs_, Open(targetPath, _))
            .WillOnce(Return(1));
        EXPECT_CALL(*lfs_, Fstat(1, _))
            .WillOnce(DoAll(SetArgPointee<1>(fileInfo),
                            Return(0)));
        EXPECT_CALL(*lfs_, Close(1))
            .Times(1);
        // 失败直接Delete
        EXPECT_CALL(*lfs_, Delete(targetPath))
            .WillOnce(Return(-1));
        // Delete 失败就返回错误码
        ASSERT_EQ(-1, pool.RecycleFile(targetPath));
    }

    // Fstat信息匹配，rename失败
    {
        FilePool pool(lfs_);
        FakePool(&pool, options, 0);
        struct stat fileInfo;
        fileInfo.st_size = CHUNK_SIZE + PAGE_SIZE;

        EXPECT_CALL(*lfs_, Open(targetPath, _))
            .WillOnce(Return(1));
        EXPECT_CALL(*lfs_, Fstat(1, _))
            .WillOnce(DoAll(SetArgPointee<1>(fileInfo),
                            Return(0)));
        EXPECT_CALL(*lfs_, Close(1))
            .Times(1);
        EXPECT_CALL(*lfs_, Rename(_, _, _))
            .WillOnce(Return(-1));
        ASSERT_EQ(-1, pool.RecycleFile(targetPath));
        ASSERT_EQ(0, pool.Size());
    }

    // Fstat信息匹配，rename成功
    {
        FilePool pool(lfs_);
        FakePool(&pool, options, 0);
        struct stat fileInfo;
        fileInfo.st_size = CHUNK_SIZE + PAGE_SIZE;

        EXPECT_CALL(*lfs_, Open(targetPath, _))
            .WillOnce(Return(1));
        EXPECT_CALL(*lfs_, Fstat(1, _))
            .WillOnce(DoAll(SetArgPointee<1>(fileInfo),
                            Return(0)));
        EXPECT_CALL(*lfs_, Close(1))
            .Times(1);
        EXPECT_CALL(*lfs_, Rename(_, _, _))
            .WillOnce(Return(0));
        ASSERT_EQ(0, pool.RecycleFile(targetPath));
        ASSERT_EQ(1, pool.Size());
    }
}

}  // namespace chunkserver
}  // namespace curve
