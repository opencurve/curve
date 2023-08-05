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
 * File Created: Friday, 7th September 2018 8:51:56 am
 * Author: tongguangxun
 */

#include <gtest/gtest.h>
#include <gmock/gmock.h>
#include <string>
#include <memory>
#include <tuple>

#include "include/chunkserver/chunkserver_common.h"
#include "src/common/bitmap.h"
#include "src/common/crc32.h"
#include "src/fs/local_filesystem.h"
#include "src/chunkserver/datastore/define.h"
#include "src/chunkserver/datastore/filename_operator.h"
#include "src/chunkserver/datastore/chunkserver_datastore.h"
#include "test/chunkserver/datastore/mock_file_pool.h"
#include "test/fs/mock_local_filesystem.h"

using curve::fs::LocalFileSystem;
using curve::fs::MockLocalFileSystem;
using curve::common::Bitmap;

using ::testing::_;
using ::testing::Ge;
using ::testing::Gt;
using ::testing::Return;
using ::testing::NotNull;
using ::testing::Matcher;
using ::testing::Mock;
using ::testing::Truly;
using ::testing::Invoke;
using ::testing::DoAll;
using ::testing::ReturnArg;
using ::testing::ElementsAre;
using ::testing::SetArgPointee;
using ::testing::SetArrayArgument;

using std::shared_ptr;
using std::make_shared;
using std::string;

namespace curve {
namespace chunkserver {

const uint32_t kLocationLimit = 3000;
const char baseDir[] = "/home/chunkserver/copyset/data";
const char chunk1[] = "chunk_1";
const char chunk1Path[] = "/home/chunkserver/copyset/data/chunk_1";
const char chunk1snap1[] = "chunk_1_snap_1";
const char chunk1snap1Path[]
    = "/home/chunkserver/copyset/data/chunk_1_snap_1";
const char chunk1snap2[] = "chunk_1_snap_2";
const char chunk1snap2Path[]
    = "/home/chunkserver/copyset/data/chunk_1_snap_2";
const char chunk2[] = "chunk_2";
const char chunk2Path[]
    = "/home/chunkserver/copyset/data/chunk_2";
const char chunk2snap1[] = "chunk_2_snap_1";
const char chunk2snap1Path[]
    = "/home/chunkserver/copyset/data/chunk_2_snap_1";
const char temp1[] = "chunk_1_tmp";
const char temp1Path[]
    = "/home/chunkserver/copyset/data/chunk_1_tmp";
const char location[] = "/file1/0@curve";
const int UT_ERRNO = 1234;

bool hasCreatFlag(int flag) {return flag & O_CREAT;}

ACTION_TEMPLATE(SetVoidArrayArgument,
                HAS_1_TEMPLATE_PARAMS(int, k),
                AND_2_VALUE_PARAMS(first, last)) {
    auto output = reinterpret_cast<char*>(::testing::get<k>(args));
    auto input = first;
    for (; input != last; ++input, ++output) {
        *output = *input;
    }
}

class CSDataStore_test
    : public testing::TestWithParam<
          std::tuple<ChunkSizeType, ChunkSizeType, PageSizeType>> {
 public:
        void SetUp() {
            chunksize_ = std::get<0>(GetParam());
            blocksize_ = std::get<1>(GetParam());
            metapagesize_ = std::get<2>(GetParam());

            chunk1MetaPage = new char[metapagesize_];
            chunk2MetaPage = new char[metapagesize_];
            chunk1SnapMetaPage = new char[metapagesize_];

            lfs_ = std::make_shared<MockLocalFileSystem>();
            fpool_ = std::make_shared<MockFilePool>(lfs_);
            DataStoreOptions options;
            options.baseDir = baseDir;
            options.chunkSize = chunksize_;
            options.blockSize = blocksize_;
            options.metaPageSize = metapagesize_;
            options.locationLimit = kLocationLimit;
            options.enableOdsyncWhenOpenChunkFile = true;
            dataStore = std::make_shared<CSDataStore>(lfs_,
                                                      fpool_,
                                                      options);
            fdMock = 100;
            memset(chunk1MetaPage, 0, metapagesize_);
            memset(chunk2MetaPage, 0, metapagesize_);
            memset(chunk1SnapMetaPage, 0, metapagesize_);
        }

        void TearDown() override {
            delete[] chunk1MetaPage;
            delete[] chunk2MetaPage;
            delete[] chunk1SnapMetaPage;
        }

        inline void FakeEncodeChunk(char* buf,
                                    SequenceNum correctedSn,
                                    SequenceNum sn,
                                    shared_ptr<Bitmap> bitmap = nullptr,
                                    const std::string& location = "") {
            ChunkFileMetaPage metaPage;
            metaPage.version = FORMAT_VERSION;
            metaPage.sn = sn;
            metaPage.correctedSn = correctedSn;
            metaPage.bitmap = bitmap;
            metaPage.location = location;
            metaPage.encode(buf);
        }

        inline void FakeEncodeSnapshot(char* buf,
                                       SequenceNum sn) {
            uint32_t bits = chunksize_ / blocksize_;
            SnapshotMetaPage metaPage;
            metaPage.version = FORMAT_VERSION;
            metaPage.sn = sn;
            metaPage.bitmap = std::make_shared<Bitmap>(bits);
            metaPage.encode(buf);
        }

        /**
         * 构造初始环境
         * datastore存在两个chunk，分别为chunk1、chunk2
         * chunk1 和 chunk2的sn都为2，correctSn为0
         * chunk1存在快照文件，快照文件版本号为1
         * chunk2不存在快照文件
         */
        void FakeEnv() {
            // fake DirExists
            EXPECT_CALL(*lfs_, DirExists(baseDir))
                .WillRepeatedly(Return(true));
            // fake List
            vector<string> fileNames;
            fileNames.push_back(chunk1);
            fileNames.push_back(chunk1snap1);
            fileNames.push_back(chunk2);
            EXPECT_CALL(*lfs_, List(baseDir, NotNull()))
                .WillRepeatedly(DoAll(SetArgPointee<1>(fileNames),
                                Return(0)));
            // fake FileExists
            ON_CALL(*lfs_, FileExists(_))
                .WillByDefault(Return(false));
            EXPECT_CALL(*lfs_, FileExists(chunk1Path))
                .WillRepeatedly(Return(true));
            EXPECT_CALL(*lfs_, FileExists(chunk2Path))
                .WillRepeatedly(Return(true));
            // fake Open
            ON_CALL(*lfs_, Open(_, _))
                .WillByDefault(Return(fdMock++));
            EXPECT_CALL(*lfs_, Open(_, Truly(hasCreatFlag)))
                .Times(0);
            EXPECT_CALL(*lfs_, Open(chunk1Path, _))
                .WillRepeatedly(Return(1));
            EXPECT_CALL(*lfs_, Open(chunk1Path, Truly(hasCreatFlag)))
                .Times(0);
            EXPECT_CALL(*lfs_, Open(chunk1snap1Path, _))
                .WillRepeatedly(Return(2));
            EXPECT_CALL(*lfs_, Open(chunk1snap1Path, Truly(hasCreatFlag)))
                .Times(0);
            EXPECT_CALL(*lfs_, Open(chunk2Path, _))
                .WillRepeatedly(Return(3));
            EXPECT_CALL(*lfs_, Open(chunk2Path, Truly(hasCreatFlag)))
                .Times(0);
            // fake fpool->GetFile()
            ON_CALL(*fpool_, GetFileImpl(_, NotNull()))
                .WillByDefault(Return(0));
            EXPECT_CALL(*fpool_, RecycleFile(_))
                .WillRepeatedly(Return(0));
            // fake Close
            ON_CALL(*lfs_, Close(_))
                .WillByDefault(Return(0));
            // fake Delete
            ON_CALL(*lfs_, Delete(_))
                .WillByDefault(Return(0));
            // fake Fsync
            ON_CALL(*lfs_, Fsync(_))
                .WillByDefault(Return(0));
            // fake Fstat
            struct stat fileInfo;
            fileInfo.st_size = chunksize_ + metapagesize_;
            EXPECT_CALL(*lfs_, Fstat(_, _))
                .WillRepeatedly(DoAll(SetArgPointee<1>(fileInfo),
                                Return(0)));
            // fake Read
            ON_CALL(*lfs_, Read(Ge(1), NotNull(), Ge(0), Gt(0)))
                .WillByDefault(ReturnArg<3>());
            // fake Write
            ON_CALL(*lfs_,
                    Write(Ge(1), Matcher<const char*>(NotNull()), Ge(0), Gt(0)))
                .WillByDefault(ReturnArg<3>());
            ON_CALL(*lfs_, Write(Ge(1), Matcher<butil::IOBuf>(_), Ge(0), Gt(0)))
                .WillByDefault(ReturnArg<3>());
            // fake read chunk1 metapage
            FakeEncodeChunk(chunk1MetaPage, 0, 2);
            EXPECT_CALL(*lfs_, Read(1, NotNull(), 0, metapagesize_))
                .WillRepeatedly(
                    DoAll(SetArrayArgument<1>(chunk1MetaPage,
                                              chunk1MetaPage + metapagesize_),
                          Return(metapagesize_)));
            // fake read chunk1's snapshot1 metapage
            FakeEncodeSnapshot(chunk1SnapMetaPage, 1);
            EXPECT_CALL(*lfs_, Read(2, NotNull(), 0, metapagesize_))
                .WillRepeatedly(DoAll(
                    SetArrayArgument<1>(chunk1SnapMetaPage,
                                        chunk1SnapMetaPage + metapagesize_),
                    Return(metapagesize_)));
            // fake read chunk2 metapage
            FakeEncodeChunk(chunk2MetaPage, 0, 2);
            EXPECT_CALL(*lfs_, Read(3, NotNull(), 0, metapagesize_))
                .WillRepeatedly(
                    DoAll(SetArrayArgument<1>(chunk2MetaPage,
                                              chunk2MetaPage + metapagesize_),
                          Return(metapagesize_)));
        }

 protected:
    int fdMock;
    std::shared_ptr<MockLocalFileSystem> lfs_;
    std::shared_ptr<MockFilePool> fpool_;
    std::shared_ptr<CSDataStore>  dataStore;
    char* chunk1MetaPage;
    char* chunk2MetaPage;
    char* chunk1SnapMetaPage;

    ChunkSizeType chunksize_;
    ChunkSizeType blocksize_;
    PageSizeType metapagesize_;
};
/**
 * ConstructorTest
 * case:测试构造参数为空的情况
 * 预期结果:进程退出
 */
TEST_P(CSDataStore_test, ConstructorTest) {
    // null param test
    DataStoreOptions options;
    options.baseDir = baseDir;
    options.chunkSize = chunksize_;
    options.blockSize = blocksize_;
    options.metaPageSize = metapagesize_;
    ASSERT_DEATH(std::make_shared<CSDataStore>(nullptr,
                                               fpool_,
                                               options),
                                               "");
    ASSERT_DEATH(std::make_shared<CSDataStore>(lfs_,
                                               nullptr,
                                               options),
                                               "");
    options.baseDir = "";
    ASSERT_DEATH(std::make_shared<CSDataStore>(lfs_,
                                               fpool_,
                                               options),
                                               "");
}

/**
 * InitializeTest
 * case:存在未知类型的文件
 * 预期结果:删除该文件，返回true
 */
TEST_P(CSDataStore_test, InitializeTest1) {
    // test unknown file
    EXPECT_CALL(*lfs_, DirExists(baseDir))
        .Times(1)
        .WillOnce(Return(true));
    EXPECT_CALL(*lfs_, Mkdir(baseDir))
        .Times(0);
    vector<string> fileNames;
    fileNames.push_back(temp1);
    EXPECT_CALL(*lfs_, List(baseDir, NotNull()))
        .Times(1)
        .WillOnce(DoAll(SetArgPointee<1>(fileNames),
                        Return(0)));
    // unknown file will be deleted
    EXPECT_TRUE(dataStore->Initialize());
}

/**
 * InitializeTest
 * case:存在快照文件，但是快照文件没有对应的chunk
 * 预期结果:删除快照文件，返回true
 */
TEST_P(CSDataStore_test, InitializeTest2) {
    // test snapshot without chunk
    EXPECT_CALL(*lfs_, DirExists(baseDir))
        .Times(1)
        .WillOnce(Return(true));
    vector<string> fileNames;
    fileNames.push_back(chunk2snap1);
    EXPECT_CALL(*lfs_, List(baseDir, NotNull()))
        .Times(1)
        .WillOnce(DoAll(SetArgPointee<1>(fileNames),
                        Return(0)));
    EXPECT_CALL(*lfs_, FileExists(chunk2Path))
        .WillRepeatedly(Return(false));
    EXPECT_TRUE(dataStore->Initialize());
}

/**
 * InitializeTest
 * case:存在chunk文件，chunk文件存在快照文件
 * 预期结果:正常加载文件，返回true
 */
TEST_P(CSDataStore_test, InitializeTest3) {
    // test chunk with snapshot
    FakeEnv();
    EXPECT_TRUE(dataStore->Initialize());
    EXPECT_CALL(*lfs_, Close(1))
        .Times(1);
    EXPECT_CALL(*lfs_, Close(2))
        .Times(1);
    EXPECT_CALL(*lfs_, Close(3))
        .Times(1);
}

/**
 * InitializeTest
 * case:存在chunk文件，chunk文件存在snapshot文件，
 *      List的时候snapshot先于chunk文件被list
 * 预期结果:返回true
 */
TEST_P(CSDataStore_test, InitializeTest4) {
    // test snapshot founded before chunk file ,
    // but open chunk file failed
    FakeEnv();
    // set snapshotfile before chunk file
    vector<string> fileNames;
    fileNames.push_back(chunk1snap1);
    fileNames.push_back(chunk1);
    EXPECT_CALL(*lfs_, List(baseDir, NotNull()))
        .Times(1)
        .WillOnce(DoAll(SetArgPointee<1>(fileNames),
                        Return(0)));
    EXPECT_TRUE(dataStore->Initialize());
    EXPECT_CALL(*lfs_, Close(1))
        .Times(1);
    EXPECT_CALL(*lfs_, Close(2))
        .Times(1);
}

/**
 * InitializeTest
 * case:存在chunk文件，chunk文件存在两个冲突的快照文件
 * 预期结果:返回false
 */
TEST_P(CSDataStore_test, InitializeTest5) {
    // test snapshot conflict
    FakeEnv();
    vector<string> fileNames;
    fileNames.push_back(chunk1);
    fileNames.push_back(chunk1snap1);
    fileNames.push_back(chunk1snap2);
    EXPECT_CALL(*lfs_, List(baseDir, NotNull()))
        .Times(1)
        .WillOnce(DoAll(SetArgPointee<1>(fileNames),
                        Return(0)));
    EXPECT_FALSE(dataStore->Initialize());
    EXPECT_CALL(*lfs_, Close(1))
        .Times(1);
    EXPECT_CALL(*lfs_, Close(2))
        .Times(1);
}

/**
 * InitializeErrorTest
 * case:data目录不存在，创建目录时失败
 * 预期结果:返回false
 */
TEST_P(CSDataStore_test, InitializeErrorTest1) {
    // dir not exist and mkdir failed
    EXPECT_CALL(*lfs_, DirExists(baseDir))
        .Times(1)
        .WillOnce(Return(false));
    EXPECT_CALL(*lfs_, Mkdir(baseDir))
        .Times(1)
        .WillOnce(Return(-UT_ERRNO));
    // List should not be called
    EXPECT_CALL(*lfs_, List(baseDir, _))
        .Times(0);
    EXPECT_FALSE(dataStore->Initialize());
}

/**
 * InitializeErrorTest
 * case:List目录时失败
 * 预期结果:返回false
 */
TEST_P(CSDataStore_test, InitializeErrorTest2) {
    // List dir failed
    EXPECT_CALL(*lfs_, DirExists(baseDir))
        .Times(1)
        .WillOnce(Return(false));
    EXPECT_CALL(*lfs_, Mkdir(baseDir))
        .Times(1)
        .WillOnce(Return(0));
    // List failed
    EXPECT_CALL(*lfs_, List(baseDir, NotNull()))
        .Times(1)
        .WillOnce(Return(-UT_ERRNO));
    EXPECT_FALSE(dataStore->Initialize());
}

/**
 * InitializeErrorTest
 * case:open chunk文件的时候出错
 * 预期结果:返回false
 */
TEST_P(CSDataStore_test, InitializeErrorTest3) {
    // test chunk open failed
    FakeEnv();
    // set open chunk file failed
    EXPECT_CALL(*lfs_, Open(chunk1Path, _))
        .WillOnce(Return(-UT_ERRNO));
    EXPECT_FALSE(dataStore->Initialize());

    // open success
    EXPECT_CALL(*lfs_, Open(chunk1Path, _))
        .WillOnce(Return(1));
    // expect call close
    EXPECT_CALL(*lfs_, Close(1))
        .Times(1);
    // stat failed
    EXPECT_CALL(*lfs_, Fstat(1, NotNull()))
        .WillOnce(Return(-UT_ERRNO));
    EXPECT_FALSE(dataStore->Initialize());

    // open success
    EXPECT_CALL(*lfs_, Open(chunk1Path, _))
        .WillOnce(Return(1));
    // expect call close
    EXPECT_CALL(*lfs_, Close(1))
        .Times(1);
    // stat success but file size not equal chunksize_ + metapagesize_
    struct stat fileInfo;
    fileInfo.st_size = chunksize_;
    EXPECT_CALL(*lfs_, Fstat(1, NotNull()))
        .WillOnce(DoAll(SetArgPointee<1>(fileInfo),
                        Return(0)));
    EXPECT_FALSE(dataStore->Initialize());

    // open success
    EXPECT_CALL(*lfs_, Open(chunk1Path, _))
        .WillOnce(Return(1));
    // expect call close
    EXPECT_CALL(*lfs_, Close(1))
        .Times(1);
    // stat success
    fileInfo.st_size = chunksize_ + metapagesize_;
    EXPECT_CALL(*lfs_, Fstat(1, NotNull()))
        .WillOnce(DoAll(SetArgPointee<1>(fileInfo),
                        Return(0)));
    // read metapage failed
    EXPECT_CALL(*lfs_, Read(1, NotNull(), 0, metapagesize_))
        .WillOnce(Return(-UT_ERRNO));
    EXPECT_FALSE(dataStore->Initialize());

    // open success
    EXPECT_CALL(*lfs_, Open(chunk1Path, _))
        .WillOnce(Return(1));
    // expect call close
    EXPECT_CALL(*lfs_, Close(1))
        .Times(1);
    // stat success
    fileInfo.st_size = chunksize_ + metapagesize_;
    EXPECT_CALL(*lfs_, Fstat(1, NotNull()))
        .WillOnce(DoAll(SetArgPointee<1>(fileInfo),
                        Return(0)));
    // read metapage success, but version incompatible
    uint8_t version = FORMAT_VERSION + 1;
    memcpy(chunk1MetaPage, &version, sizeof(uint8_t));
    EXPECT_CALL(*lfs_, Read(1, NotNull(), 0, metapagesize_))
                .WillOnce(DoAll(SetArrayArgument<1>(chunk1MetaPage,
                                chunk1MetaPage + metapagesize_),
                                Return(metapagesize_)));
    EXPECT_FALSE(dataStore->Initialize());

    // open success
    EXPECT_CALL(*lfs_, Open(chunk1Path, _))
        .WillOnce(Return(1));
    // expect call close
    EXPECT_CALL(*lfs_, Close(1))
        .Times(1);
    // stat success
    fileInfo.st_size = chunksize_ + metapagesize_;
    EXPECT_CALL(*lfs_, Fstat(1, NotNull()))
        .WillOnce(DoAll(SetArgPointee<1>(fileInfo),
                        Return(0)));
    // read metapage success, but crc check failed
    version = FORMAT_VERSION;
    chunk1MetaPage[1] += 1;  // change the page data
    memcpy(chunk1MetaPage, &version, sizeof(uint8_t));
    EXPECT_CALL(*lfs_, Read(1, NotNull(), 0, metapagesize_))
                .WillOnce(DoAll(SetArrayArgument<1>(chunk1MetaPage,
                                chunk1MetaPage + metapagesize_),
                                Return(metapagesize_)));
    EXPECT_FALSE(dataStore->Initialize());
}

/**
 * InitializeErrorTest
 * case:open 快照文件的时候出错
 * 预期结果:返回false
 */
TEST_P(CSDataStore_test, InitializeErrorTest4) {
    // test chunk open failed
    FakeEnv();
    // set open snapshot file failed
    EXPECT_CALL(*lfs_, Open(chunk1snap1Path, _))
        .WillOnce(Return(-UT_ERRNO));
    EXPECT_FALSE(dataStore->Initialize());

    // 每次重新初始化都会释放原先的资源，重新加载
    EXPECT_CALL(*lfs_, Close(1))
        .WillOnce(Return(0));
    // open success
    EXPECT_CALL(*lfs_, Open(chunk1snap1Path, _))
        .WillOnce(Return(2));
    // expect call close
    EXPECT_CALL(*lfs_, Close(2))
        .Times(1);
    // stat failed
    EXPECT_CALL(*lfs_, Fstat(2, NotNull()))
        .WillOnce(Return(-UT_ERRNO));
    EXPECT_FALSE(dataStore->Initialize());

    // 每次重新初始化都会释放原先的资源，重新加载
    EXPECT_CALL(*lfs_, Close(1))
        .WillOnce(Return(0));
    // open success
    EXPECT_CALL(*lfs_, Open(chunk1snap1Path, _))
        .WillOnce(Return(2));
    // expect call close
    EXPECT_CALL(*lfs_, Close(2))
        .Times(1);
    // stat success but file size not equal chunksize_ + metapagesize_
    struct stat fileInfo;
    fileInfo.st_size = chunksize_;
    EXPECT_CALL(*lfs_, Fstat(2, NotNull()))
        .WillOnce(DoAll(SetArgPointee<1>(fileInfo),
                        Return(0)));
    EXPECT_FALSE(dataStore->Initialize());

    // 每次重新初始化都会释放原先的资源，重新加载
    EXPECT_CALL(*lfs_, Close(1))
        .WillOnce(Return(0));
    // open success
    EXPECT_CALL(*lfs_, Open(chunk1snap1Path, _))
        .WillOnce(Return(2));
    // expect call close
    EXPECT_CALL(*lfs_, Close(2))
        .Times(1);
    // stat success
    fileInfo.st_size = chunksize_ + metapagesize_;
    EXPECT_CALL(*lfs_, Fstat(2, NotNull()))
        .WillOnce(DoAll(SetArgPointee<1>(fileInfo),
                        Return(0)));
    // read metapage failed
    EXPECT_CALL(*lfs_, Read(2, NotNull(), 0, metapagesize_))
        .WillOnce(Return(-UT_ERRNO));
    EXPECT_FALSE(dataStore->Initialize());

    // 每次重新初始化都会释放原先的资源，重新加载
    EXPECT_CALL(*lfs_, Close(1))
        .WillOnce(Return(0));
    // open success
    EXPECT_CALL(*lfs_, Open(chunk1snap1Path, _))
        .WillOnce(Return(2));
    // expect call close
    EXPECT_CALL(*lfs_, Close(2))
        .Times(1);
    // stat success
    fileInfo.st_size = chunksize_ + metapagesize_;
    EXPECT_CALL(*lfs_, Fstat(2, NotNull()))
        .WillOnce(DoAll(SetArgPointee<1>(fileInfo),
                        Return(0)));
    // read metapage success, but version incompatible
    uint8_t version = FORMAT_VERSION + 1;
    memcpy(chunk1SnapMetaPage, &version, sizeof(uint8_t));
    EXPECT_CALL(*lfs_, Read(2, NotNull(), 0, metapagesize_))
                .WillOnce(DoAll(SetArrayArgument<1>(chunk1SnapMetaPage,
                                chunk1SnapMetaPage + metapagesize_),
                                Return(metapagesize_)));
    EXPECT_FALSE(dataStore->Initialize());

    // 每次重新初始化都会释放原先的资源，重新加载
    EXPECT_CALL(*lfs_, Close(1))
        .WillOnce(Return(0));
    // open success
    EXPECT_CALL(*lfs_, Open(chunk1snap1Path, _))
        .WillOnce(Return(2));
    // expect call close
    EXPECT_CALL(*lfs_, Close(2))
        .Times(1);
    // stat success
    fileInfo.st_size = chunksize_ + metapagesize_;
    EXPECT_CALL(*lfs_, Fstat(2, NotNull()))
        .WillOnce(DoAll(SetArgPointee<1>(fileInfo),
                        Return(0)));
    // read metapage success, but crc check failed
    version = FORMAT_VERSION;
    chunk1SnapMetaPage[1] += 1;  // change the page data
    memcpy(chunk1SnapMetaPage, &version, sizeof(uint8_t));
    EXPECT_CALL(*lfs_, Read(2, NotNull(), 0, metapagesize_))
                .WillOnce(DoAll(SetArrayArgument<1>(chunk1SnapMetaPage,
                                chunk1SnapMetaPage + metapagesize_),
                                Return(metapagesize_)));
    EXPECT_FALSE(dataStore->Initialize());

    EXPECT_CALL(*lfs_, Close(1))
        .Times(1);
}

/**
 * InitializeErrorTest
 * case:存在chunk文件，chunk文件存在snapshot文件，
 *      List的时候snapshot先于chunk文件被list
 *      open chunk文件的时候出错
 * 预期结果:返回false
 */
TEST_P(CSDataStore_test, InitializeErrorTest5) {
    // test snapshot founded before chunk file ,
    // but open chunk file failed
    FakeEnv();
    // set snapshotfile before chunk file
    vector<string> fileNames;
    fileNames.push_back(chunk1snap1);
    fileNames.push_back(chunk1);
    EXPECT_CALL(*lfs_, List(baseDir, NotNull()))
        .Times(1)
        .WillOnce(DoAll(SetArgPointee<1>(fileNames),
                        Return(0)));
    // set open chunk file failed
    EXPECT_CALL(*lfs_, Open(chunk1Path, _))
        .WillRepeatedly(Return(-UT_ERRNO));
    EXPECT_FALSE(dataStore->Initialize());
}

/**
 * Test
 * case:chunk 不存在
 * 预期结果:创建chunk文件,并成功写入数据
 */
TEST_P(CSDataStore_test, WriteChunkTest1) {
    // initialize
    FakeEnv();
    EXPECT_TRUE(dataStore->Initialize());

    ChunkID id = 3;
    SequenceNum sn = 1;
    off_t offset = 0;
    size_t length = metapagesize_;
    char* buf = new char[length];
    memset(buf, 0, length);
    // create new chunk and open it
    string chunk3Path = string(baseDir) + "/" +
                        FileNameOperator::GenerateChunkFileName(id);

    // 如果sn为0，返回InvalidArgError
    EXPECT_EQ(CSErrorCode::InvalidArgError, dataStore->WriteChunk(id,
                                                                  0,
                                                                  buf,
                                                                  offset,
                                                                  length,
                                                                  nullptr));
    // expect call chunkfile pool GetFile
    EXPECT_CALL(*lfs_, FileExists(chunk3Path))
        .WillOnce(Return(false));
    EXPECT_CALL(*fpool_, GetFileImpl(chunk3Path, NotNull()))
                .WillOnce(Return(0));
    EXPECT_CALL(*lfs_, Open(chunk3Path, _))
        .Times(1)
        .WillOnce(Return(4));
    // will read metapage
    char chunk3MetaPage[metapagesize_];  // NOLINT
    memset(chunk3MetaPage, 0, sizeof(chunk3MetaPage));
    FakeEncodeChunk(chunk3MetaPage, 0, 1);
    EXPECT_CALL(*lfs_, Read(4, NotNull(), 0, metapagesize_))
        .WillOnce(DoAll(SetArrayArgument<1>(chunk3MetaPage,
                        chunk3MetaPage + metapagesize_),
                        Return(metapagesize_)));
    // will write data
    EXPECT_CALL(*lfs_, Write(4, Matcher<butil::IOBuf>(_),
                             metapagesize_ + offset, length))
        .Times(1);

    EXPECT_EQ(CSErrorCode::Success, dataStore->WriteChunk(id,
                                                          sn,
                                                          buf,
                                                          offset,
                                                          length,
                                                          nullptr));

    EXPECT_CALL(*lfs_, Sync(4))
        .WillOnce(Return(0))
        .WillOnce(Return(-1));

    // sync chunk success
    EXPECT_EQ(CSErrorCode::Success, dataStore->SyncChunk(id));

    // sync chunk failed
    EXPECT_EQ(CSErrorCode::InternalError, dataStore->SyncChunk(id));

    CSChunkInfo info;
    dataStore->GetChunkInfo(id, &info);
    ASSERT_EQ(1, info.curSn);
    ASSERT_EQ(0, info.snapSn);

    EXPECT_CALL(*lfs_, Close(1))
        .Times(1);
    EXPECT_CALL(*lfs_, Close(2))
        .Times(1);
    EXPECT_CALL(*lfs_, Close(3))
        .Times(1);
    EXPECT_CALL(*lfs_, Close(4))
        .Times(1);
    delete[] buf;
}

/**
 * WriteChunkTest
 * case:chunk存在,请求sn小于chunk的sn
 * 预期结果:拒绝写入，返回BackwardRequestError
 */
TEST_P(CSDataStore_test, WriteChunkTest2) {
    // initialize
    FakeEnv();
    // set chunk2's correctedSn as 3
    FakeEncodeChunk(chunk2MetaPage, 2, 4);
    EXPECT_TRUE(dataStore->Initialize());

    ChunkID id = 2;
    SequenceNum sn = 3;
    off_t offset = 0;
    size_t length = metapagesize_;
    char* buf = new char[length];
    memset(buf, 0, length);

    // sn<chunk.sn  sn>chunk.correctedsn
    EXPECT_EQ(CSErrorCode::BackwardRequestError,
              dataStore->WriteChunk(id,
                                    sn,
                                    buf,
                                    offset,
                                    length,
                                    nullptr));

    // sn<chunk.sn  sn==chunk.correctedsn
    sn = 2;
    EXPECT_EQ(CSErrorCode::BackwardRequestError,
              dataStore->WriteChunk(id,
                                    sn,
                                    buf,
                                    offset,
                                    length,
                                    nullptr));

    // sn<chunk.sn  sn<chunk.correctedsn
    sn = 1;
    EXPECT_EQ(CSErrorCode::BackwardRequestError,
              dataStore->WriteChunk(id,
                                    sn,
                                    buf,
                                    offset,
                                    length,
                                    nullptr));

    EXPECT_CALL(*lfs_, Close(1))
        .Times(1);
    EXPECT_CALL(*lfs_, Close(2))
        .Times(1);
    EXPECT_CALL(*lfs_, Close(3))
        .Times(1);
    delete[] buf;
}

/**
 * WriteChunkTest
 * case:chunk存在,请求sn小于chunk的correctedSn
 * 预期结果:拒绝写入，返回BackwardRequestError
 */
TEST_P(CSDataStore_test, WriteChunkTest3) {
    // initialize
    FakeEnv();
    // set chunk2's correctedSn as 3
    FakeEncodeChunk(chunk2MetaPage, 4, 2);
    EXPECT_CALL(*lfs_, Read(3, NotNull(), 0, metapagesize_))
        .WillRepeatedly(DoAll(
                        SetArrayArgument<1>(chunk2MetaPage,
                        chunk2MetaPage + metapagesize_),
                        Return(metapagesize_)));
    EXPECT_TRUE(dataStore->Initialize());

    ChunkID id = 2;
    SequenceNum sn = 3;
    off_t offset = 0;
    size_t length = metapagesize_;
    char* buf = new char[length];
    memset(buf, 0, length);

    // sn>chunk.sn  sn<chunk.correctedsn
    EXPECT_EQ(CSErrorCode::BackwardRequestError,
              dataStore->WriteChunk(id,
                                    sn,
                                    buf,
                                    offset,
                                    length,
                                    nullptr));

    // sn==chunk.sn  sn<chunk.correctedsn
    sn = 2;
    EXPECT_EQ(CSErrorCode::BackwardRequestError,
              dataStore->WriteChunk(id,
                                    sn,
                                    buf,
                                    offset,
                                    length,
                                    nullptr));

    // sn==chunk.sn  sn<chunk.correctedsn
    sn = 1;
    EXPECT_EQ(CSErrorCode::BackwardRequestError,
              dataStore->WriteChunk(id,
                                    sn,
                                    buf,
                                    offset,
                                    length,
                                    nullptr));

    EXPECT_CALL(*lfs_, Close(1))
        .Times(1);
    EXPECT_CALL(*lfs_, Close(2))
        .Times(1);
    EXPECT_CALL(*lfs_, Close(3))
        .Times(1);
    delete[] buf;
}

/**
 * WriteChunkTest
 * case:chunk存在,请求sn等于chunk的sn且不小于correctSn
 *      chunk不存在快照
 * 预期结果:直接写数据到chunk文件
 */
TEST_P(CSDataStore_test, WriteChunkTest4) {
    // initialize
    FakeEnv();
    EXPECT_TRUE(dataStore->Initialize());

    ChunkID id = 2;
    SequenceNum sn = 2;
    off_t offset = 0;
    size_t length = metapagesize_;
    char* buf = new char[length];
    memset(buf, 0, length);

    // will write data
    EXPECT_CALL(*lfs_, Write(3, Matcher<butil::IOBuf>(_),
                             metapagesize_ + offset, length))
        .Times(1);

    EXPECT_EQ(CSErrorCode::Success,
              dataStore->WriteChunk(id,
                                    sn,
                                    buf,
                                    offset,
                                    length,
                                    nullptr));
    CSChunkInfo info;
    dataStore->GetChunkInfo(id, &info);
    ASSERT_EQ(2, info.curSn);
    ASSERT_EQ(0, info.snapSn);

    // return InvalidArgError if offset+length > chunksize_
    offset = chunksize_;
    EXPECT_CALL(*lfs_, Write(3, Matcher<const char*>(NotNull()), _, __amd64))
        .Times(0);
    EXPECT_EQ(CSErrorCode::InvalidArgError,
              dataStore->WriteChunk(id,
                                    sn,
                                    buf,
                                    offset,
                                    length,
                                    nullptr));
    // return InvalidArgError if length not aligned
    offset = blocksize_;
    length = blocksize_ - 1;
    EXPECT_CALL(*lfs_, Write(3, Matcher<const char*>(NotNull()), _, _))
        .Times(0);
    EXPECT_EQ(CSErrorCode::InvalidArgError,
              dataStore->WriteChunk(id,
                                    sn,
                                    buf,
                                    offset,
                                    length,
                                    nullptr));
    // return InvalidArgError if offset not aligned
    offset = blocksize_ + 1;
    length = blocksize_;
    EXPECT_CALL(*lfs_, Write(3, Matcher<const char*>(NotNull()), _, _))
        .Times(0);
    EXPECT_EQ(CSErrorCode::InvalidArgError,
              dataStore->WriteChunk(id,
                                    sn,
                                    buf,
                                    offset,
                                    length,
                                    nullptr));


    EXPECT_CALL(*lfs_, Close(1))
        .Times(1);
    EXPECT_CALL(*lfs_, Close(2))
        .Times(1);
    EXPECT_CALL(*lfs_, Close(3))
        .Times(1);
    delete[] buf;
}

/**
 * WriteChunkTest
 * case:chunk存在,请求sn大于chunk的sn,等于correctSn,
 *      chunk不存在快照
 * 预期结果:会更新metapage，然后写数据到chunk文件
 */
TEST_P(CSDataStore_test, WriteChunkTest6) {
    // initialize
    FakeEnv();
    // set chunk2's correctedSn as 3
    FakeEncodeChunk(chunk2MetaPage, 3, 2);
    EXPECT_CALL(*lfs_, Read(3, NotNull(), 0, metapagesize_))
        .WillRepeatedly(DoAll(
                        SetArrayArgument<1>(chunk2MetaPage,
                        chunk2MetaPage + metapagesize_),
                        Return(metapagesize_)));
    EXPECT_TRUE(dataStore->Initialize());

    ChunkID id = 2;
    SequenceNum sn = 3;
    off_t offset = 0;
    size_t length = blocksize_;
    char* buf = new char[length];
    memset(buf, 0, length);
    // will update metapage
    EXPECT_CALL(*lfs_,
                Write(3, Matcher<const char*>(NotNull()), 0, metapagesize_))
        .Times(1);
    // will write data
    EXPECT_CALL(*lfs_, Write(3, Matcher<butil::IOBuf>(_),
                             metapagesize_ + offset, length))
        .Times(1);

    EXPECT_EQ(CSErrorCode::Success,
              dataStore->WriteChunk(id,
                                    sn,
                                    buf,
                                    offset,
                                    length,
                                    nullptr));
    CSChunkInfo info;
    dataStore->GetChunkInfo(id, &info);
    ASSERT_EQ(3, info.curSn);
    ASSERT_EQ(0, info.snapSn);

    EXPECT_CALL(*lfs_, Close(1))
        .Times(1);
    EXPECT_CALL(*lfs_, Close(2))
        .Times(1);
    EXPECT_CALL(*lfs_, Close(3))
        .Times(1);
    delete[] buf;
}

/**
 * WriteChunkTest
 * case:chunk存在,请求sn大于chunk的sn以及correctSn,
 *      chunk不存在快照、
 * 预期结果:会创建快照文件，更新metapage，
 * 写数据时先cow到snapshot，再写chunk文件
 */
TEST_P(CSDataStore_test, WriteChunkTest7) {
    // initialize
    FakeEnv();
    EXPECT_TRUE(dataStore->Initialize());

    ChunkID id = 2;
    SequenceNum sn = 3;
    off_t offset = 0;
    size_t length = blocksize_;
    char* buf = new char[length];
    memset(buf, 0, length);
    // will Open snapshot file, snap sn equals 2
    string snapPath = string(baseDir) + "/" +
        FileNameOperator::GenerateSnapshotName(id, 2);
    // snapshot not exists
    EXPECT_CALL(*lfs_, FileExists(snapPath))
        .WillOnce(Return(false));
    // expect call chunkfile pool GetFile
    EXPECT_CALL(*fpool_, GetFileImpl(snapPath, NotNull()))
                .WillOnce(Return(0));
    EXPECT_CALL(*lfs_, Open(snapPath, _))
        .WillOnce(Return(4));
    // will read snapshot metapage
    char metapage[metapagesize_];  // NOLINT(runtime/arrays)
    memset(metapage, 0, sizeof(metapage));
    FakeEncodeSnapshot(metapage, 2);
    EXPECT_CALL(*lfs_, Read(4, NotNull(), 0, metapagesize_))
        .WillOnce(DoAll(SetArrayArgument<1>(metapage,
                        metapage + metapagesize_),
                        Return(metapagesize_)));
    // will update metapage
    EXPECT_CALL(*lfs_,
                Write(3, Matcher<const char*>(NotNull()), 0, metapagesize_))
        .Times(1);
    // will copy on write
    EXPECT_CALL(*lfs_, Read(3, NotNull(), metapagesize_ + offset, length))
        .Times(1);
    EXPECT_CALL(*lfs_, Write(4, Matcher<const char*>(NotNull()),
                             metapagesize_ + offset, length))
        .Times(1);
    // will update snapshot metapage
    EXPECT_CALL(*lfs_,
                Write(4, Matcher<const char*>(NotNull()), 0, metapagesize_))
        .Times(1);
    // will write data
    EXPECT_CALL(*lfs_, Write(3, Matcher<butil::IOBuf>(_),
                             metapagesize_ + offset, length))
        .Times(1);

    EXPECT_EQ(CSErrorCode::Success,
              dataStore->WriteChunk(id,
                                    sn,
                                    buf,
                                    offset,
                                    length,
                                    nullptr));
    CSChunkInfo info;
    dataStore->GetChunkInfo(id, &info);
    ASSERT_EQ(3, info.curSn);
    ASSERT_EQ(2, info.snapSn);

    // 再次写同一个block的数据，不再进行cow，而是直接写入数据
    EXPECT_CALL(*lfs_, Write(3, Matcher<butil::IOBuf>(_),
                             metapagesize_ + offset, length))
        .Times(1);
    EXPECT_EQ(CSErrorCode::Success,
              dataStore->WriteChunk(id,
                                    sn,
                                    buf,
                                    offset,
                                    length,
                                    nullptr));

    // sn - 1 < chunk.sn ， 返回 BackwardRequestError
    EXPECT_EQ(CSErrorCode::BackwardRequestError,
              dataStore->WriteChunk(id,
                                    sn - 1,
                                    buf,
                                    offset,
                                    length,
                                    nullptr));

    EXPECT_CALL(*lfs_, Close(1))
        .Times(1);
    EXPECT_CALL(*lfs_, Close(2))
        .Times(1);
    EXPECT_CALL(*lfs_, Close(3))
        .Times(1);
    EXPECT_CALL(*lfs_, Close(4))
        .Times(1);
    delete[] buf;
}

/**
 * WriteChunkTest
 * case:chunk存在,请求sn等于chunk的sn且不小于correctSn
 *      chunk存在快照
 * 预期结果:先cow到snapshot，再写chunk文件
 */
TEST_P(CSDataStore_test, WriteChunkTest9) {
    // initialize
    FakeEnv();
    EXPECT_TRUE(dataStore->Initialize());

    ChunkID id = 1;
    SequenceNum sn = 2;
    off_t offset = 0;
    size_t length = blocksize_;
    char* buf = new char[length];
    memset(buf, 0, length);
    // will not create snapshot
    // will copy on write
    EXPECT_CALL(*lfs_, Read(1, NotNull(), metapagesize_ + offset, length))
        .Times(1);
    EXPECT_CALL(*lfs_, Write(2, Matcher<const char*>(NotNull()),
                             metapagesize_ + offset, length))
        .Times(1);
    // will update snapshot metapage
    EXPECT_CALL(*lfs_,
                Write(2, Matcher<const char*>(NotNull()), 0, metapagesize_))
        .Times(1);
    // will write data
    EXPECT_CALL(*lfs_, Write(1, Matcher<butil::IOBuf>(_),
                             metapagesize_ + offset, length))
        .Times(1);

    EXPECT_EQ(CSErrorCode::Success,
              dataStore->WriteChunk(id,
                                    sn,
                                    buf,
                                    offset,
                                    length,
                                    nullptr));
    CSChunkInfo info;
    dataStore->GetChunkInfo(id, &info);
    ASSERT_EQ(2, info.curSn);
    ASSERT_EQ(1, info.snapSn);

    EXPECT_CALL(*lfs_, Close(1))
        .Times(1);
    EXPECT_CALL(*lfs_, Close(2))
        .Times(1);
    EXPECT_CALL(*lfs_, Close(3))
        .Times(1);
    delete[] buf;
}

/**
 * WriteChunkTest
 * case:chunk存在,请求sn大于chunk的sn，等于correctSn
 *      chunk存在快照
 * 预期结果:更新metapage，然后写chunk文件
 */
TEST_P(CSDataStore_test, WriteChunkTest10) {
    // initialize
    FakeEnv();
    // set chunk1's correctedSn as 3
    FakeEncodeChunk(chunk1MetaPage, 3, 2);
    EXPECT_CALL(*lfs_, Read(1, NotNull(), 0, metapagesize_))
        .WillRepeatedly(DoAll(
                        SetArrayArgument<1>(chunk1MetaPage,
                        chunk1MetaPage + metapagesize_),
                        Return(metapagesize_)));
    EXPECT_TRUE(dataStore->Initialize());

    ChunkID id = 1;
    SequenceNum sn = 3;
    off_t offset = 0;
    size_t length = blocksize_;
    char* buf = new char[length];
    memset(buf, 0, length);
    // will update metapage
    EXPECT_CALL(*lfs_,
                Write(1, Matcher<const char*>(NotNull()), 0, metapagesize_))
        .Times(1);
    // will not cow
    // will write data
    EXPECT_CALL(*lfs_, Write(1, Matcher<butil::IOBuf>(_),
                             metapagesize_ + offset, length))
        .Times(1);

    EXPECT_EQ(CSErrorCode::Success,
              dataStore->WriteChunk(id,
                                    sn,
                                    buf,
                                    offset,
                                    length,
                                    nullptr));
    CSChunkInfo info;
    dataStore->GetChunkInfo(id, &info);
    ASSERT_EQ(3, info.curSn);
    ASSERT_EQ(1, info.snapSn);

    EXPECT_CALL(*lfs_, Close(1))
        .Times(1);
    EXPECT_CALL(*lfs_, Close(2))
        .Times(1);
    EXPECT_CALL(*lfs_, Close(3))
        .Times(1);
    delete[] buf;
}

/**
 * WriteChunkTest
 * case:chunk存在,请求sn大于chunk的sn和correctSn
 *      chunk存在快照,snapsn<chunk.sn
 * 预期结果:存在历史快照未删除，写失败，返回SnapshotConflictError
 */
TEST_P(CSDataStore_test, WriteChunkTest11) {
    // initialize
    FakeEnv();
    // set chunk1's correctedSn as 3
    FakeEncodeChunk(chunk1MetaPage, 3, 2);
    EXPECT_CALL(*lfs_, Read(1, NotNull(), 0, metapagesize_))
        .WillRepeatedly(DoAll(
                        SetArrayArgument<1>(chunk1MetaPage,
                        chunk1MetaPage + metapagesize_),
                        Return(metapagesize_)));
    EXPECT_TRUE(dataStore->Initialize());

    ChunkID id = 1;
    SequenceNum sn = 4;
    off_t offset = 0;
    size_t length = blocksize_;
    char* buf = new char[length];  // NOLINT
    memset(buf, 0, length);

    // sn>chunk.sn, sn>chunk.correctedsn
    EXPECT_EQ(CSErrorCode::SnapshotConflictError,
              dataStore->WriteChunk(id,
                                    sn,
                                    buf,
                                    offset,
                                    length,
                                    nullptr));

    CSChunkInfo info;
    dataStore->GetChunkInfo(id, &info);
    ASSERT_EQ(2, info.curSn);
    ASSERT_EQ(1, info.snapSn);

    EXPECT_CALL(*lfs_, Close(1))
        .Times(1);
    EXPECT_CALL(*lfs_, Close(2))
        .Times(1);
    EXPECT_CALL(*lfs_, Close(3))
        .Times(1);
    delete[] buf;
}

/**
 * WriteChunkTest
 * 写clone chunk，模拟克隆
 * case1:clone chunk存在，写入区域之前未写过
 * 预期结果1:写入数据并更新bitmap
 * case2:clone chunk存在，写入区域之前已写过
 * 预期结果2:写入数据但不会更新bitmap
 * case3:chunk存在，且是clone chunk，部分区域已写过，部分未写过
 * 预期结果3:写入数据并更新bitmap
 * case4:遍写整个chunk
 * 预期结果4:写入数据，然后clone chunk会被转为普通chunk
 */
TEST_P(CSDataStore_test, WriteChunkTest13) {
    // initialize
    FakeEnv();
    EXPECT_TRUE(dataStore->Initialize());

    ChunkID id = 3;
    SequenceNum sn = 1;
    SequenceNum correctedSn = 0;
    off_t offset = 0;
    size_t length = blocksize_;
    char* buf = new char[length];  // NOLINT
    memset(buf, 0, length);
    CSChunkInfo info;
    // 创建 clone chunk
    {
        LOG(INFO) << "case 1";
        char chunk3MetaPage[metapagesize_];  // NOLINT(runtime/arrays)
        memset(chunk3MetaPage, 0, sizeof(chunk3MetaPage));
        shared_ptr<Bitmap> bitmap =
            make_shared<Bitmap>(chunksize_ / blocksize_);
        FakeEncodeChunk(chunk3MetaPage, correctedSn, sn, bitmap, location);
        // create new chunk and open it
        string chunk3Path = string(baseDir) + "/" +
                            FileNameOperator::GenerateChunkFileName(id);
        // expect call chunkfile pool GetFile
        EXPECT_CALL(*lfs_, FileExists(chunk3Path))
            .WillOnce(Return(false));
        EXPECT_CALL(*fpool_, GetFileImpl(chunk3Path, NotNull()))
            .WillOnce(Return(0));
        EXPECT_CALL(*lfs_, Open(chunk3Path, _))
            .Times(1)
            .WillOnce(Return(4));
        // will read metapage
        EXPECT_CALL(*lfs_, Read(4, NotNull(), 0, metapagesize_))
            .WillOnce(DoAll(SetArrayArgument<1>(chunk3MetaPage,
                            chunk3MetaPage + metapagesize_),
                            Return(metapagesize_)));
        EXPECT_EQ(CSErrorCode::Success,
                  dataStore->CreateCloneChunk(id,
                                              sn,
                                              correctedSn,
                                              chunksize_,
                                              location));
    }

    // case1:chunk存在，且是clone chunk，写入区域之前未写过
    {
        LOG(INFO) << "case 2";
        id = 3;  // not exist
        offset = blocksize_;
        length = 2 * blocksize_;
        EXPECT_CALL(*lfs_, Write(4, Matcher<butil::IOBuf>(_),
                                 metapagesize_ + offset, length))
            .Times(1);
        // update metapage
        EXPECT_CALL(*lfs_,
                    Write(4, Matcher<const char*>(NotNull()), 0, metapagesize_))
            .Times(1);

        ASSERT_EQ(CSErrorCode::Success,
                  dataStore->WriteChunk(id,
                                        sn,
                                        buf,
                                        offset,
                                        length,
                                        nullptr));
        // 检查paste后chunk的状态
        ASSERT_EQ(CSErrorCode::Success, dataStore->GetChunkInfo(id, &info));
        ASSERT_EQ(true, info.isClone);
        ASSERT_EQ(1, info.bitmap->NextSetBit(0));
        ASSERT_EQ(3, info.bitmap->NextClearBit(1));
        ASSERT_EQ(Bitmap::NO_POS, info.bitmap->NextSetBit(3));
    }

    // case2:chunk存在，且是clone chunk，写入区域之前已写过
    {
        LOG(INFO) << "case 3";
        id = 3;  // not exist
        offset = blocksize_;
        length = 2 * blocksize_;
        EXPECT_CALL(*lfs_, Write(4, Matcher<butil::IOBuf>(_),
                                 metapagesize_ + offset, length))
            .Times(1);
        EXPECT_CALL(*lfs_,
                    Write(4, Matcher<const char*>(NotNull()), 0, metapagesize_))
            .Times(0);

        ASSERT_EQ(CSErrorCode::Success,
                  dataStore->WriteChunk(id,
                                        sn,
                                        buf,
                                        offset,
                                        length,
                                        nullptr));
        // paste后，chunk的状态不变
        ASSERT_EQ(CSErrorCode::Success, dataStore->GetChunkInfo(id, &info));
        ASSERT_EQ(true, info.isClone);
        ASSERT_EQ(1, info.bitmap->NextSetBit(0));
        ASSERT_EQ(3, info.bitmap->NextClearBit(1));
        ASSERT_EQ(Bitmap::NO_POS, info.bitmap->NextSetBit(3));
    }

    // case3:chunk存在，且是clone chunk，部分区域已写过，部分未写过
    {
        LOG(INFO) << "case 4";
        id = 3;  // not exist
        offset = 0;
        length = 4 * blocksize_;

        std::unique_ptr<char[]> buf(new char[length]);

        // [2 * blocksize_, 4 * blocksize_)区域已写过
        // [0, metapagesize_)为metapage
        EXPECT_CALL(*lfs_, Write(4, Matcher<butil::IOBuf>(_),
                                 offset + metapagesize_, length))
            .Times(1);
        EXPECT_CALL(*lfs_,
                    Write(4, Matcher<const char*>(NotNull()), 0, metapagesize_))
            .Times(1);

        ASSERT_EQ(CSErrorCode::Success,
                  dataStore->WriteChunk(id,
                                        sn,
                                        buf.get(),
                                        offset,
                                        length,
                                        nullptr));
        // paste后，chunk的状态不变
        ASSERT_EQ(CSErrorCode::Success, dataStore->GetChunkInfo(id, &info));
        ASSERT_EQ(true, info.isClone);
        ASSERT_EQ(0, info.bitmap->NextSetBit(0));
        ASSERT_EQ(4, info.bitmap->NextClearBit(0));
        ASSERT_EQ(Bitmap::NO_POS, info.bitmap->NextSetBit(4));
    }

    // case4:遍写整个chunk
    {
        LOG(INFO) << "case 5";
        id = 3;  // not exist
        offset = 0;
        length = chunksize_;

        std::unique_ptr<char[]> buf(new char[length]);

        // [blocksize_, 4 * blocksize_)区域已写过
        // [0, metapagesize_)为metapage
        EXPECT_CALL(*lfs_, Write(4, Matcher<butil::IOBuf>(_),
                                 offset + metapagesize_, length))
            .Times(1);
        EXPECT_CALL(*lfs_,
                    Write(4, Matcher<const char*>(NotNull()), 0, metapagesize_))
            .Times(1);

        ASSERT_EQ(CSErrorCode::Success,
                  dataStore->WriteChunk(id,
                                        sn,
                                        buf.get(),
                                        offset,
                                        length,
                                        nullptr));
        // paste后，chunk的状态不变
        ASSERT_EQ(CSErrorCode::Success, dataStore->GetChunkInfo(id, &info));
        ASSERT_EQ(false, info.isClone);
        ASSERT_EQ(nullptr, info.bitmap);
    }

    EXPECT_CALL(*lfs_, Close(1))
        .Times(1);
    EXPECT_CALL(*lfs_, Close(2))
        .Times(1);
    EXPECT_CALL(*lfs_, Close(3))
        .Times(1);
    EXPECT_CALL(*lfs_, Close(4))
        .Times(1);
    delete[] buf;
}

/**
 * WriteChunkTest
 * 写clone chunk，模拟恢复
 * case1:clone chunk 存在，sn<chunk.sn||sn<chunk.correctedSn
 * 预期结果1:写入数据，然后clone chunk会被转为普通chunk
 * case2:clone chunk存在，sn>chunk.sn,sn==chunk.correctedsn
 * 预期结果2:写入数据并更新bitmap,更新chunk.sn为sn
 * case3:clone chunk存在，sn==chunk.sn,sn==chunk.correctedsn
 * 预期结果3:写入数据并更新bitmap
 * case4:clone chunk 存在，sn>chunk.sn, sn>chunk.correctedsn
 * 预期结果4:返回StatusConflictError
 */
TEST_P(CSDataStore_test, WriteChunkTest14) {
    // initialize
    FakeEnv();
    EXPECT_TRUE(dataStore->Initialize());

    ChunkID id = 3;
    SequenceNum sn = 2;
    SequenceNum correctedSn = 3;
    off_t offset = 0;
    size_t length = blocksize_;
    char* buf = new char[length];  // NOLINT
    memset(buf, 0, length);
    CSChunkInfo info;
    // 创建 clone chunk
    {
        char chunk3MetaPage[metapagesize_];  // NOLINT(runtime/arrays)
        memset(chunk3MetaPage, 0, sizeof(chunk3MetaPage));
        shared_ptr<Bitmap> bitmap =
            make_shared<Bitmap>(chunksize_ / blocksize_);
        FakeEncodeChunk(chunk3MetaPage, correctedSn, sn, bitmap, location);
        // create new chunk and open it
        string chunk3Path = string(baseDir) + "/" +
                            FileNameOperator::GenerateChunkFileName(id);
        // expect call chunkfile pool GetFile
        EXPECT_CALL(*lfs_, FileExists(chunk3Path))
            .WillOnce(Return(false));
        EXPECT_CALL(*fpool_, GetFileImpl(chunk3Path, NotNull()))
            .WillOnce(Return(0));
        EXPECT_CALL(*lfs_, Open(chunk3Path, _))
            .Times(1)
            .WillOnce(Return(4));
        // will read metapage
        EXPECT_CALL(*lfs_, Read(4, NotNull(), 0, metapagesize_))
            .WillOnce(DoAll(SetArrayArgument<1>(chunk3MetaPage,
                            chunk3MetaPage + metapagesize_),
                            Return(metapagesize_)));
        EXPECT_EQ(CSErrorCode::Success,
                  dataStore->CreateCloneChunk(id,
                                              sn,
                                              correctedSn,
                                              chunksize_,
                                              location));
        ASSERT_EQ(CSErrorCode::Success, dataStore->GetChunkInfo(id, &info));
        ASSERT_EQ(2, info.curSn);
        ASSERT_EQ(3, info.correctedSn);
        ASSERT_EQ(0, info.snapSn);
        ASSERT_EQ(true, info.isClone);
        ASSERT_EQ(0, info.bitmap->NextClearBit(0));
        ASSERT_EQ(Bitmap::NO_POS, info.bitmap->NextSetBit(0));
    }

    // case1:clone chunk存在
    {
        LOG(INFO) << "case 1";
        // sn == chunk.sn, sn < chunk.correctedSn
        sn = 2;

        ASSERT_EQ(CSErrorCode::BackwardRequestError,
                  dataStore->WriteChunk(id,
                                        sn,
                                        buf,
                                        offset,
                                        length,
                                        nullptr));

        // sn < chunk.sn, sn < chunk.correctedSn
        sn = 1;
        ASSERT_EQ(CSErrorCode::BackwardRequestError,
                  dataStore->WriteChunk(id,
                                        sn,
                                        buf,
                                        offset,
                                        length,
                                        nullptr));
    }

    // case2:chunk存在，且是clone chunk，
    {
        LOG(INFO) << "case 2";
        id = 3;
        offset = blocksize_;
        length = 2 * blocksize_;
        sn = 3;  // sn > chunk.sn;sn == correctedsn
        EXPECT_CALL(*lfs_, Write(4, Matcher<butil::IOBuf>(_),
                                 metapagesize_ + offset, length))
            .Times(1);
        // update metapage
        EXPECT_CALL(*lfs_,
                    Write(4, Matcher<const char*>(NotNull()), 0, metapagesize_))
            .Times(2);

        ASSERT_EQ(CSErrorCode::Success,
                  dataStore->WriteChunk(id,
                                        sn,
                                        buf,
                                        offset,
                                        length,
                                        nullptr));
        // 检查paste后chunk的状态
        ASSERT_EQ(CSErrorCode::Success, dataStore->GetChunkInfo(id, &info));
        ASSERT_EQ(3, info.curSn);
        ASSERT_EQ(3, info.correctedSn);
        ASSERT_EQ(0, info.snapSn);
        ASSERT_EQ(true, info.isClone);
        ASSERT_EQ(1, info.bitmap->NextSetBit(0));
        ASSERT_EQ(3, info.bitmap->NextClearBit(1));
        ASSERT_EQ(Bitmap::NO_POS, info.bitmap->NextSetBit(3));
    }

    // case3:chunk存在，且是clone chunk
    // sn > chunk.sn;sn == correctedsn
    {
        LOG(INFO) << "case 3";
        offset = 0;
        length = 4 * blocksize_;

        std::unique_ptr<char[]> buf(new char[length]);

        // [2 * blocksize_, 4 * blocksize_)区域已写过
        // [0, blocksize_)为metapage
        EXPECT_CALL(*lfs_, Write(4, Matcher<butil::IOBuf>(_),
                                 offset + metapagesize_, length))
            .Times(1);
        EXPECT_CALL(*lfs_,
                    Write(4, Matcher<const char*>(NotNull()), 0, metapagesize_))
            .Times(1);

        ASSERT_EQ(CSErrorCode::Success,
                  dataStore->WriteChunk(id,
                                        sn,
                                        buf.get(),
                                        offset,
                                        length,
                                        nullptr));
        // paste后，chunk的状态不变
        ASSERT_EQ(CSErrorCode::Success, dataStore->GetChunkInfo(id, &info));
        ASSERT_EQ(3, info.curSn);
        ASSERT_EQ(3, info.correctedSn);
        ASSERT_EQ(0, info.snapSn);
        ASSERT_EQ(true, info.isClone);
        ASSERT_EQ(0, info.bitmap->NextSetBit(0));
        ASSERT_EQ(4, info.bitmap->NextClearBit(0));
        ASSERT_EQ(Bitmap::NO_POS, info.bitmap->NextSetBit(4));
    }

    // case3:chunk存在，且是clone chunk
    // sn > chunk.sn;sn > correctedsn
    {
        LOG(INFO) << "case 4";
        sn = 4;
        // 不会写数据
        EXPECT_CALL(*lfs_, Write(4, Matcher<butil::IOBuf>(_), _, _))
            .Times(0);

        std::unique_ptr<char[]> buf(new char[length]);

        ASSERT_EQ(CSErrorCode::StatusConflictError,
                  dataStore->WriteChunk(id,
                                        sn,
                                        buf.get(),
                                        offset,
                                        length,
                                        nullptr));
        // chunk的状态不变
        ASSERT_EQ(CSErrorCode::Success, dataStore->GetChunkInfo(id, &info));
        ASSERT_EQ(3, info.curSn);
        ASSERT_EQ(3, info.correctedSn);
        ASSERT_EQ(0, info.snapSn);
        ASSERT_EQ(true, info.isClone);
        ASSERT_EQ(0, info.bitmap->NextSetBit(0));
        ASSERT_EQ(4, info.bitmap->NextClearBit(0));
        ASSERT_EQ(Bitmap::NO_POS, info.bitmap->NextSetBit(4));
    }

    EXPECT_CALL(*lfs_, Close(1))
        .Times(1);
    EXPECT_CALL(*lfs_, Close(2))
        .Times(1);
    EXPECT_CALL(*lfs_, Close(3))
        .Times(1);
    EXPECT_CALL(*lfs_, Close(4))
        .Times(1);
    delete[] buf;
}

/**
 * WriteChunkTest
 * case:chunk存在,
 *      sn==chunk.sn
 *      sn>chunk.correctedSn
 *      chunk.sn<snap.sn
 *      chunk存在快照
 * 预期结果:先cow到snapshot，再写chunk文件
 */
TEST_P(CSDataStore_test, WriteChunkTest15) {
    // initialize
    FakeEnv();
    // fake read chunk1 metapage
    FakeEncodeChunk(chunk1MetaPage, 0, 2);
    EXPECT_CALL(*lfs_, Read(1, NotNull(), 0, metapagesize_))
        .WillRepeatedly(DoAll(
                        SetArrayArgument<1>(chunk1MetaPage,
                        chunk1MetaPage + metapagesize_),
                        Return(metapagesize_)));
    // fake read chunk1's snapshot1 metapage,chunk.sn<snap.sn
    FakeEncodeSnapshot(chunk1SnapMetaPage, 3);
    EXPECT_CALL(*lfs_, Read(2, NotNull(), 0, metapagesize_))
        .WillRepeatedly(DoAll(
                        SetArrayArgument<1>(chunk1SnapMetaPage,
                        chunk1SnapMetaPage + metapagesize_),
                        Return(metapagesize_)));
    EXPECT_TRUE(dataStore->Initialize());

    ChunkID id = 1;
    SequenceNum sn = 2;
    off_t offset = 0;
    size_t length = blocksize_;
    char* buf = new char[length];  // NOLINT
    memset(buf, 0, length);
    // will not create snapshot
    // will not copy on write
    EXPECT_CALL(*lfs_, Write(2, Matcher<const char*>(NotNull()), _, _))
        .Times(0);
    // will write data
    EXPECT_CALL(*lfs_, Write(1, Matcher<butil::IOBuf>(_),
                             metapagesize_ + offset, length))
        .Times(1);

    EXPECT_EQ(CSErrorCode::Success,
              dataStore->WriteChunk(id,
                                    sn,
                                    buf,
                                    offset,
                                    length,
                                    nullptr));

    EXPECT_CALL(*lfs_, Close(1))
        .Times(1);
    EXPECT_CALL(*lfs_, Close(2))
        .Times(1);
    EXPECT_CALL(*lfs_, Close(3))
        .Times(1);
    delete[] buf;
}

/**
 * WriteChunkTest
 * case:chunk存在,
 *      sn>chunk.sn
 *      sn>chunk.correctedSn
 *      chunk.sn==snap.sn
 *      chunk存在快照
 * 预期结果:先cow到snapshot，再写chunk文件
 */
TEST_P(CSDataStore_test, WriteChunkTest16) {
    // initialize
    FakeEnv();
    // fake read chunk1 metapage
    FakeEncodeChunk(chunk1MetaPage, 0, 2);
    EXPECT_CALL(*lfs_, Read(1, NotNull(), 0, metapagesize_))
        .WillRepeatedly(DoAll(
                        SetArrayArgument<1>(chunk1MetaPage,
                        chunk1MetaPage + metapagesize_),
                        Return(metapagesize_)));
    // fake read chunk1's snapshot1 metapage
    FakeEncodeSnapshot(chunk1SnapMetaPage, 3);
    EXPECT_CALL(*lfs_, Read(2, NotNull(), 0, metapagesize_))
        .WillRepeatedly(DoAll(
                        SetArrayArgument<1>(chunk1SnapMetaPage,
                        chunk1SnapMetaPage + metapagesize_),
                        Return(metapagesize_)));
    EXPECT_TRUE(dataStore->Initialize());

    ChunkID id = 1;
    SequenceNum sn = 3;
    off_t offset = 0;
    size_t length = blocksize_;
    char* buf = new char[length];
    memset(buf, 0, length);
    // will not create snapshot
    // will not copy on write
    EXPECT_CALL(*lfs_, Write(2, Matcher<const char*>(NotNull()), _, _))
        .Times(0);
    // will update sn
    EXPECT_CALL(*lfs_,
                Write(1, Matcher<const char*>(NotNull()), 0, metapagesize_))
        .Times(1);
    // will write data
    EXPECT_CALL(*lfs_, Write(1, Matcher<butil::IOBuf>(_),
                             metapagesize_ + offset, length))
        .Times(1);

    EXPECT_EQ(CSErrorCode::Success,
              dataStore->WriteChunk(id,
                                    sn,
                                    buf,
                                    offset,
                                    length,
                                    nullptr));

    EXPECT_CALL(*lfs_, Close(1))
        .Times(1);
    EXPECT_CALL(*lfs_, Close(2))
        .Times(1);
    EXPECT_CALL(*lfs_, Close(3))
        .Times(1);
    delete[] buf;
}

/**
 * WriteChunkTest 异常测试
 * case:创建快照文件时出错
 * 预期结果:写失败，不会改变当前chunk状态
 */
TEST_P(CSDataStore_test, WriteChunkErrorTest1) {
    // initialize
    FakeEnv();
    EXPECT_TRUE(dataStore->Initialize());

    ChunkID id = 2;
    SequenceNum sn = 3;
    off_t offset = 0;
    size_t length = blocksize_;
    char* buf = new char[length];  // NOLINT
    memset(buf, 0, length);
    string snapPath = string(baseDir) + "/" +
        FileNameOperator::GenerateSnapshotName(id, 2);

    // getchunk failed
    EXPECT_CALL(*lfs_, FileExists(snapPath))
        .WillOnce(Return(false));
    EXPECT_CALL(*fpool_, GetFileImpl(snapPath, NotNull()))
        .WillOnce(Return(-UT_ERRNO));

    EXPECT_EQ(CSErrorCode::InternalError,
              dataStore->WriteChunk(id,
                                    sn,
                                    buf,
                                    offset,
                                    length,
                                    nullptr));
    CSChunkInfo info;
    dataStore->GetChunkInfo(id, &info);
    ASSERT_EQ(2, info.curSn);
    ASSERT_EQ(0, info.snapSn);

    // expect call chunkfile pool GetFile
    EXPECT_CALL(*lfs_, FileExists(snapPath))
        .WillOnce(Return(false));
    EXPECT_CALL(*fpool_, GetFileImpl(snapPath, NotNull()))
        .WillOnce(Return(0));
    // open snapshot failed
    EXPECT_CALL(*lfs_, Open(snapPath, _))
        .WillOnce(Return(-UT_ERRNO));
    EXPECT_EQ(CSErrorCode::InternalError,
              dataStore->WriteChunk(id,
                                    sn,
                                    buf,
                                    offset,
                                    length,
                                    nullptr));
    dataStore->GetChunkInfo(id, &info);
    ASSERT_EQ(2, info.curSn);
    ASSERT_EQ(0, info.snapSn);

    // open success but read snapshot metapage failed
    EXPECT_CALL(*lfs_, FileExists(snapPath))
        .WillOnce(Return(true));
    EXPECT_CALL(*lfs_, Open(snapPath, _))
        .WillOnce(Return(4));
    EXPECT_CALL(*lfs_, Read(4, NotNull(), 0, metapagesize_))
        .WillOnce(Return(-UT_ERRNO));
    EXPECT_CALL(*lfs_, Close(4))
        .Times(1);
    EXPECT_EQ(CSErrorCode::InternalError,
              dataStore->WriteChunk(id,
                                    sn,
                                    buf,
                                    offset,
                                    length,
                                    nullptr));
    dataStore->GetChunkInfo(id, &info);
    ASSERT_EQ(2, info.curSn);
    ASSERT_EQ(0, info.snapSn);

    EXPECT_CALL(*lfs_, Close(1))
        .Times(1);
    EXPECT_CALL(*lfs_, Close(2))
        .Times(1);
    EXPECT_CALL(*lfs_, Close(3))
        .Times(1);
    delete[] buf;
}

/**
 * WriteChunkTest 异常测试
 * case:创建快照文件成功，更新metapage失败
 * 预期结果:写失败，产生快照文件，但是chunk版本号不会改变
 * 再次写入，不会生成新的快照文件
 */
TEST_P(CSDataStore_test, WriteChunkErrorTest2) {
    // initialize
    FakeEnv();
    EXPECT_TRUE(dataStore->Initialize());

    ChunkID id = 2;
    SequenceNum sn = 3;
    off_t offset = 0;
    size_t length = blocksize_;
    char* buf = new char[length];
    memset(buf, 0, length);
    // will Open snapshot file, snap sn equals 2
    string snapPath = string(baseDir) + "/" +
        FileNameOperator::GenerateSnapshotName(id, 2);
    // expect call chunk file pool GetFile
    EXPECT_CALL(*lfs_, FileExists(snapPath))
        .WillOnce(Return(false));
    EXPECT_CALL(*fpool_, GetFileImpl(snapPath, NotNull()))
        .WillOnce(Return(0));
    EXPECT_CALL(*lfs_, Open(snapPath, _))
        .WillOnce(Return(4));
    // will read snapshot metapage
    char metapage[metapagesize_];  // NOLINT(runtime/arrays)
    memset(metapage, 0, sizeof(metapage));
    FakeEncodeSnapshot(metapage, 2);
    EXPECT_CALL(*lfs_, Read(4, NotNull(), 0, metapagesize_))
        .WillOnce(DoAll(SetArrayArgument<1>(metapage,
                        metapage + metapagesize_),
                        Return(metapagesize_)));
    // write chunk metapage failed
    EXPECT_CALL(*lfs_,
                Write(3, Matcher<const char*>(NotNull()), 0, metapagesize_))
        .WillOnce(Return(-UT_ERRNO));

    EXPECT_EQ(CSErrorCode::InternalError,
              dataStore->WriteChunk(id,
                                    sn,
                                    buf,
                                    offset,
                                    length,
                                    nullptr));
    CSChunkInfo info;
    dataStore->GetChunkInfo(id, &info);
    // chunk sn not changed
    ASSERT_EQ(2, info.curSn);
    ASSERT_EQ(2, info.snapSn);

    EXPECT_CALL(*lfs_, Close(1))
        .Times(1);
    EXPECT_CALL(*lfs_, Close(2))
        .Times(1);
    EXPECT_CALL(*lfs_, Close(3))
        .Times(1);
    EXPECT_CALL(*lfs_, Close(4))
        .Times(1);
    delete[] buf;
}

/**
 * WriteChunkTest 异常测试
 * case:创建快照文件成功，更新metapage成功，cow失败
 * 预期结果:写失败，产生快照文件，chunk版本号发生变更，
 * 快照的bitmap未发生变化，再次写入，仍会进行cow
 */
TEST_P(CSDataStore_test, WriteChunkErrorTest3) {
    // initialize
    FakeEnv();
    EXPECT_TRUE(dataStore->Initialize());

    ChunkID id = 2;
    SequenceNum sn = 3;
    off_t offset = 0;
    size_t length = blocksize_;
    char* buf = new char[length];
    memset(buf, 0, length);
    // will Open snapshot file, snap sn equals 2
    string snapPath = string(baseDir) + "/" +
        FileNameOperator::GenerateSnapshotName(id, 2);
    // expect call chunk file pool GetFile
    EXPECT_CALL(*lfs_, FileExists(snapPath))
        .WillOnce(Return(false));
    EXPECT_CALL(*fpool_, GetFileImpl(snapPath, NotNull()))
        .WillOnce(Return(0));
    EXPECT_CALL(*lfs_, Open(snapPath, _))
        .WillOnce(Return(4));
    // will read snapshot metapage
    char metapage[metapagesize_];  // NOLINT(runtime/arrays)
    memset(metapage, 0, sizeof(metapage));
    FakeEncodeSnapshot(metapage, 2);
    EXPECT_CALL(*lfs_, Read(4, NotNull(), 0, metapagesize_))
        .WillOnce(DoAll(SetArrayArgument<1>(metapage,
                        metapage + metapagesize_),
                        Return(metapagesize_)));
    // will update metapage
    EXPECT_CALL(*lfs_,
                Write(3, Matcher<const char*>(NotNull()), 0, metapagesize_))
        .Times(1);

    LOG(INFO) << "case 1";
    // copy data failed
    EXPECT_CALL(*lfs_, Read(3, NotNull(), metapagesize_ + offset, length))
        .WillOnce(Return(-UT_ERRNO));

    EXPECT_EQ(CSErrorCode::InternalError,
              dataStore->WriteChunk(id,
                                    sn,
                                    buf,
                                    offset,
                                    length,
                                    nullptr));
    CSChunkInfo info;
    dataStore->GetChunkInfo(id, &info);
    ASSERT_EQ(3, info.curSn);
    ASSERT_EQ(2, info.snapSn);

    LOG(INFO) << "case 2";
    // copy data success
    EXPECT_CALL(*lfs_, Read(3, NotNull(), metapagesize_ + offset, length))
        .Times(1);
    // write data to snapshot failed
    EXPECT_CALL(*lfs_, Write(4, Matcher<const char*>(NotNull()),
                             metapagesize_ + offset, length))
        .WillOnce(Return(-UT_ERRNO));
    EXPECT_EQ(CSErrorCode::InternalError,
              dataStore->WriteChunk(id,
                                    sn,
                                    buf,
                                    offset,
                                    length,
                                    nullptr));
    dataStore->GetChunkInfo(id, &info);
    ASSERT_EQ(3, info.curSn);
    ASSERT_EQ(2, info.snapSn);

    LOG(INFO) << "case 3";
    // copy data success
    EXPECT_CALL(*lfs_, Read(3, NotNull(), metapagesize_ + offset, length))
        .Times(1);
    // write data to snapshot success
    EXPECT_CALL(*lfs_, Write(4, Matcher<const char*>(_), metapagesize_ + offset,
                             length))
        .Times(1);
    // update snapshot metapage failed
    EXPECT_CALL(*lfs_,
                Write(4, Matcher<const char*>(NotNull()), 0, metapagesize_))
        .WillOnce(Return(-UT_ERRNO));
    EXPECT_EQ(CSErrorCode::InternalError,
              dataStore->WriteChunk(id,
                                    sn,
                                    buf,
                                    offset,
                                    length,
                                    nullptr));
    dataStore->GetChunkInfo(id, &info);
    ASSERT_EQ(3, info.curSn);
    ASSERT_EQ(2, info.snapSn);

    // 再次写入仍会cow
    // will copy on write
    LOG(INFO) << "case 4";
    EXPECT_CALL(*lfs_, Read(3, NotNull(), metapagesize_ + offset, length))
        .Times(1);
    EXPECT_CALL(*lfs_, Write(4, Matcher<const char*>(NotNull()),
                             metapagesize_ + offset, length))
        .Times(1);
    // will update snapshot metapage
    EXPECT_CALL(*lfs_,
                Write(4, Matcher<const char*>(NotNull()), 0, metapagesize_))
        .Times(1);
    // will write data
    EXPECT_CALL(*lfs_, Write(3, Matcher<butil::IOBuf>(_),
                             metapagesize_ + offset, length))
        .Times(1);

    LOG(INFO) << "case 5";
    EXPECT_EQ(CSErrorCode::Success,
              dataStore->WriteChunk(id,
                                    sn,
                                    buf,
                                    offset,
                                    length,
                                    nullptr));

    EXPECT_CALL(*lfs_, Close(1))
        .Times(1);
    EXPECT_CALL(*lfs_, Close(2))
        .Times(1);
    EXPECT_CALL(*lfs_, Close(3))
        .Times(1);
    EXPECT_CALL(*lfs_, Close(4))
        .Times(1);
    delete[] buf;
}

/**
 * WriteChunkTest 异常测试
 * case:创建快照文件成功，更新metapage成功，cow成功，写数据失败
 * 预期结果:写失败，产生快照文件，chunk版本号发生变更，
 * 快照的bitmap发生变化，再次写入，直接写chunk文件
 */
TEST_P(CSDataStore_test, WriteChunkErrorTest4) {
    // initialize
    FakeEnv();
    EXPECT_TRUE(dataStore->Initialize());

    ChunkID id = 2;
    SequenceNum sn = 3;
    off_t offset = 0;
    size_t length = blocksize_;
    char buf[length];  // NOLINT
    memset(buf, 0, sizeof(buf));
    // will Open snapshot file, snap sn equals 2
    string snapPath = string(baseDir) + "/" +
        FileNameOperator::GenerateSnapshotName(id, 2);
    // expect call chunk file pool GetFile
    EXPECT_CALL(*lfs_, FileExists(snapPath))
        .WillOnce(Return(false));
    EXPECT_CALL(*fpool_, GetFileImpl(snapPath, NotNull()))
        .WillOnce(Return(0));
    EXPECT_CALL(*lfs_, Open(snapPath, _))
        .WillOnce(Return(4));
    // will read snapshot metapage
    char metapage[metapagesize_];  // NOLINT(runtime/arrays)
    memset(metapage, 0, sizeof(metapage));
    FakeEncodeSnapshot(metapage, 2);
    EXPECT_CALL(*lfs_, Read(4, NotNull(), 0, metapagesize_))
        .WillOnce(DoAll(SetArrayArgument<1>(metapage,
                        metapage + metapagesize_),
                        Return(metapagesize_)));
    // will update metapage
    EXPECT_CALL(*lfs_,
                Write(3, Matcher<const char*>(NotNull()), 0, metapagesize_))
        .Times(1);
    // will copy on write
    EXPECT_CALL(*lfs_, Read(3, NotNull(), metapagesize_ + offset, length))
        .Times(1);
    EXPECT_CALL(*lfs_, Write(4, Matcher<const char*>(NotNull()),
                             metapagesize_ + offset, length))
        .Times(1);
    // will update snapshot metapage
    EXPECT_CALL(*lfs_,
                Write(4, Matcher<const char*>(NotNull()), 0, metapagesize_))
        .Times(1);
    // write chunk failed
    EXPECT_CALL(*lfs_, Write(3, Matcher<butil::IOBuf>(_),
                             metapagesize_ + offset, length))
        .WillOnce(Return(-UT_ERRNO));

    EXPECT_EQ(CSErrorCode::InternalError,
              dataStore->WriteChunk(id,
                                    sn,
                                    buf,
                                    offset,
                                    length,
                                    nullptr));
    // 再次写入直接写chunk文件
    // will write data
    EXPECT_CALL(*lfs_, Write(3, Matcher<butil::IOBuf>(_),
                             metapagesize_ + offset, length))
        .Times(1);

    EXPECT_EQ(CSErrorCode::Success,
              dataStore->WriteChunk(id,
                                    sn,
                                    buf,
                                    offset,
                                    length,
                                    nullptr));
    EXPECT_CALL(*lfs_, Close(1))
        .Times(1);
    EXPECT_CALL(*lfs_, Close(2))
        .Times(1);
    EXPECT_CALL(*lfs_, Close(3))
        .Times(1);
    EXPECT_CALL(*lfs_, Close(4))
        .Times(1);
}

/**
 * WriteChunkTest
 * case:chunk 不存在
 * 预期结果:创建chunk文件的时候失败
 */
TEST_P(CSDataStore_test, WriteChunkErrorTest5) {
    // initialize
    FakeEnv();
    EXPECT_TRUE(dataStore->Initialize());

    ChunkID id = 3;
    SequenceNum sn = 1;
    off_t offset = 0;
    size_t length = blocksize_;
    char* buf = new char[length];
    memset(buf, 0, length);
    // create new chunk and open it
    string chunk3Path = string(baseDir) + "/" +
                        FileNameOperator::GenerateChunkFileName(id);

    // expect call chunk file pool GetFile
    EXPECT_CALL(*lfs_, FileExists(chunk3Path))
        .WillOnce(Return(false));
    EXPECT_CALL(*fpool_, GetFileImpl(chunk3Path, NotNull()))
        .WillOnce(Return(-UT_ERRNO));

    EXPECT_EQ(CSErrorCode::InternalError, dataStore->WriteChunk(id,
                                                                sn,
                                                                buf,
                                                                offset,
                                                                length,
                                                                nullptr));

    // getchunk success
    EXPECT_CALL(*lfs_, FileExists(chunk3Path))
        .WillOnce(Return(false));
    EXPECT_CALL(*fpool_, GetFileImpl(chunk3Path, NotNull()))
        .WillOnce(Return(0));
    // set open chunk file failed
    EXPECT_CALL(*lfs_, Open(chunk3Path, _))
        .WillOnce(Return(-UT_ERRNO));
    EXPECT_EQ(CSErrorCode::InternalError, dataStore->WriteChunk(id,
                                                                sn,
                                                                buf,
                                                                offset,
                                                                length,
                                                                nullptr));

    EXPECT_CALL(*lfs_, FileExists(chunk3Path))
        .WillOnce(Return(true));
    // open success
    EXPECT_CALL(*lfs_, Open(chunk3Path, _))
        .WillOnce(Return(4));
    // expect call close
    EXPECT_CALL(*lfs_, Close(4))
        .Times(1);
    // stat failed
    EXPECT_CALL(*lfs_, Fstat(4, NotNull()))
        .WillOnce(Return(-UT_ERRNO));
    EXPECT_EQ(CSErrorCode::InternalError, dataStore->WriteChunk(id,
                                                                sn,
                                                                buf,
                                                                offset,
                                                                length,
                                                                nullptr));

    EXPECT_CALL(*lfs_, FileExists(chunk3Path))
        .WillOnce(Return(true));
    // open success
    EXPECT_CALL(*lfs_, Open(chunk3Path, _))
        .WillOnce(Return(4));
    // expect call close
    EXPECT_CALL(*lfs_, Close(4))
        .Times(1);
    // stat success but file size not equal chunksize_ + metapagesize_
    struct stat fileInfo;
    fileInfo.st_size = chunksize_;
    EXPECT_CALL(*lfs_, Fstat(4, NotNull()))
        .WillOnce(DoAll(SetArgPointee<1>(fileInfo),
                        Return(0)));
    EXPECT_EQ(CSErrorCode::FileFormatError, dataStore->WriteChunk(id,
                                                                sn,
                                                                buf,
                                                                offset,
                                                                length,
                                                                nullptr));

    EXPECT_CALL(*lfs_, FileExists(chunk3Path))
        .WillOnce(Return(true));
    // open success
    EXPECT_CALL(*lfs_, Open(chunk3Path, _))
        .WillOnce(Return(4));
    // expect call close
    EXPECT_CALL(*lfs_, Close(4))
        .Times(1);
    // stat success
    fileInfo.st_size = chunksize_ + metapagesize_;
    EXPECT_CALL(*lfs_, Fstat(4, NotNull()))
        .WillOnce(DoAll(SetArgPointee<1>(fileInfo),
                        Return(0)));
    // read metapage failed
    EXPECT_CALL(*lfs_, Read(4, NotNull(), 0, metapagesize_))
        .WillOnce(Return(-UT_ERRNO));
    EXPECT_EQ(CSErrorCode::InternalError, dataStore->WriteChunk(id,
                                                                sn,
                                                                buf,
                                                                offset,
                                                                length,
                                                                nullptr));

    EXPECT_CALL(*lfs_, Close(1))
        .Times(1);
    EXPECT_CALL(*lfs_, Close(2))
        .Times(1);
    EXPECT_CALL(*lfs_, Close(3))
        .Times(1);
    delete[] buf;
}

/*
 * WriteChunkErrorTest
 * 所写chunk为clone chunk
 * case1:请求location过长，导致metapage size超出page size
 * 预期结果1:create clone chunk失败
 * case2:写数据时失败
 * 预期结果2:返回InternalError，chunk状态不变
 * case3:更新metapage时失败
 * 预期结果3:返回InternalError，chunk状态不变
 */
TEST_P(CSDataStore_test, WriteChunkErrorTest6) {
    // initialize
    FakeEnv();
    EXPECT_TRUE(dataStore->Initialize());

    ChunkID id = 3;
    SequenceNum sn = 1;
    SequenceNum correctedSn = 0;
    off_t offset = 0;
    size_t length = blocksize_;
    char buf[length];  // NOLINT
    memset(buf, 0, sizeof(buf));
    CSChunkInfo info;
    // 创建 clone chunk
    {
        string longLocation(kLocationLimit+1, 'a');
        EXPECT_EQ(CSErrorCode::InvalidArgError,
                  dataStore->CreateCloneChunk(id,
                                              sn,
                                              correctedSn,
                                              chunksize_,
                                              longLocation));
    }
    // 创建 clone chunk
    {
        char chunk3MetaPage[metapagesize_];  // NOLINT(runtime/arrays)
        memset(chunk3MetaPage, 0, sizeof(chunk3MetaPage));
        shared_ptr<Bitmap> bitmap =
            make_shared<Bitmap>(chunksize_ / metapagesize_);
        FakeEncodeChunk(chunk3MetaPage, correctedSn, sn, bitmap, location);
        // create new chunk and open it
        string chunk3Path = string(baseDir) + "/" +
                            FileNameOperator::GenerateChunkFileName(id);
        // expect call chunkfile pool GetFile
        EXPECT_CALL(*lfs_, FileExists(chunk3Path))
            .WillOnce(Return(false));
        EXPECT_CALL(*fpool_, GetFileImpl(chunk3Path, NotNull()))
            .WillOnce(Return(0));
        EXPECT_CALL(*lfs_, Open(chunk3Path, _))
            .Times(1)
            .WillOnce(Return(4));
        // will read metapage
        EXPECT_CALL(*lfs_, Read(4, NotNull(), 0, metapagesize_))
            .WillOnce(DoAll(SetArrayArgument<1>(chunk3MetaPage,
                            chunk3MetaPage + metapagesize_),
                            Return(metapagesize_)));
        EXPECT_EQ(CSErrorCode::Success,
                  dataStore->CreateCloneChunk(id,
                                              sn,
                                              correctedSn,
                                              chunksize_,
                                              location));
    }
    // case1:写数据时失败
    {
        id = 3;  // not exist
        offset = blocksize_;
        length = 2 * blocksize_;
        EXPECT_CALL(*lfs_, Write(4, Matcher<butil::IOBuf>(_),
                                 metapagesize_ + offset, length))
            .WillOnce(Return(-UT_ERRNO));
        // update metapage
        EXPECT_CALL(*lfs_,
                    Write(4, Matcher<const char*>(NotNull()), 0, metapagesize_))
            .Times(0);
        ASSERT_EQ(CSErrorCode::InternalError,
                  dataStore->WriteChunk(id,
                                        sn,
                                        buf,
                                        offset,
                                        length,
                                        nullptr));
        // 检查paste后chunk的状态
        ASSERT_EQ(CSErrorCode::Success, dataStore->GetChunkInfo(id, &info));
        ASSERT_EQ(true, info.isClone);
        ASSERT_EQ(Bitmap::NO_POS, info.bitmap->NextSetBit(0));
    }
    // case2:更新metapage时失败
    {
        id = 3;  // not exist
        offset = blocksize_;
        length = 2 * blocksize_;
        EXPECT_CALL(*lfs_, Write(4, Matcher<butil::IOBuf>(_),
                                 metapagesize_ + offset, length))
            .Times(1);
        // update metapage
        EXPECT_CALL(*lfs_,
                    Write(4, Matcher<const char*>(NotNull()), 0, metapagesize_))
            .WillOnce(Return(-UT_ERRNO));
        ASSERT_EQ(CSErrorCode::InternalError,
                  dataStore->WriteChunk(id,
                                        sn,
                                        buf,
                                        offset,
                                        length,
                                        nullptr));
        // 检查paste后chunk的状态
        ASSERT_EQ(CSErrorCode::Success, dataStore->GetChunkInfo(id, &info));
        ASSERT_EQ(true, info.isClone);
        ASSERT_EQ(Bitmap::NO_POS, info.bitmap->NextSetBit(0));
    }

    EXPECT_CALL(*lfs_, Close(1))
        .Times(1);
    EXPECT_CALL(*lfs_, Close(2))
        .Times(1);
    EXPECT_CALL(*lfs_, Close(3))
        .Times(1);
    EXPECT_CALL(*lfs_, Close(4))
        .Times(1);
}

/**
 * ReadChunkTest
 * case:chunk不存在
 * 预期结果:返回ChunkNotExistError错误码
 */
TEST_P(CSDataStore_test, ReadChunkTest1) {
    // initialize
    FakeEnv();
    EXPECT_TRUE(dataStore->Initialize());

    ChunkID id = 3;
    SequenceNum sn = 2;
    off_t offset = blocksize_;
    size_t length = blocksize_;
    char buf[length];  // NOLINT
    memset(buf, 0, sizeof(buf));
    // test chunk not exists
    EXPECT_EQ(CSErrorCode::ChunkNotExistError,
              dataStore->ReadChunk(id,
                                   sn,
                                   buf,
                                   offset,
                                   length));

    EXPECT_CALL(*lfs_, Close(1))
        .Times(1);
    EXPECT_CALL(*lfs_, Close(2))
        .Times(1);
    EXPECT_CALL(*lfs_, Close(3))
        .Times(1);
}

/**
 * ReadChunkTest
 * case:chunk存在，读取区域超过chunk大小或者offset和length未对齐
 * 预期结果:返回InvalidArgError错误码
 */
TEST_P(CSDataStore_test, ReadChunkTest2) {
    // initialize
    FakeEnv();
    EXPECT_TRUE(dataStore->Initialize());

    ChunkID id = 1;
    SequenceNum sn = 2;
    off_t offset = chunksize_;
    size_t length = blocksize_;
    char buf[length];  // NOLINT
    memset(buf, 0, sizeof(buf));
    // test read out of range
    EXPECT_EQ(CSErrorCode::InvalidArgError,
              dataStore->ReadChunk(id,
                                   sn,
                                   buf,
                                   offset,
                                   length));
    // return InvalidArgError if length not aligned
    offset = blocksize_;
    length = blocksize_ - 1;
    EXPECT_EQ(CSErrorCode::InvalidArgError,
              dataStore->ReadChunk(id,
                                   sn,
                                   buf,
                                   offset,
                                   length));
    // return InvalidArgError if offset not aligned
    offset = blocksize_ + 1;
    length = blocksize_;
    EXPECT_EQ(CSErrorCode::InvalidArgError,
              dataStore->ReadChunk(id,
                                   sn,
                                   buf,
                                   offset,
                                   length));

    EXPECT_CALL(*lfs_, Close(1))
        .Times(1);
    EXPECT_CALL(*lfs_, Close(2))
        .Times(1);
    EXPECT_CALL(*lfs_, Close(3))
        .Times(1);
}

/**
 * ReadChunkTest
 * case:正常读取存在的chunk
 * 预期结果:读取成功
 */
TEST_P(CSDataStore_test, ReadChunkTest3) {
    // initialize
    FakeEnv();
    EXPECT_TRUE(dataStore->Initialize());

    ChunkID id = 1;
    SequenceNum sn = 2;
    off_t offset = blocksize_;
    size_t length = blocksize_;
    char* buf = new char[length];
    memset(buf, 0, length);
    // test chunk exists
    EXPECT_CALL(*lfs_, Read(1, NotNull(), offset + metapagesize_, length))
        .Times(1);
    EXPECT_EQ(CSErrorCode::Success,
              dataStore->ReadChunk(id,
                                   sn,
                                   buf,
                                   offset,
                                   length));

    EXPECT_CALL(*lfs_, Close(1))
        .Times(1);
    EXPECT_CALL(*lfs_, Close(2))
        .Times(1);
    EXPECT_CALL(*lfs_, Close(3))
        .Times(1);
    delete[] buf;
}

/**
 * ReadChunkTest
 * 读取 clone chunk
 * case1:读取区域未被写过
 * 预期结果:返回PageNerverWrittenError
 * case2:读取区域部分被写过
 * 预期结果:返回PageNerverWrittenError
 * case3:读取区域已被写过
 * 预期结果:返回Success，数据成功写入
 */
TEST_P(CSDataStore_test, ReadChunkTest4) {
    // initialize
    FakeEnv();
    EXPECT_TRUE(dataStore->Initialize());

    ChunkID id = 3;
    SequenceNum sn = 1;
    SequenceNum correctedSn = 2;
    CSChunkInfo info;
    char chunk3MetaPage[metapagesize_];  // NOLINT(runtime/arrays)
    memset(chunk3MetaPage, 0, sizeof(chunk3MetaPage));
    shared_ptr<Bitmap> bitmap = make_shared<Bitmap>(chunksize_ / metapagesize_);
    bitmap->Set(0);
    FakeEncodeChunk(chunk3MetaPage, correctedSn, sn, bitmap, location);

    // create new chunk and open it
    string chunk3Path = string(baseDir) + "/" +
                        FileNameOperator::GenerateChunkFileName(id);
    // expect call chunkfile pool GetFile
    EXPECT_CALL(*lfs_, FileExists(chunk3Path))
        .WillOnce(Return(false));
    EXPECT_CALL(*fpool_, GetFileImpl(chunk3Path, NotNull()))
        .WillOnce(Return(0));
    EXPECT_CALL(*lfs_, Open(chunk3Path, _))
        .Times(1)
        .WillOnce(Return(4));
    // will read metapage
    EXPECT_CALL(*lfs_, Read(4, NotNull(), 0, metapagesize_))
        .WillOnce(DoAll(SetArrayArgument<1>(chunk3MetaPage,
                        chunk3MetaPage + metapagesize_),
                        Return(metapagesize_)));
    EXPECT_EQ(CSErrorCode::Success,
                dataStore->CreateCloneChunk(id,
                                            sn,
                                            correctedSn,
                                            chunksize_,
                                            location));

    // case1: 读取未写过区域
    off_t offset = 1 * blocksize_;
    size_t length = blocksize_;
    char buf[2 * length];  // NOLINT
    memset(buf, 0, sizeof(buf));
    EXPECT_CALL(*lfs_, Read(_, _, _, _))
        .Times(0);
    EXPECT_EQ(CSErrorCode::PageNerverWrittenError,
              dataStore->ReadChunk(id,
                                   sn,
                                   buf,
                                   offset,
                                   length));

    // case2: 读取区域部分被写过
    offset = 0;
    length = 2 * blocksize_;
    EXPECT_CALL(*lfs_, Read(_, _, _, _))
        .Times(0);
    EXPECT_EQ(CSErrorCode::PageNerverWrittenError,
              dataStore->ReadChunk(id,
                                   sn,
                                   buf,
                                   offset,
                                   length));

    // case3: 读取区域已写过
    offset = 0;
    length = blocksize_;
    EXPECT_CALL(*lfs_, Read(4, NotNull(), offset + metapagesize_, length))
        .Times(1);
    EXPECT_EQ(CSErrorCode::Success,
              dataStore->ReadChunk(id,
                                   sn,
                                   buf,
                                   offset,
                                   length));

    EXPECT_CALL(*lfs_, Close(1))
        .Times(1);
    EXPECT_CALL(*lfs_, Close(2))
        .Times(1);
    EXPECT_CALL(*lfs_, Close(3))
        .Times(1);
    EXPECT_CALL(*lfs_, Close(4))
        .Times(1);
}

/**
 * ReadChunkErrorTest
 * case:读chunk文件时出错
 * 预期结果:读取失败，返回InternalError
 */
TEST_P(CSDataStore_test, ReadChunkErrorTest1) {
    // initialize
    FakeEnv();
    EXPECT_TRUE(dataStore->Initialize());

    ChunkID id = 1;
    SequenceNum sn = 2;
    off_t offset = blocksize_;
    size_t length = blocksize_;
    char* buf = new char[length];
    memset(buf, 0, length);
    // test read chunk failed
    EXPECT_CALL(*lfs_, Read(1, NotNull(), offset + metapagesize_, length))
        .WillOnce(Return(-UT_ERRNO));
    EXPECT_EQ(CSErrorCode::InternalError,
              dataStore->ReadChunk(id,
                                   sn,
                                   buf,
                                   offset,
                                   length));

    EXPECT_CALL(*lfs_, Close(1))
        .Times(1);
    EXPECT_CALL(*lfs_, Close(2))
        .Times(1);
    EXPECT_CALL(*lfs_, Close(3))
        .Times(1);
    delete[] buf;
}

/**
 * ReadSnapshotChunkTest
 * case:chunk不存在
 * 预期结果:返回ChunkNotExistError错误码
 */
TEST_P(CSDataStore_test, ReadSnapshotChunkTest1) {
    // initialize
    FakeEnv();
    EXPECT_TRUE(dataStore->Initialize());

    ChunkID id = 3;
    SequenceNum sn = 2;
    off_t offset = blocksize_;
    size_t length = blocksize_;
    char* buf = new char[length];
    memset(buf, 0, length);
    // test chunk not exists
    EXPECT_EQ(CSErrorCode::ChunkNotExistError,
              dataStore->ReadSnapshotChunk(id,
                                           sn,
                                           buf,
                                           offset,
                                           length));

    EXPECT_CALL(*lfs_, Close(1))
        .Times(1);
    EXPECT_CALL(*lfs_, Close(2))
        .Times(1);
    EXPECT_CALL(*lfs_, Close(3))
        .Times(1);
    delete[] buf;
}

/**
 * ReadSnapshotChunkTest
 * case:chunk存在，请求版本号等于chunk版本号
 * 预期结果:读chunk的数据
 */
TEST_P(CSDataStore_test, ReadSnapshotChunkTest2) {
    // initialize
    FakeEnv();
    EXPECT_TRUE(dataStore->Initialize());

    ChunkID id = 1;
    SequenceNum sn = 2;
    off_t offset = chunksize_;
    size_t length = 2 * blocksize_;
    char* buf = new char[length];
    memset(buf, 0, length);
    // test out of range
    EXPECT_EQ(CSErrorCode::InvalidArgError,
              dataStore->ReadSnapshotChunk(id,
                                           sn,
                                           buf,
                                           offset,
                                           length));
    // test offset not aligned
    offset = chunksize_ - 1;
    length = chunksize_;
    EXPECT_EQ(CSErrorCode::InvalidArgError,
              dataStore->ReadSnapshotChunk(id,
                                           sn,
                                           buf,
                                           offset,
                                           length));
    // test length not aligned
    offset = chunksize_;
    length = chunksize_ + 1;
    EXPECT_EQ(CSErrorCode::InvalidArgError,
              dataStore->ReadSnapshotChunk(id,
                                           sn,
                                           buf,
                                           offset,
                                           length));
    // test in range
    offset = blocksize_;
    length = 2 * blocksize_;
    EXPECT_CALL(*lfs_, Read(1, NotNull(), offset + metapagesize_, length))
        .Times(1);
    EXPECT_EQ(CSErrorCode::Success,
              dataStore->ReadSnapshotChunk(id,
                                           sn,
                                           buf,
                                           offset,
                                           length));

    EXPECT_CALL(*lfs_, Close(1))
        .Times(1);
    EXPECT_CALL(*lfs_, Close(2))
        .Times(1);
    EXPECT_CALL(*lfs_, Close(3))
        .Times(1);
    delete[] buf;
}

/**
 * ReadSnapshotChunkTest
 * case:chunk存在，请求版本号等于snapshot版本号
 * 预期结果:读快照的数据
 */
TEST_P(CSDataStore_test, ReadSnapshotChunkTest3) {
    // initialize
    FakeEnv();
    EXPECT_TRUE(dataStore->Initialize());
    // fake data
    ChunkID id = 1;
    SequenceNum sn = 2;
    off_t offset = blocksize_;
    size_t length = blocksize_ * 2;
    char* writeBuf = new char[length];
    memset(writeBuf, 0, length);
    // data in [blocksize_, 2*blocksize_) will be cow
    EXPECT_CALL(*lfs_, Read(1, NotNull(), offset + metapagesize_, length))
        .Times(1);
    EXPECT_CALL(*lfs_, Write(2, Matcher<const char*>(NotNull()),
                             offset + metapagesize_, length))
        .Times(1);
    // will update snapshot metapage
    EXPECT_CALL(*lfs_,
                Write(2, Matcher<const char*>(NotNull()), 0, metapagesize_))
        .Times(1);
    // will write data
    EXPECT_CALL(*lfs_, Write(1, Matcher<butil::IOBuf>(_),
                             offset + metapagesize_, length))
        .Times(1);
    EXPECT_EQ(CSErrorCode::Success,
              dataStore->WriteChunk(id,
                                    sn,
                                    writeBuf,
                                    offset,
                                    length,
                                    nullptr));

    // test out of range
    sn = 1;
    offset = chunksize_;
    length = blocksize_ * 4;
    char* readBuf = new char[length];
    memset(readBuf, 0, length);
    EXPECT_EQ(CSErrorCode::InvalidArgError,
              dataStore->ReadSnapshotChunk(id,
                                           sn,
                                           readBuf,
                                           offset,
                                           length));
    // test in range, read [0, 4*blocksize_)
    offset = 0;
    // read chunk in[0, blocksize_) and [3*blocksize_, 4*blocksize_)
    EXPECT_CALL(*lfs_, Read(1, NotNull(), metapagesize_, blocksize_))
        .Times(1);
    EXPECT_CALL(*lfs_,
                Read(1, NotNull(), metapagesize_ + 3 * blocksize_, blocksize_))
        .Times(1);
    // read snapshot in[blocksize_, 3*blocksize_)
    EXPECT_CALL(*lfs_, Read(2, NotNull(), metapagesize_ + 1 * blocksize_,
                            2 * blocksize_))
        .Times(1);
    EXPECT_EQ(CSErrorCode::Success,
              dataStore->ReadSnapshotChunk(id,
                                           sn,
                                           readBuf,
                                           offset,
                                           length));

    EXPECT_CALL(*lfs_, Close(1))
        .Times(1);
    EXPECT_CALL(*lfs_, Close(2))
        .Times(1);
    EXPECT_CALL(*lfs_, Close(3))
        .Times(1);
    delete[] writeBuf;
    delete[] readBuf;
}

/**
 * ReadSnapshotChunkTest
 * case:chunk存在，但是请求的版本号不存在
 * 预期结果:返回ChunkNotExistError错误码
 */
TEST_P(CSDataStore_test, ReadSnapshotChunkTest4) {
    // initialize
    FakeEnv();
    EXPECT_TRUE(dataStore->Initialize());

    ChunkID id = 1;
    SequenceNum sn = 3;
    off_t offset = blocksize_;
    size_t length = blocksize_;
    char* buf = new char[length];
    memset(buf, 0, length);
    // test sn not exists
    EXPECT_EQ(CSErrorCode::ChunkNotExistError,
              dataStore->ReadSnapshotChunk(id,
                                           sn,
                                           buf,
                                           offset,
                                           length));

    EXPECT_CALL(*lfs_, Close(1))
        .Times(1);
    EXPECT_CALL(*lfs_, Close(2))
        .Times(1);
    EXPECT_CALL(*lfs_, Close(3))
        .Times(1);
    delete[] buf;
}

/**
 * ReadSnapshotChunkErrorTest
 * case:读快照时失败
 * 预期结果:返回InternalError
 */
TEST_P(CSDataStore_test, ReadSnapshotChunkErrorTest1) {
    // initialize
    FakeEnv();
    EXPECT_TRUE(dataStore->Initialize());
    // fake data
    ChunkID id = 1;
    SequenceNum sn = 2;
    off_t offset = blocksize_;
    size_t length = blocksize_ * 2;
    char* writeBuf = new char[length];  // NOLINT
    memset(writeBuf, 0, length);
    // data in [blocksize_, 2*blocksize_) will be cow
    EXPECT_CALL(*lfs_, Read(1, NotNull(), offset + metapagesize_, length))
        .Times(1);
    EXPECT_CALL(*lfs_, Write(2, Matcher<const char*>(NotNull()),
                             offset + metapagesize_, length))
        .Times(1);
    // will update snapshot metapage
    EXPECT_CALL(*lfs_,
                Write(2, Matcher<const char*>(NotNull()), 0, metapagesize_))
        .Times(1);
    // will write data
    EXPECT_CALL(*lfs_, Write(1, Matcher<butil::IOBuf>(_),
                             offset + metapagesize_, length))
        .Times(1);
    ASSERT_EQ(CSErrorCode::Success,
              dataStore->WriteChunk(id,
                                    sn,
                                    writeBuf,
                                    offset,
                                    length,
                                    nullptr));

    // test in range, read [0, 4*blocksize_)
    sn = 1;
    offset = 0;
    length = blocksize_ * 4;
    char* readBuf = new char[length];
    memset(readBuf, 0, length);
    // read chunk failed
    EXPECT_CALL(*lfs_, Read(1, NotNull(), metapagesize_, blocksize_))
        .WillOnce(Return(-UT_ERRNO));
    ASSERT_EQ(CSErrorCode::InternalError,
              dataStore->ReadSnapshotChunk(id,
                                           sn,
                                           readBuf,
                                           offset,
                                           length));

    // read snapshot failed
    EXPECT_CALL(*lfs_, Read(1, NotNull(), metapagesize_, blocksize_))
        .Times(1);
    EXPECT_CALL(*lfs_,
                Read(1, NotNull(), metapagesize_ + 3 * blocksize_, blocksize_))
        .Times(1);
    EXPECT_CALL(*lfs_, Read(2, NotNull(), metapagesize_ + 1 * blocksize_,
                            2 * blocksize_))
        .WillOnce(Return(-UT_ERRNO));
    ASSERT_EQ(CSErrorCode::InternalError,
              dataStore->ReadSnapshotChunk(id, sn, readBuf, offset, length));

    EXPECT_CALL(*lfs_, Close(1))
        .Times(1);
    EXPECT_CALL(*lfs_, Close(2))
        .Times(1);
    EXPECT_CALL(*lfs_, Close(3))
        .Times(1);
    delete[] writeBuf;
    delete[] readBuf;
}

/**
 * ReadSnapshotChunkErrorTest
 * case:chunk存在，请求版本号等于chunk版本号,读数据时失败
 * 预期结果:返回InternalError
 */
TEST_P(CSDataStore_test, ReadSnapshotChunkErrorTest2) {
    // initialize
    FakeEnv();
    EXPECT_TRUE(dataStore->Initialize());

    ChunkID id = 1;
    SequenceNum sn = 2;
    off_t offset = blocksize_;
    size_t length = 2 * blocksize_;
    char* buf = new char[length];
    memset(buf, 0, length);
    // test in range
    offset = blocksize_;
    EXPECT_CALL(*lfs_, Read(1, NotNull(), offset + metapagesize_, length))
        .WillOnce(Return(-UT_ERRNO));
    EXPECT_EQ(CSErrorCode::InternalError,
              dataStore->ReadSnapshotChunk(id,
                                           sn,
                                           buf,
                                           offset,
                                           length));

    EXPECT_CALL(*lfs_, Close(1))
        .Times(1);
    EXPECT_CALL(*lfs_, Close(2))
        .Times(1);
    EXPECT_CALL(*lfs_, Close(3))
        .Times(1);
    delete[] buf;
}

/**
 * ReadChunkMetaPageTest
 * case: read normal chunk
 * expect: read successfully
 */
TEST_P(CSDataStore_test, ReadChunkMetaDataTest1) {
    // initialize
    FakeEnv();
    EXPECT_TRUE(dataStore->Initialize());

    ChunkID id = 3;
    SequenceNum sn = 2;
    char buf[blocksize_];  // NOLINT(runtime/arrays)
    memset(buf, 0, blocksize_);
    // test chunk not exists
    EXPECT_EQ(CSErrorCode::ChunkNotExistError,
              dataStore->ReadChunkMetaPage(id, sn, buf));

    EXPECT_CALL(*lfs_, Close(1))
        .Times(1);
    EXPECT_CALL(*lfs_, Close(2))
        .Times(1);
    EXPECT_CALL(*lfs_, Close(3))
        .Times(1);
}

/**
 * ReadChunkMetaPageTest
 * case: read normal chunk
 * expect: read successfully
 */
TEST_P(CSDataStore_test, ReadChunkMetaDataTest2) {
    // initialize
    FakeEnv();
    EXPECT_TRUE(dataStore->Initialize());

    ChunkID id = 1;
    SequenceNum sn = 2;
    char buf[blocksize_];  // NOLINT(runtime/arrays)
    memset(buf, 0, blocksize_);
    // test chunk exists
    EXPECT_CALL(*lfs_, Read(1, NotNull(), 0, metapagesize_))
        .Times(1);
    EXPECT_EQ(CSErrorCode::Success,
              dataStore->ReadChunkMetaPage(id, sn, buf));

    EXPECT_CALL(*lfs_, Close(1))
        .Times(1);
    EXPECT_CALL(*lfs_, Close(2))
        .Times(1);
    EXPECT_CALL(*lfs_, Close(3))
        .Times(1);
}


/**
 * DeleteChunkTest
 * case:chunk不存在
 * 预期结果:返回成功
 */
TEST_P(CSDataStore_test, DeleteChunkTest1) {
    // initialize
    FakeEnv();
    EXPECT_TRUE(dataStore->Initialize());

    ChunkID id = 3;
    SequenceNum sn = 2;

    // test chunk not exists
    EXPECT_EQ(CSErrorCode::Success,
              dataStore->DeleteChunk(id, sn));

    EXPECT_CALL(*lfs_, Close(1))
        .Times(1);
    EXPECT_CALL(*lfs_, Close(2))
        .Times(1);
    EXPECT_CALL(*lfs_, Close(3))
        .Times(1);
}

/**
 * DeleteChunkTest
 * case:chunk存在快照文件
 * 预期结果:返回Success， chunk被删除，快照被删除
 */
TEST_P(CSDataStore_test, DeleteChunkTest2) {
    // initialize
    FakeEnv();
    EXPECT_TRUE(dataStore->Initialize());

    ChunkID id = 1;
    SequenceNum sn = 2;

    EXPECT_CALL(*lfs_, Close(1))
        .Times(1);
    EXPECT_CALL(*lfs_, Close(2))
        .Times(1);

    // delete chunk with snapshot
    EXPECT_EQ(CSErrorCode::Success,
              dataStore->DeleteChunk(id, sn));
    CSChunkInfo info;
    ASSERT_EQ(CSErrorCode::ChunkNotExistError,
              dataStore->GetChunkInfo(id, &info));

    EXPECT_CALL(*lfs_, Close(3))
        .Times(1);
}

/**
 * case:chunk存在,快照文件不存在
 * 预期结果:返回成功
 */
TEST_P(CSDataStore_test, DeleteChunkTest3) {
    // initialize
    FakeEnv();
    EXPECT_TRUE(dataStore->Initialize());

    ChunkID id = 2;
    SequenceNum sn = 2;

    // chunk will be closed
    EXPECT_CALL(*lfs_, Close(3))
        .Times(1);
    // expect to call FilePool RecycleFile
    EXPECT_CALL(*fpool_, RecycleFile(chunk2Path))
        .WillOnce(Return(0));
    EXPECT_EQ(CSErrorCode::Success,
            dataStore->DeleteChunk(id, sn));

    EXPECT_CALL(*lfs_, Close(1))
        .Times(1);
    EXPECT_CALL(*lfs_, Close(2))
        .Times(1);
}

/**
 * DeleteChunkTest
 * chunk存在,快照文件不存在
 * case1: sn<chunkinfo.sn
 * 预期结果1:返回BackwardRequestError
 * case2: sn>chunkinfo.sn
 * 预期结果2:返回成功
 */
TEST_P(CSDataStore_test, DeleteChunkTest4) {
    // initialize
    FakeEnv();
    EXPECT_TRUE(dataStore->Initialize());

    ChunkID id = 2;

    // case1
    {
        // chunk will be closed
        EXPECT_CALL(*lfs_, Close(3))
            .Times(0);
        // expect to call FilePool RecycleFile
        EXPECT_CALL(*fpool_, RecycleFile(chunk2Path))
            .Times(0);
        EXPECT_EQ(CSErrorCode::BackwardRequestError,
                dataStore->DeleteChunk(id, 1));
    }

    // case2
    {
        // chunk will be closed
        EXPECT_CALL(*lfs_, Close(3))
            .Times(1);
        // expect to call FilePool RecycleFile
        EXPECT_CALL(*fpool_, RecycleFile(chunk2Path))
            .WillOnce(Return(0));
        EXPECT_EQ(CSErrorCode::Success,
                dataStore->DeleteChunk(id, 3));
    }

    EXPECT_CALL(*lfs_, Close(1))
        .Times(1);
    EXPECT_CALL(*lfs_, Close(2))
        .Times(1);
}

/**
 * DeleteChunkErrorTest
 * case:chunk存在,快照文件不存在,recyclechunk时出错
 * 预期结果:返回成功
 */
TEST_P(CSDataStore_test, DeleteChunkErrorTest1) {
    // initialize
    FakeEnv();
    EXPECT_TRUE(dataStore->Initialize());

    ChunkID id = 2;
    SequenceNum sn = 2;
    // chunk will be closed
    EXPECT_CALL(*lfs_, Close(3))
        .Times(1);
    // expect to call FilePool RecycleFile
    EXPECT_CALL(*fpool_, RecycleFile(chunk2Path))
        .WillOnce(Return(-1));
    EXPECT_EQ(CSErrorCode::InternalError,
              dataStore->DeleteChunk(id, sn));

    EXPECT_CALL(*lfs_, Close(1))
        .Times(1);
    EXPECT_CALL(*lfs_, Close(2))
        .Times(1);
}

/**
 * DeleteSnapshotChunkOrCorrectSnTest
 * case:chunk不存在
 * 预期结果:返回成功
 */
TEST_P(CSDataStore_test, DeleteSnapshotChunkOrCorrectSnTest1) {
    // initialize
    FakeEnv();
    EXPECT_TRUE(dataStore->Initialize());

    ChunkID id = 3;
    SequenceNum fileSn = 3;
    // test chunk not exists
    EXPECT_EQ(CSErrorCode::Success,
              dataStore->DeleteSnapshotChunkOrCorrectSn(id, fileSn));

    EXPECT_CALL(*lfs_, Close(1))
        .Times(1);
    EXPECT_CALL(*lfs_, Close(2))
        .Times(1);
    EXPECT_CALL(*lfs_, Close(3))
        .Times(1);
}

// 对于DeleteSnapshotChunkOrCorrectSn来说，内部主要有两个操作
// 一个是删除快照文件，一个是修改correctedSn
// 当存在快照文件时，fileSn>=chunk的sn是判断是否要删除快照的唯一条件
// 对于correctedSn来说，fileSn大于chunk的sn以及correctedSn是判断
// 是否要修改correctedSn的唯一条件

/**
 * DeleteSnapshotChunkOrCorrectSnTest
 * case:chunk存在,snapshot存在
 *      fileSn >= chunk的sn
 *      fileSn == chunk的correctedSn
 *      chunk.sn>snap.sn
 * 预期结果:删除快照，不会修改correctedSn,返回成功
 */
TEST_P(CSDataStore_test, DeleteSnapshotChunkOrCorrectSnTest2) {
    // initialize
    FakeEnv();
    // set chunk1's correctedSn as 3
    FakeEncodeChunk(chunk1MetaPage, 3, 2);
    EXPECT_CALL(*lfs_, Read(1, NotNull(), 0, metapagesize_))
        .WillRepeatedly(DoAll(
                        SetArrayArgument<1>(chunk1MetaPage,
                        chunk1MetaPage + metapagesize_),
                        Return(metapagesize_)));
    EXPECT_TRUE(dataStore->Initialize());

    ChunkID id = 1;
    // fileSn > sn
    // fileSn == correctedSn
    SequenceNum fileSn = 3;
    // snapshot will be closed
    EXPECT_CALL(*lfs_, Close(2))
        .Times(1);
    // expect to call FilePool RecycleFile
    EXPECT_CALL(*fpool_, RecycleFile(chunk1snap1Path))
        .Times(1);
    // chunk's metapage should not be updated
    EXPECT_CALL(*lfs_,
                Write(1, Matcher<const char*>(NotNull()), 0, metapagesize_))
        .Times(0);
    EXPECT_EQ(CSErrorCode::Success,
              dataStore->DeleteSnapshotChunkOrCorrectSn(id, fileSn));

    EXPECT_CALL(*lfs_, Close(1))
        .Times(1);
    EXPECT_CALL(*lfs_, Close(3))
        .Times(1);
}

/**
 * DeleteSnapshotChunkOrCorrectSnTest
 * case:chunk存在,snapshot存在
 *      fileSn < chunk的sn
 *      此时无论correctSn为何值都不会修改correctedSn
 * 预期结果:返回成功，不会删除快照,不会修改correctedSn
 */
TEST_P(CSDataStore_test, DeleteSnapshotChunkOrCorrectSnTest3) {
    // initialize
    FakeEnv();
    // set chunk1's correctedSn as 0, sn as 3
    FakeEncodeChunk(chunk1MetaPage, 0, 3);
    EXPECT_CALL(*lfs_, Read(1, NotNull(), 0, metapagesize_))
        .WillRepeatedly(DoAll(
                        SetArrayArgument<1>(chunk1MetaPage,
                        chunk1MetaPage + metapagesize_),
                        Return(metapagesize_)));
    EXPECT_TRUE(dataStore->Initialize());

    ChunkID id = 1;
    // 2 < sn
    // 2 > correctedSn
    SequenceNum fileSn = 2;
    // snapshot should not be closed
    EXPECT_CALL(*lfs_, Close(2))
        .Times(0);
    // chunk's metapage should not be updated
    EXPECT_CALL(*lfs_,
                Write(3, Matcher<const char*>(NotNull()), 0, metapagesize_))
        .Times(0);
    EXPECT_EQ(CSErrorCode::BackwardRequestError,
              dataStore->DeleteSnapshotChunkOrCorrectSn(id, fileSn));

    // 下则用例用于补充DeleteSnapshotChunkOrCorrectSnTest2用例中
    // 当 fileSn == sn 时的边界情况
    // fileSn == sn
    // fileSn > correctedSn
    fileSn = 3;
    // snapshot will be closed
    EXPECT_CALL(*lfs_, Close(2))
        .Times(1);
    // expect to call FilePool RecycleFile
    EXPECT_CALL(*fpool_, RecycleFile(chunk1snap1Path))
        .Times(1);
    // chunk's metapage should not be updated
    EXPECT_CALL(*lfs_,
                Write(1, Matcher<const char*>(NotNull()), 0, metapagesize_))
        .Times(0);
    EXPECT_EQ(CSErrorCode::Success,
              dataStore->DeleteSnapshotChunkOrCorrectSn(id, fileSn));

    EXPECT_CALL(*lfs_, Close(1))
        .Times(1);
    EXPECT_CALL(*lfs_, Close(3))
        .Times(1);
}

/**
 * DeleteSnapshotChunkOrCorrectSnTest
 * case:chunk存在,snapshot存在
 *      fileSn > chunk的sn以及correctedSn
 * 预期结果:删除快照，并修改correctedSn,返回成功
 */
TEST_P(CSDataStore_test, DeleteSnapshotChunkOrCorrectSnTest4) {
    // initialize
    FakeEnv();
    EXPECT_TRUE(dataStore->Initialize());

    ChunkID id = 1;
    // fileSn > sn
    // fileSn > correctedSn
    SequenceNum fileSn = 3;
    // snapshot will be closed
    EXPECT_CALL(*lfs_, Close(2))
        .Times(1);
    // expect to call FilePool RecycleFile
    EXPECT_CALL(*fpool_, RecycleFile(chunk1snap1Path))
        .Times(1);
    // chunk's metapage will be updated
    EXPECT_CALL(*lfs_,
                Write(1, Matcher<const char*>(NotNull()), 0, metapagesize_))
        .Times(1);
    EXPECT_EQ(CSErrorCode::Success,
              dataStore->DeleteSnapshotChunkOrCorrectSn(id, fileSn));

    EXPECT_CALL(*lfs_, Close(1))
        .Times(1);
    EXPECT_CALL(*lfs_, Close(3))
        .Times(1);
}

/**
 * DeleteSnapshotChunkOrCorrectSnTest
 * case:chunk存在,snapshot不存在
 *      fileSn <= chunk的sn或correctedSn
 * 预期结果:不会修改correctedSn,返回成功
 */
TEST_P(CSDataStore_test, DeleteSnapshotChunkOrCorrectSnTest5) {
    // initialize
    FakeEnv();
    EXPECT_TRUE(dataStore->Initialize());

    ChunkID id = 2;
    // fileSn == sn
    // fileSn > correctedSn
    SequenceNum fileSn = 2;
    // chunk's metapage should not be updated
    EXPECT_CALL(*lfs_,
                Write(3, Matcher<const char*>(NotNull()), 0, metapagesize_))
        .Times(0);
    EXPECT_EQ(CSErrorCode::Success,
              dataStore->DeleteSnapshotChunkOrCorrectSn(id, fileSn));

    EXPECT_CALL(*lfs_, Close(1))
        .Times(1);
    EXPECT_CALL(*lfs_, Close(2))
        .Times(1);
    EXPECT_CALL(*lfs_, Close(3))
        .Times(1);
}

/**
 * DeleteSnapshotChunkOrCorrectSnTest
 * case:chunk存在,snapshot不存在
 *      fileSn > chunk的sn及correctedSn
 * 预期结果:修改correctedSn,返回成功
 */
TEST_P(CSDataStore_test, DeleteSnapshotChunkOrCorrectSnTest6) {
    // initialize
    FakeEnv();
    EXPECT_TRUE(dataStore->Initialize());

    ChunkID id = 2;
    // fileSn > sn
    // fileSn > correctedSn
    SequenceNum fileSn = 4;
    // chunk's metapage will be updated
    EXPECT_CALL(*lfs_,
                Write(3, Matcher<const char*>(NotNull()), 0, metapagesize_))
        .Times(1);
    EXPECT_EQ(CSErrorCode::Success,
              dataStore->DeleteSnapshotChunkOrCorrectSn(id, fileSn));

    EXPECT_CALL(*lfs_, Close(1))
        .Times(1);
    EXPECT_CALL(*lfs_, Close(2))
        .Times(1);
    EXPECT_CALL(*lfs_, Close(3))
        .Times(1);
}

/**
 * DeleteSnapshotChunkOrCorrectSnTest
 * case:chunk存在,snapshot不存在，chunk为clone chunk
 * 预期结果:返回StatusConflictError
 */
TEST_P(CSDataStore_test, DeleteSnapshotChunkOrCorrectSnTest7) {
    // initialize
    FakeEnv();
    EXPECT_TRUE(dataStore->Initialize());

    ChunkID id = 3;
    SequenceNum sn = 2;
    SequenceNum correctedSn = 4;
    CSChunkInfo info;
    char chunk3MetaPage[metapagesize_];  // NOLINT(runtime/arrays)
    memset(chunk3MetaPage, 0, sizeof(chunk3MetaPage));
    shared_ptr<Bitmap> bitmap = make_shared<Bitmap>(chunksize_ / blocksize_);
    FakeEncodeChunk(chunk3MetaPage, correctedSn, sn, bitmap, location);

    // create new chunk and open it
    string chunk3Path = string(baseDir) + "/" +
                        FileNameOperator::GenerateChunkFileName(id);
    // expect call chunkfile pool GetFile
    EXPECT_CALL(*lfs_, FileExists(chunk3Path))
        .WillOnce(Return(false));
    EXPECT_CALL(*fpool_, GetFileImpl(chunk3Path, NotNull()))
        .WillOnce(Return(0));
    EXPECT_CALL(*lfs_, Open(chunk3Path, _))
        .Times(1)
        .WillOnce(Return(4));
    // will read metapage
    EXPECT_CALL(*lfs_, Read(4, NotNull(), 0, metapagesize_))
        .WillOnce(DoAll(SetArrayArgument<1>(chunk3MetaPage,
                        chunk3MetaPage + metapagesize_),
                        Return(metapagesize_)));
    EXPECT_EQ(CSErrorCode::Success,
                dataStore->CreateCloneChunk(id,
                                            sn,
                                            correctedSn,
                                            chunksize_,
                                            location));

    // 无论correctedSn为多少，都返回StatusConflictError
    EXPECT_EQ(CSErrorCode::StatusConflictError,
              dataStore->DeleteSnapshotChunkOrCorrectSn(id, 1));
    EXPECT_EQ(CSErrorCode::StatusConflictError,
              dataStore->DeleteSnapshotChunkOrCorrectSn(id, 2));
    EXPECT_EQ(CSErrorCode::StatusConflictError,
              dataStore->DeleteSnapshotChunkOrCorrectSn(id, 3));
    EXPECT_EQ(CSErrorCode::StatusConflictError,
              dataStore->DeleteSnapshotChunkOrCorrectSn(id, 4));
    EXPECT_EQ(CSErrorCode::StatusConflictError,
              dataStore->DeleteSnapshotChunkOrCorrectSn(id, 5));

    EXPECT_CALL(*lfs_, Close(1))
        .Times(1);
    EXPECT_CALL(*lfs_, Close(2))
        .Times(1);
    EXPECT_CALL(*lfs_, Close(3))
        .Times(1);
    EXPECT_CALL(*lfs_, Close(4))
        .Times(1);
}

/**
 * DeleteSnapshotChunkOrCorrectSnTest
 * case:chunk存在,snapshot存在
 *      fileSn > chunk的sn
 *      fileSn > chunk的correctedSn
 *      chunk.sn==snap.sn
 * 预期结果:删除快照，不会修改correctedSn,返回成功
 */
TEST_P(CSDataStore_test, DeleteSnapshotChunkOrCorrectSnTest8) {
    // initialize
    FakeEnv();
    // fake read chunk1 metapage
    FakeEncodeChunk(chunk1MetaPage, 0, 2);
    EXPECT_CALL(*lfs_, Read(1, NotNull(), 0, metapagesize_))
        .WillRepeatedly(DoAll(
                        SetArrayArgument<1>(chunk1MetaPage,
                        chunk1MetaPage + metapagesize_),
                        Return(metapagesize_)));
    // fake read chunk1's snapshot1 metapage,chunk.sn==snap.sn
    FakeEncodeSnapshot(chunk1SnapMetaPage, 2);
    EXPECT_CALL(*lfs_, Read(2, NotNull(), 0, metapagesize_))
        .WillRepeatedly(DoAll(
                        SetArrayArgument<1>(chunk1SnapMetaPage,
                        chunk1SnapMetaPage + metapagesize_),
                        Return(metapagesize_)));
    EXPECT_TRUE(dataStore->Initialize());

    ChunkID id = 1;
    // fileSn > sn
    // fileSn > correctedSn
    SequenceNum fileSn = 3;
    // snapshot will not be closed
    EXPECT_CALL(*lfs_, Close(2))
        .Times(0);
    // expect to call FilePool RecycleFile
    EXPECT_CALL(*fpool_, RecycleFile(chunk1snap1Path))
        .Times(0);
    // chunk's metapage should be updated
    EXPECT_CALL(*lfs_,
                Write(1, Matcher<const char*>(NotNull()), 0, metapagesize_))
        .Times(1);
    EXPECT_EQ(CSErrorCode::Success,
              dataStore->DeleteSnapshotChunkOrCorrectSn(id, fileSn));

    EXPECT_CALL(*lfs_, Close(1))
        .Times(1);
    EXPECT_CALL(*lfs_, Close(2))
        .Times(1);
    EXPECT_CALL(*lfs_, Close(3))
        .Times(1);
}

/**
 * DeleteSnapshotChunkOrCorrectSnTest
 * case:chunk存在,snapshot存在
 *      fileSn == chunk的sn
 *      fileSn == chunk的correctedSn
 *      chunk.sn<snap.sn
 * 预期结果:删除快照，不会修改correctedSn,返回成功
 */
TEST_P(CSDataStore_test, DeleteSnapshotChunkOrCorrectSnTest9) {
    // initialize
    FakeEnv();
    // fake read chunk1 metapage
    FakeEncodeChunk(chunk1MetaPage, 2, 2);
    EXPECT_CALL(*lfs_, Read(1, NotNull(), 0, metapagesize_))
        .WillRepeatedly(DoAll(
                        SetArrayArgument<1>(chunk1MetaPage,
                        chunk1MetaPage + metapagesize_),
                        Return(metapagesize_)));
    // fake read chunk1's snapshot1 metapage,chunk.sn==snap.sn
    FakeEncodeSnapshot(chunk1SnapMetaPage, 3);
    EXPECT_CALL(*lfs_, Read(2, NotNull(), 0, metapagesize_))
        .WillRepeatedly(DoAll(
                        SetArrayArgument<1>(chunk1SnapMetaPage,
                        chunk1SnapMetaPage + metapagesize_),
                        Return(metapagesize_)));
    EXPECT_TRUE(dataStore->Initialize());

    ChunkID id = 1;
    // fileSn == sn
    // fileSn == correctedSn
    SequenceNum fileSn = 2;
    // snapshot will not be closed
    EXPECT_CALL(*lfs_, Close(2))
        .Times(0);
    // expect to call FilePool RecycleFile
    EXPECT_CALL(*fpool_, RecycleFile(chunk1snap1Path))
        .Times(0);
    // chunk's metapage should not be updated
    EXPECT_CALL(*lfs_,
                Write(1, Matcher<const char*>(NotNull()), 0, metapagesize_))
        .Times(0);
    EXPECT_EQ(CSErrorCode::Success,
              dataStore->DeleteSnapshotChunkOrCorrectSn(id, fileSn));

    EXPECT_CALL(*lfs_, Close(1))
        .Times(1);
    EXPECT_CALL(*lfs_, Close(2))
        .Times(1);
    EXPECT_CALL(*lfs_, Close(3))
        .Times(1);
}

/**
 * DeleteSnapshotChunkOrCorrectSnErrorTest
 * case:修改correctedSn时失败
 * 预期结果:返回失败，correctedSn的值未改变
 */
TEST_P(CSDataStore_test, DeleteSnapshotChunkOrCorrectSnErrorTest1) {
    // initialize
    FakeEnv();
    EXPECT_TRUE(dataStore->Initialize());

    ChunkID id = 2;
    // fileSn > sn
    // fileSn > correctedSn
    SequenceNum fileSn = 3;

    // write chunk metapage failed
    EXPECT_CALL(*lfs_,
                Write(3, Matcher<const char*>(NotNull()), 0, metapagesize_))
        .WillOnce(Return(-UT_ERRNO));
    EXPECT_EQ(CSErrorCode::InternalError,
              dataStore->DeleteSnapshotChunkOrCorrectSn(id, fileSn));

    // chunk's metapage will be updated
    EXPECT_CALL(*lfs_,
                Write(3, Matcher<const char*>(NotNull()), 0, metapagesize_))
        .Times(1);
    EXPECT_EQ(CSErrorCode::Success,
              dataStore->DeleteSnapshotChunkOrCorrectSn(id, fileSn));

    EXPECT_CALL(*lfs_, Close(1))
        .Times(1);
    EXPECT_CALL(*lfs_, Close(2))
        .Times(1);
    EXPECT_CALL(*lfs_, Close(3))
        .Times(1);
}

/**
 * DeleteSnapshotChunkOrCorrectSnErrorTest
 * case:回收snapshot的chunk的时候失败
 * 预期结果:返回失败
 */
TEST_P(CSDataStore_test, DeleteSnapshotChunkOrCorrectSnErrorTest2) {
    // initialize
    FakeEnv();
    EXPECT_TRUE(dataStore->Initialize());

    ChunkID id = 1;
    // fileSn > sn
    // fileSn > correctedSn
    SequenceNum fileSn = 3;
    // snapshot will be closed
    EXPECT_CALL(*lfs_, Close(2))
        .Times(1);
    // expect to call FilePool RecycleFile
    EXPECT_CALL(*fpool_, RecycleFile(chunk1snap1Path))
        .WillOnce(Return(-1));
    // chunk's metapage will be updated
    EXPECT_CALL(*lfs_,
                Write(1, Matcher<const char*>(NotNull()), 0, metapagesize_))
        .Times(0);
    EXPECT_EQ(CSErrorCode::InternalError,
              dataStore->DeleteSnapshotChunkOrCorrectSn(id, fileSn));

    EXPECT_CALL(*lfs_, Close(1))
        .Times(1);
    EXPECT_CALL(*lfs_, Close(3))
        .Times(1);
}

/**
 * CreateCloneChunkTest
 * case1:指定的chunk不存在，输入错误的参数
 * 预期结果1:返回InvalidArgError
 * case2:指定的chunk不存在，指定chunksize与配置一致
 * 预期结果2:创建成功
 * case3:指定的chunk存在，参数与原chunk一致
 * 预期结果3:返回成功
 * case4:指定的chunk存在，参数与原chunk不一致
 * 预期结果4:返回ChunkConflictError，不改变原chunk信息
 * case5:指定的chunk存在，指定chunksize与配置不一致
 * 预期结果5: 返回InvalidArgError，不改变原chunk信息
 * case6:指定的chunk存在，chunk不是clone chunk，参数与chunk信息一致
 * 预期结果:返回ChunkConflictError，不改变原chunk信息
 */
TEST_P(CSDataStore_test, CreateCloneChunkTest) {
    // initialize
    FakeEnv();
    EXPECT_TRUE(dataStore->Initialize());

    ChunkID id = 3;
    SequenceNum sn = 1;
    SequenceNum correctedSn = 2;
    CSChunkInfo info;
    char chunk3MetaPage[metapagesize_];  // NOLINT(runtime/arrays)
    memset(chunk3MetaPage, 0, sizeof(chunk3MetaPage));
    shared_ptr<Bitmap> bitmap = make_shared<Bitmap>(chunksize_ / blocksize_);
    FakeEncodeChunk(chunk3MetaPage, correctedSn, sn, bitmap, location);

    // case1:输入错误的参数
    {
        // size != chunksize
        EXPECT_EQ(CSErrorCode::InvalidArgError,
                  dataStore->CreateCloneChunk(id,
                                              sn,
                                              correctedSn,
                                              blocksize_,
                                              location));

        // sn == 0
        EXPECT_EQ(CSErrorCode::InvalidArgError,
                  dataStore->CreateCloneChunk(id,
                                              0,
                                              correctedSn,
                                              chunksize_,
                                              location));

        // location is empty
        EXPECT_EQ(CSErrorCode::InvalidArgError,
                  dataStore->CreateCloneChunk(id,
                                              sn,
                                              correctedSn,
                                              chunksize_,
                                              ""));
    }

    // case2:指定的chunk不存在，指定chunksize与配置一致
    {
        // create new chunk and open it
        string chunk3Path = string(baseDir) + "/" +
                            FileNameOperator::GenerateChunkFileName(id);
        // expect call chunkfile pool GetFile
        EXPECT_CALL(*lfs_, FileExists(chunk3Path))
            .WillOnce(Return(false));
        EXPECT_CALL(*fpool_, GetFileImpl(chunk3Path, NotNull()))
            .WillOnce(Return(0));
        EXPECT_CALL(*lfs_, Open(chunk3Path, _))
            .Times(1)
            .WillOnce(Return(4));
        // will read metapage
        EXPECT_CALL(*lfs_, Read(4, NotNull(), 0, metapagesize_))
            .WillOnce(DoAll(SetArrayArgument<1>(chunk3MetaPage,
                            chunk3MetaPage + metapagesize_),
                            Return(metapagesize_)));
        EXPECT_EQ(CSErrorCode::Success,
                  dataStore->CreateCloneChunk(id,
                                              sn,
                                              correctedSn,
                                              chunksize_,
                                              location));
        // 检查生成的clone chunk信息
        ASSERT_EQ(CSErrorCode::Success, dataStore->GetChunkInfo(id, &info));
        ASSERT_EQ(id, info.chunkId);
        ASSERT_EQ(sn, info.curSn);
        ASSERT_EQ(0, info.snapSn);
        ASSERT_EQ(correctedSn, info.correctedSn);
        ASSERT_TRUE(info.isClone);
        ASSERT_STREQ(location, info.location.c_str());
        ASSERT_EQ(Bitmap::NO_POS, info.bitmap->NextSetBit(0));
    }

    // case3:指定的chunk存在，参数与原chunk一致
    {
        EXPECT_EQ(CSErrorCode::Success,
                  dataStore->CreateCloneChunk(id,
                                              sn,
                                              correctedSn,
                                              chunksize_,
                                              location));
        // 检查生成的clone chunk信息
        ASSERT_EQ(CSErrorCode::Success, dataStore->GetChunkInfo(id, &info));
        ASSERT_EQ(id, info.chunkId);
        ASSERT_EQ(sn, info.curSn);
        ASSERT_EQ(0, info.snapSn);
        ASSERT_EQ(correctedSn, info.correctedSn);
        ASSERT_TRUE(info.isClone);
        ASSERT_STREQ(location, info.location.c_str());
        ASSERT_EQ(Bitmap::NO_POS, info.bitmap->NextSetBit(0));
    }

    // case4:指定的chunk存在，参数与原chunk不一致
    // 返回ChunkConflictError，但是不会改变原chunk信息
    {
        // 版本不一致
        EXPECT_EQ(CSErrorCode::ChunkConflictError,
                  dataStore->CreateCloneChunk(id,
                                              sn + 1,
                                              correctedSn,
                                              chunksize_,
                                              location));
        // correctedSn不一致
        EXPECT_EQ(CSErrorCode::ChunkConflictError,
                  dataStore->CreateCloneChunk(id,
                                              sn,
                                              correctedSn + 1,
                                              chunksize_,
                                              location));
        // location不一致
        EXPECT_EQ(CSErrorCode::ChunkConflictError,
                  dataStore->CreateCloneChunk(id,
                                              sn,
                                              correctedSn,
                                              chunksize_,
                                              "temp"));
        // 检查生成的clone chunk信息
        ASSERT_EQ(CSErrorCode::Success, dataStore->GetChunkInfo(id, &info));
        ASSERT_EQ(id, info.chunkId);
        ASSERT_EQ(sn, info.curSn);
        ASSERT_EQ(0, info.snapSn);
        ASSERT_EQ(correctedSn, info.correctedSn);
        ASSERT_TRUE(info.isClone);
        ASSERT_STREQ(location, info.location.c_str());
        ASSERT_EQ(Bitmap::NO_POS, info.bitmap->NextSetBit(0));
    }

    // case5:指定的chunk存在，指定chunksize与配置不一致
    // 返回InvalidArgError，但是不会改变原chunk信息
    {
        EXPECT_EQ(CSErrorCode::InvalidArgError,
                  dataStore->CreateCloneChunk(id,
                                              sn,
                                              correctedSn,
                                              chunksize_ + metapagesize_,
                                              location));
        // 检查生成的clone chunk信息
        ASSERT_EQ(CSErrorCode::Success, dataStore->GetChunkInfo(id, &info));
        ASSERT_EQ(id, info.chunkId);
        ASSERT_EQ(sn, info.curSn);
        ASSERT_EQ(0, info.snapSn);
        ASSERT_EQ(correctedSn, info.correctedSn);
        ASSERT_TRUE(info.isClone);
        ASSERT_STREQ(location, info.location.c_str());
        ASSERT_EQ(Bitmap::NO_POS, info.bitmap->NextSetBit(0));
    }

    // case6:已存在chunk，chunk不是clone chunk
    {
        // location 为空
        EXPECT_EQ(CSErrorCode::InvalidArgError,
                  dataStore->CreateCloneChunk(1,  // id
                                              2,  // sn
                                              0,  // correctedSn
                                              chunksize_,
                                              ""));

        // location 不为空
        EXPECT_EQ(CSErrorCode::ChunkConflictError,
                  dataStore->CreateCloneChunk(1,  // id
                                              2,  // sn
                                              0,  // correctedSn
                                              chunksize_,
                                              location));
    }

    EXPECT_CALL(*lfs_, Close(1))
        .Times(1);
    EXPECT_CALL(*lfs_, Close(2))
        .Times(1);
    EXPECT_CALL(*lfs_, Close(3))
        .Times(1);
    EXPECT_CALL(*lfs_, Close(4))
        .Times(1);
}

/**
 * CreateCloneChunkErrorTest
 * case:chunk不存在，调chunkFile->Open的时候失败
 * 预期结果:创建clone chunk失败
 */
TEST_P(CSDataStore_test, CreateCloneChunkErrorTest) {
    // initialize
    FakeEnv();
    EXPECT_TRUE(dataStore->Initialize());

    ChunkID id = 3;
    SequenceNum sn = 1;
    SequenceNum correctedSn = 2;
    CSChunkInfo info;
    // create new chunk and open it
    string chunk3Path = string(baseDir) + "/" +
                        FileNameOperator::GenerateChunkFileName(id);
    // expect call chunk file pool GetFile
    EXPECT_CALL(*lfs_, FileExists(chunk3Path))
        .WillOnce(Return(false));
    EXPECT_CALL(*fpool_, GetFileImpl(chunk3Path, NotNull()))
        .WillOnce(Return(-UT_ERRNO));
    EXPECT_EQ(CSErrorCode::InternalError,
              dataStore->CreateCloneChunk(id,
                                          sn,
                                          correctedSn,
                                          chunksize_,
                                          location));
    // 检查生成的clone chunk信息
    ASSERT_EQ(CSErrorCode::ChunkNotExistError,
              dataStore->GetChunkInfo(id, &info));

    EXPECT_CALL(*lfs_, Close(1))
        .Times(1);
    EXPECT_CALL(*lfs_, Close(2))
        .Times(1);
    EXPECT_CALL(*lfs_, Close(3))
        .Times(1);
}

/**
 * PasteChunkTedt
 * case1:chunk 不存在
 * 预期结果1:返回ChunkNotExistError
 * case2:chunk存在，请求偏移超过chunk文件大小或偏移长度未对齐
 * 预期结果2:返回InvalidArgError
 * case3:chunk存在，但不是clone chunk
 * 预期结果3:返回成功
 * case4:chunk存在，且是clone chunk，写入区域之前未写过
 * 预期结果4:写入数据并更新bitmap
 * case5:chunk存在，且是clone chunk，写入区域之前已写过
 * 预期结果5:无数据写入，且不会更新bitmap
 * case6:chunk存在，且是clone chunk，部分区域已写过，部分未写过
 * 预期结果6:只写入未写过数据，并更新bitmap
 * case7:遍写整个chunk
 * 预期结果7:数据写入未写过区域，然后clone chunk会被转为普通chunk
 */
TEST_P(CSDataStore_test, PasteChunkTest1) {
    // initialize
    FakeEnv();
    EXPECT_TRUE(dataStore->Initialize());

    ChunkID id = 3;
    SequenceNum sn = 1;
    SequenceNum correctedSn = 2;
    off_t offset = 0;
    size_t length = blocksize_;
    char* buf = new char[length];
    memset(buf, 0, length);
    CSChunkInfo info;
    // 创建 clone chunk
    {
        char chunk3MetaPage[metapagesize_];  // NOLINT(runtime/arrays)
        memset(chunk3MetaPage, 0, sizeof(chunk3MetaPage));
        shared_ptr<Bitmap> bitmap =
            make_shared<Bitmap>(chunksize_ / blocksize_);
        FakeEncodeChunk(chunk3MetaPage, correctedSn, sn, bitmap, location);
        // create new chunk and open it
        string chunk3Path = string(baseDir) + "/" +
                            FileNameOperator::GenerateChunkFileName(id);
        // expect call chunkfile pool GetFile
        EXPECT_CALL(*lfs_, FileExists(chunk3Path))
            .WillOnce(Return(false));
        EXPECT_CALL(*fpool_, GetFileImpl(chunk3Path, NotNull()))
            .WillOnce(Return(0));
        EXPECT_CALL(*lfs_, Open(chunk3Path, _))
            .Times(1)
            .WillOnce(Return(4));
        // will read metapage
        EXPECT_CALL(*lfs_, Read(4, NotNull(), 0, metapagesize_))
            .WillOnce(DoAll(SetArrayArgument<1>(chunk3MetaPage,
                            chunk3MetaPage + metapagesize_),
                            Return(metapagesize_)));
        EXPECT_EQ(CSErrorCode::Success,
                  dataStore->CreateCloneChunk(id,
                                              sn,
                                              correctedSn,
                                              chunksize_,
                                              location));
    }

    // case1:chunk 不存在
    {
        id = 4;  // not exist
        ASSERT_EQ(CSErrorCode::ChunkNotExistError,
                  dataStore->PasteChunk(id,
                                        buf,
                                        offset,
                                        length));
    }

    // case2:chunk存在，请求偏移超过chunk文件大小或偏移长度未对齐
    {
        id = 3;  // not exist
        offset = chunksize_;
        ASSERT_EQ(CSErrorCode::InvalidArgError,
                  dataStore->PasteChunk(id,
                                        buf,
                                        offset,
                                        length));
        offset = blocksize_ - 1;
        length = blocksize_;
        ASSERT_EQ(CSErrorCode::InvalidArgError,
                  dataStore->PasteChunk(id,
                                        buf,
                                        offset,
                                        length));
        offset = blocksize_;
        length = blocksize_ + 1;
        ASSERT_EQ(CSErrorCode::InvalidArgError,
                  dataStore->PasteChunk(id,
                                        buf,
                                        offset,
                                        length));
    }

    // case3:chunk存在，但不是clone chunk
    {
        EXPECT_CALL(*lfs_, Write(_, Matcher<const char*>(NotNull()), _, _))
            .Times(0);

        // 快照不存在
        id = 2;
        offset = 0;
        length = blocksize_;
        ASSERT_EQ(CSErrorCode::Success,
                  dataStore->PasteChunk(id,
                                        buf,
                                        offset,
                                        length));

        // 快照存在
        id = 1;
        offset = 0;
        ASSERT_EQ(CSErrorCode::Success,
                  dataStore->PasteChunk(id,
                                        buf,
                                        offset,
                                        length));
    }

    // case4:chunk存在，且是clone chunk，写入区域之前未写过
    {
        id = 3;  // not exist
        offset = blocksize_;
        length = 2 * blocksize_;
        EXPECT_CALL(*lfs_, Write(4, Matcher<const char*>(NotNull()),
                                 metapagesize_ + offset, length))
            .Times(1);
        // update metapage
        EXPECT_CALL(*lfs_,
                    Write(4, Matcher<const char*>(NotNull()), 0, metapagesize_))
            .Times(1);
        ASSERT_EQ(CSErrorCode::Success,
                  dataStore->PasteChunk(id,
                                        buf,
                                        offset,
                                        length));
        // 检查paste后chunk的状态
        ASSERT_EQ(CSErrorCode::Success, dataStore->GetChunkInfo(id, &info));
        ASSERT_EQ(true, info.isClone);
        ASSERT_EQ(1, info.bitmap->NextSetBit(0));
        ASSERT_EQ(3, info.bitmap->NextClearBit(1));
        ASSERT_EQ(Bitmap::NO_POS, info.bitmap->NextSetBit(3));
    }

    // case5:chunk存在，且是clone chunk，写入区域之前已写过
    {
        id = 3;  // not exist
        offset = blocksize_;
        length = 2 * blocksize_;
        EXPECT_CALL(*lfs_, Write(4, Matcher<const char*>(NotNull()),
                                 metapagesize_ + offset, length))
            .Times(0);
        EXPECT_CALL(*lfs_,
                    Write(4, Matcher<const char*>(NotNull()), 0, metapagesize_))
            .Times(0);
        ASSERT_EQ(CSErrorCode::Success,
                  dataStore->PasteChunk(id,
                                        buf,
                                        offset,
                                        length));
        // paste后，chunk的状态不变
        ASSERT_EQ(CSErrorCode::Success, dataStore->GetChunkInfo(id, &info));
        ASSERT_EQ(true, info.isClone);
        ASSERT_EQ(1, info.bitmap->NextSetBit(0));
        ASSERT_EQ(3, info.bitmap->NextClearBit(1));
        ASSERT_EQ(Bitmap::NO_POS, info.bitmap->NextSetBit(3));
    }
    // case6:chunk存在，且是clone chunk，部分区域已写过，部分未写过
    {
        id = 3;  // not exist
        offset = 0;
        length = 4 * blocksize_;
        // [2 * blocksize_, 4 * blocksize_)区域已写过，[0, blocksize_)为metapage
        EXPECT_CALL(*lfs_, Write(4, Matcher<const char*>(NotNull()),
                                 metapagesize_, blocksize_))
            .Times(1);
        EXPECT_CALL(*lfs_, Write(4, Matcher<const char*>(NotNull()),
                                 metapagesize_ + 3 * blocksize_, blocksize_))
            .Times(1);
        EXPECT_CALL(*lfs_,
                    Write(4, Matcher<const char*>(NotNull()), 0, metapagesize_))
            .Times(1);
        ASSERT_EQ(CSErrorCode::Success,
                  dataStore->PasteChunk(id, buf, offset, length));
        // paste后，chunk的状态不变
        ASSERT_EQ(CSErrorCode::Success, dataStore->GetChunkInfo(id, &info));
        ASSERT_EQ(true, info.isClone);
        ASSERT_EQ(0, info.bitmap->NextSetBit(0));
        ASSERT_EQ(4, info.bitmap->NextClearBit(0));
        ASSERT_EQ(Bitmap::NO_POS, info.bitmap->NextSetBit(4));
    }
    // case7:遍写整个chunk
    {
        id = 3;  // not exist
        offset = 0;
        length = chunksize_;
        // [blocksize_, 4 * blocksize_)区域已写过，[0, blocksize_)为metapage
        EXPECT_CALL(*lfs_, Write(4,
                                 Matcher<const char*>(NotNull()),
                                 metapagesize_ + 4 * blocksize_,
                                 chunksize_ - 4 * blocksize_))
            .Times(1);
        EXPECT_CALL(*lfs_,
                    Write(4, Matcher<const char*>(NotNull()), 0, metapagesize_))
            .Times(1);
        ASSERT_EQ(CSErrorCode::Success,
                  dataStore->PasteChunk(id,
                                        buf,
                                        offset,
                                        length));
        // paste后，chunk的状态不变
        ASSERT_EQ(CSErrorCode::Success, dataStore->GetChunkInfo(id, &info));
        ASSERT_EQ(false, info.isClone);
        ASSERT_EQ(nullptr, info.bitmap);
    }

    EXPECT_CALL(*lfs_, Close(1))
        .Times(1);
    EXPECT_CALL(*lfs_, Close(2))
        .Times(1);
    EXPECT_CALL(*lfs_, Close(3))
        .Times(1);
    EXPECT_CALL(*lfs_, Close(4))
        .Times(1);
    delete[] buf;
}

/*
 * PasteChunkErrorTest
 * case1:写数据时失败
 * 预期结果1:返回InternalError，chunk状态不变
 * case2:更新metapage时失败
 * 预期结果2:返回InternalError，chunk状态不变
 */
TEST_P(CSDataStore_test, PasteChunkErrorTest1) {
    // initialize
    FakeEnv();
    EXPECT_TRUE(dataStore->Initialize());

    ChunkID id = 3;
    SequenceNum sn = 1;
    SequenceNum correctedSn = 2;
    off_t offset = 0;
    size_t length = blocksize_;
    char* buf = new char[length];  // NOLINT
    memset(buf, 0, length);
    CSChunkInfo info;
    // 创建 clone chunk
    {
        char chunk3MetaPage[metapagesize_];  // NOLINT(runtime/arrays)
        memset(chunk3MetaPage, 0, sizeof(chunk3MetaPage));
        shared_ptr<Bitmap> bitmap =
            make_shared<Bitmap>(chunksize_ / blocksize_);
        FakeEncodeChunk(chunk3MetaPage, correctedSn, sn, bitmap, location);
        // create new chunk and open it
        string chunk3Path = string(baseDir) + "/" +
                            FileNameOperator::GenerateChunkFileName(id);
        // expect call chunkfile pool GetFile
        EXPECT_CALL(*lfs_, FileExists(chunk3Path))
            .WillOnce(Return(false));
        EXPECT_CALL(*fpool_, GetFileImpl(chunk3Path, NotNull()))
            .WillOnce(Return(0));
        EXPECT_CALL(*lfs_, Open(chunk3Path, _))
            .Times(1)
            .WillOnce(Return(4));
        // will read metapage
        EXPECT_CALL(*lfs_, Read(4, NotNull(), 0, metapagesize_))
            .WillOnce(DoAll(SetArrayArgument<1>(chunk3MetaPage,
                            chunk3MetaPage + metapagesize_),
                            Return(metapagesize_)));
        EXPECT_EQ(CSErrorCode::Success,
                  dataStore->CreateCloneChunk(id,
                                              sn,
                                              correctedSn,
                                              chunksize_,
                                              location));
    }
    // case1:写数据时失败
    {
        id = 3;  // not exist
        offset = blocksize_;
        length = 2 * blocksize_;
        EXPECT_CALL(*lfs_, Write(4, Matcher<const char*>(NotNull()),
                                 metapagesize_ + offset, length))
            .WillOnce(Return(-UT_ERRNO));
        // update metapage
        EXPECT_CALL(*lfs_,
                    Write(4, Matcher<const char*>(NotNull()), 0, metapagesize_))
            .Times(0);
        ASSERT_EQ(CSErrorCode::InternalError,
                  dataStore->PasteChunk(id,
                                        buf,
                                        offset,
                                        length));
        // 检查paste后chunk的状态
        ASSERT_EQ(CSErrorCode::Success, dataStore->GetChunkInfo(id, &info));
        ASSERT_EQ(true, info.isClone);
        ASSERT_EQ(Bitmap::NO_POS, info.bitmap->NextSetBit(0));
    }
    // case2:更新metapage时失败
    {
        id = 3;  // not exist
        offset = blocksize_;
        length = 2 * blocksize_;
        EXPECT_CALL(*lfs_, Write(4, Matcher<const char*>(NotNull()),
                                 metapagesize_ + offset, length))
            .Times(1);
        // update metapage
        EXPECT_CALL(*lfs_,
                    Write(4, Matcher<const char*>(NotNull()), 0, metapagesize_))
            .WillOnce(Return(-UT_ERRNO));
        ASSERT_EQ(CSErrorCode::InternalError,
                  dataStore->PasteChunk(id,
                                        buf,
                                        offset,
                                        length));
        // 检查paste后chunk的状态
        ASSERT_EQ(CSErrorCode::Success, dataStore->GetChunkInfo(id, &info));
        ASSERT_EQ(true, info.isClone);
        ASSERT_EQ(Bitmap::NO_POS, info.bitmap->NextSetBit(0));
    }

    EXPECT_CALL(*lfs_, Close(1))
        .Times(1);
    EXPECT_CALL(*lfs_, Close(2))
        .Times(1);
    EXPECT_CALL(*lfs_, Close(3))
        .Times(1);
    EXPECT_CALL(*lfs_, Close(4))
        .Times(1);
    delete[] buf;
}

/*
 * chunk不存在
 */
TEST_P(CSDataStore_test, GetHashErrorTest1) {
    // initialize
    FakeEnv();
    EXPECT_TRUE(dataStore->Initialize());

    ChunkID id = 3;
    std::string hash;

    // test chunk not exists
    EXPECT_EQ(CSErrorCode::ChunkNotExistError,
              dataStore->GetChunkHash(id,
                                      0,
                                      4096,
                                      &hash));

    EXPECT_CALL(*lfs_, Close(1))
        .Times(1);
    EXPECT_CALL(*lfs_, Close(2))
        .Times(1);
    EXPECT_CALL(*lfs_, Close(3))
        .Times(1);
}

/*
 * read报错
 */
TEST_P(CSDataStore_test, GetHashErrorTest2) {
    // initialize
    FakeEnv();
    EXPECT_TRUE(dataStore->Initialize());

    ChunkID id = 1;
    std::string hash;
    off_t offset = 0;
    size_t length = metapagesize_ + chunksize_;
    // test read chunk failed
    EXPECT_CALL(*lfs_, Read(1, NotNull(), 0, 4096))
        .WillOnce(Return(-UT_ERRNO));
    EXPECT_EQ(CSErrorCode::InternalError,
              dataStore->GetChunkHash(id,
                                      0,
                                      4096,
                                      &hash));
    EXPECT_CALL(*lfs_, Close(1))
        .Times(1);
    EXPECT_CALL(*lfs_, Close(2))
        .Times(1);
    EXPECT_CALL(*lfs_, Close(3))
        .Times(1);
}

/*
 * 获取datastore状态测试
 */
TEST_P(CSDataStore_test, GetStatusTest) {
    // initialize
    FakeEnv();
    EXPECT_TRUE(dataStore->Initialize());

    DataStoreStatus status;
    status = dataStore->GetStatus();
    ASSERT_EQ(2, status.chunkFileCount);
    // ASSERT_EQ(1, status.snapshotCount);

    EXPECT_CALL(*lfs_, Close(1))
        .Times(1);
    EXPECT_CALL(*lfs_, Close(2))
        .Times(1);
    EXPECT_CALL(*lfs_, Close(3))
        .Times(1);
}

INSTANTIATE_TEST_CASE_P(
    CSDataStoreTest,
    CSDataStore_test,
    ::testing::Values(
        //                chunk size        block size,     metapagesize
        std::make_tuple(16U * 1024 * 1024, 4096U, 4096U),
        std::make_tuple(16U * 1024 * 1024, 4096U, 8192U),
        std::make_tuple(16U * 1024 * 1024, 512U, 8192U),
        std::make_tuple(16U * 1024 * 1024, 512U, 4096U * 4)));

}  // namespace chunkserver
}  // namespace curve
