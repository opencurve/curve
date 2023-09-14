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
         * Construct initial environment
         * There are two chunks in the datastore, chunk1 and chunk2
         * The sn of chunk1 and chunk2 are both 2, and correctSn is 0
         * chunk1 has a snapshot file with version number 1
         * chunk2 does not have a snapshot file
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
 * Case: Test the case where the construction parameter is empty
 * Expected result: Process exited
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
 * Case: There is an unknown type of file
 * Expected result: Delete the file and return true
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
 * Case: There is a snapshot file, but the snapshot file does not have a corresponding chunk
 * Expected result: Delete the snapshot file and return true
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
 * Case: Chunk file exists, Chunk file has snapshot file
 * Expected result: Loading the file normally, returning true
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
 * Case: There is a chunk file, and there is a snapshot file in the chunk file,
 *        When listing, snapshots are listed before chunk files
 * Expected result: Returns true
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
 * Case: There is a chunk file, and there are two conflicting snapshot files in the chunk file
 * Expected result: returns false
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
 * Case: The data directory does not exist, creating the directory failed
 * Expected result: returns false
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
 * Case: List directory failed
 * Expected result: returns false
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
 * Case: Error opening chunk file
 * Expected result: returns false
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
 * Case: Error opening snapshot file
 * Expected result: returns false
 */
TEST_P(CSDataStore_test, InitializeErrorTest4) {
    // test chunk open failed
    FakeEnv();
    // set open snapshot file failed
    EXPECT_CALL(*lfs_, Open(chunk1snap1Path, _))
        .WillOnce(Return(-UT_ERRNO));
    EXPECT_FALSE(dataStore->Initialize());

    // Each reinitialization will release the original resources and reload them
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

    // Each reinitialization will release the original resources and reload them
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

    // Each reinitialization will release the original resources and reload them
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

    // Each reinitialization will release the original resources and reload them
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

    // Each reinitialization will release the original resources and reload them
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
 * Case: There is a chunk file, and there is a snapshot file in the chunk file,
 *       When listing, snapshots are listed before chunk files
 *       Error opening chunk file
 * Expected result: returns false
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
 * Case: chunk does not exist
 * Expected result: Create chunk file and successfully write data
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

    // If sn is 0, returns InvalidArgError
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
 * Case: chunk exists, request sn smaller than chunk's sn
 * Expected result: Refused writing, returned BackwardRequestError
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
 * Case: chunk exists, request correctedSn with sn less than chunk
 * Expected result: Refused writing, returned BackwardRequestError
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
 * Case: chunk exists, request sn to be equal to the SN of the chunk and not less than correctSn
 *       chunk does not have a snapshot
 * Expected result: Directly write data to chunk file
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
 * Case: chunk exists, request sn is greater than the sn of the chunk, equal to correctSn,
 *       chunk does not have a snapshot
 * Expected result: Metapage will be updated and data will be written to the chunk file
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
 * Case: chunk exists, request sn greater than Chunk's sn and correctSn,
 *       chunk does not have a snapshot
 * Expected result: A snapshot file will be created, and the metapage will be updated,
 * When writing data, first perform a Copy-On-Write operation to the snapshot, and then write to the chunk file
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

    // Write data for the same block again, no longer co w, but directly write the data
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

    // sn - 1 < chunk. sn, returns BackwardRequestError
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
 * Case: chunk exists, request sn to be equal to the SN of the chunk and not less than correctSn
 *       chunk has a snapshot
 * Expected result: When writing data, first perform a Copy-On-Write operation to the snapshot, and then write to the chunk file
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
 * Case: chunk exists, request sn is greater than the sn of the chunk, equal to correctSn
 *       chunk has a snapshot
 * Expected result: Update the metapage and write the chunk file
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
 * Case: chunk exists, requesting sn and correctSn with sn greater than chunk
 *       chunk has a snapshot, snapsn<chunk.sn
 * Expected result: There are historical snapshots that have not been deleted, write failed, and a SnapshotConflictError is returned
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
 * Write a clone chunk to simulate cloning
 * Case1: clone chunk exists and has not been written before writing to the region
 * Expected result 1: Write data and update bitmap
 * Case2: clone chunk exists and has been written before writing to the region
 * Expected result 2: Write data but not update bitmap
 * Case3: chunk exists and is a clone chunk. Some areas have been written, while others have not
 * Expected result 3: Write data and update bitmap
 * Case4: Overwrite the entire chunk
 * Expected result 4: Write data, and then the clone chunk will be converted to a regular chunk
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
    // Create clone chunk
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

    // Case1: chunk exists and is a clone chunk, which has not been written before writing to the region
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
        // Check the status of chunk after paste
        ASSERT_EQ(CSErrorCode::Success, dataStore->GetChunkInfo(id, &info));
        ASSERT_EQ(true, info.isClone);
        ASSERT_EQ(1, info.bitmap->NextSetBit(0));
        ASSERT_EQ(3, info.bitmap->NextClearBit(1));
        ASSERT_EQ(Bitmap::NO_POS, info.bitmap->NextSetBit(3));
    }

    // Case2: chunk exists and is a clone chunk, which has been written before writing to the region
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
        // After paste, the state of the chunk remains unchanged
        ASSERT_EQ(CSErrorCode::Success, dataStore->GetChunkInfo(id, &info));
        ASSERT_EQ(true, info.isClone);
        ASSERT_EQ(1, info.bitmap->NextSetBit(0));
        ASSERT_EQ(3, info.bitmap->NextClearBit(1));
        ASSERT_EQ(Bitmap::NO_POS, info.bitmap->NextSetBit(3));
    }

    // Case3: chunk exists and is a clone chunk. Some areas have been written, while others have not
    {
        LOG(INFO) << "case 4";
        id = 3;  // not exist
        offset = 0;
        length = 4 * blocksize_;

        std::unique_ptr<char[]> buf(new char[length]);

        // The [2 * blocksize_, 4 * blocksize_) area has been written
        // [0, metapagesize_) is the metapage
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
        // After paste, the state of the chunk remains unchanged
        ASSERT_EQ(CSErrorCode::Success, dataStore->GetChunkInfo(id, &info));
        ASSERT_EQ(true, info.isClone);
        ASSERT_EQ(0, info.bitmap->NextSetBit(0));
        ASSERT_EQ(4, info.bitmap->NextClearBit(0));
        ASSERT_EQ(Bitmap::NO_POS, info.bitmap->NextSetBit(4));
    }

    // Case4: Overwrite the entire chun
    {
        LOG(INFO) << "case 5";
        id = 3;  // not exist
        offset = 0;
        length = chunksize_;

        std::unique_ptr<char[]> buf(new char[length]);

        // The [blocksize_, 4 * blocksize_) area has been written
        // [0, metapagesize_) is the metapage
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
        // After paste, the state of the chunk remains unchanged
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
 * Write clone chunk to simulate recovery
 * Case1: clone chunk exists, sn<chunk. sn || sn<chunk.correctedSn
 * Expected result 1: Write data, and then the clone chunk will be converted to a regular chunk
 * Case2: clone chunk exists, sn>chunk.sn, sn==chunk.correctedsn
 * Expected result 2: Write data and update bitmap, update chunk.sn to sn
 * Case3: clone chunk exists, sn==chunk.sn, sn==chunk.correctedsn
 * Expected result 3: Write data and update bitmap
 * Case4: clone chunk exists, sn>chunk.sn, sn>chunk.correctedsn
 * Expected result 4: Returning StatusConflictError
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
    // Create clone chunk
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

    // Case1: clone chunk exists
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

    // Case2: chunk exists and is a clone chunk,
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
        // Check the status of chunk after paste
        ASSERT_EQ(CSErrorCode::Success, dataStore->GetChunkInfo(id, &info));
        ASSERT_EQ(3, info.curSn);
        ASSERT_EQ(3, info.correctedSn);
        ASSERT_EQ(0, info.snapSn);
        ASSERT_EQ(true, info.isClone);
        ASSERT_EQ(1, info.bitmap->NextSetBit(0));
        ASSERT_EQ(3, info.bitmap->NextClearBit(1));
        ASSERT_EQ(Bitmap::NO_POS, info.bitmap->NextSetBit(3));
    }

    // Case3: chunk exists and is a clone chunk
    // sn > chunk.sn;sn == correctedsn
    {
        LOG(INFO) << "case 3";
        offset = 0;
        length = 4 * blocksize_;

        std::unique_ptr<char[]> buf(new char[length]);

        // The [2 * blocksize_, 4 * blocksize_) area has been written
        // [0, blocksize_) is the metapage
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
        // After paste, the state of the chunk remains unchanged
        ASSERT_EQ(CSErrorCode::Success, dataStore->GetChunkInfo(id, &info));
        ASSERT_EQ(3, info.curSn);
        ASSERT_EQ(3, info.correctedSn);
        ASSERT_EQ(0, info.snapSn);
        ASSERT_EQ(true, info.isClone);
        ASSERT_EQ(0, info.bitmap->NextSetBit(0));
        ASSERT_EQ(4, info.bitmap->NextClearBit(0));
        ASSERT_EQ(Bitmap::NO_POS, info.bitmap->NextSetBit(4));
    }

    // Case3: chunk exists and is a clone chunk
    // sn > chunk.sn;sn > correctedsn
    {
        LOG(INFO) << "case 4";
        sn = 4;
        // Unable to write data
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
        // The state of the chunk remains unchanged
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
 * Case: chunk exists,
 *      sn==chunk.sn
 *      sn>chunk.correctedSn
 *      chunk.sn<snap.sn
 *      chunk has a snapshot
 * Expected result: When writing data, first perform a Copy-On-Write operation to the snapshot, and then write to the chunk file
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
 * Case: chunk exists,
 *       sn>chunk.sn
 *       sn>chunk.correctedSn
 *       chunk.sn==snap.sn
 *       chunk has a snapshot
 * Expected result: When writing data, first perform a Copy-On-Write operation to the snapshot, and then write to the chunk file
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
 * WriteChunkTest exception test
 * Case: Error creating snapshot file
 * Expected result: Write failed and will not change the current chunk state
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
 * WriteChunkTest exception test
 * Case: Successfully created snapshot file, failed to update metadata
 * Expected result: Write failed, resulting in a snapshot file, but the chunk version number will not change
 * Write again without generating a new snapshot file
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
 * WriteChunkTest exception test
 * Case: Successfully created snapshot file, updated metadata, and failed row
 * Expected result: Write failed, snapshot file generated, chunk version number changed,
 * The bitmap of the snapshot has not changed. If written again, it will still be cowed
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

    // Writing again will still slow down
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
 * WriteChunkTest exception test
 * Case: Successfully created snapshot file, updated metapage, row, and write data failed
 * Expected result: Write failed, snapshot file generated, chunk version number changed,
 * The bitmap of the snapshot has changed, write it again and directly write to the chunk file
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
    // Write directly to the chunk file again
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
 * Case: chunk does not exist
 * Expected result: Failed to create chunk file
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
 * The chunk written is a clone chunk
 * Case1: The request location is too long, causing the metapage size to exceed the page size
 * Expected result 1: Create clone chunk failed
 * Case2: Failed to write data
 * Expected result 2: InternalError returned, chunk status remains unchanged
 * Case3: Failed to update metapage
 * Expected result 3: InternalError returned, chunk status remains unchanged
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
    // Create clone chunk
    {
        string longLocation(kLocationLimit+1, 'a');
        EXPECT_EQ(CSErrorCode::InvalidArgError,
                  dataStore->CreateCloneChunk(id,
                                              sn,
                                              correctedSn,
                                              chunksize_,
                                              longLocation));
    }
    // Create clone chunk
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
    // Case1: Failed to write data
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
        // Check the status of chunk after paste
        ASSERT_EQ(CSErrorCode::Success, dataStore->GetChunkInfo(id, &info));
        ASSERT_EQ(true, info.isClone);
        ASSERT_EQ(Bitmap::NO_POS, info.bitmap->NextSetBit(0));
    }
    // Case2: Failed to update metapage
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
        // Check the status of chunk after paste
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
 * Case: chunk does not exist
 * Expected result: ChunkNotExistError error code returned
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
 * Case: chunk exists, reading area exceeds chunk size or offset and length are not aligned
 * Expected result: InvalidArgError error code returned
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
 * Case: Normal reading of existing chunks
 * Expected result: read successfully
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
 * Read clone chunk
 * Case1: The read area has not been written
 * Expected result: PageNerverWrittenError returned
 * Case2: The read area part has been written
 * Expected result: PageNerverWrittenError returned
 * Case3: The read area has been written
 * Expected result: Success returned, data successfully written
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

    // Case1: Read unwritten area
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

    // Case2: The read area part has been written
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

    // Case3: The read area has been written
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
 * Case: Error reading chunk file
 * Expected result: Read failed, returned InternalError
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
 * Case: chunk does not exist
 * Expected result: ChunkNotExistError error code returned
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
 * Case: chunk exists, request version number equal to Chunk version number
 * Expected result: Read chunk data
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
 * Case: chunk exists, request version number equal to snapshot version number
 * Expected result: Read data from snapshot
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
 * Case: chunk exists, but the requested version number does not exist
 * Expected result: ChunkNotExistError error code returned
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
 * Case: Failed to read snapshot
 * Expected result: InternalError returned
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
 * Case: chunk exists, request version number is equal to Chunk version number, failed while reading data
 * Expected result: InternalError returned
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
 * Case: chunk does not exist
 * Expected result: returned successfully
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
 * Case: Chunk has a snapshot file present
 * Expected result: Success returned, chunk deleted, snapshot deleted
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
 * Case: chunk exists, snapshot file does not exist
 * Expected result: returned successfully
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
 * chunk exists, snapshot file does not exist
 * Case1: sn<chunkinfo.sn
 * Expected result 1: BackwardRequestError returned
 * Case2: sn>chunkinfo.sn
 * Expected result 2: Success returned
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
 * Case: chunk exists, snapshot file does not exist, error occurred during recyclechunk
 * Expected result: returned successfully
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
 * Case: chunk does not exist
 * Expected result: returned successfully
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

// For DeleteSnapshotChunkOrCorrectSn, there are two main internal operations
// One is to delete the snapshot file, and the other is to modify correctedSn
// When there is a snapshot file, the sn of fileSn>=chunk is the only condition to determine whether to delete the snapshot
// For correctedSn, if fileSn is greater than chunk's sn and correctedSn is the judgment
// Do you want to modify the unique condition for correctedSn

/**
 * DeleteSnapshotChunkOrCorrectSnTest
 * Case: chunk exists, snapshot exists
 *       fileSn>=Chunk's sn
 *       fileSn==correctedSn of chunk
 *       chunk.sn>snap.sn
 * Expected result: Delete snapshot without modifying correctedSn, return success
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
 * Case: chunk exists, snapshot exists
 *       fileSn < chunk's sn
 *       At this point, regardless of the value of correctSn, correctedSn will not be modified
 * Expected result: Success returned, snapshot will not be deleted, correctedSn will not be modified
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

    // The following use case is used to supplement the DeleteSnapshotChunkOrCorrectSnTest2 use case
    // Boundary situation when fileSn == sn
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
 * Case: chunk exists, snapshot exists
 *       fileSn > chunk's sn and correctedSn
 * Expected result: Delete the snapshot and modify correctedSn, returning success
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
 * Case: chunk exists, snapshot does not exist
 *       fileSn <= SN or correctedSn of chunk
 * Expected result: CorrectedSn will not be modified, returning success
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
 * Case: chunk exists, snapshot does not exist
 *       fileSn > chunk's sn and correctedSn
 * Expected result: Modify correctedSn and return success
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
 * Case: chunk exists, snapshot does not exist, chunk is clone chunk
 * Expected result: Returning StatusConflictError
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

    // Returns StatusConflictError regardless of the number of correctedSn
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
 * Case: chunk exists, snapshot exists
 *       fileSn > chunk's sn
 *       fileSn > chunk's correctedSn
 *      chunk.sn==snap.sn
 * Expected result: Delete snapshot without modifying correctedSn, return success
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
 * Case: chunk exists, snapshot exists
 *       fileSn == SN of chunk
 *       fileSn == correctedSn of chunk
 *       chunk.sn<snap.sn
 * Expected result: Delete snapshot without modifying correctedSn, return success
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
 * Case: Failed to modify correctedSn
 * Expected result: Failed to return, the value of correctedSn has not changed
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
 * Case: Failed to recycle snapshot chunks
 * Expected result: return failed
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
 * Case1: The specified chunk does not exist, incorrect parameter input
 * Expected result 1: InvalidArgError returned
 * Case2: The specified chunk does not exist, the specified chunksize is consistent with the configuration
 * Expected result 2: Creation successful
 * Case3: The specified chunk exists, and the parameters are consistent with the original chunk
 * Expected result 3: Success returned
 * Case4: The specified chunk exists, and the parameters are inconsistent with the original chunk
 * Expected result 4: ChunkConflictError returned without changing the original chunk information
 * Case5: The specified chunk exists, but the specified chunk size is inconsistent with the configuration
 * Expected result 5: InvalidArgError returned without changing the original chunk information
 * Case6: The specified chunk exists, but the chunk is not a clone chunk. The parameters are consistent with the chunk information
 * Expected result: ChunkConflictError returned without changing the original chunk information
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

    // Case1: Input incorrect parameters
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

    // Case2: The specified chunk does not exist, the specified chunksize is consistent with the configuration
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
        // Check the generated clone chunk information
        ASSERT_EQ(CSErrorCode::Success, dataStore->GetChunkInfo(id, &info));
        ASSERT_EQ(id, info.chunkId);
        ASSERT_EQ(sn, info.curSn);
        ASSERT_EQ(0, info.snapSn);
        ASSERT_EQ(correctedSn, info.correctedSn);
        ASSERT_TRUE(info.isClone);
        ASSERT_STREQ(location, info.location.c_str());
        ASSERT_EQ(Bitmap::NO_POS, info.bitmap->NextSetBit(0));
    }

    // Case3: The specified chunk exists, and the parameters are consistent with the original chunk
    {
        EXPECT_EQ(CSErrorCode::Success,
                  dataStore->CreateCloneChunk(id,
                                              sn,
                                              correctedSn,
                                              chunksize_,
                                              location));
        // Check the generated clone chunk information
        ASSERT_EQ(CSErrorCode::Success, dataStore->GetChunkInfo(id, &info));
        ASSERT_EQ(id, info.chunkId);
        ASSERT_EQ(sn, info.curSn);
        ASSERT_EQ(0, info.snapSn);
        ASSERT_EQ(correctedSn, info.correctedSn);
        ASSERT_TRUE(info.isClone);
        ASSERT_STREQ(location, info.location.c_str());
        ASSERT_EQ(Bitmap::NO_POS, info.bitmap->NextSetBit(0));
    }

    // Case4: The specified chunk exists, and the parameters are inconsistent with the original chunk
    // Returns ChunkConflictError, but does not change the original chunk information
    {
        //Version inconsistency
        EXPECT_EQ(CSErrorCode::ChunkConflictError,
                  dataStore->CreateCloneChunk(id,
                                              sn + 1,
                                              correctedSn,
                                              chunksize_,
                                              location));
        // Inconsistent correctedSn
        EXPECT_EQ(CSErrorCode::ChunkConflictError,
                  dataStore->CreateCloneChunk(id,
                                              sn,
                                              correctedSn + 1,
                                              chunksize_,
                                              location));
        // Inconsistent location
        EXPECT_EQ(CSErrorCode::ChunkConflictError,
                  dataStore->CreateCloneChunk(id,
                                              sn,
                                              correctedSn,
                                              chunksize_,
                                              "temp"));
        // Check the generated clone chunk information
        ASSERT_EQ(CSErrorCode::Success, dataStore->GetChunkInfo(id, &info));
        ASSERT_EQ(id, info.chunkId);
        ASSERT_EQ(sn, info.curSn);
        ASSERT_EQ(0, info.snapSn);
        ASSERT_EQ(correctedSn, info.correctedSn);
        ASSERT_TRUE(info.isClone);
        ASSERT_STREQ(location, info.location.c_str());
        ASSERT_EQ(Bitmap::NO_POS, info.bitmap->NextSetBit(0));
    }

    // Case5: The specified chunk exists, but the specified chunksize is inconsistent with the configuration
    // Returns InvalidArgError, but does not change the original chunk information
    {
        EXPECT_EQ(CSErrorCode::InvalidArgError,
                  dataStore->CreateCloneChunk(id,
                                              sn,
                                              correctedSn,
                                              chunksize_ + metapagesize_,
                                              location));
        // Check the generated clone chunk information
        ASSERT_EQ(CSErrorCode::Success, dataStore->GetChunkInfo(id, &info));
        ASSERT_EQ(id, info.chunkId);
        ASSERT_EQ(sn, info.curSn);
        ASSERT_EQ(0, info.snapSn);
        ASSERT_EQ(correctedSn, info.correctedSn);
        ASSERT_TRUE(info.isClone);
        ASSERT_STREQ(location, info.location.c_str());
        ASSERT_EQ(Bitmap::NO_POS, info.bitmap->NextSetBit(0));
    }

    // Case6: Chunk already exists, chunk is not a clone chunk
    {
        // location is empty
        EXPECT_EQ(CSErrorCode::InvalidArgError,
                  dataStore->CreateCloneChunk(1,  // id
                                              2,  // sn
                                              0,  // correctedSn
                                              chunksize_,
                                              ""));

        // location is not empty
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
 * Case: chunk does not exist, failed when calling chunkFile->Open
 * Expected result: Failed to create clone chunk
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
    // Check the generated clone chunk information
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
 * Case1: Chunk does not exist
 * Expected result 1: ChunkNotExistError returned
 * Case2: chunk exists, requested offset exceeds chunk file size or offset length is not aligned
 * Expected result 2: InvalidArgError returned
 * Case3: chunk exists, but not clone chunk
 * Expected result 3: Success returned
 * Case4: chunk exists and is a clone chunk, which has not been written before writing to the region
 * Expected result 4: Write data and update bitmap
 * Case5: chunk exists and is a clone chunk, which has been written before writing to the region
 * Expected result 5: No data written and Bitmap will not be updated
 * Case6: chunk exists and is a clone chunk. Some areas have been written, while others have not
 * Expected result 6: Only write unwritten data and update bitmap
 * Case7: Overwrite the entire chunk
 * Expected result 7: Data is written to an unwritten area, and then the clone chunk will be converted to a regular chunk
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
    // Create clone chunk
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

    // Case1: chunk does not exist
    {
        id = 4;  // not exist
        ASSERT_EQ(CSErrorCode::ChunkNotExistError,
                  dataStore->PasteChunk(id,
                                        buf,
                                        offset,
                                        length));
    }

    // Case2: chunk exists, requested offset exceeds chunk file size or offset length is not aligned
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

    // Case3: chunk exists, but not clone chunk
    {
        EXPECT_CALL(*lfs_, Write(_, Matcher<const char*>(NotNull()), _, _))
            .Times(0);

        // The snapshot does not exist
        id = 2;
        offset = 0;
        length = blocksize_;
        ASSERT_EQ(CSErrorCode::Success,
                  dataStore->PasteChunk(id,
                                        buf,
                                        offset,
                                        length));

        // Snapshot exists
        id = 1;
        offset = 0;
        ASSERT_EQ(CSErrorCode::Success,
                  dataStore->PasteChunk(id,
                                        buf,
                                        offset,
                                        length));
    }

    // Case4: chunk exists and is a clone chunk, which has not been written before writing to the region
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
        // Check the status of chunk after paste
        ASSERT_EQ(CSErrorCode::Success, dataStore->GetChunkInfo(id, &info));
        ASSERT_EQ(true, info.isClone);
        ASSERT_EQ(1, info.bitmap->NextSetBit(0));
        ASSERT_EQ(3, info.bitmap->NextClearBit(1));
        ASSERT_EQ(Bitmap::NO_POS, info.bitmap->NextSetBit(3));
    }

    // Case5: chunk exists and is a clone chunk, which has been written before writing to the region
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
        // After paste, the state of the chunk remains unchanged
        ASSERT_EQ(CSErrorCode::Success, dataStore->GetChunkInfo(id, &info));
        ASSERT_EQ(true, info.isClone);
        ASSERT_EQ(1, info.bitmap->NextSetBit(0));
        ASSERT_EQ(3, info.bitmap->NextClearBit(1));
        ASSERT_EQ(Bitmap::NO_POS, info.bitmap->NextSetBit(3));
    }
    // Case6: chunk exists and is a clone chunk. Some areas have been written, while others have not
    {
        id = 3;  // not exist
        offset = 0;
        length = 4 * blocksize_;
        // [2 * blocksize_, 4 * blocksize_) area has been written, [0, blocksize_) is a metapage
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
        // After paste, the state of the chunk remains unchanged
        ASSERT_EQ(CSErrorCode::Success, dataStore->GetChunkInfo(id, &info));
        ASSERT_EQ(true, info.isClone);
        ASSERT_EQ(0, info.bitmap->NextSetBit(0));
        ASSERT_EQ(4, info.bitmap->NextClearBit(0));
        ASSERT_EQ(Bitmap::NO_POS, info.bitmap->NextSetBit(4));
    }
    // Case7: Overwrite the entire chunk
    {
        id = 3;  // not exist
        offset = 0;
        length = chunksize_;
        // [blocksize_, 4 * blocksize_) area has been written, [0, blocksize_) is a metapage
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
        // After paste, the state of the chunk remains unchanged
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
 * Case1: Failed to write data
 * Expected result 1: InternalError returned, chunk status remains unchanged
 * Case2: Failed to update metapage
 * Expected result 2: InternalError returned, chunk status remains unchanged
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
    // Create clone chunk
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
    // Case1: Failed to write data
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
        // Check the status of chunk after paste
        ASSERT_EQ(CSErrorCode::Success, dataStore->GetChunkInfo(id, &info));
        ASSERT_EQ(true, info.isClone);
        ASSERT_EQ(Bitmap::NO_POS, info.bitmap->NextSetBit(0));
    }
    // Case2: Failed to update metapage
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
        // Check the status of chunk after paste
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
 * Chunk does not exist
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
 * Read error
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
 * Obtain Datastore Status Test
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
