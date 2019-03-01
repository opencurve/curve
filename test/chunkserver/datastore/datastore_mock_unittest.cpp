/*
 * Project: curve
 * File Created: Friday, 7th September 2018 8:51:56 am
 * Author: tongguangxun
 * Copyright (c) 2018 NetEase
 */

#include <gtest/gtest.h>
#include <gmock/gmock.h>
#include <string.h>
#include <string>
#include <memory>

#include "include/chunkserver/chunkserver_common.h"
#include "src/common/bitmap.h"
#include "src/common/crc32.h"
#include "src/fs/local_filesystem.h"
#include "src/chunkserver/datastore/define.h"
#include "src/chunkserver/datastore/filename_operator.h"
#include "src/chunkserver/datastore/chunkserver_datastore.h"
#include "test/chunkserver/datastore/mock_chunkfile_pool.h"
#include "test/fs/mock_local_filesystem.h"

using curve::fs::LocalFileSystem;
using curve::fs::MockLocalFileSystem;
using curve::common::Bitmap;

using ::testing::_;
using ::testing::Ge;
using ::testing::Gt;
using ::testing::Return;
using ::testing::NotNull;
using ::testing::Mock;
using ::testing::Truly;
using ::testing::DoAll;
using ::testing::ReturnArg;
using ::testing::ElementsAre;
using ::testing::SetArgPointee;
using ::testing::SetArrayArgument;

namespace curve {
namespace chunkserver {

const ChunkSizeType CHUNK_SIZE = 16 * 1024 * 1024;
const PageSizeType PAGE_SIZE = 4096;
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

class CSDataStore_test : public testing::Test {
 public:
        void SetUp() {
            lfs_ = std::make_shared<MockLocalFileSystem>();
            fpool_ = std::make_shared<MockChunkfilePool>(lfs_);
            DataStoreOptions options;
            options.baseDir = baseDir;
            options.chunkSize = CHUNK_SIZE;
            options.pageSize = PAGE_SIZE;
            dataStore = std::make_shared<CSDataStore>(lfs_,
                                                      fpool_,
                                                      options);
            fdMock = 100;
            memset(chunk1MetaPage, 0, PAGE_SIZE);
            memset(chunk2MetaPage, 0, PAGE_SIZE);
            memset(chunk1SnapMetaPage, 0, PAGE_SIZE);
        }

        void TearDown() {}

        inline void FakeEncodeChunk(char* buf,
                                    SequenceNum correctedSn,
                                    SequenceNum sn) {
            ChunkFileMetaPage metaPage;
            metaPage.version = FORMAT_VERSION;
            metaPage.sn = sn;
            metaPage.correctedSn = correctedSn;
            metaPage.encode(buf);
        }

        inline void FakeEncodeSnapshot(char* buf,
                                       bool damaged,
                                       SequenceNum sn) {
            uint32_t bits = CHUNK_SIZE / PAGE_SIZE;
            SnapshotMetaPage metaPage;
            metaPage.version = FORMAT_VERSION;
            metaPage.sn = sn;
            metaPage.damaged = damaged;
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
            // fake fpool->GetChunk()
            ON_CALL(*fpool_, GetChunk(_, NotNull()))
                .WillByDefault(Return(0));
            EXPECT_CALL(*fpool_, RecycleChunk(_))
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
            fileInfo.st_size = CHUNK_SIZE + PAGE_SIZE;
            EXPECT_CALL(*lfs_, Fstat(_, _))
                .WillRepeatedly(DoAll(SetArgPointee<1>(fileInfo),
                                Return(0)));
            // fake Read
            ON_CALL(*lfs_, Read(Ge(1), NotNull(), Ge(0), Gt(0)))
                .WillByDefault(ReturnArg<3>());
            // fake Write
            ON_CALL(*lfs_, Write(Ge(1), NotNull(), Ge(0), Gt(0)))
                .WillByDefault(ReturnArg<3>());
            // fake read chunk1 metapage
            FakeEncodeChunk(chunk1MetaPage, 0, 2);
            EXPECT_CALL(*lfs_, Read(1, NotNull(), 0, PAGE_SIZE))
                .WillRepeatedly(DoAll(
                                SetArrayArgument<1>(chunk1MetaPage,
                                chunk1MetaPage + PAGE_SIZE),
                                Return(PAGE_SIZE)));
            // fake read chunk1's snapshot1 metapage
            FakeEncodeSnapshot(chunk1SnapMetaPage, false, 1);
            EXPECT_CALL(*lfs_, Read(2, NotNull(), 0, PAGE_SIZE))
                .WillRepeatedly(DoAll(
                                SetArrayArgument<1>(chunk1SnapMetaPage,
                                chunk1SnapMetaPage + PAGE_SIZE),
                                Return(PAGE_SIZE)));
            // fake read chunk2 metapage
            FakeEncodeChunk(chunk2MetaPage, 0, 2);
            EXPECT_CALL(*lfs_, Read(3, NotNull(), 0, PAGE_SIZE))
                .WillRepeatedly(DoAll(
                                SetArrayArgument<1>(chunk2MetaPage,
                                chunk2MetaPage + PAGE_SIZE),
                                Return(PAGE_SIZE)));
        }

 protected:
    int fdMock;
    std::shared_ptr<MockLocalFileSystem> lfs_;
    std::shared_ptr<MockChunkfilePool> fpool_;
    std::shared_ptr<CSDataStore>  dataStore;
    char chunk1MetaPage[PAGE_SIZE];
    char chunk2MetaPage[PAGE_SIZE];
    char chunk1SnapMetaPage[PAGE_SIZE];
};
/**
 * ConstructorTest
 * case:测试构造参数为空的情况
 * 预期结果:进程退出
 */
TEST_F(CSDataStore_test, ConstructorTest) {
    // null param test
    DataStoreOptions options;
    options.baseDir = baseDir;
    options.chunkSize = CHUNK_SIZE;
    options.pageSize = PAGE_SIZE;
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
TEST_F(CSDataStore_test, InitializeTest1) {
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
TEST_F(CSDataStore_test, InitializeTest2) {
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
TEST_F(CSDataStore_test, InitializeTest3) {
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
TEST_F(CSDataStore_test, InitializeTest4) {
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
TEST_F(CSDataStore_test, InitializeTest5) {
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
TEST_F(CSDataStore_test, InitializeErrorTest1) {
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
TEST_F(CSDataStore_test, InitializeErrorTest2) {
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
TEST_F(CSDataStore_test, InitializeErrorTest3) {
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
    // stat success but file size not equal CHUNK_SIZE + PAGE_SIZE
    struct stat fileInfo;
    fileInfo.st_size = CHUNK_SIZE;
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
    fileInfo.st_size = CHUNK_SIZE + PAGE_SIZE;
    EXPECT_CALL(*lfs_, Fstat(1, NotNull()))
        .WillOnce(DoAll(SetArgPointee<1>(fileInfo),
                        Return(0)));
    // read metapage failed
    EXPECT_CALL(*lfs_, Read(1, NotNull(), 0, PAGE_SIZE))
        .WillOnce(Return(-UT_ERRNO));
    EXPECT_FALSE(dataStore->Initialize());

    // open success
    EXPECT_CALL(*lfs_, Open(chunk1Path, _))
        .WillOnce(Return(1));
    // expect call close
    EXPECT_CALL(*lfs_, Close(1))
        .Times(1);
    // stat success
    fileInfo.st_size = CHUNK_SIZE + PAGE_SIZE;
    EXPECT_CALL(*lfs_, Fstat(1, NotNull()))
        .WillOnce(DoAll(SetArgPointee<1>(fileInfo),
                        Return(0)));
    // read metapage success, but version incompatible
    uint8_t version = FORMAT_VERSION + 1;
    memcpy(chunk1MetaPage, &version, sizeof(uint8_t));
    EXPECT_CALL(*lfs_, Read(1, NotNull(), 0, PAGE_SIZE))
                .WillOnce(DoAll(SetArrayArgument<1>(chunk1MetaPage,
                                chunk1MetaPage + PAGE_SIZE),
                                Return(PAGE_SIZE)));
    EXPECT_FALSE(dataStore->Initialize());

    // open success
    EXPECT_CALL(*lfs_, Open(chunk1Path, _))
        .WillOnce(Return(1));
    // expect call close
    EXPECT_CALL(*lfs_, Close(1))
        .Times(1);
    // stat success
    fileInfo.st_size = CHUNK_SIZE + PAGE_SIZE;
    EXPECT_CALL(*lfs_, Fstat(1, NotNull()))
        .WillOnce(DoAll(SetArgPointee<1>(fileInfo),
                        Return(0)));
    // read metapage success, but crc check failed
    version = FORMAT_VERSION;
    chunk1MetaPage[1] += 1;  // change the page data
    memcpy(chunk1MetaPage, &version, sizeof(uint8_t));
    EXPECT_CALL(*lfs_, Read(1, NotNull(), 0, PAGE_SIZE))
                .WillOnce(DoAll(SetArrayArgument<1>(chunk1MetaPage,
                                chunk1MetaPage + PAGE_SIZE),
                                Return(PAGE_SIZE)));
    EXPECT_FALSE(dataStore->Initialize());
}

/**
 * InitializeErrorTest
 * case:open 快照文件的时候出错
 * 预期结果:返回false
 */
TEST_F(CSDataStore_test, InitializeErrorTest4) {
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
    // stat success but file size not equal CHUNK_SIZE + PAGE_SIZE
    struct stat fileInfo;
    fileInfo.st_size = CHUNK_SIZE;
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
    fileInfo.st_size = CHUNK_SIZE + PAGE_SIZE;
    EXPECT_CALL(*lfs_, Fstat(2, NotNull()))
        .WillOnce(DoAll(SetArgPointee<1>(fileInfo),
                        Return(0)));
    // read metapage failed
    EXPECT_CALL(*lfs_, Read(2, NotNull(), 0, PAGE_SIZE))
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
    fileInfo.st_size = CHUNK_SIZE + PAGE_SIZE;
    EXPECT_CALL(*lfs_, Fstat(2, NotNull()))
        .WillOnce(DoAll(SetArgPointee<1>(fileInfo),
                        Return(0)));
    // read metapage success, but version incompatible
    uint8_t version = FORMAT_VERSION + 1;
    memcpy(chunk1SnapMetaPage, &version, sizeof(uint8_t));
    EXPECT_CALL(*lfs_, Read(2, NotNull(), 0, PAGE_SIZE))
                .WillOnce(DoAll(SetArrayArgument<1>(chunk1SnapMetaPage,
                                chunk1SnapMetaPage + PAGE_SIZE),
                                Return(PAGE_SIZE)));
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
    fileInfo.st_size = CHUNK_SIZE + PAGE_SIZE;
    EXPECT_CALL(*lfs_, Fstat(2, NotNull()))
        .WillOnce(DoAll(SetArgPointee<1>(fileInfo),
                        Return(0)));
    // read metapage success, but crc check failed
    version = FORMAT_VERSION;
    chunk1SnapMetaPage[1] += 1;  // change the page data
    memcpy(chunk1SnapMetaPage, &version, sizeof(uint8_t));
    EXPECT_CALL(*lfs_, Read(2, NotNull(), 0, PAGE_SIZE))
                .WillOnce(DoAll(SetArrayArgument<1>(chunk1SnapMetaPage,
                                chunk1SnapMetaPage + PAGE_SIZE),
                                Return(PAGE_SIZE)));
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
TEST_F(CSDataStore_test, InitializeErrorTest5) {
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
TEST_F(CSDataStore_test, WriteChunkTest1) {
    // initialize
    FakeEnv();
    EXPECT_TRUE(dataStore->Initialize());

    ChunkID id = 3;
    SequenceNum sn = 1;
    off_t offset = 0;
    size_t length = PAGE_SIZE;
    char buf[length] = {0};
    // create new chunk and open it
    string chunk3Path = string(baseDir) + "/" +
                        FileNameOperator::GenerateChunkFileName(id);
    // expect call chunkfile pool GetChunk
    EXPECT_CALL(*lfs_, FileExists(chunk3Path))
        .WillOnce(Return(false));
    EXPECT_CALL(*fpool_, GetChunk(chunk3Path, NotNull()))
                .WillOnce(Return(0));
    EXPECT_CALL(*lfs_, Open(chunk3Path, _))
        .Times(1)
        .WillOnce(Return(4));
    // will read metapage
    char chunk3MetaPage[PAGE_SIZE] = {0};
    FakeEncodeChunk(chunk3MetaPage, 0, 1);
    EXPECT_CALL(*lfs_, Read(4, NotNull(), 0, PAGE_SIZE))
        .WillOnce(DoAll(SetArrayArgument<1>(chunk3MetaPage,
                        chunk3MetaPage + PAGE_SIZE),
                        Return(PAGE_SIZE)));
    // will write data
    EXPECT_CALL(*lfs_, Write(4, NotNull(), PAGE_SIZE + offset, length))
        .Times(1);

    EXPECT_EQ(CSErrorCode::Success, dataStore->WriteChunk(id,
                                                          sn,
                                                          buf,
                                                          offset,
                                                          length,
                                                          nullptr));
    vector<SequenceNum> sns;
    dataStore->GetChunkInfo(id, &sns);
    ASSERT_THAT(sns, ElementsAre(1));

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
 * case:chunk存在,请求sn小于chunk的sn
 * 预期结果:拒绝写入，返回BackwardRequestError
 */
TEST_F(CSDataStore_test, WriteChunkTest2) {
    // initialize
    FakeEnv();
    EXPECT_TRUE(dataStore->Initialize());

    ChunkID id = 2;
    SequenceNum sn = 1;
    off_t offset = 0;
    size_t length = PAGE_SIZE;
    char buf[length] = {0};
    // will return BackwardRequestError
    EXPECT_EQ(CSErrorCode::BackwardRequestError,
              dataStore->WriteChunk(id,
                                    sn,
                                    buf,
                                    offset,
                                    length,
                                    nullptr));
    vector<SequenceNum> sns;
    dataStore->GetChunkInfo(id, &sns);
    ASSERT_THAT(sns, ElementsAre(2));

    EXPECT_CALL(*lfs_, Close(1))
        .Times(1);
    EXPECT_CALL(*lfs_, Close(2))
        .Times(1);
    EXPECT_CALL(*lfs_, Close(3))
        .Times(1);
}

/**
 * WriteChunkTest
 * case:chunk存在,请求sn小于chunk的correctedSn
 * 预期结果:拒绝写入，返回BackwardRequestError
 */
TEST_F(CSDataStore_test, WriteChunkTest3) {
    // initialize
    FakeEnv();
    // set chunk2's correctedSn as 3
    FakeEncodeChunk(chunk2MetaPage, 3, 2);
    EXPECT_CALL(*lfs_, Read(3, NotNull(), 0, PAGE_SIZE))
        .WillRepeatedly(DoAll(
                        SetArrayArgument<1>(chunk2MetaPage,
                        chunk2MetaPage + PAGE_SIZE),
                        Return(PAGE_SIZE)));
    EXPECT_TRUE(dataStore->Initialize());

    ChunkID id = 2;
    SequenceNum sn = 2;
    off_t offset = 0;
    size_t length = PAGE_SIZE;
    char buf[length] = {0};
    // will return BackwardRequestError
    EXPECT_EQ(CSErrorCode::BackwardRequestError,
              dataStore->WriteChunk(id,
                                    sn,
                                    buf,
                                    offset,
                                    length,
                                    nullptr));

    vector<SequenceNum> sns;
    dataStore->GetChunkInfo(id, &sns);
    ASSERT_THAT(sns, ElementsAre(2));

    EXPECT_CALL(*lfs_, Close(1))
        .Times(1);
    EXPECT_CALL(*lfs_, Close(2))
        .Times(1);
    EXPECT_CALL(*lfs_, Close(3))
        .Times(1);
}

/**
 * WriteChunkTest
 * case:chunk存在,请求sn等于chunk的sn且不小于correctSn
 *      chunk不存在快照
 * 预期结果:直接写数据到chunk文件
 */
TEST_F(CSDataStore_test, WriteChunkTest4) {
    // initialize
    FakeEnv();
    EXPECT_TRUE(dataStore->Initialize());

    ChunkID id = 2;
    SequenceNum sn = 2;
    off_t offset = 0;
    size_t length = PAGE_SIZE;
    char buf[length] = {0};

    // will write data
    EXPECT_CALL(*lfs_, Write(3, NotNull(), PAGE_SIZE + offset, length))
        .Times(1);
    EXPECT_EQ(CSErrorCode::Success,
              dataStore->WriteChunk(id,
                                    sn,
                                    buf,
                                    offset,
                                    length,
                                    nullptr));
    // return OutOfRangeError if offset+length > CHUNK_SIZE
    offset = CHUNK_SIZE;
    EXPECT_CALL(*lfs_, Write(3, NotNull(), PAGE_SIZE + offset, length))
        .Times(0);
    EXPECT_EQ(CSErrorCode::OutOfRangeError,
              dataStore->WriteChunk(id,
                                    sn,
                                    buf,
                                    offset,
                                    length,
                                    nullptr));
    vector<SequenceNum> sns;
    dataStore->GetChunkInfo(id, &sns);
    ASSERT_THAT(sns, ElementsAre(2));

    EXPECT_CALL(*lfs_, Close(1))
        .Times(1);
    EXPECT_CALL(*lfs_, Close(2))
        .Times(1);
    EXPECT_CALL(*lfs_, Close(3))
        .Times(1);
}

/**
 * WriteChunkTest
 * case:chunk存在,请求sn大于chunk的sn,等于correctSn,
 *      chunk不存在快照
 * 预期结果:会更新metapage，然后写数据到chunk文件
 */
TEST_F(CSDataStore_test, WriteChunkTest6) {
    // initialize
    FakeEnv();
    // set chunk2's correctedSn as 3
    FakeEncodeChunk(chunk2MetaPage, 3, 2);
    EXPECT_CALL(*lfs_, Read(3, NotNull(), 0, PAGE_SIZE))
        .WillRepeatedly(DoAll(
                        SetArrayArgument<1>(chunk2MetaPage,
                        chunk2MetaPage + PAGE_SIZE),
                        Return(PAGE_SIZE)));
    EXPECT_TRUE(dataStore->Initialize());

    ChunkID id = 2;
    SequenceNum sn = 3;
    off_t offset = 0;
    size_t length = PAGE_SIZE;
    char buf[length] = {0};
    // will update metapage
    EXPECT_CALL(*lfs_, Write(3, NotNull(), 0, PAGE_SIZE))
        .Times(1);
    // will write data
    EXPECT_CALL(*lfs_, Write(3, NotNull(), PAGE_SIZE + offset, length))
        .Times(1);

    EXPECT_EQ(CSErrorCode::Success,
              dataStore->WriteChunk(id,
                                    sn,
                                    buf,
                                    offset,
                                    length,
                                    nullptr));
    vector<SequenceNum> sns;
    dataStore->GetChunkInfo(id, &sns);
    ASSERT_THAT(sns, ElementsAre(3));

    EXPECT_CALL(*lfs_, Close(1))
        .Times(1);
    EXPECT_CALL(*lfs_, Close(2))
        .Times(1);
    EXPECT_CALL(*lfs_, Close(3))
        .Times(1);
}

/**
 * WriteChunkTest
 * case:chunk存在,请求sn大于chunk的sn以及correctSn,
 *      chunk不存在快照、
 * 预期结果:会创建快照文件，更新metapage，
 * 写数据时先cow到snapshot，再写chunk文件
 */
TEST_F(CSDataStore_test, WriteChunkTest7) {
    // initialize
    FakeEnv();
    EXPECT_TRUE(dataStore->Initialize());

    ChunkID id = 2;
    SequenceNum sn = 3;
    off_t offset = 0;
    size_t length = PAGE_SIZE;
    char buf[length] = {0};
    // will Open snapshot file, snap sn equals 2
    string snapPath = string(baseDir) + "/" +
        FileNameOperator::GenerateSnapshotName(id, 2);
    // snapshot not exists
    EXPECT_CALL(*lfs_, FileExists(snapPath))
        .WillOnce(Return(false));
    // expect call chunkfile pool GetChunk
    EXPECT_CALL(*fpool_, GetChunk(snapPath, NotNull()))
                .WillOnce(Return(0));
    EXPECT_CALL(*lfs_, Open(snapPath, _))
        .WillOnce(Return(4));
    // will read snapshot metapage
    char metapage[PAGE_SIZE] = {0};
    FakeEncodeSnapshot(metapage, false, 2);
    EXPECT_CALL(*lfs_, Read(4, NotNull(), 0, PAGE_SIZE))
        .WillOnce(DoAll(SetArrayArgument<1>(metapage,
                        metapage + PAGE_SIZE),
                        Return(PAGE_SIZE)));
    // will update metapage
    EXPECT_CALL(*lfs_, Write(3, NotNull(), 0, PAGE_SIZE))
        .Times(1);
    // will copy on write
    EXPECT_CALL(*lfs_, Read(3, NotNull(), PAGE_SIZE + offset, length))
        .Times(1);
    EXPECT_CALL(*lfs_, Write(4, NotNull(), PAGE_SIZE + offset, length))
        .Times(1);
    // will update snapshot metapage
    EXPECT_CALL(*lfs_, Write(4, NotNull(), 0, PAGE_SIZE))
        .Times(1);
    // will write data
    EXPECT_CALL(*lfs_, Write(3, NotNull(), PAGE_SIZE + offset, length))
        .Times(1);

    EXPECT_EQ(CSErrorCode::Success,
              dataStore->WriteChunk(id,
                                    sn,
                                    buf,
                                    offset,
                                    length,
                                    nullptr));
    vector<SequenceNum> sns;
    dataStore->GetChunkInfo(id, &sns);
    ASSERT_THAT(sns, ElementsAre(3, 2));

    // 再次写同一个page的数据，不再进行cow，而是直接写入数据
    EXPECT_CALL(*lfs_, Write(3, NotNull(), PAGE_SIZE + offset, length))
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
 * case:chunk存在,请求sn等于chunk的sn且不小于correctSn
 *      chunk存在快照
 * 预期结果:先cow到snapshot，再写chunk文件
 */
TEST_F(CSDataStore_test, WriteChunkTest9) {
    // initialize
    FakeEnv();
    EXPECT_TRUE(dataStore->Initialize());

    ChunkID id = 1;
    SequenceNum sn = 2;
    off_t offset = 0;
    size_t length = PAGE_SIZE;
    char buf[length] = {0};
    // will not create snapshot
    // will copy on write
    EXPECT_CALL(*lfs_, Read(1, NotNull(), PAGE_SIZE + offset, length))
        .Times(1);
    EXPECT_CALL(*lfs_, Write(2, NotNull(), PAGE_SIZE + offset, length))
        .Times(1);
    // will update snapshot metapage
    EXPECT_CALL(*lfs_, Write(2, NotNull(), 0, PAGE_SIZE))
        .Times(1);
    // will write data
    EXPECT_CALL(*lfs_, Write(1, NotNull(), PAGE_SIZE + offset, length))
        .Times(1);

    EXPECT_EQ(CSErrorCode::Success,
              dataStore->WriteChunk(id,
                                    sn,
                                    buf,
                                    offset,
                                    length,
                                    nullptr));
    vector<SequenceNum> sns;
    dataStore->GetChunkInfo(id, &sns);
    ASSERT_THAT(sns, ElementsAre(2, 1));

    EXPECT_CALL(*lfs_, Close(1))
        .Times(1);
    EXPECT_CALL(*lfs_, Close(2))
        .Times(1);
    EXPECT_CALL(*lfs_, Close(3))
        .Times(1);
}

/**
 * WriteChunkTest
 * case:chunk存在,请求sn大于chunk的sn，等于correctSn
 *      chunk存在快照
 * 预期结果:更新metapage，然后写chunk文件
 */
TEST_F(CSDataStore_test, WriteChunkTest10) {
    // initialize
    FakeEnv();
    // set chunk1's correctedSn as 3
    FakeEncodeChunk(chunk1MetaPage, 3, 2);
    EXPECT_CALL(*lfs_, Read(1, NotNull(), 0, PAGE_SIZE))
        .WillRepeatedly(DoAll(
                        SetArrayArgument<1>(chunk1MetaPage,
                        chunk1MetaPage + PAGE_SIZE),
                        Return(PAGE_SIZE)));
    EXPECT_TRUE(dataStore->Initialize());

    ChunkID id = 1;
    SequenceNum sn = 3;
    off_t offset = 0;
    size_t length = PAGE_SIZE;
    char buf[length] = {0};
    // will update metapage
    EXPECT_CALL(*lfs_, Write(1, NotNull(), 0, PAGE_SIZE))
        .Times(1);
    // will not cow
    // will write data
    EXPECT_CALL(*lfs_, Write(1, NotNull(), PAGE_SIZE + offset, length))
        .Times(1);

    EXPECT_EQ(CSErrorCode::Success,
              dataStore->WriteChunk(id,
                                    sn,
                                    buf,
                                    offset,
                                    length,
                                    nullptr));
    vector<SequenceNum> sns;
    dataStore->GetChunkInfo(id, &sns);
    ASSERT_THAT(sns, ElementsAre(3, 1));

    EXPECT_CALL(*lfs_, Close(1))
        .Times(1);
    EXPECT_CALL(*lfs_, Close(2))
        .Times(1);
    EXPECT_CALL(*lfs_, Close(3))
        .Times(1);
}

/**
 * WriteChunkTest
 * case:chunk存在,请求sn大于chunk的sn和correctSn
 *      chunk存在快照
 * 预期结果:存在历史快照未删除，写失败，返回SnapshotConflictError
 */
TEST_F(CSDataStore_test, WriteChunkTest11) {
    // initialize
    FakeEnv();
    EXPECT_TRUE(dataStore->Initialize());

    ChunkID id = 1;
    SequenceNum sn = 3;
    off_t offset = 0;
    size_t length = PAGE_SIZE;
    char buf[length] = {0};

    EXPECT_EQ(CSErrorCode::SnapshotConflictError,
              dataStore->WriteChunk(id,
                                    sn,
                                    buf,
                                    offset,
                                    length,
                                    nullptr));
    vector<SequenceNum> sns;
    dataStore->GetChunkInfo(id, &sns);
    ASSERT_THAT(sns, ElementsAre(2, 1));

    EXPECT_CALL(*lfs_, Close(1))
        .Times(1);
    EXPECT_CALL(*lfs_, Close(2))
        .Times(1);
    EXPECT_CALL(*lfs_, Close(3))
        .Times(1);
}

/**
 * WriteChunkTest
 * case:chunk存在,请求sn等于chunk的sn且不小于correctSn
 *      chunk存在快照
 *      snapshot已损坏
 * 预期结果:直接写chunk文件
 */
TEST_F(CSDataStore_test, WriteChunkTest12) {
    // initialize
    FakeEnv();
    // set snapshot damaged
    FakeEncodeSnapshot(chunk1SnapMetaPage, true, 1);
    EXPECT_CALL(*lfs_, Read(2, NotNull(), 0, PAGE_SIZE))
        .WillRepeatedly(DoAll(
                        SetArrayArgument<1>(chunk1SnapMetaPage,
                        chunk1SnapMetaPage + PAGE_SIZE),
                        Return(PAGE_SIZE)));
    EXPECT_TRUE(dataStore->Initialize());

    ChunkID id = 1;
    SequenceNum sn = 2;
    off_t offset = 0;
    size_t length = PAGE_SIZE;
    char buf[length] = {0};
    // will not create snapshot
    // will not cow
    // will write data
    EXPECT_CALL(*lfs_, Write(1, NotNull(), PAGE_SIZE + offset, length))
        .Times(1);

    EXPECT_EQ(CSErrorCode::Success,
              dataStore->WriteChunk(id,
                                    sn,
                                    buf,
                                    offset,
                                    length,
                                    nullptr));
    vector<SequenceNum> sns;
    dataStore->GetChunkInfo(id, &sns);
    ASSERT_THAT(sns, ElementsAre(2, 1));

    EXPECT_CALL(*lfs_, Close(1))
        .Times(1);
    EXPECT_CALL(*lfs_, Close(2))
        .Times(1);
    EXPECT_CALL(*lfs_, Close(3))
        .Times(1);
}

/**
 * WriteChunkTest 异常测试
 * case:创建快照文件时出错
 * 预期结果:写失败，不会改变当前chunk状态
 */
TEST_F(CSDataStore_test, WriteChunkErrorTest1) {
    // initialize
    FakeEnv();
    EXPECT_TRUE(dataStore->Initialize());

    ChunkID id = 2;
    SequenceNum sn = 3;
    off_t offset = 0;
    size_t length = PAGE_SIZE;
    char buf[length] = {0};
    string snapPath = string(baseDir) + "/" +
        FileNameOperator::GenerateSnapshotName(id, 2);

    // getchunk failed
    EXPECT_CALL(*lfs_, FileExists(snapPath))
        .WillOnce(Return(false));
    EXPECT_CALL(*fpool_, GetChunk(snapPath, NotNull()))
        .WillOnce(Return(-UT_ERRNO));
    EXPECT_EQ(CSErrorCode::InternalError,
              dataStore->WriteChunk(id,
                                    sn,
                                    buf,
                                    offset,
                                    length,
                                    nullptr));
    vector<SequenceNum> sns;
    dataStore->GetChunkInfo(id, &sns);
    ASSERT_THAT(sns, ElementsAre(2));

    // expect call chunkfile pool GetChunk
    EXPECT_CALL(*lfs_, FileExists(snapPath))
        .WillOnce(Return(false));
    EXPECT_CALL(*fpool_, GetChunk(snapPath, NotNull()))
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
    sns.clear();
    dataStore->GetChunkInfo(id, &sns);
    ASSERT_THAT(sns, ElementsAre(2));

    // open success but read snapshot metapage failed
    EXPECT_CALL(*lfs_, FileExists(snapPath))
        .WillOnce(Return(true));
    EXPECT_CALL(*lfs_, Open(snapPath, _))
        .WillOnce(Return(4));
    EXPECT_CALL(*lfs_, Read(4, NotNull(), 0, PAGE_SIZE))
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
    sns.clear();
    dataStore->GetChunkInfo(id, &sns);
    ASSERT_THAT(sns, ElementsAre(2));

    EXPECT_CALL(*lfs_, Close(1))
        .Times(1);
    EXPECT_CALL(*lfs_, Close(2))
        .Times(1);
    EXPECT_CALL(*lfs_, Close(3))
        .Times(1);
}

/**
 * WriteChunkTest 异常测试
 * case:创建快照文件成功，更新metapage失败
 * 预期结果:写失败，产生快照文件，但是chunk版本号不会改变
 * 再次写入，不会生成新的快照文件
 */
TEST_F(CSDataStore_test, WriteChunkErrorTest2) {
    // initialize
    FakeEnv();
    EXPECT_TRUE(dataStore->Initialize());

    ChunkID id = 2;
    SequenceNum sn = 3;
    off_t offset = 0;
    size_t length = PAGE_SIZE;
    char buf[length] = {0};
    // will Open snapshot file, snap sn equals 2
    string snapPath = string(baseDir) + "/" +
        FileNameOperator::GenerateSnapshotName(id, 2);
    // expect call chunk file pool GetChunk
    EXPECT_CALL(*lfs_, FileExists(snapPath))
        .WillOnce(Return(false));
    EXPECT_CALL(*fpool_, GetChunk(snapPath, NotNull()))
        .WillOnce(Return(0));
    EXPECT_CALL(*lfs_, Open(snapPath, _))
        .WillOnce(Return(4));
    // will read snapshot metapage
    char metapage[PAGE_SIZE] = {0};
    FakeEncodeSnapshot(metapage, false, 2);
    EXPECT_CALL(*lfs_, Read(4, NotNull(), 0, PAGE_SIZE))
        .WillOnce(DoAll(SetArrayArgument<1>(metapage,
                        metapage + PAGE_SIZE),
                        Return(PAGE_SIZE)));
    // write chunk metapage failed
    EXPECT_CALL(*lfs_, Write(3, NotNull(), 0, PAGE_SIZE))
        .WillOnce(Return(-UT_ERRNO));
    EXPECT_EQ(CSErrorCode::InternalError,
              dataStore->WriteChunk(id,
                                    sn,
                                    buf,
                                    offset,
                                    length,
                                    nullptr));
    vector<SequenceNum> sns;
    dataStore->GetChunkInfo(id, &sns);
    // chunk sn not changed
    ASSERT_THAT(sns, ElementsAre(2, 2));

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
 * WriteChunkTest 异常测试
 * case:创建快照文件成功，更新metapage成功，cow失败
 * 预期结果:写失败，产生快照文件，chunk版本号发生变更，
 * 快照的bitmap未发生变化，再次写入，仍会进行cow
 */
TEST_F(CSDataStore_test, WriteChunkErrorTest3) {
    // initialize
    FakeEnv();
    EXPECT_TRUE(dataStore->Initialize());

    ChunkID id = 2;
    SequenceNum sn = 3;
    off_t offset = 0;
    size_t length = PAGE_SIZE;
    char buf[length] = {0};
    // will Open snapshot file, snap sn equals 2
    string snapPath = string(baseDir) + "/" +
        FileNameOperator::GenerateSnapshotName(id, 2);
    // expect call chunk file pool GetChunk
    EXPECT_CALL(*lfs_, FileExists(snapPath))
        .WillOnce(Return(false));
    EXPECT_CALL(*fpool_, GetChunk(snapPath, NotNull()))
        .WillOnce(Return(0));
    EXPECT_CALL(*lfs_, Open(snapPath, _))
        .WillOnce(Return(4));
    // will read snapshot metapage
    char metapage[PAGE_SIZE] = {0};
    FakeEncodeSnapshot(metapage, false, 2);
    EXPECT_CALL(*lfs_, Read(4, NotNull(), 0, PAGE_SIZE))
        .WillOnce(DoAll(SetArrayArgument<1>(metapage,
                        metapage + PAGE_SIZE),
                        Return(PAGE_SIZE)));
    // will update metapage
    EXPECT_CALL(*lfs_, Write(3, NotNull(), 0, PAGE_SIZE))
        .Times(1);
    // copy data failed
    EXPECT_CALL(*lfs_, Read(3, NotNull(), PAGE_SIZE + offset, length))
        .WillOnce(Return(-UT_ERRNO));
    EXPECT_EQ(CSErrorCode::InternalError,
              dataStore->WriteChunk(id,
                                    sn,
                                    buf,
                                    offset,
                                    length,
                                    nullptr));
    vector<SequenceNum> sns;
    dataStore->GetChunkInfo(id, &sns);
    ASSERT_THAT(sns, ElementsAre(3, 2));

    // copy data success
    EXPECT_CALL(*lfs_, Read(3, NotNull(), PAGE_SIZE + offset, length))
        .Times(1);
    // write data to snapshot failed
    EXPECT_CALL(*lfs_, Write(4, NotNull(), PAGE_SIZE + offset, length))
        .WillOnce(Return(-UT_ERRNO));
    EXPECT_EQ(CSErrorCode::InternalError,
              dataStore->WriteChunk(id,
                                    sn,
                                    buf,
                                    offset,
                                    length,
                                    nullptr));
    sns.clear();
    dataStore->GetChunkInfo(id, &sns);
    ASSERT_THAT(sns, ElementsAre(3, 2));

    // copy data success
    EXPECT_CALL(*lfs_, Read(3, NotNull(), PAGE_SIZE + offset, length))
        .Times(1);
    // write data to snapshot success
    EXPECT_CALL(*lfs_, Write(4, NotNull(), PAGE_SIZE + offset, length))
        .Times(1);
    // update snapshot metapage failed
    EXPECT_CALL(*lfs_, Write(4, NotNull(), 0, PAGE_SIZE))
        .WillOnce(Return(-UT_ERRNO));
    EXPECT_EQ(CSErrorCode::InternalError,
              dataStore->WriteChunk(id,
                                    sn,
                                    buf,
                                    offset,
                                    length,
                                    nullptr));
    sns.clear();
    dataStore->GetChunkInfo(id, &sns);
    ASSERT_THAT(sns, ElementsAre(3, 2));

    // 再次写入仍会cow
    // will copy on write
    EXPECT_CALL(*lfs_, Read(3, NotNull(), PAGE_SIZE + offset, length))
        .Times(1);
    EXPECT_CALL(*lfs_, Write(4, NotNull(), PAGE_SIZE + offset, length))
        .Times(1);
    // will update snapshot metapage
    EXPECT_CALL(*lfs_, Write(4, NotNull(), 0, PAGE_SIZE))
        .Times(1);
    // will write data
    EXPECT_CALL(*lfs_, Write(3, NotNull(), PAGE_SIZE + offset, length))
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
 * WriteChunkTest 异常测试
 * case:创建快照文件成功，更新metapage成功，cow成功，写数据失败
 * 预期结果:写失败，产生快照文件，chunk版本号发生变更，
 * 快照的bitmap发生变化，再次写入，直接写chunk文件
 */
TEST_F(CSDataStore_test, WriteChunkErrorTest4) {
    // initialize
    FakeEnv();
    EXPECT_TRUE(dataStore->Initialize());

    ChunkID id = 2;
    SequenceNum sn = 3;
    off_t offset = 0;
    size_t length = PAGE_SIZE;
    char buf[length] = {0};
    // will Open snapshot file, snap sn equals 2
    string snapPath = string(baseDir) + "/" +
        FileNameOperator::GenerateSnapshotName(id, 2);
    // expect call chunk file pool GetChunk
    EXPECT_CALL(*lfs_, FileExists(snapPath))
        .WillOnce(Return(false));
    EXPECT_CALL(*fpool_, GetChunk(snapPath, NotNull()))
        .WillOnce(Return(0));
    EXPECT_CALL(*lfs_, Open(snapPath, _))
        .WillOnce(Return(4));
    // will read snapshot metapage
    char metapage[PAGE_SIZE] = {0};
    FakeEncodeSnapshot(metapage, false, 2);
    EXPECT_CALL(*lfs_, Read(4, NotNull(), 0, PAGE_SIZE))
        .WillOnce(DoAll(SetArrayArgument<1>(metapage,
                        metapage + PAGE_SIZE),
                        Return(PAGE_SIZE)));
    // will update metapage
    EXPECT_CALL(*lfs_, Write(3, NotNull(), 0, PAGE_SIZE))
        .Times(1);
    // will copy on write
    EXPECT_CALL(*lfs_, Read(3, NotNull(), PAGE_SIZE + offset, length))
        .Times(1);
    EXPECT_CALL(*lfs_, Write(4, NotNull(), PAGE_SIZE + offset, length))
        .Times(1);
    // will update snapshot metapage
    EXPECT_CALL(*lfs_, Write(4, NotNull(), 0, PAGE_SIZE))
        .Times(1);
    // write chunk failed
    EXPECT_CALL(*lfs_, Write(3, NotNull(), PAGE_SIZE + offset, length))
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
    EXPECT_CALL(*lfs_, Write(3, NotNull(), PAGE_SIZE + offset, length))
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
TEST_F(CSDataStore_test, WriteChunkErrorTest5) {
    // initialize
    FakeEnv();
    EXPECT_TRUE(dataStore->Initialize());

    ChunkID id = 3;
    SequenceNum sn = 1;
    off_t offset = 0;
    size_t length = PAGE_SIZE;
    char buf[length] = {0};
    // create new chunk and open it
    string chunk3Path = string(baseDir) + "/" +
                        FileNameOperator::GenerateChunkFileName(id);

    // expect call chunk file pool GetChunk
    EXPECT_CALL(*lfs_, FileExists(chunk3Path))
        .WillOnce(Return(false));
    EXPECT_CALL(*fpool_, GetChunk(chunk3Path, NotNull()))
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
    EXPECT_CALL(*fpool_, GetChunk(chunk3Path, NotNull()))
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
    // stat success but file size not equal CHUNK_SIZE + PAGE_SIZE
    struct stat fileInfo;
    fileInfo.st_size = CHUNK_SIZE;
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
    fileInfo.st_size = CHUNK_SIZE + PAGE_SIZE;
    EXPECT_CALL(*lfs_, Fstat(4, NotNull()))
        .WillOnce(DoAll(SetArgPointee<1>(fileInfo),
                        Return(0)));
    // read metapage failed
    EXPECT_CALL(*lfs_, Read(4, NotNull(), 0, PAGE_SIZE))
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
}

/**
 * ReadChunkTest
 * case:chunk不存在
 * 预期结果:返回ChunkNotExistError错误码
 */
TEST_F(CSDataStore_test, ReadChunkTest1) {
    // initialize
    FakeEnv();
    EXPECT_TRUE(dataStore->Initialize());

    ChunkID id = 3;
    SequenceNum sn = 2;
    off_t offset = PAGE_SIZE;
    size_t length = PAGE_SIZE;
    char buf[length] = {0};
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
 * case:chunk存在，读取区域超过chunk大小
 * 预期结果:返回OutOfRangeError错误码
 */
TEST_F(CSDataStore_test, ReadChunkTest2) {
    // initialize
    FakeEnv();
    EXPECT_TRUE(dataStore->Initialize());

    ChunkID id = 1;
    SequenceNum sn = 2;
    off_t offset = CHUNK_SIZE;
    size_t length = PAGE_SIZE;
    char buf[length] = {0};
    // test read out of range
    EXPECT_EQ(CSErrorCode::OutOfRangeError,
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
TEST_F(CSDataStore_test, ReadChunkTest3) {
    // initialize
    FakeEnv();
    EXPECT_TRUE(dataStore->Initialize());

    ChunkID id = 1;
    SequenceNum sn = 2;
    off_t offset = PAGE_SIZE;
    size_t length = PAGE_SIZE;
    char buf[length] = {0};
    // test chunk exists
    EXPECT_CALL(*lfs_, Read(1, NotNull(), offset + PAGE_SIZE, length))
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
}

/**
 * ReadChunkErrorTest
 * case:读chunk文件时出错
 * 预期结果:读取失败，返回InternalError
 */
TEST_F(CSDataStore_test, ReadChunkErrorTest1) {
    // initialize
    FakeEnv();
    EXPECT_TRUE(dataStore->Initialize());

    ChunkID id = 1;
    SequenceNum sn = 2;
    off_t offset = PAGE_SIZE;
    size_t length = PAGE_SIZE;
    char buf[length] = {0};
    // test read chunk failed
    EXPECT_CALL(*lfs_, Read(1, NotNull(), offset + PAGE_SIZE, length))
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
}

/**
 * ReadSnapshotChunkTest
 * case:chunk不存在
 * 预期结果:返回ChunkNotExistError错误码
 */
TEST_F(CSDataStore_test, ReadSnapshotChunkTest1) {
    // initialize
    FakeEnv();
    EXPECT_TRUE(dataStore->Initialize());

    ChunkID id = 3;
    SequenceNum sn = 2;
    off_t offset = PAGE_SIZE;
    size_t length = PAGE_SIZE;
    char buf[length] = {0};
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
}

/**
 * ReadSnapshotChunkTest
 * case:chunk存在，请求版本号等于chunk版本号
 * 预期结果:读chunk的数据
 */
TEST_F(CSDataStore_test, ReadSnapshotChunkTest2) {
    // initialize
    FakeEnv();
    EXPECT_TRUE(dataStore->Initialize());

    ChunkID id = 1;
    SequenceNum sn = 2;
    off_t offset = CHUNK_SIZE;
    size_t length = 2 * PAGE_SIZE;
    char buf[length] = {0};
    // test out of range
    EXPECT_EQ(CSErrorCode::OutOfRangeError,
              dataStore->ReadSnapshotChunk(id,
                                           sn,
                                           buf,
                                           offset,
                                           length));
    // test in range
    offset = PAGE_SIZE;
    EXPECT_CALL(*lfs_, Read(1, NotNull(), offset + PAGE_SIZE, length))
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
}

/**
 * ReadSnapshotChunkTest
 * case:chunk存在，请求版本号等于snapshot版本号
 * 预期结果:读快照的数据
 */
TEST_F(CSDataStore_test, ReadSnapshotChunkTest3) {
    // initialize
    FakeEnv();
    EXPECT_TRUE(dataStore->Initialize());
    // fake data
    ChunkID id = 1;
    SequenceNum sn = 2;
    off_t offset = PAGE_SIZE;
    size_t length = PAGE_SIZE * 2;
    char writeBuf[length] = {0};
    // data in [PAGE_SIZE, 2*PAGE_SIZE) will be cow
    EXPECT_CALL(*lfs_, Read(1, NotNull(), offset + PAGE_SIZE, length))
        .Times(1);
    EXPECT_CALL(*lfs_, Write(2, NotNull(), offset + PAGE_SIZE, length))
        .Times(1);
    // will update snapshot metapage
    EXPECT_CALL(*lfs_, Write(2, NotNull(), 0, PAGE_SIZE))
        .Times(1);
    // will write data
    EXPECT_CALL(*lfs_, Write(1, NotNull(), offset + PAGE_SIZE, length))
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
    offset = CHUNK_SIZE;
    length = PAGE_SIZE * 4;
    char readBuf[length] = {0};
    EXPECT_EQ(CSErrorCode::OutOfRangeError,
              dataStore->ReadSnapshotChunk(id,
                                           sn,
                                           readBuf,
                                           offset,
                                           length));
    // test in range, read [0, 4*PAGE_SIZE)
    offset = 0;
    // read chunk in[0, PAGE_SIZE) and [3*PAGE_SIZE, 4*PAGE_SIZE)
    EXPECT_CALL(*lfs_, Read(1, NotNull(), PAGE_SIZE, PAGE_SIZE))
        .Times(1);
    EXPECT_CALL(*lfs_, Read(1, NotNull(), 4 * PAGE_SIZE, PAGE_SIZE))
        .Times(1);
    // read snapshot in[PAGE_SIZE, 3*PAGE_SIZE)
    EXPECT_CALL(*lfs_, Read(2, NotNull(), 2 * PAGE_SIZE, 2 * PAGE_SIZE))
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
}

/**
 * ReadSnapshotChunkTest
 * case:chunk存在，但是请求的版本号不存在
 * 预期结果:返回ChunkNotExistError错误码
 */
TEST_F(CSDataStore_test, ReadSnapshotChunkTest4) {
    // initialize
    FakeEnv();
    EXPECT_TRUE(dataStore->Initialize());

    ChunkID id = 1;
    SequenceNum sn = 3;
    off_t offset = PAGE_SIZE;
    size_t length = PAGE_SIZE;
    char buf[length] = {0};
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
}

/**
 * ReadSnapshotChunkErrorTest
 * case:读快照时失败
 * 预期结果:返回InternalError
 */
TEST_F(CSDataStore_test, ReadSnapshotChunkErrorTest1) {
    // initialize
    FakeEnv();
    EXPECT_TRUE(dataStore->Initialize());
    // fake data
    ChunkID id = 1;
    SequenceNum sn = 2;
    off_t offset = PAGE_SIZE;
    size_t length = PAGE_SIZE * 2;
    char writeBuf[length] = {0};
    // data in [PAGE_SIZE, 2*PAGE_SIZE) will be cow
    EXPECT_CALL(*lfs_, Read(1, NotNull(), offset + PAGE_SIZE, length))
        .Times(1);
    EXPECT_CALL(*lfs_, Write(2, NotNull(), offset + PAGE_SIZE, length))
        .Times(1);
    // will update snapshot metapage
    EXPECT_CALL(*lfs_, Write(2, NotNull(), 0, PAGE_SIZE))
        .Times(1);
    // will write data
    EXPECT_CALL(*lfs_, Write(1, NotNull(), offset + PAGE_SIZE, length))
        .Times(1);
    EXPECT_EQ(CSErrorCode::Success,
              dataStore->WriteChunk(id,
                                    sn,
                                    writeBuf,
                                    offset,
                                    length,
                                    nullptr));

    // test in range, read [0, 4*PAGE_SIZE)
    sn = 1;
    offset = 0;
    length = PAGE_SIZE * 4;
    char readBuf[length] = {0};
    // read chunk failed
    EXPECT_CALL(*lfs_, Read(1, NotNull(), PAGE_SIZE, PAGE_SIZE))
        .WillOnce(Return(-UT_ERRNO));
    EXPECT_EQ(CSErrorCode::InternalError,
              dataStore->ReadSnapshotChunk(id,
                                           sn,
                                           readBuf,
                                           offset,
                                           length));

    // read snapshot failed
    EXPECT_CALL(*lfs_, Read(1, NotNull(), PAGE_SIZE, PAGE_SIZE))
        .Times(1);
    EXPECT_CALL(*lfs_, Read(1, NotNull(), 4 * PAGE_SIZE, PAGE_SIZE))
        .Times(1);
    EXPECT_CALL(*lfs_, Read(2, NotNull(), 2 * PAGE_SIZE, 2 * PAGE_SIZE))
        .WillOnce(Return(-UT_ERRNO));
    EXPECT_EQ(CSErrorCode::InternalError,
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
}

/**
 * DeleteChunkTest
 * case:chunk不存在
 * 预期结果:返回成功
 */
TEST_F(CSDataStore_test, DeleteChunkTest1) {
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
 * 预期结果:返回SnapshotExistError
 */
TEST_F(CSDataStore_test, DeleteChunkTest2) {
    // initialize
    FakeEnv();
    EXPECT_TRUE(dataStore->Initialize());

    ChunkID id = 1;
    SequenceNum sn = 2;
    // delete chunk with snapshot
    EXPECT_EQ(CSErrorCode::SnapshotExistError,
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
 * case:chunk存在,快照文件不存在
 * 预期结果:返回成功
 */
TEST_F(CSDataStore_test, DeleteChunkTest3) {
    // initialize
    FakeEnv();
    EXPECT_TRUE(dataStore->Initialize());

    ChunkID id = 2;
    SequenceNum sn = 2;
    // chunk will be closed
    EXPECT_CALL(*lfs_, Close(3))
        .Times(1);
    // expect to call chunkfilepool RecycleChunk
    EXPECT_CALL(*fpool_, RecycleChunk(chunk2Path))
        .WillOnce(Return(0));
    EXPECT_EQ(CSErrorCode::Success,
              dataStore->DeleteChunk(id, sn));

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
TEST_F(CSDataStore_test, DeleteChunkErrorTest1) {
    // initialize
    FakeEnv();
    EXPECT_TRUE(dataStore->Initialize());

    ChunkID id = 2;
    SequenceNum sn = 2;
    // chunk will be closed
    EXPECT_CALL(*lfs_, Close(3))
        .Times(1);
    // expect to call chunkfilepool RecycleChunk
    EXPECT_CALL(*fpool_, RecycleChunk(chunk2Path))
        .WillOnce(Return(-1));
    EXPECT_EQ(CSErrorCode::InternalError,
              dataStore->DeleteChunk(id, sn));

    EXPECT_CALL(*lfs_, Close(1))
        .Times(1);
    EXPECT_CALL(*lfs_, Close(2))
        .Times(1);
}

/**
 * DeleteSnapshotChunkTest
 * case:chunk不存在
 * 预期结果:返回成功
 */
TEST_F(CSDataStore_test, DeleteSnapshotChunkTest1) {
    // initialize
    FakeEnv();
    EXPECT_TRUE(dataStore->Initialize());

    ChunkID id = 3;
    SequenceNum sn = 2;
    // test chunk not exists
    EXPECT_EQ(CSErrorCode::Success,
              dataStore->DeleteSnapshotChunk(id, sn));

    EXPECT_CALL(*lfs_, Close(1))
        .Times(1);
    EXPECT_CALL(*lfs_, Close(2))
        .Times(1);
    EXPECT_CALL(*lfs_, Close(3))
        .Times(1);
}

// 对于DeleteSnapshotChunk来说，内部主要有两个操作
// 一个是删除快照文件，一个是修改correctedSn
// 当存在快照文件时，snapshotSn+1>=chunk的sn是判断是否要删除快照的唯一条件
// 对于correctedSn来说，snapshotSn+1大于chunk的sn以及correctedSn是判断
// 是否要修改correctedSn的唯一条件

/**
 * DeleteSnapshotChunkTest
 * case:chunk存在,snapshot存在
 *      snapshotSn + 1 >= chunk的sn
 *      snapshotSn + 1 <= chunk的sn或correctedSn
 * 预期结果:删除快照，不会修改correctedSn,返回成功
 */
TEST_F(CSDataStore_test, DeleteSnapshotChunkTest2) {
    // initialize
    FakeEnv();
    // set chunk1's correctedSn as 3
    FakeEncodeChunk(chunk1MetaPage, 3, 2);
    EXPECT_CALL(*lfs_, Read(1, NotNull(), 0, PAGE_SIZE))
        .WillRepeatedly(DoAll(
                        SetArrayArgument<1>(chunk1MetaPage,
                        chunk1MetaPage + PAGE_SIZE),
                        Return(PAGE_SIZE)));
    EXPECT_TRUE(dataStore->Initialize());

    ChunkID id = 1;
    // snapshotSn + 1 > sn
    // snapshotSn + 1 == correctedSn
    SequenceNum snapshotSn = 2;
    // snapshot will be closed
    EXPECT_CALL(*lfs_, Close(2))
        .Times(1);
    // expect to call chunkfilepool RecycleChunk
    EXPECT_CALL(*fpool_, RecycleChunk(chunk1snap1Path))
        .Times(1);
    // chunk's metapage should not be updated
    EXPECT_CALL(*lfs_, Write(1, NotNull(), 0, PAGE_SIZE))
        .Times(0);
    EXPECT_EQ(CSErrorCode::Success,
              dataStore->DeleteSnapshotChunk(id, snapshotSn));

    EXPECT_CALL(*lfs_, Close(1))
        .Times(1);
    EXPECT_CALL(*lfs_, Close(3))
        .Times(1);
}

/**
 * DeleteSnapshotChunkTest
 * case:chunk存在,snapshot存在
 *      snapshotSn + 1 < chunk的sn
 *      此时无论correctSn为何值都不会修改correctedSn
 * 预期结果:返回成功，不会删除快照,不会修改correctedSn
 */
TEST_F(CSDataStore_test, DeleteSnapshotChunkTest3) {
    // initialize
    FakeEnv();
    // set chunk1's correctedSn as 0, sn as 3
    FakeEncodeChunk(chunk1MetaPage, 0, 3);
    EXPECT_CALL(*lfs_, Read(1, NotNull(), 0, PAGE_SIZE))
        .WillRepeatedly(DoAll(
                        SetArrayArgument<1>(chunk1MetaPage,
                        chunk1MetaPage + PAGE_SIZE),
                        Return(PAGE_SIZE)));
    EXPECT_TRUE(dataStore->Initialize());

    ChunkID id = 1;
    // snapshotSn + 1 < sn
    // snapshotSn + 1 > correctedSn
    SequenceNum snapshotSn = 1;
    // snapshot should not be closed
    EXPECT_CALL(*lfs_, Close(2))
        .Times(0);
    // chunk's metapage should not be updated
    EXPECT_CALL(*lfs_, Write(3, NotNull(), 0, PAGE_SIZE))
        .Times(0);
    EXPECT_EQ(CSErrorCode::Success,
              dataStore->DeleteSnapshotChunk(id, snapshotSn));

    // 下则用例用于补充DeleteSnapshotChunkTest2用例中
    // 当 snapshotSn + 1 == sn 时的边界情况
    // snapshotSn + 1 == sn
    // snapshotSn + 1 > correctedSn
    snapshotSn = 2;
    // snapshot will be closed
    EXPECT_CALL(*lfs_, Close(2))
        .Times(1);
    // expect to call chunkfilepool RecycleChunk
    EXPECT_CALL(*fpool_, RecycleChunk(chunk1snap1Path))
        .Times(1);
    // chunk's metapage should not be updated
    EXPECT_CALL(*lfs_, Write(1, NotNull(), 0, PAGE_SIZE))
        .Times(0);
    EXPECT_EQ(CSErrorCode::Success,
              dataStore->DeleteSnapshotChunk(id, snapshotSn));

    EXPECT_CALL(*lfs_, Close(1))
        .Times(1);
    EXPECT_CALL(*lfs_, Close(3))
        .Times(1);
}

/**
 * DeleteSnapshotChunkTest
 * case:chunk存在,snapshot存在
 *      snapshotSn + 1 > chunk的sn以及correctedSn
 * 预期结果:删除快照，并修改correctedSn,返回成功
 */
TEST_F(CSDataStore_test, DeleteSnapshotChunkTest4) {
    // initialize
    FakeEnv();
    EXPECT_TRUE(dataStore->Initialize());

    ChunkID id = 1;
    // snapshotSn + 1 > sn
    // snapshotSn + 1 > correctedSn
    SequenceNum snapshotSn = 2;
    // snapshot will be closed
    EXPECT_CALL(*lfs_, Close(2))
        .Times(1);
    // expect to call chunkfilepool RecycleChunk
    EXPECT_CALL(*fpool_, RecycleChunk(chunk1snap1Path))
        .Times(1);
    // chunk's metapage will be updated
    EXPECT_CALL(*lfs_, Write(1, NotNull(), 0, PAGE_SIZE))
        .Times(1);
    EXPECT_EQ(CSErrorCode::Success,
              dataStore->DeleteSnapshotChunk(id, snapshotSn));

    EXPECT_CALL(*lfs_, Close(1))
        .Times(1);
    EXPECT_CALL(*lfs_, Close(3))
        .Times(1);
}

/**
 * DeleteSnapshotChunkTest
 * case:chunk存在,snapshot不存在
 *      snapshotSn + 1 <= chunk的sn或correctedSn
 * 预期结果:不会修改correctedSn,返回成功
 */
TEST_F(CSDataStore_test, DeleteSnapshotChunkTest5) {
    // initialize
    FakeEnv();
    EXPECT_TRUE(dataStore->Initialize());

    ChunkID id = 2;
    // snapshotSn + 1 == sn
    // snapshotSn + 1 > correctedSn
    SequenceNum snapshotSn = 1;
    // chunk's metapage should not be updated
    EXPECT_CALL(*lfs_, Write(3, NotNull(), 0, PAGE_SIZE))
        .Times(0);
    EXPECT_EQ(CSErrorCode::Success,
              dataStore->DeleteSnapshotChunk(id, snapshotSn));

    EXPECT_CALL(*lfs_, Close(1))
        .Times(1);
    EXPECT_CALL(*lfs_, Close(2))
        .Times(1);
    EXPECT_CALL(*lfs_, Close(3))
        .Times(1);
}

/**
 * DeleteSnapshotChunkTest
 * case:chunk存在,snapshot不存在
 *      snapshotSn + 1 > chunk的sn及correctedSn
 * 预期结果:修改correctedSn,返回成功
 */
TEST_F(CSDataStore_test, DeleteSnapshotChunkTest6) {
    // initialize
    FakeEnv();
    EXPECT_TRUE(dataStore->Initialize());

    ChunkID id = 2;
    // snapshotSn + 1 > sn
    // snapshotSn + 1 > correctedSn
    SequenceNum snapshotSn = 2;
    // chunk's metapage will be updated
    EXPECT_CALL(*lfs_, Write(3, NotNull(), 0, PAGE_SIZE))
        .Times(1);
    EXPECT_EQ(CSErrorCode::Success,
              dataStore->DeleteSnapshotChunk(id, snapshotSn));

    EXPECT_CALL(*lfs_, Close(1))
        .Times(1);
    EXPECT_CALL(*lfs_, Close(2))
        .Times(1);
    EXPECT_CALL(*lfs_, Close(3))
        .Times(1);
}

/**
 * DeleteSnapshotChunkErrorTest
 * case:修改correctedSn时失败
 * 预期结果:返回失败，correctedSn的值未改变
 */
TEST_F(CSDataStore_test, DeleteSnapshotChunkErrorTest1) {
    // initialize
    FakeEnv();
    EXPECT_TRUE(dataStore->Initialize());

    ChunkID id = 2;
    // snapshotSn + 1 > sn
    // snapshotSn + 1 > correctedSn
    SequenceNum snapshotSn = 2;

    // write chunk metapage failed
    EXPECT_CALL(*lfs_, Write(3, NotNull(), 0, PAGE_SIZE))
        .WillOnce(Return(-UT_ERRNO));
    EXPECT_EQ(CSErrorCode::InternalError,
              dataStore->DeleteSnapshotChunk(id, snapshotSn));

    // chunk's metapage will be updated
    EXPECT_CALL(*lfs_, Write(3, NotNull(), 0, PAGE_SIZE))
        .Times(1);
    EXPECT_EQ(CSErrorCode::Success,
              dataStore->DeleteSnapshotChunk(id, snapshotSn));

    EXPECT_CALL(*lfs_, Close(1))
        .Times(1);
    EXPECT_CALL(*lfs_, Close(2))
        .Times(1);
    EXPECT_CALL(*lfs_, Close(3))
        .Times(1);
}

/**
 * DeleteSnapshotChunkErrorTest
 * case:回收snapshot的chunk的时候失败
 * 预期结果:返回失败
 */
TEST_F(CSDataStore_test, DeleteSnapshotChunkErrorTest2) {
    // initialize
    FakeEnv();
    EXPECT_TRUE(dataStore->Initialize());

    ChunkID id = 1;
    // snapshotSn + 1 > sn
    // snapshotSn + 1 > correctedSn
    SequenceNum snapshotSn = 2;
    // snapshot will be closed
    EXPECT_CALL(*lfs_, Close(2))
        .Times(1);
    // expect to call chunkfilepool RecycleChunk
    EXPECT_CALL(*fpool_, RecycleChunk(chunk1snap1Path))
        .WillOnce(Return(-1));
    // chunk's metapage will be updated
    EXPECT_CALL(*lfs_, Write(1, NotNull(), 0, PAGE_SIZE))
        .Times(0);
    EXPECT_EQ(CSErrorCode::InternalError,
              dataStore->DeleteSnapshotChunk(id, snapshotSn));

    EXPECT_CALL(*lfs_, Close(1))
        .Times(1);
    EXPECT_CALL(*lfs_, Close(3))
        .Times(1);
}

}  // namespace chunkserver
}  // namespace curve

