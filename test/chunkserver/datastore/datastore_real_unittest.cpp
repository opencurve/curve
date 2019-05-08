/*
 * Project: curve
 * Created Date: Saturday January 5th 2019
 * Author: yangyaokai
 * Copyright (c) 2018 netease
 */

#include <glog/logging.h>
#include <gtest/gtest.h>
#include <gmock/gmock.h>
#include <json/json.h>
#include <climits>
#include <memory>

#include "src/common/crc32.h"
#include "src/common/curve_define.h"
#include "src/fs/local_filesystem.h"
#include "src/chunkserver/datastore/define.h"
#include "src/chunkserver/datastore/chunkfile_pool.h"
#include "src/chunkserver/datastore/chunkserver_datastore.h"
#include "test/chunkserver/datastore/chunkfilepool_helper.h"

using curve::fs::FileSystemType;
using curve::fs::LocalFileSystem;
using curve::fs::LocalFsFactory;

using curve::common::kChunkFilePoolMaigic;
using ::testing::UnorderedElementsAre;
using curve::chunkserver::ChunkfilePoolHelper;

namespace curve {
namespace chunkserver {

const ChunkSizeType CHUNK_SIZE = 16 * 1024 * 1024;
const PageSizeType PAGE_SIZE = 4096;
const string baseDir = "./data";    // NOLINT

class DatastoreRealTest : public testing::Test {
 public:
    void SetUp() {
        buf = new char[CHUNK_SIZE];
        memset(buf, 0, CHUNK_SIZE);
        lfs_ = LocalFsFactory::CreateFs(FileSystemType::EXT4, "");

        filePool_ = std::make_shared<ChunkfilePool>(lfs_);
        if (filePool_ == nullptr) {
            LOG(FATAL) << "allocate chunkfile pool failed!";
        }
        DataStoreOptions options;
        options.baseDir = baseDir;
        options.chunkSize = CHUNK_SIZE;
        options.pageSize = PAGE_SIZE;
        dataStore_ = std::make_shared<CSDataStore>(lfs_,
                                                   filePool_,
                                                   options);
        if (dataStore_ == nullptr) {
            LOG(FATAL) << "allocate chunkfile pool failed!";
        }

        uint32_t chunksize = CHUNK_SIZE;
        uint32_t metapagesize = PAGE_SIZE;
        std::string dirname = "./chunkfilepool";

        int ret = ChunkfilePoolHelper::PersistEnCodeMetaInfo(
                                                    lfs_,
                                                    chunksize,
                                                    metapagesize,
                                                    dirname,
                                                    "./chunkfilepool.meta");

        if (ret == -1) {
            LOG(ERROR) << "persist chunkfile pool meta info failed!";
            return;
        }

        std::string chunkfilepool = "./chunkfilepool.meta";
        ChunkfilePoolOptions cfop;
        cfop.chunkSize = CHUNK_SIZE;
        cfop.metaPageSize = PAGE_SIZE;
        memcpy(cfop.metaPath, chunkfilepool.c_str(), chunkfilepool.size());

        allocateChunk(lfs_, 10);
        ASSERT_TRUE(filePool_->Initialize(cfop));
        ASSERT_EQ(10, filePool_->Size());
        ASSERT_TRUE(dataStore_->Initialize());
    }

    void TearDown() {
        lfs_->Delete("./chunkfilepool");
        lfs_->Delete(baseDir);
        lfs_->Delete("./chunkfilepool.meta");
        filePool_->UnInitialize();
        delete [] buf;
        buf = nullptr;
    }

 protected:
    char* buf;
    std::shared_ptr<ChunkfilePool>  filePool_;
    std::shared_ptr<LocalFileSystem>  lfs_;
    std::shared_ptr<CSDataStore> dataStore_;
};

TEST_F(DatastoreRealTest, CombineTest) {
    ChunkID id = 1;
    SequenceNum sn = 1;
    off_t offset = 0;
    size_t length = PAGE_SIZE;
    std::string chunkPath = baseDir + "/" +
        FileNameOperator::GenerateChunkFileName(id);
    std::string snap1Path = baseDir + "/" +
        FileNameOperator::GenerateSnapshotName(id, 1);
    CSErrorCode errorCode;
    CSChunkInfo info;

    // chunk不存在时的相关验证
    {
        // 文件不存在
        ASSERT_FALSE(lfs_->FileExists(chunkPath));
        // 无法获取到chunk的版本号
        errorCode = dataStore_->GetChunkInfo(id, &info);
        ASSERT_EQ(errorCode, CSErrorCode::ChunkNotExistError);
        // 读chunk时返回ChunkNotExistError
        char readbuf[PAGE_SIZE];
        errorCode = dataStore_->ReadChunk(id, sn, readbuf, offset, length);
        ASSERT_EQ(errorCode, CSErrorCode::ChunkNotExistError);
        errorCode =
            dataStore_->ReadSnapshotChunk(id, sn, readbuf, offset, length);
        ASSERT_EQ(errorCode, CSErrorCode::ChunkNotExistError);
    }

    // 第一次写空的chunk，会生成chunk文件
    {
        memset(buf, 'a', length);
        // 先写4KB
        errorCode = dataStore_->WriteChunk(id,
                                           sn,
                                           buf + offset,
                                           offset,
                                           length,
                                           nullptr);
        ASSERT_EQ(errorCode, CSErrorCode::Success);
        // 会从chunkfilepool rename一个chunk到data目录
        ASSERT_EQ(9, filePool_->Size());
        ASSERT_TRUE(lfs_->FileExists(chunkPath));
        // 可以获取到chunk的版本号
        errorCode = dataStore_->GetChunkInfo(id, &info);
        ASSERT_EQ(errorCode, CSErrorCode::Success);
        ASSERT_EQ(1, info.curSn);
        ASSERT_EQ(0, info.snapSn);
        // 读取写入的4KB验证一下,应当与写入数据相等
        char readbuf[PAGE_SIZE];
        errorCode = dataStore_->ReadChunk(id, sn, readbuf, offset, length);
        ASSERT_EQ(errorCode, CSErrorCode::Success);
        ASSERT_EQ(0, memcmp(buf + offset, readbuf, length));
        // 没被写过的区域也可以读，但是不保证读到的数据内容
        errorCode = dataStore_->ReadChunk(id,
                                          sn,
                                          readbuf,
                                          offset + length,
                                          length);
        ASSERT_EQ(errorCode, CSErrorCode::Success);
        // 如果读超过chunk大小的区域会报错
        errorCode = dataStore_->ReadChunk(id,
                                          sn,
                                          readbuf,
                                          CHUNK_SIZE,
                                          length);
        ASSERT_EQ(errorCode, CSErrorCode::OutOfRangeError);
        // 调ReadSnapshotChunk
        errorCode = dataStore_->ReadSnapshotChunk(id,
                                                  sn,
                                                  readbuf,
                                                  offset,
                                                  length);
        ASSERT_EQ(errorCode, CSErrorCode::Success);
        ASSERT_EQ(0, memcmp(buf + offset, readbuf, length));
        // 调ReadSnapshotChunk读不存在的版本号会返回错误
        errorCode = dataStore_->ReadSnapshotChunk(id,
                                                  sn + 1,
                                                  readbuf,
                                                  offset,
                                                  length);
        ASSERT_EQ(errorCode, CSErrorCode::ChunkNotExistError);
    }

    // 模拟打快照过程
    {
        // 先将chunk的数据全部覆盖为a，方便后面验证
        memset(buf, 'a', CHUNK_SIZE);
        errorCode = dataStore_->WriteChunk(id,
                                           sn,
                                           buf,
                                           0,
                                           CHUNK_SIZE,
                                           nullptr);
        ASSERT_EQ(errorCode, CSErrorCode::Success);
        // 如果打了快照，版本号会增加，此时如果转储系统还未处理此chunk
        // 则此时写入chunk会产生快照文件
        // 往Chunk文件的[0, 4MB)写入数据，数据内容为b
        ++sn;
        offset = 0;
        length = 1024 * PAGE_SIZE;
        memset(buf, 'b', length);
        errorCode = dataStore_->WriteChunk(id,
                                           sn,
                                           buf + offset,
                                           offset,
                                           length,
                                           nullptr);
        ASSERT_EQ(errorCode, CSErrorCode::Success);
        // 产生快照文件
        errorCode = dataStore_->GetChunkInfo(id, &info);
        ASSERT_EQ(errorCode, CSErrorCode::Success);
        ASSERT_EQ(2, info.curSn);
        ASSERT_EQ(1, info.snapSn);
        ASSERT_EQ(8, filePool_->Size());
        ASSERT_TRUE(lfs_->FileExists(snap1Path));
        // 为了验证统一区域重复写入只会cow一次，
        // 向chunk文件的[2MB, 6MB)区域写入数据c
        offset = 512 * PAGE_SIZE;
        length = 1024 * PAGE_SIZE;
        memset(buf + offset, 'c', length);
        errorCode = dataStore_->WriteChunk(id,
                                           sn,
                                           buf + offset,
                                           offset,
                                           length,
                                           nullptr);
        ASSERT_EQ(errorCode, CSErrorCode::Success);
        // 此时chunk中的数据内容应该为[0,2MB]:b,[2MB, 6MB]:c,[6MB,end]:a
        char* readbuf = new char[CHUNK_SIZE];
        errorCode = dataStore_->ReadChunk(id,
                                          sn,
                                          readbuf,
                                          0,
                                          CHUNK_SIZE);
        ASSERT_EQ(errorCode, CSErrorCode::Success);
        ASSERT_EQ(0, memcmp(buf, readbuf, CHUNK_SIZE));
        // 快照中的数据内容全为a
        SequenceNum snapSn = 1;
        errorCode = dataStore_->ReadSnapshotChunk(id,
                                                  snapSn,
                                                  readbuf,
                                                  0,
                                                  CHUNK_SIZE);
        ASSERT_EQ(errorCode, CSErrorCode::Success);
        memset(buf, 'a', CHUNK_SIZE);
        ASSERT_EQ(0, memcmp(buf, readbuf, CHUNK_SIZE));
        delete[] readbuf;
        // 模拟存在快照的情况下删除chunk，会返回错误
        errorCode = dataStore_->DeleteChunk(id, sn);
        ASSERT_EQ(errorCode, CSErrorCode::SnapshotExistError);
        // 转储完之后，删除快照
        errorCode = dataStore_->DeleteSnapshotChunkOrCorrectSn(id, sn);
        ASSERT_EQ(errorCode, CSErrorCode::Success);
        ASSERT_FALSE(lfs_->FileExists(snap1Path));
        errorCode = dataStore_->GetChunkInfo(id, &info);
        ASSERT_EQ(errorCode, CSErrorCode::Success);
        ASSERT_EQ(2, info.curSn);
        ASSERT_EQ(0, info.snapSn);
        ASSERT_EQ(0, info.correctedSn);
    }

    // 模拟上次快照之后从未被写过，然后在新的快照中delete
    {
        // 假设打完文件快照后，文件版本为5，当前快照版本为4
        sn = 5;
        errorCode = dataStore_->DeleteSnapshotChunkOrCorrectSn(id, sn);
        ASSERT_EQ(errorCode, CSErrorCode::Success);
        errorCode = dataStore_->GetChunkInfo(id, &info);
        ASSERT_EQ(errorCode, CSErrorCode::Success);
        // 此时chunk的版本仍然为2
        ASSERT_EQ(2, info.curSn);
        ASSERT_EQ(0, info.snapSn);
        ASSERT_EQ(sn, info.correctedSn);
        // 删完以后如果有写入操作，不会产生快照文件，但会变更版本号
        offset = 0;
        length = 1024 * PAGE_SIZE;
        memset(buf + offset, 'd', length);
        errorCode = dataStore_->WriteChunk(id,
                                           sn,
                                           buf + offset,
                                           offset,
                                           length,
                                           nullptr);
        // 此时chunk数据为[0,4MB]:d,[4MB,6MB]:c,[6MB,16MB]:a
        ASSERT_EQ(errorCode, CSErrorCode::Success);
        // chunk 版本号变为5，不会产生快照
        errorCode = dataStore_->GetChunkInfo(id, &info);
        ASSERT_EQ(errorCode, CSErrorCode::Success);
        ASSERT_EQ(5, info.curSn);
        ASSERT_EQ(0, info.snapSn);
    }

    // 模拟删除chunk
    {
        errorCode = dataStore_->DeleteChunk(id, sn);
        ASSERT_EQ(errorCode, CSErrorCode::Success);
        errorCode = dataStore_->GetChunkInfo(id, &info);
        ASSERT_EQ(errorCode, CSErrorCode::ChunkNotExistError);
        ASSERT_FALSE(lfs_->FileExists(chunkPath));
    }
}
}  // namespace chunkserver
}  // namespace curve
