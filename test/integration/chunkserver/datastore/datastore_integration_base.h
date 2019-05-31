/*
 * Project: curve
 * Created Date: Wednesday May 22nd 2019
 * Author: yangyaokai
 * Copyright (c) 2019 netease
 */

#ifndef TEST_INTEGRATION_CHUNKSERVER_DATASTORE_DATASTORE_INTEGRATION_BASE_H_
#define TEST_INTEGRATION_CHUNKSERVER_DATASTORE_DATASTORE_INTEGRATION_BASE_H_

#include <glog/logging.h>
#include <gtest/gtest.h>
#include <gmock/gmock.h>
#include <time.h>
#include <climits>
#include <memory>

#include "src/common/concurrent/concurrent.h"
#include "src/common/timeutility.h"
#include "src/fs/local_filesystem.h"
#include "src/chunkserver/datastore/define.h"
#include "src/chunkserver/datastore/chunkfile_pool.h"
#include "src/chunkserver/datastore/chunkserver_datastore.h"
#include "test/chunkserver/datastore/chunkfilepool_helper.h"

using curve::fs::FileSystemType;
using curve::fs::LocalFileSystem;
using curve::fs::LocalFsFactory;
using curve::common::Atomic;
using curve::common::Thread;
using curve::common::TimeUtility;

using ::testing::UnorderedElementsAre;

namespace curve {
namespace chunkserver {

const uint64_t kMB = 1024 * 1024;
const ChunkSizeType CHUNK_SIZE = 16 * kMB;
const PageSizeType PAGE_SIZE = 4 * 1024;

extern const string baseDir;    // NOLINT
extern const string poolDir;  // NOLINT
extern const string poolMetaPath;  // NOLINT

/**
 * DataStore层集成LocalFileSystem层测试
 */
class DatastoreIntegrationBase : public testing::Test {
 public:
    DatastoreIntegrationBase() {}
    virtual ~DatastoreIntegrationBase() {}

    virtual void SetUp() {
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

        ChunkfilePoolHelper::PersistEnCodeMetaInfo(lfs_,
                                                   CHUNK_SIZE,
                                                   PAGE_SIZE,
                                                   poolDir,
                                                   poolMetaPath);

        InitChunkPool(10);
        ASSERT_TRUE(dataStore_->Initialize());
    }

    void InitChunkPool(int chunkNum) {
        filePool_->UnInitialize();

        ChunkfilePoolOptions cfop;
        cfop.chunkSize = CHUNK_SIZE;
        cfop.metaPageSize = PAGE_SIZE;
        memcpy(cfop.metaPath, poolMetaPath.c_str(), poolMetaPath.size());

        if (lfs_->DirExists(poolDir))
            lfs_->Delete(poolDir);
        allocateChunk(lfs_, chunkNum, poolDir);
        ASSERT_TRUE(filePool_->Initialize(cfop));
        ASSERT_EQ(chunkNum, filePool_->Size());
    }

    virtual void TearDown() {
        lfs_->Delete(poolDir);
        lfs_->Delete(baseDir);
        lfs_->Delete(poolMetaPath);
        filePool_->UnInitialize();
        dataStore_ = nullptr;
    }

 protected:
    std::shared_ptr<ChunkfilePool>  filePool_;
    std::shared_ptr<LocalFileSystem>  lfs_;
    std::shared_ptr<CSDataStore> dataStore_;
};

}  // namespace chunkserver
}  // namespace curve

#endif  // TEST_INTEGRATION_CHUNKSERVER_DATASTORE_DATASTORE_INTEGRATION_BASE_H_
