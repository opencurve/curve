/*
 *  Copyright (c) 2021 NetEase Inc.
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
 * Created Date: Wed Mar 23 2022
 * Author: huyao
 */

#include <gtest/gtest.h>
#include <gmock/gmock.h>

#include "curvefs/src/client/s3/client_s3_adaptor.h"
#include "curvefs/src/client/s3/client_s3_cache_manager.h"
#include "curvefs/test/client/mock_client_s3_cache_manager.h"

namespace curvefs {
namespace client {
using ::testing::_;
using ::testing::DoAll;
using ::testing::Invoke;
using ::testing::Return;
using ::testing::SetArgPointee;
using ::testing::SetArgReferee;
using ::testing::WithArg;

class DataCacheTest : public testing::Test {
 protected:
    DataCacheTest() {}
    ~DataCacheTest() {}
    void SetUp() override {
        S3ClientAdaptorOption option;
        option.blockSize = 1 * 1024 * 1024;
        option.chunkSize = 4 * 1024 * 1024;
        option.baseSleepUs = 500;
        option.objectPrefix = 0;
        option.pageSize = 64 * 1024;
        option.intervalSec = 5000;
        option.flushIntervalSec = 5000;
        option.readCacheMaxByte = 104857600;
        option.readCacheThreads = 5;
        option.diskCacheOpt.diskCacheType = (DiskCacheType)0;
        option.chunkFlushThreads = 5;
        s3ClientAdaptor_ = new S3ClientAdaptorImpl();
        auto fsCacheManager = std::make_shared<FsCacheManager>(
            s3ClientAdaptor_, option.readCacheMaxByte, option.writeCacheMaxByte,
            option.readCacheThreads, nullptr);
        s3ClientAdaptor_->Init(option, nullptr, nullptr, nullptr,
                               fsCacheManager, nullptr, nullptr);
        mockChunkCacheManager_ = std::make_shared<MockChunkCacheManager>();
        uint64_t offset = 512 * 1024;
        uint64_t len = 1024 * 1024;
        char *buf = new char[len];
        dataCache_ = std::make_shared<DataCache>(s3ClientAdaptor_,
                                                 mockChunkCacheManager_, offset,
                                                 len, buf, nullptr);
        delete buf;
    }
    void TearDown() override {}

 protected:
    S3ClientAdaptorImpl *s3ClientAdaptor_;
    std::shared_ptr<DataCache> dataCache_;
    std::shared_ptr<MockChunkCacheManager> mockChunkCacheManager_;
};

TEST_F(DataCacheTest, test_write1) {
    uint64_t offset = 0;
    uint64_t len = 1024 * 1024;
    char *buf = new char[len];
    std::vector<DataCachePtr> mergeDataCacheVer;
    dataCache_->Write(offset, len, buf, mergeDataCacheVer);
    ASSERT_EQ(0, dataCache_->GetChunkPos());
    ASSERT_EQ(1536 * 1024, dataCache_->GetLen());
    delete buf;
}

TEST_F(DataCacheTest, test_write2) {
    uint64_t offset = 0;
    uint64_t len = 2 * 1024 * 1024;
    char *buf = new char[len];
    std::vector<DataCachePtr> mergeDataCacheVer;
    dataCache_->Write(offset, len, buf, mergeDataCacheVer);
    ASSERT_EQ(0, dataCache_->GetChunkPos());
    ASSERT_EQ(2048 * 1024, dataCache_->GetLen());
    delete buf;
}

TEST_F(DataCacheTest, test_write3) {
    uint64_t offset = 0;
    uint64_t len = 2 * 1024 * 1024;
    char *buf = new char[len];
    std::vector<DataCachePtr> mergeDataCacheVer;
    uint64_t offset1 = len;
    auto dataCache = std::make_shared<DataCache>(
        s3ClientAdaptor_, mockChunkCacheManager_, offset1, len, buf, nullptr);
    mergeDataCacheVer.push_back(dataCache);
    dataCache_->Write(offset, len, buf, mergeDataCacheVer);
    ASSERT_EQ(0, dataCache_->GetChunkPos());
    ASSERT_EQ(4096 * 1024, dataCache_->GetLen());
    delete buf;
}

TEST_F(DataCacheTest, test_write4) {
    uint64_t offset = 512 * 1204;
    uint64_t len = 512 * 1024;
    char *buf = new char[len];
    std::vector<DataCachePtr> mergeDataCacheVer;
    dataCache_->Write(offset, len, buf, mergeDataCacheVer);
    ASSERT_EQ(512 * 1024, dataCache_->GetChunkPos());
    ASSERT_EQ(1024 * 1024, dataCache_->GetLen());
    delete buf;
}

TEST_F(DataCacheTest, test_write5) {
    uint64_t offset = 1024 * 1024;
    uint64_t len = 1024 * 1024;
    char *buf = new char[len];
    std::vector<DataCachePtr> mergeDataCacheVer;
    dataCache_->Write(offset, len, buf, mergeDataCacheVer);
    ASSERT_EQ(512 * 1024, dataCache_->GetChunkPos());
    ASSERT_EQ(1536 * 1024, dataCache_->GetLen());
    delete buf;
}

TEST_F(DataCacheTest, test_write6) {
    uint64_t offset = 1024 * 1024;
    uint64_t len = 1024 * 1024;
    char *buf = new char[len];
    std::vector<DataCachePtr> mergeDataCacheVer;
    uint64_t offset1 = offset + len;
    auto dataCache = std::make_shared<DataCache>(
        s3ClientAdaptor_, mockChunkCacheManager_, offset1, len, buf, nullptr);
    mergeDataCacheVer.push_back(dataCache);
    dataCache_->Write(offset, len, buf, mergeDataCacheVer);
    ASSERT_EQ(512 * 1024, dataCache_->GetChunkPos());
    ASSERT_EQ(2560 * 1024, dataCache_->GetLen());
    delete buf;
}

TEST_F(DataCacheTest, test_truncate1) {
    uint64_t size = 0;
    dataCache_->Truncate(size);
    ASSERT_EQ(0, dataCache_->GetLen());
}

TEST_F(DataCacheTest, test_truncate2) {
    uint64_t size = 512 * 1024;
    dataCache_->Truncate(size);
    ASSERT_EQ(512 * 1024, dataCache_->GetLen());
}

TEST_F(DataCacheTest, test_truncate3) {
    uint64_t size = 2;
    dataCache_->Truncate(size);
    ASSERT_EQ(2, dataCache_->GetLen());
}

}  // namespace client
}  // namespace curvefs
