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

class ChunkCacheManagerTest : public testing::Test {
 protected:
    ChunkCacheManagerTest() {}
    ~ChunkCacheManagerTest() {}
    void SetUp() override {
        uint64_t index = 0;

        S3ClientAdaptorOption option;
        option.blockSize = 1 * 1024 * 1024;
        option.chunkSize = 4 * 1024 * 1024;
        option.pageSize = 64 * 1024;
        option.intervalSec = 5000;
        option.flushIntervalSec = 5000;
        option.readCacheMaxByte = 104857600;
        option.writeCacheMaxByte = 10485760000;
        option.chunkFlushThreads = 5;
        option.diskCacheOpt.diskCacheType = (DiskCacheType)0;
        s3ClientAdaptor_ = new S3ClientAdaptorImpl();
        auto fsCacheManager_ = std::make_shared<FsCacheManager>(
            s3ClientAdaptor_, option.readCacheMaxByte, option.writeCacheMaxByte,
            nullptr);
        s3ClientAdaptor_->Init(option, nullptr, nullptr, nullptr,
                               fsCacheManager_, nullptr, nullptr);
        chunkCacheManager_ = std::make_shared<ChunkCacheManager>(
            index, s3ClientAdaptor_, nullptr);
    }
    void TearDown() override {
        delete s3ClientAdaptor_;
        s3ClientAdaptor_ = nullptr;
    }

 protected:
    S3ClientAdaptorImpl *s3ClientAdaptor_;
    std::shared_ptr<ChunkCacheManager> chunkCacheManager_;
};

TEST_F(ChunkCacheManagerTest, test_write_new_data) {
    uint64_t offset = 0;
    uint64_t len = 1024;
    int length = len;
    char *buf = new char[len];

    chunkCacheManager_->WriteNewDataCache(s3ClientAdaptor_, offset, len, buf);
    ASSERT_EQ(65536, s3ClientAdaptor_->GetFsCacheManager()->GetDataCacheSize());

    delete buf;
}

TEST_F(ChunkCacheManagerTest, test_add_read_data_cache) {
     VLOG(0) << "wghs000";
    uint64_t offset = 0;
    uint64_t len = 1024 * 1024;
    char *buf = new char[len];
    auto dataCache = std::make_shared<DataCache>(
        s3ClientAdaptor_, chunkCacheManager_, offset, len, buf, nullptr);
    VLOG(0) << "wghs001";
    chunkCacheManager_->AddReadDataCache(dataCache);
    VLOG(0) << "wghs002";
    ASSERT_EQ(len, s3ClientAdaptor_->GetFsCacheManager()->GetLruByte());
    offset = 2 * 1024 * 1024;
    auto dataCache1 = std::make_shared<DataCache>(
        s3ClientAdaptor_, chunkCacheManager_, offset, len, buf, nullptr);
    chunkCacheManager_->AddReadDataCache(dataCache1);
    ASSERT_EQ(2 * len, s3ClientAdaptor_->GetFsCacheManager()->GetLruByte());
    offset = 0;
    len = 512 * 1024;
    auto dataCache2 = std::make_shared<DataCache>(
        s3ClientAdaptor_, chunkCacheManager_, offset, len, buf, nullptr);
    chunkCacheManager_->AddReadDataCache(dataCache2);
    ASSERT_EQ(1.5 * 1024 * 1024,
              s3ClientAdaptor_->GetFsCacheManager()->GetLruByte());
    delete buf;
}

TEST_F(ChunkCacheManagerTest, test_find_writeable_datacache) {
    uint64_t inodeId = 1;
    uint64_t offset = 0;
    uint64_t len = 1024 * 1024;
    char *buf = new char[len];

    std::vector<DataCachePtr> mergeDataCacheVer;
    ASSERT_EQ(nullptr, chunkCacheManager_->FindWriteableDataCache(
                           offset, len, &mergeDataCacheVer, inodeId));
    chunkCacheManager_->WriteNewDataCache(s3ClientAdaptor_, offset, len, buf);
    len = 512 * 1024;
    ASSERT_NE(nullptr, chunkCacheManager_->FindWriteableDataCache(
                           offset, len, &mergeDataCacheVer, inodeId));
    ASSERT_EQ(0, mergeDataCacheVer.size());
    offset = 2 * 1024 * 1024;
    chunkCacheManager_->WriteNewDataCache(s3ClientAdaptor_, offset, len, buf);
    offset = 1024 * 1024;
    len = 1024 * 1024;
    ASSERT_NE(nullptr, chunkCacheManager_->FindWriteableDataCache(
                           offset, len, &mergeDataCacheVer, inodeId));
    ASSERT_EQ(1, mergeDataCacheVer.size());

    delete buf;
}

TEST_F(ChunkCacheManagerTest, test_read_by_write_cache) {
    uint64_t offset = 0;
    uint64_t len = 1024;
    char *buf = new char[len + 1];
    char *dataCacheBuf = new char[len];
    char *expectBuf = new char[len + 1];
    std::vector<ReadRequest> requests;
    memset(buf, 'a', len);
    memset(buf + len, 0, 1);
    memset(dataCacheBuf, 'b', len);

    chunkCacheManager_->ReadByWriteCache(offset, len, buf, 0, &requests);
    ASSERT_EQ(1, requests.size());
    requests.clear();

    chunkCacheManager_->WriteNewDataCache(s3ClientAdaptor_, 1024, len,
                                          dataCacheBuf);
    chunkCacheManager_->ReadByWriteCache(offset, len, buf, 0, &requests);
    ASSERT_EQ(1, requests.size());
    requests.clear();
    /*
                 -----               ReadData
                    ------           DataCache
           */
    offset = 512;
    chunkCacheManager_->ReadByWriteCache(offset, len, buf, 0, &requests);
    memset(expectBuf, 'a', 512);
    memset(expectBuf + 512, 'b', 512);
    memset(expectBuf + len, 0, 1);
    EXPECT_STREQ(buf, expectBuf);
    ASSERT_EQ(1, requests.size());
    requests.clear();

    len = 2048;
    delete buf;
    delete expectBuf;
    buf = new char[len + 1];
    expectBuf = new char[len + 1];
    memset(buf, 'a', len);
    memset(buf + len, 0, 1);
    chunkCacheManager_->ReadByWriteCache(offset, len, buf, 0, &requests);
    memset(expectBuf, 'a', len);
    memset(expectBuf + 512, 'b', 1024);
    memset(expectBuf + len, 0, 1);
    EXPECT_STREQ(buf, expectBuf);
    ASSERT_EQ(2, requests.size());
    requests.clear();

    len = 512;
    delete buf;
    delete expectBuf;
    buf = new char[len + 1];
    expectBuf = new char[len + 1];
    memset(buf, 'a', len - 1);
    memset(buf + len, 0, 1);
    offset = 1024;
    chunkCacheManager_->ReadByWriteCache(offset, len, buf, 0, &requests);
    memset(expectBuf, 'b', len);
    memset(expectBuf + len, 0, 1);
    EXPECT_STREQ(buf, expectBuf);
    ASSERT_EQ(0, requests.size());
    requests.clear();

    len = 1024;
    delete buf;
    delete expectBuf;
    buf = new char[len + 1];
    expectBuf = new char[len + 1];
    memset(buf, 'a', len);
    memset(buf + len, 0, 1);
    offset = 1536;
    chunkCacheManager_->ReadByWriteCache(offset, len, buf, 0, &requests);
    memset(expectBuf, 'a', len);
    memset(expectBuf, 'b', 512);
    memset(expectBuf + len, 0, 1);
    EXPECT_STREQ(buf, expectBuf);
    ASSERT_EQ(1, requests.size());
    requests.clear();

    delete buf;
    delete expectBuf;
    delete dataCacheBuf;
}

TEST_F(ChunkCacheManagerTest, test_read_by_read_cache) {
    uint64_t offset = 0;
    uint64_t len = 1024;
    char *buf = new char[len + 1];
    char *dataCacheBuf = new char[len];
    char *expectBuf = new char[len + 1];
    std::vector<ReadRequest> requests;
    memset(buf, 'a', len);
    memset(buf + len, 0, 1);
    memset(dataCacheBuf, 'b', len);

    chunkCacheManager_->ReadByReadCache(offset, len, buf, 0, &requests);
    ASSERT_EQ(1, requests.size());
    requests.clear();

    auto dataCache = std::make_shared<DataCache>(
        s3ClientAdaptor_, chunkCacheManager_, 1024, len, dataCacheBuf, nullptr);
    chunkCacheManager_->AddReadDataCache(dataCache);
    chunkCacheManager_->ReadByReadCache(offset, len, buf, 0, &requests);
    ASSERT_EQ(1, requests.size());
    requests.clear();
    /*
                 -----               ReadData
                    ------           DataCache
           */
    offset = 512;
    chunkCacheManager_->ReadByReadCache(offset, len, buf, 0, &requests);
    memset(expectBuf, 'a', 512);
    memset(expectBuf + 512, 'b', 512);
    memset(expectBuf + len, 0, 1);
    EXPECT_STREQ(buf, expectBuf);
    ASSERT_EQ(1, requests.size());
    requests.clear();

    len = 2048;
    delete buf;
    delete expectBuf;
    buf = new char[len + 1];
    expectBuf = new char[len + 1];
    memset(buf, 'a', len);
    memset(buf + len, 0, 1);
    chunkCacheManager_->ReadByReadCache(offset, len, buf, 0, &requests);
    memset(expectBuf, 'a', len);
    memset(expectBuf + 512, 'b', 1024);
    memset(expectBuf + len, 0, 1);
    EXPECT_STREQ(buf, expectBuf);
    ASSERT_EQ(2, requests.size());
    requests.clear();

    len = 512;
    delete buf;
    delete expectBuf;
    buf = new char[len + 1];
    expectBuf = new char[len + 1];
    memset(buf, 'a', len - 1);
    memset(buf + len, 0, 1);
    offset = 1024;
    chunkCacheManager_->ReadByReadCache(offset, len, buf, 0, &requests);
    memset(expectBuf, 'b', len);
    memset(expectBuf + len, 0, 1);
    EXPECT_STREQ(buf, expectBuf);
    ASSERT_EQ(0, requests.size());
    requests.clear();

    len = 1024;
    delete buf;
    delete expectBuf;
    buf = new char[len + 1];
    expectBuf = new char[len + 1];
    memset(buf, 'a', len);
    memset(buf + len, 0, 1);
    offset = 1536;
    chunkCacheManager_->ReadByReadCache(offset, len, buf, 0, &requests);
    memset(expectBuf, 'a', len);
    memset(expectBuf, 'b', 512);
    memset(expectBuf + len, 0, 1);
    EXPECT_STREQ(buf, expectBuf);
    ASSERT_EQ(1, requests.size());
    requests.clear();

    delete buf;
    delete expectBuf;
    delete dataCacheBuf;
}

TEST_F(ChunkCacheManagerTest, test_flush) {
    uint64_t inodeId = 1;
    uint64_t offset = 0;
    uint64_t len = 1024 * 1024;
    char *buf = new char[len];
    auto dataCache = std::make_shared<MockDataCache>(
        s3ClientAdaptor_, chunkCacheManager_, offset, len, buf, nullptr);
    EXPECT_CALL(*dataCache, Flush(_, _))
        .WillOnce(Return(CURVEFS_ERROR::OK))
        .WillOnce(Return(CURVEFS_ERROR::INTERNAL))
        .WillOnce(Return(CURVEFS_ERROR::OK));
    EXPECT_CALL(*dataCache, CanFlush(_))
        .WillOnce(Return(true))
        .WillOnce(Return(false))
        .WillOnce(Return(true));

    chunkCacheManager_->AddWriteDataCacheForTest(dataCache);

    ASSERT_EQ(CURVEFS_ERROR::OK,
              chunkCacheManager_->Flush(inodeId, true, true));
    chunkCacheManager_->AddWriteDataCacheForTest(dataCache);
    ASSERT_EQ(CURVEFS_ERROR::OK,
              chunkCacheManager_->Flush(inodeId, true, true));

    ASSERT_EQ(CURVEFS_ERROR::OK,
              chunkCacheManager_->Flush(inodeId, true, true));
    chunkCacheManager_->ReleaseCacheForTest();

    delete buf;
}

TEST_F(ChunkCacheManagerTest, test_release_read_dataCache) {
    uint64_t offset = 0;
    uint64_t len = 1024 * 1024;
    char *buf = new char[len];
    auto dataCache = std::make_shared<DataCache>(
        s3ClientAdaptor_, chunkCacheManager_, offset, len, buf, nullptr);
    chunkCacheManager_->AddReadDataCache(dataCache);
    chunkCacheManager_->ReleaseReadDataCache(offset);
    ASSERT_EQ(true, chunkCacheManager_->IsEmpty());

    delete buf;
}


TEST_F(ChunkCacheManagerTest, test_truncate_cache) {
    uint64_t offset = 0;
    uint64_t len = 1024 * 1024;
    char *buf = new char[len];
    auto dataCache = std::make_shared<DataCache>(
        s3ClientAdaptor_, chunkCacheManager_, offset, len, buf, nullptr);
    /*EXPECT_CALL(*dataCache, Truncate(_))
        .WillOnce(Return());*/
    chunkCacheManager_->AddWriteDataCacheForTest(dataCache);
    ASSERT_EQ(len, s3ClientAdaptor_->GetFsCacheManager()->GetDataCacheSize());
    chunkCacheManager_->TruncateCache(0);
    ASSERT_EQ(0, s3ClientAdaptor_->GetFsCacheManager()->GetDataCacheSize());

    chunkCacheManager_->AddWriteDataCacheForTest(dataCache);
    ASSERT_EQ(len, s3ClientAdaptor_->GetFsCacheManager()->GetDataCacheSize());
    chunkCacheManager_->TruncateCache(512 * 1024);
    ASSERT_EQ(len - 512 * 1024,
              s3ClientAdaptor_->GetFsCacheManager()->GetDataCacheSize());
    chunkCacheManager_->ReleaseCacheForTest();

    chunkCacheManager_->AddReadDataCache(dataCache);
    chunkCacheManager_->TruncateCache(512);
    ASSERT_EQ(true, chunkCacheManager_->IsEmpty());

    chunkCacheManager_->ReleaseCacheForTest();
    delete buf;
}

}  // namespace client
}  // namespace curvefs

