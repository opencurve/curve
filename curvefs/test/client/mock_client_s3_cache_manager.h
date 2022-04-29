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
 * Date: Friday Oct 22 15:12:14 CST 2021
 * Author: wuhanqing
 */

#ifndef CURVEFS_TEST_CLIENT_MOCK_CLIENT_S3_CACHE_MANAGER_H_
#define CURVEFS_TEST_CLIENT_MOCK_CLIENT_S3_CACHE_MANAGER_H_

#include <gmock/gmock.h>
#include <vector>
#include "curvefs/src/client/s3/client_s3_cache_manager.h"

namespace curvefs {
namespace client {

class MockFsCacheManager : public FsCacheManager {
 public:
    MockFsCacheManager() : FsCacheManager() {}
    ~MockFsCacheManager() {}
    MOCK_METHOD2(FindOrCreateFileCacheManager,
                 FileCacheManagerPtr(uint64_t fsId, uint64_t inodeId));
    MOCK_METHOD1(FindFileCacheManager, FileCacheManagerPtr(uint64_t inodeId));
    MOCK_METHOD0(GetDataCacheMaxSize, uint64_t());
    MOCK_METHOD0(GetDataCacheSize, uint64_t());
    MOCK_METHOD0(MemCacheRatio, uint64_t());
};

class MockFileCacheManager : public FileCacheManager {
 public:
    MockFileCacheManager() : FileCacheManager() {}
    ~MockFileCacheManager() {}
    MOCK_METHOD3(Write,
                 int(uint64_t offset, uint64_t length, const char *dataBuf));
    MOCK_METHOD4(Read, int(uint64_t inodeId, uint64_t offset, uint64_t length,
                           char *dataBuf));
    MOCK_METHOD2(Flush, CURVEFS_ERROR(bool force, bool toS3));
    MOCK_METHOD2(TruncateCache, void(uint64_t offset, uint64_t fileSize));
};

class MockChunkCacheManager : public ChunkCacheManager {
 public:
    MockChunkCacheManager() : ChunkCacheManager(0, nullptr) {}
    ~MockChunkCacheManager() = default;

    MOCK_METHOD1(ReleaseReadDataCache, void(uint64_t));
    MOCK_METHOD3(Flush, CURVEFS_ERROR(uint64_t inodeId, bool force, bool toS3));
    MOCK_METHOD4(FindWriteableDataCache,
                 DataCachePtr(uint64_t chunkPos, uint64_t len,
                              std::vector<DataCachePtr> *mergeDataCacheVer,
                              uint64_t inodeId));
    MOCK_METHOD4(WriteNewDataCache,
                 void(S3ClientAdaptorImpl *s3ClientAdaptor, uint32_t chunkPos,
                      uint32_t len, const char *data));
    MOCK_METHOD5(ReadByWriteCache, void(uint64_t chunkPos, uint64_t readLen,
                                        char *dataBuf, uint64_t dataBufOffset,
                                        std::vector<ReadRequest> *requests));
    MOCK_METHOD5(ReadByReadCache, void(uint64_t chunkPos, uint64_t readLen,
                                       char *dataBuf, uint64_t dataBufOffset,
                                       std::vector<ReadRequest> *requests));
    MOCK_METHOD0(ReleaseCache, void());
    MOCK_METHOD1(AddReadDataCache, void(DataCachePtr dataCache));
};

class MockDataCache : public DataCache {
 public:
    MockDataCache(S3ClientAdaptorImpl *s3ClientAdaptor,
                  ChunkCacheManagerPtr chunkCacheManager, uint64_t chunkPos,
                  uint64_t len, const char *data)
        : DataCache(s3ClientAdaptor, chunkCacheManager, chunkPos, len, data) {}
    ~MockDataCache() = default;

    MOCK_METHOD4(Write,
                 void(uint64_t chunkPos, uint64_t len, const char *data,
                      const std::vector<DataCachePtr> &mergeDataCacheVer));
    MOCK_METHOD2(Flush, CURVEFS_ERROR(uint64_t inodeId, bool toS3));
    MOCK_METHOD1(Truncate, void(uint64_t size));
    MOCK_METHOD1(CanFlush, bool(bool force));
};

}  // namespace client
}  // namespace curvefs

#endif  // CURVEFS_TEST_CLIENT_MOCK_CLIENT_S3_CACHE_MANAGER_H_
