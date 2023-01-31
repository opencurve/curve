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
 * Created Date: Mon Aug 30 2021
 * Author: hzwuhongsong
 */

#ifndef CURVEFS_TEST_CLIENT_MOCK_DISK_CACHE_MANAGER_H_
#define CURVEFS_TEST_CLIENT_MOCK_DISK_CACHE_MANAGER_H_

#include <gmock/gmock.h>

#include <string>
#include <utility>
#include <vector>
#include <set>
#include <memory>
#include <list>

#include "curvefs/src/client/s3/client_s3_adaptor.h"
#include "curvefs/src/client/s3/disk_cache_manager.h"
#include "curvefs/src/client/s3/disk_cache_manager_impl.h"

namespace curvefs {
namespace client {

class MockDiskCacheManager : public DiskCacheManager {
 public:
    MockDiskCacheManager(std::shared_ptr<PosixWrapper> posixWrapper,
                    std::shared_ptr<DiskCacheWrite> cacheWrite,
                    std::shared_ptr<DiskCacheRead> cacheRead)
                    : DiskCacheManager(posixWrapper, cacheWrite, cacheRead) {}
    ~MockDiskCacheManager() {}

    MOCK_METHOD2(Init,
                 int(std::shared_ptr<S3Client> client,
                 const S3ClientAdaptorOption option));
    MOCK_METHOD0(IsDiskCacheFull,
                 bool());
    MOCK_METHOD3(WriteReadDirect,
                  int(const std::string fileName,
                      const char* buf, uint64_t length));
    MOCK_METHOD0(IsDiskUsedInited,
                 bool());
};

class MockDiskCacheManager2 : public DiskCacheManager {
 public:
    MockDiskCacheManager2() : DiskCacheManager() {}
    ~MockDiskCacheManager2() {}

    MOCK_METHOD2(Init,
                 int(S3Client *client, const S3ClientAdaptorOption option));
    MOCK_METHOD0(IsDiskCacheFull, bool());
    MOCK_METHOD3(WriteReadDirect, int(const std::string fileName,
                                      const char *buf, uint64_t length));
    MOCK_METHOD1(IsCached, bool(const std::string));
};

class MockDiskCacheManagerImpl : public DiskCacheManagerImpl {
 public:
    MockDiskCacheManagerImpl() : DiskCacheManagerImpl() {}
    MockDiskCacheManagerImpl(std::shared_ptr<DiskCacheManager> diskCacheManager,
                             std::shared_ptr<S3Client> client)
        : DiskCacheManagerImpl(std::move(diskCacheManager), std::move(client)) {
    }
    ~MockDiskCacheManagerImpl() {}

    MOCK_METHOD1(UploadWriteCacheByInode, int(const std::string &inode));
    MOCK_METHOD1(ClearReadCache, int(const std::list<std::string> &files));
    MOCK_METHOD1(IsCached, bool(const std::string));
};

}  // namespace client
}  // namespace curvefs

#endif  // CURVEFS_TEST_CLIENT_MOCK_DISK_CACHE_MANAGER_H_
