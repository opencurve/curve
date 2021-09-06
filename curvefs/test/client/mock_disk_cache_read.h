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

#ifndef CURVEFS_TEST_CLIENT_MOCK_DISK_CACHE_READ_H_
#define CURVEFS_TEST_CLIENT_MOCK_DISK_CACHE_READ_H_

#include <gmock/gmock.h>

#include <string>
#include <vector>
#include <set>

#include "curvefs/src/client/s3/disk_cache_read.h"

namespace curvefs {
namespace client {

class MockDiskCacheRead : public DiskCacheRead {
 public:
    MockDiskCacheRead()  {}
    ~MockDiskCacheRead() {}

    MOCK_METHOD4(ReadDiskFile,
                 int(const std::string name, char* buf,
                     uint64_t offset, uint64_t length));

    MOCK_METHOD1(CreateIoDir,
                 int(bool writreDir));

    MOCK_METHOD1(IsFileExist,
                 bool(const std::string file));

    MOCK_METHOD0(GetCacheIoFullDir,
                 std::string());

    MOCK_METHOD3(LinkWriteToRead,
                 int(const std::string fileName,
                     const std::string fullWriteDir,
                     const std::string fullReadDir));

    MOCK_METHOD1(LoadAllCacheReadFile,
                 int(std::set<std::string>* cachedObj));
};


}  // namespace client
}  // namespace curvefs

#endif  // CURVEFS_TEST_CLIENT_MOCK_DISK_CACHE_READ_H_
