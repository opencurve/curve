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

#ifndef CURVEFS_TEST_CLIENT_MOCK_DISK_CACHE_WRITE_H_
#define CURVEFS_TEST_CLIENT_MOCK_DISK_CACHE_WRITE_H_

#include <gmock/gmock.h>
#include <string>
#include <vector>
#include <set>

#include "curvefs/src/client/s3/disk_cache_write.h"

namespace curvefs {
namespace client {

class MockDiskCacheWrite : public DiskCacheWrite {
 public:
    MockDiskCacheWrite() {}
    ~MockDiskCacheWrite() {}

    MOCK_METHOD4(WriteDiskFile,
                  int(const std::string fileName,
                      const char* buf, uint64_t length, bool force));

    MOCK_METHOD1(CreateIoDir,
                 int(bool writreDir));

    MOCK_METHOD1(IsFileExist,
                 bool(const std::string file));

    MOCK_METHOD0(GetCacheIoFullDir,
                 std::string());

    MOCK_METHOD0(UploadAllCacheWriteFile,
                 int());

    MOCK_METHOD1(UploadFile,
                 int(const std::string name));

    MOCK_METHOD0(AsyncUploadFunc,
                 int());

    MOCK_METHOD0(AsyncUploadRun,
                 int());

    MOCK_METHOD1(AsyncUploadEnqueue,
                void(const std::string objName));
    MOCK_METHOD1(UploadFileByInode, int(const std::string &inode));
};

}  // namespace client
}  // namespace curvefs

#endif  // CURVEFS_TEST_CLIENT_MOCK_DISK_CACHE_WRITE_H_
