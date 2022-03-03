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
 * Created Date: Friday December 7th 2018
 * Author: yangyaokai
 */

#ifndef TEST_FS_MOCK_LOCAL_FILESYSTEM_H_
#define TEST_FS_MOCK_LOCAL_FILESYSTEM_H_

#include <gmock/gmock.h>
#include <string>
#include <vector>

#include "src/fs/local_filesystem.h"

namespace curve {
namespace fs {

class MockLocalFileSystem : public LocalFileSystem {
 public:
    ~MockLocalFileSystem() {}
    MOCK_METHOD1(Init, int(const LocalFileSystemOption&));
    MOCK_METHOD2(Statfs, int(const string&, struct FileSystemInfo*));
    MOCK_METHOD2(Open, int(const string&, int));
    MOCK_METHOD1(Close, int(int));
    MOCK_METHOD1(Delete, int(const string&));
    MOCK_METHOD1(Mkdir, int(const string&));
    MOCK_METHOD1(DirExists, bool(const string&));
    MOCK_METHOD1(FileExists, bool(const string&));
    MOCK_METHOD3(Rename, int(const string&, const string&, unsigned int));
    MOCK_METHOD2(List, int(const string&, vector<string>*));
    MOCK_METHOD4(Read, int(int, char*, uint64_t, int));
    MOCK_METHOD4(Write, int(int, const char*, uint64_t, int));
    MOCK_METHOD4(Write, int(int, butil::IOBuf, uint64_t, int));
    MOCK_METHOD1(Sync, int(int fd));
    MOCK_METHOD3(Append, int(int, const char*, int));
    MOCK_METHOD4(Fallocate, int(int, int, uint64_t, int));
    MOCK_METHOD2(Fstat, int(int, struct stat*));
    MOCK_METHOD1(Fsync, int(int));
};

}  // namespace fs
}  // namespace curve

#endif  // TEST_FS_MOCK_LOCAL_FILESYSTEM_H_
