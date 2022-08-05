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
    MOCK_METHOD1(PathExists, bool(const string&));
    MOCK_METHOD3(Rename, int(const string&, const string&, unsigned int));
    MOCK_METHOD2(List, int(const string&, vector<string>*));
    MOCK_METHOD1(OpenDir, DIR*(const string& dirPath));
    MOCK_METHOD1(ReadDir, struct dirent*(DIR *dir));
    MOCK_METHOD1(CloseDir, int(DIR *dir));
    MOCK_METHOD4(Read, int(int, char*, uint64_t, int));
    MOCK_METHOD4(Read, int(int fd, butil::IOPortal* portal,
             uint64_t offset, int length));
    MOCK_METHOD4(Write, int(int, const char*, uint64_t, int));
    MOCK_METHOD4(Write, int(int, butil::IOBuf, uint64_t, int));
    MOCK_METHOD3(WriteZero, int(int fd, uint64_t offset, int length));
    MOCK_METHOD4(WriteZeroIfSupport, int(int fd, const char* buf,
            uint64_t offset, int length));
    MOCK_METHOD1(Fdatasync, int(int fd));
    MOCK_METHOD3(Append, int(int, const char*, int));
    MOCK_METHOD4(Fallocate, int(int, int, uint64_t, int));
    MOCK_METHOD2(Fstat, int(int, struct stat*));
    MOCK_METHOD1(Fsync, int(int));
    MOCK_METHOD3(Lseek, off_t(int fd, off_t offset, int whence));
    MOCK_METHOD2(Link, int(const std::string &oldPath,
        const std::string &newPath));
};

}  // namespace fs
}  // namespace curve

#endif  // TEST_FS_MOCK_LOCAL_FILESYSTEM_H_
