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

#ifndef CURVEFS_TEST_CLIENT_MOCK_TEST_POSIX_WAPPER_H_
#define CURVEFS_TEST_CLIENT_MOCK_TEST_POSIX_WAPPER_H_

#include <gmock/gmock.h>

#include "curvefs/src/common/wrap_posix.h"

namespace curvefs {
namespace client {

using curvefs::common::PosixWrapper;

class MockPosixWrapper : public PosixWrapper {
 public:
    ~MockPosixWrapper() {}
    MOCK_METHOD2(open, int(const char *, int));
    MOCK_METHOD1(close, int(int));
    MOCK_METHOD1(remove, int(const char*));
    MOCK_METHOD2(mkdir, int(const char*, mode_t));
    MOCK_METHOD2(stat, int(const char*, struct stat*));
    MOCK_METHOD2(rename, int(const char*, const char*));
    MOCK_METHOD3(renameat2, int(const char*, const char*, unsigned int));
    MOCK_METHOD1(opendir, DIR*(const char*));
    MOCK_METHOD1(readdir, struct dirent*(DIR*));
    MOCK_METHOD1(closedir, int(DIR*));
    MOCK_METHOD3(read, ssize_t(int, void*, size_t));
    MOCK_METHOD3(write, ssize_t(int, const void*, size_t));
    MOCK_METHOD4(pread, ssize_t(int, void*, size_t, off_t));
    MOCK_METHOD4(pwrite, ssize_t(int, const void*, size_t, off_t));
    MOCK_METHOD4(fallocate, int(int, int, off_t, off_t));
    MOCK_METHOD2(fstat, int(int, struct stat*));
    MOCK_METHOD1(fsync, int(int));
    MOCK_METHOD1(fdatasync, int(int));
    MOCK_METHOD2(statfs, int(const char*, struct statfs*));
    MOCK_METHOD1(uname, int(struct utsname *));
    MOCK_METHOD2(link, int(const char*, const char *));
    MOCK_METHOD3(lseek, off_t(int, off_t, int));
    MOCK_METHOD1(malloc, void*(size_t size));
    MOCK_METHOD3(memset, void*(void *s, int c, size_t n));
    MOCK_METHOD1(free, void(void *s));
};

}  // namespace client
}  // namespace curvefs

#endif  // CURVEFS_TEST_CLIENT_MOCK_TEST_POSIX_WAPPER_H_

