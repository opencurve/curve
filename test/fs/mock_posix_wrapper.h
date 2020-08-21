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
 * Created Date: Thursday December 20th 2018
 * Author: yangyaokai
 */

#ifndef TEST_FS_MOCK_POSIX_WRAPPER_H_
#define TEST_FS_MOCK_POSIX_WRAPPER_H_

#include <gmock/gmock.h>

#include "src/fs/local_filesystem.h"
#include "src/fs/wrap_posix.h"

namespace curve {
namespace fs {

class MockPosixWrapper : public PosixWrapper {
 public:
    ~MockPosixWrapper() {}
    MOCK_METHOD3(open, int(const char*, int, mode_t));
    MOCK_METHOD1(close, int(int));
    MOCK_METHOD1(remove, int(const char*));
    MOCK_METHOD2(mkdir, int(const char*, mode_t));
    MOCK_METHOD2(stat, int(const char*, struct stat*));
    MOCK_METHOD2(rename, int(const char*, const char*));
    MOCK_METHOD3(renameat2, int(const char*, const char*, unsigned int));
    MOCK_METHOD1(opendir, DIR*(const char*));
    MOCK_METHOD1(readdir, struct dirent*(DIR*));
    MOCK_METHOD1(closedir, int(DIR*));
    MOCK_METHOD4(pread, ssize_t(int, void*, size_t, off_t));
    MOCK_METHOD4(pwrite, ssize_t(int, const void*, size_t, off_t));
    MOCK_METHOD4(fallocate, int(int, int, off_t, off_t));
    MOCK_METHOD2(fstat, int(int, struct stat*));
    MOCK_METHOD1(fsync, int(int));
    MOCK_METHOD2(statfs, int(const char*, struct statfs*));
    MOCK_METHOD1(uname, int(struct utsname *));
    MOCK_METHOD2(io_setup, int(int, io_context_t *));
    MOCK_METHOD1(io_destroy, int(io_context_t));
    MOCK_METHOD3(io_submit, int(io_context_t, long, struct iocb *[]));  // NOLINT
    MOCK_METHOD5(io_getevents, int(io_context_t, long, long, struct io_event *  // NOLINT
                                                    , struct timespec *));
};

}  // namespace fs
}  // namespace curve

#endif  // TEST_FS_MOCK_POSIX_WRAPPER_H_
