/*
 * Project: curve
 * Created Date: Thursday December 20th 2018
 * Author: yangyaokai
 * Copyright (c) 2018 netease
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
    MOCK_METHOD1(opendir, DIR*(const char*));
    MOCK_METHOD1(readdir, struct dirent*(DIR*));
    MOCK_METHOD1(closedir, int(DIR*));
    MOCK_METHOD4(pread, ssize_t(int, void*, size_t, off_t));
    MOCK_METHOD4(pwrite, ssize_t(int, const void*, size_t, off_t));
    MOCK_METHOD4(fallocate, int(int, int, off_t, off_t));
    MOCK_METHOD2(fstat, int(int, struct stat*));
    MOCK_METHOD1(fsync, int(int));
    MOCK_METHOD2(statfs, int(const char*, struct statfs*));
};

}  // namespace fs
}  // namespace curve

#endif  // TEST_FS_MOCK_POSIX_WRAPPER_H_
