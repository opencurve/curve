/*
 * Project: curve
 * Created Date: Friday December 7th 2018
 * Author: yangyaokai
 * Copyright (c) 2018 netease
 */

#ifndef TEST_FS_MOCK_LOCAL_FILESYSTEM_H
#define TEST_FS_MOCK_LOCAL_FILESYSTEM_H

#include <gmock/gmock.h>
#include <string>
#include <vector>

#include "src/fs/local_filesystem.h"

namespace curve {
namespace fs {

class MockLocalFileSystem : public LocalFileSystem {
 public:
    ~MockLocalFileSystem() {}
    MOCK_METHOD0(Init, int());
    MOCK_METHOD2(Statfs, int(const string&, struct FileSystemInfo*));
    MOCK_METHOD2(Open, int(const string&, int));
    MOCK_METHOD1(Close, int(int));
    MOCK_METHOD1(Delete, int(const string&));
    MOCK_METHOD1(Mkdir, int(const string&));
    MOCK_METHOD1(DirExists, bool(const string&));
    MOCK_METHOD1(FileExists, bool(const string&));
    MOCK_METHOD2(Rename, int(const string&, const string&));
    MOCK_METHOD2(List, int(const string&, vector<string>*));
    MOCK_METHOD4(Read, int(int, char*, uint64_t, int));
    MOCK_METHOD4(Write, int(int, const char*, uint64_t, int));
    MOCK_METHOD3(Append, int(int, const char*, int));
    MOCK_METHOD4(Fallocate, int(int, int, uint64_t, int));
    MOCK_METHOD2(Fstat, int(int, struct stat*));
    MOCK_METHOD1(Fsync, int(int));
};

}  // namespace fs
}  // namespace curve

#endif  // TEST_FS_MOCK_LOCAL_FILESYSTEM_H
