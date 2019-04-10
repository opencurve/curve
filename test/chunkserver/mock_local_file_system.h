/*
 * Project: curve
 * Created Date: 18-12-28
 * Author: wudemiao
 * Copyright (c) 2018 netease
 */

#ifndef TEST_CHUNKSERVER_MOCK_LOCAL_FILE_SYSTEM_H_
#define TEST_CHUNKSERVER_MOCK_LOCAL_FILE_SYSTEM_H_

#include <gmock/gmock.h>
#include <gmock/gmock-generated-function-mockers.h>
#include <gmock/internal/gmock-generated-internal-utils.h>

#include <string>
#include <vector>

#include "src/fs/local_filesystem.h"

namespace curve {
namespace chunkserver {

using curve::fs::LocalFileSystem;
using curve::fs::FileSystemInfo;

class MockLocalFileSystem : public LocalFileSystem {
 public:
    ~MockLocalFileSystem() {}
    MOCK_METHOD0(Init, int());
    MOCK_METHOD2(Statfs, int(const string& path, struct FileSystemInfo*));
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

}  // namespace chunkserver
}  // namespace curve

#endif  // TEST_CHUNKSERVER_MOCK_LOCAL_FILE_SYSTEM_H_
