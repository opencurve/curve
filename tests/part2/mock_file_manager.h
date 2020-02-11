/*
 * Project: nebd
 * Created Date: Tuesday February 4th 2020
 * Author: yangyaokai
 * Copyright (c) 2020 netease
 */

#ifndef TESTS_PART2_MOCK_FILE_MANAGER_H_
#define TESTS_PART2_MOCK_FILE_MANAGER_H_

#include <gmock/gmock.h>
#include <string>
#include <vector>

#include "src/part2/file_manager.h"

namespace nebd {
namespace server {

class MockFileManager : public NebdFileManager {
 public:
    MockFileManager() {}
    ~MockFileManager() {}

    MOCK_METHOD1(Init, int(NebdFileManagerOption));
    MOCK_METHOD0(Fini, int());
    MOCK_METHOD0(Run, int());
    MOCK_METHOD1(UpdateFileTimestamp, int(int));
    MOCK_METHOD1(Open, int(const std::string&));
    MOCK_METHOD1(Close, int(int));
    MOCK_METHOD2(Extend, int(int, int64_t));
    MOCK_METHOD2(GetInfo, int(int, NebdFileInfo*));
    MOCK_METHOD2(StatFile, int(int, NebdFileInfo*));
    MOCK_METHOD2(Discard, int(int, NebdServerAioContext*));
    MOCK_METHOD2(AioRead, int(int, NebdServerAioContext*));
    MOCK_METHOD2(AioWrite, int(int, NebdServerAioContext*));
    MOCK_METHOD2(Flush, int(int, NebdServerAioContext*));
    MOCK_METHOD1(InvalidCache, int(int));
};

}  // namespace server
}  // namespace nebd

#endif  // TESTS_PART2_MOCK_FILE_MANAGER_H_
