/*
 * Project: nebd
 * Created Date: Thursday March 5th 2020
 * Author: yangyaokai
 * Copyright (c) 2020 netease
 */

#ifndef TESTS_PART2_MOCK_FILE_ENTITY_H_
#define TESTS_PART2_MOCK_FILE_ENTITY_H_

#include <gmock/gmock.h>
#include <string>
#include <vector>

#include "nebd/src/part2/file_entity.h"

namespace nebd {
namespace server {

class MockFileEntity : public NebdFileEntity {
 public:
    MockFileEntity() : NebdFileEntity() {}
    ~MockFileEntity() {}

    MOCK_METHOD1(Init, int(const NebdFileEntityOption&));
    MOCK_METHOD0(Open, int());
    MOCK_METHOD1(Close, int(bool));
    MOCK_METHOD1(Extend, int(int64_t));
    MOCK_METHOD1(GetInfo, int(NebdFileInfo*));
    MOCK_METHOD1(Discard, int(NebdServerAioContext*));
    MOCK_METHOD1(AioRead, int(NebdServerAioContext*));
    MOCK_METHOD1(AioWrite, int(NebdServerAioContext*));
    MOCK_METHOD1(Flush, int(NebdServerAioContext*));
    MOCK_METHOD0(InvalidCache, int());
    MOCK_CONST_METHOD0(GetFileName, const std::string());
    MOCK_CONST_METHOD0(GetFd, const int());
    MOCK_METHOD1(UpdateFileTimeStamp, void(uint64_t));
    MOCK_CONST_METHOD0(GetFileTimeStamp, const uint64_t());
    MOCK_CONST_METHOD0(GetFileStatus, const NebdFileStatus());
};

}  // namespace server
}  // namespace nebd

#endif  // TESTS_PART2_MOCK_FILE_ENTITY_H_
