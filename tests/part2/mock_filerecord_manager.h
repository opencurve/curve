/*
 * Project: nebd
 * Created Date: Sunday February 16th 2020
 * Author: yangyaokai
 * Copyright (c) 2020 netease
 */

#ifndef TESTS_PART2_MOCK_FILERECORD_MANAGER_H_
#define TESTS_PART2_MOCK_FILERECORD_MANAGER_H_

#include <gmock/gmock.h>
#include <string>
#include <vector>

#include "src/part2/filerecord_manager.h"

namespace nebd {
namespace server {

class MockFileRecordManager : public FileRecordManager {
 public:
    MockFileRecordManager() : FileRecordManager(nullptr) {}
    ~MockFileRecordManager() {}

    MOCK_METHOD0(Load, int());
    MOCK_METHOD2(GetRecord, bool(int, NebdFileRecord*));
    MOCK_METHOD2(GetRecord, bool(const std::string&, NebdFileRecord*));
    MOCK_METHOD1(UpdateRecord, bool(const NebdFileRecord&));
    MOCK_METHOD1(RemoveRecord, bool(int));
    MOCK_METHOD1(Exist, bool(int));
    MOCK_METHOD0(Clear, void(void));
    MOCK_METHOD0(ListRecords, FileRecordMap(void));
    MOCK_METHOD2(UpdateFileTimestamp, bool(int, uint64_t));
    MOCK_METHOD2(GetFileTimestamp, bool(int, uint64_t*));
    MOCK_METHOD2(UpdateFileStatus, bool(int, NebdFileStatus));
    MOCK_METHOD2(GetFileStatus, bool(int, NebdFileStatus*));
};

}  // namespace server
}  // namespace nebd

#endif  // TESTS_PART2_MOCK_FILERECORD_MANAGER_H_
