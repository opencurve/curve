/*
 * Project: nebd
 * Created Date: Tuesday January 21st 2020
 * Author: yangyaokai
 * Copyright (c) 2020 netease
 */

#ifndef TESTS_PART2_MOCK_METAFILE_MANAGER_H_
#define TESTS_PART2_MOCK_METAFILE_MANAGER_H_

#include <gmock/gmock.h>
#include <string>
#include <vector>

#include "src/part2/metafile_manager.h"

namespace nebd {
namespace server {

class MockMetaFileManager : public NebdMetaFileManager {
 public:
    MockMetaFileManager() : NebdMetaFileManager("") {}
    ~MockMetaFileManager() {}

    MOCK_METHOD1(UpdateMetaFile, int(const FileRecordMap&));
    MOCK_METHOD1(ListFileRecord, int(FileRecordMap*));
};

}  // namespace server
}  // namespace nebd

#endif  // TESTS_PART2_MOCK_METAFILE_MANAGER_H_
