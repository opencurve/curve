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
    MockMetaFileManager() {}
    ~MockMetaFileManager() {}

    MOCK_METHOD1(Init, int(const NebdMetaFileManagerOption&));
    MOCK_METHOD1(ListFileMeta, int(std::vector<NebdFileMeta>*));
    MOCK_METHOD2(UpdateFileMeta, int(const std::string&, const NebdFileMeta&));
    MOCK_METHOD1(RemoveFileMeta, int(const std::string&));
};

}  // namespace server
}  // namespace nebd

#endif  // TESTS_PART2_MOCK_METAFILE_MANAGER_H_
