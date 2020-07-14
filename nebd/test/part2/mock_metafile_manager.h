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
 * Project: nebd
 * Created Date: Tuesday January 21st 2020
 * Author: yangyaokai
 */

#ifndef NEBD_TEST_PART2_MOCK_METAFILE_MANAGER_H_
#define NEBD_TEST_PART2_MOCK_METAFILE_MANAGER_H_

#include <gmock/gmock.h>
#include <string>
#include <vector>

#include "nebd/src/part2/metafile_manager.h"

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

#endif  // NEBD_TEST_PART2_MOCK_METAFILE_MANAGER_H_
