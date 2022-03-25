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
 * Created Date: Tuesday February 4th 2020
 * Author: yangyaokai
 */

#ifndef NEBD_TEST_PART2_MOCK_FILE_MANAGER_H_
#define NEBD_TEST_PART2_MOCK_FILE_MANAGER_H_

#include <gmock/gmock.h>
#include <string>
#include <vector>

#include "nebd/src/part2/file_manager.h"

namespace nebd {
namespace server {

class MockFileManager : public NebdFileManager {
 public:
    MockFileManager() : NebdFileManager(nullptr) {}
    ~MockFileManager() {}

    MOCK_METHOD0(Fini, int());
    MOCK_METHOD0(Run, int());
    MOCK_METHOD2(Open, int(const std::string&, const OpenFlags*));
    MOCK_METHOD2(Close, int(int, bool));
    MOCK_METHOD2(Extend, int(int, int64_t));
    MOCK_METHOD2(GetInfo, int(int, NebdFileInfo*));
    MOCK_METHOD2(Discard, int(int, NebdServerAioContext*));
    MOCK_METHOD2(AioRead, int(int, NebdServerAioContext*));
    MOCK_METHOD2(AioWrite, int(int, NebdServerAioContext*));
    MOCK_METHOD2(Flush, int(int, NebdServerAioContext*));
    MOCK_METHOD1(InvalidCache, int(int));
    MOCK_METHOD1(GetFileEntity, NebdFileEntityPtr(int));
    MOCK_METHOD0(GetFileEntityMap, FileEntityMap());
};

}  // namespace server
}  // namespace nebd

#endif  // NEBD_TEST_PART2_MOCK_FILE_MANAGER_H_
