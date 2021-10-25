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
 * Created Date: Thursday March 5th 2020
 * Author: yangyaokai
 */

#ifndef NEBD_TEST_PART2_MOCK_FILE_ENTITY_H_
#define NEBD_TEST_PART2_MOCK_FILE_ENTITY_H_

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
    MOCK_CONST_METHOD0(GetFileName, std::string());
    MOCK_CONST_METHOD0(GetFd, int());
    MOCK_METHOD1(UpdateFileTimeStamp, void(uint64_t));
    MOCK_CONST_METHOD0(GetFileTimeStamp, uint64_t());
    MOCK_CONST_METHOD0(GetFileStatus, NebdFileStatus());
};

}  // namespace server
}  // namespace nebd

#endif  // NEBD_TEST_PART2_MOCK_FILE_ENTITY_H_
