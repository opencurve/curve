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

#ifndef NEBD_TEST_PART2_MOCK_REQUEST_EXECUTOR_H_
#define NEBD_TEST_PART2_MOCK_REQUEST_EXECUTOR_H_

#include <gmock/gmock.h>
#include <string>
#include <vector>
#include <memory>

#include "nebd/src/part2/request_executor.h"

namespace nebd {
namespace server {

class MockFileInstance : public NebdFileInstance {
 public:
    MockFileInstance() {}
    ~MockFileInstance() {}
};

class MockRequestExecutor : public NebdRequestExecutor {
 public:
    MockRequestExecutor() {}
    ~MockRequestExecutor() {}

    MOCK_METHOD2(Open, std::shared_ptr<NebdFileInstance>(const std::string&,
                                                         const OpenFlags*));
    MOCK_METHOD2(Reopen, std::shared_ptr<NebdFileInstance>(
        const std::string&, const ExtendAttribute&));
    MOCK_METHOD1(Close, int(NebdFileInstance*));
    MOCK_METHOD2(Extend, int(NebdFileInstance*, int64_t));
    MOCK_METHOD2(GetInfo, int(NebdFileInstance*, NebdFileInfo*));
    MOCK_METHOD2(Discard, int(NebdFileInstance*, NebdServerAioContext*));
    MOCK_METHOD2(AioRead, int(NebdFileInstance*, NebdServerAioContext*));
    MOCK_METHOD2(AioWrite, int(NebdFileInstance*, NebdServerAioContext*));
    MOCK_METHOD2(Flush, int(NebdFileInstance*, NebdServerAioContext*));
    MOCK_METHOD1(InvalidCache, int(NebdFileInstance*));
};

}  // namespace server
}  // namespace nebd

#endif  // NEBD_TEST_PART2_MOCK_REQUEST_EXECUTOR_H_
