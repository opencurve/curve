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
 * Project: curve
 * Created Date: 2021-02-24
 * Author: qinyi
 */

#ifndef TEST_CHUNKSERVER_WATCHDOG_MOCK_HELPER_H_
#define TEST_CHUNKSERVER_WATCHDOG_MOCK_HELPER_H_

#include <gmock/gmock.h>

#include <string>

#include "src/chunkserver/watchdog/helper.h"

namespace curve {
namespace chunkserver {

class MockWatchdogHelper : public WatchdogHelper {
 public:
    MOCK_METHOD2(Exec, int(const string& cmd, string* output));
    MOCK_METHOD0(OnError, void());
    MOCK_METHOD2(Open, int(const char* file, int flag));
    MOCK_METHOD1(Close, int(int fd));
    MOCK_METHOD4(Pread,
                 ssize_t(int fd, void* buf, size_t nbytes, off_t offset));
    MOCK_METHOD4(Pwrite,
                 ssize_t(int fd, const void* buf, size_t n, off_t offset));
    MOCK_METHOD2(Access, int(const char* name, int type));
    MOCK_METHOD1(Remove, int(const char* filename));
};
}  // namespace chunkserver
}  // namespace curve

#endif  // TEST_CHUNKSERVER_WATCHDOG_MOCK_HELPER_H_
