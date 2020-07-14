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
 * Created Date: 2020-01-21
 * Author: charisu
 */

#ifndef NEBD_TEST_PART2_MOCK_POSIX_WRAPPER_H_
#define NEBD_TEST_PART2_MOCK_POSIX_WRAPPER_H_

#include <gmock/gmock.h>
#include "nebd/src/common/posix_wrapper.h"

namespace nebd {
namespace common {

class MockPosixWrapper : public PosixWrapper {
 public:
    MOCK_METHOD3(open, int(const char *, int, mode_t));
    MOCK_METHOD1(close, int(int));
    MOCK_METHOD1(remove, int(const char *));
    MOCK_METHOD2(rename, int(const char *, const char *));
    MOCK_METHOD4(pwrite, ssize_t(int fd, const void *buf,
                                 size_t count, off_t offset));
};

}   // namespace common
}   // namespace nebd

#endif  // NEBD_TEST_PART2_MOCK_POSIX_WRAPPER_H_
