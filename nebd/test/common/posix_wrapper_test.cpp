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

#include <gtest/gtest.h>
#include "nebd/src/common/posix_wrapper.h"

#define FILE_PATH1 "/tmp/wraptest1"
#define FILE_PATH2 "/tmp/wraptest2"

namespace nebd {
namespace common {

TEST(PosixWrapperTest, BasicTest) {
    char buf[4096] = {0};
    PosixWrapper wrapper;
    int fd = wrapper.open(FILE_PATH1, O_CREAT|O_RDWR, 0644);
    ASSERT_GE(fd, 0);
    ASSERT_EQ(4096, wrapper.pwrite(fd, buf, 4096, 0));
    ASSERT_EQ(0, wrapper.close(fd));
    ASSERT_EQ(0, wrapper.rename(FILE_PATH1, FILE_PATH2));
    ASSERT_EQ(0, wrapper.remove(FILE_PATH2));
}

}   // namespace common
}   // namespace nebd
