/*
 * Project: nebd
 * Created Date: 2020-01-21
 * Author: charisu
 * Copyright (c) 2020 netease
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
