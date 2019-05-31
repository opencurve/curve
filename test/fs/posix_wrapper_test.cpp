/*
 * Project: curve
 * Created Date: Thursday December 27th 2018
 * Author: yangyaokai
 * Copyright (c) 2018 netease
 */

#include <gtest/gtest.h>
#include <sys/utsname.h>
#include "src/fs/wrap_posix.h"

#define DIR_PATH "wraptest"
#define FILE_PATH1 "wraptest/1"
#define FILE_PATH2 "wraptest/2"

namespace curve {
namespace fs {

class PosixWrapperTest : public testing::Test {
 public:
    PosixWrapperTest() {}
    ~PosixWrapperTest() {}
};

TEST_F(PosixWrapperTest, BasicTest) {
    char buf[4096] = {0};
    struct stat info;
    struct statfs fsinfo;
    PosixWrapper wrapper;
    ASSERT_EQ(0, wrapper.statfs("./", &fsinfo));
    ASSERT_EQ(0, wrapper.mkdir(DIR_PATH, 0755));
    int fd = wrapper.open(FILE_PATH1, O_CREAT|O_RDWR, 0644);
    ASSERT_GE(fd, 0);
    ASSERT_EQ(0, wrapper.fallocate(fd, 0, 0, 4096));
    ASSERT_EQ(4096, wrapper.pwrite(fd, buf, 4096, 0));
    ASSERT_EQ(0, wrapper.fsync(fd));
    ASSERT_EQ(0, wrapper.fstat(fd, &info));
    ASSERT_EQ(4096, wrapper.pread(fd, buf, 4096, 0));
    ASSERT_EQ(0, wrapper.close(fd));
    ASSERT_EQ(0, wrapper.stat(FILE_PATH1, &info));
    ASSERT_EQ(0, wrapper.rename(FILE_PATH1, FILE_PATH2));
    DIR* dirp = wrapper.opendir(DIR_PATH);
    ASSERT_NE(nullptr, dirp);
    ASSERT_NE(nullptr, wrapper.readdir(dirp));
    ASSERT_EQ(0, wrapper.closedir(dirp));
    ASSERT_EQ(0, wrapper.remove(FILE_PATH2));
    ASSERT_EQ(0, wrapper.remove(DIR_PATH));

    struct utsname kernel_info;
    ASSERT_EQ(0, wrapper.uname(&kernel_info));
}

TEST_F(PosixWrapperTest, Renameat2Test) {
    PosixWrapper wrapper;
    ASSERT_EQ(0, wrapper.mkdir(DIR_PATH, 0755));
    int fd = wrapper.open(FILE_PATH1, O_CREAT|O_RDWR, 0644);
    ASSERT_GE(fd, 0);
    ASSERT_EQ(0, wrapper.close(fd));
    ASSERT_EQ(0, wrapper.renameat2(FILE_PATH1, FILE_PATH2, RENAME_NOREPLACE));

    wrapper.open(FILE_PATH1, O_CREAT|O_RDWR, 0644);
    ASSERT_GE(fd, 0);
    ASSERT_EQ(0, wrapper.close(fd));
    ASSERT_EQ(-1, wrapper.renameat2(FILE_PATH1, FILE_PATH2, RENAME_NOREPLACE));
    ASSERT_EQ(0, wrapper.renameat2(FILE_PATH1, FILE_PATH2));

    ASSERT_EQ(0, wrapper.remove(FILE_PATH2));
    ASSERT_EQ(0, wrapper.remove(DIR_PATH));
}


}  // namespace fs
}  // namespace curve
