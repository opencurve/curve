/*
 * Project: nebd
 * Created Date: 2020-01-21
 * Author: charisu
 * Copyright (c) 2020 netease
 */

#ifndef SRC_COMMON_POSIX_WRAPPER_H_
#define SRC_COMMON_POSIX_WRAPPER_H_

#include <stdio.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

namespace nebd {
namespace common {
class PosixWrapper {
 public:
    PosixWrapper() {}
    virtual ~PosixWrapper() {}
    virtual int open(const char *pathname, int flags, mode_t mode);
    virtual int close(int fd);
    virtual int remove(const char *pathname);
    virtual int rename(const char *oldpath, const char *newpath);
    virtual ssize_t pwrite(int fd,
                           const void *buf,
                           size_t count,
                           off_t offset);
};
}   // namespace common
}   // namespace nebd
#endif  // SRC_COMMON_POSIX_WRAPPER_H_
