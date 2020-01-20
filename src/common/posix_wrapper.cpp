/*
 * Project: nebd
 * Created Date: 2020-01-21
 * Author: charisu
 * Copyright (c) 2020 netease
 */

#include "src/common/posix_wrapper.h"

namespace nebd {
namespace common {

int PosixWrapper::open(const char *pathname, int flags, mode_t mode) {
    return ::open(pathname, flags, mode);
}

int PosixWrapper::close(int fd) {
    return ::close(fd);
}

int PosixWrapper::remove(const char *pathname) {
    return ::remove(pathname);
}

int PosixWrapper::rename(const char *oldPath, const char *newPath) {
    return ::rename(oldPath, newPath);
}

ssize_t PosixWrapper::pwrite(int fd,
                             const void *buf,
                             size_t count,
                             off_t offset) {
    return ::pwrite(fd, buf, count, offset);
}

}   // namespace common
}   // namespace nebd
