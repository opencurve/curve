/**
 * Project: curve
 * Date: Fri Apr 24 12:35:49 CST 2020
 * Author: wuhanqing
 * Copyright (c) 2020 Netease
 */

#include "src/tools/nbd/SafeIO.h"
#include <cerrno>

#include "src/tools/nbd/util.h"

namespace curve {
namespace nbd {

ssize_t SafeIO::ReadExact(int fd, void* buf, size_t count) {
    return safe_read_exact(fd, buf, count);
}

ssize_t SafeIO::Read(int fd, void* buf, size_t count) {
    return safe_read(fd, buf, count);
}

ssize_t SafeIO::Write(int fd, const void* buf, size_t count) {
    return safe_write(fd, buf, count);
}

}  // namespace nbd
}  // namespace curve
