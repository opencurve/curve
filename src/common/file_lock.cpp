/**
 * Project: nebd
 * Create Date: 2020-01-20
 * Author: wuhanqing
 * Copyright (c) 2020 netease
 */

#include "src/common/file_lock.h"

#include <sys/file.h>
#include <butil/logging.h>

namespace nebd {
namespace common {

const int kBufSize = 128;

int FileLock::AcquireFileLock() {
    char buffer[kBufSize];

    fd_ = open(fileName_.c_str(), O_CREAT | O_RDONLY, 0644);
    if (fd_ < 0) {
        LOG(ERROR) << "open file failed, error = "
                   << strerror_r(errno, buffer, kBufSize)
                   << ", filename = " << fileName_;
        return -1;
    }

    int ret = flock(fd_, LOCK_EX | LOCK_NB);
    if (ret != 0) {
        LOG(ERROR) << "flock failed, error = "
                   << strerror_r(errno, buffer, kBufSize)
                   << ", filename = " << fileName_;
        close(fd_);
        return -1;
    }

    return 0;
}

void FileLock::ReleaseFileLock() {
    char buffer[kBufSize];
    int ret = flock(fd_, LOCK_UN);
    close(fd_);
    if (ret != 0) {
        LOG(ERROR) << "release file lock failed, error = "
                   << strerror_r(errno, buffer, kBufSize)
                   << ", fd = " << fd_;
    }
    unlink(fileName_.c_str());
}

}  // namespace common
}  // namespace nebd
