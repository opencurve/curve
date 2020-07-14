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

#include "nebd/src/common/posix_wrapper.h"

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
