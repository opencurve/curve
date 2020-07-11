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

#ifndef NEBD_SRC_COMMON_POSIX_WRAPPER_H_
#define NEBD_SRC_COMMON_POSIX_WRAPPER_H_

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
#endif  // NEBD_SRC_COMMON_POSIX_WRAPPER_H_
