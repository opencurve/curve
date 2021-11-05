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
 * Project: curve
 * Created Date: Mon Aug 30 2021
 * Author: hzwuhongsong
 */

#ifndef CURVEFS_SRC_COMMON_WRAP_POSIX_H_
#define CURVEFS_SRC_COMMON_WRAP_POSIX_H_

#include <fcntl.h>
#include <unistd.h>
#include <sys/vfs.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/utsname.h>
#include <dirent.h>
#include <string>

namespace curvefs {
namespace common {

class PosixWrapper {
 public:
    PosixWrapper() {}
    virtual ~PosixWrapper() {}
    virtual int open(const char *pathname, int flags, mode_t mode);
    virtual int close(int fd);
    virtual int remove(const char *pathname);
    virtual int mkdir(const char *pathname, mode_t mode);
    virtual int stat(const char *pathname, struct stat *buf);
    virtual int rename(const char *oldpath,
                       const char *newpath);
    virtual int renameat2(const char *oldpath,
                          const char *newpath,
                          unsigned int flags = 0);
    virtual DIR *opendir(const char *name);
    virtual struct dirent *readdir(DIR *dirp);
    virtual int closedir(DIR *dirp);
    virtual ssize_t read(int fd, void *buf, size_t count);
    virtual ssize_t write(int fd, const void *buf, size_t count);
    virtual ssize_t pread(int fd, void *buf, size_t count, off_t offset);
    virtual ssize_t pwrite(int fd,
                           const void *buf,
                           size_t count,
                           off_t offset);
    virtual int fstat(int fd, struct stat *buf);
    virtual int fallocate(int fd, int mode, off_t offset, off_t len);
    virtual int fsync(int fd);
    virtual int fdatasync(int fd);
    virtual int statfs(const char *path, struct statfs *buf);
    virtual int uname(struct utsname *buf);
    virtual int link(const char *oldpath, const char *newpath);
    virtual off_t lseek(int fd, off_t offset, int whence);
    virtual void *malloc(size_t size);
    virtual void *memset(void *s, int c, size_t n);
    virtual void free(void *s);
};

}  // namespace common
}  // namespace curvefs

#endif  // CURVEFS_SRC_COMMON_WRAP_POSIX_H_
