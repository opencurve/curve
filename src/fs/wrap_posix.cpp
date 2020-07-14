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
* Created Date: Friday December 28th 2018
* Author: yangyaokai
*/

#include <glog/logging.h>
#include <stdio.h>
#include <sys/syscall.h>

#include "src/fs/wrap_posix.h"

namespace curve {
namespace fs {

int PosixWrapper::open(const char *pathname, int flags, mode_t mode) {
    return ::open(pathname, flags, mode);
}

int PosixWrapper::close(int fd) {
    return ::close(fd);
}

int PosixWrapper::remove(const char *pathname) {
    return ::remove(pathname);
}

int PosixWrapper::mkdir(const char *pathname, mode_t mode) {
    return ::mkdir(pathname, mode);
}

int PosixWrapper::stat(const char *pathname, struct stat *buf) {
    return ::stat(pathname, buf);
}

int PosixWrapper::rename(const char *oldpath,
                         const char *newpath) {
    return ::rename(oldpath, newpath);
}

int PosixWrapper::renameat2(const char *oldpath,
                            const char *newpath,
                            unsigned int flags) {
    /*   RENAME_NOREPLACE requires support from the underlying filesystem.
     *   Support for various filesystems was added as follows:
     *   ext4 (Linux 3.15);
     *   btrfs, shmem, and cifs (Linux 3.17);
     *   xfs (Linux 4.0);
     *   Support for many other filesystems was added in Linux 4.9,
     *   including etx2, minix, reiserfs, jfs, vfat, and bpf.
     */
    return ::syscall(SYS_renameat2,
                     AT_FDCWD,
                     oldpath,
                     AT_FDCWD,
                     newpath,
                     flags);
}

DIR *PosixWrapper::opendir(const char *name) {
    return ::opendir(name);
}

struct dirent *PosixWrapper::readdir(DIR *dirp) {
    return ::readdir(dirp);
}

int PosixWrapper::closedir(DIR *dirp) {
    return ::closedir(dirp);
}

ssize_t PosixWrapper::pread(int fd, void *buf, size_t count, off_t offset) {
    return ::pread(fd, buf, count, offset);
}

ssize_t PosixWrapper::pwrite(int fd,
                             const void *buf,
                             size_t count,
                             off_t offset) {
    return ::pwrite(fd, buf, count, offset);
}

int PosixWrapper::fstat(int fd, struct stat *buf) {
    return ::fstat(fd, buf);
}

int PosixWrapper::fallocate(int fd, int mode, off_t offset, off_t len) {
    return ::posix_fallocate(fd, offset, len);
}

int PosixWrapper::fsync(int fd) {
    return ::fsync(fd);
}

int PosixWrapper::statfs(const char *path, struct statfs *buf) {
    return ::statfs(path, buf);
}

int PosixWrapper::uname(struct utsname *buf) {
    return ::uname(buf);
}

}  // namespace fs
}  // namespace curve
