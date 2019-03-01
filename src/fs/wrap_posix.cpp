/*
* Project: curve
* Created Date: Friday December 28th 2018
* Author: yangyaokai
* Copyright (c) 2018 netease
*/

#include <stdio.h>
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

int PosixWrapper::rename(const char *oldpath, const char *newpath) {
    return ::rename(oldpath, newpath);
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

}  // namespace fs
}  // namespace curve
