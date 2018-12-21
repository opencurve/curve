/*
 * Project: curve
 * Created Date: Thursday December 20th 2018
 * Author: yangyaokai
 * Copyright (c) 2018 netease
 */

#ifndef CURVE_FS_POSIX_H
#define CURVE_FS_POSIX_H

#include <fcntl.h>
#include <unistd.h>
#include <sys/vfs.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <dirent.h>

namespace curve {
namespace fs {

class PosixWrapper {
 public:
    PosixWrapper() {}
    virtual ~PosixWrapper() {}

    virtual int open(const char *pathname, int flags, mode_t mode);
    virtual int close(int fd);
    virtual int remove(const char *pathname);
    virtual int mkdir(const char *pathname, mode_t mode);
    virtual int stat(const char *pathname, struct stat *buf);
    virtual int rename(const char *oldpath, const char *newpath);
    virtual DIR *opendir(const char *name);
    virtual struct dirent *readdir(DIR *dirp);
    virtual int closedir(DIR *dirp);
    virtual ssize_t pread(int fd, void *buf, size_t count, off_t offset);
    virtual ssize_t pwrite(int fd,
                           const void *buf,
                           size_t count,
                           off_t offset);
    virtual int fstat(int fd, struct stat *buf);
    virtual int fallocate(int fd, int mode, off_t offset, off_t len);
    virtual int fsync(int fd);
    virtual int statfs(const char *path, struct statfs *buf);
};

}  // namespace fs
}  // namespace curve

#endif  // CURVE_FS_POSIX_H
