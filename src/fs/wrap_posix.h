/*
 * Project: curve
 * Created Date: Thursday December 20th 2018
 * Author: yangyaokai
 * Copyright (c) 2018 netease
 */

#ifndef SRC_FS_WRAP_POSIX_H_
#define SRC_FS_WRAP_POSIX_H_

#include <fcntl.h>
#include <unistd.h>
#include <sys/vfs.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/utsname.h>
#include <dirent.h>
#include <linux/fs.h>
#include <string>

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
    virtual int rename(const char *oldpath,
                       const char *newpath);
    virtual int renameat2(const char *oldpath,
                          const char *newpath,
                          unsigned int flags = 0);
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
    virtual int uname(struct utsname *buf);
};

}  // namespace fs
}  // namespace curve

#endif  // SRC_FS_WRAP_POSIX_H_
