/*
 * Project: curve
 * File Created: Tuesday, 11th September 2018 6:47:34 pm
 * Author: tongguangxun
 * Copyright (c) 2018 NetEase
 */

#ifndef SRC_SFS_SFSMOCK_H_
#define SRC_SFS_SFSMOCK_H_

#ifndef _GNU_SOURCE
#define _GNU_SOURCE             /* See feature_test_macros(7) */
#endif  // SRC_SFS_SFSMOCK_H_  // !1

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <stdio.h>
#include <cstdint>
#include <vector>

namespace curve {
namespace sfs {
class LocalFileSystem {
 public:
    LocalFileSystem() {}
    virtual ~LocalFileSystem() {}

    virtual int Mkfs() = 0;
    virtual int Mount() = 0;
    virtual int Umount() = 0;
    virtual int Statfs(struct FsInfo *info) = 0;

    virtual int Open(const char *path, int flags, mode_t mode) = 0;
    virtual int Close(int fd) = 0;
    virtual int Delete(const char *path) = 0;
    virtual int Mkdir(const char *dirName, int flags) = 0;
    virtual bool Rmdir(const char *dirName) = 0;
    virtual bool DirExists(const char *dirName) = 0;
    virtual bool FileExists(const char *filePath) = 0;
    virtual int Rename(const char *oldPath, const char *newPath) = 0;
    virtual int List(const char *dirName,
                     std::vector<char *> *names,
                     int start,
                     int max) = 0;
    virtual int Read(int fd, void *buf, uint64_t offset, int length) = 0;
    virtual int Write(int fd, const void *buf, uint64_t offset, int length) = 0;
    virtual int Append(int fd, void *buf, int length) = 0;
    virtual int Fallocate(int fd, int op, uint64_t offset, int length) = 0;
    virtual int Fstat(int fd, struct stat *info) = 0;
    virtual int Fsync(int fd) = 0;
    virtual int Snapshot(const char *path, const char *snapPath) = 0;
};

enum FsType {
    EXT4 = 0,
    BLUESTORE,
    SFS,
};

struct FsInfo {
    uint64_t total = 0;                  // Total bytes
    uint64_t available = 0;              // Free bytes available
    int64_t allocated = 0;               // Bytes allocated by the store
    int64_t stored = 0;                  // Bytes actually stored by the user
    void dump() const;
};

class LocalFsFactory {
 public:
    virtual ~LocalFsFactory();
    static LocalFileSystem *CreateFs(const FsType &type) {
        return localFs_;
    }
 private:
    LocalFsFactory() {}
    static LocalFileSystem *localFs_;
};
}  // namespace sfs
}  // namespace curve

#endif  // SRC_SFS_SFSMOCK_H_
