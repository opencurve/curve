/*
 * Project: curve
 * File Created: Thursday, 6th September 2018 12:41:14 pm
 * Author: tongguangxun
 * Copyright(c) 2018 NetEase
 */

#ifndef CURVE_CHUNKSERVER_CHUNKSERVER_SFS_LOCALFS_IMPL_H
#define CURVE_CHUNKSERVER_CHUNKSERVER_SFS_LOCALFS_IMPL_H

#ifndef _GNU_SOURCE
#define _GNU_SOURCE             /* See feature_test_macros(7) */
#endif  // !1

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <stdio.h>
#include <vector>

#include <cstdint>

#include "src/sfs/sfsMock.h"
#include "include/curve_compiler_specific.h"

namespace curve {
namespace chunkserver {
class CSSfsLocalFsImpl : public curve::sfs::LocalFileSystem {
 public:
    CSSfsLocalFsImpl();
    CURVE_MOCK ~CSSfsLocalFsImpl();

    int Mkfs() override;
    int Mount() override;
    int Umount() override;
    int Statfs(struct curve::sfs::FsInfo* info) override;

    CURVE_MOCK int Open(const char* path, int flags, mode_t mode);
    CURVE_MOCK int Close(int fd);
    CURVE_MOCK int Delete(const char* path);
    CURVE_MOCK int Mkdir(const char* dirName, int flags);
    CURVE_MOCK bool DirExists(const char* dirName);
    CURVE_MOCK bool FileExists(const char* filePath);
    int Rename(const char* oldPath, const char* newPath) override;
    int List(const char* dirName,
            std::vector<char*>* names,
            int start,
            int max) override;
    CURVE_MOCK int Read(int fd, void* buf, uint64_t offset, int length);
    CURVE_MOCK int Write(int fd, const void* buf, uint64_t offset, int length);
    CURVE_MOCK int Append(int fd, void* buf, int length);
    CURVE_MOCK int Fallocate(int fd, int op, uint64_t offset, int length);
    CURVE_MOCK int Fstat(int fd, struct stat* info);
    CURVE_MOCK int Fsync(int fd);
    int Snapshot(const char* path, const char* snapPath);
    bool Rmdir(const char* path);
};

}  // namespace chunkserver
}  // namespace curve

#endif  // !CURVE_CHUNKSERVER_CHUNKSERVER_SFS_LOCALFS_IMPL_H
