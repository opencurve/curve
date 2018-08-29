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

namespace curve {
namespace chunkserver {
class CSSfsLocalFsImpl : public curve::sfs::LocalFileSystem {
 public:
    CSSfsLocalFsImpl();
    ~CSSfsLocalFsImpl();

    int Mkfs() override;
    int Mount() override;
    int Umount() override;
    int Statfs(struct curve::sfs::FsInfo* info) override;

    int Open(const char* path, int flags, mode_t mode) override;
    int Close(int fd) override;
    int Delete(const char* path) override;
    int Mkdir(const char* dirName, int flags) override;
    bool DirExists(const char* dirName) override;
    bool FileExists(const char* filePath) override;
    int Rename(const char* oldPath, const char* newPath) override;
    int List(const char* dirName, std::vector<char*>* names, int start, int max) override;
    int Read(int fd, void* buf, uint64_t offset, int length) override;
    int Write(int fd, const void* buf, uint64_t offset, int length) override;
    int Append(int fd, void* buf, int length) override;
    int Fallocate(int fd, int op, uint64_t offset, int length) override;
    int Fstat(int fd, struct stat* info) override;
    int Fsync(int fd) override;
    int Snapshot(const char* path, const char* snapPath) override;
    bool Rmdir(const char* path) override;
};

}  // namespace chunkserver
}  // namespace curve

#endif  // !CURVE_CHUNKSERVER_CHUNKSERVER_SFS_LOCALFS_IMPL_H
