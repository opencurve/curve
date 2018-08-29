/*
 * Project: curve
 * File Created: Thursday, 6th September 2018 12:41:52 pm
 * Author: tongguangxun
 * Copyright(c) 2018 NetEase
 */
#include <fstream>

#include "src/chunkserver/chunkserverStorage/chunkserver_sfs_localfs_impl.h"

namespace curve {
namespace chunkserver {

CSSfsLocalFsImpl::CSSfsLocalFsImpl() {
}

CSSfsLocalFsImpl::~CSSfsLocalFsImpl() {
}

int CSSfsLocalFsImpl::Mkfs() {
    return 0;
}

int CSSfsLocalFsImpl::Mount() {
    return 0;
}

int CSSfsLocalFsImpl::Umount() {
    return 0;
}

int CSSfsLocalFsImpl::Statfs(struct curve::sfs::FsInfo* info) {
    return 0;
}

int CSSfsLocalFsImpl::Open(const char* path, int flags, mode_t mode) {
    return open(path, flags, mode);
}

int CSSfsLocalFsImpl::Close(int fd) {
    return close(fd);
}

int CSSfsLocalFsImpl::Delete(const char* path) {
    return remove(path);
}

int CSSfsLocalFsImpl::Mkdir(const char* dirName, int flags) {
    return mkdir(dirName, flags);
}

bool CSSfsLocalFsImpl::DirExists(const char* dirName) {
    return access(dirName, F_OK) == 0;
}

bool CSSfsLocalFsImpl::FileExists(const char* filePath) {
    return access(filePath, F_OK) == 0;
}

int CSSfsLocalFsImpl::Rename(const char* oldPath, const char* newPath) {
    return rename(oldPath, newPath);
}

int CSSfsLocalFsImpl::List(const char* dirName, std::vector<char*>* names, int start, int max) {
    return 0;
}

int CSSfsLocalFsImpl::Read(int fd, void* buf, uint64_t offset, int length) {
    return pread(fd, buf, length, offset);
}

int CSSfsLocalFsImpl::Write(int fd, const void* buf, uint64_t offset, int length) {
    return pwrite(fd, buf, length, offset);
}

int CSSfsLocalFsImpl::Append(int fd, void* buf, int length) {
    return 0;
}

int CSSfsLocalFsImpl::Fallocate(int fd, int op, uint64_t offset, int length) {
    return fallocate(fd, op, offset, length);
}

int CSSfsLocalFsImpl::Fstat(int fd, struct stat* info) {
    return 0;
}

int CSSfsLocalFsImpl::Fsync(int fd) {
    return fsync(fd);
}

int CSSfsLocalFsImpl::Snapshot(const char* path, const char* snapPath) {
    return 0;
}

bool CSSfsLocalFsImpl::Rmdir(const char* path) {
    return rmdir(path) == 0;
}

}  // namespace chunkserver
}  // namespace curve
