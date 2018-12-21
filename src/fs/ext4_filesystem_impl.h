/*
 * Project: curve
 * File Created: 18-10-31
 * Author: yangyaokai
 * Copyright (c) 2018 netease
 */

#ifndef CURVE_FS_EXT4_FILESYSTEM_H
#define CURVE_FS_EXT4_FILESYSTEM_H

#include <memory>
#include <string>
#include <vector>
#include <map>

#include "src/fs/local_filesystem.h"
#include "src/fs/wrap_posix.h"

const int MAX_RETYR_TIME = 3;

namespace curve {
namespace fs {
class Ext4FileSystemImpl : public LocalFileSystem {
 public:
    virtual ~Ext4FileSystemImpl();
    static std::shared_ptr<Ext4FileSystemImpl> getInstance();
    void SetPosixWrapper(std::shared_ptr<PosixWrapper> wrapper);

    int Init() override;
    int Statfs(const string& path, struct FileSystemInfo* info) override;
    int Open(const string& path, int flags) override;
    int Close(int fd) override;
    int Delete(const string& path) override;
    int Mkdir(const string& dirPath) override;
    bool DirExists(const string& dirPath) override;
    bool FileExists(const string& filePath) override;
    int Rename(const string& oldPath, const string& newPath) override;
    int List(const string& dirPath, vector<std::string>* names) override;
    int Read(int fd, char* buf, uint64_t offset, int length) override;
    int Write(int fd, const char* buf, uint64_t offset, int length) override;
    int Append(int fd, const char* buf, int length) override;
    int Fallocate(int fd, int op, uint64_t offset,
                  int length) override;
    int Fstat(int fd, struct stat* info) override;
    int Fsync(int fd) override;

 private:
    explicit Ext4FileSystemImpl(std::shared_ptr<PosixWrapper>);

 private:
    static std::shared_ptr<Ext4FileSystemImpl> self_;
    static std::mutex mutex_;
    std::shared_ptr<PosixWrapper> posixWrapper_;
};

}  // namespace fs
}  // namespace curve

#endif  // CURVE_FS_EXT4_FILESYSTEM_H
