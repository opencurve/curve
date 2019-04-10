/*
 * Project: curve
 * File Created: Wednesday, 5th September 2018 8:02:53 pm
 * Author: tongguangxun
 * Copyright(c) 2018 NetEase
 */
#ifndef SRC_CHUNKSERVER_CHUNKSERVERSTORAGE_CHUNKSERVER_SFS_ADAPTOR_H_
#define SRC_CHUNKSERVER_CHUNKSERVERSTORAGE_CHUNKSERVER_SFS_ADAPTOR_H_

#include <memory>
#include <string>
#include <vector>

#include "src/sfs/sfsMock.h"

namespace curve {
namespace chunkserver {
class CSSfsAdaptor {
 public:
    CSSfsAdaptor();
    ~CSSfsAdaptor();

    struct fd_t {
        fd_t() {
            fd_ = 0;
        }

        ~fd_t() {
        }

        explicit fd_t(int fd) {
            fd_ = fd;
        }

        int fd_;

        fd_t& operator = (const fd_t& f) {
            this->fd_ = f.fd_;
            return *this;
        }

        fd_t(const fd_t& f) {
            this->fd_ = f.fd_;
        }

        inline bool operator != (const fd_t& f1) {
            return this->fd_ != f1.fd_;
        }

        inline bool operator == (const fd_t& f1) {
            return this->fd_ == f1.fd_;
        }

        inline bool operator != (const int& fd) {
            return this->fd_ != fd;
        }

        inline bool operator == (const int& fd) {
            return this->fd_ == fd;
        }

        bool Valid() {
            return fd_ > 0;
        }
    };

    bool Initialize(const std::string& deviceID, const std::string & uri);
    void UnInitialize();

    fd_t Open(const char* path, int flags, mode_t mode);
    int Close(fd_t fd);
    int Delete(const char* path);
    int Mkdir(const char* dirName, int flags);
    bool DirExists(const char* dirName);
    bool FileExists(const char* filePath);
    int Rename(const char* oldPath, const char* newPath);
    int List(const char* dirName,
            std::vector<char*>* names,
            int start,
            int max);
    int Read(fd_t fd, void* buf, uint64_t offset, int length);
    bool Write(fd_t fd, const void* buf, uint64_t offset, int length);
    int Append(fd_t fd, void* buf, int length);
    int Fallocate(fd_t fd, int op, uint64_t offset, int length);
    int Fstat(fd_t fd, struct stat* info);
    int Fsync(fd_t fd);
    int Snapshot(const char* path, const char* snapPath);
    bool Rmdir(const char* path);

    std::string GetStoragePath() {
        return storagePath_;
    }

 private:
    curve::sfs::LocalFileSystem     * lfs_;
    std::string                     storagePath_;  // mount path
};
}  // namespace chunkserver
}  // namespace curve

#endif  // SRC_CHUNKSERVER_CHUNKSERVERSTORAGE_CHUNKSERVER_SFS_ADAPTOR_H_
