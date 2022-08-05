/*
 *  Copyright (c) 2022 NetEase Inc.
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
 * File Created: 2022-06-01
 * Author: xuchaojie
 */

#ifndef SRC_FS_PFS_FILESYSTEM_IMPL_H_
#define SRC_FS_PFS_FILESYSTEM_IMPL_H_

#include <err.h>
#include <pfs_api.h>

#include <vector>
#include <string>

#include "src/fs/local_filesystem.h"

namespace curve {
namespace fs {

void HookIOBufIOFuncs();

class PfsFileSystemImpl : public LocalFileSystem {
 public:
    virtual ~PfsFileSystemImpl() {}

    static std::shared_ptr<PfsFileSystemImpl> getInstance();

    int Init(const LocalFileSystemOption& option) override;

    int Statfs(const string& path, struct FileSystemInfo* info) override;

    int Open(const string& path, int flags) override;

    int Close(int fd) override;

    int Delete(const string& path) override;

    int Mkdir(const string& dirPath, bool create_parents = true) override;

    bool DirExists(const string& dirPath) override;

    bool FileExists(const string& filePath) override;

    bool PathExists(const string& path) override;

    int List(const string& dirPath, vector<std::string>* names) override;

    DIR* OpenDir(const string& dirPath) override;

    struct dirent* ReadDir(DIR *dir) override;

    int CloseDir(DIR *dir) override;

    int Read(int fd, char* buf, uint64_t offset, int length) override;

    int Read(int fd, butil::IOPortal* portal,
             uint64_t offset, int length) override;

    int Write(int fd, const char* buf, uint64_t offset, int length) override;

    int Write(int fd, butil::IOBuf buf, uint64_t offset, int length) override;

    int WriteZero(int fd, uint64_t offset, int length) override;

    int WriteZeroIfSupport(int fd, const char* buf,
            uint64_t offset, int length) override {
        return WriteZero(fd, offset, length);
    }

    int Fdatasync(int fd) override;

    int Append(int fd, const char* buf, int length) override;

    int Fallocate(int fd, int op, uint64_t offset, int length) override;

    int Fstat(int fd, struct stat* info) override;

    int Fsync(int fd) override;

    off_t Lseek(int fd, off_t offset, int whence) override;

    int Link(const std::string &oldPath,
        const std::string &newPath) override;

 private:
    PfsFileSystemImpl() {}

    int DoRename(const string& oldPath,
                 const string& newPath,
                 unsigned int flags) override;

 private:
    static std::shared_ptr<PfsFileSystemImpl> self_;
    static std::mutex mutex_;
};

}  // namespace fs
}  // namespace curve



#endif  // SRC_FS_PFS_FILESYSTEM_IMPL_H_
