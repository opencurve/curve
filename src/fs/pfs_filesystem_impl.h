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

class PfsFileSystemImpl : public LocalFileSystem {
 public:
    virtual ~PfsFileSystemImpl() {}

    static std::shared_ptr<PfsFileSystemImpl> getInstance();

    int Init(const LocalFileSystemOption& option) override;

    int Statfs(const string& path, struct FileSystemInfo* info) override;

    int Open(const string& path, int flags) override;

    int Close(int fd) override;

    int Delete(const string& path) override;

    int Mkdir(const string& dirPath) override;

    bool DirExists(const string& dirPath) override;

    bool FileExists(const string& filePath) override;

    int List(const string& dirPath, vector<std::string>* names) override;

    int Read(int fd, char* buf, uint64_t offset, int length) override;

    int Write(int fd, const char* buf, uint64_t offset, int length) override;

    int Write(int fd, butil::IOBuf buf, uint64_t offset,
                      int length) override;

    int Sync(int fd) override;

    int Append(int fd, const char* buf, int length) override;

    int Fallocate(int fd, int op, uint64_t offset, int length) override;

    int Fstat(int fd, struct stat* info) override;

    int Fsync(int fd) override;

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
