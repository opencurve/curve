/*
 *  Copyright (c) 2020 NetEase Inc.
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
 * File Created: 18-10-31
 * Author: yangyaokai
 */

#ifndef SRC_FS_EXT4_FILESYSTEM_IMPL_H_
#define SRC_FS_EXT4_FILESYSTEM_IMPL_H_

#include <memory>
#include <string>
#include <vector>
#include <map>
#include <thread>
#include "bthread/butex.h"
#include "src/fs/local_filesystem.h"
#include "src/fs/wrap_posix.h"

const int MAX_RETYR_TIME = 3;

namespace curve {
namespace fs {
struct CoRoutineContext {
   butil::atomic<int>* waiter;
   volatile long res;
   volatile long res2; 
};

class Ext4FileSystemImpl : public LocalFileSystem {
 public:
    virtual ~Ext4FileSystemImpl();
    static std::shared_ptr<Ext4FileSystemImpl> getInstance();
    void SetPosixWrapper(std::shared_ptr<PosixWrapper> wrapper);

    int Init(const LocalFileSystemOption& option) override;
    int Uninit();
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
    int Append(int fd, const char* buf, int length) override;
    int Fallocate(int fd, int op, uint64_t offset,
                  int length) override;
    int Fstat(int fd, struct stat* info) override;
    int Fsync(int fd) override;

 private:
    explicit Ext4FileSystemImpl(std::shared_ptr<PosixWrapper>);
    int DoRename(const string& oldPath,
                 const string& newPath,
                 unsigned int flags) override;
    bool CheckKernelVersion();
    void ReapIo();
    int ReadCoroutine_(int fd, char* buf, uint64_t offset, int length);
    int WriteCoroutine_(int fd, const char* buf, uint64_t offset, int length);
    int ReadPread_(int fd, char* buf, uint64_t offset, int length);
    int WritePwrite_(int fd, const char* buf, uint64_t offset, int length);

 private:
    static std::shared_ptr<Ext4FileSystemImpl> self_;
    static std::mutex mutex_;
    std::shared_ptr<PosixWrapper> posixWrapper_;
    bool enableRenameat2_;
    bool enableCoroutine_;
    bool enableAio_;
    int maxEvents_;
    io_context_t ctx_;
    std::thread th_;
    bool stop_;
};

}  // namespace fs
}  // namespace curve

#endif  // SRC_FS_EXT4_FILESYSTEM_IMPL_H_
