/*
 *  Copyright (c) 2023 NetEase Inc.
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
 * Project: Curve
 * Created Date: 2023-06-29
 * Author: Jingli Chen (Wine93)
 */

#ifndef CURVEFS_SRC_CLIENT_VFS_VFS_H_
#define CURVEFS_SRC_CLIENT_VFS_VFS_H_

/*
int open(String path, int flags, int mode) throws IOException;
void fstat(int fd, CurveStat stat) throws IOException;
void lstat(String path, CurveStat stat) throws IOException;
void statfs(String path, CurveStatVFS stat) throws IOException;
void unlink(String path) throws IOException;
void rmdir(String path) throws IOException;
String[] listdir(String path) throws IOException;
void setattr(String path, CurveStat stat, int mask) throws IOException;
void chmod(String path, int mode) throws IOException;
long lseek(int fd, long offset, int whence) throws IOException;
void close(int fd) throws IOException;
void shutdown() throws IOException;
void rename(String src, String dst) throws IOException;
int write(int fd, byte[] buf, long size, long offset) throws IOException;
int read(int fd, byte[] buf, long size, long offset) throws IOException;
void mkdirs(String path, int mode) throws IOException;
void fsync(int fd) throws IOException;

*/

#include <memory>

#include "curvefs/src/client/vfs/meta.h"
#include "curvefs/src/client/vfs/permission.h"
#include "curvefs/src/client/vfs/operations.h"
#include "curvefs/src/client/vfs/handler.h"

namespace curvefs {
namespace client {
namespace vfs {

#include <string>


// fd  对应关系在这一层：ino -> fd???
class VFS {
 public:
    VFS() = default();

    int Mount(const std::string& fsname,
              const std::string& mountpoint,
              Configure* cfg);

    int Umount();

    // directory interface
    int MkDir(const std::string& path, int mode);

    int OpenDir(const std::string& path, DirStream* stream);

    DirEntry ReadDir(DirStream* stream);

    int CloseDir(DirStream* stream);

    int RmDir(const std::string& path);

    // file interface
    int Open(const std::string& path, int flags, int mode);

    ssize_t Read(int fd, char* buffer, size_t count);

    ssize_t Write(int fd, char* buffer, size_t count);

    int FSync(int fd);

    int Close(int fd);

    int Unlink(const std::string& path);

    // attr / xattr
    int Stat(const std::string& path, struct stat* stat);

    int FStat(int fd, struct stat* stat);

 private:
    CURVEFS_ERROR Lookup(const std::string& pathname,
                         bool followSymlink,
                         Entry* entry);

 private:
    std::shared_ptr<Operations> op_;  // filesystem operations
    std::shared_ptr<Permission> permission_;
    std::shared_ptr<FileHandlers> handlers_;
    std::shared_ptr<Cache> cache_;
};

}  // namespace vfs
}  // namespace client
}  // namespace curvefs

#endif  // CURVEFS_SRC_CLIENT_VFS_VFS_H_
