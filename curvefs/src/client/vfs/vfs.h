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

#include <string>
#include <memory>

#include "src/common/configuration.h"
#include "curvefs/src/client/common/config.h"
#include "curvefs/src/client/vfs/meta.h"
#include "curvefs/src/client/vfs/cache.h"
#include "curvefs/src/client/vfs/config.h"
#include "curvefs/src/client/vfs/handlers.h"
#include "curvefs/src/client/vfs/permission.h"
#include "curvefs/src/client/vfs/operations.h"

namespace curvefs {
namespace client {
namespace vfs {

using ::curve::common::Configuration;
using ::curvefs::client::common::VFSOption;
using ::curvefs::client::common::FuseClientOption;

class VFS {
 public:
    // NOTE: |cfg| include all configures for client.conf
    explicit VFS(std::shared_ptr<Configure> cfg);
    VFS() = delete;

    // init
    CURVEFS_ERROR Mount(const std::string& fsname,
                        const std::string& mountpoint);

    CURVEFS_ERROR Umount(const std::string& fsname,
                         const std::string& mountpoint);

    // directory*
    CURVEFS_ERROR MkDir(const std::string& path, uint16_t mode);

    CURVEFS_ERROR MkDirs(const std::string& path, uint16_t mode);

    CURVEFS_ERROR RmDir(const std::string& path);

    CURVEFS_ERROR OpenDir(const std::string& path, DirStream* stream);

    CURVEFS_ERROR ReadDir(DirStream* stream, DirEntry* dirEntry);

    CURVEFS_ERROR CloseDir(DirStream* stream);

    // file*
    CURVEFS_ERROR Create(const std::string& path, uint16_t mode);

    CURVEFS_ERROR Open(const std::string& path,
                       uint32_t flags,
                       uint16_t mode,
                       File* file);

    CURVEFS_ERROR LSeek(uint64_t fd, uint64_t offset, int whence);

    CURVEFS_ERROR Read(uint64_t fd, char* buffer, size_t count, size_t* nread);

    CURVEFS_ERROR Write(uint64_t fd,
                        const char* buffer,
                        size_t count,
                        size_t* nwritten);

    CURVEFS_ERROR FSync(uint64_t fd);

    CURVEFS_ERROR Close(uint64_t fd);

    CURVEFS_ERROR Unlink(const std::string& path);

    // others
    CURVEFS_ERROR StatFs(struct statvfs* statvfs);

    CURVEFS_ERROR LStat(const std::string& path, struct stat* stat);

    CURVEFS_ERROR FStat(uint64_t fd, struct stat* stat);

    CURVEFS_ERROR SetAttr(const std::string& path,
                          struct stat* stat,
                          int toSet);

    CURVEFS_ERROR Chmod(const std::string& path, uint16_t mode);

    CURVEFS_ERROR Chown(const std::string& path, uint32_t uid, uint32_t gid);

    CURVEFS_ERROR Rename(const std::string& oldpath,
                         const std::string& newpath);

    CURVEFS_ERROR Remove(const std::string& path);

    CURVEFS_ERROR RemoveAll(const std::string& path);

    // utility
    void Attr2Stat(InodeAttr* attr, struct stat* stat);

#ifdef UNIT_TEST
    void SetOperations(std::shared_ptr<Operations> op) { op_ = op; }
#endif  // UNIT_TEST

 private:
    Configuration Convert(std::shared_ptr<Configure> cfg);

    void PurgeAttrCache(Ino ino);

    void PurgeEntryCache(Ino parent, const std::string& name);

    CURVEFS_ERROR DoMkDir(const std::string& path,
                          uint16_t mode,
                          EntryOut* entryOut);

    CURVEFS_ERROR DoLookup(Ino parent,
                           const std::string& name,
                           Ino* ino);

    CURVEFS_ERROR DoGetAttr(Ino ino, InodeAttr* attr);

    CURVEFS_ERROR DoReadLink(Ino ino, std::string dir, std::string* target);

    CURVEFS_ERROR Lookup(const std::string& path,
                         bool followSymlink,
                         Entry* entry);

 private:
    FuseClientOption option_;
    std::shared_ptr<FuseClient> client_;
    std::shared_ptr<Operations> op_;
    std::shared_ptr<Permission> permission_;
    std::shared_ptr<FileHandlers> handlers_;
    std::shared_ptr<EntryCache> entryCache_;
    std::shared_ptr<AttrCache> attrCache_;
};

}  // namespace vfs
}  // namespace client
}  // namespace curvefs

#endif  // CURVEFS_SRC_CLIENT_VFS_VFS_H_
