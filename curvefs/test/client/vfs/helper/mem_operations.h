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
 * Created Date: 2023-09-21
 * Author: Jingli Chen (Wine93)
 */

#ifndef CURVEFS_TEST_CLIENT_VFS_HELPER_MEM_OPERATIONS_H_
#define CURVEFS_TEST_CLIENT_VFS_HELPER_MEM_OPERATIONS_H_

#include <gmock/gmock.h>

#include <map>
#include <string>
#include <memory>
#include <vector>

#include "curvefs/src/client/vfs/operations.h"
#include "curvefs/src/client/filesystem/dir_cache.h"

namespace curvefs {
namespace client {
namespace vfs {

using ::curvefs::client::filesystem::DirEntryList;
using ::curvefs::client::filesystem::FileSystem;

class MemStorage {
 public:
    MemStorage();

    // attr*
    Ino AllocIno();

    CURVEFS_ERROR InsertAttr(Ino ino, const InodeAttr& attr);

    CURVEFS_ERROR GetAttr(Ino ino, InodeAttr* attr);

    CURVEFS_ERROR UpdateAttr(Ino ino, const InodeAttr& attr);

    CURVEFS_ERROR DeleteAttr(Ino ino);

    // entry*
    CURVEFS_ERROR InsertEntry(Ino parent, const std::string& name, Ino ino);

    CURVEFS_ERROR GetEntry(Ino parent, const std::string& name, Ino* ino);

    CURVEFS_ERROR DeleteEntry(Ino parent, const std::string& name);

    CURVEFS_ERROR GetEntries(Ino parent,
                             std::shared_ptr<DirEntryList>* entries);

 private:
    Ino maxIno_;
    std::map<Ino, InodeAttr> attrs_;
    std::map<Ino, std::map<std::string, Ino>> entries_;
};

class MemOperations : public Operations {
 public:
    MemOperations();

    // init
    CURVEFS_ERROR Mount(const std::string& fsname,
                        const std::string& mountpoint,
                        FuseClientOption option) override;

    CURVEFS_ERROR Umount(const std::string& fsname,
                         const std::string& mountpoint) override;

    // directory*
    CURVEFS_ERROR MkDir(Ino parent,
                        const std::string& name,
                        uint16_t mode,
                        EntryOut* entryOut) override;

    CURVEFS_ERROR RmDir(Ino parent, const std::string& name) override;

    CURVEFS_ERROR OpenDir(Ino ino, uint64_t* fh) override;

    CURVEFS_ERROR ReadDir(Ino ino,
                          uint64_t fh,
                          std::shared_ptr<DirEntryList>* entries) override;

    CURVEFS_ERROR CloseDir(Ino ino) override;

    // file*
    CURVEFS_ERROR Create(Ino parent,
                         const std::string& name,
                         uint16_t mode,
                         EntryOut* entryOut) override;

    CURVEFS_ERROR Open(Ino ino, uint32_t flags) override;

    CURVEFS_ERROR Read(Ino ino,
                       uint64_t offset,
                       char* buffer,
                       size_t size,
                       size_t* nread) override;

    CURVEFS_ERROR Write(Ino ino,
                        uint64_t offset,
                        const char* buffer,
                        size_t size,
                        size_t* nwritten) override;

    CURVEFS_ERROR Flush(Ino ino) override;

    CURVEFS_ERROR Close(Ino ino) override;

    CURVEFS_ERROR Unlink(Ino parent, const std::string& name) override;

    // others
    CURVEFS_ERROR StatFs(Ino ino, struct statvfs* statvfs) override;

    CURVEFS_ERROR Lookup(Ino parent,
                         const std::string& name,
                         EntryOut* entryOut) override;

    CURVEFS_ERROR GetAttr(Ino ino, AttrOut* attrOut) override;

    CURVEFS_ERROR SetAttr(Ino ino, struct stat* stat, int toSet) override;

    CURVEFS_ERROR ReadLink(Ino ino, std::string* link) override;

    CURVEFS_ERROR Rename(Ino parent,
                         const std::string& name,
                         Ino newparent,
                         const std::string& newname) override;

    // utility
    void  Attr2Stat(InodeAttr* attr, struct stat* stat) override;

 private:
    std::shared_ptr<FileSystem> fs_;
    std::shared_ptr<MemStorage> storage_;
    std::map<Ino, std::vector<char>> contents_;
};

}  // namespace vfs
}  // namespace client
}  // namespace curvefs

#endif  // CURVEFS_TEST_CLIENT_VFS_HELPER_MEM_OPERATIONS_H_
