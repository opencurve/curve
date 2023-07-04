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

#ifndef CURVEFS_TEST_CLIENT_VFS_HELPER_MOCK_OPERATIONS_H_
#define CURVEFS_TEST_CLIENT_VFS_HELPER_MOCK_OPERATIONS_H_

#include <gmock/gmock.h>

#include <string>
#include <memory>
#include <map>

#include "curvefs/src/client/vfs/operations.h"
#include "curvefs/src/client/filesystem/dir_cache.h"

namespace curvefs {
namespace client {
namespace vfs {

using ::curvefs::client::filesystem::DirEntryList;

class MemStorage {
 public:
    MemStorage();

    Ino AllocIno();

    CURVEFS_ERROR InsertAttr(Ino ino, const InodeAttr& attr);

    CURVEFS_ERROR GetAttr(Ino ino, InodeAttr* attr);

    CURVEFS_ERROR DeleteAttr(Ino ino);

    CURVEFS_ERROR InsertEntry(Ino parent, const std::string& name, Ino ino);

    CURVEFS_ERROR GetEntry(Ino parent, const std::string& name, Ino* ino);

    CURVEFS_ERROR DeleteEntry(Ino parent, const std::string& name);

    CURVEFS_ERROR GetAllEntries(Ino parent,
                                std::shared_ptr<DirEntryList>* entries);

 private:
    Ino maxIno_;
    std::map<Ino, std::map<string, Ino>> entries_;
    std::map<Ino, InodeAttr> attrs_;
};

// attr: uid,gid,size


class MemoryOperations : public operations {
 public:
    MemoryOperations();

    CURVEFS_ERROR MkDir(Ino parent,
                        const std::string& name,
                        uint16_t mode,
                        EntryOut* entryOut) = 0;

    CURVEFS_ERROR RmDir(Ino parent, const std::string& name) = 0;

    CURVEFS_ERROR OpenDir(Ino ino, uint64_t* fh) = 0;

    CURVEFS_ERROR ReadDir(Ino ino,
                          uint64_t fh,
                          std::shared_ptr<DirEntryList>* entries) = 0;

    CURVEFS_ERROR CloseDir(Ino ino) = 0;

    // file*
    CURVEFS_ERROR Create(Ino parent,
                         const std::string& name,
                         uint16_t mode,
                         EntryOut* entryOut) = 0;

    CURVEFS_ERROR Open(Ino ino, uint32_t flags) = 0;

    CURVEFS_ERROR Read(Ino ino,
                       uint64_t offset,
                       char* buffer,
                       size_t size,
                       size_t* nread) = 0;

    CURVEFS_ERROR Write(Ino ino,
                        uint64_t offset,
                        char* buffer,
                        size_t size,
                        size_t* nwritten) = 0;

    CURVEFS_ERROR Flush(Ino ino) = 0;

    CURVEFS_ERROR Close(Ino ino) = 0;

    CURVEFS_ERROR Unlink(Ino parent, const std::string& name) = 0;

    // others
    CURVEFS_ERROR StatFS(Ino ino, struct statvfs* statvfs) = 0;

    CURVEFS_ERROR Lookup(Ino parent,
                         const std::string& name,
                         EntryOut* entryOut) = 0;

    CURVEFS_ERROR GetAttr(Ino ino, AttrOut* attrOut) = 0;

    CURVEFS_ERROR SetAttr(Ino ino, struct stat* stat, int toSet) = 0;

    CURVEFS_ERROR ReadLink(Ino ino, std::string* link) = 0;

    CURVEFS_ERROR Rename(Ino parent,
                         const std::string& name,
                         Ino newparent,
                         const std::string& newname) = 0;

 private:
    Ino AllocIno();

 private:
    Ino maxIno_;
    std::map<Ino, std::map<std::string, Ino>> entries_;
    std::map<Ino, InodeAttr> attrs_;

    std::shared_ptr<MemoryStorage> storage_;

    std::map<Ino, std::vector<char>> contents_;
};

}  // namespace vfs
}  // namespace client
}  // namespace curvefs

#endif  // CURVEFS_TEST_CLIENT_VFS_HELPER_MOCK_OPERATIONS_H_
