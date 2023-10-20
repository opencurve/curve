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
 * Created Date: 2023-09-22
 * Author: Jingli Chen (Wine93)
 */

#include <gtest/gtest.h>

#include <vector>
#include <string>
#include <algorithm>

#include "curvefs/src/client/filesystem/utils.h"
#include "curvefs/src/client/vfs/meta.h"
#include "curvefs/test/client/filesystem/helper/helper.h"
#include "curvefs/test/client/vfs/helper/mem_operations.h"

namespace curvefs {
namespace client {
namespace vfs {

using ::curvefs::client::filesystem::Now;
using ::curvefs::client::filesystem::MkAttr;
using ::curvefs::client::filesystem::AttrOption;
using ::curvefs::client::filesystem::FileSystemBuilder;

MemStorage::MemStorage()
    : maxIno_(ROOT_INO),
      entries_(),
      attrs_() {
    auto option = AttrOption().uid(0).gid(0);
    auto attr = MkAttr(ROOT_INO, option);
    attrs_.emplace(ROOT_INO, attr);
}

Ino MemStorage::AllocIno() {
    return ++maxIno_;
}

CURVEFS_ERROR MemStorage::InsertAttr(Ino ino, const InodeAttr& attr) {
    if (attrs_.find(ino) != attrs_.end()) {
        return CURVEFS_ERROR::EXISTS;
    }
    attrs_.emplace(ino, attr);
    return CURVEFS_ERROR::OK;
}

CURVEFS_ERROR MemStorage::GetAttr(Ino ino, InodeAttr* attr) {
    auto iter = attrs_.find(ino);
    if (iter == attrs_.end()) {
        return CURVEFS_ERROR::NOT_EXIST;
    }
    *attr = iter->second;
    return CURVEFS_ERROR::OK;
}

CURVEFS_ERROR MemStorage::UpdateAttr(Ino ino, const InodeAttr& attr) {
    auto iter = attrs_.find(ino);
    if (iter == attrs_.end()) {
        return CURVEFS_ERROR::NOT_EXIST;
    }
    iter->second = attr;
    return CURVEFS_ERROR::OK;
}

CURVEFS_ERROR MemStorage::DeleteAttr(Ino ino) {
    auto iter = attrs_.find(ino);
    if (iter != attrs_.end()) {
        attrs_.erase(iter);
    }
    return CURVEFS_ERROR::OK;
}

CURVEFS_ERROR MemStorage::InsertEntry(Ino parent,
                                      const std::string& name,
                                      Ino ino) {
    auto iter = entries_.find(parent);
    if (iter == entries_.end()) {
        auto entries = std::map<std::string, Ino>{ { name, ino } };
        entries_.emplace(parent, entries);
        return CURVEFS_ERROR::OK;
    }

    auto& tree = iter->second;
    auto subiter = tree.find(name);
    if (subiter != tree.end()) {
        return CURVEFS_ERROR::EXISTS;
    }

    tree.emplace(name, ino);
    return CURVEFS_ERROR::OK;
}

CURVEFS_ERROR MemStorage::GetEntry(Ino parent,
                                   const std::string& name,
                                   Ino* ino) {
    auto iter = entries_.find(parent);
    if (iter == entries_.end()) {
        return CURVEFS_ERROR::NOT_EXIST;
    }

    auto& tree = iter->second;
    auto subiter = tree.find(name);
    if (subiter == tree.end()) {
        return CURVEFS_ERROR::NOT_EXIST;
    }

    *ino = subiter->second;
    return CURVEFS_ERROR::OK;
}

CURVEFS_ERROR MemStorage::DeleteEntry(Ino parent, const std::string& name) {
    auto iter = entries_.find(parent);
    if (iter != entries_.end()) {
        auto& tree = iter->second;
        tree.erase(name);
    }
    return CURVEFS_ERROR::OK;
}

CURVEFS_ERROR MemStorage::GetEntries(Ino parent,
                                     std::shared_ptr<DirEntryList>* entries) {
    auto iter = entries_.find(parent);
    if (iter == entries_.end()) {
        return CURVEFS_ERROR::OK;
    }

    DirEntry dirEntry;
    auto& tree = iter->second;
    for (const auto& entry : tree) {
        dirEntry.name = entry.first;
        dirEntry.ino = entry.second;
        (*entries)->Add(dirEntry);
    }
    return CURVEFS_ERROR::OK;
}

MemOperations::MemOperations()
    : fs_(FileSystemBuilder().Build()),
      storage_(std::make_shared<MemStorage>()),
      contents_() {}

CURVEFS_ERROR MemOperations::Mount(const std::string& fsname,
                                   const std::string& mountpoint,
                                   FuseClientOption option) {
    return CURVEFS_ERROR::OK;
}

CURVEFS_ERROR MemOperations::Umount(const std::string& fsname,
                                    const std::string& mountpoint) {
    return CURVEFS_ERROR::OK;
}

CURVEFS_ERROR MemOperations::MkDir(Ino parent,
                                   const std::string& name,
                                   uint16_t mode,
                                   EntryOut* entryOut) {
    Ino ino = storage_->AllocIno();
    auto attr = MkAttr(ino, AttrOption().type(FsFileType::TYPE_DIRECTORY));
    auto rc = storage_->InsertAttr(ino, attr);
    if (rc == CURVEFS_ERROR::OK) {
        rc = storage_->InsertEntry(parent, name, ino);
    }

    if (rc == CURVEFS_ERROR::OK) {
        entryOut->attr = attr;
    }
    return rc;
}

CURVEFS_ERROR MemOperations::RmDir(Ino parent, const std::string& name) {
    Ino ino;
    auto rc = storage_->GetEntry(parent, name, &ino);
    if (rc != CURVEFS_ERROR::OK) {
        return rc;
    }

    rc = storage_->DeleteEntry(parent, name);
    if (rc == CURVEFS_ERROR::OK) {
        rc = storage_->DeleteAttr(ino);
    }
    return rc;
}

CURVEFS_ERROR MemOperations::OpenDir(Ino ino, uint64_t* fh) {
    InodeAttr attr;
    return storage_->GetAttr(ino, &attr);
}

CURVEFS_ERROR MemOperations::ReadDir(Ino ino,
                                     uint64_t fh,
                                     std::shared_ptr<DirEntryList>* entries) {
    InodeAttr attr;
    auto rc = storage_->GetAttr(ino, &attr);
    if (rc != CURVEFS_ERROR::OK) {
        return rc;
    }
    return storage_->GetEntries(ino, entries);
}

CURVEFS_ERROR MemOperations::CloseDir(Ino ino) {
    InodeAttr attr;
    return storage_->GetAttr(ino, &attr);
}

CURVEFS_ERROR MemOperations::Create(Ino parent,
                                    const std::string& name,
                                    uint16_t mode,
                                    EntryOut* entryOut) {
    Ino ino = storage_->AllocIno();
    auto attr = MkAttr(ino,
        AttrOption().type(FsFileType::TYPE_FILE).mode(mode));
    auto rc = storage_->InsertAttr(ino, attr);
    if (rc == CURVEFS_ERROR::OK) {
        rc = storage_->InsertEntry(parent, name, ino);
    }

    if (rc == CURVEFS_ERROR::OK) {
        entryOut->attr = attr;
    }
    return rc;
}

CURVEFS_ERROR MemOperations::Open(Ino ino, uint32_t flags) {
    InodeAttr attr;
    return storage_->GetAttr(ino, &attr);
}

CURVEFS_ERROR MemOperations::Read(Ino ino,
                                  uint64_t offset,
                                  char* buffer,
                                  size_t size,
                                  size_t* nread) {
    InodeAttr attr;
    auto rc = storage_->GetAttr(ino, &attr);
    if (rc != CURVEFS_ERROR::OK) {
        return rc;
    }

    auto iter = contents_.find(ino);
    if (iter == contents_.end()) {
        *nread = 0;
        return CURVEFS_ERROR::OK;
    }

    auto content = iter->second;
    if (offset >= content.size()) {
        *nread = 0;
        return CURVEFS_ERROR::OK;
    }

    uint64_t left = offset;
    uint64_t right = std::min(offset + size, content.size());
    *nread = right - left;
    memcpy(buffer + left, content.data(), *nread);
    return CURVEFS_ERROR::OK;
}

CURVEFS_ERROR MemOperations::Write(Ino ino,
                                   uint64_t offset,
                                   const char* buffer,
                                   size_t size,
                                   size_t* nwritten) {
    InodeAttr attr;
    auto rc = storage_->GetAttr(ino, &attr);
    if (rc != CURVEFS_ERROR::OK) {
        return rc;
    }

    auto iter = contents_.find(ino);
    if (iter == contents_.end()) {
        contents_.emplace(ino, std::vector<char>(buffer, buffer + size));
        *nwritten = size;
        attr.set_length(size);
        return storage_->UpdateAttr(ino, attr);
    }

    auto& content = iter->second;
    if (offset + size > content.size()) {
        content.resize(offset + size);
    }
    memcpy(content.data() + offset, buffer, size);
    *nwritten = size;
    attr.set_length(content.size());
    return storage_->UpdateAttr(ino, attr);
}

CURVEFS_ERROR MemOperations::Flush(Ino ino) {
    InodeAttr attr;
    return storage_->GetAttr(ino, &attr);
}

CURVEFS_ERROR MemOperations::Close(Ino ino) {
    InodeAttr attr;
    return storage_->GetAttr(ino, &attr);
}

CURVEFS_ERROR MemOperations::Unlink(Ino parent, const std::string& name) {
    Ino ino;
    auto rc = storage_->GetEntry(parent, name, &ino);
    if (rc == CURVEFS_ERROR::NOT_EXIST) {
        return CURVEFS_ERROR::OK;
    }

    rc = storage_->DeleteEntry(parent, name);
    if (rc == CURVEFS_ERROR::OK) {
        rc = storage_->DeleteAttr(ino);
    }
    return rc;
}

CURVEFS_ERROR MemOperations::StatFs(Ino ino, struct statvfs* statvfs) {
    statvfs->f_blocks = 1UL << 30;
    statvfs->f_files = 1UL << 30;
    return CURVEFS_ERROR::OK;
}

CURVEFS_ERROR MemOperations::Lookup(Ino parent,
                                    const std::string& name,
                                    EntryOut* entryOut) {
    Ino ino;
    auto rc = storage_->GetEntry(parent, name, &ino);
    if (rc != CURVEFS_ERROR::OK) {
        return rc;
    }

    InodeAttr attr;
    rc = storage_->GetAttr(ino, &attr);
    if (rc == CURVEFS_ERROR::OK) {
        entryOut->attr = attr;
        fs_->SetEntryTimeout(entryOut);
    }
    return rc;
}

CURVEFS_ERROR MemOperations::GetAttr(Ino ino, AttrOut* attrOut) {
    InodeAttr attr;
    auto rc = storage_->GetAttr(ino, &attr);
    if (rc == CURVEFS_ERROR::OK) {
        attrOut->attr = attr;
        fs_->SetAttrTimeout(attrOut);
    }
    return rc;
}

CURVEFS_ERROR MemOperations::SetAttr(Ino ino, struct stat* stat, int toSet) {
    InodeAttr attr;
    auto rc = storage_->GetAttr(ino, &attr);
    if (rc != CURVEFS_ERROR::OK) {
        return rc;
    }

    if (toSet & AttrMask::SET_ATTR_MODE) {
        attr.set_mode(stat->st_mode);
    }
    if (toSet & AttrMask::SET_ATTR_UID) {
        attr.set_uid(stat->st_uid);
    }
    if (toSet & AttrMask::SET_ATTR_GID) {
        attr.set_gid(stat->st_gid);
    }
    if (toSet & AttrMask::SET_ATTR_SIZE) {
        attr.set_mode(stat->st_size);
    }
    return storage_->UpdateAttr(ino, attr);
}

CURVEFS_ERROR MemOperations::ReadLink(Ino ino, std::string* link) {
    InodeAttr attr;
    auto rc = storage_->GetAttr(ino, &attr);
    if (rc == CURVEFS_ERROR::OK) {
        *link = attr.symlink();
    }
    return rc;
}

CURVEFS_ERROR MemOperations::Rename(Ino parent,
                                    const std::string& name,
                                    Ino newparent,
                                    const std::string& newname) {
    Ino ino;
    auto rc = storage_->GetEntry(parent, name, &ino);
    if (rc == CURVEFS_ERROR::OK) {
        rc = storage_->DeleteEntry(parent, name);
    }
    if (rc != CURVEFS_ERROR::OK) {
        return rc;
    }

    rc = storage_->DeleteEntry(newparent, newname);
    if (rc == CURVEFS_ERROR::OK) {
        rc = storage_->InsertEntry(newparent, newname, ino);
    }
    return rc;
}

void MemOperations::Attr2Stat(InodeAttr* attr, struct stat* stat) {
    return fs_->Attr2Stat(attr, stat);
}

}  // namespace vfs
}  // namespace client
}  // namespace curvefs
