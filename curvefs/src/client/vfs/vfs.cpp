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

#include "absl/strings/string_view.h"
#include "absl/strings/str_split.h"

#include <string>
#include <utility>
#include <vector>

#include <ctime>
#include <iostream>
#include <unistd.h>

#include "src/common/uuid.h"
#include "curvefs/src/client/helper.h"
#include "curvefs/src/client/logger/access_log.h"
#include "curvefs/src/client/filesystem/utils.h"
#include "curvefs/src/client/filesystem/error.h"
#include "curvefs/src/client/vfs/config.h"
#include "curvefs/src/client/vfs/meta.h"
#include "curvefs/src/client/vfs/utils.h"
#include "curvefs/src/client/vfs/vfs.h"

namespace curvefs {
namespace client {
namespace vfs {

using ::curve::common::UUIDGenerator;
using ::curvefs::client::Helper;
using ::curvefs::client::logger::AccessLogGuard;
using ::curvefs::client::logger::StrFormat;
using ::curvefs::client::filesystem::IsSymlink;
using ::curvefs::client::filesystem::StrErr;
using ::curvefs::client::filesystem::StrMode;

VFS::VFS() {
    auto option = option_.vfsCacheOption;
    entryCache_ = std::make_shared<EntryCache>(option.entryCacheLruSize);
    attrCache_ = std::make_shared<AttrCache>(option.attrCacheLruSize);
    handlers_ = std::make_shared<FileHandlers>();
}

bool VFS::Convert(std::shared_ptr<Configure> cfg, Configuration* out) {
    cfg->Iterate([&](const std::string& key, const std::string& value){
        out->SetStringValue(key, value);
    });
    return true;
}

CURVEFS_ERROR VFS::Mount(const std::string& fsname,
                         const std::string& mountpoint,
                         std::shared_ptr<Configure> cfg) {
    CURVEFS_ERROR rc;
    AccessLogGuard log([&]() {
        return StrFormat("mount (%s,%s): %s", fsname, mountpoint, StrErr(rc));
    });

    Configuration config;
    bool ok = Convert(cfg, &config);
    if (!ok) {
        rc = CURVEFS_ERROR::INTERNAL;
        return rc;
    }

    // FIXME: mountpoint
    std::shared_ptr<FuseClient> client;
    auto helper = Helper();
    auto uuid = UUIDGenerator().GenerateUUID();
    auto yes = helper.NewClientForSDK(fsname, uuid, &config, &client);
    if (!yes) {
        rc = CURVEFS_ERROR::INTERNAL;
        return rc;
    }

    op_ = std::make_shared<OperationsImpl>(client);
    rc = CURVEFS_ERROR::OK;
    return rc;
}

CURVEFS_ERROR VFS::Umount() {
    CURVEFS_ERROR rc;
    AccessLogGuard log([&]() {
        return StrFormat("umount: %s", StrErr(rc));
    });

    // FIXME: let it works
    //rc = op_->Umount();
    //return rc;
    return CURVEFS_ERROR::OK;
}

CURVEFS_ERROR VFS::MkDir(const std::string& path, uint16_t mode) {
    CURVEFS_ERROR rc;
    AccessLogGuard log([&]() {
        return StrFormat("mkdir (%s): %s", path, StrErr(rc));
    });

    if (path == "/") {
        rc = CURVEFS_ERROR::EXISTS;
        return rc;
    }

    Entry parent;
    rc = Lookup(filepath::ParentDir(path), true, &parent);
    if (rc != CURVEFS_ERROR::OK) {
        return rc;
    }

    rc = op_->MkDir(parent.ino, filepath::Filename(path), mode);
    return rc;
}

CURVEFS_ERROR VFS::OpenDir(const std::string& path, DirStream* stream) {
    CURVEFS_ERROR rc;
    AccessLogGuard log([&](){
        return StrFormat("opendir (%s): %s", path, StrErr(rc));
    });

    Entry entry;
    rc = Lookup(path, true, &entry);
    if (rc != CURVEFS_ERROR::OK) {
        return rc;
    }

    stream->ino = entry.ino;
    stream->offset = 0;
    rc = op_->OpenDir(entry.ino, &stream->fh);
    return rc;
}

CURVEFS_ERROR VFS::ReadDir(DirStream* stream, DirEntry* dirEntry) {
    CURVEFS_ERROR rc;
    AccessLogGuard log([&](){
        return StrFormat("readdir (%d): %s", stream->fh, StrErr(rc));
    });

    auto entries = std::make_shared<DirEntryList>();
    rc = op_->ReadDir(stream->ino, stream->fh, &entries);
    if (rc != CURVEFS_ERROR::OK) {
        return rc;
    }

    // FIXME(Wine93): readdir once
    if (stream->offset >= entries->Size()) {
        rc = CURVEFS_ERROR::END_OF_FILE;
        return rc;
    }

    rc = CURVEFS_ERROR::OK;
    entries->At(stream->offset, dirEntry);
    stream->offset++;
    return rc;
}

CURVEFS_ERROR VFS::CloseDir(DirStream* stream) {
    CURVEFS_ERROR rc;
    AccessLogGuard log([&](){
        return StrFormat("closedir (%d): %s", stream->fh, StrErr(rc));
    });

    rc = op_->CloseDir(stream->ino);
    return rc;
}

CURVEFS_ERROR VFS::RmDir(const std::string& path) {
    CURVEFS_ERROR rc;
    AccessLogGuard log([&](){
        return StrFormat("rmdir (%s): %s", path, StrErr(rc));
    });

    Entry parent;
    rc = Lookup(filepath::ParentDir(path), true, &parent);
    if (rc != CURVEFS_ERROR::OK) {
        return rc;
    }

    rc = op_->RmDir(parent.ino, filepath::Filename(path));
    return rc;
}

CURVEFS_ERROR VFS::Create(const std::string& path, uint16_t mode) {
    CURVEFS_ERROR rc;
    AccessLogGuard log([&](){
        return StrFormat("create (%s,%s:0%04o): %s",
                         path, StrMode(mode), mode,  StrErr(rc));
    });

    Entry parent;
    rc = Lookup(filepath::ParentDir(path), true, &parent);
    if (rc != CURVEFS_ERROR::OK) {
        return rc;
    }

    EntryOut entryOut;
    rc = op_->Create(parent.ino, filepath::Filename(path), S_IFREG | mode);
    return rc;
}

CURVEFS_ERROR VFS::Open(const std::string& path,
                        uint32_t flags,
                        uint16_t mode,
                        uint64_t* fd) {
    CURVEFS_ERROR rc;
    AccessLogGuard log([&](){
        return StrFormat("open (%s,%s:0%04o): %s [fh:%d]",
                         path, StrMode(mode), mode, StrErr(rc), *fd);
    });

    Entry entry;
    rc = Lookup(path, true, &entry);
    if (rc != CURVEFS_ERROR::OK) {
        return rc;
    }

    rc = op_->Open(entry.ino, flags);
    if (rc != CURVEFS_ERROR::OK) {
        return rc;
    }

    // TODO(Wine93): O_APPEND
    *fd = handlers_->NextHandler(entry.ino);
    return CURVEFS_ERROR::OK;
}

CURVEFS_ERROR VFS::LSeek(uint64_t fd, uint64_t offset, int whence) {
    CURVEFS_ERROR rc;
    AccessLogGuard log([&](){
        return StrFormat("lseek (%d, %lu, %d): %s",
                         fd, offset, whence, StrErr(rc));
    });

    std::shared_ptr<FileHandler> fh;
    bool yes = handlers_->GetHandler(fd, &fh);
    if (!yes) {  // already closed or never opened
        rc = CURVEFS_ERROR::BAD_FD;
        return rc;
    }

    AttrOut attrOut;
    switch (whence) {
    case SEEK_SET:
        fh->offset = offset;
        break;

    case SEEK_CUR:
        fh->offset += offset;
        break;

    case SEEK_END:
        rc = op_->GetAttr(fh->ino, &attrOut);
        if (rc != CURVEFS_ERROR::OK) {
            return rc;
        }
        fh->offset = attrOut.attr.length() + offset;
        break;

    default:
        rc = CURVEFS_ERROR::INVALID_PARAM;
        return rc;
    }

    rc = CURVEFS_ERROR::OK;
    return rc;
}

CURVEFS_ERROR VFS::Read(uint64_t fd,
                        char* buffer,
                        size_t count,
                        size_t* nread) {
    CURVEFS_ERROR rc;
    AccessLogGuard log([&](){
        return StrFormat("read (%d, %zu): %s", fd, count, StrErr(rc));
    });

    std::shared_ptr<FileHandler> fh;
    bool yes = handlers_->GetHandler(fd, &fh);
    if (!yes) {  // already closed or never opened
        rc = CURVEFS_ERROR::BAD_FD;
        return rc;
    }

    rc = op_->Read(fh->ino, fh->offset, buffer, count, nread);
    if (rc == CURVEFS_ERROR::OK) {
        fh->offset += *nread;
    }
    return rc;
}

CURVEFS_ERROR VFS::Write(uint64_t fd,
                         char* buffer,
                         size_t count,
                         size_t* nwritten) {
    CURVEFS_ERROR rc;
    AccessLogGuard log([&](){
        return StrFormat("write (%d, %zu): %s", fd, count, StrErr(rc));
    });

    std::shared_ptr<FileHandler> fh;
    bool yes = handlers_->GetHandler(fd, &fh);
    if (!yes) {  // already closed or never opened
        rc = CURVEFS_ERROR::BAD_FD;
        return rc;
    }

    rc = op_->Write(fh->ino, fh->offset, buffer, count, nwritten);
    if (rc == CURVEFS_ERROR::OK) {
        fh->offset += *nwritten;
    }
    return rc;
}

CURVEFS_ERROR VFS::FSync(uint64_t fd) {
    CURVEFS_ERROR rc;
    AccessLogGuard log([&](){
        return StrFormat("fsync (%d): %s", fd, StrErr(rc));
    });

    std::shared_ptr<FileHandler> fh;
    bool yes = handlers_->GetHandler(fd, &fh);
    if (!yes) {
        rc = CURVEFS_ERROR::BAD_FD;
        return rc;
    }

    rc = op_->Flush(fh->ino);
    return rc;
}

CURVEFS_ERROR VFS::Close(uint64_t fd) {
    CURVEFS_ERROR rc;
    AccessLogGuard log([&](){
        return StrFormat("close (%d): %s", fd, StrErr(rc));
    });

    std::shared_ptr<FileHandler> fh;
    bool yes = handlers_->GetHandler(fd, &fh);
    if (!yes) {  // already closed or never opened
        rc = CURVEFS_ERROR::BAD_FD;
        return rc;
    }

    rc = op_->Flush(fh->ino);
    if (rc != CURVEFS_ERROR::OK) {
        return rc;
    }

    rc = op_->Close(fh->ino);
    if (rc == CURVEFS_ERROR::OK) {
        handlers_->FreeHandler(fd);
    }
    return rc;
}

CURVEFS_ERROR VFS::Unlink(const std::string& path) {
    CURVEFS_ERROR rc;
    AccessLogGuard log([&](){
        return StrFormat("unlink (%s): %s", path, StrErr(rc));
    });

    Entry parent;
    rc = Lookup(filepath::ParentDir(path), true, &parent);
    if (rc != CURVEFS_ERROR::OK) {
        return rc;
    }

    rc = op_->Unlink(parent.ino, filepath::Filename(path));
    return rc;
}

CURVEFS_ERROR VFS::StatFS(const std::string& path, struct statvfs* statvfs) {
    CURVEFS_ERROR rc;
    rc = op_->StatFS(ROOT_INO, statvfs);  // FIXME(Wine93): resolve path
    return rc;
}

CURVEFS_ERROR VFS::LStat(const std::string& path, struct stat* stat) {
    CURVEFS_ERROR rc;
    AccessLogGuard log([&](){
        return StrFormat("stat (%s): %s", path, StrErr(rc));
    });

    Entry entry;
    rc = Lookup(path, false, &entry);
    if (rc == CURVEFS_ERROR::OK) {
        op_->Attr2Stat(&entry.attr, stat);
    }
    return rc;
}

CURVEFS_ERROR VFS::FStat(uint64_t fd, struct stat* stat) {
    CURVEFS_ERROR rc;
    AccessLogGuard log([&](){
        return StrFormat("fstat (%d): %s", fd, StrErr(rc));
    });

    std::shared_ptr<FileHandler> fh;
    bool yes = handlers_->GetHandler(fd, &fh);
    if (!yes) {  // already closed or never opened
        rc = CURVEFS_ERROR::BAD_FD;
        return rc;
    }

    AttrOut attrOut;
    rc = op_->GetAttr(fh->ino, &attrOut);
    if (rc == CURVEFS_ERROR::OK) {
        op_->Attr2Stat(&attrOut.attr, stat);
    }
    return rc;
}

CURVEFS_ERROR VFS::SetAttr(const char* path, struct stat* stat, int toSet) {
    CURVEFS_ERROR rc;
    AccessLogGuard log([&]() {
        return StrFormat("setattr (%s, %o): %s", path, toSet, StrErr(rc));
    });

    Entry entry;
    rc = Lookup(path, true, &entry);
    if (rc != CURVEFS_ERROR::OK) {
        return rc;
    }

    rc = op_->SetAttr(entry.ino, stat, toSet);
    return rc;
}

CURVEFS_ERROR VFS::Chmod(const char* path, uint16_t mode) {
    CURVEFS_ERROR rc;
    AccessLogGuard log([&]() {
        return StrFormat("chmod (%s, %o): %s", path, mode, StrErr(rc));
    });

    Entry entry;
    rc = Lookup(path, true, &entry);
    if (rc != CURVEFS_ERROR::OK) {
        return rc;
    }

    struct stat stat;
    mode = 0100000 | mode;  // FIXME(Wine93): handle with umask and...
    stat.st_mode = mode;
    rc = op_->SetAttr(entry.ino, &stat, VFS_SET_ATTR_MODE);
    return rc;
}

CURVEFS_ERROR VFS::Rename(const std::string& oldpath,
                          const std::string& newpath) {
    CURVEFS_ERROR rc;
    AccessLogGuard log([&](){
        return StrFormat("rename (%s, %s): %s", oldpath, newpath, StrErr(rc));
    });

    Entry oldParent;
    rc = Lookup(filepath::ParentDir(oldpath), true, &oldParent);
    if (rc != CURVEFS_ERROR::OK) {
        return rc;
    }

    Entry newParent;
    rc = Lookup(filepath::ParentDir(newpath), true, &newParent);
    if (rc != CURVEFS_ERROR::OK) {
        return rc;
    }

    rc = op_->Rename(oldParent.ino, filepath::Filename(oldpath),
                     newParent.ino, filepath::Filename(newpath));
    return rc;
}

void VFS::Attr2Stat(InodeAttr* attr, struct stat* stat) {
    return op_->Attr2Stat(attr, stat);
}

CURVEFS_ERROR VFS::DoLookup(Ino parent,
                            const std::string& name,
                            Ino* ino) {
    bool yes = entryCache_->Get(parent, name, ino);
    if (yes) {
        return CURVEFS_ERROR::OK;
    }

    EntryOut entryOut;
    auto rc = op_->Lookup(parent, name, &entryOut);
    if (rc != CURVEFS_ERROR::OK) {
        return rc;
    }

    *ino = entryOut.attr.inodeid();
    entryCache_->Put(parent, name, *ino, entryOut.entryTimeout);
    attrCache_->Put(*ino, entryOut.attr, entryOut.attrTimeout);
    return CURVEFS_ERROR::OK;
}

CURVEFS_ERROR VFS::DoGetAttr(Ino ino, InodeAttr* attr) {
    bool yes = attrCache_->Get(ino, attr);
    if (yes) {
        return CURVEFS_ERROR::OK;
    }

    AttrOut attrOut;
    auto rc = op_->GetAttr(ino, &attrOut);
    if (rc != CURVEFS_ERROR::OK) {
        return rc;
    }

    attrCache_->Put(ino, attrOut.attr, attrOut.attrTimeout);
    *attr = std::move(attrOut.attr);
    return CURVEFS_ERROR::OK;
}

CURVEFS_ERROR VFS::Lookup(const std::string& path,
                          bool followSymlink,
                          Entry* entry) {
    Ino parent = ROOT_INO;
    entry->ino = ROOT_INO;
    std::vector<std::string> names = filepath::Split(path);

    // recursive lookup entry
    for (int i = 0; i < names.size(); i++) {
        std::string name = names[i];
        bool yes = permission_->Check(parent, name);
        if (!yes) {
            return CURVEFS_ERROR::NO_PERMISSION;
        }

        auto rc = DoLookup(parent, name, &entry->ino);
        if (rc == CURVEFS_ERROR::OK) {
            rc = DoGetAttr(entry->ino, &entry->attr);
        }
        if (rc != CURVEFS_ERROR::OK) {
            return rc;
        }

        // FIXME(Wine93): handle link which is realpath
        // follow symbolic link
        bool last = (i == names.size() - 1);
        if ((!last || followSymlink) && IsSymlink(entry->attr)) {
            std::string link;
            auto rc = op_->ReadLink(entry->ino, &link);
            if (rc != CURVEFS_ERROR::OK) {
                return rc;
            }
            rc = Lookup(link, followSymlink, entry);
            if (rc != CURVEFS_ERROR::OK) {
                return rc;
            }
        }

        // parent
        parent = entry->ino;
    }

    if (parent == ROOT_INO) {
        auto rc = DoGetAttr(entry->ino, &entry->attr);
        if (rc != CURVEFS_ERROR::OK) {
            return rc;
        }
    }
    return CURVEFS_ERROR::OK;
}

}  // namespace vfs
}  // namespace client
}  // namespace curvefs
