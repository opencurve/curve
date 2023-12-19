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

#include <unistd.h>

#include <ctime>
#include <string>
#include <utility>
#include <vector>
#include <iostream>

#include "absl/strings/string_view.h"
#include "absl/strings/str_split.h"
#include "curvefs/src/client/sdk_helper.h"
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

using ::curvefs::client::SDKHelper;
using ::curvefs::client::logger::AccessLogGuard;
using ::curvefs::client::logger::StrFormat;
using ::curvefs::client::filesystem::IsDir;
using ::curvefs::client::filesystem::IsSymlink;
using ::curvefs::client::filesystem::StrErr;
using ::curvefs::client::filesystem::StrMode;
using ::curvefs::client::filesystem::StrAttr;

Configuration VFS::Convert(std::shared_ptr<Configure> cfg) {
    Configuration conf;
    cfg->Iterate([&](const std::string& key, const std::string& value){
        conf.SetStringValue(key, value);
    });
    return conf;
}

VFS::VFS(std::shared_ptr<Configure> cfg) {
    auto helper = SDKHelper();
    auto conf = Convert(cfg);
    helper.InitLog(&conf);
    helper.InitOption(&conf, &option_);

    auto vfsOption = option_.vfsOption;
    auto userPermOption = vfsOption.userPermissionOption;
    auto cacheOption = vfsOption.vfsCacheOption;
    client_ = std::make_shared<FuseS3Client>();
    permission_ = std::make_shared<Permission>(userPermOption);
    entryCache_ = std::make_shared<EntryCache>(cacheOption.entryCacheLruSize);
    attrCache_ = std::make_shared<AttrCache>(cacheOption.attrCacheLruSize);
    handlers_ = std::make_shared<FileHandlers>();
    op_ = std::make_shared<OperationsImpl>(permission_, client_);
}

CURVEFS_ERROR VFS::Mount(const std::string& fsname,
                         const std::string& mountpoint) {
    CURVEFS_ERROR rc;
    AccessLogGuard log([&]() {
        return StrFormat("mount (%s,%s): %s", fsname, mountpoint, StrErr(rc));
    });

    rc = op_->Mount(fsname, mountpoint, option_);
    return rc;
}

CURVEFS_ERROR VFS::Umount(const std::string& fsname,
                          const std::string& mountpoint) {
    CURVEFS_ERROR rc;
    AccessLogGuard log([&]() {
        return StrFormat("umount (%s,%s): %s", fsname, mountpoint, StrErr(rc));
    });

    rc = op_->Umount(fsname, mountpoint);
    return rc;
}

CURVEFS_ERROR VFS::DoMkDir(const std::string& path,
                           uint16_t mode,
                           EntryOut* entryOut) {
    Entry entry, parent;
    CURVEFS_ERROR rc;
    AccessLogGuard log([&]() {
        return StrFormat("mkdir (%s,%s:0%04o): %s%s",
                         path, StrMode(mode), mode,
                         StrErr(rc), StrEntry(*entryOut));
    });

    rc = Lookup(path, true, &entry);
    if (rc == CURVEFS_ERROR::OK) {
        rc = CURVEFS_ERROR::EXISTS;
        entryOut->attr = entry.attr;
        return rc;
    }

    rc = Lookup(filepath::ParentDir(path), true, &parent);
    if (rc != CURVEFS_ERROR::OK) {
        return rc;
    } else if (!IsDir(parent.attr)) {
        return CURVEFS_ERROR::NOT_A_DIRECTORY;
    }

    rc = permission_->Check(parent.attr, Permission::WANT_WRITE);
    if (rc != CURVEFS_ERROR::OK) {
        return rc;
    }

    mode = permission_->GetFileMode(S_IFDIR, mode);
    rc = op_->MkDir(parent.ino, filepath::Filename(path), mode, entryOut);
    if (rc == CURVEFS_ERROR::OK) {
        PurgeEntryCache(parent.ino, filepath::Filename(path));
    }
    return rc;
}

CURVEFS_ERROR VFS::MkDir(const std::string& path, uint16_t mode) {
    EntryOut entryOut;
    return DoMkDir(path, mode, &entryOut);
}

CURVEFS_ERROR VFS::MkDirs(const std::string& path, uint16_t mode) {
    if (path == "/") {
        return CURVEFS_ERROR::OK;
    }

    EntryOut entryOut;
    auto rc = DoMkDir(path, mode, &entryOut);
    if (rc == CURVEFS_ERROR::OK || (rc == CURVEFS_ERROR::EXISTS &&
                                    IsDir(entryOut.attr))) {
        return CURVEFS_ERROR::OK;
    } else if (rc == CURVEFS_ERROR::NOT_EXIST) {
        rc = MkDirs(filepath::ParentDir(path), mode);
        if (rc == CURVEFS_ERROR::OK) {
            rc = MkDirs(path, mode);
        }
    }
    return rc;
}

CURVEFS_ERROR VFS::RmDir(const std::string& path) {
    Entry parent, entry;
    CURVEFS_ERROR rc;
    AccessLogGuard log([&](){
        return StrFormat("rmdir (%s): %s", path, StrErr(rc));
    });

    rc = Lookup(filepath::ParentDir(path), true, &parent);
    if (rc != CURVEFS_ERROR::OK) {
        return rc;
    }

    rc = permission_->Check(parent.attr, Permission::WANT_WRITE);
    if (rc != CURVEFS_ERROR::OK) {
        return rc;
    }

    rc = op_->RmDir(parent.ino, filepath::Filename(path));
    if (rc == CURVEFS_ERROR::OK) {
        PurgeEntryCache(parent.ino, filepath::Filename(path));
    }
    return rc;
}

CURVEFS_ERROR VFS::OpenDir(const std::string& path, DirStream* stream) {
    Entry entry;
    CURVEFS_ERROR rc;
    AccessLogGuard log([&](){
        return StrFormat("opendir (%s): %s [fh:%d]",
                         path, StrErr(rc), stream->fh);
    });

    rc = Lookup(path, true, &entry);
    if (rc != CURVEFS_ERROR::OK) {
        return rc;
    }

    rc = permission_->Check(entry.attr, Permission::WANT_READ);
    if (rc != CURVEFS_ERROR::OK) {
        return rc;
    }

    stream->ino = entry.ino;
    stream->offset = 0;
    rc = op_->OpenDir(entry.ino, &stream->fh);
    return rc;
}

CURVEFS_ERROR VFS::ReadDir(DirStream* stream, DirEntry* dirEntry) {
    uint64_t nread = 0;
    CURVEFS_ERROR rc;
    AccessLogGuard log([&](){
        return StrFormat("readdir (%d,%d): %s (%d)",
                         stream->fh, stream->offset, StrErr(rc), nread);
    });

    auto entries = std::make_shared<DirEntryList>();
    rc = op_->ReadDir(stream->ino, stream->fh, &entries);
    if (rc != CURVEFS_ERROR::OK) {
        return rc;
    }

    if (stream->offset >= entries->Size()) {
        rc = CURVEFS_ERROR::END_OF_FILE;
        return rc;
    }

    rc = CURVEFS_ERROR::OK;
    entries->At(stream->offset, dirEntry);
    stream->offset++;
    nread = 1;
    return rc;
}

CURVEFS_ERROR VFS::CloseDir(DirStream* stream) {
    CURVEFS_ERROR rc;
    AccessLogGuard log([&](){
        return StrFormat("closedir (%d): %s",
                         stream->fh, StrErr(rc));
    });

    rc = op_->CloseDir(stream->ino);
    return rc;
}

CURVEFS_ERROR VFS::Create(const std::string& path, uint16_t mode) {
    Entry parent;
    EntryOut entryOut;
    CURVEFS_ERROR rc;
    AccessLogGuard log([&](){
        return StrFormat("create (%s,%s:0%04o): %s%s",
                         path, StrMode(mode), mode,
                         StrErr(rc), StrEntry(entryOut));
    });

    rc = Lookup(filepath::ParentDir(path), true, &parent);
    if (rc != CURVEFS_ERROR::OK) {
        return rc;
    }

    rc = permission_->Check(parent.attr, Permission::WANT_WRITE);
    if (rc != CURVEFS_ERROR::OK) {
        return rc;
    }

    mode = permission_->GetFileMode(S_IFREG, mode);
    rc = op_->Create(parent.ino, filepath::Filename(path), mode, &entryOut);
    if (rc == CURVEFS_ERROR::OK) {
        PurgeEntryCache(parent.ino, filepath::Filename(path));
    }
    return rc;
}

CURVEFS_ERROR VFS::Open(const std::string& path,
                        uint32_t flags,
                        uint16_t mode,
                        uint64_t* fd) {
    Entry entry;
    CURVEFS_ERROR rc;
    AccessLogGuard log([&](){
        return StrFormat("open (%s): %s [fh:%d]", path, StrErr(rc), *fd);
    });

    rc = Lookup(path, true, &entry);
    if (rc != CURVEFS_ERROR::OK) {
        return rc;
    }

    uint16_t want = permission_->WantPermission(flags);
    rc = permission_->Check(entry.attr, want);
    if (rc != CURVEFS_ERROR::OK) {
        return rc;
    }

    FileOut fileOut;
    for (int retries = 0; retries < 2; retries++) {
        rc = op_->Open(entry.ino, flags, &fileOut);
        if (rc == CURVEFS_ERROR::OK) {
            break;
        } else if (rc == CURVEFS_ERROR::STALE) {
            PurgeAttrCache(entry.ino);
            continue;
        } else {  // Encourage error
            return rc;
        }
    }

    uint64_t offset = 0;
    uint64_t length = fileOut.attr.length();
    if (flags & O_APPEND) {
        offset = length;
    }
    *fd = handlers_->NextHandler(entry.ino, offset, length, flags);
    return CURVEFS_ERROR::OK;
}

CURVEFS_ERROR VFS::LSeek(uint64_t fd, uint64_t offset, int whence) {
    AttrOut attrOut;
    CURVEFS_ERROR rc;
    std::shared_ptr<FileHandler> fh;
    AccessLogGuard log([&](){
        return StrFormat("lseek (%d,%d,%d): %s (%d)",
                         fd, offset, whence, StrErr(rc), fh->offset);
    });

    bool yes = handlers_->GetHandler(fd, &fh);
    if (!yes) {
        rc = CURVEFS_ERROR::BAD_FD;
        return rc;
    }

    switch (whence) {
    case SEEK_SET:
        fh->offset = offset;
        break;

    case SEEK_CUR:
        fh->offset += offset;
        break;

    case SEEK_END:
        fh->offset = fh->length + offset;
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
    std::shared_ptr<FileHandler> fh;
    uint64_t offset = 0;
    AccessLogGuard log([&](){
        return StrFormat("read (%d,%d,%d): %s (%d)",
                         fd, count, offset, StrErr(rc), *nread);
    });

    bool yes = handlers_->GetHandler(fd, &fh);
    if (!yes) {
        rc = CURVEFS_ERROR::BAD_FD;
        return rc;
    }

    offset = fh->offset;
    rc = op_->Read(fh->ino, fh->offset, buffer, count, nread);
    if (rc == CURVEFS_ERROR::OK) {
        fh->offset += *nread;  // FIXME(Wine93): need lock here?
    }
    return rc;
}

CURVEFS_ERROR VFS::Write(uint64_t fd,
                         const char* buffer,
                         size_t count,
                         size_t* nwritten) {
    CURVEFS_ERROR rc;
    std::shared_ptr<FileHandler> fh;
    uint64_t offset = 0;
    AccessLogGuard log([&](){
        return StrFormat("write (%d,%d,%d): %s (%d)",
                         fd, count, offset, StrErr(rc), *nwritten);
    });

    bool yes = handlers_->GetHandler(fd, &fh);
    if (!yes) {
        rc = CURVEFS_ERROR::BAD_FD;
        return rc;
    }

    FileOut fileOut;
    offset = fh->offset;
    rc = op_->Write(fh->ino, fh->offset, buffer, count, &fileOut);
    if (rc == CURVEFS_ERROR::OK) {
        *nwritten = fileOut.nwritten;
        fh->offset += *nwritten;  // FIXME(Wine93): need lock here?
        fh->length = fileOut.attr.length();
        PurgeAttrCache(fh->ino);
    }
    return rc;
}

CURVEFS_ERROR VFS::FSync(uint64_t fd) {
    CURVEFS_ERROR rc;
    std::shared_ptr<FileHandler> fh;
    AccessLogGuard log([&](){
        return StrFormat("fsync (%d): %s", fd, StrErr(rc));
    });

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
    std::shared_ptr<FileHandler> fh;
    AccessLogGuard log([&](){
        return StrFormat("close (%d): %s", fd, StrErr(rc));
    });

    bool yes = handlers_->GetHandler(fd, &fh);
    if (!yes) {
        rc = CURVEFS_ERROR::BAD_FD;
        return rc;
    }

    uint32_t flags = fh->flags;
    if (flags & O_WRONLY || flags & O_RDWR) {
        rc = op_->Flush(fh->ino);
        if (rc != CURVEFS_ERROR::OK) {
            return rc;
        }
    }

    rc = op_->Close(fh->ino);
    if (rc == CURVEFS_ERROR::OK) {
        handlers_->FreeHandler(fd);
    }
    return rc;
}

CURVEFS_ERROR VFS::Unlink(const std::string& path) {
    Entry parent;
    CURVEFS_ERROR rc;
    AccessLogGuard log([&](){
        return StrFormat("unlink (%s): %s", path, StrErr(rc));
    });

    rc = Lookup(filepath::ParentDir(path), true, &parent);
    if (rc != CURVEFS_ERROR::OK) {
        return rc;
    }

    rc = permission_->Check(parent.attr, Permission::WANT_WRITE);
    if (rc != CURVEFS_ERROR::OK) {
        return rc;
    }

    rc = op_->Unlink(parent.ino, filepath::Filename(path));
    if (rc == CURVEFS_ERROR::OK) {
        PurgeEntryCache(parent.ino, filepath::Filename(path));
    }
    return rc;
}

CURVEFS_ERROR VFS::StatFs(struct statvfs* statvfs) {
    CURVEFS_ERROR rc;
    AccessLogGuard log([&](){
        return StrFormat("statfs : %s", StrErr(rc));
    });

    rc = op_->StatFs(ROOT_INO, statvfs);
    return rc;
}

CURVEFS_ERROR VFS::LStat(const std::string& path, struct stat* stat) {
    CURVEFS_ERROR rc;
    Entry entry;
    AccessLogGuard log([&](){
        return StrFormat("lstat (%s): %s%s",
                         path, StrErr(rc), StrAttr(entry.attr));
    });

    rc = Lookup(path, false, &entry);
    if (rc != CURVEFS_ERROR::OK) {
        return rc;
    }

    op_->Attr2Stat(&entry.attr, stat);
    return rc;
}

CURVEFS_ERROR VFS::FStat(uint64_t fd, struct stat* stat) {
    std::shared_ptr<FileHandler> fh;
    AttrOut attrOut;
    CURVEFS_ERROR rc;
    AccessLogGuard log([&](){
        return StrFormat("fstat (%d): %s%s", fd, StrErr(rc), StrAttr(attrOut));
    });

    bool yes = handlers_->GetHandler(fd, &fh);
    if (!yes) {
        rc = CURVEFS_ERROR::BAD_FD;
        return rc;
    }

    rc = op_->GetAttr(fh->ino, &attrOut);
    if (rc != CURVEFS_ERROR::OK) {
        return rc;
    }

    op_->Attr2Stat(&attrOut.attr, stat);
    return rc;
}

CURVEFS_ERROR VFS::SetAttr(const std::string& path,
                           struct stat* stat,
                           int toSet) {
    Entry entry;
    CURVEFS_ERROR rc;
    AccessLogGuard log([&]() {
        return StrFormat("setattr (%s,0x%X): %s", path, toSet, StrErr(rc));
    });

    rc = Lookup(path, true, &entry);
    if (rc != CURVEFS_ERROR::OK) {
        return rc;
    }

    auto file = entry.attr;
    bool yes = permission_->IsSuperUser() || permission_->IsFileOwner(file);
    if (!yes) {
        rc = CURVEFS_ERROR::NO_PERMISSION;
        return rc;
    }

    rc = op_->SetAttr(entry.ino, stat, toSet);
    if (rc == CURVEFS_ERROR::OK) {
        PurgeAttrCache(entry.ino);
    }
    return rc;
}

CURVEFS_ERROR VFS::Chmod(const std::string& path, uint16_t mode) {
    Entry entry;
    struct stat stat;
    CURVEFS_ERROR rc;
    AccessLogGuard log([&]() {
        return StrFormat("chmod (%s,%s:0%04o): %s",
                         path, StrMode(mode), mode, StrErr(rc));
    });

    rc = Lookup(path, true, &entry);
    if (rc != CURVEFS_ERROR::OK) {
        return rc;
    }

    auto file = entry.attr;
    bool yes = permission_->IsSuperUser() || permission_->IsFileOwner(file);
    if (!yes) {
        rc = CURVEFS_ERROR::NO_PERMISSION;
        return rc;
    }

    stat.st_mode = ((file.mode() >> 9) << 9) | mode;
    rc = op_->SetAttr(entry.ino, &stat, AttrMask::SET_ATTR_MODE);
    if (rc == CURVEFS_ERROR::OK) {
        PurgeAttrCache(entry.ino);
    }
    return rc;
}

CURVEFS_ERROR VFS::Chown(const std::string &path, uint32_t uid, uint32_t gid) {
    Entry entry;
    CURVEFS_ERROR rc;
    AccessLogGuard log([&](){
        return StrFormat("chown (%s,%d,%d): %s", path, uid, gid, StrErr(rc));
    });

    rc = Lookup(path, true, &entry);
    if (rc != CURVEFS_ERROR::OK) {
        return rc;
    }

    uint32_t set = 0;
    auto file = entry.attr;
    if (uid != file.uid()) {
        bool yes = permission_->IsSuperUser();
        if (!yes) {
            rc = CURVEFS_ERROR::NO_PERMISSION;
            return rc;
        }
        set |= AttrMask::SET_ATTR_UID;
    }

    if (gid != file.gid()) {
        bool yes = permission_->IsSuperUser() ||
            (permission_->IsFileOwner(file) && permission_->GidInGroup(gid));
        if (!yes) {
            rc = CURVEFS_ERROR::NO_PERMISSION;
            return rc;
        }
        set |= AttrMask::SET_ATTR_GID;
    }

    struct stat stat;
    stat.st_uid = uid;
    stat.st_gid = gid;
    rc = op_->SetAttr(entry.ino, &stat, set);
    if (rc == CURVEFS_ERROR::OK) {
        PurgeAttrCache(entry.ino);
    }
    return rc;
}

CURVEFS_ERROR VFS::Rename(const std::string& oldpath,
                          const std::string& newpath) {
    Entry oldParent, newParent;
    CURVEFS_ERROR rc;
    AccessLogGuard log([&](){
        return StrFormat("rename (%s, %s): %s", oldpath, newpath, StrErr(rc));
    });

    rc = Lookup(filepath::ParentDir(oldpath), true, &oldParent);
    if (rc != CURVEFS_ERROR::OK) {
        return rc;
    }

    rc = permission_->Check(oldParent.attr, Permission::WANT_WRITE);
    if (rc != CURVEFS_ERROR::OK) {
        return rc;
    }

    rc = Lookup(filepath::ParentDir(newpath), true, &newParent);
    if (rc != CURVEFS_ERROR::OK) {
        return rc;
    }

    rc = permission_->Check(newParent.attr, Permission::WANT_WRITE);
    if (rc != CURVEFS_ERROR::OK) {
        return rc;
    }

    rc = op_->Rename(oldParent.ino, filepath::Filename(oldpath),
                     newParent.ino, filepath::Filename(newpath));
    if (rc == CURVEFS_ERROR::OK) {
        PurgeEntryCache(oldParent.ino, filepath::Filename(oldpath));
        PurgeEntryCache(newParent.ino, filepath::Filename(newpath));
    }
    return rc;
}

void VFS::Attr2Stat(InodeAttr* attr, struct stat* stat) {
    return op_->Attr2Stat(attr, stat);
}

inline void VFS::PurgeAttrCache(Ino ino) {
    attrCache_->Delete(ino);
}

inline void VFS::PurgeEntryCache(Ino parent, const std::string& name) {
    entryCache_->Delete(parent, name);
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
    *attr = attrOut.attr;
    return CURVEFS_ERROR::OK;
}

CURVEFS_ERROR VFS::DoReadLink(Ino ino, std::string dir, std::string* target) {
    std::string link;
    auto rc = op_->ReadLink(ino, &link);
    if (rc != CURVEFS_ERROR::OK) {
        return rc;
    } else if (strings::HasPrefix(link, "/")) {
        *target = link;
        return CURVEFS_ERROR::OK;
    }

    // relative path
    std::vector<std::string> subpath{ dir, link };
    *target = strings::Join(subpath, "/");
    return CURVEFS_ERROR::OK;
}

CURVEFS_ERROR VFS::Lookup(const std::string& path,
                          bool followSymlink,
                          Entry* entry) {
    if (path == "/") {
        entry->ino = ROOT_INO;
        return DoGetAttr(entry->ino, &entry->attr);
    }

    // recursive lookup entry
    CURVEFS_ERROR rc;
    Ino parent = ROOT_INO;
    std::vector<std::string> names = filepath::Split(path);
    for (int i = 0; i < names.size(); i++) {
        std::string name = names[i];
        if (parent != ROOT_INO) {
            rc = permission_->Check(entry->attr, Permission::WANT_EXEC);
            if (rc != CURVEFS_ERROR::OK) {
                return rc;
            }
        }

        rc = DoLookup(parent, name, &entry->ino);
        if (rc != CURVEFS_ERROR::OK) {
            break;
        }
        rc = DoGetAttr(entry->ino, &entry->attr);
        if (rc != CURVEFS_ERROR::OK) {
            break;
        }

        // follow symbolic link
        bool last = (i == names.size() - 1);
        if ((!last || followSymlink) && IsSymlink(entry->attr)) {
            std::string target;
            std::string dir = strings::Join(names, 0, i, "/");
            rc = DoReadLink(entry->ino, dir, &target);
            if (rc != CURVEFS_ERROR::OK) {
                break;
            }

            rc = Lookup(target, followSymlink, entry);
            if (rc != CURVEFS_ERROR::OK) {
                break;
            }
        }

        // parent
        parent = entry->ino;
    }

    if (rc != CURVEFS_ERROR::OK) {
        *entry = Entry();
    }
    return rc;
}

}  // namespace vfs
}  // namespace client
}  // namespace curvefs
