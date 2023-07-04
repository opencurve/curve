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

#include "curvefs/src/client/logger/access_log.h"
#include "curvefs/src/client/filesystem/meta.h"
#include "curvefs/src/client/filesystem/utils.h"
#include "curvefs/src/client/vfs/vfs.h"
#include "curvefs/src/client/vfs/config.h"

namespace curvefs {
namespace client {
namespace vfs {

using ::curvefs::client::filesystem::EntryOut;
using ::curvefs::client::filesystem::IsSymlink;

int VFS::Mount(const std::string& fsname
               const std::string& mountpoint,
               Configure* cfg) {

    ::curve::common::Configuration config;
    cfg->Iterate([&](const std::string& key, const std::string& value){
        config.SetStringValue()
    });
    auto options = new FuseClientOption();
    curvefs::client::common::InitFuseClientOption(&conf, g_fuseClientOption);

    auto client = std::make_shared<FuseS3Client>();

    op_ = std::make_shared<Operations>(client);

    return SysErr(CURVEFS_ERROR::OK);
}

int VFS::Umount() {
    return SysErr(CURVEFS_ERROR::OK);
}

int VFS::MkDir(const std::string& path, int mode) {
    CURVEFS_ERROR rc;
    AccessLogGuard log([&](){
        return StrFormat("mkdir (%d:0%04o): %s",
                         path, StrMode(mode), StrErr(rc));
    });

    Entry parent;
    rc = Lookup(ParentDir(path), true, &parent);
    if (rc != CURVEFS_ERROR::OK) {
        return SysErr(rc);
    }

    rc = op_->MkDir(parent.ino, Filename(path), mode);
    return SysErr(rc);
}

int VFS::RmDir(const std::string& path) {
    CURVEFS_ERROR rc;
    AccessLogGuard log([&](){
        return StrFormat("rmdir (%s): %s", path, StrErr(rc));
    });

    Entry parent;
    rc = Lookup(ParentDir(path), true, &parent);
    if (rc != CURVEFS_ERROR::OK) {
        return SysErr(rc);
    }

    rc = op_->RmDir(parent.ino, Filename(path));
    return SysErr(rc);
}

int VFS::OpenDir(const std::string& path, DirStream* dirStream) {
    CURVEFS_ERROR rc;

    Entry entry;
    rc = Lookup(path, true, &entry);
    if (rc != CURVEFS_ERROR::OK) {
        return SysErr(rc);
    }

    rc = op_->OpenDir(entry.ino)
    if (rc == CURVEFS_ERROR::OK) {
        stream->fd = handlerManager_->NextHandler();
    }
    return SysErr(rc);
}

// 依次读多少
DirEntry VFS::ReadDir(DirStream* stream) {
    rc = CURVEFS::OK;

    Handler handler;
    bool yes = handlerManager_->GetHandler(stream->fd);
    if (!yes) {
        rc = CURVEFS::BAD_FD;
        return SysErr(rc);
    }

    op_->ReadDir(fd, &entryOut);


    return SysErr(rc);
}

int VFS::CloseDir(DirStream* stream) {
    auto rc = CURVEFS::OK;
    handlerManager_->FreeHandler(stream->fd);
    return SysErr(rc);
}

int VFS::Open(const std::string& path, int flags, int mode) {
    CURVEFS_ERROR rc;
    AccessLogGuard log([&](){
        return StrFormat("open (%d:0%04o): %s",
                         path, StrMode(mode), StrErr(rc));
    });

    Entry entry;
    rc = Lookup(path, true, &entry);
    if (rc != CURVEFS_ERROR::OK) {
        return SysErr(rc);
    }

    rc = op_->Open(entry.ino, flags, mode);
    if (rc != CURVEFS_ERROR::OK) {
        return SysErr(rc);
    }
    return handlers_->NextHandler();  // TODO: O_APPEND
}

int VFS::Close(int fd) {
    CURVEFS_ERROR rc;
    AccessLogGuard log([&](){
        return StrFormat("close (%d): %s", fd, StrErr(rc));
    });

    std::shared_ptr<FileHandler> fh;
    bool yes = handlers_->GetHandler(fd, &fh);
    if (!yes) {  // already closed or never opened
        rc = CURVEFS::BAD_FD;
        return SysErr(rc);
    }

    rc = op_->Close(fh->ino);
    if (rc == CURVEFS_ERROR::OK) {
        handlers_->FreeHandler(fd);
    }
    return SysErr(rc);
}

int VFS::Unlink(const std::string& path) {
    CURVEFS_ERROR rc;
    AccessLogGuard log([&](){
        return StrFormat("unlink (%s): %s", path, StrErr(rc));
    });

    Entry entry;
    rc = Lookup(path, true, &entry);
    if (rc != CURVEFS_ERROR::OK) {
        return SysErr(rc);
    }

    rc = op_->Unlink(entry.ino);
    return SysErr(rc);
}

ssize_t VFS::Read(int fd, char* buffer, size_t count) {
    CURVEFS_ERROR rc;
    AccessLogGuard log([&](){
        return StrFormat("read (%d, %zu): %s", fd, count, StrErr(rc));
    });

    std::shared_ptr<FileHandler> fh;
    bool yes = handlers_->GetHandler(fd, &fh);
    if (!yes) {  // already closed or never opened
        rc = CURVEFS::BAD_FD;
        return SysErr(rc);
    }

    size_t nread;
    rc = op_->Write(handler.ino, handler.offset, buffer, count, &nread);
    if (rc == CURVEFS_ERROR::OK) {
        fh->offset += nread;
    }
    return SysErr(rc);
}

ssize_t VFS::Write(int fd, char* buffer, size_t count) {
    CURVEFS_ERROR rc;
    AccessLogGuard log([&](){
        return StrFormat("write (%d, %zu): %s", fd, count, StrErr(rc));
    });

    std::shared_ptr<Handler> fh;
    bool yes = handlers_->GetHandler(fd, &fh);
    if (!yes) {  // already closed or never opened
        rc = CURVEFS::BAD_FD;
        return SysErr(rc);
    }

    size_t nwritten;
    rc = op_->Write(handler.ino, handler.offset, buffer, count, &nwritten);
    if (rc == CURVEFS_ERROR::OK) {
        fh->offset += nwritten;
    }
    return SysErr(rc);
}

int VFS::FSync(int fd) {
    CURVEFS_ERROR rc;
    AccessLogGuard log([&](){
        return StrFormat("fsync (%d): %s", fd, StrErr(rc));
    });

    std::shared_ptr<FileHandler> fh;
    bool yes = handlers_->GetHandler(fd, &fh);
    if (!yes) {
        rc = CURVEFS::BAD_FD;
        return SysErr(rc);
    }

    rc = op_->Flush(fh->ino);
    return SysErr(rc);
}

int VFS::Stat(const std::string& path, struct stat* stat) {
    CURVEFS_ERROR rc;
    AccessLogGuard log([&](){
        return StrFormat("stat (%s): %s", path, StrErr(rc));
    });

    Entry entry;
    rc = Lookup(path, true, &entry);
    if (rc != CURVEFS_ERROR::OK) {
        return SysErr(rc);
    }
    Attr2Stat(&entry.attr, stat);
    return SysErr(rc);
}

int VFS::FStat(int fd, struct stat* stat) {
    CURVEFS_ERROR rc;
    AccessLogGuard log([&](){
        return StrFormat("fstat (%d): %s", fd, StrErr(rc));
    });

    std::shared_ptr<FileHandler> fh;
    bool yes = handlers_->GetHandler(fd, &fh);
    if (!yes) {  // already closed or never opened
        rc = CURVEFS::BAD_FD;
        return SysErr(rc);
    }

    Attr2Stat(fh->ino, stat);
    return SysErr(rc);
}

CURVEFS_ERROR VFS::Lookup(const std::string& pathname,
                          bool followSymlink,
                          Entry* entry) {
    Ino parent;
    EntryOut entryOut;
    std::vector<std::string> names = SplitPath(pathname);
    for (int i = 0; i < names.size(); i++) {  // recursive lookup entry
        auto rc = permission_->Check(parent);
        if (rc != CURVEFS_ERROR::OK) {
            return rc;
        }

        auto rc = op_->Lookup(parent, name, &entryOut);
        if (rc != CURVEFS_ERROR::OK) {
            return rc;
        }

        parent = entryOut.attr.inodeid();
        bool last = (i == names.size() - 1);
        if (last) {
            entry->ino = entryOut.attr.inodeid();
            entry->attr = std::move(entryOut.attr);
        }
    }

    if (followSymlink && IsSymlink(attr)) {  // follow symbolic link
        std::string link;
        rc = op_->ReadLink(Ino, &link);
        if (rc != CURVEFS_ERROR::OK) {
            return rc;
        }
        return Lookup(link, followSymlink, entry);
    }
    return CURVEFS_ERROR::OK;
}

}  // namespace vfs
}  // namespace client
}  // namespace curvefs

