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
 * Created Date: 2023-07-07
 * Author: Jingli Chen (Wine93)
 */

#include "curvefs/src/client/vfs/meta.h"
#include "curvefs/src/client/vfs/operations.h"

namespace curvefs {
namespace client {
namespace vfs {

namespace {

struct fuse_req {
    fuse_ctx ctx;
};

struct FuseContext {
    FuseContext() {
        ctx = fuse_ctx();
        ctx.uid = 0;
        ctx.gid = 0;

        req = fuse_req();
        req.ctx = ctx;

        fi = fuse_file_info();
    }

    fuse_req_t Request() {
        return reinterpret_cast<fuse_req_t>(&req);
    }

    fuse_file_info* FileInfo() {
        return &fi;
    }

    fuse_req req;
    fuse_ctx ctx;
    fuse_file_info fi;
};

};  // namespace

OperationsImpl::OperationsImpl(std::shared_ptr<FuseClient> client)
    : client_(client),
      fs_(client->GetFileSystem()) {
    ctx_ = OperationsCtx();
    ctx_.uid = 0;  // TODO(Wine93): uid and gid
    ctx_.gid = 0;
}

// init
CURVEFS_ERROR OperationsImpl::Umount() {
    // client_->FuseOpDestroy(nullptr);  // FIXME(Wine93)
    client_->Fini();
    client_->UnInit();
    return CURVEFS_ERROR::OK;
}

// directory*
CURVEFS_ERROR OperationsImpl::MkDir(Ino parent,
                                    const std::string& name,
                                    uint16_t mode) {
    EntryOut entryOut;
    auto ctx = FuseContext();
    auto req = ctx.Request();
    return client_->FuseOpMkDir(req, parent, name.c_str(), mode, &entryOut);
}

CURVEFS_ERROR OperationsImpl::OpenDir(Ino ino, uint64_t* fh) {
    auto ctx = FuseContext();
    auto fi = ctx.FileInfo();
    CURVEFS_ERROR rc = fs_->OpenDir(ino, fi);
    if (rc == CURVEFS_ERROR::OK) {
        *fh = fi->fh;
    }
    return rc;
}

// Don't shock why we readdir by filesystem insted of client :)
CURVEFS_ERROR OperationsImpl::ReadDir(Ino ino,
                                      uint64_t fh,
                                      std::shared_ptr<DirEntryList>* entries) {
    auto ctx = FuseContext();
    auto fi = ctx.FileInfo();
    fi->fh = fh;
    return fs_->ReadDir(ino, fi, entries);
}

CURVEFS_ERROR OperationsImpl::CloseDir(Ino ino) {
    auto ctx = FuseContext();
    auto req = ctx.Request();
    auto fi = ctx.FileInfo();
    return client_->FuseOpRelease(req, ino, fi);
}

CURVEFS_ERROR OperationsImpl::RmDir(Ino parent, const std::string& name) {
    auto ctx = FuseContext();
    auto req = ctx.Request();
    return client_->FuseOpRmDir(req, parent, name.c_str());
}

// file*
CURVEFS_ERROR OperationsImpl::Create(Ino parent,
                                     const std::string& name,
                                     uint16_t mode) {
    auto ctx = FuseContext();
    auto req = ctx.Request();
    auto fi = ctx.FileInfo();
    EntryOut entryOut;
    auto rc = client_->FuseOpCreate(req, parent, name.c_str(),
                                    mode, fi, &entryOut);
    return rc;
}

CURVEFS_ERROR OperationsImpl::Open(Ino ino, uint32_t flags) {
    auto ctx = FuseContext();
    auto req = ctx.Request();
    auto fi = ctx.FileInfo();
    FileOut fileOut;
    fi->flags = flags;
    return client_->FuseOpOpen(req, ino, fi, &fileOut);
}

CURVEFS_ERROR OperationsImpl::Read(Ino ino,
                                   uint64_t offset,
                                   char* buffer,
                                   size_t size,
                                   size_t* nread) {
    auto ctx = FuseContext();
    auto req = ctx.Request();
    auto fi = ctx.FileInfo();
    return client_->FuseOpRead(req, ino, size, offset, fi, buffer, nread);
}

CURVEFS_ERROR OperationsImpl::Write(Ino ino,
                                    uint64_t offset,
                                    char* buffer,
                                    size_t size,
                                    size_t* nwritten) {
    auto ctx = FuseContext();
    auto req = ctx.Request();
    auto fi = ctx.FileInfo();
    FileOut fileOut;
    auto rc = client_->FuseOpWrite(req, ino, buffer, size,
                                   offset, fi, &fileOut);
    if (rc == CURVEFS_ERROR::OK) {
        *nwritten = fileOut.nwritten;
    } else {
        *nwritten = 0;
    }
    return rc;
}

CURVEFS_ERROR OperationsImpl::Flush(Ino ino) {
    auto ctx = FuseContext();
    auto req = ctx.Request();
    auto fi = ctx.FileInfo();
    return client_->FuseOpFlush(req, ino, fi);
}

CURVEFS_ERROR OperationsImpl::Close(Ino ino) {
    auto ctx = FuseContext();
    auto req = ctx.Request();
    auto fi = ctx.FileInfo();
    return client_->FuseOpRelease(req, ino, fi);
}

CURVEFS_ERROR OperationsImpl::Unlink(Ino parent, const std::string& name) {
    auto ctx = FuseContext();
    auto req = ctx.Request();
    return client_->FuseOpUnlink(req, parent, name.c_str());
}

// others
CURVEFS_ERROR OperationsImpl::Lookup(Ino parent,
                                     const std::string& name,
                                     EntryOut* entryOut) {
    auto ctx = FuseContext();
    auto req = ctx.Request();
    auto rc = client_->FuseOpLookup(req, parent, name.c_str(), entryOut);
    if (rc == CURVEFS_ERROR::OK) {
        fs_->SetEntryTimeout(entryOut);
    }
    return rc;
}

CURVEFS_ERROR OperationsImpl::GetAttr(Ino ino, AttrOut* attrOut) {
    auto ctx = FuseContext();
    auto req = ctx.Request();
    auto fi = ctx.FileInfo();
    auto rc = client_->FuseOpGetAttr(req, ino, fi, attrOut);
    if (rc == CURVEFS_ERROR::OK) {
        fs_->SetAttrTimeout(attrOut);
    }
    return rc;
}

CURVEFS_ERROR OperationsImpl::SetAttr(Ino ino, struct stat* stat, int toSet) {
    auto ctx = FuseContext();
    auto req = ctx.Request();
    auto fi = ctx.FileInfo();
    AttrOut attrOut;
    return client_->FuseOpSetAttr(req, ino, stat, toSet, fi, &attrOut);
}

CURVEFS_ERROR OperationsImpl::ReadLink(Ino ino, std::string* link) {
    auto ctx = FuseContext();
    auto req = ctx.Request();
    return client_->FuseOpReadLink(req, ino, link);
}

CURVEFS_ERROR OperationsImpl::Rename(Ino parent,
                                     const std::string& name,
                                     Ino newparent,
                                     const std::string& newname) {
    auto ctx = FuseContext();
    auto req = ctx.Request();
    return client_->FuseOpRename(req, parent, name.c_str(),
                                 newparent, newname.c_str(), 0);
}

CURVEFS_ERROR OperationsImpl::StatFS(Ino ino, struct statvfs* statvfs) {
    auto ctx = FuseContext();
    auto req = ctx.Request();
    return client_->FuseOpStatFs(req, ino, statvfs);
}

// utility
void OperationsImpl::Attr2Stat(InodeAttr* attr, struct stat* stat) {
    return fs_->Attr2Stat(attr, stat);
}

}  // namespace vfs
}  // namespace client
}  // namespace curvefs
