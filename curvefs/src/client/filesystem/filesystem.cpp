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
 * Created Date: 2023-03-08
 * Author: Jingli Chen (Wine93)
 */

#include <map>


#include "curvefs/src/client/filesystem/core.h"

namespace curvefs {
namespace client {
namespace filesystem {

static const std::map<CURVEFS_ERROR, int> err2Sys = {
    { CURVEFS_ERROR::OK, 0 },
    { CURVEFS_ERROR::NO_SPACE, ENOSPC },
    { CURVEFS_ERROR::NOTEXIST, ENOENT },
    { CURVEFS_ERROR::NOPERMISSION, EACCES },
    { CURVEFS_ERROR::INVALIDPARAM, EINVAL },
    { CURVEFS_ERROR::NOTEMPTY, ENOTEMPTY },
    { CURVEFS_ERROR::NOTSUPPORT, EOPNOTSUPP },
    { CURVEFS_ERROR::NAMETOOLONG, ENAMETOOLONG },
    { CURVEFS_ERROR::OUT_OF_RANGE, ERANGE },
    { CURVEFS_ERROR::NODATA, ENODATA },
    { CURVEFS_ERROR::EXISTS, EEXIST },
    { CURVEFS_ERROR::STALE, ESTALE },
};

FileSystem::FileSystem(FileSystemOption option,
                       std::shared_ptr<RPCClient> rpcClient)
    : option_(option),
      rpc_(rpcClient) {
    handlerManager_ = std::make_shared<HandlerManager>();
    dirCache_ = std::make_shared<DirCache>(option.dirCacheOption);
    openFiles_ = std::make_shared<OpenFiles>(option_.openFileOption);
    attrWatcher_ = std::make_shared<AttrWatcher>(option_.attrWatcherOption,
                                                 openFiles_);
}

int FileSystem::SysErr(CURVEFS_ERROR code) {
    auto it = err2Sys.find(code);
    if (it != err2Sys.end()) {
        return it->second;
    }
    return EIO;
}

void FileSystem::Attr2Stat(AttrOut* attrOut, struct stat* stat) {
    std::memset(stat, 0, sizeof(struct stat));
    InodeAttr* attr = &attrOut->attr;
    stat->st_ino = attr->inodeid();
    stat->st_mode = attr->mode();
    stat->st_nlink = attr->nlink();
    stat->st_uid = attr->uid();
    stat->st_gid = attr->gid();
    stat->st_size = attr->length();
    stat->st_rdev = attr->rdev();
    stat->st_atim.tv_sec = attr->atime(); // atime
    stat->st_atim.tv_nsec = attr->atime_ns();
    stat->st_mtim.tv_sec = attr->mtime();
    stat->st_mtim.tv_nsec = attr->mtime_ns();
    stat->st_ctim.tv_sec = attr->ctime();
    stat->st_ctim.tv_nsec = attr->ctime_ns();
    stat->st_blksize = option_.blockSize;;
    stat->st_blocks = 0;
    if (IsS3File(*attr)) {
        stat->st_blocks = (attr->length() + 511) / 512;
    }
}

void FileSystem::Entry2Param(EntryOut* entryOut,
                             fuse_entry_param* e) {
    std::memset(e, 0, sizeof(fuse_entry_param));
    e->ino = entryOut->attr.inodeid();
    e->generation = 0;
    e->entry_timeout = entryOut->entryTimeout;
    e->attr_timeout = entryOut->attrTimeout;
}

void FileSystem::SetEntryTimeout(EntryOut* entryOut) {
    auto option = option_.kernelCacheOption;
    if (IsDir(entryOut->attr)) {
        entryOut->entryTimeout = option.dirEntryTimeout;
        entryOut->attrTimeout = option.dirAttrTimeout;
    } else {
        entryOut->entryTimeout = option.entryTimeout;
        entryOut->attrTimeout = option.attrTimeout;
    }
}

void FileSystem::SetAttrTimeout(AttrOut* attrOut) {
    auto option = option_.kernelCacheOption;
    if (IsDir(attrOut->attr)) {
        attrOut->attrTimeout = option.dirAttrTimeout;
    } else {
        attrOut->attrTimeout = option.attrTimeout;
    }
}

inline std::shared_ptr<FileHandler> FileSystem::NewHandler() {
    return handlerManager_->NewHandler();
}

inline std::shared_ptr<FileHandler> FileSystem::FindHandler(uint64_t id) {
    return handlerManager_->FindHandler(id);
}

void FileSystem::ReleaseHandler(uint64_t id) {
    return handlerManager_->ReleaseHandler(id);
}

void FileSystem::ReplyError(const Context& ctx, CURVEFS_ERROR code) {
    fuse_reply_err(ctx.req, SysErr(code));
}

void FileSystem::ReplyEntry(const Context& ctx,
                            EntryOut* entryOut) {
    AttrWatcherGuard watcher(attrWatcher_, &entryOut->attr);
    fuse_entry_param e;
    SetEntryTimeout(entryOut);
    Entry2Param(entryOut, &e);
    fuse_reply_entry(ctx.req, &e);
}

void FileSystem::ReplyAttr(const Context& ctx,
                           AttrOut* attrOut) {
    AttrWatcherGuard watcher(attrWatcher_, &attrOut->attr);
    struct stat stat;
    SetAttrTimeout(attrOut);
    Attr2Stat(attrOut, &stat);
    fuse_reply_attr(ctx.req, &stat, attrOut->attrTimeout);
}

void FileSystem::AddDirEntry(const Context& ctx,
                             DirEntry* dirEntry) {
    struct stat stat;
    std::memset(&stat, 0, sizeof(stat));
    stat.st_ino = dirEntry->ino;

    // add a directory entry to the buffer
    const char* name = dirEntry->name.c_str();
    DirBufferHead* buffer = FindHandler(ctx.fi->fh)->buffer;
    buffer->size += fuse_add_direntry(ctx.req, NULL, 0, name, NULL, 0);
    buffer->p = static_cast<char *>(realloc(buffer->p, buffer->size));
    size_t oldsize = buffer->size;
    fuse_add_direntry(ctx.req,
                      buffer->p + oldsize,  // char* buf
                      buffer->size - oldsize,  // size_t bufisze
                      name, &stat, buffer->size);
}

void FileSystem::AddDirEntryPlus(const Context& ctx,
                                 DirEntry* dirEntry) {
    AttrWatcherGuard watcher(attrWatcher_, &dirEntry->attr);
    struct fuse_entry_param e;
    EntryOut entryOut(dirEntry->attr);
    SetEntryTimeout(&entryOut);
    Entry2Param(&entryOut, &e);

    // add a directory entry to the buffer with the attributes
    DirBufferHead* buffer = FindHandler(ctx.fi->fh)->buffer;
    size_t oldsize = buffer->size;
    const char* name = dirEntry->name.c_str();
    buffer->size += fuse_add_direntry_plus(ctx.req, NULL, 0, name, NULL, 0);
    buffer->p = static_cast<char *>(realloc(buffer->p, buffer->size));
    fuse_add_direntry_plus(ctx.req,
                           buffer->p + oldsize,  // char* buf
                           buffer->size - oldsize,  // size_t bufisze
                           name, &e, buffer->size);
}

CURVEFS_ERROR FileSystem::Lookup(Context* ctx,
                                 Ino parent,
                                 const std::string& name,
                                 EntryOut* out) {
    if (name.size() > option_.maxNameLength) {
        return CURVEFS_ERROR::NAMETOOLONG;
    }
    return rpc_->Lookup(parent, name, out);
}

CURVEFS_ERROR FileSystem::GetAttr(Context* ctx, Ino ino, AttrOut* out) {
    InodeAttr attr;
    auto rc = rpc_->GetAttr(ino, &attr);
    if (rc == CURVEFS_ERROR::OK) {
        *out = AttrOut(attr);
    }
    return rc;
}

CURVEFS_ERROR FileSystem::OpenDir(Context* ctx, Ino ino) {
    std::shared_ptr<DirEntryList> entries;
    auto handler = NewHandler();
    bool yes = dirCache_->Get(ino, &entries);
    if (!yes) {
        ctx->fi->fh = handler->id;
        return CURVEFS_ERROR::OK;
    }

    // revalidate directory cache
    InodeAttr attr;
    CURVEFS_ERROR rc = rpc_->GetAttr(ino, &attr);
    if (rc != CURVEFS_ERROR::OK) {
        return rc;
    }

    if (entries->GetMtime() != AttrMtime(attr)) {
        dirCache_->Drop(ino);
    }
    handler->mtime = AttrMtime(attr);
    ctx->fi->fh = handler->id;
    return CURVEFS_ERROR::OK;
}

CURVEFS_ERROR FileSystem::ReadDir(Context* ctx,
                                  Ino ino,
                                  std::shared_ptr<DirEntryList>* entries) {
    bool yes = dirCache_->Get(ino, entries);
    if (yes) {
        return CURVEFS_ERROR::OK;
    }

    CURVEFS_ERROR rc = rpc_->ReadDir(ino, entries);
    if (rc != CURVEFS_ERROR::OK) {
        return rc;
    }

    (*entries)->SetMtime(FindHandler(ctx->fi->fh)->mtime);
    dirCache_->Put(ino, *entries);
    return CURVEFS_ERROR::OK;
}

CURVEFS_ERROR FileSystem::ReleaseDir(Context* ctx, Ino ino) {
    ReleaseHandler(ctx->fi->fh);
    return CURVEFS_ERROR::OK;
}

CURVEFS_ERROR FileSystem::Open(Context* ctx, Ino ino) {
    std::shared_ptr<InodeWrapper> inode;
    bool yes = openFiles_->IsOpened(ino, &inode);
    if (yes) {
        openFiles_->Open(ino, inode);
        return fuseClient_->HandleOpenFlags(ctx, inode);
    }

    CURVEFS_ERROR rc = rpc_->Open(ino, &inode);
    if (rc != CURVEFS_ERROR::OK) {
        return rc;
    }

    TimeSpec mtime;
    yes = attrWatcher_->GetMtime(ino, &mtime);
    if (!yes) {
        // It is rare which only arise when attribute mtime evited
        return CURVEFS_ERROR::STALE;
    } else if(mtime != InodeMtime(inode)) {
        return CURVEFS_ERROR::STALE;
    }

    openFiles_->Open(ino, inode);
    return fuseClient_->HandleOpenFlags(ctx, inode);
}

CURVEFS_ERROR FileSystem::Release(Context* ctx, Ino ino) {
    openFiles_->Close(ino);
}

}  // namespace filesystem
}  // namespace client
}  // namespace curvefs
