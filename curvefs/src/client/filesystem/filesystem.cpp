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

#include "curvefs/src/client/filesystem/filesystem.h"
#include "curvefs/src/client/filesystem/utils.h"

namespace curvefs {
namespace client {
namespace filesystem {

FileSystem::FileSystem(FileSystemOption option, ExternalMember member)
    : option_(option), member(member) {
    deferSync_ = std::make_shared<DeferSync>(option.deferSyncOption);
    negative_ = std::make_shared<LookupCache>(option.lookupCacheOption);
    dirCache_ = std::make_shared<DirCache>(option.dirCacheOption);
    openFiles_ = std::make_shared<OpenFiles>(option_.openFilesOption,
                                             deferSync_);
    attrWatcher_ = std::make_shared<AttrWatcher>(option_.attrWatcherOption,
                                                 openFiles_, dirCache_);
    handlerManager_ = std::make_shared<HandlerManager>();
    rpc_ = std::make_shared<RPCClient>(option.rpcOption, member);
}

void FileSystem::Run() {
    deferSync_->Start();
    dirCache_->Start();
}

void FileSystem::Destory() {
    openFiles_->CloseAll();
    deferSync_->Stop();
    dirCache_->Stop();
}

void FileSystem::Attr2Stat(InodeAttr* attr, struct stat* stat) {
    std::memset(stat, 0, sizeof(struct stat));
    stat->st_ino = attr->inodeid();  //  inode number
    stat->st_mode = attr->mode();  // permission mode
    stat->st_nlink = attr->nlink();  // number of links
    stat->st_uid = attr->uid();  // user ID of owner
    stat->st_gid = attr->gid();  // group ID of owner
    stat->st_size = attr->length();  // total size, in bytes
    stat->st_rdev = attr->rdev();  // device ID (if special file)
    stat->st_atim.tv_sec = attr->atime();  // time of last access
    stat->st_atim.tv_nsec = attr->atime_ns();
    stat->st_mtim.tv_sec = attr->mtime();  // time of last modification
    stat->st_mtim.tv_nsec = attr->mtime_ns();
    stat->st_ctim.tv_sec = attr->ctime();  // time of last status change
    stat->st_ctim.tv_nsec = attr->ctime_ns();
    stat->st_blksize = option_.blockSize;  // blocksize for file system I/O
    stat->st_blocks = 0;  // number of 512B blocks allocated
    if (IsS3File(*attr)) {
        stat->st_blocks = (attr->length() + 511) / 512;
    }
}

void FileSystem::Entry2Param(EntryOut* entryOut,
                             fuse_entry_param* e) {
    std::memset(e, 0, sizeof(fuse_entry_param));
    e->ino = entryOut->attr.inodeid();
    e->generation = 0;
    Attr2Stat(&entryOut->attr, &e->attr);
    e->entry_timeout = entryOut->entryTimeout;
    e->attr_timeout = entryOut->attrTimeout;
}

void FileSystem::SetEntryTimeout(EntryOut* entryOut) {
    auto option = option_.kernelCacheOption;
    if (IsDir(entryOut->attr)) {
        entryOut->entryTimeout = option.dirEntryTimeoutSec;
        entryOut->attrTimeout = option.dirAttrTimeoutSec;
    } else {
        entryOut->entryTimeout = option.entryTimeoutSec;
        entryOut->attrTimeout = option.attrTimeoutSec;
    }
}

void FileSystem::SetAttrTimeout(AttrOut* attrOut) {
    auto option = option_.kernelCacheOption;
    if (IsDir(attrOut->attr)) {
        attrOut->attrTimeout = option.dirAttrTimeoutSec;
    } else {
        attrOut->attrTimeout = option.attrTimeoutSec;
    }
}

// fuse reply*
void FileSystem::ReplyError(Request req, CURVEFS_ERROR code) {
    fuse_reply_err(req, SysErr(code));
}

void FileSystem::ReplyEntry(Request req,
                            EntryOut* entryOut) {
    AttrWatcherGuard watcher(attrWatcher_, &entryOut->attr,
                             ReplyType::ATTR, true);
    fuse_entry_param e;
    SetEntryTimeout(entryOut);
    Entry2Param(entryOut, &e);
    fuse_reply_entry(req, &e);
}

void FileSystem::ReplyAttr(Request req,
                           AttrOut* attrOut) {
    AttrWatcherGuard watcher(attrWatcher_, &attrOut->attr,
                             ReplyType::ATTR, true);
    struct stat stat;
    SetAttrTimeout(attrOut);
    Attr2Stat(&attrOut->attr, &stat);
    fuse_reply_attr(req, &stat, attrOut->attrTimeout);
}

void FileSystem::ReplyReadlink(Request req, const std::string& link) {
    fuse_reply_readlink(req, link.c_str());
}

void FileSystem::ReplyOpen(Request req, FileInfo* fi) {
    fuse_reply_open(req, fi);
}

void FileSystem::ReplyOpen(Request req, FileOut* fileOut) {
    AttrWatcherGuard watcher(attrWatcher_, &fileOut->attr,
                             ReplyType::ONLY_LENGTH, true);
    fuse_reply_open(req, fileOut->fi);
}

void FileSystem::ReplyData(Request req,
                           struct fuse_bufvec *bufv,
                           enum fuse_buf_copy_flags flags) {
    fuse_reply_data(req, bufv, flags);
}

void FileSystem::ReplyWrite(Request req, FileOut* fileOut) {
    AttrWatcherGuard watcher(attrWatcher_, &fileOut->attr,
                             ReplyType::ONLY_LENGTH, true);
    fuse_reply_write(req, fileOut->nwritten);
}

void FileSystem::ReplyBuffer(Request req, const char *buf, size_t size) {
    fuse_reply_buf(req, buf, size);
}

void FileSystem::ReplyStatfs(Request req, const struct statvfs *stbuf) {
    fuse_reply_statfs(req, stbuf);
}

void FileSystem::ReplyXattr(Request req, size_t size) {
    fuse_reply_xattr(req, size);
}

void FileSystem::ReplyCreate(Request req, EntryOut* entryOut, FileInfo* fi) {
    AttrWatcherGuard watcher(attrWatcher_, &entryOut->attr,
                             ReplyType::ATTR, true);
    fuse_entry_param e;
    SetEntryTimeout(entryOut);
    Entry2Param(entryOut, &e);
    fuse_reply_create(req, &e, fi);
}

void FileSystem::AddDirEntry(Request req,
                             DirBufferHead* buffer,
                             DirEntry* dirEntry) {
    struct stat stat;
    std::memset(&stat, 0, sizeof(stat));
    stat.st_ino = dirEntry->ino;

    // add a directory entry to the buffer
    size_t oldsize = buffer->size;
    const char* name = dirEntry->name.c_str();
    buffer->size += fuse_add_direntry(req, NULL, 0, name, NULL, 0);
    buffer->p = static_cast<char *>(realloc(buffer->p, buffer->size));
    fuse_add_direntry(req,
                      buffer->p + oldsize,  // char* buf
                      buffer->size - oldsize,  // size_t bufisze
                      name, &stat, buffer->size);
}

void FileSystem::AddDirEntryPlus(Request req,
                                 DirBufferHead* buffer,
                                 DirEntry* dirEntry) {
    AttrWatcherGuard watcher(attrWatcher_, &dirEntry->attr,
                             ReplyType::ATTR, false);
    struct fuse_entry_param e;
    EntryOut entryOut(dirEntry->attr);
    SetEntryTimeout(&entryOut);
    Entry2Param(&entryOut, &e);

    // add a directory entry to the buffer with the attributes
    size_t oldsize = buffer->size;
    const char* name = dirEntry->name.c_str();
    buffer->size += fuse_add_direntry_plus(req, NULL, 0, name, NULL, 0);
    buffer->p = static_cast<char *>(realloc(buffer->p, buffer->size));
    fuse_add_direntry_plus(req,
                           buffer->p + oldsize,  // char* buf
                           buffer->size - oldsize,  // size_t bufisze
                           name, &e, buffer->size);
}

// handler*
std::shared_ptr<FileHandler> FileSystem::NewHandler() {
    return handlerManager_->NewHandler();
}

std::shared_ptr<FileHandler> FileSystem::FindHandler(uint64_t fh) {
    return handlerManager_->FindHandler(fh);
}

void FileSystem::ReleaseHandler(uint64_t fh) {
    return handlerManager_->ReleaseHandler(fh);
}

FileSystemMember FileSystem::BorrowMember() {
    return FileSystemMember(deferSync_, openFiles_, attrWatcher_);
}

// fuse request*
CURVEFS_ERROR FileSystem::Lookup(Ino parent,
                                 const std::string& name,
                                 EntryOut* entryOut) {
    if (name.size() > option_.maxNameLength) {
        return CURVEFS_ERROR::NAME_TOO_LONG;
    }

    bool yes = negative_->Get(parent, name);
    if (yes) {
        return CURVEFS_ERROR::NOT_EXIST;
    }

    auto rc = rpc_->Lookup(parent, name, entryOut);
    if (rc == CURVEFS_ERROR::OK) {
        negative_->Delete(parent, name);
    } else if (rc == CURVEFS_ERROR::NOT_EXIST) {
        negative_->Put(parent, name);
    }
    return rc;
}

CURVEFS_ERROR FileSystem::GetAttr(Ino ino, AttrOut* attrOut) {
    InodeAttr attr;
    auto rc = rpc_->GetAttr(ino, &attr);
    if (rc == CURVEFS_ERROR::OK) {
        *attrOut = AttrOut(attr);
    }
    return rc;
}

CURVEFS_ERROR FileSystem::OpenDir(Ino ino, FileInfo* fi) {
    InodeAttr attr;
    CURVEFS_ERROR rc = rpc_->GetAttr(ino, &attr);
    if (rc != CURVEFS_ERROR::OK) {
        return rc;
    }

    // revalidate directory cache
    std::shared_ptr<DirEntryList> entries;
    bool yes = dirCache_->Get(ino, &entries);
    if (yes) {
        if (entries->GetMtime() != AttrMtime(attr)) {
            dirCache_->Drop(ino);
        }
    }

    auto handler = NewHandler();
    handler->mtime = AttrMtime(attr);
    fi->fh = handler->fh;
    return CURVEFS_ERROR::OK;
}

CURVEFS_ERROR FileSystem::ReadDir(Ino ino,
                                  FileInfo* fi,
                                  std::shared_ptr<DirEntryList>* entries) {
    bool yes = dirCache_->Get(ino, entries);
    if (yes) {
        return CURVEFS_ERROR::OK;
    }

    CURVEFS_ERROR rc = rpc_->ReadDir(ino, entries);
    if (rc != CURVEFS_ERROR::OK) {
        return rc;
    }

    (*entries)->SetMtime(FindHandler(fi->fh)->mtime);
    dirCache_->Put(ino, *entries);
    return CURVEFS_ERROR::OK;
}

CURVEFS_ERROR FileSystem::ReleaseDir(Ino ino, FileInfo* fi) {
    ReleaseHandler(fi->fh);
    return CURVEFS_ERROR::OK;
}

CURVEFS_ERROR FileSystem::Open(Ino ino, FileInfo* fi) {
    std::shared_ptr<InodeWrapper> inode;
    bool yes = openFiles_->IsOpened(ino, &inode);
    if (yes) {
        openFiles_->Open(ino, inode);
        // fi->keep_cache = 1;
        return CURVEFS_ERROR::OK;
    }

    CURVEFS_ERROR rc = rpc_->Open(ino, &inode);
    if (rc != CURVEFS_ERROR::OK) {
        return rc;
    }

    /*
    TimeSpec mtime;
    yes = attrWatcher_->GetMtime(ino, &mtime);
    if (!yes) {
        // It is rare which only arise when attribute evited for attr-watcher.
        LOG(WARNING) << "open(" << ino << "): stale file handler"
                     << ": attribute not found in wacther";
        return CURVEFS_ERROR::STALE;
    } else if (mtime != InodeMtime(inode)) {
        LOG(WARNING) << "open(" << ino << "): stale file handler"
                     << ", cache(" << mtime << ") vs remote("
                     << InodeMtime(inode) << ")";
        return CURVEFS_ERROR::STALE;
    }
    */

    openFiles_->Open(ino, inode);
    return CURVEFS_ERROR::OK;
}

CURVEFS_ERROR FileSystem::Release(Ino ino) {
    openFiles_->Close(ino);
    return CURVEFS_ERROR::OK;
}

}  // namespace filesystem
}  // namespace client
}  // namespace curvefs
