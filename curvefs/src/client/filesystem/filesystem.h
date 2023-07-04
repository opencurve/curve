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

#ifndef CURVEFS_SRC_CLIENT_FILESYSTEM_FILESYSTEM_H_
#define CURVEFS_SRC_CLIENT_FILESYSTEM_FILESYSTEM_H_

#include <gtest/gtest_prod.h>

#include <memory>
#include <string>

#include "curvefs/src/client/filesystem/error.h"
#include "curvefs/src/client/common/config.h"
#include "curvefs/src/client/filesystem/package.h"
#include "curvefs/src/client/filesystem/meta.h"
#include "curvefs/src/client/filesystem/lookup_cache.h"
#include "curvefs/src/client/filesystem/dir_cache.h"
#include "curvefs/src/client/filesystem/openfile.h"
#include "curvefs/src/client/filesystem/attr_watcher.h"
#include "curvefs/src/client/filesystem/rpc_client.h"
#include "curvefs/src/client/filesystem/defer_sync.h"

namespace curvefs {
namespace client {
namespace filesystem {

using ::curvefs::client::common::FileSystemOption;

struct FileSystemMember {
    FileSystemMember(std::shared_ptr<DeferSync> deferSync,
                     std::shared_ptr<OpenFiles> openFiles,
                     std::shared_ptr<AttrWatcher> attrWatcher)
        : deferSync(deferSync),
          openFiles(openFiles),
          attrWatcher(attrWatcher) {}

    std::shared_ptr<DeferSync> deferSync;
    std::shared_ptr<OpenFiles> openFiles;
    std::shared_ptr<AttrWatcher> attrWatcher;
};

class FileSystem {
 public:
    FileSystem(struct FileSystemOption option, ExternalMember member);

    void Run();

    void Destory();

    // fuse request
    CURVEFS_ERROR Lookup(Ino parent,
                         const std::string& name,
                         EntryOut* entryOut);

    CURVEFS_ERROR GetAttr(Ino ino, AttrOut* attrOut);

    CURVEFS_ERROR OpenDir(Ino ino, FileInfo* fi);

    CURVEFS_ERROR ReadDir(Ino ino,
                          FileInfo* fi,
                          std::shared_ptr<DirEntryList>* entries);

    CURVEFS_ERROR ReleaseDir(Ino ino, FileInfo* fi);

    CURVEFS_ERROR Open(Ino ino, FileInfo* fi);

    CURVEFS_ERROR Release(Ino ino);

    CURVEFS_ERROR GetXAttr(Ino ino,
                           const std::string& key,
                           size_t size,
                           std::string* value);

    CURVEFS_ERROR SetXAttr(Ino ino,
                           const std::string& key,
                           const std::string& value,
                           int flags);

    // fuse reply: we control all replies to vfs layer in same entrance.
    void ReplyError(Request req, CURVEFS_ERROR code);

    void ReplyEntry(Request req, EntryOut* entryOut);

    void ReplyAttr(Request req, AttrOut* attrOut);

    void ReplyReadlink(Request req, const std::string& link);

    void ReplyOpen(Request req, FileInfo *fi);

    void ReplyOpen(Request req, FileOut* fileOut);

    void ReplyData(Request req,
                   struct fuse_bufvec *bufv,
                   enum fuse_buf_copy_flags flags);

    void ReplyWrite(Request req, FileOut* fileOut);

    void ReplyBuffer(Request req, const char *buf, size_t size);

    void ReplyStatfs(Request req, const struct statvfs *stbuf);

    void ReplyXattr(Request req, size_t size);

    void ReplyCreate(Request req, EntryOut* entryOut, FileInfo* fi);

    void AddDirEntry(Request req,
                     DirBufferHead* buffer,
                     DirEntry* dirEntry);

    void AddDirEntryPlus(Request req,
                         DirBufferHead* buffer,
                         DirEntry* dirEntry);

    // utility: file handler
    std::shared_ptr<FileHandler> NewHandler();

    std::shared_ptr<FileHandler> FindHandler(uint64_t fh);

    void ReleaseHandler(uint64_t fh);

    // utility: others
    FileSystemMember BorrowMember();

 private:
    FRIEND_TEST(FileSystemTest, Attr2Stat);
    FRIEND_TEST(FileSystemTest, Entry2Param);
    FRIEND_TEST(FileSystemTest, SetEntryTimeout);
    FRIEND_TEST(FileSystemTest, SetAttrTimeout);

    // utility: convert to system type.
    void Attr2Stat(InodeAttr* attr, struct stat* stat);

    void Entry2Param(EntryOut* entryOut, fuse_entry_param* e);

    // utility: set entry/attribute timeout
    void SetEntryTimeout(EntryOut* entryOut);

    void SetAttrTimeout(AttrOut* attrOut);

 private:
    FileSystemOption option_;
    ExternalMember member;
    std::shared_ptr<DeferSync> deferSync_;
    std::shared_ptr<LookupCache> negative_;
    std::shared_ptr<DirCache> dirCache_;
    std::shared_ptr<OpenFiles> openFiles_;
    std::shared_ptr<AttrWatcher> attrWatcher_;
    std::shared_ptr<HandlerManager> handlerManager_;
    std::shared_ptr<RPCClient> rpc_;
};

}  // namespace filesystem
}  // namespace client
}  // namespace curvefs

#endif  // CURVEFS_SRC_CLIENT_FILESYSTEM_FILESYSTEM_H_
