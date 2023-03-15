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

#include "curvefs/src/client/filesystem/core.h"

namespace curvefs {
namespace client {
namespace filesystem {

class FileSystem {
 public:
    FileSystem(FileSystemOption option, std::shared_ptr<RPCClient> rpcClient);

    // fuse request
    CURVEFS_ERROR Lookup(Context* ctx,
                         Ino parent,
                         const std::string& name,
                         EntryOut* out);

    CURVEFS_ERROR GetAttr(Context* ctx, Ino ino, AttrOut* out);

    CURVEFS_ERROR OpenDir(Context* ctx, Ino ino);

    CURVEFS_ERROR ReadDir(Context* ctx,
                          Ino ino,
                          std::shared_ptr<DirEntryList>* entries);

    CURVEFS_ERROR ReleaseDir(Context* ctx, Ino ino);

    CURVEFS_ERROR Open(Context* ctx, Ino ino);

    CURVEFS_ERROR Release(Context* ctx, Ino ino);

    // fuse reply: we control all replies to vfs layer in same entrance.
    void ReplyError(const Context& ctx, CURVEFS_ERROR code);

    void ReplyEntry(const Context& ctx, EntryOut* entryOut);

    void ReplyAttr(const Context& ctx, AttrOut* attrOut);

    void AddDirEntry(const Context& ctx,
                     DirEntry* dirEntry);

    void AddDirEntryPlus(const Context& ctx,
                         DirEntry* dirEntry);

 private:
    // utility: convert to system type.
    int SysErr(CURVEFS_ERROR code);

    void Attr2Stat(AttrOut* attrOut, struct stat* stat);

    void Entry2Param(EntryOut* entryOut, fuse_entry_param* e);

    // utility: set entry/attribute timeout
    void SetEntryTimeout(EntryOut* entryOut);

    void SetAttrTimeout(AttrOut* attrOut);

    // utility: others
    std::shared_ptr<FileHandler> NewHandler();

    std::shared_ptr<FileHandler> FindHandler(uint64_t id);

    void ReleaseHandler(uint64_t id);

 private:
    FileSystemOption option_;
    std::shared_ptr<RPCClient> rpc_;
    std::shared_ptr<DirCache> dirCache_;
    std::shared_ptr<OpenFiles> openFiles_;
    std::shared_ptr<AttrWatcher> attrWatcher_;
    std::shared_ptr<HandlerManager> handlerManager_;
    // I know you might be wondering why i added |fuseClient| object
    // in filesystem, because the logic of client is SO CONFUSING that
    // we have to reuse some code to make sure the logic is correct.
    std::shared_ptr<FuseClient> fuseClient_;
};

}  // namespace filesystem
}  // namespace client
}  // namespace curvefs

#endif  // CURVEFS_SRC_CLIENT_FILESYSTEM_FILESYSTEM_H_
