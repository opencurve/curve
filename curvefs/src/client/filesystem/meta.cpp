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
 * Created Date: 2023-03-06
 * Author: Jingli Chen (Wine93)
 */

#include <sys/stat.h>

#include "absl/strings/str_format.h"
#include "curvefs/src/client/filesystem/meta.h"

namespace curvefs {
namespace client {
namespace filesystem {

HandlerManager::HandlerManager()
    : mutex_(),
      dirBuffer_(std::make_shared<DirBuffer>()),
      handlers_() {}

HandlerManager::~HandlerManager() {
    dirBuffer_->DirBufferFreeAll();
}

std::shared_ptr<FileHandler> HandlerManager::NewHandler() {
    UniqueLock lk(mutex_);
    auto handler = std::make_shared<FileHandler>();
    handler->fh = dirBuffer_->DirBufferNew();
    handler->buffer = dirBuffer_->DirBufferGet(handler->fh);
    handler->padding = false;
    handlers_.emplace(handler->fh, handler);
    return handler;
}

std::shared_ptr<FileHandler> HandlerManager::FindHandler(uint64_t fh) {
    UniqueLock lk(mutex_);
    auto iter = handlers_.find(fh);
    if (iter == handlers_.end()) {
        return nullptr;
    }
    return iter->second;
}

void HandlerManager::ReleaseHandler(uint64_t fh) {
    UniqueLock lk(mutex_);
    dirBuffer_->DirBufferRelease(fh);
    handlers_.erase(fh);
}

std::string StrMode(uint16_t mode) {
    static std::map<uint16_t, char> type2char = {
        { S_IFSOCK, 's' },
        { S_IFLNK, 'l' },
        { S_IFREG, '-' },
        { S_IFBLK, 'b' },
        { S_IFDIR, 'd' },
        { S_IFCHR, 'c' },
        { S_IFIFO, 'f' },
        { 0, '?' },
    };

    std::string s("?rwxrwxrwx");
    s[0] = type2char[mode & (S_IFMT & 0xffff)];
    if (mode & S_ISUID) {
        s[3] = 's';
    }
    if (mode & S_ISGID) {
        s[6] = 's';
    }
    if (mode & S_ISVTX) {
        s[9] = 't';
    }

    for (auto i = 0; i < 9; i++) {
        if ((mode & (1 << i)) == 0) {
            if ((s[9-i] == 's') || (s[9-i] == 't')) {
                s[9-i] &= 0xDF;
            } else {
                s[9-i] = '-';
            }
        }
    }
    return s;
}

namespace {

std::string Attr2Str(const InodeAttr& attr) {
    if (!attr.IsInitialized()) {
        return "";
    }

    std::string smode;
    return absl::StrFormat(" (%d,[%s:0%06o,%d,%d,%d,%d,%d,%d,%d])",
        attr.inodeid(), StrMode(attr.mode()).c_str(), attr.mode(), attr.nlink(),
        attr.uid(), attr.gid(),
        attr.atime(), attr.mtime(), attr.ctime(),
        attr.length());
}

}  // namespace

std::string StrEntry(EntryOut entryOut) {
    return Attr2Str(entryOut.attr);
}

std::string StrAttr(AttrOut attrOut) {
    return Attr2Str(attrOut.attr);
}

std::string StrAttr(InodeAttr attr) {
    return Attr2Str(attr);
}

}  // namespace filesystem
}  // namespace client
}  // namespace curvefs
