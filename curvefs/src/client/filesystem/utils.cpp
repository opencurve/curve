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

#include "curvefs/src/client/filesystem/core.h"

namespace curvefs {
namespace client {
namespace filesystem {

HandlerManager::HandlerManager() {
    dirBuffer_ = std::make_shared<DirBuffer>();
}

std::shared_ptr<FileHandler> HandlerManager::NewHandler() {
    UniqueLock lk(mutex_);
    auto handler = std::make_shared<FileHandler>();
    handler->id = dirBuffer_->DirBufferNew();
    handler->buffer = dirBuffer_->DirBufferGet(handler->id);
    handlers_.emplace(handler->id, handler);
    return handler;
}

std::shared_ptr<FileHandler> HandlerManager::FindHandler(uint64_t id) {
    UniqueLock lk(mutex_);
    auto iter = handlers_.find(id);
    if (iter == handlers_.end()) {
        return nullptr;
    }
    return iter->second;
}

void HandlerManager::ReleaseHandler(uint64_t id) {
    UniqueLock lk(mutex_);
    dirBuffer_->DirBufferRelease(id);
    handlers_.erase(id);
}

inline bool IsDir(const InodeAttr& attr) {
    return attr.type() == FsFileType::TYPE_DIRECTORY;
}

inline bool IsS3File(const InodeAttr& attr) {
    return attr.type() == FsFileType::TYPE_S3;
}

inline bool IsVolmeFile(const InodeAttr& attr) {
    return attr.type() == FsFileType::TYPE_FILE;
}

inline bool IsSymLink(const InodeAttr& attr) {
    return attr.type() == FsFileType::TYPE_SYM_LINK;
}

inline TimeSpec AttrMtime(const InodeAttr& attr) {
    return TimeSpec(attr.mtime(), attr.mtime_ns());
}

inline TimeSpec InodeMtime(const std::shared_ptr<InodeWrapper> inode) {
    InodeAttr attr;
    inode->GetInodeAttr(&attr);
    return AttrMtime(attr);
}

}  // namespace filesystem
}  // namespace client
}  // namespace curvefs
