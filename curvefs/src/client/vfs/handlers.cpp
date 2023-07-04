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
 * Created Date: 2023-07-04
 * Author: Jingli Chen (Wine93)
 */

#include "curvefs/src/client/vfs/handlers.h"

namespace curvefs {
namespace client {
namespace vfs {

FileHandlers::FileHandlers()
    : nextHandler_(0),
      handlers_() {}

uint64_t FileHandlers::NextHandler(Ino ino) {
    LockGuard lk(mutex_);
    handlers_[nextHandler_] = std::make_shared<FileHandler>(ino);
    return nextHandler_++;
}

bool FileHandlers::GetHandler(uint64_t fd,
                              std::shared_ptr<FileHandler>* handler) {
    LockGuard lk(mutex_);
    auto iter = handlers_.find(fd);
    if (iter != handlers_.end()) {
        *handler = iter->second;
        return true;
    }
    return false;
}

void FileHandlers::FreeHandler(uint64_t fd) {
    LockGuard lk(mutex_);
    auto iter = handlers_.find(fd);
    if (iter != handlers_.end()) {
        handlers_.erase(iter);
    }
}

}  // namespace vfs
}  // namespace client
}  // namespace curvefs
