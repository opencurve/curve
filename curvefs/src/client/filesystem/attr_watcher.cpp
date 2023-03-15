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
 * Created Date: 2023-03-09
 * Author: Jingli Chen (Wine93)
 */

#include "curvefs/src/client/filesystem/core.h"

namespace curvefs {
namespace client {
namespace filesystem {

AttrWatcher::AttrWatcher(AttrWatcherOption option,
                         std::shared_ptr<OpenFiles> openFiles)
    : modifiedAt_(std::make_shared<LRUType>(option.lruSize)),
      openFiles_(openFiles) {}

void AttrWatcher::RemeberMtime(const InodeAttr& attr) {
    WriteLockGuard lk(rwlock_);
    modifiedAt_->Put(attr.inodeid(), AttrMtime(attr));
}

bool AttrWatcher::GetMtime(Ino ino, TimeSpec* time) {
    ReadLockGuard lk(rwlock_);
    return modifiedAt_->Get(ino, time);
}

AttrWatcherGuard::AttrWatcherGuard(std::shared_ptr<AttrWatcher> watcher,
                                   InodeAttr* attr)
    : watcher(watcher), attr(attr) {
    uint64_t length;
    Ino ino = attr->inodeid();
    bool yes = watcher->openFiles_->GetLength(ino, &length);
    if (yes) {
        attr->set_length(length);
    }
}

AttrWatcherGuard::~AttrWatcherGuard() {
    watcher->RemeberMtime(*attr);
}

}  // namespace filesystem
}  // namespace client
}  // namespace curvefs
