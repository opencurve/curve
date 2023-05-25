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

#include "curvefs/src/client/filesystem/utils.h"
#include "curvefs/src/client/filesystem/attr_watcher.h"

namespace curvefs {
namespace client {
namespace filesystem {

AttrWatcher::AttrWatcher(AttrWatcherOption option,
                         std::shared_ptr<OpenFiles> openFiles,
                         std::shared_ptr<DirCache> dirCache)
    : modifiedAt_(std::make_shared<LRUType>(option.lruSize)),
      openFiles_(openFiles),
      dirCache_(dirCache) {}

void AttrWatcher::RemeberMtime(const InodeAttr& attr) {
    WriteLockGuard lk(rwlock_);
    modifiedAt_->Put(attr.inodeid(), AttrMtime(attr));
}

bool AttrWatcher::GetMtime(Ino ino, TimeSpec* time) {
    ReadLockGuard lk(rwlock_);
    return modifiedAt_->Get(ino, time);
}

void AttrWatcher::UpdateDirEntryAttr(Ino ino, const InodeAttr& attr) {
    std::shared_ptr<DirEntryList> entries;
    for (const auto parent : attr.parent()) {
        bool yes = dirCache_->Get(parent, &entries);
        if (!yes) {
            continue;
        }

        entries->UpdateAttr(ino, attr);

        VLOG(1) << "Write back attribute to dir entry cache: ino = " << ino
                << ", attr = " << attr.ShortDebugString();
    }
}

void AttrWatcher::UpdateDirEntryLength(Ino ino, const InodeAttr& open) {
    std::shared_ptr<DirEntryList> entries;
    for (const auto parent : open.parent()) {
        bool yes = dirCache_->Get(parent, &entries);
        if (!yes) {
            continue;
        }

        entries->UpdateLength(ino, open);

        VLOG(1) << "Write back file length to dir entry cache: ino = " << ino
                << ", attr = " << open.ShortDebugString();
    }
}

AttrWatcherGuard::AttrWatcherGuard(std::shared_ptr<AttrWatcher> watcher,
                                   InodeAttr* attr,
                                   ReplyType type,
                                   bool writeBack)
    : watcher(watcher), attr(attr), type(type), writeBack(writeBack) {
    InodeAttr open;
    Ino ino = attr->inodeid();
    bool yes = watcher->openFiles_->GetFileAttr(ino, &open);
    if (!yes) {
        return;
    }

    attr->set_length(open.length());
    attr->set_mtime(open.mtime());
    attr->set_mtime_ns(open.mtime_ns());
    if (AttrCtime(open) > AttrCtime(*attr)) {
        attr->set_ctime(open.ctime());
        attr->set_ctime_ns(open.ctime_ns());
    }
}

AttrWatcherGuard::~AttrWatcherGuard() {
    switch (type) {
        case ReplyType::ATTR:
            watcher->RemeberMtime(*attr);
            if (writeBack) {
                watcher->UpdateDirEntryAttr(attr->inodeid(), *attr);
            }
            break;

        case ReplyType::ONLY_LENGTH:
            if (writeBack) {
                watcher->UpdateDirEntryLength(attr->inodeid(), *attr);
            }
            break;
    }
}

}  // namespace filesystem
}  // namespace client
}  // namespace curvefs
