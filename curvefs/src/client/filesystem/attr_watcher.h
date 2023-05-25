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

#ifndef CURVEFS_SRC_CLIENT_FILESYSTEM_ATTR_WATCHER_H_
#define CURVEFS_SRC_CLIENT_FILESYSTEM_ATTR_WATCHER_H_

#include <memory>

#include "src/common/lru_cache.h"
#include "curvefs/src/client/common/config.h"
#include "curvefs/src/client/filesystem/meta.h"
#include "curvefs/src/client/filesystem/openfile.h"

namespace curvefs {
namespace client {
namespace filesystem {

using ::curve::common::LRUCache;
using ::curve::common::RWLock;
using ::curve::common::ReadLockGuard;
using ::curve::common::WriteLockGuard;
using ::curvefs::client::common::AttrWatcherOption;

class AttrWatcher {
 public:
    using LRUType = LRUCache<Ino, struct TimeSpec>;

 public:
    AttrWatcher(AttrWatcherOption option,
                std::shared_ptr<OpenFiles> openFiles,
                std::shared_ptr<DirCache> dirCache);

    void RemeberMtime(const InodeAttr& attr);

    bool GetMtime(Ino ino, TimeSpec* time);

    void UpdateDirEntryAttr(Ino ino, const InodeAttr& attr);

    void UpdateDirEntryLength(Ino ino, const InodeAttr& open);

 private:
    friend class AttrWatcherGuard;

 private:
    RWLock rwlock_;
    std::shared_ptr<LRUType> modifiedAt_;
    std::shared_ptr<OpenFiles> openFiles_;
    std::shared_ptr<DirCache> dirCache_;
};


enum class ReplyType {
    ATTR,
    ONLY_LENGTH
};

/*
 * each attribute reply to kernel, the watcher will:
 *  before reply:
 *    1) set attibute length if the corresponding file is opened
 *  after reply:
 *    1) remeber attribute modified time.
 *    2) write back attribute to dir entry cache if |writeBack| is true,
 *       because the dir-entry attribute maybe stale.
 */
struct AttrWatcherGuard {
 public:
    AttrWatcherGuard(std::shared_ptr<AttrWatcher> watcher,
                     InodeAttr* attr,
                     ReplyType type,
                     bool writeBack);

    ~AttrWatcherGuard();

 private:
    std::shared_ptr<AttrWatcher> watcher;
    InodeAttr* attr;
    ReplyType type;
    bool writeBack;
};

}  // namespace filesystem
}  // namespace client
}  // namespace curvefs

#endif  // CURVEFS_SRC_CLIENT_FILESYSTEM_ATTR_WATCHER_H_
