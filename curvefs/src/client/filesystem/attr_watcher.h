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

#include "curvefs/src/client/filesystem/core.h"

namespace curvefs {
namespace client {
namespace filesystem {

class AttrWatcher {
 public:
    using LRUType = LRUCache<Ino, struct TimeSpec>;

 public:
    AttrWatcher(AttrWatcherOption option, std::shared_ptr<OpenFiles> openFiles);

    void RemeberMtime(const InodeAttr& attr);

    bool GetMtime(Ino ino, TimeSpec* time);

 private:
    friend class AttrWatcherGuard;

 private:
    RWLock rwlock_;
    std::shared_ptr<LRUType> modifiedAt_;
    std::shared_ptr<OpenFiles> openFiles_;
};

/*
 * each attribute reply to kernel, the watcher will:
 *   1) set attibute length if the corresponding file is opened
 *   2) remeber attribute modified time
 */
struct AttrWatcherGuard {
 public:
    AttrWatcherGuard(std::shared_ptr<AttrWatcher> watcher,
                     InodeAttr* attr);

    ~AttrWatcherGuard();

 private:
    std::shared_ptr<AttrWatcher> watcher;
    InodeAttr* attr;
};

}  // namespace filesystem
}  // namespace client
}  // namespace curvefs

#endif  // CURVEFS_SRC_CLIENT_FILESYSTEM_ATTR_WATCHER_H_
