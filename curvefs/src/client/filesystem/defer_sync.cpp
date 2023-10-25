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

#include <vector>
#include <memory>

#include "curvefs/src/client/filesystem/defer_sync.h"
#include "curvefs/src/client/filesystem/utils.h"

namespace curvefs {
namespace client {
namespace filesystem {

DeferSync::DeferSync(DeferSyncOption option)
    : option_(option),
      mutex_(),
      running_(false),
      thread_(),
      sleeper_(),
      inodes_() {
}

void DeferSync::Start() {
    if (!running_.exchange(true)) {
        thread_ = std::thread(&DeferSync::SyncTask, this);
        LOG(INFO) << "Defer sync thread start success";
    }
}

void DeferSync::Stop() {
    if (running_.exchange(false)) {
        LOG(INFO) << "Stop defer sync thread...";
        sleeper_.interrupt();
        thread_.join();
        LOG(INFO) << "Defer sync thread stopped";
    }
}

void DeferSync::SyncTask() {
    std::vector<std::shared_ptr<InodeWrapper>> inodes;
    for ( ;; ) {
        bool running = sleeper_.wait_for(std::chrono::seconds(option_.delay));

        {
            LockGuard lk(mutex_);
            inodes.swap(inodes_);
        }
        for (const auto& inode : inodes) {
            UniqueLock lk(inode->GetUniqueLock());
            inode->Async(nullptr, true);
        }
        inodes.clear();

        if (!running) {
            break;
        }
    }
}

void DeferSync::Push(const std::shared_ptr<InodeWrapper>& inode) {
    LockGuard lk(mutex_);
    inodes_.emplace_back(inode);
}

bool DeferSync::IsDefered(Ino ino, InodeAttr* attr) {
    LockGuard lk(mutex_);
    for (const auto& inode : inodes_) {
        if (inode->GetInodeId() == ino) {
            inode->GetInodeAttr(attr);
            return true;
        }
    }
    return false;
}

}  // namespace filesystem
}  // namespace client
}  // namespace curvefs
