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

using ::curve::common::LockGuard;
using ::curve::common::ReadLockGuard;
using ::curve::common::WriteLockGuard;
using ::curvefs::client::filesystem::AttrCtime;

#define RETURN_FALSE_IF_CTO_ON() \
    do {                         \
        if (cto_) {              \
            return false;        \
        }                        \
    } while (0)

DeferInodes::DeferInodes(bool cto)
    : cto_(cto),
      rwlock_(),
      inodes_() {}

bool DeferInodes::Add(const std::shared_ptr<InodeWrapper>& inode) {
    RETURN_FALSE_IF_CTO_ON();
    WriteLockGuard lk(rwlock_);
    Ino ino = inode->GetInodeId();
    auto ret = inodes_.emplace(ino, inode);
    auto iter = ret.first;
    bool yes = ret.second;
    if (!yes) {  // already exists
        iter->second = inode;
    }
    return true;
}

bool DeferInodes::Get(Ino ino, std::shared_ptr<InodeWrapper>* inode) {
    RETURN_FALSE_IF_CTO_ON();
    ReadLockGuard lk(rwlock_);
    auto iter = inodes_.find(ino);
    if (iter == inodes_.end()) {
        return false;
    }
    *inode = iter->second;
    return true;
}

bool DeferInodes::Remove(const std::shared_ptr<InodeWrapper>& inode) {
    RETURN_FALSE_IF_CTO_ON();
    WriteLockGuard lk(rwlock_);
    InodeAttr attr;
    inode->GetInodeAttrLocked(&attr);
    auto iter = inodes_.find(attr.inodeid());
    if (iter == inodes_.end()) {
        return false;
    }

    InodeAttr defered;
    iter->second->GetInodeAttrLocked(&defered);
    if (AttrCtime(attr) < AttrCtime(defered)) {
        // it means the old defered inode already replaced by the lastest one,
        // so we can't remove it before it synced yet.
        return false;
    }
    inodes_.erase(iter);
    return true;
}

size_t DeferInodes::Size() {
    ReadLockGuard lk(rwlock_);
    return inodes_.size();
}

SyncInodeClosure::SyncInodeClosure(const std::shared_ptr<DeferInodes>& inodes,
                                   const std::shared_ptr<InodeWrapper>& inode)
    : inodes_(inodes), inode_(inode) {}

void SyncInodeClosure::Run() {
    std::unique_ptr<SyncInodeClosure> self_guard(this);
    MetaStatusCode rc = GetStatusCode();
    if (rc == MetaStatusCode::OK || rc == MetaStatusCode::NOT_FOUND) {
        inodes_->Remove(inode_);
    }
}

DeferSync::DeferSync(bool cto, DeferSyncOption option)
    : cto_(cto),
      option_(option),
      mutex_(),
      running_(false),
      thread_(),
      sleeper_(),
      pending_(),
      inodes_(std::make_shared<DeferInodes>(cto)) {}

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

SyncInodeClosure* DeferSync::NewSyncInodeClosure(
    const std::shared_ptr<InodeWrapper>& inode) {
    // NOTE: we only store the defer inodes in nocto scenario,
    // which means we don't need to remove the inode from defer inodes
    // even if the inode already synced done in cto scenario.
    if (cto_) {
        return nullptr;
    }
    return new SyncInodeClosure(inodes_, inode);
}

void DeferSync::SyncTask() {
    std::vector<std::shared_ptr<InodeWrapper>> syncing;
    for ( ;; ) {
        bool running = sleeper_.wait_for(std::chrono::seconds(option_.delay));

        {
            LockGuard lk(mutex_);
            syncing.swap(pending_);
        }
        for (const auto& inode : syncing) {
            auto closure = NewSyncInodeClosure(inode);
            UniqueLock lk(inode->GetUniqueLock());
            inode->Async(closure, true);
        }
        syncing.clear();

        if (!running) {
            break;
        }
    }
}

void DeferSync::Push(const std::shared_ptr<InodeWrapper>& inode) {
    LockGuard lk(mutex_);
    pending_.emplace_back(inode);
    inodes_->Add(inode);
}

bool DeferSync::IsDefered(Ino ino, std::shared_ptr<InodeWrapper>* inode) {
    return inodes_->Get(ino, inode);
}

}  // namespace filesystem
}  // namespace client
}  // namespace curvefs
