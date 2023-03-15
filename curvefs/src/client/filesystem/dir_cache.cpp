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
 * Created Date: 2023-03-07
 * Author: Jingli Chen (Wine93)
 */

#include "curvefs/src/client/filesystem/core.h"

namespace curvefs {
namespace client {
namespace filesystem {

DirEntryList::DirEntryList(TimeSpec mtime)
    : rwlock_(),
      mtime_(mtime),
      entries_(),
      attrs_() { }

size_t DirEntryList::Size() {
    ReadLockGuard lk(rwlock_);
    return entries_.size();
}

void DirEntryList::Add(const DirEntry& dirEntry) {
    WriteLockGuard lk(rwlock_);
    entries_.push_back(std::move(dirEntry));
    attrs_[dirEntry.ino] = &entries_.back();
}

bool DirEntryList::Get(Ino ino, DirEntry* dirEntry) {
    ReadLockGuard lk(rwlock_);
    auto iter = attrs_.find(ino);
    if (iter == attrs_.end()) {
        return false;
    }
    dirEntry = iter->second;
    return true;
}

void DirEntryList::Iterate(IterateHandler handler) {
    ReadLockGuard lk(rwlock_);
    for (auto iter = entries_.begin(); iter != entries_.end(); iter++) {
        handler(*iter);
    }
}

void DirEntryList::Clear() {
    WriteLockGuard lk(rwlock_);
    entries_.clear();
    attrs_.clear();
}

inline void DirEntryList::SetMtime(TimeSpec mtime) {
    WriteLockGuard lk(rwlock_);
    mtime_ = mtime;
}

inline TimeSpec DirEntryList::GetMtime() {
    ReadLockGuard lk(rwlock_);
    return mtime_;
}

DirCache::DirCache(DirCacheOption option)
    : rwlock_(),
      nentries_(0),
      option_(option) {
    lru_ = std::make_shared<LRUType>(0);  // control size by ourself
    mq_ = std::make_shared<MessageQueueType>("dircache", 10000);
    mq_->Sub([&](const std::shared_ptr<DirEntryList>& entries){
        entries->Clear();
    });
    mq_->Run();
}

void DirCache::Delete(Ino parent, std::shared_ptr<DirEntryList> entries) {
    nentries_ -= entries->Size();
    mq_->Pub(entries);
    lru_->Remove(parent);
}

void DirCache::Evit(size_t size) {
    Ino parent;
    std::shared_ptr<DirEntryList> entries;
    while (nentries_ + size >= option_.lruSize) {
        bool yes = lru_->GetLast(&parent, &entries);
        if (!yes) {
            break;
        }
        Delete(parent, entries);
    }
}

void DirCache::Put(Ino parent, std::shared_ptr<DirEntryList> entries) {
    WriteLockGuard lk(rwlock_);
    Evit(entries->Size());
    lru_->Put(parent, entries);
}

bool DirCache::Get(Ino parent, std::shared_ptr<DirEntryList>* entries) {
    ReadLockGuard lk(rwlock_);
    return lru_->Get(parent, entries);
}

void DirCache::Drop(Ino parent) {
    WriteLockGuard lk(rwlock_);
    std::shared_ptr<DirEntryList> entries;
    bool yes = lru_->Get(parent, &entries);
    if (yes) {
        Delete(parent, entries);
    }
}

}  // namespace filesystem
}  // namespace client
}  // namespace curvefs
