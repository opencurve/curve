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

#include <utility>

#include "curvefs/src/client/filesystem/dir_cache.h"
#include "curvefs/src/client/filesystem/utils.h"

namespace curvefs {
namespace client {
namespace filesystem {

DirEntryList::DirEntryList()
    : rwlock_(),
      mtime_(),
      entries_(),
      index_() {}

size_t DirEntryList::Size() {
    ReadLockGuard lk(rwlock_);
    return entries_.size();
}

void DirEntryList::Add(const DirEntry& dirEntry) {
    WriteLockGuard lk(rwlock_);
    entries_.push_back(std::move(dirEntry));
    index_[dirEntry.ino] = entries_.size() - 1;
}

bool DirEntryList::Get(Ino ino, DirEntry* dirEntry) {
    ReadLockGuard lk(rwlock_);
    auto iter = index_.find(ino);
    if (iter == index_.end()) {
        return false;
    }

    *dirEntry = entries_[iter->second];
    return true;
}

bool DirEntryList::UpdateAttr(Ino ino, const InodeAttr& attr) {
    WriteLockGuard lk(rwlock_);
    auto iter = index_.find(ino);
    if (iter == index_.end()) {
        return false;
    }

    DirEntry* dirEntry = &entries_[iter->second];
    dirEntry->attr = std::move(attr);
    return true;
}

bool DirEntryList::UpdateLength(Ino ino, const InodeAttr& open) {
    WriteLockGuard lk(rwlock_);
    auto iter = index_.find(ino);
    if (iter == index_.end()) {
        return false;
    }

    DirEntry* dirEntry = &entries_[iter->second];
    InodeAttr* attr = &(dirEntry->attr);
    attr->set_length(open.length());
    attr->set_mtime(open.mtime());
    attr->set_mtime_ns(open.mtime_ns());
    if (AttrCtime(open) > AttrCtime(*attr)) {
        attr->set_ctime(open.ctime());
        attr->set_ctime_ns(open.ctime_ns());
    }
    return true;
}

void DirEntryList::Iterate(IterateHandler handler) {
    ReadLockGuard lk(rwlock_);
    for (auto iter = entries_.begin(); iter != entries_.end(); iter++) {
        handler(&(*iter));
    }
}

void DirEntryList::Clear() {
    WriteLockGuard lk(rwlock_);
    entries_.clear();
    index_.clear();
}

void DirEntryList::SetMtime(TimeSpec mtime) {
    WriteLockGuard lk(rwlock_);
    mtime_ = mtime;
}

TimeSpec DirEntryList::GetMtime() {
    ReadLockGuard lk(rwlock_);
    return mtime_;
}

DirCache::DirCache(DirCacheOption option)
    : rwlock_(),
      nentries_(0),
      option_(option) {
    lru_ = std::make_shared<LRUType>(0);  // control size by ourself
    mq_ = std::make_shared<MessageQueueType>("dircache", 10000);
    mq_->Subscribe([&](const std::shared_ptr<DirEntryList>& entries){
        entries->Clear();
    });
    metric_ = std::make_shared<DirCacheMetric>();

    LOG(INFO) << "Using directory lru cache, capacity = " << option_.lruSize;
}

void DirCache::Start() {
    mq_->Start();
}

void DirCache::Stop() {
    WriteLockGuard lk(rwlock_);
    Evit(option_.lruSize);
    mq_->Stop();
}

void DirCache::Delete(Ino parent,
                      std::shared_ptr<DirEntryList> entries,
                      bool evit) {
    size_t ndelete = entries->Size();
    nentries_ -=  ndelete;
    metric_->AddEntries(-static_cast<int64_t>(ndelete));
    mq_->Publish(entries);  // clear entries in background
    lru_->Remove(parent);

    VLOG(1) << "Delete directory cache (evit=" << evit << "): "
            << "parent = " << parent
            << ", mtime = " << entries->GetMtime()
            << ", delete size = " << ndelete
            << ", nentries = " << nentries_;
}

void DirCache::Evit(size_t size) {
    Ino parent;
    std::shared_ptr<DirEntryList> entries;
    while (nentries_ + size >= option_.lruSize) {
        bool yes = lru_->GetLast(&parent, &entries);
        if (!yes) {
            break;
        }
        Delete(parent, entries, true);
    }
}

void DirCache::Put(Ino parent, std::shared_ptr<DirEntryList> entries) {
    WriteLockGuard lk(rwlock_);
    if (entries->Size() == 0) {  // TODO(Wine93): cache it!
        return;
    }

    Evit(entries->Size());  // it guarantee put entries success
    lru_->Put(parent, entries);
    int64_t ninsert = entries->Size();
    nentries_ += ninsert;
    metric_->AddEntries(static_cast<int64_t>(ninsert));

    VLOG(1) << "Insert directory cache: parent = " << parent
            << ", mtime = " << entries->GetMtime()
            << ", insert size = " << ninsert
            << ", nentries = " << nentries_;
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
        Delete(parent, entries, false);
    }
}

}  // namespace filesystem
}  // namespace client
}  // namespace curvefs
