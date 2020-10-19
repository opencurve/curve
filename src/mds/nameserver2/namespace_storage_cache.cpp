/*
 *  Copyright (c) 2020 NetEase Inc.
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
 * Project: curve
 * Created Date: Thur Apr 16th 2019
 * Author: lixiaocui
 */

#include <glog/logging.h>
#include "src/mds/nameserver2/namespace_storage_cache.h"

namespace curve {
namespace mds {
void LRUCache::Put(const std::string &key, const std::string &value) {
    ::curve::common::WriteLockGuard guard(lock_);
    PutLocked(key, value);
}

bool LRUCache::Get(const std::string &key, std::string *value) {
    ::curve::common::WriteLockGuard guard(lock_);
    auto iter = cache_.find(key);
    if (iter == cache_.end()) {
        cacheMetrics_->OnCacheMiss();
        return false;
    }

    cacheMetrics_->OnCacheHit();

    // update the position of the target item in the list
    MoveToFront(iter->second);
    *value = cache_[key]->value;
    return true;
}

void LRUCache::Remove(const std::string &key) {
    ::curve::common::WriteLockGuard guard(lock_);
    RemoveLocked(key);
}

std::shared_ptr<NameserverCacheMetrics> LRUCache::GetCacheMetrics() const {
    return  cacheMetrics_;
}

void LRUCache::PutLocked(const std::string &key, const std::string &value) {
    auto iter = cache_.find(key);

    // delete the old value if already exist
    if (iter != cache_.end()) {
        RemoveElement(iter->second);
    }

    // put new value
    Item kv{key, value};
    ll_.push_front(kv);
    cache_[key] = ll_.begin();
    cacheMetrics_->UpdateAddToCacheCount();
    cacheMetrics_->UpdateAddToCacheBytes(key.size() + value.size());
    if (maxCount_ != 0 && ll_.size() > maxCount_) {
        RemoveOldest();
    }
}

void LRUCache::RemoveLocked(const std::string &key) {
    auto iter = cache_.find(key);
    if (iter != cache_.end()) {
        RemoveElement(iter->second);
    }
}

void LRUCache::MoveToFront(const std::list<Item>::iterator &elem) {
    Item duplica{elem->key, elem->value};
    ll_.erase(elem);
    ll_.push_front(duplica);
    cache_[duplica.key] = ll_.begin();
}

void LRUCache::RemoveOldest() {
    if (ll_.begin() != ll_.end()) {
        RemoveElement(--ll_.end());
    }
}

void LRUCache::RemoveElement(const std::list<Item>::iterator &elem) {
    cacheMetrics_->UpdateRemoveFromCacheCount();
    cacheMetrics_->UpdateRemoveFromCacheBytes(
        elem->key.size() + elem->value.size());

    auto iter = cache_.find(elem->key);
    cache_.erase(iter);
    ll_.erase(elem);
}

}  // namespace mds
}  // namespace curve
