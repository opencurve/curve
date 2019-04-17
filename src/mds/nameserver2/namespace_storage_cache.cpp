/*
 * Project: curve
 * Created Date: Thur Apr 16th 2019
 * Author: lixiaocui
 * Copyright (c) 2018 netease
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
        return false;
    }

    // 更新元素在列表中的位置
    MoveToFront(iter->second);
    *value = cache_[key]->value;
    return true;
}

void LRUCache::Remove(const std::string &key) {
    ::curve::common::WriteLockGuard guard(lock_);
    RemoveLocked(key);
}

void LRUCache::PutLocked(const std::string &key, const std::string &value) {
    auto iter = cache_.find(key);

    // 如果已存在，删除旧值
    if (iter != cache_.end()) {
        RemoveElement(iter->second);
    }

    // put新值
    Item kv{key, value};
    ll_.push_front(kv);
    cache_[key] = ll_.begin();
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
    ll_.erase(elem);
    ll_.push_front(*elem);
    cache_[elem->key] = ll_.begin();
}

void LRUCache::RemoveOldest() {
    if (ll_.begin() != ll_.end()) {
        RemoveElement(--ll_.end());
    }
}

void LRUCache::RemoveElement(const std::list<Item>::iterator &elem) {
    ll_.erase(elem);
    auto iter = cache_.find(elem->key);
    cache_.erase(iter);
}

}  // namespace mds
}  // namespace curve


