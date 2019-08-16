/*
 * Project: curve
 * Created Date: Thu Aug 08 2019
 * Author: xuchaojie
 * Copyright (c) 2019 netease
 */

#include "src/common/concurrent/name_lock.h"

#include <string>
#include <map>
#include <memory>

namespace curve {
namespace common {

NameLock::NameLock(int bucketNum) {
    locks_.reserve(bucketNum);
    for (int i = 0; i < bucketNum; i++) {
        locks_.push_back(std::make_shared<LockBucket>());
    }
}

void NameLock::Lock(const std::string &lockStr) {
    LockEntryPtr entry = NULL;

    int bucketOffset = GetBucketOffset(lockStr);
    LockBucketPtr lockBucket = locks_[bucketOffset];

    {
        LockGuard guard(lockBucket->mu);
        auto it = lockBucket->lockMap.find(lockStr);
        if (it == lockBucket->lockMap.end()) {
            entry = std::make_shared<LockEntry>();
            entry->ref_.store(1);
            lockBucket->lockMap.emplace(lockStr, entry);
        } else {
            entry = it->second;
            entry->ref_.fetch_add(1);
        }
    }
    entry->lock_.lock();
}

bool NameLock::TryLock(const std::string &lockStr) {
    LockEntryPtr entry = NULL;

    int bucketOffset = GetBucketOffset(lockStr);
    LockBucketPtr lockBucket = locks_[bucketOffset];

    {
        LockGuard guard(lockBucket->mu);
        auto it = lockBucket->lockMap.find(lockStr);
        if (it == lockBucket->lockMap.end()) {
            entry = std::make_shared<LockEntry>();
            entry->ref_.store(1);
            lockBucket->lockMap.emplace(lockStr, entry);
        } else {
            entry = it->second;
            entry->ref_.fetch_add(1);
        }
    }
    if (entry->lock_.try_lock()) {
        return true;
    } else {
        LockGuard guard(lockBucket->mu);
        if (entry->ref_.fetch_sub(1) == 1) {
            lockBucket->lockMap.erase(lockStr);
        }
        return false;
    }
}

void NameLock::Unlock(const std::string &lockStr) {
    int bucketOffset = GetBucketOffset(lockStr);
    LockBucketPtr lockBucket = locks_[bucketOffset];

    LockGuard guard(lockBucket->mu);
    auto it = lockBucket->lockMap.find(lockStr);
    if (it != lockBucket->lockMap.end()) {
        LockEntryPtr entry = it->second;
        entry->lock_.unlock();
        if (entry->ref_.fetch_sub(1) == 1) {
            lockBucket->lockMap.erase(it);
        }
    }
}

int NameLock::GetBucketOffset(const std::string &lockStr) {
    std::hash<std::string> hash_fn;
    return hash_fn(lockStr) % locks_.size();
}


}   // namespace common
}   // namespace curve
