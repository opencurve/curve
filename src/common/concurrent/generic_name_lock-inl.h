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
 * Created Date: Thu Aug 08 2019
 * Author: xuchaojie
 */

#ifndef SRC_COMMON_CONCURRENT_GENERIC_NAME_LOCK_INL_H_
#define SRC_COMMON_CONCURRENT_GENERIC_NAME_LOCK_INL_H_

#include <string>
#include <map>
#include <memory>
#include "src/common/concurrent/concurrent.h"

namespace curve {
namespace common {

template <typename MutexT>
GenericNameLock<MutexT>::GenericNameLock(int bucketNum) {
    locks_.reserve(bucketNum);
    for (int i = 0; i < bucketNum; i++) {
        locks_.push_back(std::unique_ptr<LockBucket>(new LockBucket()));
    }
}

template <typename MutexT>
void GenericNameLock<MutexT>::Lock(const std::string &lockStr) {
    LockEntryPtr entry = nullptr;

    int bucketOffset = GetBucketOffset(lockStr);
    auto* lockBucket = locks_[bucketOffset].get();

    {
        LockGuard guard(lockBucket->mu);
        auto it = lockBucket->lockMap.find(lockStr);
        if (it == lockBucket->lockMap.end()) {
            std::unique_ptr<LockEntry> e(new LockEntry());
            entry = e.get();
            lockBucket->lockMap.emplace(lockStr, std::move(e));
        } else {
            entry = it->second.get();
            entry->ref_.fetch_add(1);
        }
    }
    entry->lock_.lock();
}

template <typename MutexT>
bool GenericNameLock<MutexT>::TryLock(const std::string &lockStr) {
    LockEntryPtr entry = nullptr;

    int bucketOffset = GetBucketOffset(lockStr);
    auto* lockBucket = locks_[bucketOffset].get();

    {
        LockGuard guard(lockBucket->mu);
        auto it = lockBucket->lockMap.find(lockStr);
        if (it == lockBucket->lockMap.end()) {
            std::unique_ptr<LockEntry> e(new LockEntry());
            entry = e.get();
            lockBucket->lockMap.emplace(lockStr, std::move(e));
        } else {
            entry = it->second.get();
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

template <typename MutexT>
void GenericNameLock<MutexT>::Unlock(const std::string &lockStr) {
    int bucketOffset = GetBucketOffset(lockStr);
    auto* lockBucket = locks_[bucketOffset].get();

    LockGuard guard(lockBucket->mu);
    auto it = lockBucket->lockMap.find(lockStr);
    if (it != lockBucket->lockMap.end()) {
        LockEntryPtr entry = it->second.get();
        entry->lock_.unlock();
        if (entry->ref_.fetch_sub(1) == 1) {
            lockBucket->lockMap.erase(it);
        }
    }
}

template <typename MutexT>
int GenericNameLock<MutexT>::GetBucketOffset(const std::string &lockStr) {
    std::hash<std::string> hash_fn;
    return hash_fn(lockStr) % locks_.size();
}


}   // namespace common
}   // namespace curve

#endif  // SRC_COMMON_CONCURRENT_GENERIC_NAME_LOCK_INL_H_
