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
 * Created Date: 2019-3-22
 * Author: hzchenwei7
 */

// Copyright (c) 2016, Baidu.com, Inc. All Rights Reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
#include "src/mds/nameserver2/file_lock.h"
#include <string.h>
#include <glog/logging.h>
#include <utility>
#include "src/common/hash.h"
#include "src/common/string_util.h"

namespace curve {
namespace mds {
FileLockManager::FileLockManager(int bucketNum) {
    locks_.reserve(bucketNum);
    for (int i = 0; i < bucketNum; i++) {
        locks_.push_back(new LockBucket);
    }
}

FileLockManager::~FileLockManager() {
    for (auto iter = locks_.begin(); iter != locks_.end(); iter++) {
        delete *iter;
    }
    locks_.clear();
}

void FileLockManager::ReadLock(const std::string& filePath) {
    std::vector<std::string> paths;
    common::SplitString(filePath, "/", &paths);
    // first lock "/"
    LockInternal("/", kRead);
    std::string curPath;
    for (size_t i = 0; i < paths.size(); i++) {
        curPath += ("/" + paths[i]);
        LockInternal(curPath, kRead);
    }
}

void FileLockManager::WriteLock(const std::string& filePath) {
    std::vector<std::string> paths;
    common::SplitString(filePath, "/", &paths);
    // first lock "/"
    if (paths.size() == 0) {
        LockInternal("/", kWrite);
        return;
    }
    LockInternal("/", kRead);
    std::string curPath;
    for (size_t i = 0; i < paths.size() - 1; i++) {
        curPath += ("/" + paths[i]);
        LockInternal(curPath, kRead);
    }
    curPath += ("/" + paths.back());
    LockInternal(curPath, kWrite);
}

void FileLockManager::Unlock(const std::string& filePath) {
    std::vector<std::string> paths;
    common::SplitString(filePath, "/", &paths);
    std::string path;
    for (size_t i = 0; i < paths.size(); i++) {
        path += ("/" + paths[i]);
    }

    std::string curPath = path;
    for (size_t i = 0; i < paths.size() ; i++) {
        UnlockInternal(curPath);
        curPath.resize(curPath.find_last_of('/'));
    }
    // last unlock "/"
    UnlockInternal("/");
}

void FileLockManager::LockInternal(const std::string& path,
                                   LockType lockType) {
    LockEntry* entry = NULL;

    int bucketOffset = GetBucketOffset(path);
    LockBucket* lockBucket = locks_[bucketOffset];

    {
        std::lock_guard<std::mutex> guard(lockBucket->mu);
        auto it = lockBucket->lockMap.find(path);
        if (it == lockBucket->lockMap.end()) {
            entry = new LockEntry();
            // hold a ref for lockMap_
            entry->ref_.store(1);
            lockBucket->lockMap.insert(std::make_pair(path, entry));
        } else {
            entry = it->second;
            entry->ref_.fetch_add(1);
        }
    }

    if (lockType == kRead) {
        // get read lock
        bthread_rwlock_rdlock(&entry->rwLock_);
    } else {
        // get write lock
        bthread_rwlock_wrlock(&entry->rwLock_);
    }
}

void FileLockManager::UnlockInternal(const std::string& path) {
    int bucketOffset = GetBucketOffset(path);
    LockBucket* lockBucket = locks_[bucketOffset];

    std::lock_guard<std::mutex> guard(lockBucket->mu);
    auto it = lockBucket->lockMap.find(path);
    assert(it != lockBucket->lockMap.end());
    LockEntry* entry = it->second;
    // release lock
    bthread_rwlock_unlock(&entry->rwLock_);
    if (entry->ref_.fetch_sub(1) == 1) {
        // we are the last holder
        /// TODO maybe don't need to deconstruct immediately
        delete entry;
        lockBucket->lockMap.erase(it);
    }
}

int FileLockManager::GetBucketOffset(const std::string& path) {
    return common::Hash(path.c_str(), path.size(), 0) % locks_.size();
}

size_t FileLockManager::GetLockEntryNum() {
    size_t sum = 0;
    for (size_t i = 0; i < locks_.size(); i++) {
        sum += locks_[i]->lockMap.size();
    }
    return sum;
}

FileReadLockGuard::FileReadLockGuard(FileLockManager *fileLockManager,
                                     const std::string &path) {
    fileLockManager_ = fileLockManager;
    path_ = path;
    fileLockManager_->ReadLock(path_);
}

FileReadLockGuard::~FileReadLockGuard() {
    fileLockManager_->Unlock(path_);
}

FileWriteLockGuard::FileWriteLockGuard(FileLockManager *fileLockManager,
                                       const std::string &path) {
    fileLockManager_ = fileLockManager;
    path_.push_back(path);
    fileLockManager_->WriteLock(path);
}
// this interface is for locking newfile and oldfile at the same time when renaming //NOLINT
FileWriteLockGuard::FileWriteLockGuard(FileLockManager *fileLockManager,
                                       const std::string &path1,
                                       const std::string &path2) {
    fileLockManager_ = fileLockManager;
    int r = strcmp(path1.c_str(), path2.c_str());
    if (r == 0) {
        path_.push_back(path1);
        fileLockManager_->WriteLock(path1);
    } else if (r < 0) {
        path_.push_back(path1);
        path_.push_back(path2);
        fileLockManager_->WriteLock(path1);
        fileLockManager_->WriteLock(path2);
    } else {
        path_.push_back(path2);
        path_.push_back(path1);
        fileLockManager_->WriteLock(path2);
        fileLockManager_->WriteLock(path1);
    }
}
FileWriteLockGuard::~FileWriteLockGuard() {
    if (path_.size() == 1) {
        fileLockManager_->Unlock(path_[0]);
    } else {
        fileLockManager_->Unlock(path_[1]);
        fileLockManager_->Unlock(path_[0]);
    }
}
}  // namespace mds
}  // namespace curve
