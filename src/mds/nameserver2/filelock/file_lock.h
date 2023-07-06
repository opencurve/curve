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
 * Created Date: 19-03-20
 * Author: hzchenwei7
 */

// Copyright (c) 2016, Baidu.com, Inc. All Rights Reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//

#ifndef SRC_MDS_NAMESERVER2_FILELOCK_FILE_LOCK_H_
#define SRC_MDS_NAMESERVER2_FILELOCK_FILE_LOCK_H_

#include <bthread/bthread.h>

#include <string>
#include <vector>
#include <memory>
#include <unordered_map>
#include "src/common/concurrent/concurrent.h"

namespace curve {
namespace mds {
// FileLockManager is for locking and unlocking path
class FileLockManager {
 public:
    /**
     * @brief constructor of FileLockManager，FileLockManager is organized in
     *        map<path, lockEntry>, and there's a mutex for protecting the map.
     *        In order to relief conflicts caused by the concurrent visits
     *        toward this map, the map is scattered into different lock buckets
     *        using a hash function hash(path).
     * @param bucketNum
     */
    explicit FileLockManager(int bucketNum);
    ~FileLockManager();

    /**
     * @brief When apply read lock on a path, every level of it start from
     *        the root will be locked.
     *        Here's an example of applying read lock on file "/dir1/dir2/file1"
     *        1. apply read lock on root path "/"
     *        2. apply read lock on path "/dir1"
     *        3. apply read lock on path "/dir1/dir2"
     *        4. apply read lock on "/dir1/dir2/file1"
     * @param filePath: the file path on which the read lock will be applied
     */
    void ReadLock(const std::string& filePath);

    /**
     * @brief For write lock, it's similar with read lock, but only the last
     *        level will be applied a write lock. For case when there's only
     *        root path input, apply write on the root path.
     *        Here we also provide a similar example: apply write lock on file
     *        "/dir1/dir2/file1":
     *        1. apply read lock on root path "/"
     *        2. apply read lock on path "/dir1"
     *        3. apply read lock on path "/dir1/dir2"
     *        4. apply write lock on "/dir1/dir2/file1"
     * @param filePath: the file path on which the write lock will be applied
     */
    void WriteLock(const std::string& filePath);

    /**
     * @brief unlock for filePath
     * @param filePath: to be unlocked
     */
    void Unlock(const std::string& filePath);

    // method for unit test
    size_t GetLockEntryNum();

 private:
    enum LockType {
        kRead,
        kWrite
    };
    struct LockEntry {
        common::Atomic<uint32_t> ref_;
        bthread_rwlock_t rwLock_;

        LockEntry() { bthread_rwlock_init(&rwLock_, NULL); }

        ~LockEntry() {
            bthread_rwlock_destroy(&rwLock_);
        }
    };
    struct LockBucket {
        common::Mutex mu;
        std::unordered_map<std::string, LockEntry*> lockMap;
    };
    void LockInternal(const std::string& path, LockType lockType);
    void UnlockInternal(const std::string& path);
    int GetBucketOffset(const std::string& path);

 private:
    std::vector<LockBucket*> locks_;
};

// encapsulation of applying read lock logic on a path
// the path will be locked in the constructor, and unlock in the destructor
class FileReadLockGuard {
 public:
    /**
     * @brief constructor that applies read lock on the path
     * @param fileLockManager FileLockManager that applies the lock
     * @param path the path on which the lock applied
     */
    FileReadLockGuard(FileLockManager *fileLockManager,
                            const std::string &path);

    /**
     * @brief the destructor will be responsible for unlocking
     */
    ~FileReadLockGuard();
 private:
    FileLockManager *fileLockManager_;
    std::string path_;
};

// encapsulation of applying write lock logic on a path
// the path will be locked in the constructor, and unlock in the destructor
class FileWriteLockGuard {
 public:
    /**
     * @brief constructor that applies write lock on the path
     * @param fileLockManager FileLockManager that applies the lock
     * @param path the path on which the lock applied
     */
    FileWriteLockGuard(FileLockManager *fileLockManager,
                        const std::string &path);

   /**
     * @brief another constructor that applies write lock on two paths. This
     *        is for rename operation that two files are involved.
     *        The order of applying write lock will be based on the
     *        lexicographical order of two paths:
     *        if order(path1) == order(path2), apply write lock only on path1
     *        if order(path1) < order(path2), apply the lock on path1 then path2
     *        if order(path1) < order(path2), vice versa
     * @param fileLockManager
     * @param path1
     * @param path2
     */
    FileWriteLockGuard(FileLockManager *fileLockManager,
                                const std::string &path1,
                                const std::string &path2);

    /**
     * @brief destructor for unlocking
     */
    ~FileWriteLockGuard();

 private:
    FileLockManager *fileLockManager_;
    std::vector<std::string> path_;
};
}  // namespace mds
}  // namespace curve

#endif  // SRC_MDS_NAMESERVER2_FILELOCK_FILE_LOCK_H_
