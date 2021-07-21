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

#ifndef SRC_COMMON_CONCURRENT_GENERIC_NAME_LOCK_H_
#define SRC_COMMON_CONCURRENT_GENERIC_NAME_LOCK_H_

#include <string>
#include <unordered_map>
#include <vector>
#include <memory>
#include "src/common/concurrent/concurrent.h"


namespace curve {
namespace common {

template <typename MutexT>
class GenericNameLock  {
 public:
    explicit GenericNameLock(int bucketNum = 256);

    GenericNameLock(const GenericNameLock&) = delete;
    GenericNameLock& operator=(const GenericNameLock&) = delete;

    /**
     * @brief Lock the specified string
     *
     * @param lockStr Locked string
     */
    void Lock(const std::string &lockStr);

    /**
     * @brief Try to specify sting lock
     *
     * @param lockStr Locked string
     *
     * @retval true when succeeded
     * @retval false when failed
     */
    bool TryLock(const std::string &lockStr);

    /**
     * @brief Unlock the specified string
     *
     * @param lockStr Locked string
     */
    void Unlock(const std::string &lockStr);


 private:
    struct LockEntry {
        Atomic<uint32_t> ref_;
        MutexT lock_;
    };
    using LockEntryPtr = std::shared_ptr<LockEntry>;

    struct LockBucket {
        MutexT mu;
        std::unordered_map<std::string, LockEntryPtr> lockMap;
    };
    using LockBucketPtr = std::shared_ptr<LockBucket>;

    using LockGuard = std::lock_guard<MutexT>;

    int GetBucketOffset(const std::string &lockStr);

 private:
    std::vector<LockBucketPtr> locks_;
};

template <typename MutexT>
class GenericNameLockGuard {
 public:
    GenericNameLockGuard(GenericNameLock<MutexT> &lock, const std::string &lockStr) :  //NOLINT
        lock_(lock),
        lockStr_(lockStr),
        release_(false) {
        lock_.Lock(lockStr_);
    }

    GenericNameLockGuard(const GenericNameLockGuard&) = delete;
    GenericNameLockGuard& operator=(const GenericNameLockGuard&) = delete;

    ~GenericNameLockGuard() {
        if (!release_) {
            lock_.Unlock(lockStr_);
        }
    }

    void Release() {
        release_ = true;
    }

 private:
    GenericNameLock<MutexT> &lock_;
    std::string lockStr_;
    bool release_;
};

}   // namespace common
}   // namespace curve

#include "src/common/concurrent/generic_name_lock-inl.h"

#endif  // SRC_COMMON_CONCURRENT_GENERIC_NAME_LOCK_H_
