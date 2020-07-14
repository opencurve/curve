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
 * Project: nebd
 * Created Date: Thu Aug 08 2019
 * Author: xuchaojie
 */

#ifndef NEBD_SRC_COMMON_NAME_LOCK_H_
#define NEBD_SRC_COMMON_NAME_LOCK_H_

#include <string>
#include <unordered_map>
#include <vector>
#include <memory>
#include <atomic>
#include <mutex>  // NOLINT

#include "src/common/uncopyable.h"

namespace nebd {
namespace common {

class NameLock : public curve::common::Uncopyable {
 public:
    explicit NameLock(int bucketNum = 256);

    /**
     * @brief 对指定string加锁
     *
     * @param lockStr 被加锁的string
     */
    void Lock(const std::string &lockStr);

    /**
     * @brief 尝试指定sting加锁
     *
     * @param lockStr 被加锁的string
     *
     * @retval 成功
     * @retval 失败
     */
    bool TryLock(const std::string &lockStr);

    /**
     * @brief 对指定string解锁
     *
     * @param lockStr 被加锁的string
     */
    void Unlock(const std::string &lockStr);


 private:
    struct LockEntry {
        std::atomic<uint32_t> ref_;
        std::mutex lock_;
    };
    using LockEntryPtr = std::shared_ptr<LockEntry>;

    struct LockBucket {
        std::mutex mu;
        std::unordered_map<std::string, LockEntryPtr> lockMap;
    };
    using LockBucketPtr = std::shared_ptr<LockBucket>;

    int GetBucketOffset(const std::string &lockStr);

 private:
    std::vector<LockBucketPtr> locks_;
};

class NameLockGuard : public curve::common::Uncopyable {
 public:
    NameLockGuard(NameLock &lock, const std::string &lockStr) :  //NOLINT
        lock_(lock),
        lockStr_(lockStr) {
        lock_.Lock(lockStr_);
    }

    ~NameLockGuard() {
        lock_.Unlock(lockStr_);
    }

 private:
    NameLock &lock_;
    std::string lockStr_;
};


}   // namespace common
}   // namespace nebd


#endif  // NEBD_SRC_COMMON_NAME_LOCK_H_
