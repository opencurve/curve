/*
 * Project: curve
 * Created Date: Thu Aug 08 2019
 * Author: xuchaojie
 * Copyright (c) 2019 netease
 */

#ifndef SRC_COMMON_CONCURRENT_NAME_LOCK_H_
#define SRC_COMMON_CONCURRENT_NAME_LOCK_H_

#include <string>
#include <unordered_map>
#include <vector>
#include <memory>
#include "src/common/concurrent/concurrent.h"


namespace curve {
namespace common {

class NameLock : public Uncopyable {
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
        Atomic<uint32_t> ref_;
        Mutex lock_;
    };
    using LockEntryPtr = std::shared_ptr<LockEntry>;

    struct LockBucket {
        Mutex mu;
        std::unordered_map<std::string, LockEntryPtr> lockMap;
    };
    using LockBucketPtr = std::shared_ptr<LockBucket>;

    int GetBucketOffset(const std::string &lockStr);

 private:
    std::vector<LockBucketPtr> locks_;
};

class NameLockGuard : public Uncopyable {
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
}   // namespace curve


#endif  // SRC_COMMON_CONCURRENT_NAME_LOCK_H_
