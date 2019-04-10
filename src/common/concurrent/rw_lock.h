/*
 * Project: curve
 * Created Date: 18-10-11
 * Author: wudemiao
 * Copyright (c) 2018 netease
 */

#ifndef SRC_COMMON_CONCURRENT_RW_LOCK_H_
#define SRC_COMMON_CONCURRENT_RW_LOCK_H_

#include <pthread.h>
#include <assert.h>

#include "src/common/uncopyable.h"

namespace curve {
namespace common {

class RWLock : public Uncopyable {
 public:
    RWLock() {
        pthread_rwlock_init(&rwlock_, nullptr);
    }

    ~RWLock() {
        pthread_rwlock_destroy(&rwlock_);
    }

    void WRLock() {
        pthread_rwlock_wrlock(&rwlock_);
    }

    int TryWRLock() {
        return pthread_rwlock_trywrlock(&rwlock_);
    }

    void RDLock() {
        pthread_rwlock_rdlock(&rwlock_);
    }

    int TryRDLock() {
        return pthread_rwlock_tryrdlock(&rwlock_);
    }

    void Unlock() {
        pthread_rwlock_unlock(&rwlock_);
    }

 private:
    pthread_rwlock_t rwlock_;
};  // RWLock class

class ReadLockGuard : public Uncopyable {
 public:
    explicit ReadLockGuard(RWLock &rwlock) : rwlock_(rwlock) {
        rwlock_.RDLock();
    }

    ~ReadLockGuard() {
        rwlock_.Unlock();
    }

 private:
    RWLock &rwlock_;
};  // ReadLockGuard class

class WriteLockGuard : public Uncopyable {
 public:
    explicit WriteLockGuard(RWLock &rwlock) : rwlock_(rwlock) {
        rwlock_.WRLock();
    }

    ~WriteLockGuard() {
        rwlock_.Unlock();
    }

 private:
    RWLock &rwlock_;
};  // WriteLockGuard class

}  // namespace common
}  // namespace curve

#endif  // SRC_COMMON_CONCURRENT_RW_LOCK_H_
