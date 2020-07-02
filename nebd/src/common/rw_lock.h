/*
 * Project: nebd
 * Created Date: 18-10-11
 * Author: wudemiao
 * Copyright (c) 2018 netease
 */

#ifndef SRC_COMMON_RW_LOCK_H_
#define SRC_COMMON_RW_LOCK_H_

#include <pthread.h>
#include <assert.h>
#include <glog/logging.h>
#include <bthread/bthread.h>

#include "nebd/src/common/uncopyable.h"

namespace nebd {
namespace common {

class RWLockBase : public Uncopyable {
 public:
    RWLockBase() {}
    virtual ~RWLockBase() {}
    virtual void WRLock() = 0;
    virtual int TryWRLock() = 0;
    virtual void RDLock() = 0;
    virtual int TryRDLock() = 0;
    virtual void Unlock() = 0;
};

class RWLock :  public RWLockBase {
 public:
    RWLock() {
        pthread_rwlock_init(&rwlock_, nullptr);
    }

    ~RWLock() {
        pthread_rwlock_destroy(&rwlock_);
    }

    void WRLock() override {
        int ret = pthread_rwlock_wrlock(&rwlock_);
        CHECK(0 == ret) << "wlock failed: " << ret << ", " << strerror(ret);
    }

    int TryWRLock() override {
        return pthread_rwlock_trywrlock(&rwlock_);
    }

    void RDLock() override {
        int ret = pthread_rwlock_rdlock(&rwlock_);
        CHECK(0 == ret) << "rlock failed: " << ret << ", " << strerror(ret);
    }

    int TryRDLock() override {
        return pthread_rwlock_tryrdlock(&rwlock_);
    }

    void Unlock() override {
        pthread_rwlock_unlock(&rwlock_);
    }

 private:
    pthread_rwlock_t rwlock_;
};  // RWLock class

class BthreadRWLock :  public RWLockBase {
 public:
    BthreadRWLock() {
        bthread_rwlock_init(&rwlock_, nullptr);
    }

    ~BthreadRWLock() {
        bthread_rwlock_destroy(&rwlock_);
    }

    void WRLock() override {
        int ret = bthread_rwlock_wrlock(&rwlock_);
        CHECK(0 == ret) << "wlock failed: " << ret << ", " << strerror(ret);
    }

    int TryWRLock() override {
        // not support yet
        return EINVAL;
    }

    void RDLock() override {
        int ret = bthread_rwlock_rdlock(&rwlock_);
        CHECK(0 == ret) << "rlock failed: " << ret << ", " << strerror(ret);
    }

    int TryRDLock() override {
        // not support yet
        return EINVAL;
    }

    void Unlock() override {
        bthread_rwlock_unlock(&rwlock_);
    }

 private:
    bthread_rwlock_t rwlock_;
};  // RWLock class

class ReadLockGuard : public Uncopyable {
 public:
    explicit ReadLockGuard(RWLockBase &rwlock) : rwlock_(rwlock) {
        rwlock_.RDLock();
    }

    ~ReadLockGuard() {
        rwlock_.Unlock();
    }

 private:
    RWLockBase &rwlock_;
};  // ReadLockGuard class

class WriteLockGuard : public Uncopyable {
 public:
    explicit WriteLockGuard(RWLockBase &rwlock) : rwlock_(rwlock) {
        rwlock_.WRLock();
    }

    ~WriteLockGuard() {
        rwlock_.Unlock();
    }

 private:
    RWLockBase &rwlock_;
};  // WriteLockGuard class

}  // namespace common
}  // namespace nebd

#endif  // SRC_COMMON_RW_LOCK_H_
