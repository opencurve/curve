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
#include <glog/logging.h>
#include <bthread/bthread.h>

#include "src/common/uncopyable.h"

namespace curve {
namespace common {

class RWLockBase : public Uncopyable {
 public:
    virtual void WRLock() = 0;
    virtual int TryWRLock() = 0;
    virtual void RDLock() = 0;
    virtual int TryRDLock() = 0;
    virtual void Unlock() = 0;

 protected:
    RWLockBase() = default;
    virtual ~RWLockBase() = default;
};

class PthreadRWLockBase : public RWLockBase {
 public:
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

 protected:
    PthreadRWLockBase() = default;
    virtual ~PthreadRWLockBase() = default;

    pthread_rwlock_t rwlock_;
    pthread_rwlockattr_t rwlockAttr_;
};

class RWLock : public PthreadRWLockBase {
 public:
    RWLock() {
        pthread_rwlock_init(&rwlock_, nullptr);
    }

    ~RWLock() {
        pthread_rwlock_destroy(&rwlock_);
    }
};  // RWLock class

class WritePreferedRWLock : public PthreadRWLockBase {
 public:
    WritePreferedRWLock() {
        pthread_rwlockattr_init(&rwlockAttr_);
        pthread_rwlockattr_setkind_np(
            &rwlockAttr_,
            PTHREAD_RWLOCK_PREFER_WRITER_NONRECURSIVE_NP);

        pthread_rwlock_init(&rwlock_, &rwlockAttr_);
    }

    ~WritePreferedRWLock() {
        pthread_rwlockattr_destroy(&rwlockAttr_);
        pthread_rwlock_destroy(&rwlock_);
    }
};

class BthreadRWLock : public RWLockBase {
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
};  // BthreadRWLock

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
}  // namespace curve

#endif  // SRC_COMMON_CONCURRENT_RW_LOCK_H_
