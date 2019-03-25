/*
 * Project: curve
 * Created Date: 18-10-12
 * Author: wudemiao
 * Copyright (c) 2018 netease
 */

#include <gtest/gtest.h>
#include <errno.h>

#include <thread>   // NOLINT

#include "src/common/concurrent/rw_lock.h"

namespace curve {
namespace common {

TEST(RWLockTest, basic_test) {
    RWLock rwlock;
    {
        ReadLockGuard readLockGuard(rwlock);
        ASSERT_TRUE(true);
    }
    {
        WriteLockGuard writeLockGuard(rwlock);
        ASSERT_TRUE(true);
    }
    {
        WriteLockGuard writeLockGuard(rwlock);
        ASSERT_TRUE(true);
    }
    {
        ReadLockGuard readLockGuard1(rwlock);
        ReadLockGuard readLockGuard2(rwlock);
        ASSERT_TRUE(true);
    }
    {
        ReadLockGuard readLockGuard1(rwlock);
        ReadLockGuard readLockGuard2(rwlock);
        ReadLockGuard readLockGuard3(rwlock);
        ReadLockGuard readLockGuard4(rwlock);
        ASSERT_TRUE(true);
    }
    {
        ReadLockGuard readLockGuard(rwlock);
        ASSERT_EQ(0, rwlock.TryRDLock());
        ASSERT_EQ(EBUSY, rwlock.TryWRLock());
        /* be careful */
        rwlock.Unlock();
    }
    {
        WriteLockGuard writeLockGuard(rwlock);
        ASSERT_EQ(EBUSY, rwlock.TryRDLock());
        ASSERT_EQ(EBUSY, rwlock.TryWRLock());
    }
    uint64_t writeCnt = 0;
    auto writeFunc = [&] {
        for (uint64_t i = 0; i < 10000; ++i) {
            WriteLockGuard writeLockGuard(rwlock);
            ++writeCnt;
        }
    };
    auto readFunc = [&] {
        for (uint64_t i = 0; i < 10000; ++i) {
            ReadLockGuard readLockGuard(rwlock);
            auto j = writeCnt + i;
        }
    };
    {
        std::thread t1(writeFunc);
        std::thread t2(readFunc);
        std::thread t3(writeFunc);
        std::thread t4(readFunc);
        std::thread t5(writeFunc);
        std::thread t6(writeFunc);
        t1.join();
        t2.join();
        t3.join();
        t4.join();
        t5.join();
        t6.join();

        ASSERT_EQ(4 * 10000, writeCnt);
    }
}

}  // namespace common
}  // namespace curve
