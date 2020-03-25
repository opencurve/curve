/*
 * Project: curve
 * Created Date: 18-10-12
 * Author: wudemiao
 * Copyright (c) 2018 netease
 */

#include <gtest/gtest.h>
#include <errno.h>

#include <thread>   // NOLINT
#include <vector>

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

TEST(WriterPreferedRWLockTest, WriteLockLatency) {
    WritePreferedRWLock lock;
    volatile bool running = true;

    std::vector<std::thread> ths;

    for (int i = 0; i < 10; ++i) {
        ths.emplace_back([&]() {
            while (running) {
                ReadLockGuard guard(lock);
                std::this_thread::sleep_for(std::chrono::microseconds(1));
            }
        });
    }

    {
        auto start = std::chrono::system_clock::now();
        WriteLockGuard guard(lock);
        auto end = std::chrono::system_clock::now();
        auto elpased =  std::chrono::duration_cast<std::chrono::microseconds>(
            end - start).count();

        ASSERT_GE(1000, elpased);
    }

    running = false;
    for (auto& th : ths) {
        th.join();
    }
}

TEST(BthreadRWLockTest, basic_test) {
    BthreadRWLock rwlock;
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
        ASSERT_EQ(EINVAL, rwlock.TryRDLock());
        ASSERT_EQ(EINVAL, rwlock.TryWRLock());
        /* be careful */
        rwlock.Unlock();
    }
    {
        WriteLockGuard writeLockGuard(rwlock);
        ASSERT_EQ(EINVAL, rwlock.TryRDLock());
        ASSERT_EQ(EINVAL, rwlock.TryWRLock());
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
