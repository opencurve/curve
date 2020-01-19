/*
 * Project: nebd
 * Created Date: Fri Aug 09 2019
 * Author: xuchaojie
 * Copyright (c) 2019 netease
 */

#include <gtest/gtest.h>
#include <random>
#include <thread>

#include "src/common/name_lock.h"

namespace nebd {
namespace common {

TEST(TestNameLock, TestNameLockBasic) {
    NameLock lock1, lock2, lock3;

    // lock测试
    lock1.Lock("str1");
    // 同锁不同str可lock不死锁
    lock1.Lock("str2");
    // 不同锁同str可lock不死锁
    lock2.Lock("str1");



    // 同锁同str TryLock失败
    ASSERT_FALSE(lock1.TryLock("str1"));
    // 同锁不同str TryLock成功
    ASSERT_TRUE(lock1.TryLock("str3"));
    // 不同锁同str TryLock成功
    ASSERT_TRUE(lock3.TryLock("str1"));

    // unlock测试
    lock1.Unlock("str1");
    lock1.Unlock("str2");
    lock1.Unlock("str3");
    lock2.Unlock("str1");
    lock3.Unlock("str1");
    // 未锁unlock ok
    lock2.Unlock("str2");
}

TEST(TestNameLock, TestNameLockGuardBasic) {
    NameLock lock1, lock2;
    {
        NameLockGuard guard1(lock1, "str1");
        NameLockGuard guard2(lock1, "str2");
        NameLockGuard guard3(lock2, "str1");
        // 作用域内加锁成功，不可再加锁
        ASSERT_FALSE(lock1.TryLock("str1"));
        ASSERT_FALSE(lock1.TryLock("str2"));
        ASSERT_FALSE(lock2.TryLock("str1"));
    }
    // 作用域外自动解锁，可再加锁
    ASSERT_TRUE(lock1.TryLock("str1"));
    ASSERT_TRUE(lock1.TryLock("str2"));
    ASSERT_TRUE(lock2.TryLock("str1"));
    lock1.Unlock("str1");
    lock1.Unlock("str2");
    lock2.Unlock("str1");
}

TEST(TestNameLock, TestNameLockConcurrent) {
    NameLock lock1;
    auto worker = [&] (const std::string &str) {
        for (int i = 0; i < 10000; i++) {
            NameLockGuard guard(lock1, str);
        }
    };

    std::vector<std::thread> threadpool;
    for (auto &t : threadpool) {
        std::string str1 = "aaaa";
        std::string str2 = "bbbb";
        std::srand(std::time(nullptr));
        std::string rstr = (std::rand() / 2) ? str1 : str2;
        t = std::thread(worker, rstr);
    }

    for (auto &t : threadpool) {
        t.join();
    }
}



}   // namespace common
}   // namespace nebd
