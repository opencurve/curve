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
 * Created Date: Fri Aug 09 2019
 * Author: xuchaojie
 */

#include <gtest/gtest.h>
#include <random>
#include <thread>  // NOLINT

#include "nebd/src/common/name_lock.h"

namespace nebd {
namespace common {

TEST(TestNameLock, TestNameLockBasic) {
    NameLock lock1, lock2, lock3;

    // Lock test
    lock1.Lock("str1");
    // Same lock but different strs can lock without deadlock
    lock1.Lock("str2");
    // Different locks with the same str can lock without deadlock
    lock2.Lock("str1");



    // Same lock with str TryLock failed
    ASSERT_FALSE(lock1.TryLock("str1"));
    // Same lock different str TryLock successful
    ASSERT_TRUE(lock1.TryLock("str3"));
    // Different locks with str TryLock succeeded
    ASSERT_TRUE(lock3.TryLock("str1"));

    // Unlock test
    lock1.Unlock("str1");
    lock1.Unlock("str2");
    lock1.Unlock("str3");
    lock2.Unlock("str1");
    lock3.Unlock("str1");
    // Unlock OK
    lock2.Unlock("str2");
}

TEST(TestNameLock, TestNameLockGuardBasic) {
    NameLock lock1, lock2;
    {
        NameLockGuard guard1(lock1, "str1");
        NameLockGuard guard2(lock1, "str2");
        NameLockGuard guard3(lock2, "str1");
        // Successfully locked within the scope, unable to lock again
        ASSERT_FALSE(lock1.TryLock("str1"));
        ASSERT_FALSE(lock1.TryLock("str2"));
        ASSERT_FALSE(lock2.TryLock("str1"));
    }
    // Automatically unlocking outside the scope, with the option to add locks again
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
