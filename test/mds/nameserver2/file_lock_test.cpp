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
 * Created Date: 2019-04-03
 * Author: hzchenwei7
 */
#include <glog/logging.h>
#include <gtest/gtest.h>
#include <gmock/gmock.h>
#include <thread>  // NOLINT
#include "src/mds/nameserver2/file_lock.h"

using ::testing::AtLeast;
using ::testing::StrEq;
using ::testing::_;
using ::testing::Return;
using ::testing::ReturnArg;
using ::testing::DoAll;
using ::testing::SetArgPointee;

namespace curve {
namespace mds {

FileLockManager flm(4);

class FileLockManagerTest: public ::testing::Test {
 public:
    FileLockManagerTest() {}
};

void WriteLock(const std::string& filePath, bool unlock = false) {
    flm.WriteLock(filePath);
    sleep(1);
    if (unlock) {
        flm.Unlock(filePath);
    }
}

void ReadLock(const std::string& filePath, bool unlock = false) {
    flm.ReadLock(filePath);
    sleep(1);
    if (unlock) {
        flm.Unlock(filePath);
    }
}

void Unlock(const std::string& filePath) {
    flm.Unlock(filePath);
}

TEST_F(FileLockManagerTest, Basic) {
    std::string filePath1 = "/home/dir1/file1";
    std::string filePath2 = "/home/dir2/file2";

    flm.WriteLock(filePath1);
    flm.Unlock(filePath1);

    flm.ReadLock(filePath1);
    flm.Unlock(filePath1);

    flm.ReadLock(filePath1);
    flm.ReadLock(filePath1);
    flm.Unlock(filePath1);
    flm.Unlock(filePath1);

    flm.WriteLock("/");
    flm.Unlock("/");

    flm.ReadLock("/");
    flm.Unlock("/");
}

TEST_F(FileLockManagerTest, RandomReadWriteLock) {
    std::vector<common::Thread> threads;
    std::srand(std::time(nullptr));
    std::string filePath = "/home/dir1/file1";
    for (int i = 0; i < 10; i++) {
        int r = std::rand() % 2;
        if (r == 1) {
            threads.emplace_back(std::bind(WriteLock, filePath, true));
        } else {
            threads.emplace_back(std::bind(ReadLock, filePath, true));
        }
    }

    for (auto& iter : threads) {
        if (iter.joinable()) {
            iter.join();
        }
    }

    ASSERT_EQ(flm.GetLockEntryNum(), 0);
}

TEST_F(FileLockManagerTest, UnlockInAnotherThread) {
    std::string filePath = "/home/dir1/file1";
    common::Thread t1(std::bind(WriteLock, filePath, false));
    // wait for task to be executed
    t1.join();
    Unlock(filePath);
}

class FileReadLockGuardTest: public ::testing::Test {
 public:
    FileReadLockGuardTest() {}
};

TEST_F(FileReadLockGuardTest, LockUnlockTest) {
    {
        FileReadLockGuard guard(&flm, "/");
    }

    {
        FileReadLockGuard guard(&flm, "/a");
    }

    {
        FileReadLockGuard guard(&flm, "/a/b");
    }

    ASSERT_EQ(flm.GetLockEntryNum(), 0);
}

class FileWriteLockGuardTest: public ::testing::Test {
 public:
    FileWriteLockGuardTest() {}
};

TEST_F(FileWriteLockGuardTest, LockUnlockTest) {
    {
        FileWriteLockGuard guard(&flm, "/");
    }

    {
        FileWriteLockGuard guard(&flm, "/a");
    }

    {
        FileWriteLockGuard guard(&flm, "/a/b");
    }

    {
        FileWriteLockGuard guard(&flm, "/a", "/a");
    }

    {
        FileWriteLockGuard guard(&flm, "/a", "/b");
    }

    {
        FileWriteLockGuard guard(&flm, "/b", "/a");
    }

    ASSERT_EQ(flm.GetLockEntryNum(), 0);
}

// 以下这种情况，跑测试的时候会出现Segmentation fault，是锁的实现机制的问题
// 要避免这样使用锁，已在代码里进行规避，以下注释的测试保留，提醒使用者注意
/*
TEST_F(FileWriteLockGuardTest, LockUnlockTest1) {
    {
        FileWriteLockGuard guard(&flm, "/", "/a");
    }

    {
        FileWriteLockGuard guard(&flm, "/a", "/");
    }
}
*/
}  // namespace mds
}  // namespace curve
