/*
 * Project: curve
 * Created Date: 2019-04-03
 * Author: hzchenwei7
 * Copyright (c) 2018 netease
 */
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
    std::vector<std::thread> threads;
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
    std::thread t1(std::bind(WriteLock, filePath, false));
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
}  // namespace mds
}  // namespace curve
