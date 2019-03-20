/*
 * Project: curve
 * Created Date: 19-03-20
 * Author: hzchenwei7
 * Copyright (c) 2018 netease
 */

// Copyright (c) 2016, Baidu.com, Inc. All Rights Reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//

#ifndef SRC_MDS_NAMESERVER2_FILE_LOCK_H_
#define SRC_MDS_NAMESERVER2_FILE_LOCK_H_

#include <string>
#include <vector>
#include <memory>
#include <unordered_map>
#include "src/common/concurrent/concurrent.h"

namespace curve {
namespace mds {
// FileLockManager完成对path进行加锁解锁逻辑
class FileLockManager {
 public:
    /**
     * @brief FileLockManager的构造函数，FileLockManager使用<path, lockEntry>的
     *        map方式进行组织，并用mutex对map进行保护。为了减少对map并发访问的冲突，
     *        把map按照hash(path)的方式打散到不同的lock bucket中。
     * @param bucketNum bucket的个数
     */
    explicit FileLockManager(int bucketNum);
    ~FileLockManager();

    /**
     * @brief 对filePath加读锁，从根目录开始，对每层路径分别加读锁，
     *        以对文件"/dir1/dir2/file1"加读锁为例：
     *        第一步，先对根目录"/"加读锁。
     *        第二步，对"/dir1"加读锁。
     *        第三步，对"/dir1/dir2"加读锁。
     *        第四步，对"/dir1/dir2/file1"加读锁。
     * @param filePath 需要加锁的path
     */
    void ReadLock(const std::string& filePath);

    /**
     * @brief 对filePath加写锁，从根目录开始，对每层路径分别加读锁，最后那层加写锁
     *        只有根目录，只对根目录加写锁。
     *        以对文件"/dir1/dir2/file1"加写锁为例：
     *        第一步，先对根目录"/"加读锁。
     *        第二步，对"/dir1"加读锁。
     *        第三步，对"/dir1/dir2"加读锁。
     *        第四步，对"/dir1/dir2/file1"加写锁。
     * @param filePath 需要加锁的path
     */
    void WriteLock(const std::string& filePath);

    /**
     * @brief 对filePath解锁
     * @param filePath 需要解锁的path
     */
    void Unlock(const std::string& filePath);

    // 为了方便单元测试
    size_t GetLockEntryNum();

 private:
    enum LockType {
        kRead,
        kWrite
    };
    struct LockEntry {
        common::Atomic<uint32_t> ref_;
        common::RWLock rwLock_;
    };
    struct LockBucket {
        common::Mutex mu;
        std::unordered_map<std::string, LockEntry*> lockMap;
    };
    void LockInternal(const std::string& path, LockType lockType);
    void UnlockInternal(const std::string& path);
    int GetBucketOffset(const std::string& path);

 private:
    std::vector<LockBucket*> locks_;
};

// 对path加读锁逻辑进行封装，构造函数中对path进行加锁，析构函数中对path进行解锁
class FileReadLockGuard {
 public:
    /**
     * @brief 构造函数对path加读锁
     * @param fileLockManager 进行加锁的FileLockManager
     * @param path 需要加锁的path
     */
    FileReadLockGuard(FileLockManager *fileLockManager,
                            const std::string &path);

    /**
     * @brief 析构函数对path解锁
     */
    ~FileReadLockGuard();
 private:
    FileLockManager *fileLockManager_;
    std::string path_;
};

// 对path加写锁逻辑进行封装，构造函数中对path进行加锁，析构函数中对path进行解锁
class FileWriteLockGuard {
 public:
    /**
     * @brief 构造函数对path加写锁
     * @param fileLockManager 进行加锁的FileLockManager
     * @param path 需要加锁的path
     */
    FileWriteLockGuard(FileLockManager *fileLockManager,
                        const std::string &path);

    /**
     * @brief 构造函数对两个path加写锁，传入两个path，该接口用来在rename的时候，
     *        对newfile和oldfile同时加锁。
     *        对path加锁，按照path1和path2的字典序进行加锁。
     *        如果path1 == path2，只对path1加锁
     *        如果path1 < path2，先对path1加锁，再对path2加锁
     *        如果path1 > path2，先对path2加锁，再对path1加锁
     * @param fileLockManager 进行加锁的FileLockManager
     * @param path1 需要加锁的path之一
     *        path2 需要加锁的path之一
     */
    FileWriteLockGuard(FileLockManager *fileLockManager,
                                const std::string &path1,
                                const std::string &path2);

    /**
     * @brief 析构函数对path解锁
     */
    ~FileWriteLockGuard();

 private:
    FileLockManager *fileLockManager_;
    std::vector<std::string> path_;
};
}  // namespace mds
}  // namespace curve

#endif  // SRC_MDS_NAMESERVER2_FILE_LOCK_H_
