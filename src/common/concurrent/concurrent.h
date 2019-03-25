/*
 * Project: curve
 * File Created: Monday, 25th March 2019 11:47:46 am
 * Author: tongguangxun
 * Copyright (c)￼ 2018 netease
 */

#include <atomic>
#include <thread>   // NOLINT
#include <condition_variable>   // NOLINT

#include "src/common/concurrent/concurrent/count_down_event.h"
#include "src/common/concurrent/concurrent/spinlock.h"
#include "src/common/concurrent/concurrent/rw_lock.h"

namespace curve {
namespace common {

// curve公共组件命名空间替换
using Atomic                = std::atomic;
using Mutex                 = std::mutex;
using Thread                = std::thread;
using LockGuard             = std::lock_guard<mutex>;
using UniqueLock            = std::unique_lock<mutex>;
using ConditionVariable     = std::condition_variable;

// curve内部定义的锁组件
using RWLock                = RWLock;
using SpinLock              = SpinLock;
using ReadLockGuard         = ReadLockGuard;
using WriteLockGuard        = WriteLockGuard;

// curve内部定义的线程组件
using TaskQueue             = TaskQueue;
using ThreadPool            = ThreadPool;
using TaskThreadPool        = TaskThreadPool;
}   // namespace common
}   // namespace curve
