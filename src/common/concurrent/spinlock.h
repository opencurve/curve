/*
 * Project: curve
 * File Created: Saturday, 29th September 2018 3:10:00 pm
 * Author: tongguangxun
 * Copyright (c) 2018 NetEase
 */

#ifndef SRC_COMMON_CONCURRENT_SPINLOCK_H_
#define SRC_COMMON_CONCURRENT_SPINLOCK_H_

#include <atomic>
#include "include/curve_compiler_specific.h"

namespace curve {
namespace common {
class SpinLock {
 public:
    SpinLock(): flag_(ATOMIC_FLAG_INIT) {
    }
    ~SpinLock() {}

    void Lock() {
        while (flag_.test_and_set(std::memory_order_acquire)) {}
    }

    void UnLock() {
        flag_.clear(std::memory_order_release);
    }

 private:
    CURVE_CACHELINE_ALIGNMENT std::atomic_flag flag_;
};
}   // namespace common
}   // namespace curve
#endif  // SRC_COMMON_CONCURRENT_SPINLOCK_H_
