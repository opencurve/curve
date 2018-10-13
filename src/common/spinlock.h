/*
 * Project: curve
 * File Created: Saturday, 29th September 2018 3:10:00 pm
 * Author: tongguangxun
 * Copyright (c) 2018 NetEase
 */

#ifndef CURVE_LIBCURVE_SPINLOCK_H
#define CURVE_LIBCURVE_SPINLOCK_H

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
    std::atomic_flag CURVE_CACHELINE_ALIGNMENT flag_;
};
}   // namespace client
}   // namespace curve
#endif  // CURVE_LIBCURVE_SPINLOCK_H
