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
 * File Created: Saturday, 29th September 2018 3:10:00 pm
 * Author: tongguangxun
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
