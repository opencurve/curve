/* -*- mode: c++; c-basic-offset: 4; indent-tabs-mode: nil; tab-width: 4 -*- */
#ifndef ATOMIC_QUEUE_ATOMIC_QUEUE_SPIN_LOCK_H_INCLUDED
#define ATOMIC_QUEUE_ATOMIC_QUEUE_SPIN_LOCK_H_INCLUDED

// Copyright (c) 2019 Maxim Egorushkin. MIT License. See the full licence in file LICENSE.

#include "atomic_queue.h"
#include "spinlock.h"

#include <mutex>
#include <cassert>

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

namespace atomic_queue {

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

template<class M>
struct ScopedLockType {
    using type = typename M::scoped_lock;
};

template<>
struct ScopedLockType<std::mutex> {
    using type = std::unique_lock<std::mutex>;
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

template<class T, class Mutex, unsigned SIZE, bool MINIMIZE_CONTENTION>
class AtomicQueueMutexT {
    static constexpr unsigned size_ = MINIMIZE_CONTENTION ? details::round_up_to_power_of_2(SIZE) : SIZE;

    Mutex mutex_;
    alignas(CACHE_LINE_SIZE) unsigned head_ = 0;
    alignas(CACHE_LINE_SIZE) unsigned tail_ = 0;
    alignas(CACHE_LINE_SIZE) T q_[size_] = {};

    static constexpr int SHUFFLE_BITS = details::GetIndexShuffleBits<MINIMIZE_CONTENTION, size_, CACHE_LINE_SIZE / sizeof(T)>::value;

    using ScopedLock = typename ScopedLockType<Mutex>::type;

public:
    using value_type = T;

    template<class U>
    bool try_push(U&& element) noexcept {
        ScopedLock lock(mutex_);
        if(ATOMIC_QUEUE_LIKELY(head_ - tail_ < size_)) {
            q_[details::remap_index<SHUFFLE_BITS>(head_ % size_)] = std::forward<U>(element);
            ++head_;
            return true;
        }
        return false;
    }

    bool try_pop(T& element) noexcept {
        ScopedLock lock(mutex_);
        if(ATOMIC_QUEUE_LIKELY(head_ != tail_)) {
            element = std::move(q_[details::remap_index<SHUFFLE_BITS>(tail_ % size_)]);
            ++tail_;
            return true;
        }
        return false;
    }

    bool was_empty() const noexcept {
        return static_cast<int>(head_ - tail_) <= 0;
    }

    bool was_full() const noexcept {
        return static_cast<int>(head_ - tail_) >= static_cast<int>(size_);
    }
};

template<class T, unsigned SIZE, class Mutex, bool MINIMIZE_CONTENTION = true>
using AtomicQueueMutex = AtomicQueueMutexT<T, Mutex, SIZE, MINIMIZE_CONTENTION>;

template<class T, unsigned SIZE, bool MINIMIZE_CONTENTION = true>
using AtomicQueueSpinlock = AtomicQueueMutexT<T, Spinlock, SIZE, MINIMIZE_CONTENTION>;

// template<class T, unsigned SIZE, bool MINIMIZE_CONTENTION = true>
// using AtomicQueueSpinlockHle = AtomicQueueMutexT<T, SpinlockHle, SIZE, MINIMIZE_CONTENTION>;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

} // namespace atomic_queue

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#endif // ATOMIC_QUEUE_ATOMIC_QUEUE_SPIN_LOCK_H_INCLUDED
