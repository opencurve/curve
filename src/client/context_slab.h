/*
 * Project: curve
 * File Created: Tuesday, 18th September 2018 3:41:02 pm
 * Author: tongguangxun
 * Copyright (c) 2018 NetEase
 */
#ifndef CURVE_IOCONTEXT_CACHE_H
#define CURVE_IOCONTEXT_CACHE_H

#include <gflags/gflags.h>

#include <memory>
#include <list>
#include <atomic>
#include <thread>   //NOLINT
#include <chrono>   //NOLINT

#include "src/common/spinlock.h"
#include "include/curve_compiler_specific.h"

DECLARE_int32(pre_allocate_context_num);

using curve::common::SpinLock;

/**
 * io context cache serve like simple linux slab
 */
namespace curve {
namespace client {

#define LOCK_HERE   spinlock_.Lock();
#define UNLOCK_HERE spinlock_.UnLock();

class IOContext;
class RequestContext;

template <class T>
class ContextSlab {
 public:
    ContextSlab() {
    }
    virtual ~ContextSlab() {
        UnInitialize();
    }

    virtual bool Initialize() {
        return PreAllocateInternal();
    }

    virtual void UnInitialize() {
        std::for_each(contextslab_.begin(), contextslab_.end(), [](T* ctx){
            delete ctx;
        });
        contextslab_.clear();
    }

    virtual size_t Size() {
        return contextslab_.size();
    }

    virtual T* Get() {
        LOCK_HERE
        T* temp = nullptr;
        if (CURVE_LIKELY(!contextslab_.empty())) {
            temp = contextslab_.front();
            contextslab_.pop_front();
        } else {
            temp = new (std::nothrow) T(this);
            if (temp == nullptr) {
                abort();
            }
        }
        UNLOCK_HERE
        return temp;
    }

    virtual void Recyle(T* torecyle) {
        LOCK_HERE
        torecyle->Reset();
        contextslab_.push_front(torecyle);
        while (contextslab_.size() >
                2 * FLAGS_pre_allocate_context_num) {
            auto temp = contextslab_.front();
            contextslab_.pop_front();
            delete temp;
        }
        UNLOCK_HERE
    }

    virtual bool PreAllocateInternal() {
        for (int i = 0; i < FLAGS_pre_allocate_context_num; i++) {
            T* temp = new (std::nothrow) T(this);
            if (CURVE_UNLIKELY(temp == nullptr)) {
                UnInitialize();
                return false;
            } else {
                contextslab_.push_front(temp);
            }
        }
        return true;
    }

 protected:
    SpinLock CURVE_CACHELINE_ALIGNMENT       spinlock_;
    std::list<T*>  CURVE_CACHELINE_ALIGNMENT contextslab_;
};


class IOContextSlab {
 public:
    IOContextSlab();
    ~IOContextSlab();

    bool Initialize();
    void UnInitialize();
    IOContext* Get();
    void Recyle(IOContext* torecyle);
    bool PreAllocateInternal();

    size_t Size() {
        return contextslab_.size();
    }
 private:
    std::atomic<uint64_t> infilghtIOContextNum_;
    SpinLock CURVE_CACHELINE_ALIGNMENT       spinlock_;
    std::list<IOContext*>  CURVE_CACHELINE_ALIGNMENT contextslab_;
};
}   // namespace client
}   // namespace curve
#endif  // !CURVE_IOCONTEXT_CACHE_H
