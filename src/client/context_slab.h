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

class RequestContextSlab {
 public:
    RequestContextSlab();
    virtual ~RequestContextSlab();

    virtual bool Initialize();
    virtual void UnInitialize();
    virtual RequestContext* Get();
    virtual void Recyle(RequestContext* torecyle);
    virtual bool PreAllocateInternal();
    virtual size_t Size();

 protected:
    SpinLock CURVE_CACHELINE_ALIGNMENT       spinlock_;
    std::list<RequestContext*>  CURVE_CACHELINE_ALIGNMENT contextslab_;
};


class IOContextSlab {
 public:
    IOContextSlab();
    CURVE_MOCK ~IOContextSlab();

    CURVE_MOCK bool Initialize();
    CURVE_MOCK void UnInitialize();
    CURVE_MOCK IOContext* Get();
    CURVE_MOCK void Recyle(IOContext* torecyle);
    CURVE_MOCK bool PreAllocateInternal();

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
