/*
 * Project: curve
 * File Created: Tuesday, 18th September 2018 3:41:02 pm
 * Author: tongguangxun
 * Copyright (c) 2018 NetEase
 */
#ifndef CURVE_IOCONTEXT_CACHE_H
#define CURVE_IOCONTEXT_CACHE_H

#include <gflags/gflags.h>

#include <iostream>
#include <memory>
#include <list>
#include <atomic>
#include <mutex>     // NOLINT
#include <thread>   //NOLINT
#include <chrono>   //NOLINT
#include <condition_variable>    // NOLINT

#include "src/client/client_config.h"
#include "src/common/spinlock.h"
#include "include/curve_compiler_specific.h"

using curve::common::SpinLock;
using curve::client::ClientConfig;

namespace curve {
namespace client {

class IOTracker;
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

 private:
    uint64_t   pre_allocate_context_num_;
    CURVE_CACHELINE_ALIGNMENT SpinLock spinlock_;
    CURVE_CACHELINE_ALIGNMENT std::list<RequestContext*> contextslab_;
};

class IOTrackerSlab {
 public:
    IOTrackerSlab();
    virtual ~IOTrackerSlab();

    virtual bool Initialize();
    virtual void UnInitialize();
    virtual IOTracker* Get();
    virtual void Recyle(IOTracker* torecyle);
    virtual bool PreAllocateInternal();

    inline void WaitInflightIOComeBack() {
       waitinflightio_.store(true, std::memory_order_relaxed);
    }

    inline size_t Size() {
       return contextslab_.size();
    }

    inline void IncremInflightIONum() {
       inflightio_.fetch_add(1, std::memory_order_relaxed);
    }

    inline void DecremInflightIONum() {
       {
          std::unique_lock<std::mutex> lk(mtx_);
          inflightio_.fetch_sub(1, std::memory_order_relaxed);
       }
       waitcv_.notify_one();
    }

 private:
    void WaitInternal() {
       {
          std::unique_lock<std::mutex> lk(mtx_);
          waitcv_.wait(lk, [this]()->bool{
                return inflightio_.load(std::memory_order_relaxed) == 0;
          });
       }
       waitinflightio_.store(false, std::memory_order_relaxed);
    }

    // inflight io
    std::mutex  mtx_;
    std::condition_variable waitcv_;
    uint64_t   pre_allocate_context_num_;
    CURVE_CACHELINE_ALIGNMENT std::atomic<bool> waitinflightio_;
    CURVE_CACHELINE_ALIGNMENT std::atomic<uint64_t> inflightio_;

    CURVE_CACHELINE_ALIGNMENT SpinLock spinlock_;
    CURVE_CACHELINE_ALIGNMENT std::list<IOTracker*> contextslab_;
};
}   // namespace client
}   // namespace curve
#endif  // !CURVE_IOCONTEXT_CACHE_H
