/*
 * Project: curve
 * File Created: Tuesday, 18th September 2018 3:41:14 pm
 * Author: tongguangxun
 * Copyright (c) 2018 NetEase
 */

#include <mutex>    //NOLINT
#include <memory>
#include <algorithm>

#include "src/client/client_common.h"
#include "test/backup/context_slab.h"
#include "src/client/io_tracker.h"
#include "src/client/request_context.h"

namespace curve {
namespace client {
    RequestContextSlab::RequestContextSlab() {
    }

    RequestContextSlab::~RequestContextSlab() {
    }

    bool RequestContextSlab::Initialize() {
        pre_allocate_context_num_ = ClientConfig::GetContextSlabOption().
                                    pre_allocate_context_num;
        return PreAllocateInternal();
    }

    void RequestContextSlab::UnInitialize() {
        spinlock_.Lock();
        std::for_each(contextslab_.begin(), contextslab_.end(), [](RequestContext* ctx){    // NOLINT
            delete ctx;
        });
        contextslab_.clear();
        spinlock_.UnLock();
    }

    size_t RequestContextSlab::Size() {
        spinlock_.Lock();
        size_t size = contextslab_.size();
        spinlock_.UnLock();
        return size;
    }

    RequestContext* RequestContextSlab::Get() {
        spinlock_.Lock();
        RequestContext* temp = nullptr;
        if (CURVE_LIKELY(!contextslab_.empty())) {
            temp = contextslab_.front();
            contextslab_.pop_front();
        } else {
            temp = new (std::nothrow) RequestContext(this);
        }
        spinlock_.UnLock();
        return temp;
    }

    void RequestContextSlab::Recyle(RequestContext* torecyle) {
        spinlock_.Lock();
        torecyle->Reset();
        contextslab_.push_front(torecyle);
        while (contextslab_.size() > 2 * pre_allocate_context_num_) {
            auto temp = contextslab_.front();
            contextslab_.pop_front();
            delete temp;
        }
        spinlock_.UnLock();
    }

    bool RequestContextSlab::PreAllocateInternal() {
        for (int i = 0; i < pre_allocate_context_num_; i++) {
            RequestContext* temp = new (std::nothrow) RequestContext(this);
            contextslab_.push_front(temp);
        }
        return true;
    }

    IOTrackerSlab::IOTrackerSlab():
                    waitinflightio_(false),
                    inflightio_(0) {
    }

    IOTrackerSlab::~IOTrackerSlab() {
    }

    bool IOTrackerSlab::Initialize() {
        pre_allocate_context_num_ = ClientConfig::GetContextSlabOption().
                                    pre_allocate_context_num;
        return PreAllocateInternal();
    }

    bool IOTrackerSlab::PreAllocateInternal() {
        for (int i = 0; i < pre_allocate_context_num_; i++) {
            IOTracker* temp = new (std::nothrow) IOTracker(this);
            contextslab_.push_front(temp);
        }
        return true;
    }

    IOTracker* IOTrackerSlab::Get() {
        spinlock_.Lock();
        if (waitinflightio_.load(std::memory_order_relaxed)) {
            spinlock_.UnLock();
            WaitInternal();
            spinlock_.Lock();
        }
        IOTracker* temp = nullptr;
        if (CURVE_LIKELY(!contextslab_.empty())) {
            temp = contextslab_.front();
            /**
             * only the io context is not busy
             * we can reuse again, in some case
             * the IO has return but the context
             * dose not idle, such as the request
             * list not all success. in this case
             * we will return the IO immediately,
             * but the context will reuse when all
             * request_context come back.
             * in sync mode, IO context return and
             * recyle self may happen concurrently.
             * is this case, we should hold the IsBusy
             * flag until sync mode return.
             */ 
            if (CURVE_UNLIKELY(!temp->IsBusy())) {
                contextslab_.pop_front();
                spinlock_.UnLock();
                IncremInflightIONum();
                return temp;
            }
        }

        temp = new (std::nothrow) IOTracker(this);
        spinlock_.UnLock();
        IncremInflightIONum();
        return temp;
    }

    void IOTrackerSlab::UnInitialize() {
        WaitInternal();

        spinlock_.Lock();
        std::for_each(contextslab_.begin(), contextslab_.end(), [](IOTracker* ctx){     // NOLINT
            delete ctx;
        });
        contextslab_.clear();
        spinlock_.UnLock();
    }

    void IOTrackerSlab::Recyle(IOTracker* torecyle) {
        spinlock_.Lock();
        torecyle->Reset();
        contextslab_.push_front(torecyle);
        DecremInflightIONum();
        while (contextslab_.size() > 2 * pre_allocate_context_num_) {
            auto temp = contextslab_.front();
            contextslab_.pop_front();
            delete temp;
        }
        spinlock_.UnLock();
    }
}   // namespace client
}   // namespace curve
