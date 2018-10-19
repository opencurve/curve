/*
 * Project: curve
 * File Created: Tuesday, 18th September 2018 3:41:14 pm
 * Author: tongguangxun
 * Copyright (c) 2018 NetEase
 */

#include <mutex>    //NOLINT
#include <memory>
#include <algorithm>

#include "src/client/context_slab.h"
#include "src/client/io_context.h"
#include "src/client/request_context.h"

DEFINE_int32(pre_allocate_context_num,
            1024,
            "preallocate context struct, in case frequently new and free");


namespace curve {
namespace client {
    IOContextSlab::IOContextSlab() {
        infilghtIOContextNum_.store(0, std::memory_order_release);
    }

    IOContextSlab::~IOContextSlab() {
    }

    bool IOContextSlab::Initialize() {
        return PreAllocateInternal();
    }

    bool IOContextSlab::PreAllocateInternal() {
        for (int i = 0; i < FLAGS_pre_allocate_context_num; i++) {
            IOContext* temp = new (std::nothrow) IOContext(this);
            if (CURVE_UNLIKELY(temp == nullptr)) {
                UnInitialize();
                return false;
            } else {
                contextslab_.push_front(temp);
            }
        }
        return true;
    }

    IOContext* IOContextSlab::Get() {
        LOCK_HERE
        IOContext* temp = nullptr;
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
            if (!temp->IsBusy()) {
                contextslab_.pop_front();
                UNLOCK_HERE
                infilghtIOContextNum_.fetch_add(1,
                        std::memory_order_acq_rel);
                return temp;
            }
        }

        temp = new (std::nothrow) IOContext(this);
        if (temp == nullptr) {
            abort();
        }
        UNLOCK_HERE
        infilghtIOContextNum_.fetch_add(1, std::memory_order_acq_rel);
        return temp;
    }

    void IOContextSlab::UnInitialize() {
        while (infilghtIOContextNum_.load(std::memory_order_acquire)) {
            std::this_thread::sleep_for(std::chrono::milliseconds(500));
        }

        std::for_each(contextslab_.begin(),
                    contextslab_.end(),
                    [](IOContext* ctx){
            delete ctx;
        });
        contextslab_.clear();
    }

    void IOContextSlab::Recyle(IOContext* torecyle) {
        LOCK_HERE
        infilghtIOContextNum_.fetch_sub(1, std::memory_order_acq_rel);
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
}   // namespace client
}   // namespace curve
