/*
 * Project: curve
 * File Created: Monday, 17th September 2018 3:26:18 pm
 * Author: tongguangxun
 * Copyright (c) 2018 NetEase
 */

#include <glog/logging.h>

#include "src/client/splitor.h"
#include "src/client/request_scheduler.h"
#include "src/client/io_context.h"
#include "src/client/request_closure.h"

namespace curve {
namespace client {

IOContext::IOContext(IOContextSlab* ioctxslab) {
    iocontextslab_ = ioctxslab;
    Reset();
}

IOContext::~IOContext() {
}

void IOContext::StartRead(CurveAioContext* aioctx,
                            MetaCache* mc,
                            RequestContextSlab* reqslab,
                            char* buf,
                            off_t offset,
                            size_t length) {
    data_ = buf;
    offset_ = offset;
    length_ = length;
    aioctx_ = aioctx;
    type_ = OpType::READ;
    reqlist_.clear();
    isReturned_.store(false, std::memory_order_release);

    int ret = -1;
    do {
        if (-1 == (ret = Splitor::IO2ChunkRequests(this,
                                                mc,
                                                reqslab,
                                                &reqlist_,
                                                data_,
                                                offset_,
                                                length_))) {
            LOG(ERROR) << "split READ io request failed!";
            break;
        }
        reqcount_ = reqlist_.size();
        if (-1 == (ret = scheduler_->ScheduleRequest(reqlist_))) {
            LOG(ERROR) << "Schedule request failed!";
            break;
        }
    } while (0);

    if (ret == -1) {
        LOG(ERROR) << "StartRead failed, return and recyle resource!";
        success_ = false;
        errorcode_ = -1;
        Done();
        RecycleSelf();
    }
}

void IOContext::StartWrite(CurveAioContext* aioctx,
                            MetaCache* mc,
                            RequestContextSlab* reqslab,
                            const char* buf,
                            off_t offset,
                            size_t length) {
    data_ = buf;
    offset_ = offset;
    length_ = length;
    aioctx_ = aioctx;
    type_ = OpType::WRITE;
    reqlist_.clear();
    isReturned_.store(false, std::memory_order_release);

    int ret = -1;
    do {
        if (-1 == (ret = Splitor::IO2ChunkRequests(this,
                                                mc,
                                                reqslab,
                                                &reqlist_,
                                                data_,
                                                offset_,
                                                length_))) {
            LOG(ERROR) << "split WRITE io request failed!";
            break;
        }
        reqcount_ = reqlist_.size();
        if (-1 == (ret = scheduler_->ScheduleRequest(reqlist_))) {
            LOG(ERROR) << "Schedule request failed!";
            break;
        }
    } while (0);

    if (ret == -1) {
        LOG(ERROR) << "StartWrite failed, return and recyle resource!";
        success_ = false;
        errorcode_ = -1;
        Done();
        RecycleSelf();
    }
}

void IOContext::HandleResponse(RequestContext* reqctx) {
    if (!isReturned_.load(std::memory_order_acquire)) {
        errorcode_ = reqctx->done_->GetErrorCode();
        success_ = (errorcode_ == 0);
        if (!success_) {
            LOG(WARNING) << "one of the request list got failed, "
                        << "return the result immediately!";
            Done();
        }
    }

    uint32_t ret = donecount_.fetch_add(1, std::memory_order_acq_rel);
    if (ret + 1 == reqcount_) {
        if (!isReturned_.load(std::memory_order_acquire)) {
            Done();
        }
        RecycleSelf();
    }
}

void IOContext::Reset() {
    isReturned_.store(false, std::memory_order_release);
    donecount_.store(0, std::memory_order_release);
    data_ = nullptr;
    offset_ = 0;
    length_ = 0;
    iocond_.Reset();
    aioctx_ = nullptr;
    success_ = true;
    errorcode_ = -1;
    reqcount_ = 0;
    type_ = OpType::UNKNOWN;
    reqlist_.clear();
}

int IOContext::Wait() {
    int ret = iocond_.Wait();;
    iocontextslab_->Recyle(this);
    return ret;
}

void IOContext::Done() {
    /**
     *  if aioctx_ is nullptr, the IO is sync mode 
     */
    if (aioctx_ == nullptr) {
        if (success_) {
            iocond_.Complete(length_);
        } else {
            /**
             * FIXME (tongguangxun): errorcode should be specify not confilct with 
             * length we just return -1 to user. latter we will and global error.
             */
            iocond_.Complete(-1);
        }
    } else {
        // TODO(tongguangxun): need set success and errorcode status
        aioctx_->err = errorcode_ == 0 ? LIBCURVE_ERROR_NOERROR
                     : LIBCURVE_ERROR_UNKNOWN;
        aioctx_->ret = length_;
        aioctx_->cb(aioctx_);
    }
    isReturned_.store(true, std::memory_order_release);
}

void IOContext::RecycleSelf() {
    for (auto iter : reqlist_) {
        iter->RecyleSelf();
    }
    if (aioctx_ != nullptr) {
        iocontextslab_->Recyle(this);
    }
}

void IOContext::SetScheduler(RequestScheduler* scheduler) {
    scheduler_ = scheduler;
}
}   // namespace client
}   // namespace curve
