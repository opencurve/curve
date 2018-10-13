/*
 * Project: curve
 * File Created: Monday, 17th September 2018 4:19:54 pm
 * Author: tongguangxun
 * Copyright (c) 2018 NetEase
 */

#include <gflags/gflags.h>
#include <glog/logging.h>

#include "src/client/request_context.h"
#include "src/client/request_closure.h"

namespace curve {
namespace client {

RequestContext::RequestContext(RequestContextSlab* reqctxslab) {
    done_ = new (std::nothrow) RequestClosure(this);
    if (done_ == nullptr) {
        LOG(ERROR) << "allocate request_closure failed!";
    }
    Reset();
    reqctxslab_ = reqctxslab;
}

RequestContext::~RequestContext() {
    if (done_ != nullptr) {
        delete done_;
    }
    done_ = nullptr;
}

void RequestContext::RecyleSelf() {
    reqctxslab_->Recyle(this);
}

void RequestContext::Reset() {
    data_ = nullptr;
    offset_ = 0;
    rawlength_ = 0;
    chunkid_ = 0;
    copysetid_ = 0;
    logicpoolid_ = 0;
    done_->Reset();
}
}  // namespace client
}  // namespace curve
