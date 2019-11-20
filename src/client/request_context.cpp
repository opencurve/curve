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

std::atomic<uint64_t> RequestContext::reqCtxID_(1);

RequestContext::RequestContext() {
    readBuffer_ = nullptr;
    writeBuffer_ = nullptr;
    chunkinfodetail_ = nullptr;

    id_         = reqCtxID_.fetch_add(1);

    seq_        = 0;
    offset_     = 0;
    rawlength_  = 0;

    appliedindex_ = 0;
}
bool RequestContext::Init() {
    done_ = new (std::nothrow) RequestClosure(this);
    return done_ != nullptr;
}

void RequestContext::UnInit() {
    delete done_;
}

RequestContext::~RequestContext() {
}
}  // namespace client
}  // namespace curve
