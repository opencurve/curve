/*
 * Project: curve
 * File Created: Tuesday, 9th October 2018 7:03:23 pm
 * Author: tongguangxun
 * Copyright (c) 2018 NetEase
 */

#include "src/client/request_closure.h"
#include "src/client/io_context.h"
#include "src/client/request_context.h"

namespace curve {
namespace client {

RequestClosure::RequestClosure(RequestContext* reqctx) {
    errcode_ = -1;
    reqCtx_ = reqctx;
}

RequestClosure::~RequestClosure() {
    Reset();
}

void RequestClosure::SetIOContext(IOContext* ioctx) {
    ctx_ = ioctx;
}

void RequestClosure::SetFailed(int errorcode) {
    errcode_ = errorcode;
}

void RequestClosure::Run() {
    ctx_->HandleResponse(reqCtx_);
}

int RequestClosure::GetErrorCode() {
    return errcode_;
}

RequestContext* RequestClosure::GetReqCtx() {
    return reqCtx_;
}

void RequestClosure::Reset() {
    errcode_ = 0;
    ctx_ = nullptr;
}
}   // namespace client
}   // namespace curve
