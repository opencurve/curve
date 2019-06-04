/*
 * Project: curve
 * File Created: Tuesday, 9th October 2018 7:03:23 pm
 * Author: tongguangxun
 * Copyright (c) 2018 NetEase
 */

#include "src/client/request_closure.h"
#include "src/client/io_tracker.h"
#include "src/client/request_context.h"

namespace curve {
namespace client {

RequestClosure::RequestClosure(RequestContext* reqctx) {
    errcode_ = -1;
    reqCtx_ = reqctx;
    metric_ = nullptr;
}

IOTracker* RequestClosure::GetIOTracker() {
    return tracker_;
}

void RequestClosure::SetIOTracker(IOTracker* track) {
    tracker_ = track;
}

void RequestClosure::SetFailed(int errorcode) {
    errcode_ = errorcode;
}

void RequestClosure::Run() {
    tracker_->HandleResponse(reqCtx_);
}

int RequestClosure::GetErrorCode() {
    return errcode_;
}

RequestContext* RequestClosure::GetReqCtx() {
    return reqCtx_;
}

void RequestClosure::SetFileMetric(FileMetric_t* fm) {
    metric_ = fm;
}

FileMetric_t* RequestClosure::GetMetric() {
    return metric_;
}

void RequestClosure::SetStartTime(uint64_t start) {
    starttime_ = start;
}

uint64_t RequestClosure::GetStartTime() {
    return starttime_;
}

}   // namespace client
}   // namespace curve
