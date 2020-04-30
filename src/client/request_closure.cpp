/*
 * Project: curve
 * File Created: Tuesday, 9th October 2018 7:03:23 pm
 * Author: tongguangxun
 * Copyright (c) 2018 NetEase
 */

#include "src/client/request_closure.h"
#include "src/client/io_tracker.h"
#include "src/client/request_context.h"
#include "src/client/chunk_closure.h"
#include "src/client/iomanager.h"

using curve::common::ReadLockGuard;
using curve::common::WriteLockGuard;

namespace curve {
namespace client {

RequestClosure::RequestClosure(RequestContext* reqctx) : ioManager_(nullptr) {
    suspendRPC_ = false;
    managerID_ = 0;
    retryTimes_ = 0;
    errcode_ = -1;
    reqCtx_ = reqctx;
    metric_ = nullptr;
    nextTimeoutMS_ = 0;
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
    ReleaseInflightRPCToken();
    if (suspendRPC_) {
        MetricHelper::DecremIOSuspendNum(metric_);
    }
    tracker_->HandleResponse(reqCtx_);
}

int RequestClosure::GetErrorCode() {
    return errcode_;
}

RequestContext* RequestClosure::GetReqCtx() {
    return reqCtx_;
}

void RequestClosure::SetFileMetric(FileMetric* fm) {
    metric_ = fm;
}

void RequestClosure::SetIOManager(IOManager* ioManager) {
    ioManager_ = ioManager;
}

FileMetric* RequestClosure::GetMetric() {
    return metric_;
}

void RequestClosure::SetStartTime(uint64_t start) {
    starttime_ = start;
}

uint64_t RequestClosure::GetStartTime() {
    return starttime_;
}

void RequestClosure::GetInflightRPCToken() {
    if (ioManager_ != nullptr) {
        ioManager_->GetInflightRpcToken();
        MetricHelper::IncremInflightRPC(metric_);
    }
}

void RequestClosure::ReleaseInflightRPCToken() {
    if (ioManager_ != nullptr) {
        ioManager_->ReleaseInflightRpcToken();
        MetricHelper::DecremInflightRPC(metric_);
    }
}
}   // namespace client
}   // namespace curve
