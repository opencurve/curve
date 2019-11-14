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

using curve::common::ReadLockGuard;
using curve::common::WriteLockGuard;

namespace curve {
namespace client {
RWLock RequestClosure::rwLock_;
std::map<IOManagerID, InflightControl*> RequestClosure::inflightCntlMap_;

RequestClosure::RequestClosure(RequestContext* reqctx) {
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

void RequestClosure::SetFileMetric(FileMetric_t* fm) {
    metric_ = fm;
}

void RequestClosure::SetIOManagerID(IOManagerID id) {
    managerID_ = id;
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

int RequestClosure::AddInflightCntl(IOManagerID id,
                                     InFlightIOCntlInfo_t opt) {
    WriteLockGuard lk(rwLock_);
    auto it = inflightCntlMap_.find(id);
    if (it == inflightCntlMap_.end()) {
        auto cntl = new (std::nothrow) InflightControl();
        if (cntl == nullptr) {
            LOG(ERROR) << "InflightControl allocate failed!";
            return -1;
        }
        cntl->SetMaxInflightNum(opt.maxInFlightRPCNum);
        inflightCntlMap_.emplace(id, cntl);
    }
    return 0;
}

void RequestClosure::DeleteInflightCntl(IOManagerID id) {
    WriteLockGuard lk(rwLock_);
    auto it = inflightCntlMap_.find(id);
    if (it != inflightCntlMap_.end()) {
        delete it->second;
        inflightCntlMap_.erase(it);
    }
}

void RequestClosure::GetInflightRPCToken() {
    ReadLockGuard lk(rwLock_);
    auto it = inflightCntlMap_.find(managerID_);
    if (it != inflightCntlMap_.end()) {
        it->second->GetInflightToken();
        MetricHelper::IncremInflightRPC(metric_);
    }
}

void RequestClosure::ReleaseInflightRPCToken() {
    ReadLockGuard lk(rwLock_);
    auto it = inflightCntlMap_.find(managerID_);
    if (it != inflightCntlMap_.end()) {
        it->second->ReleaseInflightToken();
        MetricHelper::DecremInflightRPC(metric_);
    }
}
}   // namespace client
}   // namespace curve
