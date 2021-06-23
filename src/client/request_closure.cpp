/*
 *  Copyright (c) 2020 NetEase Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

/*
 * Project: curve
 * File Created: Tuesday, 9th October 2018 7:03:23 pm
 * Author: tongguangxun
 */

#include "src/client/request_closure.h"

#include <memory>

#include "src/client/io_tracker.h"
#include "src/client/iomanager.h"
#include "src/client/request_context.h"

namespace curve {
namespace client {

void RequestClosure::Run() {
    ReleaseInflightRPCToken();
    if (suspendRPC_) {
        MetricHelper::DecremIOSuspendNum(metric_);
    }
    tracker_->HandleResponse(reqCtx_);
}

void RequestClosure::GetInflightRPCToken() {
    if (ioManager_ != nullptr) {
        ioManager_->GetInflightRpcToken();
        MetricHelper::IncremInflightRPC(metric_);
        ownInflight_ = true;
    }
}

void RequestClosure::ReleaseInflightRPCToken() {
    if (ioManager_ != nullptr && ownInflight_) {
        ioManager_->ReleaseInflightRpcToken();
        MetricHelper::DecremInflightRPC(metric_);
    }
}

PaddingReadClosure::PaddingReadClosure(RequestContext* requestCtx,
                                       RequestScheduler* scheduler)
    : RequestClosure(requestCtx), alignedCtx_(nullptr), scheduler_(scheduler) {
    GenAlignedRequest();
}

void PaddingReadClosure::Run() {
    std::unique_ptr<PaddingReadClosure> selfGuard(this);
    std::unique_ptr<RequestContext> ctxGuard(alignedCtx_);

    const int errCode = GetErrorCode();
    if (errCode != 0) {
        HandleError(errCode);
        return;
    }

    switch (reqCtx_->optype_) {
        case OpType::READ:
            HandleRead();
            break;
        case OpType::WRITE:
            HandleWrite();
            break;
        default:
            HandleError(-1);
            LOG(ERROR) << "Unexpected original request optype: "
                       << static_cast<int>(reqCtx_->optype_)
                       << ", request context: " << *reqCtx_;
    }
}

void PaddingReadClosure::HandleRead() {
    brpc::ClosureGuard doneGuard(reqCtx_->done_);

    // copy data to original read request
    auto nc = alignedCtx_->readData_.append_to(
        &reqCtx_->readData_, reqCtx_->rawlength_,
        reqCtx_->offset_ - alignedCtx_->offset_);

    if (nc != reqCtx_->rawlength_) {
        LOG(ERROR) << "Copy read data failed, coyied bytes: " << nc
                   << ", expected bytes: " << reqCtx_->rawlength_;
        reqCtx_->done_->SetFailed(-1);
    } else {
        reqCtx_->done_->SetFailed(0);
    }
}

void PaddingReadClosure::HandleWrite() {
    // padding data
    butil::IOBuf alignedData;
    uint64_t bytes = 0;
    uint64_t pos = 0;

    switch (reqCtx_->padding.type) {
        case RequestContext::Padding::Left:
            alignedCtx_->readData_.append_to(
                &alignedData, reqCtx_->offset_ - alignedCtx_->offset_);
            reqCtx_->writeData_.append_to(&alignedData, reqCtx_->rawlength_);

            reqCtx_->offset_ = alignedCtx_->offset_;
            reqCtx_->rawlength_ = alignedData.size();
            break;
        case RequestContext::Padding::Right:
            reqCtx_->writeData_.append_to(&alignedData, reqCtx_->rawlength_);
            bytes = (alignedCtx_->offset_ + alignedCtx_->rawlength_) -
                    (reqCtx_->offset_ + reqCtx_->rawlength_);
            alignedCtx_->readData_.append_to(&alignedData, bytes,
                                             alignedCtx_->rawlength_ - bytes);

            reqCtx_->rawlength_ = alignedData.size();
            break;
        case RequestContext::Padding::ALL:
            bytes = reqCtx_->offset_ - alignedCtx_->offset_;
            alignedCtx_->readData_.append_to(&alignedData, bytes);
            pos += bytes;
            reqCtx_->writeData_.append_to(&alignedData, reqCtx_->rawlength_);
            pos += reqCtx_->rawlength_;
            bytes = alignedCtx_->rawlength_ - bytes;
            alignedCtx_->readData_.append_to(&alignedData, bytes, pos);

            reqCtx_->offset_ = alignedCtx_->offset_;
            reqCtx_->rawlength_ = alignedData.size();
            break;
        default:
            break;
    }

    // mark original request aligned and swap data
    reqCtx_->padding.aligned = true;
    reqCtx_->writeData_.swap(alignedData);

    // reschedule original requst
    int ret = scheduler_->ReSchedule(reqCtx_);
    if (ret != 0) {
        LOG(ERROR) << "Reschedule original request failed";
        reqCtx_->done_->SetFailed(-1);
        reqCtx_->done_->Run();
    }
}

void PaddingReadClosure::GenAlignedRequest() {
    alignedCtx_ = new RequestContext();

    alignedCtx_->optype_ = OpType::READ;  // set to read
    alignedCtx_->padding.aligned = true;  // set to aligned

    // copy request context
    alignedCtx_->idinfo_ = reqCtx_->idinfo_;
    alignedCtx_->offset_ = reqCtx_->padding.offset;
    alignedCtx_->rawlength_ = reqCtx_->padding.length;
    alignedCtx_->done_ = this;
    alignedCtx_->seq_ = reqCtx_->seq_;
    alignedCtx_->appliedindex_ = reqCtx_->appliedindex_;
    alignedCtx_->chunksize_ = reqCtx_->chunksize_;
    alignedCtx_->location_ = reqCtx_->location_;
    alignedCtx_->sourceInfo_ = reqCtx_->sourceInfo_;
    alignedCtx_->correctedSeq_ = reqCtx_->correctedSeq_;
    alignedCtx_->id_ = RequestContext::GetNextRequestContextId();
}

void PaddingReadClosure::HandleError(int errCode) {
    brpc::ClosureGuard doneGuard(reqCtx_->done_);
    reqCtx_->done_->SetFailed(errCode);

    LOG(ERROR) << "Padding read request failed, request: " << *alignedCtx_
               << ", error: " << errCode;
}

}  // namespace client
}  // namespace curve
