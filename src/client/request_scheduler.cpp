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
 * Created Date: 18-9-26
 * Author: wudemiao
 */

#include "src/client/request_scheduler.h"

#include <brpc/closure_guard.h>
#include <glog/logging.h>

#include "src/client/chunk_closure.h"
#include "src/client/request_closure.h"
#include "src/client/request_context.h"

namespace curve {
namespace client {

RequestScheduler::~RequestScheduler() {}

int RequestScheduler::Init(const RequestScheduleOption& reqSchdulerOpt,
                           MetaCache* metaCache, FileMetric* fm) {
    blockIO_.store(false);
    reqschopt_ = reqSchdulerOpt;

    int rc = 0;
    rc = queue_.Init(reqschopt_.scheduleQueueCapacity);
    if (0 != rc) {
        return -1;
    }

    rc = threadPool_.Init(reqschopt_.scheduleThreadpoolSize,
                          std::bind(&RequestScheduler::Process, this));
    if (0 != rc) {
        return -1;
    }

    rc = client_.Init(metaCache, reqschopt_.ioSenderOpt, this, fm);
    if (0 != rc) {
        return -1;
    }

    LOG(INFO) << "RequestScheduler conf info: "
              << "scheduleQueueCapacity = " << reqschopt_.scheduleQueueCapacity
              << ", scheduleThreadpoolSize = "
              << reqschopt_.scheduleThreadpoolSize;
    return 0;
}

int RequestScheduler::Run() {
    if (!running_.exchange(true, std::memory_order_acq_rel)) {
        stop_.store(false, std::memory_order_release);
        threadPool_.Start();
    }
    return 0;
}

int RequestScheduler::Fini() {
    if (running_.exchange(false, std::memory_order_acq_rel)) {
        for (int i = 0; i < threadPool_.NumOfThreads(); ++i) {
            // notify the wait thread
            BBQItem<RequestContext*> stopReq(nullptr, true);
            queue_.PutBack(stopReq);
        }
        threadPool_.Stop();
    }

    return 0;
}

int RequestScheduler::ScheduleRequest(
    const std::vector<RequestContext*>& requests) {
    if (running_.load(std::memory_order_acquire)) {
        /* TODO(wudemiao): Consider QoS in the later stage */
        for (auto it : requests) {
            // skip the fake request
            if (!it->idinfo_.chunkExist) {
                if (it->sourceInfo_.cloneFileSource.empty()) {
                    it->done_->Run();
                }
                continue;
            }

            BBQItem<RequestContext*> req(it);
            queue_.PutBack(req);
        }
        return 0;
    }
    return -1;
}

int RequestScheduler::ScheduleRequest(RequestContext* request) {
    if (running_.load(std::memory_order_acquire)) {
        BBQItem<RequestContext*> req(request);
        queue_.PutBack(req);
        return 0;
    }
    return -1;
}

int RequestScheduler::ReSchedule(RequestContext* request) {
    if (running_.load(std::memory_order_acquire)) {
        BBQItem<RequestContext*> req(request);
        queue_.PutFront(req);
        return 0;
    }
    return -1;
}

void RequestScheduler::WakeupBlockQueueAtExit() {
    // When the scheduler exits, it is necessary to clear the contents of the
    // queue and notify the copyset client The current operation is in the exit
    // state, and the copyset client will respond to the inflight RPC Under
    // normal circumstances, the queue content must be completely cleared after
    // Fini calls are completed But when the session refresh fails, the
    // scheduler cannot continue issuing RPC request, therefore blockingQueue
    // needs to be set_ Sign to inform scheduler Throw all the content in the
    // queue to the copyset client because in the session After the renewal
    // fails, the copyset client will return all IO failures to the scheduler
    // The module does not need to handle specific RPC requests, and is the
    // responsibility of the copyset client.
    client_.ResetExitFlag();
    blockingQueue_ = false;
    std::atomic_thread_fence(std::memory_order_acquire);
    leaseRefreshcv_.notify_all();
}

void RequestScheduler::Process() {
    while ((running_.load(std::memory_order_acquire) ||
            !queue_.Empty())  // flush all request in the queue
           && !stop_.load(std::memory_order_acquire)) {
        WaitValidSession();
        BBQItem<RequestContext*> item = queue_.TakeFront();
        if (!item.IsStop()) {
            RequestContext* req = item.Item();
            ProcessOne(req);
        } else {
            /**
             * Once a stop item is encountered, all threads can exit because at
             * this point All requests in the queue have been processed
             */
            stop_.store(true, std::memory_order_release);
        }
    }
}

void RequestScheduler::ProcessOne(RequestContext* ctx) {
    brpc::ClosureGuard guard(ctx->done_);

    switch (ctx->optype_) {
        case OpType::READ:
            ctx->done_->GetInflightRPCToken();
            client_.ReadChunk(ctx->idinfo_, ctx->seq_, ctx->offset_,
                              ctx->rawlength_, ctx->sourceInfo_,
                              guard.release());
            break;
        case OpType::WRITE:
            ctx->done_->GetInflightRPCToken();
            client_.WriteChunk(ctx->idinfo_, ctx->fileId_, ctx->epoch_,
                               ctx->seq_, ctx->writeData_, ctx->offset_,
                               ctx->rawlength_, ctx->sourceInfo_,
                               guard.release());
            break;
        case OpType::READ_SNAP:
            client_.ReadChunkSnapshot(ctx->idinfo_, ctx->seq_, ctx->offset_,
                                      ctx->rawlength_, guard.release());
            break;
        case OpType::DELETE_SNAP:
            client_.DeleteChunkSnapshotOrCorrectSn(
                ctx->idinfo_, ctx->correctedSeq_, guard.release());
            break;
        case OpType::GET_CHUNK_INFO:
            client_.GetChunkInfo(ctx->idinfo_, guard.release());
            break;
        case OpType::CREATE_CLONE:
            client_.CreateCloneChunk(ctx->idinfo_, ctx->location_, ctx->seq_,
                                     ctx->correctedSeq_, ctx->chunksize_,
                                     guard.release());
            break;
        case OpType::RECOVER_CHUNK:
            client_.RecoverChunk(ctx->idinfo_, ctx->offset_, ctx->rawlength_,
                                 guard.release());
            break;
        default:
            /* In the later stage of TODO(wudemiao), the entire link error was
             * sent and processed uniformly */
            ctx->done_->SetFailed(-1);
            LOG(ERROR) << "unknown op type: OpType::UNKNOWN";
    }
}

}  // namespace client
}  // namespace curve
