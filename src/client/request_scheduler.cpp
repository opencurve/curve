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

#include "src/client/request_context.h"
#include "src/client/request_closure.h"
#include "src/client/chunk_closure.h"

namespace curve {
namespace client {

RequestScheduler::~RequestScheduler() {}

int RequestScheduler::Init(const RequestScheduleOption& reqSchdulerOpt,
                           MetaCache* metaCache,
                           FileMetric* fm) {
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
              << "scheduleQueueCapacity = "
              << reqschopt_.scheduleQueueCapacity
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
            BBQItem<RequestContext *> stopReq(nullptr, true);
            queue_.PutBack(stopReq);
        }
        threadPool_.Stop();
    }

    return 0;
}

int RequestScheduler::ScheduleRequest(
    const std::vector<RequestContext*>& requests) {
    if (running_.load(std::memory_order_acquire)) {
        /* TODO(wudemiao): 后期考虑 qos */
        for (auto it : requests) {
            // skip the fake request
            if (!it->idinfo_.chunkExist) {
                if (it->sourceInfo_.cloneFileSource.empty()) {
                    it->done_->Run();
                }
                continue;
            }

            BBQItem<RequestContext *> req(it);
            queue_.PutBack(req);
        }
        return 0;
    }
    return -1;
}

int RequestScheduler::ScheduleRequest(RequestContext *request) {
    if (running_.load(std::memory_order_acquire)) {
        BBQItem<RequestContext *> req(request);
        queue_.PutBack(req);
        return 0;
    }
    return -1;
}

int RequestScheduler::ReSchedule(RequestContext *request) {
    if (running_.load(std::memory_order_acquire)) {
        BBQItem<RequestContext *> req(request);
        queue_.PutFront(req);
        return 0;
    }
    return -1;
}

void RequestScheduler::WakeupBlockQueueAtExit() {
    // 在scheduler退出的时候要把队列的内容清空, 通知copyset client
    // 当前操作是退出状态，copyset client会针对inflight RPC做响应处理
    // 正常情况下队列内容一定会在Fini调用结束之后全部清空
    // 但是在session刷新失败的时候，scheduler无法继续下发
    // RPC请求，所以需要设置blockingQueue_标志，告知scheduler
    // 把队列里内容统统扔到copyset client，因为在session
    // 续约失败后copyset client会将IO全部失败返回，scheduler
    // 模块不需要处理具体RPC请求，由copyset client负责。
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
             * 一旦遇到stop item，所有线程都可以退出，因为此时
             * queue里面所有的request都被处理完了
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
            client_.ReadChunk(ctx, guard.release());
            break;
        case OpType::WRITE:
            ctx->done_->GetInflightRPCToken();
            client_.WriteChunk(ctx, guard.release());
            break;
        case OpType::READ_SNAP:
            client_.ReadChunkSnapshot(
                ctx->idinfo_, ctx->seq_, ctx->snaps_, ctx->offset_,
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
            /* TODO(wudemiao) 后期整个链路错误发统一了在处理 */
            ctx->done_->SetFailed(-1);
            LOG(ERROR) << "unknown op type: OpType::UNKNOWN";
    }
}

}   // namespace client
}   // namespace curve
