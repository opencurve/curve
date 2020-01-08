/*
 * Project: curve
 * Created Date: 18-9-26
 * Author: wudemiao
 * Copyright (c) 2018 netease
 */

#include "src/client/request_scheduler.h"

#include <brpc/closure_guard.h>
#include <glog/logging.h>

#include "src/client/request_context.h"
#include "src/client/request_closure.h"
#include "src/client/chunk_closure.h"

namespace curve {
namespace client {

RequestScheduler::~RequestScheduler() {
}

int RequestScheduler::Init(const RequestScheduleOption_t& reqSchdulerOpt,
                           MetaCache *metaCache,
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

int RequestScheduler::ScheduleRequest(const std::list<RequestContext *> requests) {   //NOLINT
    if (running_.load(std::memory_order_acquire)) {
        /* TODO(wudemiao): 后期考虑 qos */
        for (auto it : requests) {
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
    while ((running_.load(std::memory_order_acquire)
        || !queue_.Empty())  // flush all request in the queue
        && !stop_.load(std::memory_order_acquire)) {
        WaitValidSession();
        BBQItem<RequestContext *> item = queue_.TakeFront();
        if (!item.IsStop()) {
            RequestContext *req = item.Item();
            brpc::ClosureGuard guard(req->done_);
            switch (req->optype_) {
                case OpType::READ:
                    DVLOG(9) << "Processing read request, buf header: "
                             << " buf: " << *(unsigned int*)req->readBuffer_;
                    {
                        req->done_->GetInflightRPCToken();
                        client_.ReadChunk(req->idinfo_,
                                        req->seq_,
                                        req->offset_,
                                        req->rawlength_,
                                        req->appliedindex_,
                                        guard.release());
                    }
                    break;
                case OpType::WRITE:
                    DVLOG(9) << "Processing write request, buf header: "
                             << " buf: " << *(unsigned int*)req->writeBuffer_;
                    {
                        req->done_->GetInflightRPCToken();
                        client_.WriteChunk(req->idinfo_,
                                        req->seq_,
                                        req->writeBuffer_,
                                        req->offset_,
                                        req->rawlength_,
                                        guard.release());
                    }
                    break;
                case OpType::READ_SNAP:
                    client_.ReadChunkSnapshot(req->idinfo_,
                                        req->seq_,
                                        req->offset_,
                                        req->rawlength_,
                                        guard.release());
                    break;
                case OpType::DELETE_SNAP:
                    client_.DeleteChunkSnapshotOrCorrectSn(req->idinfo_,
                                        req->correctedSeq_,
                                        guard.release());
                    break;
                case OpType::GET_CHUNK_INFO:
                    client_.GetChunkInfo(req->idinfo_,
                                        guard.release());
                    break;
                case OpType::CREATE_CLONE:
                    client_.CreateCloneChunk(req->idinfo_,
                                        req->location_,
                                        req->seq_,
                                        req->correctedSeq_,
                                        req->chunksize_,
                                        guard.release());
                    break;
                case OpType::RECOVER_CHUNK:
                    client_.RecoverChunk(req->idinfo_,
                                        req->offset_,
                                        req->rawlength_,
                                        guard.release());
                    break;
                default:
                    /* TODO(wudemiao) 后期整个链路错误发统一了在处理 */
                    req->done_->SetFailed(-1);
                    LOG(ERROR) << "unknown op type: OpType::UNKNOWN";
            }
        } else {
            /**
             * 一旦遇到stop item，所有线程都可以退出，因为此时
             * queue里面所有的request都被处理完了
             */
            stop_.store(true, std::memory_order_release);
        }
    }
}

}   // namespace client
}   // namespace curve
