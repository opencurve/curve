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

namespace curve {
namespace client {

RequestScheduler::~RequestScheduler() {
}

int RequestScheduler::Init(const RequestScheduleOption_t& reqSchdulerOpt,
                           MetaCache *metaCache,
                           FileMetric_t* fm) {
    blockIO_.store(false);
    reqschopt_ = reqSchdulerOpt;
    // 调度队列的深度会影响client端整体吞吐，这个队列存放的是异步IO任务。
    // 队列深度与maxInFlightIONum数量有关系，其深度应该大于等于maxInFlightIONum
    // 因为如果小于maxInFlightIONum，client端的整体pipeline效果就会受到影响。
    if (0 != queue_.Init(reqschopt_.queueCapacity)) {
        return -1;
    }
    if (0 != threadPool_.Init(reqschopt_.threadpoolSize,
                              std::bind(&RequestScheduler::Process, this))) {
        return -1;
    }
    if (0 != client_.Init(metaCache, reqschopt_.ioSenderOpt, fm)) {
        return -1;
    }
    confMetric_.queueCapacity.set_value(reqschopt_.queueCapacity);
    confMetric_.threadpoolSize.set_value(reqschopt_.threadpoolSize);

    LOG(INFO) << "RequestScheduler conf info: "
              << "queueCapacity = " << reqschopt_.queueCapacity
              << ", threadpoolSize = " << reqschopt_.threadpoolSize;
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
            queue_.Put(stopReq);
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
            queue_.Put(req);
        }
        return 0;
    }
    return -1;
}

int RequestScheduler::ScheduleRequest(RequestContext *request) {
    if (running_.load(std::memory_order_acquire)) {
        BBQItem<RequestContext *> req(request);
        queue_.Put(req);
        return 0;
    }
    return -1;
}

void RequestScheduler::Process() {
    while ((running_.load(std::memory_order_acquire)
        || !queue_.Empty())  // clear all request in the queue
        && !stop_.load(std::memory_order_acquire)) {
        BBQItem<RequestContext *> item = queue_.Take();
        if (!item.IsStop()) {
            // 在发送IO之前先获取token
            // 在Take操作之后gettoken防止无法结束scheduler
            GetIOToken();
            RequestContext *req = item.Item();
            brpc::ClosureGuard guard(req->done_);

            switch (req->optype_) {
                case OpType::READ:
                    DVLOG(9) << "Processing read request, buf header: "
                             << " buf: " << *(unsigned int*)req->readBuffer_;
                    client_.ReadChunk(req->idinfo_,
                                      req->seq_,
                                      req->offset_,
                                      req->rawlength_,
                                      req->appliedindex_,
                                      guard.release());
                    break;
                case OpType::WRITE:
                    DVLOG(9) << "Processing write request, buf header: "
                             << " buf: " << *(unsigned int*)req->writeBuffer_;
                    client_.WriteChunk(req->idinfo_,
                                       req->seq_,
                                       req->writeBuffer_,
                                       req->offset_,
                                       req->rawlength_,
                                       guard.release());
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
