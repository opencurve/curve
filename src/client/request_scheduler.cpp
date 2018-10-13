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

int RequestScheduler::Init(int capacity,
                           int threadNums,
                           RequestSenderManager *senderManager,
                           MetaCache *metaCache) {
    if (0 != queue_.Init(capacity)) {
        return -1;
    }
    if (0 != threadPool_.Init(threadNums,
                              std::bind(&RequestScheduler::Process, this))) {
        return -1;
    }
    if (0 != client_.Init(senderManager, metaCache)) {
        return -1;
    }
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
            /* notify the wait thread */
            BBQItem<RequestContext *> stopReq(nullptr, true);
            queue_.Put(stopReq);
        }
        threadPool_.Stop();
    }
    return 0;
}

int RequestScheduler::ScheduleRequest(const std::list<RequestContext *> &requests) {   //NOLINT
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
        || !queue_.Empty())  /* clear all request in the queue */
        && !stop_.load(std::memory_order_acquire)) {
        BBQItem<RequestContext *> item = queue_.Take();
        if (!item.IsStop()) {
            RequestContext *req = item.Item();
            brpc::ClosureGuard guard(req->done_);

            switch (req->optype_) {
                case OpType::READ:
                    client_.ReadChunk(req->logicpoolid_,
                                      req->copysetid_,
                                      req->chunkid_,
                                      req->offset_,
                                      req->rawlength_,
                                      guard.release());
                    break;
                case OpType::WRITE:
                    client_.WriteChunk(req->logicpoolid_,
                                       req->copysetid_,
                                       req->chunkid_,
                                       req->data_,
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
            /* 一旦遇到 stop item，所有线程都可以退出 */
            stop_.store(true, std::memory_order_release);
        }
    }
}

}   // namespace client
}   // namespace curve
