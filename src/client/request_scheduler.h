/*
 * Project: curve
 * Created Date: 18-9-26
 * Author: wudemiao
 * Copyright (c) 2018 netease
 */

#ifndef CURVE_CLIENT_REQUEST_SCHEULER_H
#define CURVE_CLIENT_REQUEST_SCHEULER_H

#include <list>

#include "src/common/uncopyable.h"
#include "src/common/bounded_blocking_queue.h"
#include "src/common/thread_pool.h"
#include "src/client/client_common.h"
#include "src/client/copyset_client.h"
#include "include/curve_compiler_specific.h"

namespace curve {
namespace client {

using curve::common::ThreadPool;
using curve::common::BoundedBlockingQueue;
using curve::common::BBQItem;
using curve::common::Uncopyable;

class RequestContext;

class RequestScheduler : public Uncopyable {
 public:
    RequestScheduler()
        : running_(false),
          stop_(true),
          client_() {}
    virtual ~RequestScheduler();

    CURVE_MOCK int Init(int capacity,
             int threadNums,
             RequestSenderManager *senderManager,
             MetaCache *metaCache);
    CURVE_MOCK int Run();
    CURVE_MOCK int Fini();
    CURVE_MOCK int ScheduleRequest(const std::list<RequestContext *> &requests);
    CURVE_MOCK int ScheduleRequest(RequestContext *request);

 private:
    void Process();

 private:
    BoundedBlockingQueue<BBQItem<RequestContext *>> queue_;
    ThreadPool threadPool_;
    std::atomic<bool> running_;
    std::atomic<bool> stop_;
    CopysetClient client_;
};

}   // namespace client
}   // namespace curve

#endif  // CURVE_CLIENT_REQUEST_SCHEULER_H
