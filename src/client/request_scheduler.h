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
#include "src/client/config_info.h"
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
/**
 * 请求调度器，上层拆分的I/O会交给Scheduler的线程池
 * 分发到具体的ChunkServer，后期QoS也会放在这里处理
 */
class RequestScheduler : public Uncopyable {
 public:
    RequestScheduler()
        : running_(false),
          stop_(true),
          client_() {}
    virtual ~RequestScheduler();

    virtual int Init(RequestScheduleOption_t reqSchdulerOpt,
                     MetaCache *metaCache);
    /**
     * 启动Scheduler的线程池开始处理request
     * 启动之后才能push request，除此之外，只有当
     * queue里面的任务都被处理完了，才会Scheduler
     * 的 thread pool里面的所有线程都退出
     * @return 0成功，-1失败
     */
    virtual int Run();

    /**
     * Stop Scheduler，一旦调用了Fini，那么
     * 此Scheduler不再接收新的request
     * @return 0成功，-1失败
     */
    virtual int Fini();

    /**
     * 将request push到Scheduler处理
     * @param requests:请求列表
     * @return 0成功，-1失败
     */
    virtual int ScheduleRequest(const std::list<RequestContext *> &requests);

    /**
     * 将request push到Scheduler处理
     * @param request:一个request
     * @return 0成功，-1失败
     */
    virtual int ScheduleRequest(RequestContext *request);

 private:
    /**
     * Thread pool的运行函数，会从queue中取request进行处理
     */
    void Process();

 private:
    // 线程池和queue容量的配置参数
    RequestScheduleOption_t reqschopt_;
    // 存放 request 的队列
    BoundedBlockingQueue<BBQItem<RequestContext *>> queue_;
    // 处理 request 的线程池
    ThreadPool threadPool_;
    // Scheduler 运行标记，只有运行了，才接收 request
    std::atomic<bool> running_;
    // stop thread pool 标记，当调用 Scheduler Fini
    // 之后且 queue 里面的 request 都处理完了，就可以
    // 让所有处理线程退出了
    std::atomic<bool> stop_;
    // 访问复制组Chunk的客户端
    CopysetClient client_;
};

}   // namespace client
}   // namespace curve

#endif  // CURVE_CLIENT_REQUEST_SCHEULER_H
