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

#ifndef SRC_CLIENT_REQUEST_SCHEDULER_H_
#define SRC_CLIENT_REQUEST_SCHEDULER_H_

#include <vector>

#include "src/common/uncopyable.h"
#include "src/client/config_info.h"
#include "src/common/concurrent/bounded_blocking_queue.h"
#include "src/common/concurrent/thread_pool.h"
#include "src/client/client_common.h"
#include "src/client/copyset_client.h"
#include "include/curve_compiler_specific.h"

namespace curve {
namespace client {

using curve::common::ThreadPool;
using curve::common::BoundedBlockingDeque;
using curve::common::BBQItem;
using curve::common::Uncopyable;

struct RequestContext;
/**
 * 请求调度器，上层拆分的I/O会交给Scheduler的线程池
 * 分发到具体的ChunkServer，后期QoS也会放在这里处理
 */
class RequestScheduler : public Uncopyable {
 public:
    RequestScheduler()
        : running_(false),
          stop_(true),
          client_(),
          blockingQueue_(true) {}
    virtual ~RequestScheduler();

    /**
     * 初始化
     * @param: reqSchdulerOpt为scheduler的配置选项
     * @param: metacache为meta信息
     * @param: filematric为文件的metric信息
     */
    virtual int Init(const RequestScheduleOption& reqSchdulerOpt,
                     MetaCache *metaCache,
                     FileMetric* fileMetric = nullptr);
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
    virtual int ScheduleRequest(const std::vector<RequestContext*>& requests);

    /**
     * 将request push到Scheduler处理
     * @param request:一个request
     * @return 0成功，-1失败
     */
    virtual int ScheduleRequest(RequestContext *request);

    /**
     * 对于需要重新入队的RPC将其放在头部
     */
    virtual int ReSchedule(RequestContext *request);

    /**
     * 关闭scheduler之前如果队列在sessionnotvalid睡眠就将其唤醒
     */
    virtual void WakeupBlockQueueAtExit();

    /**
     * 当LeaseExecutor续约失败的时候，调用LeaseTimeoutDisableIO
     * 后续的IO调度会被阻塞
     */
    void LeaseTimeoutBlockIO() {
        std::unique_lock<std::mutex> lk(leaseRefreshmtx_);
        blockIO_.store(true);
        client_.StartRecycleRetryRPC();
    }

    /**
     * 当lease又续约成功的时候，LeaseExecutor调用该接口恢复IO,
     * IO调度被恢复
     */
    void ResumeIO() {
        std::unique_lock<std::mutex> lk(leaseRefreshmtx_);
        blockIO_.store(false);
        leaseRefreshcv_.notify_all();
        client_.ResumeRPCRetry();
    }

    /**
     * 测试使用，获取队列
     */
    BoundedBlockingDeque<BBQItem<RequestContext*>>* GetQueue() {
        return &queue_;
    }

 private:
    /**
     * Thread pool的运行函数，会从queue中取request进行处理
     */
    void Process();

    void ProcessAligned(RequestContext* ctx);

    void ProcessUnaligned(RequestContext* ctx);

    void WaitValidSession() {
        // lease续约失败的时候需要阻塞IO直到续约成功
        if (blockIO_.load(std::memory_order_acquire) && blockingQueue_) {
            std::unique_lock<std::mutex> lk(leaseRefreshmtx_);
            leaseRefreshcv_.wait(lk, [&]() -> bool {
                return !blockIO_.load() || !blockingQueue_;
            });
        }
    }

 private:
    // 线程池和queue容量的配置参数
    RequestScheduleOption reqschopt_;
    // 存放 request 的队列
    BoundedBlockingDeque<BBQItem<RequestContext *>> queue_;
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
    // 续约失败，卡住IO
    std::atomic<bool> blockIO_;
    // 此锁与LeaseRefreshcv_条件变量配合使用
    // 在leasee续约失败的时候，所有新下发的IO被阻塞直到续约成功
    std::mutex    leaseRefreshmtx_;
    // 条件变量，用于唤醒和hang IO
    std::condition_variable leaseRefreshcv_;
    // 阻塞队列
    bool blockingQueue_;
};

}   // namespace client
}   // namespace curve

#endif  // SRC_CLIENT_REQUEST_SCHEDULER_H_
