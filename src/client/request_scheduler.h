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
 *Request the scheduler, and the split I/O from the upper layer will be handed over to the scheduler's thread pool
 *Distribute to specific ChunkServers, where QoS will also be handled in the future
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
     *Initialize
     * @param: reqSchdulerOpt is the configuration option for the scheduler
     * @param: metaCache is the meta information
     * @param: fileMetric is the metric information of the file
     */
    virtual int Init(const RequestScheduleOption& reqSchdulerOpt,
                     MetaCache *metaCache,
                     FileMetric* fileMetric = nullptr);
    /**
     *Starting the thread pool of the scheduler to start processing requests
     *After startup, the request can only be pushed. In addition, only when
     *Only when all the tasks in the queue are processed can the scheduler
     *All threads in the thread pool exit
     * @return 0 succeeded, -1 failed
     */
    virtual int Run();

    /**
     *Stop Scheduler, once Fini is called, then
     *This scheduler no longer receives new requests
     * @return 0 succeeded, -1 failed
     */
    virtual int Fini();

    /**
     *Push the request to the scheduler for processing
     * @param requests: Request List
     * @return 0 succeeded, -1 failed
     */
    virtual int ScheduleRequest(const std::vector<RequestContext*>& requests);

    /**
     *Push the request to the scheduler for processing
     * @param request: A request
     * @return 0 succeeded, -1 failed
     */
    virtual int ScheduleRequest(RequestContext *request);

    /**
     *For RPCs that need to be re queued, place them at the top
     */
    virtual int ReSchedule(RequestContext *request);

    /**
     *Before closing the scheduler, if the queue is in sessionnotvalid, wake it up
     */
    virtual void WakeupBlockQueueAtExit();

    /**
     *When LeaseExecutor renewal fails, call LeaseTimeoutDisableIO
     *Subsequent IO scheduling will be blocked
     */
    void LeaseTimeoutBlockIO() {
        std::unique_lock<std::mutex> lk(leaseRefreshmtx_);
        blockIO_.store(true);
        client_.StartRecycleRetryRPC();
    }

    /**
     *When the lease is successfully renewed, the LeaseExecutor calls the interface to restore IO,
     *IO scheduling restored
     */
    void ResumeIO() {
        std::unique_lock<std::mutex> lk(leaseRefreshmtx_);
        blockIO_.store(false);
        leaseRefreshcv_.notify_all();
        client_.ResumeRPCRetry();
    }

    /**
     *Test usage, obtain queue
     */
    BoundedBlockingDeque<BBQItem<RequestContext*>>* GetQueue() {
        return &queue_;
    }

 private:
    /**
     *The run function of the Thread pool will retrieve the request from the queue for processing
     */
    void Process();

    void ProcessOne(RequestContext* ctx);

    void WaitValidSession() {
        //When the lease renewal fails, IO needs to be blocked until the renewal is successful
        if (blockIO_.load(std::memory_order_acquire) && blockingQueue_) {
            std::unique_lock<std::mutex> lk(leaseRefreshmtx_);
            leaseRefreshcv_.wait(lk, [&]() -> bool {
                return !blockIO_.load() || !blockingQueue_;
            });
        }
    }

 private:
    //Configuration parameters for thread pool and queue capacity
    RequestScheduleOption reqschopt_;
    //Queue for storing requests
    BoundedBlockingDeque<BBQItem<RequestContext *>> queue_;
    //Thread pool for processing requests
    ThreadPool threadPool_;
    //The scheduler runs the flag and only receives requests when it is running
    std::atomic<bool> running_;
    //Stop thread pool flag, when calling Scheduler Fini
    //After processing all the requests in the queue, you can proceed
    //Let all processing threads exit
    std::atomic<bool> stop_;
    //Client accessing replication group Chunk
    CopysetClient client_;
    //Renewal failed, IO stuck
    std::atomic<bool> blockIO_;
    //This lock is associated with LeaseRefreshcv_ Using Conditional Variables Together
    //When lease renewal fails, all newly issued IO is blocked until the renewal is successful
    std::mutex    leaseRefreshmtx_;
    //Conditional variables for wake-up and hang IO
    std::condition_variable leaseRefreshcv_;
    //Blocking queue
    bool blockingQueue_;
};

}   // namespace client
}   // namespace curve

#endif  // SRC_CLIENT_REQUEST_SCHEDULER_H_
