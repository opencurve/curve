/*
 *  Copyright (c) 2023 NetEase Inc.
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
 * File Created: 20230521
 * Author: Xinlong-Chen
 */

#ifndef CURVEFS_SRC_METASERVER_COPYSET_CONCURRENT_APPLY_QUEUE_H_
#define CURVEFS_SRC_METASERVER_COPYSET_CONCURRENT_APPLY_QUEUE_H_

#include <bthread/condition_variable.h>
#include <bthread/mutex.h>
#include <glog/logging.h>

#include <atomic>
#include <thread>
#include <unordered_map>
#include <utility>

#include "include/curve_compiler_specific.h"
#include "src/common/concurrent/count_down_event.h"
#include "src/common/concurrent/task_queue.h"
#include "curvefs/src/metaserver/copyset/operator_type.h"

namespace curvefs {
namespace metaserver {
namespace copyset {

using curve::common::CountDownEvent;
using curve::common::GenericTaskQueue;

struct ApplyOption {
    int wconcurrentsize = 3;
    int wqueuedepth = 1;
    int rconcurrentsize = 1;
    int rqueuedepth = 1;
    ApplyOption(int wsize, int wdepth, int rsize, int rdepth) :
        wconcurrentsize(wsize),
        wqueuedepth(wdepth),
        rconcurrentsize(rsize),
        rqueuedepth(rdepth) {}
    ApplyOption() {}
};

enum class ThreadPoolType {READ, WRITE};

/*
TODO: this moudle is same as curvebs's ConcurrentApplyModule,
      only Schedule function is different.
      we can make ApplyQueue to a base class,
      and define Schedule with virtual function,
      derive class override Schedule.
*/ 

class CURVE_CACHELINE_ALIGNMENT ApplyQueue {
 public:
    ApplyQueue(): start_(false),
                  rconcurrentsize_(0),
                  rqueuedepth_(0),
                  wconcurrentsize_(0),
                  wqueuedepth_(0),
                  cond_(0) {}

    /**
     * Init: initialize ApplyQueue
     * @param[in] wconcurrentsize: num of write threads
     * @param[in] wqueuedepth: depth of write queue in ervery thread
     * @param[in] rconcurrentsizee: num of read threads
     * @param[in] wqueuedephth: depth of read queue in every thread
     */
    bool Init(const ApplyOption &opt);

    /**
     * Push: apply task will be push to ApplyQueue
     * @param[in] key: used to hash task to specified queue
     * @param[in] optype: operation type defined in proto
     * @param[in] f: task
     * @param[in] args: param to excute task
     */
    template <class F, class... Args>
    bool Push(uint64_t key, OperatorType optype, F&& f, Args&&... args) {
        switch (Schedule(optype)) {
            case ThreadPoolType::READ:
                rapplyMap_[Hash(key, rconcurrentsize_)]->tq.Push(
                        std::forward<F>(f), std::forward<Args>(args)...);
                break;
            case ThreadPoolType::WRITE:
                wapplyMap_[Hash(key, wconcurrentsize_)]->tq.Push(
                        std::forward<F>(f), std::forward<Args>(args)...);
                break;
        }

        return true;
    }

    /**
     * Flush: finish all task in write threads
     */
    void Flush();
    void FlushAll();

    void Stop();

 private:
    bool CheckOptAndInit(const ApplyOption &option);

    void Run(ThreadPoolType type, int index);

    static ThreadPoolType Schedule(OperatorType optype);

    void InitThreadPool(ThreadPoolType type, int concorrent, int depth);

    static int Hash(uint64_t key, int concurrent) {
        return key % concurrent;
    }

 private:
    struct TaskThread {
        std::thread th;
        GenericTaskQueue<bthread::Mutex, bthread::ConditionVariable> tq;
        explicit TaskThread(size_t capacity) : tq(capacity) {}
    };

    std::atomic<bool> start_;
    int rconcurrentsize_;
    int rqueuedepth_;
    int wconcurrentsize_;
    int wqueuedepth_;
    CountDownEvent cond_;
    CURVE_CACHELINE_ALIGNMENT std::unordered_map<int, TaskThread*> wapplyMap_;
    CURVE_CACHELINE_ALIGNMENT std::unordered_map<int, TaskThread*> rapplyMap_;
};
}   // namespace copyset
}   // namespace metaserver
}   // namespace curvefs

#endif  // CURVEFS_SRC_METASERVER_COPYSET_CONCURRENT_APPLY_QUEUE_H_
