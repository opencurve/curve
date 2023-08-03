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
 * File Created: 20200813
 * Author: lixiaocui
 */

#ifndef SRC_CHUNKSERVER_CONCURRENT_APPLY_CONCURRENT_APPLY_H_
#define SRC_CHUNKSERVER_CONCURRENT_APPLY_CONCURRENT_APPLY_H_

#include <bthread/condition_variable.h>
#include <bthread/mutex.h>
#include <glog/logging.h>

#include <atomic>
#include <condition_variable>  // NOLINT
#include <mutex>               // NOLINT
#include <thread>              // NOLINT
#include <unordered_map>
#include <utility>

#include "include/curve_compiler_specific.h"
#include "proto/chunk.pb.h"
#include "src/common/concurrent/count_down_event.h"
#include "src/common/concurrent/task_queue.h"

using curve::common::CountDownEvent;
using curve::chunkserver::CHUNK_OP_TYPE;

namespace curve {
namespace chunkserver {
namespace concurrent {

using ::curve::common::GenericTaskQueue;

struct ConcurrentApplyOption {
    int wconcurrentsize;
    int wqueuedepth;
    int rconcurrentsize;
    int rqueuedepth;
};

enum class ThreadPoolType {READ, WRITE};

class CURVE_CACHELINE_ALIGNMENT ConcurrentApplyModule {
 public:
    ConcurrentApplyModule(): start_(false),
                             rconcurrentsize_(0),
                             rqueuedepth_(0),
                             wconcurrentsize_(0),
                             wqueuedepth_(0),
                             cond_(0) {}

    /**
     * Init: initialize ConcurrentApplyModule
     * @param[in] wconcurrentsize: num of write threads
     * @param[in] wqueuedepth: depth of write queue in ervery thread
     * @param[in] rconcurrentsizee: num of read threads
     * @param[in] wqueuedephth: depth of read queue in every thread
     */
    bool Init(const ConcurrentApplyOption &opt);

    /**
     * Push: apply task will be push to ConcurrentApplyModule
     * @param[in] key: used to hash task to specified queue
     * @param[in] optype: operation type defined in proto
     * @param[in] f: task
     * @param[in] args: param to excute task
     */
    template <class F, class... Args>
    bool Push(uint64_t key, CHUNK_OP_TYPE optype, F&& f, Args&&... args) {
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

    void Stop();

 private:
    bool checkOptAndInit(const ConcurrentApplyOption &option);

    void Run(ThreadPoolType type, int index);

    static ThreadPoolType Schedule(CHUNK_OP_TYPE optype);

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

    bool start_;
    int rconcurrentsize_;
    int rqueuedepth_;
    int wconcurrentsize_;
    int wqueuedepth_;
    CountDownEvent cond_;
    CURVE_CACHELINE_ALIGNMENT std::unordered_map<int, TaskThread*> wapplyMap_;
    CURVE_CACHELINE_ALIGNMENT std::unordered_map<int, TaskThread*> rapplyMap_;
};
}   // namespace concurrent
}   // namespace chunkserver
}   // namespace curve

#endif  // SRC_CHUNKSERVER_CONCURRENT_APPLY_CONCURRENT_APPLY_H_
