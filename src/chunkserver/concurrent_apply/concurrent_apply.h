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

#include <glog/logging.h>
#include <unistd.h>
#include <atomic>
#include <mutex>    // NOLINT
#include <thread>    // NOLINT
#include <unordered_map>
#include <utility>
#include <condition_variable>    // NOLINT

#include "src/common/concurrent/task_queue.h"
#include "src/common/concurrent/count_down_event.h"
#include "proto/chunk.pb.h"
#include "include/curve_compiler_specific.h"

using curve::common::TaskQueue;
using curve::common::CountDownEvent;
using curve::chunkserver::CHUNK_OP_TYPE;

namespace curve {
namespace chunkserver {
namespace concurrent {

struct ConcurrentApplyOption {
    int wconcurrentsize;
    int wqueuedepth;
    int rconcurrentsize;
    int rqueuedepth;

    ConcurrentApplyOption(int wconcurrentsize = 0, int wqueuedepth = 0,
                          int rconcurrentsize = 0, int rqueuedepth = 0)
        : wconcurrentsize(wconcurrentsize), wqueuedepth(wqueuedepth),
          rconcurrentsize(rconcurrentsize), rqueuedepth(rqueuedepth) {}
};

enum class ThreadPoolType {READ, WRITE};

class CURVE_CACHELINE_ALIGNMENT ConcurrentApplyModule {
 public:
    ConcurrentApplyModule(): start_(false),
                             rconcurrentsize_(0),
                             wconcurrentsize_(0),
                             rqueuedepth_(0),
                             wqueuedepth_(0),
                             cond_(0) {}
    ~ConcurrentApplyModule() {}

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
    template<class F, class... Args>
    bool Push(uint64_t key, CHUNK_OP_TYPE optype, F&& f, Args&&... args) {
        auto task = std::bind(std::forward<F>(f), std::forward<Args>(args)...);
        switch (Schedule(optype)) {
            case ThreadPoolType::READ:
                rapplyMap_[Hash(key, rconcurrentsize_)]->tq.Push(task);
                break;
            case ThreadPoolType::WRITE:
                wapplyMap_[Hash(key, wconcurrentsize_)]->tq.Push(task);
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

    ThreadPoolType Schedule(CHUNK_OP_TYPE optype);

    void InitThreadPool(ThreadPoolType type, int concorrent, int depth);

    int Hash(uint64_t key, int concurrent) {
        return key % concurrent;
    }

 private:
    typedef uint8_t threadIndex;
    typedef struct taskthread {
        std::thread th;
        TaskQueue tq;
        taskthread(size_t capacity):tq(capacity) {}
        ~taskthread() = default;
    } taskthread_t;

    bool start_;
    int rconcurrentsize_;
    int rqueuedepth_;
    int wconcurrentsize_;
    int wqueuedepth_;
    CountDownEvent cond_;
    CURVE_CACHELINE_ALIGNMENT std::unordered_map<threadIndex, taskthread_t*> wapplyMap_; // NOLINT
    CURVE_CACHELINE_ALIGNMENT std::unordered_map<threadIndex, taskthread_t*> rapplyMap_;   // NOLINT
};
}   // namespace concurrent
}   // namespace chunkserver
}   // namespace curve

#endif  // SRC_CHUNKSERVER_CONCURRENT_APPLY_CONCURRENT_APPLY_H_
