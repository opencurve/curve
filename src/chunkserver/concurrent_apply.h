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
 * File Created: Tuesday, 11th December 2018 6:26:24 pm
 * Author: tongguangxun
 */

#ifndef SRC_CHUNKSERVER_CONCURRENT_APPLY_H_
#define SRC_CHUNKSERVER_CONCURRENT_APPLY_H_

#include <glog/logging.h>
#include <unistd.h>
#include <atomic>
#include <mutex>    // NOLINT
#include <thread>    // NOLINT
#include <unordered_map>
#include <utility>
#include <condition_variable>    // NOLINT

#include "src/common/concurrent/task_queue.h"
#include "include/curve_compiler_specific.h"
#include "src/common/concurrent/count_down_event.h"

using curve::common::TaskQueue;
using curve::common::CountDownEvent;
namespace curve {
namespace chunkserver {

class CURVE_CACHELINE_ALIGNMENT ConcurrentApplyModule {
 public:
    ConcurrentApplyModule();
    ~ConcurrentApplyModule();
    /**
     * @param: concurrentsize是当前并发模块的并发大小
     * @param: queuedepth是当前并发模块每个队列的深度控制
     */
    bool Init(int concurrentsize, int queuedepth);

    /**
     * raft apply线程会将task push到后台队列
     * @param: key用于将task哈希到指定队列
     * @param: f为要执行的task
     * @param: args为执行task的参数
     */
    template<class F, class... Args>
    bool Push(uint64_t key, F&& f, Args&&... args) {
        if (!isStarted_) {
            LOG(WARNING) << "concurrent module not start!";
            return false;
        }

        auto task = std::bind(std::forward<F>(f), std::forward<Args>(args)...);
        applypoolMap_[Hash(key)]->tq.Push(task);
        return true;
    };                                                                                  // NOLINT

    // raft snapshot之前需要将队列中的IO全部落盘。
    void Flush();
    void Stop();

 private:
    void Run(int index);
    inline int Hash(uint64_t key) {
        return key % concurrentsize_;
    }

 private:
    typedef uint8_t threadIndex;
    typedef struct taskthread {
        std::thread th;
        TaskQueue tq;
        taskthread(size_t capacity):tq(capacity) {}
        ~taskthread() = default;
    } taskthread_t;

    // 常规的stop和start控制变量
    bool stop_;
    bool isStarted_;
    // 每个队列的深度
    int queuedepth_;
    // 并发度
    int concurrentsize_;
    // 用于统一启动后台线程完全创建完成的条件变量
    CountDownEvent cond_;
    // 存储threadindex与taskthread的映射关系
    CURVE_CACHELINE_ALIGNMENT std::unordered_map<threadIndex, taskthread_t*> applypoolMap_;     // NOLINT
};
}   // namespace chunkserver
}   // namespace curve

#endif  // SRC_CHUNKSERVER_CONCURRENT_APPLY_H_
