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
 * Created Date: Wed Dec 12 2018
 * Author: xuchaojie
 */

#ifndef SRC_SNAPSHOTCLONESERVER_COMMON_THREAD_POOL_H_
#define SRC_SNAPSHOTCLONESERVER_COMMON_THREAD_POOL_H_

#include <memory>
#include "src/common/concurrent/task_thread_pool.h"
#include "src/snapshotcloneserver/common/task.h"

namespace curve {
namespace snapshotcloneserver {

/**
 * @brief snapshot thread pool
 */
class ThreadPool {
 public:
     /**
      * @brief constructor
      *
      * @param threadNum maximum number of threads
      */
    explicit ThreadPool(int threadNum)
        : threadNum_(threadNum) {}
    /**
     * @brief Start Thread Pool
     */
    int Start();

    /**
     * @brief Stop thread pool
     */
    void Stop();

    /**
     * @brief Add snapshot task
     *
     * @param task snapshot task
     */
    void PushTask(std::shared_ptr<Task> task) {
        threadPool_.Enqueue(task->clousre());
    }

    /**
     * @brief Add snapshot task
     *
     * @param task snapshot task
     */
    void PushTask(Task* task) {
        threadPool_.Enqueue(task->clousre());
    }

 private:
    /**
     * @brief Universal Thread Pool
     */
    curve::common::TaskThreadPool<> threadPool_;
    /**
     * @brief Number of threads
     */
    int threadNum_;
};

}  // namespace snapshotcloneserver
}  // namespace curve

#endif  // SRC_SNAPSHOTCLONESERVER_COMMON_THREAD_POOL_H_
