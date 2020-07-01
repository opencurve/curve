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
 * @brief 快照线程池
 */
class ThreadPool {
 public:
     /**
      * @brief 构造函数
      *
      * @param threadNum 最大线程数
      */
    explicit ThreadPool(int threadNum)
        : threadNum_(threadNum) {}
    /**
     * @brief 启动线程池
     */
    int Start();

    /**
     * @brief 停止线程池
     */
    void Stop();

    /**
     * @brief 添加快照任务
     *
     * @param task 快照任务
     */
    void PushTask(std::shared_ptr<Task> task) {
        threadPool_.Enqueue(task->clousre());
    }

    /**
     * @brief 添加快照任务
     *
     * @param task 快照任务
     */
    void PushTask(Task* task) {
        threadPool_.Enqueue(task->clousre());
    }

 private:
    /**
     * @brief 通用线程池
     */
    curve::common::TaskThreadPool threadPool_;
    /**
     * @brief 线程数
     */
    int threadNum_;
};

}  // namespace snapshotcloneserver
}  // namespace curve

#endif  // SRC_SNAPSHOTCLONESERVER_COMMON_THREAD_POOL_H_
