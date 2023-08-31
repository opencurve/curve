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
 * Created Date: 2023-07-20
 * Author: xuchaojie
 */

#ifndef SRC_MDS_NAMESERVER2_TASK_MANAGER_H_
#define SRC_MDS_NAMESERVER2_TASK_MANAGER_H_

#include <map>
#include "src/common/concurrent/task_thread_pool.h"
#include "src/mds/nameserver2/clean_task.h"

namespace curve {
namespace mds {

class CancelableTask : public Task {
 public:
    CancelableTask()
     : isCanceld_(false) {}

    virtual ~CancelableTask() = default;

    void Cancel() {
        isCanceld_ = true;
    }

    bool IsCanceled() {
        return isCanceld_;
    }

    TaskStatus GetStatus() {
        return GetTaskProgress().GetStatus();
    }

    uint64_t GetProgress() {
       return GetTaskProgress().GetProgress();
    }

 private:
    bool isCanceld_;
};

class TaskManager {
 public:
    explicit TaskManager(const std::shared_ptr<ChannelPool> &channelPool,
       int threadNum = 10, int checkPeriod = 10000)
     : channelPool_(channelPool),
       threadNum_(threadNum),
       checkPeriod_(checkPeriod),
       stopFlag_(true) {}

    ~TaskManager() {
        Stop();
    }

    /**
     * @brief 启动worker线程池、启动检查线程
     *
     */
    bool Start(void);

    /**
     * @brief 停止worker线程池、启动检查线程
     *
     */
    bool Stop(void);

    bool SubmitTask(const std::shared_ptr<CancelableTask> &task);

    bool CancelTask(uint64_t taskId) {
        // TODO(xuchaojie); 任务取消
        return false;
    }

    std::shared_ptr<CancelableTask> GetTask(uint64_t taskId);

 private:
    void CheckResult(void);

 private:
    // 连接池，和clean_task_manager, chunkserverclient共享，没有任务在执行时清空
    std::shared_ptr<ChannelPool> channelPool_;

    int threadNum_;
    ::curve::common::TaskThreadPool<> *taskWorkers_;
    std::map<uint64_t, std::shared_ptr<CancelableTask>> taskMap_;
    common::Mutex mutex_;

    common::Thread *checkThread_;
    int checkPeriod_;

    Atomic<bool> stopFlag_;
    InterruptibleSleeper sleeper_;
};

}  // namespace mds
}  // namespace curve

#endif  // SRC_MDS_NAMESERVER2_TASK_MANAGER_H_
