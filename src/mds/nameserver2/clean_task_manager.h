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
 * Created Date: Tuesday December 18th 2018
 * Author: hzsunjianliang
 */
#ifndef SRC_MDS_NAMESERVER2_CLEAN_TASK_MANAGER_H_
#define SRC_MDS_NAMESERVER2_CLEAN_TASK_MANAGER_H_

#include <memory>
#include <mutex>   //NOLINT
#include <thread>  //NOLINT
#include <unordered_map>

#include "src/common/channel_pool.h"
#include "src/common/concurrent/concurrent.h"
#include "src/common/concurrent/task_thread_pool.h"
#include "src/common/interruptible_sleeper.h"
#include "src/mds/common/mds_define.h"
#include "src/mds/nameserver2/clean_task.h"

using ::curve::common::Atomic;
using ::curve::common::ChannelPool;
using ::curve::common::InterruptibleSleeper;

namespace curve {
namespace mds {

class CleanTaskManager {
 public:
    /**
     * @brief Initialize TaskManager
     * @param channelPool: Connection Pool
     * @param threadNum: Number of worker threads
     * @param checkPeriod: Periodic task check thread time, ms
     */
    explicit CleanTaskManager(std::shared_ptr<ChannelPool> channelPool,
                              int threadNum = 10, int checkPeriod = 10000);
    ~CleanTaskManager() { Stop(); }

    /**
     * @brief: Start worker thread pool, start check thread
     *
     */
    bool Start(void);

    /**
     * @brief: Stop worker thread pool, start check thread
     *
     */
    bool Stop(void);

    /**
     * @brief Push task to thread pool
     * @param task: corresponding work task
     * @return: Is the task successfully pushed? If a corresponding task already
     * exists, is it pushed
     */
    bool PushTask(std::shared_ptr<Task> task);

    /**
     * @brief Get the current task
     * @param id: The relevant file InodeID of the corresponding task
     * @return returns the shared_ptr of the corresponding task or return
     * nullptr if it does not exist
     */
    std::shared_ptr<Task> GetTask(TaskIDType id);

 private:
    void CheckCleanResult(void);

 private:
    int threadNum_;
    ::curve::common::TaskThreadPool<>* cleanWorkers_;
    // for period check snapshot delete status
    std::unordered_map<TaskIDType, std::shared_ptr<Task>> cleanTasks_;
    common::Mutex mutex_;
    common::Thread* checkThread_;
    int checkPeriod_;

    Atomic<bool> stopFlag_;
    InterruptibleSleeper sleeper_;
    // Connection pool, shared with chunkserverClient, no tasks cleared during
    // execution
    std::shared_ptr<ChannelPool> channelPool_;
};

}  //  namespace mds
}  //  namespace curve

#endif  // SRC_MDS_NAMESERVER2_CLEAN_TASK_MANAGER_H_
