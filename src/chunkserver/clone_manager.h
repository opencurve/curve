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
 * Created Date: Monday March 11th 2019
 * Author: yangyaokai
 */

#ifndef SRC_CHUNKSERVER_CLONE_MANAGER_H_
#define SRC_CHUNKSERVER_CLONE_MANAGER_H_

#include <glog/logging.h>
#include <google/protobuf/stubs/callback.h>
#include <thread>  // NOLINT
#include <mutex>   // NOLINT
#include <memory>
#include <vector>
#include <string>

#include "include/chunkserver/chunkserver_common.h"
#include "src/common/concurrent/task_thread_pool.h"
#include "src/chunkserver/clone_task.h"
#include "src/chunkserver/clone_core.h"

namespace curve {
namespace chunkserver {

using curve::common::TaskThreadPool;

class ReadChunkRequest;

struct CloneOptions {
    // Core logic processing class
    std::shared_ptr<CloneCore> core;
    // Maximum number of threads
    uint32_t threadNum;
    // Maximum Queue Capacity
    uint32_t queueCapacity;
    // Period of task status check, in ms
    uint32_t checkPeriod;
    CloneOptions() : core(nullptr)
                   , threadNum(10)
                   , queueCapacity(100)
                   , checkPeriod(5000) {}
};

class CloneManager {
 public:
    CloneManager();
    virtual ~CloneManager();

    /**
     * Init
     *
     * @param options[in]:Init params
     * @return Error code
     */
    virtual int Init(const CloneOptions& options);

    /**
     * Start all threads
     *
     * @return Return 0 for success, -1 for failure
     */
    virtual int Run();

    /**
     * Stop all threads
     *
     * @return Return 0 for success, -1 for failure
     */
    virtual int Fini();

    /**
     * Generate clone tasks
     * @param request[in]:Request info
     * @return:Return the generated clone task, or nullptr if the generation fails
     */
    virtual std::shared_ptr<CloneTask> GenerateCloneTask(
        std::shared_ptr<ReadChunkRequest> request,
        ::google::protobuf::Closure* done);

    /**
     * Issue clone tasks, generate clone tasks to be processed in a thread pool
     * @param task[in]:Clone tasks
     * @return  Return true for success, false for failure
     */
    virtual bool IssueCloneTask(std::shared_ptr<CloneTask> cloneTask);

 private:
    // Clone task management related options and initialize them when calling Init
    CloneOptions options_;
    // Asynchronous thread pool for handling cloning tasks
    std::shared_ptr<TaskThreadPool<>> tp_;
    // Whether the current thread pool is working or not
    std::atomic<bool> isRunning_;
};

}  // namespace chunkserver
}  // namespace curve

#endif  // SRC_CHUNKSERVER_CLONE_MANAGER_H_
