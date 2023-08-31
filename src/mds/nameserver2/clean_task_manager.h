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

#include <unordered_map>
#include <thread> //NOLINT
#include <mutex>  //NOLINT
#include <memory>
#include "src/common/concurrent/task_thread_pool.h"
#include "src/common/interruptible_sleeper.h"
#include "src/common/concurrent/concurrent.h"
#include "src/common/channel_pool.h"
#include "src/mds/common/mds_define.h"
#include "src/mds/nameserver2/clean_task.h"

using ::curve::common::Atomic;
using ::curve::common::InterruptibleSleeper;
using ::curve::common::ChannelPool;

namespace curve {
namespace mds {

class CleanTaskManager {
 public:
    /**
     *  @brief 初始化TaskManager
     *  @param channelPool: 连接池
     *  @param threadNum: worker线程的数量
     *  @param checkPeriod: 周期性任务检查线程时间, ms
     */
    explicit CleanTaskManager(std::shared_ptr<ChannelPool> channelPool,
                              int threadNum = 10, int checkPeriod = 10000);
    ~CleanTaskManager() {
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

    /**
     *  @brief 向线程池推送task
     *  @param task: 对应的工作任务
     *  @return 推送task是否成功，如已存在对应的任务，推送失败
     */
    bool PushTask(std::shared_ptr<Task> task);

    /**
     * @brief 获取当前的task
     * @param id: 对应任务的相关文件InodeID
     * @return 返回对应task的shared_ptr 或者 不存在返回nullptr
     */
    std::shared_ptr<Task> GetTask(TaskIDType id);

    /**
     *  @brief 向线程池推送某文件的快照删除任务，若该文件的快照删除任务已存在，
     *         则将快照放入该任务内部的待删除快照列表等待删除
     *  @param task: 对应某文件的快照删除工作任务
     *  @param snapfileInfo: 待删除的快照信息
     *  @return 推送task是否成功，如已存在对应的任务，推送失败
     */
    bool PushTask(std::shared_ptr<SnapShotBatchCleanTask> task, const FileInfo &snapfileInfo);

    /**
     * @brief 获取快照删除task中指定快照sn的task
     * @param id: 对应任务的相关文件InodeID
     * @param sn: 指定快照的sn
     * @return 返回对应task的shared_ptr 或者 不存在返回nullptr
     */
    std::shared_ptr<Task> GetTask(TaskIDType id, TaskIDType sn);
 private:
    void CheckCleanResult(void);

 private:
    int threadNum_;
    ::curve::common::TaskThreadPool<> *cleanWorkers_;
    // for period check snapshot delete status
    std::unordered_map<TaskIDType, std::shared_ptr<Task>> cleanTasks_;
    common::Mutex mutex_;
    common::Thread *checkThread_;
    int checkPeriod_;

    Atomic<bool> stopFlag_;
    InterruptibleSleeper sleeper_;
    // 连接池，和task_manager, chunkserverClient共享，没有任务在执行时清空
    std::shared_ptr<ChannelPool> channelPool_;

    // 针对同一文件的本地多层快照的清除任务放到一起串行执行，防止并发删除导致的快照数据破坏和快照遗留问题
    // 尽量按快照sn从小到大开始删除，以减少额外的快照数据搬迁
    std::unordered_map<TaskIDType, std::shared_ptr<SnapShotBatchCleanTask>> cleanBatchSnapTasks_;
    common::Mutex mutexBatch_;
};

}   //  namespace mds
}   //  namespace curve

#endif  // SRC_MDS_NAMESERVER2_CLEAN_TASK_MANAGER_H_
