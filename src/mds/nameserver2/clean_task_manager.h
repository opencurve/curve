/*
 * Project: curve
 * Created Date: Tuesday December 18th 2018
 * Author: hzsunjianliang
 * Copyright (c) 2018 netease
 */
#ifndef SRC_MDS_NAMESERVER2_CLEAN_TASK_MANAGER_H_
#define SRC_MDS_NAMESERVER2_CLEAN_TASK_MANAGER_H_

#include <unordered_map>
#include <thread> //NOLINT
#include <mutex>  //NOLINT
#include "src/common/task_thread_pool.h"
#include "src/mds/nameserver2/define.h"
#include "src/mds/nameserver2/clean_task.h"

namespace curve {
namespace mds {

class CleanTaskManager {
 public:
    /**
     *  @brief 初始化TaskManager
     *  @param threadNum: worker线程的数量
     *  @param checkPeriod: 周期性任务检查线程时间, ms
     */
    explicit CleanTaskManager(int threadNum = 10, int checkPeriod = 10000);
    ~CleanTaskManager() {}

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
     *  @return 推送task是否成功，如已存在对应的任务，推送是吧
     */
    bool PushTask(std::shared_ptr<Task> task);

    /**
     * @brief 获取当前的task
     * @param id: 对应任务的相关文件InodeID
     * @return 返回对应task的shared_ptr 或者 不存在返回nullptr
     */
    std::shared_ptr<Task> GetTask(TaskIDType id);

 private:
    void CheckCleanResult(void);

 private:
    int threadNum_;
    ::curve::common::TaskThreadPool *cleanWorkers_;
    // for period check snapshot delete status
    std::unordered_map<TaskIDType, std::shared_ptr<Task>> cleanTasks_;
    std::mutex mutex_;
    std::thread *checkThread_;
    int checkPeriod_;

    bool stopFlag_;
};

}   //  namespace mds
}   //  namespace curve

#endif  // SRC_MDS_NAMESERVER2_CLEAN_TASK_MANAGER_H_
