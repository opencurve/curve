/*
 * Project: curve
 * Created Date: Wed Mar 20 2019
 * Author: xuchaojie
 * Copyright (c) 2018 netease
 */

#ifndef SRC_SNAPSHOTCLONESERVER_CLONE_CLONE_TASK_MANAGER_H_
#define SRC_SNAPSHOTCLONESERVER_CLONE_CLONE_TASK_MANAGER_H_

#include <memory>
#include <map>
#include <atomic>
#include <string>
#include <list>
#include <thread>  // NOLINT

#include "src/snapshotcloneserver/clone/clone_task.h"
#include "src/snapshotcloneserver/common/thread_pool.h"
#include "src/common/concurrent/rw_lock.h"
#include "src/snapshotcloneserver/common/define.h"
#include "src/snapshotcloneserver/common/config.h"
#include "src/snapshotcloneserver/common/snapshotclone_metric.h"


using ::curve::common::RWLock;
using ::curve::common::ReadLockGuard;
using ::curve::common::WriteLockGuard;

namespace curve {
namespace snapshotcloneserver {

class CloneTaskManager {
 public:
    explicit CloneTaskManager(std::shared_ptr<CloneMetric> cloneMetric)
        : isStop_(true),
          cloneMetric_(cloneMetric),
          cloneTaskManagerScanIntervalMs_(0),
          clonePoolThreadNum_(0) {}

    ~CloneTaskManager() {
        Stop();
    }

    int Init(std::shared_ptr<ThreadPool> pool,
        const SnapshotCloneServerOptions &option) {
        cloneTaskManagerScanIntervalMs_ =
            option.cloneTaskManagerScanIntervalMs;
        clonePoolThreadNum_ =
            option.clonePoolThreadNum;
        threadpool_ = pool;
        return kErrCodeSuccess;
    }

    int Start();


    void Stop();


    int PushTask(std::shared_ptr<CloneTaskBase> task);

    std::shared_ptr<CloneTaskBase> GetTask(const TaskIdType &taskId) const;

 private:
    void BackEndThreadFunc();
    void ScanWorkingTask();

 private:
    // 后端线程
    std::thread backEndThread;

    //  id->克隆任务表
    std::map<TaskIdType, std::shared_ptr<CloneTaskBase> > cloneTaskMap_;
    mutable RWLock cloneTaskMapLock_;

    // 克隆恢复工作队列，key 为destination, 多个克隆或恢复的目标不能为同一个
    std::map<std::string, std::shared_ptr<CloneTaskBase> > cloningTasks_;
    mutable std::mutex cloningTasksLock_;

    std::shared_ptr<ThreadPool> threadpool_;

    // 当前任务管理是否停止，用于支持start，stop功能
    std::atomic_bool isStop_;

    // metric
    std::shared_ptr<CloneMetric> cloneMetric_;

    // CloneTaskManager 后台线程扫描间隔
    uint32_t cloneTaskManagerScanIntervalMs_;
    // 克隆恢复工作线程数
    int clonePoolThreadNum_;
};

}  // namespace snapshotcloneserver
}  // namespace curve






#endif  // SRC_SNAPSHOTCLONESERVER_CLONE_CLONE_TASK_MANAGER_H_
