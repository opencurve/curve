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
#include "src/snapshotcloneserver/clone/clone_core.h"

using ::curve::common::RWLock;
using ::curve::common::ReadLockGuard;
using ::curve::common::WriteLockGuard;
using ::curve::common::Mutex;
using ::curve::common::LockGuard;

namespace curve {
namespace snapshotcloneserver {

class CloneTaskManager {
 public:
    explicit CloneTaskManager(
        std::shared_ptr<CloneCore> core,
        std::shared_ptr<CloneMetric> cloneMetric)
        : isStop_(true),
          core_(core),
          cloneMetric_(cloneMetric),
          cloneTaskManagerScanIntervalMs_(0) {}

    ~CloneTaskManager() {
        Stop();
    }

    int Init(std::shared_ptr<ThreadPool> stage1Pool,
        std::shared_ptr<ThreadPool> stage2Pool,
        std::shared_ptr<ThreadPool> commonPool,
        const SnapshotCloneServerOptions &option) {
        cloneTaskManagerScanIntervalMs_ =
            option.cloneTaskManagerScanIntervalMs;
        stage1Pool_ = stage1Pool;
        stage2Pool_ = stage2Pool;
        commonPool_ = commonPool;
        return kErrCodeSuccess;
    }

    int Start();

    void Stop();

    /**
     * @brief 往任务管理器中加入任务
     *
     * 用于非Lazy克隆及其他删除克隆等管控面的请求
     *
     * @param task 任务
     *
     * @return 错误码
     */
    int PushCommonTask(
        std::shared_ptr<CloneTaskBase> task);

    /**
     * @brief 往任务管理器中加入LazyClone阶段一的的任务
     *
     * @param task 任务
     *
     * @return 错误码
     */
    int PushStage1Task(
        std::shared_ptr<CloneTaskBase> task);

    /**
     * @brief 往任务管理器中加入LazyClone阶段二的的任务
     *
     *  目前只用于重启恢复时，将Lazy克隆恢复克隆数据阶段的任务加入任务管理器
     *
     * @param task 任务
     *
     * @return 错误码
     */
    int PushStage2Task(
        std::shared_ptr<CloneTaskBase> task);

    std::shared_ptr<CloneTaskBase> GetTask(const TaskIdType &taskId) const;

 private:
    void BackEndThreadFunc();
    void ScanCommonTasks();
    void ScanStage1Tasks();
    void ScanStage2Tasks();

    /**
     * @brief 往对应线程池和map中push任务
     *
     * @param task 任务
     * @param taskMap 任务表
     * @param taskMapMutex 任务表和线程池的锁
     * @param taskPool 线程池
     *
     * @return 错误码
     */
    int PushTaskInternal(
        std::shared_ptr<CloneTaskBase> task,
        std::map<std::string, std::shared_ptr<CloneTaskBase> > *taskMap,
        Mutex *taskMapMutex,
        std::shared_ptr<ThreadPool> taskPool);

 private:
    // 后端线程
    std::thread backEndThread;

    //  id->克隆任务表
    std::map<TaskIdType, std::shared_ptr<CloneTaskBase> > cloneTaskMap_;
    mutable RWLock cloneTaskMapLock_;

    // 存放stage1Pool_池的当前任务，key为destination
    std::map<std::string, std::shared_ptr<CloneTaskBase> > stage1TaskMap_;
    mutable Mutex stage1TasksLock_;

    // 存放stage1Poo2_池的当前任务，key为destination
    std::map<std::string, std::shared_ptr<CloneTaskBase> > stage2TaskMap_;
    mutable Mutex stage2TasksLock_;;

    // 存放commonPool_池的当前任务
    std::map<std::string, std::shared_ptr<CloneTaskBase> > commonTaskMap_;
    mutable Mutex commonTasksLock_;;

    // 用于Lazy克隆元数据部分的线程池
    std::shared_ptr<ThreadPool> stage1Pool_;

    // 用于Lazy克隆数据部分的线程池
    std::shared_ptr<ThreadPool> stage2Pool_;

    // 用于非Lazy克隆和删除克隆等其他管控面的请求的线程池
    std::shared_ptr<ThreadPool> commonPool_;

    // 当前任务管理是否停止，用于支持start，stop功能
    std::atomic_bool isStop_;

    // clone core
    std::shared_ptr<CloneCore> core_;

    // metric
    std::shared_ptr<CloneMetric> cloneMetric_;

    // CloneTaskManager 后台线程扫描间隔
    uint32_t cloneTaskManagerScanIntervalMs_;
};

}  // namespace snapshotcloneserver
}  // namespace curve






#endif  // SRC_SNAPSHOTCLONESERVER_CLONE_CLONE_TASK_MANAGER_H_
