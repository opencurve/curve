/*
 * Project: curve
 * Created Date: Fri Dec 14 2018
 * Author: xuchaojie
 * Copyright (c) 2018 netease
 */

#ifndef CURVE_SRC_SNAPSHOT_SNAPSHOT_TASK_MANAGER_H_
#define CURVE_SRC_SNAPSHOT_SNAPSHOT_TASK_MANAGER_H_

#include <memory>
#include <map>
#include <atomic>
#include <string>
#include <list>
#include <thread>  // NOLINT

#include "src/snapshot/snapshot_task.h"
#include "src/snapshot/snapshot_thread_pool.h"
#include "src/common/concurrent/rw_lock.h"
#include "src/snapshot/snapshot_define.h"

using ::curve::common::RWLock;
using ::curve::common::ReadLockGuard;
using ::curve::common::WriteLockGuard;

namespace curve {
namespace snapshotserver {

// 快照后台线程扫描等待队列和工作队列的扫描周期(单位：ms)
const uint32_t kWaitingTaskScanTimeMs = 1000;

/**
 * @brief 快照任务管理器类
 */
class SnapshotTaskManager {
 public:
     /**
      * @brief 默认构造函数
      */
    SnapshotTaskManager()
        : isStop(true) {}

    /**
     * @brief 析构函数
     */
    ~SnapshotTaskManager() {
        Stop();
    }

    /**
     * @brief 初始化
     *
     * @param maxThreadNum 最大线程数
     *
     * @return 错误码
     */
    int Init(uint32_t maxThreadNum);

    /**
     * @brief 启动
     *
     * @return 错误码
     */
    int Start();

    /**
     * @brief 停止服务
     *
     * @return 错误码
     */
    int Stop();

    /**
     * @brief 添加任务
     *
     * @param task 快照任务
     *
     * @return 错误码
     */
    int PushTask(std::shared_ptr<SnapshotTask> task);

    /**
     * @brief 获取任务
     *
     * @param taskId 任务id
     *
     * @return 快照任务指针
     */
    std::shared_ptr<SnapshotTask> GetTask(const TaskIdType &taskId) const;

    /**
     * @brief 取消任务
     *
     * @param taskId 任务id
     *
     * @return 错误码
     */
    int CancelTask(const TaskIdType &taskId);

 private:
    /**
     * @brief 后台线程执行函数
     *
     * 定期执行扫描等待队列函数与扫描工作队列函数。
     */
    void BackEndThreadFunc();
    /**
     * @brief 扫描等待任务队列函数
     *
     * 扫描等待队列，判断工作队列中当前文件
     * 是否有正在执行的快照，若没有则放入工作队列
     *
     */
    void ScanWaitingTask();
    /**
     * @brief 扫描工作队列函数
     *
     * 扫描工作队列，判断工作队列中当前
     * 快照任务是否已完成，若完成则移出工作队列
     *
     */
    void ScanWorkingTask();

 private:
    // 后端线程
    std::thread backEndThread;

    // id->快照任务表
    std::map<TaskIdType, std::shared_ptr<SnapshotTask> > taskMap_;
    mutable RWLock taskMapLock_;

    // 快照等待队列
    std::list<std::shared_ptr<SnapshotTask> > waitingTasks_;
    mutable std::mutex waitingTasksLock_;

    // 快照工作队列,实际是个map，其中key是文件名，以便于查询
    std::map<std::string, std::shared_ptr<SnapshotTask> > workingTasks_;
    mutable std::mutex workingTasksLock_;

    // 快照异步执行线程池
    std::shared_ptr<SnapshotThreadPool> threadpool_;

    // 当前任务管理是否停止，用于支持start，stop功能
    std::atomic_bool isStop;
};

}  // namespace snapshotserver
}  // namespace curve

#endif  // CURVE_SRC_SNAPSHOT_SNAPSHOT_TASK_MANAGER_H_
