/*
 * Project: curve
 * Created Date: Wed Dec 12 2018
 * Author: xuchaojie
 * Copyright (c) 2018 netease
 */

#ifndef CURVE_SRC_SNAPSHOT_SNAPSHOT_TASK_H_
#define CURVE_SRC_SNAPSHOT_SNAPSHOT_TASK_H_

#include <functional>
#include <string>
#include <memory>

#include "src/snapshot/snapshot_core.h"
#include "src/snapshot/snapshot_define.h"

namespace curve {
namespace snapshotserver {

using TaskIdType = UUID;

/**
 * @brief 快照任务信息
 */
class SnapshotTaskInfo {
 public:
     /**
      * @brief 构造函数
      *
      * @param snapInfo 快照信息
      */
    explicit SnapshotTaskInfo(const SnapshotInfo &snapInfo)
        : snapshotInfo_(snapInfo),
          progress_(0),
          isFinish_(false),
          isCanceled_(false) {}

    /**
     * @brief 获取快照信息
     *
     * @return 快照信息
     */
    SnapshotInfo& GetSnapshotInfo() {
        return snapshotInfo_;
    }

    /**
     * @brief 获取快照uuid
     *
     * @return 快照uuid
     */
    UUID GetUuid() const {
        return snapshotInfo_.GetUuid();
    }

    /**
     * @brief 获取文件名
     *
     * @return 文件名
     */
    std::string GetFileName() const {
        return snapshotInfo_.GetFileName();
    }

    /**
     * @brief 设置快照完成度百分比
     *
     * @param persent 快照完成度百分比
     */
    void SetProgress(uint32_t persent) {
        progress_ = persent;
    }

    /**
     * @brief 获取快照完成度百分比
     *
     * @return 快照完成度百分比
     */
    uint32_t GetProgress() const {
        return progress_;
    }

    /**
     * @brief 完成快照
     */
    void Finish() {
        isFinish_ = true;
    }

    /**
     * @brief 获取快照是否完成
     *
     * @retval true 快照完成
     * @retval false 快照未完成
     */
    bool IsFinish() const {
        return isFinish_;
    }

    /**
     * @brief 取消快照
     */
    void Cancel() {
        isCanceled_ = true;
    }

    /**
     * @brief 获取快照是否取消
     *
     * @retval true 快照已取消
     * @retval false 快照未取消
     */
    bool IsCanceled() const {
        return isCanceled_;
    }

    /**
     * @brief 锁定快照
     *
     *  用于同步快照完成和取消功能
     *  1. 快照完成前，先锁定快照，然后判断快照是否取消，
     *  若已取消，则释放锁，
     *  否则执行快照完成逻辑之后释放锁。
     *  2. 快照取消前，先锁定快照，然后判断快照是否完成，
     *  若已完成，则释放锁，
     *  否则执行快照取消逻辑之后释放锁。
     */
    void Lock() {
        lock_.lock();
    }

    /**
     * @brief 释放锁定快照
     */
    void UnLock() {
        lock_.unlock();
    }

 private:
    // 快照信息
    SnapshotInfo snapshotInfo_;
    // 快照完成度百分比
    uint32_t progress_;
    // 快照任务是否结束
    bool isFinish_;
    // 快照是否被取消
    bool isCanceled_;
    mutable std::mutex lock_;
};

/**
 * @brief 快照任务基类
 */
class SnapshotTask {
 public:
     /**
      * @brief 构造函数
      *
      * @param taskId 快照任务id
      * @param taskInfo 快照任务信息
      */
    SnapshotTask(const TaskIdType &taskId,
        std::shared_ptr<SnapshotTaskInfo> taskInfo)
        : taskId_(taskId),
          taskInfo_(taskInfo) {}

    virtual ~SnapshotTask() {}

    SnapshotTask(const SnapshotTask&) = delete;
    SnapshotTask& operator=(const SnapshotTask&) = delete;
    SnapshotTask(SnapshotTask&&) = default;
    SnapshotTask& operator=(SnapshotTask&&) = default;

    /**
     * @brief 获取快照任务执行体闭包
     *
     * @return 快照任务执行体
     */
    virtual std::function<void()> clousre() {
        return [this] () {
            Run();
        };
    }

    /**
     * @brief 获取快照任务id
     *
     * @return 快照任务id
     */
    TaskIdType GetTaskId() const {
        return taskId_;
    }

    /**
     * @brief 获取快照任务信息对象指针
     *
     * @return 快照任务信息对象指针
     */
    std::shared_ptr<SnapshotTaskInfo> GetTaskInfo() const {
        return taskInfo_;
    }

    /**
     * @brief 快照执行函数接口
     */
    virtual void Run() = 0;

 protected:
    // 快照id
    TaskIdType taskId_;
    // 快照任务信息
    std::shared_ptr<SnapshotTaskInfo> taskInfo_;
};

/**
 * @brief 创建快照任务
 */
class SnapshotCreateTask : public SnapshotTask {
 public:
     /**
      * @brief 构造函数
      *
      * @param taskId 快照任务id
      * @param taskInfo 快照任务信息
      * @param core 快照核心逻辑对象
      */
    SnapshotCreateTask(const TaskIdType &taskId,
        std::shared_ptr<SnapshotTaskInfo> taskInfo,
        std::shared_ptr<SnapshotCore> core)
        : SnapshotTask(taskId, taskInfo),
          core_(core) {}

    /**
     * @brief 快照执行函数
     */
    void Run() override {
        core_->HandleCreateSnapshotTask(taskInfo_);
    }

 private:
    // 快照核心逻辑对象
    std::shared_ptr<SnapshotCore> core_;
};

/**
 * @brief 删除快照任务
 */
class SnapshotDeleteTask : public SnapshotTask {
 public:
     /**
      * @brief 构造函数
      *
      * @param taskId 快照任务id
      * @param taskInfo 快照任务信息
      * @param core 快照核心逻辑对象
      */
    SnapshotDeleteTask(const TaskIdType &taskId,
        std::shared_ptr<SnapshotTaskInfo> taskInfo,
        std::shared_ptr<SnapshotCore> core)
        : SnapshotTask(taskId, taskInfo),
          core_(core) {}

    /**
     * @brief 快照执行函数
     */
    void Run() override {
        core_->HandleDeleteSnapshotTask(taskInfo_);
    }

 private:
    // 快照核心逻辑对象
    std::shared_ptr<SnapshotCore> core_;
};

}  // namespace snapshotserver
}  // namespace curve

#endif  // CURVE_SRC_SNAPSHOT_SNAPSHOT_TASK_H_
