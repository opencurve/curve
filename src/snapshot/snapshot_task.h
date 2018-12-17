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

class SnapshotTaskInfo {
 public:
    explicit SnapshotTaskInfo(const SnapshotInfo &snapInfo)
        : snapshotInfo_(snapInfo),
          progress_(0),
          isFinish_(false),
          isCanceled_(false) {}

    SnapshotInfo& GetSnapshotInfo() {
        return snapshotInfo_;
    }

    UUID GetUuid() const {
        return snapshotInfo_.GetUuid();
    }

    std::string GetFileName() const {
        return snapshotInfo_.GetFileName();
    }

    void SetProgress(uint32_t persent) {
        progress_ = persent;
    }

    uint32_t GetProgress() const {
        return progress_;
    }

    void Finish() {
        isFinish_ = true;
    }

    bool IsFinish() const {
        return isFinish_;
    }

    void Cancel() {
        isCanceled_ = true;
    }

    bool IsCanceled() const {
        return isCanceled_;
    }

    void Lock() {
        lock_.lock();
    }
    void UnLock() {
        lock_.unlock();
    }

 private:
    SnapshotInfo snapshotInfo_;
    uint32_t progress_;   //完成度
    bool isFinish_;   //是否完成
    bool isCanceled_;
    mutable std::mutex lock_;
};

class SnapshotTask {
 public:
    SnapshotTask(const TaskIdType &taskId,
        std::shared_ptr<SnapshotTaskInfo> taskInfo)
        : taskId_(taskId),
          taskInfo_(taskInfo) {}

    virtual ~SnapshotTask() {}

    SnapshotTask(const SnapshotTask&) = delete;
    SnapshotTask& operator=(const SnapshotTask&) = delete;
    SnapshotTask(SnapshotTask&&) = default;
    SnapshotTask& operator=(SnapshotTask&&) = default;

    virtual std::function<void()> clousre() {
        return [this] () {
            Run();
        };
    }

    TaskIdType GetTaskId() const {
        return taskId_;
    }

    std::shared_ptr<SnapshotTaskInfo> GetTaskInfo() const {
        return taskInfo_;
    }

    virtual void Run() = 0;

 protected:
    TaskIdType taskId_;
    std::shared_ptr<SnapshotTaskInfo> taskInfo_;
};

class SnapshotCreateTask : public SnapshotTask {
 public:
    SnapshotCreateTask(const TaskIdType &taskId,
        std::shared_ptr<SnapshotTaskInfo> taskInfo,
        std::shared_ptr<SnapshotCore> core)
        : SnapshotTask(taskId, taskInfo),
          core_(core) {}

    void Run() override {
        core_->HandleCreateSnapshotTask(taskInfo_);
    }

 private:
    std::shared_ptr<SnapshotCore> core_;
};

class SnapshotDeleteTask : public SnapshotTask {
 public:
    SnapshotDeleteTask(const TaskIdType &taskId,
        std::shared_ptr<SnapshotTaskInfo> taskInfo,
        std::shared_ptr<SnapshotCore> core)
        : SnapshotTask(taskId, taskInfo),
          core_(core) {}

    void Run() override {
        core_->HandleDeleteSnapshotTask(taskInfo_);
    }

 private:
    std::shared_ptr<SnapshotCore> core_;
};

}  // namespace snapshotserver
}  // namespace curve

#endif  // CURVE_SRC_SNAPSHOT_SNAPSHOT_TASK_H_
