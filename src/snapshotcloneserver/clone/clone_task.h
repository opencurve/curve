/*
 * Project: curve
 * Created Date: Thu Mar 21 2019
 * Author: xuchaojie
 * Copyright (c) 2018 netease
 */

#ifndef SRC_SNAPSHOTCLONESERVER_CLONE_CLONE_TASK_H_
#define SRC_SNAPSHOTCLONESERVER_CLONE_CLONE_TASK_H_

#include <string>
#include <memory>

#include "src/snapshotcloneserver/clone/clone_core.h"
#include "src/snapshotcloneserver/common/define.h"
#include "src/snapshotcloneserver/common/task.h"
#include "src/snapshotcloneserver/common/task_info.h"

namespace curve {
namespace snapshotcloneserver {

class CloneTaskInfo : public TaskInfo {
 public:
    explicit CloneTaskInfo(const CloneInfo &cloneInfo)
        : TaskInfo(),
          cloneInfo_(cloneInfo) {}

    CloneInfo& GetCloneInfo() {
        return cloneInfo_;
    }

 private:
    CloneInfo cloneInfo_;
};

class CloneTask : public Task {
 public:
    CloneTask(const TaskIdType &taskId,
        std::shared_ptr<CloneTaskInfo> taskInfo,
        std::shared_ptr<CloneCore> core)
        : Task(taskId),
          taskInfo_(taskInfo),
          core_(core) {}

    std::shared_ptr<CloneTaskInfo> GetTaskInfo() const {
        return taskInfo_;
    }

    void Run() override {
        core_->HandleCloneOrRecoverTask(taskInfo_);
    }

 protected:
    std::shared_ptr<CloneTaskInfo> taskInfo_;
    std::shared_ptr<CloneCore> core_;
};


}  // namespace snapshotcloneserver
}  // namespace curve

#endif  // SRC_SNAPSHOTCLONESERVER_CLONE_CLONE_TASK_H_
