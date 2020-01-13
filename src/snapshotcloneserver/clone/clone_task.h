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
#include "src/snapshotcloneserver/common/snapshotclone_metric.h"
#include "src/snapshotcloneserver/common/curvefs_client.h"
#include "src/snapshotcloneserver/clone/clone_closure.h"

namespace curve {
namespace snapshotcloneserver {

class CloneTaskInfo : public TaskInfo {
 public:
    CloneTaskInfo(const CloneInfo &cloneInfo,
        std::shared_ptr<CloneInfoMetric> metric,
        std::shared_ptr<CloneClosure> closure)
        : TaskInfo(),
          cloneInfo_(cloneInfo),
          metric_(metric),
          closure_(closure) {}

    CloneInfo& GetCloneInfo() {
        return cloneInfo_;
    }

    TaskIdType GetTaskId() const {
        return cloneInfo_.GetTaskId();
    }

    void UpdateMetric() {
        metric_->Update(this);
    }

    std::shared_ptr<CloneClosure> GetClosure() {
        return closure_;
    }

 private:
    CloneInfo cloneInfo_;
    std::shared_ptr<CloneInfoMetric> metric_;
    std::shared_ptr<CloneClosure> closure_;
};



class CloneTaskBase : public Task {
 public:
    CloneTaskBase(const TaskIdType &taskId,
        std::shared_ptr<CloneTaskInfo> taskInfo,
        std::shared_ptr<CloneCore> core)
        : Task(taskId),
          taskInfo_(taskInfo),
          core_(core) {}

    std::shared_ptr<CloneTaskInfo> GetTaskInfo() const {
        return taskInfo_;
    }

 protected:
    std::shared_ptr<CloneTaskInfo> taskInfo_;
    std::shared_ptr<CloneCore> core_;
};

class CloneTask : public CloneTaskBase {
 public:
    CloneTask(const TaskIdType &taskId,
        std::shared_ptr<CloneTaskInfo> taskInfo,
        std::shared_ptr<CloneCore> core)
        : CloneTaskBase(taskId, taskInfo, core) {}

    void Run() override {
        core_->HandleCloneOrRecoverTask(taskInfo_);
    }
};


class CloneCleanTask : public CloneTaskBase {
 public:
    CloneCleanTask(const TaskIdType &taskId,
        std::shared_ptr<CloneTaskInfo> taskInfo,
        std::shared_ptr<CloneCore> core)
        : CloneTaskBase(taskId, taskInfo, core) {}

    void Run() override {
        core_->HandleCleanCloneOrRecoverTask(taskInfo_);
    }
};

struct SnapCloneCommonClosure : public SnapCloneClosure {
    explicit SnapCloneCommonClosure(std::shared_ptr<TaskTracker> tracker)
        : tracker_(tracker) {}
    void Run() {
        std::unique_ptr<SnapCloneCommonClosure> self_guard(this);
        tracker_->HandleResponse(GetRetCode());
    }
    std::shared_ptr<TaskTracker> tracker_;
};


struct RecoverChunkContext {
    // chunkid 信息
    ChunkIDInfo cidInfo;
    // chunk的分片index
    uint64_t partIndex;
    // 总的chunk分片数
    uint64_t totalPartNum;
    // 分片大小
    uint64_t partSize;
    // RecoverChunk请求的返回值
    int retCode;
    // taskid
    TaskIdType taskid;
};

using RecoverChunkContextPtr = std::shared_ptr<RecoverChunkContext>;

struct RecoverChunkClosure : public SnapCloneClosure {
    RecoverChunkClosure(std::shared_ptr<RecoverChunkTaskTracker> tracker,
        RecoverChunkContextPtr context)
        : tracker_(tracker),
          context_(context) {}
    void Run() {
        std::unique_ptr<RecoverChunkClosure> self_guard(this);
        context_->retCode = GetRetCode();
        if (context_->retCode < 0) {
            LOG(ERROR) << "RecoverChunkClosure return fail"
                       << ", ret = " << context_->retCode
                       << ", logicalPoolId = "
                       << context_->cidInfo.lpid_
                       << ", copysetId = " << context_->cidInfo.cpid_
                       << ", chunkId = " << context_->cidInfo.cid_
                       << ", partIndex = " << context_->partIndex
                       << ", partSize = " << context_->partSize
                       << ", taskid = " << context_->taskid;
        }
        tracker_->PushResultContext(context_);
        tracker_->HandleResponse(context_->retCode);
    }
    std::shared_ptr<RecoverChunkTaskTracker> tracker_;
    RecoverChunkContextPtr context_;
};

}  // namespace snapshotcloneserver
}  // namespace curve

#endif  // SRC_SNAPSHOTCLONESERVER_CLONE_CLONE_TASK_H_
