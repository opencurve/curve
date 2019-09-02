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


struct CreateCloneChunkTaskInfo : public TaskInfo {
    std::string location_;
    ChunkIDInfo chunkidinfo_;
    uint64_t sn_;
    uint64_t csn_;
    uint64_t chunkSize_;

    CreateCloneChunkTaskInfo(
        const std::string &location,
        const ChunkIDInfo &chunkidinfo,
        uint64_t sn,
        uint64_t csn,
        uint64_t chunkSize)
        : TaskInfo(),
          location_(location),
          chunkidinfo_(chunkidinfo),
          sn_(sn),
          csn_(csn),
          chunkSize_(chunkSize) {}
};


class CreateCloneChunkTask : public TrackerTask {
 public:
     CreateCloneChunkTask(const TaskIdType &taskId,
        std::shared_ptr<CreateCloneChunkTaskInfo> taskInfo,
        std::shared_ptr<CurveFsClient> client)
        : TrackerTask(taskId),
          taskInfo_(taskInfo),
          client_(client) {}

    std::shared_ptr<CreateCloneChunkTaskInfo> GetTaskInfo() const {
        return taskInfo_;
    }

    void Run() override {
        std::unique_ptr<CreateCloneChunkTask> self_guard(this);
        int ret  = client_->CreateCloneChunk(taskInfo_->location_,
            taskInfo_->chunkidinfo_,
            taskInfo_->sn_,
            taskInfo_->csn_,
            taskInfo_->chunkSize_);
        if (ret != LIBCURVE_ERROR::OK) {
            LOG(ERROR) << "CreateCloneChunk fail"
                       << ", ret = " << ret
                       << ", location = " << taskInfo_->location_
                       << ", logicalPoolId = " << taskInfo_->chunkidinfo_.lpid_
                       << ", copysetId = " << taskInfo_->chunkidinfo_.cpid_
                       << ", chunkId = " << taskInfo_->chunkidinfo_.cid_
                       << ", seqNum = " << taskInfo_->sn_
                       << ", csn = " << taskInfo_->csn_;
        }
        GetTracker()->HandleResponse(ret);
    }

 protected:
    std::shared_ptr<CreateCloneChunkTaskInfo> taskInfo_;
    std::shared_ptr<CurveFsClient> client_;
};


}  // namespace snapshotcloneserver
}  // namespace curve

#endif  // SRC_SNAPSHOTCLONESERVER_CLONE_CLONE_TASK_H_
