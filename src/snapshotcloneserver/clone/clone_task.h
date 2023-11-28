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
 * Created Date: Thu Mar 21 2019
 * Author: xuchaojie
 */

#ifndef SRC_SNAPSHOTCLONESERVER_CLONE_CLONE_TASK_H_
#define SRC_SNAPSHOTCLONESERVER_CLONE_CLONE_TASK_H_

#include <memory>
#include <string>

#include "src/common/concurrent/dlock.h"
#include "src/common/snapshotclone/snapshotclone_define.h"
#include "src/snapshotcloneserver/clone/clone_closure.h"
#include "src/snapshotcloneserver/clone/clone_core.h"
#include "src/snapshotcloneserver/common/curvefs_client.h"
#include "src/snapshotcloneserver/common/snapshotclone_metric.h"
#include "src/snapshotcloneserver/common/task.h"
#include "src/snapshotcloneserver/common/task_info.h"

using ::curve::common::DLock;

namespace curve {
namespace snapshotcloneserver {

class CloneTaskInfo : public TaskInfo {
 public:
    CloneTaskInfo(const CloneInfo& cloneInfo,
                  std::shared_ptr<CloneInfoMetric> metric,
                  std::shared_ptr<CloneClosure> closure)
        : TaskInfo(),
          cloneInfo_(cloneInfo),
          metric_(metric),
          closure_(closure) {}

    CloneInfo& GetCloneInfo() { return cloneInfo_; }

    const CloneInfo& GetCloneInfo() const { return cloneInfo_; }

    TaskIdType GetTaskId() const { return cloneInfo_.GetTaskId(); }

    void UpdateMetric() { metric_->Update(this); }

    std::shared_ptr<CloneClosure> GetClosure() { return closure_; }

 private:
    CloneInfo cloneInfo_;
    std::shared_ptr<CloneInfoMetric> metric_;
    std::shared_ptr<CloneClosure> closure_;
};

std::ostream& operator<<(std::ostream& os, const CloneTaskInfo& taskInfo);

class CloneTaskBase : public Task {
 public:
    CloneTaskBase(const TaskIdType& taskId,
                  std::shared_ptr<CloneTaskInfo> taskInfo,
                  std::shared_ptr<CloneCore> core)
        : Task(taskId), taskInfo_(taskInfo), core_(core) {}

    std::shared_ptr<CloneTaskInfo> GetTaskInfo() const { return taskInfo_; }

 protected:
    std::shared_ptr<CloneTaskInfo> taskInfo_;
    std::shared_ptr<CloneCore> core_;
};

class CloneTask : public CloneTaskBase {
 public:
    CloneTask(const TaskIdType& taskId, std::shared_ptr<CloneTaskInfo> taskInfo,
              std::shared_ptr<CloneCore> core)
        : CloneTaskBase(taskId, taskInfo, core) {}

    void Run() override {
        // get dlock
        std::shared_ptr<CloneClosure> closure = taskInfo_->GetClosure();
        if (nullptr != closure) {
            std::shared_ptr<DLock> dlock = closure->GetDLock();
            if (nullptr != dlock) {
                if (EtcdErrCode::EtcdOK != dlock->Lock()) {
                    LOG(ERROR) << "Get dlock failed in CloneTask, "
                               << "dlock key is " << dlock->GetPrefix();
                    return;
                }
            }
        }

        core_->HandleCloneOrRecoverTask(taskInfo_);
    }
};

class CloneCleanTask : public CloneTaskBase {
 public:
    CloneCleanTask(const TaskIdType& taskId,
                   std::shared_ptr<CloneTaskInfo> taskInfo,
                   std::shared_ptr<CloneCore> core)
        : CloneTaskBase(taskId, taskInfo, core) {}

    void Run() override { core_->HandleCleanCloneOrRecoverTask(taskInfo_); }
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

struct CreateCloneChunkContext {
    // Data source
    std::string location;
    // Chunkid information
    ChunkIDInfo cidInfo;
    // seqNum
    uint64_t sn;
    // correctSn
    uint64_t csn;
    // chunk size
    uint64_t chunkSize;
    // Return value
    int retCode;
    // taskid
    TaskIdType taskid;
    // Asynchronous request start time
    uint64_t startTime;
    // Total retry time for asynchronous requests
    uint64_t clientAsyncMethodRetryTimeSec;
    // Chunk Information
    struct CloneChunkInfo* cloneChunkInfo;
};

using CreateCloneChunkContextPtr = std::shared_ptr<CreateCloneChunkContext>;

struct CreateCloneChunkClosure : public SnapCloneClosure {
    CreateCloneChunkClosure(
        std::shared_ptr<CreateCloneChunkTaskTracker> tracker,
        CreateCloneChunkContextPtr context)
        : tracker_(tracker), context_(context) {}
    void Run() {
        std::unique_ptr<CreateCloneChunkClosure> self_guard(this);
        context_->retCode = GetRetCode();
        if (context_->retCode < 0) {
            LOG(WARNING) << "CreateCloneChunkClosure return fail"
                         << ", ret = " << context_->retCode
                         << ", location = " << context_->location
                         << ", logicalPoolId = " << context_->cidInfo.lpid_
                         << ", copysetId = " << context_->cidInfo.cpid_
                         << ", chunkId = " << context_->cidInfo.cid_
                         << ", seqNum = " << context_->sn
                         << ", csn = " << context_->csn
                         << ", taskid = " << context_->taskid;
        }
        tracker_->PushResultContext(context_);
        tracker_->HandleResponse(context_->retCode);
    }
    std::shared_ptr<CreateCloneChunkTaskTracker> tracker_;
    CreateCloneChunkContextPtr context_;
};

struct RecoverChunkContext {
    // Chunkid information
    ChunkIDInfo cidInfo;
    // Chunk's sharding index
    uint64_t partIndex;
    // Total Chunk Fragments
    uint64_t totalPartNum;
    // Slice size
    uint64_t partSize;
    // Return value
    int retCode;
    // taskid
    TaskIdType taskid;
    // Asynchronous request start time
    uint64_t startTime;
    // Total retry time for asynchronous requests
    uint64_t clientAsyncMethodRetryTimeSec;
};

using RecoverChunkContextPtr = std::shared_ptr<RecoverChunkContext>;

struct RecoverChunkClosure : public SnapCloneClosure {
    RecoverChunkClosure(std::shared_ptr<RecoverChunkTaskTracker> tracker,
                        RecoverChunkContextPtr context)
        : tracker_(tracker), context_(context) {}
    void Run() {
        std::unique_ptr<RecoverChunkClosure> self_guard(this);
        context_->retCode = GetRetCode();
        if (context_->retCode < 0) {
            LOG(WARNING) << "RecoverChunkClosure return fail"
                         << ", ret = " << context_->retCode
                         << ", logicalPoolId = " << context_->cidInfo.lpid_
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
