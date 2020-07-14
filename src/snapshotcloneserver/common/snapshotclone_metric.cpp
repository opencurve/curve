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
 * Created Date: Tue Sep 10 2019
 * Author: xuchaojie
 */

#include "src/snapshotcloneserver/common/snapshotclone_metric.h"
#include "src/snapshotcloneserver/snapshot/snapshot_task.h"
#include "src/snapshotcloneserver/clone/clone_task.h"


namespace curve {
namespace snapshotcloneserver {

void SnapshotInfoMetric::Update(SnapshotTaskInfo *taskInfo) {
    SnapshotInfo &snapInfo = taskInfo->GetSnapshotInfo();
    metric.Set("UUID", snapInfo.GetUuid());
    metric.Set("User", snapInfo.GetUser());
    metric.Set("FileName", snapInfo.GetFileName());
    metric.Set("SnapshotName", snapInfo.GetSnapshotName());
    metric.Set("SeqNum", std::to_string(snapInfo.GetSeqNum()));
    metric.Set("ChunkSize", std::to_string(snapInfo.GetChunkSize()));
    metric.Set("SegmentSize", std::to_string(snapInfo.GetSegmentSize()));
    metric.Set("FileLength", std::to_string(snapInfo.GetFileLength()));
    metric.Set("CreateTime", std::to_string(snapInfo.GetCreateTime()));
    metric.Set("Status", std::to_string(
        static_cast<int>(snapInfo.GetStatus())));

    metric.Set("Progress", std::to_string(taskInfo->GetProgress()));

    metric.Update();
}

void CloneMetric::UpdateBeforeTaskBegin(
    const CloneTaskType &taskType) {
    if (CloneTaskType::kClone == taskType) {
        cloneDoing << 1;
    } else {
        recoverDoing << 1;
    }
}

void CloneMetric::UpdateAfterTaskFinish(
    const CloneTaskType &taskType,
    const CloneStatus &status) {
    if (CloneStatus::done == status ||
        CloneStatus::metaInstalled == status) {
        if (CloneTaskType::kClone == taskType) {
            cloneDoing << -1;
            cloneSucceed << 1;
        } else {
            recoverDoing << -1;
            recoverSucceed << 1;
        }
    } else if (CloneStatus::error == status) {
        if (CloneTaskType::kClone == taskType) {
            cloneDoing << -1;
            cloneFailed << 1;
        } else {
            recoverDoing << -1;
            recoverFailed << 1;
        }
    } else {
        LOG(ERROR) << "CloneMetric::UpdateAfterTaskFinish when not finish! "
                   << "status = " << static_cast<int>(status);
    }
}

void CloneMetric::UpdateFlattenTaskBegin() {
    flattenDoing << 1;
}

void CloneMetric::UpdateAfterFlattenTaskFinish(
    const CloneStatus &status) {
    if (CloneStatus::done == status) {
        flattenDoing << -1;
        flattenSucceed << 1;
    } else if (CloneStatus::error == status) {
        flattenDoing << -1;
        flattenFailed << 1;
    } else {
        LOG(ERROR) << "CloneMetric::UpdateAfterFlattenTaskFinish "
                   << "when not finish! "
                   << "status = " << static_cast<int>(status);
    }
}

void CloneInfoMetric::Update(CloneTaskInfo *taskInfo) {
    CloneInfo &cloneInfo = taskInfo->GetCloneInfo();
    metric.Set("UUID", cloneInfo.GetTaskId());
    metric.Set("User", cloneInfo.GetUser());
    metric.Set("CloneTaskType",
        (cloneInfo.GetTaskType() == CloneTaskType::kClone) ?
            "Clone" : "Recover");
    metric.Set("Source", cloneInfo.GetSrc());
    metric.Set("Destination", cloneInfo.GetDest());
    metric.Set("OriginId", std::to_string(cloneInfo.GetOriginId()));
    metric.Set("DestId", std::to_string(cloneInfo.GetDestId()));
    metric.Set("CreateTime", std::to_string(cloneInfo.GetTime()));
    metric.Set("FileType",
        (cloneInfo.GetFileType() == CloneFileType::kFile) ?
            "File" : "Snapshot");
    metric.Set("Lazy",
        (cloneInfo.GetIsLazy()) ? "True" : "False");
    metric.Set("Step", std::to_string(
        static_cast<int>(cloneInfo.GetNextStep())));
    metric.Set("Status", std::to_string(
        static_cast<int>(cloneInfo.GetStatus())));

    metric.Set("Progress", std::to_string(taskInfo->GetProgress()));

    metric.Update();
}


}  // namespace snapshotcloneserver
}  // namespace curve
