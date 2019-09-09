/*
 * Project: curve
 * Created Date: Tue Sep 10 2019
 * Author: xuchaojie
 * Copyright (c) 2019 netease
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
