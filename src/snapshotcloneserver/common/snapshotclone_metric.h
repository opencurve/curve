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
 * Created Date: Mon Sep 09 2019
 * Author: xuchaojie
 */

#ifndef SRC_SNAPSHOTCLONESERVER_COMMON_SNAPSHOTCLONE_METRIC_H_
#define SRC_SNAPSHOTCLONESERVER_COMMON_SNAPSHOTCLONE_METRIC_H_

#include <bvar/bvar.h>
#include <string>
#include <map>
#include <memory>
#include "src/common/stringstatus.h"
#include "src/snapshotcloneserver/common/snapshotclone_meta_store.h"

using ::curve::common::StringStatus;

namespace curve {
namespace snapshotcloneserver {

// forward declaration
class SnapshotTaskInfo;
class CloneTaskInfo;

static uint32_t GetSnapshotTotalNum(void *arg) {
    SnapshotCloneMetaStore *metaStore =
        reinterpret_cast<SnapshotCloneMetaStore*>(arg);
    uint32_t snapshotCount = 0;
    if (metaStore != nullptr) {
        snapshotCount = metaStore->GetSnapshotCount();
    }
    return snapshotCount;
}

struct SnapshotMetric {
    const std::string SnapshotMetricPrefix =
        "snapshotcloneserver_snapshot_metric_";

    // 正在进行的快照数量
    bvar::Adder<uint32_t> snapshotDoing;
    // 正在等待的快照数量
    bvar::Adder<uint32_t> snapshotWaiting;
    // 累计成功的快照数量
    bvar::Adder<uint32_t> snapshotSucceed;
    // 累计失败的快照数量
    bvar::Adder<uint32_t> snapshotFailed;

    std::shared_ptr<SnapshotCloneMetaStore> metaStore_;
    // 系统内快照总量
    bvar::PassiveStatus<uint32_t> snapshotNum;

    explicit SnapshotMetric(std::shared_ptr<SnapshotCloneMetaStore> metaStore) :
        snapshotDoing(SnapshotMetricPrefix, "snapshot_doing"),
        snapshotWaiting(SnapshotMetricPrefix, "snapshot_waiting"),
        snapshotSucceed(SnapshotMetricPrefix, "snapshot_succeed"),
        snapshotFailed(SnapshotMetricPrefix, "snapshot_failed"),
        metaStore_(metaStore),
        snapshotNum(SnapshotMetricPrefix + "snapshot_total_num",
            GetSnapshotTotalNum, metaStore_.get()) {}
};

struct SnapshotInfoMetric {
    const std::string SnapshotInfoMetricPrefix =
        "snapshotcloneserver_snapshotInfo_metric_";
    StringStatus metric;

    explicit SnapshotInfoMetric(const std::string &snapshotId) {
        metric.ExposeAs(SnapshotInfoMetricPrefix, snapshotId);
    }

    void Update(SnapshotTaskInfo *taskInfo);
};

struct CloneMetric {
    const std::string CloneMetricPrefix =
        "snapshotcloneserver_clone_metric_";

    // 正在执行的克隆任务数量
    bvar::Adder<uint32_t> cloneDoing;
    // 累计成功的克隆任务数量
    bvar::Adder<uint32_t> cloneSucceed;
    // 累计失败的克隆任务数量
    bvar::Adder<uint32_t> cloneFailed;

    // 正在执行的恢复任务数量
    bvar::Adder<uint32_t> recoverDoing;
    // 累计成功的恢复任务数量
    bvar::Adder<uint32_t> recoverSucceed;
    // 累计失败的恢复任务数量
    bvar::Adder<uint32_t> recoverFailed;

    // 正在执行的Flatten任务数量
    bvar::Adder<uint32_t> flattenDoing;
    // 累计成功的Flatten任务数量
    bvar::Adder<uint32_t> flattenSucceed;
    // 累计失败的Flatten任务数量
    bvar::Adder<uint32_t> flattenFailed;

    CloneMetric() :
        cloneDoing(CloneMetricPrefix, "clone_doing"),
        cloneSucceed(CloneMetricPrefix, "clone_succeed"),
        cloneFailed(CloneMetricPrefix, "clone_failed"),
        recoverDoing(CloneMetricPrefix, "recover_doing"),
        recoverSucceed(CloneMetricPrefix, "recover_succeed"),
        recoverFailed(CloneMetricPrefix, "recover_failed"),
        flattenDoing(CloneMetricPrefix, "flatten_doing"),
        flattenSucceed(CloneMetricPrefix, "flatten_succeed"),
        flattenFailed(CloneMetricPrefix, "flatten_failed") {}

    void UpdateBeforeTaskBegin(
        const CloneTaskType &taskType);

    void UpdateAfterTaskFinish(
        const CloneTaskType &taskType,
        const CloneStatus &status);

    void UpdateFlattenTaskBegin();

    void UpdateAfterFlattenTaskFinish(
        const CloneStatus &status);
};

struct CloneInfoMetric {
    const std::string CloneInfoMetricPrefix =
        "snapshotcloneserver_cloneInfo_metric_";
    StringStatus metric;

    explicit CloneInfoMetric(const std::string &cloneTaskId) {
        metric.ExposeAs(CloneInfoMetricPrefix, cloneTaskId);
    }

    void Update(CloneTaskInfo *taskInfo);
};


}  // namespace snapshotcloneserver
}  // namespace curve

#endif  // SRC_SNAPSHOTCLONESERVER_COMMON_SNAPSHOTCLONE_METRIC_H_
