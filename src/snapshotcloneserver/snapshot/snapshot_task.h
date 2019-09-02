/*
 * Project: curve
 * Created Date: Wed Dec 12 2018
 * Author: xuchaojie
 * Copyright (c) 2018 netease
 */

#ifndef SRC_SNAPSHOTCLONESERVER_SNAPSHOT_SNAPSHOT_TASK_H_
#define SRC_SNAPSHOTCLONESERVER_SNAPSHOT_SNAPSHOT_TASK_H_

#include <string>
#include <memory>

#include "src/snapshotcloneserver/snapshot/snapshot_core.h"
#include "src/snapshotcloneserver/common/define.h"
#include "src/snapshotcloneserver/common/task.h"
#include "src/snapshotcloneserver/common/task_info.h"
#include "src/snapshotcloneserver/common/snapshotclone_metric.h"

namespace curve {
namespace snapshotcloneserver {

/**
 * @brief 快照任务信息
 */
class SnapshotTaskInfo : public TaskInfo {
 public:
     /**
      * @brief 构造函数
      *
      * @param snapInfo 快照信息
      */
    explicit SnapshotTaskInfo(const SnapshotInfo &snapInfo,
        std::shared_ptr<SnapshotInfoMetric> metric)
        : TaskInfo(),
          snapshotInfo_(snapInfo),
          metric_(metric) {}

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

    void UpdateMetric() {
        metric_->Update(this);
    }

 private:
    // 快照信息
    SnapshotInfo snapshotInfo_;
    // metric 信息
    std::shared_ptr<SnapshotInfoMetric> metric_;
};


class SnapshotTask : public Task {
 public:
    /**
      * @brief 构造函数
      *
      * @param taskId 快照任务id
      * @param taskInfo 快照任务信息
      */
    SnapshotTask(const TaskIdType &taskId,
        std::shared_ptr<SnapshotTaskInfo> taskInfo,
        std::shared_ptr<SnapshotCore> core)
        : Task(taskId),
          taskInfo_(taskInfo),
          core_(core) {}

    /**
     * @brief 获取快照任务信息对象指针
     *
     * @return 快照任务信息对象指针
     */
    std::shared_ptr<SnapshotTaskInfo> GetTaskInfo() const {
        return taskInfo_;
    }

 protected:
    // 快照任务信息
    std::shared_ptr<SnapshotTaskInfo> taskInfo_;
    // 快照核心逻辑对象
    std::shared_ptr<SnapshotCore> core_;
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
        : SnapshotTask(taskId, taskInfo, core) {}

    /**
     * @brief 快照执行函数
     */
    void Run() override {
        core_->HandleCreateSnapshotTask(taskInfo_);
    }
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
        : SnapshotTask(taskId, taskInfo, core) {}

    /**
     * @brief 快照执行函数
     */
    void Run() override {
        core_->HandleDeleteSnapshotTask(taskInfo_);
    }
};

struct TransferSnapshotDataChunkTaskInfo : public TaskInfo {
    ChunkDataName name_;
    uint64_t chunkSize_;
    ChunkIDInfo cidInfo_;
    uint64_t chunkSplitSize_;

    TransferSnapshotDataChunkTaskInfo(const ChunkDataName &name,
        uint64_t chunkSize,
        const ChunkIDInfo &cidInfo,
        uint64_t chunkSplitSize)
        : name_(name),
          chunkSize_(chunkSize),
          cidInfo_(cidInfo),
          chunkSplitSize_(chunkSplitSize) {}
};

class TransferSnapshotDataChunkTask : public TrackerTask {
 public:
    TransferSnapshotDataChunkTask(const TaskIdType &taskId,
        std::shared_ptr<TransferSnapshotDataChunkTaskInfo> taskInfo,
        std::shared_ptr<CurveFsClient> client,
        std::shared_ptr<SnapshotDataStore> dataStore)
        : TrackerTask(taskId),
          taskInfo_(taskInfo),
          client_(client),
          dataStore_(dataStore) {}

    std::shared_ptr<TransferSnapshotDataChunkTaskInfo> GetTaskInfo() const {
        return taskInfo_;
    }

    void Run() override {
        std::unique_ptr<TransferSnapshotDataChunkTask> self_guard(this);
        int ret = TransferSnapshotDataChunk();
        GetTracker()->HandleResponse(ret);
    }

 private:
    /**
     * @brief 转储快照单个chunk
     *
     * @return 错误码
     */
    int TransferSnapshotDataChunk();

 protected:
    std::shared_ptr<TransferSnapshotDataChunkTaskInfo> taskInfo_;
    std::shared_ptr<CurveFsClient> client_;
    std::shared_ptr<SnapshotDataStore> dataStore_;
};


}  // namespace snapshotcloneserver
}  // namespace curve

#endif  // SRC_SNAPSHOTCLONESERVER_SNAPSHOT_SNAPSHOT_TASK_H_
