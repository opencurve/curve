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
 * Created Date: Wed Dec 12 2018
 * Author: xuchaojie
 */

#ifndef SRC_SNAPSHOTCLONESERVER_SNAPSHOT_SNAPSHOT_TASK_H_
#define SRC_SNAPSHOTCLONESERVER_SNAPSHOT_SNAPSHOT_TASK_H_

#include <list>
#include <memory>
#include <string>

#include "src/common/snapshotclone/snapshotclone_define.h"
#include "src/snapshotcloneserver/common/snapshotclone_metric.h"
#include "src/snapshotcloneserver/common/task.h"
#include "src/snapshotcloneserver/common/task_info.h"
#include "src/snapshotcloneserver/common/task_tracker.h"
#include "src/snapshotcloneserver/snapshot/snapshot_core.h"

namespace curve {
namespace snapshotcloneserver {

/**
 * @brief snapshot task information
 */
class SnapshotTaskInfo : public TaskInfo {
 public:
    /**
     * @brief constructor
     *
     * @param snapInfo snapshot information
     */
    explicit SnapshotTaskInfo(const SnapshotInfo& snapInfo,
                              std::shared_ptr<SnapshotInfoMetric> metric)
        : TaskInfo(), snapshotInfo_(snapInfo), metric_(metric) {}

    /**
     * @brief Get snapshot information
     *
     * @return snapshot information
     */
    SnapshotInfo& GetSnapshotInfo() { return snapshotInfo_; }

    /**
     * @brief Get snapshot uuid
     *
     * @return snapshot uuid
     */
    UUID GetUuid() const { return snapshotInfo_.GetUuid(); }

    /**
     * @brief Get file name
     *
     * @return file name
     */
    std::string GetFileName() const { return snapshotInfo_.GetFileName(); }

    void UpdateMetric() { metric_->Update(this); }

 private:
    // Snapshot Information
    SnapshotInfo snapshotInfo_;
    // Metric Information
    std::shared_ptr<SnapshotInfoMetric> metric_;
};

class SnapshotTask : public Task {
 public:
    /**
     * @brief constructor
     *
     * @param taskId Snapshot task ID
     * @param taskInfo snapshot task information
     */
    SnapshotTask(const TaskIdType& taskId,
                 std::shared_ptr<SnapshotTaskInfo> taskInfo,
                 std::shared_ptr<SnapshotCore> core)
        : Task(taskId), taskInfo_(taskInfo), core_(core) {}

    /**
     * @brief Get snapshot task information object pointer
     *
     * @return Snapshot task information object pointer
     */
    std::shared_ptr<SnapshotTaskInfo> GetTaskInfo() const { return taskInfo_; }

 protected:
    // Snapshot Task Information
    std::shared_ptr<SnapshotTaskInfo> taskInfo_;
    // Snapshot Core Logical Object
    std::shared_ptr<SnapshotCore> core_;
};

/**
 * @brief Create snapshot task
 */
class SnapshotCreateTask : public SnapshotTask {
 public:
    /**
     * @brief constructor
     *
     * @param taskId Snapshot task ID
     * @param taskInfo snapshot task information
     * @param core snapshot core logical object
     */
    SnapshotCreateTask(const TaskIdType& taskId,
                       std::shared_ptr<SnapshotTaskInfo> taskInfo,
                       std::shared_ptr<SnapshotCore> core)
        : SnapshotTask(taskId, taskInfo, core) {}

    /**
     * @brief snapshot execution function
     */
    void Run() override { core_->HandleCreateSnapshotTask(taskInfo_); }
};

/**
 * @brief Delete snapshot task
 */
class SnapshotDeleteTask : public SnapshotTask {
 public:
    /**
     * @brief constructor
     *
     * @param taskId Snapshot task ID
     * @param taskInfo snapshot task information
     * @param core snapshot core logical object
     */
    SnapshotDeleteTask(const TaskIdType& taskId,
                       std::shared_ptr<SnapshotTaskInfo> taskInfo,
                       std::shared_ptr<SnapshotCore> core)
        : SnapshotTask(taskId, taskInfo, core) {}

    /**
     * @brief snapshot execution function
     */
    void Run() override { core_->HandleDeleteSnapshotTask(taskInfo_); }
};

struct ReadChunkSnapshotContext {
    // Chunkid information
    ChunkIDInfo cidInfo;
    // seq
    uint64_t seqNum;
    // Fragmented index
    uint64_t partIndex;
    // Sliced buffer
    std::unique_ptr<char[]> buf;
    // Slice length
    uint64_t len;
    // Return value
    int retCode;
    // Asynchronous request start time
    uint64_t startTime;
    // Total retry time for asynchronous requests
    uint64_t clientAsyncMethodRetryTimeSec;
};

using ReadChunkSnapshotContextPtr = std::shared_ptr<ReadChunkSnapshotContext>;
using ReadChunkSnapshotTaskTracker =
    ContextTaskTracker<ReadChunkSnapshotContextPtr>;

struct ReadChunkSnapshotClosure : public SnapCloneClosure {
    ReadChunkSnapshotClosure(
        std::shared_ptr<ReadChunkSnapshotTaskTracker> tracker,
        std::shared_ptr<ReadChunkSnapshotContext> context)
        : tracker_(tracker), context_(context) {}
    void Run() override;
    std::shared_ptr<ReadChunkSnapshotTaskTracker> tracker_;
    std::shared_ptr<ReadChunkSnapshotContext> context_;
};

struct TransferSnapshotDataChunkTaskInfo : public TaskInfo {
    ChunkDataName name_;
    uint64_t chunkSize_;
    ChunkIDInfo cidInfo_;
    uint64_t chunkSplitSize_;
    uint64_t clientAsyncMethodRetryTimeSec_;
    uint64_t clientAsyncMethodRetryIntervalMs_;
    uint32_t readChunkSnapshotConcurrency_;

    TransferSnapshotDataChunkTaskInfo(const ChunkDataName& name,
                                      uint64_t chunkSize,
                                      const ChunkIDInfo& cidInfo,
                                      uint64_t chunkSplitSize,
                                      uint64_t clientAsyncMethodRetryTimeSec,
                                      uint64_t clientAsyncMethodRetryIntervalMs,
                                      uint32_t readChunkSnapshotConcurrency)
        : name_(name),
          chunkSize_(chunkSize),
          cidInfo_(cidInfo),
          chunkSplitSize_(chunkSplitSize),
          clientAsyncMethodRetryTimeSec_(clientAsyncMethodRetryTimeSec),
          clientAsyncMethodRetryIntervalMs_(clientAsyncMethodRetryIntervalMs),
          readChunkSnapshotConcurrency_(readChunkSnapshotConcurrency) {}
};

class TransferSnapshotDataChunkTask : public TrackerTask {
 public:
    TransferSnapshotDataChunkTask(
        const TaskIdType& taskId,
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
     * @brief Dump snapshot single chunk
     *
     * @return error code
     */
    int TransferSnapshotDataChunk();

    /**
     * @brief Start asynchronous ReadSnapshotChunk
     *
     * @param tracker asynchronous ReadSnapshotChunk tracker
     * @param context ReadSnapshotChunk context
     *
     * @return error code
     */
    int StartAsyncReadChunkSnapshot(
        std::shared_ptr<ReadChunkSnapshotTaskTracker> tracker,
        std::shared_ptr<ReadChunkSnapshotContext> context);

    /**
     * @brief Process the results of ReadChunkSnapshot and try again
     *
     * @param tracker asynchronous ReadSnapshotChunk tracker
     * @param transferTask Dump Task
     * @param results ReadChunkSnapshot result list
     *
     * @return error code
     */
    int HandleReadChunkSnapshotResultsAndRetry(
        std::shared_ptr<ReadChunkSnapshotTaskTracker> tracker,
        std::shared_ptr<TransferTask> transferTask,
        const std::list<ReadChunkSnapshotContextPtr>& results);

 protected:
    std::shared_ptr<TransferSnapshotDataChunkTaskInfo> taskInfo_;
    std::shared_ptr<CurveFsClient> client_;
    std::shared_ptr<SnapshotDataStore> dataStore_;
};

}  // namespace snapshotcloneserver
}  // namespace curve

#endif  // SRC_SNAPSHOTCLONESERVER_SNAPSHOT_SNAPSHOT_TASK_H_
