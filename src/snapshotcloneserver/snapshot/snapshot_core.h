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
 * Created Date: Sat Dec 15 2018
 * Author: xuchaojie
 */

#ifndef SRC_SNAPSHOTCLONESERVER_SNAPSHOT_SNAPSHOT_CORE_H_
#define SRC_SNAPSHOTCLONESERVER_SNAPSHOT_SNAPSHOT_CORE_H_

#include <map>
#include <memory>
#include <string>
#include <vector>

#include "src/common/concurrent/name_lock.h"
#include "src/common/snapshotclone/snapshotclone_define.h"
#include "src/snapshotcloneserver/common/config.h"
#include "src/snapshotcloneserver/common/curvefs_client.h"
#include "src/snapshotcloneserver/common/snapshot_reference.h"
#include "src/snapshotcloneserver/common/snapshotclone_meta_store.h"
#include "src/snapshotcloneserver/common/thread_pool.h"
#include "src/snapshotcloneserver/snapshot/snapshot_data_store.h"

using ::curve::common::NameLock;

namespace curve {
namespace snapshotcloneserver {

class SnapshotTaskInfo;

/**
 * @brief Snapshot index block mapping table for file
 */
struct FileSnapMap {
    std::vector<ChunkIndexData> maps;

    /**
     * @brief to obtain whether the current chunk data exists in the current
     * mapping table
     *
     * @param name chunk data object
     *
     * @retval true exists
     * @retval false does not exist
     */
    bool IsExistChunk(const ChunkDataName& name) const {
        bool find = false;
        for (auto& v : maps) {
            find = v.IsExistChunkDataName(name);
            if (find) {
                break;
            }
        }
        return find;
    }
};

/**
 * @brief snapshot core module
 */
class SnapshotCore {
 public:
    SnapshotCore() {}
    virtual ~SnapshotCore() {}

    /**
     * @brief Create snapshot pre operation
     *
     * @param file file name
     * @param user username
     * @param snapshotName SnapshotName
     * @param[out] snapInfo snapshot information
     *
     * @return error code
     */
    virtual int CreateSnapshotPre(const std::string& file,
                                  const std::string& user,
                                  const std::string& snapshotName,
                                  SnapshotInfo* snapInfo) = 0;

    /**
     * @brief Execute the task of creating a snapshot and update the progress
     * Step 1, build a snapshot file mapping and put MateObj
     * Step 2, read the chunk file from curvefs and put DataObj
     * Step 3, delete the temporary snapshot in curves
     * Step 4, update status
     *
     * @param task snapshot task information
     */
    virtual void HandleCreateSnapshotTask(
        std::shared_ptr<SnapshotTaskInfo> task) = 0;

    /**
     * @brief Delete snapshot pre operation
     * Update the snapshot records in the database to a deleting state
     *
     * @param uuid Snapshot uuid
     * @param user username
     * @param fileName File name
     * @param[out] snapInfo snapshot information
     *
     * @return error code
     */
    virtual int DeleteSnapshotPre(UUID uuid, const std::string& user,
                                  const std::string& fileName,
                                  SnapshotInfo* snapInfo) = 0;

    /**
     * @brief Execute the delete snapshot task and update the progress
     *
     * @param task snapshot task information
     */
    virtual void HandleDeleteSnapshotTask(
        std::shared_ptr<SnapshotTaskInfo> task) = 0;

    /**
     * @brief Get snapshot information of files
     *
     * @param file file name
     * @param info snapshot information list
     *
     * @return error code
     */
    virtual int GetFileSnapshotInfo(const std::string& file,
                                    std::vector<SnapshotInfo>* info) = 0;

    /**
     * @brief Get all snapshot information
     *
     * @param list snapshot information list
     *
     * @return error code
     */
    virtual int GetSnapshotList(std::vector<SnapshotInfo>* list) = 0;

    virtual int GetSnapshotInfo(const UUID uuid, SnapshotInfo* info) = 0;

    virtual int HandleCancelUnSchduledSnapshotTask(
        std::shared_ptr<SnapshotTaskInfo> task) = 0;

    /**
     * @brief Handle cancel snapshot task which is scheduled
     * @param[in] task pointer to snapshot task
     * @return kErrCodeCannotCancelFinished if task has finished,
     *         kErrCodeSuccess if cancel success,
     *         else return kErrCodeInternalError
     */
    virtual int HandleCancelScheduledSnapshotTask(
        std::shared_ptr<SnapshotTaskInfo> task) = 0;
};

class SnapshotCoreImpl : public SnapshotCore {
 public:
    /**
     * @brief constructor
     *
     * @param client curve client object
     * @param metaStore MetaStorage Object
     * @param dataStore data storage object
     */
    SnapshotCoreImpl(std::shared_ptr<CurveFsClient> client,
                     std::shared_ptr<SnapshotCloneMetaStore> metaStore,
                     std::shared_ptr<SnapshotDataStore> dataStore,
                     std::shared_ptr<SnapshotReference> snapshotRef,
                     const SnapshotCloneServerOptions& option)
        : client_(client),
          metaStore_(metaStore),
          dataStore_(dataStore),
          snapshotRef_(snapshotRef),
          chunkSplitSize_(option.chunkSplitSize),
          checkSnapshotStatusIntervalMs_(option.checkSnapshotStatusIntervalMs),
          maxSnapshotLimit_(option.maxSnapshotLimit),
          snapshotCoreThreadNum_(option.snapshotCoreThreadNum),
          mdsSessionTimeUs_(option.mdsSessionTimeUs),
          clientAsyncMethodRetryTimeSec_(option.clientAsyncMethodRetryTimeSec),
          clientAsyncMethodRetryIntervalMs_(
              option.clientAsyncMethodRetryIntervalMs),
          readChunkSnapshotConcurrency_(option.readChunkSnapshotConcurrency) {
        threadPool_ =
            std::make_shared<ThreadPool>(option.snapshotCoreThreadNum);
    }

    int Init();

    ~SnapshotCoreImpl() { threadPool_->Stop(); }

    // Public interface definition can be found in the SnapshotCore interface
    // annotation
    int CreateSnapshotPre(const std::string& file, const std::string& user,
                          const std::string& snapshotName,
                          SnapshotInfo* snapInfo) override;

    void HandleCreateSnapshotTask(
        std::shared_ptr<SnapshotTaskInfo> task) override;

    int DeleteSnapshotPre(UUID uuid, const std::string& user,
                          const std::string& fileName,
                          SnapshotInfo* snapInfo) override;

    void HandleDeleteSnapshotTask(
        std::shared_ptr<SnapshotTaskInfo> task) override;

    int GetFileSnapshotInfo(const std::string& file,
                            std::vector<SnapshotInfo>* info) override;

    int GetSnapshotInfo(const UUID uuid, SnapshotInfo* info) override;

    int GetSnapshotList(std::vector<SnapshotInfo>* list) override;

    int HandleCancelUnSchduledSnapshotTask(
        std::shared_ptr<SnapshotTaskInfo> task) override;

    int HandleCancelScheduledSnapshotTask(
        std::shared_ptr<SnapshotTaskInfo> task) override;

 private:
    /**
     * @brief Build snapshot file mapping
     *
     * @param fileName File name
     * @param seqNum snapshot version number
     * @param fileSnapshotMap snapshot file mapping table
     *
     * @return error code
     */
    int BuildSnapshotMap(const std::string& fileName, uint64_t seqNum,
                         FileSnapMap* fileSnapshotMap);

    /**
     * @brief Build Segment Information
     *
     * @param info snapshot information
     * @param segInfos Segment Information Table
     *
     * @return error code
     */
    int BuildSegmentInfo(const SnapshotInfo& info,
                         std::map<uint64_t, SegmentInfo>* segInfos);

    /**
     * @brief Create a snapshot on curves
     *
     * @param fileName File name
     * @param info snapshot information
     * @param task snapshot task information
     *
     * @return error code
     */
    int CreateSnapshotOnCurvefs(const std::string& fileName, SnapshotInfo* info,
                                std::shared_ptr<SnapshotTaskInfo> task);

    /**
     * @brief Delete snapshot on curves
     *
     * @param info snapshot information
     *
     * @return error code
     */
    int DeleteSnapshotOnCurvefs(const SnapshotInfo& info);

    /**
     * @brief Build Index Block
     *
     * @param info snapshot information
     * @param[out] indexData index block
     * @param[out] segInfos Segment Information
     * @param task snapshot task information
     *
     * @return error code
     */
    int BuildChunkIndexData(const SnapshotInfo& info, ChunkIndexData* indexData,
                            std::map<uint64_t, SegmentInfo>* segInfos,
                            std::shared_ptr<SnapshotTaskInfo> task);

    using ChunkDataExistFilter = std::function<bool(const ChunkDataName&)>;

    /**
     * @brief Dump snapshot process
     *
     * @param indexData index block
     * @param info snapshot information
     * @param segInfos Segment Information
     * @param filter Dump data block filter
     * @param task snapshot task information
     *
     * @return error code
     */
    int TransferSnapshotData(const ChunkIndexData indexData,
                             const SnapshotInfo& info,
                             const std::map<uint64_t, SegmentInfo>& segInfos,
                             const ChunkDataExistFilter& filter,
                             std::shared_ptr<SnapshotTaskInfo> task);

    /**
     * @brief Start cancel, update task status, update database status
     *
     * @param task snapshot task information
     *
     * @return error code
     */
    int StartCancel(std::shared_ptr<SnapshotTaskInfo> task);

    /**
     * @brief: Cancel the snapshot process after dumping data
     *
     * @param task snapshot task information
     * @param indexData index block
     * @param fileSnapshotMap snapshot file mapping table
     */
    void CancelAfterTransferSnapshotData(std::shared_ptr<SnapshotTaskInfo> task,
                                         const ChunkIndexData& indexData,
                                         const FileSnapMap& fileSnapshotMap);

    /**
     * @brief Cancel the snapshot process after creating the index block
     *
     * @param task snapshot task information
     */
    void CancelAfterCreateChunkIndexData(
        std::shared_ptr<SnapshotTaskInfo> task);

    /**
     * @brief: Cancel the snapshot process after creating a snapshot on curves
     *
     * @param task snapshot task information
     */
    void CancelAfterCreateSnapshotOnCurvefs(
        std::shared_ptr<SnapshotTaskInfo> task);

    /**
     * @brief in Mate data storage, delete snapshot
     *
     * @param task snapshot task information
     */
    void HandleClearSnapshotOnMateStore(std::shared_ptr<SnapshotTaskInfo> task);

    /**
     * @brief successfully processed the snapshot creation task
     *
     * @param task snapshot task information
     */
    void HandleCreateSnapshotSuccess(std::shared_ptr<SnapshotTaskInfo> task);

    /**
     * @brief processing failed snapshot creation task process
     *
     * @param task snapshot task information
     */
    void HandleCreateSnapshotError(std::shared_ptr<SnapshotTaskInfo> task);

    /**
     * @brief failed to process the delete snapshot task process
     *
     * @param task snapshot task information
     */
    void HandleDeleteSnapshotError(std::shared_ptr<SnapshotTaskInfo> task);

    /**
     * @brief Attempt to clean up failed snapshots before creating them,
     * otherwise they may fail again
     *
     * @param task snapshot task information
     * @return error code
     */
    int ClearErrorSnapBeforeCreateSnapshot(
        std::shared_ptr<SnapshotTaskInfo> task);

 private:
    // Curvefs client object
    std::shared_ptr<CurveFsClient> client_;
    // Meta Data Storage
    std::shared_ptr<SnapshotCloneMetaStore> metaStore_;
    // Data storage
    std::shared_ptr<SnapshotDataStore> dataStore_;
    // Snapshot Reference Count Management Module
    std::shared_ptr<SnapshotReference> snapshotRef_;

    // Thread pool for executing concurrent steps
    std::shared_ptr<ThreadPool> threadPool_;

    // Lock the file name of the snapshot to prevent concurrent snapshots.
    // Snapshots of the same file need to be queued
    NameLock snapshotNameLock_;

    // Dump chunk shard size
    uint64_t chunkSplitSize_;
    // CheckSnapShotStatus call interval
    uint32_t checkSnapshotStatusIntervalMs_;
    // Maximum Snapshots
    uint32_t maxSnapshotLimit_;
    // Number of threads
    uint32_t snapshotCoreThreadNum_;
    // Session timeout
    uint32_t mdsSessionTimeUs_;
    // Total retry time for client asynchronous callback requests
    uint64_t clientAsyncMethodRetryTimeSec_;
    // Call client asynchronous method retry interval
    uint64_t clientAsyncMethodRetryIntervalMs_;
    // The concurrency of asynchronous ReadChunkSnapshots
    uint32_t readChunkSnapshotConcurrency_;
};

}  // namespace snapshotcloneserver
}  // namespace curve

#endif  // SRC_SNAPSHOTCLONESERVER_SNAPSHOT_SNAPSHOT_CORE_H_
