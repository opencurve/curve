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
 * Created Date: Wed Mar 20 2019
 * Author: xuchaojie
 */

#ifndef SRC_SNAPSHOTCLONESERVER_CLONE_CLONE_CORE_H_
#define SRC_SNAPSHOTCLONESERVER_CLONE_CLONE_CORE_H_

#include <memory>
#include <string>
#include <vector>
#include <map>
#include <list>

#include "src/snapshotcloneserver/common/curvefs_client.h"
#include "src/common/snapshotclone/snapshotclone_define.h"
#include "src/snapshotcloneserver/common/snapshotclone_meta_store.h"
#include "src/snapshotcloneserver/snapshot/snapshot_data_store.h"
#include "src/snapshotcloneserver/common/snapshot_reference.h"
#include "src/snapshotcloneserver/clone/clone_reference.h"
#include "src/snapshotcloneserver/common/thread_pool.h"
#include "src/common/concurrent/name_lock.h"

using ::curve::common::NameLock;

namespace curve {
namespace snapshotcloneserver {

class CloneTaskInfo;

class CloneCore {
 public:
    CloneCore() {}
    virtual ~CloneCore() {}

    /**
     * @brief Clone or restore task ahead
     *
     * @param source Clone or restore source
     * @param user username
     * @param destination The target file name for cloning or restoring
     * @param lazyFlag is lazy
     * @param taskType clone or restore
     * @param poolset The poolset of the target file during cloning
     * @param[out] info Clone or restore task information
     *
     * @return error code
     */
    virtual int CloneOrRecoverPre(const UUID &source,
                         const std::string &user,
                         const std::string &destination,
                         bool lazyFlag,
                         CloneTaskType taskType,
                         std::string poolset,
                         CloneInfo *info) = 0;

    /**
     * @brief Processing cloning or recovery tasks
     *
     * @param task Clone or restore task
     */
    virtual void HandleCloneOrRecoverTask(
        std::shared_ptr<CloneTaskInfo> task) = 0;

    /**
     * @brief Clean clone or restore tasks ahead
     *
     * @param user username
     * @param taskId Task Id
     * @param[out] cloneInfo Clone or restore information
     *
     * @return error code
     */
    virtual int CleanCloneOrRecoverTaskPre(const std::string &user,
        const TaskIdType &taskId,
        CloneInfo *cloneInfo) = 0;


    /**
     * @brief Asynchronous processing of clean clone or restore tasks
     *
     * @param task Clone or restore task
     */
    virtual void HandleCleanCloneOrRecoverTask(
        std::shared_ptr<CloneTaskInfo> task) = 0;

    /**
     *Pre work for installing clone file data in  @brief
     *- Conduct necessary inspections
     *- Obtain and return clone information
     *- Update database status
     *
     * @param user username
     * @param taskId Task Id
     * @param[out] cloneInfo clone information
     *
     * @return error code
     */
    virtual int FlattenPre(
        const std::string &user,
        const TaskIdType &taskId,
        CloneInfo *cloneInfo) = 0;

    /**
     * @brief Get a list of all clone/restore tasks for resuming execution after reboot
     *
     * @param[out] cloneInfos Clone/Restore Task List
     *
     * @return error code
     */
    virtual int GetCloneInfoList(std::vector<CloneInfo> *cloneInfos) = 0;

    /**
     * @brief Get the clone/restore task for the specified ID
     *
     * @param taskId Task ID
     * @param cloneInfo Clone/Restore Task
     *
     * @retVal 0 successfully obtained
     * @retVal -1 failed to obtain
     */
    virtual int GetCloneInfo(TaskIdType taskId, CloneInfo *cloneInfo) = 0;

    /**
     * @brief Get the clone/restore task for the specified file name
     *
     * @param fileName File name
     * @param list Clone/Restore Task List
     *
     * @retVal 0 successfully obtained
     * @retVal -1 failed to obtain
     */
    virtual int GetCloneInfoByFileName(
    const std::string &fileName, std::vector<CloneInfo> *list) = 0;

    /**
     * @brief Get snapshot reference management module
     *
     * @return Snapshot Reference Management Module
     */
    virtual std::shared_ptr<SnapshotReference> GetSnapshotRef() = 0;


    /**
     * @brief Get Mirror Reference Management Module
     *
     * @return Image Reference Management Module
     */
    virtual std::shared_ptr<CloneReference> GetCloneRef() = 0;


    /**
     * @brief Remove clone/restore task
     *
     * @param task Clone task
     *
     * @return error code
     */
    virtual int HandleRemoveCloneOrRecoverTask(
        std::shared_ptr<CloneTaskInfo> task) = 0;

    /**
     * @brief Check if the file exists
     *
     * @param filename File name
     *
     * @return error code
     */
    virtual int CheckFileExists(const std::string &filename,
                                uint64_t inodeId) = 0;

    /**
     * @brief Delete cloneInfo
     *
     * @param cloneInfo CloneInfo to be deleted
     *
     * @return error code
     */
    virtual int HandleDeleteCloneInfo(const CloneInfo &cloneInfo) = 0;
};

/**
 * @brief Chunk information required for cloning/restoring
 */
struct CloneChunkInfo {
    //The ID information of the chunk
    ChunkIDInfo chunkIdInfo;
    //Location information, if on s3, it is objectName, otherwise on curves, it is offset
    std::string location;
    //The version number of the chunk
    uint64_t seqNum;
    //Does Chunk require recover
    bool needRecover;
};

//The segment information required for cloning/recovery, where key is ChunkIndex In Segment and value is chunk information
using CloneSegmentInfo = std::map<uint64_t, CloneChunkInfo>;
//The segment information table required for cloning/recovery, where the key is segmentIndex
using CloneSegmentMap = std::map<uint64_t, CloneSegmentInfo>;

class CloneCoreImpl : public CloneCore {
 public:
     static const std::string kCloneTempDir;

 public:
    CloneCoreImpl(
        std::shared_ptr<CurveFsClient> client,
        std::shared_ptr<SnapshotCloneMetaStore> metaStore,
        std::shared_ptr<SnapshotDataStore> dataStore,
        std::shared_ptr<SnapshotReference> snapshotRef,
        std::shared_ptr<CloneReference> cloneRef,
        const SnapshotCloneServerOptions option)
      : client_(client),
        metaStore_(metaStore),
        dataStore_(dataStore),
        snapshotRef_(snapshotRef),
        cloneRef_(cloneRef),
        cloneChunkSplitSize_(option.cloneChunkSplitSize),
        cloneTempDir_(option.cloneTempDir),
        mdsRootUser_(option.mdsRootUser),
        createCloneChunkConcurrency_(option.createCloneChunkConcurrency),
        recoverChunkConcurrency_(option.recoverChunkConcurrency),
        clientAsyncMethodRetryTimeSec_(option.clientAsyncMethodRetryTimeSec),
        clientAsyncMethodRetryIntervalMs_(
            option.clientAsyncMethodRetryIntervalMs) {}

    ~CloneCoreImpl() {
    }

    int Init();

    int CloneOrRecoverPre(const UUID &source,
         const std::string &user,
         const std::string &destination,
         bool lazyFlag,
         CloneTaskType taskType,
         std::string poolset,
         CloneInfo *info) override;

    void HandleCloneOrRecoverTask(std::shared_ptr<CloneTaskInfo> task) override;

    int CleanCloneOrRecoverTaskPre(const std::string &user,
        const TaskIdType &taskId,
        CloneInfo *cloneInfo) override;

    void HandleCleanCloneOrRecoverTask(
        std::shared_ptr<CloneTaskInfo> task) override;

    int FlattenPre(
        const std::string &user,
        const std::string &fileName,
        CloneInfo *cloneInfo) override;

    int GetCloneInfoList(std::vector<CloneInfo> *taskList) override;
    int GetCloneInfo(TaskIdType taskId, CloneInfo *cloneInfo) override;

    int GetCloneInfoByFileName(
        const std::string &fileName, std::vector<CloneInfo> *list) override;

    std::shared_ptr<SnapshotReference> GetSnapshotRef() {
        return snapshotRef_;
    }

    std::shared_ptr<CloneReference> GetCloneRef() {
        return cloneRef_;
    }

    int HandleRemoveCloneOrRecoverTask(
        std::shared_ptr<CloneTaskInfo> task) override;

    int CheckFileExists(const std::string &filename,
                        uint64_t inodeId) override;
    int HandleDeleteCloneInfo(const CloneInfo &cloneInfo) override;

 private:
    /**
     * @brief Build clone/restore file information from snapshot
     *
     * @param task task information
     * @param[out] newFileInfo Newly constructed file information
     * @param[out] segInfos The segment information of the newly constructed file
     *
     * @return error code
     */
    int BuildFileInfoFromSnapshot(
        std::shared_ptr<CloneTaskInfo> task,
        FInfo *newFileInfo,
        CloneSegmentMap *segInfos);

    /**
     * @brief Build clone/restore file information from source files
     *
     * @param task task information
     * @param[out] newFileInfo Newly constructed file information
     * @param[out] segInfos The segment information of the newly constructed file
     *
     * @return error code
     */
    int BuildFileInfoFromFile(
        std::shared_ptr<CloneTaskInfo> task,
        FInfo *newFileInfo,
        CloneSegmentMap *segInfos);


    /**
     * @brief to determine if it is necessary to update chunkIdInfo in CloneChunkInfo information
     *
     * @param task task information
     *
     * @retVal true needs to be updated
     * @retVal false No update required
     */
    bool NeedUpdateCloneMeta(
        std::shared_ptr<CloneTaskInfo> task);

    /**
     * @brief: Determine whether to retry after clone failure
     *
     * @param task task information
     * @param retCode error code
     *
     * @retVal true requires
     * @retVal false No need
     */
    bool NeedRetry(std::shared_ptr<CloneTaskInfo> task,
        int retCode);

    /**
     * @brief Create metadata information for clone or update metadata information
     *
     * @param task task information
     * @param[int][out] fInfo Newly created file information
     * @param[int][out] segInfosThe segment information of the file
     *
     * @return error code
     */
    int CreateOrUpdateCloneMeta(
        std::shared_ptr<CloneTaskInfo> task,
        FInfo *fInfo,
        CloneSegmentMap *segInfos);

    /**
     * @brief Create a new clone file
     *
     * @param task task information
     * @param fInfo File information to be created
     *
     * @return error code
     */
    int CreateCloneFile(
        std::shared_ptr<CloneTaskInfo> task,
        const FInfo &fInfo);

    /**
     * @brief Create source information for new files (create segments)
     *
     * @param task task information
     * @param fInfo File information for new files
     * @param segInfos The segment information required for a new file
     *
     * @return error code
     */
    int CreateCloneMeta(
        std::shared_ptr<CloneTaskInfo> task,
        FInfo *fInfo,
        CloneSegmentMap *segInfos);

    /**
     * @brief Create a chunk for a new clone file
     *
     * @param task task information
     * @param fInfo File information for new files
     * @param segInfos The segment information required for a new file
     *
     * @return error code
     */
    int CreateCloneChunk(
        std::shared_ptr<CloneTaskInfo> task,
        const FInfo &fInfo,
        CloneSegmentMap *segInfos);

    /**
     * @brief Start asynchronous request for CreateCloneChunk
     *
     * @param task task information
     * @param tracker CreateCloneChunk Task Tracker
     * @param context Create CloneChunk context
     *
     * @return error code
     */
    int StartAsyncCreateCloneChunk(
        std::shared_ptr<CloneTaskInfo> task,
        std::shared_ptr<CreateCloneChunkTaskTracker> tracker,
        std::shared_ptr<CreateCloneChunkContext> context);

    /**
     * @brief Process the results of CreateCloneChunk and try again
     *
     * @param task task information
     * @param tracker CreateCloneChunk Task Tracker
     * @param results CreateCloneChunk result list
     *
     * @return error code
     */
    int HandleCreateCloneChunkResultsAndRetry(
        std::shared_ptr<CloneTaskInfo> task,
        std::shared_ptr<CreateCloneChunkTaskTracker> tracker,
        const std::list<CreateCloneChunkContextPtr> &results);

    /**
     *Notify mds to complete the step of creating source data with  @brief
     *
     * @param task task information
     * @param fInfo File information for new files
     * @param segInfos The segment information required for a new file
     *
     * @return error code
     */
    int CompleteCloneMeta(
        std::shared_ptr<CloneTaskInfo> task,
        const FInfo &fInfo,
        const CloneSegmentMap &segInfos);

    /**
     * @brief Restore Chunk, that is, notify Chunkserver to copy data
     *
     * @param task task information
     * @param fInfo File information for new files
     * @param segInfos The segment information required for a new file
     *
     * @return error code
     */
    int RecoverChunk(
        std::shared_ptr<CloneTaskInfo> task,
        const FInfo &fInfo,
        const CloneSegmentMap &segInfos);

    /**
     * @brief Start asynchronous request for RecoverChunk
     *
     * @param task task information
     * @param tracker RecoverChunk Asynchronous task tracker
     * @param context RecoverChunk Context
     *
     * @return error code
     */
    int StartAsyncRecoverChunkPart(
        std::shared_ptr<CloneTaskInfo> task,
        std::shared_ptr<RecoverChunkTaskTracker> tracker,
        std::shared_ptr<RecoverChunkContext> context);

    /**
     * @brief Continue requests for other parts of the RecoverChunk and wait for certain RecoverChunks to be completed
     *
     * @param task task information
     * @param tracker RecoverChunk Asynchronous task tracker
     * @param[out] completeChunkNum Number of chunks completed
     *
     * @return error code
     */
    int ContinueAsyncRecoverChunkPartAndWaitSomeChunkEnd(
        std::shared_ptr<CloneTaskInfo> task,
        std::shared_ptr<RecoverChunkTaskTracker> tracker,
        uint64_t *completeChunkNum);

    /**
     * @brief Modify the owner of the cloned file
     *
     * @param task task information
     * @param fInfo File information for new files
     *
     * @return error code
     */
    int ChangeOwner(
        std::shared_ptr<CloneTaskInfo> task,
        const FInfo &fInfo);

    /**
     * @brief Rename clone file
     *
     * @param task task information
     * @param fInfo File information for new files
     *
     * @return error code
     */
    int RenameCloneFile(
        std::shared_ptr<CloneTaskInfo> task,
        const FInfo &fInfo);

    /**
     * @brief Notify mds to complete data creation
     *
     * @param task task information
     * @param fInfo File information for new files
     * @param segInfos The segment information required for a new file
     *
     * @return error code
     */
    int CompleteCloneFile(
        std::shared_ptr<CloneTaskInfo> task,
        const FInfo &fInfo,
        const CloneSegmentMap &segInfos);

    /**
     * @brief: When cloning from a snapshot, update the snapshot status and notify the clone to complete
     *
     * @param task task information
     *
     * @return error code
     */
    int UpdateSnapshotStatus(
        std::shared_ptr<CloneTaskInfo> task);

    /**
     * @brief Processing Lazy Clone/Restore Phase 1 End
     *
     * @param task task information
     */
    void HandleLazyCloneStage1Finish(
        std::shared_ptr<CloneTaskInfo> task);

    /**
     * @brief Successfully processed clone/restore
     *
     * @param task task information
     */
    void HandleCloneSuccess(std::shared_ptr<CloneTaskInfo> task);


    /**
     * @brief processing clone or restore failed
     *
     * @param task task information
     * @param retCode pending error code
     */
    void HandleCloneError(std::shared_ptr<CloneTaskInfo> task,
        int retCode);

    /**
     * @brief Lazy Clone failed to process Clone task and retry
     *
     * @param task task information
     */
    void HandleCloneToRetry(std::shared_ptr<CloneTaskInfo> task);

    /**
     * @brief Successfully processed cleanup clone or restore task
     *
     * @param task task information
     */
    void HandleCleanSuccess(std::shared_ptr<CloneTaskInfo> task);

    /**
     * @brief processing cleanup clone or recovery task failed
     *
     * @param task task information
     */
    void HandleCleanError(std::shared_ptr<CloneTaskInfo> task);

    bool IsLazy(std::shared_ptr<CloneTaskInfo> task);
    bool IsSnapshot(std::shared_ptr<CloneTaskInfo> task);
    bool IsFile(std::shared_ptr<CloneTaskInfo> task);
    bool IsRecover(std::shared_ptr<CloneTaskInfo> task);
    bool IsClone(std::shared_ptr<CloneTaskInfo> task);

 private:
    std::shared_ptr<CurveFsClient> client_;
    std::shared_ptr<SnapshotCloneMetaStore> metaStore_;
    std::shared_ptr<SnapshotDataStore> dataStore_;
    std::shared_ptr<SnapshotReference> snapshotRef_;
    std::shared_ptr<CloneReference> cloneRef_;

    //Clone chunk shard size
    uint64_t cloneChunkSplitSize_;
    //Clone temporary directory
    std::string cloneTempDir_;
    // mds root user
    std::string mdsRootUser_;
    //Number of asynchronous requests made simultaneously by CreateCloneChunk
    uint32_t createCloneChunkConcurrency_;
    //Number of asynchronous requests simultaneously made by RecoverChunk
    uint32_t recoverChunkConcurrency_;
    //Client asynchronous request retry time
    uint64_t clientAsyncMethodRetryTimeSec_;
    //Call client asynchronous method retry interval
    uint64_t clientAsyncMethodRetryIntervalMs_;
};

}  // namespace snapshotcloneserver
}  // namespace curve

#endif  // SRC_SNAPSHOTCLONESERVER_CLONE_CLONE_CORE_H_
