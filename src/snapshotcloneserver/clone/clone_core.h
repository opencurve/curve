/*
 * Project: curve
 * Created Date: Wed Mar 20 2019
 * Author: xuchaojie
 * Copyright (c) 2018 netease
 */

#ifndef SRC_SNAPSHOTCLONESERVER_CLONE_CLONE_CORE_H_
#define SRC_SNAPSHOTCLONESERVER_CLONE_CLONE_CORE_H_

#include <memory>
#include <string>
#include <vector>
#include <map>

#include "src/snapshotcloneserver/common/curvefs_client.h"
#include "src/snapshotcloneserver/common/define.h"
#include "src/snapshotcloneserver/common/snapshotclone_meta_store.h"
#include "src/snapshotcloneserver/snapshot/snapshot_data_store.h"
#include "src/snapshotcloneserver/common/snapshot_reference.h"
#include "src/snapshotcloneserver/clone/clone_reference.h"
#include "src/snapshotcloneserver/common/thread_pool.h"

namespace curve {
namespace snapshotcloneserver {

class CloneTaskInfo;

class CloneCore {
 public:
    CloneCore() {}
    virtual ~CloneCore() {}

    /**
     * @brief 克隆或恢复任务前置
     *
     * @param source 克隆或恢复源
     * @param user 用户名
     * @param destination 克隆或恢复的目标文件名
     * @param lazyFlag 是否lazy
     * @param taskType 克隆或恢复
     * @param[out] info 克隆或恢复任务信息
     *
     * @return 错误码
     */
    virtual int CloneOrRecoverPre(const UUID &source,
                         const std::string &user,
                         const std::string &destination,
                         bool lazyFlag,
                         CloneTaskType taskType,
                         CloneInfo *info) = 0;

    /**
     * @brief 处理克隆或恢复任务
     *
     * @param task 克隆或恢复任务
     */
    virtual void HandleCloneOrRecoverTask(
        std::shared_ptr<CloneTaskInfo> task) = 0;

    /**
     * @brief 清理克隆或恢复任务前置
     *
     * @param user 用户名
     * @param taskId 任务Id
     * @param[out] cloneInfo 克隆或恢复信息
     *
     * @return 错误码
     */
    virtual int CleanCloneOrRecoverTaskPre(const std::string &user,
        const TaskIdType &taskId,
        CloneInfo *cloneInfo) = 0;


    /**
     * @brief 异步处理清理克隆或恢复任务
     *
     * @param task 克隆或恢复任务
     */
    virtual void HandleCleanCloneOrRecoverTask(
        std::shared_ptr<CloneTaskInfo> task) = 0;

    /**
     * @brief 获取全部克隆/恢复任务列表，用于重启后恢复执行
     *
     * @param[out] cloneInfos 克隆/恢复任务列表
     *
     * @return 错误码
     */
    virtual int GetCloneInfoList(std::vector<CloneInfo> *cloneInfos) = 0;

    /**
     * @brief 获取指定id的克隆/恢复任务
     *
     * @param taskId  任务id
     * @param cloneInfo 克隆/恢复任务
     *
     * @retVal 0  获取成功
     * @retVal -1 获取失败
     */
    virtual int GetCloneInfo(TaskIdType taskId, CloneInfo *cloneInfo) = 0;


    /**
     * @brief 获取快照引用管理模块
     *
     * @return 快照引用管理模块
     */
    virtual std::shared_ptr<SnapshotReference> GetSnapshotRef() = 0;


    /**
     * @brief 获取镜像引用管理模块
     *
     * @return 镜像引用管理模块
     */
    virtual std::shared_ptr<CloneReference> GetCloneRef() = 0;
};

/**
 * @brief  克隆/恢复所需chunk信息
 */
struct CloneChunkInfo {
    // 该chunk的id信息
    ChunkIDInfo chunkIdInfo;
    // 位置信息，如果在s3上，是objectName，否则在curvefs上，则是offset
    std::string location;
    // 该chunk的版本号
    uint64_t seqNum;
};

// 克隆/恢复所需segment信息，key是ChunkIndex In Segment, value是chunk信息
using CloneSegmentInfo = std::map<uint64_t, CloneChunkInfo>;
// 克隆/恢复所需segment信息表，key是segmentIndex
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
        cloneCoreThreadNum_(option.cloneCoreThreadNum) {
          threadPool_ = std::make_shared<ThreadPool>(option.cloneCoreThreadNum);
          recoverChunkPool_ =
              std::make_shared<ThreadPool>(option.cloneCoreThreadNum);
    }

    ~CloneCoreImpl() {
        threadPool_->Stop();
        recoverChunkPool_->Stop();
    }

    int Init();

    int CloneOrRecoverPre(const UUID &source,
         const std::string &user,
         const std::string &destination,
         bool lazyFlag,
         CloneTaskType taskType,
         CloneInfo *info) override;

    void HandleCloneOrRecoverTask(std::shared_ptr<CloneTaskInfo> task) override;

    int CleanCloneOrRecoverTaskPre(const std::string &user,
        const TaskIdType &taskId,
        CloneInfo *cloneInfo) override;

    void HandleCleanCloneOrRecoverTask(
        std::shared_ptr<CloneTaskInfo> task) override;

    int GetCloneInfoList(std::vector<CloneInfo> *taskList) override;
    int GetCloneInfo(TaskIdType taskId, CloneInfo *cloneInfo) override;


    std::shared_ptr<SnapshotReference> GetSnapshotRef() {
        return snapshotRef_;
    }

    std::shared_ptr<CloneReference> GetCloneRef() {
        return cloneRef_;
    }

 private:
    /**
     * @brief 从快照构建克隆/恢复的文件信息
     *
     * @param task 任务信息
     * @param[out] newFileInfo 新构建的文件信息
     * @param[out] segInfos 新构建文件的segment信息
     *
     * @return 错误码
     */
    int BuildFileInfoFromSnapshot(
        std::shared_ptr<CloneTaskInfo> task,
        FInfo *newFileInfo,
        CloneSegmentMap *segInfos);

    /**
     * @brief 从源文件构建克隆/恢复的文件信息
     *
     * @param task 任务信息
     * @param[out] newFileInfo 新构建的文件信息
     * @param[out] segInfos 新构建文件的segment信息
     *
     * @return 错误码
     */
    int BuildFileInfoFromFile(
        std::shared_ptr<CloneTaskInfo> task,
        FInfo *newFileInfo,
        CloneSegmentMap *segInfos);


    /**
     * @brief 判断是否需要更新CloneChunkInfo信息中的chunkIdInfo
     *
     * @param task 任务信息
     *
     * @retVal true 需要更新
     * @retVal false 不需要更新
     */
    bool NeedUpdateCloneMeta(
        std::shared_ptr<CloneTaskInfo> task);

    /**
     * @brief 创建clone的元数据信息或更新元数据信息
     *
     * @param task 任务信息
     * @param[int][out] fInfo 新创建的文件信息
     * @param[int][out] segInfos 文件的segment信息
     *
     * @return 错误码
     */
    int CreateOrUpdateCloneMeta(
        std::shared_ptr<CloneTaskInfo> task,
        FInfo *fInfo,
        CloneSegmentMap *segInfos);

    /**
     * @brief 创建新clone文件
     *
     * @param task 任务信息
     * @param fInfo 需创建的文件信息
     *
     * @return 错误码
     */
    int CreateCloneFile(
        std::shared_ptr<CloneTaskInfo> task,
        const FInfo &fInfo);

    /**
     * @brief 创建新文件的源信息（创建segment）
     *
     * @param task 任务信息
     * @param fInfo 新文件的文件信息
     * @param segInfos 新文件所需的segment信息
     *
     * @return 错误码
     */
    int CreateCloneMeta(
        std::shared_ptr<CloneTaskInfo> task,
        FInfo *fInfo,
        CloneSegmentMap *segInfos);

    /**
     * @brief 创建新clone文件的chunk
     *
     * @param task 任务信息
     * @param fInfo 新文件的文件信息
     * @param segInfos 新文件所需的segment信息
     *
     * @return 错误码
     */
    int CreateCloneChunk(
        std::shared_ptr<CloneTaskInfo> task,
        const FInfo &fInfo,
        const CloneSegmentMap &segInfos);

    /**
     * @brief 通知mds完成源数据创建步骤
     *
     * @param task 任务信息
     * @param fInfo 新文件的文件信息
     * @param segInfos 新文件所需的segment信息
     *
     * @return 错误码
     */
    int CompleteCloneMeta(
        std::shared_ptr<CloneTaskInfo> task,
        const FInfo &fInfo,
        const CloneSegmentMap &segInfos);

    /**
     * @brief 恢复chunk，即通知chunkserver拷贝数据
     *
     * @param task 任务信息
     * @param fInfo 新文件的文件信息
     * @param segInfos 新文件所需的segment信息
     *
     * @return 错误码
     */
    int RecoverChunk(
        std::shared_ptr<CloneTaskInfo> task,
        const FInfo &fInfo,
        const CloneSegmentMap &segInfos);

    /**
     * @brief 修改克隆文件的owner
     *
     * @param task 任务信息
     * @param fInfo 新文件的文件信息
     *
     * @return 错误码
     */
    int ChangeOwner(
        std::shared_ptr<CloneTaskInfo> task,
        const FInfo &fInfo);

    /**
     * @brief 重命名克隆文件
     *
     * @param task 任务信息
     * @param fInfo 新文件的文件信息
     *
     * @return 错误码
     */
    int RenameCloneFile(
        std::shared_ptr<CloneTaskInfo> task,
        const FInfo &fInfo);

    /**
     * @brief 通知mds完成数据创建
     *
     * @param task 任务信息
     * @param fInfo 新文件的文件信息
     * @param segInfos 新文件所需的segment信息
     *
     * @return 错误码
     */
    int CompleteCloneFile(
        std::shared_ptr<CloneTaskInfo> task,
        const FInfo &fInfo,
        const CloneSegmentMap &segInfos);

    /**
     * @brief 从快照克隆时，更新快照状态，通知克隆完成
     *
     * @param task 任务信息
     *
     * @return 错误码
     */
    int UpdateSnapshotStatus(
        std::shared_ptr<CloneTaskInfo> task);

    /**
     * @brief 处理克隆/恢复成功
     *
     * @param task 任务信息
     */
    void HandleCloneSuccess(std::shared_ptr<CloneTaskInfo> task);


    /**
     * @brief 处理克隆或恢复失败
     *
     * @param task 任务信息
     * @param retCode 待处理的错误码
     */
    void HandleCloneError(std::shared_ptr<CloneTaskInfo> task,
        int retCode);

    /**
     * @brief Lazy Clone 情况下处理Clone任务失败重试
     *
     * @param task 任务信息
     */
    void HandleCloneToRetry(std::shared_ptr<CloneTaskInfo> task);

    /**
     * @brief 处理清理克隆或恢复任务成功
     *
     * @param task 任务信息
     */
    void HandleCleanSuccess(std::shared_ptr<CloneTaskInfo> task);

    /**
     * @brief  处理清理克隆或恢复任务失败
     *
     * @param task 任务信息
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

    // 执行并发步骤的线程池
    std::shared_ptr<ThreadPool> threadPool_;
    // 执行RecoverChunk并发的线程池
    std::shared_ptr<ThreadPool> recoverChunkPool_;

    // clone chunk分片大小
    uint64_t cloneChunkSplitSize_;
    // 克隆临时目录
    std::string cloneTempDir_;
    // mds root user
    std::string mdsRootUser_;
    // 线程数
    uint32_t cloneCoreThreadNum_;
};

}  // namespace snapshotcloneserver
}  // namespace curve

#endif  // SRC_SNAPSHOTCLONESERVER_CLONE_CLONE_CORE_H_
