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

#include <memory>
#include <string>
#include <vector>
#include <list>
#include <map>

#include "src/snapshotcloneserver/common/curvefs_client.h"
#include "src/snapshotcloneserver/common/snapshotclone_meta_store.h"
#include "src/snapshotcloneserver/snapshot/snapshot_data_store.h"
#include "src/common/snapshotclone/snapshotclone_define.h"
#include "src/snapshotcloneserver/common/config.h"
#include "src/snapshotcloneserver/common/snapshot_reference.h"
#include "src/common/concurrent/name_lock.h"
#include "src/snapshotcloneserver/common/thread_pool.h"
#include "src/common/interruptible_sleeper.h"

using ::curve::common::NameLock;
using ::curve::common::InterruptibleSleeper;

namespace curve {
namespace snapshotcloneserver {

class SnapshotTaskInfo;

/**
 * @brief 文件的快照索引块映射表
 */
struct FileSnapMap {
    std::vector<ChunkIndexData> maps;

    /**
     * @brief 获取当前映射表中是否存在当前chunk数据
     *
     * @param name chunk数据对象
     *
     * @retval true 存在
     * @retval false 不存在
     */
    bool IsExistChunk(const ChunkDataName &name) const {
        bool find = false;
        for (auto &v : maps) {
            find = v.IsExistChunkDataName(name);
            if (find) {
                break;
            }
        }
        return find;
    }
};

/**
 * @brief 快照核心模块
 */
class SnapshotCore {
 public:
    SnapshotCore() {}
    virtual ~SnapshotCore() {}

    /**
     * @brief 创建快照前置操作
     *
     * @param file 文件名
     * @param user 用户名
     * @param snapshotName 快照名
     * @param[out] snapInfo 快照信息
     *
     * @return 错误码
     */
    virtual int CreateSnapshotPre(const std::string &file,
        const std::string &user,
        const std::string &snapshotName,
        SnapshotInfo *snapInfo) = 0;

    /**
     * @brief 创建同步快照前置操作(多层秒级快照的创建快照方式采用同步，而非异步任务方式)
     *
     * @param file 文件名
     * @param user 用户名
     * @param snapshotName 快照名
     * @param[out] snapInfo 快照信息
     *
     * @return 错误码
     */
    virtual int CreateLocalSnapshot(const std::string &file,
        const std::string &user,
        const std::string &snapshotName,
        SnapshotInfo *snapInfo) = 0;

    /**
     * @brief 执行创建快照任务并更新progress
     * 第一步，构建快照文件映射, put MateObj
     * 第二步，从curvefs读取chunk文件，并put DataObj
     * 第三步，删除curvefs中的临时快照
     * 第四步，update status
     *
     * @param task 快照任务信息
     */
    virtual void HandleCreateSnapshotTask(
        std::shared_ptr<SnapshotTaskInfo> task) = 0;

    /**
     * @brief 删除快照前置操作
     * 更新数据库中的快照记录为deleting状态
     *
     * @param uuid 快照uuid
     * @param user 用户名
     * @param fileName 文件名
     * @param[out] snapInfo 快照信息
     *
     * @return 错误码
     */
    virtual int DeleteSnapshotPre(
        UUID uuid,
        const std::string &user,
        const std::string &fileName,
        SnapshotInfo *snapInfo) = 0;

    /**
     * @brief 删除同步快照前置操作
     * 更新数据库中的快照记录为deleting状态
     *
     * @param uuid 快照uuid
     * @param user 用户名
     * @param fileName 文件名
     * @param[out] snapInfo 快照信息
     *
     * @return 错误码
     */
    virtual int DeleteLocalSnapshot(
        UUID uuid,
        const std::string &user,
        const std::string &fileName) = 0;

    /**
     * @brief 执行删除快照任务并更新progress
     *
     * @param task 快照任务信息
     */
    virtual void HandleDeleteSnapshotTask(
        std::shared_ptr<SnapshotTaskInfo> task) = 0;

    virtual int GetFileInfo(const std::string &file,
        const std::string &user,
        FInfo *fInfo) = 0;

    /**
     * @brief 获取文件的快照信息
     *
     * @param file 文件名
     * @param info 快照信息列表
     *
     * @return 错误码
     */
    virtual int GetFileSnapshotInfo(const std::string &file,
        std::vector<SnapshotInfo> *info) = 0;

    /**
     * @brief get localsnapshot status
     *
     * @param file  volume name
     * @param user  owner of the volume
     * @param seq  seq of the snapshot
     * @param status  status of the snapshot
     * @param progress  progress of the snapshot
     *
     * @return  error code
     */
    virtual int GetLocalSnapshotStatus(const std::string &file,
        const std::string &user,
        uint64_t seq,
        Status *status,
        uint32_t *progress) = 0;

    /**
     * @brief 获取全部快照信息
     *
     * @param list 快照信息列表
     *
     * @return 错误码
     */
    virtual int GetSnapshotList(std::vector<SnapshotInfo> *list) = 0;


    virtual int GetSnapshotInfo(const UUID uuid,
        SnapshotInfo *info) = 0;

    virtual int GetSnapshotInfo(const std::string &file,
        const std::string &snapshotName,
        SnapshotInfo *info) = 0;

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
      * @brief 构造函数
      *
      * @param client curve客户端对象
      * @param metaStore  meta存储对象
      * @param dataStore  data存储对象
      */
    SnapshotCoreImpl(
        std::shared_ptr<CurveFsClient> client,
        std::shared_ptr<SnapshotCloneMetaStore> metaStore,
        std::shared_ptr<SnapshotDataStore> dataStore,
        std::shared_ptr<SnapshotReference> snapshotRef,
        const SnapshotCloneServerOptions &option)
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
        threadPool_ = std::make_shared<ThreadPool>(
            option.snapshotCoreThreadNum);
      checkPeriod_ = option.localSnapshotBackendCheckIntervalMs;
    }

    int Init();

    ~SnapshotCoreImpl() {
        threadPool_->Stop();
        checkThread_->join();
        delete checkThread_;
    }

    // 公有接口定义见SnapshotCore接口注释
    int CreateSnapshotPre(const std::string &file,
        const std::string &user,
        const std::string &snapshotName,
        SnapshotInfo *snapInfo) override;

    int CreateLocalSnapshot(const std::string &file,
        const std::string &user,
        const std::string &snapshotName,
        SnapshotInfo *snapInfo) override;

    void HandleCreateSnapshotTask(
        std::shared_ptr<SnapshotTaskInfo> task) override;

    int DeleteSnapshotPre(UUID uuid,
        const std::string &user,
        const std::string &fileName,
        SnapshotInfo *snapInfo) override;

    int DeleteLocalSnapshot(UUID uuid,
        const std::string &user,
        const std::string &fileName) override;

    void HandleDeleteSnapshotTask(
        std::shared_ptr<SnapshotTaskInfo> task) override;

    int GetFileInfo(const std::string &file,
        const std::string &user,
        FInfo *fInfo) override;

    int GetFileSnapshotInfo(const std::string &file,
        std::vector<SnapshotInfo> *info) override;

    int GetLocalSnapshotStatus(const std::string &file,
        const std::string &user,
        uint64_t seq,
        Status *status,
        uint32_t *progress) override;

    int GetSnapshotInfo(const UUID uuid,
        SnapshotInfo *info) override;

    int GetSnapshotInfo(const std::string &file,
        const std::string &snapshotName,
        SnapshotInfo *info) override;

    int GetSnapshotList(std::vector<SnapshotInfo> *list) override;

    int HandleCancelUnSchduledSnapshotTask(
        std::shared_ptr<SnapshotTaskInfo> task) override;

    int HandleCancelScheduledSnapshotTask(
        std::shared_ptr<SnapshotTaskInfo> task) override;

 private:
    /**
     * @brief 构建快照文件映射
     *
     * @param fileName 文件名
     * @param seqNum 快照版本号
     * @param fileSnapshotMap 快照文件映射表
     *
     * @return 错误码
     */
    int BuildSnapshotMap(const std::string &fileName,
        uint64_t seqNum,
        FileSnapMap *fileSnapshotMap);

    /**
     * @brief 构建Segment信息
     *
     * @param info 快照信息
     * @param segInfos Segment信息表
     *
     * @return 错误码
     */
    int BuildSegmentInfo(
        const SnapshotInfo &info,
        std::map<uint64_t, SegmentInfo> *segInfos);

    /**
     * @brief 在curvefs上创建快照
     *
     * @param fileName 文件名
     * @param info 快照信息
     * @param task 快照任务信息
     *
     * @return 错误码
     */
    int CreateSnapshotOnCurvefs(
        const std::string &fileName,
        SnapshotInfo *info,
        std::shared_ptr<SnapshotTaskInfo> task);

    /**
     * @brief 删除curvefs上的快照
     *
     * @param info 快照信息
     *
     * @return 错误码
     */
    int DeleteSnapshotOnCurvefs(const SnapshotInfo &info);

    /**
     * @brief 构建索引块
     *
     * @param info 快照信息
     * @param[out] indexData 索引块
     * @param[out] segInfos Segment信息
     * @param task 快照任务信息
     *
     * @return 错误码
     */
    int BuildChunkIndexData(
        const SnapshotInfo &info,
        ChunkIndexData *indexData,
        std::map<uint64_t, SegmentInfo> *segInfos,
        std::shared_ptr<SnapshotTaskInfo> task);

    using ChunkDataExistFilter =
        std::function<bool(const ChunkDataName &)>;

    /**
     * @brief 转储快照过程
     *
     * @param indexData 索引块
     * @param info 快照信息
     * @param segInfos Segment信息
     * @param filter 转储数据块过滤器
     * @param task 快照任务信息
     *
     * @return  错误码
     */
    int TransferSnapshotData(
        const ChunkIndexData indexData,
        const SnapshotInfo &info,
        const std::map<uint64_t, SegmentInfo> &segInfos,
        const ChunkDataExistFilter &filter,
        std::shared_ptr<SnapshotTaskInfo> task);

    /**
     * @brief 开始cancel，更新任务状态，更新数据库状态
     *
     * @param task 快照任务信息
     *
     * @return  错误码
     */
    int StartCancel(
        std::shared_ptr<SnapshotTaskInfo> task);

    /**
     * @brief 转储数据之后取消快照过程
     *
     * @param task 快照任务信息
     * @param indexData 索引块
     * @param fileSnapshotMap 快照文件映射表
     */
    void CancelAfterTransferSnapshotData(
        std::shared_ptr<SnapshotTaskInfo> task,
        const ChunkIndexData &indexData,
        const FileSnapMap &fileSnapshotMap);

    /**
     * @brief 创建索引块之后取消快照过程
     *
     * @param task 快照任务信息
     */
    void CancelAfterCreateChunkIndexData(
        std::shared_ptr<SnapshotTaskInfo> task);

    /**
     * @brief 在curvefs上创建快照之后取消快照过程
     *
     * @param task 快照任务信息
     */
    void CancelAfterCreateSnapshotOnCurvefs(
        std::shared_ptr<SnapshotTaskInfo> task);

    /**
     * @brief 在Mate数据存储在删除快照
     *
     * @param task 快照任务信息
     */
    void HandleClearSnapshotOnMateStore(
        std::shared_ptr<SnapshotTaskInfo> task);

    /**
     * @brief 处理创建快照任务成功
     *
     * @param task 快照任务信息
     */
    void HandleCreateSnapshotSuccess(
        std::shared_ptr<SnapshotTaskInfo> task);

    /**
     * @brief 处理创建快照任务失败过程
     *
     * @param task 快照任务信息
     */
    void HandleCreateSnapshotError(
        std::shared_ptr<SnapshotTaskInfo> task);

    /**
     * @brief 处理删除快照任务失败过程
     *
     * @param task 快照任务信息
     */
    void HandleDeleteSnapshotError(
        std::shared_ptr<SnapshotTaskInfo> task);


    /**
     * @brief 创建快照前尝试清理失败的快照，否则可能会再次失败
     *
     * @param task 快照任务信息
     * @return 错误码
     */
    int ClearErrorSnapBeforeCreateSnapshot(
        std::shared_ptr<SnapshotTaskInfo> task);

    /**
     * @brief check local snapshot deleting or not
     */
    void CheckLocalSnapshot();

 private:
    // curvefs客户端对象
    std::shared_ptr<CurveFsClient> client_;
    // meta数据存储
    std::shared_ptr<SnapshotCloneMetaStore> metaStore_;
    // data数据存储
    std::shared_ptr<SnapshotDataStore> dataStore_;
    // 快照引用计数管理模块
    std::shared_ptr<SnapshotReference> snapshotRef_;

    // checking local snapshot deleting
    common::Thread *checkThread_;
    int checkPeriod_;
    std::list<SnapshotInfo> deletingSnapshots_;
    common::Mutex deletingSnapshotsMutex_;
    InterruptibleSleeper sleeper_;

    // 执行并发步骤的线程池
    std::shared_ptr<ThreadPool> threadPool_;

    // 锁住打快照的文件名，防止并发同时对其打快照，同一文件的快照需排队
    NameLock snapshotNameLock_;

    // 转储chunk分片大小
    uint64_t chunkSplitSize_;
    // CheckSnapShotStatus调用间隔
    uint32_t checkSnapshotStatusIntervalMs_;
    // 最大快照数
    uint32_t maxSnapshotLimit_;
    // 线程数
    uint32_t snapshotCoreThreadNum_;
    // session超时时间
    uint32_t mdsSessionTimeUs_;
    // client异步回调请求的重试总时间
    uint64_t clientAsyncMethodRetryTimeSec_;
    // 调用client异步方法重试时间间隔
    uint64_t clientAsyncMethodRetryIntervalMs_;
    // 异步ReadChunkSnapshot的并发数
    uint32_t readChunkSnapshotConcurrency_;
};

}  // namespace snapshotcloneserver
}  // namespace curve

#endif  // SRC_SNAPSHOTCLONESERVER_SNAPSHOT_SNAPSHOT_CORE_H_
