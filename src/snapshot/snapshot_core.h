/*
 * Project: curve
 * Created Date: Sat Dec 15 2018
 * Author: xuchaojie
 * Copyright (c) 2018 netease
 */

#ifndef CURVE_SRC_SNAPSHOT_SNAPSHOTCORE_H_
#define CURVE_SRC_SNAPSHOT_SNAPSHOTCORE_H_

#include <memory>
#include <string>
#include <vector>
#include <map>

#include "src/snapshot/curvefs_client.h"
#include "src/snapshot/snapshot_meta_store.h"
#include "src/snapshot/snapshot_data_store.h"
#include "src/snapshot/UUID_generator.h"
#include "src/snapshot/snapshot_define.h"

namespace curve {
namespace snapshotserver {

class SnapshotTaskInfo;

struct FileSnapMap {
    std::vector<ChunkIndexData> maps;

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

class SnapshotCore {
 public:
    SnapshotCore() {}
    virtual ~SnapshotCore() {}

    virtual int CreateSnapshotPre(const std::string &file,
        const std::string &user,
        const std::string &snapshotName,
        SnapshotInfo *snapInfo) = 0;

    virtual void HandleCreateSnapshotTask(
        std::shared_ptr<SnapshotTaskInfo> task) = 0;

    virtual int DeleteSnapshotPre(
        UUID uuid,
        const std::string &user,
        const std::string &fileName,
        SnapshotInfo *snapInfo) = 0;

    virtual void HandleDeleteSnapshotTask(
        std::shared_ptr<SnapshotTaskInfo> task) = 0;

    virtual int GetFileSnapshotInfo(const std::string &file,
        std::vector<SnapshotInfo> *info) = 0;

    virtual int GetSnapshotList(std::vector<SnapshotInfo> *list) = 0;
};

class SnapshotCoreImpl : public SnapshotCore {
 public:
    SnapshotCoreImpl(
        std::shared_ptr<UUIDGenerator> idGen,
        std::shared_ptr<CurveFsClient> client,
        std::shared_ptr<SnapshotMetaStore> metaStore,
        std::shared_ptr<SnapshotDataStore> dataStore)
    : UUIDGenerator_(idGen),
      client_(client),
      metaStore_(metaStore),
      dataStore_(dataStore) {}

    /**
     * @brief 创建快照前置操作
     * 数据库中插入一条快照记录
     */
    int CreateSnapshotPre(const std::string &file,
        const std::string &user,
        const std::string &snapshotName,
        SnapshotInfo *snapInfo) override;

    /**
     * @brief 执行创建快照任务并更新progress
     * 第一步，构建快照文件映射, put MateObj
     * 第二步，从curvefs读取chunk文件，并put DataObj
     * 第三步，删除curvefs中的临时快照
     * 第四步，update status
     *
     * @param task 快照任务信息
     */
    void HandleCreateSnapshotTask(
        std::shared_ptr<SnapshotTaskInfo> task) override;

    /**
     * @brief 删除快照前置操作
     * 更新数据库中的快照记录为deleting状态
     */
    int DeleteSnapshotPre(UUID uuid,
        const std::string &user,
        const std::string &fileName,
        SnapshotInfo *snapInfo) override;

    /**
     * @brief 执行删除快照任务并更新progress
     *
     * @param task
     */
    void HandleDeleteSnapshotTask(
        std::shared_ptr<SnapshotTaskInfo> task) override;

    /**
     * @brief 获取快照信息
     *
     */
    int GetFileSnapshotInfo(const std::string &file,
        std::vector<SnapshotInfo> *info) override;


    int GetSnapshotList(std::vector<SnapshotInfo> *list) override;

 private:
    int BuildSnapshotMap(const std::string &fileName,
        uint64_t seqNum,
        FileSnapMap *fileSnapshotMap);

    int BuildSegmentInfo(
        const SnapshotInfo &info,
        std::vector<SegmentInfo> *segInfos);

    int CreateSnapshotOnCurvefs(
        const std::string &fileName,
        SnapshotInfo *info,
        std::shared_ptr<SnapshotTaskInfo> task);

    int BuildChunkIndexData(
        const SnapshotInfo &info,
        ChunkIndexData *indexData,
        std::vector<SegmentInfo> *segInfos,
        std::shared_ptr<SnapshotTaskInfo> task);

    using ChunkDataExistFilter =
        std::function<bool(const ChunkDataName &)>;

    int TransferSnapshotData(
        const ChunkIndexData indexData,
        const SnapshotInfo &info,
        const std::vector<SegmentInfo> &segInfos,
        const ChunkDataExistFilter &filter,
        std::shared_ptr<SnapshotTaskInfo> task);

    int TransferSnapshotDataChunk(
        const ChunkDataName &name,
        uint64_t chunkSize,
        const ChunkIDInfo &cidInfo);

    void CancelAfterTransferSnapshotData(
        std::shared_ptr<SnapshotTaskInfo> task,
        const ChunkIndexData &indexData,
        const FileSnapMap &fileSnapshotMap);

    void CancelAfterCreateChunkIndexData(
        std::shared_ptr<SnapshotTaskInfo> task);

    void CancelAfterCreateSnapshotOnCurvefs(
        std::shared_ptr<SnapshotTaskInfo> task);

    void HandleClearSnapshotOnMateStore(
        std::shared_ptr<SnapshotTaskInfo> task);

    void HandleCreateSnapshotError(
        std::shared_ptr<SnapshotTaskInfo> task);

    void HandleDeleteSnapshotError(
        std::shared_ptr<SnapshotTaskInfo> task);

 private:
    std::shared_ptr<UUIDGenerator> UUIDGenerator_;
    std::shared_ptr<CurveFsClient> client_;
    std::shared_ptr<SnapshotMetaStore> metaStore_;
    std::shared_ptr<SnapshotDataStore> dataStore_;
};

}  // namespace snapshotserver
}  // namespace curve

#endif  // CURVE_SRC_SNAPSHOT_SNAPSHOTCORE_H_
