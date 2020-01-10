/*
 * Project: curve
 * Created Date: Fri Dec 14 2018
 * Author: xuchaojie
 * Copyright (c) 2018 netease
 */

#ifndef SRC_SNAPSHOTCLONESERVER_SNAPSHOT_SNAPSHOT_SERVICE_MANAGER_H_
#define SRC_SNAPSHOTCLONESERVER_SNAPSHOT_SNAPSHOT_SERVICE_MANAGER_H_

#include <memory>
#include <string>
#include <vector>

#include "src/snapshotcloneserver/snapshot/snapshot_core.h"
#include "src/snapshotcloneserver/snapshot/snapshot_task.h"
#include "src/snapshotcloneserver/snapshot/snapshot_task_manager.h"
#include "src/snapshotcloneserver/common/define.h"
#include "src/snapshotcloneserver/common/config.h"
#include "json/json.h"

namespace curve {
namespace snapshotcloneserver {

/**
 * @brief 文件单个快照信息
 */
class FileSnapshotInfo {
 public:
    FileSnapshotInfo() = default;

     /**
      * @brief 构造函数
      *
      * @param snapInfo 快照信息
      * @param snapProgress 快照完成度百分比
      */
    FileSnapshotInfo(const SnapshotInfo &snapInfo,
        uint32_t snapProgress)
        : snapInfo_(snapInfo),
          snapProgress_(snapProgress) {}

    void SetSnapshotInfo(const SnapshotInfo &snapInfo) {
        snapInfo_ = snapInfo;
    }

    SnapshotInfo GetSnapshotInfo() const {
        return snapInfo_;
    }

    void SetSnapProgress(uint32_t progress) {
        snapProgress_ = progress;
    }

    uint32_t GetSnapProgress() const {
        return snapProgress_;
    }

    Json::Value ToJsonObj() const {
        Json::Value fileSnapObj;
        SnapshotInfo snap = GetSnapshotInfo();
        fileSnapObj["UUID"] = snap.GetUuid();
        fileSnapObj["User"] = snap.GetUser();
        fileSnapObj["File"] = snap.GetFileName();
        fileSnapObj["SeqNum"] = snap.GetSeqNum();
        fileSnapObj["Name"] = snap.GetSnapshotName();
        fileSnapObj["Time"] = snap.GetCreateTime();
        fileSnapObj["FileLength"] = snap.GetFileLength();
        fileSnapObj["Status"] = static_cast<int>(snap.GetStatus());
        fileSnapObj["Progress"] = GetSnapProgress();
        return fileSnapObj;
    }

    void LoadFromJsonObj(const Json::Value &jsonObj) {
        SnapshotInfo snapInfo;
        snapInfo.SetUuid(jsonObj["UUID"].asString());
        snapInfo.SetUser(jsonObj["User"].asString());
        snapInfo.SetFileName(jsonObj["File"].asString());
        snapInfo.SetSeqNum(jsonObj["SeqNum"].asUInt64());
        snapInfo.SetSnapshotName(jsonObj["Name"].asString());
        snapInfo.SetCreateTime(jsonObj["Time"].asUInt64());
        snapInfo.SetFileLength(jsonObj["FileLength"].asUInt64());
        snapInfo.SetStatus(static_cast<Status>(jsonObj["Status"].asUInt()));
        SetSnapshotInfo(snapInfo);
        SetSnapProgress(jsonObj["Progress"].asUInt());
    }

 private:
    // 快照信息
    SnapshotInfo snapInfo_;
    // 快照处理进度百分比
    uint32_t snapProgress_;
};

class SnapshotServiceManager {
 public:
     /**
      * @brief 构造函数
      *
      * @param taskMgr 快照任务管理类对象
      * @param core 快照核心模块
      */
    SnapshotServiceManager(
        std::shared_ptr<SnapshotTaskManager> taskMgr,
        std::shared_ptr<SnapshotCore> core)
          : taskMgr_(taskMgr),
            core_(core) {}

    virtual ~SnapshotServiceManager() {}

    /**
     * @brief 初始化
     *
     * @return 错误码
     */
    virtual int Init(const SnapshotCloneServerOptions &option);

    /**
     * @brief 启动服务
     *
     * @return 错误码
     */
    virtual int Start();

    /**
     * @brief 停止服务
     *
     */
    virtual void Stop();

    /**
     * @brief 创建快照服务
     *
     * @param file 文件名
     * @param user 文件所属用户
     * @param snapshotName 快照名
     * @param uuid 快照uuid
     *
     * @return 错误码
     */
    virtual int CreateSnapshot(const std::string &file,
        const std::string &user,
        const std::string &snapshotName,
        UUID *uuid);

    /**
     * @brief 删除快照服务
     *
     * @param uuid 快照uuid
     * @param user 快照文件的用户
     * @param file 快照所属文件的文件名
     *
     * @return 错误码
     */
    virtual int DeleteSnapshot(const UUID &uuid,
        const std::string &user,
        const std::string &file);

    /**
     * @brief 取消快照服务
     *
     * @param uuid 快照的uuid
     * @param user 快照的用户
     * @param file 快照所属文件的文件名
     *
     * @return 错误码
     */
    virtual int CancelSnapshot(const UUID &uuid,
        const std::string &user,
        const std::string &file);

    /**
     * @brief 获取文件的快照信息服务接口
     *
     * @param file 文件名
     * @param user 用户名
     * @param info 快照信息列表
     *
     * @return 错误码
     */
    virtual int GetFileSnapshotInfo(const std::string &file,
        const std::string &user,
        std::vector<FileSnapshotInfo> *info);

    /**
     * @brief 根据Id获取文件的快照信息
     *
     * @param file 文件名
     * @param user 用户名
     * @param uuid 快照Id
     * @param info 快照信息列表
     *
     * @return 错误码
     */
    virtual int GetFileSnapshotInfoById(const std::string &file,
        const std::string &user,
        const UUID &uuid,
        std::vector<FileSnapshotInfo> *info);

    /**
     * @brief 恢复快照任务接口
     *
     * @return 错误码
     */
    virtual int RecoverSnapshotTask();

 private:
    /**
     * @brief 根据快照信息获取快照任务信息
     *
     * @param snapInfos 快照信息
     * @param user 用户名
     * @param[out] info 快照任务信息
     *
     * @return 错误码
     */
    int GetFileSnapshotInfoInner(
        std::vector<SnapshotInfo> snapInfos,
        const std::string &user,
        std::vector<FileSnapshotInfo> *info);

 private:
    // 快照任务管理类对象
    std::shared_ptr<SnapshotTaskManager> taskMgr_;
    // 快照核心模块
    std::shared_ptr<SnapshotCore> core_;
};

}  // namespace snapshotcloneserver
}  // namespace curve

#endif  // SRC_SNAPSHOTCLONESERVER_SNAPSHOT_SNAPSHOT_SERVICE_MANAGER_H_
