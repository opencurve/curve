/*
 * Project: curve
 * Created Date: Fri 12 Apr 2019 05:24:18 PM CST
 * Author: xuchaojie
 * Copyright (c) 2019 netease
 */
#ifndef SRC_SNAPSHOTCLONESERVER_CLONE_CLONE_SERVICE_MANAGER_H_
#define SRC_SNAPSHOTCLONESERVER_CLONE_CLONE_SERVICE_MANAGER_H_

#include <string>
#include <vector>
#include <memory>

#include "src/snapshotcloneserver/clone/clone_core.h"
#include "src/snapshotcloneserver/clone/clone_task.h"
#include "src/snapshotcloneserver/clone/clone_task_manager.h"
#include "src/snapshotcloneserver/common/define.h"
#include "src/snapshotcloneserver/common/config.h"
#include "src/snapshotcloneserver/clone/clone_closure.h"

namespace curve {
namespace snapshotcloneserver {

class TaskCloneInfo {
 public:
    TaskCloneInfo() = default;

    TaskCloneInfo(const CloneInfo &cloneInfo,
        uint32_t progress)
        : cloneInfo_(cloneInfo),
          cloneProgress_(progress) {}

    void SetCloneInfo(const CloneInfo &cloneInfo) {
        cloneInfo_ = cloneInfo;
    }

    CloneInfo GetCloneInfo() const {
        return cloneInfo_;
    }

    void SetCloneProgress(uint32_t progress) {
        cloneProgress_ = progress;
    }

    uint32_t GetCloneProgress() const {
        return cloneProgress_;
    }

    Json::Value ToJsonObj() const {
        Json::Value cloneTaskObj;
        CloneInfo info = GetCloneInfo();
        cloneTaskObj["UUID"] = info.GetTaskId();
        cloneTaskObj["User"] = info.GetUser();
        cloneTaskObj["File"] = info.GetDest();
        cloneTaskObj["TaskType"] = static_cast<int> (
            info.GetTaskType());
        cloneTaskObj["TaskStatus"] = static_cast<int> (
            info.GetStatus());
        cloneTaskObj["Time"] = info.GetTime();
        return cloneTaskObj;
    }

    void LoadFromJsonObj(const Json::Value &jsonObj) {
        CloneInfo info;
        info.SetTaskId(jsonObj["UUID"].asString());
        info.SetUser(jsonObj["User"].asString());
        info.SetDest(jsonObj["File"].asString());
        info.SetTaskType(static_cast<CloneTaskType>(
            jsonObj["TaskType"].asInt()));
        info.SetStatus(static_cast<CloneStatus>(
            jsonObj["TaskStatus"].asInt()));
        info.SetTime(jsonObj["Time"].asUInt64());
        SetCloneInfo(info);
    }

 private:
     CloneInfo cloneInfo_;
     uint32_t cloneProgress_;
};

class CloneServiceManager {
 public:
    CloneServiceManager(
        std::shared_ptr<CloneTaskManager> cloneTaskMgr,
        std::shared_ptr<CloneCore> cloneCore)
          : cloneTaskMgr_(cloneTaskMgr),
            cloneCore_(cloneCore) {}
    virtual ~CloneServiceManager() {}

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
     * @brief 从文件或快照克隆出一个文件
     *
     * @param source  文件或快照的uuid
     * @param user  文件或快照的用户
     * @param destination 目标文件
     * @param lazyFlag  是否lazy模式
     * @param closure 异步回调实体
     * @param[out] taskId 任务ID
     *
     * @return 错误码
     */
    virtual int CloneFile(const UUID &source,
        const std::string &user,
        const std::string &destination,
        bool lazyFlag,
        std::shared_ptr<CloneClosure> closure,
        TaskIdType *taskId);

    /**
     * @brief 从文件或快照恢复一个文件
     *
     * @param source  文件或快照的uuid
     * @param user  文件或快照的用户
     * @param destination 目标文件名
     * @param lazyFlag  是否lazy模式
     * @param closure 异步回调实体
     * @param[out] taskId 任务ID
     *
     * @return 错误码
     */
    virtual int RecoverFile(const UUID &source,
        const std::string &user,
        const std::string &destination,
        bool lazyFlag,
        std::shared_ptr<CloneClosure> closure,
        TaskIdType *taskId);

    /**
     * @brief 查询某个用户的克隆/恢复任务信息
     *
     * @param user 用户名
     * @param taskId 指定的任务Id, 为nullptr时不指定
     * @param fileName 指定的文件名, 为nullptr时不指定
     * @param info 克隆/恢复任务信息
     *
     * @return 错误码
     */
    virtual int GetCloneTaskInfo(const std::string &user,
        std::vector<TaskCloneInfo> *info);

    /**
     * @brief 通过Id查询某个用户的克隆/恢复任务信息
     *
     * @param user 用户名
     * @param taskId 指定的任务Id
     * @param info 克隆/恢复任务信息
     *
     * @return 错误码
     */
    virtual int GetCloneTaskInfoById(
        const std::string &user,
        const TaskIdType &taskId,
        std::vector<TaskCloneInfo> *info);

    /**
     * @brief 通过文件名查询某个用户的克隆/恢复任务信息
     *
     * @param user 用户名
     * @param fileName 指定的文件名
     * @param info 克隆/恢复任务信息
     *
     * @return 错误码
     */
    virtual int GetCloneTaskInfoByName(
        const std::string &user,
        const std::string &fileName,
        std::vector<TaskCloneInfo> *info);

    /**
     * @brief 清除失败的clone/Recover任务、状态、文件
     *
     * @param user 用户名
     * @param taskId 任务Id
     *
     * @return 错误码
     */
    virtual int CleanCloneTask(const std::string &user,
        const TaskIdType &taskId);

    /**
     * @brief 重启后恢复未完成clone和recover任务
     *
     * @return 错误码
     */
    virtual int RecoverCloneTask();

 private:
    /**
     * @brief 根据指定克隆/恢复信息获取克隆/恢复任务信息
     *
     * @param cloneInfos 克隆/恢复信息
     * @param[out] info 克隆/恢复任务信息
     *
     * @return 错误码
     */
    int GetCloneTaskInfoInner(std::vector<CloneInfo> cloneInfos,
        const std::string &user,
        std::vector<TaskCloneInfo> *info);

    /**
     * @brief 根据克隆任务信息恢复克隆任务
     *
     * @param cloneInfo 克隆任务信息
     *
     * @return 错误码
     */
    int RecoverCloneTaskInternal(const CloneInfo &cloneInfo);

    /**
     * @brief 根据克隆任务信息恢复清除克隆任务
     *
     * @param cloneInfo 克隆任务信息
     *
     * @return 错误码
     */
    int RecoverCleanTaskInternal(const CloneInfo &cloneInfo);

 private:
    std::shared_ptr<CloneTaskManager> cloneTaskMgr_;
    std::shared_ptr<CloneCore> cloneCore_;
};

}  // namespace snapshotcloneserver
}  // namespace curve

#endif  // SRC_SNAPSHOTCLONESERVER_CLONE_CLONE_SERVICE_MANAGER_H_
