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

#include "src/snapshotcloneserver/clone/clone_core.h"
#include "src/snapshotcloneserver/clone/clone_task.h"
#include "src/snapshotcloneserver/clone/clone_task_manager.h"
#include "src/snapshotcloneserver/common/define.h"

namespace curve {
namespace snapshotcloneserver {

class TaskCloneInfo {
 public:
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
    virtual int Init();

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
     *
     * @return 错误码
     */
    virtual int CloneFile(const UUID &source,
        const std::string &user,
        const std::string &destination,
        bool lazyFlag);

    /**
     * @brief 从文件或快照恢复一个文件
     *
     * @param source  文件或快照的uuid
     * @param user  文件或快照的用户
     * @param destination 目标文件名
     * @param lazyFlag  是否lazy模式
     *
     * @return 错误码
     */
    virtual int RecoverFile(const UUID &source,
        const std::string &user,
        const std::string &destination,
        bool lazyFlag);

    /**
     * @brief 查询某个用户的克隆/恢复任务信息
     *
     * @param user 用户名
     * @param info 克隆/恢复任务信息
     *
     * @return 错误码
     */
    virtual int GetCloneTaskInfo(const std::string &user,
        std::vector<TaskCloneInfo> *info);

    /**
     * @brief 重启后恢复未完成clone和recover任务
     *
     * @return 错误码
     */
    virtual int RecoverCloneTask();

 private:
    std::shared_ptr<CloneTaskManager> cloneTaskMgr_;
    std::shared_ptr<CloneCore> cloneCore_;
};

}  // namespace snapshotcloneserver
}  // namespace curve

#endif  // SRC_SNAPSHOTCLONESERVER_CLONE_CLONE_SERVICE_MANAGER_H_
