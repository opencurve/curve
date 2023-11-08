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
 * Created Date: Fri 12 Apr 2019 05:24:18 PM CST
 * Author: xuchaojie
 */
#ifndef SRC_SNAPSHOTCLONESERVER_CLONE_CLONE_SERVICE_MANAGER_H_
#define SRC_SNAPSHOTCLONESERVER_CLONE_CLONE_SERVICE_MANAGER_H_

#include <string>
#include <vector>
#include <memory>

#include "src/common/wait_interval.h"
#include "src/snapshotcloneserver/clone/clone_core.h"
#include "src/snapshotcloneserver/clone/clone_task.h"
#include "src/snapshotcloneserver/clone/clone_task_manager.h"
#include "src/common/snapshotclone/snapshotclone_define.h"
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
        cloneTaskObj["Src"] = info.GetSrc();
        cloneTaskObj["TaskType"] = static_cast<int> (
            info.GetTaskType());
        cloneTaskObj["TaskStatus"] = static_cast<int> (
            info.GetStatus());
        cloneTaskObj["IsLazy"] = info.GetIsLazy();
        cloneTaskObj["NextStep"] = static_cast<int> (info.GetNextStep());
        cloneTaskObj["Time"] = info.GetTime();
        cloneTaskObj["Progress"] = GetCloneProgress();
        cloneTaskObj["FileType"] = static_cast<int> (info.GetFileType());
        return cloneTaskObj;
    }

    void LoadFromJsonObj(const Json::Value &jsonObj) {
        CloneInfo info;
        info.SetTaskId(jsonObj["UUID"].asString());
        info.SetUser(jsonObj["User"].asString());
        info.SetDest(jsonObj["File"].asString());
        info.SetSrc(jsonObj["Src"].asString());
        info.SetTaskType(static_cast<CloneTaskType>(
            jsonObj["TaskType"].asInt()));
        info.SetStatus(static_cast<CloneStatus>(
            jsonObj["TaskStatus"].asInt()));
        info.SetIsLazy(jsonObj["IsLazy"].asBool());
        info.SetNextStep(static_cast<CloneStep>(jsonObj["NextStep"].asInt()));
        info.SetTime(jsonObj["Time"].asUInt64());
        info.SetFileType(static_cast<CloneFileType>(
            jsonObj["FileType"].asInt()));
        SetCloneInfo(info);
    }

 private:
     CloneInfo cloneInfo_;
     uint32_t cloneProgress_;
};

class CloneFilterCondition {
 public:
    CloneFilterCondition()
                   : uuid_(nullptr),
                    source_(nullptr),
                    destination_(nullptr),
                    user_(nullptr),
                    status_(nullptr),
                    type_(nullptr) {}

    CloneFilterCondition(const std::string *uuid, const std::string *source,
                        const std::string *destination, const std::string *user,
                        const std::string *status, const std::string *type)
                   : uuid_(uuid),
                    source_(source),
                    destination_(destination),
                    user_(user),
                    status_(status),
                    type_(type) {}
    bool IsMatchCondition(const CloneInfo &cloneInfo);

    void SetUuid(const std::string *uuid) {
        uuid_ = uuid;
    }
    void SetSource(const std::string *source) {
        source_ = source;
    }
    void SetDestination(const std::string *destination) {
        destination_ = destination;
    }
    void SetUser(const std::string *user) {
        user_ = user;
    }
    void SetStatus(const std::string *status) {
        status_ = status;
    }
    void SetType(const std::string *type) {
        type_ = type;
    }

 private:
    const std::string *uuid_;
    const std::string *source_;
    const std::string *destination_;
    const std::string *user_;
    const std::string *status_;
    const std::string *type_;
};
class CloneServiceManagerBackend {
 public:
    CloneServiceManagerBackend() {}
    virtual ~CloneServiceManagerBackend() {}

    /**
     * @brief 后台扫描线程执行函数，扫描克隆卷是否存在
     *
     */
    virtual void Func() = 0;

    virtual void Init(uint32_t recordIntevalMs, uint32_t roundIntevalMs) = 0;

    virtual void Start() = 0;

    virtual void Stop() = 0;
};

class CloneServiceManagerBackendImpl : public CloneServiceManagerBackend {
 public:
    explicit CloneServiceManagerBackendImpl(
        std::shared_ptr<CloneCore> cloneCore)
          : cloneCore_(cloneCore),
            isStop_(true) {
    }

    ~CloneServiceManagerBackendImpl() {
    }

    void Func() override;
    void Init(uint32_t recordIntevalMs, uint32_t roundIntevalMs) override;
    void Start() override;
    void Stop() override;

 private:
    std::shared_ptr<CloneCore> cloneCore_;
    // 后台扫描线程，扫描clone卷是否存在
    std::thread backEndReferenceScanThread_;
    // 当前后台扫描是否停止，用于支持start，stop功能
    std::atomic_bool isStop_;
    // 后台扫描线程记录使用定时器
    common::WaitInterval recordWaitInterval_;
    // 后台扫描线程每轮使用定时器
    common::WaitInterval roundWaitInterval_;
};

class CloneServiceManager {
 public:
    CloneServiceManager(
        std::shared_ptr<CloneTaskManager> cloneTaskMgr,
        std::shared_ptr<CloneCore> cloneCore,
        std::shared_ptr<CloneServiceManagerBackend> cloneServiceManagerBackend)
          : cloneTaskMgr_(cloneTaskMgr),
            cloneCore_(cloneCore),
            cloneServiceManagerBackend_(cloneServiceManagerBackend) {
        destFileLock_ = std::make_shared<NameLock>();
    }
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
     * @brief local clone
     *
     * @param file  source volume of clone
     * @param snapshotName  source snapshot name of clone
     * @param user  user of the clone destination volume
     * @param destination destination volume of clone
     * @param poolset poolset of the clone destination volume
     *
     * @return error code
     */
    virtual int CloneLocal(const std::string &file,
        const std::string &snapshotName,
        const std::string &user,
        const std::string &destination,
        const std::string &poolset,
        bool readonlyFlag);

    /**
     * @brief local flatten
     *
     * @param file  source volume of flatten
     * @param user  user of the flatten destination volume
     *
     * @return  error code
     */
    virtual int FlattenLocal(const std::string &file,
        const std::string &user);

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
        const std::string &poolset,
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
     * @brief 安装克隆文件的数据，用于Lazy克隆
     *
     * @param user 用户
     * @param taskId 任务ID
     *
     * @return 错误码
     */
    virtual int Flatten(
        const std::string &user,
        const TaskIdType &taskId);

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
     * @brief 通过过滤条件查询某个用户的克隆/恢复任务信息
     *
     * @param filter 过滤条件
     * @param info 克隆/恢复任务信息
     *
     * @return 错误码
     */
    virtual int GetCloneTaskInfoByFilter(const CloneFilterCondition &filter,
                            std::vector<TaskCloneInfo> *info);

    /**
     * @brief 查询src是否有依赖
     *
     * @param src 指定的文件名
     * @param refStatus 0表示没有依赖，1表示有依赖，2表示需要进一步确认
     * @param needCheckFiles 需要进一步确认的文件列表
     *
     * @return 错误码
     */
    virtual int GetCloneRefStatus(const std::string &src,
        CloneRefStatus *refStatus,
        std::vector<CloneInfo> *needCheckFiles);

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
     * @brief 从给定的任务列表中获取指定用户的任务集
     *
     * @param cloneInfos 克隆/恢复信息
     * @param user 用户信息
     * @param[out] info 克隆/恢复任务信息
     *
     * @return 错误码
     */
    int GetCloneTaskInfoInner(std::vector<CloneInfo> cloneInfos,
        const std::string &user,
        std::vector<TaskCloneInfo> *info);

    /**
     * @brief 从给定的任务列表中获取符合过滤条件的任务集
     *
     * @param cloneInfos 克隆/恢复信息
     * @param filter 过滤条件
     * @param[out] info 克隆/恢复任务信息
     *
     * @return 错误码
     */
    int GetCloneTaskInfoInner(std::vector<CloneInfo> cloneInfos,
        CloneFilterCondition filter,
        std::vector<TaskCloneInfo> *info);

    /**
     * @brief 获取已经完成任务信息
     *
     * @param taskId 任务ID
     * @param taskCloneInfoOut 克隆任务信息
     *
     * @return 错误码
     */
    int GetFinishedCloneTask(
        const TaskIdType &taskId,
        TaskCloneInfo *taskCloneInfoOut);

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

    /**
     * @brief 构建和push Lazy的任务
     *
     * @param cloneInfo 克隆任务信息
     * @param closure 异步回调实体
     *
     * @return 错误码
     */
    int BuildAndPushCloneOrRecoverLazyTask(
        CloneInfo cloneInfo,
        std::shared_ptr<CloneClosure> closure);

    /**
     * @brief 构建和push 非Lazy的任务
     *
     * @param cloneInfo 克隆任务信息
     * @param closure 异步回调实体
     *
     * @return 错误码
     */
    int BuildAndPushCloneOrRecoverNotLazyTask(
        CloneInfo cloneInfo,
        std::shared_ptr<CloneClosure> closure);

 private:
    std::shared_ptr<NameLock> destFileLock_;
    std::shared_ptr<CloneTaskManager> cloneTaskMgr_;
    std::shared_ptr<CloneCore> cloneCore_;
    std::shared_ptr<CloneServiceManagerBackend> cloneServiceManagerBackend_;
};



}  // namespace snapshotcloneserver
}  // namespace curve

#endif  // SRC_SNAPSHOTCLONESERVER_CLONE_CLONE_SERVICE_MANAGER_H_
