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
#include "src/common/concurrent/dlock.h"

namespace curve {
namespace snapshotcloneserver {

using DLockOpts = ::curve::common::DLockOpts;

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
     * @brief Background scan thread execution function to scan for the existence of cloned volumes
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
    //Background scan thread to check if clone volume exists
    std::thread backEndReferenceScanThread_;
    //Is the current background scanning stopped? Used to support start and stop functions
    std::atomic_bool isStop_;
    //Using a timer for background scanning thread records
    common::WaitInterval recordWaitInterval_;
    //The backend scanning thread uses a timer for each round
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
     * @brief initialization
     *
     * @return error code
     */
    virtual int Init(const SnapshotCloneServerOptions &option);

    /**
     * @brief Start Service
     *
     * @return error code
     */
    virtual int Start();

    /**
     * @brief Stop service
     *
     */
    virtual void Stop();

    /**
     * @brief Clone a file from a file or snapshot
     *
     * @param source Uuid of file or snapshot
     * @param user The user of the file or snapshot
     * @param destination destination destination file
     * @param lazyFlag Is in lazy mode
     * @param closure asynchronous callback entity
     * @param[out] taskId Task ID
     *
     * @return error code
     */
    virtual int CloneFile(const UUID &source,
        const std::string &user,
        const std::string &destination,
        const std::string &poolset,
        bool lazyFlag,
        std::shared_ptr<CloneClosure> closure,
        TaskIdType *taskId);

    /**
     * @brief Restore a file from a file or snapshot
     *
     * @param source Uuid of file or snapshot
     * @param user The user of the file or snapshot
     * @param destination destination destination file name
     * @param lazyFlag Is in lazy mode
     * @param closure asynchronous callback entity
     * @param[out] taskId Task ID
     *
     * @return error code
     */
    virtual int RecoverFile(const UUID &source,
        const std::string &user,
        const std::string &destination,
        bool lazyFlag,
        std::shared_ptr<CloneClosure> closure,
        TaskIdType *taskId);

    /**
     * @brief Install data from clone files for Lazy cloning
     *
     * @param user user
     * @param taskId Task ID
     *
     * @return error code
     */
    virtual int Flatten(
        const std::string &user,
        const TaskIdType &taskId);

    /**
     * @brief: Query the clone/restore task information of a certain user
     *
     * @param user username
     * @param info Clone/Restore Task Information
     *
     * @return error code
     */
    virtual int GetCloneTaskInfo(const std::string &user,
        std::vector<TaskCloneInfo> *info);

    /**
     * @brief: Query the clone/restore task information of a certain user through ID
     *
     * @param user username
     * @param taskId Task Id specified 
     * @param info Clone/Restore Task Information
     *
     * @return error code
     */
    virtual int GetCloneTaskInfoById(
        const std::string &user,
        const TaskIdType &taskId,
        std::vector<TaskCloneInfo> *info);

    /**
     * @brief Query the clone/restore task information of a certain user through a file name
     *
     * @param user username
     * @param fileName The file name specified
     * @param info Clone/Restore Task Information
     *
     * @return error code
     */
    virtual int GetCloneTaskInfoByName(
        const std::string &user,
        const std::string &fileName,
        std::vector<TaskCloneInfo> *info);

    /**
     * @brief: Query a user's clone/restore task information through filtering criteria
     *
     * @param filter filtering conditions
     * @param info Clone/Restore Task Information
     *
     * @return error code
     */
    virtual int GetCloneTaskInfoByFilter(const CloneFilterCondition &filter,
                            std::vector<TaskCloneInfo> *info);

    /**
     * @brief: Check if src has dependencies
     *
     * @param src specified file name
     * @param refStatus 0 indicates no dependencies, 1 indicates dependencies, and 2 indicates further confirmation is needed
     * @param needCheckFiles List of files that require further confirmation
     *
     * @return error code
     */
    virtual int GetCloneRefStatus(const std::string &src,
        CloneRefStatus *refStatus,
        std::vector<CloneInfo> *needCheckFiles);

    /**
     * @brief Clear failed clone/recover tasks, status, files
     *
     * @param user username
     * @param taskId Task Id
     *
     * @return error code
     */
    virtual int CleanCloneTask(const std::string &user,
        const TaskIdType &taskId);

    /**
     * @brief: Restore unfinished clone and recover tasks after restarting
     *
     * @return error code
     */
    virtual int RecoverCloneTask();

    // for test
    void SetDLock(std::shared_ptr<DLock> dlock) {
        dlock_ = dlock;
    }

 private:
    /**
     * @brief Get the task set of the specified user from the given task list
     *
     * @param cloneInfos Clone/Restore Information
     * @param user user information
     * @param[out] info Clone/restore task information
     *
     * @return error code
     */
    int GetCloneTaskInfoInner(std::vector<CloneInfo> cloneInfos,
        const std::string &user,
        std::vector<TaskCloneInfo> *info);

    /**
     * @brief Retrieve task sets that meet the filtering criteria from the given task list
     *
     * @param cloneInfos Clone/Restore Information
     * @param filter filtering conditions
     * @param[out] info Clone/restore task information
     *
     * @return error code
     */
    int GetCloneTaskInfoInner(std::vector<CloneInfo> cloneInfos,
        CloneFilterCondition filter,
        std::vector<TaskCloneInfo> *info);

    /**
     * @brief to obtain completed task information
     *
     * @param taskId Task ID
     * @param taskCloneInfoOut Clone task information
     *
     * @return error code
     */
    int GetFinishedCloneTask(
        const TaskIdType &taskId,
        TaskCloneInfo *taskCloneInfoOut);

    /**
     * @brief Restore clone task based on clone task information
     *
     * @param cloneInfo Clone task information
     *
     * @return error code
     */
    int RecoverCloneTaskInternal(const CloneInfo &cloneInfo);

    /**
     * @brief Restore and clear clone tasks based on clone task information
     *
     * @param cloneInfo Clone task information
     *
     * @return error code
     */
    int RecoverCleanTaskInternal(const CloneInfo &cloneInfo);

    /**
     *Task of building and pushing Lazy  @brief
     *
     * @param cloneInfo Clone task information
     * @param closure asynchronous callback entity
     *
     * @return error code
     */
    int BuildAndPushCloneOrRecoverLazyTask(
        CloneInfo cloneInfo,
        std::shared_ptr<CloneClosure> closure);

    /**
     * @brief Build and push non Lazy tasks
     *
     * @param cloneInfo Clone task information
     * @param closure asynchronous callback entity
     *
     * @return error code
     */
    int BuildAndPushCloneOrRecoverNotLazyTask(
        CloneInfo cloneInfo,
        std::shared_ptr<CloneClosure> closure);

 private:
    std::shared_ptr<DLockOpts> dlockOpts_;
    std::shared_ptr<DLock> dlock_;
    std::shared_ptr<NameLock> destFileLock_;
    std::shared_ptr<CloneTaskManager> cloneTaskMgr_;
    std::shared_ptr<CloneCore> cloneCore_;
    std::shared_ptr<CloneServiceManagerBackend> cloneServiceManagerBackend_;
};



}  // namespace snapshotcloneserver
}  // namespace curve

#endif  // SRC_SNAPSHOTCLONESERVER_CLONE_CLONE_SERVICE_MANAGER_H_
