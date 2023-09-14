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
 * Created Date: Fri Dec 14 2018
 * Author: xuchaojie
 */

#ifndef SRC_SNAPSHOTCLONESERVER_SNAPSHOT_SNAPSHOT_SERVICE_MANAGER_H_
#define SRC_SNAPSHOTCLONESERVER_SNAPSHOT_SNAPSHOT_SERVICE_MANAGER_H_

#include <memory>
#include <string>
#include <vector>

#include "src/snapshotcloneserver/snapshot/snapshot_core.h"
#include "src/snapshotcloneserver/snapshot/snapshot_task.h"
#include "src/snapshotcloneserver/snapshot/snapshot_task_manager.h"
#include "src/common/snapshotclone/snapshotclone_define.h"
#include "src/snapshotcloneserver/common/config.h"
#include "json/json.h"

namespace curve {
namespace snapshotcloneserver {

/**
 * @brief file single snapshot information
 */
class FileSnapshotInfo {
 public:
    FileSnapshotInfo() = default;

     /**
      * @brief constructor
      *
      * @param snapInfo snapshot information
      * @param snapProgress snapshot completion percentage
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
    // Snapshot Information
    SnapshotInfo snapInfo_;
    // Snapshot processing progress percentage
    uint32_t snapProgress_;
};

class SnapshotFilterCondition {
 public:
    SnapshotFilterCondition()
                   : uuid_(nullptr),
                    file_(nullptr),
                    user_(nullptr),
                    status_(nullptr) {}

    SnapshotFilterCondition(const std::string *uuid, const std::string *file,
                        const std::string *user,
                        const std::string *status)
                   : uuid_(uuid),
                    file_(file),
                    user_(user),
                    status_(status) {}
    bool IsMatchCondition(const SnapshotInfo &snapInfo);

    void SetUuid(const std::string *uuid) {
        uuid_ = uuid;
    }

    void SetFile(const std::string *file) {
        file_ = file;
    }

    void SetUser(const std::string *user) {
        user_ = user;
    }

    void SetStatus(const std::string *status) {
        status_ = status;
    }


 private:
    const std::string *uuid_;
    const std::string *file_;
    const std::string *user_;
    const std::string *status_;
};

class SnapshotServiceManager {
 public:
     /**
      * @brief constructor
      *
      * @param taskMgr snapshot task management class object
      * @param core snapshot core module
      */
    SnapshotServiceManager(
        std::shared_ptr<SnapshotTaskManager> taskMgr,
        std::shared_ptr<SnapshotCore> core)
          : taskMgr_(taskMgr),
            core_(core) {}

    virtual ~SnapshotServiceManager() {}

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
     * @brief Create snapshot service
     *
     * @param file file name
     * @param user The user to whom the file belongs
     * @param snapshotName SnapshotName
     * @param uuid Snapshot uuid
     *
     * @return error code
     */
    virtual int CreateSnapshot(const std::string &file,
        const std::string &user,
        const std::string &snapshotName,
        UUID *uuid);

    /**
     * @brief Delete snapshot service
     *
     * @param uuid Snapshot uuid
     * @param user The user of the snapshot file
     * @param file The file name of the file to which the snapshot belongs
     *
     * @return error code
     */
    virtual int DeleteSnapshot(const UUID &uuid,
        const std::string &user,
        const std::string &file);

    /**
     * @brief Cancel snapshot service
     *
     * @param uuid The uuid of the snapshot
     * @param user snapshot user
     * @param file The file name of the file to which the snapshot belongs
     *
     * @return error code
     */
    virtual int CancelSnapshot(const UUID &uuid,
        const std::string &user,
        const std::string &file);

    /**
     * @brief Gets the snapshot information service interface for files
     *
     * @param file file name
     * @param user username
     * @param info snapshot information list
     *
     * @return error code
     */
    virtual int GetFileSnapshotInfo(const std::string &file,
        const std::string &user,
        std::vector<FileSnapshotInfo> *info);

    /**
     * @brief Obtain snapshot information of the file based on the ID
     *
     * @param file file name
     * @param user username
     * @param uuid SnapshotId
     * @param info snapshot information list
     *
     * @return error code
     */
    virtual int GetFileSnapshotInfoById(const std::string &file,
        const std::string &user,
        const UUID &uuid,
        std::vector<FileSnapshotInfo> *info);

    /**
     * @brief Get snapshot list
     *
     * @param filter filtering conditions
     * @param info snapshot information list
     *
     * @return error code
     */
    virtual int GetSnapshotListByFilter(const SnapshotFilterCondition &filter,
                    std::vector<FileSnapshotInfo> *info);

    /**
     * @brief Restore Snapshot Task Interface
     *
     * @return error code
     */
    virtual int RecoverSnapshotTask();

 private:
    /**
     * @brief Obtain snapshot task information based on snapshot information
     *
     * @param snapInfos snapshot information
     * @param user username
     * @param[out] info snapshot task information
     *
     * @return error code
     */
    int GetFileSnapshotInfoInner(
        std::vector<SnapshotInfo> snapInfos,
        const std::string &user,
        std::vector<FileSnapshotInfo> *info);

    /**
     * @brief Obtain snapshot task information based on snapshot information
     *
     * @param snapInfos snapshot information
     * @param filter filtering conditions
     * @param[out] info snapshot task information
     *
     * @return error code
     */
    int GetSnapshotListInner(
        std::vector<SnapshotInfo> snapInfos,
        SnapshotFilterCondition filter,
        std::vector<FileSnapshotInfo> *info);

 private:
    // Snapshot Task Management Class Object
    std::shared_ptr<SnapshotTaskManager> taskMgr_;
    // Snapshot Core Module
    std::shared_ptr<SnapshotCore> core_;
};

}  // namespace snapshotcloneserver
}  // namespace curve

#endif  // SRC_SNAPSHOTCLONESERVER_SNAPSHOT_SNAPSHOT_SERVICE_MANAGER_H_
