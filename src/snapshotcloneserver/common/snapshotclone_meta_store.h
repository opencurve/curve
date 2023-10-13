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

/*************************************************************************
> File Name: snapshot_meta_store.h
> Author:
> Created Time: Fri Dec 14 18:25:30 2018
 ************************************************************************/

#ifndef SRC_SNAPSHOTCLONESERVER_COMMON_SNAPSHOTCLONE_META_STORE_H_
#define SRC_SNAPSHOTCLONESERVER_COMMON_SNAPSHOTCLONE_META_STORE_H_

#include <map>
#include <memory>
#include <mutex>  //NOLINT
#include <string>
#include <vector>

#include "src/common/concurrent/concurrent.h"
#include "src/common/snapshotclone/snapshotclone_define.h"
#include "src/snapshotcloneserver/common/config.h"
#include "src/snapshotcloneserver/common/snapshotclone_info.h"

namespace curve {
namespace snapshotcloneserver {

using CASFunc = std::function<SnapshotInfo*(SnapshotInfo*)>;

class SnapshotCloneMetaStore {
 public:
    SnapshotCloneMetaStore() {}
    virtual ~SnapshotCloneMetaStore() {}
    // Add a snapshot information record
    /**
     * Add a snapshot record to metastore
     * @param snapshot information structure
     * @return: 0 insertion successful/-1 insertion failed
     */
    virtual int AddSnapshot(const SnapshotInfo& snapinfo) = 0;
    /**
     * Delete a snapshot record from metastore
     * @param The uuid of the snapshot task, globally unique
     * @return 0 successfully deleted/-1 failed to delete
     */
    virtual int DeleteSnapshot(const UUID& uuid) = 0;
    /**
     * Update snapshot records
     * @param snapshot information structure
     * @return: 0 successfully updated/-1 failed to update
     */
    virtual int UpdateSnapshot(const SnapshotInfo& snapinfo) = 0;

    /**
     * @brief Compare and set snapshot
     * @param[in] uuid the uuid for snapshot
     * @param[in] cas the function for compare and set snapshot,
     *            return nullptr if not needed to set snapshot,
     *            else return the pointer of snapshot to set
     * @return 0 if set snapshot success or not needed to set snapshot,
     *         else return -1
     */
    virtual int CASSnapshot(const UUID& uuid, CASFunc cas) = 0;

    /**
     * Obtain snapshot information for the specified snapshot
     * @param uuid of snapshot
     * @param pointer to save snapshot information
     * @return 0 successfully obtained/-1 failed to obtain
     */
    virtual int GetSnapshotInfo(const UUID& uuid, SnapshotInfo* info) = 0;
    /**
     * Obtain a list of snapshot information for the specified file
     * @param file name
     * @param vector pointer to save snapshot information
     * @return 0 successfully obtained/-1 failed to obtain
     */
    virtual int GetSnapshotList(const std::string& filename,
                                std::vector<SnapshotInfo>* v) = 0;
    /**
     * Obtain a list of all snapshot information
     * @param vector pointer to save snapshot information
     * @return: 0 successfully obtained/-1 failed to obtain
     */
    virtual int GetSnapshotList(std::vector<SnapshotInfo>* list) = 0;

    /**
     * @brief Total number of snapshots taken
     *
     * @return Total number of snapshots
     */
    virtual uint32_t GetSnapshotCount() = 0;

    /**
     * @brief Insert a clone task record into metastore
     * @param clone records information
     * @return: 0 insertion successful/-1 insertion failed
     */
    virtual int AddCloneInfo(const CloneInfo& cloneInfo) = 0;
    /**
     * @brief Delete a clone task record from metastore
     * @param Task ID of clone task
     * @return: 0 successfully deleted/-1 failed to delete
     */
    virtual int DeleteCloneInfo(const std::string& taskID) = 0;
    /**
     * @brief Update a clone task record
     * @param clone records information
     * @return: 0 successfully updated/-1 failed to update
     */
    virtual int UpdateCloneInfo(const CloneInfo& cloneInfo) = 0;
    /**
     * @brief Get clone task information for the specified task ID
     * @param clone Task ID
     * @param[out] pointer to clone record information
     * @return: 0 successfully obtained/-1 failed to obtain
     */
    virtual int GetCloneInfo(const std::string& taskID, CloneInfo* info) = 0;

    /**
     * @brief Get clone task information for the specified file
     *
     * @param fileName File name
     * @param[out]  pointer to clone record information
     * @return: 0 successfully obtained/-1 failed to obtain
     */
    virtual int GetCloneInfoByFileName(const std::string& fileName,
                                       std::vector<CloneInfo>* list) = 0;

    /**
     * @brief Get a list of information for all clone tasks
     * @param[out] just wants to clone the task vector pointer
     * @return: 0 successfully obtained/-1 failed to obtain
     */
    virtual int GetCloneInfoList(std::vector<CloneInfo>* list) = 0;
};

}  // namespace snapshotcloneserver
}  // namespace curve
#endif  // SRC_SNAPSHOTCLONESERVER_COMMON_SNAPSHOTCLONE_META_STORE_H_
