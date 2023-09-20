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

#include <vector>
#include <string>
#include <map>
#include <memory>
#include <mutex> //NOLINT

#include "src/common/snapshotclone/snapshotclone_define.h"
#include "src/snapshotcloneserver/common/config.h"
#include "src/common/concurrent/concurrent.h"
#include "src/snapshotcloneserver/common/snapshotclone_info.h"

namespace curve {
namespace snapshotcloneserver {

using CASFunc = std::function<SnapshotInfo*(SnapshotInfo*)>;

class SnapshotCloneMetaStore {
 public:
    SnapshotCloneMetaStore() {}
    virtual ~SnapshotCloneMetaStore() {}
    // 添加一条快照信息记录
    /**
     * 添加一条快照记录到metastore中
     * @param 快照信息结构体
     * @return: 0 插入成功/ -1 插入失败
     */
    virtual int AddSnapshot(const SnapshotInfo &snapinfo) = 0;
    /**
     * 从metastore删除一条快照记录
     * @param 快照任务的uuid，全局唯一
     * @return 0 删除成功/ -1 删除失败
     */
    virtual int DeleteSnapshot(const UUID &uuid) = 0;
    /**
     * 更新快照记录
     * @param 快照信息结构体
     * @return: 0 更新成功/ -1 更新失败
     */
    virtual int UpdateSnapshot(const SnapshotInfo &snapinfo) = 0;

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
     * 获取指定快照的快照信息
     * @param 快照的uuid
     * @param 保存快照信息的指针
     * @return 0 获取成功/ -1 获取失败
     */
    virtual int GetSnapshotInfo(const UUID &uuid, SnapshotInfo *info) = 0;

    virtual int GetSnapshotInfo(
        const std::string &file, const std::string &snapshotName,
        SnapshotInfo *info) = 0;

    /**
     * 获取指定文件的快照信息列表
     * @param 文件名
     * @param 保存快照信息的vector指针
     * @return 0 获取成功/ -1 获取失败
     */
    virtual int GetSnapshotList(const std::string &filename,
                                std::vector<SnapshotInfo> *v) = 0;
    /**
     * 获取全部的快照信息列表
     * @param 保存快照信息的vector指针
     * @return: 0 获取成功/ -1 获取失败
     */
    virtual int GetSnapshotList(std::vector<SnapshotInfo> *list) = 0;

    /**
     * @brief 获取快照总数
     *
     * @return 快照总数
     */
    virtual uint32_t GetSnapshotCount() = 0;

    /**
     * @brief 插入一条clone任务记录到metastore
     * @param clone记录信息
     * @return: 0 插入成功/ -1 插入失败
     */
    virtual int AddCloneInfo(const CloneInfo &cloneInfo) = 0;
    /**
     * @brief 从metastore删除一条clone任务记录
     * @param clone任务的任务id
     * @return: 0 删除成功/ -1 删除失败
     */
    virtual int DeleteCloneInfo(const std::string &taskID) = 0;
    /**
     * @brief 更新一条clone任务记录
     * @param clone记录信息
     * @return: 0 更新成功/ -1 更新失败
     */
    virtual int UpdateCloneInfo(const CloneInfo &cloneInfo) = 0;
    /**
     * @brief 获取指定task id的clone任务信息
     * @param clone任务id
     * @param[out] clone记录信息的指针
     * @return: 0 获取成功/ -1 获取失败
     */
    virtual int GetCloneInfo(const std::string &taskID, CloneInfo *info) = 0;

    /**
     * @brief 获取指定文件的clone任务信息
     *
     * @param fileName 文件名
     * @param[out] clone记录信息的指针
     * @return: 0 获取成功/ -1 获取失败
     */
    virtual int GetCloneInfoByFileName(
        const std::string &fileName, std::vector<CloneInfo> *list) = 0;

    /**
     * @brief 获取所有clone任务的信息列表
     * @param[out] 只想clone任务vector指针
     * @return: 0 获取成功/ -1 获取失败
     */
    virtual int GetCloneInfoList(std::vector<CloneInfo> *list) = 0;
};

}  // namespace snapshotcloneserver
}  // namespace curve
#endif  // SRC_SNAPSHOTCLONESERVER_COMMON_SNAPSHOTCLONE_META_STORE_H_
