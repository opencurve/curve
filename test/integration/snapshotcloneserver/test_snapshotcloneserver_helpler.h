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
 * Created Date: Mon Oct 21 2019
 * Author: xuchaojie
 */

#ifndef TEST_INTEGRATION_SNAPSHOTCLONESERVER_TEST_SNAPSHOTCLONESERVER_HELPLER_H_
#define TEST_INTEGRATION_SNAPSHOTCLONESERVER_TEST_SNAPSHOTCLONESERVER_HELPLER_H_

#include <json/json.h>
#include <string>
#include <vector>

#include "src/client/libcurve_file.h"
#include "src/snapshotcloneserver/snapshot/snapshot_service_manager.h"
#include "src/snapshotcloneserver/clone/clone_service_manager.h"

namespace curve {
namespace snapshotcloneserver {

int SendRequest(const std::string &url, Json::Value *jsonObj);

int MakeSnapshot(
    const std::string &user,
    const std::string &fileName,
    const std::string &snapName,
    std::string *uuidOut);

int CancelSnapshot(
    const std::string &user,
    const std::string &fileName,
    const std::string &uuid);

int GetSnapshotInfo(
    const std::string &user,
    const std::string &fileName,
    const std::string &uuid,
    FileSnapshotInfo *info);

int ListFileSnapshotInfo(
    const std::string &user,
    const std::string &fileName,
    int limit,
    int offset,
    std::vector<FileSnapshotInfo> *infoVec);

int DeleteSnapshot(
    const std::string &user,
    const std::string &fileName,
    const std::string &uuid);

int CloneOrRecover(
    const std::string &action,
    const std::string &user,
    const std::string &src,
    const std::string &dst,
    bool lazy,
    std::string *uuidOut);

int Flatten(
    const std::string &user,
    const std::string &uuid);

int GetCloneTaskInfo(
    const std::string &user,
    const std::string &uuid,
    TaskCloneInfo *info);

int ListCloneTaskInfo(
    const std::string &user,
    int limit,
    int offset,
    std::vector<TaskCloneInfo> *infoVec);

int CleanCloneTask(
    const std::string &user,
    const std::string &uuid);

bool CheckSnapshotSuccess(
    const std::string &user,
    const std::string &file,
    const std::string &uuid);

int DeleteAndCheckSnapshotSuccess(
    const std::string &user,
    const std::string &file,
    const std::string &uuid);

bool CheckCloneOrRecoverSuccess(
    const std::string &user,
    const std::string &uuid,
    bool isClone);

bool WaitMetaInstalledSuccess(
    const std::string &user,
    const std::string &uuid,
    bool isClone);

}  // namespace snapshotcloneserver
}  // namespace curve

#endif  // TEST_INTEGRATION_SNAPSHOTCLONESERVER_TEST_SNAPSHOTCLONESERVER_HELPLER_H_
