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
 * Created Date: Wed Dec 04 2019
 * Author: xuchaojie
 */

#include <fiu-control.h>
#include <fiu.h>
#include <string>
#include <vector>

#include "test/integration/snapshotcloneserver/fake_snapshotclone_meta_store.h"

namespace curve {
namespace snapshotcloneserver {

int FakeSnapshotCloneMetaStore::AddSnapshot(const SnapshotInfo &info) {
    fiu_return_on(
        "test/integration/snapshotcloneserver/FakeSnapshotCloneMetaStore.AddSnapshot", -1);  // NOLINT
    std::lock_guard<std::mutex> guard(snapInfos_mutex);
    snapInfos_.emplace(info.GetUuid(), info);
    return 0;
}

int FakeSnapshotCloneMetaStore::DeleteSnapshot(const UUID &uuid) {
    fiu_return_on(
        "test/integration/snapshotcloneserver/FakeSnapshotCloneMetaStore.DeleteSnapshot", -1);  // NOLINT
    std::lock_guard<std::mutex> guard(snapInfos_mutex);
    auto search = snapInfos_.find(uuid);
    if (search != snapInfos_.end()) {
        snapInfos_.erase(search);
    }
    return 0;
}

int FakeSnapshotCloneMetaStore::UpdateSnapshot(const SnapshotInfo &info) {
    fiu_return_on(
        "test/integration/snapshotcloneserver/FakeSnapshotCloneMetaStore.UpdateSnapshot", -1);  // NOLINT
    std::lock_guard<std::mutex> guard(snapInfos_mutex);
    auto search = snapInfos_.find(info.GetUuid());
    if (search != snapInfos_.end()) {
        search->second = info;
    } else {
        return -1;
    }
    return 0;
}

int FakeSnapshotCloneMetaStore::GetSnapshotInfo(
    const UUID &uuid, SnapshotInfo *info) {
    std::lock_guard<std::mutex> guard(snapInfos_mutex);
    auto search = snapInfos_.find(uuid);
    if (search != snapInfos_.end()) {
        *info = search->second;
        return 0;
    }
    return -1;
}

int FakeSnapshotCloneMetaStore::GetSnapshotList(const std::string &name,
        std::vector<SnapshotInfo> *v) {
    std::lock_guard<std::mutex> guard(snapInfos_mutex);
    for (auto it = snapInfos_.begin();
             it != snapInfos_.end();
             it++) {
        if (name.compare(it->second.GetFileName()) == 0) {
            v->push_back(it->second);
        }
    }
    if (v->size() != 0) {
        return 0;
    }
    return -1;
}

int FakeSnapshotCloneMetaStore::GetSnapshotList(
    std::vector<SnapshotInfo> *list) {
    std::lock_guard<std::mutex> guard(snapInfos_mutex);
    for (auto it = snapInfos_.begin();
              it != snapInfos_.end();
              it++) {
       list->push_back(it->second);
    }
    return 0;
}

uint32_t FakeSnapshotCloneMetaStore::GetSnapshotCount() {
    return snapInfos_.size();
}

int FakeSnapshotCloneMetaStore::AddCloneInfo(const CloneInfo &info) {
    fiu_return_on(
        "test/integration/snapshotcloneserver/FakeSnapshotCloneMetaStore.AddCloneInfo", -1);  // NOLINT
    curve::common::WriteLockGuard guard(cloneInfos_lock_);
    cloneInfos_.emplace(info.GetTaskId(), info);
    return 0;
}

int FakeSnapshotCloneMetaStore::DeleteCloneInfo(const std::string &taskID) {
    curve::common::WriteLockGuard guard(cloneInfos_lock_);
    auto search = cloneInfos_.find(taskID);
    if (search != cloneInfos_.end()) {
        cloneInfos_.erase(search);
    }
    return 0;
}

int FakeSnapshotCloneMetaStore::UpdateCloneInfo(const CloneInfo &info) {
    fiu_return_on(
        "test/integration/snapshotcloneserver/FakeSnapshotCloneMetaStore.UpdateCloneInfo", -1);  // NOLINT
    curve::common::WriteLockGuard guard(cloneInfos_lock_);
    auto search = cloneInfos_.find(info.GetTaskId());
    if (search != cloneInfos_.end()) {
        search->second = info;
    } else {
        return -1;
    }
    return 0;
}

int FakeSnapshotCloneMetaStore::GetCloneInfo(
    const std::string &taskID, CloneInfo *info) {
    curve::common::ReadLockGuard guard(cloneInfos_lock_);
    auto search = cloneInfos_.find(taskID);
    if (search != cloneInfos_.end()) {
        *info = search->second;
        return 0;
    }
    return -1;
}

int FakeSnapshotCloneMetaStore::GetCloneInfoByFileName(
        const std::string &fileName, std::vector<CloneInfo> *list) {
    return -1;
}

int FakeSnapshotCloneMetaStore::GetCloneInfoList(
    std::vector<CloneInfo> *v) {
    curve::common::ReadLockGuard guard(cloneInfos_lock_);
    for (auto it = cloneInfos_.begin();
             it != cloneInfos_.end();
             it++) {
            v->push_back(it->second);
    }
    if (v->size() != 0) {
        return 0;
    }
    return -1;
}



}  // namespace snapshotcloneserver
}  // namespace curve
