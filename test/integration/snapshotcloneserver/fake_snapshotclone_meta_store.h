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

#ifndef TEST_INTEGRATION_SNAPSHOTCLONESERVER_FAKE_SNAPSHOTCLONE_META_STORE_H_
#define TEST_INTEGRATION_SNAPSHOTCLONESERVER_FAKE_SNAPSHOTCLONE_META_STORE_H_

#include <vector>
#include <string>
#include <map>

#include "src/snapshotcloneserver/common/snapshotclone_meta_store.h"

namespace curve {
namespace snapshotcloneserver {

class FakeSnapshotCloneMetaStore : public SnapshotCloneMetaStore {
 public:
    int AddSnapshot(const SnapshotInfo &snapinfo) override;
    int DeleteSnapshot(const UUID &uuid) override;
    int UpdateSnapshot(const SnapshotInfo &snapinfo) override;
    int CASSnapshot(const UUID& uuid, CASFunc cas) override;
    int GetSnapshotInfo(const UUID &uuid, SnapshotInfo *info) override;
    int GetSnapshotList(const std::string &filename,
                        std::vector<SnapshotInfo> *v) override;
    int GetSnapshotList(std::vector<SnapshotInfo> *list) override;
    uint32_t GetSnapshotCount() override;

    int AddCloneInfo(const CloneInfo &cloneInfo) override;

    int DeleteCloneInfo(const std::string &taskID) override;

    int UpdateCloneInfo(const CloneInfo &cloneInfo) override;

    int GetCloneInfo(const std::string &taskID, CloneInfo *info) override;

    int GetCloneInfoByFileName(
        const std::string &fileName, std::vector<CloneInfo> *list) override;

    int GetCloneInfoList(std::vector<CloneInfo> *list) override;

 private:
    std::map<UUID, SnapshotInfo> snapInfos_;
    std::mutex snapInfos_mutex;

    std::map<std::string, CloneInfo> cloneInfos_;
    curve::common::RWLock cloneInfos_lock_;
};


}  // namespace snapshotcloneserver
}  // namespace curve

#endif  // TEST_INTEGRATION_SNAPSHOTCLONESERVER_FAKE_SNAPSHOTCLONE_META_STORE_H_
