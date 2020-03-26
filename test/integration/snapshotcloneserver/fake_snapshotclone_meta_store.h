/*
 * Project: curve
 * Created Date: Wed Dec 04 2019
 * Author: xuchaojie
 * Copyright (c) 2019 netease
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
    int Init(const SnapshotCloneMetaStoreOptions &options) override;
    int AddSnapshot(const SnapshotInfo &snapinfo) override;
    int DeleteSnapshot(const UUID &uuid) override;
    int UpdateSnapshot(const SnapshotInfo &snapinfo) override;
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
