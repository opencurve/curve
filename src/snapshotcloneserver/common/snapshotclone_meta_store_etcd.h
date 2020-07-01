/*
 * Project: curve
 * Created Date: Wed Jul 01 2020
 * Author: xuchaojie
 * Copyright (c) 2020 netease
 */

#ifndef SRC_SNAPSHOTCLONESERVER_COMMON_SNAPSHOTCLONE_META_STORE_ETCD_H_
#define SRC_SNAPSHOTCLONESERVER_COMMON_SNAPSHOTCLONE_META_STORE_ETCD_H_

#include <vector>
#include <memory>
#include <map>
#include <string>

#include "src/snapshotcloneserver/common/snapshotclone_meta_store.h"
#include "src/kvstorageclient/etcd_client.h"
#include "src/snapshotcloneserver/common/snapshotclonecodec.h"
#include "src/common/concurrent/concurrent.h"
#include "src/common/concurrent/rw_lock.h"

using ::curve::kvstorage::KVStorageClient;
using ::curve::common::RWLock;
using ::curve::common::ReadLockGuard;
using ::curve::common::WriteLockGuard;

namespace curve {
namespace snapshotcloneserver {

class SnapshotCloneMetaStoreEtcd : public SnapshotCloneMetaStore {
 public:
    SnapshotCloneMetaStoreEtcd(std::shared_ptr<KVStorageClient> client,
        std::shared_ptr<SnapshotCloneCodec> codec)
    : client_(client),
      codec_(codec) {}

    int Init();

    int AddSnapshot(const SnapshotInfo &info) override;

    int DeleteSnapshot(const UUID &uuid) override;

    int UpdateSnapshot(const SnapshotInfo &info) override;

    int GetSnapshotInfo(const UUID &uuid, SnapshotInfo *info) override;

    int GetSnapshotList(const std::string &filename,
                        std::vector<SnapshotInfo> *v) override;

    int GetSnapshotList(std::vector<SnapshotInfo> *list) override;

    uint32_t GetSnapshotCount() override;

    int AddCloneInfo(const CloneInfo &info) override;

    int DeleteCloneInfo(const std::string &uuid) override;

    int UpdateCloneInfo(const CloneInfo &info) override;

    int GetCloneInfo(const std::string &uuid, CloneInfo *info) override;

    int GetCloneInfoByFileName(
        const std::string &fileName, std::vector<CloneInfo> *list) override;

    int GetCloneInfoList(std::vector<CloneInfo> *list) override;

 private:
    /**
     * @brief 加载快照信息
     *
     * @return 0 加载成功/ -1 加载失败
     */
    int LoadSnapshotInfos();

    /**
     * @brief 加载克隆信息
     *
     * @return 0 加载成功/ -1 加载失败
     */
    int LoadCloneInfos();

 private:
    std::shared_ptr<KVStorageClient> client_;
    std::shared_ptr<SnapshotCloneCodec> codec_;

    // key is UUID, map 需要考虑并发保护
    std::map<UUID, SnapshotInfo> snapInfos_;
    // snap info lock
    RWLock snapInfos_mutex;
    // key is TaskIdType, map 需要考虑并发保护
    std::map<std::string, CloneInfo> cloneInfos_;
    // clone info map lock
    RWLock cloneInfos_lock_;
};

}  // namespace snapshotcloneserver
}  // namespace curve

#endif  // SRC_SNAPSHOTCLONESERVER_COMMON_SNAPSHOTCLONE_META_STORE_ETCD_H_
