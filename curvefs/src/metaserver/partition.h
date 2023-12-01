/*
 *  Copyright (c) 2021 NetEase Inc.
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
 * @Project: curve
 * @Date: 2021-08-30 19:48:38
 * @Author: chenwei
 */

#ifndef CURVEFS_SRC_METASERVER_PARTITION_H_
#define CURVEFS_SRC_METASERVER_PARTITION_H_
#include <cstdint>
#include <list>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "curvefs/proto/common.pb.h"
#include "curvefs/proto/metaserver.pb.h"
#include "curvefs/src/common/define.h"
#include "curvefs/src/metaserver/dentry_manager.h"
#include "curvefs/src/metaserver/dentry_storage.h"
#include "curvefs/src/metaserver/inode_manager.h"
#include "curvefs/src/metaserver/inode_storage.h"
#include "curvefs/src/metaserver/storage/iterator.h"
#include "curvefs/src/metaserver/trash_manager.h"

namespace curvefs {
namespace metaserver {
using curvefs::common::AppliedIndex;
using curvefs::common::PartitionInfo;
using curvefs::common::PartitionStatus;
using ::curvefs::metaserver::storage::Iterator;
using ::curvefs::metaserver::storage::KVStorage;
using S3ChunkInfoMap = google::protobuf::Map<uint64_t, S3ChunkInfoList>;

// skip ROOTINODEID and RECYCLEINODEID
constexpr uint64_t kMinPartitionStartId = ROOTINODEID + 2;

class Partition {
 public:
    Partition(PartitionInfo partition, std::shared_ptr<KVStorage> kvStorage,
              bool startCompact = true, bool startVolumeDeallocate = true);
    Partition() = default;
    bool Init();

    // dentry
    MetaStatusCode CreateDentry(const Dentry& dentry,
        const Time& tm, int64_t logIndex, TxLock* txLock = nullptr);

    MetaStatusCode LoadDentry(
        const DentryVec& vec, bool merge, int64_t logIndex);

    MetaStatusCode DeleteDentry(const Dentry& dentry,
        const Time& tm, int64_t logIndex, TxLock* txLock = nullptr);

    MetaStatusCode GetDentry(Dentry* dentry, TxLock* txLock = nullptr);

    MetaStatusCode ListDentry(const Dentry& dentry,
                              std::vector<Dentry>* dentrys, uint32_t limit,
                              bool onlyDir = false, TxLock* txLock = nullptr);

    void ClearDentry();

    MetaStatusCode HandleRenameTx(const std::vector<Dentry>& dentrys,
                                  int64_t logIndex);

    bool InsertPendingTx(const PrepareRenameTxRequest& pendingTx);

    bool FindPendingTx(PrepareRenameTxRequest* pendingTx);

    void SerializeRenameTx(const RenameTx& in, PrepareRenameTxRequest* out);

    MetaStatusCode PrewriteRenameTx(const std::vector<Dentry>& dentrys,
        const TxLock& txLock, int64_t logIndex, TxLock* out);

    MetaStatusCode CheckTxStatus(const std::string& primaryKey,
        uint64_t startTs, uint64_t curTimestamp, int64_t logIndex);

    MetaStatusCode ResolveTxLock(const Dentry& dentry, uint64_t startTs,
        uint64_t commitTs, int64_t logIndex);

    MetaStatusCode CommitTx(const std::vector<Dentry>& dentrys,
        uint64_t startTs, uint64_t commitTs, int64_t logIndex);

    // inode
    MetaStatusCode CreateInode(const InodeParam& param, Inode* inode,
                               int64_t logIndex);

    MetaStatusCode CreateRootInode(const InodeParam& param, int64_t logIndex);

    MetaStatusCode CreateManageInode(const InodeParam& param,
                                     ManageInodeType manageType, Inode* inode,
                                     int64_t logIndex);

    MetaStatusCode GetInode(uint32_t fsId, uint64_t inodeId, Inode* inode);

    void LoadDeletedInodes();

    MetaStatusCode GetInodeAttr(uint32_t fsId, uint64_t inodeId,
                                InodeAttr* attr);

    MetaStatusCode GetXAttr(uint32_t fsId, uint64_t inodeId, XAttr* xattr);

    MetaStatusCode DeleteInode(uint32_t fsId, uint64_t inodeId,
                               int64_t logIndex);

    MetaStatusCode UpdateInode(const UpdateInodeRequest& request,
                               int64_t logIndex);

    MetaStatusCode GetOrModifyS3ChunkInfo(uint32_t fsId, uint64_t inodeId,
                                          const S3ChunkInfoMap& map2add,
                                          const S3ChunkInfoMap& map2del,
                                          bool returnS3ChunkInfoMap,
                                          std::shared_ptr<Iterator>* iterator,
                                          int64_t logIndex);

    MetaStatusCode PaddingInodeS3ChunkInfo(int32_t fsId, uint64_t inodeId,
                                           S3ChunkInfoMap* m,
                                           uint64_t limit = 0);

    MetaStatusCode UpdateVolumeExtent(uint32_t fsId, uint64_t inodeId,
                                      const VolumeExtentSliceList& extents,
                                      int64_t logIndex);

    MetaStatusCode UpdateVolumeExtentSlice(uint32_t fsId, uint64_t inodeId,
                                           const VolumeExtentSlice& slice,
                                           int64_t logIndex);

    MetaStatusCode GetVolumeExtent(uint32_t fsId, uint64_t inodeId,
                                   const std::vector<uint64_t>& slices,
                                   VolumeExtentSliceList* extents);

    MetaStatusCode UpdateDeallocatableBlockGroup(
        const UpdateDeallocatableBlockGroupRequest& request, int64_t logIndex);

    virtual MetaStatusCode GetAllBlockGroup(
        std::vector<DeallocatableBlockGroup>* deallocatableBlockGroupVec);

    bool GetInodeIdList(std::list<uint64_t>* InodeIdList);

    // if partition has no inode or no dentry, it is deletable
    bool IsDeletable();

    // check if fsid matchs and inode range belongs to this partition
    bool IsInodeBelongs(uint32_t fsId, uint64_t inodeId) const;

    // check if fsid match this partition
    bool IsInodeBelongs(uint32_t fsId) const;

    virtual uint32_t GetPartitionId() const {
        return partitionInfo_.partitionid();
    }

    virtual uint32_t GetPoolId() const { return partitionInfo_.poolid(); }

    virtual uint32_t GetCopySetId() const { return partitionInfo_.copysetid(); }

    virtual uint32_t GetFsId() const { return partitionInfo_.fsid(); }

    PartitionInfo GetPartitionInfo();

    // get new inode id in partition range.
    // if no available inode id in this partiton ,return UINT64_MAX
    uint64_t GetNewInodeId();

    uint64_t GetInodeNum();

    uint64_t GetDentryNum();

    bool EmptyInodeStorage();

    void SetStatus(PartitionStatus status) {
        partitionInfo_.set_status(status);
    }

    PartitionStatus GetStatus() { return partitionInfo_.status(); }

    void StartS3Compact();

    void CancelS3Compact();

    void StartVolumeDeallocate();

    void CancelVolumeDeallocate();

    void SetVolumeDeallocate(uint64_t fsId, uint64_t blockGroupOffset);

    std::string GetInodeTablename();

    std::string GetDentryTablename();

    std::shared_ptr<Iterator> GetAllInode();

    std::shared_ptr<Iterator> GetAllDentry();

    std::shared_ptr<Iterator> GetAllS3ChunkInfoList();

    std::shared_ptr<Iterator> GetAllVolumeExtentList();

    bool Clear();

    void SetManageFlag(bool flag) { partitionInfo_.set_manageflag(flag); }

    bool GetManageFlag() {
        if (partitionInfo_.has_manageflag()) {
            return partitionInfo_.manageflag();
        } else {
            return false;
        }
    }

 private:
    std::shared_ptr<KVStorage> kvStorage_;
    std::shared_ptr<NameGenerator> nameGen_;

    std::shared_ptr<InodeStorage> inodeStorage_;
    std::shared_ptr<DentryStorage> dentryStorage_;

    std::shared_ptr<InodeManager> inodeManager_;
    std::shared_ptr<DentryManager> dentryManager_;
    std::shared_ptr<TxManager> txManager_;

    PartitionInfo partitionInfo_;
};
}  // namespace metaserver
}  // namespace curvefs
#endif  // CURVEFS_SRC_METASERVER_PARTITION_H_
