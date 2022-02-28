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
#include <list>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>
#include "curvefs/proto/common.pb.h"
#include "curvefs/src/common/define.h"
#include "curvefs/src/metaserver/dentry_manager.h"
#include "curvefs/src/metaserver/dentry_storage.h"
#include "curvefs/src/metaserver/inode_manager.h"
#include "curvefs/src/metaserver/inode_storage.h"
#include "curvefs/src/metaserver/s3compact.h"
#include "curvefs/src/metaserver/trash_manager.h"
#include "curvefs/src/metaserver/storage/iterator.h"

namespace curvefs {
namespace metaserver {
using curvefs::common::PartitionInfo;
using curvefs::common::PartitionStatus;
using ::curvefs::metaserver::storage::KVStorage;
using ::curvefs::metaserver::storage::Iterator;

constexpr uint64_t kMinPartitionStartId = ROOTINODEID + 1;

class Partition {
 public:
    Partition(const PartitionInfo& paritionInfo,
              std::shared_ptr<KVStorage> kvStorage);

    ~Partition() {}

    // dentry
    MetaStatusCode CreateDentry(const Dentry& dentry, bool isLoadding = false);

    MetaStatusCode DeleteDentry(const Dentry& dentry);

    MetaStatusCode GetDentry(Dentry* dentry);

    MetaStatusCode ListDentry(const Dentry& dentry,
                              std::vector<Dentry>* dentrys,
                              uint32_t limit,
                              bool onlyDir = false);

    void ClearDentry();

    MetaStatusCode HandleRenameTx(const std::vector<Dentry>& dentrys);

    bool InsertPendingTx(const PrepareRenameTxRequest& pendingTx);

    bool FindPendingTx(PrepareRenameTxRequest* pendingTx);

    // inode
    MetaStatusCode CreateInode(uint32_t fsId, uint64_t length, uint32_t uid,
                               uint32_t gid, uint32_t mode, FsFileType type,
                               const std::string& symlink, uint64_t rdev,
                               Inode* inode);
    MetaStatusCode CreateRootInode(uint32_t fsId, uint32_t uid, uint32_t gid,
                                   uint32_t mode);
    MetaStatusCode GetInode(uint32_t fsId, uint64_t inodeId, Inode* inode);

    MetaStatusCode GetInodeAttr(uint32_t fsId, uint64_t inodeId,
                                InodeAttr* attr);

    MetaStatusCode GetXAttr(uint32_t fsId, uint64_t inodeId, XAttr* xattr);

    MetaStatusCode DeleteInode(uint32_t fsId, uint64_t inodeId);

    MetaStatusCode UpdateInode(const UpdateInodeRequest& request);

    MetaStatusCode GetOrModifyS3ChunkInfo(
        uint32_t fsId, uint64_t inodeId,
        const google::protobuf::Map<uint64_t, S3ChunkInfoList>& s3ChunkInfoAdd,
        const google::protobuf::Map<uint64_t, S3ChunkInfoList>&
            s3ChunkInfoRemove,
        bool returnS3ChunkInfoMap,
        google::protobuf::Map<uint64_t, S3ChunkInfoList>* out,
        bool fromS3Compaction);

    MetaStatusCode InsertInode(const Inode& inode);

    void GetInodeIdList(std::list<uint64_t>* InodeIdList);

    // if patition has no inode or no dentry, it is deletable
    bool IsDeletable();

    // check if fsid matchs and inode range belongs to this partition
    bool IsInodeBelongs(uint32_t fsId, uint64_t inodeId);

    // check if fsid match this partition
    bool IsInodeBelongs(uint32_t fsId);

    uint32_t GetPartitionId();

    uint32_t GetPoolId() { return partitionInfo_.poolid(); }

    uint32_t GetCopySetId() { return partitionInfo_.copysetid(); }

    uint32_t GetFsId() { return partitionInfo_.fsid(); }

    PartitionInfo GetPartitionInfo();

    std::shared_ptr<Iterator> GetAllInode();

    std::shared_ptr<Iterator> GetAllDentry();

    bool ClearAllInode();

    bool ClearAllDentry();

    // get new inode id in partition range.
    // if no available inode id in this partiton ,return UINT64_MAX
    uint64_t GetNewInodeId();

    uint32_t GetInodeNum();

    uint32_t GetDentryNum();

    void SetStatus(PartitionStatus status) {
        partitionInfo_.set_status(status);
    }

    PartitionStatus GetStatus() { return partitionInfo_.status(); }

    void ClearS3Compact() { s3compact_ = nullptr; }

    std::string GetStorageTablename();

 private:
    std::shared_ptr<InodeStorage> inodeStorage_;
    std::shared_ptr<DentryStorage> dentryStorage_;
    std::shared_ptr<InodeManager> inodeManager_;
    std::shared_ptr<TrashImpl> trash_;
    std::shared_ptr<DentryManager> dentryManager_;
    std::shared_ptr<TxManager> txManager_;

    PartitionInfo partitionInfo_;
    std::shared_ptr<S3Compact> s3compact_;
};
}  // namespace metaserver
}  // namespace curvefs
#endif  // CURVEFS_SRC_METASERVER_PARTITION_H_
