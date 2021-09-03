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
#include "curvefs/proto/common.pb.h"
#include "curvefs/src/metaserver/dentry_manager.h"
#include "curvefs/src/metaserver/dentry_storage.h"
#include "curvefs/src/metaserver/inode_manager.h"
#include "curvefs/src/metaserver/inode_storage.h"

namespace curvefs {
namespace metaserver {
using curvefs::common::PartitionInfo;

class Partition {
 public:
    explicit Partition(const PartitionInfo& paritionInfo);

    // dentry
    MetaStatusCode CreateDentry(const Dentry& dentry);

    MetaStatusCode GetDentry(uint32_t fsId, uint64_t parentId,
                             const std::string& name, Dentry* dentry);

    MetaStatusCode DeleteDentry(uint32_t fsId, uint64_t parentId,
                                const std::string& name);

    MetaStatusCode ListDentry(uint32_t fsId, uint64_t dirId,
                              std::list<Dentry>* dentryList);

    // inode
    MetaStatusCode CreateInode(uint32_t fsId, uint64_t length, uint32_t uid,
                               uint32_t gid, uint32_t mode, FsFileType type,
                               const std::string& symlink, Inode* inode);
    MetaStatusCode CreateRootInode(uint32_t fsId, uint32_t uid, uint32_t gid,
                                   uint32_t mode);
    MetaStatusCode GetInode(uint32_t fsId, uint64_t inodeId, Inode* inode);

    MetaStatusCode DeleteInode(uint32_t fsId, uint64_t inodeId);

    MetaStatusCode UpdateInode(const Inode& inode);

    // TODO(huyao): delete version
    //  MetaStatusCode UpdateInodeVersion(uint32_t fsId, uint64_t inodeId,
    //                                    uint64_t* version);

    MetaStatusCode InsertInode(const Inode& inode);

    bool IsDeletable();

    bool IsInodeBelongs(uint32_t fsId, uint64_t inodeId);

    uint32_t GetPartitionId();

    PartitionInfo GetPartitionInfo();

    std::unordered_map<InodeKey, Inode, hashInode>* GetInodeContainer();

    std::unordered_map<DentryKey, Dentry, HashDentry>* GetDentryContainer();

    // get new inode id in partition range.
    // if no aviable inode id in this partiton ,return UINT64_MAX
    uint64_t GetNewInodeId();

 private:
    // std::atomic<uint64_t> nextInodeId_;
    std::shared_ptr<InodeStorage> inodeStorage_;
    std::shared_ptr<DentryStorage> dentryStorage_;
    std::shared_ptr<InodeManager> inodeManager_;
    std::shared_ptr<DentryManager> dentryManager_;
    // TODO(cw123) : add txmanager
    PartitionInfo partitionInfo_;
};
}  // namespace metaserver
}  // namespace curvefs
#endif  // CURVEFS_SRC_METASERVER_PARTITION_H_
