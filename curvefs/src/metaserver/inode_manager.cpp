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
 * Project: curve
 * Created Date: 2021-05-19
 * Author: chenwei
 */
#include <glog/logging.h>
#include "curvefs/src/metaserver/inode_manager.h"
#include "src/common/timeutility.h"

namespace curvefs {
namespace metaserver {
MetaStatusCode InodeManager::CreateInode(uint32_t fsId, uint64_t length,
                                uint32_t uid, uint32_t gid, uint32_t mode,
                                FsFileType type, std::string symlink,
                                Inode *newInode) {
    if (type == FsFileType::TYPE_SYM_LINK
        && symlink.empty()) {
        return MetaStatusCode::SYM_LINK_EMPTY;
    }

    // 1、generate inode
    Inode inode;
    inode.set_inodeid(GetNextId());
    inode.set_fsid(fsId);
    inode.set_length(length);
    inode.set_uid(uid);
    inode.set_gid(gid);
    inode.set_mode(mode);
    inode.set_type(type);
    inode.set_mtime(curve::common::TimeUtility::GetTimeofDayMs());
    inode.set_atime(curve::common::TimeUtility::GetTimeofDayMs());
    inode.set_ctime(curve::common::TimeUtility::GetTimeofDayMs());
    inode.set_nlink(0);
    if (type == FsFileType::TYPE_SYM_LINK) {
        inode.set_symlink(symlink);
    }
    // 2、insert inode
    MetaStatusCode status = inodeStorage_->Insert(inode);
    if (status == MetaStatusCode::OK) {
        newInode->CopyFrom(inode);
    }
    return status;
}

uint64_t InodeManager::GetNextId() {
    return nextInodeId_.fetch_add(1, std::memory_order_relaxed);
}

MetaStatusCode InodeManager::GetInode(uint32_t fsId, uint64_t inodeId,
                                        Inode *inode) {
    MetaStatusCode status = inodeStorage_->Get(InodeKey(fsId, inodeId),
                                    inode);
    return status;
}

MetaStatusCode InodeManager::DeleteInode(uint32_t fsId, uint64_t inodeId) {
    MetaStatusCode status = inodeStorage_->Delete(InodeKey(fsId, inodeId));
    return status;
}

MetaStatusCode InodeManager::UpdateInode(const Inode &inode) {
    MetaStatusCode status = inodeStorage_->Update(inode);
    return status;
}
}  // namespace metaserver
}  // namespace curvefs
