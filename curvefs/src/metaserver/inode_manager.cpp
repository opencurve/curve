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

#include "curvefs/src/metaserver/inode_manager.h"
#include <glog/logging.h>
#include "curvefs/src/common/define.h"
#include "src/common/timeutility.h"

using ::curve::common::TimeUtility;

namespace curvefs {
namespace metaserver {
MetaStatusCode InodeManager::CreateInode(uint32_t fsId, uint64_t inodeId,
                                         uint64_t length, uint32_t uid,
                                         uint32_t gid, uint32_t mode,
                                         FsFileType type,
                                         const std::string &symlink,
                                         Inode *newInode) {
    VLOG(1) << "CreateInode, fsId = " << fsId << ", length = " << length
            << ", uid = " << uid << ", gid = " << gid << ", mode = " << mode
            << ", type =" << FsFileType_Name(type) << ", symlink = " << symlink;
    if (type == FsFileType::TYPE_SYM_LINK && symlink.empty()) {
        return MetaStatusCode::SYM_LINK_EMPTY;
    }

    // 1. generate inode
    Inode inode;
    GenerateInodeInternal(inodeId, fsId, length, uid, gid, mode, type,
                          &inode);
    if (type == FsFileType::TYPE_SYM_LINK) {
        inode.set_symlink(symlink);
    }

    // 2. insert inode
    MetaStatusCode ret = inodeStorage_->Insert(inode);
    if (ret != MetaStatusCode::OK) {
        LOG(ERROR) << "CreateInode fail, fsId = " << fsId
                   << ", length = " << length << ", uid = " << uid
                   << ", gid = " << gid << ", mode = " << mode
                   << ", type =" << FsFileType_Name(type)
                   << ", symlink = " << symlink
                   << ", ret = " << MetaStatusCode_Name(ret);
        return ret;
    }

    newInode->CopyFrom(inode);
    VLOG(1) << "CreateInode success, fsId = " << fsId << ", length = " << length
            << ", uid = " << uid << ", gid = " << gid << ", mode = " << mode
            << ", type =" << FsFileType_Name(type) << ", symlink = " << symlink
            << " ," << inode.ShortDebugString();

    return MetaStatusCode::OK;
}

MetaStatusCode InodeManager::CreateRootInode(uint32_t fsId, uint32_t uid,
                                             uint32_t gid, uint32_t mode) {
    VLOG(1) << "CreateRootInode, fsId = " << fsId << ", uid = " << uid
            << ", gid = " << gid << ", mode = " << mode;

    // 1. generate inode
    Inode inode;
    uint64_t length = 0;
    GenerateInodeInternal(ROOTINODEID, fsId, length, uid, gid, mode,
                          FsFileType::TYPE_DIRECTORY, &inode);
    // 2. insert inode
    MetaStatusCode ret = inodeStorage_->Insert(inode);
    if (ret != MetaStatusCode::OK) {
        LOG(ERROR) << "CreateRootInode fail, fsId = " << fsId
                   << ", uid = " << uid << ", gid = " << gid
                   << ", mode = " << mode
                   << ", ret = " << MetaStatusCode_Name(ret);
        return ret;
    }

    VLOG(1) << "CreateRootInode success, inode: " << inode.ShortDebugString();
    return MetaStatusCode::OK;
}

void InodeManager::GenerateInodeInternal(uint64_t inodeId, uint32_t fsId,
                                         uint64_t length, uint32_t uid,
                                         uint32_t gid, uint32_t mode,
                                         FsFileType type, Inode *inode) {
    inode->set_inodeid(inodeId);
    inode->set_fsid(fsId);
    inode->set_length(length);
    inode->set_uid(uid);
    inode->set_gid(gid);
    inode->set_mode(mode);
    inode->set_type(type);
    uint64_t time = curve::common::TimeUtility::GetTimeofDaySec();
    inode->set_mtime(time);
    inode->set_atime(time);
    inode->set_ctime(time);
    inode->set_openflag(false);
    if (FsFileType::TYPE_DIRECTORY == type) {
        inode->set_nlink(2);
    } else {
        inode->set_nlink(1);
    }
    return;
}

MetaStatusCode InodeManager::GetInode(uint32_t fsId, uint64_t inodeId,
                                      Inode *inode) {
    VLOG(1) << "GetInode, fsId = " << fsId << ", inodeId = " << inodeId;
    MetaStatusCode ret = inodeStorage_->Get(InodeKey(fsId, inodeId), inode);
    if (ret != MetaStatusCode::OK) {
        LOG(ERROR) << "GetInode fail, fsId = " << fsId
                   << ", inodeId = " << inodeId
                   << ", ret = " << MetaStatusCode_Name(ret);
        return ret;
    }

    VLOG(1) << "GetInode success, fsId = " << fsId << ", inodeId = " << inodeId
            << ", " << inode->ShortDebugString();

    return MetaStatusCode::OK;
}

MetaStatusCode InodeManager::DeleteInode(uint32_t fsId, uint64_t inodeId) {
    VLOG(1) << "DeleteInode, fsId = " << fsId << ", inodeId = " << inodeId;
    MetaStatusCode ret = inodeStorage_->Delete(InodeKey(fsId, inodeId));
    if (ret != MetaStatusCode::OK) {
        LOG(ERROR) << "DeleteInode fail, fsId = " << fsId
                   << ", inodeId = " << inodeId
                   << ", ret = " << MetaStatusCode_Name(ret);
        return ret;
    }

    VLOG(1) << "DeleteInode success, fsId = " << fsId
            << ", inodeId = " << inodeId;
    return MetaStatusCode::OK;
}

MetaStatusCode InodeManager::UpdateInode(const Inode &inode) {
    VLOG(1) << "UpdateInode, " << inode.ShortDebugString();

    Inode old;
    MetaStatusCode ret = inodeStorage_->Get(
        InodeKey(inode.fsid(), inode.inodeid()), &old);
    if (ret != MetaStatusCode::OK) {
        LOG(ERROR) << "GetInode fail, " << inode.ShortDebugString()
                   << ", ret = " << MetaStatusCode_Name(ret);
        return ret;
    }

    if (old.nlink() != 0 && inode.nlink() == 0) {
        uint32_t now = TimeUtility::GetTimeofDaySec();
        const_cast<Inode&>(inode).set_dtime(now);
    }

    ret = inodeStorage_->Update(inode);
    if (ret != MetaStatusCode::OK) {
        LOG(ERROR) << "UpdateInode fail, " << inode.ShortDebugString()
                   << ", ret = " << MetaStatusCode_Name(ret);
        return ret;
    }

    if (old.nlink() != 0 && inode.nlink() == 0) {
        trash_->Add(inode.fsid(), inode.inodeid(), inode.dtime());
    }

    VLOG(1) << "UpdateInode success, " << inode.ShortDebugString();
    return MetaStatusCode::OK;
}

MetaStatusCode InodeManager::InsertInode(const Inode &inode) {
    VLOG(1) << "InsertInode, " << inode.ShortDebugString();

    // 2. insert inode
    MetaStatusCode ret = inodeStorage_->Insert(inode);
    if (ret != MetaStatusCode::OK) {
        LOG(ERROR) << "InsertInode fail, " << inode.ShortDebugString()
                   << ", ret = " << MetaStatusCode_Name(ret);
        return ret;
    }

    if (inode.nlink() == 0) {
        trash_->Add(inode.fsid(), inode.inodeid(), inode.dtime());
    }

    return MetaStatusCode::OK;
}
}  // namespace metaserver
}  // namespace curvefs
