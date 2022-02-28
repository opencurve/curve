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
#include <google/protobuf/util/message_differencer.h>
#include <list>

#include "curvefs/src/common/define.h"
#include "src/common/timeutility.h"

using ::curve::common::TimeUtility;
using ::curve::common::NameLockGuard;
using ::google::protobuf::util::MessageDifferencer;

namespace curvefs {
namespace metaserver {
MetaStatusCode InodeManager::CreateInode(uint32_t fsId, uint64_t inodeId,
                                         uint64_t length, uint32_t uid,
                                         uint32_t gid, uint32_t mode,
                                         FsFileType type,
                                         const std::string &symlink,
                                         uint64_t rdev,
                                         Inode *newInode) {
    VLOG(1) << "CreateInode, fsId = " << fsId << ", length = " << length
            << ", uid = " << uid << ", gid = " << gid << ", mode = " << mode
            << ", type =" << FsFileType_Name(type) << ", symlink = " << symlink
            << ", rdev = " << rdev;
    if (type == FsFileType::TYPE_SYM_LINK && symlink.empty()) {
        return MetaStatusCode::SYM_LINK_EMPTY;
    }

    // 1. generate inode
    Inode inode;
    GenerateInodeInternal(inodeId, fsId, length, uid, gid, mode, type, rdev,
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
            << ", rdev = " << rdev
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
                          FsFileType::TYPE_DIRECTORY, 0, &inode);

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
                                         FsFileType type, uint64_t rdev,
                                         Inode *inode) {
    inode->set_inodeid(inodeId);
    inode->set_fsid(fsId);
    inode->set_length(length);
    inode->set_uid(uid);
    inode->set_gid(gid);
    inode->set_mode(mode);
    inode->set_type(type);
    inode->set_rdev(rdev);

    struct timespec now;
    clock_gettime(CLOCK_REALTIME, &now);
    inode->set_mtime(now.tv_sec);
    inode->set_mtime_ns(now.tv_nsec);
    inode->set_atime(now.tv_sec);
    inode->set_atime_ns(now.tv_nsec);
    inode->set_ctime(now.tv_sec);
    inode->set_ctime_ns(now.tv_nsec);

    inode->set_openmpcount(0);
    if (FsFileType::TYPE_DIRECTORY == type) {
        inode->set_nlink(2);
        // set summary xattr
        inode->mutable_xattr()->insert({XATTRFILES, "0"});
        inode->mutable_xattr()->insert({XATTRSUBDIRS, "0"});
        inode->mutable_xattr()->insert({XATTRENTRIES, "0"});
        inode->mutable_xattr()->insert({XATTRFBYTES, "0"});
    } else {
        inode->set_nlink(1);
    }
    return;
}

MetaStatusCode InodeManager::GetInode(uint32_t fsId, uint64_t inodeId,
                                      Inode *inode) {
    VLOG(1) << "GetInode, fsId = " << fsId << ", inodeId = " << inodeId;
    NameLockGuard lg(inodeLock_, GetInodeLockName(fsId, inodeId));
    MetaStatusCode ret = inodeStorage_->Get(Key4Inode(fsId, inodeId), inode);
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

MetaStatusCode InodeManager::GetInodeAttr(uint32_t fsId, uint64_t inodeId,
                                          InodeAttr *attr) {
    VLOG(1) << "GetInodeAttr, fsId = " << fsId << ", inodeId = " << inodeId;
    NameLockGuard lg(inodeLock_, GetInodeLockName(fsId, inodeId));
    MetaStatusCode ret = inodeStorage_->GetAttr(Key4Inode(fsId, inodeId), attr);
    if (ret != MetaStatusCode::OK) {
        LOG(ERROR) << "GetInodeAttr fail, fsId = " << fsId
                   << ", inodeId = " << inodeId
                   << ", ret = " << MetaStatusCode_Name(ret);
        return ret;
    }

    VLOG(1) << "GetInodeAttr success, fsId = " << fsId
            << ", inodeId = " << inodeId
            << ", " << attr->ShortDebugString();

    return MetaStatusCode::OK;
}

MetaStatusCode InodeManager::GetXAttr(uint32_t fsId, uint64_t inodeId,
                                      XAttr *xattr) {
    VLOG(1) << "GetXAttr, fsId = " << fsId << ", inodeId = " << inodeId;
    NameLockGuard lg(inodeLock_, GetInodeLockName(fsId, inodeId));
    xattr->set_inodeid(inodeId);
    xattr->set_fsid(fsId);
    MetaStatusCode ret =
        inodeStorage_->GetXAttr(Key4Inode(fsId, inodeId), xattr);
    if (ret != MetaStatusCode::OK) {
        LOG(ERROR) << "GetXAttr fail, fsId = " << fsId
                   << ", inodeId = " << inodeId
                   << ", ret = " << MetaStatusCode_Name(ret);
        return ret;
    }

    VLOG(1) << "GetXAttr success, fsId = " << fsId << ", inodeId = " << inodeId
            << ", " << xattr->ShortDebugString();

    return MetaStatusCode::OK;
}

MetaStatusCode InodeManager::DeleteInode(uint32_t fsId, uint64_t inodeId) {
    VLOG(1) << "DeleteInode, fsId = " << fsId << ", inodeId = " << inodeId;
    NameLockGuard lg(inodeLock_, GetInodeLockName(fsId, inodeId));
    MetaStatusCode ret = inodeStorage_->Delete(Key4Inode(fsId, inodeId));
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

MetaStatusCode InodeManager::UpdateInode(const UpdateInodeRequest &request) {
    VLOG(1) << "UpdateInode, " << request.ShortDebugString();
    NameLockGuard lg(inodeLock_, GetInodeLockName(
            request.fsid(), request.inodeid()));

    Inode old;
    MetaStatusCode ret = inodeStorage_->Get(
        Key4Inode(request.fsid(), request.inodeid()), &old);
    if (ret != MetaStatusCode::OK) {
        LOG(ERROR) << "GetInode fail, " << request.ShortDebugString()
                   << ", ret: " << MetaStatusCode_Name(ret);
        return ret;
    }

    bool needUpdate = false;
    bool needAddTrash = false;

#define UPDATE_INODE(param)                  \
    if (request.has_##param()) {            \
        old.set_##param(request.param()); \
        needUpdate = true; \
    }

    UPDATE_INODE(length)
    UPDATE_INODE(ctime)
    UPDATE_INODE(ctime_ns)
    UPDATE_INODE(mtime)
    UPDATE_INODE(mtime_ns)
    UPDATE_INODE(atime)
    UPDATE_INODE(atime_ns)
    UPDATE_INODE(uid)
    UPDATE_INODE(gid)
    UPDATE_INODE(mode)

    if (request.has_nlink()) {
        if (old.nlink() != 0 && request.nlink() == 0) {
            uint32_t now = TimeUtility::GetTimeofDaySec();
            old.set_dtime(now);
            needAddTrash = true;
        }
        old.set_nlink(request.nlink());
        needUpdate = true;
    }

    if (request.has_volumeextentlist()) {
        VLOG(1) << "update inode has extent";
        old.mutable_volumeextentlist()->CopyFrom(request.volumeextentlist());
        needUpdate = true;
    }

    if (!request.xattr().empty()) {
        VLOG(1) << "update inode has xattr";
        *(old.mutable_xattr()) = request.xattr();
        needUpdate = true;
    }

    // TODO(@one): openmpcount is incorrect in exceptional cases
    // 1. rpc retry: metaserver update ok but client do not have response, it
    // will retry.
    // 2. client exits unexpectedly: openmpcount will not be updated forerver.
    // if inode is in delete status, operation of DeleteIndoe will be performed
    // incorrectly.
    if (request.has_inodeopenstatuschange() && old.has_openmpcount() &&
        InodeOpenStatusChange::NOCHANGE != request.inodeopenstatuschange()) {
        VLOG(1) << "update inode open status";
        int32_t oldcount = old.openmpcount();
        int32_t newcount =
            request.inodeopenstatuschange() == InodeOpenStatusChange::OPEN
                ? oldcount + 1
                : oldcount - 1;
        if (newcount < 0) {
            LOG(ERROR) << "open mount point for inode: " << request.inodeid()
                       << " is " << newcount;
        } else {
            old.set_openmpcount(newcount);
            needUpdate = true;
        }
    }

    if (needUpdate) {
        ret = inodeStorage_->Update(old);
        if (ret != MetaStatusCode::OK) {
            LOG(ERROR) << "UpdateInode fail, " << request.ShortDebugString()
                       << ", ret: " << MetaStatusCode_Name(ret);
            return ret;
        }
    }

    if (needAddTrash) {
        trash_->Add(old.fsid(), old.inodeid(), old.dtime());
    }

    VLOG(1) << "UpdateInode success, " << request.ShortDebugString();
    return MetaStatusCode::OK;
}

MetaStatusCode InodeManager::GetOrModifyS3ChunkInfo(
    uint32_t fsId, uint64_t inodeId,
    const S3ChunkInfoMap& map2add,
    std::shared_ptr<Iterator>* iterator,
    bool returnS3ChunkInfoMap,
    bool compaction) {
    VLOG(1) << "GetOrModifyS3ChunkInfo, fsId: " << fsId
            << ", inodeId: " << inodeId;

    NameLockGuard lg(inodeLock_, GetInodeLockName(fsId, inodeId));

    // TODO(@Wine93): check if inode exist

    if (!map2add.empty()) {
        for (const auto& item : map2add) {
            uint64_t chunkIndex = item.first;
            auto list2add = item.second;
            MetaStatusCode rc = inodeStorage_->AppendS3ChunkInfoList(
                fsId, inodeId, chunkIndex, list2add, compaction);
            if (rc != MetaStatusCode::OK) {
                return rc;
            }
        }
    }

    // return if needed
    if (returnS3ChunkInfoMap) {
        *iterator = inodeStorage_->GetInodeS3ChunkInfoList(fsId, inodeId);
        if ((*iterator)->Status() != 0) {
            return MetaStatusCode::STORAGE_INTERNAL_ERROR;
        }
    }

    VLOG(1) << "GetOrModifyS3ChunkInfo success, fsId: " << fsId
            << ", inodeId: " << inodeId;
    return MetaStatusCode::OK;
}

MetaStatusCode InodeManager::PaddingInodeS3ChunkInfo(int32_t fsId,
                                                     uint64_t inodeId,
                                                     Inode* inode) {
    NameLockGuard lg(inodeLock_, GetInodeLockName(fsId, inodeId));
    VLOG(1) << "PaddingInodeS3ChunkInfo, fsId: " << fsId
            << ", inodeId: " << inodeId;
    return inodeStorage_->PaddingInodeS3ChunkInfo(fsId, inodeId, inode);
}

MetaStatusCode InodeManager::UpdateInodeWhenCreateOrRemoveSubNode(
    uint32_t fsId, uint64_t inodeId, bool isCreate) {
    VLOG(1) << "UpdateInodeWhenCreateOrRemoveSubNode, fsId = " << fsId
            << ", inodeId = " << inodeId
            << ", isCreate = " << isCreate;
    NameLockGuard lg(inodeLock_, GetInodeLockName(fsId, inodeId));

    Inode inode;
    MetaStatusCode ret = inodeStorage_->Get(
        Key4Inode(fsId, inodeId), &inode);
    if (ret != MetaStatusCode::OK) {
        LOG(ERROR) << "GetInode fail, " << inode.ShortDebugString()
                   << ", ret = " << MetaStatusCode_Name(ret);
        return ret;
    }
    uint32_t oldNlink = inode.nlink();
    if (oldNlink == 0) {
        // already be deleted
        return MetaStatusCode::OK;
    }
    if (isCreate) {
        inode.set_nlink(++oldNlink);
    } else {
        inode.set_nlink(--oldNlink);
    }

    struct timespec now;
    clock_gettime(CLOCK_REALTIME, &now);
    inode.set_ctime(now.tv_sec);
    inode.set_ctime_ns(now.tv_nsec);
    inode.set_mtime(now.tv_sec);
    inode.set_mtime_ns(now.tv_nsec);

    ret = inodeStorage_->Update(inode);
    if (ret != MetaStatusCode::OK) {
        LOG(ERROR) << "UpdateInode fail, " << inode.ShortDebugString()
                   << ", ret = " << MetaStatusCode_Name(ret);
        return ret;
    }

    VLOG(1) << "UpdateInodeWhenCreateOrRemoveSubNode success, "
            << inode.ShortDebugString();
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

bool InodeManager::GetInodeIdList(std::list<uint64_t>* inodeIdList) {
    return inodeStorage_->GetInodeIdList(inodeIdList);
}
}  // namespace metaserver
}  // namespace curvefs
