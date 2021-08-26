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
 * Created Date: Thur May 27 2021
 * Author: xuchaojie
 */

#include "curvefs/src/client/fuse_client.h"

#include <list>
#include <algorithm>
#include <string>
#include <vector>

#include "curvefs/src/client/fuse_common.h"
#include "curvefs/src/client/extent_manager.h"
#include "src/common/timeutility.h"

using ::curvefs::common::Volume;
using ::curvefs::common::S3Info;

namespace curvefs {
namespace client {

CURVEFS_ERROR FuseClient::Init(const FuseClientOption &option) {
    mdsBase_ = new MDSBaseClient();
    CURVEFS_ERROR ret = mdsClient_->Init(option.mdsOpt, mdsBase_);
    if (ret != CURVEFS_ERROR::OK) {
        return ret;
    }

    metaBase_ = new MetaServerBaseClient();
    ret = metaClient_->Init(option.metaOpt, metaBase_);
    if (ret != CURVEFS_ERROR::OK) {
        return ret;
    }

    spaceBase_ = new SpaceBaseClient();
    ret = spaceClient_->Init(option.spaceOpt, spaceBase_);
    if (ret != CURVEFS_ERROR::OK) {
        return ret;
    }

    ret = dentryManager_->Init(option.dcacheOpt);
    if (ret != CURVEFS_ERROR::OK) {
        return ret;
    }

    ret = extManager_->Init(option.extentManagerOpt);
    return ret;
}

void FuseClient::UnInit() {
    delete mdsBase_;
    mdsBase_ = nullptr;

    delete spaceBase_;
    spaceBase_ = nullptr;

    delete metaBase_;
    metaBase_ = nullptr;
}

std::ostream &operator<<(std::ostream &os, const struct stat &attr) {
    os << "{ st_ino = " << attr.st_ino
       << ", st_mode = " << attr.st_mode
       << ", st_nlink = " << attr.st_nlink
       << ", st_uid = " << attr.st_uid
       << ", st_gid = " << attr.st_gid
       << ", st_size = " << attr.st_size
       << ", st_atime = " << attr.st_atime
       << ", st_mtime = " << attr.st_mtime
       << ", st_ctime = " << attr.st_ctime
       << "}" << std::endl;
    return os;
}

void FuseClient::InitTxId(const FsInfo& fsInfo) {
    for (const auto& partitionTxId : fsInfo.partitiontxids()) {
        auto partitionId = partitionTxId.partitionid();
        auto txId = partitionTxId.txid();
        partitionTxIds_[partitionId] = txId;
    }
}

void FuseClient::GetAttrFromInode(const Inode &inode, struct stat *attr) {
    attr->st_ino = inode.inodeid();
    attr->st_mode = inode.mode();
    attr->st_nlink = inode.nlink();
    attr->st_uid = inode.uid();
    attr->st_gid = inode.gid();
    attr->st_size = inode.length();
    attr->st_atime = inode.atime();
    attr->st_mtime = inode.mtime();
    attr->st_ctime = inode.ctime();
    LOG(INFO) << "GetAttrFromInode attr =  " << *attr;
}

void FuseClient::GetDentryParamFromInode(
    const Inode &inode, fuse_entry_param *param) {
    memset(param, 0, sizeof(fuse_entry_param));
    param->ino = inode.inodeid();
    param->generation = 0;
    GetAttrFromInode(inode, &param->attr);
    param->attr_timeout = option_.attrTimeOut;
    param->entry_timeout = option_.entryTimeOut;
}

CURVEFS_ERROR FuseClient::FuseOpLookup(fuse_req_t req, fuse_ino_t parent,
    const char *name, fuse_entry_param *e) {
    Dentry dentry;
    CURVEFS_ERROR ret = dentryManager_->GetDentry(
        parent, name, partitionTxIds_[0], &dentry);
    if (ret != CURVEFS_ERROR::OK) {
        LOG(ERROR) << "dentryManager_ get dentry fail, ret = " << ret
                   << ", parent inodeid = " << parent
                   << ", name = " << name
                   << ", txId = " << partitionTxIds_[0];
        return ret;
    }
    Inode inode;
    fuse_ino_t ino = dentry.inodeid();
    ret = inodeManager_->GetInode(ino, &inode);
    if (ret != CURVEFS_ERROR::OK) {
        LOG(ERROR) << "inodeManager get inode fail, ret = " << ret
                  << ", inodeid = " << ino;
        return ret;
    }
    GetDentryParamFromInode(inode, e);
    return ret;
}

CURVEFS_ERROR FuseClient::FuseOpOpen(fuse_req_t req, fuse_ino_t ino,
          struct fuse_file_info *fi) {
    Inode inode;
    CURVEFS_ERROR ret = inodeManager_->GetInode(ino, &inode);
    if (ret != CURVEFS_ERROR::OK) {
        LOG(ERROR) << "inodeManager get inode fail, ret = " << ret
                  << ", inodeid = " << ino;
        return ret;
    }

    if (fi->flags & O_TRUNC) {
        if (fi->flags & O_WRONLY || fi->flags & O_RDWR) {
            int tRet = Truncate(&inode, 0);
            if (tRet < 0) {
                LOG(ERROR) << "truncate file fail, ret = " << ret
                           << ", inodeid = " << ino;
                return CURVEFS_ERROR::FAILED;
            }
        } else {
            return CURVEFS_ERROR::NOPERMISSION;
        }
    }
    return ret;
}

CURVEFS_ERROR FuseClient::MakeNode(fuse_req_t req, fuse_ino_t parent,
        const char *name, mode_t mode, FsFileType type,
        fuse_entry_param *e) {
    const struct fuse_ctx *ctx = fuse_req_ctx(req);
    InodeParam param;
    param.fsId = fsInfo_->fsid();
    param.length = 0;
    param.uid = ctx->uid;
    param.gid = ctx->gid;
    param.mode = mode;
    param.type = type;

    Inode inode;
    CURVEFS_ERROR ret = inodeManager_->CreateInode(param, &inode);
    if (ret != CURVEFS_ERROR::OK) {
        LOG(ERROR) << "inodeManager CreateInode fail, ret = " << ret
                  << ", parent = " << parent
                  << ", name = " << name
                  << ", mode = " << mode;
        return ret;
    }
    Dentry dentry;
    dentry.set_fsid(fsInfo_->fsid());
    dentry.set_inodeid(inode.inodeid());
    dentry.set_parentinodeid(parent);
    dentry.set_name(name);
    // TODO(Wine93): get txid by partition id
    dentry.set_txid(partitionTxIds_[0]);
    if (type == FsFileType::TYPE_FILE || type == FsFileType::TYPE_S3) {
        dentry.set_flag(DentryFlag::TYPE_FILE_FLAG);
    }

    ret = dentryManager_->CreateDentry(dentry);
    if (ret != CURVEFS_ERROR::OK) {
        LOG(ERROR) << "dentryManager_ CreateDentry fail, ret = " << ret
                  << ", parent = " << parent
                  << ", name = " << name
                  << ", txId = " << partitionTxIds_[0]
                  << ", mode = " << mode;
        return ret;
    }

    GetDentryParamFromInode(inode, e);
    return ret;
}

CURVEFS_ERROR FuseClient::FuseOpMkDir(fuse_req_t req, fuse_ino_t parent,
        const char *name, mode_t mode,
        fuse_entry_param *e) {
    return MakeNode(req, parent, name,
        S_IFDIR | mode, FsFileType::TYPE_DIRECTORY, e);
}

CURVEFS_ERROR FuseClient::FuseOpUnlink(fuse_req_t req, fuse_ino_t parent,
    const char *name) {
    return RemoveNode(req, parent, name);
}
CURVEFS_ERROR FuseClient::RemoveNode(fuse_req_t req, fuse_ino_t parent,
    const char *name) {
    Dentry dentry;
    CURVEFS_ERROR ret = dentryManager_->GetDentry(
        parent, name, partitionTxIds_[0], &dentry);
    if (ret != CURVEFS_ERROR::OK) {
        LOG(ERROR) << "dentryManager_ GetDentry fail, ret = " << ret
                   << ", parent = " << parent
                   << ", name = " << name
                   << ", txId = " << partitionTxIds_[0];
        return ret;
    }
    ret = dentryManager_->DeleteDentry(parent, name, partitionTxIds_[0]);
    if (ret != CURVEFS_ERROR::OK) {
        LOG(ERROR) << "dentryManager_ DeleteDentry fail, ret = " << ret
                   << ", parent = " << parent
                   << ", name = " << name
                   << ", txId = " << partitionTxIds_[0];
        return ret;
    }
    // TODO(xuchaojie) : judge can inode be deleted
    ret = inodeManager_->DeleteInode(dentry.inodeid());
    if (ret != CURVEFS_ERROR::OK) {
        LOG(ERROR) << "inodeManager_ DeleteInode fail, ret = " << ret
                  << ", parent = " << parent
                  << ", name = " << name
                  << ", inode = " << dentry.inodeid();
        return ret;
    }
    return ret;
}

CURVEFS_ERROR FuseClient::FuseOpRmDir(fuse_req_t req, fuse_ino_t parent,
    const char *name) {
    return RemoveNode(req, parent, name);
}

CURVEFS_ERROR FuseClient::FuseOpOpenDir(fuse_req_t req, fuse_ino_t ino,
         struct fuse_file_info *fi) {
    Inode inode;
    CURVEFS_ERROR ret = inodeManager_->GetInode(ino, &inode);
    if (ret != CURVEFS_ERROR::OK) {
        LOG(ERROR) << "inodeManager get inode fail, ret = " << ret
                  << ", inodeid = " << ino;
        return ret;
    }

    uint64_t dindex = dirBuf_->DirBufferNew();
    fi->fh = dindex;

    // TODO(xuchaojie): fix it
    return ret;
}

static void dirbuf_add(fuse_req_t req,
    struct DirBufferHead *b, const Dentry &dentry) {
    struct stat stbuf;
    size_t oldsize = b->size;
    b->size += fuse_add_direntry(req, NULL, 0, dentry.name().c_str(), NULL, 0);
    b->p = static_cast<char *>(realloc(b->p, b->size));
    memset(&stbuf, 0, sizeof(stbuf));
    stbuf.st_ino = dentry.inodeid();
    fuse_add_direntry(req, b->p + oldsize, b->size - oldsize,
        dentry.name().c_str(), &stbuf, b->size);
}

CURVEFS_ERROR FuseClient::FuseOpReadDir(
        fuse_req_t req, fuse_ino_t ino, size_t size, off_t off,
        struct fuse_file_info *fi,
        char **buffer,
        size_t *rSize) {
    LOG(INFO) << "FuseOpReadDir ino = " << ino
              << ", size = " << size
              << ", off = " << off;
    Inode inode;
    CURVEFS_ERROR ret = inodeManager_->GetInode(ino, &inode);
    if (ret != CURVEFS_ERROR::OK) {
        LOG(ERROR) << "inodeManager get inode fail, ret = " << ret
                  << ", inodeid = " << ino;
        return ret;
    }

    uint64_t dindex = fi->fh;
    DirBufferHead *bufHead = dirBuf_->DirBufferGet(dindex);
    auto txId = partitionTxIds_[0];
    if (!bufHead->wasRead) {
        std::list<Dentry> dentryList;
        ret = dentryManager_->ListDentry(ino, txId, &dentryList, 0);
        if (ret != CURVEFS_ERROR::OK) {
            LOG(ERROR) << "dentryManager_ ListDentry fail, ret = " << ret
                       << ", parent = " << ino
                       << ", txId = " << partitionTxIds_[0];
            return ret;
        }
        for (const auto &dentry : dentryList) {
            dirbuf_add(req, bufHead, dentry);
        }
        bufHead->wasRead = true;
    }
    if (off < bufHead->size) {
        *buffer = bufHead->p + off;
        *rSize = std::min(bufHead->size - off, size);
    } else {
        *buffer = nullptr;
        *rSize = 0;
    }
    return ret;
}

// TODO(Wine93): we should improve the check for whether a directory is empty
CURVEFS_ERROR FuseClient::Overwrite(const Dentry& dentry, uint64_t txId) {
    auto rc = CURVEFS_ERROR::OK;
    if ((dentry.flag() & DentryFlag::TYPE_FILE_FLAG) == 0) {  // directory
        std::list<Dentry> dentrys;
        rc = dentryManager_->ListDentry(dentry.inodeid(), txId, &dentrys, 1);
        if (rc == CURVEFS_ERROR::OK && !dentrys.empty()) {
            LOG(ERROR) << "The directory is not empty"
                       << ", dentry = (" << dentry.ShortDebugString() << ")";
            rc = CURVEFS_ERROR::NOTEMPTY;
        }
    }

    return rc;
}

CURVEFS_ERROR FuseClient::FuseOpRename(fuse_req_t req,
                                       fuse_ino_t parent,
                                       const char* name,
                                       fuse_ino_t newparent,
                                       const char* newname) {
    Dentry src, dst;
    auto partitionId = 0;
    auto txId = partitionTxIds_[partitionId];

    // step1: precheck for rename operator
    auto rc = dentryManager_->GetDentry(parent, name, txId, &src);
    if (rc != CURVEFS_ERROR::OK) {
        LOG(ERROR) << "Failed to get dentry, retCode = " << rc;
        return rc;
    }

    uint64_t oldInodeId = 0;
    rc = dentryManager_->GetDentry(newparent, newname, txId, &dst);
    if (rc == CURVEFS_ERROR::NOTEXIST) {  // not exist
        // do nothing
    } else if (rc == CURVEFS_ERROR::OK) {  // exist
        oldInodeId = dst.inodeid();
        if ((rc = Overwrite(dst, txId)) != CURVEFS_ERROR::OK) {
            return rc;
        }
    } else {  // error
        LOG(ERROR) << "Failed to get dentry, retCode = " << rc;
        return rc;
    }

    // step2: generate dentrys for rename tx
    // dentry = { fsId, parentId, name, inodeId, txId + 1, flag }
    auto srcDentry = Dentry(src);
    srcDentry.set_txid(txId + 1);
    srcDentry.set_flag(DentryFlag::DELETE_MARK_FLAG);

    auto dstDentry = Dentry(src);
    dstDentry.set_parentinodeid(newparent);
    dstDentry.set_name(newname);
    dstDentry.set_txid(txId + 1);

    VLOG(1) << "Prepare rename tx: "
            << "src dentry = (" << srcDentry.ShortDebugString() << ")"
            << ", dst dentry = (" << dstDentry.ShortDebugString() << ")";

    // step3: prepare rename tx
    rc = dentryManager_->Rename(srcDentry, dstDentry);
    if (rc != CURVEFS_ERROR::OK) {
        LOG(ERROR) << "Rename failed, retCode = " << rc;
        return rc;
    }

    // step4: commit rename tx
    PartitionTxId partitionTxId;
    partitionTxId.set_partitionid(partitionId);
    partitionTxId.set_txid(txId + 1);
    std::vector<PartitionTxId> txIds{ partitionTxId };
    rc = mdsClient_->CommitTx(srcDentry.fsid(), txIds);
    if (rc != CURVEFS_ERROR::OK) {
        LOG(ERROR) << "CommitTx failed, retCode = " << rc;
        return rc;
    }

    // step5: delete old inode if it exist
    if (oldInodeId != 0) {
        rc = inodeManager_->DeleteInode(oldInodeId);
        if (rc != CURVEFS_ERROR::OK) {
            LOG(ERROR) << "DeleteInode fail, retCode = " << rc
                       << ", inodeId = " << oldInodeId;
        }
    }

    // step6: update dentry cache and update txid cache
    dentryManager_->DeleteCache(srcDentry.parentinodeid(), srcDentry.name());
    dentryManager_->InsertOrReplaceCache(dstDentry, true);
    partitionTxIds_[0] = txId + 1;

    return CURVEFS_ERROR::OK;
}

CURVEFS_ERROR FuseClient::FuseOpGetAttr(fuse_req_t req, fuse_ino_t ino,
         struct fuse_file_info *fi, struct stat *attr) {
    Inode inode;
    CURVEFS_ERROR ret = inodeManager_->GetInode(ino, &inode);
    if (ret != CURVEFS_ERROR::OK) {
        LOG(ERROR) << "inodeManager get inode fail, ret = " << ret
                  << ", inodeid = " << ino;
        return ret;
    }
    memset(attr, 0, sizeof(*attr));
    GetAttrFromInode(inode, attr);
    return ret;
}

CURVEFS_ERROR FuseClient::FuseOpSetAttr(
        fuse_req_t req, fuse_ino_t ino, struct stat *attr,
        int to_set, struct fuse_file_info *fi, struct stat *attrOut) {
    LOG(INFO) << "FuseOpSetAttr to_set = " << to_set;
    Inode inode;
    CURVEFS_ERROR ret = inodeManager_->GetInode(ino, &inode);
    if (ret != CURVEFS_ERROR::OK) {
        LOG(ERROR) << "inodeManager get inode fail, ret = " << ret
                  << ", inodeid = " << ino;
        return ret;
    }

    if (to_set & FUSE_SET_ATTR_MODE) {
        inode.set_mode(attr->st_mode);
    }
    if (to_set & FUSE_SET_ATTR_UID) {
        inode.set_uid(attr->st_uid);
    }
    if (to_set & FUSE_SET_ATTR_GID) {
        inode.set_gid(attr->st_gid);
    }
    if (to_set & FUSE_SET_ATTR_SIZE) {
        inode.set_length(attr->st_size);
    }
    if (to_set & FUSE_SET_ATTR_ATIME) {
        inode.set_atime(attr->st_atime);
    }
    if (to_set & FUSE_SET_ATTR_MTIME) {
        inode.set_mtime(attr->st_mtime);
    }
    uint64_t nowTime = TimeUtility::GetTimeofDaySec();
    if (to_set & FUSE_SET_ATTR_ATIME_NOW) {
        inode.set_atime(nowTime);
    }
    if (to_set & FUSE_SET_ATTR_MTIME_NOW) {
        inode.set_mtime(nowTime);
    }
    if (to_set & FUSE_SET_ATTR_CTIME) {
        inode.set_ctime(attr->st_ctime);
    }
    ret = inodeManager_->UpdateInode(inode);
    if (ret != CURVEFS_ERROR::OK) {
        LOG(ERROR) << "inodeManager get inode fail, ret = " << ret
                  << ", inodeid = " << ino;
        return ret;
    }
    memset(attrOut, 0, sizeof(*attrOut));
    GetAttrFromInode(inode, attrOut);
    return ret;
}


}  // namespace client
}  // namespace curvefs
