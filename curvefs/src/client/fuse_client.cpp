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

#include "curvefs/src/client/fuse_common.h"
#include "curvefs/src/client/extent_manager.h"
#include "src/common/timeutility.h"

namespace curvefs {
namespace client {

void FuseClient::init(void *userdata, struct fuse_conn_info *conn) {
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
}

void FuseClient::GetDentryParamFromInode(
    const Inode &inode, fuse_entry_param *param) {
    memset(param, 0, sizeof(fuse_entry_param));
    param->ino = inode.inodeid();
    param->generation = 0;
    GetAttrFromInode(inode, &param->attr);
    param->attr_timeout = 1.0;
    param->entry_timeout = 1.0;
}


void FuseClient::lookup(fuse_req_t req, fuse_ino_t parent, const char *name) {
    Dentry dentry;
    CURVEFS_ERROR ret = dentryManager_->GetDentry(parent, name, &dentry);
    if (ret != CURVEFS_ERROR::OK) {
        LOG(ERROR) << "dentryManager_ get dentry fail, ret = " << ret
                   << ", parent inodeid = " << parent
                   << ", name = " << name;
        fuse_reply_err_by_errcode(req, ret);
        return;
    }
    Inode inode;
    fuse_ino_t ino = dentry.inodeid();
    ret = inodeManager_->GetInode(ino, &inode);
    if (ret != CURVEFS_ERROR::OK) {
        LOG(ERROR) << "inodeManager get inode fail, ret = " << ret
                  << ", inodeid = " << ino;
        fuse_reply_err_by_errcode(req, ret);
        return;
    }

    fuse_entry_param e;
    GetDentryParamFromInode(inode, &e);
    fuse_reply_entry(req, &e);
    return;
}

void FuseClient::write(fuse_req_t req, fuse_ino_t ino, const char *buf,
          size_t size, off_t off, struct fuse_file_info *fi) {
    Inode inode;
    CURVEFS_ERROR ret = inodeManager_->GetInode(ino, &inode);
    if (ret != CURVEFS_ERROR::OK) {
        LOG(ERROR) << "inodeManager get inode fail, ret = " << ret
                  << ", inodeid = " << ino;
        fuse_reply_err_by_errcode(req, ret);
        return;
    }
    std::list<ExtentAllocInfo> toAllocExtents;
    ret = GetToAllocExtents(inode.volumeextentlist(),
        off, size, &toAllocExtents);
    if (toAllocExtents.size() != 0) {
        std::list<Extent> allocatedExtents;
        ret = metaClient_->AllocExtents(
            fsInfo_->fsid(), toAllocExtents, &allocatedExtents);
        if (ret != CURVEFS_ERROR::OK) {
            LOG(ERROR) << "metaClient alloc extents fail, ret = " << ret;
            fuse_reply_err_by_errcode(req, ret);
            return;
        }
        ret = MergeAllocedExtents(
            toAllocExtents,
            allocatedExtents,
            inode.mutable_volumeextentlist());
        if (ret != CURVEFS_ERROR::OK) {
            LOG(ERROR) << "toAllocExtents and allocatedExtents not match, "
                       << "ret = " << ret;
            CURVEFS_ERROR ret2 = metaClient_->DeAllocExtents(
                fsInfo_->fsid(), allocatedExtents);
            if (ret2 != CURVEFS_ERROR::OK) {
                LOG(ERROR) << "DeAllocExtents fail, ret = " << ret;
            }
            fuse_reply_err_by_errcode(req, ret);
            return;
        }
    }

    std::list<PExtent> pExtents;
    ret = DivideExtents(inode.volumeextentlist(),
        off, size,
        &pExtents);
    if (ret != CURVEFS_ERROR::OK) {
        LOG(ERROR) << "DivideExtents fail, ret = " << ret;
        fuse_reply_err_by_errcode(req, ret);
        return;
    }

    for (const auto &ext : pExtents) {
        ret = blockDeviceClient_->Write(buf + (ext.lOffset - off),
            ext.pOffset, ext.len);
        if (ret != CURVEFS_ERROR::OK) {
            LOG(ERROR) << "block device write fail, ret = " << ret;
            fuse_reply_err_by_errcode(req, ret);
            return;
        }
    }

    ret = MarkExtentsWritten(off, size, inode.mutable_volumeextentlist());
    if (ret != CURVEFS_ERROR::OK) {
        LOG(ERROR) << "MarkExtentsWritten fail, ret =  " << ret;
        fuse_reply_err_by_errcode(req, ret);
        return;
    }
    // update file len
    if (inode.length() < off + size) {
        inode.set_length(off + size);
    }
    ret = inodeManager_->UpdateInode(inode);
    if (ret != CURVEFS_ERROR::OK) {
        LOG(ERROR) << "UpdateInode fail, ret = " << ret;
        fuse_reply_err_by_errcode(req, ret);
        return;
    }
    fuse_reply_write(req, size);
    return;
}

void FuseClient::read(fuse_req_t req, fuse_ino_t ino, size_t size, off_t off,
          struct fuse_file_info *fi) {
    Inode inode;
    CURVEFS_ERROR ret = inodeManager_->GetInode(ino, &inode);
    if (ret != CURVEFS_ERROR::OK) {
        LOG(ERROR) << "inodeManager get inode fail, ret = " << ret
                  << ", inodeid = " << ino;
        fuse_reply_err_by_errcode(req, ret);
        return;
    }
    size_t len = 0;
    if (inode.length() < off + size) {
        len = inode.length() - off;
    } else {
        len = size;
    }

    std::list<PExtent> pExtents;
    ret = DivideExtents(inode.volumeextentlist(),
        off, len, &pExtents);
    if (ret != CURVEFS_ERROR::OK) {
        LOG(ERROR) << "DivideExtents fail, ret = " << ret;
        fuse_reply_err_by_errcode(req, ret);
        return;
    }
    std::unique_ptr<char[]> buffer(new char[len]());
    memset(buffer.get(), 0, len);
    for (const auto &ext : pExtents) {
        if (!ext.UnWritten) {
            ret = blockDeviceClient_->Read(buffer.get() + (ext.lOffset - off),
                ext.pOffset, ext.len);
            if (ret != CURVEFS_ERROR::OK) {
                LOG(ERROR) << "block device read fail, ret = " << ret;
                fuse_reply_err_by_errcode(req, ret);
                return;
            }
        }
    }
    fuse_reply_buf(req, buffer.get(), len);
    return;
}

void FuseClient::open(fuse_req_t req, fuse_ino_t ino,
          struct fuse_file_info *fi) {
    Inode inode;
    CURVEFS_ERROR ret = inodeManager_->GetInode(ino, &inode);
    if (ret != CURVEFS_ERROR::OK) {
        LOG(ERROR) << "inodeManager get inode fail, ret = " << ret
                  << ", inodeid = " << ino;
        fuse_reply_err_by_errcode(req, ret);
        return;
    }
    // TODO(xuchaojie): fix it
    fuse_reply_open(req, fi);
    return;
}

void FuseClient::create(fuse_req_t req, fuse_ino_t parent, const char *name,
          mode_t mode, struct fuse_file_info *fi) {
    InodeParam param; param.fsId = fsInfo_->fsid();
    param.length = 0;
    param.mode = mode;
    param.type = FsFileType::TYPE_FILE;

    Inode inode;
    CURVEFS_ERROR ret = inodeManager_->CreateInode(param, &inode);
    if (ret != CURVEFS_ERROR::OK) {
        LOG(ERROR) << "inodeManager CreateInode fail, ret = " << ret
                  << ", parent = " << parent
                  << ", name = " << name
                  << ", mode = " << mode;
        fuse_reply_err_by_errcode(req, ret);
        return;
    }
    Dentry dentry;
    dentry.set_fsid(fsInfo_->fsid());
    dentry.set_inodeid(inode.inodeid());
    dentry.set_parentinodeid(parent);
    dentry.set_name(name);
    ret = dentryManager_->CreateDentry(dentry);
    if (ret != CURVEFS_ERROR::OK) {
        LOG(ERROR) << "dentryManager_ CreateDentry fail, ret = " << ret
                  << ", parent = " << parent
                  << ", name = " << name
                  << ", mode = " << mode;
        fuse_reply_err_by_errcode(req, ret);
        return;
    }

    fuse_entry_param e;
    GetDentryParamFromInode(inode, &e);
    fuse_reply_create(req, &e, fi);
    return;
}

void FuseClient::mknod(fuse_req_t req, fuse_ino_t parent, const char *name,
          mode_t mode, dev_t rdev) {
    InodeParam param; param.fsId = fsInfo_->fsid();
    param.length = 0;
    param.mode = mode;
    param.type = FsFileType::TYPE_FILE;

    Inode inode;
    CURVEFS_ERROR ret = inodeManager_->CreateInode(param, &inode);
    if (ret != CURVEFS_ERROR::OK) {
        LOG(ERROR) << "inodeManager CreateInode fail, ret = " << ret
                  << ", parent = " << parent
                  << ", name = " << name
                  << ", mode = " << mode;
        fuse_reply_err_by_errcode(req, ret);
        return;
    }
    Dentry dentry;
    dentry.set_fsid(fsInfo_->fsid());
    dentry.set_inodeid(inode.inodeid());
    dentry.set_parentinodeid(parent);
    dentry.set_name(name);
    ret = dentryManager_->CreateDentry(dentry);
    if (ret != CURVEFS_ERROR::OK) {
        LOG(ERROR) << "dentryManager_ CreateDentry fail, ret = " << ret
                  << ", parent = " << parent
                  << ", name = " << name
                  << ", mode = " << mode;
        fuse_reply_err_by_errcode(req, ret);
        return;
    }

    fuse_entry_param e;
    GetDentryParamFromInode(inode, &e);
    fuse_reply_entry(req, &e);
    return;
}

void FuseClient::mkdir(fuse_req_t req, fuse_ino_t parent, const char *name,
           mode_t mode) {
    InodeParam param; param.fsId = fsInfo_->fsid();
    param.length = 0;
    param.mode = mode;
    param.type = FsFileType::TYPE_DIRECTORY;

    Inode inode;
    CURVEFS_ERROR ret = inodeManager_->CreateInode(param, &inode);
    if (ret != CURVEFS_ERROR::OK) {
        LOG(ERROR) << "inodeManager CreateInode fail, ret = " << ret
                  << ", parent = " << parent
                  << ", name = " << name
                  << ", mode = " << mode;
        fuse_reply_err_by_errcode(req, ret);
        return;
    }
    Dentry dentry;
    dentry.set_fsid(fsInfo_->fsid());
    dentry.set_inodeid(inode.inodeid());
    dentry.set_parentinodeid(parent);
    dentry.set_name(name);
    ret = dentryManager_->CreateDentry(dentry);
    if (ret != CURVEFS_ERROR::OK) {
        LOG(ERROR) << "dentryManager_ CreateDentry fail, ret = " << ret
                  << ", parent = " << parent
                  << ", name = " << name
                  << ", mode = " << mode;
        fuse_reply_err_by_errcode(req, ret);
        return;
    }

    fuse_entry_param e;
    GetDentryParamFromInode(inode, &e);
    fuse_reply_entry(req, &e);
    return;
}

void FuseClient::unlink(fuse_req_t req, fuse_ino_t parent, const char *name) {
    Dentry dentry;
    CURVEFS_ERROR ret = dentryManager_->GetDentry(parent, name, &dentry);
    if (ret != CURVEFS_ERROR::OK) {
        LOG(ERROR) << "dentryManager_ GetDentry fail, ret = " << ret
                  << ", parent = " << parent
                  << ", name = " << name;
        fuse_reply_err_by_errcode(req, ret);
        return;
    }
    ret = dentryManager_->DeleteDentry(parent, name);
    if (ret != CURVEFS_ERROR::OK) {
        LOG(ERROR) << "dentryManager_ DeleteDentry fail, ret = " << ret
                  << ", parent = " << parent
                  << ", name = " << name;
        fuse_reply_err_by_errcode(req, ret);
        return;
    }
    ret = inodeManager_->DeleteInode(dentry.inodeid());
    if (ret != CURVEFS_ERROR::OK) {
        LOG(ERROR) << "inodeManager_ DeleteInode fail, ret = " << ret
                  << ", parent = " << parent
                  << ", name = " << name
                  << ", inode = " << dentry.inodeid();
        fuse_reply_err_by_errcode(req, ret);
        return;
    }

    fuse_reply_err(req, 0);
    return;
}

void FuseClient::rmdir(fuse_req_t req, fuse_ino_t parent, const char *name) {
    Dentry dentry;
    CURVEFS_ERROR ret = dentryManager_->GetDentry(parent, name, &dentry);
    if (ret != CURVEFS_ERROR::OK) {
        LOG(ERROR) << "dentryManager_ GetDentry fail, ret = " << ret
                  << ", parent = " << parent
                  << ", name = " << name;
        fuse_reply_err_by_errcode(req, ret);
        return;
    }
    ret = dentryManager_->DeleteDentry(parent, name);
    if (ret != CURVEFS_ERROR::OK) {
        LOG(ERROR) << "dentryManager_ DeleteDentry fail, ret = " << ret
                  << ", parent = " << parent
                  << ", name = " << name;
        fuse_reply_err_by_errcode(req, ret);
        return;
    }
    ret = inodeManager_->DeleteInode(dentry.inodeid());
    if (ret != CURVEFS_ERROR::OK) {
        LOG(ERROR) << "inodeManager_ DeleteInode fail, ret = " << ret
                  << ", parent = " << parent
                  << ", name = " << name
                  << ", inode = " << dentry.inodeid();
        fuse_reply_err_by_errcode(req, ret);
        return;
    }

    fuse_reply_err(req, 0);
    return;
}

void FuseClient::opendir(fuse_req_t req, fuse_ino_t ino,
         struct fuse_file_info *fi) {
    Inode inode;
    CURVEFS_ERROR ret = inodeManager_->GetInode(ino, &inode);
    if (ret != CURVEFS_ERROR::OK) {
        LOG(ERROR) << "inodeManager get inode fail, ret = " << ret
                  << ", inodeid = " << ino;
        fuse_reply_err_by_errcode(req, ret);
        return;
    }

    uint32_t dindex = dirBuf_->DirBufferNew();
    fi->fh = dindex;

    // TODO(xuchaojie): fix it
    fuse_reply_open(req, fi);
    return;
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

void FuseClient::readdir(fuse_req_t req, fuse_ino_t ino, size_t size, off_t off,
         struct fuse_file_info *fi) {
    Inode inode;
    CURVEFS_ERROR ret = inodeManager_->GetInode(ino, &inode);
    if (ret != CURVEFS_ERROR::OK) {
        LOG(ERROR) << "inodeManager get inode fail, ret = " << ret
                  << ", inodeid = " << ino;
        fuse_reply_err_by_errcode(req, ret);
        return;
    }

    uint32_t dindex = fi->fh;
    DirBufferHead *bufHead = dirBuf_->DirBufferGet(dindex);
    if (!bufHead->wasRead) {
        std::list<Dentry> dentryList;
        ret = dentryManager_->ListDentry(ino, &dentryList);
        if (ret != CURVEFS_ERROR::OK) {
            LOG(ERROR) << "dentryManager_ ListDentry fail, ret = " << ret
                      << ", parent = " << ino;
            fuse_reply_err_by_errcode(req, ret);
            return;
        }
        for (const auto &dentry : dentryList) {
            dirbuf_add(req, bufHead, dentry);
        }
    }
    if (off < bufHead->size) {
        fuse_reply_buf(req, bufHead->p + off,
                    std::min(bufHead->size - off, size));
    } else {
        fuse_reply_buf(req, NULL, 0);
    }
}

void FuseClient::getattr(fuse_req_t req, fuse_ino_t ino,
         struct fuse_file_info *fi) {
    Inode inode;
    CURVEFS_ERROR ret = inodeManager_->GetInode(ino, &inode);
    if (ret != CURVEFS_ERROR::OK) {
        LOG(ERROR) << "inodeManager get inode fail, ret = " << ret
                  << ", inodeid = " << ino;
        fuse_reply_err_by_errcode(req, ret);
        return;
    }
    struct stat attr;
    memset(&attr, 0, sizeof(attr));
    GetAttrFromInode(inode, &attr);
    fuse_reply_attr(req, &attr, 1.0);
    return;
}

void FuseClient::setattr(fuse_req_t req, fuse_ino_t ino, struct stat *attr,
        int to_set, struct fuse_file_info *fi) {
    Inode inode;
    CURVEFS_ERROR ret = inodeManager_->GetInode(ino, &inode);
    if (ret != CURVEFS_ERROR::OK) {
        LOG(ERROR) << "inodeManager get inode fail, ret = " << ret
                  << ", inodeid = " << ino;
        fuse_reply_err_by_errcode(req, ret);
        return;
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
    uint64_t nowTime = TimeUtility::GetTimeofDayMs();
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
        fuse_reply_err_by_errcode(req, ret);
        return;
    }
    struct stat attrOut;
    memset(&attrOut, 0, sizeof(attrOut));
    GetAttrFromInode(inode, &attrOut);
    fuse_reply_attr(req, &attrOut, 1.0);
    return;
}

void FuseClient::fuse_reply_err_by_errcode(
    fuse_req_t req, CURVEFS_ERROR errcode) {
    switch (errcode) {
        case CURVEFS_ERROR::NO_SPACE:
            fuse_reply_err(req, ENOSPC);
        default:
            fuse_reply_err(req, EIO);
            break;
    }
}



}  // namespace client
}  // namespace curvefs
