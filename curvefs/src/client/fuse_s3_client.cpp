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

#include "curvefs/src/client/fuse_s3_client.h"

#include <memory>
#include <string>

namespace curvefs {
namespace client {

CURVEFS_ERROR FuseS3Client::Init(const FuseClientOption &option) {
    CURVEFS_ERROR ret = FuseClient::Init(option);
    if (ret != CURVEFS_ERROR::OK) {
        return ret;
    }

    s3Client_ = std::make_shared<S3ClientImpl>();
    s3Client_->Init(option.s3Opt.s3AdaptrOpt);

    ret = s3Adaptor_->Init(option.s3Opt.s3ClientAdaptorOpt, s3Client_.get(),
                           inodeManager_, mdsClient_);
    return ret;
}

void FuseS3Client::UnInit() {
    s3Adaptor_->Stop();
    FuseClient::UnInit();
}

CURVEFS_ERROR FuseS3Client::FuseOpInit(
    void* userdata, struct fuse_conn_info* conn) {
    CURVEFS_ERROR ret = FuseClient::FuseOpInit(userdata, conn);
    if (init_) {
        s3Adaptor_->SetFsId(fsInfo_->fsid());
    }
    return ret;
}

CURVEFS_ERROR FuseS3Client::CreateFs(void *userdata, FsInfo *fsInfo) {
    struct MountOption *mOpts = (struct MountOption *)userdata;
    std::string fsName = (mOpts->fsName == nullptr) ? "" : mOpts->fsName;
    ::curvefs::common::S3Info s3Info;
    s3Info.set_ak(option_.s3Opt.s3AdaptrOpt.ak);
    s3Info.set_sk(option_.s3Opt.s3AdaptrOpt.sk);
    s3Info.set_endpoint(option_.s3Opt.s3AdaptrOpt.s3Address);
    s3Info.set_bucketname(option_.s3Opt.s3AdaptrOpt.bucketName);
    s3Info.set_blocksize(option_.s3Opt.s3ClientAdaptorOpt.blockSize);
    s3Info.set_chunksize(option_.s3Opt.s3ClientAdaptorOpt.chunkSize);
    // fsBlockSize means min allocsize, for s3, we do not need this.
    FSStatusCode ret = mdsClient_->CreateFsS3(fsName, 1, s3Info);
    if (ret != FSStatusCode::OK) {
        return CURVEFS_ERROR::INTERNAL;
    }

    return CURVEFS_ERROR::OK;
}

CURVEFS_ERROR FuseS3Client::FuseOpWrite(fuse_req_t req, fuse_ino_t ino,
                                        const char *buf, size_t size, off_t off,
                                        struct fuse_file_info *fi,
                                        size_t *wSize) {
    // check align
    if (fi->flags & O_DIRECT) {
        if (!(is_aligned(off, DirectIOAlignemnt) &&
              is_aligned(size, DirectIOAlignemnt)))
            return CURVEFS_ERROR::INVALIDPARAM;
    }

    int wRet = s3Adaptor_->Write(ino, off, size, buf);
    if (wRet < 0) {
        LOG(ERROR) << "s3Adaptor_ write failed, ret = " << wRet;
        return CURVEFS_ERROR::INTERNAL;
    }

    std::shared_ptr<InodeWrapper> inodeWrapper;
    CURVEFS_ERROR ret = inodeManager_->GetInode(ino, inodeWrapper);
    if (ret != CURVEFS_ERROR::OK) {
        LOG(ERROR) << "inodeManager get inode fail, ret = " << ret
                   << ", inodeid = " << ino;
        return ret;
    }

    ::curve::common::UniqueLock lgGuard = inodeWrapper->GetUniqueLock();
    Inode inode = inodeWrapper->GetInodeUnlocked();

    *wSize = wRet;
    // update file len
    if (inode.length() < off + *wSize) {
        inode.set_length(off + *wSize);
    }
    uint64_t nowTime = TimeUtility::GetTimeofDaySec();
    inode.set_mtime(nowTime);
    inode.set_ctime(nowTime);

    inodeWrapper->SwapInode(&inode);
    inodeManager_->ShipToFlush(inodeWrapper);

    if (fi->flags & O_DIRECT || fi->flags & O_SYNC || fi->flags & O_DSYNC) {
        // Todo: do some cache flush later
    }
    return ret;
}

CURVEFS_ERROR FuseS3Client::FuseOpRead(fuse_req_t req, fuse_ino_t ino,
                                       size_t size, off_t off,
                                       struct fuse_file_info *fi, char *buffer,
                                       size_t *rSize) {
    // check align
    if (fi->flags & O_DIRECT) {
        if (!(is_aligned(off, DirectIOAlignemnt) &&
              is_aligned(size, DirectIOAlignemnt)))
            return CURVEFS_ERROR::INVALIDPARAM;
    }

    std::shared_ptr<InodeWrapper> inodeWrapper;
    CURVEFS_ERROR ret = inodeManager_->GetInode(ino, inodeWrapper);
    if (ret != CURVEFS_ERROR::OK) {
        LOG(ERROR) << "inodeManager get inode fail, ret = " << ret
                   << ", inodeid = " << ino;
        return ret;
    }
    Inode inode = inodeWrapper->GetInodeLocked();

    size_t len = 0;
    if (inode.length() <= off) {
        *rSize = 0;
        return CURVEFS_ERROR::OK;
    } else if (inode.length() < off + size) {
        len = inode.length() - off;
    } else {
        len = size;
    }

    // Read do not change inode. so we do not get lock here.
    int rRet = s3Adaptor_->Read(&inode, off, len, buffer);
    if (rRet < 0) {
        LOG(ERROR) << "s3Adaptor_ read failed, ret = " << rRet;
        return CURVEFS_ERROR::INTERNAL;
    }
    *rSize = rRet;

    ::curve::common::UniqueLock lgGuard = inodeWrapper->GetUniqueLock();
    Inode newInode = inodeWrapper->GetInodeUnlocked();

    uint64_t nowTime = TimeUtility::GetTimeofDaySec();
    newInode.set_ctime(nowTime);
    newInode.set_atime(nowTime);

    inodeWrapper->SwapInode(&newInode);
    inodeManager_->ShipToFlush(inodeWrapper);

    VLOG(6) << "read end, read size = " << *rSize;
    return ret;
}

CURVEFS_ERROR FuseS3Client::FuseOpCreate(fuse_req_t req, fuse_ino_t parent,
                                         const char *name, mode_t mode,
                                         struct fuse_file_info *fi,
                                         fuse_entry_param *e) {
    CURVEFS_ERROR ret =
        MakeNode(req, parent, name, mode, FsFileType::TYPE_S3, e);
    if (ret != CURVEFS_ERROR::OK) {
        return ret;
    }
    return FuseOpOpen(req, e->ino, fi);
}

CURVEFS_ERROR FuseS3Client::FuseOpMkNod(fuse_req_t req, fuse_ino_t parent,
                                        const char *name, mode_t mode,
                                        dev_t rdev, fuse_entry_param *e) {
    return MakeNode(req, parent, name, mode, FsFileType::TYPE_S3, e);
}

CURVEFS_ERROR FuseS3Client::FuseOpFsync(fuse_req_t req, fuse_ino_t ino,
                                        int datasync,
                                        struct fuse_file_info *fi) {
    VLOG(3) << "fsync, ino = " << ino << ", datasync = " << datasync;

    CURVEFS_ERROR ret = s3Adaptor_->Flush(ino);
    if (ret != CURVEFS_ERROR::OK) {
        LOG(ERROR) << "s3Adaptor_ flush failed, ret = " << ret
                   << ", inodeid = " << ino;
        return ret;
    }
    if (datasync != 0) {
        return CURVEFS_ERROR::OK;
    }
    std::shared_ptr<InodeWrapper> inodeWrapper;
    ret = inodeManager_->GetInode(ino, inodeWrapper);
    if (ret != CURVEFS_ERROR::OK) {
        LOG(ERROR) << "inodeManager get inode fail, ret = " << ret
                   << ", inodeid = " << ino;
        return ret;
    }
    ::curve::common::UniqueLock lgGuard = inodeWrapper->GetUniqueLock();
    return inodeWrapper->Sync();
}

CURVEFS_ERROR FuseS3Client::Truncate(Inode *inode, uint64_t length) {
    return s3Adaptor_->Truncate(inode, 0);
}

void FuseS3Client::FlushData() {
    CURVEFS_ERROR ret = CURVEFS_ERROR::UNKNOWN;
    do {
        ret = s3Adaptor_->FsSync();
    } while (ret != CURVEFS_ERROR::OK);
}

}  // namespace client
}  // namespace curvefs
