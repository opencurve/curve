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

#include <string>
#include <memory>

#include "curvefs/src/client/fuse_s3_client.h"

namespace curvefs {
namespace client {

CURVEFS_ERROR FuseS3Client::Init(const FuseClientOption &option) {
    CURVEFS_ERROR ret = FuseClient::Init(option);
    if (ret != CURVEFS_ERROR::OK) {
        return ret;
    }
    S3ClientAdaptorOption s3AdaptorOption;
    s3AdaptorOption.blockSize = option.s3Opt.blocksize;
    s3AdaptorOption.chunkSize = option.s3Opt.chunksize;
    // TODO(huyao) : s3Adaptor should not need metaServerEps
    // s3AdaptorOption.metaServerEps = option.metaOpt.msaddr;
    s3AdaptorOption.allocateServerEps = option.spaceOpt.spaceaddr;

    s3AdaptorOption.diskCacheOpt.cacheDir =
        option.s3Opt.diskCacheOpt.cacheDir;
    s3AdaptorOption.diskCacheOpt.enableDiskCache =
        option.s3Opt.diskCacheOpt.enableDiskCache;
     s3AdaptorOption.diskCacheOpt.forceFlush =
        option.s3Opt.diskCacheOpt.forceFlush;
     s3AdaptorOption.diskCacheOpt.fullRatio =
        option.s3Opt.diskCacheOpt.fullRatio;
     s3AdaptorOption.diskCacheOpt.safeRatio =
        option.s3Opt.diskCacheOpt.safeRatio;

    s3Client_ = std::make_shared<S3ClientImpl>();
    s3Client_->Init(option.s3Opt.s3AdaptrOpt);

    s3Adaptor_->Init(s3AdaptorOption, s3Client_.get(), inodeManager_);
    return ret;
}

void FuseS3Client::UnInit() {
    s3Adaptor_->Stop();
    FuseClient::UnInit();
}

void FuseS3Client::FuseOpInit(void *userdata, struct fuse_conn_info *conn) {
    struct MountOption *mOpts = (struct MountOption *) userdata;
    std::string mountPointStr =
        (mOpts->mountPoint == nullptr) ? "" : mOpts->mountPoint;
    std::string fsName = (mOpts->fsName == nullptr) ? "" : mOpts->fsName;
    std::string user = (mOpts->user == nullptr) ? "" : mOpts->user;

    std::string mountPointWithHost;
    int retVal = AddHostNameToMountPointStr(mountPointStr, &mountPointWithHost);
    if (retVal < 0) {
        return;
    }

    FsInfo fsInfo;
    FSStatusCode ret = mdsClient_->GetFsInfo(fsName, &fsInfo);
    if (ret != FSStatusCode::OK) {
        if (FSStatusCode::NOT_FOUND == ret) {
            LOG(INFO) << "The fsName not exist, try to CreateFs"
                      << ", fsName = " << fsName;

            ::curvefs::common::S3Info s3Info;
            s3Info.set_ak(option_.s3Opt.s3AdaptrOpt.ak);
            s3Info.set_sk(option_.s3Opt.s3AdaptrOpt.sk);
            s3Info.set_endpoint(option_.s3Opt.s3AdaptrOpt.s3Address);
            s3Info.set_bucketname(option_.s3Opt.s3AdaptrOpt.bucketName);
            s3Info.set_blocksize(option_.s3Opt.blocksize);
            s3Info.set_chunksize(option_.s3Opt.chunksize);
            // TODO(xuchaojie) : where to get 4096?
            ret = mdsClient_->CreateFsS3(fsName, 4096, s3Info);

            if (ret != FSStatusCode::OK) {
                LOG(ERROR) << "CreateFs failed, ret = " << ret
                           << ", fsName = " << fsName;
                return;
            }
        } else {
            LOG(ERROR) << "GetFsInfo failed, ret = " << ret
                       << ", fsName = " << fsName;
            return;
        }
    }
    ret = mdsClient_->MountFs(fsName, mountPointWithHost, &fsInfo);
    if (ret != FSStatusCode::OK) {
        LOG(ERROR) << "MountFs failed, ret = " << ret
                   << ", fsName = " << fsName
                   << ", mountPoint = " << mountPointWithHost;
        return;
    }
    fsInfo_ = std::make_shared<FsInfo>(fsInfo);
    inodeManager_->SetFsId(fsInfo.fsid());
    dentryManager_->SetFsId(fsInfo.fsid());
    LOG(INFO) << "Mount " << fsName
              << " on " << mountPointWithHost
              << " success!";
    return;
}

void FuseS3Client::FuseOpDestroy(void *userdata) {
    struct MountOption *mOpts = (struct MountOption *) userdata;
    std::string fsName = (mOpts->fsName == nullptr) ? "" : mOpts->fsName;
    std::string mountPointStr =
        (mOpts->mountPoint == nullptr) ? "" : mOpts->mountPoint;

    std::string mountPointWithHost;
    int retVal = AddHostNameToMountPointStr(mountPointStr, &mountPointWithHost);
    if (retVal < 0) {
        return;
    }

    FSStatusCode ret = mdsClient_->UmountFs(fsInfo_->fsname(),
        mountPointWithHost);
    if (ret != FSStatusCode::OK) {
        LOG(ERROR) << "UmountFs failed, ret = " << ret
                   << ", fsName = " << fsName
                   << ", mountPoint = " << mountPointWithHost;
        return;
    }

    FlushAll();

    dirBuf_->DirBufferFreeAll();

    LOG(INFO) << "Umount " << fsName
              << " on " << mountPointWithHost
              << " success!";
    return;
}

CURVEFS_ERROR FuseS3Client::FuseOpWrite(fuse_req_t req, fuse_ino_t ino,
    const char *buf, size_t size, off_t off,
    struct fuse_file_info *fi, size_t *wSize) {
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

    ::curve::common::UniqueLock lgGuard = inodeWrapper->GetUniqueLock();
    Inode inode = inodeWrapper->GetInodeUnlocked();

    int wRet = s3Adaptor_->Write(&inode, off, size, buf);
    if (wRet < 0) {
        LOG(ERROR) << "s3Adaptor_ write failed, ret = " << wRet;
        return CURVEFS_ERROR::INTERNAL;
    }
    *wSize = wRet;
    // update file len
    if (inode.length() < off + size) {
        inode.set_length(off + size);
    }
    uint64_t nowTime = TimeUtility::GetTimeofDaySec();
    inode.set_mtime(nowTime);
    inode.set_ctime(nowTime);

    inodeWrapper->SwapInode(&inode);
    dirtyMap_.emplace(inodeWrapper->GetInodeId(), inodeWrapper);

    if (fi->flags & O_DIRECT || fi->flags & O_SYNC || fi->flags & O_DSYNC) {
        // Todo: do some cache flush later
    }
    return ret;
}

CURVEFS_ERROR FuseS3Client::FuseOpRead(fuse_req_t req,
    fuse_ino_t ino, size_t size, off_t off,
    struct fuse_file_info *fi,
    char *buffer, size_t *rSize) {
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

    ::curve::common::UniqueLock lgGuard = inodeWrapper->GetUniqueLock();
    Inode inode = inodeWrapper->GetInodeUnlocked();

    size_t len = 0;
    if (inode.length() < off + size) {
        len = inode.length() - off;
    } else {
        len = size;
    }
    int rRet = s3Adaptor_->Read(&inode, off, len, buffer);
    if (rRet < 0) {
        LOG(ERROR) << "s3Adaptor_ read failed, ret = " << rRet;
        return CURVEFS_ERROR::INTERNAL;
    }
    *rSize = rRet;

    uint64_t nowTime = TimeUtility::GetTimeofDaySec();
    inode.set_ctime(nowTime);
    inode.set_atime(nowTime);

    inodeWrapper->SwapInode(&inode);
    dirtyMap_.emplace(inodeWrapper->GetInodeId(), inodeWrapper);

    LOG(INFO) << "read end, read size = " << *rSize;
    return ret;
}

CURVEFS_ERROR FuseS3Client::FuseOpCreate(fuse_req_t req, fuse_ino_t parent,
    const char *name, mode_t mode, struct fuse_file_info *fi,
    fuse_entry_param *e) {
    CURVEFS_ERROR ret = MakeNode(
        req, parent, name, mode, FsFileType::TYPE_S3, e);
    if (ret != CURVEFS_ERROR::OK) {
        return ret;
    }
    return FuseOpOpen(req, e->ino, fi);
}

CURVEFS_ERROR FuseS3Client::FuseOpMkNod(fuse_req_t req, fuse_ino_t parent,
        const char *name, mode_t mode, dev_t rdev,
        fuse_entry_param *e) {
    return MakeNode(req, parent, name, mode, FsFileType::TYPE_S3, e);
}

CURVEFS_ERROR FuseS3Client::FuseOpFsync(fuse_req_t req, fuse_ino_t ino,
    int datasync, struct fuse_file_info *fi) {
    LOG(INFO) << "fsync, ino = " << ino
              << ", datasync = " << datasync;
    std::shared_ptr<InodeWrapper> inodeWrapper;
    CURVEFS_ERROR ret = inodeManager_->GetInode(ino, inodeWrapper);
    if (ret != CURVEFS_ERROR::OK) {
        LOG(ERROR) << "inodeManager get inode fail, ret = " << ret
                  << ", inodeid = " << ino;
        return ret;
    }
    ::curve::common::UniqueLock lgGuard = inodeWrapper->GetUniqueLock();
    Inode inode = inodeWrapper->GetInodeUnlocked();
    ret = s3Adaptor_->Flush(&inode);
    if (ret != CURVEFS_ERROR::OK) {
        LOG(ERROR) << "s3Adaptor_ flush failed, ret = " << ret
                  << ", inodeid = " << inode.inodeid();
        return ret;
    }
    inodeWrapper->SwapInode(&inode);
    if (datasync != 0) {
        return CURVEFS_ERROR::OK;
    }
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
