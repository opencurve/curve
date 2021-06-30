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
    s3AdaptorOption.metaServerEps = option.metaOpt.msaddr;
    s3AdaptorOption.allocateServerEps = option.spaceOpt.spaceaddr;

    s3Client_ = std::make_shared<S3ClientImpl>();
    s3Client_->Init(option.s3Opt.s3AdaptrOpt);

    s3Adaptor_->Init(s3AdaptorOption, s3Client_.get());
    return ret;
}

void FuseS3Client::UnInit() {
    FuseClient::UnInit();
}

void FuseS3Client::FuseOpInit(void *userdata, struct fuse_conn_info *conn) {
    struct MountOption *mOpts = (struct MountOption *) userdata;
    std::string mountPointStr =
        (mOpts->mountPoint == nullptr) ? "" : mOpts->mountPoint;
    std::string fsName = (mOpts->fsName == nullptr) ? "" : mOpts->fsName;
    std::string user = (mOpts->user == nullptr) ? "" : mOpts->user;

    FsInfo fsInfo;
    CURVEFS_ERROR ret = mdsClient_->GetFsInfo(fsName, &fsInfo);
    if (ret != CURVEFS_ERROR::OK) {
        if (CURVEFS_ERROR::NOTEXIST == ret) {
            LOG(INFO) << "The fsName not exist, try to CreateFs"
                      << ", fsName = " << fsName;

            S3Info s3Info;
            s3Info.set_ak(option_.s3Opt.s3AdaptrOpt.ak);
            s3Info.set_sk(option_.s3Opt.s3AdaptrOpt.sk);
            s3Info.set_endpoint(option_.s3Opt.s3AdaptrOpt.s3Address);
            s3Info.set_bucketname(option_.s3Opt.s3AdaptrOpt.bucketName);
            s3Info.set_blocksize(option_.s3Opt.blocksize);
            s3Info.set_chunksize(option_.s3Opt.chunksize);
            // TODO(xuchaojie) : where to get 4096?
            ret = mdsClient_->CreateFsS3(fsName, 4096, s3Info);

            if (ret != CURVEFS_ERROR::OK) {
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
    ret = mdsClient_->MountFs(fsName, mountPointStr, &fsInfo);
    if (ret != CURVEFS_ERROR::OK) {
        LOG(ERROR) << "MountFs failed, ret = " << ret
                   << ", fsName = " << fsName
                   << ", mountPoint = " << mountPointStr;
        return;
    }
    fsInfo_ = std::make_shared<FsInfo>(fsInfo);
    inodeManager_->SetFsId(fsInfo.fsid());
    dentryManager_->SetFsId(fsInfo.fsid());

    LOG(INFO) << "Mount " << fsName
              << " on " << mountPointStr
              << " success!";
    return;
}

void FuseS3Client::FuseOpDestroy(void *userdata) {
    struct MountOption *mOpts = (struct MountOption *) userdata;
    std::string fsName = (mOpts->fsName == nullptr) ? "" : mOpts->fsName;
    std::string mountPointStr =
        (mOpts->mountPoint == nullptr) ? "" : mOpts->mountPoint;
    CURVEFS_ERROR ret = mdsClient_->UmountFs(fsInfo_->fsname(),
        mountPointStr);
    if (ret != CURVEFS_ERROR::OK) {
        LOG(ERROR) << "UmountFs failed, ret = " << ret
                   << ", fsName = " << fsName
                   << ", mountPoint = " << mountPointStr;
        return;
    }

    LOG(INFO) << "Umount " << fsName
              << " on " << mountPointStr
              << " success!";
    return;
}

CURVEFS_ERROR FuseS3Client::FuseOpWrite(fuse_req_t req, fuse_ino_t ino,
    const char *buf, size_t size, off_t off,
    struct fuse_file_info *fi, size_t *wSize) {
    Inode inode;
    CURVEFS_ERROR ret = inodeManager_->GetInode(ino, &inode);
    if (ret != CURVEFS_ERROR::OK) {
        LOG(ERROR) << "inodeManager get inode fail, ret = " << ret
                  << ", inodeid = " << ino;
        return ret;
    }
    int wRet = s3Adaptor_->Write(&inode, off, size, buf);
    if (wRet < 0) {
        LOG(ERROR) << "s3Adaptor_ write failed, ret = " << wRet;
        return CURVEFS_ERROR::FAILED;
    }
    *wSize = wRet;
    // update file len
    if (inode.length() < off + size) {
        inode.set_length(off + size);
    }

    LOG(INFO) << "UpdateInode inode = " << inode.DebugString();
    ret = inodeManager_->UpdateInode(inode);
    if (ret != CURVEFS_ERROR::OK) {
        LOG(ERROR) << "UpdateInode fail, ret = " << ret;
        return ret;
    }
    return ret;
}

CURVEFS_ERROR FuseS3Client::FuseOpRead(fuse_req_t req,
        fuse_ino_t ino, size_t size, off_t off,
        struct fuse_file_info *fi,
        char *buffer,
        size_t *rSize) {
    Inode inode;
    CURVEFS_ERROR ret = inodeManager_->GetInode(ino, &inode);
    if (ret != CURVEFS_ERROR::OK) {
        LOG(ERROR) << "inodeManager get inode fail, ret = " << ret
                  << ", inodeid = " << ino;
        return ret;
    }
    size_t len = 0;
    if (inode.length() < off + size) {
        len = inode.length() - off;
    } else {
        len = size;
    }
    int rRet = s3Adaptor_->Read(&inode, off, len, buffer);
    if (rRet < 0) {
        LOG(ERROR) << "s3Adaptor_ read failed, ret = " << rRet;
        return CURVEFS_ERROR::FAILED;
    }
    *rSize = rRet;

    LOG(INFO) << "UpdateInode inode = " << inode.DebugString();
    ret = inodeManager_->UpdateInode(inode);
    if (ret != CURVEFS_ERROR::OK) {
        LOG(ERROR) << "UpdateInode fail, ret = " << ret;
        return ret;
    }
    LOG(INFO) << "read end, read size = " << *rSize;
    return ret;
}

CURVEFS_ERROR FuseS3Client::FuseOpCreate(fuse_req_t req, fuse_ino_t parent,
    const char *name, mode_t mode, struct fuse_file_info *fi,
    fuse_entry_param *e) {
    return MakeNode(req, parent, name, mode, FsFileType::TYPE_S3, e);
}

CURVEFS_ERROR FuseS3Client::FuseOpMkNod(fuse_req_t req, fuse_ino_t parent,
        const char *name, mode_t mode, dev_t rdev,
        fuse_entry_param *e) {
    return MakeNode(req, parent, name, mode, FsFileType::TYPE_S3, e);
}

}  // namespace client
}  // namespace curvefs
