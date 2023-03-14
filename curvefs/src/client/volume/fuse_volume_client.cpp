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

#include "curvefs/src/client/volume/fuse_volume_client.h"

#include <butil/time.h>
#include <bvar/bvar.h>

#include <memory>
#include <string>

#include "absl/cleanup/cleanup.h"
#include "absl/memory/memory.h"
#include "curvefs/proto/mds.pb.h"
#include "curvefs/src/client/error_code.h"
#include "curvefs/src/client/volume/default_volume_storage.h"
#include "curvefs/src/client/volume/extent_cache.h"
#include "curvefs/src/volume/common.h"
#include "curvefs/src/volume/option.h"

namespace curvefs {
namespace client {

using ::curvefs::volume::SpaceManagerImpl;
using ::curvefs::volume::SpaceManagerOption;
using ::curvefs::volume::BlockDeviceClientOptions;
using ::curvefs::volume::BlockDeviceClientImpl;

CURVEFS_ERROR FuseVolumeClient::Init(const FuseClientOption &option) {
    CURVEFS_ERROR ret = FuseClient::Init(option);
    if (ret != CURVEFS_ERROR::OK) {
        LOG(ERROR) << "Init failed: " << ret;
        return ret;
    }

    auto fsCacheManager = std::make_shared<FsCacheManager>(
        dynamic_cast<StorageAdaptor*>(storageAdaptor_.get()),
        option.s3Opt.s3ClientAdaptorOpt.readCacheMaxByte,
        option.s3Opt.s3ClientAdaptorOpt.writeCacheMaxByte,
        nullptr);  // bs no need cache cluster
    ret = storageAdaptor_->Init(option,
        inodeManager_, mdsClient_, fsCacheManager,
        nullptr, nullptr, fsInfo_);  // no need cache cluster and diskcache

    return ret;
}

void FuseVolumeClient::UnInit() {
    storageAdaptor_->Stop();
    FuseClient::UnInit();
}

CURVEFS_ERROR FuseVolumeClient::FuseOpInit(void *userdata,
                                           struct fuse_conn_info *conn) {
    auto ret = FuseClient::FuseOpInit(userdata, conn);
    if (ret != CURVEFS_ERROR::OK) {
        LOG(ERROR) << "fuse op init failed, error: " << ret;
        return ret;
    }
    Mountpoint mountPoint = GetMountPoint();
    std::string mountOwner = mountPoint.hostname() + ":" +
        std::to_string(mountPoint.port()) +
        ":" + mountPoint.path();
    storageAdaptor_->SetMountOwner(mountOwner);
    storageAdaptor_->FuseOpInit(userdata, conn);
    LOG(INFO) << "fuse op Init success: " << mountOwner;
    return CURVEFS_ERROR::OK;
}

CURVEFS_ERROR FuseVolumeClient::FuseOpWrite(fuse_req_t req, fuse_ino_t ino,
        const char *buf, size_t size, off_t off,
        struct fuse_file_info *fi,
        size_t *wSize) {
    // check align
    if (fi->flags & O_DIRECT) {
        if (!(is_aligned(off, DirectIOAlignment) &&
              is_aligned(size, DirectIOAlignment)))
            return CURVEFS_ERROR::INVALIDPARAM;
    }
    uint64_t start = butil::cpuwide_time_us();
    int wRet = storageAdaptor_->Write(ino, off, size, buf);
    if (wRet < 0) {
        LOG(ERROR) << "storageAdaptor_ write failed, ret = " << wRet;
        return CURVEFS_ERROR::INTERNAL;
    }

    if (fsMetric_.get() != nullptr) {
        fsMetric_->userWrite.bps.count << wRet;
        fsMetric_->userWrite.qps.count << 1;
        uint64_t duration = butil::cpuwide_time_us() - start;
        fsMetric_->userWrite.latency << duration;
        fsMetric_->userWriteIoSize.set_value(wRet);
    }

    std::shared_ptr<InodeWrapper> inodeWrapper;
    CURVEFS_ERROR ret = inodeManager_->GetInode(ino, inodeWrapper);
    if (ret != CURVEFS_ERROR::OK) {
        LOG(ERROR) << "inodeManager get inode fail, ret = " << ret
                   << ", inodeid = " << ino;
        return ret;
    }

    ::curve::common::UniqueLock lgGuard = inodeWrapper->GetUniqueLock();

    *wSize = wRet;
    size_t changeSize = 0;
    // update file len
    if (inodeWrapper->GetLengthLocked() < off + *wSize) {
        changeSize = off + *wSize - inodeWrapper->GetLengthLocked();
        inodeWrapper->SetLengthLocked(off + *wSize);
    }

    inodeWrapper->UpdateTimestampLocked(kModifyTime | kChangeTime);

    inodeManager_->ShipToFlush(inodeWrapper);

    if (fi->flags & O_DIRECT || fi->flags & O_SYNC || fi->flags & O_DSYNC) {
        // Todo: do some cache flush later
    }

    if (enableSumInDir_ && changeSize != 0) {
        const Inode* inode = inodeWrapper->GetInodeLocked();
        XAttr xattr;
        xattr.mutable_xattrinfos()->insert({XATTRFBYTES,
            std::to_string(changeSize)});
        for (const auto &it : inode->parent()) {
            auto tret = xattrManager_->UpdateParentInodeXattr(it, xattr, true);
            if (tret != CURVEFS_ERROR::OK) {
                LOG(ERROR) << "UpdateParentInodeXattr failed,"
                           << " inodeId = " << it
                           << ", xattr = " << xattr.DebugString();
            }
        }
    }
    return ret;
}

CURVEFS_ERROR FuseVolumeClient::FuseOpRead(fuse_req_t req, fuse_ino_t ino,
                                       size_t size, off_t off,
                                       struct fuse_file_info *fi, char *buffer,
                                       size_t *rSize) {
    // check align
    if (fi->flags & O_DIRECT) {
        if (!(is_aligned(off, DirectIOAlignment) &&
              is_aligned(size, DirectIOAlignment)))
            return CURVEFS_ERROR::INVALIDPARAM;
    }

    uint64_t start = butil::cpuwide_time_us();
    std::shared_ptr<InodeWrapper> inodeWrapper;
    CURVEFS_ERROR ret = inodeManager_->GetInode(ino, inodeWrapper);
    if (ret != CURVEFS_ERROR::OK) {
        LOG(ERROR) << "inodeManager get inode fail, ret = " << ret
                   << ", inodeid = " << ino;
        return ret;
    }
    uint64_t fileSize = inodeWrapper->GetLength();

    size_t len = 0;
    if (fileSize <= off) {
        *rSize = 0;
        return CURVEFS_ERROR::OK;
    } else if (fileSize < off + size) {
        len = fileSize - off;
    } else {
        len = size;
    }

    int rRet = storageAdaptor_->Read(ino, off, len, buffer);
    if (rRet < 0) {
        LOG(ERROR) << "storageAdaptor_ read failed, ret = " << rRet;
        return CURVEFS_ERROR::INTERNAL;
    }
    *rSize = rRet;

    if (fsMetric_.get() != nullptr) {
        fsMetric_->userRead.bps.count << rRet;
        fsMetric_->userRead.qps.count << 1;
        uint64_t duration = butil::cpuwide_time_us() - start;
        fsMetric_->userRead.latency << duration;
        fsMetric_->userReadIoSize.set_value(rRet);
    }

    ::curve::common::UniqueLock lgGuard = inodeWrapper->GetUniqueLock();
    inodeWrapper->UpdateTimestampLocked(kAccessTime);
    inodeManager_->ShipToFlush(inodeWrapper);

    VLOG(9) << "read end, read size = " << *rSize;
    return ret;
}

CURVEFS_ERROR FuseVolumeClient::FuseOpCreate(fuse_req_t req, fuse_ino_t parent,
                                             const char *name, mode_t mode,
                                             struct fuse_file_info *fi,
                                             fuse_entry_param *e) {
    VLOG(3) << "FuseOpCreate, parent: " << parent
              << ", name: " << name
              << ", mode: " << mode;
    CURVEFS_ERROR ret =
        MakeNode(req, parent, name, mode, FsFileType::TYPE_FILE, 0, false, e);
    if (ret != CURVEFS_ERROR::OK) {
        return ret;
    }
    return FuseOpOpen(req, e->ino, fi);
}

CURVEFS_ERROR FuseVolumeClient::FuseOpMkNod(fuse_req_t req, fuse_ino_t parent,
                                            const char *name, mode_t mode,
                                            dev_t rdev, fuse_entry_param *e) {
    VLOG(3) << "FuseOpMkNod, parent: " << parent << ", name: " << name
            << ", mode: " << mode << ", rdev: " << rdev;
    return MakeNode(req, parent, name, mode, FsFileType::TYPE_FILE, rdev,
                    false, e);
}

CURVEFS_ERROR FuseVolumeClient::FuseOpLink(fuse_req_t req, fuse_ino_t ino,
                                     fuse_ino_t newparent, const char *newname,
                                     fuse_entry_param *e) {
    VLOG(1) << "FuseOpLink, ino: " << ino << ", newparent: " << newparent
            << ", newname: " << newname;
    return FuseClient::FuseOpLink(
        req, ino, newparent, newname, FsFileType::TYPE_FILE, e);
}

CURVEFS_ERROR FuseVolumeClient::FuseOpUnlink(fuse_req_t req, fuse_ino_t parent,
                                             const char *name) {
    VLOG(1) << "FuseOpUnlink, parent: " << parent << ", name: " << name;
    return RemoveNode(req, parent, name, FsFileType::TYPE_FILE);
}

CURVEFS_ERROR FuseVolumeClient::FuseOpFsync(fuse_req_t req, fuse_ino_t ino,
                                            int datasync,
                                            struct fuse_file_info *fi) {
    VLOG(3) << "FuseOpFsync start, ino: " << ino << ", datasync: " << datasync;
    CURVEFS_ERROR ret = CURVEFS_ERROR::OK;
    ret = dynamic_cast<VolumeClientAdaptorImpl*>(
      storageAdaptor_.get())->getUnderStorage()->Flush(ino);
    if (ret != CURVEFS_ERROR::OK) {
        LOG(ERROR) << "Storage flush ino: " << ino << " failed, error: " << ret;
        return ret;
    }

    if (datasync) {
        VLOG(3) << "FuseOpFsync end, ino: " << ino
                << ", datasync: " << datasync;
        return CURVEFS_ERROR::OK;
    }

    std::shared_ptr<InodeWrapper> inodeWrapper;
    ret = inodeManager_->GetInode(ino, inodeWrapper);
    if (ret != CURVEFS_ERROR::OK) {
        LOG(ERROR) << "Get inode fail, ino: " << ino << ", ret: " << ret;
        return ret;
    }

    auto lk = inodeWrapper->GetUniqueLock();
    return inodeWrapper->Sync();
}

CURVEFS_ERROR FuseVolumeClient::Truncate(InodeWrapper *inode, uint64_t length) {
    // Todo: call volume truncate
    return CURVEFS_ERROR::OK;
}

CURVEFS_ERROR FuseVolumeClient::FuseOpFlush(fuse_req_t req, fuse_ino_t ino,
                                            struct fuse_file_info *fi) {
    VLOG(9) << "FuseOpFlush, ino: " << ino;

    CURVEFS_ERROR ret =   dynamic_cast< VolumeClientAdaptorImpl *>(
      storageAdaptor_.get())->getUnderStorage()->Flush(ino);
    LOG_IF(ERROR, ret != CURVEFS_ERROR::OK)
        << "Flush error, ino: " << ino << ", error: " << ret;

    return ret;
}

void FuseVolumeClient::FlushData() {
    // TODO(xuchaojie) : flush volume data
}

void FuseVolumeClient::SetSpaceManagerForTesting(SpaceManager *manager) {
    dynamic_cast< VolumeClientAdaptorImpl *>(
      storageAdaptor_.get())->getSpaceManager().reset(manager);
}

void FuseVolumeClient::SetVolumeStorageForTesting(VolumeStorage *storage) {
    dynamic_cast< VolumeClientAdaptorImpl *>(
      storageAdaptor_.get())->getUnderStorage().reset(storage);
}

}  // namespace client
}  // namespace curvefs
