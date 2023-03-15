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


#include <memory>
#include <vector>

#include "curvefs/src/client/fuse_s3_client.h"
#include "curvefs/src/client/kvclient/memcache_client.h"

namespace curvefs {
namespace client {
namespace common {

DECLARE_bool(enableCto);
DECLARE_bool(supportKVcache);

}  // namespace common
}  // namespace client
}  // namespace curvefs

namespace curvefs {
namespace client {

using curvefs::client::common::FLAGS_supportKVcache;
using curvefs::client::common::FLAGS_enableCto;
using curvefs::mds::topology::MemcacheClusterInfo;
using curvefs::mds::topology::MemcacheServerInfo;

CURVEFS_ERROR FuseS3Client::Init(const FuseClientOption &option) {
    FuseClientOption opt(option);

    CURVEFS_ERROR ret = FuseClient::Init(opt);
    if (ret != CURVEFS_ERROR::OK) {
        return ret;
    }

    // init kvcache
    if (FLAGS_supportKVcache && !InitKVCache(option.kvClientManagerOpt)) {
        return CURVEFS_ERROR::INTERNAL;
    }

    // set fs S3Option
    const auto& s3Info = fsInfo_->detail().s3info();
    ::curve::common::S3InfoOption fsS3Option;
    ::curvefs::client::common::S3Info2FsS3Option(s3Info, &fsS3Option);
    SetFuseClientS3Option(&opt, fsS3Option);

    auto s3Client = std::make_shared<S3ClientImpl>();
    s3Client->Init(opt.s3Opt.s3AdaptrOpt);
    auto fsCacheManager = std::make_shared<FsCacheManager>(
        dynamic_cast<S3ClientAdaptorImpl *>(s3Adaptor_.get()),
        opt.s3Opt.s3ClientAdaptorOpt.readCacheMaxByte,
        opt.s3Opt.s3ClientAdaptorOpt.writeCacheMaxByte);
    if (opt.s3Opt.s3ClientAdaptorOpt.diskCacheOpt.diskCacheType !=
        DiskCacheType::Disable) {
        auto s3DiskCacheClient = std::make_shared<S3ClientImpl>();
        s3DiskCacheClient->Init(opt.s3Opt.s3AdaptrOpt);
        auto wrapper = std::make_shared<PosixWrapper>();
        auto diskCacheRead = std::make_shared<DiskCacheRead>();
        auto diskCacheWrite = std::make_shared<DiskCacheWrite>();
        auto diskCacheManager = std::make_shared<DiskCacheManager>(
            wrapper, diskCacheWrite, diskCacheRead);
        auto diskCacheManagerImpl = std::make_shared<DiskCacheManagerImpl>(
            diskCacheManager, s3DiskCacheClient);
        ret = s3Adaptor_->Init(opt.s3Opt.s3ClientAdaptorOpt, s3Client,
                               inodeManager_, mdsClient_, fsCacheManager,
                               diskCacheManagerImpl, true);
    } else {
        ret = s3Adaptor_->Init(opt.s3Opt.s3ClientAdaptorOpt, s3Client,
                               inodeManager_, mdsClient_, fsCacheManager,
                               nullptr, true);
    }
    return ret;
}


bool FuseS3Client::InitKVCache(const KVClientManagerOpt &opt) {
     // get kvcache cluster
    MemcacheClusterInfo kvcachecluster;
    if (!mdsClient_->AllocOrGetMemcacheCluster(fsInfo_->fsid(),
                                               &kvcachecluster)) {
        LOG(ERROR) << "FLAGS_supportKVcache = " << FLAGS_supportKVcache
                   << ", but AllocOrGetMemcacheCluster fail";
        return false;
    }

    // init kvcache client
    auto memcacheClient = std::make_shared<MemCachedClient>();
    if (!memcacheClient->Init(kvcachecluster)) {
        LOG(ERROR) << "FLAGS_supportKVcache = " << FLAGS_supportKVcache
                   << ", but init memcache client fail";
        return false;
    }

    g_kvClientManager = new KVClientManager();
    if (!g_kvClientManager->Init(opt, memcacheClient)) {
        LOG(ERROR) << "FLAGS_supportKVcache = " << FLAGS_supportKVcache
                   << ", but init kvClientManager fail";
        return false;
    }

    return true;
}

void FuseS3Client::UnInit() {
    FuseClient::UnInit();
    s3Adaptor_->Stop();
    curve::common::S3Adapter::Shutdown();
}

CURVEFS_ERROR FuseS3Client::FuseOpInit(void *userdata,
                                       struct fuse_conn_info *conn) {
    CURVEFS_ERROR ret = FuseClient::FuseOpInit(userdata, conn);
    if (init_) {
        s3Adaptor_->SetFsId(fsInfo_->fsid());
        s3Adaptor_->InitMetrics(fsInfo_->fsname());
    }
    return ret;
}

CURVEFS_ERROR FuseS3Client::FuseOpWrite(fuse_req_t req, fuse_ino_t ino,
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
    int wRet = s3Adaptor_->Write(ino, off, size, buf);
    if (wRet < 0) {
        LOG(ERROR) << "s3Adaptor_ write failed, ret = " << wRet;
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

CURVEFS_ERROR FuseS3Client::FuseOpRead(fuse_req_t req, fuse_ino_t ino,
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

    // Read do not change inode. so we do not get lock here.
    int rRet = s3Adaptor_->Read(ino, off, len, buffer);
    if (rRet < 0) {
        LOG(ERROR) << "s3Adaptor_ read failed, ret = " << rRet;
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

CURVEFS_ERROR FuseS3Client::FuseOpCreate(fuse_req_t req, fuse_ino_t parent,
                                         const char *name, mode_t mode,
                                         struct fuse_file_info *fi,
                                         fuse_entry_param *e) {
    VLOG(1) << "FuseOpCreate, parent: " << parent << ", name: " << name
            << ", mode: " << mode;
    CURVEFS_ERROR ret =
        MakeNode(req, parent, name, mode, FsFileType::TYPE_S3, 0, false, e);
    if (ret != CURVEFS_ERROR::OK) {
        return ret;
    }
    return FuseOpOpen(req, e->ino, fi);
}

CURVEFS_ERROR FuseS3Client::FuseOpMkNod(fuse_req_t req, fuse_ino_t parent,
                                        const char *name, mode_t mode,
                                        dev_t rdev, fuse_entry_param *e) {
    VLOG(1) << "FuseOpMkNod, parent: " << parent << ", name: " << name
            << ", mode: " << mode << ", rdev: " << rdev;
    return MakeNode(req, parent, name, mode, FsFileType::TYPE_S3, rdev, false,
                    e);
}

CURVEFS_ERROR FuseS3Client::FuseOpLink(fuse_req_t req, fuse_ino_t ino,
                                     fuse_ino_t newparent, const char *newname,
                                     fuse_entry_param *e) {
    VLOG(1) << "FuseOpLink, ino: " << ino << ", newparent: " << newparent
            << ", newname: " << newname;
    return FuseClient::FuseOpLink(
        req, ino, newparent, newname, FsFileType::TYPE_S3, e);
}

CURVEFS_ERROR FuseS3Client::FuseOpUnlink(fuse_req_t req, fuse_ino_t parent,
                                         const char *name) {
    VLOG(1) << "FuseOpUnlink, parent: " << parent << ", name: " << name;
    return RemoveNode(req, parent, name, FsFileType::TYPE_S3);
}

CURVEFS_ERROR FuseS3Client::FuseOpFsync(fuse_req_t req, fuse_ino_t ino,
                                        int datasync,
                                        struct fuse_file_info *fi) {
    VLOG(1) << "FuseOpFsync, ino: " << ino << ", datasync: " << datasync;

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

CURVEFS_ERROR FuseS3Client::Truncate(InodeWrapper *inode, uint64_t length) {
    return s3Adaptor_->Truncate(inode, length);
}

CURVEFS_ERROR FuseS3Client::FuseOpFlush(fuse_req_t req, fuse_ino_t ino,
                                        struct fuse_file_info *fi) {
    VLOG(1) << "FuseOpFlush, ino: " << ino;
    CURVEFS_ERROR ret = CURVEFS_ERROR::OK;

    // if enableCto, flush all write cache both in memory cache and disk cache
    if (FLAGS_enableCto) {
        ret = s3Adaptor_->FlushAllCache(ino);
        if (ret != CURVEFS_ERROR::OK) {
            LOG(ERROR) << "FuseOpFlush, flush all cache fail, ret = " << ret
                       << ", ino: " << ino;
            return ret;
        }
        VLOG(3) << "FuseOpFlush, flush to s3 ok";

        std::shared_ptr<InodeWrapper> inodeWrapper;
        ret = inodeManager_->GetInode(ino, inodeWrapper);
        if (ret != CURVEFS_ERROR::OK) {
            LOG(ERROR) << "FuseOpFlush, inodeManager get inode fail, ret = "
                       << ret << ", ino: " << ino;
            return ret;
        }

        ::curve::common::UniqueLock lgGuard = inodeWrapper->GetUniqueLock();
        ret = inodeWrapper->Sync();
        if (ret != CURVEFS_ERROR::OK) {
            LOG(ERROR) << "FuseOpFlush, inode sync s3 chunk info fail, ret = "
                       << ret << ", ino: " << ino;
            return ret;
        }
    // if disableCto, flush just flush data in memory
    } else {
        ret = s3Adaptor_->Flush(ino);
        if (ret != CURVEFS_ERROR::OK) {
            LOG(ERROR) << "FuseOpFlush, flush to diskcache failed, ret = "
                       << ret << ", ino: " << ino;
            return ret;
        }
    }

    VLOG(1) << "FuseOpFlush, ino: " << ino << " flush ok";
    fs_->Close(ino);
    return CURVEFS_ERROR::OK;
}

void FuseS3Client::FlushData() {
    CURVEFS_ERROR ret = CURVEFS_ERROR::UNKNOWN;
    do {
        ret = s3Adaptor_->FsSync();
    } while (ret != CURVEFS_ERROR::OK);
}

}  // namespace client
}  // namespace curvefs
