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

#include <cstdint>
#include <memory>
#include <vector>

#include "curvefs/src/client/filesystem/xattr.h"
#include "curvefs/src/client/kvclient/memcache_client.h"
#include "curvefs/src/client/rpcclient/fsdelta_updater.h"
#include "curvefs/src/client/rpcclient/fsquota_checker.h"

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

using curvefs::client::common::FLAGS_enableCto;
using curvefs::client::common::FLAGS_supportKVcache;
using ::curvefs::client::filesystem::XATTR_DIR_FBYTES;
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

    const uint64_t writeCacheMaxByte =
        opt.s3Opt.s3ClientAdaptorOpt.writeCacheMaxByte;
    if (writeCacheMaxByte < MIN_WRITE_CACHE_SIZE) {
        LOG(ERROR) << "writeCacheMaxByte is too small"
                   << ", at least " << MIN_WRITE_CACHE_SIZE << " (8MB)"
                      ", writeCacheMaxByte = " << writeCacheMaxByte;
        return CURVEFS_ERROR::CACHE_TOO_SMALL;
    }

    auto fsCacheManager = std::make_shared<FsCacheManager>(
        dynamic_cast<S3ClientAdaptorImpl *>(s3Adaptor_.get()),
        opt.s3Opt.s3ClientAdaptorOpt.readCacheMaxByte, writeCacheMaxByte,
        opt.s3Opt.s3ClientAdaptorOpt.readCacheThreads, kvClientManager_);
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
                               diskCacheManagerImpl, kvClientManager_, true);
    } else {
        ret = s3Adaptor_->Init(opt.s3Opt.s3ClientAdaptorOpt, s3Client,
                               inodeManager_, mdsClient_, fsCacheManager,
                               nullptr, kvClientManager_, true);
    }
    ioLatencyMetric_ = absl::make_unique<metric::FuseS3ClientIOLatencyMetric>(
        fsInfo_->fsname());
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
    if (!memcacheClient->Init(kvcachecluster, fsInfo_->fsname())) {
        LOG(ERROR) << "FLAGS_supportKVcache = " << FLAGS_supportKVcache
                   << ", but init memcache client fail";
        return false;
    }

    kvClientManager_ = std::make_shared<KVClientManager>();
    if (!kvClientManager_->Init(opt, memcacheClient, fsInfo_->fsname())) {
        LOG(ERROR) << "FLAGS_supportKVcache = " << FLAGS_supportKVcache
                   << ", but init kvClientManager fail";
        return false;
    }

    if (warmupManager_ != nullptr) {
        warmupManager_->SetKVClientManager(kvClientManager_);
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
                                        FileOut* fileOut) {
    size_t *wSize = &fileOut->nwritten;
    // check align
    if (fi->flags & O_DIRECT) {
        if (!(is_aligned(off, DirectIOAlignment) &&
              is_aligned(size, DirectIOAlignment)))
            return CURVEFS_ERROR::INVALID_PARAM;
    }
    uint64_t start = butil::cpuwide_time_us();
    int wRet = s3Adaptor_->Write(ino, off, size, buf);
    if (wRet < 0) {
        LOG(ERROR) << "s3Adaptor_ write failed, ret = " << wRet;
        return CURVEFS_ERROR::INTERNAL;
    }
    uint64_t mid = butil::cpuwide_time_us();
    ioLatencyMetric_->writeDataLatency << mid - start;

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
        if (!FsQuotaChecker::GetInstance().QuotaBytesCheck(changeSize)) {
            return CURVEFS_ERROR::NO_SPACE;
        }
    }

    inodeWrapper->UpdateTimestampLocked(kModifyTime | kChangeTime);

    inodeManager_->ShipToFlush(inodeWrapper);

    if (fi->flags & O_DIRECT || fi->flags & O_SYNC || fi->flags & O_DSYNC) {
        // Todo: do some cache flush later
    }

    if (enableSumInDir_ && changeSize != 0) {
        const Inode* inode = inodeWrapper->GetInodeLocked();
        XAttr xattr;
        xattr.mutable_xattrinfos()->insert(
            {XATTR_DIR_FBYTES, std::to_string(changeSize)});
        for (const auto &it : inode->parent()) {
            auto tret = xattrManager_->UpdateParentInodeXattr(it, xattr, true);
            if (tret != CURVEFS_ERROR::OK) {
                LOG(ERROR) << "UpdateParentInodeXattr failed,"
                           << " inodeId = " << it
                           << ", xattr = " << xattr.DebugString();
            }
        }
    }

    FsDeltaUpdater::GetInstance().UpdateDeltaBytes(changeSize);

    inodeWrapper->GetInodeAttrLocked(&fileOut->attr);

    if (fsMetric_.get() != nullptr) {
        fsMetric_->userWrite.bps.count << wRet;
        fsMetric_->userWrite.qps.count << 1;
        uint64_t duration = butil::cpuwide_time_us() - start;
        fsMetric_->userWrite.latency << duration;
        fsMetric_->userWriteIoSize.set_value(wRet);
    }
    ioLatencyMetric_->writeAttrLatency << butil::cpuwide_time_us() - mid;
    return ret;
}

CURVEFS_ERROR FuseS3Client::FuseOpRead(fuse_req_t req, fuse_ino_t ino,
                                       size_t size, off_t off,
                                       struct fuse_file_info *fi, char *buffer,
                                       size_t *rSize) {
    (void)req;
    // check align
    if (fi->flags & O_DIRECT) {
        if (!(is_aligned(off, DirectIOAlignment) &&
              is_aligned(size, DirectIOAlignment)))
            return CURVEFS_ERROR::INVALID_PARAM;
    }

    uint64_t start = butil::cpuwide_time_us();
    std::shared_ptr<InodeWrapper> inodeWrapper;
    CURVEFS_ERROR ret = inodeManager_->GetInode(ino, inodeWrapper);
    if (ret != CURVEFS_ERROR::OK) {
        LOG(ERROR) << "inodeManager get inode fail, ret = " << ret
                   << ", inodeid = " << ino;
        return ret;
    }
    uint64_t mid = butil::cpuwide_time_us();
    ioLatencyMetric_->readAttrLatency << mid - start;
    uint64_t fileSize = inodeWrapper->GetLength();

    size_t len = 0;
    if (static_cast<int64_t>(fileSize) <= off) {
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
    ioLatencyMetric_->readDataLatency << butil::cpuwide_time_us() - mid;

    ::curve::common::UniqueLock lgGuard = inodeWrapper->GetUniqueLock();
    inodeWrapper->UpdateTimestampLocked(kAccessTime);
    inodeManager_->ShipToFlush(inodeWrapper);

    if (fsMetric_.get() != nullptr) {
        fsMetric_->userRead.bps.count << rRet;
        fsMetric_->userRead.qps.count << 1;
        uint64_t duration = butil::cpuwide_time_us() - start;
        fsMetric_->userRead.latency << duration;
        fsMetric_->userReadIoSize.set_value(rRet);
    }

    VLOG(9) << "read end, read size = " << *rSize;
    return ret;
}

CURVEFS_ERROR FuseS3Client::FuseOpCreate(fuse_req_t req, fuse_ino_t parent,
                                         const char *name, mode_t mode,
                                         struct fuse_file_info *fi,
                                         EntryOut* entryOut) {
    VLOG(1) << "FuseOpCreate, parent: " << parent << ", name: " << name
            << ", mode: " << mode;

    std::shared_ptr<InodeWrapper> inode;
    CURVEFS_ERROR ret =
        MakeNode(req, parent, name, mode, FsFileType::TYPE_S3, 0, false, inode);
    if (ret != CURVEFS_ERROR::OK) {
        return ret;
    }

    auto openFiles = fs_->BorrowMember().openFiles;
    openFiles->Open(inode->GetInodeId(), inode);

    inode->GetInodeAttr(&entryOut->attr);
    return CURVEFS_ERROR::OK;
}

CURVEFS_ERROR FuseS3Client::FuseOpMkNod(fuse_req_t req,
                                        fuse_ino_t parent,
                                        const char* name,
                                        mode_t mode,
                                        dev_t rdev,
                                        EntryOut* entryOut) {
    VLOG(1) << "FuseOpMkNod, parent: " << parent << ", name: " << name
            << ", mode: " << mode << ", rdev: " << rdev;

    std::shared_ptr<InodeWrapper> inode;
    CURVEFS_ERROR rc = MakeNode(req, parent, name, mode,
                                FsFileType::TYPE_S3, rdev, false,
                                inode);
    if (rc != CURVEFS_ERROR::OK) {
        return rc;
    }

    InodeAttr attr;
    inode->GetInodeAttr(&attr);
    *entryOut = EntryOut(attr);
    return CURVEFS_ERROR::OK;
}

CURVEFS_ERROR FuseS3Client::FuseOpLink(fuse_req_t req,
                                       fuse_ino_t ino,
                                       fuse_ino_t newparent,
                                       const char* newname,
                                       EntryOut* entryOut) {
    VLOG(1) << "FuseOpLink, ino: " << ino << ", newparent: " << newparent
            << ", newname: " << newname;
    return FuseClient::FuseOpLink(
        req, ino, newparent, newname, FsFileType::TYPE_S3, entryOut);
}

CURVEFS_ERROR FuseS3Client::FuseOpUnlink(fuse_req_t req, fuse_ino_t parent,
                                         const char *name) {
    VLOG(1) << "FuseOpUnlink, parent: " << parent << ", name: " << name;
    return RemoveNode(req, parent, name, FsFileType::TYPE_S3);
}

CURVEFS_ERROR FuseS3Client::FuseOpFsync(fuse_req_t req, fuse_ino_t ino,
                                        int datasync,
                                        struct fuse_file_info *fi) {
    (void)req;
    (void)fi;
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

CURVEFS_ERROR FuseS3Client::UpdateS3Info(const std::string& fsName,
                                         const curvefs::common::S3Info& s3Info,
                                         FsInfo* fsInfo) {
    ::curve::common::S3InfoOption s3InfoOption;
    ::curvefs::client::common::S3Info2FsS3Option(s3Info, &s3InfoOption);
    FSStatusCode updateStatusCode =
        mdsClient_->UpdateS3Info(fsName, s3Info, fsInfo);
    if (updateStatusCode != FSStatusCode::OK) {
        LOG(ERROR) << "Update s3 info error code: (FSStatusCode)"
                   << updateStatusCode;
        return CURVEFS_ERROR::UPDATE_S3_INFO_FAILED;
    }

    if (option_.s3Opt.s3AdaptrOpt.ak != s3Info.ak() ||
        option_.s3Opt.s3AdaptrOpt.sk != s3Info.sk() ||
        option_.s3Opt.s3AdaptrOpt.s3Address != s3Info.endpoint() ||
        option_.s3Opt.s3AdaptrOpt.bucketName != s3Info.bucketname()) {

        option_.s3Opt.s3AdaptrOpt.s3Address = s3Info.endpoint();
        option_.s3Opt.s3AdaptrOpt.ak = s3Info.ak();
        option_.s3Opt.s3AdaptrOpt.sk = s3Info.sk();
        option_.s3Opt.s3AdaptrOpt.bucketName = s3Info.bucketname();

        s3Adaptor_->GetS3Client()->Reinit(option_.s3Opt.s3AdaptrOpt);
    }

    return CURVEFS_ERROR::OK;
}

CURVEFS_ERROR FuseS3Client::FuseOpFlush(fuse_req_t req, fuse_ino_t ino,
                                        struct fuse_file_info *fi) {
    (void)req;
    (void)fi;
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
