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

#ifndef CURVEFS_SRC_CLIENT_INODE_WRAPPER_H_
#define CURVEFS_SRC_CLIENT_INODE_WRAPPER_H_

#include <sys/stat.h>

#include <utility>
#include <memory>
#include <string>

#include "curvefs/src/common/define.h"
#include "curvefs/proto/metaserver.pb.h"
#include "curvefs/src/client/error_code.h"
#include "curvefs/src/client/rpcclient/metaserver_client.h"
#include "src/common/concurrent/concurrent.h"
#include "curvefs/src/client/volume/extent_cache.h"

using ::curvefs::metaserver::Inode;
using ::curvefs::metaserver::InodeOpenStatusChange;
using ::curvefs::metaserver::VolumeExtentList;
using ::curvefs::metaserver::S3ChunkInfoList;
using ::curvefs::metaserver::S3ChunkInfo;

namespace curvefs {
namespace client {

enum InodeStatus {
    Normal = 0,
    Error = -1,
};

// TODO(xuchaojie) : get from conf maybe?
const uint32_t kOptimalIOBlockSize = 0x10000u;

using rpcclient::MetaServerClient;
using rpcclient::MetaServerClientImpl;

std::ostream &operator<<(std::ostream &os, const struct stat &attr);
void AppendS3ChunkInfoToMap(uint64_t chunkIndex, const S3ChunkInfo &info,
    google::protobuf::Map<uint64_t, S3ChunkInfoList> *s3ChunkInfoMap);

class InodeWrapper : public std::enable_shared_from_this<InodeWrapper> {
 public:
    InodeWrapper(const Inode &inode,
        const std::shared_ptr<MetaServerClient> &metaClient)
      : inode_(inode),
        status_(InodeStatus::Normal),
        metaClient_(metaClient),
        openCount_(0),
        dirty_(false) {
        BuildExtentCache();
        }

    InodeWrapper(Inode &&inode,
        const std::shared_ptr<MetaServerClient> &metaClient)
      : inode_(std::move(inode)),
        status_(InodeStatus::Normal),
        metaClient_(metaClient),
        openCount_(0),
        dirty_(false) {
        BuildExtentCache();
        }

    ~InodeWrapper() {}

    uint64_t GetInodeId() const {
        return inode_.inodeid();
    }

    uint32_t GetFsId() const {
        return inode_.fsid();
    }

    FsFileType GetType() const {
        return inode_.type();
    }

    std::string GetSymlinkStr() const {
        curve::common::UniqueLock lg(mtx_);
        return inode_.symlink();
    }

    void SetLength(uint64_t len) {
        inode_.set_length(len);
        dirty_ = true;
    }

    void SetType(FsFileType type) {
        inode_.set_type(type);
        dirty_ = true;
    }

    uint64_t GetLength() const {
        curve::common::UniqueLock lg(mtx_);
        return inode_.length();
    }

    void SetUid(uint32_t uid) {
        inode_.set_uid(uid);
        dirty_ = true;
    }

    void SetGid(uint32_t gid) {
        inode_.set_gid(gid);
        dirty_ = true;
    }

    void SetMode(uint32_t mode) {
        inode_.set_mode(mode);
        dirty_ = true;
    }

    Inode GetInodeUnlocked() const {
        return inode_;
    }

    Inode GetInodeLocked() const {
        curve::common::UniqueLock lg(mtx_);
        return inode_;
    }

    Inode* GetMutableInodeUnlocked() {
        dirty_ = true;
        return &inode_;
    }

    void GetInodeAttrLocked(struct stat *attr) {
        curve::common::UniqueLock lg(mtx_);
        GetInodeAttrUnLocked(attr);
        return;
    }

    void GetInodeAttrUnLocked(struct stat *attr) {
        memset(attr, 0, sizeof(*attr));
        attr->st_ino = inode_.inodeid();
        attr->st_mode = inode_.mode();
        attr->st_nlink = inode_.nlink();
        attr->st_uid = inode_.uid();
        attr->st_gid = inode_.gid();
        attr->st_size = inode_.length();
        attr->st_rdev = inode_.rdev();
        attr->st_atim.tv_sec = inode_.atime();
        attr->st_atim.tv_nsec = inode_.atime_ns();
        attr->st_mtim.tv_sec = inode_.mtime();
        attr->st_mtim.tv_nsec = inode_.mtime_ns();
        attr->st_ctim.tv_sec = inode_.ctime();
        attr->st_ctim.tv_nsec = inode_.ctime_ns();
        attr->st_blksize = kOptimalIOBlockSize;

        switch (inode_.type()) {
            case metaserver::TYPE_S3:
                attr->st_blocks = (inode_.length() + 511) / 512;
                break;
            default:
                attr->st_blocks = 0;
                break;
        }

        VLOG(6) << "GetInodeAttr attr =  " << *attr
                << ", inodeid = " << inode_.inodeid();
        return;
    }

    void GetInodeAttrLocked(InodeAttr *attr) {
        curve::common::UniqueLock lg(mtx_);
        attr->set_inodeid(inode_.inodeid());
        attr->set_fsid(inode_.fsid());
        attr->set_length(inode_.length());
        attr->set_ctime(inode_.ctime());
        attr->set_ctime_ns(inode_.ctime_ns());
        attr->set_mtime(inode_.mtime());
        attr->set_mtime_ns(inode_.mtime_ns());
        attr->set_atime(inode_.atime());
        attr->set_atime_ns(inode_.atime_ns());
        attr->set_uid(inode_.uid());
        attr->set_gid(inode_.gid());
        attr->set_mode(inode_.mode());
        attr->set_nlink(inode_.nlink());
        attr->set_type(inode_.type());
        if (inode_.has_symlink()) {
            attr->set_symlink(inode_.symlink());
        }
        if (inode_.has_rdev()) {
            attr->set_rdev(inode_.rdev());
        }
        if (inode_.has_dtime()) {
            attr->set_dtime(inode_.dtime());
        }
        if (inode_.has_openmpcount()) {
            attr->set_openmpcount(inode_.openmpcount());
        }
    }

    void GetXattrLocked(XAttr *xattr) {
        curve::common::UniqueLock lg(mtx_);
        xattr->set_fsid(inode_.fsid());
        xattr->set_inodeid(inode_.inodeid());
        *(xattr->mutable_xattrinfos()) = inode_.xattr();
    }

    bool GetXattrUnLocked(const char *name, std::string *value) {
        auto it = inode_.xattr().find(name);
        if (it != inode_.xattr().end()) {
            *value = it->second;
            return true;
        }
        return false;
    }

    void UpdateInode(const Inode &inode) {
        inode_ = inode;
        dirty_ = true;
    }

    void SwapInode(Inode *other) {
        inode_.Swap(other);
        dirty_ = true;
    }

    curve::common::UniqueLock GetUniqueLock() {
        return curve::common::UniqueLock(mtx_);
    }

    CURVEFS_ERROR UpdateParentLocked(uint64_t oldParent, uint64_t newParent);

    // dir will not update parent
    CURVEFS_ERROR LinkLocked(uint64_t parent = 0);

    CURVEFS_ERROR UnLinkLocked(uint64_t parent = 0);

    CURVEFS_ERROR IncreaseNLink();

    CURVEFS_ERROR DecreaseNLink();

    CURVEFS_ERROR Sync() {
        VLOG(9) << "sync inode: " << inode_.ShortDebugString();

        switch (inode_.type()) {
            case FsFileType::TYPE_S3: {
                auto ret = SyncAttr();
                if (ret != CURVEFS_ERROR::OK) {
                    return ret;
                }

                return SyncS3ChunkInfo();
            }

            case FsFileType::TYPE_VOLUME: {
                return SyncFullInode();
            }

            default:
                return CURVEFS_ERROR::INVALIDPARAM;
        }
    }

    CURVEFS_ERROR SyncFullInode();

    CURVEFS_ERROR SyncAttr();

    CURVEFS_ERROR SyncS3ChunkInfo();

    void FlushAsync() {
        FlushAttrAsync();

        if (inode_.type() == FsFileType::TYPE_S3) {
            FlushS3ChunkInfoAsync();
        }
    }

    void FlushAttrAsync();

    void FlushXattrAsync();

    void FlushS3ChunkInfoAsync();

    CURVEFS_ERROR RefreshS3ChunkInfo();

    CURVEFS_ERROR Open();

    bool IsOpen();

    CURVEFS_ERROR Release();

    void MarkDirty() {
        dirty_ = true;
    }

    bool IsDirty() const {
        return dirty_;
    }

    void AppendS3ChunkInfo(uint64_t chunkIndex, const S3ChunkInfo &info) {
        curve::common::UniqueLock lg(mtx_);
        AppendS3ChunkInfoToMap(chunkIndex, info, &s3ChunkInfoAdd_);
        AppendS3ChunkInfoToMap(chunkIndex, info,
            inode_.mutable_s3chunkinfomap());
    }

    void MarkInodeError() {
        // TODO(xuchaojie) : when inode is marked error, prevent futher write.
        status_ = InodeStatus::Error;
    }

    void LockSyncingInode() const {
        syncingInodeMtx_.lock();
    }

    void ReleaseSyncingInode() const {
        syncingInodeMtx_.unlock();
    }

    void LockSyncingXattr() const {
        syncingXattrMtx_.lock();
    }

    void ReleaseSyncingXattr() const {
        syncingXattrMtx_.unlock();
    }

    curve::common::UniqueLock GetSyncingInodeUniqueLock() {
        return curve::common::UniqueLock(syncingInodeMtx_);
    }

    void LockSyncingS3ChunkInfo() const {
        syncingS3ChunkInfoMtx_.lock();
    }

    void ReleaseSyncingS3ChunkInfo() const {
        syncingS3ChunkInfoMtx_.unlock();
    }

    curve::common::UniqueLock GetSyncingS3ChunkInfoUniqueLock() {
        return curve::common::UniqueLock(syncingS3ChunkInfoMtx_);
    }

    void SetOpenCount(uint32_t openCount) { openCount_ = openCount; }

    ExtentCache* GetMutableExtentCache() {
        curve::common::UniqueLock lk(mtx_);
        dirty_ = true;
        return &extentCache_;
    }

 private:
    CURVEFS_ERROR UpdateInodeStatus(InodeOpenStatusChange statusChange);

    void BuildExtentCache();

    // TODO(wuhanqing): separate `volumeextentmap` from `Inode`
    void AddVolumeExtentMapToInode();

 private:
    Inode inode_;
    uint32_t openCount_;
    InodeStatus status_;

    google::protobuf::Map<uint64_t, S3ChunkInfoList> s3ChunkInfoAdd_;

    std::shared_ptr<MetaServerClient> metaClient_;
    bool dirty_;
    mutable ::curve::common::Mutex mtx_;

    mutable ::curve::common::Mutex syncingInodeMtx_;
    mutable ::curve::common::Mutex syncingXattrMtx_;
    mutable ::curve::common::Mutex syncingS3ChunkInfoMtx_;

    mutable ::curve::common::Mutex syncingVolumeExtentsMtx_;
    ExtentCache extentCache_;
};

}  // namespace client
}  // namespace curvefs

#endif  // CURVEFS_SRC_CLIENT_INODE_WRAPPER_H_
