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

#include <gtest/gtest_prod.h>
#include <climits>
#include <cstdint>
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
using ::curvefs::metaserver::S3ChunkInfoList;
using ::curvefs::metaserver::S3ChunkInfo;

namespace curvefs {
namespace client {

constexpr int kAccessTime = 1 << 0;
constexpr int kChangeTime = 1 << 1;
constexpr int kModifyTime = 1 << 2;

#define REFRESH_NLINK_IF_NEED               \
do {                                        \
    if (!isNlinkValid_) {                   \
        CURVEFS_ERROR ret = RefreshNlink(); \
        if (ret != CURVEFS_ERROR::OK) {     \
            return ret;                     \
        }                                   \
    }                                       \
} while (0)                                 \

using ::curvefs::metaserver::VolumeExtentList;

enum class InodeStatus {
    kNormal = 0,
    kError = -1,
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
    InodeWrapper(Inode inode, std::shared_ptr<MetaServerClient> metaClient)
        : inode_(std::move(inode)),
          status_(InodeStatus::kNormal),
          isNlinkValid_(true),
          metaClient_(std::move(metaClient)),
          dirty_(false) {}

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
        curve::common::UniqueLock lg(mtx_);
        SetLengthLocked(len);
    }

    void SetLengthLocked(uint64_t len) {
        inode_.set_length(len);
        dirtyAttr_.set_length(len);
        dirty_ = true;
    }

    void SetType(FsFileType type) {
        inode_.set_type(type);
        dirtyAttr_.set_type(type);
        dirty_ = true;
    }

    uint64_t GetLengthLocked() const {
        return inode_.length();
    }

    uint64_t GetLength() const {
        curve::common::UniqueLock lg(mtx_);
        return inode_.length();
    }

    void SetUid(uint32_t uid) {
        inode_.set_uid(uid);
        dirtyAttr_.set_uid(uid);
        dirty_ = true;
    }

    void SetGid(uint32_t gid) {
        inode_.set_gid(gid);
        dirtyAttr_.set_gid(gid);
        dirty_ = true;
    }

    void SetMode(uint32_t mode) {
        inode_.set_mode(mode);
        dirtyAttr_.set_mode(mode);
        dirty_ = true;
    }

    Inode GetInode() const {
        curve::common::UniqueLock lg(mtx_);
        return inode_;
    }

    // Get an immutable inode.
    //
    // The const return value is used to forbid modify inode through this
    // interface, all modification operations should using `SetXXX()`.
    const Inode* GetInodeLocked() const {
        return &inode_;
    }

    // Update timestamp of inode.
    //
    // flags can be any combination of kAccessTime/kModifyTime/kChangeTime
    void UpdateTimestampLocked(int flags);
    void UpdateTimestampLocked(const timespec& now, int flags);

    // Merge incoming extended attributes.
    //
    // Existing attributes will be overwritten, new attributes well be inserted.
    void MergeXAttrLocked(
        const google::protobuf::Map<std::string, std::string>& xattrs);

    CURVEFS_ERROR GetInodeAttrLocked(InodeAttr *attr) {
        REFRESH_NLINK_IF_NEED;

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
        *(attr->mutable_parent()) = inode_.parent();
        if (inode_.has_symlink()) {
            attr->set_symlink(inode_.symlink());
        }
        if (inode_.has_rdev()) {
            attr->set_rdev(inode_.rdev());
        }
        if (inode_.has_dtime()) {
            attr->set_dtime(inode_.dtime());
        }
        if (inode_.xattr_size() > 0) {
            *(attr->mutable_xattr()) = inode_.xattr();
        }
        return CURVEFS_ERROR::OK;
    }

    void GetInodeAttr(InodeAttr *attr) {
        curve::common::UniqueLock lg(mtx_);
        GetInodeAttrLocked(attr);
    }

    XAttr GetXattr() const {
        XAttr ret;
        curve::common::UniqueLock lg(mtx_);
        ret.set_fsid(inode_.fsid());
        ret.set_inodeid(inode_.inodeid());
        *(ret.mutable_xattrinfos()) = inode_.xattr();
        return ret;
    }

    curve::common::UniqueLock GetUniqueLock() {
        return curve::common::UniqueLock(mtx_);
    }

    const google::protobuf::RepeatedField<uint64_t>& GetParentLocked() {
        return inode_.parent();
    }

    CURVEFS_ERROR UpdateParent(uint64_t oldParent, uint64_t newParent);

    // dir will not update parent
    CURVEFS_ERROR Link(uint64_t parent = 0);

    CURVEFS_ERROR UnLink(uint64_t parent = 0);

    // mark nlink invalid, need to refresh from metaserver
    void InvalidateNlink() {
        isNlinkValid_ = false;
    }

    void ResetNlinkValid() {
        isNlinkValid_ = true;
    }

    bool IsNlinkValid() {
        return isNlinkValid_;
    }

    CURVEFS_ERROR RefreshNlink();

    CURVEFS_ERROR Sync(bool internal = false);

    CURVEFS_ERROR SyncAttr(bool internal = false);

    void FlushAsync();

    void FlushAttrAsync();

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

    void ClearDirty() {
        dirty_ = false;
    }

    bool S3ChunkInfoEmpty() {
        curve::common::UniqueLock lg(mtx_);
        return s3ChunkInfoAdd_.empty();
    }

    bool S3ChunkInfoEmptyNolock() {
        return s3ChunkInfoAdd_.empty();
    }

    void AppendS3ChunkInfo(uint64_t chunkIndex, const S3ChunkInfo &info) {
        curve::common::UniqueLock lg(mtx_);
        AppendS3ChunkInfoToMap(chunkIndex, info, &s3ChunkInfoAdd_);
        AppendS3ChunkInfoToMap(chunkIndex, info,
            inode_.mutable_s3chunkinfomap());
    }

    google::protobuf::Map<uint64_t, S3ChunkInfoList>* GetChunkInfoMap() {
        return inode_.mutable_s3chunkinfomap();
    }

    void MarkInodeError() {
        // TODO(xuchaojie) : when inode is marked error, prevent futher write.
        status_ = InodeStatus::kError;
    }

    void LockSyncingInode() const {
        syncingInodeMtx_.lock();
    }

    void ReleaseSyncingInode() const {
        syncingInodeMtx_.unlock();
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

    ExtentCache* GetMutableExtentCache() {
        curve::common::UniqueLock lk(mtx_);
        return &extentCache_;
    }

    CURVEFS_ERROR RefreshVolumeExtent();

 private:
    CURVEFS_ERROR SyncS3ChunkInfo(bool internal = false);

 private:
    friend class UpdateVolumeExtentClosure;

    CURVEFS_ERROR FlushVolumeExtent();
    void FlushVolumeExtentAsync();

 private:
    FRIEND_TEST(TestInodeWrapper, TestUpdateInodeAttrIncrementally);

    Inode inode_;

    // dirty attributes, and needs update to metaserver
    InodeAttr dirtyAttr_;

    InodeStatus status_;

    bool isNlinkValid_;

    google::protobuf::Map<uint64_t, S3ChunkInfoList> s3ChunkInfoAdd_;

    std::shared_ptr<MetaServerClient> metaClient_;
    bool dirty_;
    mutable ::curve::common::Mutex mtx_;

    mutable ::curve::common::Mutex syncingInodeMtx_;
    mutable ::curve::common::Mutex syncingS3ChunkInfoMtx_;

    mutable ::curve::common::Mutex syncingVolumeExtentsMtx_;
    ExtentCache extentCache_;
};

}  // namespace client
}  // namespace curvefs

#endif  // CURVEFS_SRC_CLIENT_INODE_WRAPPER_H_
