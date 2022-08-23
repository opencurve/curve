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

#include "curvefs/src/client/inode_wrapper.h"

#include <glog/logging.h>

#include <cstddef>
#include <ctime>
#include <memory>
#include <mutex>
#include <sstream>
#include <vector>

#include "curvefs/proto/metaserver.pb.h"
#include "curvefs/src/client/async_request_closure.h"
#include "curvefs/src/client/error_code.h"
#include "curvefs/src/client/rpcclient/metaserver_client.h"
#include "curvefs/src/client/rpcclient/task_excutor.h"

using ::curvefs::metaserver::MetaStatusCode_Name;

namespace curvefs {
namespace client {

using rpcclient::MetaServerClient;
using rpcclient::MetaServerClientDone;
using rpcclient::MetaServerClientImpl;

std::ostream &operator<<(std::ostream &os, const struct stat &attr) {
    os << "{ st_ino = " << attr.st_ino << ", st_mode = " << attr.st_mode
       << ", st_nlink = " << attr.st_nlink << ", st_uid = " << attr.st_uid
       << ", st_gid = " << attr.st_gid << ", st_size = " << attr.st_size
       << ", st_atim.tv_sec = " << attr.st_atim.tv_sec
       << ", st_atim.tv_nsec = " << attr.st_atim.tv_nsec
       << ", st_mtim.tv_sec = " << attr.st_mtim.tv_sec
       << ", st_mtim.tv_nsec = " << attr.st_mtim.tv_nsec
       << ", st_ctim.tv_sec = " << attr.st_ctim.tv_sec
       << ", st_ctim.tv_nsec = " << attr.st_ctim.tv_nsec
       << "}" << std::endl;
    return os;
}

void AppendS3ChunkInfoToMap(uint64_t chunkIndex, const S3ChunkInfo &info,
    google::protobuf::Map<uint64_t, S3ChunkInfoList> *s3ChunkInfoMap) {
    VLOG(9) << "AppendS3ChunkInfoToMap chunkIndex: " << chunkIndex
            << "s3chunkInfo { chunkId: " << info.chunkid()
            << ", compaction: " << info.compaction()
            << ", offset: " << info.offset() << ", len: " << info.len()
            << ", zero: " << info.zero();
    auto it = s3ChunkInfoMap->find(chunkIndex);
    if (it == s3ChunkInfoMap->end()) {
        S3ChunkInfoList s3ChunkInfoList;
        S3ChunkInfo *tmp = s3ChunkInfoList.add_s3chunks();
        tmp->CopyFrom(info);
        s3ChunkInfoMap->insert({chunkIndex, s3ChunkInfoList});
    } else {
        S3ChunkInfo *tmp = it->second.add_s3chunks();
        tmp->CopyFrom(info);
    }
}

class UpdateInodeAsyncDone : public MetaServerClientDone {
 public:
    explicit UpdateInodeAsyncDone(
        const std::shared_ptr<InodeWrapper> &inodeWrapper):
        inodeWrapper_(inodeWrapper) {}
    ~UpdateInodeAsyncDone() {}

    void Run() override {
        std::unique_ptr<UpdateInodeAsyncDone> self_guard(this);
        MetaStatusCode ret = GetStatusCode();
        if (ret != MetaStatusCode::OK && ret != MetaStatusCode::NOT_FOUND) {
            LOG(ERROR) << "metaClient_ UpdateInode failed, "
                       << "MetaStatusCode: " << ret
                       << ", MetaStatusCode_Name: " << MetaStatusCode_Name(ret)
                       << ", inodeid: " << inodeWrapper_->GetInodeId();
            inodeWrapper_->MarkInodeError();
        }
        inodeWrapper_->ReleaseSyncingInode();
    };

 private:
    std::shared_ptr<InodeWrapper> inodeWrapper_;
};

class GetOrModifyS3ChunkInfoAsyncDone : public MetaServerClientDone {
 public:
    explicit GetOrModifyS3ChunkInfoAsyncDone(
        const std::shared_ptr<InodeWrapper> &inodeWrapper):
        inodeWrapper_(inodeWrapper) {}
    ~GetOrModifyS3ChunkInfoAsyncDone() {}

    void Run() override {
        std::unique_ptr<GetOrModifyS3ChunkInfoAsyncDone> self_guard(this);
        MetaStatusCode ret = GetStatusCode();
        if (ret != MetaStatusCode::OK && ret != MetaStatusCode::NOT_FOUND) {
            LOG(ERROR) << "metaClient_ GetOrModifyS3ChunkInfo failed, "
                << "MetaStatusCode: " << ret
                << ", MetaStatusCode_Name: " << MetaStatusCode_Name(ret)
                << ", inodeid: " << inodeWrapper_->GetInodeId();
            inodeWrapper_->MarkInodeError();
        }
        inodeWrapper_->ReleaseSyncingS3ChunkInfo();
    };

 private:
    std::shared_ptr<InodeWrapper> inodeWrapper_;
};

CURVEFS_ERROR InodeWrapper::SyncAttr(bool internal) {
    curve::common::UniqueLock lock = GetSyncingInodeUniqueLock();
    if (dirty_) {
        MetaStatusCode ret = metaClient_->UpdateInodeAttrWithOutNlink(
            inode_.fsid(), inode_.inodeid(), dirtyAttr_,
            InodeOpenStatusChange::NOCHANGE, internal);

        if (ret != MetaStatusCode::OK) {
            LOG(ERROR) << "metaClient_ UpdateInodeAttrWithOutNlink failed, "
                       << "MetaStatusCode: " << ret
                       << ", MetaStatusCode_Name: " << MetaStatusCode_Name(ret)
                       << ", inodeid: " << inode_.inodeid();
            return MetaStatusCodeToCurvefsErrCode(ret);
        }

        dirty_ = false;
        dirtyAttr_.Clear();
    }
    return CURVEFS_ERROR::OK;
}

CURVEFS_ERROR InodeWrapper::SyncS3ChunkInfo(bool internal) {
    curve::common::UniqueLock lock = GetSyncingS3ChunkInfoUniqueLock();
    if (!s3ChunkInfoAdd_.empty()) {
        MetaStatusCode ret = metaClient_->GetOrModifyS3ChunkInfo(
            inode_.fsid(), inode_.inodeid(), s3ChunkInfoAdd_, false, nullptr,
            internal);
        if (ret != MetaStatusCode::OK) {
            LOG(ERROR) << "metaClient_ GetOrModifyS3ChunkInfo failed, "
                       << "MetaStatusCode: " << ret
                       << ", MetaStatusCode_Name: " << MetaStatusCode_Name(ret)
                       << ", inodeid: " << inode_.inodeid();
            return MetaStatusCodeToCurvefsErrCode(ret);
        }
        s3ChunkInfoAdd_.clear();
    }
    return CURVEFS_ERROR::OK;
}

void InodeWrapper::FlushAttrAsync() {
    if (dirty_) {
        LockSyncingInode();
        auto *done = new UpdateInodeAsyncDone(shared_from_this());
        metaClient_->UpdateInodeAttrWithOutNlinkAsync(
            inode_.fsid(), inode_.inodeid(), dirtyAttr_, done);
        dirty_ = false;
        dirtyAttr_.Clear();
    }
}

void InodeWrapper::FlushS3ChunkInfoAsync() {
    if (!s3ChunkInfoAdd_.empty()) {
        LockSyncingS3ChunkInfo();
         auto *done = new GetOrModifyS3ChunkInfoAsyncDone(shared_from_this());
        metaClient_->GetOrModifyS3ChunkInfoAsync(
            inode_.fsid(), inode_.inodeid(), s3ChunkInfoAdd_,
            done);
        s3ChunkInfoAdd_.clear();
    }
}

CURVEFS_ERROR InodeWrapper::FlushVolumeExtent() {
    std::lock_guard<::curve::common::Mutex> guard(syncingVolumeExtentsMtx_);
    if (!extentCache_.HasDirtyExtents()) {
        return CURVEFS_ERROR::OK;
    }

    UpdateVolumeExtentClosure closure(shared_from_this(), true);
    auto dirtyExtents = extentCache_.GetDirtyExtents();
    VLOG(9) << "FlushVolumeExtent, ino: " << inode_.inodeid()
            << ", dirty extents: " << dirtyExtents.ShortDebugString();
    metaClient_->AsyncUpdateVolumeExtent(inode_.fsid(), inode_.inodeid(),
                                         dirtyExtents, &closure);
    return closure.Wait();
}

void InodeWrapper::FlushVolumeExtentAsync() {
    syncingVolumeExtentsMtx_.lock();
    if (!extentCache_.HasDirtyExtents()) {
        syncingVolumeExtentsMtx_.unlock();
        return;
    }

    auto dirtyExtents = extentCache_.GetDirtyExtents();
    auto *closure = new UpdateVolumeExtentClosure(shared_from_this(), false);
    metaClient_->AsyncUpdateVolumeExtent(inode_.fsid(), inode_.inodeid(),
                                         dirtyExtents, closure);
}

CURVEFS_ERROR InodeWrapper::RefreshS3ChunkInfo() {
    curve::common::UniqueLock lock = GetSyncingS3ChunkInfoUniqueLock();
    google::protobuf::Map<
                uint64_t, S3ChunkInfoList> s3ChunkInfoMap;
    MetaStatusCode ret = metaClient_->GetOrModifyS3ChunkInfo(
        inode_.fsid(), inode_.inodeid(), s3ChunkInfoAdd_,
        true, &s3ChunkInfoMap);
    if (ret != MetaStatusCode::OK) {
        LOG(ERROR) << "metaClient_ GetOrModifyS3ChunkInfo failed, "
                   << "MetaStatusCode: " << ret
                   << ", MetaStatusCode_Name: " << MetaStatusCode_Name(ret)
                   << ", inodeid: " << inode_.inodeid();
        return MetaStatusCodeToCurvefsErrCode(ret);
    }
    inode_.mutable_s3chunkinfomap()->swap(s3ChunkInfoMap);
    s3ChunkInfoAdd_.clear();
    return CURVEFS_ERROR::OK;
}

CURVEFS_ERROR InodeWrapper::Link(uint64_t parent) {
    curve::common::UniqueLock lg(mtx_);
    REFRESH_NLINK_IF_NEED;
    uint32_t old = inode_.nlink();
    inode_.set_nlink(old + 1);
    dirtyAttr_.set_nlink(inode_.nlink());
    VLOG(3) << "LinkLocked, inodeid = " << inode_.inodeid()
            << ", newnlink = " << inode_.nlink();

    UpdateTimestampLocked(kChangeTime | kModifyTime);
    if (inode_.type() != FsFileType::TYPE_DIRECTORY && parent != 0) {
        inode_.add_parent(parent);
        dirtyAttr_.add_parent(parent);
    }

    MetaStatusCode ret = metaClient_->UpdateInodeAttr(
        inode_.fsid(), inode_.inodeid(), dirtyAttr_);
    if (ret != MetaStatusCode::OK) {
        inode_.set_nlink(old);
        LOG(ERROR) << "metaClient_ UpdateInodeAttr failed"
                   <<", MetaStatusCode = " << ret
                   << ", MetaStatusCode_Name = " << MetaStatusCode_Name(ret)
                   << ", inodeid = " << inode_.inodeid();
        return MetaStatusCodeToCurvefsErrCode(ret);
    }

    dirty_ = false;
    dirtyAttr_.Clear();
    return CURVEFS_ERROR::OK;
}

CURVEFS_ERROR InodeWrapper::UnLink(uint64_t parent) {
    curve::common::UniqueLock lg(mtx_);
    REFRESH_NLINK_IF_NEED;
    uint32_t old = inode_.nlink();
    VLOG(1) << "Unlink inode = " << inode_.DebugString();
    if (old > 0) {
        uint32_t newnlink = old - 1;
        if (newnlink == 1 && inode_.type() == FsFileType::TYPE_DIRECTORY) {
            newnlink--;
        }
        inode_.set_nlink(newnlink);
        VLOG(3) << "UnLinkLocked, inodeid = " << inode_.inodeid()
                << ", newnlink = " << inode_.nlink()
                << ", type = " << inode_.type();
        UpdateTimestampLocked(kChangeTime | kModifyTime);
        // newnlink == 0 will be deleted at metaserver
        // dir will not update parent
        // parent = 0; is useless
        if (newnlink != 0 && inode_.type() != FsFileType::TYPE_DIRECTORY &&
            parent != 0) {
            auto parents = inode_.mutable_parent();
            for (auto iter = parents->begin(); iter != parents->end(); iter++) {
                if (*iter == parent) {
                    parents->erase(iter);
                    break;
                }
            }
        }

        if (inode_.type() == FsFileType::TYPE_FILE) {
            // TODO(all): Maybe we need separate unlink from UpdateInode,
            // because it's wired that we need to update extents when
            // unlinking inode. Currently, the reason is when unlinking an
            // inode, we delete the inode from InodeCacheManager,
            // so next time read the inode again,  InodeCacheManager will fetch
            // inode from metaserver. If we don't update extents here,
            // read will read nothing.
            // scenario: pjdtest/tests/unlink/14.t
            //           1. open a file with O_RDWR
            //           2. write "hello, world"
            //           3. unlink this file
            //           4. pread from 0 to 13, expected "hello, world"
            auto err = FlushVolumeExtent();
            if (err != CURVEFS_ERROR::OK) {
                LOG(ERROR) << "Flush volume extent failed, inodeid: "
                           << inode_.inodeid() << ", error: " << err;
                return err;
            }
        }

        dirtyAttr_.set_nlink(inode_.nlink());
        *dirtyAttr_.mutable_parent() = inode_.parent();

        MetaStatusCode ret = metaClient_->UpdateInodeAttr(
            inode_.fsid(), inode_.inodeid(), dirtyAttr_);
        if (ret != MetaStatusCode::OK) {
            LOG(ERROR) << "metaClient_ UpdateInodeAttr failed"
                       << ", MetaStatusCode = " << ret
                       << ", MetaStatusCode_Name = " << MetaStatusCode_Name(ret)
                       << ", inodeid = " << inode_.inodeid();
            return MetaStatusCodeToCurvefsErrCode(ret);
        }
        dirty_ = false;
        dirtyAttr_.Clear();
        return CURVEFS_ERROR::OK;
    }
    LOG(ERROR) << "Unlink find nlink <= 0, nlink = " << old
               << ", inode = " << inode_.inodeid();
    return CURVEFS_ERROR::INTERNAL;
}

CURVEFS_ERROR InodeWrapper::UpdateParent(
    uint64_t oldParent, uint64_t newParent) {
    curve::common::UniqueLock lg(mtx_);
    auto parents = inode_.mutable_parent();
    for (auto iter = parents->begin(); iter != parents->end(); iter++) {
        if (*iter == oldParent) {
            parents->erase(iter);
            break;
        }
    }
    inode_.add_parent(newParent);
    *dirtyAttr_.mutable_parent() = inode_.parent();

    MetaStatusCode ret = metaClient_->UpdateInodeAttrWithOutNlink(
        inode_.fsid(), inode_.inodeid(), dirtyAttr_);
    if (ret != MetaStatusCode::OK) {
        LOG(ERROR) << "metaClient_ UpdateInodeAttrWithOutNlink failed"
                   << ", MetaStatusCode = " << ret
                   << ", MetaStatusCode_Name = " << MetaStatusCode_Name(ret)
                   << ", inodeid = " << inode_.inodeid();
        return MetaStatusCodeToCurvefsErrCode(ret);
    }
    dirty_ = false;
    dirtyAttr_.Clear();
    return CURVEFS_ERROR::OK;
}

CURVEFS_ERROR InodeWrapper::Sync(bool internal) {
    VLOG(9) << "sync inode: " << inode_.ShortDebugString();

    // TODO(all): maybe we should update inode attribute and data indices
    //            in single rpc
    auto ret = SyncAttr(internal);
    if (ret != CURVEFS_ERROR::OK) {
        return ret;
    }

    switch (inode_.type()) {
        case FsFileType::TYPE_S3:
            return SyncS3ChunkInfo(internal);
        case FsFileType::TYPE_FILE:
            return FlushVolumeExtent();
        default:
            return CURVEFS_ERROR::OK;
    }
}

void InodeWrapper::FlushAsync() {
    // TODO(all): maybe we should update inode attribute and data indices
    //            in single rpc
    FlushAttrAsync();

    switch (inode_.type()) {
        case FsFileType::TYPE_S3:
            return FlushS3ChunkInfoAsync();
        case FsFileType::TYPE_FILE:
            return FlushVolumeExtentAsync();
        default:
            break;
    }
}

CURVEFS_ERROR InodeWrapper::RefreshVolumeExtent() {
    VolumeExtentList extents;
    auto st = metaClient_->GetVolumeExtent(inode_.fsid(), inode_.inodeid(),
                                           true, &extents);
    if (st == MetaStatusCode::OK) {
        VLOG(9) << "Refresh volume extent success, inodeid: "
                << inode_.inodeid() << ", extents: ["
                << extents.ShortDebugString() << "]";
        extentCache_.Build(extents);
    } else {
        LOG(ERROR) << "GetVolumeExtent failed, inodeid: " << inode_.inodeid();
    }

    return MetaStatusCodeToCurvefsErrCode(st);
}

CURVEFS_ERROR InodeWrapper::RefreshNlink() {
    InodeAttr attr;
    auto ret = metaClient_->GetInodeAttr(
        inode_.fsid(), inode_.inodeid(), &attr);
    if (ret != MetaStatusCode::OK) {
        VLOG(3) << "RefreshNlink failed, fsId: " << inode_.fsid()
                << ", inodeid: " << inode_.inodeid();
        return CURVEFS_ERROR::INTERNAL;
    }
    inode_.set_nlink(attr.nlink());
    VLOG(3) << "RefreshNlink from metaserver, newnlink: " << attr.nlink();
    ResetNlinkValid();
    return CURVEFS_ERROR::OK;
}

void InodeWrapper::MergeXAttrLocked(
    const google::protobuf::Map<std::string, std::string>& xattrs) {
    auto helper =
        [](const google::protobuf::Map<std::string, std::string>& incoming,
           google::protobuf::Map<std::string, std::string>* xattrs) {
            for (const auto& attr : incoming) {
                (*xattrs)[attr.first] = attr.second;
            }
        };

    helper(xattrs, inode_.mutable_xattr());
    helper(xattrs, dirtyAttr_.mutable_xattr());
    dirty_ = true;
}

void InodeWrapper::UpdateTimestampLocked(int flags) {
    struct timespec now;
    clock_gettime(CLOCK_REALTIME, &now);
    return UpdateTimestampLocked(now, flags);
}

void InodeWrapper::UpdateTimestampLocked(const timespec& now, int flags) {
    if (flags & kAccessTime) {
        inode_.set_atime(now.tv_sec);
        inode_.set_atime_ns(now.tv_nsec);
        dirtyAttr_.set_atime(now.tv_sec);
        dirtyAttr_.set_atime_ns(now.tv_nsec);
        dirty_ = true;
    }

    if (flags & kChangeTime) {
        inode_.set_ctime(now.tv_sec);
        inode_.set_ctime_ns(now.tv_nsec);
        dirtyAttr_.set_ctime(now.tv_sec);
        dirtyAttr_.set_ctime_ns(now.tv_nsec);
        dirty_ = true;
    }

    if (flags & kModifyTime) {
        inode_.set_mtime(now.tv_sec);
        inode_.set_mtime_ns(now.tv_nsec);
        dirtyAttr_.set_mtime(now.tv_sec);
        dirtyAttr_.set_mtime_ns(now.tv_nsec);
        dirty_ = true;
    }
}

}  // namespace client
}  // namespace curvefs
