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
#include "curvefs/src/client/xattr_manager.h"
#include "include/curve_compiler_specific.h"

using ::curvefs::metaserver::MetaStatusCode_Name;

namespace curvefs {
namespace client {

using rpcclient::MetaServerClient;
using rpcclient::MetaServerClientImpl;
using rpcclient::DataIndices;

bvar::Adder<int64_t> g_alive_inode_count{"alive_inode_count"};

#define REFRESH_NLINK                       \
do {                                        \
    CURVEFS_ERROR ret = RefreshNlink();     \
    if (ret != CURVEFS_ERROR::OK) {         \
        return ret;                         \
    }                                       \
} while (0)

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
            << ", s3chunkInfo { chunkId: " << info.chunkid()
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
    UpdateInodeAsyncDone(const std::shared_ptr<InodeWrapper>& inodeWrapper,
                         MetaServerClientDone* parent)
        : inodeWrapper_(inodeWrapper), parent_(parent) {}

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

        if (parent_ != nullptr) {
            parent_->SetMetaStatusCode(ret);
            parent_->Run();
        }
    };

 private:
    std::shared_ptr<InodeWrapper> inodeWrapper_;
    MetaServerClientDone* parent_;
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

CURVEFS_ERROR InodeWrapper::GetInodeAttrLocked(InodeAttr *attr) {
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

void InodeWrapper::GetInodeAttr(InodeAttr *attr) {
    curve::common::UniqueLock lg(mtx_);
    GetInodeAttrLocked(attr);
}

CURVEFS_ERROR InodeWrapper::SyncAttr(bool internal) {
    curve::common::UniqueLock lock = GetSyncingInodeUniqueLock();
    if (dirty_) {
        MetaStatusCode ret = metaClient_->UpdateInodeAttrWithOutNlink(
            inode_.fsid(), inode_.inodeid(), dirtyAttr_,
            nullptr, internal);

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
        ClearS3ChunkInfoAdd();
    }
    return CURVEFS_ERROR::OK;
}

void InodeWrapper::AsyncFlushAttr(MetaServerClientDone* done,
                                  bool /*internal*/) {
    if (dirty_) {
        LockSyncingInode();
        metaClient_->UpdateInodeWithOutNlinkAsync(
            inode_.fsid(), inode_.inodeid(), dirtyAttr_,
            new UpdateInodeAsyncDone(shared_from_this(), done));
        dirty_ = false;
        dirtyAttr_.Clear();
        return;
    }

    if (done != nullptr) {
        done->SetMetaStatusCode(MetaStatusCode::OK);
        done->Run();
    }
}

void InodeWrapper::FlushS3ChunkInfoAsync() {
    if (!s3ChunkInfoAdd_.empty()) {
        LockSyncingS3ChunkInfo();
         auto *done = new GetOrModifyS3ChunkInfoAsyncDone(shared_from_this());
        metaClient_->GetOrModifyS3ChunkInfoAsync(
            inode_.fsid(), inode_.inodeid(), s3ChunkInfoAdd_,
            done);
        ClearS3ChunkInfoAdd();
    }
}

CURVEFS_ERROR InodeWrapper::FlushVolumeExtent() {
    std::lock_guard<::curve::common::Mutex> guard(syncingVolumeExtentsMtx_);
    if (!extentCache_.HasDirtyExtents()) {
        return CURVEFS_ERROR::OK;
    }

    UpdateVolumeExtentClosure closure(shared_from_this(), true);
    auto dirtyExtents = extentCache_.GetDirtyExtents();
    VLOG(3) << "FlushVolumeExtent, ino: " << inode_.inodeid()
            << ", dirty extents: " << dirtyExtents.ShortDebugString();
    CHECK_GT(dirtyExtents.slices_size(), 0);
    metaClient_->AsyncUpdateVolumeExtent(inode_.fsid(), inode_.inodeid(),
                                         dirtyExtents, &closure);
    return closure.Wait();
}

void InodeWrapper::FlushVolumeExtentAsync() {
    syncingVolumeExtentsMtx_.lock();
    if (!extentCache_.HasDirtyExtents()) {
        VLOG(3) << "FlushVolumeExtentAsync, ino: " << inode_.inodeid()
                << ", no dirty extents";
        syncingVolumeExtentsMtx_.unlock();
        return;
    }

    auto dirtyExtents = extentCache_.GetDirtyExtents();
    VLOG(3) << "FlushVolumeExtent, ino: " << inode_.inodeid()
            << ", dirty extents: " << dirtyExtents.ShortDebugString();
    CHECK_GT(dirtyExtents.slices_size(), 0);
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
    auto before = s3ChunkInfoSize_;
    inode_.mutable_s3chunkinfomap()->swap(s3ChunkInfoMap);
    UpdateS3ChunkInfoMetric(CalS3ChunkInfoSize() - before);
    ClearS3ChunkInfoAdd();
    UpdateMaxS3ChunkInfoSize();
    lastRefreshTime_ = ::curve::common::TimeUtility::GetTimeofDaySec();
    return CURVEFS_ERROR::OK;
}

CURVEFS_ERROR InodeWrapper::Link(uint64_t parent) {
    curve::common::UniqueLock lg(mtx_);
    REFRESH_NLINK;
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
    REFRESH_NLINK;
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
    CURVEFS_ERROR ret = CURVEFS_ERROR::OK;
    switch (inode_.type()) {
        case FsFileType::TYPE_S3:
            ret = SyncS3(internal);
            break;
        case FsFileType::TYPE_FILE:
            ret = SyncAttr(internal);
            if (ret != CURVEFS_ERROR::OK) {
                break;
            }
            ret = FlushVolumeExtent();
            break;
        case FsFileType::TYPE_DIRECTORY:
            ret = SyncAttr(internal);
            break;
        default:
            break;
    }
    return ret;
}

void InodeWrapper::Async(MetaServerClientDone *done, bool internal) {
    VLOG(9) << "async inode: " << inode_.ShortDebugString();

    switch (inode_.type()) {
        case FsFileType::TYPE_S3:
            return AsyncS3(done, internal);
        case FsFileType::TYPE_FILE:
            return AsyncFlushAttrAndExtents(done, internal);
        case FsFileType::TYPE_DIRECTORY:
            FALLTHROUGH_INTENDED;
        case FsFileType::TYPE_SYM_LINK:
            return AsyncFlushAttr(done, internal);
    }

    CHECK(false) << "Unexpected inode type: " << inode_.type() << ", "
                 << inode_.ShortDebugString();
}

void InodeWrapper::AsyncFlushAttrAndExtents(MetaServerClientDone *done,
                                            bool /*internal*/) {
    if (dirty_ || extentCache_.HasDirtyExtents()) {
        LockSyncingInode();
        syncingVolumeExtentsMtx_.lock();
        DataIndices indices;
        if (extentCache_.HasDirtyExtents()) {
            indices.volumeExtents = extentCache_.GetDirtyExtents();
        }

        metaClient_->UpdateInodeWithOutNlinkAsync(
            inode_.fsid(), inode_.inodeid(), dirtyAttr_,
            new UpdateInodeAttrAndExtentClosure{shared_from_this(), done},
            std::move(indices));

        dirty_ = false;
        dirtyAttr_.Clear();
        return;
    }

    // nothing to update
    if (done != nullptr) {
        done->SetMetaStatusCode(MetaStatusCode::OK);
        done->Run();
    }
}

CURVEFS_ERROR InodeWrapper::SyncS3(bool internal) {
    curve::common::UniqueLock lock = GetSyncingInodeUniqueLock();
    curve::common::UniqueLock lockS3chunkInfo =
        GetSyncingS3ChunkInfoUniqueLock();
    if (dirty_ || !s3ChunkInfoAdd_.empty()) {
        MetaStatusCode ret = metaClient_->UpdateInodeAttrWithOutNlink(
            inode_.fsid(), inode_.inodeid(), dirtyAttr_,
            &s3ChunkInfoAdd_, internal);

        if (ret != MetaStatusCode::OK) {
            LOG(ERROR) << "metaClient_ UpdateInodeAttrWithOutNlink failed, "
                       << "MetaStatusCode: " << ret
                       << ", MetaStatusCode_Name: " << MetaStatusCode_Name(ret)
                       << ", inodeid: " << inode_.inodeid();
            return MetaStatusCodeToCurvefsErrCode(ret);
        }
        ClearS3ChunkInfoAdd();
        dirty_ = false;
        dirtyAttr_.Clear();
    }
    return CURVEFS_ERROR::OK;
}

namespace {
class UpdateInodeAsyncS3Done : public MetaServerClientDone {
 public:
    explicit UpdateInodeAsyncS3Done(
        const std::shared_ptr<InodeWrapper>& inodeWrapper,
        MetaServerClientDone* parent)
        : inodeWrapper_(inodeWrapper), parent_(parent) {}

    void Run() override {
        std::unique_ptr<UpdateInodeAsyncS3Done> self_guard(this);
        MetaStatusCode ret = GetStatusCode();
        if (ret != MetaStatusCode::OK && ret != MetaStatusCode::NOT_FOUND) {
            LOG(ERROR) << "metaClient_ UpdateInode failed, "
                       << "MetaStatusCode: " << ret
                       << ", MetaStatusCode_Name: " << MetaStatusCode_Name(ret)
                       << ", inodeid: " << inodeWrapper_->GetInodeId();
            inodeWrapper_->MarkInodeError();
        }
        VLOG(9) << "inode " << inodeWrapper_->GetInodeId() << " async success.";
        inodeWrapper_->ReleaseSyncingInode();
        inodeWrapper_->ReleaseSyncingS3ChunkInfo();

        if (parent_ != nullptr) {
            parent_->SetMetaStatusCode(ret);
            parent_->Run();
        }
    };

 private:
    std::shared_ptr<InodeWrapper> inodeWrapper_;
    MetaServerClientDone* parent_;
};
}  // namespace

void InodeWrapper::AsyncS3(MetaServerClientDone *done, bool internal) {
    if (dirty_ || !s3ChunkInfoAdd_.empty()) {
        LockSyncingInode();
        LockSyncingS3ChunkInfo();
        DataIndices indices;
        if (!s3ChunkInfoAdd_.empty()) {
            indices.s3ChunkInfoMap = std::move(s3ChunkInfoAdd_);
        }
        metaClient_->UpdateInodeWithOutNlinkAsync(
            inode_.fsid(), inode_.inodeid(), dirtyAttr_,
            new UpdateInodeAsyncS3Done{shared_from_this(), done},
            std::move(indices));
        dirty_ = false;
        dirtyAttr_.Clear();
        ClearS3ChunkInfoAdd();
        return;
    }

    if (done != nullptr) {
        done->SetMetaStatusCode(MetaStatusCode::OK);
        done->Run();
    }
}

CURVEFS_ERROR InodeWrapper::RefreshVolumeExtent() {
    VolumeExtentList extents;
    auto st = metaClient_->GetVolumeExtent(inode_.fsid(), inode_.inodeid(),
                                           true, &extents);
    VLOG(9) << "RefreshVolumeExtent, ino: " << inode_.inodeid()
            << ", extents: " << extents.ShortDebugString();
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
