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
#include <sstream>

#include "curvefs/src/client/rpcclient/metaserver_client.h"

using ::curvefs::metaserver::MetaStatusCode_Name;

namespace curvefs {
namespace client {

using rpcclient::MetaServerClient;
using rpcclient::MetaServerClientImpl;
using rpcclient::MetaServerClientDone;

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
class UpdateXattrAsyncDone : public MetaServerClientDone {
 public:
    UpdateXattrAsyncDone(
        const std::shared_ptr<InodeWrapper> &inodeWrapper):
        inodeWrapper_(inodeWrapper) {}
    ~UpdateXattrAsyncDone() {}

    void Run() override {
        std::unique_ptr<UpdateXattrAsyncDone> self_guard(this);
        MetaStatusCode ret = GetStatusCode();
        if (ret != MetaStatusCode::OK && ret != MetaStatusCode::NOT_FOUND) {
            LOG(ERROR) << "metaClient_ UpdateXattr failed, "
                       << "MetaStatusCode: " << ret
                       << ", MetaStatusCode_Name: " << MetaStatusCode_Name(ret)
                       << ", inodeid: " << inodeWrapper_->GetInodeId();
            inodeWrapper_->MarkInodeError();
        }
        inodeWrapper_->ReleaseSyncingXattr();
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

CURVEFS_ERROR InodeWrapper::SyncFullInode() {
    std::lock_guard<std::mutex> lk(syncingXattrMtx_);
    std::lock_guard<std::mutex> lk2(syncingVolumeExtentsMtx_);

    if (!dirty_) {
        return CURVEFS_ERROR::OK;
    }

    AddVolumeExtentMapToInode();
    auto ret = metaClient_->UpdateInode(inode_);
    if (ret != MetaStatusCode::OK) {
        LOG(ERROR) << "update inode failed, error: " << MetaStatusCode_Name(ret)
                   << ", inodeid: " << inode_.inodeid();
        return MetaStatusCodeToCurvefsErrCode(ret);
    }

    dirty_ = false;
    return CURVEFS_ERROR::OK;
}

CURVEFS_ERROR InodeWrapper::SyncAttr() {
    curve::common::UniqueLock lock = GetSyncingInodeUniqueLock();
    if (dirty_) {
        AddVolumeExtentMapToInode();
        MetaStatusCode ret = metaClient_->UpdateInode(inode_);

        if (ret != MetaStatusCode::OK) {
            LOG(ERROR) << "metaClient_ UpdateInode failed, "
                       << "MetaStatusCode: " << ret
                       << ", MetaStatusCode_Name: " << MetaStatusCode_Name(ret)
                       << ", inodeid: " << inode_.inodeid();
            return MetaStatusCodeToCurvefsErrCode(ret);
        }

        dirty_ = false;
    }
    return CURVEFS_ERROR::OK;
}

CURVEFS_ERROR InodeWrapper::SyncS3ChunkInfo() {
    curve::common::UniqueLock lock = GetSyncingS3ChunkInfoUniqueLock();
    if (!s3ChunkInfoAdd_.empty()) {
        MetaStatusCode ret = metaClient_->GetOrModifyS3ChunkInfo(
            inode_.fsid(), inode_.inodeid(), s3ChunkInfoAdd_);
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

        if (inode_.type() == FsFileType::TYPE_FILE) {
            auto tmp = extentCache_.ToInodePb();
            inode_.mutable_volumeextentmap()->swap(tmp);
        }

        auto *done = new UpdateInodeAsyncDone(shared_from_this());
        metaClient_->UpdateInodeAsync(inode_, done);
        dirty_ = false;
    }
}

void InodeWrapper::FlushXattrAsync() {
    LockSyncingXattr();
    auto *done = new UpdateXattrAsyncDone(shared_from_this());
    metaClient_->UpdateXattrAsync(inode_, done);
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

CURVEFS_ERROR InodeWrapper::LinkLocked(uint64_t parent) {
    curve::common::UniqueLock lg(mtx_);
    uint32_t old = inode_.nlink();
    uint64_t oldCTime = inode_.ctime();
    inode_.set_nlink(old + 1);

    struct timespec now;
    clock_gettime(CLOCK_REALTIME, &now);
    inode_.set_ctime(now.tv_sec);
    inode_.set_ctime_ns(now.tv_nsec);
    inode_.set_mtime(now.tv_sec);
    inode_.set_mtime_ns(now.tv_nsec);
    if (inode_.type() != FsFileType::TYPE_DIRECTORY && parent != 0) {
        inode_.add_parent(parent);
    }
    AddVolumeExtentMapToInode();
    MetaStatusCode ret = metaClient_->UpdateInode(inode_);
    if (ret != MetaStatusCode::OK) {
        inode_.set_nlink(old);
        LOG(ERROR) << "metaClient_ UpdateInode failed, MetaStatusCode = " << ret
                   << ", MetaStatusCode_Name = " << MetaStatusCode_Name(ret)
                   << ", inodeid = " << inode_.inodeid();
        return MetaStatusCodeToCurvefsErrCode(ret);
    }
    dirty_ = false;
    return CURVEFS_ERROR::OK;
}

CURVEFS_ERROR InodeWrapper::IncreaseNLink() {
    curve::common::UniqueLock lg(mtx_);
    uint32_t old = inode_.nlink();
    inode_.set_nlink(old + 1);

    struct timespec now;
    clock_gettime(CLOCK_REALTIME, &now);
    inode_.set_ctime(now.tv_sec);
    inode_.set_ctime_ns(now.tv_nsec);
    inode_.set_mtime(now.tv_sec);
    inode_.set_mtime_ns(now.tv_nsec);

    dirty_ = true;
    return CURVEFS_ERROR::OK;
}

CURVEFS_ERROR InodeWrapper::UnLinkLocked(uint64_t parent) {
    curve::common::UniqueLock lg(mtx_);
    uint32_t old = inode_.nlink();
    VLOG(1) << "Unlink inode = " << inode_.DebugString();
    if (old > 0) {
        uint32_t newnlink = old - 1;
        if (newnlink == 1 && inode_.type() == FsFileType::TYPE_DIRECTORY) {
            newnlink--;
        }
        inode_.set_nlink(newnlink);
        struct timespec now;
        clock_gettime(CLOCK_REALTIME, &now);
        inode_.set_ctime(now.tv_sec);
        inode_.set_ctime_ns(now.tv_nsec);
        inode_.set_mtime(now.tv_sec);
        inode_.set_mtime_ns(now.tv_nsec);
        // newlink == 0 will be deleted at metasever
        // dir will not update parent
        // parent = 0; is useless
        if (newnlink != 0 && inode_.type() != FsFileType::TYPE_DIRECTORY
            && parent != 0) {
            auto parents = inode_.mutable_parent();
            for (auto iter = parents->begin(); iter != parents->end(); iter++) {
                if (*iter == parent) {
                    parents->erase(iter);
                    break;
                }
            }
        }

        // TODO(all): Maybe we need separate unlink from UpdateInode, because
        //            it's wired that we need to update extents when unlinking
        //            inode.
        //            Currently, the reason is when unlinking an inode,
        //            we delete the inode from InodeCacheManager, so next time
        //            read the inode again, InodeCacheManager will fetch inode
        //            from metaserver. If we don't update extents here, read
        //            will read nothing.
        //            scenario: pjdtest/tests/unlink/14.t
        //                      1. open a file with O_RDWR
        //                      2. write "hello, world"
        //                      3. unlink this file
        //                      4. pread from 0 to 13, expected "hello, world"
        AddVolumeExtentMapToInode();
        MetaStatusCode ret = metaClient_->UpdateInode(inode_);
        VLOG(6) << "UnLinkInode, inodeid = " << inode_.inodeid()
                << ", nlink = " << inode_.nlink();
        if (ret != MetaStatusCode::OK) {
            LOG(ERROR) << "metaClient_ UpdateInode failed, MetaStatusCode = "
                       << ret
                       << ", MetaStatusCode_Name = " << MetaStatusCode_Name(ret)
                       << ", inodeid = " << inode_.inodeid();
            return MetaStatusCodeToCurvefsErrCode(ret);
        }
        dirty_ = false;
        return CURVEFS_ERROR::OK;
    }
    LOG(ERROR) << "Unlink find nlink <= 0, nlink = " << old
               << ", inode = " << inode_.inodeid();
    return CURVEFS_ERROR::INTERNAL;
}

CURVEFS_ERROR InodeWrapper::DecreaseNLink() {
    curve::common::UniqueLock lg(mtx_);
    uint32_t old = inode_.nlink();
    if (old > 0) {
        uint32_t newnlink = old - 1;
        if (newnlink == 1 && inode_.type() == FsFileType::TYPE_DIRECTORY) {
            newnlink--;
        }
        inode_.set_nlink(newnlink);
        struct timespec now;
        clock_gettime(CLOCK_REALTIME, &now);
        inode_.set_ctime(now.tv_sec);
        inode_.set_ctime_ns(now.tv_nsec);
        inode_.set_mtime(now.tv_sec);
        inode_.set_mtime_ns(now.tv_nsec);
        dirty_ = true;
        return CURVEFS_ERROR::OK;
    }
    LOG(ERROR) << "DecreaseNLink find nlink <= 0, nlink = " << old;
    return CURVEFS_ERROR::INTERNAL;
}

CURVEFS_ERROR InodeWrapper::Open() {
    CURVEFS_ERROR ret = CURVEFS_ERROR::OK;
    if (0 == openCount_) {
        ret = UpdateInodeStatus(InodeOpenStatusChange::OPEN);
        if (ret != CURVEFS_ERROR::OK) {
            return ret;
        }
    }
    openCount_++;
    return CURVEFS_ERROR::OK;
}

bool InodeWrapper::IsOpen() { return openCount_ > 0; }

CURVEFS_ERROR InodeWrapper::Release() {
    CURVEFS_ERROR ret = CURVEFS_ERROR::OK;
    if (1 == openCount_) {
        ret = UpdateInodeStatus(InodeOpenStatusChange::CLOSE);
        if (ret != CURVEFS_ERROR::OK) {
            return ret;
        }
    }
    openCount_--;
    return CURVEFS_ERROR::OK;
}

CURVEFS_ERROR
InodeWrapper::UpdateInodeStatus(InodeOpenStatusChange statusChange) {
    AddVolumeExtentMapToInode();
    MetaStatusCode ret = metaClient_->UpdateInode(inode_, statusChange);
    if (ret != MetaStatusCode::OK) {
        LOG(ERROR) << "metaClient_ UpdateInode failed, MetaStatusCode = " << ret
                   << ", MetaStatusCode_Name = " << MetaStatusCode_Name(ret)
                   << ", inodeid = " << inode_.inodeid();
        return MetaStatusCodeToCurvefsErrCode(ret);
    }
    dirty_ = false;
    return CURVEFS_ERROR::OK;
}

CURVEFS_ERROR InodeWrapper::UpdateParentLocked(
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

    AddVolumeExtentMapToInode();
    MetaStatusCode ret = metaClient_->UpdateInode(inode_);
    if (ret != MetaStatusCode::OK) {
        LOG(ERROR) << "metaClient_ UpdateInode failed, MetaStatusCode = " << ret
                   << ", MetaStatusCode_Name = " << MetaStatusCode_Name(ret)
                   << ", inodeid = " << inode_.inodeid();
        return MetaStatusCodeToCurvefsErrCode(ret);
    }
    dirty_ = false;
    return CURVEFS_ERROR::OK;
}

static std::ostream &operator<<(
    std::ostream &os,
    const google::protobuf::Map<uint64_t, curvefs::metaserver::VolumeExtentList>
        &exts) {
    if (exts.empty()) {
        os << "[empty]";
        return os;
    }

    for (const auto &range : exts) {
        for (const auto &ext : range.second.volumeextents()) {
            os << ext.ShortDebugString();
        }
    }

    return os;
}

void InodeWrapper::BuildExtentCache() {
    if (inode_.type() != FsFileType::TYPE_FILE) {
        return;
    }

    VLOG(9) << "Build extent for inode: " << inode_.ShortDebugString()
            << ", extents: " << inode_.volumeextentmap().size();
    extentCache_.Build(inode_.volumeextentmap());
}

void InodeWrapper::AddVolumeExtentMapToInode() {
    if (inode_.type() != FsFileType::TYPE_FILE) {
        return;
    }

    auto tmp = extentCache_.ToInodePb();
    if (tmp.empty()) {
        return;
    }

    VLOG(9) << "Volume extent map, ino: " << inode_.inodeid()
            << ", extents: " << tmp;
    inode_.mutable_volumeextentmap()->swap(tmp);
}

}  // namespace client
}  // namespace curvefs
