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

CURVEFS_ERROR InodeWrapper::SyncAttr() {
    curve::common::UniqueLock lock = GetSyncingInodeUniqueLock();
    if (dirty_) {
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
        auto *done = new UpdateInodeAsyncDone(shared_from_this());
        metaClient_->UpdateInodeAsync(inode_, done);
        dirty_ = false;
    }
}

void InodeWrapper::FlushXattrAsync() {
    if (dirty_) {
        LockSyncingXattr();
        auto *done = new UpdateXattrAsyncDone(shared_from_this());
        metaClient_->UpdateXattrAsync(inode_, done);
        dirty_ = false;
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

CURVEFS_ERROR InodeWrapper::LinkLocked() {
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

CURVEFS_ERROR InodeWrapper::UnLinkLocked() {
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
    LOG(ERROR) << "Unlink find nlink <= 0, nlink = " << old;
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
    openFlag_ = true;
    return UpdateInodeStatus(InodeOpenStatusChange::OPEN);
}

bool InodeWrapper::IsOpen() { return openFlag_; }

CURVEFS_ERROR InodeWrapper::Release() {
    openFlag_ = false;
    return UpdateInodeStatus(InodeOpenStatusChange::CLOSE);
}

CURVEFS_ERROR
InodeWrapper::UpdateInodeStatus(InodeOpenStatusChange statusChange) {
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

}  // namespace client
}  // namespace curvefs
