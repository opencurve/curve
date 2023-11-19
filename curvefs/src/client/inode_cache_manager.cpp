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

#include "curvefs/src/client/inode_cache_manager.h"

#include <glog/logging.h>
#include <cstdint>
#include <map>
#include <memory>
#include <utility>
#include "curvefs/proto/metaserver.pb.h"
#include "curvefs/src/client/filesystem/error.h"
#include "curvefs/src/client/filesystem/utils.h"
#include "curvefs/src/client/filesystem/defer_sync.h"
#include "curvefs/src/client/inode_wrapper.h"

using ::curvefs::metaserver::Inode;
using ::curvefs::metaserver::MetaStatusCode_Name;

namespace curvefs {
namespace client {
namespace common {
DECLARE_bool(enableCto);
}  // namespace common
}  // namespace client
}  // namespace curvefs

namespace curvefs {
namespace client {

using ::curvefs::client::filesystem::ToFSError;
using ::curvefs::client::filesystem::AttrCtime;

using NameLockGuard = ::curve::common::GenericNameLockGuard<Mutex>;
using curvefs::client::common::FLAGS_enableCto;

#define RETURN_IF_CTO_ON() \
    do {                   \
        if (cto_) {        \
            return;        \
        }                  \
    } while (0)

DeferWatcher::DeferWatcher(bool cto, std::shared_ptr<DeferSync> deferSync)
    : cto_(cto),
      deferSync_(deferSync),
      deferAttrs_() {}

void DeferWatcher::PreGetAttrs(const std::set<uint64_t>& inos) {
    RETURN_IF_CTO_ON();
    InodeAttr attr;
    std::shared_ptr<InodeWrapper> inode;
    for (const auto& ino : inos) {
        bool yes = deferSync_->IsDefered(ino, &inode);
        if (!yes) {
            continue;
        }
        inode->GetInodeAttr(&attr);
        deferAttrs_.emplace(ino, attr);
    }
}

bool DeferWatcher::TryUpdate(InodeAttr* attr) {
    Ino ino = attr->inodeid();
    auto iter = deferAttrs_.find(ino);
    if (iter == deferAttrs_.end()) {
        return false;
    }

    auto& defered = iter->second;
    if (AttrCtime(*attr) > AttrCtime(defered)) {
        return false;
    }
    *attr = defered;
    return true;
}

void DeferWatcher::PostGetAttrs(std::list<InodeAttr>* attrs) {
    RETURN_IF_CTO_ON();
    if (deferAttrs_.size() == 0) {
        return;
    }
    for (auto& attr : *attrs) {
        TryUpdate(&attr);
    }
}

void DeferWatcher::PostGetAttrs(std::map<uint64_t, InodeAttr>* attrs) {
    RETURN_IF_CTO_ON();
    if (deferAttrs_.size() == 0) {
        return;
    }
    for (auto& item : *attrs) {
        auto& attr = item.second;
        TryUpdate(&attr);
    }
}

#define GET_INODE_REMOTE(FSID, INODEID, OUT, STREAMING)                        \
    MetaStatusCode ret = metaClient_->GetInode(FSID, INODEID, OUT, STREAMING); \
    if (ret != MetaStatusCode::OK) {                                           \
        LOG_IF(ERROR, ret != MetaStatusCode::NOT_FOUND)                        \
            << "metaClient_ GetInode failed, MetaStatusCode = " << ret         \
            << ", MetaStatusCode_Name = " << MetaStatusCode_Name(ret)          \
            << ", inodeid = " << INODEID;                                      \
        return ToFSError(ret);                            \
    }

#define REFRESH_DATA_REMOTE(OUT, STREAMING)                                    \
    CURVEFS_ERROR rc = RefreshData(OUT, STREAMING);                            \
    if (rc != CURVEFS_ERROR::OK) {                                             \
        return rc;                                                             \
    }

CURVEFS_ERROR
InodeCacheManagerImpl::GetInode(uint64_t inodeId,
                                std::shared_ptr<InodeWrapper> &out) {
    NameLockGuard lock(nameLock_, std::to_string(inodeId));
    bool yes = openFiles_->IsOpened(inodeId, &out);
    if (yes) {
        return CURVEFS_ERROR::OK;
    }

    bool cto = FLAGS_enableCto;
    if (!cto && deferSync_->IsDefered(inodeId, &out)) {
        return CURVEFS_ERROR::OK;
    }

    // get inode from metaserver
    Inode inode;
    bool streaming = false;
    GET_INODE_REMOTE(fsId_, inodeId, &inode, &streaming);
    out = std::make_shared<InodeWrapper>(
        std::move(inode), metaClient_, s3ChunkInfoMetric_, option_.maxDataSize,
        option_.refreshDataIntervalSec);

    // refresh data
    VLOG(9) << "get inode: " << inodeId << " from icache fail, get from remote";
    REFRESH_DATA_REMOTE(out, streaming);

    return CURVEFS_ERROR::OK;
}

CURVEFS_ERROR InodeCacheManagerImpl::GetInodeAttr(uint64_t inodeId,
                                                  InodeAttr *out) {
    NameLockGuard lock(nameLock_, std::to_string(inodeId));
    std::set<uint64_t> inodeIds;
    std::list<InodeAttr> attrs;
    inodeIds.emplace(inodeId);

    bool cto = FLAGS_enableCto;
    auto watcher = std::make_shared<DeferWatcher>(cto, deferSync_);
    watcher->PreGetAttrs(inodeIds);

    MetaStatusCode ret =
        metaClient_->BatchGetInodeAttr(fsId_, inodeIds, &attrs);
    if (MetaStatusCode::OK != ret) {
        LOG(ERROR) << "metaClient BatchGetInodeAttr failed"
                   << ", inodeId = " << inodeId << ", MetaStatusCode = " << ret
                   << ", MetaStatusCode_Name = " << MetaStatusCode_Name(ret);
        return ToFSError(ret);
    }

    if (attrs.size() != 1) {
        LOG(ERROR) << "metaClient BatchGetInodeAttr error,"
                   << " getSize is 1, inodeId = " << inodeId
                   << "but real size = " << attrs.size();
        return CURVEFS_ERROR::INTERNAL;
    }

    watcher->PostGetAttrs(&attrs);
    *out = *attrs.begin();
    return CURVEFS_ERROR::OK;
}

CURVEFS_ERROR InodeCacheManagerImpl::BatchGetInodeAttr(
    std::set<uint64_t> *inodeIds,
    std::list<InodeAttr> *attrs) {
    if (inodeIds->empty()) {
        return CURVEFS_ERROR::OK;
    }

    bool cto = FLAGS_enableCto;
    auto watcher = std::make_shared<DeferWatcher>(cto, deferSync_);
    watcher->PreGetAttrs(*inodeIds);

    MetaStatusCode ret = metaClient_->BatchGetInodeAttr(fsId_, *inodeIds,
        attrs);
    if (MetaStatusCode::OK != ret) {
        LOG(ERROR) << "metaClient BatchGetInodeAttr failed, MetaStatusCode = "
                   << ret << ", MetaStatusCode_Name = "
                   << MetaStatusCode_Name(ret);
    }
    watcher->PostGetAttrs(attrs);
    return ToFSError(ret);
}

CURVEFS_ERROR InodeCacheManagerImpl::BatchGetInodeAttrAsync(
    uint64_t parentId,
    std::set<uint64_t> *inodeIds,
    std::map<uint64_t, InodeAttr> *attrs) {
    NameLockGuard lg(asyncNameLock_, std::to_string(parentId));

    if (inodeIds->empty()) {
        return CURVEFS_ERROR::OK;
    }

    bool cto = FLAGS_enableCto;
    auto watcher = std::make_shared<DeferWatcher>(cto, deferSync_);
    watcher->PreGetAttrs(*inodeIds);

    // split inodeIds by partitionId and batch limit
    std::vector<std::vector<uint64_t>> inodeGroups;
    if (!metaClient_->SplitRequestInodes(fsId_, *inodeIds, &inodeGroups)) {
        return CURVEFS_ERROR::NOT_EXIST;
    }

    ::curve::common::Mutex mutex;
    std::shared_ptr<CountDownEvent> cond =
        std::make_shared<CountDownEvent>(inodeGroups.size());
    for (const auto& it : inodeGroups) {
        VLOG(3) << "BatchGetInodeAttrAsync Send " << it.size();
        auto* done = new BatchGetInodeAttrAsyncDone(attrs, &mutex, cond);
        MetaStatusCode ret = metaClient_->BatchGetInodeAttrAsync(fsId_, it,
                                                                 done);
        if (MetaStatusCode::OK != ret) {
            LOG(ERROR) << "metaClient BatchGetInodeAsync failed,"
                       << " MetaStatusCode = " << ret
                       << ", MetaStatusCode_Name = "
                       << MetaStatusCode_Name(ret);
        }
    }

    // wait for all sudrequest finished
    cond->Wait();
    watcher->PostGetAttrs(attrs);
    return CURVEFS_ERROR::OK;
}

CURVEFS_ERROR InodeCacheManagerImpl::BatchGetXAttr(
    std::set<uint64_t> *inodeIds,
    std::list<XAttr> *xattr) {
    if (inodeIds->empty()) {
        return CURVEFS_ERROR::OK;
    }

    MetaStatusCode ret = metaClient_->BatchGetXAttr(fsId_, *inodeIds, xattr);
    if (MetaStatusCode::OK != ret) {
        LOG(ERROR) << "metaClient BatchGetXAttr failed, MetaStatusCode = "
                   << ret << ", MetaStatusCode_Name = "
                   << MetaStatusCode_Name(ret);
    }
    return ToFSError(ret);
}

CURVEFS_ERROR InodeCacheManagerImpl::CreateInode(
    const InodeParam &param,
    std::shared_ptr<InodeWrapper> &out) {
    Inode inode;
    MetaStatusCode ret = metaClient_->CreateInode(param, &inode);
    if (ret != MetaStatusCode::OK) {
        LOG(ERROR) << "metaClient_ CreateInode failed, MetaStatusCode = " << ret
                   << ", MetaStatusCode_Name = " << MetaStatusCode_Name(ret);
        return ToFSError(ret);
    }
    out = std::make_shared<InodeWrapper>(std::move(inode), metaClient_,
        s3ChunkInfoMetric_, option_.maxDataSize,
        option_.refreshDataIntervalSec);
    return CURVEFS_ERROR::OK;
}

CURVEFS_ERROR InodeCacheManagerImpl::CreateManageInode(
    const InodeParam &param,
    std::shared_ptr<InodeWrapper> &out) {
    Inode inode;
    MetaStatusCode ret = metaClient_->CreateManageInode(param, &inode);
    if (ret != MetaStatusCode::OK) {
        LOG(ERROR) << "metaClient_ CreateManageInode failed, MetaStatusCode = "
                   << ret << ", MetaStatusCode_Name = "
                   << MetaStatusCode_Name(ret);
        return ToFSError(ret);
    }
    out = std::make_shared<InodeWrapper>(std::move(inode), metaClient_,
        s3ChunkInfoMetric_, option_.maxDataSize,
        option_.refreshDataIntervalSec);
    return CURVEFS_ERROR::OK;
}

CURVEFS_ERROR InodeCacheManagerImpl::DeleteInode(uint64_t inodeId) {
    NameLockGuard lock(nameLock_, std::to_string(inodeId));
    MetaStatusCode ret = metaClient_->DeleteInode(fsId_, inodeId);
    if (ret != MetaStatusCode::OK && ret != MetaStatusCode::NOT_FOUND) {
        LOG(ERROR) << "metaClient_ DeleteInode failed, MetaStatusCode = " << ret
                   << ", MetaStatusCode_Name = " << MetaStatusCode_Name(ret)
                   << ", inodeId = " << inodeId;
        return ToFSError(ret);
    }
    return CURVEFS_ERROR::OK;
}

void InodeCacheManagerImpl::ShipToFlush(
    const std::shared_ptr<InodeWrapper>& inode) {
    deferSync_->Push(inode);
}

CURVEFS_ERROR
InodeCacheManagerImpl::RefreshData(std::shared_ptr<InodeWrapper> &inode,
                                   bool streaming) {
    auto type = inode->GetType();
    CURVEFS_ERROR rc = CURVEFS_ERROR::OK;

    switch (type) {
    case FsFileType::TYPE_S3:
        if (streaming) {
            // NOTE: if the s3chunkinfo inside inode is too large,
            // we should invoke RefreshS3ChunkInfo() to receive s3chunkinfo
            // by streaming and padding its into inode.
            rc = inode->RefreshS3ChunkInfo();
            LOG_IF(ERROR, rc != CURVEFS_ERROR::OK)
                << "RefreshS3ChunkInfo() failed, retCode = " << rc;
        }
        break;

    case FsFileType::TYPE_FILE: {
        if (inode->GetLength() > 0) {
            rc = inode->RefreshVolumeExtent();
            LOG_IF(ERROR, rc != CURVEFS_ERROR::OK)
                << "RefreshVolumeExtent failed, error: " << rc;
        }
        break;
    }

    default:
        rc = CURVEFS_ERROR::OK;
    }

    return rc;
}

}  // namespace client
}  // namespace curvefs
