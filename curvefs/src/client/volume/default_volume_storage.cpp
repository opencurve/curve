/*
 *  Copyright (c) 2022 NetEase Inc.
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
 * Date: Monday Mar 14 17:40:21 CST 2022
 * Author: wuhanqing
 */

#include "curvefs/src/client/volume/default_volume_storage.h"

#include <bvar/bvar.h>
#include <gflags/gflags.h>
#include <glog/logging.h>

#include <memory>
#include <sstream>
#include <type_traits>
#include <vector>

#include "absl/meta/type_traits.h"
#include "curvefs/src/client/inode_cache_manager.h"
#include "curvefs/src/client/inode_wrapper.h"
#include "curvefs/src/client/volume/extent_cache.h"
#include "curvefs/src/client/volume/utils.h"
#include "curvefs/src/common/metric_utils.h"

namespace curvefs {
namespace client {

namespace common {
DECLARE_bool(enableCto);
}  // namespace common

using ::curvefs::common::LatencyUpdater;

namespace {

template <typename IOPart,
          typename = absl::void_t<decltype(std::declval<IOPart>().offset),
                                  decltype(std::declval<IOPart>().length)>>
std::ostream& operator<<(std::ostream& os, const std::vector<IOPart>& iov) {
    if (iov.empty()) {
        os << "empty";
        return os;
    }

    std::ostringstream oss;
    oss << "{";
    for (const auto& io : iov) {
        oss << io.offset << "~" << io.length << ",";
    }

    oss << "}";
    os << oss.str();

    return os;
}

}  // namespace

ssize_t DefaultVolumeStorage::Write(uint64_t ino,
                                    off_t offset,
                                    size_t len,
                                    const char* data) {
    VLOG(9) << "writer start, ino: " << ino << ", offset: " << offset
            << ", len: " << len;

    std::shared_ptr<InodeWrapper> inodeWrapper;
    LatencyUpdater updater(&metric_.writeLatency);
    auto ret = inodeCacheManager_->GetInode(ino, inodeWrapper);
    if (ret != CURVEFS_ERROR::OK) {
        LOG(ERROR) << "Get inode error, ino: " << ino << ", ret: " << ret;
        return static_cast<ssize_t>(ret);
    }

    auto extentCache = inodeWrapper->GetMutableExtentCache();

    std::vector<WritePart> writes;
    if (!PrepareWriteRequest(offset, len, data, extentCache, spaceManager_,
                             &writes)) {
        LOG(ERROR) << "Prepare write requests error, ino: " << ino
                   << ", offset: " << offset << ", len: " << len;
        return static_cast<ssize_t>(CURVEFS_ERROR::IO_ERROR);
    }

    VLOG(9) << "write ino: " << ino << ", offset: " << offset
            << ", len: " << len << ", block write requests: " << writes;

    ssize_t nr = blockDeviceClient_->Writev(writes);
    // TODO(wuhanqing): enable check `nr != len`, currently, backend storage
    // will return larger value if write request is smaller than backend
    // storage's block size
    if (nr < 0 /*|| nr != len*/) {
        LOG(ERROR) << "Block device write error, ino: " << ino
                   << ", offset: " << offset << ", length: " << len
                   << ", nr: " << nr;
        return static_cast<ssize_t>(CURVEFS_ERROR::IO_ERROR);
    }

    extentCache->MarkWritten(offset, len);

    {
        auto lk = inodeWrapper->GetUniqueLock();
        auto* inode = inodeWrapper->GetMutableInodeUnlocked();
        UpdateInodeTimestamp(inode, kModifyTime | kChangeTime);

        if (offset + len > inode->length()) {
            inode->set_length(offset + len);
        }
    }

    inodeCacheManager_->ShipToFlush(inodeWrapper);

    VLOG(9) << "writer end, ino: " << ino << ", offset: " << offset
            << ", len: " << len;

    return nr;
}

ssize_t DefaultVolumeStorage::Read(uint64_t ino,
                                   off_t offset,
                                   size_t len,
                                   char* data) {
    VLOG(9) << "read start, ino: " << ino << ", offset: " << offset
            << ", len: " << len;

    std::shared_ptr<InodeWrapper> inodeWrapper;
    LatencyUpdater updater(&metric_.readLatency);
    auto ret = inodeCacheManager_->GetInode(ino, inodeWrapper);
    if (ret != CURVEFS_ERROR::OK) {
        LOG(ERROR) << "Get inode error, ino: " << ino << ", ret: " << ret;
        return static_cast<ssize_t>(ret);
    }

    auto extentCache = inodeWrapper->GetMutableExtentCache();
    std::vector<ReadPart> reads;
    std::vector<ReadPart> holes;
    extentCache->DivideForRead(offset, len, data, &reads, &holes);

    VLOG(9) << "read ino: " << ino << ", offset: " << offset << ", len: " << len
            << ", read holes: " << holes;

    for (auto& hole : holes) {
        memset(hole.data, 0, hole.length);
    }

    if (!reads.empty()) {
        VLOG(9) << "read ino: " << ino << ", offset: " << offset
            << ", len: " << len << ", block read requests: " << reads;

        ssize_t nr = blockDeviceClient_->Readv(reads);
        // TODO(wuhanqing): enable check `(nr+total) != len`, currently, backend
        // storage will return larger value if write request is smaller than
        // backend storage's block size
        if (nr < 0 /*|| (nr + total) != len*/) {
            LOG(ERROR) << "Block device read error, ino: " << ino
                       << ", offset: " << offset << ", length: " << len;
            return static_cast<ssize_t>(CURVEFS_ERROR::IO_ERROR);
        }
    }

    // TODO(all): check whether inode is opened with 'NO_ATIME'
    auto* inode = inodeWrapper->GetMutableInodeUnlocked();
    UpdateInodeTimestamp(inode, kAccessTime);
    inodeCacheManager_->ShipToFlush(inodeWrapper);

    VLOG(9) << "read end, ino: " << ino << ", offset: " << offset
            << ", len: " << len;

    return len;
}

bool DefaultVolumeStorage::Flush(uint64_t ino) {
    if (!common::FLAGS_enableCto) {
        return true;
    }

    LatencyUpdater updater(&metric_.flushLatency);
    std::shared_ptr<InodeWrapper> inodeWrapper;
    auto ret = inodeCacheManager_->GetInode(ino, inodeWrapper);
    if (ret != CURVEFS_ERROR::OK) {
        LOG(ERROR) << "Get inode error, ino: " << ino << ", ret: " << ret;
        return false;
    }

    auto lk = inodeWrapper->GetUniqueLock();
    ret = inodeWrapper->Sync();
    LOG_IF(ERROR, ret != CURVEFS_ERROR::OK)
        << "Flush sync inode error, ino: " << ino << ", ret: " << ret;
    return ret == CURVEFS_ERROR::OK;
}

bool DefaultVolumeStorage::Shutdown() {
    return true;
}

}  // namespace client
}  // namespace curvefs
