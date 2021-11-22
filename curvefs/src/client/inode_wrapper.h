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

#include "curvefs/proto/metaserver.pb.h"
#include "curvefs/src/client/error_code.h"
#include "curvefs/src/client/rpcclient/metaserver_client.h"
#include "src/common/concurrent/concurrent.h"

using ::curvefs::metaserver::Inode;
using ::curvefs::metaserver::VolumeExtentList;
using ::curvefs::metaserver::S3ChunkInfoList;

namespace curvefs {
namespace client {

// TODO(xuchaojie) : get from conf maybe?
const uint32_t kOptimalIOBlockSize = 0x10000u;

using rpcclient::MetaServerClient;
using rpcclient::MetaServerClientImpl;

std::ostream &operator<<(std::ostream &os, const struct stat &attr);

class InodeWrapper {
 public:
    InodeWrapper(const Inode &inode,
        const std::shared_ptr<MetaServerClient> &metaClient)
      : inode_(inode),
        openCount_(0),
        metaClient_(metaClient),
        dirty_(false) {}

    InodeWrapper(Inode &&inode,
        const std::shared_ptr<MetaServerClient> &metaClient)
      : inode_(std::move(inode)),
        openCount_(0),
        metaClient_(metaClient),
        dirty_(false) {}

    ~InodeWrapper() {}

    uint64_t GetInodeId() const {
        return inode_.inodeid();
    }

    uint32_t GetFsId() const {
        return inode_.fsid();
    }

    std::string GetSymlinkStr() const {
        curve::common::UniqueLock lg(mtx_);
        return inode_.symlink();
    }

    bool Dirty() const {
        return dirty_;
    }

    void SetLength(uint64_t len) {
        inode_.set_length(len);
        dirty_ = true;
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
        attr->st_blocks = (inode_.length() + 511) / 512;
        attr->st_rdev = inode_.rdev();
        attr->st_atim.tv_sec = inode_.atime();
        attr->st_atim.tv_nsec = inode_.atime_ns();
        attr->st_mtim.tv_sec = inode_.mtime();
        attr->st_mtim.tv_nsec = inode_.mtime_ns();
        attr->st_ctim.tv_sec = inode_.ctime();
        attr->st_ctim.tv_nsec = inode_.ctime_ns();
        attr->st_blksize = kOptimalIOBlockSize;
        VLOG(6) << "GetInodeAttr attr =  " << *attr
                << ", inodeid = " << inode_.inodeid();
        return;
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

    CURVEFS_ERROR LinkLocked();

    CURVEFS_ERROR UnLinkLocked();

    CURVEFS_ERROR Sync();

    CURVEFS_ERROR Open();

    bool IsOpen();

    CURVEFS_ERROR Release();

    void SetOpenCount(uint32_t openCount) {
        openCount_ = openCount;
    }

    uint32_t GetOpenCount() const {
        return openCount_;
    }

    void MarkDirty() {
        dirty_ = true;
    }

    bool isDirty() const {
        return dirty_;
    }

 private:
    CURVEFS_ERROR SetOpenFlag(bool flag);

 private:
     Inode inode_;
     uint32_t openCount_;

     std::shared_ptr<MetaServerClient> metaClient_;
     bool dirty_;
     mutable ::curve::common::Mutex mtx_;
};

}  // namespace client
}  // namespace curvefs

#endif  // CURVEFS_SRC_CLIENT_INODE_WRAPPER_H_
