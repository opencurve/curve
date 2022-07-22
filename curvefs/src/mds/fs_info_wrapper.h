
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
 * @Project: curve
 * @Date: Fri Jul 23 16:37:33 CST 2021
 * @Author: wuhanqing
 */

#ifndef CURVEFS_SRC_MDS_FS_INFO_WRAPPER_H_
#define CURVEFS_SRC_MDS_FS_INFO_WRAPPER_H_

#include <algorithm>
#include <string>
#include <utility>
#include <vector>

#include "curvefs/proto/mds.pb.h"

namespace curvefs {
namespace mds {

using ::curvefs::common::FSType;

// A wrapper for proto FsInfo
class FsInfoWrapper {
    friend class PersisKVStorage;

 public:
    FsInfoWrapper() = default;

    explicit FsInfoWrapper(const FsInfo& fsInfo) : fsInfo_(fsInfo) {}

    explicit FsInfoWrapper(FsInfo&& fsInfo)
        : fsInfo_(std::move(fsInfo)) {}

    FsInfoWrapper(const ::curvefs::mds::CreateFsRequest* request,
                  uint64_t fsId,
                  uint64_t rootInodeId);

    FsInfoWrapper(const FsInfoWrapper& other) = default;
    FsInfoWrapper& operator=(const FsInfoWrapper& other) = default;

    FsInfoWrapper(FsInfoWrapper&& other) noexcept = default;
    FsInfoWrapper& operator=(FsInfoWrapper&& other) noexcept = default;

    void SetFsType(FSType type) {
        fsInfo_.set_fstype(type);
    }

    void SetStatus(FsStatus status) {
        fsInfo_.set_status(status);
    }

    void SetFsName(const std::string& name) {
        fsInfo_.set_fsname(name);
    }

    void SetCapacity(uint64_t capacity) {
        fsInfo_.set_capacity(capacity);
    }

    void SetOwner(const std::string& owner) {
        fsInfo_.set_owner(owner);
    }

    FSType GetFsType() const {
        return fsInfo_.fstype();
    }

    FsStatus GetStatus() const {
        return fsInfo_.status();
    }

    std::string GetFsName() const {
        return fsInfo_.fsname();
    }

    uint64_t GetFsId() const {
        return fsInfo_.fsid();
    }

    uint64_t GetBlockSize() const {
        return fsInfo_.blocksize();
    }

    uint64_t GetCapacity() const {
        return fsInfo_.capacity();
    }

    std::string GetOwner() const {
        return fsInfo_.owner();
    }

    bool IsMountPointEmpty() const {
        return fsInfo_.mountpoints_size() == 0;
    }

    bool IsMountPointExist(const Mountpoint& mp) const;

    void AddMountPoint(const Mountpoint& mp);

    FSStatusCode DeleteMountPoint(const Mountpoint& mp);

    std::vector<Mountpoint> MountPoints() const;

    const FsInfo& ProtoFsInfo() const & {
        return fsInfo_;
    }

    FsInfo ProtoFsInfo() && {
        return std::move(fsInfo_);
    }

    const FsDetail& GetFsDetail() const {
        return fsInfo_.detail();
    }

    bool GetEnableSumInDir() const {
        return fsInfo_.enablesumindir();
    }

    uint64_t IncreaseFsTxSequence(const std::string& owner) {
        if (!fsInfo_.has_txowner() || fsInfo_.txowner() != owner) {
            uint64_t txSequence = 0;
            if (fsInfo_.has_txsequence()) {
                txSequence = fsInfo_.txsequence();
            }
            fsInfo_.set_txsequence(txSequence + 1);
            fsInfo_.set_txowner(owner);
        }
        return fsInfo_.txsequence();
    }

    uint64_t GetFsTxSequence() {
        return fsInfo_.has_txsequence() ? fsInfo_.txsequence() : 0;
    }

    void SetVolumeSize(uint64_t size);

 private:
    FsInfo fsInfo_;
};

}  // namespace mds
}  // namespace curvefs

#endif  // CURVEFS_SRC_MDS_FS_INFO_WRAPPER_H_
