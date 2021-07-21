
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

#include <string>
#include <utility>
#include <vector>

#include "curvefs/proto/mds.pb.h"
#include "src/common/concurrent/rw_lock.h"
#include "src/kvstorageclient/etcd_client.h"

namespace curvefs {
namespace mds {

// A wrapper for proto FsInfo
class FsInfoWrapper {
    friend class PersisKVStorage;

 public:
    FsInfoWrapper() : fsInfo_() {}

    FsInfoWrapper(const FsInfo& fsInfo) : fsInfo_(fsInfo) {}  // NOLINT

    FsInfoWrapper(FsInfo&& fsInfo) : fsInfo_(std::move(fsInfo)) {}  // NOLINT

    FsInfoWrapper(const FsInfoWrapper& other) : fsInfo_(other.fsInfo_) {}

    FsInfoWrapper(FsInfoWrapper&& other) : FsInfoWrapper() {
        Swap(other);
    }

    FsInfoWrapper& operator=(FsInfoWrapper other) {
        Swap(other);
        return *this;
    }

    // FSType
    void SetFsType(FSType type) {
        fsInfo_.set_fstype(type);
    }

    // FsStatus
    void SetStatus(FsStatus status) {
        fsInfo_.set_status(status);
    }

    // FSType
    FSType FsType() const {
        return fsInfo_.fstype();
    }

    // FsStatus
    FsStatus Status() const {
        return fsInfo_.status();
    }

    // FsName
    std::string FsName() const {
        return fsInfo_.fsname();
    }

    // FsId
    uint64_t FsId() const {
        return fsInfo_.fsid();
    }

    bool IsMountPointEmpty() const {
        return fsInfo_.mountpoints_size() == 0;
    }

    bool IsMountPointExist(const std::string& mp) const;

    void AddMountPoint(const std::string& mp);

    FSStatusCode DeleteMountPoint(const std::string& mp);

    std::vector<std::string> MountPoints() const;

    // merge another fs info
    // currently, only merge mountpoints
    void Merge(const FsInfoWrapper& another);

    void Swap(FsInfoWrapper& other) {
        fsInfo_.Swap(&other.fsInfo_);
    }

    FsInfo ProtoFsInfo() const {
        return fsInfo_;
    }

 private:
    void MergeMountPoints(const FsInfoWrapper& another);

 private:
    FsInfo fsInfo_;
};

FsInfoWrapper GenerateFsInfoWrapper(const std::string& fsName, uint64_t fsId,
                                    uint64_t blocksize, uint64_t rootinodeid,
                                    const FsDetail& detail);

}  // namespace mds
}  // namespace curvefs

#endif  // CURVEFS_SRC_MDS_FS_INFO_WRAPPER_H_
