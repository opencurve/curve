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
 * @Date: 2021-06-04 14:11:09
 * @Author: chenwei
 */
#ifndef CURVEFS_SRC_MDS_FS_H_
#define CURVEFS_SRC_MDS_FS_H_

#include <string>
#include <list>
#include "curvefs/proto/mds.pb.h"
#include "src/common/concurrent/rw_lock.h"

using curve::common::ReadLockGuard;
using curve::common::WriteLockGuard;
using curve::common::RWLock;
using curvefs::common::Volume;

namespace curvefs {
namespace mds {
class MdsFsInfo {
 public:
    MdsFsInfo() {}
    MdsFsInfo(uint32_t fsId, std::string fsName, uint64_t rootInodeId,
         uint64_t capacity, uint64_t blockSize, const common::Volume& volume);

    /**
     * @brief convert MdsFsInfo to FsInfo
     *
     * @param[out] file: the FsInfo
     */
    void ConvertToProto(FsInfo *file);

    /**
     * @brief check if mountpoint is empty
     *
     * @return If mountpoint of fs is empty, return true; else return false
     */
    bool MountPointEmpty();

    /**
     * @brief check if this mountpoint is exist in this fs
     *
     * @param[in] mountpoint: the mountpoint need to check
     *
     * @return If the mountpoint is exist in fs, return true; else return false
     */
    bool MountPointExist(const MountPoint& mountpoint);

    /**
     * @brief add one mount point to fs
     *
     * @param[in] mountpoint: the mountpoint need to add
     */
    void AddMountPoint(const MountPoint& mountpoint);

    /**
     * @brief delete one mount point from fs
     *
     * @param[in] mountpoint: the mountpoint need to delete
     *
     * @return If success, return OK; else return NOT_FOUND
     */
    FSStatusCode DeleteMountPoint(const MountPoint& mountpoint);

    uint32_t GetFsId() const;
    std::string GetFsName() const;
    Volume GetVolumeInfo();
    std::list<MountPoint> GetMountPointList();

 private:
    uint32_t fsId_;
    std::string fsName_;
    uint64_t rootInodeId_;
    uint64_t capacity_;
    uint64_t blockSize_;
    Volume volume_;
    uint32_t mountNum_;
    std::list<MountPoint> mountPointList_;
    //  curve::common::RWLock rwLock_;
};

}  // namespace mds
}  // namespace curvefs
#endif  // CURVEFS_SRC_MDS_FS_H_
