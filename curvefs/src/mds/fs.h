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

#include <map>
#include <list>
#include <string>
#include "curvefs/proto/mds.pb.h"
#include "src/common/concurrent/rw_lock.h"

using curve::common::ReadLockGuard;
using curve::common::WriteLockGuard;
using curve::common::RWLock;
using curvefs::common::Volume;
using curvefs::common::FSType;
using curvefs::common::S3Info;

namespace curvefs {
namespace mds {
class MdsFsInfo {
 public:
    MdsFsInfo() {}
    MdsFsInfo(uint32_t fsId, const std::string& fsName, FsStatus status,
              uint64_t rootInodeId, uint64_t capacity, uint64_t blockSize)
        : fsId_(fsId),
          fsName_(fsName),
          status_(status),
          rootInodeId_(rootInodeId),
          capacity_(capacity),
          blockSize_(blockSize) {}

    virtual ~MdsFsInfo() {}

    /**
     * @brief convert MdsFsInfo to FsInfo
     *
     * @param[out] file: the FsInfo
     */
    virtual void ConvertToProto(FsInfo* file);

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
    bool MountPointExist(const std::string& mountpoint);

    /**
     * @brief add one mount point to fs
     *
     * @param[in] mountpoint: the mountpoint need to add
     */
    void AddMountPoint(const std::string& mountpoint);

    /**
     * @brief delete one mount point from fs
     *
     * @param[in] mountpoint: the mountpoint need to delete
     *
     * @return If success, return OK; else return NOT_FOUND
     */
    FSStatusCode DeleteMountPoint(const std::string& mountpoint);

    uint32_t GetFsId();
    std::string GetFsName();
    std::list<std::string> GetMountPointList();
    void SetStatus(FsStatus status);
    FsStatus GetStatus();
    FSType GetFsType();
    void SetFsType(FSType type);
    void SetTxId(uint64_t copysetId, uint64_t txId);

 private:
    uint32_t fsId_;
    std::string fsName_;
    FsStatus status_;
    uint64_t rootInodeId_;
    uint64_t capacity_;
    uint64_t blockSize_;
    FSType type_;
    std::list<std::string> mountPointList_;
    std::map<uint64_t, uint64_t> partitionTxIds_;

 protected:
    curve::common::RWLock rwLock_;
};

class MdsS3FsInfo : public MdsFsInfo {
 public:
    MdsS3FsInfo() : MdsFsInfo() {}
    MdsS3FsInfo(uint32_t fsId, const std::string& fsName, FsStatus status,
                uint64_t rootInodeId, uint64_t capacity, uint64_t blockSize,
                const S3Info& s3Info)
        : MdsFsInfo(fsId, fsName, status, rootInodeId, capacity, blockSize) {
        MdsFsInfo::SetFsType(FSType::TYPE_S3);
        s3Info_.CopyFrom(s3Info);
    }
    void ConvertToProto(FsInfo* file);

 private:
    S3Info s3Info_;
};

class MdsVolumeFsInfo : public MdsFsInfo {
 public:
    MdsVolumeFsInfo(uint32_t fsId, const std::string& fsName, FsStatus status,
                    uint64_t rootInodeId, uint64_t capacity, uint64_t blockSize,
                    const Volume& volume)
        : MdsFsInfo(fsId, fsName, status, rootInodeId, capacity, blockSize) {
        MdsFsInfo::SetFsType(::curvefs::common::FSType::TYPE_VOLUME);
        volume_.CopyFrom(volume);
    }
    void ConvertToProto(FsInfo* file);

 private:
    Volume volume_;
};
}  // namespace mds
}  // namespace curvefs
#endif  // CURVEFS_SRC_MDS_FS_H_
