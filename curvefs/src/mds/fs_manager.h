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
 * Created Date: 2021-05-19
 * Author: chenwei
 */

#ifndef CURVEFS_SRC_MDS_FS_MANAGER_H_
#define CURVEFS_SRC_MDS_FS_MANAGER_H_

#include <atomic>
#include <memory>
#include <string>
#include <vector>

#include "curvefs/proto/mds.pb.h"
#include "curvefs/src/common/define.h"
#include "curvefs/src/mds/fs.h"
#include "curvefs/src/mds/fs_storage.h"
#include "curvefs/src/mds/metaserver_client.h"
#include "curvefs/src/mds/space_client.h"

namespace curvefs {
namespace mds {
class FsFactory {
 public:
    virtual ~FsFactory() {}
    virtual std::shared_ptr<MdsFsInfo> GeneratorFsInfo(uint32_t fsId) = 0;
};

class FsVolumeFactory : public FsFactory {
 public:
    FsVolumeFactory(const std::string& fsName, FsStatus status,
                    uint64_t rootInodeId, uint64_t capacity, uint64_t blockSize,
                    const Volume& volume)
        : fsName_(fsName),
          status_(status),
          rootInodeId_(rootInodeId),
          capacity_(capacity),
          blockSize_(blockSize),
          volume_(volume) {}

    ~FsVolumeFactory() = default;
    std::shared_ptr<MdsFsInfo> GeneratorFsInfo(uint32_t fsId) override {
        return std::make_shared<MdsVolumeFsInfo>(fsId, fsName_, status_,
                                                 rootInodeId_, capacity_,
                                                 blockSize_, volume_);
    }

 private:
    std::string fsName_;
    FsStatus status_;
    uint64_t rootInodeId_;
    uint64_t capacity_;
    uint64_t blockSize_;
    Volume volume_;
};

class FsS3Factory : public FsFactory {
 public:
    FsS3Factory(const std::string& fsName, FsStatus status,
                uint64_t rootInodeId, uint64_t capacity, uint64_t blockSize,
                const S3Info& s3Info)
        : fsName_(fsName),
          status_(status),
          rootInodeId_(rootInodeId),
          capacity_(capacity),
          blockSize_(blockSize),
          s3Info_(s3Info) {}

    ~FsS3Factory() = default;
    std::shared_ptr<MdsFsInfo> GeneratorFsInfo(uint32_t fsId) override {
        return std::make_shared<MdsS3FsInfo>(fsId, fsName_, status_,
                                             rootInodeId_, capacity_,
                                             blockSize_, s3Info_);
    }

 private:
    std::string fsName_;
    FsStatus status_;
    uint64_t rootInodeId_;
    uint64_t capacity_;
    uint64_t blockSize_;
    S3Info s3Info_;
};

class FsManager {
 public:
    FsManager(std::shared_ptr<FsStorage> fsStorage,
              std::shared_ptr<SpaceClient> spaceClient,
              std::shared_ptr<MetaserverClient> metaserverClient)
        : fsStorage_(fsStorage),
          spaceClient_(spaceClient),
          metaserverClient_(metaserverClient) {}

    bool Init();

    void Uninit();

    /**
     * @brief create fs, the fs name can not repeate
     *
     * @param[in] fsName: the fs name, can't be repeated
     * @param[in] blockSize: space alloc must align this blockSize
     * @param[in] volume: the fs alloc space for file from the volume
     * @param[out] fsInfo: the fs created
     *
     * @return If success return OK; if fsName exist, return FS_EXIST;
     *         else return error code
     */
    FSStatusCode CreateFs(const std::string& fsName, uint64_t blockSize,
                          const Volume& volume, FsInfo* fsInfo);

    /**
     * @brief create fs, the fs name can not repeate
     *
     * @param[in] fsName: the fs name, can't be repeated
     * @param[in] blockSize: space alloc must align this blockSize
     * @param[in] s3Info: the fs alloc space for file from the s3
     * @param[out] fsInfo: the fs created
     *
     * @return If success return OK; if fsName exist, return FS_EXIST;
     *         else return error code
     */
    FSStatusCode CreateFs(const std::string& fsName, uint64_t blockSize,
                          const S3Info& s3Info, FsInfo* fsInfo);

    /**
     * @brief delete fs, fs must unmount first
     *
     * @param[in] fsName: the fsName of fs which want to delete
     *
     * @return If success return OK; if fs has mount point, return FS_BUSY;
     *         else return error code
     */
    FSStatusCode DeleteFs(const std::string& fsName);

    /**
     * @brief Mount fs, mount point can not repeate. It will increate
     * mountNum.
     *        If before mount, the mountNum is 0, call spaceClient to
     * InitSpace.
     *
     * @param[in] fsName: fsname of fs
     * @param[in] mountpoint: where the fs mount
     * @param[out] fsInfo: return the fsInfo
     *
     * @return If success return OK;
     *         if fs has same mount point, return MOUNT_POINT_EXIST;
     *         else return error code
     */
    FSStatusCode MountFs(const std::string& fsName,
                         const std::string& mountpoint, FsInfo* fsInfo);

    /**
     * @brief Umount fs, it will decreate mountNum.
     *        If mountNum decreate to zero, call spaceClient to UnInitSpace
     *
     * @param[in] fsName: fsname of fs
     * @param[in] mountpoint: the mountpoint need to umount
     *
     * @return If success return OK;
     *         else return error code
     */
    FSStatusCode UmountFs(const std::string& fsName,
                          const std::string& mountpoint);

    /**
     * @brief get fs info by fsname
     *
     * @param[in] fsName: the fsname want to get
     * @param[out] fsInfo: the fsInfo got
     *
     * @return If success return OK; else return error code
     */
    FSStatusCode GetFsInfo(const std::string& fsName, FsInfo* fsInfo);

    /**
     * @brief get fs info by fsid
     *
     * @param[in] fsId: the fsid want to get
     * @param[out] fsInfo: the fsInfo got
     *
     * @return If success return OK; else return error code
     */
    FSStatusCode GetFsInfo(uint32_t fsId, FsInfo* fsInfo);

    /**
     * @brief get fs info by fsid and fsname
     *
     * @param[in] fsId: the fsid of fs want to get
     * @param[in] fsName: the fsname of fs want to get
     * @param[out] fsInfo: the fsInfo got
     *
     * @return If success return OK; else return error code
     */
    FSStatusCode GetFsInfo(const std::string& fsName, uint32_t fsId,
                           FsInfo* fsInfo);

    FSStatusCode CommitTx(uint32_t fsId,
                          const std::vector<PartitionTxId>& txIds);

 private:
    uint32_t GetNextFsId();
    uint64_t GetRootId();
    FSStatusCode CleanFsInodeAndDentry(uint32_t fsId);
    FSStatusCode CreateFsIntenal(const std::string& fsName,
                                 std::shared_ptr<FsFactory> factory,
                                 FsInfo* fsInfo);

 private:
    std::atomic<uint64_t> nextFsId_;
    std::shared_ptr<FsStorage> fsStorage_;
    std::shared_ptr<SpaceClient> spaceClient_;
    std::shared_ptr<MetaserverClient> metaserverClient_;
};
}  // namespace mds
}  // namespace curvefs

#endif  // CURVEFS_SRC_MDS_FS_MANAGER_H_
