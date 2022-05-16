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
#include <set>
#include <string>
#include <vector>

#include "curvefs/proto/mds.pb.h"
#include "curvefs/proto/topology.pb.h"
#include "curvefs/src/common/define.h"
#include "curvefs/src/mds/common/types.h"
#include "curvefs/src/mds/fs_info_wrapper.h"
#include "curvefs/src/mds/fs_storage.h"
#include "curvefs/src/mds/metaserverclient/metaserver_client.h"
#include "curvefs/src/mds/space/manager.h"
#include "curvefs/src/mds/topology/topology.h"
#include "curvefs/src/mds/topology/topology_manager.h"
#include "curvefs/src/mds/topology/topology_storage_codec.h"
#include "curvefs/src/mds/topology/topology_storge_etcd.h"
#include "curvefs/src/mds/dlock/dlock.h"
#include "src/common/concurrent/concurrent.h"
#include "src/common/concurrent/name_lock.h"
#include "src/common/interruptible_sleeper.h"
#include "src/common/s3_adapter.h"

namespace curvefs {
namespace mds {

using ::curvefs::mds::space::SpaceManager;
using ::curvefs::mds::topology::Topology;
using ::curvefs::mds::topology::TopologyManager;

using ::curve::common::Thread;
using ::curve::common::InterruptibleSleeper;
using ::curve::common::Atomic;
using ::curve::common::InterruptibleSleeper;
using ::curve::common::S3Adapter;
using ::curve::common::Thread;
using ::curvefs::mds::topology::PartitionTxId;
using ::curvefs::mds::topology::Topology;
using ::curvefs::mds::topology::TopologyManager;

using ::curvefs::mds::dlock::DLock;
using ::curvefs::mds::Mountpoint;

struct FsManagerOption {
    uint32_t backEndThreadRunInterSec;
    uint32_t spaceReloadConcurrency = 10;
    curve::common::S3AdapterOption s3AdapterOption;
};

class FsManager {
 public:
    FsManager(const std::shared_ptr<FsStorage>& fsStorage,
              const std::shared_ptr<SpaceManager>& spaceManager,
              const std::shared_ptr<MetaserverClient>& metaserverClient,
              const std::shared_ptr<TopologyManager>& topoManager,
              const std::shared_ptr<S3Adapter>& s3Adapter,
              const std::shared_ptr<DLock>& dlock,
              const FsManagerOption& option)
        : fsStorage_(fsStorage),
          spaceManager_(spaceManager),
          metaserverClient_(metaserverClient),
          topoManager_(topoManager),
          s3Adapter_(s3Adapter),
          dlock_(dlock),
          nameLock_(),
          isStop_(true),
          option_(option) {}

    bool Init();
    void Run();
    void Uninit();
    void BackEndFunc();
    void ScanFs(const FsInfoWrapper& wrapper);

    /**
     * @brief create fs, the fs name can not repeat
     *
     * @param CreateFsRequest request
     * @return FSStatusCode If success return OK; if fsName exist, return
     * FS_EXIST;
     *         else return error code
     */
    FSStatusCode CreateFs(const ::curvefs::mds::CreateFsRequest* request,
                          FsInfo* fsInfo);

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
     * @brief Mount fs, mount point can not repeat. It will increate
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
                         const Mountpoint& mountpoint, FsInfo* fsInfo);

    /**
     * @brief Umount fs, it will decrease mountNum.
     *        If mountNum decrease to zero, call spaceClient to UnInitSpace
     *
     * @param[in] fsName: fsname of fs
     * @param[in] mountpoint: the mountpoint need to umount
     *
     * @return If success return OK;
     *         else return error code
     */
    FSStatusCode UmountFs(const std::string& fsName,
                          const Mountpoint& mountpoint);

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

    void GetAllFsInfo(::google::protobuf::RepeatedPtrField<
                      ::curvefs::mds::FsInfo>* fsInfoVec);

    void RefreshSession(
        const google::protobuf::RepeatedPtrField<
            curvefs::mds::topology::PartitionTxId> &txIds,
        google::protobuf::RepeatedPtrField<PartitionTxId> *needUpdate);

    void GetLatestTxId(const GetLatestTxIdRequest* request,
                       GetLatestTxIdResponse* response);

    void CommitTx(const CommitTxRequest* request,
                  CommitTxResponse* response);

 private:
     // return 0: ExactlySame; 1: uncomplete, -1: neither
    int IsExactlySameOrCreateUnComplete(const std::string& fsName,
                                        FSType fsType, uint64_t blocksize,
                                        const FsDetail& detail);

    // send request to metaserver to DeletePartition, if response returns
    // FSStatusCode::OK or FSStatusCode::UNDER_DELETING, returns true;
    // else returns false
    bool DeletePartiton(std::string fsName, const PartitionInfo& partition);

    // set partition status to DELETING in topology
    bool SetPartitionToDeleting(const PartitionInfo& partition);

    FSStatusCode ReloadMountedFsVolumeSpace();

    void GetLatestTxId(const uint32_t fsId,
                       std::vector<PartitionTxId>* txIds);

    FSStatusCode IncreaseFsTxSequence(const std::string& fsName,
                                      const std::string& owner,
                                      uint64_t* sequence);

    FSStatusCode GetFsTxSequence(const std::string& fsName,
                                 uint64_t* sequence);

 private:
    uint64_t GetRootId();

 private:
    std::shared_ptr<FsStorage> fsStorage_;
    std::shared_ptr<SpaceManager> spaceManager_;
    std::shared_ptr<MetaserverClient> metaserverClient_;
    curve::common::GenericNameLock<Mutex> nameLock_;
    std::shared_ptr<TopologyManager> topoManager_;
    std::shared_ptr<S3Adapter> s3Adapter_;
    std::shared_ptr<DLock> dlock_;

    // Manage fs backgroud delete threads
    Thread backEndThread_;
    Atomic<bool> isStop_;
    InterruptibleSleeper sleeper_;
    FsManagerOption option_;
};
}  // namespace mds
}  // namespace curvefs

#endif  // CURVEFS_SRC_MDS_FS_MANAGER_H_
