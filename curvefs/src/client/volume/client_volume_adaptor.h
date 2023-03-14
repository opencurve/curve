/*
 *  Copyright (c) 2023 NetEase Inc.
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
 * Created Date: Thur March 14 2023
 * Author: wuhongsong
 */

#ifndef CURVEFS_SRC_CLIENT_VOLUME_CLIENT_VOLUME_ADAPTOR_H_
#define CURVEFS_SRC_CLIENT_VOLUME_CLIENT_VOLUME_ADAPTOR_H_

#include <bthread/execution_queue.h>

#include <memory>
#include <string>
#include <vector>

#include "curvefs/proto/common.pb.h"
#include "curvefs/proto/mds.pb.h"
#include "curvefs/proto/metaserver.pb.h"

#include "src/common/wait_interval.h"
#include "curvefs/src/client/common/common.h"
#include "curvefs/src/client/common/config.h"
#include "curvefs/src/client/client_storage_adaptor.h"
#include "curvefs/src/client/error_code.h"
#include "curvefs/src/client/inode_cache_manager.h"
#include "curvefs/src/client/rpcclient/mds_client.h"
#include "curvefs/src/client/s3/client_s3.h"

#include "curvefs/src/client/volume/default_volume_storage.h"
#include "curvefs/src/client/volume/volume_storage.h"
#include "curvefs/src/volume/block_device_client.h"
#include "curvefs/src/volume/space_manager.h"

namespace curvefs {
namespace client {

using common::VolumeOption;
using ::curvefs::volume::BlockDeviceClient;
using ::curvefs::volume::BlockDeviceClientImpl;
using ::curvefs::volume::BlockDeviceClientOptions;
using ::curvefs::volume::SpaceManager;
using ::curvefs::volume::SpaceManagerImpl;
using ::curvefs::volume::SpaceManagerOption;

// client use volume internal interface
class VolumeClientAdaptorImpl : public StorageAdaptor {
 public:
    VolumeClientAdaptorImpl() : StorageAdaptor(),
      blockDeviceClient_(std::make_shared<BlockDeviceClientImpl>()) {}

    explicit VolumeClientAdaptorImpl(const std::shared_ptr<
      BlockDeviceClient> &blockDeviceClient) : StorageAdaptor(),
      blockDeviceClient_(blockDeviceClient) {}

    virtual ~VolumeClientAdaptorImpl() {
        LOG(INFO) << "delete VolumeClientAdaptorImpl";
    }

 public:
    /// @brief
    /// @param option
    /// @param inodeManager
    /// @param mdsClient
    /// @param fsCacheManager
    /// @param diskCacheManagerImpl
    /// @param kvClientManager
    /// @param fsInfo
    /// @return
    CURVEFS_ERROR Init(const FuseClientOption &option,
      std::shared_ptr<InodeCacheManager> inodeManager,
      std::shared_ptr<MdsClient> mdsClient,
      std::shared_ptr<FsCacheManager> fsCacheManager,
      std::shared_ptr<DiskCacheManagerImpl> diskCacheManagerImpl,
      std::shared_ptr<KVClientManager> kvClientManager,
      std::shared_ptr<FsInfo> fsInfo) override;

    /// @brief
    /// @param userdata
    /// @param conn
    /// @param fsid
    /// @param fsname
    /// @return
    CURVEFS_ERROR FuseOpInit(void *userdata,
      struct fuse_conn_info *conn) override;

    int Stop() override;

    /// @brief
    /// @param req
    /// @param writeOffset
    /// @return
    CURVEFS_ERROR FlushDataCache(const UperFlushRequest& req,
      uint64_t* writeOffset);

    CURVEFS_ERROR ReadFromLowlevel(UperReadRequest request) override;

    // TODO(@hzwuhongsong)
    virtual CURVEFS_ERROR Truncate(InodeWrapper *inodeWrapper,
      uint64_t size) { return CURVEFS_ERROR::OK;}

    /// @brief
    /// @return
    std::shared_ptr<VolumeStorage> getUnderStorage() {
        return storage_;
    }

 private:
    std::shared_ptr<BlockDeviceClient> blockDeviceClient_;
    std::unique_ptr<SpaceManager> spaceManager_;
    std::shared_ptr<VolumeStorage> storage_;
    VolumeOption volOpts_;
};

}  // namespace client
}  // namespace curvefs

#endif  // CURVEFS_SRC_CLIENT_VOLUME_CLIENT_VOLUME_ADAPTOR_H_
