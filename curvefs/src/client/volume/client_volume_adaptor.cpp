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

#include <utility>

#include "curvefs/src/client/volume/client_volume_adaptor.h"

#include "curvefs/src/client/rpcclient/mds_client.h"

namespace curvefs {
namespace client {

#define VOLUME_BLOCK_SIZE 4194304
#define VOLUME_CHUNK_SIZE 67108864

CURVEFS_ERROR VolumeClientAdaptorImpl::Init(const FuseClientOption &option,
         std::shared_ptr<InodeCacheManager> inodeManager,
         std::shared_ptr<MdsClient> mdsClient,
         std::shared_ptr<FsCacheManager> fsCacheManager,
         std::shared_ptr<DiskCacheManagerImpl> diskCacheManagerImpl,
         std::shared_ptr<KVClientManager> kvClientManager,
         std::shared_ptr<FsInfo> fsInfo) {
    LOG(INFO) << "volume adaptor init start.";
    volOpts_ = option.volumeOpt;
    SetBlockSize(VOLUME_BLOCK_SIZE);
    SetChunkSize(VOLUME_CHUNK_SIZE);
    auto ret = StorageAdaptor::Init(option, inodeManager,
      mdsClient, fsCacheManager, diskCacheManagerImpl, kvClientManager, fsInfo);
    if (ret != CURVEFS_ERROR::OK) {
        return ret;
    }
    BlockDeviceClientOptions opts;
    opts.configPath = option.bdevOpt.configPath;
    auto ret2 = blockDeviceClient_->Init(opts);
    if (!ret2) {
        LOG(ERROR) << "Init block device client failed, " << ret2;
        return CURVEFS_ERROR::INTERNAL;
    }

    LOG(INFO) << "volume adaptor init sucess.";
    return ret;
}

CURVEFS_ERROR VolumeClientAdaptorImpl::FuseOpInit(void *userdata,
  struct fuse_conn_info *conn) {
    LOG(INFO) << "volume fuse op init start!";
    const auto &vol = fsInfo_->detail().volume();
    const auto &volName = vol.volumename();
    const auto &user = vol.user();
    auto ret = blockDeviceClient_->Open(volName, user);
    if (!ret) {
        LOG(ERROR) << "BlockDeviceClientImpl open failed, ret = " << ret
                   << ", volName = " << volName << ", user = " << user;
        return CURVEFS_ERROR::INTERNAL;
    }

    SpaceManagerOption option;
    option.blockGroupManagerOption.fsId = fsInfo_->fsid();

    option.blockGroupManagerOption.owner = GetMountOwner();

    option.blockGroupManagerOption.blockGroupAllocateOnce =
        volOpts_.allocatorOption.blockGroupOption.allocateOnce;
    option.blockGroupManagerOption.blockGroupSize =
        fsInfo_->detail().volume().blockgroupsize();
    option.blockGroupManagerOption.blockSize =
        fsInfo_->detail().volume().blocksize();

    option.allocatorOption.type = volOpts_.allocatorOption.type;
    option.allocatorOption.bitmapAllocatorOption.sizePerBit =
        volOpts_.allocatorOption.bitmapAllocatorOption.sizePerBit;
    option.allocatorOption.bitmapAllocatorOption.smallAllocProportion =
        volOpts_.allocatorOption.bitmapAllocatorOption.smallAllocProportion;

    spaceManager_ = absl::make_unique<SpaceManagerImpl>(
        option, mdsClient_, blockDeviceClient_);

    storage_ = absl::make_unique<DefaultVolumeStorage>(spaceManager_.get(),
      blockDeviceClient_.get(), GetInodeCacheManager().get());

    ExtentCacheOption extentOpt;
    extentOpt.blockSize = vol.blocksize();
    extentOpt.sliceSize = vol.slicesize();

    ExtentCache::SetOption(extentOpt);
    LOG(INFO) << "volume fuse op init sucess.";
    return CURVEFS_ERROR::OK;
}

int VolumeClientAdaptorImpl::Stop() {
    LOG(INFO) << "volume adaptor stop...";
    StorageAdaptor::Stop();
    if (nullptr != storage_) {
        storage_->Shutdown();
    }
    if (nullptr != spaceManager_) {
        spaceManager_->Shutdown();
    }
    blockDeviceClient_->UnInit();
    LOG(INFO) << "volume adaptor stop sucess.";
    return 0;
}

CURVEFS_ERROR VolumeClientAdaptorImpl::FlushDataCache(
  const UperFlushRequest& req, uint64_t* writeOffset) {
    uint64_t inodeId = req.inodeId;
    uint64_t inodeOffset = req.offset;
    uint64_t len = req.length;
    const char* data = req.buf;
    VLOG(9) << "volume flush dataCache, inode: "
       << inodeId<< ", offset: " << inodeOffset
       << ", length: " << len;

    CURVEFS_ERROR ret = storage_->Write(inodeId, inodeOffset, len, data);
    if (ret != CURVEFS_ERROR::OK) {
        VLOG(0) << "volume flush dataCache err, inode: "
            << inodeId<< ", offset: " << inodeOffset
            << ", length: " << len;
        return ret;
    }

    VLOG(9) << "volume flush dataCache end, inode: "
       << inodeId<< ", offset: " << inodeOffset
       << ", length: " << len;
// 这个正确么？
    *writeOffset = len;
    return CURVEFS_ERROR::OK;
}

CURVEFS_ERROR VolumeClientAdaptorImpl::ReadFromLowlevel(
  UperReadRequest uperRequest) {
    uint64_t inodeId = uperRequest.inodeId;
    VLOG(9) << "read lowlevel start, inodeId is: " << inodeId;
    std::vector<ReadRequest> requests;
    char *buf = uperRequest.buf;;
    requests = std::move(uperRequest.requests);
    uint64_t chunkSize = GetChunkSize();
    CURVEFS_ERROR ret;
    for (auto req : requests) {
        VLOG(9) << "read from storage " << req.DebugString();
        uint64_t len = req.len;
        uint64_t readOffset = chunkSize * req.index + req.chunkPos;
        ret = storage_->Read(
          inodeId, readOffset, len, buf + req.bufOffset);
        if (ret != CURVEFS_ERROR::OK) {
            VLOG(0) << "volume flush dataCache err, inode: "
                << inodeId << ", offset: " << readOffset
                << ", length: " << len;
            return ret;
        }
    }

    VLOG(9) << "read lowlevel end, inodeId is: "<< inodeId;
    return ret;
}

}  // namespace client
}  // namespace curvefs
