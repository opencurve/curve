#include "curvefs/src/client/volume/client_volume_adaptor.h"


#include "curvefs/src/client/rpcclient/mds_client.h"

namespace curvefs {
namespace client {

using ::curvefs::volume::BlockDeviceClientOptions;
using ::curvefs::volume::SpaceManagerOption;
using ::curvefs::volume::BlockDeviceClientOptions;
using ::curvefs::volume::BlockDeviceClientImpl;


CURVEFS_ERROR
    VolumeClientAdaptorImpl::Init(const FuseClientOption &option,
         std::shared_ptr<InodeCacheManager> inodeManager,
         std::shared_ptr<MdsClient> mdsClient,
         std::shared_ptr<FsCacheManager> fsCacheManager,
         std::shared_ptr<DiskCacheManagerImpl> diskCacheManagerImpl,
         bool startBackGround, std::shared_ptr<FsInfo> fsInfo) {
    CURVEFS_ERROR ret = CURVEFS_ERROR::OK;

    LOG(ERROR) << "whs volume adaptor fuse init client0 !";
    ret = StorageAdaptor::Init(option, inodeManager,
      mdsClient, fsCacheManager, diskCacheManagerImpl, true, fsInfo);
    if (ret != CURVEFS_ERROR::OK) {
        return ret;
    }

    volOpts_ = option.volumeOpt;
    BlockDeviceClientOptions opts;
    opts.configPath = option.bdevOpt.configPath;

    bool ret2 = blockDeviceClient_->Init(opts);
    if (!ret2) {
        LOG(ERROR) << "Init block device client failed";
        return CURVEFS_ERROR::INTERNAL;
    }
    LOG(ERROR) << "whs volume adaptor fuse init client1 !";
    return ret;
}

CURVEFS_ERROR VolumeClientAdaptorImpl::FuseOpInit(void *userdata,
  struct fuse_conn_info *conn, uint64_t fsid, std::string fsname) {
    LOG(ERROR) << "whs volume adaptor fuse init 1!";
    StorageAdaptor::FuseOpInit(userdata, conn, fsid, fsname);
    const auto &vol = GetFsInfo()->detail().volume();
    const auto &volName = vol.volumename();
    const auto &user = vol.user();

    auto ret2 = blockDeviceClient_->Open(volName, user);
    if (!ret2) {
        LOG(ERROR) << "BlockDeviceClientImpl open failed, ret = " << ret2
                   << ", volName = " << volName << ", user = " << user;
        return CURVEFS_ERROR::INTERNAL;
    }

    SpaceManagerOption option;
    option.blockGroupManagerOption.fsId = GetFsInfo()->fsid();

    option.blockGroupManagerOption.owner = GetMountOwner();

    option.blockGroupManagerOption.blockGroupAllocateOnce =
        volOpts_.allocatorOption.blockGroupOption.allocateOnce;
    option.blockGroupManagerOption.blockGroupSize =
        GetFsInfo()->detail().volume().blockgroupsize();
    option.blockGroupManagerOption.blockSize =
        GetFsInfo()->detail().volume().blocksize();

    option.allocatorOption.type = volOpts_.allocatorOption.type;
    option.allocatorOption.bitmapAllocatorOption.sizePerBit =
        volOpts_.allocatorOption.bitmapAllocatorOption.sizePerBit;
    option.allocatorOption.bitmapAllocatorOption.smallAllocProportion =
        volOpts_.allocatorOption.bitmapAllocatorOption.smallAllocProportion;

    spaceManager_ = absl::make_unique<SpaceManagerImpl>(
        option, GetMdsClient(), blockDeviceClient_);

    storage_ = absl::make_unique<DefaultVolumeStorage>(
        spaceManager_.get(), blockDeviceClient_.get(), GetInodeCacheManager().get());

    ExtentCacheOption extentOpt;
    extentOpt.blockSize = vol.blocksize();
    extentOpt.sliceSize = vol.slicesize();

    ExtentCache::SetOption(extentOpt);

    return CURVEFS_ERROR::OK;
}

CURVEFS_ERROR VolumeClientAdaptorImpl::FlushDataCache(const ClientRequest& req, uint64_t* writeOffset) {
    uint64_t inodeId = req.inodeId;
    uint64_t inodeOffset = req.offset;
    uint64_t len = req.length;
    const char* data = req.buf;
    VLOG(0) << "whs volume flush dataCache, inode: "
       << inodeId<< ", offset: " << inodeOffset
       << ", length: " << len;

/* 这里sync怎么处理?
struct ClientRequest {
    uint64_t inodeId;
    const char *buf;
    uint64_t length;
    uint64_t offset;
    uint64_t chunkId;  // just s3 need
    bool sync;
};

*/
    
    CURVEFS_ERROR ret = storage_->Write(inodeId, inodeOffset, len, data);
    if (ret != CURVEFS_ERROR::OK) {
        VLOG(0) << "whs volume flush dataCache err, inode: "
            << inodeId<< ", offset: " << inodeOffset
            << ", length: " << len;
        return ret;
    }
                                       
    VLOG(0) << "whs volume flush dataCache end, inode: "
       << inodeId<< ", offset: " << inodeOffset
       << ", length: " << len;
// 这个正确么？ 
    *writeOffset = len;
    return CURVEFS_ERROR::OK;
}

CURVEFS_ERROR VolumeClientAdaptorImpl::ReadFromLowlevel(uint64_t inodeId, uint64_t offset, uint64_t length, char *buf) {
    VLOG(0) << "whs read lowlevel start, inode: "
        << inodeId<< ", offset: " << offset
        << ", length: " << length;

    CURVEFS_ERROR ret = storage_->Read(inodeId, offset, length, buf);
    if (ret != CURVEFS_ERROR::OK) {
        VLOG(0) << "whs volume flush dataCache err, inode: "
            << inodeId << ", offset: " << offset
            << ", length: " << length;
        return ret;
    }
    VLOG(0) << "whs read lowlevel end, inode: "
        << inodeId<< ", offset: " << offset
        << ", length: " << length;
    
    return ret;
}

}  // namespace client
}  // namespace curvefs