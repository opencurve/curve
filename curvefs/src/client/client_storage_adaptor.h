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

#ifndef CURVEFS_SRC_CLIENT_CLIENT_STORAGE_ADAPTOR_H_
#define CURVEFS_SRC_CLIENT_CLIENT_STORAGE_ADAPTOR_H_

#include <memory>
#include <string>

#include "curvefs/proto/common.pb.h"
#include "curvefs/proto/mds.pb.h"
#include "curvefs/proto/metaserver.pb.h"

#include "curvefs/src/client/cache/diskcache/disk_cache_manager_impl.h"
#include "curvefs/src/client/cache/fuse_client_cache_manager.h"
#include "curvefs/src/client/common/common.h"
#include "curvefs/src/client/common/config.h"
#include "curvefs/src/client/error_code.h"
#include "curvefs/src/common/define.h"
#include "curvefs/src/client/inode_cache_manager.h"
#include "curvefs/src/client/metric/client_metric.h"
#include "curvefs/src/client/rpcclient/mds_client.h"
#include "curvefs/src/client/s3/client_s3.h"

using ::curve::common::Thread;
using ::curve::common::TaskThreadPool;
using curvefs::client::common::FuseClientOption;
using curvefs::client::common::DiskCacheType;
using curvefs::client::metric::IoMetric;
using curvefs::client::rpcclient::MdsClient;
using curvefs::metaserver::Inode;

namespace curvefs {
namespace client {

class DiskCacheManagerImpl;
class FlushChunkCacheContext;
class ChunkCacheManager;

// callback function for FlushChunkCache
using FlushChunkCacheCallBack = std::function<
  void(const std::shared_ptr<FlushChunkCacheContext>&)>;


/// @brief flush chunk cache context
/// @param inode inode id
/// @param chunkCacheManptr chunk cache manager
/// @param force force flush memory cache
/// @param cb callback function
/// @param retCode error code
struct FlushChunkCacheContext {
    uint64_t inode;
    ChunkCacheManagerPtr chunkCacheManptr;
    bool force;
    FlushChunkCacheCallBack cb;
    CURVEFS_ERROR retCode;
};

// the base class of the underlying storage adaptation layer
class StorageAdaptor {
 public:
    StorageAdaptor() {}
    virtual ~StorageAdaptor() {
        Stop();
    }

    /// @brief init storage adaptor
    /// @param option fuse client option
    /// @param inodeManager inode cache manager
    /// @param mdsClient mds client
    /// @param fsCacheManager fscache manager
    /// @param diskCacheManagerImpl disk cache manager
    /// @param kvClientManager kv client manager
    /// @param fsInfo file system information
    /// @return error code
    virtual CURVEFS_ERROR
    Init(const FuseClientOption &option,
        std::shared_ptr<InodeCacheManager> inodeManager,
        std::shared_ptr<MdsClient> mdsClient,
        std::shared_ptr<FsCacheManager> fsCacheManager,
        std::shared_ptr<DiskCacheManagerImpl> diskCacheManagerImpl,
        std::shared_ptr<KVClientManager> kvClientManager,
        std::shared_ptr<FsInfo> fsInfo);

    virtual CURVEFS_ERROR FuseOpInit(void *userdata,
        struct fuse_conn_info *conn);

    virtual int Stop();

    /// @brief flush data cache to backend storage(s3 or volume)
    /// @param req flush request
    /// @param writeOffset length that has been written to backend storage
    /// @return error code
    virtual CURVEFS_ERROR FlushDataCache(const UperFlushRequest& req,
      uint64_t* writeOffset) = 0;

    /// @brief read data from backend storage(s3 or volume)
    /// @param request read request
    /// @return error code
    virtual CURVEFS_ERROR ReadFromLowlevel(UperReadRequest request) = 0;

    /// @brief wrire data to memory cache
    /// @param inodeId file inode id
    /// @param offset offset of data in file
    /// @param length length of data
    /// @param buf data need to write
    /// @return length of data that has been written to memory cache
    virtual int Write(uint64_t inodeId, uint64_t offset,
      uint64_t length, const char *buf);

    /// @brief read data from memory cache
    /// @param inodeId file inode id
    /// @param offset offset of data in file
    /// @param length length need to read
    /// @param buf data buffer
    /// @return length of data that has been read from memory cache
    virtual int Read(uint64_t inodeId, uint64_t offset,
      uint64_t length, char *buf);

    virtual CURVEFS_ERROR Truncate(InodeWrapper* inodeWrapper,
      uint64_t size) = 0;

    /// @brief allocate chunk id from mds
    /// @param fsId file system id
    /// @param idNum chunks number need to allocate
    /// @param chunkId chunk id that has been allocated
    /// @return error code
    FSStatusCode AllocChunkId(uint32_t fsId,
      uint32_t idNum, uint64_t *chunkId) {
        return mdsClient_->AllocS3ChunkId(fsId, idNum, chunkId);
    }

    /// @brief release all cache of file
    /// @param inodeId file inode id
    void ReleaseCache(uint64_t inodeId);

    virtual CURVEFS_ERROR Flush(uint64_t inodeId);
    virtual CURVEFS_ERROR FlushAllCache(uint64_t inodeId);
    virtual CURVEFS_ERROR FsSync();

    /// @brief enqueue flush chunk cache context
    void Enqueue(std::shared_ptr<FlushChunkCacheContext> context);

    void FsSyncSignal() {
        std::lock_guard<std::mutex> lk(mtx_);
        VLOG(3) << "fs sync signal";
        cond_.notify_one();
    }

    void FsSyncSignalAndDataCacheInc() {
        std::lock_guard<std::mutex> lk(mtx_);
        fsCacheManager_->DataCacheNumInc();
        VLOG(3) << "fs sync signal";
        cond_.notify_one();
    }

/*** get and set element ***/

    void SetFsId(uint32_t fsId) {
        fsId_ = fsId;
    }

    uint64_t GetBlockSize() {
        return blockSize_;
    }

    uint64_t GetChunkSize() {
        return chunkSize_;
    }

    void SetBlockSize(const uint64_t& blockSize) {
        blockSize_ = blockSize;
    }

    void SetChunkSize(const uint64_t& chunkSize) {
        chunkSize_ = chunkSize;
    }

    std::shared_ptr<FsCacheManager> GetFsCacheManager() {
        return fsCacheManager_;
    }

    uint32_t GetFlushInterval() { return flushIntervalSec_; }

    uint32_t GetDiskCacheType() {
        return diskCacheType_;
    }

    bool DisableDiskCache() {
        return diskCacheType_ == DiskCacheType::Disable;
    }

    bool HasDiskCache() {
        return diskCacheType_ != DiskCacheType::Disable;
    }

    bool IsReadCache() {
        return diskCacheType_ == DiskCacheType::OnlyRead;
    }

    bool IsReadWriteCache() {
        return diskCacheType_ == DiskCacheType::ReadWrite;
    }
    virtual std::shared_ptr<InodeCacheManager> GetInodeCacheManager() {
        return inodeManager_;
    }

    std::shared_ptr<DiskCacheManagerImpl> GetDiskCacheManager() {
        return diskCacheManagerImpl_;
    }

    uint32_t GetFsId() {
        return fsId_;
    }

    uint32_t GetPageSize() {
        return pageSize_;
    }

    void DisableBgFlush() {
        enableBgFlush_ = false;
    }

    std::shared_ptr<IoMetric> GetMetric() {
      return ioMetric_;
    }

    void CollectMetrics(InterfaceMetric *interface,
      int count, uint64_t start) {
        interface->bps.count << count;
        interface->qps.count << 1;
        interface->latency << (butil::cpuwide_time_us() - start);
    }

    void SetDiskCache(DiskCacheType type) {
       diskCacheType_ = type;
    }

    CachePoily GetCachePolicy(bool sync) {
      const bool mayCache =
        HasDiskCache() && !GetDiskCacheManager()->IsDiskCacheFull() && !sync;
        if (IsReadCache() && mayCache) {
            return CachePoily::RCache;
        } else if (IsReadWriteCache() && mayCache) {
            return CachePoily::WRCache;
        } else {
            return CachePoily::NCache;
        }
    }

    std::shared_ptr<MdsClient> GetMdsClient() {
        return mdsClient_;
    }

    std::string GetMountOwner() {
        return mountOwner_;
    }

    void SetMountOwner(const std::string& mountOwner) {
        mountOwner_ = mountOwner;
    }

 private:
    int FlushChunkClosure(std::shared_ptr<FlushChunkCacheContext> context);
    int ClearDiskCache(int64_t inodeId);
    void BackGroundFlush();

    void InitMetrics(const std::string &fsName) {
        fsName_ = fsName;
        ioMetric_ = std::make_shared<IoMetric>(fsName);
        // init disk cache metrics(needed)
        if (HasDiskCache()) {
        diskCacheManagerImpl_->InitMetrics(fsName);
        }
    }

 protected:
  std::shared_ptr<FsCacheManager> fsCacheManager_;
  std::shared_ptr<MdsClient> mdsClient_;
  std::shared_ptr<FsInfo> fsInfo_;

 private:
    uint64_t blockSize_;
    uint64_t chunkSize_;
    uint32_t pageSize_;
    uint32_t fuseMaxSize_;
    uint32_t flushIntervalSec_;
    uint32_t chunkFlushThreads_;
    uint32_t memCacheNearfullRatio_;
    uint32_t throttleBaseSleepUs_;
    Thread bgFlushThread_;
    std::atomic<bool> toStop_;
    std::mutex mtx_;
    std::mutex ioMtx_;
    std::condition_variable cond_;
    curve::common::WaitInterval waitInterval_;
    std::shared_ptr<InodeCacheManager> inodeManager_;
    std::shared_ptr<DiskCacheManagerImpl> diskCacheManagerImpl_;
    DiskCacheType diskCacheType_;
    std::atomic<uint64_t> pendingReq_;
    uint32_t fsId_;
    std::string fsName_;
    std::string mountOwner_;
    std::shared_ptr<IoMetric> ioMetric_;
    bool enableBgFlush_ = true;

    TaskThreadPool<bthread::Mutex, bthread::ConditionVariable>
        taskPool_;
};

}  // namespace client
}  // namespace curvefs

#endif  // CURVEFS_SRC_CLIENT_CLIENT_STORAGE_ADAPTOR_H_
