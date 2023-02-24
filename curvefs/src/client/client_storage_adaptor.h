

#ifndef CURVEFS_SRC_CLIENT_CLIENT_STORAGE_ADAPTOR_H_
#define CURVEFS_SRC_CLIENT_CLIENT_STORAGE_ADAPTOR_H_

#include "curvefs/src/client/error_code.h"
#include "curvefs/src/client/under_storage.h"
#include "curvefs/src/client/s3/disk_cache_manager_impl.h"
#include "curvefs/proto/metaserver.pb.h"
#include "curvefs/src/common/define.h"



#include "curvefs/proto/common.pb.h"
#include "curvefs/proto/mds.pb.h"

#include "curvefs/src/client/common/common.h"
#include "curvefs/src/client/common/config.h"
#include "curvefs/src/client/error_code.h"
#include "curvefs/src/client/inode_cache_manager.h"
#include "curvefs/src/client/rpcclient/mds_client.h"
#include "curvefs/src/client/s3/client_s3.h"
#include "curvefs/src/client/cache/client_cache_manager.h"
#include "src/common/wait_interval.h"





using ::curve::common::Thread;
using ::curve::common::TaskThreadPool;
using curvefs::client::common::S3ClientAdaptorOption;
using curvefs::client::common::FuseClientOption;
using curvefs::client::common::DiskCacheType;
using curvefs::metaserver::Inode;
using curvefs::metaserver::S3ChunkInfo;
using curvefs::metaserver::S3ChunkInfoList;
using curvefs::client::rpcclient::MdsClient;

namespace curvefs{
namespace client {

class DiskCacheManagerImpl;
class FlushChunkCacheContext;
class ChunkCacheManager;

using FlushChunkCacheCallBack = std::function<
  void(const std::shared_ptr<FlushChunkCacheContext>&)>;

struct FlushChunkCacheContext {
    uint64_t inode;
    ChunkCacheManagerPtr chunkCacheManptr;
    bool force;
    FlushChunkCacheCallBack cb;
    CURVEFS_ERROR retCode;
};

/*
whs: todo 还有很多S3相关字眼  --- 记得搜索替换
*/

enum class CachePoily {
    NCache,
    RCache,
    WRCache,
};

/* the base class of the underlying storage adaptation layer */
class StorageAdaptor {
 public:
    StorageAdaptor(bool s3Adaptor) : s3Adaptor_(s3Adaptor) {}
    virtual ~StorageAdaptor() = default;

//    virtual CURVEFS_ERROR Init(const FuseClientOption &option);

    virtual CURVEFS_ERROR
    Init(const FuseClientOption &option,
         std::shared_ptr<InodeCacheManager> inodeManager,
         std::shared_ptr<MdsClient> mdsClient,
         std::shared_ptr<FsCacheManager> fsCacheManager,
         std::shared_ptr<DiskCacheManagerImpl> diskCacheManagerImpl,
         bool startBackGround,
         std::shared_ptr<FsInfo> fsInfo);
         
    /**
     * @brief Initailize some options for s3 adaptor
     */
    // virtual CURVEFS_ERROR Init() = 0;
    /**
     * @brief write data
     */
    virtual CURVEFS_ERROR FlushDataCache(const ClientRequest& req, uint64_t* writeOffset) = 0;
    int Write(uint64_t inodeId, uint64_t offset, uint64_t length, const char *buf);

    /**
     * @brief read data
     */
    int Read(uint64_t inodeId, uint64_t offset, uint64_t length, char *buf);
    virtual CURVEFS_ERROR ReadFromLowlevel(uint64_t inodeId, uint64_t offset, uint64_t length, char *buf) = 0;
                    
    // whs need todo
    int ReadS3(uint64_t inodeId, uint64_t offset, uint64_t length, char *buf);
    
    virtual CURVEFS_ERROR Truncate(InodeWrapper
      *inodeWrapper,uint64_t size) = 0;

    virtual int ReadKVRequest(const std::vector<S3ReadRequest> &kvRequests,
      char *dataBuf, uint64_t fileLen) = 0;

    // whs need
    FSStatusCode AllocChunkId(uint32_t fsId, uint32_t idNum, uint64_t *chunkId) {
        return mdsClient_->AllocS3ChunkId(fsId, idNum, chunkId);
    }
    void ReleaseCache(uint64_t inodeId) {};
    CURVEFS_ERROR Flush(uint64_t inodeId) {return CURVEFS_ERROR::OK;}
    CURVEFS_ERROR FlushAllCache(uint64_t inodeId) {return CURVEFS_ERROR::OK;}
    CURVEFS_ERROR FsSync() {return CURVEFS_ERROR::OK;}
    int Stop() {return 0;}
    void SetFsId(uint32_t fsId) {
        fsId_ = fsId;
    }
    uint64_t GetBlockSize() {
        return blockSize_;
    }
    uint64_t GetChunkSize() {
        return chunkSize_;
    }
    std::shared_ptr<FsCacheManager> GetFsCacheManager() {
        return fsCacheManager_;
    }
    uint32_t GetFlushInterval() { return flushIntervalSec_; }
    uint32_t GetPrefetchBlocks() {
        return prefetchBlocks_;
    }
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
    uint32_t GetFsId() {
        return fsId_;
    }
    uint32_t GetPageSize() {
        return pageSize_;
    }
    void InitMetrics(const std::string &fsName) {
      fsName_ = fsName;
      // s3独有的
      // s3Metric_ = std::make_shared<S3Metric>(fsName);
      if (HasDiskCache()) {
        diskCacheManagerImpl_->InitMetrics(fsName);
      }
    }
    void CollectMetrics(InterfaceMetric *interface, int count, uint64_t start) {}
    void SetDiskCache(DiskCacheType type) {
       diskCacheType_ = type;
    }

    uint32_t GetMaxReadRetryIntervalMs() const {
        return maxReadRetryIntervalMs_;
    }

    uint32_t GetReadRetryIntervalMs() const {
        return readRetryIntervalMs_;
    }
/*
    virtual std::shared_ptr<UnderStorage> GetUnderStorage() {
        return nullptr;
    }
*/
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

    std::shared_ptr<FsInfo> GetFsInfo() {
        return fsInfo_;
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

    bool IsS3Adaptor() {
        return s3Adaptor_;
    }

    void Enqueue(std::shared_ptr<FlushChunkCacheContext> context);
    virtual CURVEFS_ERROR FuseOpInit(void *userdata, struct fuse_conn_info *conn, uint64_t fsid, std::string fsname);
private:
     using AsyncDownloadTask = std::function<void()>;
    static int ExecAsyncDownloadTask(void* meta, bthread::TaskIterator<AsyncDownloadTask>& iter);  // NOLINT
    int FlushChunkClosure(std::shared_ptr<FlushChunkCacheContext> context);
    int ClearDiskCache(int64_t inodeId);

 protected:
  std::shared_ptr<FsCacheManager> fsCacheManager_;
  std::shared_ptr<MdsClient> mdsClient_;

private:
    uint64_t blockSize_;
    uint64_t chunkSize_;
    uint32_t fuseMaxSize_;
    uint32_t prefetchBlocks_;
    uint32_t prefetchExecQueueNum_;
    std::string allocateServerEps_;
    uint32_t flushIntervalSec_;
    uint32_t chunkFlushThreads_;
    uint32_t memCacheNearfullRatio_;
    uint32_t throttleBaseSleepUs_;
    uint32_t maxReadRetryIntervalMs_;
    uint32_t readRetryIntervalMs_;
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
    std::shared_ptr<FsInfo> fsInfo_;
    std::vector<bthread::ExecutionQueueId<AsyncDownloadTask>> downloadTaskQueues_;
    uint32_t pageSize_;
    bool s3Adaptor_; // whether S3Adaptor or not

    void PushAsyncTask(const AsyncDownloadTask& task) {
        static thread_local unsigned int seed = time(nullptr);

        int idx = rand_r(&seed) % downloadTaskQueues_.size();
        int rc = bthread::execution_queue_execute(
                   downloadTaskQueues_[idx], task);

        if (CURVE_UNLIKELY(rc != 0)) {
            task();
        }
    }

    TaskThreadPool<bthread::Mutex, bthread::ConditionVariable>
        taskPool_;

    void BackGroundFlush();

};
}    
}

#endif  // CURVEFS_SRC_CLIENT_CLIENT_STORAGE_ADAPTOR_H_ 
