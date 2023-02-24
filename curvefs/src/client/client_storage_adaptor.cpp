#include <brpc/channel.h>
#include <brpc/controller.h>
#include <algorithm>
#include <list>

#include "absl/memory/memory.h"
#include "curvefs/src/client/s3/client_s3_adaptor.h"
#include "curvefs/src/common/s3util.h"

namespace curvefs {

namespace client {

CURVEFS_ERROR
StorageAdaptor::Init(const FuseClientOption &fuseOption,
    std::shared_ptr<InodeCacheManager> inodeManager,
    std::shared_ptr<MdsClient> mdsClient,
    std::shared_ptr<FsCacheManager> fsCacheManager,
    std::shared_ptr<DiskCacheManagerImpl> diskCacheManagerImpl,
    bool startBackGround,
    std::shared_ptr<FsInfo> fsInfo) {

/*
startBackGround 这个参数之前是默认参数

*/
     LOG(ERROR) << "whsnew001 StorageAdaptor client0 !";
    startBackGround = true;
    S3ClientAdaptorOption option = fuseOption.s3Opt.s3ClientAdaptorOpt;
// 这里其实不需要了，因为传递进来之前已经处理了
/*
强行设置
*/
   LOG(ERROR) << "whs StorageAdaptor client1: " << option.blockSize
              << " pagesize: " << option.pageSize ;
 // 当前s3的相关值都是tool工具指定的  -- volume也必须要这个信息
    option.blockSize = 4194304;
    option.chunkSize = 67108864;
    option.pageSize = 65536;

    LOG(ERROR) << "whs StorageAdaptor client01 !";

        LOG(ERROR) << "whs StorageAdaptor client02!";
        pendingReq_ = 0;
        blockSize_ = option.blockSize;
        chunkSize_ = option.chunkSize;
        pageSize_ = option.pageSize;
        LOG(ERROR) << "whs StorageAdaptor client1 !" << blockSize_;
        if (chunkSize_ % blockSize_ != 0) {
            LOG(ERROR) << "chunkSize:" << chunkSize_
                    << " is not integral multiple for the blockSize:"
                    << blockSize_;
            return CURVEFS_ERROR::INVALIDPARAM;
        }

    LOG(ERROR) << "whs StorageAdaptor client03!";
    fuseMaxSize_ = option.fuseMaxSize;
    prefetchBlocks_ = option.prefetchBlocks;
    prefetchExecQueueNum_ = option.prefetchExecQueueNum;
    diskCacheType_ = option.diskCacheOpt.diskCacheType;
    memCacheNearfullRatio_ = option.nearfullRatio;
    throttleBaseSleepUs_ = option.baseSleepUs;
    flushIntervalSec_ = option.flushIntervalSec;
    chunkFlushThreads_ = option.chunkFlushThreads;
    maxReadRetryIntervalMs_ = option.maxReadRetryIntervalMs;
    readRetryIntervalMs_ = option.readRetryIntervalMs;
    inodeManager_ = inodeManager;
    mdsClient_ = mdsClient;
    fsCacheManager_ = fsCacheManager;
    waitInterval_.Init(option.intervalSec * 1000);
    diskCacheManagerImpl_ = diskCacheManagerImpl;
    fsInfo_ = fsInfo;
    LOG(ERROR) << "whs StorageAdaptor client2 !";
    if (HasDiskCache()) {
        diskCacheManagerImpl_ = diskCacheManagerImpl;
        if (diskCacheManagerImpl_->Init(option) < 0) {
            LOG(ERROR) << "Init disk cache failed";
            return CURVEFS_ERROR::INTERNAL;
        }
/*
        // init rpc send exec-queue
        downloadTaskQueues_.resize(prefetchExecQueueNum_);
        for (auto &q : downloadTaskQueues_) {
            int rc = bthread::execution_queue_start(
                &q, nullptr, &S3ClientAdaptorImpl::ExecAsyncDownloadTask, this);
            if (rc != 0) {
                LOG(ERROR) << "Init AsyncRpcQueues failed";
                return CURVEFS_ERROR::INTERNAL;
            }
        }
*/
    }
    if (startBackGround) {
        toStop_.store(false, std::memory_order_release);
        bgFlushThread_ = Thread(&StorageAdaptor::BackGroundFlush, this);
    }

    LOG(INFO) << "StorageAdaptor Init. block size:" << blockSize_
              << ", chunk size: " << chunkSize_
              << ", prefetchBlocks: " << prefetchBlocks_
              << ", prefetchExecQueueNum: " << prefetchExecQueueNum_
              << ", intervalSec: " << option.intervalSec
              << ", flushIntervalSec: " << option.flushIntervalSec
              << ", writeCacheMaxByte: " << option.writeCacheMaxByte
              << ", readCacheMaxByte: " << option.readCacheMaxByte
              << ", nearfullRatio: " << option.nearfullRatio
              << ", baseSleepUs: " << option.baseSleepUs;
    // start chunk flush threads
    taskPool_.Start(chunkFlushThreads_);

    return CURVEFS_ERROR::OK;
}

CURVEFS_ERROR StorageAdaptor::FuseOpInit(void *userdata, struct fuse_conn_info *conn,
  uint64_t fsid, std::string fsname) {
    VLOG(9) << " storage adaptor fuse init.";
    SetFsId(fsid);
    InitMetrics(fsname);
    return CURVEFS_ERROR::OK;
}

int StorageAdaptor::Write(uint64_t inodeId, uint64_t offset,
                               uint64_t length, const char *buf) {
    VLOG(6) << "write start offset:" << offset << ", len:" << length
            << ", fsId:" << fsId_ << ", inodeId:" << inodeId;

// fsCacheManager_->FlowControl(length);

    uint64_t start = butil::cpuwide_time_us();
    FileCacheManagerPtr fileCacheManager =
        fsCacheManager_->FindOrCreateFileCacheManager(fsId_, inodeId);
    {
        std::lock_guard<std::mutex> lockguard(ioMtx_);
        pendingReq_.fetch_add(1, std::memory_order_seq_cst);
        VLOG(6) << "pendingReq_ is: " << pendingReq_;
        uint64_t pendingReq = pendingReq_.load(std::memory_order_seq_cst);
        fsCacheManager_->DataCacheByteInc(length);
        uint64_t size = fsCacheManager_->GetDataCacheSize();
        uint64_t maxSize = fsCacheManager_->GetDataCacheMaxSize();
        if ((size + pendingReq * fuseMaxSize_) >= maxSize) {
            VLOG(6) << "write cache is full, wait flush. size: " << size
                    << ", maxSize:" << maxSize;
            // offer to do flush
            waitInterval_.StopWait();
            fsCacheManager_->WaitFlush();
        }
    }

    uint64_t memCacheRatio = fsCacheManager_->MemCacheRatio();
    int64_t exceedRatio = memCacheRatio - memCacheNearfullRatio_;
    if (exceedRatio > 0) {
        // offer to do flush
        waitInterval_.StopWait();
        // upload to s3 derectly or cache disk full
        bool needSleep =
            (DisableDiskCache() || IsReadCache()) ||
            (IsReadWriteCache() && diskCacheManagerImpl_->IsDiskCacheFull());
        if (needSleep) {
            uint32_t exponent = pow(2, (exceedRatio) / 10);
            bthread_usleep(throttleBaseSleepUs_ * exceedRatio * exponent);
            VLOG(6) << "write cache nearfull and use ratio is: "
                    << memCacheRatio << ", exponent is: " << exponent;
        }
    }

    int ret = fileCacheManager->Write(offset, length, buf);
    pendingReq_.fetch_sub(1, std::memory_order_seq_cst);

    fsCacheManager_->DataCacheByteDec(length);
/*
    if (s3Metric_.get() != nullptr) {
        CollectMetrics(&s3Metric_->adaptorWrite, ret, start);
        s3Metric_->writeSize.set_value(length);
    }
*/
    VLOG(6) << "write end inodeId:" << inodeId << ",ret:" << ret
            << ", pendingReq_ is: " << pendingReq_;
    return ret;
}

int StorageAdaptor::Read(uint64_t inodeId, uint64_t offset, uint64_t length, char *buf) {
    VLOG(6) << "read start offset:" << offset << ", len:" << length
            << ", fsId:" << fsId_ << ", inodeId:" << inodeId;
    uint64_t start = butil::cpuwide_time_us();
    FileCacheManagerPtr fileCacheManager =
        fsCacheManager_->FindOrCreateFileCacheManager(fsId_, inodeId);

    int ret = fileCacheManager->Read(inodeId, offset, length, buf);
    VLOG(6) << "read end inodeId:" << inodeId << ",ret:" << ret;
    if (ret < 0) {
        return ret;
    }
/*
    if (s3Metric_.get() != nullptr) {
        CollectMetrics(&s3Metric_->adaptorRead, ret, start);
        s3Metric_->readSize.set_value(length);
    }
*/
    VLOG(6) << "read end offset:" << offset << ", len:" << length
            << ", fsId:" << fsId_ << ", inodeId:" << inodeId;
    return ret;
}

// whs need todo
int StorageAdaptor::ReadS3(uint64_t inodeId, uint64_t offset, uint64_t length, char *buf) {
    VLOG(6) << "read start offset:" << offset << ", len:" << length
            << ", fsId:" << fsId_ << ", inodeId:" << inodeId;
    uint64_t start = butil::cpuwide_time_us();
    FileCacheManagerPtr fileCacheManager =
        fsCacheManager_->FindOrCreateFileCacheManager(fsId_, inodeId);

    int ret = fileCacheManager->ReadS3(inodeId, offset, length, buf);
    VLOG(6) << "read end inodeId:" << inodeId << ",ret:" << ret;
    if (ret < 0) {
        return ret;
    }
/*
    if (s3Metric_.get() != nullptr) {
        CollectMetrics(&s3Metric_->adaptorRead, ret, start);
        s3Metric_->readSize.set_value(length);
    }
*/
    VLOG(6) << "read end offset:" << offset << ", len:" << length
            << ", fsId:" << fsId_ << ", inodeId:" << inodeId;
    return ret;
}

void StorageAdaptor::BackGroundFlush() {
    while (!toStop_.load(std::memory_order_acquire)) {
        {
            std::unique_lock<std::mutex> lck(mtx_);
            if (fsCacheManager_->GetDataCacheNum() == 0) {
                VLOG(3) << "BackGroundFlush has no write cache, so wait";
                cond_.wait(lck);
            }
        }
        if (fsCacheManager_->MemCacheRatio() > memCacheNearfullRatio_) {
            VLOG(3) << "BackGroundFlush radically, write cache num is: "
                    << fsCacheManager_->GetDataCacheNum()
                    << "cache ratio is: " << fsCacheManager_->MemCacheRatio();
            fsCacheManager_->FsSync(true);

        } else {
            waitInterval_.WaitForNextExcution();
            VLOG(6) << "BackGroundFlush, write cache num is:"
                    << fsCacheManager_->GetDataCacheNum()
                    << "cache ratio is: " << fsCacheManager_->MemCacheRatio();
            fsCacheManager_->FsSync(false);
            VLOG(6) << "background fssync end";
        }
    }
    return;
}

void StorageAdaptor::Enqueue(
  std::shared_ptr<FlushChunkCacheContext> context) {
    auto task = [this, context]() {
        this->FlushChunkClosure(context);
    };
    taskPool_.Enqueue(task);
}

int StorageAdaptor::FlushChunkClosure(
  std::shared_ptr<FlushChunkCacheContext> context) {
    VLOG(9) << "FlushChunkCacheClosure start: " << context->inode;
    CURVEFS_ERROR ret = context->chunkCacheManptr->Flush(
      context->inode, context->force);
    // set the returned value
    // it is need in FlushChunkCacheCallBack
    context->retCode = ret;
    context->cb(context);
    VLOG(9) << "FlushChunkCacheClosure end: " << context->inode;
    return 0;
}

}
}