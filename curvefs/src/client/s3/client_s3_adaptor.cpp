#include "curvefs/src/client/s3/client_s3_adaptor.h"
#include "curvefs/src/client/s3/disk_cache_manager_impl.h"
#include "curvefs/src/common/s3util.h"

namespace curvefs {
namespace client {
namespace common {
DECLARE_bool(enableCto);
}  // namespace common
}  // namespace client
}  // namespace curvefs
namespace curvefs {
namespace client {


CURVEFS_ERROR S3ClientAdaptorImpl::Init(const FuseClientOption &option,
    std::shared_ptr<InodeCacheManager> inodeManager,
    std::shared_ptr<MdsClient> mdsClient,
    std::shared_ptr<FsCacheManager> fsCacheManager,
    std::shared_ptr<DiskCacheManagerImpl> diskCacheManagerImpl,
    bool startBackGround, std::shared_ptr<FsInfo> fsInfo) {

    LOG(ERROR) << "whs S3ClientAdaptorImpl client0 !";
    CURVEFS_ERROR ret = StorageAdaptor::Init(option, inodeManager,
      mdsClient, fsCacheManager, diskCacheManagerImpl, true, fsInfo);
    if (ret != CURVEFS_ERROR::OK) {
        return ret;
    }

    ::curve::common::S3InfoOption fsS3Option;
    client_ = std::make_shared<S3ClientImpl>();
    client_->Init(option.s3Opt.s3AdaptrOpt);

    LOG(ERROR) << "whs S3ClientAdaptorImpl client1 !";
    return ret;
}


/*
CURVEFS_ERROR S3ClientAdaptorImpl::FuseOpInit(void *userdata,
                                       struct fuse_conn_info *conn) {
    CURVEFS_ERROR ret = StorageAdaptor::FuseOpInit(userdata, conn);
    if (init_) {
        SetFsId(fsInfo_->fsid());  
        InitMetrics(fsInfo_->fsname());
    }
    return ret;
}
*/

CURVEFS_ERROR S3ClientAdaptorImpl::FlushDataCache(const ClientRequest& req, uint64_t* writeOffset) {
    LOG(ERROR) << "whs FlushDataCache 00 !";
    // generate flush task
    std::vector<std::shared_ptr<PutObjectAsyncContext>> s3Tasks;
    std::vector<std::shared_ptr<SetKVCacheTask>> kvCacheTasks;
    CURVEFS_ERROR ret = PrepareFlushTasks(req, &s3Tasks, &kvCacheTasks, writeOffset);
    LOG(ERROR) << "whs FlushDataCache 11!";
    if (CURVEFS_ERROR::OK != ret) {
        LOG(ERROR) << "whs FlushDataCache 22 !";
        return ret;
    }

    // exec flush task
    FlushTaskExecute(GetCachePolicy(req.sync), s3Tasks, kvCacheTasks);
    LOG(ERROR) << "whs FlushDataCache 33 !";
    return CURVEFS_ERROR::OK;
}


/* 缓存集群当前有不一致
CURVEFS_ERROR S3ClientAdaptorImpl::FlushDataCache(const ClientRequest& req, uint64_t* writeOffset) {
    LOG(ERROR) << "whs FlushDataCache 00 !";
    // generate flush task
    std::vector<std::shared_ptr<PutObjectAsyncContext>> s3Tasks;
    std::vector<std::shared_ptr<SetKVCacheTask>> kvCacheTasks;
    CURVEFS_ERROR ret = PrepareFlushTasks(req, &s3Tasks, &kvCacheTasks, writeOffset);
    LOG(ERROR) << "whs FlushDataCache 11!";
    if (CURVEFS_ERROR::OK != ret) {
        LOG(ERROR) << "whs FlushDataCache 22 !";
        return ret;
    }

    // exec flush task
    FlushTaskExecute(GetCachePolicy(req.sync), s3Tasks, kvCacheTasks);
    LOG(ERROR) << "whs FlushDataCache 33 !";
    return CURVEFS_ERROR::OK;
}
*/

CURVEFS_ERROR S3ClientAdaptorImpl::PrepareFlushTasks(const ClientRequest& req,
  std::vector<std::shared_ptr<PutObjectAsyncContext>> *s3Tasks,
  std::vector<std::shared_ptr<SetKVCacheTask>> *kvCacheTasks,
  uint64_t* writeOffset) {
    
    // generate flush task
    uint64_t chunkSize = GetChunkSize();
    uint64_t blockSize = GetBlockSize();
    uint64_t chunkPos = req.offset % chunkSize;
    uint64_t blockPos = chunkPos % blockSize;
    uint64_t blockIndex = chunkPos / blockSize;
    // uint64_t blockPos = chunkPos_ % blockSize;
    // uint64_t blockIndex = chunkPos_ / blockSize;
    uint64_t remainLen = req.length;
    uint64_t chunkId = req.chunkId;
    uint64_t fsId = GetFsId();
    uint64_t inodeId = req.inodeId; 
    while (remainLen > 0) {
        uint64_t curentLen =
            blockPos + remainLen > blockSize ? blockSize - blockPos : remainLen;

        // generate flush to disk or s3 task
        std::string objectName = curvefs::common::s3util::GenObjName(
            chunkId, blockIndex, 0, fsId, inodeId);
        int ret = 0;
        uint64_t start = butil::cpuwide_time_us();
        auto context = std::make_shared<PutObjectAsyncContext>();
        context->key = objectName;
        context->buffer = req.buf + (*writeOffset);
        // context->buffer = data + (*writeOffset);
        context->bufferSize = curentLen;
        context->startTime = butil::cpuwide_time_us();
        s3Tasks->emplace_back(context);

        // generate flush to kvcache task
        if (g_kvClientManager) {
            auto task = std::make_shared<SetKVCacheTask>();
            task->key = objectName;
            task->value = req.buf + (*writeOffset);
            // task->value = data + (*writeOffset);
            task->length = curentLen;
            kvCacheTasks->emplace_back(task);
        }

        remainLen -= curentLen;
        blockIndex++;
        (*writeOffset) += curentLen;
        blockPos = (blockPos + curentLen) % blockSize;
    }
    return CURVEFS_ERROR::OK;
}

void S3ClientAdaptorImpl::FlushTaskExecute(
    CachePoily cachePoily,
    const std::vector<std::shared_ptr<PutObjectAsyncContext>> &s3Tasks,
    const std::vector<std::shared_ptr<SetKVCacheTask>> &kvCacheTasks) {
LOG(ERROR) << "whs FlushTaskExecute 00 !";
    // callback
    std::atomic<uint64_t> s3PendingTaskCal(s3Tasks.size());
    std::atomic<uint64_t> kvPendingTaskCal(kvCacheTasks.size());
    CountDownEvent s3TaskEnvent(s3PendingTaskCal);
    CountDownEvent kvTaskEnvent(kvPendingTaskCal);

    PutObjectAsyncCallBack s3cb =
        [&](const std::shared_ptr<PutObjectAsyncContext> &context) {
            if (context->retCode == 0) {
                /*
                if (s3Metric_.get() != nullptr) {
                    CollectMetrics(
                        &s3Metric_->adaptorWriteS3,
                        context->bufferSize, context->startTime);
                }
            */
                if (CachePoily::RCache == cachePoily) {
                    VLOG(9) << "write to read cache, name = " << context->key;
                    GetDiskCacheManager()->Enqueue(context, true);
                }

                // Don't move the if sentence to the front
                // it will cause core dumped because s3Metric_
                // will be destructed before being accessed
                s3TaskEnvent.Signal();
                return;
            }

            LOG(WARNING) << "Put object failed, key: " << context->key;
            GetS3Client()->UploadAsync(context);
        };

    SetKVCacheDone kvdone = [&](const std::shared_ptr<SetKVCacheTask> &task) {
        kvTaskEnvent.Signal();
        return;
    };
LOG(ERROR) << "whs FlushTaskExecute 11 !";
    // s3task execute
    if (s3PendingTaskCal.load()) {
        std::for_each(
            s3Tasks.begin(), s3Tasks.end(),
            [&](const std::shared_ptr<PutObjectAsyncContext> &context) {
                context->cb = s3cb;
                if (CachePoily::WRCache == cachePoily) {
                    GetDiskCacheManager()->Enqueue(context);
                } else {
                    GetS3Client()->UploadAsync(context);
                }
            });
    }
    // kvtask execute
    if (g_kvClientManager && kvPendingTaskCal.load()) {
        std::for_each(kvCacheTasks.begin(), kvCacheTasks.end(),
                      [&](const std::shared_ptr<SetKVCacheTask> &task) {
                          task->done = kvdone;
                          g_kvClientManager->Set(task);
                      });
        kvTaskEnvent.Wait();
    }
LOG(ERROR) << "whs FlushTaskExecute 22 !";
    s3TaskEnvent.Wait();
LOG(ERROR) << "whs FlushTaskExecute 33 !";
}

void S3ClientAdaptorImpl::GetChunkLoc(uint64_t offset, uint64_t *index,
                                   uint64_t *chunkPos, uint64_t *chunkSize) {
    *chunkSize = GetChunkSize();
    *index = offset / *chunkSize;
    *chunkPos = offset % *chunkSize;
}

void S3ClientAdaptorImpl::GetBlockLoc(uint64_t offset, uint64_t *chunkIndex,
                                   uint64_t *chunkPos, uint64_t *blockIndex,
                                   uint64_t *blockPos) {
    uint64_t chunkSize = 0;
    uint64_t blockSize = GetBlockSize();
    GetChunkLoc(offset, chunkIndex, chunkPos, &chunkSize);

    *blockIndex = offset % chunkSize / blockSize;
    *blockPos = offset % chunkSize % blockSize;
}

void S3ClientAdaptorImpl::PrefetchForBlock(const S3ReadRequest &req,
                                       uint64_t fileLen, uint64_t blockSize,
                                       uint64_t chunkSize,
                                       uint64_t startBlockIndex) {
                              /* whs
    uint32_t prefetchBlocks = s3ClientAdaptor_->GetPrefetchBlocks();
    


    std::vector<std::pair<std::string, uint64_t>> prefetchObjs;

    uint64_t blockIndex = startBlockIndex;
    for (uint32_t i = 0; i < prefetchBlocks; i++) {
        std::string name = curvefs::common::s3util::GenObjName(
            req.chunkId, blockIndex, req.compaction, req.fsId, req.inodeId);
        uint64_t maxReadLen = (blockIndex + 1) * blockSize;
        uint64_t needReadLen = maxReadLen > fileLen
                                   ? fileLen - blockIndex * blockSize
                                   : blockSize;

        prefetchObjs.push_back(std::make_pair(name, needReadLen));

        blockIndex++;
        if (maxReadLen > fileLen || blockIndex >= chunkSize / blockSize) {
            break;
        }
    }

    PrefetchS3Objs(prefetchObjs);
    */
}

bool S3ClientAdaptorImpl::ReadKVRequestFromLocalCache(const std::string &name,
                                                      char *databuf,
                                                      uint64_t offset,
                                                      uint64_t len) {
    uint64_t start = butil::cpuwide_time_us();

    bool mayCached = HasDiskCache() && GetDiskCacheManager()->IsCached(name);
    if (!mayCached) {
        return false;
    }

    if (0 > GetDiskCacheManager()->Read(name, databuf, offset, len)) {
        LOG(WARNING) << "object " << name << " not cached in disk";
        return false;
    }
/* whs
    if (s3ClientAdaptor_->s3Metric_.get()) {
        s3ClientAdaptor_->CollectMetrics(
            &s3ClientAdaptor_->s3Metric_->adaptorReadS3, len, start);
    }
*/
    return true;
}

bool S3ClientAdaptorImpl::ReadKVRequestFromRemoteCache(const std::string &name,
                                                    char *databuf,
                                                    uint64_t offset,
                                                    uint64_t length) {
    if (!g_kvClientManager) {
        return false;
    }

    return g_kvClientManager->Get(name, databuf, offset, length);
}

bool S3ClientAdaptorImpl::ReadKVRequestFromS3(const std::string &name,
                                           char *databuf, uint64_t offset,
                                           uint64_t length, int *ret) {
    uint64_t start = butil::cpuwide_time_us();
    *ret = GetS3Client()->Download(name, databuf, offset,
                                                length);
 
 /*
    if (*ret < 0) {
        LOG(ERROR) << "object " << name << " read from s3 fail, ret = " << *ret;
        return false;
    } else if (s3ClientAdaptor_->s3Metric_.get()) {
        s3ClientAdaptor_->CollectMetrics(
            &s3ClientAdaptor_->s3Metric_->adaptorReadS3, length, start);
    }
*/
    return true;
}

int S3ClientAdaptorImpl::ReadKVRequest(
    const std::vector<S3ReadRequest> &kvRequests, char *dataBuf,
    uint64_t fileLen) {

    for (auto req = kvRequests.begin(); req != kvRequests.end(); req++) {
        VLOG(6) << "read from kv request " << req->DebugString();

        uint64_t chunkIndex = 0, chunkPos = 0, blockIndex = 0, blockPos = 0;
        uint64_t chunkSize = GetChunkSize();
        uint64_t blockSize = GetBlockSize();
        GetBlockLoc(req->offset, &chunkIndex, &chunkPos, &blockIndex,
                    &blockPos);

        // prefetch
        if (HasDiskCache()) {
            PrefetchForBlock(*req, fileLen, blockSize, chunkSize, blockIndex);
        }

        // read request
        // |--------------------------------|----------------------------------|
        // 0                             blockSize                   2*blockSize
        //                blockPos                   length + blockPos
        //                   |-------------------------|
        //                   |--------------|
        //                    currentReadLen
        uint64_t length = req->len;
        uint64_t currentReadLen = 0;
        uint64_t readBufOffset = 0;
        uint64_t objectOffset = req->objectOffset;

        while (length > 0) {
            int ret = 0;
            currentReadLen =
                length + blockPos > blockSize ? blockSize - blockPos : length;
            assert(blockPos >= objectOffset);
            std::string name = curvefs::common::s3util::GenObjName(
                req->chunkId, blockIndex, req->compaction, req->fsId,
                req->inodeId);
            char *currentBuf = dataBuf + req->readOffset + readBufOffset;

            // read from localcache -> remotecache -> s3
            if (ReadKVRequestFromLocalCache(name, currentBuf,
                                            blockPos - objectOffset,
                                            currentReadLen)) {
                VLOG(9) << "read " << name << " from local cache ok";
            } else if (ReadKVRequestFromRemoteCache(name, currentBuf,
                                                    blockPos - objectOffset,
                                                    currentReadLen)) {
                VLOG(9) << "read " << name << " from remote cache ok";
            } else if (ReadKVRequestFromS3(name, currentBuf,
                                           blockPos - objectOffset,
                                           currentReadLen, &ret)) {
                VLOG(9) << "read " << name << " from s3 ok";
            } else {
                LOG(ERROR) << "read " << name << " fail";
                return ret;
            }

            // update param
            {
                length -= currentReadLen;         // Remaining read data length
                readBufOffset += currentReadLen;  // next read offset
                blockIndex++;
                blockPos = (blockPos + currentReadLen) % blockSize;
                objectOffset = 0;
            }
        }

// whs：共用的
        // add data to memory read cache
        if (!curvefs::client::common::FLAGS_enableCto) {
            FileCacheManagerPtr fileCacheManager = nullptr;
           fileCacheManager =
              fsCacheManager_->FindFileCacheManager(req->inodeId);
            if (nullptr == fileCacheManager) {
                // whs: 这个返回值可以么？
                LOG(ERROR) << "get fileCacheManager fail";
                return -1;
            }
            auto chunkCacheManager =
              fileCacheManager->FindOrCreateChunkCacheManager(chunkIndex);
            WriteLockGuard writeLockGuard(chunkCacheManager->rwLockChunk_);
            // whs：这里用this指针可以么?
            DataCachePtr dataCache = std::make_shared<DataCache>(
                this, chunkCacheManager, chunkPos, req->len,
                dataBuf + req->readOffset);
            chunkCacheManager->AddReadDataCache(dataCache);
        }
    }
    return 0;
}

}
}