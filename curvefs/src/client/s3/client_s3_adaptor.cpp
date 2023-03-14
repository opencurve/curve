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
 * Created Date: 21-5-31
 * Author: huyao
 */

#include <utility>
#include <map>

#include "curvefs/src/client/s3/client_s3_adaptor.h"
#include "curvefs/src/client/cache/diskcache/disk_cache_manager_impl.h"
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
    std::shared_ptr<KVClientManager> kvClientManager,
    std::shared_ptr<FsInfo> fsInfo) {
    LOG(INFO) << "S3ClientAdaptorImpl init start.";
    const S3ClientAdaptorOption& clientOption =
      option.s3Opt.s3ClientAdaptorOpt;
    prefetchBlocks_ = clientOption.prefetchBlocks;
    prefetchExecQueueNum_ = clientOption.prefetchExecQueueNum;
    readRetryIntervalMs_ = clientOption.readRetryIntervalMs;
    SetBlockSize(clientOption.blockSize);
    SetChunkSize(clientOption.chunkSize);
    CURVEFS_ERROR ret = StorageAdaptor::Init(option, inodeManager,
      mdsClient, fsCacheManager, diskCacheManagerImpl,
      nullptr, fsInfo);
    if (ret != CURVEFS_ERROR::OK) {
        return ret;
    }
    if (nullptr != diskCacheManagerImpl) {
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
    }
    if (nullptr == client_) {
        client_ = std::make_shared<S3ClientImpl>();
    }

    client_->Init(option.s3Opt.s3AdaptrOpt);
    kvClientManager_ = kvClientManager;
    LOG(INFO) << "S3ClientAdaptorImpl init success.";
    return ret;
}

int S3ClientAdaptorImpl::Stop() {
    LOG(INFO) << "start Stopping S3ClientAdaptor.";
    if (HasDiskCache()) {
        for (auto &q : downloadTaskQueues_) {
            bthread::execution_queue_stop(q);
            bthread::execution_queue_join(q);
        }
    }
    StorageAdaptor::Stop();
    client_->Deinit();
    curve::common::S3Adapter::Shutdown();
    LOG(INFO) << "Stop S3ClientAdaptor success.";
    return 0;
}

CURVEFS_ERROR S3ClientAdaptorImpl::FlushDataCache(
  const UperFlushRequest& req, uint64_t* writeOffset) {
    uint64_t start = butil::cpuwide_time_us();
    // generate flush task
    std::vector<std::shared_ptr<PutObjectAsyncContext>> s3Tasks;
    std::vector<std::shared_ptr<SetKVCacheTask>> kvCacheTasks;
    CURVEFS_ERROR ret = PrepareFlushTasks(req, &s3Tasks,
      &kvCacheTasks, writeOffset);
    VLOG(9) << "FlushDataCache, s3 task size is: " <<  s3Tasks.size()
               << ", kv task size is: " << kvCacheTasks.size();
    if (CURVEFS_ERROR::OK != ret) {
        LOG(ERROR) << "FlushDataCache error: " << ret;
        return ret;
    }

    // exec flush task
    FlushTaskExecute(GetCachePolicy(req.sync), s3Tasks, kvCacheTasks);
    if (nullptr != GetMetric()) {
        CollectMetrics(&GetMetric()->adaptorFlushBackend, 1, start);
    }
    return CURVEFS_ERROR::OK;
}

CURVEFS_ERROR S3ClientAdaptorImpl::PrepareFlushTasks(
  const UperFlushRequest& req,
  std::vector<std::shared_ptr<PutObjectAsyncContext>> *s3Tasks,
  std::vector<std::shared_ptr<SetKVCacheTask>> *kvCacheTasks,
  uint64_t* writeOffset) {
    // generate flush task
    uint64_t chunkSize = GetChunkSize();
    uint64_t blockSize = GetBlockSize();
    uint64_t chunkPos = req.chunkPos;
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
        if (kvClientManager_) {
            auto task = std::make_shared<SetKVCacheTask>();
            uint64_t start = butil::cpuwide_time_us();
            task->key = objectName;
            task->value = req.buf + (*writeOffset);
            // task->value = data + (*writeOffset);
            task->length = curentLen;
            task->startTime = start;
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
    VLOG(9) << "FlushTaskExecute start.";
    // callback
    std::atomic<uint64_t> s3PendingTaskCal(s3Tasks.size());
    std::atomic<uint64_t> kvPendingTaskCal(kvCacheTasks.size());
    CountDownEvent s3TaskEnvent(s3PendingTaskCal);
    CountDownEvent kvTaskEnvent(kvPendingTaskCal);
    PutObjectAsyncCallBack s3cb =
        [&](const std::shared_ptr<PutObjectAsyncContext> &context) {
            if (context->retCode == 0) {
                if (nullptr != GetMetric()) {
                    CollectMetrics(&GetMetric()->adaptorWriteS3,
                      context->bufferSize, context->startTime);
                }
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
        if (nullptr != GetMetric()) {
            CollectMetrics(&GetMetric()->adaptorWriteKvCache,
                task->length, task->startTime);
        }
        kvTaskEnvent.Signal();
        return;
    };

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
    if (kvClientManager_ && kvPendingTaskCal.load()) {
        std::for_each(kvCacheTasks.begin(), kvCacheTasks.end(),
                      [&](const std::shared_ptr<SetKVCacheTask> &task) {
                          task->done = kvdone;
                          kvClientManager_->Set(task);
                      });
        kvTaskEnvent.Wait();
    }
    s3TaskEnvent.Wait();
    VLOG(9) << "FlushTaskExecute sucess.";
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

void S3ClientAdaptorImpl::PrefetchS3Objs(uint64_t inodeId,
    const std::vector<std::pair<std::string, uint64_t>> &prefetchObjs) {
    for (auto &obj : prefetchObjs) {
        std::string name = obj.first;
        uint64_t readLen = obj.second;
        curve::common::LockGuard lg(downloadMtx_);
        if (downloadingObj_.find(name) != downloadingObj_.end()) {
            VLOG(9) << "obj is already in downloading: " << name
                    << ", size: " << downloadingObj_.size();
            continue;
        }
        if (GetDiskCacheManager()->IsCached(name)) {
            VLOG(9) << "downloading is exist in cache: " << name
                    << ", size: " << downloadingObj_.size();
            continue;
        }
        VLOG(9) << "download start, inode is: " << inodeId
                << ", obj name is: " << name
                << ", downloading size is: " << downloadingObj_.size();
        downloadingObj_.emplace(name);

        auto inode = inodeId;
        auto task = [this, name, inode, readLen]() {
            char *dataCacheS3 = new char[readLen];
            auto context = std::make_shared<GetObjectAsyncContext>();
            context->key = name;
            context->buf = dataCacheS3;
            context->offset = 0;
            context->len = readLen;
            context->cb = AsyncPrefetchCallback{inode, this};
            VLOG(9) << "prefetch start: " << context->key
                    << ", len: " << context->len;
            GetS3Client()->DownloadAsync(context);
        };
        PushAsyncTask(task);
    }
    return;
}

void S3ClientAdaptorImpl::PrefetchForBlock(const S3ReadRequest &req,
                                       uint64_t fileLen, uint64_t blockSize,
                                       uint64_t chunkSize,
                                       uint64_t startBlockIndex) {
    uint32_t prefetchBlocks = GetPrefetchBlocks();
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
    PrefetchS3Objs(req.inodeId, prefetchObjs);
}

void S3ClientAdaptorImpl::HandleReadRequest(
    const ReadRequest &request, const S3ChunkInfo &s3ChunkInfo,
    std::vector<ReadRequest> *addReadRequests,
    std::vector<uint64_t> *deletingReq, std::vector<S3ReadRequest> *requests,
    char *dataBuf, uint64_t fsId, uint64_t inodeId) {
    uint64_t blockSize = GetBlockSize();
    uint64_t chunkSize = GetChunkSize();
    S3ReadRequest s3Request;
    uint64_t s3ChunkInfoOffset = s3ChunkInfo.offset();
    uint64_t s3ChunkInfoLen = s3ChunkInfo.len();
    uint64_t fileOffset = request.index * chunkSize + request.chunkPos;
    uint64_t length = request.len;
    uint64_t bufOffset = request.bufOffset;
    uint64_t readOffset = 0;

    VLOG(9) << "HandleReadRequest request index:" << request.index
            << ",chunkPos:" << request.chunkPos << ",len:" << request.len
            << ",bufOffset:" << request.bufOffset;
    VLOG(9) << "HandleReadRequest s3info chunkid:" << s3ChunkInfo.chunkid()
            << ",offset:" << s3ChunkInfoOffset << ",len:" << s3ChunkInfoLen
            << ",compaction:" << s3ChunkInfo.compaction()
            << ",zero:" << s3ChunkInfo.zero();
    /*
             -----             read block
                    ------     S3ChunkInfo
    */
    if (fileOffset + length <= s3ChunkInfoOffset) {
        return;
        /*
             -----              ------------   read block           -
                ------             -----       S3ChunkInfo
        */
    } else if ((s3ChunkInfoOffset > fileOffset) &&
               (s3ChunkInfoOffset < fileOffset + length)) {
        ReadRequest splitRequest;
        splitRequest.index = request.index;
        splitRequest.chunkPos = request.chunkPos;
        splitRequest.len = s3ChunkInfoOffset - fileOffset;
        splitRequest.bufOffset = bufOffset;
        addReadRequests->emplace_back(splitRequest);
        deletingReq->emplace_back(request.chunkPos);
        readOffset += splitRequest.len;
        /*
             -----                 read block           -
                ------             S3ChunkInfo
        */
        if (fileOffset + length <= s3ChunkInfoOffset + s3ChunkInfoLen) {
            if (s3ChunkInfo.zero()) {
                memset(static_cast<char *>(dataBuf) + bufOffset + readOffset, 0,
                       fileOffset + length - s3ChunkInfoOffset);
            } else {
                s3Request.chunkId = s3ChunkInfo.chunkid();
                s3Request.offset = s3ChunkInfoOffset;
                s3Request.len = fileOffset + length - s3ChunkInfoOffset;
                s3Request.objectOffset =
                    s3ChunkInfoOffset % chunkSize % blockSize;
                s3Request.readOffset = bufOffset + readOffset;
                s3Request.compaction = s3ChunkInfo.compaction();
                s3Request.fsId = fsId;
                s3Request.inodeId = inodeId;
                requests->push_back(s3Request);
            }
            /*
                                 ------------   read block           -
                                    -----       S3ChunkInfo
            */
        } else {
            if (s3ChunkInfo.zero()) {
                memset(static_cast<char *>(dataBuf) + bufOffset + readOffset, 0,
                       s3ChunkInfoLen);
            } else {
                s3Request.chunkId = s3ChunkInfo.chunkid();
                s3Request.offset = s3ChunkInfoOffset;
                s3Request.len = s3ChunkInfoLen;
                s3Request.objectOffset =
                    s3ChunkInfoOffset % chunkSize % blockSize;
                s3Request.readOffset = bufOffset + readOffset;
                s3Request.compaction = s3ChunkInfo.compaction();
                s3Request.fsId = fsId;
                s3Request.inodeId = inodeId;
                requests->push_back(s3Request);
            }
            ReadRequest splitRequest;

            readOffset += s3ChunkInfoLen;
            splitRequest.index = request.index;
            splitRequest.chunkPos = request.chunkPos + readOffset;
            splitRequest.len =
                fileOffset + length - (s3ChunkInfoOffset + s3ChunkInfoLen);
            splitRequest.bufOffset = bufOffset + readOffset;
            addReadRequests->emplace_back(splitRequest);
        }
        /*
              ----                      ---------   read block
            ----------                --------      S3ChunkInfo
        */
    } else if ((s3ChunkInfoOffset <= fileOffset) &&
               (s3ChunkInfoOffset + s3ChunkInfoLen > fileOffset)) {
        deletingReq->emplace_back(request.chunkPos);
        /*
              ----                    read block
            ----------                S3ChunkInfo
        */
        if (fileOffset + length <= s3ChunkInfoOffset + s3ChunkInfoLen) {
            if (s3ChunkInfo.zero()) {
                memset(static_cast<char *>(dataBuf) + bufOffset + readOffset, 0,
                       length);
            } else {
                s3Request.chunkId = s3ChunkInfo.chunkid();
                s3Request.offset = fileOffset;
                s3Request.len = length;
                if (fileOffset / blockSize == s3ChunkInfoOffset / blockSize) {
                    s3Request.objectOffset =
                        s3ChunkInfoOffset % chunkSize % blockSize;
                } else {
                    s3Request.objectOffset = 0;
                }
                s3Request.readOffset = bufOffset + readOffset;
                s3Request.compaction = s3ChunkInfo.compaction();
                s3Request.fsId = fsId;
                s3Request.inodeId = inodeId;
                requests->push_back(s3Request);
            }
            /*
                                      ---------   read block
                                    --------      S3ChunkInfo
            */
        } else {
            if (s3ChunkInfo.zero()) {
                memset(static_cast<char *>(dataBuf) + bufOffset + readOffset, 0,
                       s3ChunkInfoOffset + s3ChunkInfoLen - fileOffset);
            } else {
                s3Request.chunkId = s3ChunkInfo.chunkid();
                s3Request.offset = fileOffset;
                s3Request.len = s3ChunkInfoOffset + s3ChunkInfoLen - fileOffset;
                if (fileOffset / blockSize == s3ChunkInfoOffset / blockSize) {
                    s3Request.objectOffset =
                        s3ChunkInfoOffset % chunkSize % blockSize;
                } else {
                    s3Request.objectOffset = 0;
                }
                s3Request.readOffset = bufOffset + readOffset;
                s3Request.compaction = s3ChunkInfo.compaction();
                s3Request.fsId = fsId;
                s3Request.inodeId = inodeId;
                requests->push_back(s3Request);
            }
            readOffset += s3ChunkInfoOffset + s3ChunkInfoLen - fileOffset;
            ReadRequest splitRequest;
            splitRequest.index = request.index;
            splitRequest.chunkPos = request.chunkPos + s3ChunkInfoOffset +
                                    s3ChunkInfoLen - fileOffset;
            splitRequest.len =
                fileOffset + length - (s3ChunkInfoOffset + s3ChunkInfoLen);
            splitRequest.bufOffset = bufOffset + readOffset;
            addReadRequests->emplace_back(splitRequest);
        }
        /*
                    -----  read block
            ----           S3ChunkInfo
        do nothing
        */
    } else {
    }
}

void S3ClientAdaptorImpl::GenerateS3Request(ReadRequest request,
                                         const S3ChunkInfoList &s3ChunkInfoList,
                                         char *dataBuf,
                                         std::vector<S3ReadRequest> *requests,
                                         uint64_t fsId, uint64_t inodeId) {
    // first is chunkPos, user read request is split into multiple,
    // and emplace in the readRequests;
    std::map<uint64_t, ReadRequest> readRequests;

    VLOG(9) << "GenerateS3Request start request index:" << request.index
            << ",chunkPos:" << request.chunkPos << ",len:" << request.len
            << ",bufOffset:" << request.bufOffset;
    readRequests.emplace(request.chunkPos, request);
    for (int i = s3ChunkInfoList.s3chunks_size() - 1; i >= 0; i--) {
        const S3ChunkInfo &s3ChunkInfo = s3ChunkInfoList.s3chunks(i);
        // readRequests is split by current s3ChunkInfo, emplace_back to the
        // addReadRequests
        std::vector<ReadRequest> addReadRequests;
        // if readRequest is split to one or two, old readRequest should be
        // delete,
        std::vector<uint64_t> deletingReq;
        for (auto readRequestIter = readRequests.begin();
             readRequestIter != readRequests.end(); readRequestIter++) {
            HandleReadRequest(readRequestIter->second, s3ChunkInfo,
                              &addReadRequests, &deletingReq, requests, dataBuf,
                              fsId, inodeId);
        }

        for (auto iter = deletingReq.begin(); iter != deletingReq.end();
             iter++) {
            readRequests.erase(*iter);
        }

        for (auto addIter = addReadRequests.begin();
             addIter != addReadRequests.end(); addIter++) {
            auto ret = readRequests.emplace(addIter->chunkPos, *addIter);
            if (!ret.second) {
                LOG(ERROR) << "read request emplace failed. chunkPos:"
                           << addIter->chunkPos << ",len:" << addIter->len
                           << ",index:" << addIter->index
                           << ",bufOffset:" << addIter->bufOffset;
            }
        }

        if (readRequests.empty()) {
            VLOG(6) << "readRequests has hit s3ChunkInfos.";
            break;
        }
    }

    for (auto emptyIter = readRequests.begin(); emptyIter != readRequests.end();
         emptyIter++) {
        VLOG(9) << "empty buf index:" << emptyIter->second.index
                << ", chunkPos:" << emptyIter->second.chunkPos
                << ", len:" << emptyIter->second.len
                << ", bufOffset:" << emptyIter->second.bufOffset;
        memset(dataBuf + emptyIter->second.bufOffset, 0, emptyIter->second.len);
    }

    auto s3RequestIter = requests->begin();
    for (; s3RequestIter != requests->end(); s3RequestIter++) {
        VLOG(9) << "s3Request chunkid:" << s3RequestIter->chunkId
                << ",offset:" << s3RequestIter->offset
                << ",len:" << s3RequestIter->len
                << ",objectOffset:" << s3RequestIter->objectOffset
                << ",readOffset:" << s3RequestIter->readOffset
                << ",fsid:" << s3RequestIter->fsId
                << ",inodeId:" << s3RequestIter->inodeId
                << ",compaction:" << s3RequestIter->compaction;
    }

    return;
}

int S3ClientAdaptorImpl::GenerateKVReuqest(
    const std::shared_ptr<InodeWrapper> &inodeWrapper,
    const std::vector<ReadRequest> &readRequest, char *dataBuf,
    std::vector<S3ReadRequest> *kvRequest) {

    ::curve::common::UniqueLock lgGuard = inodeWrapper->GetUniqueLock();
    const Inode *inode = inodeWrapper->GetInodeLocked();
    const auto *s3chunkinfo = inodeWrapper->GetChunkInfoMap();
    VLOG(9) << "process inode: " << inode->DebugString();
    for_each(
        readRequest.begin(), readRequest.end(), [&](const ReadRequest &req) {
            VLOG(6) << req.DebugString();
            auto infoIter = s3chunkinfo->find(req.index);
            if (infoIter == s3chunkinfo->end()) {
                VLOG(6) << "inode = " << inode->inodeid()
                        << " s3chunkinfo do not find index = " << req.index;
                memset(dataBuf + req.bufOffset, 0, req.len);
                return;
            } else {
                std::vector<S3ReadRequest> tmpKVRequests;
                GenerateS3Request(req, infoIter->second, dataBuf,
                                  &tmpKVRequests, inode->fsid(),
                                  inode->inodeid());
                kvRequest->insert(kvRequest->end(), tmpKVRequests.begin(),
                                  tmpKVRequests.end());
            }
        });

    VLOG(9) << "process inode: " << S3ReadRequestVecDebugString(*kvRequest)
            << " ok";

    return 0;
}

CURVEFS_ERROR S3ClientAdaptorImpl::ReadFromLowlevel(UperReadRequest request) {
    uint64_t start = butil::cpuwide_time_us();
    // generate read request
    std::vector<ReadRequest> readRequests;
    readRequests = std::move(request.requests);
    uint64_t inodeId = request.inodeId;
    char* readBuf = request.buf;
    std::shared_ptr<InodeWrapper> inodeWrapper;
    inodeWrapper = request.inodeWapper;
    // generate kv request
    std::vector<S3ReadRequest> uperRequests;
    GenerateKVReuqest(inodeWrapper, readRequests, readBuf,
      &uperRequests);

    uint32_t retry = 0;
    do {
        // read from kv cluster (localcache -> remote kv cluster -> s3)
        // localcache/remote kv cluster fail will not return error code.
        // Failure to read from s3 will eventually return failure.
        int ret = ReadKVRequest(uperRequests, readBuf,
          inodeWrapper->GetLength());
        if (ret >= 0) {
            // read ok
            break;
        } else if (ret == -2) {
            // TODO(@anybody): ret should replace the current number with a
            // meaningful error code
            //  read from s3 not exist
            // 1. may be the metaserver compaction update inode is not
            //    synchronized to the client. clear inodecache && get agin
            // 2. if it returns -2 multiple times, it may be the data of a
            //    client has not been flushed back to s3, and need keep
            //    retrying.
            if (0 != HandleReadS3NotExist(ret, retry++, inodeWrapper)) {
                return CURVEFS_ERROR::INTERNAL;
            }
        } else {
            LOG(INFO) << "read inode = " << inodeId
                      << " from s3 failed, ret = " << ret;
            return CURVEFS_ERROR::INTERNAL;
        }
    } while (1);

    if (nullptr != GetMetric()) {
        CollectMetrics(&GetMetric()->adaptorReadBackend, 1, start);
    }
    return CURVEFS_ERROR::OK;
}

int S3ClientAdaptorImpl::HandleReadS3NotExist(
    int ret, uint32_t retry,
    const std::shared_ptr<InodeWrapper> &inodeWrapper) {
    uint32_t retryIntervalMs = GetReadRetryIntervalMs();

    if (retry == 1) {
        curve::common::UniqueLock lgGuard = inodeWrapper->GetUniqueLock();
        if (CURVEFS_ERROR::OK != inodeWrapper->RefreshS3ChunkInfo()) {
            LOG(ERROR) << "refresh inode: " << inodeWrapper->GetInodeId()
                       << " fail";
            return -1;
        }
    } else if (retry * retryIntervalMs < maxReadRetryIntervalMs_) {
        LOG(WARNING) << "read inode: " << inodeWrapper->GetInodeId()
                     << " retry: " << retry;
        bthread_usleep(retryIntervalMs * retry * 1000);
    } else {
        LOG(WARNING) << "read inode: " << inodeWrapper->GetInodeId()
            << " retry: " << retry << ", reach max interval: "
            << maxReadRetryIntervalMs_ << "ms";
        bthread_usleep(maxReadRetryIntervalMs_ * 1000);
    }

    return 0;
}

bool S3ClientAdaptorImpl::ReadKVRequestFromLocalCache(const std::string &name,
                                                      char *databuf,
                                                      uint64_t offset,
                                                      uint64_t len) {
    bool mayCached = HasDiskCache() && GetDiskCacheManager()->IsCached(name);
    if (!mayCached) {
        return false;
    }
    uint64_t start = butil::cpuwide_time_us();
    if (0 > GetDiskCacheManager()->Read(name, databuf, offset, len)) {
        LOG(WARNING) << "object " << name << " not cached in disk";
        return false;
    }
    if (nullptr != GetMetric()) {
        CollectMetrics(&GetMetric()->adaptorReadDiskCache,
          len, start);
    }
    return true;
}

bool S3ClientAdaptorImpl::ReadKVRequestFromRemoteCache(const std::string &name,
                                                    char *databuf,
                                                    uint64_t offset,
                                                    uint64_t length) {
    if (!kvClientManager_) {
        return false;
    }
    uint64_t start = butil::cpuwide_time_us();
    auto task = std::make_shared<GetKVCacheTask>(name, databuf, offset, length);
    CountDownEvent event(1);
    task->done = [&](const std::shared_ptr<GetKVCacheTask> &task) {
        event.Signal();
        return;
    };
    kvClientManager_->Get(task);
    event.Wait();
    if (nullptr != GetMetric()) {
        CollectMetrics(&GetMetric()->adaptorReadKvCache,
          length, start);
    }
    return task->res;
}

bool S3ClientAdaptorImpl::ReadKVRequestFromS3(const std::string &name,
                                           char *databuf, uint64_t offset,
                                           uint64_t length, int *ret) {
    uint64_t start = butil::cpuwide_time_us();
    *ret = GetS3Client()->Download(name, databuf, offset,
                                                length);
    if (*ret < 0) {
        LOG(ERROR) << "object " << name << " read from s3 fail, ret = " << *ret;
        return false;
    }
    if (nullptr != GetMetric()) {
        CollectMetrics(&GetMetric()->adaptorReadS3,
          length, start);
    }
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

        // add data to memory read cache
        if (!curvefs::client::common::FLAGS_enableCto) {
            FileCacheManagerPtr fileCacheManager = nullptr;
           fileCacheManager =
              fsCacheManager_->FindFileCacheManager(req->inodeId);
            if (nullptr == fileCacheManager) {
                LOG(ERROR) << "get fileCacheManager fail";
                return -1;
            }
            auto chunkCacheManager =
              fileCacheManager->FindOrCreateChunkCacheManager(chunkIndex);
            WriteLockGuard writeLockGuard(chunkCacheManager->rwLockChunk_);
            DataCachePtr dataCache = std::make_shared<DataCache>(
                this, chunkCacheManager, chunkPos, req->len,
                dataBuf + req->readOffset, nullptr);
            chunkCacheManager->AddReadDataCache(dataCache);
        }
    }
    return 0;
}

CURVEFS_ERROR S3ClientAdaptorImpl::Truncate(
  InodeWrapper *inodeWrapper, uint64_t size) {
    const auto *inode = inodeWrapper->GetInodeLocked();
    uint64_t fileSize = inode->length();
    uint64_t chunkSize = GetChunkSize();
    if (size < fileSize) {
        VLOG(6) << "Truncate size:" << size
                << " less than fileSize:" << fileSize;
        FileCacheManagerPtr fileCacheManager =
            fsCacheManager_->FindOrCreateFileCacheManager(GetFsId(),
                                                          inode->inodeid());
        fileCacheManager->TruncateCache(size, fileSize);
        return CURVEFS_ERROR::OK;
    } else if (size == fileSize) {
        return CURVEFS_ERROR::OK;
    } else {
        VLOG(6) << "Truncate size:" << size << " more than fileSize"
                << fileSize;
        uint64_t offset = fileSize;
        uint64_t len = size - fileSize;
        uint64_t index = offset / chunkSize;
        uint64_t chunkPos = offset % chunkSize;
        uint64_t n = 0;
        uint64_t beginChunkId;
        FSStatusCode ret;
        uint64_t fsId = inode->fsid();
        uint32_t chunkIdNum = len / chunkSize + 1;
        ret = AllocChunkId(fsId, chunkIdNum, &beginChunkId);
        if (ret != FSStatusCode::OK) {
            LOG(ERROR) << "Truncate alloc s3 chunkid fail. ret:" << ret;
            return CURVEFS_ERROR::INTERNAL;
        }
        uint64_t chunkId = beginChunkId;
        while (len > 0) {
            if (chunkPos + len > chunkSize) {
                n = chunkSize - chunkPos;
            } else {
                n = len;
            }
            assert(chunkId <= (beginChunkId + chunkIdNum - 1));
            S3ChunkInfo *tmp;
            auto* s3ChunkInfoMap = inodeWrapper->GetChunkInfoMap();
            auto s3chunkInfoListIter = s3ChunkInfoMap->find(index);
            if (s3chunkInfoListIter == s3ChunkInfoMap->end()) {
                S3ChunkInfoList s3chunkInfoList;
                tmp = s3chunkInfoList.add_s3chunks();
                tmp->set_chunkid(chunkId);
                tmp->set_offset(offset);
                tmp->set_len(n);
                tmp->set_size(n);
                tmp->set_zero(true);
                s3ChunkInfoMap->insert({index, s3chunkInfoList});
            } else {
                S3ChunkInfoList &s3chunkInfoList = s3chunkInfoListIter->second;
                tmp = s3chunkInfoList.add_s3chunks();
                tmp->set_chunkid(chunkId);
                tmp->set_offset(offset);
                tmp->set_len(n);
                tmp->set_size(n);
                tmp->set_zero(true);
            }
            len -= n;
            index++;
            chunkPos = (chunkPos + n) % chunkSize;
            offset += n;
            chunkId++;
        }
        return CURVEFS_ERROR::OK;
    }
}

int S3ClientAdaptorImpl::ExecAsyncDownloadTask(
    void *meta,
    bthread::TaskIterator<AsyncDownloadTask> &iter) {  // NOLINT
    if (iter.is_queue_stopped()) {
        return 0;
    }

    for (; iter; ++iter) {
        auto &task = *iter;
        task();
    }

    return 0;
}

}  // namespace client
}  // namespace curvefs
