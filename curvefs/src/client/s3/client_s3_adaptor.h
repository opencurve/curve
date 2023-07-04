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

#ifndef CURVEFS_SRC_CLIENT_S3_CLIENT_S3_ADAPTOR_H_
#define CURVEFS_SRC_CLIENT_S3_CLIENT_S3_ADAPTOR_H_

#include <bthread/execution_queue.h>

#include <memory>
#include <utility>
#include <set>
#include <string>
#include <vector>

#include "curvefs/proto/common.pb.h"
#include "curvefs/proto/mds.pb.h"
#include "curvefs/proto/metaserver.pb.h"
#include "curvefs/src/client/common/common.h"
#include "curvefs/src/client/common/config.h"
#include "curvefs/src/client/client_storage_adaptor.h"
#include "curvefs/src/client/kvclient/kvclient_manager.h"
#include "curvefs/src/client/kvclient/kvclient.h"
#include "curvefs/src/client/error_code.h"
#include "curvefs/src/client/inode_wrapper.h"
#include "curvefs/src/client/inode_cache_manager.h"
#include "curvefs/src/client/rpcclient/mds_client.h"
#include "curvefs/src/client/s3/client_s3.h"
#include "curvefs/src/client/cache/fuse_client_cache_manager.h"
#include "curvefs/src/client/cache/diskcache/disk_cache_manager_impl.h"
#include "src/common/wait_interval.h"

namespace curvefs {
namespace client {

using curve::common::GetObjectAsyncCallBack;
using curve::common::PutObjectAsyncCallBack;
using curve::common::S3Adapter;
using curvefs::client::common::S3ClientAdaptorOption;
using curvefs::client::metric::IoMetric;

/// @brief s3 read request
/// @param chunkId chunk id
/// @param offset file offset
/// @param len read length
/// @param objectOffset first offset in the block
/// @param readOffset read buf offset
/// @param fsId file system id
/// @param inodeId inode id
/// @param compaction compaction flag
struct S3ReadRequest {
    uint64_t chunkId;
    uint64_t offset;
    uint64_t len;
    uint64_t objectOffset;
    uint64_t readOffset;
    uint64_t fsId;
    uint64_t inodeId;
    uint64_t compaction;

    std::string DebugString() const {
        std::ostringstream os;
        os << "S3ReadRequest ( chunkId = " << chunkId << ", offset = " << offset
           << ", len = " << len << ", objectOffset = " << objectOffset
           << ", readOffset = " << readOffset << ", fsId = " << fsId
           << ", inodeId = " << inodeId << ", compaction = " << compaction
           << " )";
        return os.str();
    }
};

inline std::string
S3ReadRequestVecDebugString(const std::vector<S3ReadRequest> &reqs) {
    std::ostringstream os;
    for_each(reqs.begin(), reqs.end(),
             [&](const S3ReadRequest &req) { os << req.DebugString() << " "; });
    return os.str();
}

// client use s3 internal interface
class S3ClientAdaptorImpl : public StorageAdaptor {
 public:
    S3ClientAdaptorImpl() : StorageAdaptor() {}

    // for unittest
    explicit S3ClientAdaptorImpl(std::shared_ptr<
      S3Client> client) : StorageAdaptor() {
        client_ = client;
    }

    virtual ~S3ClientAdaptorImpl() {
        LOG(INFO) << "delete S3ClientAdaptorImpl";
    }

    /// @brief init s3 storage adaptor
    /// @param option fuse client option
    /// @param inodeManager inode cache manager
    /// @param mdsClient mds client
    /// @param fsCacheManager fscache manager
    /// @param diskCacheManagerImpl disk cache manager
    /// @param kvClientManager kv client manager
    /// @param fsInfo file system information
    /// @return error code
    CURVEFS_ERROR Init(const FuseClientOption &option,
         std::shared_ptr<InodeCacheManager> inodeManager,
         std::shared_ptr<MdsClient> mdsClient,
         std::shared_ptr<FsCacheManager> fsCacheManager,
         std::shared_ptr<DiskCacheManagerImpl> diskCacheManagerImpl,
         std::shared_ptr<KVClientManager> kvClientManager,
         std::shared_ptr<FsInfo> fsInfo) override;

    int Stop() override;

    /// @brief read data from s3 storage
    /// @param request read request
    /// @return error code
    CURVEFS_ERROR FlushDataCache(const UperFlushRequest& req,
      uint64_t* writeOffset) override;

    /// @brief read data from s3 storage
    /// @param request read request
    /// @return error code
    CURVEFS_ERROR ReadFromLowlevel(UperReadRequest request) override;

    CURVEFS_ERROR Truncate(InodeWrapper *inodeWrapper, uint64_t size);

    uint32_t GetObjectPrefix() {
        return objectPrefix_;
    }

    std::shared_ptr<S3Client> GetS3Client() { return client_; }

    CURVEFS_ERROR FuseOpInit(void *userdata,
      struct fuse_conn_info *conn) override {
        StorageAdaptor::FuseOpInit(userdata, conn);
        return CURVEFS_ERROR::OK;
    }

 private:
     enum class ReadStatus {
        OK = 0,
        S3_READ_FAIL = -1,
        S3_NOT_EXIST = -2,
    };

    ReadStatus toReadStatus(const int retCode) {
        ReadStatus st = ReadStatus::OK;
        if (retCode < 0) {
            st = (retCode == -2) ? ReadStatus::S3_NOT_EXIST
                                 : ReadStatus::S3_READ_FAIL;
        }
        return st;
    }

    S3ClientAdaptorImpl::ReadStatus ReadKVRequest(
      const std::vector<S3ReadRequest> &kvRequests,
      char *dataBuf, uint64_t fileLen);

    CURVEFS_ERROR PrepareFlushTasks(const UperFlushRequest& req,
      std::vector<std::shared_ptr<PutObjectAsyncContext>> *s3Tasks,
      std::vector<std::shared_ptr<SetKVCacheTask>> *kvCacheTasks,
      uint64_t* writeOffset);

    void FlushTaskExecute(CachePolicy cachePoily,
      const std::vector<std::shared_ptr<PutObjectAsyncContext>> &s3Tasks,
      const std::vector<std::shared_ptr<SetKVCacheTask>> &kvCacheTasks);

    void PrefetchS3Objs(uint64_t inodeId,
      const std::vector<std::pair<std::string, uint64_t>> &prefetchObjs);

    void HandleReadRequest(
      const ReadRequest &request, const ChunkInfo &ChunkInfo,
      std::vector<ReadRequest> *addReadRequests,
      std::vector<uint64_t> *deletingReq, std::vector<S3ReadRequest> *requests,
      char *dataBuf, uint64_t fsId, uint64_t inodeId);

    void GenerateS3Request(ReadRequest request,
      const ChunkInfoList &ChunkInfoList,
      char *dataBuf,
      std::vector<S3ReadRequest> *requests,
      uint64_t fsId,
      uint64_t inodeId);

    // miss read from memory read/write cache, need read from
    // kv(localdisk/remote cache/s3)
    int GenerateKVReuqest(const std::shared_ptr<InodeWrapper> &inodeWrapper,
                          const std::vector<ReadRequest> &readRequest,
                          char *dataBuf, std::vector<S3ReadRequest> *kvRequest);

    int HandleReadS3NotExist(uint32_t retry,
      const std::shared_ptr<InodeWrapper> &inodeWrapper);

    bool ReadKVRequestFromS3(const std::string &name,
      char *databuf, uint64_t offset, uint64_t length, int *ret);

    bool ReadKVRequestFromRemoteCache(const std::string &name,
      char *databuf, uint64_t offset, uint64_t length);

    bool ReadKVRequestFromLocalCache(const std::string &name, char *databuf,
      uint64_t offset, uint64_t len);

    // thread function for ReadKVRequest
    void ProcessKVRequest(const S3ReadRequest &req, char *dataBuf,
        uint64_t fileLen,
        std::once_flag &cancelFlag,     // NOLINT
        std::atomic<bool> &isCanceled,  // NOLINT
        std::atomic<int> &retCode);     // NOLINT

    void PrefetchForBlock(const S3ReadRequest &req, uint64_t fileLen,
      uint64_t blockSize, uint64_t chunkSize, uint64_t startBlockIndex);

    void GetChunkLoc(uint64_t offset, uint64_t *index,
      uint64_t *chunkPos, uint64_t *chunkSize);

    void GetBlockLoc(uint64_t offset, uint64_t *chunkIndex, uint64_t *chunkPos,
      uint64_t *blockIndex, uint64_t *blockPos);

    uint32_t GetPrefetchBlocks() {
        return prefetchBlocks_;
    }

    uint32_t GetReadRetryIntervalMs() const {
        return readRetryIntervalMs_;
    }

    using AsyncDownloadTask = std::function<void()>;
    static int ExecAsyncDownloadTask(void* meta, bthread::TaskIterator<AsyncDownloadTask>& iter);  // NOLINT

 public:
  void PushAsyncTask(const AsyncDownloadTask& task) {
        static thread_local unsigned int seed = time(nullptr);

        int idx = rand_r(&seed) % downloadTaskQueues_.size();
        int rc = bthread::execution_queue_execute(
                   downloadTaskQueues_[idx], task);

        if (CURVE_UNLIKELY(rc != 0)) {
            task();
        }
    }

 private:
  class AsyncPrefetchCallback {
   public:
      AsyncPrefetchCallback(uint64_t inode, S3ClientAdaptorImpl *s3Client)
          : inode_(inode), s3Client_(s3Client) {}

      void operator()(const S3Adapter *,
                      const std::shared_ptr<GetObjectAsyncContext> &context) {
          std::unique_ptr<char[]> guard(context->buf);

          if (context->retCode != 0) {
              LOG(WARNING) << "prefetch failed, key: " << context->key;
              return;
          }

          int ret = s3Client_->GetDiskCacheManager()->WriteReadDirect(
              context->key, context->buf, context->actualLen);
          if (ret < 0) {
              LOG_EVERY_SECOND(INFO) <<
                "prefetch failed, write read directly failed, key: "
                << context->key;
          }
          {
            curve::common::LockGuard lg(s3Client_->downloadMtx_);
            s3Client_->downloadingObj_.erase(context->key);
          }
          VLOG(9) << "prefetch end, objectname is: " << context->key
                  << ", len is: " << context->len
                  << ", actual len is: " <<  context->actualLen;
      }

   private:
      const uint64_t inode_;
      S3ClientAdaptorImpl *s3Client_;
  };

 protected:
  curve::common::Mutex downloadMtx_;
  std::set<std::string> downloadingObj_;

 private:
    std::vector<bthread::ExecutionQueueId<AsyncDownloadTask>>
      downloadTaskQueues_;
    // prefetch blocks nums
    uint32_t prefetchBlocks_;
    // prefetch thread nums
    uint32_t prefetchExecQueueNum_;
    // read and max retry times when read s3 failed
    uint32_t readRetryIntervalMs_;
    uint32_t objectPrefix_;
    uint32_t maxReadRetryIntervalMs_;
    // s3 client manager(put or get object form s3)
    std::shared_ptr<S3Client> client_;
    // kv client manager
    std::shared_ptr<KVClientManager> kvClientManager_;
};

}  // namespace client
}  // namespace curvefs

#endif  // CURVEFS_SRC_CLIENT_S3_CLIENT_S3_ADAPTOR_H_
