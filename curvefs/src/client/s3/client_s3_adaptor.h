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

#include <memory>
#include <string>
#include <vector>

#include "curvefs/proto/common.pb.h"
#include "curvefs/proto/mds.pb.h"
#include "curvefs/proto/metaserver.pb.h"
#include "curvefs/proto/space.pb.h"
#include "curvefs/src/client/common/config.h"
#include "curvefs/src/client/error_code.h"
#include "curvefs/src/client/inode_cache_manager.h"
#include "curvefs/src/client/rpcclient/mds_client.h"
#include "curvefs/src/client/s3/client_s3.h"
#include "curvefs/src/client/s3/client_s3_cache_manager.h"
#include "curvefs/src/client/s3/disk_cache_manager_impl.h"
#include "src/common/wait_interval.h"
namespace curvefs {
namespace client {

using ::curve::common::Thread;
using curvefs::client::common::S3ClientAdaptorOption;
using curvefs::metaserver::Inode;
using curvefs::metaserver::S3ChunkInfo;
using curvefs::metaserver::S3ChunkInfoList;
using curvefs::space::AllocateS3ChunkRequest;
using curvefs::space::AllocateS3ChunkResponse;
using rpcclient::MdsClient;

class DiskCacheManagerImpl;

class S3ClientAdaptor {
 public:
    S3ClientAdaptor() {}
    virtual ~S3ClientAdaptor() {}
    /**
     * @brief Initailize s3 client
     * @param[in] options the options for s3 client
     */
    virtual CURVEFS_ERROR Init(const S3ClientAdaptorOption &option,
                               S3Client *client,
                               std::shared_ptr<InodeCacheManager> inodeManager,
                               std::shared_ptr<MdsClient> mdsClient) = 0;
    /**
     * @brief write data to s3
     * @param[in] options the options for s3 client
     */
    virtual int Write(uint64_t inodeId, uint64_t offset, uint64_t length,
                      const char *buf) = 0;
    virtual int Read(Inode *inode, uint64_t offset, uint64_t length,
                     char *buf) = 0;
    virtual CURVEFS_ERROR Truncate(Inode *inode, uint64_t size) = 0;
    virtual void ReleaseCache(uint64_t inodeId) = 0;
    virtual CURVEFS_ERROR Flush(uint64_t inodeId) = 0;
    virtual CURVEFS_ERROR FsSync() = 0;
    virtual int Stop() = 0;
    virtual FSStatusCode AllocS3ChunkId(uint32_t fsId, uint64_t *chunkId) = 0;
    virtual void SetFsId(uint32_t fsId) = 0;
};

// client use s3 internal interface
class S3ClientAdaptorImpl : public S3ClientAdaptor {
 public:
    S3ClientAdaptorImpl() {}
    virtual ~S3ClientAdaptorImpl() {}
    /**
     * @brief Initailize s3 client
     * @param[in] options the options for s3 client
     */
    CURVEFS_ERROR Init(const S3ClientAdaptorOption &option, S3Client *client,
                       std::shared_ptr<InodeCacheManager> inodeManager,
                       std::shared_ptr<MdsClient> mdsClient);
    /**
     * @brief write data to s3
     * @param[in] options the options for s3 client
     */
    int Write(uint64_t inodeId, uint64_t offset, uint64_t length,
              const char *buf);
    int Read(Inode *inode, uint64_t offset, uint64_t length, char *buf);
    CURVEFS_ERROR Truncate(Inode *inode, uint64_t size);
    void ReleaseCache(uint64_t inodeId);
    CURVEFS_ERROR Flush(uint64_t inodeId);
    CURVEFS_ERROR FsSync();
    int Stop();
    uint64_t GetBlockSize() { return blockSize_; }
    uint64_t GetChunkSize() { return chunkSize_; }
    std::shared_ptr<FsCacheManager> GetFsCacheManager() {
        return fsCacheManager_;
    }
    uint32_t GetFlushInterval() { return flushIntervalSec_; }
    bool EnableDiskCache() { return enableDiskCache_; }
    S3Client *GetS3Client() { return client_; }
    std::shared_ptr<InodeCacheManager> GetInodeCacheManager() {
        return inodeManager_;
    }
    std::shared_ptr<DiskCacheManagerImpl> GetDiskCacheManager() {
        return diskCacheManagerImpl_;
    }
    FSStatusCode AllocS3ChunkId(uint32_t fsId, uint64_t *chunkId);
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
    void SetFsId(uint32_t fsId) { fsId_ = fsId; }
    uint32_t GetFsId() { return fsId_; }

 private:
    void BackGroundFlush();

 private:
    S3Client *client_;
    uint64_t blockSize_;
    uint64_t chunkSize_;
    std::string allocateServerEps_;
    uint32_t flushIntervalSec_;
    uint32_t memCacheNearfullRatio_;
    uint32_t throttleBaseSleepUs_;
    Thread bgFlushThread_;
    std::atomic<bool> toStop_;
    std::mutex mtx_;
    std::condition_variable cond_;
    curve::common::WaitInterval waitIntervalSec_;
    std::shared_ptr<FsCacheManager> fsCacheManager_;
    std::shared_ptr<InodeCacheManager> inodeManager_;
    std::shared_ptr<DiskCacheManagerImpl> diskCacheManagerImpl_;
    bool enableDiskCache_;
    std::shared_ptr<MdsClient> mdsClient_;
    uint32_t fsId_;
};

}  // namespace client
}  // namespace curvefs

#endif  // CURVEFS_SRC_CLIENT_S3_CLIENT_S3_ADAPTOR_H_
