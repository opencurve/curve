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

#include <string>
#include <vector>
#include <memory>
#include "curvefs/proto/common.pb.h"
#include "curvefs/proto/mds.pb.h"
#include "curvefs/proto/metaserver.pb.h"
#include "curvefs/proto/space.pb.h"
#include "curvefs/src/client/error_code.h"
#include "curvefs/src/client/inode_cache_manager.h"
#include "curvefs/src/client/s3/client_s3.h"
#include "curvefs/src/client/s3/client_s3_cache_manager.h"
#include "curvefs/src/client/s3/disk_cache_manager_impl.h"
#include "src/common/wait_interval.h"

namespace curvefs {
namespace client {

using curvefs::metaserver::Inode;
using curvefs::metaserver::S3ChunkInfo;
using curvefs::metaserver::S3ChunkInfoList;
using curvefs::space::AllocateS3ChunkRequest;
using curvefs::space::AllocateS3ChunkResponse;
using ::curve::common::Thread;

struct S3ClientAdaptorOption {
    uint64_t blockSize;
    uint64_t chunkSize;
    std::string metaServerEps;
    std::string allocateServerEps;
    uint32_t intervalSec;
    uint32_t flushInterval;
    DiskCacheOption diskCacheOpt;
};

class S3ClientAdaptor {
 public:
    S3ClientAdaptor() {}
    virtual ~S3ClientAdaptor() {}
    /**
     * @brief Initailize s3 client
     * @param[in] options the options for s3 client
     */
    virtual void Init(const S3ClientAdaptorOption& option, S3Client* client,
                      std::shared_ptr<InodeCacheManager> inodeManager) = 0;
    /**
     * @brief write data to s3
     * @param[in] options the options for s3 client
     */
    virtual int Write(Inode* inode, uint64_t offset, uint64_t length,
                      const char* buf) = 0;
    virtual int Read(Inode* inode, uint64_t offset, uint64_t length,
                     char* buf) = 0;
    virtual CURVEFS_ERROR Truncate(Inode* inode, uint64_t size) = 0;
    virtual void ReleaseCache(uint64_t inodeId) = 0;
    virtual CURVEFS_ERROR Flush(Inode* inode) = 0;
    virtual CURVEFS_ERROR FsSync() = 0;
    virtual int Stop() = 0;
};

// client使用s3存储的内部接口
class S3ClientAdaptorImpl : public S3ClientAdaptor {
 public:
    S3ClientAdaptorImpl() {}
    virtual ~S3ClientAdaptorImpl() {}
    /**
     * @brief Initailize s3 client
     * @param[in] options the options for s3 client
     */
    void Init(const S3ClientAdaptorOption& option, S3Client* client,
              std::shared_ptr<InodeCacheManager> inodeManager);
    /**
     * @brief write data to s3
     * @param[in] options the options for s3 client
     */
    int Write(Inode* inode, uint64_t offset, uint64_t length, const char* buf);
    int Read(Inode* inode, uint64_t offset, uint64_t length, char* buf);
    CURVEFS_ERROR Truncate(Inode* inode, uint64_t size);
    void ReleaseCache(uint64_t inodeId);
    CURVEFS_ERROR Flush(Inode* inode);
    CURVEFS_ERROR FsSync();
    int Stop();
    uint64_t GetBlockSize() {
        return blockSize_;
    }
    uint64_t GetChunkSize() {
        return chunkSize_;
    }
    std::shared_ptr<FsCacheManager> GetFsCacheManager() {
        return fsCacheManager_;
    }
    uint32_t GetFlushInterval() {
        return flushIntervalSec_;
    }
    bool EnableDiskCache() {
        return enableDiskCache_;
    }
    S3Client* GetS3Client() {
        return client_;
    }
    std::shared_ptr<InodeCacheManager> GetInodeCacheManager() {
        return inodeManager_;
    }
    std::shared_ptr<DiskCacheManagerImpl> GetDiskCacheManager() {
        return diskCacheManagerImpl_;
    }
    CURVEFS_ERROR AllocS3ChunkId(uint32_t fsId, uint64_t* chunkId);

 private:
    void BackGroundFlush();

 private:
    S3Client* client_;
    uint64_t blockSize_;
    uint64_t chunkSize_;
    std::string metaServerEps_;
    std::string allocateServerEps_;
    uint32_t flushIntervalSec_;
    Thread bgFlushThread_;
    std::atomic<bool> toStop_;
    std::mutex mtx_;
    std::condition_variable cond_;
    curve::common::WaitInterval waitIntervalSec_;
    std::shared_ptr<FsCacheManager> fsCacheManager_;
    std::shared_ptr<InodeCacheManager> inodeManager_;
    std::shared_ptr<DiskCacheManagerImpl> diskCacheManagerImpl_;
    bool enableDiskCache_;
};

}  // namespace client
}  // namespace curvefs

#endif  // CURVEFS_SRC_CLIENT_S3_CLIENT_S3_ADAPTOR_H_
