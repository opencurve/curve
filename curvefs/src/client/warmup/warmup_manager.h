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
 * Created Date: 2023-01-31
 * Author: chengyi01
 */

#ifndef CURVEFS_SRC_CLIENT_WARMUP_WARMUP_MANAGER_H_
#define CURVEFS_SRC_CLIENT_WARMUP_WARMUP_MANAGER_H_

#include <algorithm>
#include <atomic>
#include <cstdint>
#include <deque>
#include <functional>
#include <iterator>
#include <list>
#include <map>
#include <memory>
#include <mutex>
#include <set>
#include <string>
#include <utility>
#include <vector>
#include <unordered_map>

#include "curvefs/src/client/common/common.h"
#include "curvefs/src/client/dentry_cache_manager.h"
#include "curvefs/src/client/fuse_common.h"
#include "curvefs/src/client/inode_cache_manager.h"
#include "curvefs/src/client/kvclient/kvclient_manager.h"
#include "curvefs/src/client/rpcclient/metaserver_client.h"
#include "curvefs/src/client/s3/client_s3_adaptor.h"
#include "curvefs/src/client/s3/client_s3_cache_manager.h"
#include "src/common/concurrent/concurrent.h"
#include "src/common/concurrent/rw_lock.h"
#include "curvefs/src/common/task_thread_pool.h"
#include "curvefs/src/client/metric/client_metric.h"

namespace curvefs {
namespace client {
namespace warmup {

using common::FuseClientOption;

using ThreadPool = curvefs::common::TaskThreadPool2<bthread::Mutex,
                                                    bthread::ConditionVariable>;
using curve::common::BthreadRWLock;

using curvefs::client::common::WarmupStorageType;

class WarmupFile {
 public:
    explicit WarmupFile(fuse_ino_t key = 0, uint64_t fileLen = 0)
        : key_(key), fileLen_(fileLen) {}

    fuse_ino_t GetKey() const { return key_; }
    uint64_t GetFileLen() const { return fileLen_; }
    bool operator==(const WarmupFile &other) const {
        return key_ == other.key_;
    }

 private:
    fuse_ino_t key_;
    uint64_t fileLen_;
};

using WarmupFilelist = WarmupFile;

class WarmupInodes {
 public:
    explicit WarmupInodes(fuse_ino_t key = 0,
                          std::set<fuse_ino_t> list = std::set<fuse_ino_t>())
        : key_(key), readAheadFiles_(std::move(list)) {}

    fuse_ino_t GetKey() const { return key_; }
    const std::set<fuse_ino_t> &GetReadAheadFiles() const {
        return readAheadFiles_;
    }

    void AddFileInode(fuse_ino_t file) { readAheadFiles_.emplace(file); }

 private:
    fuse_ino_t key_;
    std::set<fuse_ino_t> readAheadFiles_;
};


using FuseOpReadFunctionType =
    std::function<CURVEFS_ERROR(fuse_req_t, fuse_ino_t, size_t, off_t,
                                struct fuse_file_info *, char *, size_t *)>;

class WarmupProgress {
 public:
    explicit WarmupProgress(WarmupStorageType type = curvefs::client::common::
                                WarmupStorageType::kWarmupStorageTypeUnknown)
        : total_(0), finished_(0), storageType_(type) {}

    WarmupProgress(const WarmupProgress &wp)
        : total_(wp.total_), finished_(wp.finished_),
          storageType_(wp.storageType_) {}

    void AddTotal(uint64_t add) {
        std::lock_guard<std::mutex> lock(totalMutex_);
        total_ += add;
    }

    WarmupProgress &operator=(const WarmupProgress &wp) {
        total_ = wp.total_;
        finished_ = wp.finished_;
        return *this;
    }

    void FinishedPlusOne() {
        std::lock_guard<std::mutex> lock(finishedMutex_);
        ++finished_;
    }

    uint64_t GetTotal() {
        std::lock_guard<std::mutex> lock(totalMutex_);
        return total_;
    }

    uint64_t GetFinished() {
        std::lock_guard<std::mutex> lock(finishedMutex_);
        return finished_;
    }

    std::string ToString() {
        std::lock_guard<std::mutex> lockT(totalMutex_);
        std::lock_guard<std::mutex> lockF(finishedMutex_);
        return "total:" + std::to_string(total_) +
               ",finished:" + std::to_string(finished_);
    }

    WarmupStorageType GetStorageType() {
        return storageType_;
    }

 private:
    uint64_t total_;
    std::mutex totalMutex_;
    uint64_t finished_;
    std::mutex finishedMutex_;
    WarmupStorageType storageType_;
};

class WarmupManager {
 public:
    WarmupManager()
        : mounted_(false),
          metaClient_(std::make_shared<MetaServerClientImpl>()),
          inodeManager_(std::make_shared<InodeCacheManagerImpl>(metaClient_)),
          dentryManager_(
              std::make_shared<DentryCacheManagerImpl>(metaClient_)) {
        kvClientManager_ = nullptr;
    }

    explicit WarmupManager(std::shared_ptr<MetaServerClient> metaClient,
                           std::shared_ptr<InodeCacheManager> inodeManager,
                           std::shared_ptr<DentryCacheManager> dentryManager,
                           std::shared_ptr<FsInfo> fsInfo,
                           FuseOpReadFunctionType readFunc,
                           std::shared_ptr<KVClientManager> kvClientManager)
        : mounted_(false), metaClient_(std::move(metaClient)),
          inodeManager_(std::move(inodeManager)),
          dentryManager_(std::move(dentryManager)), fsInfo_(std::move(fsInfo)),
          fuseOpRead_(std::move(readFunc)),
          kvClientManager_(std::move(kvClientManager)) {}

    virtual void Init(const FuseClientOption &option) {
        option_ = option;
    }
    virtual void UnInit() { ClearWarmupProcess(); }

    virtual bool AddWarmupFilelist(fuse_ino_t key, WarmupStorageType type) = 0;
    virtual bool AddWarmupFile(fuse_ino_t key, const std::string &path,
                               WarmupStorageType type) = 0;

    void SetMounted(bool mounted) {
        mounted_.store(mounted, std::memory_order_release);
    }

    void SetFsInfo(const std::shared_ptr<FsInfo> &fsinfo) { fsInfo_ = fsinfo; }

    void SetFuseOpRead(const FuseOpReadFunctionType &read) {
        fuseOpRead_ = read;
    }

    void SetKVClientManager(std::shared_ptr<KVClientManager> kvClientManager) {
        kvClientManager_ = std::move(kvClientManager);
    }

    /**
     * @brief
     *
     * @param key
     * @param progress
     * @return true
     * @return false no this warmup task or finished
     */
    bool QueryWarmupProgress(fuse_ino_t key, WarmupProgress *progress) {
        bool ret = true;
        ReadLockGuard lock(inode2ProgressMutex_);
        auto iter = FindWarmupProgressByKeyLocked(key);
        if (iter != inode2Progress_.end()) {
            *progress = iter->second;
        } else {
            ret = false;
        }
        return ret;
    }

    virtual void CollectMetrics(InterfaceMetric *interface, int count,
                                uint64_t start);

 protected:
    /**
     * @brief Add warmupProcess
     *
     * @return true
     * @return false warmupProcess has been added
     */
    virtual bool AddWarmupProcess(fuse_ino_t key, WarmupStorageType type) {
        WriteLockGuard lock(inode2ProgressMutex_);
        auto ret = inode2Progress_.emplace(key, WarmupProgress(type));
        return ret.second;
    }

    /**
     * @brief
     * Please use it with the lock inode2ProgressMutex_
     * @param key
     * @return std::unordered_map<fuse_ino_t, WarmupProgress>::iterator
     */
    std::unordered_map<fuse_ino_t, WarmupProgress>::iterator
    FindWarmupProgressByKeyLocked(fuse_ino_t key) {
        return inode2Progress_.find(key);
    }

    virtual void ClearWarmupProcess() {
        WriteLockGuard lock(inode2ProgressMutex_);
        inode2Progress_.clear();
    }

 protected:
    std::atomic<bool> mounted_;

    // metaserver client
    std::shared_ptr<MetaServerClient> metaClient_;

    // inode cache manager
    std::shared_ptr<InodeCacheManager> inodeManager_;

    // dentry cache manager
    std::shared_ptr<DentryCacheManager> dentryManager_;

    // filesystem info
    std::shared_ptr<FsInfo> fsInfo_;

    // FuseOpRead
    FuseOpReadFunctionType fuseOpRead_;

    // warmup progress
    std::unordered_map<fuse_ino_t, WarmupProgress> inode2Progress_;
    BthreadRWLock inode2ProgressMutex_;

    std::shared_ptr<KVClientManager> kvClientManager_ = nullptr;

    FuseClientOption option_;
};

class WarmupManagerS3Impl : public WarmupManager {
 public:
    explicit WarmupManagerS3Impl(
        std::shared_ptr<MetaServerClient> metaClient,
        std::shared_ptr<InodeCacheManager> inodeManager,
        std::shared_ptr<DentryCacheManager> dentryManager,
        std::shared_ptr<FsInfo> fsInfo, FuseOpReadFunctionType readFunc,
        std::shared_ptr<S3ClientAdaptor> s3Adaptor,
        std::shared_ptr<KVClientManager> kvClientManager)
        : WarmupManager(std::move(metaClient), std::move(inodeManager),
                        std::move(dentryManager), std::move(fsInfo),
                        std::move(readFunc), std::move(kvClientManager)),
          s3Adaptor_(std::move(s3Adaptor)) {}

    bool AddWarmupFilelist(fuse_ino_t key, WarmupStorageType type) override;
    bool AddWarmupFile(fuse_ino_t key, const std::string &path,
                       WarmupStorageType type) override;

    void Init(const FuseClientOption &option) override;
    void UnInit() override;

 private:
    void BackGroundFetch();

    void GetWarmupList(const WarmupFilelist &filelist,
                       std::vector<std::string> *list);

    void FetchDentryEnqueue(fuse_ino_t key, const std::string &file);

    void LookPath(fuse_ino_t key, std::string file);

    void FetchDentry(fuse_ino_t key, fuse_ino_t ino, const std::string &file);

    void FetchChildDentry(fuse_ino_t key, fuse_ino_t ino);

    /**
     * @brief
     * Please use it with the lock warmupInodesDequeMutex_
     * @param key
     * @return std::deque<WarmupInodes>::iterator
     */
    std::deque<WarmupInodes>::iterator
    FindWarmupInodesByKeyLocked(fuse_ino_t key) {
        return std::find_if(warmupInodesDeque_.begin(),
                            warmupInodesDeque_.end(),
                            [key](const WarmupInodes &inodes) {
                                return key == inodes.GetKey();
                            });
    }

    /**
     * @brief
     * Please use it with the lock warmupFilelistDequeMutex_
     * @param key
     * @return std::deque<WarmupFilelist>::iterator
     */
    std::deque<WarmupFilelist>::iterator
    FindWarmupFilelistByKeyLocked(fuse_ino_t key) {
        return std::find_if(warmupFilelistDeque_.begin(),
                            warmupFilelistDeque_.end(),
                            [key](const WarmupFilelist &filelist_) {
                                return key == filelist_.GetKey();
                            });
    }

    /**
     * @brief
     * Please use it with the lock inode2FetchDentryPoolMutex_
     * @param key
     * @return std::unordered_map<fuse_ino_t,
     * std::unique_ptr<ThreadPool>>::iterator
     */
    std::unordered_map<fuse_ino_t, std::unique_ptr<ThreadPool>>::iterator
    FindFetchDentryPoolByKeyLocked(fuse_ino_t key) {
        return inode2FetchDentryPool_.find(key);
    }

    /**
     * @brief
     * Please use it with the lock inode2FetchS3ObjectsPoolMutex_
     * @param key
     * @return std::unordered_map<fuse_ino_t,
     * std::unique_ptr<ThreadPool>>::iterator
     */
    std::unordered_map<fuse_ino_t, std::unique_ptr<ThreadPool>>::iterator
    FindFetchS3ObjectsPoolByKeyLocked(fuse_ino_t key) {
        return inode2FetchS3ObjectsPool_.find(key);
    }

    void FetchDataEnqueue(fuse_ino_t key, fuse_ino_t ino);

    using S3ChunkInfoMapType = google::protobuf::Map<uint64_t, S3ChunkInfoList>;

    // travel all chunks
    void TravelChunks(fuse_ino_t key, fuse_ino_t ino,
                      const S3ChunkInfoMapType &s3ChunkInfoMap);

    using ObjectListType = std::list<std::pair<std::string, uint64_t>>;
    // travel and download all objs belong to the chunk
    void TravelChunk(fuse_ino_t ino, const S3ChunkInfoList &chunkInfo,
                     ObjectListType *prefetchObjs);

    // warmup all the prefetchObjs
    void WarmUpAllObjs(
        fuse_ino_t key,
        const std::list<std::pair<std::string, uint64_t>> &prefetchObjs);

    /**
     * @brief Whether the warmup task[key] is completed (or terminated)
     *
     * @return true
     * @return false
     */
    bool ProgressDone(fuse_ino_t key);

    void ScanCleanFetchDentryPool();

    void ScanCleanFetchS3ObjectsPool();

    void ScanCleanWarmupProgress();

    void ScanWarmupInodes();

    void ScanWarmupFilelist();

    void AddFetchDentryTask(fuse_ino_t key, std::function<void()> task);

    void AddFetchS3objectsTask(fuse_ino_t key, std::function<void()> task);

    void
    PutObjectToCache(fuse_ino_t key,
                     const std::shared_ptr<GetObjectAsyncContext> &context);

 protected:
    std::deque<WarmupFilelist> warmupFilelistDeque_;
    mutable RWLock warmupFilelistDequeMutex_;

    bool initbgFetchThread_ = false;
    Thread bgFetchThread_;
    std::atomic<bool> bgFetchStop_;

    // TODO(chengyi01): limit thread nums
    std::unordered_map<fuse_ino_t, std::unique_ptr<ThreadPool>>
        inode2FetchDentryPool_;
    mutable RWLock inode2FetchDentryPoolMutex_;

    std::deque<WarmupInodes> warmupInodesDeque_;
    mutable RWLock warmupInodesDequeMutex_;

    // s3 adaptor
    std::shared_ptr<S3ClientAdaptor> s3Adaptor_;

    // TODO(chengyi01): limit thread nums
    std::unordered_map<fuse_ino_t, std::unique_ptr<ThreadPool>>
        inode2FetchS3ObjectsPool_;
    mutable RWLock inode2FetchS3ObjectsPoolMutex_;

    curvefs::client::metric::WarmupManagerS3Metric warmupS3Metric_;
};

}  // namespace warmup
}  // namespace client
}  // namespace curvefs

#endif  // CURVEFS_SRC_CLIENT_WARMUP_WARMUP_MANAGER_H_
