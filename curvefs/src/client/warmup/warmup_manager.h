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
#include <unordered_map>
#include <utility>
#include <vector>

#include "curvefs/src/client/common/common.h"
#include "curvefs/src/client/dentry_manager.h"
#include "curvefs/src/client/fuse_common.h"
#include "curvefs/src/client/inode_manager.h"
#include "curvefs/src/client/kvclient/kvclient_manager.h"
#include "curvefs/src/client/metric/client_metric.h"
#include "curvefs/src/client/rpcclient/metaserver_client.h"
#include "curvefs/src/client/s3/client_s3_adaptor.h"
#include "curvefs/src/client/s3/client_s3_cache_manager.h"
#include "curvefs/src/common/task_thread_pool.h"
#include "src/common/concurrent/concurrent.h"
#include "src/common/concurrent/rw_lock.h"

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
    bool operator==(const WarmupFile& other) const {
        return key_ == other.key_;
    }

 private:
    fuse_ino_t key_;
    uint64_t fileLen_;
};

class WarmupFilelist : public WarmupFile {
 public:
    explicit WarmupFilelist(fuse_ino_t key = 0, uint64_t fileLen = 0,
                            const std::string& mountPoint = "",
                            const std::string& root = "")
        : WarmupFile(key, fileLen), mountPoint_(mountPoint), root_(root) {}

    std::string GetMountPoint() const { return mountPoint_; }
    std::string GetRoot() const { return root_; }

 private:
    std::string mountPoint_;
    std::string root_;
};

class WarmupInodes {
 public:
    explicit WarmupInodes(fuse_ino_t key = 0,
                          std::set<fuse_ino_t> list = std::set<fuse_ino_t>())
        : key_(key), readAheadFiles_(std::move(list)) {}

    fuse_ino_t GetKey() const { return key_; }
    const std::set<fuse_ino_t>& GetReadAheadFiles() const {
        return readAheadFiles_;
    }

    void AddFileInode(fuse_ino_t file) { readAheadFiles_.emplace(file); }

 private:
    fuse_ino_t key_;
    std::set<fuse_ino_t> readAheadFiles_;
};

class WarmupProgress {
 public:
    explicit WarmupProgress(WarmupStorageType type = curvefs::client::common::
                                WarmupStorageType::kWarmupStorageTypeUnknown,
                            std::string filePath = "")
        : total_(0),
          finished_(0),
          storageType_(type),
          filePathInClient_(filePath) {}

    WarmupProgress(const WarmupProgress& wp)
        : total_(wp.total_),
          finished_(wp.finished_),
          storageType_(wp.storageType_),
          filePathInClient_(wp.filePathInClient_) {}

    void AddTotal(uint64_t add) {
        std::lock_guard<std::mutex> lock(totalMutex_);
        total_ += add;
    }

    WarmupProgress& operator=(const WarmupProgress& wp) {
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

    std::string GetFilePathInClient() { return filePathInClient_; }

    WarmupStorageType GetStorageType() { return storageType_; }

 private:
    uint64_t total_;
    std::mutex totalMutex_;
    uint64_t finished_;
    std::mutex finishedMutex_;
    WarmupStorageType storageType_;
    std::string filePathInClient_;
};

using FuseOpReadFunctionType =
    std::function<CURVEFS_ERROR(fuse_req_t, fuse_ino_t, size_t, off_t,
                                struct fuse_file_info*, char*, size_t*)>;

using FuseOpReadLinkFunctionType =
    std::function<CURVEFS_ERROR(fuse_req_t, fuse_ino_t, std::string*)>;

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
                           FuseOpReadLinkFunctionType readLinkFunc,
                           std::shared_ptr<KVClientManager> kvClientManager)
        : mounted_(false),
          metaClient_(std::move(metaClient)),
          inodeManager_(std::move(inodeManager)),
          dentryManager_(std::move(dentryManager)),
          fsInfo_(std::move(fsInfo)),
          fuseOpRead_(std::move(readFunc)),
          fuseOpReadLink_(std::move(readLinkFunc)),
          kvClientManager_(std::move(kvClientManager)) {}

    virtual void Init(const FuseClientOption& option) { option_ = option; }
    virtual void UnInit() { ClearWarmupProcess(); }

    virtual bool AddWarmupFilelist(fuse_ino_t key, WarmupStorageType type,
                                   const std::string& path,
                                   const std::string& mount_point,
                                   const std::string& root) = 0;
    virtual bool AddWarmupFile(fuse_ino_t key, const std::string& path,
                               WarmupStorageType type) = 0;

    virtual bool CancelWarmupFileOrFilelist(fuse_ino_t key) = 0;
    virtual bool CancelWarmupDependentQueue(fuse_ino_t key) = 0;

    void SetMounted(bool mounted) {
        mounted_.store(mounted, std::memory_order_release);
    }

    void SetFsInfo(const std::shared_ptr<FsInfo>& fsinfo) { fsInfo_ = fsinfo; }

    void SetFuseOpRead(const FuseOpReadFunctionType& read) {
        fuseOpRead_ = read;
    }

    void SetFuseOpReadLink(const FuseOpReadLinkFunctionType& readLink) {
        fuseOpReadLink_ = readLink;
    }

    void SetKVClientManager(std::shared_ptr<KVClientManager> kvClientManager) {
        kvClientManager_ = std::move(kvClientManager);
    }

    using Filepath2WarmupProgressMap =
        std::unordered_map<std::string, WarmupProgress>;
    /**
     * @brief
     *
     * @param key
     * @param progress
     * @return true
     * @return false no this warmup task or finished
     */
    bool QueryWarmupProgress(fuse_ino_t key, WarmupProgress* progress) {
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

    bool ListWarmupProgress(Filepath2WarmupProgressMap* filepath2progress) {
        ReadLockGuard lock(inode2ProgressMutex_);

        for (auto fileProgressInfoIt = inode2Progress_.begin();
             fileProgressInfoIt != inode2Progress_.end();
             ++fileProgressInfoIt) {
            filepath2progress->emplace(
                fileProgressInfoIt->second.GetFilePathInClient(),
                WarmupProgress(fileProgressInfoIt->second));
        }

        return !inode2Progress_.empty();
    }

    void CollectMetrics(InterfaceMetric* interface, int count, uint64_t start);

 protected:
    /**
     * @brief Add warmupProcess
     *
     * @return true
     * @return false warmupProcess has been added
     */
    virtual bool AddWarmupProcessLocked(fuse_ino_t key, const std::string& path,
                                        WarmupStorageType type) {
        auto retPg = inode2Progress_.emplace(key, WarmupProgress(type, path));
        return retPg.second;
    }

    virtual bool CancelWarmupProcess(fuse_ino_t key) {
        WriteLockGuard lock(inode2ProgressMutex_);

        bool keyExists = inode2Progress_.find(key) != inode2Progress_.end();

        if (keyExists) {
            inode2Progress_.erase(key);
        }

        return keyExists;
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

    // FuseOpReadLink
    FuseOpReadLinkFunctionType fuseOpReadLink_;

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
        FuseOpReadLinkFunctionType readLinkFunc,
        std::shared_ptr<KVClientManager> kvClientManager,
        std::shared_ptr<S3ClientAdaptor> s3Adaptor)
        : WarmupManager(std::move(metaClient), std::move(inodeManager),
                        std::move(dentryManager), std::move(fsInfo),
                        std::move(readFunc), std::move(readLinkFunc),
                        std::move(kvClientManager)),
          s3Adaptor_(std::move(s3Adaptor)) {}

    bool AddWarmupFilelist(fuse_ino_t key, WarmupStorageType type,
                           const std::string& path,
                           const std::string& mount_point,
                           const std::string& root) override;
    bool AddWarmupFile(fuse_ino_t key, const std::string& path,
                       WarmupStorageType type) override;

    bool CancelWarmupFileOrFilelist(fuse_ino_t key) override;
    bool CancelWarmupDependentQueue(fuse_ino_t key) override;

    void Init(const FuseClientOption& option) override;
    void UnInit() override;

 protected:
    void BackGroundFetch();

    void GetWarmupList(const WarmupFilelist& filelist,
                       std::vector<std::string>* list);

    void AlignFilelistPathsToCurveFs(const WarmupFilelist& filelist,
                                     std::vector<std::string>* list);

    void FetchDentryEnqueue(fuse_ino_t key, const std::string& file);

    void LookPath(fuse_ino_t key, std::string file);

    /**
     * @brief Given parent inode id and subpath to get the parent of realpath
     * and lastname. If there is a soft link in the middle, it will be converted
     * automatically. for example: inode = 1(/a), subPath{b,c}(b/c) then the ret
     * is the inodeid of /a/b, last name is c for example: inode = 1(/a) subPath
     * = {b,c,d}(b/c/d), d link to ".." then the ret is the inodeid of /a, last
     * name is b
     *
     * @param parent
     * @param subPath
     * @param ret the parent of realpath
     * @param lastPath the last name of realpath
     * @param symlink_depth per sym link, ++symlink_depth;
     * @return false: subPath isn't belong to inode
     */
    bool GetInodeSubPathParent(fuse_ino_t inode,
                               const std::vector<std::string>& subPath,
                               fuse_ino_t* ret, std::string* lastPath,
                               uint32_t* symlink_depth);

    void FetchDentry(fuse_ino_t key, fuse_ino_t ino, const std::string& file,
                     uint32_t symlink_depth);

    void FetchChildDentry(fuse_ino_t key, fuse_ino_t ino,
                          uint32_t symlink_depth);

    /**
     * @brief
     * Please use it with the lock warmupInodesDequeMutex_
     * @param key
     * @return std::deque<WarmupInodes>::iterator
     */
    std::deque<WarmupInodes>::iterator FindWarmupInodesByKeyLocked(
        fuse_ino_t key) {
        return std::find_if(warmupInodesDeque_.begin(),
                            warmupInodesDeque_.end(),
                            [key](const WarmupInodes& inodes) {
                                return key == inodes.GetKey();
                            });
    }

    /**
     * @brief
     * Please use it with the lock warmupFilelistDequeMutex_
     * @param key
     * @return std::deque<WarmupFilelist>::iterator
     */
    std::deque<WarmupFilelist>::iterator FindWarmupFilelistByKeyLocked(
        fuse_ino_t key) {
        return std::find_if(warmupFilelistDeque_.begin(),
                            warmupFilelistDeque_.end(),
                            [key](const WarmupFilelist& filelist_) {
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
                      const S3ChunkInfoMapType& s3ChunkInfoMap);

    using ObjectListType = std::list<std::pair<std::string, uint64_t>>;
    // travel and download all objs belong to the chunk
    void TravelChunk(fuse_ino_t ino, const S3ChunkInfoList& chunkInfo,
                     ObjectListType* prefetchObjs);

    // warmup all the prefetchObjs
    void WarmUpAllObjs(
        fuse_ino_t key,
        const std::list<std::pair<std::string, uint64_t>>& prefetchObjs);

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

    void PutObjectToCache(
        fuse_ino_t key, const std::shared_ptr<GetObjectAsyncContext>& context);

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
