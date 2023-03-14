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

#include "curvefs/src/client/warmup/warmup_manager.h"

#include <glog/logging.h>
#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <deque>
#include <list>
#include <memory>
#include <utility>

#include "curvefs/src/client/common/common.h"
#include "curvefs/src/client/inode_wrapper.h"
#include "curvefs/src/client/kvclient/kvclient_manager.h"
#include "curvefs/src/client/cache/fuse_client_cache_manager.h"
#include "curvefs/src/common/s3util.h"
#include "src/common/concurrent/concurrent.h"
#include "src/common/string_util.h"


namespace curvefs {
namespace client {
namespace warmup {

using curve::common::WriteLockGuard;

#define WARMUP_CHECKINTERVAL_US (1000 * 1000)

bool WarmupManagerS3Impl::AddWarmupFilelist(fuse_ino_t key,
                                            WarmupStorageType type) {
    if (!mounted_.load(std::memory_order_acquire)) {
        LOG(ERROR) << "not mounted";
        return false;
    }
    // add warmup Progress
    if (AddWarmupProcess(key, type)) {
        VLOG(9) << "add warmup list task:" << key;
        WriteLockGuard lock(warmupFilelistDequeMutex_);
        auto iter = FindWarmupFilelistByKeyLocked(key);
        if (iter == warmupFilelistDeque_.end()) {
            std::shared_ptr<InodeWrapper> inodeWrapper;
            CURVEFS_ERROR ret = inodeManager_->GetInode(key, inodeWrapper);
            if (ret != CURVEFS_ERROR::OK) {
                LOG(ERROR) << "inodeManager get inode fail, ret = " << ret
                           << ", inodeid = " << key;
                return false;
            }
            uint64_t len = inodeWrapper->GetLength();
            warmupFilelistDeque_.emplace_back(key, len);
        }
    }  // Skip already added
    return true;
}

bool WarmupManagerS3Impl::AddWarmupFile(fuse_ino_t key, const std::string &path,
                                        WarmupStorageType type) {
    if (!mounted_.load(std::memory_order_acquire)) {
        LOG(ERROR) << "not mounted";
        return false;
    }
    // add warmup Progress
    if (AddWarmupProcess(key, type)) {
        VLOG(9) << "add warmup single task:" << key;
        FetchDentryEnqueue(key, path);
    }
    return true;
}

void WarmupManagerS3Impl::UnInit() {
    bgFetchStop_.store(true, std::memory_order_release);
    if (initbgFetchThread_) {
        bgFetchThread_.join();
    }

    for (auto &task : inode2FetchDentryPool_) {
        task.second->Stop();
    }
    WriteLockGuard lockDentry(inode2FetchDentryPoolMutex_);
    inode2FetchDentryPool_.clear();

    for (auto &task : inode2FetchS3ObjectsPool_) {
        task.second->Stop();
    }
    WriteLockGuard lockS3Objects(inode2FetchS3ObjectsPoolMutex_);
    inode2FetchS3ObjectsPool_.clear();

    WriteLockGuard lockInodes(warmupInodesDequeMutex_);
    warmupInodesDeque_.clear();

    WriteLockGuard lockFileList(warmupFilelistDequeMutex_);
    warmupFilelistDeque_.clear();

    WarmupManager::UnInit();
}

void WarmupManagerS3Impl::Init(const FuseClientOption &option) {
    WarmupManager::Init(option);
    bgFetchStop_.store(false, std::memory_order_release);
    bgFetchThread_ = Thread(&WarmupManagerS3Impl::BackGroundFetch, this);
    initbgFetchThread_ = true;
}

void WarmupManagerS3Impl::BackGroundFetch() {
    while (!bgFetchStop_.load(std::memory_order_acquire)) {
        usleep(WARMUP_CHECKINTERVAL_US);
        ScanWarmupFilelist();
        ScanWarmupInodes();
        ScanCleanFetchS3ObjectsPool();
        ScanCleanFetchDentryPool();
        ScanCleanWarmupProgress();
    }
}

void WarmupManagerS3Impl::GetWarmupList(const WarmupFilelist &filelist,
                                        std::vector<std::string> *list) {
    struct fuse_file_info fi {};
    fi.flags &= ~O_DIRECT;
    size_t rSize = 0;
    std::unique_ptr<char[]> data(new char[filelist.GetFileLen() + 1]);
    std::memset(data.get(), 0, filelist.GetFileLen());
    data[filelist.GetFileLen()] = '\n';
    fuseOpRead_(nullptr, filelist.GetKey(), filelist.GetFileLen(), 0, &fi,
                data.get(), &rSize);
    std::string file = data.get();
    VLOG(9) << "file is: " << file;
    // remove enter, newline, blank
    std::string blanks("\r\n ");
    file.erase(0, file.find_first_not_of(blanks));
    file.erase(file.find_last_not_of(blanks) + 1);
    VLOG(9) << "after del file is: " << file;
    curve::common::AddSplitStringToResult(file, "\n", list);
}

void WarmupManagerS3Impl::FetchDentryEnqueue(fuse_ino_t key,
                                             const std::string &file) {
    VLOG(9) << "FetchDentryEnqueue start: " << key << " file: " << file;
    auto task = [this, key, file]() { LookPath(key, file); };
    AddFetchDentryTask(key, task);
    VLOG(9) << "FetchDentryEnqueue end: " << key << " file: " << file;
}

void WarmupManagerS3Impl::LookPath(fuse_ino_t key, std::string file) {
    VLOG(9) << "LookPath start key: " << key << " file: " << file;
    std::vector<std::string> splitPath;
    // remove enter, newline, blank
    std::string blanks("\r\n ");
    file.erase(0, file.find_first_not_of(blanks));
    file.erase(file.find_last_not_of(blanks) + 1);
    if (file.empty()) {
        VLOG(9) << "empty path";
        return;
    }
    bool isRoot = false;
    if (file == "/") {
        splitPath.push_back(file);
        isRoot = true;
    } else {
        curve::common::AddSplitStringToResult(file, "/", &splitPath);
    }
    VLOG(6) << "splitPath size is: " << splitPath.size();
    if (splitPath.size() == 1 && isRoot) {
        VLOG(9) << "i am root";
        auto task = [this, key]() {
            FetchChildDentry(key, fsInfo_->rootinodeid());
        };
        AddFetchDentryTask(key, task);
        return;
    } else if (splitPath.size() == 1) {
        VLOG(9) << "parent is root: " << fsInfo_->rootinodeid()
                << ", path is: " << splitPath[0];
        auto task = [this, key, splitPath]() {
            FetchDentry(key, fsInfo_->rootinodeid(), splitPath[0]);
        };
        AddFetchDentryTask(key, task);
        return;
    } else if (splitPath.size() > 1) {  // travel path
        VLOG(9) << "traverse path start: " << splitPath.size();
        std::string lastName = splitPath.back();
        splitPath.pop_back();
        fuse_ino_t ino = fsInfo_->rootinodeid();
        for (auto iter : splitPath) {
            VLOG(9) << "traverse path: " << iter << "ino is: " << ino;
            Dentry dentry;
            std::string pathName = iter;
            CURVEFS_ERROR ret =
                dentryManager_->GetDentry(ino, pathName, &dentry);
            if (ret != CURVEFS_ERROR::OK) {
                if (ret != CURVEFS_ERROR::NOTEXIST) {
                    LOG(WARNING)
                        << "dentryManager_ get dentry fail, ret = " << ret
                        << ", parent inodeid = " << ino << ", name = " << file;
                }
                VLOG(9) << "FetchDentry error: " << ret;
                return;
            }
            ino = dentry.inodeid();
        }
        auto task = [this, key, ino, lastName]() {
            FetchDentry(key, ino, lastName);
        };
        AddFetchDentryTask(key, task);
        VLOG(9) << "ino is: " << ino << " lastname is: " << lastName;
        return;
    } else {
        VLOG(3) << "unknown path";
    }
    VLOG(9) << "LookPath start end: " << key << " file: " << file;
}

void WarmupManagerS3Impl::FetchDentry(fuse_ino_t key, fuse_ino_t ino,
                                      const std::string &file) {
    VLOG(9) << "FetchDentry start: " << file << ", ino: " << ino
            << " key: " << key;
    Dentry dentry;
    CURVEFS_ERROR ret = dentryManager_->GetDentry(ino, file, &dentry);
    if (ret != CURVEFS_ERROR::OK) {
        if (ret != CURVEFS_ERROR::NOTEXIST) {
            LOG(WARNING) << "dentryManager_ get dentry fail, ret = " << ret
                         << ", parent inodeid = " << ino << ", name = " << file;
        } else {
            LOG(ERROR) << "FetchDentry key: " << key << " file: " << file
                       << " errorCode: " << ret;
        }
        return;
    }
    if (FsFileType::TYPE_S3 == dentry.type()) {
        WriteLockGuard lock(warmupInodesDequeMutex_);
        auto iterDeque = FindWarmupInodesByKeyLocked(key);
        if (iterDeque == warmupInodesDeque_.end()) {
            warmupInodesDeque_.emplace_back(
                key, std::set<fuse_ino_t>{dentry.inodeid()});
        } else {
            iterDeque->AddFileInode(dentry.inodeid());
        }
        return;
    } else if (FsFileType::TYPE_DIRECTORY == dentry.type()) {
        auto task = [this, key, dentry]() {
            FetchChildDentry(key, dentry.inodeid());
        };
        AddFetchDentryTask(key, task);
        VLOG(9) << "FetchDentry: " << dentry.inodeid();
        return;

    } else if (FsFileType::TYPE_SYM_LINK == dentry.type()) {
        // skip links
    } else {
        VLOG(3) << "unkown, file: " << file << ", ino: " << ino;
        return;
    }
    VLOG(9) << "FetchDentry end: " << file << ", ino: " << ino;
}

void WarmupManagerS3Impl::FetchChildDentry(fuse_ino_t key, fuse_ino_t ino) {
    VLOG(9) << "FetchChildDentry start: key:" << key << " inode: " << ino;
    std::list<Dentry> dentryList;
    auto limit = option_.listDentryLimit;
    CURVEFS_ERROR ret = dentryManager_->ListDentry(ino, &dentryList, limit);
    if (ret != CURVEFS_ERROR::OK) {
        LOG(ERROR) << "dentryManager_ ListDentry fail, ret = " << ret
                   << ", parent = " << ino;
        return;
    }
    for (const auto &dentry : dentryList) {
        VLOG(9) << "FetchChildDentry: key:" << key
                << " dentry: " << dentry.name();
        if (FsFileType::TYPE_S3 == dentry.type()) {
            WriteLockGuard lock(warmupInodesDequeMutex_);
            auto iterDeque = FindWarmupInodesByKeyLocked(key);
            if (iterDeque == warmupInodesDeque_.end()) {
                warmupInodesDeque_.emplace_back(
                    key, std::set<fuse_ino_t>{dentry.inodeid()});
            } else {
                iterDeque->AddFileInode(dentry.inodeid());
            }
            VLOG(9) << "FetchChildDentry: " << dentry.inodeid();
        } else if (FsFileType::TYPE_DIRECTORY == dentry.type()) {
            auto task = [this, key, dentry]() {
                FetchChildDentry(key, dentry.inodeid());
            };
            AddFetchDentryTask(key, task);
            VLOG(9) << "FetchChildDentry: " << dentry.inodeid();
        } else if (FsFileType::TYPE_SYM_LINK == dentry.type()) {  // need todo
        } else {
            VLOG(9) << "unknown type";
        }
    }
    VLOG(9) << "FetchChildDentry end: key:" << key << " inode: " << ino;
}

void WarmupManagerS3Impl::FetchDataEnqueue(fuse_ino_t key, fuse_ino_t ino) {
    VLOG(9) << "FetchDataEnqueue start: key:" << key << " inode: " << ino;
    auto task = [key, ino, this]() {
        std::shared_ptr<InodeWrapper> inodeWrapper;
        CURVEFS_ERROR ret = inodeManager_->GetInode(ino, inodeWrapper);
        if (ret != CURVEFS_ERROR::OK) {
            LOG(ERROR) << "inodeManager get inode fail, ret = " << ret
                       << ", inodeid = " << ino;
            return;
        }
        S3ChunkInfoMapType s3ChunkInfoMap;
        {
            ::curve::common::UniqueLock lgGuard = inodeWrapper->GetUniqueLock();
            s3ChunkInfoMap = *inodeWrapper->GetChunkInfoMap();
        }
        if (s3ChunkInfoMap.empty()) {
            return;
        }
        TravelChunks(key, ino, s3ChunkInfoMap);
    };
    AddFetchS3objectsTask(key, task);
    VLOG(9) << "FetchDataEnqueue end: key:" << key << " inode: " << ino;
}

void WarmupManagerS3Impl::TravelChunks(
    fuse_ino_t key, fuse_ino_t ino, const S3ChunkInfoMapType &s3ChunkInfoMap) {
    VLOG(9) << "travel chunk start: " << ino
            << ", size: " << s3ChunkInfoMap.size();
    for (auto const &infoIter : s3ChunkInfoMap) {
        VLOG(9) << "travel chunk: " << infoIter.first;
        std::list<std::pair<std::string, uint64_t>> prefetchObjs;
        TravelChunk(ino, infoIter.second, &prefetchObjs);
        {
            ReadLockGuard lock(inode2ProgressMutex_);
            auto iter = FindWarmupProgressByKeyLocked(key);
            if (iter != inode2Progress_.end()) {
                iter->second.AddTotal(prefetchObjs.size());
            } else {
                LOG(ERROR) << "no such warmup progress: " << key;
            }
        }
        auto task = [this, key, prefetchObjs]() {
            WarmUpAllObjs(key, prefetchObjs);
        };
        AddFetchS3objectsTask(key, task);
    }
    VLOG(9) << "travel chunks end";
}

void WarmupManagerS3Impl::TravelChunk(fuse_ino_t ino,
                                      const S3ChunkInfoList &chunkInfo,
                                      ObjectListType *prefetchObjs) {
    uint64_t blockSize = s3Adaptor_->GetBlockSize();
    uint64_t chunkSize = s3Adaptor_->GetChunkSize();
    uint64_t offset, len, chunkid, compaction;
    for (size_t i = 0; i < chunkInfo.s3chunks_size(); i++) {
        auto const &chunkinfo = chunkInfo.s3chunks(i);
        auto fsId = fsInfo_->fsid();
        chunkid = chunkinfo.chunkid();
        compaction = chunkinfo.compaction();
        offset = chunkinfo.offset();
        len = chunkinfo.len();
        // the offset in the chunk
        uint64_t chunkPos = offset % chunkSize;
        // the offset in the block
        uint64_t blockPos = chunkPos % blockSize;
        // the first blockIndex
        uint64_t blockIndexBegin = chunkPos / blockSize;

        if (len < blockSize) {  // just one block
            auto objectName = curvefs::common::s3util::GenObjName(
                chunkid, blockIndexBegin, compaction, fsId, ino);
            prefetchObjs->push_back(std::make_pair(objectName, len));
        } else {
            // the offset in the block
            uint64_t blockPos = chunkPos % blockSize;

            // firstly, let's get the size in the first block
            // then, subtract the length in the first block
            // to obtain the remaining length
            // lastly, We need to judge the last block is full or not
            uint64_t firstBlockSize =
                (blockPos != 0) ? blockSize - blockPos : blockSize;
            uint64_t leftSize = len - firstBlockSize;
            uint32_t blockCounts = (leftSize % blockSize == 0)
                                       ? (leftSize / blockSize + 1)
                                       : (leftSize / blockSize + 1 + 1);
            // so we can get the last blockIndex
            // because the bolck Index is cumulative
            uint64_t blockIndexEnd = blockIndexBegin + blockCounts - 1;

            // the size of the last block
            uint64_t lastBlockSize = leftSize % blockSize;
            // whether the first block or the last block is full or not
            bool firstBlockFull = (blockPos == 0);
            bool lastBlockFull = (lastBlockSize == 0);
            // the start and end block Index that need travel
            uint64_t travelStartIndex, travelEndIndex;
            // if the block is full, the size is needed download
            // of the obj is blockSize. Otherwise, the value is special.
            if (!firstBlockFull) {
                travelStartIndex = blockIndexBegin + 1;
                auto objectName = curvefs::common::s3util::GenObjName(
                    chunkid, blockIndexBegin, compaction, fsId, ino);
                prefetchObjs->push_back(
                    std::make_pair(objectName, firstBlockSize));
            } else {
                travelStartIndex = blockIndexBegin;
            }
            if (!lastBlockFull) {
                // block index is greater than or equal to 0
                travelEndIndex = (blockIndexEnd == blockIndexBegin)
                                     ? blockIndexEnd
                                     : blockIndexEnd - 1;
                auto objectName = curvefs::common::s3util::GenObjName(
                    chunkid, blockIndexEnd, compaction, fsId, ino);
                // there is no need to care about the order
                // in which objects are downloaded
                prefetchObjs->push_back(
                    std::make_pair(objectName, lastBlockSize));
            } else {
                travelEndIndex = blockIndexEnd;
            }
            VLOG(9) << "travel obj, ino: " << ino << ", chunkid: " << chunkid
                    << ", blockCounts: " << blockCounts
                    << ", compaction: " << compaction
                    << ", blockSize: " << blockSize
                    << ", chunkSize: " << chunkSize << ", offset: " << offset
                    << ", blockIndexBegin: " << blockIndexBegin
                    << ", blockIndexEnd: " << blockIndexEnd << ", len: " << len
                    << ", firstBlockSize: " << firstBlockSize
                    << ", lastBlockSize: " << lastBlockSize
                    << ", blockPos: " << blockPos << ", chunkPos: " << chunkPos;
            for (auto blockIndex = travelStartIndex;
                 blockIndex <= travelEndIndex; blockIndex++) {
                auto objectName = curvefs::common::s3util::GenObjName(
                    chunkid, blockIndex, compaction, fsId, ino);
                prefetchObjs->push_back(std::make_pair(objectName, blockSize));
            }
        }
    }
}

// TODO(hzwuhongsong): These logics are very similar to other place,
// try to merge it
void WarmupManagerS3Impl::WarmUpAllObjs(
    fuse_ino_t key,
    const std::list<std::pair<std::string, uint64_t>> &prefetchObjs) {
    std::atomic<uint64_t> pendingReq(0);
    curve::common::CountDownEvent cond(1);
    uint64_t start = butil::cpuwide_time_us();
    // callback function
    GetObjectAsyncCallBack cb =
        [&](const S3Adapter *adapter,
            const std::shared_ptr<GetObjectAsyncContext> &context) {
            if (bgFetchStop_.load(std::memory_order_acquire)) {
                VLOG(9) << "need stop warmup";
                cond.Signal();
                return;
            }
            if (context->retCode == 0) {
                VLOG(9) << "Get Object success: " << context->key;
                PutObjectToCache(key, context->key, context->buf, context->len);
                CollectMetrics(&warmupS3Metric_.warmupS3Cached, context->len,
                               start);
                warmupS3Metric_.warmupS3CacheSize << context->len;
                if (pendingReq.fetch_sub(1, std::memory_order_seq_cst) == 1) {
                    VLOG(6) << "pendingReq is over";
                    cond.Signal();
                }
                delete[] context->buf;
                return;
            }
            warmupS3Metric_.warmupS3Cached.eps.count << 1;
            if (++context->retry >= option_.downloadMaxRetryTimes) {
                if (pendingReq.fetch_sub(1, std::memory_order_seq_cst) == 1) {
                    VLOG(6) << "pendingReq is over";
                    cond.Signal();
                }
                VLOG(9) << "Up to max retry times, "
                        << "download object failed, key: " << context->key;
                delete[] context->buf;
                return;
            }

            LOG(WARNING) << "Get Object failed, key: " << context->key
                         << ", offset: " << context->offset;

            dynamic_cast<S3ClientAdaptorImpl *>(
              s3Adaptor_.get())->GetS3Client()->DownloadAsync(context);
        };

    pendingReq.fetch_add(prefetchObjs.size(), std::memory_order_seq_cst);
    if (pendingReq.load(std::memory_order_seq_cst)) {
        VLOG(9) << "wait for pendingReq";
        for (auto iter : prefetchObjs) {
            VLOG(9) << "download start: " << iter.first;
            std::string name = iter.first;
            uint64_t readLen = iter.second;
            {
                ReadLockGuard lock(inode2ProgressMutex_);
                auto iterProgress = FindWarmupProgressByKeyLocked(key);
                if (iterProgress->second.GetStorageType() ==
                        curvefs::client::common::WarmupStorageType::
                            kWarmupStorageTypeDisk &&
                    s3Adaptor_->GetDiskCacheManager()->IsCached(name)) {
                    // storage in disk and has cached
                    pendingReq.fetch_sub(1);
                    continue;
                }
            }
            char *cacheS3 = new char[readLen];
            memset(cacheS3, 0, readLen);
            auto context = std::make_shared<GetObjectAsyncContext>();
            context->key = name;
            context->buf = cacheS3;
            context->offset = 0;
            context->len = readLen;
            context->cb = cb;
            context->retry = 0;

            dynamic_cast<S3ClientAdaptorImpl *>(
              s3Adaptor_.get())->GetS3Client()->DownloadAsync(context);
        }
        if (pendingReq.load())
            cond.Wait();
    }
}

bool WarmupManagerS3Impl::ProgressDone(fuse_ino_t key) {
    bool ret;
    {
        ReadLockGuard lockList(warmupFilelistDequeMutex_);
        ret =
            FindWarmupFilelistByKeyLocked(key) == warmupFilelistDeque_.end();
    }

    {
        ReadLockGuard lockDentry(inode2FetchDentryPoolMutex_);
        ret = ret && (FindFetchDentryPoolByKeyLocked(key) ==
                      inode2FetchDentryPool_.end());
    }

    {
        ReadLockGuard lockInodes(warmupInodesDequeMutex_);
        ret = ret &&
              (FindWarmupInodesByKeyLocked(key) == warmupInodesDeque_.end());
    }


    {
        ReadLockGuard lockS3Objects(inode2FetchS3ObjectsPoolMutex_);
        ret = ret && (FindFetchS3ObjectsPoolByKeyLocked(key) ==
                      inode2FetchS3ObjectsPool_.end());
    }
    return ret;
}

void WarmupManagerS3Impl::ScanCleanFetchDentryPool() {
    // clean inode2FetchDentryPool_
    WriteLockGuard lock(inode2FetchDentryPoolMutex_);
    for (auto iter = inode2FetchDentryPool_.begin();
         iter != inode2FetchDentryPool_.end();) {
        std::deque<WarmupInodes>::iterator iterInode;
        if (iter->second->QueueSize() == 0) {
            VLOG(9) << "remove FetchDentry task: " << iter->first;
            iter->second->Stop();
            iter = inode2FetchDentryPool_.erase(iter);
        } else {
            ++iter;
        }
    }
}

void WarmupManagerS3Impl::ScanCleanFetchS3ObjectsPool() {
    // clean inode2FetchS3ObjectsPool_
    WriteLockGuard lock(inode2FetchS3ObjectsPoolMutex_);
    for (auto iter = inode2FetchS3ObjectsPool_.begin();
         iter != inode2FetchS3ObjectsPool_.end();) {
        if (iter->second->QueueSize() == 0) {
            VLOG(9) << "remove FetchS3object task: " << iter->first;
            iter->second->Stop();
            iter = inode2FetchS3ObjectsPool_.erase(iter);
        } else {
            ++iter;
        }
    }
}

void WarmupManagerS3Impl::ScanCleanWarmupProgress() {
    // clean done warmupProgress
    ReadLockGuard lock(inode2ProgressMutex_);
    for (auto iter = inode2Progress_.begin(); iter != inode2Progress_.end();) {
        if (ProgressDone(iter->first)) {
            VLOG(9) << "warmup key: " << iter->first << " done!";
            iter = inode2Progress_.erase(iter);
        } else {
            ++iter;
        }
    }
}

void WarmupManagerS3Impl::ScanWarmupInodes() {
    // file need warmup
    WriteLockGuard lock(warmupInodesDequeMutex_);
    if (!warmupInodesDeque_.empty()) {
        WarmupInodes inodes = warmupInodesDeque_.front();
        for (auto const &iter : inodes.GetReadAheadFiles()) {
            VLOG(9) << "BackGroundFetch: key: " << inodes.GetKey()
                    << " inode:" << iter;
            FetchDataEnqueue(inodes.GetKey(), iter);
        }
        warmupInodesDeque_.pop_front();
    }
}

void WarmupManagerS3Impl::ScanWarmupFilelist() {
    // Use a write lock to ensure that all parsing tasks are added.
    WriteLockGuard lock(warmupFilelistDequeMutex_);
    if (!warmupFilelistDeque_.empty()) {
        WarmupFilelist warmupFilelist = warmupFilelistDeque_.front();
        VLOG(9) << "warmup ino: " << warmupFilelist.GetKey()
                << " len is: " << warmupFilelist.GetFileLen();

        std::vector<std::string> warmuplist;
        GetWarmupList(warmupFilelist, &warmuplist);
        for (auto filePath : warmuplist) {
            FetchDentryEnqueue(warmupFilelist.GetKey(), filePath);
        }
        warmupFilelistDeque_.pop_front();
    }
}

void WarmupManagerS3Impl::AddFetchDentryTask(fuse_ino_t key,
                                             std::function<void()> task) {
    VLOG(9) << "add fetchDentry task: " << key;
    if (!bgFetchStop_.load(std::memory_order_acquire)) {
        WriteLockGuard lock(inode2FetchDentryPoolMutex_);
        auto iter = inode2FetchDentryPool_.find(key);
        if (iter == inode2FetchDentryPool_.end()) {
            std::unique_ptr<ThreadPool> tp = absl::make_unique<ThreadPool>();
            tp->Start(option_.warmupThreadsNum);
            iter = inode2FetchDentryPool_.emplace(key, std::move(tp)).first;
        }
        if (!iter->second->Enqueue(task)) {
            LOG(ERROR) << "key:" << key
                       << " fetch dentry thread pool has been stoped!";
        }
        VLOG(9) << "add fetchDentry task: " << key << " finished";
    }
}

void WarmupManagerS3Impl::AddFetchS3objectsTask(fuse_ino_t key,
                                                std::function<void()> task) {
    VLOG(9) << "add fetchS3Objects task: " << key;
    if (!bgFetchStop_.load(std::memory_order_acquire)) {
        WriteLockGuard lock(inode2FetchS3ObjectsPoolMutex_);
        auto iter = inode2FetchS3ObjectsPool_.find(key);
        if (iter == inode2FetchS3ObjectsPool_.end()) {
            std::unique_ptr<ThreadPool> tp = absl::make_unique<ThreadPool>();
            tp->Start(option_.warmupThreadsNum);
            iter = inode2FetchS3ObjectsPool_.emplace(key, std::move(tp)).first;
        }
        if (!iter->second->Enqueue(task)) {
            LOG(ERROR) << "key:" << key
                       << " fetch s3 objects thread pool has been stoped!";
        }
        VLOG(9) << "add fetchS3Objects task: " << key << " finished";
    }
}

void WarmupManagerS3Impl::PutObjectToCache(fuse_ino_t key,
                                           const std::string &filename,
                                           const char *data, uint64_t len) {
    ReadLockGuard lock(inode2ProgressMutex_);
    auto iter = FindWarmupProgressByKeyLocked(key);
    if (iter == inode2Progress_.end()) {
        VLOG(9) << "no this warmup task progress: " << key;
        return;
    }
    int ret;
    // update progress
    iter->second.FinishedPlusOne();
    switch (iter->second.GetStorageType()) {
    case curvefs::client::common::WarmupStorageType::kWarmupStorageTypeDisk:
        ret = s3Adaptor_->GetDiskCacheManager()->WriteReadDirect(filename, data,
                                                                 len);
        if (ret < 0) {
            LOG_EVERY_SECOND(INFO)
                << "write read directly failed, key: " << filename;
        }
        break;
    case curvefs::client::common::WarmupStorageType::kWarmupStorageTypeKvClient:
        if (kvClientManager_ != nullptr) {
            kvClientManager_->Set(
                std::make_shared<SetKVCacheTask>(filename, data, len));
        }
        break;
    default:
        LOG_EVERY_N(ERROR, 1000) << "unsupported warmup storage type";
    }
}

void WarmupManager::CollectMetrics(InterfaceMetric *interface, int count,
                                   uint64_t start) {
    interface->bps.count << count;
    interface->qps.count << 1;
    interface->latency << (butil::cpuwide_time_us() - start);
}

}  // namespace warmup
}  // namespace client
}  // namespace curvefs
