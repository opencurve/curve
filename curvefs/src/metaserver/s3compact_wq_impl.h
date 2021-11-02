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
 * @Project: curve
 * @Date: 2021-09-07
 * @Author: majie1
 */

#ifndef CURVEFS_SRC_METASERVER_S3COMPACT_WQ_IMPL_H_
#define CURVEFS_SRC_METASERVER_S3COMPACT_WQ_IMPL_H_

#include <algorithm>
#include <condition_variable>
#include <deque>
#include <list>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "curvefs/src/metaserver/copyset/copyset_node.h"
#include "curvefs/src/metaserver/inode_storage.h"
#include "curvefs/src/metaserver/s3compact_manager.h"
#include "src/common/concurrent/task_thread_pool.h"
#include "src/common/configuration.h"
#include "src/common/s3_adapter.h"

using curve::common::Configuration;
using curve::common::InitS3AdaptorOption;
using curve::common::S3Adapter;
using curve::common::S3AdapterOption;
using curve::common::TaskThreadPool;
using curvefs::metaserver::copyset::CopysetNode;

namespace curvefs {
namespace metaserver {

class CopysetNodeWrapper {
 public:
    explicit CopysetNodeWrapper(CopysetNode* copysetNode)
        : copysetNode_(copysetNode) {}
    virtual ~CopysetNodeWrapper() {}
    CopysetNode* copysetNode_;
    virtual bool IsLeaderTerm() {
        if (copysetNode_ == nullptr) return false;
        return copysetNode_->IsLeaderTerm();
    }
    virtual bool IsValid() {
        return copysetNode_ != nullptr;
    }
    CopysetNode* Get() {
        return copysetNode_;
    }
};

class S3CompactWorkQueueImpl : public TaskThreadPool<> {
 public:
    S3CompactWorkQueueImpl(std::shared_ptr<S3AdapterManager> s3adapterManager,
                           std::shared_ptr<S3InfoCache> s3infoCache,
                           const S3CompactWorkQueueOption& opts)
        : s3adapterManager_(s3adapterManager),
          s3infoCache_(s3infoCache),
          opts_(opts) {}

    std::shared_ptr<S3AdapterManager> s3adapterManager_;
    std::shared_ptr<S3InfoCache> s3infoCache_;
    S3CompactWorkQueueOption opts_;
    std::deque<InodeKey> compactingInodes_;
    void Enqueue(std::shared_ptr<InodeStorage> inodeStorage, InodeKey inodeKey,
                 PartitionInfo pinfo, CopysetNode* copyset);
    std::function<void()> Dequeue();
    void ThreadFunc();

    // node for building valid list
    struct Node {
        uint64_t begin;
        uint64_t end;
        uint64_t chunkid;
        uint64_t compaction;
        uint64_t chunkoff;
        bool zero;
        Node(uint64_t begin, uint64_t end, uint64_t chunkid,
             uint64_t compaction, uint64_t chunkoff, bool zero)
            : begin(begin),
              end(end),
              chunkid(chunkid),
              compaction(compaction),
              chunkoff(chunkoff),
              zero(zero) {}
    };
    // closure for updating inode, simply wait
    class UpdateInodeClosure : public google::protobuf::Closure {
     private:
        std::mutex mutex_;
        std::condition_variable cond_;
        bool runned_ = false;

     public:
        void Run() override {
            std::lock_guard<std::mutex> l(mutex_);
            runned_ = true;
            cond_.notify_one();
        }

        void WaitRunned() {
            std::unique_lock<std::mutex> ul(mutex_);
            cond_.wait(ul, [this]() { return runned_; });
        }
    };

    std::vector<uint64_t> GetNeedCompact(
        const ::google::protobuf::Map<uint64_t, S3ChunkInfoList>&
            s3chunkinfoMap);
    void DeleteObjs(const std::vector<std::string>& objsAdded,
                    S3Adapter* s3adapter);
    std::list<struct Node> BuildValidList(
        const S3ChunkInfoList& s3chunkinfolist, uint64_t inodeLen);
    int ReadFullChunk(const std::list<struct Node>& validList, uint64_t fsId,
                      uint64_t inodeId, uint64_t blockSize,
                      std::string* fullChunk, uint64_t* newChunkId,
                      uint64_t* newCompaction, S3Adapter* s3adapter);
    virtual MetaStatusCode UpdateInode(CopysetNode* copysetNode,
                                       const PartitionInfo& pinfo,
                                       const Inode& inode);
    int WriteFullChunk(const std::string& fullChunk, uint64_t fsId,
                       uint64_t inodeId, uint64_t blockSize,
                       uint64_t newChunkid, uint64_t newCompaction,
                       std::vector<std::string>* objsAdded,
                       S3Adapter* s3adapter);
    // func bind with task
    void CompactChunks(std::shared_ptr<InodeStorage> inodeStorage,
                       InodeKey inodeKey, PartitionInfo pinfo,
                       std::shared_ptr<CopysetNodeWrapper> copysetNodeWrapper,
                       std::shared_ptr<S3AdapterManager> s3adapterManager,
                       std::shared_ptr<S3InfoCache> s3infoCache);
};

}  // namespace metaserver
}  // namespace curvefs

#endif  // CURVEFS_SRC_METASERVER_S3COMPACT_WQ_IMPL_H_
