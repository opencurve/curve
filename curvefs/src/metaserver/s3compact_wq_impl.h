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
using curve::common::InitS3AdaptorOptionExceptFsS3Option;
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
    void Enqueue(std::shared_ptr<InodeManager> inodeManager, InodeKey inodeKey,
                 PartitionInfo pinfo, CopysetNode* copyset);
    std::function<void()> Dequeue();
    void ThreadFunc();

    struct S3CompactTask {
        std::shared_ptr<InodeManager> inodeManager;
        InodeKey inodeKey;
        PartitionInfo pinfo;
        std::shared_ptr<CopysetNodeWrapper> copysetNodeWrapper;
    };

    struct S3CompactCtx {
        uint64_t inodeId;
        uint64_t fsId;
        PartitionInfo pinfo;
        uint64_t blockSize;
        uint64_t chunkSize;
        uint64_t s3adapterIndex;
        S3Adapter* s3adapter;
    };

    struct S3NewChunkInfo {
        uint64_t newChunkId;
        uint64_t newOff;
        uint64_t newCompaction;
    };

    struct S3Request {
        uint64_t reqIndex;
        bool zero;
        std::string objName;
        uint64_t off;
        uint64_t len;

        S3Request(uint64_t reqIndex, bool zero, std::string objName,
                  uint64_t off, uint64_t len)
            : reqIndex(reqIndex),
              zero(zero),
              objName(objName),
              off(off),
              len(len) {}
    };

    // node for building valid list
    struct Node {
        uint64_t begin;
        uint64_t end;
        uint64_t chunkid;
        uint64_t compaction;
        uint64_t chunkoff;
        uint64_t chunklen;
        bool zero;
        Node(uint64_t begin, uint64_t end, uint64_t chunkid,
             uint64_t compaction, uint64_t chunkoff, uint64_t chunklen,
             bool zero)
            : begin(begin),
              end(end),
              chunkid(chunkid),
              compaction(compaction),
              chunkoff(chunkoff),
              chunklen(chunklen),
              zero(zero) {}
    };

    // closure for updating inode, simply wait
    class GetOrModifyS3ChunkInfoClosure : public google::protobuf::Closure {
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
    bool CompactPrecheck(const struct S3CompactTask& task, Inode* inode,
                         std::vector<uint64_t>* needCompact);
    S3Adapter* SetupS3Adapter(uint64_t fsid, uint64_t* s3adapterIndex,
                              uint64_t* blockSize, uint64_t* chunkSize);
    void DeleteObjs(const std::vector<std::string>& objsAdded,
                    S3Adapter* s3adapter);
    std::list<struct Node> BuildValidList(
        const S3ChunkInfoList& s3chunkinfolist, uint64_t inodeLen);
    void GenS3ReadRequests(const struct S3CompactCtx& ctx,
                           const std::list<struct Node>& validList,
                           std::vector<struct S3Request>* reqs,
                           struct S3NewChunkInfo* newChunkInfo);
    int ReadFullChunk(const struct S3CompactCtx& ctx,
                      const std::list<struct Node>& validList,
                      std::string* fullChunk,
                      struct S3NewChunkInfo* newChunkInfo);
    virtual MetaStatusCode UpdateInode(
        CopysetNode* copysetNode, const PartitionInfo& pinfo, uint64_t inodeId,
        ::google::protobuf::Map<uint64_t, S3ChunkInfoList>&& s3ChunkInfoAdd,
        ::google::protobuf::Map<uint64_t, S3ChunkInfoList>&& s3ChunkInfoRemove);
    int WriteFullChunk(const struct S3CompactCtx& ctx,
                       const struct S3NewChunkInfo& newChunkInfo,
                       const std::string& fullChunk,
                       std::vector<std::string>* objsAdded);
    void CompactChunk(
        const struct S3CompactCtx& compactCtx, uint64_t index,
        const Inode& inode,
        std::unordered_map<uint64_t, std::vector<std::string>>* objsAddedMap,
        ::google::protobuf::Map<uint64_t, S3ChunkInfoList>* s3ChunkInfoAdd,
        ::google::protobuf::Map<uint64_t, S3ChunkInfoList>* s3ChunkInfoRemove);

    void DeleteObjsOfS3ChunkInfoList(const struct S3CompactCtx& ctx,
                                     const S3ChunkInfoList& s3chunkinfolist);
    // func bind with task
    void CompactChunks(const struct S3CompactTask& task);
};

}  // namespace metaserver
}  // namespace curvefs

#endif  // CURVEFS_SRC_METASERVER_S3COMPACT_WQ_IMPL_H_
