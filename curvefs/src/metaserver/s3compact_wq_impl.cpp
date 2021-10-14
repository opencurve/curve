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

#include "curvefs/src/metaserver/s3compact_wq_impl.h"

#include <algorithm>
#include <deque>
#include <list>
#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "curvefs/src/common/s3util.h"
#include "curvefs/src/metaserver/copyset/meta_operator.h"

using curve::common::Configuration;
using curve::common::InitS3AdaptorOption;
using curve::common::S3Adapter;
using curve::common::S3AdapterOption;
using curve::common::TaskThreadPool;
using curvefs::metaserver::copyset::UpdateInodeOperator;

namespace curvefs {
namespace metaserver {

void S3CompactWorkQueueImpl::Enqueue(std::shared_ptr<InodeStorage> inodeStorage,
                                     InodeKey inodeKey, PartitionInfo pinfo,
                                     CopysetNode* copysetNode) {
    std::unique_lock<std::mutex> guard(mutex_);

    // inodeKey already in working queue, just return
    if (std::find(compactingInodes_.begin(), compactingInodes_.end(),
                  inodeKey) != compactingInodes_.end()) {
        return;
    }

    while (IsFullUnlock()) {
        notFull_.wait(guard);
    }
    compactingInodes_.push_back(inodeKey);
    CopysetNodeWrapper copysetNodeWrapper(copysetNode);
    auto task =
        std::bind(&S3CompactWorkQueueImpl::CompactChunks, this, inodeStorage,
                  std::ref(inodeKey), std::ref(pinfo), &copysetNodeWrapper);
    // am i copysetnode leader?_
    queue_.push_back(std::move(task));
    notEmpty_.notify_one();
}

std::function<void()> S3CompactWorkQueueImpl::Dequeue() {
    std::unique_lock<std::mutex> guard(mutex_);
    while (queue_.empty() && running_.load(std::memory_order_acquire)) {
        notEmpty_.wait(guard);
    }
    std::function<void()> task;
    if (!queue_.empty()) {
        task = std::move(queue_.front());
        queue_.pop_front();
        compactingInodes_.pop_front();
        notFull_.notify_one();
    }

    return task;
}

void S3CompactWorkQueueImpl::ThreadFunc() {
    while (running_.load(std::memory_order_acquire)) {
        Task task(Dequeue());
        if (task) {
            task();
        }
    }
}

std::vector<uint64_t> S3CompactWorkQueueImpl::GetNeedCompact(
    const ::google::protobuf::Map<uint64_t, S3ChunkInfoList>& s3chunkinfoMap) {
    std::vector<uint64_t> needCompact;
    for (const auto& item : s3chunkinfoMap) {
        if (needCompact.size() >= opts_.maxChunksPerCompact) {
            break;
        }
        if (item.second.s3chunks_size() > opts_.fragmentThreshold) {
            needCompact.push_back(item.first);
        }
    }
    return needCompact;
}

void S3CompactWorkQueueImpl::DeleteObjs(const std::vector<std::string>& objs) {
    for (const auto& obj : objs) {
        const Aws::String aws_key(obj.c_str(), obj.size());
        s3Adapter_->DeleteObject(aws_key);  // don't care success or not
    }
}

std::list<struct S3CompactWorkQueueImpl::Node>
S3CompactWorkQueueImpl::BuildValidList(const S3ChunkInfoList& s3chunkinfolist,
                                       uint64_t inodeLen) {
    std::list<struct S3CompactWorkQueueImpl::Node> validList;
    std::function<void(uint64_t, uint64_t, uint64_t, uint64_t, uint64_t,
                       uint64_t)>
        addToValidList;
    // always add newer s3chunkinfo to list
    addToValidList = [&](uint64_t begin, uint64_t end, uint64_t chunkid,
                         uint64_t compaction, uint64_t chunkoff,
                         uint64_t zero) {
        // maybe truncated smaller
        if (end > inodeLen) end = inodeLen;
        for (auto it = validList.begin(); it != validList.end();) {
            // merge from list head to tail
            // try merge B to existing A
            // data from B is newer than A
            uint64_t nodeBegin = it->begin;
            uint64_t nodeEnd = it->end;
            uint64_t nodeChunkid = it->chunkid;
            uint64_t nodeCompaction = it->compaction;
            uint64_t nodeChunkoff = it->chunkoff;
            bool nodeZero = it->zero;
            if (end <= nodeBegin) {
                // A,B no overlap, B is left A, just put add to list
                validList.emplace(it, begin, end, chunkid, compaction, chunkoff,
                                  zero);
                return;
            } else if (begin >= nodeEnd) {
                // A,B no overlap, B is right of A
                // skip current node to next node
                ++it;
            } else if (begin <= nodeBegin && end >= nodeEnd) {
                // B contains A, 1 part or split to 2 part
                it->begin = begin;
                it->chunkid = chunkid;
                it->compaction = compaction;
                it->chunkoff = chunkoff;
                it->zero = zero;
                if (end > nodeEnd) {
                    addToValidList(nodeEnd, end, chunkid, compaction, chunkoff,
                                   zero);
                }
                return;
            } else if (begin >= nodeBegin && end <= nodeEnd) {
                // A contains B, 1 part or split to 2 part
                it->begin = end;
                auto front = validList.emplace(it, begin, end, chunkid,
                                               compaction, chunkoff, zero);
                if (nodeBegin < begin) {
                    validList.emplace(front, nodeBegin, begin, nodeChunkid,
                                      nodeCompaction, nodeChunkoff, nodeZero);
                }
                return;
            } else if (end > nodeEnd && begin > nodeBegin && begin < nodeEnd) {
                // A,B overlap, B right A split to 3 part
                it->begin = begin;
                it->chunkid = chunkid;
                it->compaction = compaction;
                it->chunkoff = chunkoff;
                it->zero = zero;
                validList.emplace(it, nodeBegin, begin, nodeChunkid,
                                  nodeCompaction, nodeChunkoff, nodeZero);
                addToValidList(nodeEnd, end, chunkid, compaction, chunkoff,
                               zero);
                return;
            } else if (begin < nodeBegin && end < nodeEnd && end > nodeBegin) {
                // A,B overlap, B left A, split to 2 part
                it->begin = end;
                validList.emplace(it, begin, end, chunkid, compaction, chunkoff,
                                  zero);
                return;
            }
        }
        validList.emplace_back(begin, end, chunkid, compaction, chunkoff, zero);
    };

    for (auto i = 0; i < s3chunkinfolist.s3chunks_size(); i++) {
        auto chunkinfo = s3chunkinfolist.s3chunks(i);
        addToValidList(chunkinfo.offset(), chunkinfo.offset() + chunkinfo.len(),
                       chunkinfo.chunkid(), chunkinfo.compaction(),
                       chunkinfo.offset(), chunkinfo.zero());
    }

    // merge same chunkid continuous nodes
    for (auto curr = validList.begin();;) {
        auto next = std::next(curr);
        if (next == validList.end()) break;
        if (curr->end == next->begin) {
            if (curr->chunkid == next->chunkid) {
                next->begin = curr->begin;
                validList.erase(curr);
            }
        }
        curr = next;
    }
    return validList;
}

int S3CompactWorkQueueImpl::ReadFullChunk(
    const std::list<struct S3CompactWorkQueueImpl::Node>& validList,
    uint64_t fsId, uint64_t inodeId, uint64_t blockSize, std::string* fullChunk,
    uint64_t* newChunkid, uint64_t* newCompaction) {
    for (auto curr = validList.begin(); curr != validList.end();) {
        auto next = std::next(curr);
        if (curr->zero) {
            fullChunk->append(curr->end - curr->begin, '\0');
            curr = next;
            continue;
        }
        // block index is relatively in a s3chunkinfo
        for (uint64_t index = (curr->begin - curr->chunkoff) / blockSize;
             index * blockSize + curr->chunkoff < curr->end; index++) {
            // read the block obj
            std::string objName = curvefs::common::s3util::GenObjName(
                curr->chunkid, index, curr->compaction, fsId, inodeId);
            std::string buf;
            const Aws::String aws_key(objName.c_str(), objName.size());
            int ret = s3Adapter_->GetObject(aws_key, &buf);
            if (ret != 0) {
                LOG(WARNING) << "Get S3 obj " << objName << " failed.";
                return ret;
            } else {
                uint64_t blockBegin = index * blockSize + curr->chunkoff;
                uint64_t blockEnd = (index + 1) * blockSize + curr->chunkoff;
                if (curr->begin >= blockBegin && curr->end <= blockEnd) {
                    // all what we need is only part of block
                    (*fullChunk) += buf.substr(curr->begin - blockBegin,
                                               curr->end - curr->begin);
                } else if (curr->begin >= blockBegin && curr->end > blockEnd) {
                    // not last block, what we need is part of block
                    (*fullChunk) += buf.substr(curr->begin - blockBegin,
                                               blockEnd - curr->begin);
                } else if (curr->begin < blockBegin && curr->end > blockEnd) {
                    // what we need is full block
                    (*fullChunk) += buf.substr(0, blockSize);
                } else if (curr->begin < blockBegin && curr->end <= blockEnd) {
                    // last block, what we need is part of block
                    (*fullChunk) += buf.substr(0, curr->end - blockBegin);
                }
            }
        }

        if (curr->chunkid >= *newChunkid) {
            (*newChunkid) = curr->chunkid;
            (*newCompaction) = curr->compaction;
        }
        if (next != validList.end() && curr->end < next->begin) {
            // hole, append 0
            fullChunk->append(next->begin - curr->end, '\0');
        }
        curr = next;
    }
    // inc compaction
    (*newCompaction) += 1;
    return 0;
}

MetaStatusCode S3CompactWorkQueueImpl::UpdateInode(CopysetNode* copysetNode,
                                                   const PartitionInfo& pinfo,
                                                   const Inode& inode) {
    UpdateInodeRequest request;
    request.set_poolid(pinfo.poolid());
    request.set_copysetid(pinfo.copysetid());
    request.set_partitionid(pinfo.partitionid());
    request.set_fsid(pinfo.fsid());
    request.set_inodeid(inode.inodeid());
    *request.mutable_s3chunkinfomap() = inode.s3chunkinfomap();
    UpdateInodeResponse response;
    S3CompactWorkQueueImpl::UpdateInodeClosure done;
    auto UpdateInodeOp = new UpdateInodeOperator(copysetNode, nullptr, &request,
                                                 &response, &done);
    UpdateInodeOp->Propose();
    done.WaitRunned();
    return response.statuscode();
}

int S3CompactWorkQueueImpl::WriteFullChunk(
    const std::string& fullChunk, uint64_t fsId, uint64_t inodeId,
    uint64_t blockSize, uint64_t newChunkid, uint64_t newCompaction,
    std::vector<std::string>* objsAdded) {
    uint64_t chunkLen = fullChunk.length();
    for (uint64_t index = 0; index * blockSize < chunkLen; index += 1) {
        std::string objName = curvefs::common::s3util::GenObjName(
            newChunkid, index, newCompaction, fsId, inodeId);
        const Aws::String aws_key(objName.c_str(), objName.size());
        int ret;
        if (chunkLen > (index + 1) * blockSize) {
            ret = s3Adapter_->PutObject(
                aws_key, fullChunk.substr(index * blockSize, blockSize));
        } else {
            // last block
            ret = s3Adapter_->PutObject(
                aws_key, fullChunk.substr(index * blockSize,
                                          chunkLen - index * blockSize));
        }
        if (ret != 0) {
            LOG(WARNING) << "Put S3 object " << objName << " failed";
            return ret;
        } else {
            objsAdded->emplace_back(std::move(objName));
        }
    }
    return 0;
}

void S3CompactWorkQueueImpl::CompactChunks(
    std::shared_ptr<InodeStorage> inodeStorage, const InodeKey& inodeKey,
    const PartitionInfo& pinfo, CopysetNodeWrapper* copysetNodeWrapper) {
    // am i copysetnode leader?
    if (!copysetNodeWrapper->IsLeaderTerm()) return;

    // inode exist?
    Inode inode;
    MetaStatusCode ret = inodeStorage->Get(inodeKey, &inode);
    if (ret != MetaStatusCode::OK) {
        LOG(WARNING) << "GetInode fail, inodeKey = " << inodeKey.fsId << ","
                     << inodeKey.inodeId
                     << ", ret = " << MetaStatusCode_Name(ret);
        return;
    }

    // deleted?
    if (inode.nlink() == 0) {
        return;
    }

    // s3chunkinfomap size 0?
    if (inode.s3chunkinfomap().size() == 0) {
        return;
    }
    uint64_t fsId = inode.fsid();
    uint64_t inodeId = inode.inodeid();
    uint64_t inodeLen = inode.length();

    // need compact?
    const auto origMap(inode.s3chunkinfomap());
    std::vector<uint64_t> needCompact(GetNeedCompact(origMap));
    if (needCompact.empty()) {
        return;
    }

    // let's compact
    // 1. write new objs, each chunk one by one
    uint64_t blockSize = opts_.blockSize;
    std::vector<std::string> objsAdded;
    std::vector<uint64_t> indexToDelete;
    for (const auto& index : needCompact) {
        // 1.1 build valid list
        auto s3chunkVec =
            inode.mutable_s3chunkinfomap()->at(index).mutable_s3chunks();
        // sort s3chunks to make small chunkid -> big chunkid
        // bigger chunkid means newer data
        std::sort(s3chunkVec->begin(), s3chunkVec->end(),
                  [](const S3ChunkInfo& a, const S3ChunkInfo& b) {
                      return a.chunkid() < b.chunkid();
                  });
        const auto& s3chunkinfolist = origMap.at(index);

        std::list<struct S3CompactWorkQueueImpl::Node> validList(
            BuildValidList(s3chunkinfolist, inodeLen));
        // 1.2 gen obj names and read s3
        std::string fullChunk;
        uint64_t newChunkid = 0;
        uint64_t newCompaction = 0;
        int ret = ReadFullChunk(validList, fsId, inodeId, blockSize, &fullChunk,
                                &newChunkid, &newCompaction);
        if (ret != 0) {
            LOG(WARNING) << "S3Compact ReadFullChunk failed, index " << index;
            DeleteObjs(objsAdded);
            return;
        }
        // 1.3 then write objs with newChunkid and newCompaction
        ret = WriteFullChunk(fullChunk, fsId, inodeId, blockSize, newChunkid,
                             newCompaction, &objsAdded);
        if (ret != 0) {
            LOG(WARNING) << "S3Compact WriteFullChunk failed, index " << index;
            DeleteObjs(objsAdded);
            return;
        }
        // 1.4 update inode locally
        // record delete
        indexToDelete.emplace_back(index);
        // rm all
        s3chunkVec->Clear();
        // add new
        S3ChunkInfo toAdd;
        toAdd.set_chunkid(newChunkid);
        toAdd.set_compaction(newCompaction);
        toAdd.set_offset(validList.begin()->begin);
        toAdd.set_len(fullChunk.length());
        toAdd.set_size(fullChunk.length());
        toAdd.set_zero(false);
        *s3chunkVec->Add() = std::move(toAdd);
    }

    // 2. update inode
    ret = UpdateInode(copysetNodeWrapper->get(), pinfo, inode);
    if (ret != MetaStatusCode::OK) {
        LOG(WARNING) << "S3Compact UpdateInode failed, inodeKey = "
                     << inodeKey.fsId << "," << inodeKey.inodeId
                     << ", ret = " << MetaStatusCode_Name(ret);
        DeleteObjs(objsAdded);
        return;
    }

    // 3. delete old objs
    for (const auto& index : indexToDelete) {
        const auto& s3chunkinfolist = origMap.at(index);
        for (auto i = 0; i < s3chunkinfolist.s3chunks_size(); i++) {
            const auto& chunkinfo = s3chunkinfolist.s3chunks(i);
            uint64_t off = chunkinfo.offset();
            uint64_t len = chunkinfo.len();
            while (off < len) {
                std::string objName = curvefs::common::s3util::GenObjName(
                    chunkinfo.chunkid(), (off - chunkinfo.offset()) / blockSize,
                    chunkinfo.compaction(), fsId, inodeId);
                const Aws::String aws_key(objName.c_str(), objName.size());
                s3Adapter_->DeleteObject(aws_key);  // don't care success or not
                off += blockSize;
            }
        }
    }
}

}  // namespace metaserver
}  // namespace curvefs
