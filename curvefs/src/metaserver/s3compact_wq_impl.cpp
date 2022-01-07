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
using curvefs::metaserver::copyset::GetOrModifyS3ChunkInfoOperator;

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
    auto copysetNodeWrapper = std::make_shared<CopysetNodeWrapper>(copysetNode);
    auto task = std::bind(&S3CompactWorkQueueImpl::CompactChunks, this,
                          inodeStorage, inodeKey, pinfo, copysetNodeWrapper,
                          s3adapterManager_, s3infoCache_);
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
            VLOG(9) << "s3compact: reach max chunks to compact per time";
            break;
        }
        if (item.second.s3chunks_size() > opts_.fragmentThreshold) {
            needCompact.push_back(item.first);
        }
    }
    return needCompact;
}

void S3CompactWorkQueueImpl::DeleteObjs(const std::vector<std::string>& objs,
                                        S3Adapter* s3adapter) {
    for (const auto& obj : objs) {
        const Aws::String aws_key(obj.c_str(), obj.size());
        int ret =
            s3adapter->DeleteObject(aws_key);  // don't care success or not
        if (ret != 0) {
            VLOG(9) << "s3compact: delete " << obj << " failed";
        }
    }
}

std::list<struct S3CompactWorkQueueImpl::Node>
S3CompactWorkQueueImpl::BuildValidList(const S3ChunkInfoList& s3chunkinfolist,
                                       uint64_t inodeLen) {
    std::list<struct S3CompactWorkQueueImpl::Node> validList;
    std::function<void(struct Node)> addToValidList;
    // always add newer s3chunkinfo to list
    // [begin, end]
    addToValidList = [&](struct Node newNode) {
        // maybe truncated smaller
        if (newNode.end >= inodeLen) newNode.end = inodeLen - 1;
        for (auto it = validList.begin(); it != validList.end();) {
            // merge from list head to tail
            // try merge B to existing A
            // data from B is newer than A
            uint64_t nodeBegin = it->begin;
            uint64_t nodeEnd = it->end;
            bool nodeZero = it->zero;
            if (newNode.end < nodeBegin) {
                // A,B no overlap, B is left A, just add to list
                validList.emplace(it, newNode);
                return;
            } else if (newNode.begin > nodeEnd) {
                // A,B no overlap, B is right of A
                // skip current node to next node
                ++it;
            } else if (newNode.begin <= nodeBegin && newNode.end >= nodeEnd) {
                // B contains A, 1 part or split to 2 part
                *it = newNode;
                it->end = nodeEnd;
                if (newNode.end > nodeEnd) {
                    auto n = newNode;
                    n.begin = nodeEnd + 1;
                    addToValidList(std::move(n));
                }
                return;
            } else if (newNode.begin >= nodeBegin && newNode.end <= nodeEnd) {
                // A contains B, 1 part or split to 2or3 part
                // begin == nodeBegin && end == nodeEnd has already
                // processed in previous elseif
                auto n = *it;
                if (newNode.end < nodeEnd) {
                    it->begin = newNode.end + 1;
                    auto front = validList.emplace(it, newNode);
                    if (nodeBegin < newNode.begin) {
                        n.end = newNode.begin - 1;
                        validList.emplace(front, n);
                    }
                } else {
                    *it = newNode;
                    it->end = nodeEnd;
                    if (nodeBegin < newNode.begin) {
                        n.end = newNode.begin - 1;
                        validList.emplace(it, n);
                    }
                }
                return;
            } else if (newNode.end > nodeEnd && newNode.begin > nodeBegin &&
                       newNode.begin <= nodeEnd) {
                // A,B overlap, B right A split to 3 part
                auto n1 = *it;
                *it = newNode;
                it->end = nodeEnd;
                n1.end = newNode.begin - 1;
                validList.emplace(it, n1);
                auto n2 = newNode;
                n2.begin = nodeEnd + 1;
                addToValidList(std::move(n2));
                return;
            } else if (newNode.begin < nodeBegin && newNode.end < nodeEnd &&
                       newNode.end >= nodeBegin) {
                // A,B overlap, B left A, split to 2 part
                it->begin = newNode.end + 1;
                validList.emplace(it, newNode);
                return;
            }
        }
        validList.emplace_back(newNode);
    };

    for (auto i = 0; i < s3chunkinfolist.s3chunks_size(); i++) {
        auto chunkinfo = s3chunkinfolist.s3chunks(i);
        struct Node n {
            chunkinfo.offset(), chunkinfo.offset() + chunkinfo.len() - 1,
                chunkinfo.chunkid(), chunkinfo.compaction(), chunkinfo.offset(),
                chunkinfo.len(), chunkinfo.zero()
        };
        addToValidList(std::move(n));
    }

    // merge same chunkid continuous nodes
    for (auto curr = validList.begin();;) {
        auto next = std::next(curr);
        if (next == validList.end()) break;
        if (curr->end == next->begin - 1) {
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
    uint64_t fsId, uint64_t inodeId, uint64_t blockSize, uint64_t chunkSize,
    std::string* fullChunk, uint64_t* newChunkid, uint64_t* newCompaction,
    S3Adapter* s3adapter) {
    for (auto curr = validList.begin(); curr != validList.end();) {
        auto next = std::next(curr);
        if (curr->zero) {
            fullChunk->append(curr->end - curr->begin + 1, '\0');
            curr = next;
            continue;
        }

        VLOG(9) << "s3compact: read [" << curr->begin << "-" << curr->end
                << "]";
        uint64_t beginRoundDown = curr->begin / chunkSize * chunkSize;
        uint64_t startIndex = (curr->begin - beginRoundDown) / blockSize;
        for (uint64_t index = startIndex;
             beginRoundDown + index * blockSize <= curr->end; index++) {
            // read the block obj
            std::string objName = curvefs::common::s3util::GenObjName(
                curr->chunkid, index, curr->compaction, fsId, inodeId);
            VLOG(9) << "s3compact: get " << objName;
            std::string buf;
            const Aws::String aws_key(objName.c_str(), objName.size());
            int ret = s3adapter->GetObject(aws_key, &buf);
            if (ret != 0) {
                LOG(WARNING) << "Get S3 obj " << objName << " failed.";
                return ret;
            } else {
                uint64_t s3objBegin = std::max(
                    curr->chunkoff, beginRoundDown + index * blockSize);
                uint64_t s3objEnd =
                    std::min(curr->chunkoff + curr->chunklen - 1,
                             beginRoundDown + (index + 1) * blockSize - 1);
                VLOG(9) << "s3compact: get success " << s3objBegin << "-"
                        << s3objEnd;
                if (curr->begin >= s3objBegin && curr->end <= s3objEnd) {
                    // all what we need is only part of block
                    (*fullChunk) += buf.substr(curr->begin - s3objBegin,
                                               curr->end - curr->begin + 1);
                } else if (curr->begin >= s3objBegin && curr->end > s3objEnd) {
                    // not last block, what we need is part of block
                    (*fullChunk) += buf.substr(curr->begin - s3objBegin,
                                               s3objEnd - curr->begin + 1);
                } else if (curr->begin < s3objBegin && curr->end > s3objEnd) {
                    // what we need is full block
                    (*fullChunk) += buf.substr(0, blockSize);
                } else if (curr->begin < s3objBegin && curr->end <= s3objEnd) {
                    // last block, what we need is part of block
                    (*fullChunk) += buf.substr(0, curr->end - s3objBegin + 1);
                    break;
                }
                VLOG(9) << "s3compact: append to output success " << objName;
            }
        }

        if (curr->chunkid >= *newChunkid) {
            (*newChunkid) = curr->chunkid;
            (*newCompaction) = curr->compaction;
        }
        if (next != validList.end() && curr->end + 1 < next->begin) {
            // hole, append 0
            fullChunk->append(next->begin - curr->end - 1, '\0');
        }
        curr = next;
    }
    // inc compaction
    (*newCompaction) += 1;
    return 0;
}

MetaStatusCode S3CompactWorkQueueImpl::UpdateInode(
    CopysetNode* copysetNode, const PartitionInfo& pinfo, uint64_t inodeId,
    ::google::protobuf::Map<uint64_t, S3ChunkInfoList>&& s3ChunkInfoAdd,
    ::google::protobuf::Map<uint64_t, S3ChunkInfoList>&& s3ChunkInfoRemove) {
    GetOrModifyS3ChunkInfoRequest request;
    request.set_poolid(pinfo.poolid());
    request.set_copysetid(pinfo.copysetid());
    request.set_partitionid(pinfo.partitionid());
    request.set_fsid(pinfo.fsid());
    request.set_inodeid(inodeId);
    *request.mutable_s3chunkinfoadd() = std::move(s3ChunkInfoAdd);
    *request.mutable_s3chunkinforemove() = std::move(s3ChunkInfoRemove);
    request.set_returninode(false);
    GetOrModifyS3ChunkInfoResponse response;
    S3CompactWorkQueueImpl::GetOrModifyS3ChunkInfoClosure done;
    // if copysetnode change to nullptr, maybe crash
    auto GetOrModifyS3ChunkInfoOp = new GetOrModifyS3ChunkInfoOperator(
        copysetNode, nullptr, &request, &response, &done);
    GetOrModifyS3ChunkInfoOp->Propose();
    done.WaitRunned();
    return response.statuscode();
}

int S3CompactWorkQueueImpl::WriteFullChunk(
    const std::string& fullChunk, uint64_t fsId, uint64_t inodeId,
    uint64_t blockSize, uint64_t chunkSize, uint64_t newChunkid,
    uint64_t newCompaction, uint64_t newOff,
    std::vector<std::string>* objsAdded, S3Adapter* s3adapter) {
    uint64_t chunkLen = fullChunk.length();
    uint64_t offRoundDown = newOff / chunkSize * chunkSize;
    uint64_t startIndex = (newOff - newOff / chunkSize * chunkSize) / blockSize;
    for (uint64_t index = startIndex;
         index * blockSize + offRoundDown < newOff + chunkLen; index += 1) {
        std::string objName = curvefs::common::s3util::GenObjName(
            newChunkid, index, newCompaction, fsId, inodeId);
        VLOG(9) << "s3compact: put " << objName;
        const Aws::String aws_key(objName.c_str(), objName.size());
        int ret;
        uint64_t s3objBegin =
            std::max(newOff, offRoundDown + index * blockSize);
        uint64_t s3objEnd = std::min(
            newOff + chunkLen - 1, offRoundDown + (index + 1) * blockSize - 1);
        VLOG(9) << "s3compact: [" << s3objBegin << "-" << s3objEnd << "]";
        ret = s3adapter->PutObject(
            aws_key,
            fullChunk.substr(s3objBegin - newOff, s3objEnd - s3objBegin + 1));
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
    std::shared_ptr<InodeStorage> inodeStorage, InodeKey inodeKey,
    PartitionInfo pinfo, std::shared_ptr<CopysetNodeWrapper> copysetNodeWrapper,
    std::shared_ptr<S3AdapterManager> s3adapterManager,
    std::shared_ptr<S3InfoCache> s3infoCache) {
    auto cleanup = absl::MakeCleanup([this, inodeKey]() {
        std::lock_guard<std::mutex> guard(mutex_);
        auto it = std::find(compactingInodes_.begin(), compactingInodes_.end(),
                            inodeKey);
        if (it != compactingInodes_.end()) {
            compactingInodes_.erase(it);
        }
    });

    VLOG(6) << "s3compact: try to compact, fsId: " << inodeKey.fsId
            << " , inodeId: " << inodeKey.inodeId;
    // am i copysetnode leader?
    if (!copysetNodeWrapper->IsLeaderTerm()) {
        VLOG(6) << "s3compact: i am not the leader, finish";
        return;
    }

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
        VLOG(6) << "s3compact: inode already deleted";
        return;
    }

    // s3chunkinfomap size 0?
    if (inode.s3chunkinfomap().size() == 0) {
        VLOG(6) << "s3compact: empty s3chunkinfomap";
        return;
    }
    uint64_t fsId = inode.fsid();
    uint64_t inodeId = inode.inodeid();
    uint64_t inodeLen = inode.length();

    // need compact?
    const auto& origMap = inode.s3chunkinfomap();
    std::vector<uint64_t> needCompact(GetNeedCompact(origMap));
    if (needCompact.empty()) {
        VLOG(6) << "s3compact: no need to compact " << inode.inodeid();
        return;
    }

    // let's compact
    // 0. get s3adapter&s3info, set s3adapter
    // include ak, sk, addr, bucket, blocksize, chunksize
    uint64_t blockSize, chunkSize;
    auto pairResult = s3adapterManager->GetS3Adapter();
    uint64_t s3adapterIndex = pairResult.first;
    S3Adapter* s3adapter = pairResult.second;
    if (s3adapter == nullptr) {
        VLOG(3) << "s3compact: fail to get s3adapter";
        return;
    }

    S3Info s3info;
    int status = s3infoCache->GetS3Info(fsId, &s3info);
    if (status == 0) {
        blockSize = s3info.blocksize();
        chunkSize = s3info.chunksize();
        if (s3adapter->GetS3Ak() != s3info.ak() ||
            s3adapter->GetS3Sk() != s3info.sk() ||
            s3adapter->GetS3Endpoint() != s3info.endpoint()) {
            s3adapter->Reinit(s3info.ak(), s3info.sk(), s3info.endpoint(),
                              s3adapterManager_->GetBasicS3AdapterOption());
        }
        Aws::String bucketName(s3info.bucketname().c_str(),
                               s3info.bucketname().size());
        if (s3adapter->GetBucketName() != bucketName) {
            s3adapter->SetBucketName(bucketName);
        }
        VLOG(6) << "s3compact: set s3info success, ak: " << s3info.ak()
                << ", sk: " << s3info.sk()
                << ", endpoint: " << s3info.endpoint()
                << ", bucket: " << s3info.bucketname();
    } else {
        VLOG(6) << "s3compact: fail to get s3info " << fsId;
        return;
    }
    VLOG(6) << "s3compact: set s3adapter " << s3info.ak() << ", " << s3info.sk()
            << ", " << s3info.endpoint() << ", " << s3info.bucketname();
    // 1. write new objs, each chunk one by one
    std::vector<std::string> objsAdded;
    ::google::protobuf::Map<uint64_t, S3ChunkInfoList> s3ChunkInfoAdd;
    ::google::protobuf::Map<uint64_t, S3ChunkInfoList> s3ChunkInfoRemove;
    std::vector<uint64_t> indexToDelete;
    for (const auto& index : needCompact) {
        VLOG(6) << "s3compact: currindex " << index;
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
        VLOG(6) << "s3compact: finish build valid list";
        // 1.2 gen obj names and read s3
        std::string fullChunk;
        uint64_t newChunkid = 0;
        uint64_t newCompaction = 0;
        int ret =
            ReadFullChunk(validList, fsId, inodeId, blockSize, chunkSize,
                          &fullChunk, &newChunkid, &newCompaction, s3adapter);
        if (ret != 0) {
            LOG(WARNING) << "S3Compact ReadFullChunk failed, index " << index;
            s3infoCache_->InvalidateS3Info(fsId);
            DeleteObjs(objsAdded, s3adapter);
            s3adapterManager->ReleaseS3Adapter(s3adapterIndex);
            return;
        }
        VLOG(6) << "s3compact: finish read full chunk, size: "
                << fullChunk.size();
        // 1.3 then write objs with newChunkid and newCompaction
        uint64_t newOff = validList.front().chunkoff;
        ret = WriteFullChunk(fullChunk, fsId, inodeId, blockSize, chunkSize,
                             newChunkid, newCompaction, newOff, &objsAdded,
                             s3adapter);
        if (ret != 0) {
            LOG(WARNING) << "S3Compact WriteFullChunk failed, index " << index;
            s3infoCache_->InvalidateS3Info(fsId);
            DeleteObjs(objsAdded, s3adapter);
            s3adapterManager->ReleaseS3Adapter(s3adapterIndex);
            return;
        }
        VLOG(6) << "s3compact: finish write full chunk";
        // 1.4 record add/delete
        // to add
        S3ChunkInfoList toAddList;
        S3ChunkInfo toAdd;
        toAdd.set_chunkid(newChunkid);
        toAdd.set_compaction(newCompaction);
        toAdd.set_offset(validList.begin()->begin);
        toAdd.set_len(fullChunk.length());
        toAdd.set_size(fullChunk.length());
        toAdd.set_zero(false);
        *toAddList.add_s3chunks() = std::move(toAdd);
        s3ChunkInfoAdd.insert({index, std::move(toAddList)});
        // to remove
        indexToDelete.emplace_back(index);
        s3ChunkInfoRemove.insert({index, origMap.at(index)});
    }

    // 2. update inode
    VLOG(6) << "s3compact: start update inode";
    if (!copysetNodeWrapper->IsValid()) return;
    ret = UpdateInode(copysetNodeWrapper->Get(), pinfo, inodeId,
                      std::move(s3ChunkInfoAdd), std::move(s3ChunkInfoRemove));
    if (ret != MetaStatusCode::OK) {
        LOG(WARNING) << "S3Compact UpdateInode failed, inodeKey = "
                     << inodeKey.fsId << "," << inodeKey.inodeId
                     << ", ret = " << MetaStatusCode_Name(ret);
        DeleteObjs(objsAdded, s3adapter);
        s3adapterManager->ReleaseS3Adapter(s3adapterIndex);
        return;
    }
    VLOG(6) << "s3compact: finish update inode";

    // 3. delete old objs
    VLOG(6) << "s3compact: start delete old objs";
    for (const auto& index : indexToDelete) {
        const auto& s3chunkinfolist = origMap.at(index);
        for (auto i = 0; i < s3chunkinfolist.s3chunks_size(); i++) {
            const auto& chunkinfo = s3chunkinfolist.s3chunks(i);
            uint64_t off = chunkinfo.offset();
            uint64_t len = chunkinfo.len();
            uint64_t offRoundDown = off / chunkSize * chunkSize;
            uint64_t startIndex = (off - offRoundDown) / blockSize;
            for (uint64_t index = startIndex;
                 offRoundDown + index * blockSize < off + len; index++) {
                std::string objName = curvefs::common::s3util::GenObjName(
                    chunkinfo.chunkid(), index, chunkinfo.compaction(), fsId,
                    inodeId);
                VLOG(6) << "s3compact: delete " << objName;
                const Aws::String aws_key(objName.c_str(), objName.size());
                int r = s3adapter->DeleteObject(
                    aws_key);  // don't care success or not
                if (r != 0)
                    VLOG(6) << "s3compact: delete obj " << objName << "failed.";
            }
        }
    }
    VLOG(6) << "s3compact: finish delete objs";
    s3adapterManager->ReleaseS3Adapter(s3adapterIndex);
    VLOG(6) << "s3compact: compact successfully";
}

}  // namespace metaserver
}  // namespace curvefs
