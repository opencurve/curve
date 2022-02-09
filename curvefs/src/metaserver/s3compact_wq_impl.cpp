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

void S3CompactWorkQueueImpl::Enqueue(std::shared_ptr<InodeManager> inodeManager,
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
    struct S3CompactTask t {
        inodeManager, inodeKey, pinfo,
            std::make_shared<CopysetNodeWrapper>(copysetNode)
    };
    auto task = std::bind(&S3CompactWorkQueueImpl::CompactChunks, this, t);
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
        VLOG(9) << "s3compact: delete " << obj;
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

    VLOG(9) << "s3compact: list s3chunkinfo list";
    for (auto i = 0; i < s3chunkinfolist.s3chunks_size(); i++) {
        auto chunkinfo = s3chunkinfolist.s3chunks(i);
        // print s3chunkinfolist
        struct Node n {
            chunkinfo.offset(), chunkinfo.offset() + chunkinfo.len() - 1,
                chunkinfo.chunkid(), chunkinfo.compaction(), chunkinfo.offset(),
                chunkinfo.len(), chunkinfo.zero()
        };
        VLOG(9) << "chunkid: " << chunkinfo.chunkid()
                << ", offset:" << chunkinfo.offset()
                << ", len:" << chunkinfo.len()
                << ", compaction:" << chunkinfo.compaction()
                << ", zero: " << chunkinfo.zero();
        addToValidList(std::move(n));
    }

    // merge same chunkid continuous nodes
    for (auto curr = validList.begin();;) {
        auto next = std::next(curr);
        if (next == validList.end()) break;
        if (curr->end == next->begin - 1) {
            if (curr->chunkid == next->chunkid &&
                curr->compaction == next->compaction) {
                next->begin = curr->begin;
                validList.erase(curr);
            }
        }
        curr = next;
    }
    return validList;
}

void S3CompactWorkQueueImpl::GenS3ReadRequests(
    const struct S3CompactCtx& ctx, const std::list<struct Node>& validList,
    std::vector<struct S3Request>* reqs, struct S3NewChunkInfo* newChunkInfo) {
    int reqIndex = 0;
    uint64_t newChunkId = 0;
    uint64_t newCompaction = 0;
    for (auto curr = validList.begin(); curr != validList.end();) {
        auto next = std::next(curr);
        if (curr->zero) {
            reqs->emplace_back(reqIndex++, true, "", 0,
                               curr->end - curr->begin + 1);
            curr = next;
            continue;
        }

        const auto& blockSize = ctx.blockSize;
        const auto& chunkSize = ctx.chunkSize;
        uint64_t beginRoundDown = curr->begin / chunkSize * chunkSize;
        uint64_t startIndex = (curr->begin - beginRoundDown) / blockSize;
        for (uint64_t index = startIndex;
             beginRoundDown + index * blockSize <= curr->end; index++) {
            // read the block obj
            std::string objName = curvefs::common::s3util::GenObjName(
                curr->chunkid, index, curr->compaction, ctx.fsId, ctx.inodeId);
            uint64_t s3objBegin =
                std::max(curr->chunkoff, beginRoundDown + index * blockSize);
            uint64_t s3objEnd =
                std::min(curr->chunkoff + curr->chunklen - 1,
                         beginRoundDown + (index + 1) * blockSize - 1);
            if (curr->begin >= s3objBegin && curr->end <= s3objEnd) {
                // all what we need is only part of block
                reqs->emplace_back(reqIndex++, false, std::move(objName),
                                   curr->begin - s3objBegin,
                                   curr->end - curr->begin + 1);
            } else if (curr->begin >= s3objBegin && curr->end > s3objEnd) {
                // not last block, what we need is part of block
                reqs->emplace_back(reqIndex++, false, std::move(objName),
                                   curr->begin - s3objBegin,
                                   s3objEnd - curr->begin + 1);
            } else if (curr->begin < s3objBegin && curr->end > s3objEnd) {
                // what we need is full block
                reqs->emplace_back(reqIndex++, false, std::move(objName), 0,
                                   blockSize);
            } else if (curr->begin < s3objBegin && curr->end <= s3objEnd) {
                // last block, what we need is part of block
                reqs->emplace_back(reqIndex++, false, std::move(objName), 0,
                                   curr->end - s3objBegin + 1);
                break;
            }
        }

        if (curr->chunkid >= newChunkId) {
            newChunkId = curr->chunkid;
            newCompaction = curr->compaction;
        }
        if (next != validList.end() && curr->end + 1 < next->begin) {
            // hole, append 0
            reqs->emplace_back(reqIndex++, true, "", 0,
                               next->begin - curr->end - 1);
        }
        curr = next;
    }
    // inc compaction
    newCompaction += 1;
    newChunkInfo->newChunkId = newChunkId;
    newChunkInfo->newOff = validList.front().chunkoff;
    newChunkInfo->newCompaction = newCompaction;
}

int S3CompactWorkQueueImpl::ReadFullChunk(
    const struct S3CompactCtx& ctx, const std::list<struct Node>& validList,
    std::string* fullChunk, struct S3NewChunkInfo* newChunkInfo) {
    std::vector<struct S3Request> s3reqs;
    std::vector<std::string> readContent;
    // generate s3request first
    GenS3ReadRequests(ctx, validList, &s3reqs, newChunkInfo);
    VLOG(9) << "s3compact: s3 request generated";
    for (const auto& s3req : s3reqs) {
        VLOG(9) << "index:" << s3req.reqIndex << ", zero:" << s3req.zero
                << ", s3objname:" << s3req.objName << ", off:" << s3req.off
                << ", len:" << s3req.len;
    }
    readContent.resize(s3reqs.size());
    std::unordered_map<std::string, std::vector<struct S3Request*>> objReqs;
    for (auto& req : s3reqs) {
        if (req.zero) {
            objReqs["zero"].emplace_back(&req);
        } else {
            objReqs[req.objName].emplace_back(&req);
        }
    }
    // process zero first and will not fail
    if (objReqs.find("zero") != objReqs.end()) {
        for (const auto& req : objReqs["zero"]) {
            readContent[req->reqIndex] = std::string(req->len, '\0');
        }
        objReqs.erase("zero");
    }
    // read and process objs one by one
    uint64_t retry = 0;
    for (auto it = objReqs.begin(); it != objReqs.end(); it++) {
        std::string buf;
        const std::string& objName = it->first;
        const auto& reqs = it->second;
        const Aws::String aws_key(objName.c_str(), objName.size());
        const auto maxRetry = opts_.s3ReadMaxRetry;
        const auto retryInterval = opts_.s3ReadRetryInterval;
        while (retry <= maxRetry) {
            // why we need retry
            // if you enable client's diskcache,
            // metadata may be newer than data in s3
            // which means you cannot read data from s3
            // we have to wait data to be flushed to s3
            int ret = ctx.s3adapter->GetObject(aws_key, &buf);
            if (ret != 0) {
                LOG(WARNING)
                    << "s3compact: get s3 obj " << objName << " failed";
                if (retry == maxRetry) return -1;  // no chance
                retry++;
                LOG(WARNING) << "s3compact: will retry after " << retryInterval
                             << " seconds, current retry time:" << retry;
                std::this_thread::sleep_for(
                    std::chrono::seconds(retryInterval));
                continue;
            }
            for (const auto& req : reqs) {
                readContent[req->reqIndex] = buf.substr(req->off, req->len);
            }
            break;
        }
    }

    // merge all read content
    for (auto content : readContent) {
        (*fullChunk) += std::move(content);
    }

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
    request.set_returns3chunkinfomap(false);
    request.set_froms3compaction(true);
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
    const struct S3CompactCtx& ctx, const struct S3NewChunkInfo& newChunkInfo,
    const std::string& fullChunk, std::vector<std::string>* objsAdded) {
    uint64_t chunkLen = fullChunk.length();
    const auto& blockSize = ctx.blockSize;
    const auto& chunkSize = ctx.chunkSize;
    const auto& newOff = newChunkInfo.newOff;
    uint64_t offRoundDown = newOff / chunkSize * chunkSize;
    uint64_t startIndex = (newOff - newOff / chunkSize * chunkSize) / blockSize;
    for (uint64_t index = startIndex;
         index * blockSize + offRoundDown < newOff + chunkLen; index += 1) {
        std::string objName = curvefs::common::s3util::GenObjName(
            newChunkInfo.newChunkId, index, newChunkInfo.newCompaction,
            ctx.fsId, ctx.inodeId);
        const Aws::String aws_key(objName.c_str(), objName.size());
        int ret;
        uint64_t s3objBegin =
            std::max(newOff, offRoundDown + index * blockSize);
        uint64_t s3objEnd = std::min(
            newOff + chunkLen - 1, offRoundDown + (index + 1) * blockSize - 1);
        VLOG(9) << "s3compact: put " << objName << ", [" << s3objBegin << "-"
                << s3objEnd << "]";
        ret = ctx.s3adapter->PutObject(
            aws_key,
            fullChunk.substr(s3objBegin - newOff, s3objEnd - s3objBegin + 1));
        if (ret != 0) {
            LOG(WARNING) << "s3compact: put s3 object " << objName << " failed";
            return ret;
        } else {
            objsAdded->emplace_back(std::move(objName));
        }
    }
    return 0;
}

bool S3CompactWorkQueueImpl::CompactPrecheck(
    const struct S3CompactTask& task, Inode* inode,
    std::vector<uint64_t>* needCompact) {
    // am i copysetnode leader?
    if (!task.copysetNodeWrapper->IsLeaderTerm()) {
        VLOG(6) << "s3compact: i am not the leader, finish";
        return false;
    }

    // inode exist?
    MetaStatusCode ret = task.inodeManager->GetInode(
        task.inodeKey.fsId, task.inodeKey.inodeId, inode);
    if (ret != MetaStatusCode::OK) {
        LOG(WARNING) << "s3compact: GetInode fail, inodeKey = "
                     << task.inodeKey.fsId << "," << task.inodeKey.inodeId
                     << ", ret = " << MetaStatusCode_Name(ret);
        return false;
    }

    // deleted?
    if (inode->nlink() == 0) {
        VLOG(6) << "s3compact: inode is already deleted";
        return false;
    }

    // s3chunkinfomap size 0?
    if (inode->s3chunkinfomap().size() == 0) {
        VLOG(6) << "s3compact: empty s3chunkinfomap";
        return false;
    }
    // need compact?
    *needCompact = GetNeedCompact(inode->s3chunkinfomap());
    if (needCompact->empty()) {
        VLOG(6) << "s3compact: no need to compact " << inode->inodeid();
        return false;
    }

    // pass
    return true;
}

S3Adapter* S3CompactWorkQueueImpl::SetupS3Adapter(uint64_t fsId,
                                                  uint64_t* s3adapterIndex,
                                                  uint64_t* blockSize,
                                                  uint64_t* chunkSize) {
    auto pairResult = s3adapterManager_->GetS3Adapter();
    *s3adapterIndex = pairResult.first;
    auto s3adapter = pairResult.second;
    if (s3adapter == nullptr) {
        LOG(WARNING) << "s3compact: fail to get s3adapter";
        return nullptr;
    }

    S3Info s3info;
    int status = s3infoCache_->GetS3Info(fsId, &s3info);
    if (status == 0) {
        *blockSize = s3info.blocksize();
        *chunkSize = s3info.chunksize();
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
        LOG(WARNING) << "s3compact: fail to get s3info of " << fsId;
        return nullptr;
    }
    VLOG(6) << "s3compact: set s3adapter " << s3info.ak() << ", " << s3info.sk()
            << ", " << s3info.endpoint() << ", " << s3info.bucketname();
    return s3adapter;
}

void S3CompactWorkQueueImpl::CompactChunk(
    const struct S3CompactCtx& compactCtx, uint64_t index, const Inode& inode,
    std::unordered_map<uint64_t, std::vector<std::string>>* objsAddedMap,
    ::google::protobuf::Map<uint64_t, S3ChunkInfoList>* s3ChunkInfoAdd,
    ::google::protobuf::Map<uint64_t, S3ChunkInfoList>* s3ChunkInfoRemove) {
    auto cleanup = absl::MakeCleanup(
        [&]() { VLOG(6) << "s3compact: exit index " << index; });
    VLOG(6) << "s3compact: begin to compact index " << index;
    const auto& s3chunkinfolist = inode.s3chunkinfomap().at(index);
    // 1.1 build valid list
    std::list<struct S3CompactWorkQueueImpl::Node> validList(
        BuildValidList(s3chunkinfolist, inode.length()));
    VLOG(6) << "s3compact: finish build valid list";
    VLOG(9) << "s3compact: show valid list";
    for (const auto& node : validList) {
        VLOG(9) << "[" << node.begin << "-" << node.end
                << "], chunkid:" << node.chunkid
                << ", chunkoff:" << node.chunkoff
                << ", chunklen:" << node.chunklen << ", zero:" << node.zero;
    }
    // 1.2  first read full chunk
    struct S3NewChunkInfo newChunkInfo;
    std::string fullChunk;
    int ret = ReadFullChunk(compactCtx, validList, &fullChunk, &newChunkInfo);
    if (ret != 0) {
        LOG(WARNING) << "s3compact: ReadFullChunk failed, index " << index;
        s3infoCache_->InvalidateS3Info(
            compactCtx.fsId);  // maybe s3info changed?
        return;
    }
    VLOG(6) << "s3compact: finish read full chunk, size: " << fullChunk.size();
    VLOG(6) << "s3compact: new s3chunk info will be id:"
            << newChunkInfo.newChunkId << ", off:" << newChunkInfo.newOff
            << ", compaction:" << newChunkInfo.newCompaction;
    // 1.3 then write objs with newChunkid and newCompaction
    std::vector<std::string> objsAdded;
    ret = WriteFullChunk(compactCtx, newChunkInfo, fullChunk, &objsAdded);
    if (ret != 0) {
        LOG(WARNING) << "s3compact: WriteFullChunk failed, index " << index;
        s3infoCache_->InvalidateS3Info(
            compactCtx.fsId);  // maybe s3info changed?
        DeleteObjs(objsAdded, compactCtx.s3adapter);
        return;
    }
    VLOG(6) << "s3compact: finish write full chunk";
    // 1.4 record add/delete
    objsAddedMap->emplace(index, std::move(objsAdded));
    // to add
    S3ChunkInfoList toAddList;
    S3ChunkInfo toAdd;
    toAdd.set_chunkid(newChunkInfo.newChunkId);
    toAdd.set_compaction(newChunkInfo.newCompaction);
    toAdd.set_offset(newChunkInfo.newOff);
    toAdd.set_len(fullChunk.length());
    toAdd.set_size(fullChunk.length());
    toAdd.set_zero(false);
    *toAddList.add_s3chunks() = std::move(toAdd);
    s3ChunkInfoAdd->insert({index, std::move(toAddList)});
    // to remove
    s3ChunkInfoRemove->insert({index, s3chunkinfolist});
}

void S3CompactWorkQueueImpl::DeleteObjsOfS3ChunkInfoList(
    const struct S3CompactCtx& ctx, const S3ChunkInfoList& s3chunkinfolist) {
    for (auto i = 0; i < s3chunkinfolist.s3chunks_size(); i++) {
        const auto& chunkinfo = s3chunkinfolist.s3chunks(i);
        uint64_t off = chunkinfo.offset();
        uint64_t len = chunkinfo.len();
        uint64_t offRoundDown = off / ctx.chunkSize * ctx.chunkSize;
        uint64_t startIndex = (off - offRoundDown) / ctx.blockSize;
        for (uint64_t index = startIndex;
             offRoundDown + index * ctx.blockSize < off + len; index++) {
            std::string objName = curvefs::common::s3util::GenObjName(
                chunkinfo.chunkid(), index, chunkinfo.compaction(), ctx.fsId,
                ctx.inodeId);
            VLOG(6) << "s3compact: delete " << objName;
            const Aws::String aws_key(objName.c_str(), objName.size());
            int r = ctx.s3adapter->DeleteObject(
                aws_key);  // don't care success or not
            if (r != 0)
                VLOG(6) << "s3compact: delete obj " << objName << "failed.";
        }
    }
}

void S3CompactWorkQueueImpl::CompactChunks(const struct S3CompactTask& task) {
    auto cleanup = absl::MakeCleanup([this, task]() {
        std::lock_guard<std::mutex> guard(mutex_);
        auto it = std::find(compactingInodes_.begin(), compactingInodes_.end(),
                            task.inodeKey);
        if (it != compactingInodes_.end()) {
            compactingInodes_.erase(it);
        }
        VLOG(6) << "s3compact: exit compaction";
    });

    VLOG(6) << "s3compact: try to compact, fsId: " << task.inodeKey.fsId
            << " , inodeId: " << task.inodeKey.inodeId;

    Inode inode;
    std::vector<uint64_t> needCompact;
    if (!CompactPrecheck(task, &inode, &needCompact)) return;
    uint64_t fsId = inode.fsid();
    uint64_t inodeId = inode.inodeid();

    // let's compact
    // 0. get s3adapter&s3info, set s3adapter
    // include ak, sk, addr, bucket, blocksize, chunksize
    uint64_t blockSize, chunkSize;
    uint64_t s3adapterIndex;
    S3Adapter* s3adapter = SetupS3Adapter(task.inodeKey.fsId, &s3adapterIndex,
                                          &blockSize, &chunkSize);
    if (s3adapter == nullptr) return;

    // 1. read full chunk & write new objs, each chunk one by one
    struct S3CompactCtx compactCtx {
        task.inodeKey.inodeId, task.inodeKey.fsId, task.pinfo, blockSize,
            chunkSize, s3adapterIndex, s3adapter
    };
    std::unordered_map<uint64_t, std::vector<std::string>> objsAddedMap;
    ::google::protobuf::Map<uint64_t, S3ChunkInfoList> s3ChunkInfoAdd;
    ::google::protobuf::Map<uint64_t, S3ChunkInfoList> s3ChunkInfoRemove;
    std::vector<uint64_t> indexToDelete;
    VLOG(6) << "s3compact: begin to compact fsId:" << fsId
            << ", inodeId:" << inodeId;
    for (const auto& index : needCompact) {
        // sort s3chunks to make small chunkid -> big chunkid
        // bigger chunkid means newer data
        auto s3chunkVec =
            inode.mutable_s3chunkinfomap()->at(index).mutable_s3chunks();
        std::sort(s3chunkVec->begin(), s3chunkVec->end(),
                  [](const S3ChunkInfo& a, const S3ChunkInfo& b) {
                      return a.chunkid() < b.chunkid();
                  });
        CompactChunk(compactCtx, index, inode, &objsAddedMap, &s3ChunkInfoAdd,
                     &s3ChunkInfoRemove);
    }
    if (s3ChunkInfoAdd.empty()) {
        VLOG(6) << "s3compact: do nothing to metadata";
        s3adapterManager_->ReleaseS3Adapter(s3adapterIndex);
        return;
    }

    // 2. update inode
    VLOG(6) << "s3compact: start update inode";
    if (!task.copysetNodeWrapper->IsValid()) {
        VLOG(6) << "s3compact: invalid copysetNode";
        s3adapterManager_->ReleaseS3Adapter(s3adapterIndex);
        return;
    }
    auto ret =
        UpdateInode(task.copysetNodeWrapper->Get(), compactCtx.pinfo, inodeId,
                    std::move(s3ChunkInfoAdd), std::move(s3ChunkInfoRemove));
    if (ret != MetaStatusCode::OK) {
        LOG(WARNING) << "s3compact: UpdateInode failed, inodeKey = "
                     << compactCtx.fsId << "," << compactCtx.inodeId
                     << ", ret = " << MetaStatusCode_Name(ret);
        for (const auto& item : objsAddedMap) {
            DeleteObjs(item.second, s3adapter);
        }
        s3adapterManager_->ReleaseS3Adapter(s3adapterIndex);
        return;
    }
    VLOG(6) << "s3compact: finish update inode";

    // 3. delete old objs
    VLOG(6) << "s3compact: start delete old objs";
    for (const auto& item : objsAddedMap) {
        const auto& l = inode.s3chunkinfomap().at(item.first);
        DeleteObjsOfS3ChunkInfoList(compactCtx, l);
    }
    VLOG(6) << "s3compact: finish delete objs";
    s3adapterManager_->ReleaseS3Adapter(s3adapterIndex);
    VLOG(6) << "s3compact: compact successfully";
}

}  // namespace metaserver
}  // namespace curvefs
