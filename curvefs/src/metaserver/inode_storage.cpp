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
 * Created Date: 2021-05-19
 * Author: chenwei
 */

#include "curvefs/src/metaserver/inode_storage.h"

#include <algorithm>
#include <vector>

namespace curvefs {
namespace metaserver {
MetaStatusCode MemoryInodeStorage::Insert(const Inode &inode) {
    WriteLockGuard writeLockGuard(rwLock_);
    auto it = inodeMap_.emplace(InodeKey(inode), inode);
    if (it.second == false) {
        return MetaStatusCode::INODE_EXIST;
    }
    return MetaStatusCode::OK;
}

MetaStatusCode MemoryInodeStorage::Get(const InodeKey &key, Inode *inode) {
    ReadLockGuard readLockGuard(rwLock_);
    auto it = inodeMap_.find(key);
    if (it == inodeMap_.end()) {
        return MetaStatusCode::NOT_FOUND;
    }
    *inode = it->second;
    return MetaStatusCode::OK;
}

MetaStatusCode MemoryInodeStorage::Delete(const InodeKey &key) {
    WriteLockGuard writeLockGuard(rwLock_);
    auto it = inodeMap_.find(key);
    if (it != inodeMap_.end()) {
        inodeMap_.erase(it);
        return MetaStatusCode::OK;
    }
    return MetaStatusCode::NOT_FOUND;
}

MetaStatusCode MemoryInodeStorage::Update(const Inode &inode) {
    WriteLockGuard writeLockGuard(rwLock_);
    auto it = inodeMap_.find(InodeKey(inode));
    if (it == inodeMap_.end()) {
        return MetaStatusCode::NOT_FOUND;
    }
    if (inode.s3chunkinfomap().empty()) {
        inodeMap_[InodeKey(inode)] = inode;
    } else {
        ::google::protobuf::Map<uint64_t, S3ChunkInfoList> result;
        const auto& mapA = inode.s3chunkinfomap();
        const auto& mapB = it->second.s3chunkinfomap();
        MergeTwoS3ChunkInfoMap(mapA, mapB, &result);
        Inode newInode(inode);
        *newInode.mutable_s3chunkinfomap() = std::move(result);
        inodeMap_[InodeKey(inode)] = newInode;
    }
    return MetaStatusCode::OK;
}

int MemoryInodeStorage::Count() {
    ReadLockGuard readLockGuard(rwLock_);
    return inodeMap_.size();
}

InodeStorage::ContainerType* MemoryInodeStorage::GetContainer() {
    return &inodeMap_;
}

void MemoryInodeStorage::MergeTwoS3ChunkInfoMap(
    const ::google::protobuf::Map<uint64_t, S3ChunkInfoList>& mapA,
    const ::google::protobuf::Map<uint64_t, S3ChunkInfoList>& mapB,
    ::google::protobuf::Map<uint64_t, S3ChunkInfoList>* result) {
    assert(!mapA.empty());
    assert(!mapB.empty());
    // base map indices should always contains extra's all indices
    const auto& baseMap = mapA.size() > mapB.size() ? mapA : mapB;
    const auto& extraMap = mapA.size() <= mapB.size() ? mapA : mapB;
    for (const auto& item : baseMap) {
        uint64_t index = item.first;
        if (extraMap.find(index) == extraMap.end()) {
            result->insert({index, baseMap.at(index)});
            continue;
        }

        S3ChunkInfoList validList;
        const auto& listA = baseMap.at(index);
        const auto& listB = extraMap.at(index);
        auto getMinChunkId = [](const S3ChunkInfoList& l) -> uint64_t {
            assert(l.s3chunks_size());
            uint64_t minChunkId = l.s3chunks(0).chunkid();
            for (auto i = 1; i < l.s3chunks_size(); i++) {
                uint64_t chunkid = l.s3chunks(i).chunkid();
                minChunkId = chunkid < minChunkId ? chunkid : minChunkId;
            }
            return minChunkId;
        };
        // max chunkid of two min chunkid
        uint64_t minChunkId =
            std::max(getMinChunkId(listA), getMinChunkId(listB));
        std::vector<uint64_t> added;
        auto add = [&](const S3ChunkInfoList& l) {
            for (auto i = 0; i < l.s3chunks_size(); i++) {
                auto s3chunkA = l.s3chunks(i);
                uint64_t chunkid = s3chunkA.chunkid();
                if (chunkid > minChunkId) {
                    if (std::find(added.begin(), added.end(), chunkid) ==
                        added.end()) {
                        auto ref = validList.add_s3chunks();
                        *ref = s3chunkA;
                        added.emplace_back(std::move(chunkid));
                    }
                } else if (chunkid == minChunkId) {
                    // resolve situation, 2 different chunk with same minchunkid
                    // compact: from [1, 2, 3] to [3]
                    // orig: [1, 2, 3, 4]
                    auto it = std::find(added.begin(), added.end(), chunkid);
                    if (it == added.end()) {
                        auto ref = validList.add_s3chunks();
                        *ref = s3chunkA;
                        added.emplace_back(std::move(chunkid));
                    } else {
                        auto s3chunks = validList.mutable_s3chunks();
                        auto s3chunkBIt =
                            std::find_if(s3chunks->begin(), s3chunks->end(),
                                         [&](const S3ChunkInfo& c) {
                                             return c.chunkid() == chunkid;
                                         });
                        // compare compaction
                        if (s3chunkA.compaction() > s3chunkBIt->compaction()) {
                            *s3chunkBIt = s3chunkA;
                        }
                    }
                }
            }
        };
        add(listA);
        add(listB);
        result->insert({index, std::move(validList)});
    }
}

}  // namespace metaserver
}  // namespace curvefs
