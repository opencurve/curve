/*
 *  Copyright (c) 2020 NetEase Inc.
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
 * Created Date: Tuesday September 11th 2018
 * Author: hzsunjianliang
 */

#ifndef SRC_MDS_NAMESERVER2_CHUNK_ALLOCATOR_H_
#define SRC_MDS_NAMESERVER2_CHUNK_ALLOCATOR_H_

#include <stdint.h>
#include <vector>
#include <map>
#include <string>
#include <memory>
#include "src/mds/common/mds_define.h"
#include "src/mds/nameserver2/idgenerator/chunk_id_generator.h"
#include "src/mds/topology/topology_chunk_allocator.h"

using ::curve::mds::topology::TopologyChunkAllocator;

namespace curve {
namespace mds {

class ChunkSegmentAllocator {
 public:
    virtual ~ChunkSegmentAllocator() {}

    virtual bool AllocateChunkSegment(FileType type,
                                      SegmentSizeType segmentSize,
                                      ChunkSizeType chunkSize,
                                      const std::string& pstName,
                                      offset_t offset,
                                      PageFileSegment* segment) = 0;

    virtual bool CloneChunkSegment(const PageFileSegment &srcSegment,
        PageFileSegment *segment) = 0;

    virtual void GetRemainingSpaceInLogicalPool(
        const std::vector<PoolIdType>& logicalPools,
        std::map<PoolIdType, double>* remianingSpace,
        const std::string& pstName) = 0;
};

class ChunkSegmentAllocatorImpl: public ChunkSegmentAllocator {
 public:
    using CopysetIdInfo = ::curve::mds::topology::CopysetIdInfo;

    explicit ChunkSegmentAllocatorImpl(
                        std::shared_ptr<TopologyChunkAllocator> topologyAdmin,
                        std::shared_ptr<ChunkIDGenerator> chunkIDGenerator) {
        topologyChunkAllocator_ = topologyAdmin;
        chunkIDGenerator_ = chunkIDGenerator;
    }

    bool AllocateChunkSegment(FileType type,
        SegmentSizeType segmentSize, ChunkSizeType chunkSize,
        const std::string& pstName, offset_t offset,
        PageFileSegment *segment) override;

    void GetRemainingSpaceInLogicalPool(
        const std::vector<PoolIdType>& logicalPools,
        std::map<PoolIdType, double>* remianingSpace,
        const std::string& pstName) override {
            return topologyChunkAllocator_->GetRemainingSpaceInLogicalPool(
                            logicalPools, remianingSpace, pstName);
        }

    bool CloneChunkSegment(const PageFileSegment &srcSegment,
        PageFileSegment *segment) override;

 private:
    std::shared_ptr<TopologyChunkAllocator> topologyChunkAllocator_;
    std::shared_ptr<ChunkIDGenerator> chunkIDGenerator_;
};

}  // namespace mds
}  // namespace curve
#endif   // SRC_MDS_NAMESERVER2_CHUNK_ALLOCATOR_H_
