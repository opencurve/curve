/*
 * Project: curve
 * Created Date: Tuesday September 11th 2018
 * Author: hzsunjianliang
 * Copyright (c) 2018 netease
 */

#ifndef SRC_MDS_NAMESERVER2_CHUNK_ALLOCATOR_H_
#define SRC_MDS_NAMESERVER2_CHUNK_ALLOCATOR_H_

#include <stdint.h>
#include <vector>
#include "src/mds/nameserver2/define.h"
#include "src/mds/nameserver2/inode_id_generator.h"
#include "src/mds/nameserver2/chunk_id_generator.h"
#include "src/mds/topology/topology_admin.h"

using ::curve::mds::topology::TopologyAdmin;

namespace curve {
namespace mds {

class ChunkSegmentAllocator {
 public:
    virtual ~ChunkSegmentAllocator() {}

    virtual bool AllocateChunkSegment(FileType type,
        SegmentSizeType segmentSize, ChunkSizeType chunkSize,
        offset_t offset, PageFileSegment *segment) = 0;
};


class ChunkSegmentAllocatorImpl: public ChunkSegmentAllocator {
 public:
    using CopysetIdInfo = ::curve::mds::topology::CopysetIdInfo;

    explicit ChunkSegmentAllocatorImpl(
                        std::shared_ptr<TopologyAdmin> topologyAdmin,
                        std::shared_ptr<ChunkIDGenerator> chunkIDGenerator) {
        topologyAdmin_ = topologyAdmin;
        chunkIDGenerator_ = chunkIDGenerator;
    }

    ~ChunkSegmentAllocatorImpl() {
        topologyAdmin_ = nullptr;
        chunkIDGenerator_ = nullptr;
    }

    bool AllocateChunkSegment(FileType type,
        SegmentSizeType segmentSize, ChunkSizeType chunkSize,
        offset_t offset, PageFileSegment *segment) override;

 private:
    std::shared_ptr<TopologyAdmin> topologyAdmin_;
    std::shared_ptr<ChunkIDGenerator> chunkIDGenerator_;
};

}  // namespace mds
}  // namespace curve
#endif   // SRC_MDS_NAMESERVER2_CHUNK_ALLOCATOR_H_
