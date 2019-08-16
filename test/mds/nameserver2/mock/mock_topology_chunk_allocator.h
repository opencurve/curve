/*
 * Project: curve
 * Created Date: Monday October 15th 2018
 * Author: hzsunjianliang
 * Copyright (c) 2018 netease
 */

#ifndef TEST_MDS_NAMESERVER2_MOCK_MOCK_TOPOLOGY_CHUNK_ALLOCATOR_H_
#define TEST_MDS_NAMESERVER2_MOCK_MOCK_TOPOLOGY_CHUNK_ALLOCATOR_H_

#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <vector>
#include "src/mds/topology/topology_chunk_allocator.h"

using ::curve::mds::topology::TopologyChunkAllocator;

namespace curve {
namespace mds {

class  MockTopologyChunkAllocator: public TopologyChunkAllocator {
 public:
     using CopysetIdInfo = ::curve::mds::topology::CopysetIdInfo;

    ~MockTopologyChunkAllocator() {}
    MOCK_METHOD4(AllocateChunkRandomInSingleLogicalPool,
        bool(FileType, uint32_t,
            ChunkSizeType chunkSize, std::vector<CopysetIdInfo> *));

    MOCK_METHOD4(AllocateChunkRoundRobinInSingleLogicalPool,
        bool(FileType, uint32_t,
            ChunkSizeType chunkSize, std::vector<CopysetIdInfo> *));
};

}  // namespace mds
}  // namespace curve
#endif  // TEST_MDS_NAMESERVER2_MOCK_MOCK_TOPOLOGY_CHUNK_ALLOCATOR_H_
