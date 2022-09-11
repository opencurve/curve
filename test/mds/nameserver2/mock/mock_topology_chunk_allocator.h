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
 * Created Date: Monday October 15th 2018
 * Author: hzsunjianliang
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
    MOCK_METHOD0(GetpoolUsagelimit, uint64_t());
};

}  // namespace mds
}  // namespace curve
#endif  // TEST_MDS_NAMESERVER2_MOCK_MOCK_TOPOLOGY_CHUNK_ALLOCATOR_H_
