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
 * Created Date: Friday September 14th 2018
 * Author: hzsunjianliang
 */

#ifndef  TEST_MDS_NAMESERVER2_MOCK_MOCK_CHUNK_ALLOCATE_H_
#define  TEST_MDS_NAMESERVER2_MOCK_MOCK_CHUNK_ALLOCATE_H_

#include <gmock/gmock.h>
#include "src/mds/nameserver2/chunk_allocator.h"

namespace curve {
namespace mds {
class MockChunkAllocator: public ChunkSegmentAllocator {
 public:
    ~MockChunkAllocator() {}
    MOCK_METHOD4(AllocateChunkSegment, bool(SegmentSizeType,
      ChunkSizeType, offset_t, PageFileSegment*));

    MOCK_METHOD5(AllocateChunkSegment, bool(FileType, SegmentSizeType,
      ChunkSizeType, offset_t, PageFileSegment*));
};
}  // namespace mds
}  // namespace curve
#endif  // TEST_MDS_NAMESERVER2_MOCK_MOCK_CHUNK_ALLOCATE_H_
