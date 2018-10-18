/*
 * Project: curve
 * Created Date: Friday September 14th 2018
 * Author: hzsunjianliang
 * Copyright (c) 2018 netease
 */

#ifndef  TEST_NAMESERVER2_MOCK_CHUNK_ALLOCATE_H_
#define  TEST_NAMESERVER2_MOCK_CHUNK_ALLOCATE_H_

#include <gmock/gmock.h>
#include "src/nameserver2/chunk_allocator.h"

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
#endif  // TEST_NAMESERVER2_MOCK_CHUNK_ALLOCATE_H_
