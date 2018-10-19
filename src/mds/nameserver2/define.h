/*
 * Project: curve
 * Created Date: Tuesday September 11th 2018
 * Author: hzsunjianliang
 * Copyright (c) 2018 netease
 */

#ifndef SRC_MDS_NAMESERVER2_DEFINE_H_
#define SRC_MDS_NAMESERVER2_DEFINE_H_

#include <stdint.h>

namespace curve {
namespace mds {

typedef uint64_t InodeID;
typedef uint64_t ChunkID;

typedef  uint64_t SnapShotID;

const uint64_t kKB = 1024;
const uint64_t kMB = 1024*kKB;
const uint64_t kGB = 1024*kMB;

const uint64_t DefaultChunkSize = 16 * kMB;
const uint64_t DefaultSegmentSize = kGB * 1;
const uint64_t kMiniFileLength = DefaultSegmentSize * 10;

typedef  uint64_t offset_t;
typedef uint16_t LogicalPoolID;
typedef uint32_t SegmentSizeType;
typedef uint32_t ChunkSizeType;

}  // namespace mds
}  // namespace curve

#endif   // SRC_MDS_NAMESERVER2_DEFINE_H_
