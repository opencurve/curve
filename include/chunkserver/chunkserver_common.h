/*
 * Project: curve
 * Created Date: Thursday August 30th 2018
 * Author: tongguangxun
 * Copyright (c)￼ 2018 netease
 */

#ifndef CURVE_CHUNKSERVER_DEFINITIONS_H
#define CURVE_CHUNKSERVER_DEFINITIONS_H

#include <cstdint>

namespace curve {
namespace chunkserver {

/* for IDs */
using LogicPoolID   = uint16_t;
using CopysetID     = uint32_t;
using ChunkID       = uint64_t;
using SnapshotID    = uint16_t;

}  // namespace chunkserver
}  // namespace curve

#endif  // !CURVE_CHUNKSERVER_DEFINITIONS_H
