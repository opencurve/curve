/*
 * Project: curve
 * File Created: Tuesday, 19th March 2019 5:31:24 pm
 * Author: tongguangxun
 * Copyright (c)￼ 2018 netease
 */

#ifndef SRC_COMMON_CURVE_DEFINE_H_
#define SRC_COMMON_CURVE_DEFINE_H_

#include <butil/endpoint.h>
#include <butil/status.h>
#include <braft/configuration.h>
#include <unistd.h>


namespace curve {
namespace common {
// curve系统中共用的定义，对于各模块自己独有的放在各模块自己的define中
using ChunkID           = uint64_t;
using CopysetID         = uint32_t;
using ChunkIndex        = uint32_t;
using LogicPoolID       = uint32_t;
using ChunkServerID     = uint32_t;
using SnapshotID        = uint64_t;
using SequenceNum       = uint64_t;

using FileSeqType       = uint64_t;
using PageSizeType      = uint32_t;
using ChunkSizeType     = uint32_t;
using SegmentSizeType   = uint32_t;

using Status            = butil::Status;
using EndPoint          = butil::EndPoint;

using kKB               = 1024;
using kMB               = 1024*kKB;
using kGB               = 1024*kMB;
}  // namespace common
}  // namespace curve

#endif  // SRC_COMMON_CURVE_DEFINE_H_
