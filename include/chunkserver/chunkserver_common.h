/*
 * Project: curve
 * Created Date: Thursday August 30th 2018
 * Author: tongguangxun
 * Copyright (c)￼ 2018 netease
 */

#ifndef INCLUDE_CHUNKSERVER_CHUNKSERVER_COMMON_H_
#define INCLUDE_CHUNKSERVER_CHUNKSERVER_COMMON_H_

#include <braft/configuration.h>
#include <braft/file_system_adaptor.h>

#include <cstdint>

namespace curve {
namespace chunkserver {

/* for IDs */
using LogicPoolID   = uint32_t;
using CopysetID     = uint32_t;
using ChunkID       = uint64_t;
using SnapshotID    = uint64_t;
using SequenceNum   = uint64_t;

using ChunkSizeType = uint32_t;
using PageSizeType = uint32_t;


// braft
using Configuration = braft::Configuration;
using GroupId = braft::GroupId;
using PeerId = braft::PeerId;
using Node = braft::Node;
using NodeOptions = braft::NodeOptions;
using FileSystemAdaptor = braft::FileSystemAdaptor;
using DirReader = braft::DirReader;
using PosixFileSystemAdaptor = braft::PosixFileSystemAdaptor;

}  // namespace chunkserver
}  // namespace curve

#endif  // INCLUDE_CHUNKSERVER_CHUNKSERVER_COMMON_H_
