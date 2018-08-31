/*
 * Project: curve
 * Created Date: 18-8-31
 * Author: wudemiao
 * Copyright (c) 2018 netease
 */

#ifndef CURVE_CHUNKSERVER_TYPE_H
#define CURVE_CHUNKSERVER_TYPE_H

#include <braft/configuration.h>
#include <braft/file_system_adaptor.h>

#include <cstdint>

namespace curve {
namespace chunkserver {

using LogicPoolID = uint16_t;
using CopysetID = uint32_t;

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

#endif  // CURVE_CHUNKSERVER_TYPE_H
